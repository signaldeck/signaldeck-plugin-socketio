from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import request
from flask_socketio import emit, join_room, leave_room

from .service import SCHEMA_VERSION, SocketIOService

_registered = False


def _as_dict(
    data: Any,
) -> dict | None:
    if isinstance(data, dict):
        return data

    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return None

        return parsed if isinstance(parsed, dict) else None

    return None


def _parse_history_sync(data: dict):
    """Parse the optional, type-scoped join_room history configuration.

    None means a legacy join (unchanged history behavior).
    A nonempty list means paged DB-backed synchronization.
    """
    if "sync" not in data:
        return None
    requests = data["sync"]
    if not isinstance(requests, list) or not 1 <= len(requests) <= 16:
        raise ValueError("sync must contain between 1 and 16 type requests")
    result = []
    seen_types = set()
    for request in requests:
        if not isinstance(request, dict):
            raise ValueError("Each sync entry must be an object")
        event_type = request.get("type")
        days = request.get("days")
        if not isinstance(event_type, str) or not 1 <= len(event_type.strip()) <= 128:
            raise ValueError("sync type must be a nonempty string")
        event_type = event_type.strip()
        if event_type in seen_types:
            raise ValueError("Duplicate sync type")
        if days is not None and (type(days) is not int or not 1 <= days <= 36500):
            raise ValueError("sync days must be a positive integer or null")
        seen_types.add(event_type)
        result.append((event_type, days))
    return result


def register_socketio_events(
    socketio,
    service: SocketIOService,
    context=None,
) -> None:
    global _registered

    if _registered:
        return

    _registered = True

    logger = (
        getattr(context, "logger", None)
        or logging.getLogger(__name__)
    )
    registry = service.registry

    def send_filtered_history(sid, room, requests, sync_id, page_size):
        """Stream filtered history to the joining client in bounded pages.

        Emission uses the same 'missed_events' event as the legacy protocol;
        completion/error is signalled separately so a client can distinguish
        an empty history from an unsuccessful transfer.
        """
        counts = {}
        try:
            for event_type, days in requests:
                date_from = (
                    datetime.now(timezone.utc) - timedelta(days=days)
                    if days is not None else None
                )
                count = 0
                cursor = None
                while True:
                    if room not in registry.rooms_for_sid(sid):
                        return  # Client disconnected or left the room.
                    events, next_cursor = service.get_history_page(
                        room=room,
                        event_type=event_type,
                        days=days,
                        date_from=date_from,
                        limit=page_size,
                        cursor=cursor,
                    )
                    if events:
                        socketio.emit("missed_events", events, to=sid)
                        count += len(events)
                    if next_cursor is None:
                        break
                    if next_cursor == cursor:
                        raise RuntimeError("History pagination cursor did not advance")
                    cursor = next_cursor
                    socketio.sleep(0)
                counts[event_type] = count
            if room in registry.rooms_for_sid(sid):
                socketio.emit("history_sync_complete", {
                    "ok": True,
                    "room": room,
                    "syncId": sync_id,
                    "counts": counts,
                }, to=sid)
        except Exception:
            logger.exception("Type-filtered history sync failed for room=%s", room)
            if room in registry.rooms_for_sid(sid):
                socketio.emit("history_sync_complete", {
                    "ok": False,
                    "room": room,
                    "syncId": sync_id,
                    "error": "history_sync_failed",
                }, to=sid)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @socketio.on("connect")
    def handle_connect(auth):
        if not isinstance(auth, dict):
            logger.warning(
                "Rejected Socket.IO connection without auth payload"
            )
            return False

        user_id = auth.get("id")
        username = auth.get("username")

        if user_id is None or str(user_id).strip() == "":
            logger.warning(
                "Rejected Socket.IO connection without user id"
            )
            return False

        sid = request.sid
        old_sid = registry.connect(
            user_id,
            sid,
            username,
        )

        if old_sid and old_sid != sid:
            try:
                socketio.server.disconnect(
                    old_sid,
                    namespace="/",
                )
            except Exception:
                logger.exception(
                    "Could not disconnect superseded Socket.IO session"
                )

        client = registry.get_by_sid(sid)

        logger.info(
            "Socket.IO client connected: user=%s sid=%s",
            client["name"] if client else user_id,
            sid,
        )

    @socketio.on("disconnect")
    def handle_disconnect(reason=None):
        sid = request.sid
        client = registry.get_by_sid(sid)
        rooms = registry.rooms_for_sid(sid)

        if client is not None:
            for room in rooms:
                emit(
                    "event",
                    service.create_system_event(
                        room=room,
                        kind="room.left",
                        text=(
                            f'{client["name"]} hat den Raum '
                            f'{room} verlassen'
                        ),
                        actor_id=client["id"],
                        actor_name=client["name"],
                    ),
                    room=room,
                    include_self=False,
                )

        disconnected = registry.disconnect(sid)

        if disconnected is not None:
            logger.info(
                "Socket.IO client disconnected: "
                "user=%s sid=%s reason=%s",
                disconnected["name"],
                sid,
                reason,
            )

    # ------------------------------------------------------------------
    # Room subscriptions / history
    # ------------------------------------------------------------------

    @socketio.on("join_room")
    def handle_join_room(data):
        data = _as_dict(data)
        if data is None:
            return {
                "ok": False,
                "error": "invalid_payload",
            }

        room = data.get("room")

        last_event_id = data.get("lastEventID")
        try:
            sync_requests = _parse_history_sync(data)
            auto_sync = data.get("autoSync") is True
            if auto_sync and sync_requests is not None:
                raise ValueError("autoSync and sync are mutually exclusive")
            page_size = 50
            sync_id = ""
            if sync_requests is not None or auto_sync:
                page_size = data.get("pageSize", 50)
                if type(page_size) is not int or not 1 <= page_size <= 100:
                    raise ValueError("pageSize must be between 1 and 100")
                sync_id = data.get("syncId", "")
                if not isinstance(sync_id, str) or len(sync_id) > 128:
                    raise ValueError("syncId must be a short string")
        except ValueError as exc:
            return {"ok": False, "error": "invalid_sync", "detail": str(exc)}

        if room is None or str(room).strip() == "":
            return {
                "ok": False,
                "error": "room_required",
            }

        room = str(room)

        try:
            client_config = service.room_client_config(room)
            allowed_types = {"chat", "image", "system", *client_config["objectTypes"]}
            if auto_sync:
                sync_requests = [
                    ("chat", 7), ("image", 7), ("system", 7),
                    *[(t, item["sync_days"]) for t, item in client_config["objectTypes"].items()],
                ]
            elif sync_requests is not None:
                # Older clients may still request appointment on every room.
                # Server definitions are authoritative: do not expose undefined types.
                sync_requests = [(t, d) for t, d in sync_requests if t in allowed_types]
        except ValueError as exc:
            return {"ok": False, "error": "invalid_room_config", "detail": str(exc)}

        client = registry.get_by_sid(request.sid)
        if client is None:
            return {
                "ok": False,
                "error": "unknown_client",
            }

        if sync_requests is not None and not service.may_persist(room) and not auto_sync:
            return {"ok": False, "error": "room_not_persistent"}

        join_room(room)
        registry.add_room(
            request.sid,
            room,
        )

        logger.info(
            "Socket.IO client %s joined room %s",
            client["name"],
            room,
        )

        emit(
            "event",
            service.create_system_event(
                room=room,
                kind="room.joined",
                text=(
                    f'{client["name"]} ist dem Raum '
                    f'{room} beigetreten.'
                ),
                actor_id=client["id"],
                actor_name=client["name"],
            ),
            room=room,
        )

        if service.may_persist(room) and sync_requests is not None:
            # Capture request.sid before leaving the Flask request context.
            socketio.start_background_task(
                send_filtered_history,
                request.sid, room, sync_requests, sync_id, page_size,
            )
        elif service.may_persist(room):
            try:
                events = service.get_missed_events(
                    room,
                    last_event_id,
                )

                emit(
                    "missed_events",
                    events,
                    room=request.sid,
                )

                logger.info(
                    "Delivered %s persisted events to "
                    "user=%s room=%s (lastEventID=%s)",
                    len(events),
                    client["name"],
                    room,
                    last_event_id,
                )
            except Exception:
                logger.exception(
                    "Unable to load persisted Socket.IO "
                    "history for room=%s",
                    room,
                )

                emit(
                    "missed_events",
                    [],
                    room=request.sid,
                )

        elif sync_requests is not None and not auto_sync:
            # Legacy explicit sync requires persistence.
            return {"ok": False, "error": "room_not_persistent"}
        elif last_event_id is not None:
            emit(
                "request_new_events",
                {
                    "room": room,
                    "lastEventID": last_event_id,
                    "requestSID": request.sid,
                },
                include_self=False,
                room=room,
            )

        return {"ok": True, "roomConfig": client_config}

    @socketio.on("deliver_new_events")
    def handle_deliver_new_events(
        request_sid,
        events,
    ):
        client = registry.get_by_sid(request.sid)

        if client is None:
            return {
                "ok": False,
                "error": "unknown_client",
            }

        if not request_sid:
            return {
                "ok": False,
                "error": "request_sid_required",
            }

        if not isinstance(events, list):
            return {
                "ok": False,
                "error": "events_required",
            }

        logger.info(
            "Socket.IO client %s provides missed events "
            "to sid=%s count=%s",
            client["name"],
            request_sid,
            len(events),
        )

        emit(
            "missed_events",
            events,
            room=request_sid,
        )

        return {"ok": True}

    @socketio.on("leave_room")
    def handle_leave_room(data):
        data = _as_dict(data)
        if data is None:
            return {
                "ok": False,
                "error": "invalid_payload",
            }

        room = data.get("room")

        if room is None or str(room).strip() == "":
            return {
                "ok": False,
                "error": "room_required",
            }

        room = str(room)

        client = registry.get_by_sid(request.sid)
        if client is None:
            return {
                "ok": False,
                "error": "unknown_client",
            }

        was_member = registry.remove_room(
            request.sid,
            room,
        )

        leave_room(room)

        if was_member:
            logger.info(
                "Socket.IO client %s left room %s",
                client["name"],
                room,
            )

            emit(
                "event",
                service.create_system_event(
                    room=room,
                    kind="room.left",
                    text=(
                        f'{client["name"]} hat den Raum '
                        f'{room} verlassen'
                    ),
                    actor_id=client["id"],
                    actor_name=client["name"],
                ),
                room=room,
            )

        return {"ok": True}

    # ------------------------------------------------------------------
    # Generic client -> room events
    # ------------------------------------------------------------------

    def process_client_event(
        data: Any,
    ) -> dict[str, Any]:
        data = _as_dict(data)
        if data is None:
            return {
                "ok": False,
                "error": "invalid_payload",
            }

        room = data.get("room")
        if room is None or str(room).strip() == "":
            return {
                "ok": False,
                "error": "room_required",
            }

        room = str(room)

        client = registry.get_by_sid(request.sid)
        if client is None:
            return {
                "ok": False,
                "error": "unknown_client",
            }

        if not registry.is_in_room(
            request.sid,
            room,
        ):
            logger.warning(
                "Rejected event from user=%s to room=%s: "
                "client is not a member",
                client["name"],
                room,
            )

            return {
                "ok": False,
                "error": "not_in_room",
            }

        schema_version = data.get(
            "schemaVersion",
            SCHEMA_VERSION,
        )

        try:
            schema_version = int(schema_version)
        except (TypeError, ValueError):
            return {
                "ok": False,
                "error": "invalid_schema_version",
            }

        if schema_version != SCHEMA_VERSION:
            return {
                "ok": False,
                "error": "unsupported_schema_version",
                "supported": SCHEMA_VERSION,
            }

        event_type = str(
            data.get("type") or ""
        ).strip()

        if not event_type:
            return {
                "ok": False,
                "error": "type_required",
            }

        # Only built-in chat/images and explicitly enabled additional
        # object types may be sent by clients. System events are server-owned.
        try:
            allowed = {"chat", "image", *service.room_object_types(room)}
        except ValueError:
            return {"ok": False, "error": "invalid_room_config"}
        if event_type not in allowed:
            return {"ok": False, "error": "type_not_allowed"}

        operation = str(
            data.get("operation") or "upsert"
        ).strip()

        if not operation:
            return {
                "ok": False,
                "error": "operation_required",
            }

        payload = data.get("payload")
        if not isinstance(payload, Mapping):
            return {
                "ok": False,
                "error": "payload_object_required",
            }

        try:
            # The authenticated Socket.IO session is authoritative for the
            # sender. Client-supplied transmitter/transmitterName/receivedAt
            # fields are intentionally ignored.
            event = service.create_event(
                room=room,
                payload=payload,
                transmitter=client["id"],
                transmitter_name=client["name"],
                event_type=event_type,
                operation=operation,
                event_id=data.get("id"),
                object_id=data.get("objectId"),
                revision=data.get("revision", 1),
                schema_version=schema_version,
                created_at=data.get("createdAt"),
                key_id=data.get("key_id"),
            )
        except ValueError as exc:
            logger.warning(
                "Rejected invalid Socket.IO event: "
                "user=%s room=%s error=%s",
                client["name"],
                room,
                exc,
            )

            return {
                "ok": False,
                "error": "invalid_event",
            }

        logger.info(
            "Socket.IO event: user=%s room=%s "
            "id=%s type=%s operation=%s objectId=%s revision=%s",
            client["name"],
            room,
            event["id"],
            event["type"],
            event["operation"],
            event["objectId"],
            event["revision"],
        )

        try:
            service.record_event(event)
        except Exception:
            # Persistence must never prevent live delivery.
            logger.exception(
                "Unable to persist Socket.IO event "
                "for room=%s id=%s",
                room,
                event["id"],
            )

        emit(
            "event",
            event,
            room=room,
            include_self=False,
        )

        # Route only explicitly configured, validated cleartext messages.
        # Listener failures must not alter successful Socket.IO delivery.
        try:
            service.incoming_router.publish(context, event)
        except Exception:
            logger.exception("Unable to publish routed incoming Socket.IO message")

        return {
            "ok": True,
            "id": event["id"],
            "receivedAt": event["receivedAt"],
        }

    @socketio.on("send_event")
    def handle_send_event(data):
        return process_client_event(data)

    @socketio.on_error_default
    def handle_error(error):
        logger.error(
            "Unhandled Socket.IO event error",
            exc_info=(
                type(error),
                error,
                error.__traceback__,
            ),
        )
