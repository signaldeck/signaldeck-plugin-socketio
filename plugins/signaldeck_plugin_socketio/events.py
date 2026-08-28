from __future__ import annotations

import json
import logging
from collections.abc import Mapping
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

        if service.may_persist(room):
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

        return {"ok": True}

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
