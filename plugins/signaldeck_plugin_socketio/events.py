from __future__ import annotations

import json
import logging
from typing import Any

from flask import request
from flask_socketio import emit, join_room, leave_room

from .service import SocketIOService

_registered = False


def _as_dict(data: Any) -> dict | None:
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def register_socketio_events(socketio, service: SocketIOService, context=None) -> None:
    global _registered
    if _registered:
        return
    _registered = True

    logger = getattr(context, "logger", None) or logging.getLogger(__name__)
    registry = service.registry

    @socketio.on("connect")
    def handle_connect(auth):
        if not isinstance(auth, dict):
            logger.warning("Rejected Socket.IO connection without auth payload")
            return False

        user_id = auth.get("id")
        username = auth.get("username")

        if user_id is None or str(user_id).strip() == "":
            logger.warning("Rejected Socket.IO connection without user id")
            return False

        sid = request.sid
        old_sid = registry.connect(user_id, sid, username)

        if old_sid and old_sid != sid:
            try:
                socketio.server.disconnect(old_sid, namespace="/")
            except Exception:
                logger.exception("Could not disconnect superseded Socket.IO session")

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
                    "message",
                    service.create_system_message(
                        room=room,
                        content=f'{client["name"]} hat den Raum {room} verlassen',
                    ),
                    room=room,
                    include_self=False,
                )

        disconnected = registry.disconnect(sid)
        if disconnected is not None:
            logger.info(
                "Socket.IO client disconnected: user=%s sid=%s reason=%s",
                disconnected["name"],
                sid,
                reason,
            )

    @socketio.on("join_room")
    def handle_join_room(data):
        data = _as_dict(data)
        if data is None:
            return {"ok": False, "error": "invalid_payload"}

        room = data.get("room")
        last_message_id = data.get("lastMessageID")

        if room is None or str(room).strip() == "":
            return {"ok": False, "error": "room_required"}

        room = str(room)
        client = registry.get_by_sid(request.sid)
        if client is None:
            return {"ok": False, "error": "unknown_client"}

        join_room(room)
        registry.add_room(request.sid, room)

        logger.info("Socket.IO client %s joined room %s", client["name"], room)

        emit(
            "message",
            service.create_system_message(
                room=room,
                content=f'{client["name"]} ist dem Raum {room} beigetreten.',
            ),
            room=room,
        )

        if service.may_persist(room):
            try:
                messages = service.get_missed_messages(
                    room,
                    last_message_id,
                )
                emit(
                    "missed_messages",
                    messages,
                    room=request.sid,
                )
                logger.info(
                    "Delivered %s persisted messages to user=%s room=%s "
                    "(lastMessageID=%s)",
                    len(messages),
                    client["name"],
                    room,
                    last_message_id,
                )
            except Exception:
                logger.exception(
                    "Unable to load persisted Socket.IO history for room=%s",
                    room,
                )
                emit(
                    "missed_messages",
                    [],
                    room=request.sid,
                )
        elif last_message_id is not None:
            emit(
                "request_new_messages",
                {
                    "room": room,
                    "lastMessageID": last_message_id,
                    "requestSID": request.sid,
                },
                include_self=False,
                room=room,
            )

        return {"ok": True}

    @socketio.on("deliver_new_message")
    def handle_deliver_new_messages(request_sid, messages):
        client = registry.get_by_sid(request.sid)
        if client is None:
            return {"ok": False, "error": "unknown_client"}
        if not request_sid:
            return {"ok": False, "error": "request_sid_required"}

        logger.info(
            "Socket.IO client %s provides missed messages to sid=%s",
            client["name"],
            request_sid,
        )
        emit("missed_messages", messages, room=request_sid)
        return {"ok": True}

    @socketio.on("leave_room")
    def handle_leave_room(data):
        data = _as_dict(data)
        if data is None:
            return {"ok": False, "error": "invalid_payload"}

        room = data.get("room")
        if room is None or str(room).strip() == "":
            return {"ok": False, "error": "room_required"}

        room = str(room)
        client = registry.get_by_sid(request.sid)
        if client is None:
            return {"ok": False, "error": "unknown_client"}

        was_member = registry.remove_room(request.sid, room)
        leave_room(room)

        if was_member:
            logger.info("Socket.IO client %s left room %s", client["name"], room)
            emit(
                "message",
                service.create_system_message(
                    room=room,
                    content=f'{client["name"]} hat den Raum {room} verlassen',
                ),
                room=room,
            )

        return {"ok": True}

    @socketio.on("send_message")
    def handle_send_message(data):
        data = _as_dict(data)
        if data is None:
            return {"ok": False, "error": "invalid_payload"}

        room = data.get("room")
        content = data.get("content")

        if room is None or str(room).strip() == "":
            return {"ok": False, "error": "room_required"}
        if content is None:
            return {"ok": False, "error": "content_required"}

        room = str(room)
        client = registry.get_by_sid(request.sid)
        if client is None:
            return {"ok": False, "error": "unknown_client"}

        if not registry.is_in_room(request.sid, room):
            logger.warning(
                "Rejected message from user=%s to room=%s: client is not a member",
                client["name"],
                room,
            )
            return {"ok": False, "error": "not_in_room"}

        message = service.create_message(
            room=room,
            content=content,
            transmitter=client["id"],
            transmitter_name=client["name"],
            message_id=data.get("id"),
            date=data.get("date"),
            content_type=str(data.get("contentType") or "text"),
            content_uri=data.get("contentUri"),
            # Client messages cannot spoof server/system messages.
            is_system=False,
        )

        logger.info(
            "Socket.IO message: user=%s room=%s id=%s",
            client["name"],
            room,
            message["id"],
        )

        try:
            service.record_message(message)
        except Exception:
            # Persistence must never prevent live delivery.
            logger.exception(
                "Unable to persist Socket.IO message for room=%s id=%s",
                room,
                message["id"],
            )

        emit(
            "message",
            message,
            room=room,
            include_self=False,
        )
        return {"ok": True}

    @socketio.on_error_default
    def handle_error(error):
        logger.error(
            "Unhandled Socket.IO event error",
            exc_info=(type(error), error, error.__traceback__),
        )
