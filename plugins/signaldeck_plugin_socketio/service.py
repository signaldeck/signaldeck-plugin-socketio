from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import RLock
from typing import Any
from uuid import uuid4

from .registry import ClientRegistry


@dataclass(frozen=True)
class RoomSettings:
    name: str
    display_name: str
    persist: bool = False
    configured: bool = True
    options: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "persist": self.persist,
            "configured": self.configured,
            **self.options,
        }


class SocketIOService:
    DEFAULT_CONFIG = {
        "cors_allowed_origins": "*",
        "ping_interval": 25,
        "ping_timeout": 20,
        "max_http_buffer_size": 1_000_000,
        "server_sender_id": "signaldeck",
        "server_sender_name": "SignalDeck",
    }

    def __init__(self) -> None:
        self.registry = ClientRegistry()
        self._config_lock = RLock()
        self._config: dict[str, Any] = dict(self.DEFAULT_CONFIG)
        self._rooms: dict[str, RoomSettings] = {}
        self._socketio = None
        self._message_recorder_owner = None
        self._message_recorder = None
        self._history_provider_owner = None
        self._history_provider = None

    def configure(self, config: dict | None) -> None:
        config = config or {}

        with self._config_lock:
            for key in self.DEFAULT_CONFIG:
                if key in config:
                    self._config[key] = config[key]

            rooms: dict[str, RoomSettings] = {}
            for room_config in config.get("rooms", []):
                if not isinstance(room_config, dict):
                    raise ValueError("Each entry in 'rooms' must be an object")

                name = str(room_config.get("name", "")).strip()
                if not name:
                    raise ValueError("Each configured room requires a non-empty 'name'")
                if name in rooms:
                    raise ValueError(f"Room '{name}' is configured more than once")

                known_keys = {"name", "display_name", "persist"}
                options = {
                    key: value
                    for key, value in room_config.items()
                    if key not in known_keys
                }

                rooms[name] = RoomSettings(
                    name=name,
                    display_name=str(room_config.get("display_name") or name),
                    persist=bool(room_config.get("persist", False)),
                    configured=True,
                    options=options,
                )

            self._rooms = rooms

    @property
    def config(self) -> dict[str, Any]:
        with self._config_lock:
            return dict(self._config)

    def room_settings(self, room: str) -> RoomSettings:
        room = str(room)
        with self._config_lock:
            configured = self._rooms.get(room)
            if configured is not None:
                return configured

        # Unknown rooms are valid, but never persistent.
        return RoomSettings(
            name=room,
            display_name=room,
            persist=False,
            configured=False,
        )

    def configured_rooms(self) -> list[RoomSettings]:
        with self._config_lock:
            return list(self._rooms.values())

    def all_room_names(self) -> list[str]:
        names = {room.name for room in self.configured_rooms()}
        names.update(self.registry.active_rooms())
        return sorted(names)

    def room_overview(self, room: str) -> dict[str, Any]:
        settings = self.room_settings(room)
        clients = self.registry.clients_for_room(room)
        return {
            **settings.to_dict(),
            "active": bool(clients),
            "client_count": len(clients),
            "clients": clients,
        }

    def rooms_overview(self) -> list[dict[str, Any]]:
        return [self.room_overview(room) for room in self.all_room_names()]

    def may_persist(self, room: str) -> bool:
        return self.room_settings(room).persist

    # ------------------------------------------------------------------
    # Canonical chat payload
    # ------------------------------------------------------------------

    @staticmethod
    def _millis(value: Any | None) -> int:
        if value is None:
            return int(time.time() * 1000)
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(time.time() * 1000)

    def create_message(
        self,
        *,
        room: str,
        content: Any,
        transmitter: Any,
        transmitter_name: Any | None = None,
        message_id: Any | None = None,
        date: Any | None = None,
        content_type: str = "text",
        is_system: bool = False,
        content_uri: str | None = None,
    ) -> dict[str, Any]:
        return {
            "id": str(message_id or uuid4()),
            "date": self._millis(date),
            "transmitter": str(transmitter) if transmitter is not None else None,
            "transmitterName": (
                str(transmitter_name)
                if transmitter_name is not None
                else None
            ),
            "room": str(room),
            "content": str(content),
            "contentUri": content_uri,
            "contentType": str(content_type or "text"),
            "isSystem": bool(is_system),
        }

    def create_system_message(self, *, room: str, content: str) -> dict[str, Any]:
        cfg = self.config
        return self.create_message(
            room=room,
            content=content,
            transmitter=cfg["server_sender_id"],
            transmitter_name=cfg["server_sender_name"],
            is_system=True,
        )

    # ------------------------------------------------------------------
    # PersistData bridge
    # ------------------------------------------------------------------

    def bind_message_recorder(self, owner, recorder) -> None:
        self._message_recorder_owner = owner
        self._message_recorder = recorder

    def unbind_message_recorder(self, owner) -> None:
        if self._message_recorder_owner is owner:
            self._message_recorder_owner = None
            self._message_recorder = None

    def record_message(self, message: dict[str, Any]) -> bool:
        room = str(message.get("room", ""))
        if not room or not self.may_persist(room):
            return False
        if self._message_recorder is None:
            return False

        self._message_recorder(dict(message))
        return True

    def bind_history_provider(self, owner, provider) -> None:
        self._history_provider_owner = owner
        self._history_provider = provider

    def unbind_history_provider(self, owner) -> None:
        if self._history_provider_owner is owner:
            self._history_provider_owner = None
            self._history_provider = None

    def get_missed_messages(self, room: str, last_message_id: Any) -> list[dict]:
        if not self.may_persist(room):
            raise ValueError(
                f"Room '{room}' is not persistent and has no server-side history"
            )
        if self._history_provider is None:
            raise RuntimeError("No Socket.IO history provider is registered")
        return self._history_provider(
            room=room,
            last_message_id=last_message_id,
        )

    # ------------------------------------------------------------------
    # Socket.IO binding / server-originated messages
    # ------------------------------------------------------------------

    def bind_socketio(self, socketio) -> None:
        self._socketio = socketio

    @property
    def socketio(self):
        if self._socketio is None:
            raise RuntimeError("Socket.IO service is not registered on a Flask app")
        return self._socketio

    def emit(self, event: str, data: Any = None, **kwargs) -> None:
        self.socketio.emit(event, data, **kwargs)

    def send_server_message(
        self,
        content: Any,
        *,
        room: str | None = None,
        content_type: str = "text",
    ) -> dict[str, Any]:
        cfg = self.config
        message = self.create_message(
            room=room or "",
            content=content,
            transmitter=cfg["server_sender_id"],
            transmitter_name=cfg["server_sender_name"],
            content_type=content_type,
            is_system=False,
        )

        if room:
            self.record_message(message)

        kwargs = {"room": room} if room else {}
        self.emit("message", message, **kwargs)
        return message

    def disconnect_all_clients(self) -> tuple[int, list[tuple[str, Exception]]]:
        if self._socketio is None:
            return 0, []

        sids = self.registry.active_sids()
        errors: list[tuple[str, Exception]] = []

        for sid in sids:
            try:
                self._socketio.server.disconnect(sid, namespace="/")
            except Exception as exc:
                errors.append((sid, exc))

        return len(sids), errors


service = SocketIOService()
