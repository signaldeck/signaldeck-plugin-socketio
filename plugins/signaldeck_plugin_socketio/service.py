from __future__ import annotations

import json
import time
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import RLock
from typing import Any
from uuid import uuid4

from .blob_store import BlobStore
from .registry import ClientRegistry


SCHEMA_VERSION = 1
DEFAULT_TYPE = "chat"
DEFAULT_OPERATION = "upsert"
SYSTEM_TYPE = "system"
SYSTEM_OPERATION = "notify"


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
        "blob_store_path": "data/socketio-blobs",
        "max_blob_size_bytes": 50 * 1024 * 1024,
    }

    def __init__(self) -> None:
        self.registry = ClientRegistry()
        self._config_lock = RLock()
        self._config: dict[str, Any] = dict(self.DEFAULT_CONFIG)
        self._rooms: dict[str, RoomSettings] = {}
        self._socketio = None
        self._blob_store: BlobStore | None = None
        self._blob_store_signature: tuple[str, int] | None = None
        self.logger = logging.getLogger(__name__)

        self._event_recorder_owner = None
        self._event_recorder = None
        self._history_provider_owner = None
        self._history_provider = None

        self._message_bus_unsubscribe = None

    # ------------------------------------------------------------------
    # Configuration / rooms
    # ------------------------------------------------------------------

    def configure(self, config: dict | None) -> None:
        config = config or {}

        with self._config_lock:
            for key in self.DEFAULT_CONFIG:
                if key in config:
                    self._config[key] = config[key]

            rooms: dict[str, RoomSettings] = {}

            for room_config in config.get("rooms", []):
                if not isinstance(room_config, dict):
                    raise ValueError(
                        "Each entry in 'rooms' must be an object"
                    )

                name = str(room_config.get("name", "")).strip()
                if not name:
                    raise ValueError(
                        "Each configured room requires a non-empty 'name'"
                    )

                if name in rooms:
                    raise ValueError(
                        f"Room '{name}' is configured more than once"
                    )

                known_keys = {
                    "name",
                    "display_name",
                    "persist",
                }

                options = {
                    key: value
                    for key, value in room_config.items()
                    if key not in known_keys
                }

                rooms[name] = RoomSettings(
                    name=name,
                    display_name=str(
                        room_config.get("display_name") or name
                    ),
                    persist=bool(
                        room_config.get("persist", False)
                    ),
                    configured=True,
                    options=options,
                )

            self._rooms = rooms
            self._blob_store = None
            self._blob_store_signature = None

    @property
    def config(self) -> dict[str, Any]:
        with self._config_lock:
            return dict(self._config)

    @property
    def blob_store(self) -> BlobStore:
        with self._config_lock:
            path = str(self._config["blob_store_path"])
            max_size = int(self._config["max_blob_size_bytes"])
            signature = (path, max_size)

            if self._blob_store is None or self._blob_store_signature != signature:
                self._blob_store = BlobStore(path, max_size)
                self._blob_store_signature = signature

            return self._blob_store

    def bind_message_bus(self, unsubscribe) -> None:
        self.unbind_message_bus()
        self._message_bus_unsubscribe = unsubscribe

    def unbind_message_bus(self) -> None:
        if self._message_bus_unsubscribe is not None:
            self._message_bus_unsubscribe()
            self._message_bus_unsubscribe = None

    def room_settings(self, room: str) -> RoomSettings:
        room = str(room)

        with self._config_lock:
            configured = self._rooms.get(room)
            if configured is not None:
                return configured

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
        names = {
            room.name
            for room in self.configured_rooms()
        }
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
        return [
            self.room_overview(room)
            for room in self.all_room_names()
        ]

    def may_persist(self, room: str) -> bool:
        return self.room_settings(room).persist

    # ------------------------------------------------------------------
    # Generic event envelope
    # ------------------------------------------------------------------

    @staticmethod
    def _millis(
        value: Any | None,
        *,
        default: int | None = None,
    ) -> int:
        if value is None:
            if default is not None:
                return default
            return int(time.time() * 1000)

        try:
            return int(value)
        except (TypeError, ValueError):
            if default is not None:
                return default
            return int(time.time() * 1000)

    @staticmethod
    def _positive_int(
        value: Any,
        *,
        default: int,
    ) -> int:
        try:
            result = int(value)
        except (TypeError, ValueError):
            return default

        return result if result > 0 else default

    @staticmethod
    def _normalize_payload(
        payload: Any,
    ) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            raise ValueError(
                "Event payload must be a JSON object/dict. "
                "Nested objects and arrays are allowed."
            )

        normalized = dict(payload)

        try:
            json.dumps(
                normalized,
                ensure_ascii=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Event payload contains non-JSON-serializable values"
            ) from exc

        return normalized

    def create_event(
        self,
        *,
        room: str,
        payload: Mapping[str, Any],
        transmitter: Any,
        transmitter_name: Any | None = None,
        event_type: str = DEFAULT_TYPE,
        operation: str = DEFAULT_OPERATION,
        event_id: Any | None = None,
        object_id: Any | None = None,
        revision: Any = 1,
        schema_version: Any = SCHEMA_VERSION,
        created_at: Any | None = None,
        key_id: Any | None = None,
    ) -> dict[str, Any]:
        event_id = str(event_id or uuid4()).strip()
        event_type = str(event_type or "").strip()
        operation = str(operation or "").strip()
        object_id = str(object_id or event_id).strip()

        if not event_id:
            raise ValueError("Event id must not be empty")
        if not event_type:
            raise ValueError("Event type must not be empty")
        if not operation:
            raise ValueError("Event operation must not be empty")
        if not object_id:
            raise ValueError("Event objectId must not be empty")

        key_id_value = None
        if key_id is not None:
            key_id_value = str(key_id).strip()
            if not key_id_value:
                raise ValueError("Event key_id must not be empty when present")

        received_at = int(time.time() * 1000)
        created_at = self._millis(
            created_at,
            default=received_at,
        )

        transmitter_value = (
            str(transmitter).strip()
            if transmitter is not None
            else ""
        )
        if not transmitter_value:
            raise ValueError(
                "Event transmitter must not be empty"
            )

        transmitter_name_value = str(
            transmitter_name
            if transmitter_name is not None
            else transmitter_value
        )

        event = {
            "schemaVersion": self._positive_int(
                schema_version,
                default=SCHEMA_VERSION,
            ),
            "id": event_id,
            "createdAt": created_at,
            "receivedAt": received_at,
            "room": str(room),
            "transmitter": transmitter_value,
            "transmitterName": transmitter_name_value,
            "type": event_type,
            "operation": operation,
            "objectId": object_id,
            "revision": self._positive_int(
                revision,
                default=1,
            ),
            "payload": self._normalize_payload(payload),
        }

        if key_id_value is not None:
            event["key_id"] = key_id_value

        return event

    def create_system_event(
        self,
        *,
        room: str,
        kind: str,
        text: str,
        actor_id: Any | None = None,
        actor_name: Any | None = None,
    ) -> dict[str, Any]:
        cfg = self.config

        payload: dict[str, Any] = {
            "kind": str(kind),
            "text": str(text),
        }

        if actor_id is not None or actor_name is not None:
            payload["actor"] = {
                "id": (
                    str(actor_id)
                    if actor_id is not None
                    else None
                ),
                "name": (
                    str(actor_name)
                    if actor_name is not None
                    else None
                ),
            }

        return self.create_event(
            room=room,
            payload=payload,
            transmitter=cfg["server_sender_id"],
            transmitter_name=cfg["server_sender_name"],
            event_type=SYSTEM_TYPE,
            operation=SYSTEM_OPERATION,
        )

    # ------------------------------------------------------------------
    # PersistData bridge
    # ------------------------------------------------------------------

    def bind_event_recorder(
        self,
        owner,
        recorder,
    ) -> None:
        self._event_recorder_owner = owner
        self._event_recorder = recorder

    def unbind_event_recorder(
        self,
        owner,
    ) -> None:
        if self._event_recorder_owner is owner:
            self._event_recorder_owner = None
            self._event_recorder = None

    def record_event(
        self,
        event: dict[str, Any],
    ) -> bool:
        room = str(event.get("room", ""))

        if not room or not self.may_persist(room):
            return False

        if self._event_recorder is None:
            return False

        self._event_recorder(dict(event))
        return True

    def bind_history_provider(
        self,
        owner,
        provider,
    ) -> None:
        self._history_provider_owner = owner
        self._history_provider = provider

    def unbind_history_provider(
        self,
        owner,
    ) -> None:
        if self._history_provider_owner is owner:
            self._history_provider_owner = None
            self._history_provider = None

    def get_missed_events(
        self,
        room: str,
        last_event_id: Any,
    ) -> list[dict]:
        if not self.may_persist(room):
            raise ValueError(
                f"Room '{room}' is not persistent and has no "
                "server-side history"
            )

        if self._history_provider is None:
            raise RuntimeError(
                "No Socket.IO history provider is registered"
            )

        return self._history_provider(
            room=room,
            last_event_id=last_event_id,
        )

    # ------------------------------------------------------------------
    # Socket.IO binding / server-originated events
    # ------------------------------------------------------------------

    def bind_socketio(self, socketio) -> None:
        self._socketio = socketio

    @property
    def socketio(self):
        if self._socketio is None:
            raise RuntimeError(
                "Socket.IO service is not registered on a Flask app"
            )
        return self._socketio

    def emit(
        self,
        event: str,
        data: Any = None,
        **kwargs,
    ) -> None:
        self.socketio.emit(
            event,
            data,
            **kwargs,
        )

    def send_server_event(
        self,
        *,
        payload: Mapping[str, Any],
        room: str | None = None,
        event_type: str = DEFAULT_TYPE,
        operation: str = DEFAULT_OPERATION,
        object_id: str | None = None,
        revision: int = 1,
        event_id: str | None = None,
        created_at: Any | None = None,
        key_id: str | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        self.logger.info(
            "Sending server-originated Socket.IO event to room '%s' with payload: %s",
            room or "",
            payload,
        )
        event = self.create_event(
            room=room or "",
            payload=payload,
            transmitter=cfg["server_sender_id"],
            transmitter_name=cfg["server_sender_name"],
            event_type=event_type,
            operation=operation,
            object_id=object_id,
            revision=revision,
            event_id=event_id,
            created_at=created_at,
            key_id=key_id,
        )

        if room:
            self.record_event(event)

        kwargs = {"room": room} if room else {}
        self.emit(
            "event",
            event,
            **kwargs,
        )

        return event

    def disconnect_all_clients(
        self,
    ) -> tuple[int, list[tuple[str, Exception]]]:
        if self._socketio is None:
            return 0, []

        sids = self.registry.active_sids()
        errors: list[tuple[str, Exception]] = []

        for sid in sids:
            try:
                self._socketio.server.disconnect(
                    sid,
                    namespace="/",
                )
            except Exception as exc:
                errors.append((sid, exc))

        return len(sids), errors


service = SocketIOService()
