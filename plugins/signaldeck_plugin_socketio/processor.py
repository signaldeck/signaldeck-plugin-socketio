from __future__ import annotations

from email.mime import message
from email.mime import message
import json
import time
from collections.abc import Mapping
from datetime import datetime, timedelta
from threading import Condition, RLock
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from signaldeck_sdk import DisplayProcessor, PersistData

from .commands import SocketIOSendCommand
from .displaydata import SocketIODisplayData
from .history import MessageHistory
from .service import service


HISTORY_DAYS = 7
SCHEMA_VERSION = 1
DEFAULT_TYPE = "chat"
DEFAULT_OPERATION = "upsert"

EVENT_PERSIST_FIELDS = [
    {"name": "date", "dtype": "datetime", "display_name": "Received At"},
    {"name": "createdAt", "dtype": "datetime", "display_name": "Created At"},
    {"name": "room", "dtype": "str", "display_name": "Room"},
    {"name": "transmitter", "dtype": "str", "display_name": "Transmitter"},
    {
        "name": "transmitterName",
        "dtype": "str",
        "display_name": "Transmitter Name",
    },
    {"name": "schemaVersion", "dtype": "int", "display_name": "Schema Version"},
    {"name": "id", "dtype": "str", "display_name": "Event ID"},
    {"name": "type", "dtype": "str", "display_name": "Type"},
    {"name": "operation", "dtype": "str", "display_name": "Operation"},
    {"name": "objectId", "dtype": "str", "display_name": "Object ID"},
    {"name": "revision", "dtype": "int", "display_name": "Revision"},
    {"name": "payload", "dtype": "str", "display_name": "Payload JSON"},
]

HISTORY_FIELDS = [
    "createdAt",
    "room",
    "transmitter",
    "transmitterName",
    "schemaVersion",
    "id",
    "type",
    "operation",
    "objectId",
    "revision",
    "payload",
]


class SocketIOProcessor(PersistData, DisplayProcessor):
    """Socket.IO transport with opaque, generic event persistence.

    Wire format (schemaVersion=1):

        {
            "schemaVersion": 1,
            "id": "event-uuid",
            "createdAt": 1787745600000,
            "receivedAt": 1787745600100,
            "room": "family",
            "transmitter": "user-123",
            "transmitterName": "Stephan",
            "type": "appointment",
            "operation": "upsert",
            "objectId": "appointment-4711",
            "revision": 4,
            "payload": { ... arbitrary nested JSON object ... }
        }

    SignalDeck only interprets the transport metadata needed for routing,
    history and deduplication. ``type``, ``operation``, ``objectId``,
    ``revision`` and especially ``payload`` remain opaque application data.

    PersistData stores ``payload`` as a JSON string. On the wire it is restored
    to a JSON object, so nested dictionaries/lists are preserved without
    flattening.
    """

    def __init__(self, name, config, ctx, valueProvider, collect_data):
        config = self._normalize_persist_config(config)
        super().__init__(name, config, ctx, valueProvider, collect_data)

        self._persist_lock = RLock()
        self._persist_condition = Condition()
        self._pending_persist_callbacks = 0
        self._history = MessageHistory(max_age=timedelta(days=HISTORY_DAYS))

        service.configure(self.config)
        self._message_routes = self._build_message_routes(
            self.config.get("message_routes", [])
        )

        self._message_bus_unsubscribe = (
            self.ctx.message_bus.subscribe(self._on_signaldeck_message)
        )
        self._validate_persistence_configuration()

        service.bind_event_recorder(self, self.record_event)
        service.bind_history_provider(self, self.get_missed_events)

    @staticmethod
    def _normalize_persist_config(config: dict) -> dict:
        normalized = dict(config)
        persist_configs = []

        for persist_config in config.get("persist", []):
            item = dict(persist_config)
            item["fields"] = [dict(field) for field in EVENT_PERSIST_FIELDS]
            persist_configs.append(item)

        if "persist" in config:
            normalized["persist"] = persist_configs

        return normalized

    def _persistent_rooms(self) -> list[str]:
        return [
            room.name
            for room in service.configured_rooms()
            if room.persist
        ]

    def _validate_persistence_configuration(self) -> None:
        if self._persistent_rooms() and not self.config.get("persist"):
            raise ValueError(
                "Socket.IO rooms are configured with persist=true, but the "
                "SocketIOProcessor has no top-level PersistData 'persist' "
                "configuration. Configure at least one DataStore."
            )

    def _build_message_routes(
        self,
        routes: list[dict],
    ) -> dict[str, tuple[str, ...]]:
        result: dict[str, list[str]] = {}

        for route in routes:
            if not isinstance(route, dict):
                raise ValueError(
                    "Each entry in 'message_routes' must be an object"
                )

            source = str(route.get("source", "")).strip()
            if not source:
                raise ValueError(
                    "Each message route requires a non-empty 'source'"
                )

            rooms = route.get("rooms", [])
            if not isinstance(rooms, list):
                raise ValueError(
                    f"Message route '{source}' requires 'rooms' to be a list"
                )

            normalized_rooms = [
                str(room).strip()
                for room in rooms
                if str(room).strip()
            ]

            if not normalized_rooms:
                raise ValueError(
                    f"Message route '{source}' requires at least one room"
                )

            result.setdefault(source, []).extend(normalized_rooms)

        return {
            source: tuple(dict.fromkeys(rooms))
            for source, rooms in result.items()
        }

    # ------------------------------------------------------------------
    # Generic SignalDeck -> Socket.IO event conversion
    # ------------------------------------------------------------------

    def _on_signaldeck_message(self, message) -> None:
        
        rooms = self._message_routes.get(message.source, ())
        metadata = message.metadata or {}
        self.logger.info(f"Sending message from {message.source} to rooms: {rooms}.")
        for room in rooms:
            event = self._build_server_event(
                room=room,
                content=message.content,
                metadata=metadata,
            )
            self._send_event(event, room=room)

    def _build_server_event(
        self,
        *,
        room: str,
        content: Any,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = metadata or {}
        event_id = str(metadata.get("id") or uuid4())
        event_type = str(metadata.get("type") or DEFAULT_TYPE).strip()
        operation = str(
            metadata.get("operation") or DEFAULT_OPERATION
        ).strip()
        object_id = str(metadata.get("object_id") or event_id).strip()
        revision = self._positive_int(metadata.get("revision"), default=1)

        if not event_type:
            raise ValueError("SignalDeck event type must not be empty")
        if not operation:
            raise ValueError("SignalDeck event operation must not be empty")
        if not object_id:
            raise ValueError("SignalDeck event objectId must not be empty")

        payload = metadata.get("payload")
        if payload is None:
            if isinstance(content, Mapping):
                payload = dict(content)
            elif event_type == "chat":
                payload = {
                    "text": str(content),
                    "contentType": str(
                        metadata.get("content_type") or "text"
                    ),
                    "system": bool(metadata.get("system", False)),
                }
            else:
                payload = {"value": content}

        payload = self._normalize_payload(payload)
        now = self._now()
        created_at = self._coerce_datetime(
            metadata.get("created_at"),
            default=now,
        )

        service_config = service.config

        return {
            "schemaVersion": SCHEMA_VERSION,
            "id": event_id,
            "createdAt": self._millis_from_datetime(created_at),
            "receivedAt": self._millis_from_datetime(now),
            "room": room,
            "transmitter": str(
                service_config.get("server_sender_id", "signaldeck")
            ),
            "transmitterName": str(
                service_config.get("server_sender_name", "SignalDeck")
            ),
            "type": event_type,
            "operation": operation,
            "objectId": object_id,
            "revision": revision,
            "payload": payload,
        }

    def _send_event(
        self,
        event: dict[str, Any],
        *,
        room: str | None,
    ) -> None:
        normalized = self._normalize_event(event)

        if room:
            self._record_normalized_event(normalized)

        wire_event = self._event_to_wire(normalized)

        if room:
            service.emit("event", wire_event, room=room)
        else:
            # Broadcast events without a room are intentionally not persisted.
            service.emit("event", wire_event)

    # ------------------------------------------------------------------
    # Generic event normalization
    # ------------------------------------------------------------------

    def _normalize_event(
        self,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate and normalize a schemaVersion=1 event."""
        if not isinstance(event, Mapping):
            raise ValueError("Socket.IO event must be an object")

        now = self._now()

        schema_version = self._positive_int(
            event.get("schemaVersion"),
            default=0,
        )
        if schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported schemaVersion={schema_version}; "
                f"expected {SCHEMA_VERSION}"
            )

        event_id = str(event.get("id") or "").strip()
        room = str(event.get("room") or "").strip()
        transmitter = str(event.get("transmitter") or "").strip()
        transmitter_name = str(
            event.get("transmitterName") or transmitter
        ).strip()
        event_type = str(event.get("type") or "").strip()
        operation = str(event.get("operation") or "").strip()
        object_id = str(event.get("objectId") or "").strip()
        revision = self._positive_int(
            event.get("revision"),
            default=0,
        )
        payload = self._normalize_payload(
            event.get("payload")
        )

        if not event_id:
            raise ValueError("Event id must not be empty")
        if not room:
            raise ValueError("Event room must not be empty")
        if not transmitter:
            raise ValueError("Event transmitter must not be empty")
        if not event_type:
            raise ValueError("Event type must not be empty")
        if not operation:
            raise ValueError("Event operation must not be empty")
        if not object_id:
            raise ValueError("Event objectId must not be empty")
        if revision <= 0:
            raise ValueError("Event revision must be greater than zero")

        created_at = self._coerce_datetime(
            event.get("createdAt"),
            default=now,
        )
        received_at = self._coerce_datetime(
            event.get("receivedAt"),
            default=now,
        )

        return {
            "schemaVersion": schema_version,
            "id": event_id,
            "createdAt": created_at,
            "receivedAt": received_at,
            "room": room,
            "transmitter": transmitter,
            "transmitterName": transmitter_name,
            "type": event_type,
            "operation": operation,
            "objectId": object_id,
            "revision": revision,
            "payload": payload,
            # MessageHistory currently uses `date` for retention internally.
            "date": received_at,
        }

    @staticmethod
    def _normalize_payload(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            raise ValueError(
                "Event payload must be a JSON object/dict. It may contain "
                "arbitrarily nested objects and arrays."
            )

        normalized = dict(payload)

        try:
            json.dumps(normalized, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Event payload must contain JSON-serializable values only"
            ) from exc

        return normalized

    @staticmethod
    def _positive_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default

        return parsed if parsed > 0 else default

    # ------------------------------------------------------------------
    # Time helpers
    # ------------------------------------------------------------------

    def _timezone(self):
        return ZoneInfo(self.config.get("timezone", "Europe/Berlin"))

    def _now(self) -> datetime:
        return self.ctx.date.now(self._timezone())

    def _coerce_datetime(
        self,
        value: Any,
        *,
        default: datetime,
    ) -> datetime:
        if value is None:
            return default

        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=self._timezone())
            return value.astimezone(self._timezone())

        try:
            # Wire timestamps are milliseconds since epoch.
            return datetime.fromtimestamp(
                int(value) / 1000.0,
                tz=self._timezone(),
            )
        except (TypeError, ValueError, OSError):
            return default

    @staticmethod
    def _millis_from_datetime(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    @staticmethod
    def _missing(value: Any) -> bool:
        if value is None:
            return True
        try:
            return bool(value != value)  # NaN / NaT
        except Exception:
            return False

    @staticmethod
    def _single_value(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    # ------------------------------------------------------------------
    # Display / value-provider helpers
    # ------------------------------------------------------------------

    @property
    def connected_clients(self) -> int:
        return service.registry.connected_count()

    @property
    def users(self) -> list[dict]:
        return service.registry.users()

    @property
    def rooms(self) -> list[str]:
        return service.registry.active_rooms()

    @property
    def room_overview(self) -> list[dict]:
        return service.rooms_overview()

    def get_clients_for_room(self, room: str) -> list[dict]:
        return service.registry.clients_for_room(room)

    def get_room_settings(self, room: str) -> dict:
        return service.room_settings(room).to_dict()

    def get_room_overview(self, room: str) -> dict:
        return service.room_overview(room)

    def registerCommands(self, cmd):
        cmd.registerCmd(SocketIOSendCommand(service))

    # ------------------------------------------------------------------
    # PersistData lifecycle / history bootstrap
    # ------------------------------------------------------------------

    def init_current_vals(self, config=None):
        if not self._persistent_rooms():
            self._history.clear()
            return None

        records = self.get_records(
            fields=HISTORY_FIELDS,
            days=HISTORY_DAYS,
            config=self.config,
        )

        normalized = self._records_from_dataframe(records)
        persistent_rooms = set(self._persistent_rooms())
        normalized = [
            record
            for record in normalized
            if record["room"] in persistent_rooms
        ]

        self._history.replace(normalized, now=self._now())

        self.ctx.logger.info(
            "Loaded %s Socket.IO events into history (max %s days)",
            len(normalized),
            HISTORY_DAYS,
        )
        return records

    def _records_from_dataframe(self, dataframe) -> list[dict]:
        if dataframe is None or getattr(dataframe, "empty", True):
            return []

        records = []

        for index, row in dataframe.iterrows():
            received_at = row.get("date", index)
            if hasattr(received_at, "to_pydatetime"):
                received_at = received_at.to_pydatetime()
            if not isinstance(received_at, datetime):
                continue

            created_at = row.get("createdAt")
            if hasattr(created_at, "to_pydatetime"):
                created_at = created_at.to_pydatetime()
            if not isinstance(created_at, datetime):
                created_at = received_at

            room = row.get("room")
            transmitter = row.get("transmitter")
            event_id = row.get("id")
            event_type = row.get("type")
            object_id = row.get("objectId")
            payload_json = row.get("payload")

            if any(
                self._missing(value)
                for value in (
                    room,
                    transmitter,
                    event_id,
                    event_type,
                    object_id,
                    payload_json,
                )
            ):
                continue

            try:
                payload = json.loads(str(payload_json))
            except (TypeError, ValueError, json.JSONDecodeError):
                self.ctx.logger.warning(
                    "Ignoring persisted Socket.IO event with invalid payload "
                    "JSON: id=%s room=%s",
                    event_id,
                    room,
                )
                continue

            if not isinstance(payload, dict):
                self.ctx.logger.warning(
                    "Ignoring persisted Socket.IO event whose payload is not "
                    "an object: id=%s room=%s",
                    event_id,
                    room,
                )
                continue

            transmitter_name = row.get("transmitterName")
            if self._missing(transmitter_name):
                transmitter_name = (
                    service.registry.display_name_for_user_id(transmitter)
                    or str(transmitter)
                )

            records.append(
                {
                    "schemaVersion": self._positive_int(
                        row.get("schemaVersion"),
                        default=SCHEMA_VERSION,
                    ),
                    "id": str(event_id),
                    "createdAt": created_at,
                    "receivedAt": received_at,
                    "room": str(room),
                    "transmitter": str(transmitter),
                    "transmitterName": str(transmitter_name),
                    "type": str(event_type),
                    "operation": str(
                        row.get("operation") or DEFAULT_OPERATION
                    ),
                    "objectId": str(object_id),
                    "revision": self._positive_int(
                        row.get("revision"),
                        default=1,
                    ),
                    "payload": payload,
                    "date": received_at,
                }
            )

        return records

    # ------------------------------------------------------------------
    # New events / PersistData
    # ------------------------------------------------------------------

    def record_event(self, event: dict[str, Any]) -> None:
        normalized = self._normalize_event(event)
        self._record_normalized_event(normalized)

    def _record_normalized_event(
        self,
        event: Mapping[str, Any],
    ) -> None:
        room = str(event["room"])

        if not room or not service.may_persist(room):
            return

        data = self._event_to_persist_data(event)
        cache_record = dict(event)

        inserted = self._history.append(
            cache_record,
            now=event["receivedAt"],
        )
        if not inserted:
            self.ctx.logger.debug(
                "Socket.IO history already contains room=%s id=%s",
                room,
                event["id"],
            )
            return

        loop = getattr(self.valueProvider, "loop", None)
        if loop is not None and loop.is_running():
            with self._persist_condition:
                self._pending_persist_callbacks += 1

            try:
                loop.call_soon_threadsafe(
                    self._save_event_from_runtime,
                    data,
                )
            except Exception:
                with self._persist_condition:
                    self._pending_persist_callbacks -= 1
                    self._persist_condition.notify_all()
                raise
        else:
            self._save_event(data)

    def _event_to_persist_data(
        self,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            # PersistData/SqliteStore use the conventional technical `date`
            # field as the record timestamp. Semantically this is receivedAt.
            "date": event["receivedAt"],
            "createdAt": event["createdAt"],
            "room": str(event["room"]),
            "transmitter": str(event["transmitter"]),
            "transmitterName": str(event["transmitterName"]),
            "schemaVersion": int(event["schemaVersion"]),
            "id": str(event["id"]),
            "type": str(event["type"]),
            "operation": str(event["operation"]),
            "objectId": str(event["objectId"]),
            "revision": int(event["revision"]),
            "payload": json.dumps(
                event["payload"],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }

    def _save_event_from_runtime(self, data: dict) -> None:
        try:
            self._save_event(data)
        finally:
            with self._persist_condition:
                self._pending_persist_callbacks -= 1
                self._persist_condition.notify_all()

    def _save_event(self, data: dict) -> None:
        with self._persist_lock:
            self.save_data(data)

    def _wait_for_pending_persist_callbacks(
        self,
        timeout: float = 5.0,
    ) -> bool:
        deadline = time.monotonic() + timeout

        with self._persist_condition:
            while self._pending_persist_callbacks > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._persist_condition.wait(timeout=remaining)

            return True

    # ------------------------------------------------------------------
    # History lookup
    # ------------------------------------------------------------------

    def get_missed_events(
        self,
        *,
        room: str,
        last_event_id: Any,
    ) -> list[dict]:
        if not service.may_persist(room):
            return []

        events = self._history.messages_after(
            room,
            last_event_id,
            now=self._now(),
        )

        return [
            self._event_to_wire(event)
            for event in events
        ]

    def _event_to_wire(
        self,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "schemaVersion": int(event["schemaVersion"]),
            "id": str(event["id"]),
            "createdAt": self._millis_from_datetime(event["createdAt"]),
            "receivedAt": self._millis_from_datetime(event["receivedAt"]),
            "room": str(event["room"]),
            "transmitter": str(event["transmitter"]),
            "transmitterName": str(event["transmitterName"]),
            "type": str(event["type"]),
            "operation": str(event["operation"]),
            "objectId": str(event["objectId"]),
            "revision": int(event["revision"]),
            "payload": dict(event["payload"]),
        }

    # ------------------------------------------------------------------
    # DisplayProcessor
    # ------------------------------------------------------------------

    def getDisplayData(self, value, actionHash, **kwargs):
        selected = self._single_value(value)

        if selected is None or selected == "" or selected == "overview":
            return SocketIODisplayData(self.ctx, actionHash).for_overview(self)

        return SocketIODisplayData(self.ctx, actionHash).for_room(
            self,
            str(selected),
        )

    def getTemplate(self, value):
        selected = self._single_value(value)
        if selected is None or selected == "" or selected == "overview":
            return "socketio/overview.html"
        return "socketio/room.html"

    def performActions(self, value, actionHash, **kwargs):
        if kwargs.get("socketio_action") != "send":
            return

        content = kwargs.get("message")
        if content is None or str(content) == "":
            return

        room = kwargs.get("room")
        if room is not None:
            room = str(room).strip() or None

        event = self._build_server_event(
            room=room or "",
            content=content,
            metadata={
                "type": "chat",
                "operation": "upsert",
                "content_type": "text",
            },
        )
        self._send_event(event, room=room)

    def shutdown(self):
        if self._message_bus_unsubscribe is not None:
            self._message_bus_unsubscribe()
            self._message_bus_unsubscribe = None

        service.unbind_event_recorder(self)
        service.unbind_history_provider(self)

        disconnected, disconnect_errors = service.disconnect_all_clients()
        if disconnected:
            self.ctx.logger.info(
                "Disconnected %s Socket.IO clients during shutdown",
                disconnected,
            )
        for sid, error in disconnect_errors:
            self.ctx.logger.warning(
                "Unable to disconnect Socket.IO sid=%s during shutdown: %r",
                sid,
                error,
            )

        if not self._wait_for_pending_persist_callbacks():
            self.ctx.logger.warning(
                "Timed out waiting for %s pending Socket.IO PersistData callbacks",
                self._pending_persist_callbacks,
            )

        service.registry.clear()
        self._history.clear()
        super().shutdown()
