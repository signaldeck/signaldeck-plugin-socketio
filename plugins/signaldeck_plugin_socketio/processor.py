from __future__ import annotations

import time
from datetime import datetime, timedelta
from threading import Condition, RLock
from typing import Any
from zoneinfo import ZoneInfo

from signaldeck_sdk import DisplayProcessor, PersistData

from .displaydata import SocketIODisplayData
from .history import MessageHistory
from .service import service


HISTORY_DAYS = 7

MESSAGE_PERSIST_FIELDS = [
    {"name": "date", "dtype": "datetime", "display_name": "Date"},
    {"name": "room", "dtype": "str", "display_name": "Room"},
    {"name": "transmitter", "dtype": "str", "display_name": "Transmitter"},
    {"name": "transmitterName", "dtype": "str", "display_name": "Transmitter Name"},
    {"name": "id", "dtype": "str", "display_name": "Message ID"},
    {"name": "content", "dtype": "str", "display_name": "Content"},
    {"name": "contentType", "dtype": "str", "display_name": "Content Type"},
    {"name": "isSystem", "dtype": "int", "display_name": "System Message"},
]

HISTORY_FIELDS = [
    "room",
    "transmitter",
    "transmitterName",
    "id",
    "content",
    "contentType",
    "isSystem",
]


class SocketIOProcessor(PersistData, DisplayProcessor):
    def __init__(self, name, config, ctx, valueProvider, collect_data):
        config = self._normalize_persist_config(config)
        super().__init__(name, config, ctx, valueProvider, collect_data)

        self._persist_lock = RLock()
        self._persist_condition = Condition()
        self._pending_persist_callbacks = 0
        self._history = MessageHistory(max_age=timedelta(days=HISTORY_DAYS))

        service.configure(self.config)
        self._validate_persistence_configuration()
        service.bind_message_recorder(self, self.record_message)
        service.bind_history_provider(self, self.get_missed_messages)

    @staticmethod
    def _normalize_persist_config(config: dict) -> dict:
        normalized = dict(config)
        persist_configs = []

        for persist_config in config.get("persist", []):
            item = dict(persist_config)
            item["fields"] = [dict(field) for field in MESSAGE_PERSIST_FIELDS]
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

    def _timezone(self):
        return ZoneInfo(self.config.get("timezone", "Europe/Berlin"))

    def _now(self) -> datetime:
        return self.ctx.date.now(self._timezone())

    def _datetime_from_millis(self, value: Any) -> datetime:
        try:
            return datetime.fromtimestamp(
                int(value) / 1000.0,
                tz=self._timezone(),
            )
        except (TypeError, ValueError, OSError):
            return self._now()

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
            "Loaded %s Socket.IO history records (max %s days)",
            len(normalized),
            HISTORY_DAYS,
        )
        return records

    def _records_from_dataframe(self, dataframe) -> list[dict]:
        if dataframe is None or getattr(dataframe, "empty", True):
            return []

        records = []

        for index, row in dataframe.iterrows():
            date = row.get("date", index)
            if hasattr(date, "to_pydatetime"):
                date = date.to_pydatetime()
            if not isinstance(date, datetime):
                continue

            room = row.get("room")
            transmitter = row.get("transmitter")
            message_id = row.get("id")
            content = row.get("content")

            if any(
                self._missing(value)
                for value in (room, transmitter, message_id, content)
            ):
                continue

            transmitter_name = row.get("transmitterName")
            if self._missing(transmitter_name):
                transmitter_name = (
                    service.registry.display_name_for_user_id(transmitter)
                    or str(transmitter)
                )

            content_type = row.get("contentType")
            if self._missing(content_type):
                content_type = "text"

            is_system = row.get("isSystem")
            if self._missing(is_system):
                is_system = False
            else:
                try:
                    is_system = bool(int(is_system))
                except (TypeError, ValueError):
                    is_system = str(is_system).lower() == "true"

            records.append(
                {
                    "room": str(room),
                    "transmitter": str(transmitter),
                    "transmitterName": str(transmitter_name),
                    "id": str(message_id),
                    "content": str(content),
                    "contentType": str(content_type),
                    "isSystem": bool(is_system),
                    "date": date,
                }
            )

        return records

    # ------------------------------------------------------------------
    # New messages / PersistData
    # ------------------------------------------------------------------

    def record_message(self, message: dict[str, Any]) -> None:
        room = str(message.get("room", ""))
        if not room or not service.may_persist(room):
            return

        data = {
            "room": room,
            "transmitter": str(message.get("transmitter") or ""),
            "transmitterName": str(
                message.get("transmitterName")
                or message.get("transmitter")
                or ""
            ),
            "id": str(message.get("id") or ""),
            "content": str(message.get("content") or ""),
            "contentType": str(message.get("contentType") or "text"),
            "isSystem": 1 if bool(message.get("isSystem", False)) else 0,
            "date": self._datetime_from_millis(message.get("date")),
        }

        now = self._now()
        cache_record = {
            **data,
            "isSystem": bool(data["isSystem"]),
        }

        inserted = self._history.append(cache_record, now=now)
        if not inserted:
            self.ctx.logger.debug(
                "Socket.IO history already contains room=%s id=%s",
                room,
                data["id"],
            )
            return

        loop = getattr(self.valueProvider, "loop", None)
        if loop is not None and loop.is_running():
            with self._persist_condition:
                self._pending_persist_callbacks += 1

            try:
                loop.call_soon_threadsafe(
                    self._save_message_from_runtime,
                    data,
                )
            except Exception:
                with self._persist_condition:
                    self._pending_persist_callbacks -= 1
                    self._persist_condition.notify_all()
                raise
        else:
            self._save_message(data)

    def _save_message_from_runtime(self, data: dict) -> None:
        try:
            self._save_message(data)
        finally:
            with self._persist_condition:
                self._pending_persist_callbacks -= 1
                self._persist_condition.notify_all()

    def _save_message(self, data: dict) -> None:
        with self._persist_lock:
            self.save_data(data)

    def _wait_for_pending_persist_callbacks(self, timeout: float = 5.0) -> bool:
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

    def get_missed_messages(
        self,
        *,
        room: str,
        last_message_id: Any,
    ) -> list[dict]:
        if not service.may_persist(room):
            return []

        messages = self._history.messages_after(
            room,
            last_message_id,
            now=self._now(),
        )

        result = []
        for message in messages:
            transmitter = str(message["transmitter"])
            transmitter_name = message.get("transmitterName")
            if not transmitter_name:
                transmitter_name = (
                    service.registry.display_name_for_user_id(transmitter)
                    or transmitter
                )

            result.append(
                {
                    "id": str(message["id"]),
                    "date": self._millis_from_datetime(message["date"]),
                    "transmitter": transmitter,
                    "transmitterName": str(transmitter_name),
                    "room": str(message["room"]),
                    "content": str(message["content"]),
                    "contentUri": None,
                    "contentType": str(message.get("contentType") or "text"),
                    "isSystem": bool(message.get("isSystem", False)),
                }
            )

        return result

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

        service.send_server_message(
            content,
            room=room,
        )

    def shutdown(self):
        service.unbind_message_recorder(self)
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
