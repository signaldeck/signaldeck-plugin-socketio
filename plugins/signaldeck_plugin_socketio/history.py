from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Iterable


class MessageHistory:
    """Thread-safe bounded-by-age in-memory message history.

    The cache is deliberately independent from PersistData/DataStore. PersistData
    is only used by the processor to load records at startup and to persist new
    records. Runtime lookups by message id are served entirely from this cache.
    """

    def __init__(self, max_age: timedelta = timedelta(days=7)) -> None:
        self.max_age = max_age
        self._lock = RLock()
        self._messages: dict[str, deque[dict[str, Any]]] = {}
        self._id_to_seq: dict[str, dict[str, int]] = {}
        self._next_seq: dict[str, int] = {}

    @staticmethod
    def _id_key(message_id: Any) -> str:
        return str(message_id)

    @staticmethod
    def _timestamp(value: datetime) -> float:
        return value.timestamp()

    def clear(self) -> None:
        with self._lock:
            self._messages.clear()
            self._id_to_seq.clear()
            self._next_seq.clear()

    def replace(self, records: Iterable[dict[str, Any]], *, now: datetime) -> None:
        with self._lock:
            self.clear()
            ordered = sorted(
                (dict(record) for record in records),
                key=lambda record: self._timestamp(record["date"]),
            )
            for record in ordered:
                self._append_locked(record)
            self._prune_locked(now)

    def append(self, record: dict[str, Any], *, now: datetime) -> bool:
        with self._lock:
            inserted = self._append_locked(dict(record))
            self._prune_locked(now)
            return inserted

    def _append_locked(self, record: dict[str, Any]) -> bool:
        room = str(record["room"])
        message_id = self._id_key(record["id"])

        room_index = self._id_to_seq.setdefault(room, {})
        if message_id in room_index:
            # A retry/replay of the same client-side message id must not create
            # another history entry.
            return False

        seq = self._next_seq.get(room, 0)
        self._next_seq[room] = seq + 1

        record["_seq"] = seq
        self._messages.setdefault(room, deque()).append(record)
        room_index[message_id] = seq
        return True

    def _prune_locked(self, now: datetime) -> None:
        cutoff_ts = self._timestamp(now - self.max_age)

        for room in list(self._messages.keys()):
            messages = self._messages[room]
            room_index = self._id_to_seq.get(room, {})

            while messages and self._timestamp(messages[0]["date"]) < cutoff_ts:
                removed = messages.popleft()
                key = self._id_key(removed["id"])
                if room_index.get(key) == removed["_seq"]:
                    room_index.pop(key, None)

            if not messages:
                self._messages.pop(room, None)
                self._id_to_seq.pop(room, None)
                self._next_seq.pop(room, None)

    def messages_after(
        self,
        room: str,
        last_message_id: Any,
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Return messages newer than last_message_id.

        If the id is not in the retained 7-day cache, all retained messages for
        the room are returned. This covers both an id older than the retention
        window and a room whose persistence was enabled only recently.
        """
        with self._lock:
            self._prune_locked(now)

            room = str(room)
            messages = self._messages.get(room)
            if not messages:
                return []

            seq = self._id_to_seq.get(room, {}).get(
                self._id_key(last_message_id)
            )

            if seq is None:
                selected = list(messages)
            else:
                selected = [
                    message
                    for message in messages
                    if message["_seq"] > seq
                ]

            return [
                {
                    key: value
                    for key, value in message.items()
                    if not key.startswith("_")
                }
                for message in selected
            ]

    def room_size(self, room: str, *, now: datetime) -> int:
        with self._lock:
            self._prune_locked(now)
            return len(self._messages.get(str(room), ()))
