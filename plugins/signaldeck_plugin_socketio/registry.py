\
from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Any


@dataclass
class ClientState:
    user_id: Any
    username: str
    sid: str | None
    status: str = "Connected"
    active_rooms: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "id": self.user_id,
            "name": self.username,
            "status": self.status,
            "sid": self.sid,
            "active_rooms": sorted(self.active_rooms),
        }


class ClientRegistry:
    """Thread-safe in-memory client and room registry.

    The registry deliberately stores no message contents and no history.
    A user id represents a single active Socket.IO connection. If the same
    user id reconnects, the newer connection replaces the older one.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._users: dict[str, ClientState] = {}
        self._sid_to_user_id: dict[str, str] = {}

    @staticmethod
    def _key(user_id: Any) -> str:
        return str(user_id)

    def connect(self, user_id: Any, sid: str, username: str | None) -> str | None:
        key = self._key(user_id)
        display_name = str(username or key).strip() or key

        with self._lock:
            old_sid: str | None = None
            existing = self._users.get(key)
            if existing is not None:
                old_sid = existing.sid
                if old_sid:
                    self._sid_to_user_id.pop(old_sid, None)

            self._users[key] = ClientState(
                user_id=user_id,
                username=display_name,
                sid=sid,
                status="Connected",
            )
            self._sid_to_user_id[sid] = key
            return old_sid

    def disconnect(self, sid: str) -> dict | None:
        with self._lock:
            key = self._sid_to_user_id.pop(sid, None)
            if key is None:
                return None

            client = self._users.get(key)
            if client is None or client.sid != sid:
                return None

            snapshot = client.to_dict()
            client.sid = None
            client.status = "Disconnected"
            client.active_rooms.clear()
            return snapshot

    def get_by_sid(self, sid: str) -> dict | None:
        with self._lock:
            key = self._sid_to_user_id.get(sid)
            if key is None:
                return None
            client = self._users.get(key)
            if client is None:
                return None
            return client.to_dict()

    def add_room(self, sid: str, room: str) -> bool:
        with self._lock:
            client = self._client_for_sid_locked(sid)
            if client is None:
                return False
            client.active_rooms.add(room)
            return True

    def remove_room(self, sid: str, room: str) -> bool:
        with self._lock:
            client = self._client_for_sid_locked(sid)
            if client is None:
                return False
            existed = room in client.active_rooms
            client.active_rooms.discard(room)
            return existed

    def is_in_room(self, sid: str, room: str) -> bool:
        with self._lock:
            client = self._client_for_sid_locked(sid)
            return client is not None and room in client.active_rooms

    def rooms_for_sid(self, sid: str) -> list[str]:
        with self._lock:
            client = self._client_for_sid_locked(sid)
            return sorted(client.active_rooms) if client else []

    def clients_for_room(self, room: str) -> list[dict]:
        with self._lock:
            return [
                client.to_dict()
                for client in self._users.values()
                if client.status == "Connected" and room in client.active_rooms
            ]

    def users(self) -> list[dict]:
        with self._lock:
            return [client.to_dict() for client in self._users.values()]

    def display_name_for_user_id(self, user_id: Any) -> str | None:
        with self._lock:
            client = self._users.get(self._key(user_id))
            return client.username if client is not None else None

    def active_rooms(self) -> list[str]:
        with self._lock:
            rooms: set[str] = set()
            for client in self._users.values():
                if client.status == "Connected":
                    rooms.update(client.active_rooms)
            return sorted(rooms)

    def connected_count(self) -> int:
        with self._lock:
            return sum(
                1 for client in self._users.values()
                if client.status == "Connected" and client.sid is not None
            )

    def active_sids(self) -> list[str]:
        """Return a thread-safe snapshot of currently connected Socket.IO SIDs."""
        with self._lock:
            return [
                client.sid
                for client in self._users.values()
                if client.status == "Connected" and client.sid is not None
            ]

    def clear(self) -> None:
        with self._lock:
            self._users.clear()
            self._sid_to_user_id.clear()

    def _client_for_sid_locked(self, sid: str) -> ClientState | None:
        key = self._sid_to_user_id.get(sid)
        if key is None:
            return None
        return self._users.get(key)
