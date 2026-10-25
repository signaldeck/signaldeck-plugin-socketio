"""Opt-in routing of validated incoming Socket.IO chat events to SignalDeck."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class IncomingMessageRouter:
    def __init__(self, routes: Any):
        if not isinstance(routes, list):
            raise ValueError("incoming_message_routes must be a list")
        self.routes = []
        for route in routes:
            if not isinstance(route, dict) or set(route) != {
                "rooms", "type", "prefix", "source"
            }:
                raise ValueError(
                    "incoming_message_routes entries require rooms, type, prefix and source"
                )
            rooms = route["rooms"]
            if (not isinstance(rooms, list) or not rooms
                    or any(not isinstance(r, str) or not r.strip() for r in rooms)):
                raise ValueError("incoming_message_routes rooms must be nonempty strings")
            event_type, prefix, source = (
                route["type"], route["prefix"], route["source"]
            )
            if (event_type != "chat" or not isinstance(prefix, str)
                    or not prefix.strip() or prefix != prefix.strip()
                    or not isinstance(source, str) or not source.strip()):
                raise ValueError("Invalid inbound route type, prefix or source")
            self.routes.append((frozenset(rooms), prefix.casefold(), source))

    def publish(self, context: Any, event: Mapping[str, Any]) -> int:
        """Only a matched cleartext chat event crosses the message bus."""
        if context is None or event.get("type") != "chat" or event.get("operation") != "upsert":
            return 0
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            return 0
        text = payload.get("text")
        if (not isinstance(text, str) or not text
                or payload.get("contentType", "text") != "text"
                or payload.get("system") is True):
            return 0

        count = 0
        for rooms, prefix, source in self.routes:
            if event.get("room") not in rooms:
                continue
            # Require a word boundary, not "@hausmeister".
            if text[:len(prefix)].casefold() != prefix:
                continue
            remainder = text[len(prefix):]
            if remainder and not remainder[0].isspace():
                continue
            prompt = remainder.strip()
            if not prompt:
                continue
            context.message(
                source=source,
                content=prompt,
                metadata={
                    "room": event["room"],
                    "event_id": event["id"],
                    "transmitter": event["transmitter"],
                    "transmitter_name": event.get("transmitterName", ""),
                },
            )
            count += 1
        return count
