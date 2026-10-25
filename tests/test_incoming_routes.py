"""Selective inbound message routing never publishes ordinary room traffic."""
from types import SimpleNamespace
import pytest

from signaldeck_plugin_socketio.incoming_routes import IncomingMessageRouter


class Context:
    def __init__(self):
        self.messages = []

    def message(self, **kwargs):
        self.messages.append(kwargs)


def event(**changes):
    result = {
        "type": "chat", "operation": "upsert", "room": "haus",
        "id": "message-1", "transmitter": "user-1",
        "transmitterName": "Test User",
        "payload": {"text": "@haus Wie warm ist es im Garten?",
                    "contentType": "text", "system": False},
    }
    result.update(changes)
    return result


def router():
    return IncomingMessageRouter([{
        "rooms": ["haus"], "type": "chat",
        "prefix": "@haus", "source": "socketio.ai",
    }])


def test_only_configured_prefix_publishes_stripped_prompt():
    context = Context()
    assert router().publish(context, event()) == 1
    assert context.messages == [{
        "source": "socketio.ai",
        "content": "Wie warm ist es im Garten?",
        "metadata": {
            "room": "haus", "event_id": "message-1",
            "transmitter": "user-1", "transmitter_name": "Test User",
        },
    }]


@pytest.mark.parametrize("overrides", [
    {"room": "privat"},
    {"type": "appointment"},
    {"operation": "delete"},
    {"payload": {"text": "normale Nachricht"}},
    {"payload": {"text": "@hausmeister geht vorbei"}},
    {"payload": {"text": "@haus"}},
    {"payload": {"text": "@haus   "}},
    {"payload": {"text": "@haus Bitte", "contentType": "image"}},
    {"payload": {"text": "@haus Bitte", "system": True}},
    {"payload": {"ciphertext": "encoded-secret"}},
    {"payload": {"text": None}},
])
def test_unmatched_traffic_is_not_published(overrides):
    context = Context()
    assert router().publish(context, event(**overrides)) == 0
    assert context.messages == []


def test_case_insensitive_prefix_requires_word_boundary():
    context = Context()
    assert router().publish(context, event(
        payload={"text": "@HAUS: was geht?"}
    )) == 0
    assert router().publish(context, event(
        payload={"text": "@HAUS  was geht?"}
    )) == 1
    assert context.messages[0]["content"] == "was geht?"


@pytest.mark.parametrize("bad", [
    {}, {"rooms": [], "type": "chat", "prefix": "@haus", "source": "ai"},
    {"rooms": ["haus"], "type": "appointment", "prefix": "@haus", "source": "ai"},
    {"rooms": ["haus"], "type": "chat", "prefix": "", "source": "ai"},
    {"rooms": ["haus"], "type": "chat", "prefix": "@haus", "source": ""},
])
def test_invalid_routes_fail_during_startup(bad):
    with pytest.raises(ValueError):
        IncomingMessageRouter([bad])


def test_default_does_not_publish_anything():
    ctx = Context()
    assert IncomingMessageRouter([]).publish(ctx, event()) == 0
    assert not ctx.messages
