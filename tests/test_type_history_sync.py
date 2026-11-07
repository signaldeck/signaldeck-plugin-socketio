"""Compatibility and page-streaming tests for join_room history synchronization."""
import pytest
from flask import Flask
from flask_socketio import SocketIO

from signaldeck_plugin_socketio import events as socket_events
from signaldeck_plugin_socketio.service import SocketIOService


@pytest.mark.parametrize("bad", [
    [], [{"type": ""}], [{"type": "chat", "days": 0}],
    [{"type": "chat", "days": True}], [{"type": "chat", "days": -1}],
    [{"type": "chat"}, {"type": "chat", "days": 7}],
    "chat", [{"type": "chat", "days": "7"}],
])
def test_sync_validation_rejects_bad_requests(bad):
    with pytest.raises(ValueError):
        socket_events._parse_history_sync({"sync": bad})


def test_legacy_join_has_no_type_filter():
    assert socket_events._parse_history_sync({"room": "family", "lastEventID": "last"}) is None


def test_typed_sync_supports_unlimited_appointments_and_short_lived_chat():
    assert socket_events._parse_history_sync({
        "sync": [{"type": "chat", "days": 7}, {"type": "appointment", "days": None}],
    }) == [("chat", 7), ("appointment", None)]


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(socket_events, "_registered", False)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-key"
    socketio = SocketIO(app, async_mode="threading")
    service = SocketIOService()
    service.configure({"rooms": [{"name": "family", "persist": True}, {"name": "temp", "persist": False}]})
    socket_events.register_socketio_events(socketio, service)
    yield socketio, app, service
    monkeypatch.setattr(socket_events, "_registered", False)


def test_filtered_join_streams_same_missed_events_and_completion(server):
    socketio, app, service = server
    calls = []

    def page_provider(**kwargs):
        calls.append(kwargs)
        kind = kwargs["event_type"]
        if kind == "chat":
            return ([{"id": "c1", "type": "chat"}], "next") if kwargs["cursor"] is None else ([{"id": "c2", "type": "chat"}], None)
        return ([{"id": "a1", "type": "appointment"}], None)

    service.bind_history_page_provider(object(), page_provider)
    client = socketio.test_client(app, auth={"id": "user", "username": "User"})
    assert client.is_connected()
    response = client.emit("join_room", {
        "room": "family", "syncId": "test-sync", "pageSize": 1,
        "sync": [{"type": "chat", "days": 7}, {"type": "appointment", "days": None}],
    }, callback=True)
    assert response["ok"] is True

    received = []
    for _ in range(50):
        received.extend(client.get_received())
        if any(item["name"] == "history_sync_complete" for item in received):
            break
        socketio.sleep(0.02)

    batches = [item["args"][0] for item in received if item["name"] == "missed_events"]
    assert [[event["id"] for event in batch] for batch in batches] == [["c1"], ["c2"], ["a1"]]
    completed = [item["args"][0] for item in received if item["name"] == "history_sync_complete"]
    assert completed == [{
        "ok": True, "room": "family", "syncId": "test-sync",
        "counts": {"chat": 2, "appointment": 1},
    }]
    assert calls[0]["date_from"] is not None
    assert calls[0]["date_from"] == calls[1]["date_from"], "Date window must remain stable across pages"
    assert calls[-1]["date_from"] is None
    client.disconnect()


def test_legacy_join_calls_original_provider(server):
    socketio, app, service = server
    calls = []
    service.bind_history_provider(object(), lambda **kwargs: calls.append(kwargs) or [{"id": "old"}])
    client = socketio.test_client(app, auth={"id": "u1"})
    response = client.emit("join_room", {"room": "family", "lastEventID": "previous"}, callback=True)
    assert response["ok"] is True
    messages = client.get_received()
    assert any(item["name"] == "missed_events" and item["args"][0] == [{"id": "old"}] for item in messages)
    assert not any(item["name"] == "history_sync_complete" for item in messages)
    assert calls == [{"room": "family", "last_event_id": "previous"}]
    client.disconnect()


def test_typed_sync_rejects_transient_room_without_joining(server):
    socketio, app, service = server
    client = socketio.test_client(app, auth={"id": "u1"})
    ack = client.emit("join_room", {"room": "temp", "sync": [{"type": "chat", "days": 7}]}, callback=True)
    assert ack["ok"] is False
    assert ack["error"] == "room_not_persistent"
    client.disconnect()
