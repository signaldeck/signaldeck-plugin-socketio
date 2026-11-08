"""Room capabilities, opaque extra types and backwards-compatible joins."""
from flask import Flask
from flask_socketio import SocketIO
from signaldeck_plugin_socketio import events as socket_events
from signaldeck_plugin_socketio.service import SocketIOService
import pytest

@pytest.fixture
def setup_server(monkeypatch):
    monkeypatch.setattr(socket_events, "_registered", False)
    service = SocketIOService()
    service.configure({
        "object_types": {"appointment": {"sync_days": None}},
        "rooms": [
            {"name": "family", "display_name": "Familie", "persist": True, "object_types": ["appointment"]},
            {"name": "haus", "persist": False},
        ],
        "incoming_message_routes": [
            {"rooms": ["haus"], "type": "chat", "prefix": "@haus", "source": "socketio.ai"}
        ],
    })
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"
    socketio = SocketIO(app, async_mode="threading")
    socket_events.register_socketio_events(socketio, service)
    yield socketio, app, service
    monkeypatch.setattr(socket_events, "_registered", False)

def test_capabilities_are_room_scoped(setup_server):
    _, _, service = setup_server
    assert service.room_client_config("family") == {
        "objectTypes": {"appointment": {"sync_days": None}}, "aiPrefixes": [], "displayName": "Familie", "persistent": True
    }
    assert service.room_client_config("haus") == {
        "objectTypes": {}, "aiPrefixes": ["@haus"], "displayName": "haus", "persistent": False
    }
    assert service.room_client_config("unknown") == {
        "objectTypes": {}, "aiPrefixes": [], "displayName": "unknown", "persistent": False
    }

def test_join_ack_contains_public_capabilities(setup_server):
    socketio, app, service = setup_server
    client = socketio.test_client(app, auth={"id": "user"})
    ack = client.emit("join_room", {"room": "haus", "autoSync": True}, callback=True)
    assert ack["ok"] is True
    assert ack["roomConfig"]["objectTypes"] == {}
    assert ack["roomConfig"]["aiPrefixes"] == ["@haus"]
    assert ack["roomConfig"]["displayName"] == "haus"
    assert ack["roomConfig"]["persistent"] is False
    client.disconnect()

def test_display_name_lookup_also_covers_unsubscribed_rooms(setup_server):
    socketio, app, _ = setup_server
    client = socketio.test_client(app, auth={"id": "user"})
    ack = client.emit("get_room_display_names", callback=True)
    assert ack == {
        "ok": True,
        "rooms": {"family": "Familie", "haus": "haus"},
    }
    client.disconnect()


def test_auto_sync_only_configured_types(setup_server):
    socketio, app, service = setup_server
    calls = []
    def fetch(**kwargs):
        calls.append(kwargs["event_type"])
        return [], None
    service.bind_history_page_provider(object(), fetch)
    client = socketio.test_client(app, auth={"id": "user"})
    ack = client.emit("join_room", {"room": "family", "autoSync": True}, callback=True)
    assert ack["ok"]
    for _ in range(50):
        if len(calls) >= 4:
            break
        socketio.sleep(0.01)
    assert calls == ["chat", "image", "system", "appointment"]
    client.disconnect()

def test_invalid_room_object_type_rejected():
    service = SocketIOService()
    with pytest.raises(ValueError):
        service.configure({"rooms": [{"name": "family", "object_types": ["missing"]}]})

def test_extra_types_need_explicit_room_enablement(setup_server):
    socketio, app, _ = setup_server
    client = socketio.test_client(app, auth={"id": "user"})
    client.emit("join_room", {"room": "haus"}, callback=True)
    event = {
        "schemaVersion": 1, "id": "1", "createdAt": 1760000000000,
        "room": "haus", "type": "appointment", "operation": "upsert",
        "objectId": "a1", "revision": 1, "payload": {"title": "test"},
    }
    ack = client.emit("send_event", event, callback=True)
    assert ack["ok"] is False
    assert ack["error"] == "type_not_allowed"
    client.disconnect()
