\
from signaldeck_plugin_socketio.registry import ClientRegistry


def test_connect_join_disconnect():
    registry = ClientRegistry()

    registry.connect("user-1", "sid-1", "Alice")
    assert registry.connected_count() == 1

    assert registry.add_room("sid-1", "private")
    assert registry.is_in_room("sid-1", "private")
    assert registry.active_rooms() == ["private"]

    client = registry.clients_for_room("private")[0]
    assert client["name"] == "Alice"

    disconnected = registry.disconnect("sid-1")
    assert disconnected["name"] == "Alice"
    assert registry.connected_count() == 0
    assert registry.active_rooms() == []


def test_reconnect_replaces_old_sid():
    registry = ClientRegistry()

    assert registry.connect("user-1", "sid-1", "Alice") is None
    old_sid = registry.connect("user-1", "sid-2", "Alice")

    assert old_sid == "sid-1"
    assert registry.get_by_sid("sid-1") is None
    assert registry.get_by_sid("sid-2")["name"] == "Alice"
    assert registry.connected_count() == 1
