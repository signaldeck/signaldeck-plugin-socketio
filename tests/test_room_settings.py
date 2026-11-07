from signaldeck_plugin_socketio.service import SocketIOService


def test_configured_room_can_enable_persistence_policy():
    service = SocketIOService()
    service.configure({
        "rooms": [
            {"name": "family", "display_name": "Familie", "persist": True}
        ]
    })

    settings = service.room_settings("family")

    assert settings.configured is True
    assert settings.persist is True
    assert service.may_persist("family") is True


def test_unknown_room_is_always_non_persistent():
    service = SocketIOService()
    service.configure({
        "rooms": [
            {"name": "family", "persist": True}
        ]
    })

    settings = service.room_settings("runtime-room")

    assert settings.configured is False
    assert settings.persist is False
    assert service.may_persist("runtime-room") is False


def test_only_persistent_configured_room_calls_recorder():
    service = SocketIOService()
    calls = []

    owner = object()
    service.bind_event_recorder(owner, lambda event: calls.append(event))
    service.configure({
        "rooms": [
            {"name": "saved", "persist": True},
            {"name": "transient", "persist": False},
        ]
    })

    saved = service.create_event(
        room="saved", transmitter="user-1",
        event_id="m-1", payload={"text": "one"},
    )
    transient = service.create_event(
        room="transient", transmitter="user-1",
        event_id="m-2", payload={"text": "two"},
    )
    unknown = service.create_event(
        room="unknown", transmitter="user-1",
        event_id="m-3", payload={"text": "three"},
    )

    assert service.record_event(saved) is True
    assert service.record_event(transient) is False
    assert service.record_event(unknown) is False
    assert calls == [saved]


def test_persistent_room_uses_bound_history_provider():
    service = SocketIOService()
    service.configure({
        "rooms": [
            {"name": "saved", "persist": True},
            {"name": "transient", "persist": False},
        ]
    })

    owner = object()
    calls = []
    service.bind_history_provider(
        owner,
        lambda **kwargs: calls.append(kwargs) or [{"id": "2", "content": "x"}],
    )

    result = service.get_missed_events("saved", "1")

    assert result == [{"id": "2", "content": "x"}]
    assert calls == [{"room": "saved", "last_event_id": "1"}]
