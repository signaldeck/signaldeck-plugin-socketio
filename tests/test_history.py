from datetime import datetime, timedelta, timezone

from signaldeck_plugin_socketio.history import MessageHistory


def _message(mid, date, room="private"):
    return {
        "room": room,
        "transmitter": "user-1",
        "id": mid,
        "content": f"message-{mid}",
        "date": date,
    }


def test_messages_after_known_id():
    now = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
    history = MessageHistory(timedelta(days=7))
    history.replace(
        [
            _message("1", now - timedelta(hours=3)),
            _message("2", now - timedelta(hours=2)),
            _message("3", now - timedelta(hours=1)),
        ],
        now=now,
    )

    result = history.messages_after("private", "1", now=now)

    assert [message["id"] for message in result] == ["2", "3"]


def test_unknown_id_returns_complete_retained_history():
    now = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
    history = MessageHistory(timedelta(days=7))
    history.replace(
        [
            _message("2", now - timedelta(hours=2)),
            _message("3", now - timedelta(hours=1)),
        ],
        now=now,
    )

    result = history.messages_after("private", "old-id", now=now)

    assert [message["id"] for message in result] == ["2", "3"]


def test_history_is_strictly_limited_to_seven_days():
    now = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
    history = MessageHistory(timedelta(days=7))
    history.replace(
        [
            _message("too-old", now - timedelta(days=7, seconds=1)),
            _message("edge", now - timedelta(days=7)),
            _message("new", now),
        ],
        now=now,
    )

    result = history.messages_after("private", "missing", now=now)

    assert [message["id"] for message in result] == ["edge", "new"]


def test_duplicate_message_id_is_not_added_twice():
    now = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
    history = MessageHistory(timedelta(days=7))

    assert history.append(_message("1", now), now=now) is True
    assert history.append(_message("1", now), now=now) is False
    assert history.room_size("private", now=now) == 1
