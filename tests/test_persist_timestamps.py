"""Persist unique technical timestamps without changing events or the SQLite schema."""
from datetime import datetime, timezone
from threading import RLock

import pandas as pd

from signaldeck_sdk import RecordsPage
from signaldeck_plugin_socketio.processor import SocketIOProcessor


def make_processor(last_saved=None):
    processor = SocketIOProcessor.__new__(SocketIOProcessor)
    processor._timestamp_lock = RLock()
    processor._last_persist_timestamp_ms = None
    processor.config = {"timezone": "Europe/Berlin"}
    calls = []

    def history(**kwargs):
        calls.append(kwargs)
        if last_saved is None:
            return RecordsPage(pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC")))
        dt = datetime.fromtimestamp(last_saved / 1000, timezone.utc)
        return RecordsPage(pd.DataFrame({"id": ["last"]}, index=pd.DatetimeIndex([dt], name="date")))

    processor.get_records_page = history
    return processor, calls


def example_event():
    created = datetime.fromtimestamp(1000, tz=timezone.utc)
    received = datetime.fromtimestamp(2000, tz=timezone.utc)
    return {
        "schemaVersion": 1,
        "id": "event-1",
        "createdAt": created,
        "receivedAt": received,
        "room": "family",
        "transmitter": "a",
        "transmitterName": "A",
        "type": "appointment",
        "operation": "upsert",
        "objectId": "object-1",
        "revision": 1,
        "payload": {"text": "test"},
    }


def test_unique_persist_timestamp_preserves_received_at(monkeypatch):
    processor, calls = make_processor()
    monkeypatch.setattr("signaldeck_plugin_socketio.processor.time.time", lambda: 10000.123)
    event = example_event()
    first = processor._event_to_persist_data(event)
    second = processor._event_to_persist_data(event)
    assert int(second["date"].timestamp() * 1000) == int(first["date"].timestamp() * 1000) + 1
    assert first["receivedAt"] == event["receivedAt"]
    assert second["receivedAt"] == event["receivedAt"]
    assert first["createdAt"] == event["createdAt"]
    assert calls == [{"fields": ["id"], "limit": 1, "order": "desc"}]


def test_counter_is_seeded_from_latest_persisted_record(monkeypatch):
    processor, _ = make_processor(last_saved=20000)
    monkeypatch.setattr("signaldeck_plugin_socketio.processor.time.time", lambda: 10.0)
    assert processor._next_persist_timestamp_ms() == 20001


def test_old_persisted_events_use_date_as_received_at():
    processor, _ = make_processor()
    persist_date = datetime(2026, 1, 3, tzinfo=timezone.utc)
    data = pd.DataFrame([{
        "room": "family",
        "id": "event-1",
        "transmitter": "a",
        "type": "chat",
        "objectId": "event-1",
        "payload": '{"text":"hello"}',
    }], index=pd.DatetimeIndex([persist_date], name="date"))
    events = processor._records_from_dataframe(data)
    assert len(events) == 1
    assert events[0]["receivedAt"] == persist_date
