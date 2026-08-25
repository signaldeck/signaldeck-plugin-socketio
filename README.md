# signaldeck-plugin-socketio

Socket.IO communication plugin for SignalDeck.

## Features

- Socket.IO integrated into the SignalDeck Flask application
- `async_mode="threading"`; no eventlet dependency
- thread-safe client/room registry
- SignalDeck `DisplayProcessor` UI
- existing Socket.IO and HTTP endpoints retained
- room-specific persistence policy
- message history through normal SignalDeck `PersistData`
- maximum server history window: 7 days
- no Socket.IO-specific DataStore implementation

## Room configuration

```json
{
  "rooms": [
    {
      "name": "family",
      "display_name": "Familie",
      "persist": true
    },
    {
      "name": "private",
      "display_name": "Privat",
      "persist": false
    }
  ]
}
```

Unknown rooms remain valid but always behave as:

```text
configured = false
persist = false
```

## Persisted message format

For rooms with `persist: true`:

```json
{
  "room": "<room name>",
  "transmitter": "<user id>",
  "id": "<message id>",
  "content": "<message content>",
  "date": "<server receive time>"
}
```

The DataStore is selected through the normal SignalDeck PersistData config:

```json
{
  "persist": [
    {
      "type": "sqlite_socketio"
    }
  ]
}
```

The plugin injects its fixed message fields into this PersistData config.

## PersistData.get_records()

For history bootstrap the processor expects the generic PersistData API:

```python
get_records(
    fields: list[str],
    days: int,
    config=None,
) -> pandas.DataFrame
```

The Socket.IO processor calls:

```python
self.get_records(
    fields=["room", "transmitter", "id", "content"],
    days=7,
    config=self.config,
)
```

The returned DataFrame may contain `date` as a column or use the record
timestamp as its index.

There is deliberately no Socket.IO-specific SQLite/DataStore subclass.

## 7-day RAM history

When the processor starts:

```text
PersistData.get_records(days=7)
        ↓
MessageHistory RAM cache
        ↓
room + message-id index
```

Every new persistent message is added immediately to this RAM cache and then
written through `PersistData.save_data()`.

The cache removes anything older than exactly seven days. Therefore the server
never returns history older than seven days even if the configured DataStore
retains records for longer.

## Missed-message behavior

When a client joins with `lastMessageID`:

```text
persist = false
    -> request_new_messages
    -> other room clients answer as before

persist = true
    -> no request_new_messages
    -> query the 7-day RAM cache
    -> emit missed_messages directly
```

If `lastMessageID` is found, only newer messages are returned.

If `lastMessageID` is no longer in the retained history, the complete remaining
history of at most seven days is returned. This also handles the case where
persistence was enabled only after the client already had older messages.

A persistent room never falls back to peer history on a server-history error.

## HTTP endpoints

Existing endpoints remain available:

- `GET /plugin/socketio/health`
- `GET /plugin/socketio/clients/<room>`
- `POST /plugin/socketio/send`

## Development

The sample Docker setup uses the normal SignalDeck SQLite plugin:

```text
signaldeck_plugin_sqlite.persistence.sqlite_store.SqliteStore
```

Start with:

```bash
docker compose up --build
```

## Encryption

Not implemented yet. A later end-to-end encryption layer can encrypt `content`
on the Android clients; SignalDeck/PersistData can then keep treating it as an
opaque string.
