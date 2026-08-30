# signaldeck-plugin-socketio

Socket.IO communication plugin for SignalDeck.

## Features

- Socket.IO integrated into the SignalDeck Flask application
- `async_mode="threading"`; no eventlet dependency
- thread-safe client/room registry
- SignalDeck `DisplayProcessor` UI
- generic event transport with opaque payloads
- room-specific persistence policy
- event history through normal SignalDeck `PersistData`
- maximum server history window: 7 days
- no Socket.IO-specific DataStore implementation
- opaque HTTP blob storage with independent read/delete capabilities

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

## Persisted event format

For rooms with `persist: true`, the generic event envelope is persisted. `payload`
is stored as opaque JSON and optional `key_id` is preserved. The backend does not
interpret application-specific event types or encrypted payloads.

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

The plugin injects its fixed event fields into this PersistData config.

## PersistData.get_records()

For history bootstrap the processor expects the generic PersistData API:

```python
get_records(
    fields: list[str],
    days: int,
    config=None,
) -> pandas.DataFrame
```

There is deliberately no Socket.IO-specific SQLite/DataStore subclass.

## 7-day RAM history

When the processor starts:

```text
PersistData.get_records(days=7)
        ↓
MessageHistory RAM cache
        ↓
room + event-id index
```

Every new persistent event is added immediately to this RAM cache and then
written through `PersistData.save_data()`.

The cache removes anything older than exactly seven days. Therefore the server
never returns history older than seven days even if the configured DataStore
retains records for longer.

## Missed-event behavior

For non-persistent rooms, clients can supply missing events peer-to-peer. For
persistent rooms the server answers from the retained seven-day history.

## HTTP endpoints

Existing endpoints remain available:

- `GET /plugin/socketio/health`
- `GET /plugin/socketio/clients/<room>`
- `POST /plugin/socketio/send`

### Opaque blob storage

The blob store is intentionally domain-neutral. It knows nothing about images,
video, audio, rooms, event types, encryption or expiry. Clients may upload any
binary byte sequence and reference the resulting capabilities from their own
event payload schema.

Configuration defaults:

```json
{
  "blob_store_path": "data/socketio-blobs",
  "max_blob_size_bytes": 52428800
}
```

`max_blob_size_bytes` defaults to 50 MiB and is enforced while streaming the
request body, not only through `Content-Length`.

#### Upload

```http
POST /plugin/socketio/blobs
Content-Type: application/octet-stream

<opaque bytes>
```

Response:

```json
{
  "blobId": "...",
  "readToken": "...",
  "deleteToken": "...",
  "size": 12345
}
```

The read and delete tokens are independent random capabilities. They are
returned only when the blob is created. The store persists only SHA-256 hashes
of them.

#### Download

```http
GET /plugin/socketio/blobs/<blobId>
Authorization: Bearer <readToken>
```

The response is always `application/octet-stream`; the server does not know the
actual content type. HTTP conditional/range handling is delegated to Flask's
`send_file` implementation.

#### Delete

```http
DELETE /plugin/socketio/blobs/<blobId>
Authorization: Bearer <deleteToken>
```

Deletion is idempotent. A client may safely issue it after another client has
already removed the blob.

Expiry is deliberately not implemented by the blob store. An application can
keep `expiresAt` inside its (possibly end-to-end encrypted) event payload. A
client that determines that the content has expired can use the `deleteToken`
to remove the blob.

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

End-to-end encryption remains a client concern. SignalDeck preserves optional
`key_id` transport metadata and treats encrypted payloads and encrypted blobs as
opaque data. The server neither owns nor receives room or blob encryption keys.
