from io import BytesIO

import pytest

from signaldeck_plugin_socketio.blob_store import (
    BlobNotFound,
    BlobStore,
    BlobTooLarge,
    InvalidBlobToken,
)


def test_blob_round_trip_uses_separate_capabilities(tmp_path):
    store = BlobStore(tmp_path, max_blob_size_bytes=1024)
    credentials = store.create(BytesIO(b"encrypted-bytes"))

    assert credentials.size == len(b"encrypted-bytes")
    assert credentials.read_token != credentials.delete_token

    handle = store.open_for_read(
        credentials.blob_id,
        credentials.read_token,
    )
    assert handle.path.read_bytes() == b"encrypted-bytes"

    with pytest.raises(InvalidBlobToken):
        store.open_for_read(credentials.blob_id, credentials.delete_token)

    with pytest.raises(InvalidBlobToken):
        store.delete(credentials.blob_id, credentials.read_token)

    assert store.delete(
        credentials.blob_id,
        credentials.delete_token,
    ) is True
    assert store.delete(
        credentials.blob_id,
        credentials.delete_token,
    ) is False

    with pytest.raises(BlobNotFound):
        store.open_for_read(
            credentials.blob_id,
            credentials.read_token,
        )


def test_blob_tokens_are_not_stored_in_plaintext(tmp_path):
    store = BlobStore(tmp_path, max_blob_size_bytes=1024)
    credentials = store.create(BytesIO(b"secret"))

    metadata = (tmp_path / f"{credentials.blob_id}.json").read_text()

    assert credentials.read_token not in metadata
    assert credentials.delete_token not in metadata
    assert "readTokenHash" in metadata
    assert "deleteTokenHash" in metadata


def test_blob_size_limit_is_enforced_while_streaming(tmp_path):
    store = BlobStore(tmp_path, max_blob_size_bytes=4)

    with pytest.raises(BlobTooLarge):
        store.create(BytesIO(b"12345"))

    assert list(tmp_path.glob("*.blob")) == []
    assert list(tmp_path.glob("*.json")) == []
