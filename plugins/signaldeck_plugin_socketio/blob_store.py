from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4


_METADATA_VERSION = 1
_CHUNK_SIZE = 64 * 1024


class BlobStoreError(Exception):
    pass


class BlobNotFound(BlobStoreError):
    pass


class InvalidBlobToken(BlobStoreError):
    pass


class BlobTooLarge(BlobStoreError):
    pass


@dataclass(frozen=True)
class BlobCredentials:
    blob_id: str
    read_token: str
    delete_token: str
    size: int


@dataclass(frozen=True)
class BlobReadHandle:
    path: Path
    size: int


class BlobStore:
    """Filesystem-backed storage for opaque binary blobs.

    The store deliberately has no concept of event types, MIME types, rooms or
    expiry. Read and delete access are capability based. The capability tokens
    are returned once on creation; only SHA-256 hashes are stored on disk.
    """

    def __init__(self, root: str | Path, max_blob_size_bytes: int) -> None:
        self.root = Path(root)
        self.max_blob_size_bytes = int(max_blob_size_bytes)
        if self.max_blob_size_bytes <= 0:
            raise ValueError("max_blob_size_bytes must be greater than zero")

    def create(
        self,
        stream: BinaryIO,
        *,
        content_length: int | None = None,
    ) -> BlobCredentials:
        if (
            content_length is not None
            and content_length > self.max_blob_size_bytes
        ):
            raise BlobTooLarge(
                f"blob exceeds maximum size of {self.max_blob_size_bytes} bytes"
            )

        self.root.mkdir(parents=True, exist_ok=True)

        blob_id = uuid4().hex
        read_token = secrets.token_urlsafe(32)
        delete_token = secrets.token_urlsafe(32)

        data_path = self._data_path(blob_id)
        metadata_path = self._metadata_path(blob_id)
        temp_data_path = self.root / f".{blob_id}.{uuid4().hex}.blob.tmp"
        temp_metadata_path = self.root / f".{blob_id}.{uuid4().hex}.json.tmp"

        size = 0
        try:
            with temp_data_path.open("xb") as target:
                while True:
                    chunk = stream.read(_CHUNK_SIZE)
                    if not chunk:
                        break

                    size += len(chunk)
                    if size > self.max_blob_size_bytes:
                        raise BlobTooLarge(
                            "blob exceeds maximum size of "
                            f"{self.max_blob_size_bytes} bytes"
                        )
                    target.write(chunk)

                target.flush()
                os.fsync(target.fileno())

            metadata = {
                "version": _METADATA_VERSION,
                "blobId": blob_id,
                "size": size,
                "createdAt": int(time.time() * 1000),
                "readTokenHash": self._token_hash(read_token),
                "deleteTokenHash": self._token_hash(delete_token),
            }

            with temp_metadata_path.open("x", encoding="utf-8") as target:
                json.dump(metadata, target, separators=(",", ":"))
                target.flush()
                os.fsync(target.fileno())

            os.replace(temp_data_path, data_path)
            os.replace(temp_metadata_path, metadata_path)

            self._restrict_permissions(data_path)
            self._restrict_permissions(metadata_path)
        except Exception:
            temp_data_path.unlink(missing_ok=True)
            temp_metadata_path.unlink(missing_ok=True)
            data_path.unlink(missing_ok=True)
            metadata_path.unlink(missing_ok=True)
            raise

        return BlobCredentials(
            blob_id=blob_id,
            read_token=read_token,
            delete_token=delete_token,
            size=size,
        )

    def open_for_read(self, blob_id: str, read_token: str) -> BlobReadHandle:
        metadata = self._load_metadata(blob_id)
        if not self._token_matches(read_token, metadata["readTokenHash"]):
            raise InvalidBlobToken("invalid read token")

        path = self._data_path(blob_id)
        if not path.is_file():
            raise BlobNotFound(blob_id)

        return BlobReadHandle(path=path, size=int(metadata["size"]))

    def delete(self, blob_id: str, delete_token: str) -> bool:
        """Delete a blob.

        Returns ``False`` when the blob is already absent. This makes DELETE
        idempotent at the HTTP layer without retaining tombstones.
        """
        try:
            metadata = self._load_metadata(blob_id)
        except BlobNotFound:
            return False

        if not self._token_matches(delete_token, metadata["deleteTokenHash"]):
            raise InvalidBlobToken("invalid delete token")

        self._data_path(blob_id).unlink(missing_ok=True)
        self._metadata_path(blob_id).unlink(missing_ok=True)
        return True

    def _load_metadata(self, blob_id: str) -> dict:
        self._validate_blob_id(blob_id)
        path = self._metadata_path(blob_id)
        try:
            with path.open("r", encoding="utf-8") as source:
                metadata = json.load(source)
        except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
            raise BlobNotFound(blob_id) from error

        if (
            metadata.get("version") != _METADATA_VERSION
            or metadata.get("blobId") != blob_id
            or not isinstance(metadata.get("size"), int)
            or not isinstance(metadata.get("readTokenHash"), str)
            or not isinstance(metadata.get("deleteTokenHash"), str)
        ):
            raise BlobNotFound(blob_id)

        return metadata

    def _data_path(self, blob_id: str) -> Path:
        self._validate_blob_id(blob_id)
        return self.root / f"{blob_id}.blob"

    def _metadata_path(self, blob_id: str) -> Path:
        self._validate_blob_id(blob_id)
        return self.root / f"{blob_id}.json"

    @staticmethod
    def _validate_blob_id(blob_id: str) -> None:
        if len(blob_id) != 32 or any(
            character not in "0123456789abcdef"
            for character in blob_id
        ):
            raise BlobNotFound(blob_id)

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @classmethod
    def _token_matches(cls, token: str, expected_hash: str) -> bool:
        return hmac.compare_digest(cls._token_hash(token), expected_hash)

    @staticmethod
    def _restrict_permissions(path: Path) -> None:
        try:
            path.chmod(0o600)
        except OSError:
            # Best effort only. Filesystem/application sandbox permissions still
            # apply on platforms that do not support POSIX chmod semantics.
            pass
