from __future__ import annotations

from flask import Blueprint, jsonify, request, send_file

from .blob_store import BlobNotFound, BlobTooLarge, InvalidBlobToken
from .service import SocketIOService


def _bearer_token() -> str | None:
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def create_blueprint(service: SocketIOService) -> Blueprint:
    bp = Blueprint(
        "signaldeck_socketio",
        __name__,
        template_folder="templates",
        url_prefix="/plugin/socketio",
    )

    @bp.get("/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "connected_clients": service.registry.connected_count(),
                "rooms": service.registry.active_rooms(),
            }
        )

    @bp.get("/clients/<room>")
    def clients_for_room(room: str):
        return jsonify(
            {
                "room": room,
                "clients": service.registry.clients_for_room(room),
            }
        )

    @bp.post("/send")
    def send_message():
        data = request.get_json(silent=True) or {}
        content = data.get("message", data.get("content"))
        if content is None:
            return jsonify({"status": "error", "error": "message_required"}), 400

        room = data.get("room")
        message = service.send_server_message(
            content,
            room=str(room) if room else None,
            content_type=str(data.get("contentType") or "text"),
        )

        return jsonify(
            {
                "status": "sent",
                "room": room,
                "message": message,
            }
        )

    @bp.post("/blobs")
    def create_blob():
        try:
            credentials = service.blob_store.create(
                request.stream,
                content_length=request.content_length,
            )
        except BlobTooLarge:
            return jsonify({"error": "blob_too_large"}), 413

        response = jsonify(
            {
                "blobId": credentials.blob_id,
                "readToken": credentials.read_token,
                "deleteToken": credentials.delete_token,
                "size": credentials.size,
            }
        )
        response.status_code = 201
        response.headers["Cache-Control"] = "no-store"
        return response

    @bp.get("/blobs/<blob_id>")
    def read_blob(blob_id: str):
        token = _bearer_token()
        if token is None:
            return jsonify({"error": "read_token_required"}), 401

        try:
            handle = service.blob_store.open_for_read(blob_id, token)
        except InvalidBlobToken:
            return jsonify({"error": "invalid_read_token"}), 403
        except BlobNotFound:
            return jsonify({"error": "blob_not_found"}), 404

        response = send_file(
            handle.path,
            mimetype="application/octet-stream",
            as_attachment=False,
            conditional=True,
            etag=True,
            max_age=0,
        )
        response.headers["Cache-Control"] = "private, no-store"
        # Do not overwrite Content-Length here. send_file sets the correct
        # length for both full responses and conditional/range responses.
        return response

    @bp.delete("/blobs/<blob_id>")
    def delete_blob(blob_id: str):
        token = _bearer_token()
        if token is None:
            return jsonify({"error": "delete_token_required"}), 401

        try:
            deleted = service.blob_store.delete(blob_id, token)
        except InvalidBlobToken:
            return jsonify({"error": "invalid_delete_token"}), 403

        # DELETE is intentionally idempotent. If a previous client already
        # removed the blob, the desired state is still achieved.
        return jsonify({"deleted": deleted}), 200

    return bp
