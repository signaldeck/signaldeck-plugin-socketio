from __future__ import annotations

from flask import Blueprint, jsonify, request

from .service import SocketIOService


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

    return bp
