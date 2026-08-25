from __future__ import annotations

from signaldeck_sdk import DisplayData


class SocketIODisplayData(DisplayData):
    def for_overview(self, processor):
        self.processor = processor
        self.mode = "overview"
        self.connected_clients = processor.connected_clients
        self.users = processor.users
        self.rooms = processor.room_overview
        return self

    def for_room(self, processor, room_name: str):
        self.processor = processor
        self.mode = "room"
        self.connected_clients = processor.connected_clients
        self.room = processor.get_room_overview(room_name)
        self.users = processor.users
        return self

    def buttons(self) -> dict:
        buttons = {
            "refresh": {
                "name": "refresh",
                "text": self.ctx.t("signaldeck_plugin_socketio.refresh"),
                "params": {
                    "socketio_action": "refresh"
                }
            }
        }

        if self.mode == "overview":
            buttons["send"] = {
                "name": "send",
                "text": self.ctx.t("signaldeck_plugin_socketio.send"),
                "params": {
                    "socketio_action": "send",
                    "room": "@socketio_room",
                    "message": "@socketio_message"
                }
            }
        elif self.mode == "room":
            buttons["send"] = {
                "name": "send",
                "text": self.ctx.t("signaldeck_plugin_socketio.send"),
                "params": {
                    "socketio_action": "send",
                    "room": self.room["name"],
                    "message": "@socketio_message"
                }
            }

        return buttons
