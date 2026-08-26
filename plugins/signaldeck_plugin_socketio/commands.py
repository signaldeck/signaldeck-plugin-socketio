from signaldeck_sdk import Command

from .service import SocketIOService, service


class SocketIOSendCommand(Command):
    def __init__(self, socketio_service: SocketIOService = service):
        super().__init__(
            "socketio_send",
            "Send a server-originated Socket.IO message to a room. Usage: socketio_send <room_name> <message>",
        )
        self.service = socketio_service

    async def run(self, room_name, *message_parts, cmdRes=None, stopEvent=None):
        room = str(room_name).strip()
        if not room:
            raise ValueError("socketio_send requires a non-empty room name")

        message = " ".join(str(part) for part in message_parts).strip()
        if not message:
            raise ValueError("socketio_send requires a message")

        self.service.send_server_message(message, room=room)

        if cmdRes is not None:
            cmdRes.appendState(
                self,
                msg=f"Sent Socket.IO message to room '{room}'",
            )
