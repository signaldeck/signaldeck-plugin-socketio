import asyncio

import pytest

from signaldeck_plugin_socketio.commands import SocketIOSendCommand
from signaldeck_plugin_socketio.service import SocketIOService


class SocketIOStub:
    def __init__(self):
        self.calls = []

    def emit(self, event, data=None, **kwargs):
        self.calls.append((event, data, kwargs))


def test_socketio_send_command_has_useful_help_text():
    command = SocketIOSendCommand(SocketIOService())

    assert command.name == "socketio_send"
    assert "socketio_send <room_name> <message>" in command.help


def test_socketio_send_command_sends_message_to_room():
    service = SocketIOService()
    socketio = SocketIOStub()
    service.bind_socketio(socketio)
    command = SocketIOSendCommand(service)

    asyncio.run(command.run("family", "Hello", "from", "script"))

    assert len(socketio.calls) == 1
    event, payload, kwargs = socketio.calls[0]
    assert event == "message"
    assert kwargs == {"room": "family"}
    assert payload["room"] == "family"
    assert payload["content"] == "Hello from script"
    assert payload["transmitter"] == "signaldeck"
    assert payload["transmitterName"] == "SignalDeck"


def test_socketio_send_command_requires_message():
    service = SocketIOService()
    command = SocketIOSendCommand(service)

    with pytest.raises(ValueError, match="requires a message"):
        asyncio.run(command.run("family"))
