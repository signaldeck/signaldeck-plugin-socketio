from __future__ import annotations

import logging

from flask_socketio import SocketIO

from .events import register_socketio_events
from .routes import create_blueprint
from .service import service

PLUGIN_ID = "signaldeck_plugin_socketio"
PLUGIN_VERSION = "0.6.0"

socketio = SocketIO()
_logger = logging.getLogger(__name__)


def _run_socketio(app, **kwargs):
    # register_server_runner is only used by `signaldeck run` (development).
    # Production still starts through Gunicorn.
    kwargs.setdefault("allow_unsafe_werkzeug", True)
    return socketio.run(app, **kwargs)


def register_app(app, context) -> None:
    """Register Socket.IO and plugin HTTP routes on the SignalDeck Flask app."""
    extension_key = "signaldeck.socketio"
    if extension_key in app.extensions:
        return

    cfg = service.config

    socketio.init_app(
        app,
        async_mode="threading",
        cors_allowed_origins=cfg["cors_allowed_origins"],
        ping_interval=cfg["ping_interval"],
        ping_timeout=cfg["ping_timeout"],
        max_http_buffer_size=cfg["max_http_buffer_size"],
    )
    service.bind_socketio(socketio)
    register_socketio_events(socketio, service, context)

    if "signaldeck_socketio" not in app.blueprints:
        app.register_blueprint(create_blueprint(service))

    app.extensions[extension_key] = service

    server_runner = app.extensions.get("signaldeck.server_runner")
    if server_runner is not None:
        server_runner.register(PLUGIN_ID, _run_socketio)

    _logger.info(
        "SignalDeck Socket.IO plugin registered (async_mode=%s)",
        socketio.async_mode,
    )



def register(app, ctx=None) -> None:
    """Compatibility with older SignalDeck cores."""
    register_app(app, ctx)
