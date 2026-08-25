FROM signaldeck:dev

RUN pip install --no-cache-dir \
    "flask-socketio>=5.5,<6" \
    "simple-websocket>=1.1,<2" \
    "git+https://github.com/signaldeck/signaldeck-plugin-sqlite.git@main"
