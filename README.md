# Flask-uWSGI-WS

A high-performance WebSocket extension for Flask applications powered by uWSGI.
Compatible with Python 3.12 and newer versions.

## Example Usage

```python
from flask import Flask
from flask_uwsgi_ws import WebSocket

app = Flask(__name__)
ws = WebSocket(app)

@ws.route('/echo')
def echo(ws):
    while True:
        msg = ws.receive()
        ws.send(msg)

if __name__ == '__main__':
    app.run(debug=True, threads=16)
```

## Installation

To install Flask-uWSGI-WS, you'll need to install uWSGI with SSL support first:

### For Ubuntu/Debian:
```bash
CFLAGS="-I/usr/include/openssl" LDFLAGS="-L/usr/lib/x86_64-linux-gnu" UWSGI_PROFILE_OVERRIDE=ssl=true pip install --no-cache-dir uwsgi --no-binary :all:
```

### For macOS (Apple Silicon):
```bash
CFLAGS="-I/opt/homebrew/opt/openssl@3/include" \
LDFLAGS="-L/opt/homebrew/opt/openssl@3/lib" \
UWSGI_PROFILE_OVERRIDE=ssl=true pip install --no-cache-dir uwsgi --no-binary :all:
```

Then install Flask-uWSGI-WS:
```bash
pip install flask-uwsgi-ws
```

## Deployment

You can use uWSGI's built-in HTTP router to get up and running quickly:

```bash
$ uwsgi --master --http :8080 --http-websockets --wsgi-file app.py
```

...or call app.run, passing uwsgi any arguments you like:

```python
app.run(debug=True, host='localhost', port=8080, master=true, processes=8)
```

### Using with Gevent

uWSGI supports several concurrency models, including Gevent. To use Gevent, import `GeventWebSocket`:

```python
from flask_uwsgi_ws import GeventWebSocket
```

Then run uWSGI with the gevent loop engine:

```bash
$ uwsgi --master --http :8080 --http-websockets --gevent 100 --wsgi-file app.py
```

...or in your code:

```python
app.run(debug=True, gevent=100)
```

## Development

To use Flask's interactive debugger, install werkzeug's DebuggedApplication middleware:

```python
from werkzeug.debug import DebuggedApplication
app.wsgi_app = DebuggedApplication(app.wsgi_app, True)
```

Then run uWSGI with a single worker:

```bash
$ uwsgi --master --http :8080 --http-websockets --wsgi-file --workers 1 --threads 8 app.py
```

If you use `app.run(debug=True)`, Flask-uWSGI-WS will do this automatically.

## WebSocket API

Flask-uWSGI-WS handles the WebSocket handshake and provides a websocket client with the following methods:

- `websocket.recv()` - Receive a message
- `websocket.send(msg)` - Send a message
- `websocket.send_binary(msg)` - Send a binary message
- `websocket.recv_nb()` - Non-blocking receive
- `websocket.send_from_sharedarea(id, pos)` - Send from shared memory area
- `websocket.send_binary_from_sharedarea(id, pos)` - Send binary from shared memory area