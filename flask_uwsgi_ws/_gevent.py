import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar

from gevent import spawn, wait
from gevent.event import Event
from gevent.monkey import patch_all
from gevent.queue import Queue, Empty
from gevent.select import select

from werkzeug.exceptions import HTTPException
from werkzeug.routing import MapAdapter

from .websocket import WebSocket, WebSocketMiddleware
from ._uwsgi import uwsgi


# Type alias for message data
WSMessage = Union[str, bytes, None]
T = TypeVar('T')


@dataclass
class GeventWebSocketClient:
    """WebSocket client implementation using gevent for asynchronous I/O."""
    environ: Dict[str, Any]
    fd: int
    send_event: Event
    send_queue: Queue
    recv_event: Event
    recv_queue: Queue
    timeout: int = 5
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    connected: bool = True

    def send(self, msg: WSMessage, binary: bool = True) -> None:
        """Send a message to the client."""
        if binary:
            return self.send_binary(msg)
        self.send_queue.put(msg)
        self.send_event.set()

    def send_binary(self, msg: WSMessage) -> None:
        """Send a binary message to the client."""
        self.send_queue.put(msg)
        self.send_event.set()

    def receive(self) -> WSMessage:
        """Receive a message from the client (alias for recv)."""
        return self.recv()

    def recv(self) -> WSMessage:
        """Receive a message from the client."""
        return self.recv_queue.get()

    def close(self) -> None:
        """Close the connection."""
        self.connected = False


class GeventWebSocketMiddleware(WebSocketMiddleware):
    """WebSocket middleware implementation using gevent."""
    client = GeventWebSocketClient

    def __call__(self, environ: Dict[str, Any], start_response: Callable) -> List[bytes]:
        """Handle the WSGI request."""
        urls: MapAdapter = self.websocket.url_map.bind_to_environ(environ)
        try:
            endpoint, args = urls.match()
            handler = self.websocket.view_functions[endpoint]
        except HTTPException:
            handler = None

        # If not a WebSocket request or no handler, pass to WSGI app
        if not handler or 'HTTP_SEC_WEBSOCKET_KEY' not in environ:
            return self.wsgi_app(environ, start_response)

        # Perform WebSocket handshake
        uwsgi.websocket_handshake(
            environ['HTTP_SEC_WEBSOCKET_KEY'],
            environ.get('HTTP_ORIGIN', '')
        )

        # Setup communication channels
        send_event, send_queue = Event(), Queue()
        recv_event, recv_queue = Event(), Queue()

        # Create WebSocket client instance
        client = self.client(
            environ=environ, 
            fd=uwsgi.connection_fd(), 
            send_event=send_event,
            send_queue=send_queue, 
            recv_event=recv_event, 
            recv_queue=recv_queue,
            timeout=self.websocket.timeout
        )

        # Spawn handler to process client events
        handler = spawn(handler, client, **args)

        # Define listener function that waits for socket events
        def listener(client: GeventWebSocketClient) -> None:
            select([client.fd], [], [], client.timeout)
            recv_event.set()
            
        # Spawn initial listener
        listening = spawn(listener, client)

        # Main event loop
        while True:
            # Check if client disconnected
            if not client.connected:
                recv_queue.put(None)
                listening.kill()
                handler.join(client.timeout)
                return []

            # Wait for any events (handler, send, receive)
            wait([handler, send_event, recv_event], None, 1)

            # Process send events
            if send_event.is_set():
                try:
                    while True:
                        uwsgi.websocket_send(send_queue.get_nowait())
                except Empty:
                    send_event.clear()
                except IOError:
                    client.connected = False

            # Process receive events
            elif recv_event.is_set():
                recv_event.clear()
                try:
                    # Read all available messages until an empty message
                    message: WSMessage = True
                    while message:
                        message = uwsgi.websocket_recv_nb()
                        recv_queue.put(message)
                    listening = spawn(listener, client)
                except IOError:
                    client.connected = False

            # Handle completion of handler
            elif handler.ready():
                listening.kill()
                return []


class GeventWebSocket(WebSocket):
    """WebSocket implementation using gevent."""
    middleware = GeventWebSocketMiddleware

    def init_app(self, app) -> None:
        """Initialize the Flask application with gevent patches."""
        aggressive = app.config.get('UWSGI_WEBSOCKET_AGGRESSIVE_PATCH', True)
        patch_all(aggressive=aggressive)
        super().init_app(app)
