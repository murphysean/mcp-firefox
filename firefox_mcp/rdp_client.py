"""Firefox Remote Debugging Protocol TCP client with background message pump."""

import asyncio
import json
from typing import Any, Callable


class FirefoxRDPClient:
    """Async client for Firefox's Remote Debugging Protocol over TCP."""

    def __init__(self, host: str = "localhost", port: int = 6000):
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()
        self._reader_task: asyncio.Task | None = None
        # Pending requests: maps actor -> Future for next response from that actor
        self._pending: dict[str, asyncio.Future] = {}
        # Event handlers: maps event type -> callback
        self._event_handlers: dict[str, Callable[[dict], None]] = {}
        # Fallback for unsolicited messages
        self._unsolicited: list[dict] = []
        # Discovered actors
        self.root_actor: str = "root"
        self.tab_actor: str | None = None
        self.target_actor: str | None = None
        self.console_actor: str | None = None
        self.inspector_actor: str | None = None
        self.network_actor: str | None = None

    async def connect(self):
        """Connect to Firefox and discover actors."""
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        # Start background reader
        self._reader_task = asyncio.create_task(self._read_loop())
        # Firefox sends a greeting on connect - wait for it
        greeting = await self._recv_one()
        # Discover tabs and get target actors
        tabs_resp = await self.send(self.root_actor, "listTabs")
        if tabs := tabs_resp.get("tabs"):
            tab = tabs[0]
            self.tab_actor = tab.get("actor")
            target_resp = await self.send(self.tab_actor, "getTarget")
            if frame := target_resp.get("frame"):
                self.target_actor = frame.get("actor")
                self.console_actor = frame.get("consoleActor")
                self.inspector_actor = frame.get("inspectorActor")
                self.network_actor = frame.get("networkContentActor")
        return greeting

    async def disconnect(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

    def on_event(self, event_type: str, handler: Callable[[dict], None]):
        """Register a handler for unsolicited event messages by type."""
        self._event_handlers[event_type] = handler

    def remove_event(self, event_type: str):
        """Remove an event handler."""
        self._event_handlers.pop(event_type, None)

    async def send(self, to: str, msg_type: str, **params) -> dict[str, Any]:
        """Send a message to an actor and return the response."""
        async with self._lock:
            fut = asyncio.get_event_loop().create_future()
            self._pending[to] = fut
            msg = {"to": to, "type": msg_type, **params}
            await self._send(msg)
        return await fut

    async def send_raw(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a raw JSON payload and return the response."""
        to = payload.get("to", "")
        async with self._lock:
            fut = asyncio.get_event_loop().create_future()
            self._pending[to] = fut
            await self._send(payload)
        return await fut

    async def send_and_collect(self, to: str, msg_type: str, **params) -> dict[str, Any]:
        """Send a message, return the first response from that actor.
        Used internally when the lock is already held or for special flows."""
        async with self._lock:
            fut = asyncio.get_event_loop().create_future()
            self._pending[to] = fut
            msg = {"to": to, "type": msg_type, **params}
            await self._send(msg)
        return await fut

    async def _send(self, msg: dict):
        data = json.dumps(msg)
        frame = f"{len(data)}:{data}"
        self._writer.write(frame.encode())
        await self._writer.drain()

    async def _recv_one(self) -> dict[str, Any]:
        """Read a single frame from the connection (used before reader loop starts)."""
        return await self._read_frame()

    async def _read_frame(self) -> dict[str, Any]:
        """Read one length-prefixed JSON frame."""
        length_bytes = b""
        while True:
            ch = await self._reader.read(1)
            if not ch:
                raise ConnectionError("Connection closed")
            if ch == b":":
                break
            length_bytes += ch
        length = int(length_bytes)
        data = await self._reader.readexactly(length)
        return json.loads(data)

    async def _read_loop(self):
        """Background task that reads messages and dispatches them."""
        try:
            while True:
                msg = await self._read_frame()
                sender = msg.get("from", "")
                msg_type = msg.get("type", "")

                # Check if there's a pending request for this actor
                if sender in self._pending:
                    fut = self._pending.pop(sender)
                    if not fut.done():
                        fut.set_result(msg)
                        continue

                # Check event handlers by message type
                if msg_type in self._event_handlers:
                    self._event_handlers[msg_type](msg)
                    continue

                # Fallback: stash unsolicited
                self._unsolicited.append(msg)
                # Cap unsolicited buffer
                if len(self._unsolicited) > 1000:
                    self._unsolicited = self._unsolicited[-500:]
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            pass

    # For backward compat with evaluate_js which needs raw send+recv under lock
    async def _send_locked(self, msg: dict):
        """Send a message while already holding the lock."""
        await self._send(msg)

    async def send_and_wait_type(self, to: str, msg_type: str, wait_type: str, **params) -> dict[str, Any]:
        """Send a message and wait for a response with a specific 'type' field value."""
        async with self._lock:
            fut = asyncio.get_event_loop().create_future()
            # Register with a special key so the read loop can match by type
            key = f"__wait_type__{wait_type}"
            self._pending[key] = fut
            # Also register for the 'from' actor in case we get a direct response
            self._pending[to] = asyncio.get_event_loop().create_future()
            msg = {"to": to, "type": msg_type, **params}
            await self._send(msg)
        # We need to intercept by type - patch the read loop dispatch
        # Actually, let's use a simpler approach: register a temporary event handler
        self._pending.pop(to, None)  # remove the from-based one
        self._pending.pop(key, None)
        # Use a future + event handler pattern
        result_fut = asyncio.get_event_loop().create_future()

        def _handler(m):
            if not result_fut.done():
                result_fut.set_result(m)
                self.remove_event(wait_type)

        self.on_event(wait_type, _handler)
        # Re-send under lock
        async with self._lock:
            msg = {"to": to, "type": msg_type, **params}
            await self._send(msg)
        return await result_fut

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()
