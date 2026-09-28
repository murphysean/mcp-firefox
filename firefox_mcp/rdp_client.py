"""Firefox Remote Debugging Protocol (RDP) TCP client with a background message pump.

Firefox's RDP speaks length-prefixed JSON over TCP: each message is
``<byte-length>:<json>``.  A background reader task owns the socket and
dispatches every inbound packet:

* a packet **without** a ``type`` field is a *response* (or an error) to a
  request, and resolves the oldest pending request for the ``from`` actor;
* a packet **with** a ``type`` field is an unsolicited *event*.

That distinction is the only reliable correlation mechanism: Firefox does not
echo a ``requestId`` back to this protocol version, and actors emit both
responses and events on the same connection.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import deque
from collections.abc import Callable
from typing import Any

MAX_FRAME_BYTES = 64 * 1024 * 1024
"""Refuse absurd frame lengths so a desynchronised stream cannot exhaust memory."""

DEFAULT_TIMEOUT = 30.0
"""Default seconds to wait for a request response."""

LONG_STRING_CHUNK = 8192
"""Characters requested per ``substring`` call when draining a longString."""

LONG_STRING_MAX_CHARS = 4 * 1024 * 1024
"""Cap on how much of a longString we materialise into Python memory."""

# Matches a top-level `await` keyword that is not a property access such as
# `foo.await` or an identifier like `awaited`.
_AWAIT_RE = re.compile(r"(?:^|[^.\w$])await\s")


class RDPError(Exception):
    """Base class for protocol-level failures."""


class RDPConnectionError(RDPError):
    """The TCP connection to Firefox is gone."""


class RDPTimeout(RDPError):
    """Firefox did not answer a request in time."""


class RDPServerError(RDPError):
    """Firefox answered with a protocol error packet."""

    def __init__(self, error: str, message: str = "", sender: str = "") -> None:
        self.error = error
        self.message = message
        self.sender = sender
        super().__init__(f"{error}: {message}" if message else error)


def _new_future() -> asyncio.Future[dict[str, Any]]:
    return asyncio.get_running_loop().create_future()


class FirefoxRDPClient:
    """Async client for Firefox's Remote Debugging Protocol over TCP."""

    def __init__(
        self, host: str = "localhost", port: int = 6000, *, timeout: float = DEFAULT_TIMEOUT
    ):
        self.host = host
        self.port = port
        self.timeout = timeout

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._closed = True

        # actor -> FIFO of futures awaiting a *response* (packets with no `type`).
        self._pending: dict[str, deque[asyncio.Future[dict[str, Any]]]] = {}
        # actor -> FIFO of futures awaiting an `evaluationResult` event. Ordered,
        # so the Nth result from an actor satisfies the Nth outstanding evaluate().
        self._eval_waiters: dict[str, deque[asyncio.Future[dict[str, Any]]]] = {}
        # event type -> handlers
        self._event_handlers: dict[str, list[Callable[[dict[str, Any]], None]]] = {}
        # Packets that matched neither a pending request nor a handler.
        self._unmatched: deque[dict[str, Any]] = deque(maxlen=200)

        # Discovered actors.
        self.root_actor: str = "root"
        self.tab_actor: str | None = None
        self.target_actor: str | None = None
        self.console_actor: str | None = None
        self.inspector_actor: str | None = None
        self.network_actor: str | None = None
        self.process_actor: str | None = None
        self.chrome_console_actor: str | None = None

    # ------------------------------------------------------------------ setup

    async def connect(self) -> dict[str, Any]:
        """Connect to Firefox, consume the greeting, and resolve actors."""
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        # Firefox pushes a greeting immediately on connect. Read it *before*
        # starting the read loop so the loop cannot swallow it.
        greeting = await self._read_frame()
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_loop(), name="firefox-rdp-reader")
        await self.resolve_actors()
        return greeting

    async def disconnect(self) -> None:
        """Tear down the connection, failing anything still in flight."""
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._writer = None
        self._reader = None
        self._fail_in_flight(RDPConnectionError("disconnected"))

    @property
    def connected(self) -> bool:
        if self._reader_task is None or self._reader_task.done():
            return False
        if self._closed or self._writer is None:
            return False
        return not self._writer.is_closing()

    # ------------------------------------------------------------------ events

    def on_event(self, event_type: str, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a handler for unsolicited events of ``event_type``."""
        handlers = self._event_handlers.setdefault(event_type, [])
        if handler not in handlers:
            handlers.append(handler)

    def remove_event(
        self, event_type: str, handler: Callable[[dict[str, Any]], None] | None = None
    ) -> None:
        """Remove one handler, or every handler for ``event_type`` when omitted."""
        if handler is None:
            self._event_handlers.pop(event_type, None)
            return
        handlers = self._event_handlers.get(event_type)
        if not handlers:
            return
        if handler in handlers:
            handlers.remove(handler)
        if not handlers:
            self._event_handlers.pop(event_type, None)

    # ---------------------------------------------------------------- requests

    async def send(
        self, to: str, msg_type: str, *, timeout: float | None = None, **params: Any
    ) -> dict[str, Any]:
        """Send a request to an actor and await its response packet.

        ``timeout`` overrides the client default for this call (useful for
        best-effort cleanup messages that Firefox may never answer).
        """
        msg: dict[str, Any] = {"to": to, "type": msg_type}
        msg.update(params)
        return await self._request(msg, timeout)

    async def send_raw(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send an arbitrary payload and await the response from its ``to`` actor."""
        return await self._request(dict(payload))

    async def _request(self, msg: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        to = msg.get("to")
        if not isinstance(to, str):
            raise RDPError("message is missing a 'to' actor")
        if not self.connected:
            raise RDPConnectionError("not connected to Firefox")

        fut = _new_future()
        queue = self._pending.setdefault(to, deque())
        queue.append(fut)
        try:
            await self._send(msg)
            resp = await asyncio.wait_for(fut, timeout or self.timeout)
        except TimeoutError as exc:
            raise RDPTimeout(f"no response from actor '{to}' for '{msg.get('type')}'") from exc
        finally:
            _discard(queue, fut)
            if not queue:
                self._pending.pop(to, None)

        if "error" in resp:
            raise RDPServerError(
                str(resp.get("error", "unknown")),
                str(resp.get("message", "")),
                str(resp.get("from", "")),
            )
        return resp

    # -------------------------------------------------------------- evaluation

    async def evaluate(
        self,
        actor: str,
        expression: str,
        *,
        await_promise: bool = True,
        timeout: float | None = None,
        max_chars: int = LONG_STRING_MAX_CHARS,
    ) -> dict[str, Any]:
        """Evaluate ``expression`` in an actor's console.

        Returns the raw ``evaluationResult`` packet (inspect ``hasException``).
        Promises are awaited when ``await_promise`` is set, and a literal
        top-level ``await`` in the source is retried inside an async IIFE
        because Firefox's console rejects top-level await in plain scripts.
        """
        result = await self._evaluate_once(actor, expression, await_promise, timeout)
        if _is_top_level_await_rejection(result) and _AWAIT_RE.search(expression):
            for candidate in (
                f"(async () => ({expression}))()",  # expression body: keeps the value
                f"(async () => {{ {expression} }})()",  # statement body: for multi-statement source
            ):
                retry = await self._evaluate_once(actor, candidate, await_promise, timeout)
                if not _is_syntax_error(retry):
                    result = retry
                    break
        result["result"] = await self._resolve_result(result.get("result"), max_chars, timeout)
        return result

    async def _evaluate_once(
        self, actor: str, expression: str, await_promise: bool, timeout: float | None
    ) -> dict[str, Any]:
        fut = _new_future()
        waiters = self._eval_waiters.setdefault(actor, deque())
        waiters.append(fut)  # registered before send so no result can be missed
        try:
            ack = await self.send(
                actor, "evaluateJSAsync", text=expression, mapped={"await": await_promise}
            )
            if "error" in ack:
                raise RDPServerError(str(ack["error"]), str(ack.get("message", "")), actor)
            result = await asyncio.wait_for(fut, timeout or self.timeout)
        except TimeoutError as exc:
            raise RDPTimeout(f"evaluation timed out in actor '{actor}'") from exc
        finally:
            _discard(waiters, fut)
            if not waiters:
                self._eval_waiters.pop(actor, None)
        return result

    async def _resolve_result(self, result: Any, max_chars: int, timeout: float | None) -> Any:
        """Materialise a longString result into an actual string."""
        if isinstance(result, dict) and result.get("type") == "longString":
            return await self.read_long_string(result, max_chars=max_chars, timeout=timeout)
        return result

    async def read_long_string(
        self,
        descriptor: dict[str, Any],
        *,
        max_chars: int = LONG_STRING_MAX_CHARS,
        timeout: float | None = None,
    ) -> str:
        """Drain a Firefox ``longString`` actor into a Python string."""
        actor = descriptor.get("actor")
        if not isinstance(actor, str):
            return ""
        try:
            total = int(descriptor.get("length", 0))
        except (TypeError, ValueError):
            total = 0
        limit = min(total, max_chars)
        chunks: list[str] = []
        pos = 0
        while pos < limit:
            end = min(pos + LONG_STRING_CHUNK, limit)
            resp = await self._request(
                {"to": actor, "type": "substring", "start": pos, "end": end}, timeout
            )
            chunk = resp.get("substring")
            if not isinstance(chunk, str) or not chunk:
                break
            chunks.append(chunk)
            pos = end
        text = "".join(chunks)
        if total > limit:
            text += f"\n... [truncated at {limit} of {total} characters]"
        return text

    # ------------------------------------------------------------------- tabs

    async def list_tabs(self) -> list[dict[str, Any]]:
        """Return Firefox's open tabs."""
        resp = await self.send(self.root_actor, "listTabs")
        tabs = resp.get("tabs")
        return tabs if isinstance(tabs, list) else []

    async def resolve_actors(self, tab_actor: str | None = None) -> dict[str, Any]:
        """Resolve the actor ids for a tab (defaults to the active tab)."""
        if tab_actor is not None:
            self.tab_actor = tab_actor
        if self.tab_actor is None:
            tabs = await self.list_tabs()
            if not tabs:
                self._clear_actors()
                return {}
            self.tab_actor = str(tabs[0]["actor"])

        target = await self.send(self.tab_actor, "getTarget")
        frame = target.get("frame")
        if not isinstance(frame, dict):
            self._clear_actors()
            return {}
        self.target_actor = frame.get("actor")
        self.console_actor = frame.get("consoleActor")
        self.inspector_actor = frame.get("inspectorActor")
        self.network_actor = frame.get("networkContentActor")
        return frame

    async def select_tab(self, index: int = 0) -> dict[str, Any]:
        """Switch the client to the tab at ``index`` and resolve its actors."""
        tabs = await self.list_tabs()
        if not tabs:
            raise RDPError("no tabs are open")
        if not 0 <= index < len(tabs):
            raise RDPError(f"tab index {index} out of range (0-{len(tabs) - 1})")
        self.tab_actor = str(tabs[index]["actor"])
        self.chrome_console_actor = None
        frame = await self.resolve_actors()
        return {"index": index, "tab": tabs[index], "frame": frame}

    async def ensure_chrome_console(self) -> str:
        """Resolve the parent-process (chrome) console actor, for privileged APIs."""
        if self.chrome_console_actor is not None:
            return self.chrome_console_actor
        if self.process_actor is None:
            resp = await self.send(self.root_actor, "getProcess", id=0)
            descriptor = resp.get("processDescriptor")
            if not isinstance(descriptor, dict) or not descriptor.get("actor"):
                raise RDPError("could not resolve the parent process actor")
            self.process_actor = str(descriptor["actor"])
        target = await self.send(self.process_actor, "getTarget")
        process = target.get("process")
        console = process.get("consoleActor") if isinstance(process, dict) else None
        if not console:
            raise RDPError(
                "could not resolve the chrome console actor "
                "(set devtools.chrome.enabled=true in about:config)"
            )
        self.chrome_console_actor = str(console)
        return self.chrome_console_actor

    def invalidate_target(self) -> None:
        """Forget tab-scoped actors after a navigation invalidates them."""
        self._clear_actors()
        self.chrome_console_actor = None

    def _clear_actors(self) -> None:
        self.target_actor = None
        self.console_actor = None
        self.inspector_actor = None
        self.network_actor = None

    # ------------------------------------------------------------- transport

    async def _send(self, msg: dict[str, Any]) -> None:
        if self._writer is None:
            raise RDPConnectionError("not connected to Firefox")
        data = json.dumps(msg)
        frame = f"{len(data)}:{data}".encode()
        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()

    async def _read_frame(self) -> dict[str, Any]:
        """Read one length-prefixed JSON frame."""
        reader = self._reader
        if reader is None:
            raise RDPConnectionError("not connected to Firefox")
        try:
            prefix = await reader.readuntil(b":")
        except asyncio.IncompleteReadError as exc:
            raise RDPConnectionError("connection closed by Firefox") from exc
        except asyncio.LimitOverrunError as exc:
            raise RDPConnectionError("frame length prefix exceeds buffer limit") from exc
        try:
            length = int(prefix[:-1])
        except ValueError as exc:
            raise RDPConnectionError(f"malformed frame length prefix: {prefix!r}") from exc
        if length < 0 or length > MAX_FRAME_BYTES:
            raise RDPConnectionError(f"refusing frame of {length} bytes")
        try:
            data = await reader.readexactly(length)
        except asyncio.IncompleteReadError as exc:
            raise RDPConnectionError("connection closed mid-frame") from exc
        parsed = json.loads(data)
        if not isinstance(parsed, dict):
            raise RDPConnectionError(f"expected a JSON object, got {type(parsed).__name__}")
        return parsed

    async def _read_loop(self) -> None:
        """Own the socket: read frames, dispatch them, and fail gracefully."""
        try:
            while True:
                self._dispatch(await self._read_frame())
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # connection loss or protocol desync
            self._closed = True
            self._fail_in_flight(RDPConnectionError(f"RDP reader stopped: {exc}"))

    def _dispatch(self, msg: dict[str, Any]) -> None:
        """Route one inbound packet to a pending request or an event handler."""
        sender = msg.get("from")
        sender = sender if isinstance(sender, str) else ""
        msg_type = msg.get("type")

        if msg_type is None:
            # A response (or an error packet): satisfies the oldest request for
            # this actor. Firefox does not echo requestId, so FIFO ordering is
            # the correlation we have.
            if _resolve_oldest(self._pending.get(sender), msg):
                return
            self._unmatched.append(msg)
            return

        if msg_type == "evaluationResult" and _resolve_oldest(self._eval_waiters.get(sender), msg):
            return

        handlers = self._event_handlers.get(str(msg_type))
        if handlers:
            for handler in list(handlers):
                try:
                    handler(msg)
                except Exception:
                    # A misbehaving handler must never kill the reader task.
                    pass
            return

        self._unmatched.append(msg)

    def _fail_in_flight(self, exc: Exception) -> None:
        """Fail every queued future so callers surface the error instead of hanging."""
        for queues in (self._pending, self._eval_waiters):
            for queue in queues.values():
                while queue:
                    fut = queue.popleft()
                    if not fut.done():
                        fut.set_exception(exc)
        self._pending.clear()
        self._eval_waiters.clear()


def _resolve_oldest(
    queue: deque[asyncio.Future[dict[str, Any]]] | None, msg: dict[str, Any]
) -> bool:
    """Resolve the oldest live future in ``queue``; return False if there is none."""
    if not queue:
        return False
    while queue:
        fut = queue.popleft()
        if not fut.done():
            fut.set_result(msg)
            return True
    return False


def _discard(
    queue: deque[asyncio.Future[dict[str, Any]]], fut: asyncio.Future[dict[str, Any]]
) -> None:
    try:
        queue.remove(fut)
    except ValueError:
        pass


def _is_top_level_await_rejection(msg: dict[str, Any]) -> bool:
    if msg.get("topLevelAwaitRejected"):
        return True
    return msg.get("errorMessageName") == "JSMSG_AWAIT_OUTSIDE_ASYNC_OR_MODULE"


def _is_syntax_error(msg: dict[str, Any]) -> bool:
    return msg.get("errorMessageName", "").startswith("JSMSG_") and msg.get("hasException") is True
