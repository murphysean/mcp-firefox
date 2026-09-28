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

Actor model
-----------

Firefox has two overlapping actor generations:

* the **legacy** surface (``root.listTabs``, ``tabDescriptor.getTarget``,
  ``navigateTo`` on the tab descriptor) which Mozilla is retiring, and
* the **watcher/target** surface (``getWatcher``, ``watchTargets``,
  ``target-available-form``) used by its own DevTools front-end.

This client uses the watcher/target surface as the single source of truth for
per-tab actors, so there is exactly one code path for resolution, navigation and
target switching.  Two legacy calls deliberately remain, because they are still
the best available API for what they do:

* ``root.listTabs`` — the only clean tab enumeration.  Watching frame targets
  from the parent process also surfaces extension/worker frames and reports
  ``isTopLevelTarget`` as false for real tabs, so it cannot replace this.
* ``processDescriptor.getTarget`` — the unambiguous way to reach the parent
  process (chrome) console, which privileged APIs such as ``drawSnapshot`` need.

Everything else goes through :class:`TabSession`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

MAX_FRAME_BYTES = 64 * 1024 * 1024
"""Refuse absurd frame lengths so a desynchronised stream cannot exhaust memory."""

DEFAULT_TIMEOUT = 30.0
"""Default seconds to wait for a request response."""

MAX_INFLIGHT_PER_ACTOR = 64
"""Back-pressure: cap queued requests per actor so a runaway caller cannot
accumulate unbounded futures (and, because responses are matched FIFO, cannot
silently corrupt correlation)."""

LONG_STRING_CHUNK = 8192
"""Characters requested per ``substring`` call when draining a longString."""

LONG_STRING_MAX_CHARS = 4 * 1024 * 1024
"""Cap on how much of a longString we materialise into Python memory."""

TARGET_WAIT_TIMEOUT = 10.0
"""How long to wait for a tab's initial target after subscribing."""

NAVIGATE_TIMEOUT = 20.0
"""How long to wait for the replacement target after a navigation."""

# Matches a top-level `await` keyword that is not a property access such as
# `foo.await` or an identifier like `awaited`.
_AWAIT_RE = re.compile(r"(?:^|[^.\w$])await\s")

_ABOUT_BLANK = "about:blank"


class RDPError(Exception):
    """Base class for protocol-level failures."""


class RDPConnectionError(RDPError):
    """The TCP connection to Firefox is gone."""


class RDPTimeout(RDPError):
    """Firefox did not answer a request in time."""


class RDPBackpressure(RDPError):
    """Too many requests are already queued for one actor."""


class RDPServerError(RDPError):
    """Firefox answered with a protocol error packet."""

    def __init__(self, error: str, message: str = "", sender: str = "") -> None:
        self.error = error
        self.message = message
        self.sender = sender
        super().__init__(f"{error}: {message}" if message else error)


def _new_future() -> asyncio.Future[dict[str, Any]]:
    return asyncio.get_running_loop().create_future()


def _new_signal() -> asyncio.Future[None]:
    """A future used purely as a completion signal (resolved with None)."""
    return asyncio.get_running_loop().create_future()


@dataclass
class TabSession:
    """All actor state for one tab, resolved through the watcher/target protocol.

    Keyed internally by the tab *descriptor* actor, which is stable across
    navigation. The ``browsing_context_id`` is deliberately *not* the key: it
    changes when Firefox switches targets on navigation (observed 13 -> 14),
    which would orphan a bcID-keyed session and silently drop the replacement
    target announcement.
    """

    tab_actor: str
    browsing_context_id: int
    title: str | None = None
    url: str | None = None
    selected: bool = False

    watcher_actor: str | None = None
    target_actor: str | None = None
    console_actor: str | None = None
    inspector_actor: str | None = None
    network_actor: str | None = None

    #: Resolved when the tab's target is available; swapped on navigation.
    _target_ready: asyncio.Future[None] | None = field(default=None, repr=False, compare=False)

    @property
    def ready(self) -> bool:
        """True when the session can be used for page interaction."""
        return self.target_actor is not None and self.console_actor is not None

    def apply_target(self, target: dict[str, Any]) -> None:
        """Adopt a target-available-form payload."""
        self.target_actor = target.get("actor")
        self.console_actor = target.get("consoleActor")
        self.inspector_actor = target.get("inspectorActor")
        self.network_actor = target.get("networkContentActor")
        # Track the browsing context: Firefox allocates a new one on navigation.
        bc_id = target.get("browsingContextID")
        if isinstance(bc_id, int):
            self.browsing_context_id = bc_id
        self.title = target.get("title") or self.title
        self.url = target.get("url") or self.url

    def clear_target(self) -> None:
        """Forget the target after it is destroyed (e.g. by a navigation)."""
        self.target_actor = None
        self.console_actor = None
        self.inspector_actor = None
        self.network_actor = None


class FirefoxRDPClient:
    """Async client for Firefox's Remote Debugging Protocol over TCP."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6000,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        max_inflight_per_actor: int = MAX_INFLIGHT_PER_ACTOR,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_inflight_per_actor = max_inflight_per_actor

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

        # Tab sessions, keyed by the stable tab-descriptor actor.
        self._sessions: dict[str, TabSession] = {}
        self._sessions_by_bc: dict[int, TabSession] = {}
        self.active_tab_actor: str | None = None

        self.root_actor: str = "root"
        self.process_actor: str | None = None
        self.chrome_console_actor: str | None = None

    # ------------------------------------------------------------------ setup

    async def connect(self) -> dict[str, Any]:
        """Connect to Firefox and consume the greeting.

        Actor resolution is lazy: the first tool call opens a tab session.
        """
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        # Firefox pushes a greeting immediately on connect. Read it *before*
        # starting the read loop so the loop cannot swallow it.
        greeting = await self._read_frame()
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_loop(), name="firefox-rdp-reader")
        self.on_event("target-available-form", self._on_target_available)
        self.on_event("target-destroyed-form", self._on_target_destroyed)
        return greeting

    async def disconnect(self) -> None:
        """Tear down the connection, failing anything still in flight."""
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
            self._reader_task = None
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
                await self._writer.wait_closed()
        self._writer = None
        self._reader = None
        self._sessions.clear()
        self._sessions_by_bc.clear()
        self.active_tab_actor = None
        self.chrome_console_actor = None
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
        with contextlib.suppress(ValueError):
            handlers.remove(handler)
        if not handlers:
            self._event_handlers.pop(event_type, None)

    def _on_target_available(self, msg: dict[str, Any]) -> None:
        """Adopt a new target for the matching tab (includes post-navigation).

        Correlated by the event's *sender*: Firefox delivers target events from
        the tab's watcher actor, which is stable across navigation, whereas the
        target's ``browsingContextID`` changes when switching targets.
        """
        target = msg.get("target")
        if not isinstance(target, dict):
            return
        sender = msg.get("from")
        session = self._session_for_watcher(sender)
        if session is None:
            bc_id = target.get("browsingContextID")
            if isinstance(bc_id, int):
                session = self._sessions_by_bc.get(bc_id)
        if session is None:
            return
        session.apply_target(target)
        ready = session._target_ready
        if ready is not None and not ready.done():
            ready.set_result(None)

    def _on_target_destroyed(self, msg: dict[str, Any]) -> None:
        """Invalidate the target that Firefox just tore down."""
        target = msg.get("target")
        if not isinstance(target, dict):
            return
        actor = target.get("actor")
        if not isinstance(actor, str):
            return
        for session in self._sessions.values():
            if session.target_actor == actor:
                session.clear_target()

    def _session_for_watcher(self, sender: Any) -> TabSession | None:
        """Map a watcher actor id back to its tab session."""
        if not isinstance(sender, str):
            return None
        for session in self._sessions.values():
            if session.watcher_actor == sender:
                return session
        return None

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

        queue = self._pending.setdefault(to, deque())
        if len(queue) >= self.max_inflight_per_actor:
            # Back-pressure rather than unbounded growth. Also protects
            # correlation: responses are matched FIFO, so an arbitrarily deep
            # queue would make a late timeout far more likely.
            raise RDPBackpressure(
                f"{len(queue)} requests already in flight for actor '{to}' "
                f"(limit {self.max_inflight_per_actor})"
            )

        fut = _new_future()
        queue.append(fut)
        try:
            await self._send(msg)
            resp = await asyncio.wait_for(fut, timeout or self.timeout)
        except TimeoutError as exc:
            raise RDPTimeout(f"no response from actor '{to}' for '{msg.get('type')}'") from exc
        finally:
            # Runs on cancellation too, so an abandoned request cannot linger and
            # steal the next response for this actor.
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
        waiters = self._eval_waiters.setdefault(actor, deque())
        if len(waiters) >= self.max_inflight_per_actor:
            raise RDPBackpressure(
                f"{len(waiters)} evaluations already in flight for actor '{actor}' "
                f"(limit {self.max_inflight_per_actor})"
            )
        fut = _new_future()
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
        """Return Firefox's open tabs (legacy enumeration — see module docstring)."""
        resp = await self.send(self.root_actor, "listTabs")
        tabs = resp.get("tabs")
        return [t for t in tabs if isinstance(t, dict)] if isinstance(tabs, list) else []

    async def use_tab(self, index: int = 0) -> TabSession:
        """Make the tab at ``index`` active, opening a session for it if needed.

        Actors are resolved through the watcher/target protocol, not
        ``tabDescriptor.getTarget``.
        """
        tabs = await self.list_tabs()
        if not tabs:
            raise RDPError("no tabs are open")
        if not 0 <= index < len(tabs):
            raise RDPError(f"tab index {index} out of range (0-{len(tabs) - 1})")

        tab = tabs[index]
        bc_id = tab.get("browsingContextID")
        tab_actor = tab.get("actor")
        if not isinstance(bc_id, int) or not isinstance(tab_actor, str):
            raise RDPError("tab descriptor is missing browsingContextID/actor")

        session = self._sessions.get(tab_actor)
        if session is None:
            session = TabSession(tab_actor=tab_actor, browsing_context_id=bc_id)
            self._sessions[tab_actor] = session
        session.title = tab.get("title") or session.title
        session.url = tab.get("url") or session.url
        session.selected = bool(tab.get("selected"))
        self._sessions_by_bc[session.browsing_context_id] = session

        self.active_tab_actor = tab_actor
        if not session.ready:
            await self._open_session(session)
        return session

    async def _open_session(self, session: TabSession) -> None:
        """Subscribe a tab's watcher and wait for its first target."""
        ready = _new_signal()
        session._target_ready = ready
        try:
            watcher = await self.send(
                session.tab_actor,
                "getWatcher",
                # Without this Firefox emits no target events at all, and
                # navigation does not hand us a replacement target.
                isServerTargetSwitchingEnabled=True,
                isPopupDebuggingEnabled=False,
            )
            watcher_actor = watcher.get("actor")
            if not watcher_actor:
                raise RDPError("could not resolve a watcher actor for the tab")
            session.watcher_actor = str(watcher_actor)

            # The first target arrives as an event, so subscribe then wait.
            await self.send(session.watcher_actor, "watchTargets", targetType="frame")
            try:
                await asyncio.wait_for(asyncio.shield(ready), TARGET_WAIT_TIMEOUT)
            except TimeoutError as exc:
                raise RDPTimeout(
                    f"no target became available for tab {session.browsing_context_id} "
                    f"within {TARGET_WAIT_TIMEOUT}s"
                ) from exc
        finally:
            session._target_ready = None

    async def navigate(self, url: str, timeout: float = NAVIGATE_TIMEOUT) -> TabSession:
        """Navigate the active tab and wait for the replacement target.

        Navigation destroys the current target and Firefox hands us a new one for
        the same browsing context, so this waits for that handoff — otherwise the
        next call would race against a dead actor.
        """
        session = await self.active_session()
        # Capture the actor *before* subscribing to the handoff: once Firefox
        # destroys the target the field is cleared by the event handler.
        target_actor = session.target_actor
        if not target_actor:
            raise RDPError("tab has no target actor; is it still open?")

        ready = _new_signal()
        session._target_ready = ready
        try:
            await self.send(target_actor, "navigateTo", url=url)
            await asyncio.wait_for(asyncio.shield(ready), timeout)
        except TimeoutError as exc:
            raise RDPTimeout(f"no replacement target after navigating to {url}") from exc
        finally:
            session._target_ready = None
        return session

    async def active_session(self) -> TabSession:
        """Return the active tab's session, opening one if necessary."""
        if self.active_tab_actor is not None:
            session = self._sessions.get(self.active_tab_actor)
            if session is not None and session.ready:
                return session
        return await self.use_tab(0)

    async def ensure_chrome_console(self) -> str:
        """Resolve the parent-process (chrome) console actor, for privileged APIs.

        This keeps the legacy ``getTarget`` call: ``getProcess(id=0)`` is
        explicitly the parent process, so its target is unambiguous.  Watching
        process targets instead returns several candidates with no reliable way
        to tell which is the parent.
        """
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

    def forget_sessions(self) -> None:
        """Drop all cached tab sessions (they will be re-resolved on demand)."""
        self._sessions.clear()
        self._sessions_by_bc.clear()
        self.active_tab_actor = None
        self.chrome_console_actor = None

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
        for session in self._sessions.values():
            ready = session._target_ready
            if ready is not None and not ready.done():
                ready.set_exception(exc)


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
    with contextlib.suppress(ValueError):
        queue.remove(fut)


def _is_top_level_await_rejection(msg: dict[str, Any]) -> bool:
    if msg.get("topLevelAwaitRejected"):
        return True
    return msg.get("errorMessageName") == "JSMSG_AWAIT_OUTSIDE_ASYNC_OR_MODULE"


def _is_syntax_error(msg: dict[str, Any]) -> bool:
    return msg.get("errorMessageName", "").startswith("JSMSG_") and msg.get("hasException") is True
