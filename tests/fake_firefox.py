"""Fake Firefox RDP server used by the test suite.

Implements just enough of the protocol to exercise the client: the greeting,
length-prefixed framing, response/event discrimination (responses have no
`type` key), interleaved events, and error packets.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any


class FakeRDPFirefox:
    """A minimal in-process stand-in for Firefox's remote debugging server."""

    def __init__(self) -> None:
        self.server: asyncio.Server | None = None
        self.port: int = 0
        self._client_writer: asyncio.StreamWriter | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._client_writers: list[asyncio.StreamWriter] = []
        # handler: (msg, responder) -> None
        self.handlers: dict[str, Callable[[dict[str, Any], Responder], Any]] = {}
        self.received: list[dict[str, Any]] = []
        self.tabs: list[dict[str, Any]] = [
            {
                "actor": "server1.tabDescriptor1",
                "browsingContextID": 10,
                "selected": True,
                "title": "Tab One",
                "url": "https://example.com/",
            }
        ]
        self.on_connect: Callable[[Responder], Any] | None = None
        self.greeting: dict[str, Any] = {
            "from": "root",
            "applicationType": "browser",
            "testConnectionPrefix": "server1.conn0.",
            "traits": {},
        }
        # When set, the server crashes the handler for this message type.
        self.raise_on: set[str] = set()
        # Mirrors Firefox: the watcher actor dies once resources are unwatched.
        self.watcher_alive: bool = True

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        server, self.server = self.server, None
        if server is not None:
            server.close()
        # Never await wait_closed() here: on some CPython versions it waits for
        # live connection handlers, which would deadlock teardown.
        for writer in self._client_writers:
            try:
                writer.close()
            except Exception:
                pass
        for task in list(self._handler_tasks):
            task.cancel()
        current = asyncio.current_task()
        pending = [t for t in self._handler_tasks if t is not current]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._handler_tasks.clear()
        self._client_writers.clear()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._handler_tasks.add(asyncio.current_task())
        self._client_writer = writer
        self._client_writers.append(writer)
        try:
            await self._write(writer, self.greeting)
            if self.on_connect is not None:
                await self.on_connect(Responder(self, writer, "root"))
            while True:
                msg = await self._read(reader)
                if msg is None:
                    break
                self.received.append(msg)
                await self._process(dict(msg), Responder(self, writer, str(msg.get("to", ""))))
        except (asyncio.IncompleteReadError, ConnectionResetError, asyncio.CancelledError):
            pass
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass
            task = asyncio.current_task()
            if task is not None:
                self._handler_tasks.discard(task)

    async def _process(self, msg: dict[str, Any], responder: Responder) -> None:
        msg_type = str(msg.get("type", ""))
        if msg_type in self.raise_on:
            raise RuntimeError(f"fake server blew up handling {msg_type}")
        handler = self.handlers.get(msg_type)
        if handler is None:
            await responder.error(
                "unrecognizedPacketType", f"Actor {responder.to} does not recognize {msg_type}"
            )
            return
        result = handler(msg, responder)
        if asyncio.iscoroutine(result):
            await result

    # -------------------------------------------------------------- framing

    async def _write(self, writer: asyncio.StreamWriter, msg: dict[str, Any]) -> None:
        data = json.dumps(msg)
        writer.write(f"{len(data)}:{data}".encode())
        await writer.drain()

    async def _read(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
        try:
            prefix = await reader.readuntil(b":")
        except asyncio.IncompleteReadError:
            return None
        n = int(prefix[:-1])
        try:
            data = await reader.readexactly(n)
        except asyncio.IncompleteReadError:
            return None
        parsed = json.loads(data)
        return parsed if isinstance(parsed, dict) else None


class Responder:
    """Server-side helper for answering one request."""

    def __init__(self, server: FakeRDPFirefox, writer: asyncio.StreamWriter, to: str) -> None:
        self.server = server
        self.writer = writer
        self.to = to

    async def reply(self, **fields: Any) -> None:
        """Send a normal response packet (deliberately without a `type` key)."""
        await self.server._write(self.writer, {"from": self.to, **fields})

    async def event(self, msg_type: str, **fields: Any) -> None:
        """Send an unsolicited event packet."""
        await self.server._write(self.writer, {"from": self.to, "type": msg_type, **fields})

    async def error(self, error: str, message: str = "") -> None:
        await self.server._write(self.writer, {"from": self.to, "error": error, "message": message})


def default_handlers(server: FakeRDPFirefox) -> None:
    """Install the handlers that mirror a real Firefox instance."""

    async def list_tabs(msg: dict[str, Any], r: Responder) -> None:
        await r.reply(tabs=server.tabs)

    async def get_target(msg: dict[str, Any], r: Responder) -> None:
        # Real Firefox answers getTarget differently for a tab vs the parent
        # process: a `frame` for the former, a `process` for the latter.
        if r.to == "server1.processDescriptor3":
            await r.reply(
                process={
                    "actor": "server1.processTarget1",
                    "consoleActor": "server1.chromeConsole1",
                }
            )
            return
        await r.reply(
            frame={
                "actor": "server1.child2/windowGlobalTarget2",
                "consoleActor": "server1.child2/consoleActor3",
                "inspectorActor": "server1.child2/inspectorActor4",
                "networkContentActor": "server1.child2/networkContentActor14",
                "url": "https://example.com/",
            }
        )

    async def evaluate_js_async(msg: dict[str, Any], r: Responder) -> None:
        text = str(msg.get("text", ""))
        rid = "rid-1"
        await r.reply(resultID=rid)
        # A real instance interleaves unrelated events before the result.
        await r.event("frameUpdate", frames=[{"id": 7, "isTopLevel": True}])
        if "await " in text and not text.strip().startswith(("(async", "async")):
            await r.event(
                "evaluationResult",
                resultID=rid,
                result={"type": "undefined"},
                hasException=True,
                errorMessageName="JSMSG_AWAIT_OUTSIDE_ASYNC_OR_MODULE",
                topLevelAwaitRejected=True,
            )
            return
        value = evaluate_probe(text)
        if isinstance(value, dict) and value.get("__long__"):
            await r.event(
                "evaluationResult",
                resultID=rid,
                result={
                    "type": "longString",
                    "actor": "server1.longstr1",
                    "length": value["length"],
                },
                hasException=False,
            )
            return
        if isinstance(value, dict) and value.get("__throw__"):
            await r.event(
                "evaluationResult",
                resultID=rid,
                result={"type": "undefined"},
                hasException=True,
                exceptionMessage=value["__throw__"],
            )
            return
        await r.event("evaluationResult", resultID=rid, result=value, hasException=False)

    async def substring(msg: dict[str, Any], r: Responder) -> None:
        start = int(msg.get("start", 0))
        end = int(msg.get("end", 0))
        await r.reply(substring=("x" * 100_000)[start:end])

    async def get_watcher(msg: dict[str, Any], r: Responder) -> None:
        server.watcher_alive = True
        await r.reply(actor="server1.watcher3", traits={})

    async def get_process(msg: dict[str, Any], r: Responder) -> None:
        await r.reply(processDescriptor={"actor": "server1.processDescriptor3"})

    async def watch_targets(msg: dict[str, Any], r: Responder) -> None:
        await r.reply()

    async def watch_resources(msg: dict[str, Any], r: Responder) -> None:
        await r.reply()

    async def unwatch_resources(msg: dict[str, Any], r: Responder) -> None:
        # Faithful to Firefox: unwatching resources tears down the watcher actor,
        # after which it stops answering anything else.
        server.watcher_alive = False
        await r.reply()

    async def unwatch_targets(msg: dict[str, Any], r: Responder) -> None:
        if not server.watcher_alive:
            return  # actor is gone: no reply, exactly like the real thing
        await r.reply()

    server.handlers.update(
        {
            "listTabs": list_tabs,
            "getTarget": get_target,
            "evaluateJSAsync": evaluate_js_async,
            "substring": substring,
            "getWatcher": get_watcher,
            "getProcess": get_process,
            "watchTargets": watch_targets,
            "watchResources": watch_resources,
            "unwatchResources": unwatch_resources,
            "unwatchTargets": unwatch_targets,
        }
    )


def evaluate_probe(text: str) -> Any:
    """Map a small set of probe expressions onto predictable values."""
    if "LONG" in text:
        return {"__long__": True, "length": 50_000}
    if "THROW" in text:
        return {"__throw__": "boom"}
    if "1+1" in text:
        return 2
    return "ok"
