"""Fake Firefox RDP server used by the test suite.

Implements just enough of the protocol to exercise the client: the greeting,
length-prefixed framing, response/event discrimination (responses have no
`type` key), interleaved events, error packets, and — importantly — the
watcher/target protocol with the real lifecycle constraints:

* `getWatcher` without ``isServerTargetSwitchingEnabled=True`` yields **no**
  target events at all;
* the tab target is announced asynchronously as ``target-available-form`` after
  ``watchTargets(FRAME)``;
* navigation destroys the target and announces a **new** target actor for the
  same browsing context;
* ``unwatchResources`` tears the watcher down, after which it stops answering.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from collections.abc import Callable
from typing import Any

TAB_BC_ID = 10


class FakeRDPFirefox:
    """A minimal in-process stand-in for Firefox's remote debugging server."""

    def __init__(self) -> None:
        self.server: asyncio.Server | None = None
        self.port: int = 0
        self._client_writer: asyncio.StreamWriter | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._client_writers: list[asyncio.StreamWriter] = []

        self.handlers: dict[str, Callable[[dict[str, Any], Responder], Any]] = {}
        self.received: list[dict[str, Any]] = []

        self.tabs: list[dict[str, Any]] = [
            {
                "actor": "server1.tabDescriptor1",
                "browsingContextID": TAB_BC_ID,
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

        self.raise_on: set[str] = set()

        # Watcher/target state, mirroring Firefox's real behaviour.
        self.watcher_seq = itertools.count(1)
        self.target_seq = itertools.count(1)
        self.actor_seq = itertools.count(1)
        self.watcher_alive = True
        self.switching_enabled = False
        self.watching_frames = False
        self.targets: dict[str, dict[str, Any]] = {}
        self.current_target: dict[str, Any] | None = None
        self.current_watcher_actor: str | None = None
        # watcher actor -> owning tab info, so target announcements match the tab.
        self.sessions: dict[str, dict[str, Any]] = {}
        # Firefox allocates a fresh browsingContextID on navigation.
        self.next_browsing_context_id = TAB_BC_ID + 1

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
        root = Responder(self, writer, "root")
        try:
            await self._write(writer, self.greeting)
            if self.on_connect is not None:
                await self.on_connect(root)
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
                "unrecognizedPacketType",
                f"Actor {responder.to} does not recognize '{msg_type}'",
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

    # ------------------------------------------------- watcher/target helpers

    def make_target(
        self,
        *,
        url: str | None = None,
        title: str = "Tab One",
        browsing_context_id: int = TAB_BC_ID,
    ) -> dict[str, Any]:
        """Mint a target actor the way Firefox does."""
        n = next(self.target_seq)
        suffix = next(self.actor_seq)
        prefix = "server1.watcher2.process5//"
        return {
            "actor": f"{prefix}windowGlobalTarget{suffix}",
            "targetType": "frame",
            "browsingContextID": browsing_context_id,
            "innerWindowId": 10_000 + n,
            "isTopLevelTarget": True,
            "isPopup": False,
            "isPrivate": False,
            "title": title,
            "url": url or "https://example.com/",
            "consoleActor": f"{prefix}consoleActor{suffix}",
            "inspectorActor": f"{prefix}inspectorActor{suffix}",
            "networkContentActor": f"{prefix}networkContentActor{suffix}",
            "screenshotContentActor": f"{prefix}screenshotContentActor{suffix}",
        }

    async def announce_target(
        self, writer: asyncio.StreamWriter, target: dict[str, Any], watcher_actor: str
    ) -> None:
        """Push a target-available-form for a newly adopted target.

        The sender must be the tab's watcher actor: that is how a client
        correlates target events to a tab, since the target's
        browsingContextID changes on navigation.
        """
        self.targets[target["actor"]] = target
        self.current_target = target
        self.current_watcher_actor = watcher_actor
        await self._write(
            writer,
            {"from": watcher_actor, "type": "target-available-form", "target": target},
        )

    async def destroy_current_target(self, writer: asyncio.StreamWriter) -> None:
        """Push target-destroyed-form for the active target, as navigation does."""
        target = self.current_target
        if target is None:
            return
        sender = self.current_watcher_actor or "server1.watcher"
        self.targets.pop(target["actor"], None)
        self.current_target = None
        await self._write(
            writer,
            {
                "from": sender,
                "type": "target-destroyed-form",
                "target": {
                    "actor": target["actor"],
                    "innerWindowId": target["innerWindowId"],
                    "isTopLevelTarget": True,
                },
                "options": {"isTargetSwitching": True},
            },
        )


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
    """Install handlers mirroring a real Firefox instance."""

    async def list_tabs(msg: dict[str, Any], r: Responder) -> None:
        await r.reply(tabs=server.tabs)

    async def get_watcher(msg: dict[str, Any], r: Responder) -> None:
        # Real Firefox emits no target events unless switching is enabled.
        server.switching_enabled = bool(msg.get("isServerTargetSwitchingEnabled"))
        server.watcher_alive = True
        watcher = f"server1.watcher{next(server.watcher_seq)}"
        # Remember which tab this watcher belongs to, so we can announce the
        # matching target.
        tab = next(
            (t for t in server.tabs if t.get("actor") == r.to),
            None,
        )
        if tab is not None:
            server.sessions[watcher] = {
                "browsing_context_id": tab.get("browsingContextID"),
                "url": tab.get("url"),
                "title": tab.get("title", "Tab One"),
            }
        await r.reply(actor=watcher, traits={})

    async def watch_targets(msg: dict[str, Any], r: Responder) -> None:
        if msg.get("targetType") != "frame":
            await r.reply()
            return
        server.watching_frames = True
        await r.reply()
        if not server.switching_enabled:
            return
        # Firefox announces the target of the tab whose watcher we asked; the
        # watcher actor is derived from that tab descriptor.
        session = server.sessions.get(r.to)
        browsing_context_id = session["browsing_context_id"] if session else TAB_BC_ID
        await server.announce_target(
            r.writer,
            server.make_target(
                browsing_context_id=browsing_context_id,
                url=(session or {}).get("url"),
                title=(session or {}).get("title", "Tab One"),
            ),
            r.to,
        )

    async def unwatch_targets(msg: dict[str, Any], r: Responder) -> None:
        if not server.watcher_alive:
            return  # actor is gone: no reply, exactly like the real thing
        server.watching_frames = False
        await r.reply()

    async def unwatch_resources(msg: dict[str, Any], r: Responder) -> None:
        # Faithful to Firefox: unwatching resources tears down the watcher actor,
        # after which it stops answering anything else.
        server.watcher_alive = False
        await r.reply()

    async def watch_resources(msg: dict[str, Any], r: Responder) -> None:
        await r.reply()

    async def navigate_to(msg: dict[str, Any], r: Responder) -> None:
        target = server.current_target
        # navigateTo is addressed to the target actor, but target events come
        # from the watcher actor, so capture the watcher before replying.
        watcher_actor = server.current_watcher_actor or "server1.watcher"
        # Firefox tears the current target down and announces a replacement with
        # a NEW browsingContextID for the same tab.
        if target is not None:
            await server.destroy_current_target(r.writer)
        await r.reply()
        server.tabs[0]["browsingContextID"] = server.next_browsing_context_id
        server.next_browsing_context_id += 1
        await server.announce_target(
            r.writer,
            server.make_target(
                url=str(msg.get("url")),
                title="Navigated",
                browsing_context_id=server.tabs[0]["browsingContextID"],
            ),
            watcher_actor,
        )
        if server.tabs:
            server.tabs[0]["url"] = str(msg.get("url", ""))
            server.tabs[0]["title"] = "Navigated"

    async def get_process(msg: dict[str, Any], r: Responder) -> None:
        await r.reply(
            processDescriptor={
                "actor": "server1.processDescriptor3",
                "id": msg.get("id", 0),
                "isParent": True,
            }
        )

    async def get_target(msg: dict[str, Any], r: Responder) -> None:
        # Real Firefox answers differently for a tab vs the parent process: a
        # `frame` for the former, a `process` for the latter.
        if r.to == "server1.processDescriptor3":
            await r.reply(
                process={
                    "actor": "server1.parentProcessTarget1",
                    "consoleActor": "server1.chromeConsole1",
                }
            )
            return
        target = server.current_target or server.make_target()
        await r.reply(
            frame={
                "actor": target["actor"],
                "consoleActor": target["consoleActor"],
                "inspectorActor": target["inspectorActor"],
                "networkContentActor": target["networkContentActor"],
                "url": target["url"],
            }
        )

    async def evaluate_js_async(msg: dict[str, Any], r: Responder) -> None:
        text = str(msg.get("text", ""))
        # Only known console actors answer.
        if "consoleActor" not in r.to and r.to != "server1.chromeConsole1":
            await r.error("noSuchActor", f"No such actor for ID: {r.to}")
            return
        rid = f"rid-{next(server.actor_seq)}"
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

    async def never_answers(msg: dict[str, Any], r: Responder) -> None:
        return

    server.handlers.update(
        {
            "listTabs": list_tabs,
            "getWatcher": get_watcher,
            "watchTargets": watch_targets,
            "unwatchTargets": unwatch_targets,
            "watchResources": watch_resources,
            "unwatchResources": unwatch_resources,
            "navigateTo": navigate_to,
            "getProcess": get_process,
            "getTarget": get_target,
            "evaluateJSAsync": evaluate_js_async,
            "substring": substring,
            "neverAnswers": never_answers,
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
