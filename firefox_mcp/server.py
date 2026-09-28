"""Firefox DevTools MCP server — exposes Firefox's Remote Debugging Protocol as MCP tools."""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass, field
from typing import Any

from mcp.server.mcpserver import MCPServer

from .rdp_client import (
    FirefoxRDPClient,
    RDPError,
    RDPTimeout,
    TabSession,
)

mcp = MCPServer("firefox-devtools")

DEFAULT_RDP_HOST = os.environ.get("FIREFOX_RDP_HOST", "localhost")
DEFAULT_RDP_PORT = int(os.environ.get("FIREFOX_RDP_PORT", "6000"))
DEFAULT_MCP_HOST = os.environ.get("FIREFOX_MCP_HOST", "127.0.0.1")
DEFAULT_MCP_PORT = int(os.environ.get("FIREFOX_MCP_PORT", "8090"))

MAX_BODY_CHARS = 100_000
"""Per-response cap when fetching network response bodies."""

_client: FirefoxRDPClient | None = None


# --------------------------------------------------------------------- helpers


def _json(payload: Any) -> str:
    return json.dumps(payload, default=str, indent=2)


def _error(exc: Exception) -> str:
    return _json({"error": type(exc).__name__, "message": str(exc)})


async def get_client() -> FirefoxRDPClient:
    """Return a live client, reconnecting when the connection has died."""
    global _client
    if _client is None or not _client.connected:
        _client = FirefoxRDPClient(DEFAULT_RDP_HOST, DEFAULT_RDP_PORT)
        await _client.connect()
    return _client


async def _reset_client() -> None:
    """Drop the cached client so the next call reconnects from scratch."""
    global _client
    if _client is not None:
        await _client.disconnect()
    _client = None


async def _session(tab_index: int | None = None) -> TabSession:
    """Resolve the tab session a tool should operate on."""
    client = await get_client()
    if tab_index is not None:
        return await client.use_tab(tab_index)
    return await client.active_session()


def _require_console(session: TabSession) -> str:
    if not session.console_actor:
        raise RDPError("no console actor for the current tab; is a page loaded?")
    return session.console_actor


# ---------------------------------------------------------------- page tools


@mcp.tool()
async def evaluate_js(expression: str, tab_index: int | None = None) -> str:
    """Evaluate a JavaScript expression in the current tab's console.

    Returns a JSON object with `result`, `hasException` and, when the expression
    threw, `exceptionMessage`. Promises are awaited automatically, and a literal
    top-level `await` is re-wrapped in an async function on your behalf — so
    `await fetch(...).then(r => r.json())` works even though Firefox's console
    rejects a bare top-level await.

    Note: a non-primitive result comes back as an RDP object *grip* (a reference
    plus a small preview), not the full value. Use `JSON.stringify(...)` for a
    plain string, or navigate the grip with `raw_rdp_command`.
    """
    try:
        session = await _session(tab_index)
        client = await get_client()
        return _json(await client.evaluate(_require_console(session), expression))
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def list_tabs() -> str:
    """List all open browser tabs with their index, title, URL and selected state.

    Use the returned `index` with other tools' `tab_index` argument to target a
    specific tab. Indices shift as tabs open and close, so re-resolve before
    relying on one.
    """
    try:
        client = await get_client()
        tabs = await client.list_tabs()
        active = client.active_tab_actor
        return _json(
            [
                {
                    "index": i,
                    "title": t.get("title"),
                    "url": t.get("url"),
                    "selected": t.get("selected", False),
                    "is_active_target": t.get("actor") == active,
                }
                for i, t in enumerate(tabs)
            ]
        )
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def select_tab(index: int = 0) -> str:
    """Target a specific tab for subsequent tool calls.

    Args:
        index: Zero-based tab index as reported by `list_tabs`.
    """
    try:
        client = await get_client()
        session = await client.use_tab(index)
        return _json(
            {
                "status": "selected",
                "index": index,
                "title": session.title,
                "url": session.url,
                "browsing_context_id": session.browsing_context_id,
            }
        )
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def navigate(url: str, tab_index: int | None = None) -> str:
    """Navigate the current tab to a URL.

    Firefox destroys the tab's debugging target on navigation and issues a
    replacement, so this waits for the handoff before returning. That means the
    very next tool call sees the new page rather than a dead actor.
    """
    try:
        client = await get_client()
        if tab_index is not None:
            await client.use_tab(tab_index)
        session = await client.navigate(url)
        return _json({"status": "navigated", "url": url, "title": session.title})
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def get_page_source(tab_index: int | None = None) -> str:
    """Return the current page's full HTML source."""
    try:
        client = await get_client()
        session = await _session(tab_index)
        resp = await client.evaluate(
            _require_console(session), "document.documentElement.outerHTML"
        )
        if resp.get("hasException"):
            return _json({"error": "EvaluationError", "message": resp.get("exceptionMessage")})
        result = resp.get("result")
        return result if isinstance(result, str) else _json(result)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def get_console_messages(tab_index: int | None = None) -> str:
    """Get cached console messages (page errors and console API calls) for the current tab."""
    try:
        client = await get_client()
        session = await _session(tab_index)
        resp = await client.send(
            _require_console(session),
            "getCachedMessages",
            messageTypes=["PageError", "ConsoleAPI"],
        )
        return _json(resp.get("messages", []))
    except Exception as exc:
        return _error(exc)


_READ_PAGE_JS = """(() => {
    const drop = [
        'script', 'style', 'noscript', 'iframe', 'svg',
        'nav', 'header', 'footer', 'aside', 'form',
        '[role="navigation"]', '[role="banner"]', '[role="complementary"]',
        '[aria-hidden="true"]',
        '.ad', '.ads', '.advert', '.advertisement', '.sidebar', '.menu',
        '.nav', '.breadcrumb', '.pagination', '.share', '.social',
        '.cookie', '.consent', '.newsletter', '.subscribe', '.related',
        '.comments', '#comments'
    ];
    const clone = document.cloneNode(true);
    for (const sel of drop) {
        for (const el of clone.querySelectorAll(sel)) el.remove();
    }
    const main =
        clone.querySelector(
            'main, article, [role="main"], .post-content, .article-body, .entry-content, #content'
        )
        || clone.body
        || clone.documentElement;
    if (!main) return '';
    return (main.innerText || main.textContent || '')
        .replace(/[ \\t]+\\n/g, '\\n')
        .replace(/\\n{3,}/g, '\\n\\n')
        .trim();
})()"""


@mcp.tool()
async def read_page(tab_index: int | None = None) -> str:
    """Extract the readable text content of the current page.

    Strips scripts, navigation, ads and boilerplate, then returns the main
    article/content text. Works well on news sites, blogs and documentation.
    Falls back to the whole body when no main-content element is detected.
    """
    try:
        client = await get_client()
        session = await _session(tab_index)
        resp = await client.evaluate(_require_console(session), _READ_PAGE_JS)
        if resp.get("hasException"):
            return _json({"error": "EvaluationError", "message": resp.get("exceptionMessage")})
        result = resp.get("result")
        return result if isinstance(result, str) else _json(result)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def raw_rdp_command(payload: str) -> str:
    """Send a raw JSON payload to Firefox RDP and return the response.

    The payload must be a JSON string with at least `to` and `type` fields.
    Use this to experiment with RDP commands not covered by the other tools.

    Example: {"to": "root", "type": "listTabs"}
    """
    try:
        client = await get_client()
        try:
            msg = json.loads(payload)
        except json.JSONDecodeError as exc:
            return _json({"error": "InvalidJSON", "message": str(exc)})
        if not isinstance(msg, dict):
            return _json({"error": "InvalidPayload", "message": "payload must be a JSON object"})
        return _json(await client.send_raw(msg))
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------- screenshot

_SNAPSHOT_JS = """(async () => {{
    const bc = BrowsingContext.get({browsing_context_id});
    if (!bc) throw new Error('browsing context {browsing_context_id} is gone');
    const snapshot = await bc.currentWindowGlobal.drawSnapshot({rect}, 1.0, 'white'{options});
    const canvas = new OffscreenCanvas(snapshot.width, snapshot.height);
    const ctx = canvas.getContext('2d');
    ctx.drawImage(snapshot, 0, 0);
    const w = snapshot.width, h = snapshot.height;
    snapshot.close();
    const blob = await canvas.convertToBlob({{type: '{image_type}'}});
    const buf = await blob.arrayBuffer();
    const b64 = ChromeUtils.base64URLEncode(new Uint8Array(buf), {{pad: true}})
        .replace(/-/g, '+').replace(/_/g, '/');
    return JSON.stringify({{data_url: 'data:{image_type};base64,' + b64, width: w, height: h}});
}})()"""


async def _measure_selector(client: FirefoxRDPClient, session: TabSession, selector: str) -> str:
    """Return a JS DOMRect literal for ``selector``, or raise if unmeasurable."""
    console = _require_console(session)
    rect_js = (
        "(() => { const el = document.querySelector("
        + json.dumps(selector)
        + "); if (!el) return null; const r = el.getBoundingClientRect();"
        + " return JSON.stringify({x: r.x, y: r.y, width: r.width, height: r.height}); })()"
    )
    resp = await client.evaluate(console, rect_js)
    rect_raw = resp.get("result")
    if not rect_raw or rect_raw == "null":
        raise RDPError(f"no element matches {selector!r}")
    try:
        rect = json.loads(rect_raw)
    except (TypeError, ValueError) as exc:
        raise RDPError(f"could not measure {selector!r}") from exc
    if not rect.get("width") or not rect.get("height"):
        raise RDPError(f"{selector!r} has zero size and cannot be captured")
    return "new DOMRect({x}, {y}, {width}, {height})".format(**rect)


async def _measure_fullpage(client: FirefoxRDPClient, session: TabSession) -> str:
    """Return a JS DOMRect literal covering the whole scrollable page."""
    resp = await client.evaluate(
        _require_console(session),
        "JSON.stringify({"
        "w: Math.max(document.documentElement.scrollWidth,"
        " document.documentElement.clientWidth),"
        "h: Math.max(document.documentElement.scrollHeight,"
        " document.documentElement.clientHeight)"
        "})",
    )
    try:
        size = json.loads(resp.get("result") or "{}")
        if not isinstance(size, dict):
            size = {}
    except (TypeError, ValueError):
        size = {}
    if not size.get("w") or not size.get("h"):
        raise RDPError("could not measure the page")
    return f"new DOMRect(0, 0, {size['w']}, {size['h']})"


@mcp.tool()
async def screenshot(
    selector: str = "",
    fullpage: bool = False,
    tab_index: int | None = None,
    save_path: str = "",
    image_type: str = "png",
) -> str:
    """Take a screenshot of the current tab.

    Capture happens in Firefox's parent process, which rasterises the tab's
    window global — so this captures real rendered pixels rather than a DOM
    reconstruction. Requires `devtools.chrome.enabled=true` in about:config.

    Args:
        selector: CSS selector to capture a single element. Empty = whole viewport.
        fullpage: Capture the entire scrollable page instead of just the viewport.
        tab_index: Tab to capture; defaults to whichever tab is currently targeted.
        save_path: Optional file path; when set the image is written there and the
            base64 payload is omitted from the response (keeps the result small).
        image_type: `png` (default) or `jpeg`.

    Returns JSON with `data_url` (a `data:image/...;base64,...` string), plus
    `width`/`height`. When `save_path` is given, returns `path` and `bytes`
    instead of the (potentially multi-megabyte) data URL.
    """
    try:
        client = await get_client()
        session = await _session(tab_index)

        if selector:
            rect = await _measure_selector(client, session, selector)
            options = ""
        elif fullpage:
            rect = await _measure_fullpage(client, session)
            options = ", {inScrollView: false}"
        else:
            rect = "null"
            options = ""

        # drawSnapshot lives in the parent process, so this must run against the
        # chrome console, not the content console.
        chrome_console = await client.ensure_chrome_console()
        js = _SNAPSHOT_JS.format(
            browsing_context_id=session.browsing_context_id,
            rect=rect,
            options=options,
            image_type=image_type,
        )
        resp = await client.evaluate(chrome_console, js)
        if resp.get("hasException"):
            return _json(
                {
                    "error": "ScreenshotFailed",
                    "message": resp.get("exceptionMessage") or str(resp.get("exception")),
                }
            )
        raw = resp.get("result")
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = {"data_url": raw}
        elif isinstance(raw, dict):
            payload = raw
        else:
            return _json({"error": "ScreenshotFailed", "message": "unexpected empty result"})

        if not payload.get("data_url"):
            return _json({"error": "ScreenshotFailed", "message": "no image data returned"})

        if save_path:
            return _json(await _write_image(payload, save_path, image_type))
        return _json(payload)
    except RDPError as exc:
        return _json({"error": type(exc).__name__, "message": str(exc)})
    except Exception as exc:
        return _error(exc)


async def _write_image(payload: dict[str, Any], save_path: str, image_type: str) -> dict[str, Any]:
    """Decode a data URL to disk and describe the result (no base64 in reply)."""
    data_url = str(payload.get("data_url", ""))
    header, _, b64 = data_url.partition(",")
    try:
        blob = base64.b64decode(b64)
    except (binascii.Error, ValueError) as exc:
        return {"error": "ScreenshotFailed", "message": f"bad base64: {exc}"}
    try:
        with open(save_path, "wb") as fh:
            fh.write(blob)
    except OSError as exc:
        return {"error": "WriteFailed", "message": str(exc)}
    kind = header.removeprefix("data:").split(";")[0] if header else image_type
    return {
        "path": save_path,
        "bytes": len(blob),
        "width": payload.get("width"),
        "height": payload.get("height"),
        "content_type": kind,
    }


# ----------------------------------------------------------- network capture


@dataclass
class _Capture:
    """Accumulates network events for one capture session."""

    watcher_actor: str
    events: dict[Any, dict[str, Any]] = field(default_factory=dict)
    order: list[Any] = field(default_factory=list)

    def add(self, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            if entry.get("resourceType") != "network-event":
                continue
            rid = entry.get("resourceId")
            if rid is None:
                continue
            if rid not in self.events:
                self.events[rid] = dict(entry)
                self.order.append(rid)
            else:
                # Keep the richest copy of the fields we already know.
                for key, value in entry.items():
                    self.events[rid].setdefault(key, value)

    def update(self, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            rid = entry.get("resourceId")
            if rid is None or rid not in self.events:
                continue
            updates = entry.get("resourceUpdates")
            if isinstance(updates, dict):
                self.events[rid].update(updates)

    def summary(self) -> list[dict[str, Any]]:
        out = []
        for rid in self.order:
            ev = self.events[rid]
            cause = ev.get("cause")
            out.append(
                {
                    "url": ev.get("url"),
                    "method": ev.get("method"),
                    "status": ev.get("status"),
                    "statusText": ev.get("statusText"),
                    "contentType": ev.get("mimeType"),
                    "contentSize": ev.get("contentSize"),
                    "transferSize": ev.get("transferredSize"),
                    "started": ev.get("startedDateTime"),
                    "isXHR": ev.get("isXHR"),
                    "cause": cause.get("type") if isinstance(cause, dict) else None,
                    "resourceId": rid,
                    "actor": ev.get("actor"),
                }
            )
        return out


_capture: _Capture | None = None


def _capture_entries(msg: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten RDP's `[[resourceType, [entries...]], ...]` batch shape.

    Firefox nests each resource batch as a two-element list rather than a flat
    list of entries.
    """
    out: list[dict[str, Any]] = []
    for batch in msg.get("array") or []:
        if isinstance(batch, dict):
            out.append(batch)
        elif isinstance(batch, (list, tuple)):
            for item in batch:
                if isinstance(item, dict):
                    out.append(item)
                elif isinstance(item, (list, tuple)):
                    out.extend(x for x in item if isinstance(x, dict))
    return out


@mcp.tool()
async def start_capture(tab_index: int | None = None) -> str:
    """Start capturing network requests on the current tab.

    Subscribes to Firefox's watcher so requests and responses are accumulated
    until `read_capture` or `stop_capture` is called. Start it before triggering
    the network activity you care about (before navigating, for full coverage).
    """
    global _capture
    try:
        client = await get_client()
        session = await _session(tab_index)
        if _capture is not None:
            return _json({"status": "already_capturing", "events_so_far": len(_capture.order)})
        if not session.watcher_actor:
            return _json({"error": "NoWatcher", "message": "tab has no watcher actor"})

        capture = _Capture(watcher_actor=session.watcher_actor)

        def _on_available(msg: dict[str, Any]) -> None:
            capture.add(_capture_entries(msg))

        def _on_updated(msg: dict[str, Any]) -> None:
            capture.update(_capture_entries(msg))

        client.on_event("resources-available-array", _on_available)
        client.on_event("resources-updated-array", _on_updated)

        await client.send(session.watcher_actor, "watchResources", resourceTypes=["network-event"])

        _capture = capture
        return _json({"status": "capturing", "watcher": session.watcher_actor})
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def read_capture(include_bodies: bool = False, only_errors: bool = False) -> str:
    """Read captured network events.

    Args:
        include_bodies: Also fetch response bodies (slower; capped per body).
        only_errors: Only return responses that failed or returned status >= 400.

    Returns a summary per request: URL, method, status, content type, sizes and
    the resource id.
    """
    try:
        client = await get_client()
        if _capture is None:
            return _json(
                {
                    "error": "NoCapture",
                    "message": "no capture in progress; call start_capture first",
                }
            )

        entries = _capture.summary()
        if only_errors:
            entries = [
                e
                for e in entries
                if (isinstance(e.get("status"), int) and e["status"] >= 400)
                or e.get("status") is None
            ]

        if include_bodies:
            for entry in entries:
                await _attach_body(client, entry)

        return _json({"count": len(entries), "events": entries})
    except Exception as exc:
        return _error(exc)


async def _attach_body(client: FirefoxRDPClient, entry: dict[str, Any]) -> None:
    """Fetch and attach a response body, tolerating per-request failures."""
    actor = entry.get("actor")
    if not actor:
        return
    try:
        resp = await client.send(str(actor), "getResponseContent")
    except (RDPError, RDPTimeout):
        entry["body"] = None
        return
    content = resp.get("content")
    if not isinstance(content, dict):
        entry["body"] = None
        return
    text: Any = content.get("text")
    if isinstance(text, dict) and text.get("type") == "longString":
        try:
            text = await client.read_long_string(text, max_chars=MAX_BODY_CHARS)
        except (RDPError, RDPTimeout):
            text = None
    if isinstance(text, str):
        entry["body"] = text[:MAX_BODY_CHARS]
    elif text is None and resp.get("contentDiscarded"):
        entry["body"] = None
        entry["bodyDiscarded"] = True


@mcp.tool()
async def stop_capture() -> str:
    """Stop capturing network events and clean up. Returns the final count."""
    global _capture
    try:
        client = await get_client()
        if _capture is None:
            return _json({"error": "NoCapture", "message": "no capture in progress"})

        capture = _capture
        _capture = None

        # Order matters: unwatching resources tears the watcher actor down, after
        # which it stops answering. Send unwatchTargets FIRST, and treat both as
        # best-effort with a short timeout so teardown can never hang the call.
        for msg_type, params in (
            ("unwatchTargets", {"targetType": "frame"}),
            ("unwatchResources", {"resourceTypes": ["network-event"]}),
        ):
            try:
                await client.send(capture.watcher_actor, msg_type, timeout=2.0, **params)
            except (RDPError, RDPTimeout, TimeoutError):
                pass

        client.remove_event("resources-available-array")
        client.remove_event("resources-updated-array")

        return _json({"status": "stopped", "total_events_captured": len(capture.order)})
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def reconnect() -> str:
    """Drop the RDP connection and reconnect to Firefox.

    Useful after Firefox restarts, or when a tool reports a connection error.
    Actor state is discarded, so the next tool call re-resolves the active tab.
    """
    try:
        await _reset_client()
        client = await get_client()
        tabs = await client.list_tabs()
        return _json(
            {
                "status": "reconnected",
                "host": DEFAULT_RDP_HOST,
                "port": DEFAULT_RDP_PORT,
                "tabs": len(tabs),
            }
        )
    except (RDPError, OSError) as exc:
        # A refused/unreachable port is an expected outcome, not a crash.
        return _json(
            {
                "status": "failed",
                "host": DEFAULT_RDP_HOST,
                "port": DEFAULT_RDP_PORT,
                "message": f"{type(exc).__name__}: {exc}",
            }
        )
    except Exception as exc:
        return _error(exc)


def main() -> None:
    mcp.run(
        transport="streamable-http",
        host=DEFAULT_MCP_HOST,
        port=DEFAULT_MCP_PORT,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
