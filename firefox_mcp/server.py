"""Firefox DevTools MCP Server - streaming HTTP transport."""

import asyncio
import json
import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from .rdp_client import FirefoxRDPClient

mcp = FastMCP("firefox-devtools", host="0.0.0.0", port=8090, stateless_http=True)

# Global RDP client instance
_client: FirefoxRDPClient | None = None

# Network capture state
_capture_watcher: str | None = None
_capture_events: list[dict] = []


async def get_client() -> FirefoxRDPClient:
    """Get or create the RDP client connection."""
    global _client
    if _client is None or not _client.connected:
        host = os.environ.get("FIREFOX_RDP_HOST", "localhost")
        port = int(os.environ.get("FIREFOX_RDP_PORT", "6000"))
        _client = FirefoxRDPClient(host, port)
        await _client.connect()
    elif _client.target_actor is None and _client.tab_actor:
        target_resp = await _client.send(_client.tab_actor, "getTarget")
        if frame := target_resp.get("frame"):
            _client.target_actor = frame.get("actor")
            _client.console_actor = frame.get("consoleActor")
            _client.inspector_actor = frame.get("inspectorActor")
            _client.network_actor = frame.get("networkContentActor")
    return _client


async def _eval_js(client: FirefoxRDPClient, console_actor: str, expression: str) -> dict:
    """Evaluate JS and wait for the evaluationResult message via event handler."""
    fut = asyncio.get_event_loop().create_future()

    def _on_result(msg):
        if msg.get("type") == "evaluationResult" and not fut.done():
            fut.set_result(msg)
            client.remove_event("evaluationResult")

    client.on_event("evaluationResult", _on_result)
    # Send the eval - the initial ack (with resultID) will be dispatched to _pending
    # but we want the evaluationResult event
    await client.send(console_actor, "evaluateJSAsync", text=expression, mapped={"await": True})
    return await fut


@mcp.tool()
async def evaluate_js(expression: str) -> str:
    """Evaluate a JavaScript expression in the current tab's console and return the result."""
    client = await get_client()
    if not client.console_actor:
        return json.dumps({"error": "No console actor found. Is a tab open?"})
    resp = await _eval_js(client, client.console_actor, expression)
    return json.dumps(resp, default=str)


@mcp.tool()
async def list_tabs() -> str:
    """List all open browser tabs with their URLs and titles."""
    client = await get_client()
    resp = await client.send(client.root_actor, "listTabs")
    tabs = resp.get("tabs", [])
    return json.dumps([{"title": t.get("title"), "url": t.get("url"), "actor": t.get("actor")} for t in tabs], indent=2)


@mcp.tool()
async def navigate(url: str) -> str:
    """Navigate the current tab to a URL."""
    client = await get_client()
    if not client.target_actor:
        return json.dumps({"error": "No target actor found"})
    resp = await client.send(client.target_actor, "navigateTo", url=url)
    client.target_actor = None
    client.console_actor = None
    client.inspector_actor = None
    client.network_actor = None
    return json.dumps(resp, default=str)


@mcp.tool()
async def get_page_source() -> str:
    """Get the HTML source of the current page via console evaluation."""
    return await evaluate_js("document.documentElement.outerHTML")


@mcp.tool()
async def get_console_messages() -> str:
    """Get cached console messages from the current tab."""
    client = await get_client()
    if not client.console_actor:
        return json.dumps({"error": "No console actor found"})
    resp = await client.send(client.console_actor, "getCachedMessages", messageTypes=["PageError", "ConsoleAPI"])
    return json.dumps(resp, default=str)


@mcp.tool()
async def read_page() -> str:
    """Extract the readable text content from the current page.

    Uses heuristics to strip navigation, ads, and boilerplate, returning
    the main article/content text. Works well on news sites, blogs, and
    documentation pages.
    """
    js = """(() => {
        const remove = ['script','style','nav','header','footer','aside',
            'iframe','noscript','.ad,.ads,.advertisement,.sidebar,.menu,.nav',
            '[role="navigation"],[role="banner"],[role="complementary"]'];
        const clone = document.cloneNode(true);
        for (const sel of remove) {
            clone.querySelectorAll(sel).forEach(el => el.remove());
        }
        const main = clone.querySelector('main, article, [role="main"], .post-content, .article-body, .entry-content, #content')
            || clone.body;
        return main.innerText.replace(/\\n{3,}/g, '\\n\\n').trim();
    })()"""
    return await evaluate_js(js)


@mcp.tool()
async def raw_rdp_command(payload: str) -> str:
    """Send a raw JSON payload to Firefox RDP and return the response.

    The payload should be a JSON string with at minimum 'to' and 'type' fields.
    Use this to experiment with RDP commands not covered by other tools.

    Example: {"to": "root", "type": "listTabs"}
    """
    client = await get_client()
    try:
        msg = json.loads(payload)
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})
    resp = await client.send_raw(msg)
    return json.dumps(resp, default=str)


@mcp.tool()
async def screenshot(selector: str = "", fullpage: bool = False) -> str:
    """Take a screenshot of the current tab. Returns a base64 PNG data URL.

    Args:
        selector: CSS selector to screenshot a specific element. Empty for viewport.
        fullpage: Capture the full scrollable page instead of just the viewport.
    """
    client = await get_client()
    tabs_resp = await client.send(client.root_actor, "listTabs")
    tabs = tabs_resp.get("tabs", [])
    if not tabs:
        return json.dumps({"error": "No tabs open"})
    bc_id = tabs[0].get("browsingContextID")

    proc_resp = await client.send(client.root_actor, "getProcess", id=0)
    proc_actor = proc_resp.get("processDescriptor", {}).get("actor")
    target_resp = await client.send(proc_actor, "getTarget")
    chrome_console = target_resp.get("process", {}).get("consoleActor")
    if not chrome_console:
        return json.dumps({"error": "Could not get chrome console actor"})

    if selector:
        rect_result = await _eval_js(client, client.console_actor, f"JSON.stringify(document.querySelector('{selector}').getBoundingClientRect())")
        if rect_result.get("hasException"):
            return json.dumps({"error": f"Selector '{selector}' not found"})
        rect_str = rect_result.get("result", "{}")
        js = f"""(async () => {{
            const bc = BrowsingContext.get({bc_id});
            const r = {rect_str};
            const rect = new DOMRect(r.x, r.y, r.width, r.height);
            const snapshot = await bc.currentWindowGlobal.drawSnapshot(rect, 1.0, 'rgb(255,255,255)', false);
            const canvas = new OffscreenCanvas(snapshot.width, snapshot.height);
            const ctx = canvas.getContext('2d');
            ctx.drawImage(snapshot, 0, 0);
            snapshot.close();
            const blob = await canvas.convertToBlob({{type: 'image/png'}});
            const buf = await blob.arrayBuffer();
            return 'data:image/png;base64,' + ChromeUtils.base64URLEncode(new Uint8Array(buf), {{pad: true}}).replace(/-/g,'+').replace(/_/g,'/');
        }})()"""
    else:
        js = f"""(async () => {{
            const bc = BrowsingContext.get({bc_id});
            const snapshot = await bc.currentWindowGlobal.drawSnapshot(null, 1.0, 'rgb(255,255,255)', {'true' if fullpage else 'false'});
            const canvas = new OffscreenCanvas(snapshot.width, snapshot.height);
            const ctx = canvas.getContext('2d');
            ctx.drawImage(snapshot, 0, 0);
            snapshot.close();
            const blob = await canvas.convertToBlob({{type: 'image/png'}});
            const buf = await blob.arrayBuffer();
            return 'data:image/png;base64,' + ChromeUtils.base64URLEncode(new Uint8Array(buf), {{pad: true}}).replace(/-/g,'+').replace(/_/g,'/');
        }})()"""

    result = await _eval_js(client, chrome_console, js)
    if result.get("hasException"):
        return json.dumps({"error": result.get("exceptionMessage", "Screenshot failed")})
    data = result.get("result")
    # Handle longString
    if isinstance(data, dict) and data.get("type") == "longString":
        sub_resp = await client.send(data["actor"], "substring", start=0, end=data["length"])
        return sub_resp.get("substring", "")
    return data if isinstance(data, str) else json.dumps(data, default=str)


# --- Network capture tools ---

@mcp.tool()
async def start_capture() -> str:
    """Start capturing network requests on the current tab.

    Subscribes to network events via Firefox's watcher. All HTTP requests/responses
    will be accumulated until read_capture or stop_capture is called.
    Must be started BEFORE page load to capture all requests.
    """
    global _capture_watcher, _capture_events
    client = await get_client()

    if _capture_watcher:
        return json.dumps({"status": "already_capturing", "events_so_far": len(_capture_events)})

    _capture_events = []

    # Get watcher from tab descriptor
    watcher_resp = await client.send(
        client.tab_actor, "getWatcher",
        isServerTargetSwitchingEnabled=True, isPopupDebuggingEnabled=False
    )
    watcher_actor = watcher_resp.get("actor")
    if not watcher_actor:
        return json.dumps({"error": "Could not get watcher actor"})

    _capture_watcher = watcher_actor

    # Register handler for network events
    def _on_resources(msg):
        for resource in msg.get("array", []):
            if resource.get("resourceType") == "network-event":
                _capture_events.append(resource)

    client.on_event("resources-available-array", _on_resources)

    # Also handle updates to existing events
    def _on_updates(msg):
        for update in msg.get("array", []):
            if update.get("resourceType") == "network-event":
                # Match by resourceId and merge
                rid = update.get("resourceId")
                for ev in _capture_events:
                    if ev.get("resourceId") == rid:
                        ev.update(update)
                        break

    client.on_event("resources-updated-array", _on_updates)

    # Watch targets and resources
    await client.send(watcher_actor, "watchTargets", targetType="frame")
    await client.send(watcher_actor, "watchResources", resourceTypes=["network-event"])

    return json.dumps({"status": "capturing", "watcher": watcher_actor})


@mcp.tool()
async def read_capture(include_bodies: bool = False) -> str:
    """Read captured network events.

    Args:
        include_bodies: If true, fetch response bodies for each request (slower).

    Returns a list of captured network requests with URL, method, status, content-type, and timing.
    """
    global _capture_events
    client = await get_client()

    if not _capture_watcher:
        return json.dumps({"error": "No capture in progress. Call start_capture first."})

    # Summarize events
    summary = []
    for ev in _capture_events:
        entry = {
            "url": ev.get("url"),
            "method": ev.get("method"),
            "status": ev.get("status"),
            "statusText": ev.get("statusText"),
            "contentType": ev.get("mimeType"),
            "transferSize": ev.get("transferredSize"),
            "startTime": ev.get("startedMs"),
            "resourceId": ev.get("resourceId"),
        }
        # Include response body if requested
        if include_bodies and ev.get("actor"):
            try:
                content_resp = await client.send(ev["actor"], "getResponseContent")
                content = content_resp.get("content", {})
                text = content.get("text", "")
                if isinstance(text, dict) and text.get("type") == "longString":
                    sub = await client.send(text["actor"], "substring", start=0, end=min(text["length"], 10000))
                    entry["body"] = sub.get("substring", "")[:10000]
                elif isinstance(text, str):
                    entry["body"] = text[:10000]
            except Exception:
                entry["body"] = None
        summary.append(entry)

    return json.dumps({"count": len(summary), "events": summary}, indent=2, default=str)


@mcp.tool()
async def stop_capture() -> str:
    """Stop capturing network events and clean up.

    Returns final count of captured events.
    """
    global _capture_watcher, _capture_events
    client = await get_client()

    if not _capture_watcher:
        return json.dumps({"error": "No capture in progress."})

    # Unsubscribe
    try:
        await client.send(_capture_watcher, "unwatchResources", resourceTypes=["network-event"])
        await client.send(_capture_watcher, "unwatchTargets", targetType="frame")
    except Exception:
        pass

    client.remove_event("resources-available-array")
    client.remove_event("resources-updated-array")

    count = len(_capture_events)
    _capture_watcher = None
    _capture_events = []

    return json.dumps({"status": "stopped", "total_events_captured": count})


def main():
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
