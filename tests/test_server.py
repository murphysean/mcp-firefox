"""Tests for the MCP tool layer: JSON contracts, capture parsing, screenshots.

The tools are plain functions under the `@mcp.tool()` decorator, so they can be
called directly. No network or browser is required for most of these.
"""

from __future__ import annotations

import asyncio
import base64
import json

import pytest

from firefox_mcp import server
from firefox_mcp.rdp_client import RDPConnectionError
from tests.test_rdp_client import connect  # reuse the connect helper


@pytest.fixture
async def live(fake, monkeypatch):
    """Point the server module's client cache at a connected fake Firefox."""
    client = await connect(fake)
    monkeypatch.setattr(server, "_client", client)
    monkeypatch.setattr(server, "_capture", None)
    yield server
    await client.disconnect()
    monkeypatch.setattr(server, "_client", None)


def load(text: str):
    return json.loads(text)


# ------------------------------------------------------------- capture parsing


def test_capture_entries_flattens_nested_batch_shape():
    """Firefox batches resources as [[type, [entries...]]], not a flat list."""
    msg = {
        "array": [
            [
                "network-event",
                [
                    {"resourceType": "network-event", "resourceId": 1, "url": "a"},
                    {"resourceType": "network-event", "resourceId": 2, "url": "b"},
                ],
            ]
        ]
    }
    entries = server._capture_entries(msg)
    assert [e["resourceId"] for e in entries] == [1, 2]


def test_capture_entries_handles_flat_list():
    entries = server._capture_entries(
        {"array": [{"resourceType": "network-event", "resourceId": 1}]}
    )
    assert len(entries) == 1


def test_capture_entries_handles_missing_or_odd_array():
    assert server._capture_entries({}) == []
    assert server._capture_entries({"array": None}) == []
    assert server._capture_entries({"array": [None, 5, "x"]}) == []


def test_capture_dedupes_and_merges_updates():
    cap = server._Capture(watcher_actor="w")
    cap.add([{"resourceType": "network-event", "resourceId": 1, "url": "a", "method": "GET"}])
    cap.add([{"resourceType": "network-event", "resourceId": 1, "url": "a", "method": "GET"}])
    cap.update(
        [{"resourceId": 1, "resourceUpdates": {"status": 200, "mimeType": "application/json"}}]
    )
    summary = cap.summary()
    assert len(summary) == 1  # deduped
    assert summary[0]["status"] == 200  # merged from the update
    assert summary[0]["method"] == "GET"  # original preserved


def test_capture_update_for_unknown_resource_is_ignored():
    cap = server._Capture(watcher_actor="w")
    cap.update([{"resourceId": 999, "resourceUpdates": {"status": 200}}])
    assert cap.summary() == []


def test_capture_preserves_insertion_order():
    cap = server._Capture(watcher_actor="w")
    for rid in (3, 1, 2):
        cap.add([{"resourceType": "network-event", "resourceId": rid, "url": str(rid)}])
    assert [e["resourceId"] for e in cap.summary()] == [3, 1, 2]


# ------------------------------------------------------------------- read_page


async def test_read_page_returns_text(live):
    out = await live.read_page()
    assert out == "ok"  # fake evaluates READ_PAGE js to the default probe value


async def test_read_page_reports_console_exception(live):
    async def boom(actor, expression, **kwargs):
        return {"hasException": True, "exceptionMessage": "kaboom"}

    live._client.evaluate = boom
    out = load(await live.read_page())
    assert out["error"] == "EvaluationError"
    assert out["message"] == "kaboom"


async def test_tools_report_error_without_connection(monkeypatch):
    monkeypatch.setattr(server, "_client", None)
    monkeypatch.setattr(server, "DEFAULT_RDP_PORT", 1)  # nothing listening
    out = load(await server.list_tabs())
    assert "error" in out


# ---------------------------------------------------------------- evaluate_js


async def test_evaluate_js_contract(live):
    out = load(await live.evaluate_js("1+1"))
    assert out["result"] == 2
    assert out["hasException"] is False


async def test_evaluate_js_reports_exception(live):
    out = load(await live.evaluate_js("THROW"))
    assert out["hasException"] is True


async def test_evaluate_js_supports_toplevel_await(live):
    out = load(await live.evaluate_js("await new Promise(r => r(9))"))
    assert out["hasException"] is False


# -------------------------------------------------------------------- list_tabs


async def test_list_tabs_includes_index(live):
    out = load(await live.list_tabs())
    assert isinstance(out, list)
    assert out[0]["index"] == 0
    assert out[0]["title"] == "Tab One"


async def test_select_tab_by_index(live):
    out = load(await live.select_tab(0))
    assert out["status"] == "selected"
    assert out["title"] == "Tab One"


# -------------------------------------------------------------------- navigate


async def test_navigate_invalidates_actors_and_reports_status(live, fake):
    fake.handlers["navigateTo"] = lambda msg, r: r.reply()
    out = load(await live.navigate("https://example.com/"))
    assert out == {"status": "navigated", "url": "https://example.com/"}
    # child actors must be forgotten so the next call re-resolves
    assert live._client.target_actor is None
    assert live._client.chrome_console_actor is None


# ------------------------------------------------------------------ screenshot


async def test_screenshot_returns_data_url(live):
    payload = {"data_url": "data:image/png;base64,QUJD", "width": 100, "height": 50}
    live._client.evaluate = _fake_evaluate(json.dumps(payload))
    out = load(await live.screenshot())
    assert out["data_url"].startswith("data:image/png;base64,")
    assert out["width"] == 100


async def test_screenshot_save_path_writes_file(live, tmp_path):
    raw = b"\x89PNG\r\n\x1a\nfakebytes"
    b64 = base64.b64encode(raw).decode()
    payload = {"data_url": f"data:image/png;base64,{b64}", "width": 8, "height": 8}
    live._client.evaluate = _fake_evaluate(json.dumps(payload))
    dest = tmp_path / "shot.png"
    out = load(await live.screenshot(save_path=str(dest)))
    assert dest.read_bytes() == raw
    assert out["bytes"] == len(raw)
    assert "data_url" not in out


async def test_screenshot_reports_selector_not_found(live):
    responses = ["null"]
    live._client.evaluate = _fake_evaluate(*responses)
    out = load(await live.screenshot(selector="#nope"))
    assert out["error"] == "SelectorNotFound"


async def test_screenshot_surfaces_chrome_exception(live, fake):
    async def boom(actor, expr, **kw):
        return {
            "hasException": True,
            "exceptionMessage": "Argument 4 can't be converted to a dictionary.",
        }

    live._client.evaluate = boom
    out = load(await live.screenshot())
    assert out["error"] == "ScreenshotFailed"
    assert "Argument 4" in out["message"]


async def test_screenshot_fullpage_measures_page_first(live):
    calls = []

    async def spy(actor, expr, **kw):
        calls.append(expr)
        if "scrollWidth" in expr:
            return {"result": json.dumps({"w": 1354, "h": 3000})}
        return {
            "result": json.dumps(
                {"data_url": "data:image/png;base64,QQ==", "width": 1354, "height": 3000}
            )
        }

    live._client.evaluate = spy
    out = load(await live.screenshot(fullpage=True))
    assert out["width"] == 1354
    assert any("scrollWidth" in c for c in calls), "fullpage must measure the page"


async def test_screenshot_bad_base64_is_reported(live, tmp_path):
    payload = {"data_url": "data:image/png;base64,!!!not-base64!!!", "width": 1, "height": 1}
    live._client.evaluate = _fake_evaluate(json.dumps(payload))
    out = load(await live.screenshot(save_path=str(tmp_path / "x.png")))
    assert out["error"] == "ScreenshotFailed"


def _fake_evaluate(*results):
    """Return an awaitable evaluate() stub yielding the given results in order."""
    queue = list(results)

    async def _eval(actor, expression, **kwargs):
        assert queue, "evaluate() called more times than the test expected"
        value = queue.pop(0)
        if isinstance(value, dict) and "hasException" in value:
            return value
        return {"result": value, "hasException": False}

    return _eval


# --------------------------------------------------------------------- capture


async def test_capture_lifecycle(live, fake):
    out = load(await live.start_capture())
    assert out["status"] == "capturing"

    again = load(await live.start_capture())
    assert again["status"] == "already_capturing"

    stopped = load(await live.stop_capture())
    assert stopped["status"] == "stopped"


async def test_read_capture_without_start_is_an_error(live):
    out = load(await live.read_capture())
    assert out["error"] == "NoCapture"


async def test_read_capture_collects_events_from_rdp_batches(live, fake):
    await live.start_capture()

    # Simulate Firefox pushing a network-event batch in its nested shape.
    msg = {
        "array": [
            [
                "network-event",
                [
                    {
                        "resourceType": "network-event",
                        "resourceId": 42,
                        "url": "https://example.com/api",
                        "method": "GET",
                        "actor": "server1.netEvent1",
                    }
                ],
            ]
        ]
    }
    for handler in live._client._event_handlers.get("resources-available-array", []):
        handler(msg)
    for handler in live._client._event_handlers.get("resources-updated-array", []):
        handler(
            {
                "array": [
                    [
                        "network-event",
                        [
                            {
                                "resourceId": 42,
                                "resourceUpdates": {"status": 200, "mimeType": "application/json"},
                            }
                        ],
                    ]
                ]
            }
        )

    out = load(await live.read_capture())
    assert out["count"] == 1
    assert out["events"][0]["status"] == 200
    assert out["events"][0]["url"] == "https://example.com/api"


async def test_read_capture_only_errors_filter(live):
    await live.start_capture()
    msg = {
        "array": [
            [
                "network-event",
                [
                    {"resourceType": "network-event", "resourceId": 1, "url": "ok", "actor": "a1"},
                    {"resourceType": "network-event", "resourceId": 2, "url": "bad", "actor": "a2"},
                ],
            ]
        ]
    }
    for handler in live._client._event_handlers["resources-available-array"]:
        handler(msg)
    for handler in live._client._event_handlers["resources-updated-array"]:
        handler(
            {"array": [["network-event", [{"resourceId": 1, "resourceUpdates": {"status": 200}}]]]}
        )
    for handler in live._client._event_handlers["resources-updated-array"]:
        handler(
            {"array": [["network-event", [{"resourceId": 2, "resourceUpdates": {"status": 503}}]]]}
        )

    out = load(await live.read_capture(only_errors=True))
    assert out["count"] == 1
    assert out["events"][0]["url"] == "bad"


async def test_stop_capture_completes_even_when_watcher_is_gone(live, fake):
    """Regression: unwatchResources kills the watcher, so stop_capture hung.

    Real Firefox stops answering the watcher actor after unwatchResources, so
    stop_capture must send unwatchTargets first and never block on cleanup.
    """
    await live.start_capture()
    out = load(await asyncio.wait_for(live.stop_capture(), timeout=10))
    assert out["status"] == "stopped"
    assert fake.watcher_alive is False  # resources were unwatched


async def test_stop_capture_without_start_is_an_error(live):
    out = load(await live.stop_capture())
    assert out["error"] == "NoCapture"


# ------------------------------------------------------------------ raw + misc


async def test_raw_rdp_command_rejects_bad_json(live):
    out = load(await live.raw_rdp_command("{not json"))
    assert out["error"] == "InvalidJSON"


async def test_raw_rdp_command_rejects_non_object(live):
    out = load(await live.raw_rdp_command("[1,2,3]"))
    assert out["error"] == "InvalidPayload"


async def test_raw_rdp_command_passes_payload_through(live):
    out = load(await live.raw_rdp_command(json.dumps({"to": "root", "type": "listTabs"})))
    assert "tabs" in out


async def test_reconnect_reports_status(monkeypatch):
    """reconnect() must not raise even when Firefox is unreachable."""
    monkeypatch.setattr(server, "_client", None)
    monkeypatch.setattr(server, "DEFAULT_RDP_PORT", 1)
    out = load(await server.reconnect())
    assert out["status"] in {"failed", "reconnected"}


def test_connection_error_surfaces_as_json():
    out = load(server._error(RDPConnectionError("boom")))
    assert out == {"error": "RDPConnectionError", "message": "boom"}
