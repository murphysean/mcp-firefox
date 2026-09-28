"""Tests for the RDP client: framing, dispatch, correlation, evaluation."""

from __future__ import annotations

import asyncio
import json

import pytest

from firefox_mcp.rdp_client import (
    FirefoxRDPClient,
    RDPConnectionError,
    RDPServerError,
    RDPTimeout,
)
from tests.fake_firefox import FakeRDPFirefox


async def connect(fake: FakeRDPFirefox, **kw) -> FirefoxRDPClient:
    """Connect a client to the fake server (imported by other test modules)."""
    client = FirefoxRDPClient("127.0.0.1", fake.port, **kw)
    await client.connect()
    return client


# ------------------------------------------------------------------ handshake


async def test_connect_returns_greeting_and_resolves_actors(fake):
    client = await connect(fake)
    try:
        assert client.connected
        assert client.root_actor == "root"
        assert client.tab_actor == "server1.tabDescriptor1"
        assert client.target_actor == "server1.child2/windowGlobalTarget2"
        assert client.console_actor == "server1.child2/consoleActor3"
    finally:
        await client.disconnect()


async def test_greeting_is_not_swallowed_by_read_loop(fake):
    """Regression: starting the read loop before reading the greeting hung connect."""
    client = await connect(fake)
    try:
        # If the greeting had been consumed by the reader, actor discovery would
        # have failed and target_actor would be None.
        assert client.target_actor is not None
    finally:
        await client.disconnect()


# ------------------------------------------------------------------ dispatch


async def test_response_without_type_key_resolves_request(fake):
    client = await connect(fake)
    try:
        resp = await client.send("root", "listTabs")
        assert "tabs" in resp
        assert len(resp["tabs"]) == 1
    finally:
        await client.disconnect()


async def test_type_key_marks_a_packet_as_an_event_not_a_response(fake):
    """A packet with a `type` must never be consumed as a request response."""
    client = await connect(fake)
    try:
        seen = []
        client.on_event("frameUpdate", seen.append)
        # evaluateJSAsync emits a frameUpdate before its evaluationResult, and
        # the ack (no type) must resolve the request while the event is routed.
        result = await client.evaluate("server1.child2/consoleActor3", "1+1")
        assert result["result"] == 2
        assert seen, "frameUpdate event should have been dispatched to the handler"
    finally:
        await client.disconnect()


async def test_server_error_packet_raises(fake):
    client = await connect(fake)
    try:
        with pytest.raises(RDPServerError) as exc:
            await client.send("root", "noSuchCommand")
        assert "unrecognizedPacketType" in str(exc.value)
    finally:
        await client.disconnect()


async def test_error_packet_does_not_break_later_requests(fake):
    client = await connect(fake)
    try:
        with pytest.raises(RDPServerError):
            await client.send("root", "noSuchCommand")
        resp = await client.send("root", "listTabs")
        assert "tabs" in resp
    finally:
        await client.disconnect()


# ------------------------------------------------------- concurrent requests


async def test_concurrent_requests_to_same_actor_both_resolve(fake):
    """FIFO correlation: two in-flight requests to one actor must not clobber."""
    client = await connect(fake)
    try:
        before = len(fake.received)
        first, second = await asyncio.gather(
            client.send("root", "listTabs"),
            client.send("root", "listTabs"),
        )
        assert "tabs" in first and "tabs" in second
        assert len(fake.received) - before == 2
    finally:
        await client.disconnect()


async def test_concurrent_requests_to_different_actors(fake):
    client = await connect(fake)
    try:
        a, b = await asyncio.gather(
            client.send("root", "listTabs"),
            client.send("server1.tabDescriptor1", "getTarget"),
        )
        assert "tabs" in a
        assert "frame" in b
    finally:
        await client.disconnect()


async def test_request_times_out_without_hanging(fake):
    fake.handlers["neverAnswers"] = lambda msg, r: None  # accepts but never replies
    client = await connect(fake, timeout=0.4)
    try:
        with pytest.raises(RDPTimeout):
            await client.send("root", "neverAnswers")
    finally:
        await client.disconnect()


# ------------------------------------------------------------- disconnection


async def test_pending_requests_fail_when_connection_drops(fake):
    """When the socket dies, in-flight requests must error rather than hang."""
    client = await connect(fake, timeout=5)
    try:
        # Abruptly drop the connection out from under an in-flight request.
        assert fake._client_writer is not None
        fake._client_writer.transport.abort()
        with pytest.raises((RDPConnectionError, RDPTimeout)):
            await client.send("root", "listTabs")
    finally:
        await client.disconnect()


async def test_connected_is_false_after_disconnect(fake):
    client = await connect(fake)
    assert client.connected
    await client.disconnect()
    assert not client.connected


async def test_send_on_closed_client_raises(fake):
    client = await connect(fake)
    await client.disconnect()
    with pytest.raises(RDPConnectionError):
        await client.send("root", "listTabs")


async def test_handler_exception_does_not_kill_reader(fake):
    """A broken event handler must not take down the whole connection."""
    client = await connect(fake)
    try:

        def boom(_msg):
            raise RuntimeError("handler blew up")

        client.on_event("frameUpdate", boom)
        result = await client.evaluate("server1.child2/consoleActor3", "1+1")
        assert result["result"] == 2
        assert client.connected
    finally:
        await client.disconnect()


# ---------------------------------------------------------------- evaluation


async def test_evaluate_plain_expression(fake):
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "1+1")
        assert resp["result"] == 2
        assert resp["hasException"] is False
    finally:
        await client.disconnect()


async def test_console_throw_is_reported_not_raised(fake):
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "THROW")
        assert resp["hasException"] is True
        assert resp["exceptionMessage"] == "boom"
    finally:
        await client.disconnect()


async def test_long_string_result_is_drained(fake):
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "LONG")
        assert isinstance(resp["result"], str)
        assert len(resp["result"]) == 50_000
        assert set(resp["result"]) == {"x"}
    finally:
        await client.disconnect()


async def test_long_string_truncation_is_annotated(fake):
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "LONG", max_chars=1000)
        assert resp["result"].startswith("x" * 1000)
        assert "truncated at 1000 of 50000" in resp["result"]
    finally:
        await client.disconnect()


async def test_toplevel_await_is_rewrapped_and_retried(fake):
    """`await x` must succeed even though Firefox rejects a bare top-level await."""
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "await 5")
        assert resp["hasException"] is False
        assert resp["result"] == "ok"
    finally:
        await client.disconnect()


async def test_legit_syntax_error_is_not_retried_forever(fake):
    client = await connect(fake)
    try:
        resp = await client.evaluate("server1.child2/consoleActor3", "THROW")
        assert resp["hasException"] is True
    finally:
        await client.disconnect()


async def test_concurrent_evaluations_pair_correctly(fake):
    """Each caller must receive its own result, not another's."""
    client = await connect(fake)
    try:
        r1, r2, r3 = await asyncio.gather(
            client.evaluate("server1.child2/consoleActor3", "1+1"),
            client.evaluate("server1.child2/consoleActor3", "1+1"),
            client.evaluate("server1.child2/consoleActor3", "1+1"),
        )
        assert [r["result"] for r in (r1, r2, r3)] == [2, 2, 2]
    finally:
        await client.disconnect()


# ---------------------------------------------------------------------- tabs


async def test_select_tab_targets_requested_index(fake):
    fake.tabs.append(
        {
            "actor": "server1.tabDescriptor2",
            "browsingContextID": 11,
            "selected": False,
            "title": "Tab Two",
            "url": "https://example.org/",
        }
    )
    client = await connect(fake)
    try:
        info = await client.select_tab(1)
        assert client.tab_actor == "server1.tabDescriptor2"
        assert info["tab"]["title"] == "Tab Two"
    finally:
        await client.disconnect()


async def test_select_tab_out_of_range(fake):
    client = await connect(fake)
    try:
        with pytest.raises(Exception):
            await client.select_tab(99)
    finally:
        await client.disconnect()


async def test_invalidate_target_clears_child_actors(fake):
    client = await connect(fake)
    try:
        client.invalidate_target()
        assert client.target_actor is None
        assert client.console_actor is None
        assert client.chrome_console_actor is None
    finally:
        await client.disconnect()


# --------------------------------------------------------------- framing edge


async def test_oversized_frame_is_refused(fake):
    """A desynchronised length prefix must not allocate unbounded memory."""
    client = await connect(fake)
    try:
        assert fake._client_writer is not None
        fake._client_writer.write(b"99999999999999:")
        await fake._client_writer.drain()
        with pytest.raises((RDPConnectionError, RDPTimeout)):
            await client.send("root", "listTabs")
    finally:
        await client.disconnect()


async def test_malformed_length_prefix_is_refused(fake):
    client = await connect(fake)
    try:
        assert fake._client_writer is not None
        fake._client_writer.write(b"not-a-number:")
        await fake._client_writer.drain()
        with pytest.raises((RDPConnectionError, RDPTimeout)):
            await client.send("root", "listTabs")
    finally:
        await client.disconnect()


async def test_unicode_and_multibyte_payloads_round_trip(fake):
    """Length prefixes are byte counts, not character counts."""
    client = await connect(fake)
    try:
        fake.tabs[0]["title"] = "日本語 — emoji 🎉 — ok"
        resp = await client.send("root", "listTabs")
        assert resp["tabs"][0]["title"] == "日本語 — emoji 🎉 — ok"
    finally:
        await client.disconnect()


async def test_large_payload_round_trip(fake):
    client = await connect(fake)
    try:
        blob = "y" * 200_000
        fake.tabs[0]["title"] = blob
        resp = await client.send("root", "listTabs")
        assert resp["tabs"][0]["title"] == blob
    finally:
        await client.disconnect()


async def test_raw_rdp_payload_is_passed_through(fake):
    client = await connect(fake)
    try:
        resp = await client.send_raw({"to": "root", "type": "listTabs"})
        assert "tabs" in resp
        assert json.loads(json.dumps(fake.received[-1]))["type"] == "listTabs"
    finally:
        await client.disconnect()
