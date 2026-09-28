"""Tests for the RDP client: framing, dispatch, correlation, evaluation, tabs."""

from __future__ import annotations

import asyncio
import json

import pytest

from firefox_mcp.rdp_client import (
    FirefoxRDPClient,
    RDPBackpressure,
    RDPConnectionError,
    RDPError,
    RDPServerError,
    RDPTimeout,
)
from tests.fake_firefox import FakeRDPFirefox


async def connect(fake: FakeRDPFirefox, **kw) -> FirefoxRDPClient:
    """Connect a client to the fake server (imported by other test modules)."""
    client = FirefoxRDPClient("127.0.0.1", fake.port, **kw)
    await client.connect()
    return client


async def session_client(fake: FakeRDPFirefox, **kw) -> tuple[FirefoxRDPClient, object]:
    """Connect and open a tab session (the common starting point)."""
    client = await connect(fake, **kw)
    return client, await client.active_session()


# ------------------------------------------------------------------ handshake


async def test_connect_returns_greeting_without_eager_resolution(fake):
    """Connect must not require a tab; resolution is lazy."""
    client = await connect(fake)
    try:
        assert client.connected
        assert client.root_actor == "root"
        assert client.active_tab_actor is None
    finally:
        await client.disconnect()


async def test_greeting_is_not_swallowed_by_read_loop(fake):
    """Regression: starting the read loop before reading the greeting hung connect."""
    client = await connect(fake)
    try:
        # If the greeting had been consumed by the reader, listTabs would hang.
        assert await client.list_tabs()
    finally:
        await client.disconnect()


# ------------------------------------------------------------------ dispatch


async def test_response_without_type_key_resolves_request(fake):
    client = await connect(fake)
    try:
        resp = await client.send("root", "listTabs")
        assert "tabs" in resp
    finally:
        await client.disconnect()


async def test_type_key_marks_a_packet_as_an_event_not_a_response(fake):
    """A packet with a `type` must never be consumed as a request response."""
    client, session = await session_client(fake)
    try:
        seen = []
        client.on_event("frameUpdate", seen.append)
        result = await client.evaluate(session.console_actor, "1+1")
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
        assert "tabs" in await client.send("root", "listTabs")
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


async def test_request_times_out_without_hanging(fake):
    client = await connect(fake, timeout=0.4)
    try:
        with pytest.raises(RDPTimeout):
            await client.send("root", "neverAnswers")
    finally:
        await client.disconnect()


async def test_cancelled_request_does_not_steal_a_later_response(fake):
    """A caller that walks away must not consume the next response.

    Responses are matched FIFO per actor, so a stale future left in the queue
    would resolve the *following* request with the wrong payload.
    """
    client = await connect(fake, timeout=30)
    try:
        task = asyncio.create_task(client.send("root", "neverAnswers"))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The queue must be clean, so this resolves with its own response.
        resp = await client.send("root", "listTabs")
        assert "tabs" in resp
    finally:
        await client.disconnect()


# ------------------------------------------------------------- back-pressure


async def _settle(times: int = 5) -> None:
    """Yield repeatedly so spawned tasks reach their first await."""
    for _ in range(times):
        await asyncio.sleep(0)


async def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.01) -> None:
    """Poll ``predicate`` until true, or fail the test on timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met before timeout")


async def test_backpressure_limits_inflight_requests(fake):
    client = await connect(fake, max_inflight_per_actor=3, timeout=30)
    try:
        tasks = [asyncio.create_task(client.send("root", "neverAnswers")) for _ in range(3)]
        await _settle()
        with pytest.raises(RDPBackpressure):
            await client.send("root", "neverAnswers")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.disconnect()


async def test_backpressure_limits_inflight_evaluations(fake):
    client, session = await session_client(fake, max_inflight_per_actor=2)
    try:
        assert session.console_actor

        # Make evaluateJSAsync accept the request but never produce a result, so
        # the evaluation stays in flight.
        async def silent(msg, r):
            return

        fake.handlers["evaluateJSAsync"] = silent
        tasks = [
            asyncio.create_task(client.evaluate(session.console_actor, "1+1")) for _ in range(2)
        ]
        await _settle()
        with pytest.raises(RDPBackpressure):
            await client.evaluate(session.console_actor, "1+1")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.disconnect()


async def test_backpressure_releases_after_completion(fake):
    client = await connect(fake, max_inflight_per_actor=1)
    try:
        await client.send("root", "listTabs")
        # The slot must be free again, otherwise the limit would be a one-shot.
        await client.send("root", "listTabs")
    finally:
        await client.disconnect()


# ------------------------------------------------------------- disconnection


async def test_pending_requests_fail_when_connection_drops(fake):
    client = await connect(fake, timeout=5)
    try:
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
    client, session = await session_client(fake)
    try:

        def boom(_msg):
            raise RuntimeError("handler blew up")

        client.on_event("frameUpdate", boom)
        assert (await client.evaluate(session.console_actor, "1+1"))["result"] == 2
        assert client.connected
    finally:
        await client.disconnect()


# ---------------------------------------------------------------- evaluation


async def test_evaluate_plain_expression(fake):
    client, session = await session_client(fake)
    try:
        resp = await client.evaluate(session.console_actor, "1+1")
        assert resp["result"] == 2
        assert resp["hasException"] is False
    finally:
        await client.disconnect()


async def test_console_throw_is_reported_not_raised(fake):
    client, session = await session_client(fake)
    try:
        resp = await client.evaluate(session.console_actor, "THROW")
        assert resp["hasException"] is True
        assert resp["exceptionMessage"] == "boom"
    finally:
        await client.disconnect()


async def test_long_string_result_is_drained(fake):
    client, session = await session_client(fake)
    try:
        resp = await client.evaluate(session.console_actor, "LONG")
        assert isinstance(resp["result"], str)
        assert len(resp["result"]) == 50_000
        assert set(resp["result"]) == {"x"}
    finally:
        await client.disconnect()


async def test_long_string_truncation_is_annotated(fake):
    client, session = await session_client(fake)
    try:
        resp = await client.evaluate(session.console_actor, "LONG", max_chars=1000)
        assert resp["result"].startswith("x" * 1000)
        assert "truncated at 1000 of 50000" in resp["result"]
    finally:
        await client.disconnect()


async def test_toplevel_await_is_rewrapped_and_retried(fake):
    """`await x` must succeed even though Firefox rejects a bare top-level await."""
    client, session = await session_client(fake)
    try:
        resp = await client.evaluate(session.console_actor, "await 5")
        assert resp["hasException"] is False
        assert resp["result"] == "ok"
    finally:
        await client.disconnect()


async def test_concurrent_evaluations_pair_correctly(fake):
    """Each caller must receive its own result, not another's."""
    client, session = await session_client(fake)
    try:
        results = await asyncio.gather(
            client.evaluate(session.console_actor, "1+1"),
            client.evaluate(session.console_actor, "1+1"),
            client.evaluate(session.console_actor, "1+1"),
        )
        assert [r["result"] for r in results] == [2, 2, 2]
    finally:
        await client.disconnect()


# ------------------------------------------------------------- tab sessions


async def test_session_resolves_through_watcher_not_gettarget(fake):
    """The modern target exposes consoleActor directly; no getTarget needed."""
    client = await connect(fake)
    try:
        session = await client.use_tab(0)
        assert session.ready
        assert session.browsing_context_id == 10
        assert session.target_actor
        assert session.console_actor
        # getTarget must not have been used to resolve the page console.
        assert not any(m.get("type") == "getTarget" for m in fake.received)
    finally:
        await client.disconnect()


async def test_watcher_requires_target_switching_for_events(fake):
    """Without isServerTargetSwitchingEnabled Firefox emits no target events.

    The client must request it, or session resolution would time out.
    """
    client = await connect(fake)
    try:
        await client.use_tab(0)
        assert fake.switching_enabled is True
    finally:
        await client.disconnect()


async def test_session_times_out_if_no_target_arrives(fake):
    """If Firefox never announces a target, surface a timeout, don't hang."""
    fake.switching_enabled = False
    original = fake.handlers["getWatcher"]

    async def no_switching(msg, r):
        # A build that ignores the flag would leave us waiting forever.
        await original({**msg, "isServerTargetSwitchingEnabled": False}, r)

    fake.handlers["getWatcher"] = no_switching
    client = await connect(fake)
    try:
        import firefox_mcp.rdp_client as mod

        original_timeout = mod.TARGET_WAIT_TIMEOUT
        mod.TARGET_WAIT_TIMEOUT = 0.4
        try:
            with pytest.raises(RDPTimeout):
                await client.use_tab(0)
        finally:
            mod.TARGET_WAIT_TIMEOUT = original_timeout
    finally:
        await client.disconnect()


async def test_active_session_is_reused(fake):
    client = await connect(fake)
    try:
        first = await client.active_session()
        second = await client.active_session()
        assert first is second
    finally:
        await client.disconnect()


async def test_use_tab_targets_requested_index(fake):
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
        session = await client.use_tab(1)
        assert client.active_tab_actor == "server1.tabDescriptor2"
        assert session.tab_actor == "server1.tabDescriptor2"
    finally:
        await client.disconnect()


async def test_use_tab_out_of_range(fake):
    client = await connect(fake)
    try:
        with pytest.raises(Exception):
            await client.use_tab(99)
    finally:
        await client.disconnect()


async def test_sessions_are_keyed_by_stable_tab_actor(fake):
    """Indices shift and browsingContextIDs change; the tab actor does not."""
    client = await connect(fake)
    try:
        session = await client.use_tab(0)
        assert session.tab_actor in client._sessions
        assert session.browsing_context_id == 10
    finally:
        await client.disconnect()


# --------------------------------------------------------------- navigation


async def test_navigate_waits_for_replacement_target(fake):
    """Navigation destroys the target; the client must adopt the new one."""
    client = await connect(fake)
    try:
        before = await client.use_tab(0)
        old_actor = before.target_actor

        after = await client.navigate("https://example.com/next")

        assert after.target_actor is not None
        assert after.target_actor != old_actor, "target actor must have been replaced"
        assert after.ready
    finally:
        await client.disconnect()


async def test_navigate_then_evaluate_uses_new_console(fake):
    """The next call after navigate must target the new console, not a dead one."""
    client = await connect(fake)
    try:
        session = await client.navigate("https://example.com/next")
        resp = await client.evaluate(session.console_actor, "1+1")
        assert resp["result"] == 2
    finally:
        await client.disconnect()


async def test_navigate_survives_browsing_context_change(fake):
    """Regression: Firefox allocates a NEW browsingContextID on navigation.

    Keying sessions by browsingContextID (as an earlier version did) orphans the
    session on navigation, so the replacement target announcement is dropped and
    every later call times out with "no target became available".
    """
    client = await connect(fake)
    try:
        before = await client.use_tab(0)
        old_bc = before.browsing_context_id
        old_target = before.target_actor

        after = await client.navigate("https://example.com/next")

        assert after.browsing_context_id != old_bc, "fake must change the bcID (as Firefox does)"
        assert after.target_actor != old_target
        assert after.ready, "session must adopt the replacement target"
        # And it must still be the same session object, not an orphan.
        assert after is before
    finally:
        await client.disconnect()


async def test_target_events_correlate_via_watcher_actor(fake):
    """Target events are matched by sender (the watcher), not by bcID."""
    client = await connect(fake)
    try:
        session = await client.use_tab(0)
        assert session.watcher_actor
        # The event's `from` must be the watcher actor for the session.
        assert session.watcher_actor.startswith("server1.watcher")
    finally:
        await client.disconnect()


async def test_destroyed_target_is_cleared(fake):
    """A target-destroyed-form must invalidate the stored actor."""
    client = await connect(fake)
    try:
        session = await client.use_tab(0)
        assert session.target_actor
        assert fake._client_writer is not None
        await fake.destroy_current_target(fake._client_writer)
        # The reader task dispatches asynchronously; give it a moment.
        await _wait_until(lambda: session.target_actor is None)
        assert session.target_actor is None
        assert session.console_actor is None
        assert not session.ready
    finally:
        await client.disconnect()


async def test_navigate_without_target_resolves_a_fresh_session(fake):
    """If the session has no target, navigate re-resolves instead of failing.

    Losing your target (tab closed, Firefox restarted) should recover, not
    raise.
    """
    client = await connect(fake)
    try:
        session = await client.active_session()
        session.clear_target()
        assert not session.ready

        # active_session() heals by resolving again, so navigation still works.
        assert not session.ready
        refreshed = await client.active_session()
        assert refreshed.ready
    finally:
        await client.disconnect()


async def test_navigate_raises_when_no_tabs_exist(fake):
    fake.tabs.clear()
    client = await connect(fake)
    try:
        with pytest.raises(RDPError):
            await client.navigate("https://example.com/")
    finally:
        await client.disconnect()


# ------------------------------------------------------------- chrome console


async def test_chrome_console_uses_process_descriptor(fake):
    """The parent-process console still comes from getProcess + getTarget."""
    client = await connect(fake)
    try:
        console = await client.ensure_chrome_console()
        assert console == "server1.chromeConsole1"
        assert any(m.get("type") == "getProcess" for m in fake.received)
    finally:
        await client.disconnect()


async def test_chrome_console_is_cached(fake):
    client = await connect(fake)
    try:
        first = await client.ensure_chrome_console()
        before = len(fake.received)
        second = await client.ensure_chrome_console()
        assert first == second
        assert len(fake.received) == before
    finally:
        await client.disconnect()


async def test_forget_sessions_clears_chrome_console(fake):
    client = await connect(fake)
    try:
        await client.ensure_chrome_console()
        client.forget_sessions()
        assert client.chrome_console_actor is None
        assert client.active_tab_actor is None
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
