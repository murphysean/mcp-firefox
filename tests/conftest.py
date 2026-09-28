"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from tests.fake_firefox import FakeRDPFirefox, default_handlers


@pytest.fixture
async def fake():
    """A running fake Firefox RDP server, torn down after the test."""
    server = FakeRDPFirefox()
    default_handlers(server)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
