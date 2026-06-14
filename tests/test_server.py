"""Smoke tests for the MCP server module."""

from firefox_mcp.server import mcp


def test_server_has_name():
    assert mcp.name == "firefox-devtools"
