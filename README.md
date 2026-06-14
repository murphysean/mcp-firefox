# mcp-firefox

An MCP (Model Context Protocol) server that connects to Firefox's Remote Debugging Protocol (RDP) over TCP, exposing browser DevTools capabilities as tools for LLMs.

## How It Works

Firefox has a built-in Remote Debugging Protocol that speaks length-prefixed JSON over TCP. This server maintains a persistent connection to that port and translates MCP tool calls into RDP messages, returning the results to the LLM.

```
LLM ↔ MCP (HTTP :8090) ↔ Firefox RDP (TCP :6000)
```

The RDP client uses a background reader task to handle both request/response pairs and unsolicited events (like network traffic), enabling real-time capture features.

## Tools

| Tool | Description |
|------|-------------|
| `evaluate_js` | Evaluate a JavaScript expression in the current tab's console (supports top-level await) |
| `list_tabs` | List all open browser tabs with URLs and titles |
| `navigate` | Navigate the current tab to a URL |
| `get_page_source` | Get the full HTML source of the current page |
| `get_console_messages` | Get cached console messages (errors, logs) |
| `read_page` | Extract readable text content from the page (strips nav, ads, boilerplate) |
| `screenshot` | Take a screenshot — viewport, full page, or a specific CSS selector element |
| `start_capture` | Start capturing network requests (like opening the Network tab) |
| `read_capture` | Read captured network events (URLs, methods, status, timing, optionally bodies) |
| `stop_capture` | Stop network capture and clean up |
| `raw_rdp_command` | Send arbitrary RDP JSON for experimentation |

## Firefox Setup

### 1. Enable Remote Debugging

Open `about:config` in Firefox and set:

| Preference | Value | Purpose |
|-----------|-------|---------|
| `devtools.debugger.remote-enabled` | `true` | Allow remote debug connections |
| `devtools.debugger.prompt-connection` | `false` | Skip the "allow connection?" dialog (optional) |
| `devtools.chrome.enabled` | `true` | Enable chrome debugging |

### 2. Start the Debug Listener

```bash
firefox --start-debugger-server 6000
```

This opens a TCP listener on localhost:6000. Your tabs and session restore normally.

### 3. Verify

```bash
ss -tlnp | grep 6000
```

You should see Firefox listening on 127.0.0.1:6000.

## Installation

Requires Python 3.11+. Uses [uv](https://docs.astral.sh/uv/) for dependency management:

```bash
uv venv
uv pip install -e .
```

## Running

```bash
.venv/bin/firefox-mcp
```

The MCP server starts on `http://0.0.0.0:8090/mcp` using streamable HTTP transport (stateless mode).

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FIREFOX_RDP_HOST` | `localhost` | Firefox RDP host |
| `FIREFOX_RDP_PORT` | `6000` | Firefox RDP port |

## MCP Client Configuration

```json
{
  "mcpServers": {
    "firefox-devtools": {
      "url": "http://localhost:8090/mcp"
    }
  }
}
```

## Usage Examples

### Page Content & Screenshots

```
# Read article text from a news site you're logged into
> read_page

# Take a screenshot of a specific element
> screenshot(selector=".article-body")

# Full page screenshot
> screenshot(fullpage=True)
```

### Network Capture

Capture API traffic from web apps (streaming services, SPAs, etc.):

```
# Start recording network traffic
> start_capture

# Navigate to the app or trigger actions
> navigate("https://www.disneyplus.com/home")

# See what API calls were made
> read_capture

# Get response bodies (JSON payloads, manifests, etc.)
> read_capture(include_bodies=True)

# Done
> stop_capture
```

### Raw RDP Exploration

```
> raw_rdp_command({"to": "root", "type": "listTabs"})
> raw_rdp_command({"to": "<consoleActor>", "type": "getCachedMessages", "messageTypes": ["PageError", "ConsoleAPI"]})
```

Use `list_tabs` first to discover actor IDs, then experiment with `raw_rdp_command`.
