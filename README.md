# mcp-firefox

An [MCP](https://modelcontextprotocol.io) server that connects to Firefox's Remote Debugging
Protocol (RDP) over TCP, exposing browser DevTools capabilities as tools for LLMs.

## How It Works

Firefox ships with a built-in Remote Debugging Protocol that speaks length-prefixed JSON over
TCP. This server holds a persistent connection to that port, translates MCP tool calls into RDP
messages, and returns the results:

```
LLM ↔ MCP (HTTP :8090) ↔ Firefox RDP (TCP :6000)
```

A background reader task owns the socket and dispatches every inbound packet:

* a packet **without** a `type` field is a **response** (or an error), and resolves the oldest
  pending request for the addressing actor;
* a packet **with** a `type` field is an unsolicited **event**.

That distinction is the only reliable correlation mechanism — Firefox does not echo a
`requestId` to this protocol version, and actors emit both responses and events on the same
connection. Getting it wrong is subtle: an event can resolve a request (hanging the caller), or
a response can be swallowed as an event (hanging it *and* losing the data).

Promises are awaited for you, and a literal top-level `await` is automatically re-wrapped in an
async function, because Firefox's console rejects a bare top-level `await` in a plain script.

## Requirements

- Python 3.11+
- Firefox with the remote debugging server enabled (see below)
- [uv](https://docs.astral.sh/uv/) for dependency management

## Installation

```bash
uv venv
uv pip install -e ".[dev]"    # omit [dev] for a runtime-only install
```

## Firefox Setup

### 1. Enable Remote Debugging

In `about:config`, set:

| Preference | Value | Purpose |
|-----------|-------|---------|
| `devtools.debugger.remote-enabled` | `true` | Allow remote debug connections |
| `devtools.debugger.prompt-connection` | `false` | Skip the "allow connection?" dialog |
| `devtools.chrome.enabled` | `true` | Enable chrome debugging (**required** for `screenshot`) |

Without `devtools.chrome.enabled`, screenshot capture cannot reach the privileged APIs it needs.

### 2. Start the Debug Listener

```bash
# Native install
firefox --start-debugger-server 6000

# Flatpak
flatpak run org.mozilla.firefox --start-debugger-server 6000
```

Firefox will not start a second instance against a profile that is already open, so launch it
*with* the flag, or use a dedicated debug profile:

```bash
# Flatpak, isolated profile (recommended — avoids lock contention with your normal browser)
flatpak run org.mozilla.firefox --no-remote \
  --profile ~/.var/app/org.mozilla.firefox/config/mozilla/firefox/debug \
  --start-debugger-server 6000
```

> **Flatpak path note:** the `--profile` path is resolved *outside* the sandbox, so pass the host
> path (`~/.var/app/org.mozilla.firefox/config/...`). Flatpak maps the sandbox's `~/.config` onto
> that directory, so sandbox-style paths like `~/.config/mozilla/firefox/debug` do **not** work.

> **Snap note:** snap-packaged Firefox builds do not support `--start-debugger-server`.

### 3. Verify

```bash
ss -tln | grep 6000
```

You should see Firefox listening on `127.0.0.1:6000`.

## Running

```bash
.venv/bin/firefox-mcp
```

Starts a streamable-HTTP MCP server. By default it binds **`127.0.0.1:8090`**.

| Variable | Default | Description |
|----------|---------|-------------|
| `FIREFOX_RDP_HOST` | `localhost` | Firefox RDP host |
| `FIREFOX_RDP_PORT` | `6000` | Firefox RDP port |
| `FIREFOX_MCP_HOST` | `127.0.0.1` | MCP server bind address |
| `FIREFOX_MCP_PORT` | `8090` | MCP server port |

> **Security:** this server grants unauthenticated JavaScript execution, page reading and
> screenshots of a browser that is typically logged into everything. It binds to loopback only.
> Change `FIREFOX_MCP_HOST` only if you fully understand the exposure.

## MCP Client Configuration

```json
{
  "mcpServers": {
    "firefox-devtools": {
      "url": "http://127.0.0.1:8090/mcp"
    }
  }
}
```

## Tools

| Tool | Description |
|------|-------------|
| `evaluate_js` | Evaluate a JS expression in the current tab's console (promises and top-level `await` supported) |
| `list_tabs` | List open tabs with index, title, URL and actor id |
| `select_tab` | Target a specific tab (by index) for subsequent calls |
| `navigate` | Navigate the current tab to a URL |
| `get_page_source` | Get the full HTML source of the current page |
| `get_console_messages` | Get cached console messages (errors, logs) |
| `read_page` | Extract readable text content (strips nav, ads, boilerplate) |
| `screenshot` | Screenshot the viewport, full page, or a CSS-selected element |
| `start_capture` | Start capturing network requests |
| `read_capture` | Read captured requests (optionally with response bodies, or errors only) |
| `stop_capture` | Stop network capture and clean up |
| `raw_rdp_command` | Send arbitrary RDP JSON for experimentation |
| `reconnect` | Drop and re-establish the RDP connection |

Most tools accept an optional `tab_index` to target a specific tab, using the index reported by
`list_tabs`.

## Usage Examples

### Page Content & Screenshots

```
# Read article text from a news site you're logged into
> read_page

# Screenshot a specific element
> screenshot(selector=".article-body")

# Full-page screenshot, written straight to disk (keeps the result small)
> screenshot(fullpage=True, save_path="/tmp/page.png")
```

`screenshot` returns a `data:image/...;base64,...` URL by default. Passing `save_path` writes the
decoded image to disk and returns `{path, bytes, width, height}` instead, which avoids pushing
megabytes of base64 through the model context.

### Tab Targeting

```
> list_tabs
> select_tab(index=2)          # all later calls now target tab 2
> evaluate_js("document.title")
```

### Network Capture

Capture API traffic from web apps (SPAs, streaming services, etc.):

```
# Start recording network traffic (before triggering it, for full coverage)
> start_capture

# Navigate and trigger app behaviour
> navigate("https://www.example.com/home")

# See what requests were made
> read_capture

# Include response bodies
> read_capture(include_bodies=True)

# Only failures
> read_capture(only_errors=True)

# Done
> stop_capture
```

### Raw RDP Exploration

```
> raw_rdp_command({"to": "root", "type": "listTabs"})
> raw_rdp_command({"to": "<consoleActor>", "type": "getCachedMessages",
                   "messageTypes": ["PageError", "ConsoleAPI"]})
```

Use `list_tabs` first to discover actor ids, then experiment with `raw_rdp_command`.

## Troubleshooting

| Symptom | Cause / Fix |
|---------|-------------|
| `ConnectionRefusedError` on first call | Firefox isn't running with `--start-debugger-server`. |
| Connect hangs, Firefox shows an "allow connection?" prompt | Set `devtools.debugger.prompt-connection=false`. |
| `screenshot` returns `could not resolve the chrome console actor` | Set `devtools.chrome.enabled=true`. |
| "profile already in use" on launch | Firefox is already running on that profile. Close it, or use a separate `--profile`. |
| Tools fail right after Firefox restarts | Call `reconnect`. |
| `stop_capture` hangs | Was a real bug (see below); fixed — ensure you're on this version. |

## Development

```bash
uv pip install -e ".[dev]"

uv run ruff check .          # lint
uv run ruff format --check . # formatting
uv run mypy firefox_mcp      # types (strict)
uv run pytest                # tests
```

The test suite uses an in-process fake Firefox (`tests/fake_firefox.py`) that reproduces the
protocol's real quirks — nested resource batches, responses lacking a `type` key, interleaved
events, and the watcher actor dying after `unwatchResources` — so most behaviour is covered
without needing a browser.

## Notes & Limitations

- **Legacy actor protocol.** The server uses Firefox's older actor surface (`listTabs`,
  `getTarget`, `navigateTo`, `evaluateJSAsync`). It works on current Firefox (verified against
  156), but Mozilla is gradually retiring parts of it in favour of the WebDriver BiDi-based
  Remote Agent.
- **One connection per server process.** All tabs are reached through a single RDP connection;
  tab targeting is by actor, not by separate sessions.
- **Capture must start before the traffic.** Network capture only sees requests made after
  `start_capture`.
- **Object results are grips.** Evaluating an expression that returns a non-primitive yields an
  RDP object *grip* (a reference with a small preview), not the full serialised value. For a
  plain value, return a JSON string (`JSON.stringify(...)`) or a primitive from your expression.
- **No authentication.** See the security note under *Running*.
