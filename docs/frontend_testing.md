# Frontend Testing Agent

A configuration-driven frontend testing agent using the official Microsoft Playwright MCP server with CDP (Chrome DevTools Protocol) connection for browser state persistence.

## Production Status

✅ **Production Ready** - The frontend tester agent has been successfully tested and is ready for production use.

### What Was Fixed

**Browser State Preservation Issue**: Previously, browser state (cookies, localStorage, sessionStorage) was not persisting between tool executions. This was resolved by implementing a proper CDP (Chrome DevTools Protocol) connection to a persistent Chrome instance.

**Solution**: Instead of launching a new browser for each tool call, the agent now connects to a running Chrome instance via CDP on port 9222. This maintains:
- Browser cookies and sessions
- localStorage and sessionStorage
- Page state and navigation history
- Form data and input values

## Features

- **CDP Connection**: Maintains browser state between tool executions
- **Navigation**: Navigate to URLs, go back/forward, reload
- **Form Interactions**: Fill inputs, select options, check/uncheck boxes
- **Element Actions**: Click, hover, double-click, drag and drop
- **Assertions**: Verify text content, element visibility
- **Screenshots**: Take page screenshots for debugging
- **Headless/Headed Mode**: Configurable browser display mode
- **Persian Language Support**: Tested with Persian/Farsi websites (example.com)

## CDP Connection Overview

The frontend tester agent uses CDP (Chrome DevTools Protocol) to connect to a running Chrome/Chromium instance. This approach provides:

1. **State Persistence**: Browser context is maintained between tool executions
2. **Reusable Session**: Same browser session across multiple test steps
3. **Better Reliability**: No need to relaunch browser for each action
4. **Visual Debugging**: Can see the browser window during execution

### How CDP Works

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Tester Agent                     │
│  (Python/LangGraph with Playwright MCP)                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ CDP Connection (WebSocket)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Chrome with Remote Debugging                    │
│  Port: 9222 (configurable via CDP_PORT env var)             │
│  - Maintains browser state                                  │
│  - Handles all browser operations                           │
│  - Persists cookies, localStorage, sessionStorage           │
└─────────────────────────────────────────────────────────────┘
```

## Test Results

### Persian Site Workflow Test

The frontend tester agent has been successfully tested against a Persian-language website. The complete workflow was verified:

| Test Step | Status | Description |
|-----------|--------|-------------|
| Network Connectivity | ✅ Passed | Chrome CDP connection established on port 9222 |
| Browser Launch | ✅ Passed | Chrome started in headed mode with remote debugging |
| Page Navigation | ✅ Passed | Successfully navigated to https://example.com/ |
| Email Input | ✅ Passed | JavaScript injection filled email field with test data |
| Button Click | ✅ Passed | Successfully clicked 'شروع' (Start) button |
| State Preservation | ✅ Passed | Browser state maintained between tool calls |
| Screenshot Capture | ✅ Passed | Screenshots captured at each step |

### Headed vs Headless Mode Comparison

| Feature | Headed Mode | Headless Mode |
|---------|-------------|---------------|
| **Visibility** | Browser window visible | No visible window |
| **Debugging** | Easy to observe actions | Requires screenshots/logs |
| **Performance** | Slightly slower | Faster execution |
| **State Persistence** | Excellent | Good (may vary) |
| **Use Case** | Development/Debugging | CI/CD Pipelines |
| **Command** | `./scripts/start_chrome_debug.sh` | `./scripts/start_chrome_debug_headless.sh` |
| **Chrome Flags** | Default headed flags | `--headless=new` + GPU disabled |

### Test Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `scripts/start_chrome_debug.sh` | Start Chrome in headed mode | `./scripts/start_chrome_debug.sh` |
| `scripts/start_chrome_debug_headless.sh` | Start Chrome in headless mode | `./scripts/start_chrome_debug_headless.sh` |
| `scripts/test_form.py` | Direct form interaction test | `uv run python scripts/test_form.py` |
| `scripts/test_frontend_tester.py` | Full agent test | `uv run python scripts/test_frontend_tester.py` |

## Setup

```bash
# Install dependencies with uv
uv sync

# Install the official Microsoft Playwright MCP server
npm install -g @playwright/mcp

# Install Playwright browsers
playwright install
```

## Starting Chrome with Remote Debugging

### Option 1: Using the Helper Script (Recommended)

**Headed Mode (Visible Browser):**
```bash
# Start Chrome with CDP on default port 9222
./scripts/start_chrome_debug.sh

# Or with custom port
CDP_PORT=9223 ./scripts/start_chrome_debug.sh
```

**Headless Mode (No Visible Window):**
```bash
# Start Chrome in headless mode
./scripts/start_chrome_debug_headless.sh

# Or with custom port
CDP_PORT=9223 ./scripts/start_chrome_debug_headless.sh
```

### Option 2: Manual Chrome Startup

```bash
# Start Chrome with remote debugging (headed)
google-chrome \
    --remote-debugging-port=9222 \
    --user-data-dir=/tmp/chrome-debug \
    --no-first-run \
    --no-default-browser-check \
    &

# Start Chrome in headless mode
google-chrome \
    --headless=new \
    --remote-debugging-port=9222 \
    --user-data-dir=/tmp/chrome-debug-headless \
    --no-sandbox \
    --disable-dev-shm-usage \
    &

# Verify Chrome is running
curl http://localhost:9222/json/version
```

### Option 3: Environment Variables

```bash
# Set custom port
export CDP_PORT=9222
export USER_DATA_DIR=/tmp/chrome-debug

# Start Chrome (headed)
./scripts/start_chrome_debug.sh

# Or for headless mode
export USER_DATA_DIR=/tmp/chrome-debug-headless
./scripts/start_chrome_debug_headless.sh
```

## Quick Start

### Prerequisites

1. **Start Chrome with CDP** (choose one):
   ```bash
   # Headed mode (recommended for debugging)
   ./scripts/start_chrome_debug.sh
   
   # Headless mode (for CI/CD)
   ./scripts/start_chrome_debug_headless.sh
   ```

2. **Verify Chrome is running**:
   ```bash
   curl http://localhost:9222/json/version
   ```

### Running Tests

```bash
# Run the example.com form test (recommended first test)
uv run python scripts/test_form.py

# Run a full agent test
uv run python scripts/test_frontend_tester.py

# Run in headless mode (requires headless Chrome)
uv run python scripts/test_frontend_tester.py --headless

# Run the simple test runner
uv run python scripts/run_frontend_tester.py "Navigate to https://example.com"

# Run in headed mode (visible browser)
uv run python scripts/run_frontend_tester.py --headed "Test form submission"

# Run a preset test
uv run python scripts/run_frontend_tester.py --preset example
```

### Verifying the Test

After running `test_form.py`, check for the generated screenshots:
- `01-form-initial.png` - Initial page load
- `02-form-email-filled.png` - After email input
- `03-form-after-click.png` - After button click

## Configuration

Edit `agent_config/frontend_tester.yml` to customize:

```yaml
features:
  mcp:
    enabled: true
    servers:
      playwright:
        transport: "stdio"
        command: "npx"
        args: 
          - "-y"
          - "@playwright/mcp@latest"
          - "--cdp-url"
          - "http://localhost:9222"
```

### Environment Variable Overrides

You can override the CDP URL using environment variables:

```bash
# Override CDP URL
export PLAYWRIGHT_CDP_URL=http://localhost:9222
```

### Headless Mode

Set `headless_mode` in the initial state:
- `true`: Run browser without UI
- `false` (default): Show browser window for debugging

## Available Playwright Tools

| Tool | Description |
|------|-------------|
| `browser_navigate` | Navigate to a URL |
| `browser_click` | Click an element |
| `browser_fill_form` | Fill an input field |
| `browser_evaluate` | Execute JavaScript |
| `browser_take_screenshot` | Take screenshot |
| `browser_wait_for` | Wait for element or condition |
| `browser_snapshot` | Get DOM snapshot |
| `browser_hover` | Hover over element |
| `browser_select_option` | Select from dropdown |
| `browser_press_key` | Press keyboard key |

## Example Test Scenarios

### Form Testing

```python
test_scenario = """
1. Navigate to https://example.com
2. Fill email field with "test@example.com"
3. Fill password field with "password123"
4. Click submit button
5. Verify success message appears
"""
```

### Navigation Testing

```python
test_scenario = """
1. Start at https://example.com
2. Click on "More" link
3. Navigate to "About" page
4. Verify page title
5. Go back to home
6. Take screenshot
"""
```

### Persian Site Testing

The Persian site test demonstrates the agent's ability to handle:
- Persian/Farsi text content
- RTL (right-to-left) layouts
- JavaScript-based form interactions
- Dynamic content loading

```python
test_scenario = """
Navigate to https://example.com and:
1. Wait for page to load
2. Find the email input field
3. Fill with test email
4. Find and click 'شروع' (Start) button
5. Take screenshot to verify action
"""
```

### Complete Workflow Test

```bash
# Step 1: Start Chrome in headed mode
./scripts/start_chrome_debug.sh

# Step 2: Run the example form test
uv run python scripts/test_form.py

# Step 3: Check the generated screenshots
ls -la *.png

# Step 4: Stop Chrome when done
pkill -f 'chrome.*remote-debugging-port=9222'
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Chrome Not Starting

**Symptom**: Chrome fails to start or crashes immediately

**Solution**:
```bash
# Check if Chrome is installed
which google-chrome
which chromium
which chromium-browser

# If Chrome is not found, install it
# Ubuntu/Debian:
sudo apt install chromium

# Fedora/RHEL:
sudo dnf install chromium

# macOS:
brew install chromium
```

#### 2. Port Already in Use

**Symptom**: `Error: Port 9222 already in use`

**Solution**:
```bash
# Check what's using port 9222
lsof -i :9222

# Kill the process if needed
kill <PID>

# Or use a different port
export CDP_PORT=9223
./scripts/start_chrome_debug.sh
```

#### 3. CDP URL Not Reachable

**Symptom**: MCP server cannot connect to Chrome

**Solution**:
```bash
# Verify Chrome is responding
curl http://localhost:9222/json/version

# Check Chrome logs (for headless mode)
tail -f /tmp/chrome-headless.log

# Restart Chrome
pkill -f 'chrome.*remote-debugging-port'
./scripts/start_chrome_debug.sh
```

#### 4. State Not Persisting Between Tool Calls

**Symptom**: Cookies, localStorage, or form data is lost between actions

**Solution**:
1. **Verify CDP Connection**: Ensure Chrome is running on the correct port
2. **Use Single Browser Instance**: Don't restart Chrome between test steps
3. **Use Headed Mode**: Headless mode may have reduced state persistence
4. **Check User Data Dir**: Ensure the same `--user-data-dir` is used

```bash
# Verify CDP connection
curl http://localhost:9222/json/version

# Should return JSON with webSocketDebuggerUrl
```

#### 5. Element Not Found

**Symptom**: Tool cannot locate the target element

**Solution**:
- Use more specific CSS selectors
- Add wait steps before interaction
- Take screenshot to see current page state
- Use `browser_wait_for` to wait for elements
- Check if element is in iframe or shadow DOM

#### 6. Timeout Errors

**Symptom**: Operations timeout before completion

**Solution**:
- Increase timeout in configuration
- Add wait steps after navigation
- Check network connectivity
- Verify Chrome is running: `curl http://localhost:9222/json/version`

#### 7. Headless Mode Issues

**Symptom**: Tests work in headed mode but fail in headless mode

**Solution**:
- Use `--headless=new` (new headless mode, Chrome 120+)
- Add `--disable-gpu` flag
- Add `--virtual-time-budget` for proper rendering
- Consider using headed mode for complex interactions

### Debugging Tips

1. **Use Headed Mode**: Set `headless_mode: false` when debugging
2. **Take Screenshots**: Capture state at each step
3. **Check Logs**: Review Chrome logs (`/tmp/chrome-headless.log`)
4. **Verify CDP**: Confirm connection with `curl http://localhost:9222/json/version`
5. **Use JavaScript**: For complex interactions, use `browser_evaluate` with custom JS

### Getting Help

If you encounter issues not covered here:

1. Check the [GitHub Issues](https://github.com/your-org/general-agent-framework/issues)
2. Review Chrome DevTools Protocol documentation
3. Check Playwright MCP server documentation
4. Enable verbose logging:
   ```bash
   export DEBUG=1
   uv run python scripts/test_form.py
   ```

### CDP Connection Issues

#### Chrome Not Starting

```bash
# Check if Chrome is installed
which google-chrome
which chromium
which chromium-browser

# If Chrome is not found, install it
# Ubuntu/Debian:
sudo apt install chromium

# Fedora/RHEL:
sudo dnf install chromium

# macOS:
brew install chromium
```

#### Port Already in Use

```bash
# Check what's using port 9222
lsof -i :9222

# Kill the process if needed
kill <PID>

# Or use a different port
export CDP_PORT=9223
./scripts/start_chrome_debug.sh
```

#### CDP URL Not Reachable

```bash
# Verify Chrome is responding
curl http://localhost:9222/json/version

# Check Chrome logs
# Look for errors in terminal where Chrome is running

# Restart Chrome
pkill -f 'chrome.*remote-debugging-port'
./scripts/start_chrome_debug.sh
```

### MCP Server Issues

#### MCP Server Not Starting

```bash
# Install the official Microsoft Playwright MCP server globally
npm install -g @playwright/mcp

# Or run with npx
npx -y @playwright/mcp@latest --cdp-url http://localhost:9222
```

#### Tools Not Available

```bash
# Verify MCP configuration in agent_config/frontend_tester.yml
# Check that the CDP URL matches your Chrome debugging port

# Restart the agent after starting Chrome
uv run python scripts/test_frontend_tester.py
```

### Browser Issues

#### Browser Not Launching

- Ensure Playwright is installed: `playwright install`
- Check browser permissions
- Try headed mode for debugging: `headless_mode: false`

#### Element Not Found

- Use more specific CSS selectors
- Add wait steps before interaction
- Take screenshot to see current page state
- Use `browser_wait_for` to wait for elements

#### Timeout Errors

- Increase timeout in configuration
- Add wait steps after navigation
- Check network connectivity
- Verify Chrome is running: `curl http://localhost:9222/json/version`

### State Not Persisting

If browser state (cookies, localStorage) is not persisting between tool calls:

1. **Verify CDP Connection**: Check that Chrome is running on the correct port
2. **Check Single Browser**: Ensure only one Chrome instance is running with CDP
3. **Avoid Headless Mode**: Use headed mode for better state persistence
4. **Check User Data Dir**: Ensure the same `--user-data-dir` is used

```bash
# Verify CDP connection
curl http://localhost:9222/json/version

# Should return JSON with webSocketDebuggerUrl
```

## Agent State

The agent maintains the following state:

| Field | Type | Description |
|-------|------|-------------|
| `user_input` | str | Test scenario description |
| `test_plan` | dict | Step-by-step test plan |
| `next_action` | dict | Next action to execute |
| `tool_calls` | list | List of tool calls to execute |
| `has_tool_calls` | bool | Whether there are tool calls (routing flag) |
| `test_complete` | bool | Whether test is complete (routing flag) |
| `tool_result` | dict | Result from tool execution |
| `current_url` | str | Current page URL |
| `test_status` | str | planning/executing/completed/failed |
| `last_dom_context` | str | Text-based DOM snapshot |
| `headless_mode` | bool | Browser display mode |
| `test_results` | list | Test results and assertions |
| `error_message` | str | Error message if failed |
| `messages` | list | Chat history for multi-turn interactions |

## File Structure

```
agent_config/
  frontend_tester.yml     # Agent configuration with CDP support

prompts/frontend_tester/
  planner.jinja2          # Test planning template
  navigator.jinja2        # Action decision template

scripts/
  start_chrome_debug.sh   # Chrome startup script with CDP
  test_frontend_tester.py # Test script with Chrome management
  run_frontend_tester.py  # Simple test runner

docs/
  frontend_testing.md     # This documentation
```

## Best Practices

1. **Use CDP for State Persistence**: Always connect to an existing Chrome instance via CDP
2. **Reuse Browser Session**: Don't restart Chrome between test steps
3. **Use Headed Mode for Debugging**: Set `headless_mode: false` when debugging
4. **Take Screenshots**: Use `browser_take_screenshot` to verify actions
5. **Wait for Elements**: Use `browser_wait_for` to handle dynamic content
6. **Clean Up**: Stop Chrome when done: `pkill -f 'chrome.*remote-debugging-port'`
7. **Use Headless for CI/CD**: Use headless mode for automated pipelines
8. **Test with Real Sites**: Use example.com as a reference implementation

## Related Documentation

- [Main README](../README.md)
- [Authentication](../docs/authentication.md)
- [Architecture](../.kilocode/rules/memory-bank/architecture.md)
- [Playwright MCP Server](https://github.com/microsoft/playwright-mcp)
- [Chrome DevTools Protocol](https://chromedevtools.github.io/devtools-protocol/)




# Start Chrome with CDP (headed mode for visual debugging)
./scripts/start_chrome_debug.sh

# Or headless mode for CI/CD
./scripts/start_chrome_debug_headless.sh

# Run the test
uv run python scripts/test_form.py