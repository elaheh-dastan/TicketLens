# MCP Node Tooling

This folder contains Node.js tooling to run MCP (Model Context Protocol) servers that integrate with the Python agent framework. These servers enable browser automation, filesystem access, and other capabilities.

## Quick Start

### 1. Install Node.js (first time only)

From the repo root:

```bash
bash scripts/setup_node.sh
```

This will:
- Install `nvm` (Node Version Manager) if needed
- Install Node.js LTS via `nvm`
- Run `npm install` to fetch MCP server packages

**Alternative**: Install Node directly via your system package manager (Ubuntu/Debian):
```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
cd mcp/node && npm install
```

### 2. Start an MCP Server

**Chrome DevTools MCP** (recommended for browser automation):
```bash
bash scripts/start_chrome_mcp.sh
```

Or manually:
```bash
cd mcp/node
npm run start-chrome-mcp
```

**Playwright MCP** (alternative browser automation):
```bash
cd mcp/node
npm run start-playwright-mcp
```

## Supported MCP Servers

| Server | Purpose | Installed | Start Command |
|--------|---------|-----------|----------------|
| `chrome-devtools-mcp` | Browser automation via Chrome DevTools Protocol | Yes | `npm run start-chrome-mcp` |
| `playwright-mcp` | Browser automation via Playwright | Yes | `npm run start-playwright-mcp` |

## Configuration

After starting an MCP server, register it with the Python agent framework in `.env` or code:

```env
MCP__ENABLED=true
MCP__SERVERS=["mcp://chrome-devtools"]
MCP__CHROME__DEVTOOLS__ENABLED=true
MCP__CHROME__DEVTOOLS__REMOTE__DEBUGGING__PORT=9222
```

Then in Python:

```python
from src.mcp import get_mcp_manager

mgr = get_mcp_manager()
tools = await mgr.list_tools("chrome-devtools")
result = await mgr.call_tool("chrome-devtools", "navigate", {"url": "https://example.com"})
```

## Troubleshooting

**"nvm: command not found"**
- Run `bash scripts/setup_node.sh` again, or manually source nvm:
  ```bash
  export NVM_DIR="$HOME/.nvm"
  source "$NVM_DIR/nvm.sh"
  ```

**"npm: command not found"**
- Ensure Node is installed: `node --version`
- If not, run `bash scripts/setup_node.sh`

**MCP server won't start**
- Check the server package's README for exact startup commands
- Verify dependencies installed: `npm list`
- Try running directly: `npx chrome-devtools-mcp`

## Docker Alternative

If you prefer not to install Node locally, run MCP servers in Docker:

```bash
docker run -p 9222:9222 your-mcp-server-image
```

Then connect via:
```python
from src.mcp.clients import ChromeDevToolsMCPClient

client = ChromeDevToolsMCPClient("ws://localhost:9222/mcp")
```
