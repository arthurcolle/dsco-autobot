#!/bin/bash
# Start the Codex Apps MCP server (stdio bridge to ToolManagement API)
set -e
cd "$(dirname "$0")"
export PYTHONPATH=src:${PYTHONPATH:-}
export TOOLMANAGEMENT_API_URL=${TOOLMANAGEMENT_API_URL:-https://tools.distributed.systems}
exec .venv/bin/python -m native.codex_apps_mcp.server
