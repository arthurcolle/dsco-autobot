#!/bin/bash
#
# Start Distributed Search MCP - Native Server
#
# Usage:
#   ./start.sh                    # Start with defaults
#   PORT=8111 ./start.sh          # Custom port
#   JINA_API_KEY=xxx ./start.sh   # With API key
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Defaults
export PORT="${PORT:-4001}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export PYTHONPATH="${PROJECT_ROOT}:${PROJECT_ROOT}/src:$PYTHONPATH"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Starting Distributed Search MCP                     ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Port: $PORT"
echo "║  Registry: ${REGISTRY_URL:-none}"
echo "║  API Key: ${JINA_API_KEY:+configured}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

cd "$PROJECT_ROOT"

# Run server
python -m native.distributed_search_mcp.server
