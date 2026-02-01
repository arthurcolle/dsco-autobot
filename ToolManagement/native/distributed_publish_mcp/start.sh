#!/bin/bash
#
# Start Distributed Publish MCP - Native Server
#
# Usage:
#   ./start.sh                    # Start with defaults
#   PORT=4013 ./start.sh          # Custom port
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Defaults
export PORT="${PORT:-4003}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export PYTHONPATH="${PROJECT_ROOT}:${PROJECT_ROOT}/src:$PYTHONPATH"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Starting Distributed Publish MCP                    ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Port: $PORT"
echo "║  Registry: ${REGISTRY_URL:-none}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

cd "$PROJECT_ROOT"

# Run server
python -m native.distributed_publish_mcp.server
