#!/bin/bash
# Start the Tool Management Registry Server with Native MCPs
#
# This script starts the central registry and automatically initializes
# all three native MCP servers:
#   - distributed-search-mcp (port 4001)
#   - distributed-research-mcp (port 4002)
#   - distributed-publish-mcp (port 4003)
#
# Usage:
#   ./start_registry.sh                    # Start with native MCPs
#   AUTO_START_NATIVE_MCPS=0 ./start_registry.sh  # Without native MCPs
#

set -e

cd "$(dirname "$0")"

# Set PYTHONPATH to include src and native directories
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src:$(pwd)"

# Create data directory
mkdir -p data

# =============================================================================
# CONFIGURATION
# =============================================================================

# Core settings
export DATA_DIR="${DATA_DIR:-./data}"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-2016}"
export DEBUG="${DEBUG:-1}"
export RELOAD="${RELOAD:-1}"
export AUTO_START_QUEUE="${AUTO_START_QUEUE:-1}"

# Native MCP settings
export AUTO_START_NATIVE_MCPS="${AUTO_START_NATIVE_MCPS:-1}"
export SEARCH_MCP_PORT="${SEARCH_MCP_PORT:-4001}"
export RESEARCH_MCP_PORT="${RESEARCH_MCP_PORT:-4002}"
export PUBLISH_MCP_PORT="${PUBLISH_MCP_PORT:-4003}"
export MEMORY_MCP_PORT="${MEMORY_MCP_PORT:-4004}"
export MEMORY_DATA_DIR="${MEMORY_DATA_DIR:-./data/memory}"

# External MCP Backend URLs (optional - native MCPs take priority)
export PARALLEL_AI_URL="${PARALLEL_AI_URL:-http://localhost:2040}"
export JINA_MCP_URL="${JINA_MCP_URL:-http://localhost:8787}"

# API Keys (optional)
export JINA_API_KEY="${JINA_API_KEY:-}"
export PARALLEL_API_KEY="${PARALLEL_API_KEY:-}"

# =============================================================================
# DISPLAY CONFIGURATION
# =============================================================================

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║         Tool Management Registry with Native MCPs                ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Registry: http://$HOST:$PORT                                    "
echo "║  Docs: http://localhost:$PORT/docs                               "
echo "╠══════════════════════════════════════════════════════════════════╣"
if [[ "$AUTO_START_NATIVE_MCPS" == "1" ]]; then
echo "║  Native MCPs (auto-started):                                     "
echo "║    • distributed-search-mcp   → http://localhost:$SEARCH_MCP_PORT"
echo "║    • distributed-research-mcp → http://localhost:$RESEARCH_MCP_PORT"
echo "║    • distributed-publish-mcp  → http://localhost:$PUBLISH_MCP_PORT"
echo "║    • distributed-memory-mcp   → http://localhost:$MEMORY_MCP_PORT"
else
echo "║  Native MCPs: DISABLED                                           "
fi
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  External Backends (if available):                               "
echo "║    • Parallel AI: $PARALLEL_AI_URL"
echo "║    • Jina MCP: $JINA_MCP_URL"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  API Keys:                                                       "
echo "║    • JINA_API_KEY: ${JINA_API_KEY:+configured}${JINA_API_KEY:-not set}"
echo "║    • PARALLEL_API_KEY: ${PARALLEL_API_KEY:+configured}${PARALLEL_API_KEY:-not set}"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Endpoints:                                                      "
echo "║    • Health: http://localhost:$PORT/health                       "
echo "║    • Tools: http://localhost:$PORT/api/v1/tools                  "
echo "║    • Native MCPs: http://localhost:$PORT/api/v1/native           "
echo "║    • Admin: http://localhost:$PORT/api/v1/admin                  "
echo "║    • Gateway: http://localhost:$PORT/api/v1/gateway              "
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# =============================================================================
# DEPENDENCY CHECK
# =============================================================================

if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -e . || pip install fastapi uvicorn httpx pydantic websockets
fi

# =============================================================================
# START SERVER
# =============================================================================

echo "Starting server..."
echo ""

python3 -m uvicorn tool_management.app:app \
    --host "$HOST" \
    --port "$PORT" \
    --reload
