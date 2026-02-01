#!/bin/bash
#
# Start Native MCPs - All Three Native MCP Implementations
#
# This script starts all native MCP servers:
#   - distributed-search-mcp (port 4001) - Web search, content extraction
#   - distributed-research-mcp (port 4002) - Deep research, task execution
#   - distributed-publish-mcp (port 4003) - Webhooks, events, syndication
#
# Usage:
#   ./start_native_mcps.sh              # Start all servers
#   ./start_native_mcps.sh --test       # Start and run connectivity test
#   ./start_native_mcps.sh --search     # Start only search MCP
#   ./start_native_mcps.sh --research   # Start only research MCP
#   ./start_native_mcps.sh --publish    # Start only publish MCP
#
# Environment:
#   JINA_API_KEY        - Jina AI API key (optional)
#   PARALLEL_API_KEY    - Parallel AI API key (optional)
#   REGISTRY_URL        - Central registry URL (optional)
#   SEARCH_MCP_PORT     - Search MCP port (default: 4001)
#   RESEARCH_MCP_PORT   - Research MCP port (default: 4002)
#   PUBLISH_MCP_PORT    - Publish MCP port (default: 4003)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
export SEARCH_MCP_PORT="${SEARCH_MCP_PORT:-4001}"
export RESEARCH_MCP_PORT="${RESEARCH_MCP_PORT:-4002}"
export PUBLISH_MCP_PORT="${PUBLISH_MCP_PORT:-4003}"
export PYTHONPATH="${SCRIPT_DIR}:${SCRIPT_DIR}/src:$PYTHONPATH"

# Parse arguments
LAUNCHER_ARGS=""
if [[ "$1" == "--test" ]]; then
    LAUNCHER_ARGS="--test --wait-for-ready"
elif [[ "$1" == "--search" ]]; then
    LAUNCHER_ARGS="--search-only"
elif [[ "$1" == "--research" ]]; then
    LAUNCHER_ARGS="--research-only"
elif [[ "$1" == "--publish" ]]; then
    LAUNCHER_ARGS="--publish-only"
fi

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║              Starting Native MCPs                                ║"
echo "║           All Three Native MCP Implementations                   ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Search MCP:   http://localhost:${SEARCH_MCP_PORT}                              ║"
echo "║  Research MCP: http://localhost:${RESEARCH_MCP_PORT}                              ║"
echo "║  Publish MCP:  http://localhost:${PUBLISH_MCP_PORT}                              ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

cd "$SCRIPT_DIR"

# Run launcher
python -m native.launcher $LAUNCHER_ARGS
