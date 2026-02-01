#!/bin/bash
#
# Start Distributed Research MCP - Native Server (Parallel AI Integration)
#
# Usage:
#   ./start.sh                       # Start with defaults (v1)
#   ./start.sh --v2                  # Start v2 (NativeMCPBase)
#   PORT=4002 ./start.sh             # Custom port
#   PARALLEL_API_KEY=xxx ./start.sh  # With Parallel AI API key
#
# Environment:
#   PORT                - Server port (default: 4002)
#   REGISTRY_URL        - Central registry URL (default: http://localhost:2016)
#   PARALLEL_API_KEY    - Parallel AI API key (required for full functionality)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check for v2 flag
USE_V2=false
if [[ "$1" == "--v2" ]] || [[ "$1" == "-v2" ]]; then
    USE_V2=true
fi

# Defaults
export PORT="${PORT:-4002}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export PYTHONPATH="${PROJECT_ROOT}:${PROJECT_ROOT}/src:$PYTHONPATH"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       Distributed Research MCP - Parallel AI Integration     ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Port: $PORT"
echo "║  Registry: ${REGISTRY_URL:-none}"
echo "║  Parallel API: ${PARALLEL_API_KEY:+✓ configured}"
if [[ "$USE_V2" == true ]]; then
echo "║  Version: v2 (NativeMCPBase)"
else
echo "║  Version: v1 (standalone)"
fi
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Parallel AI Endpoints:"
echo "║  • /v1beta/search       - Deep web search"
echo "║  • /v1beta/extract      - Structured extraction"
echo "║  • /v1/tasks/runs       - Autonomous task execution"
echo "║  • /v1beta/tasks/groups - Parallel batch tasks"
echo "║  • /v1beta/findall/runs - Entity prospecting"
echo "║  • /v1alpha/monitors    - Web monitoring"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

cd "$PROJECT_ROOT"

# Run appropriate server version
if [[ "$USE_V2" == true ]]; then
    python -m native.distributed_research_mcp.server_v2 --port "$PORT"
else
    python -m native.distributed_research_mcp.server
fi
