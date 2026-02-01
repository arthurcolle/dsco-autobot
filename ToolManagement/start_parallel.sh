#!/bin/bash
# Start the Parallel AI MCP Service (registers with central registry)

set -e
cd "$(dirname "$0")"

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
mkdir -p data

# Configuration
export PORT="${PORT:-8002}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export SERVICE_HOST="${SERVICE_HOST:-localhost}"
export PARALLEL_API_KEY="${PARALLEL_API_KEY:-}"

echo "Starting Parallel AI MCP Service..."
echo "   Port: $PORT"
echo "   Registry: $REGISTRY_URL"
echo ""

python3 -m deploy.parallel_service
