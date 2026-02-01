#!/bin/bash
# Start the Jina MCP Service (registers with central registry)

set -e
cd "$(dirname "$0")"

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
mkdir -p data

# Configuration
export PORT="${PORT:-8001}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export SERVICE_HOST="${SERVICE_HOST:-localhost}"
export JINA_API_KEY="${JINA_API_KEY:-}"

echo "Starting Jina MCP Service..."
echo "   Port: $PORT"
echo "   Registry: $REGISTRY_URL"
echo ""

python3 -m deploy.jina_service
