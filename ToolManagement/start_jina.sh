#!/bin/bash
# Start the Jina MCP Service (registers with central registry)

set -e
cd "$(dirname "$0")"

# Load gitignored .env so JINA_API_KEY and friends are available
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
mkdir -p data

# Prefer the project venv; fall back to system python3
PYTHON="${PYTHON:-$(pwd)/.venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON=python3

# Configuration
export PORT="${PORT:-8001}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export SERVICE_HOST="${SERVICE_HOST:-localhost}"
export JINA_API_KEY="${JINA_API_KEY:-}"

if [ -z "$JINA_API_KEY" ]; then
    echo "WARNING: JINA_API_KEY is empty — Jina API calls will fail (set it in .env)"
fi

echo "Starting Jina MCP Service..."
echo "   Port: $PORT"
echo "   Registry: $REGISTRY_URL"
echo "   Python: $PYTHON"
echo ""

exec "$PYTHON" -m deploy.jina_service
