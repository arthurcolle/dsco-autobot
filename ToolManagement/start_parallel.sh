#!/bin/bash
# Start the Parallel AI MCP Service (registers with central registry)

set -e
cd "$(dirname "$0")"

# Load gitignored .env so PARALLEL_API_KEY and friends are available
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
export PORT="${PORT:-8002}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:2016}"
export SERVICE_HOST="${SERVICE_HOST:-localhost}"
export PARALLEL_API_KEY="${PARALLEL_API_KEY:-}"

if [ -z "$PARALLEL_API_KEY" ]; then
    echo "WARNING: PARALLEL_API_KEY is empty — Parallel API calls will fail (set it in .env)"
fi

echo "Starting Parallel AI MCP Service..."
echo "   Port: $PORT"
echo "   Registry: $REGISTRY_URL"
echo "   Python: $PYTHON"
echo ""

exec "$PYTHON" -m deploy.parallel_service
