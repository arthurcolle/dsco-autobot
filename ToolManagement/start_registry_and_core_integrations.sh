#!/bin/bash
# Start the Tool Management Registry + all core integrations from a clean checkout.
#
# This is a superset of start_registry.sh: it provisions a Python 3.12 virtualenv,
# installs ALL core dependencies from requirements.txt (duckdb, trafilatura,
# sse-starlette, crypto libs, etc.), optionally installs the heavy ML extras, and
# then boots the registry with the native MCP servers enabled.
#
# Native MCPs (auto-started by the app):
#   - distributed-search-mcp   (port 4001)
#   - distributed-research-mcp (port 4002)
#   - distributed-publish-mcp  (port 4003)
#   - distributed-memory-mcp   (port 4004)
#
# Usage:
#   ./start_registry_and_core_integrations.sh              # core deps + native MCPs
#   INSTALL_ML=1 ./start_registry_and_core_integrations.sh # also install torch/transformers/etc.
#   FORCE_REINSTALL=1 ./start_registry_and_core_integrations.sh  # re-run dependency install
#   AUTO_START_NATIVE_MCPS=0 ./start_registry_and_core_integrations.sh  # registry only

set -euo pipefail

cd "$(dirname "$0")"

# Set PYTHONPATH to include src and native directories
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src:$(pwd)"

# Create data directories
mkdir -p data data/memory

# =============================================================================
# CONFIGURATION
# =============================================================================

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
export VIDEO_QA_MCP_PORT="${VIDEO_QA_MCP_PORT:-4005}"
export DOCUMENT_QA_MCP_PORT="${DOCUMENT_QA_MCP_PORT:-4006}"
export MEMORY_DATA_DIR="${MEMORY_DATA_DIR:-./data/memory}"

# External MCP Backend URLs (optional - native MCPs take priority)
export PARALLEL_AI_URL="${PARALLEL_AI_URL:-http://localhost:2040}"
export JINA_MCP_URL="${JINA_MCP_URL:-http://localhost:8787}"

# API Keys (optional)
export JINA_API_KEY="${JINA_API_KEY:-}"
export PARALLEL_API_KEY="${PARALLEL_API_KEY:-}"

# Mask secrets in the banner (show only that they are configured)
mask() { [ -n "${1:-}" ] && echo "configured" || echo "not set"; }

# =============================================================================
# DISPLAY CONFIGURATION
# =============================================================================

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║     Tool Management Registry + Core Integrations                ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Registry: http://$HOST:$PORT"
echo "║  Docs: http://localhost:$PORT/docs"
echo "╠══════════════════════════════════════════════════════════════════╣"
if [[ "$AUTO_START_NATIVE_MCPS" == "1" ]]; then
echo "║  Native MCPs (auto-started):"
echo "║    • distributed-search-mcp   → http://localhost:$SEARCH_MCP_PORT"
echo "║    • distributed-research-mcp → http://localhost:$RESEARCH_MCP_PORT"
echo "║    • distributed-publish-mcp  → http://localhost:$PUBLISH_MCP_PORT"
echo "║    • distributed-memory-mcp   → http://localhost:$MEMORY_MCP_PORT"
echo "║    • distributed-video-qa     → http://localhost:$VIDEO_QA_MCP_PORT"
echo "║    • distributed-document-qa  → http://localhost:$DOCUMENT_QA_MCP_PORT"
else
echo "║  Native MCPs: DISABLED"
fi
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  External Backends (if available):"
echo "║    • Parallel AI: $PARALLEL_AI_URL"
echo "║    • Jina MCP: $JINA_MCP_URL"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  API Keys:"
echo "║    • JINA_API_KEY: $(mask "$JINA_API_KEY")"
echo "║    • PARALLEL_API_KEY: $(mask "$PARALLEL_API_KEY")"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# =============================================================================
# DEPENDENCY PROVISIONING
# =============================================================================

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: 'uv' is required but not installed. See https://docs.astral.sh/uv/" >&2
    exit 1
fi

VENV_DIR="${VENV_DIR:-.venv}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtualenv at $VENV_DIR (Python $PYTHON_VERSION)..."
    uv venv --python "$PYTHON_VERSION" "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# Install core deps if a sentinel import is missing, or when forced.
if [ "${FORCE_REINSTALL:-0}" == "1" ] || ! python3 -c "import fastapi, duckdb, trafilatura, sse_starlette" 2>/dev/null; then
    echo "Installing core dependencies from requirements.txt..."
    uv pip install -r requirements.txt
fi

# Optional heavy ML stack.
if [ "${INSTALL_ML:-0}" == "1" ]; then
    if ! python3 -c "import torch" 2>/dev/null; then
        echo "Installing ML extras (torch, transformers, peft, sentence-transformers, spacy, chromadb)..."
        uv pip install \
            "torch>=2.1.0" \
            "transformers>=4.36.0" \
            "peft>=0.7.0" \
            "accelerate>=0.25.0" \
            "sentence-transformers>=3.0.0" \
            "spacy>=3.7.0" \
            "chromadb>=0.5.0"
    fi
fi

# =============================================================================
# START SERVER
# =============================================================================

echo "Starting server..."
echo ""

RELOAD_FLAG=()
[[ "$RELOAD" == "1" ]] && RELOAD_FLAG=(--reload)

exec python3 -m uvicorn tool_management.app:app \
    --host "$HOST" \
    --port "$PORT" \
    ${RELOAD_FLAG[@]+"${RELOAD_FLAG[@]}"}
