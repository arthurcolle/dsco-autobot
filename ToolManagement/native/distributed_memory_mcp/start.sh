#!/bin/bash
# Start the Distributed Memory MCP server

set -e
cd "$(dirname "$0")/../.."

export PYTHONPATH="${PYTHONPATH:-}:$(pwd):$(pwd)/src"
export MEMORY_MCP_PORT="${MEMORY_MCP_PORT:-4004}"
export MEMORY_DATA_DIR="${MEMORY_DATA_DIR:-data/memory}"

echo "Starting Distributed Memory MCP on port $MEMORY_MCP_PORT..."
python3 -m native.distributed_memory_mcp.server --port "$MEMORY_MCP_PORT" --data-dir "$MEMORY_DATA_DIR"
