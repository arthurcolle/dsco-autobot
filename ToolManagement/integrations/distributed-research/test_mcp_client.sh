#!/bin/bash

# Test script for MCP client connecting to local server
set -e

echo "🧪 Testing MCP client connection to localhost:2040..."

# Check if server is running
if ! curl -s http://localhost:2040/health > /dev/null 2>&1; then
    echo "❌ Server is not running on port 2040!"
    echo "   Run ./start_server.sh first in another terminal"
    exit 1
fi

echo "✅ Server is running"
echo ""
echo "🔌 Connecting MCP client to http://localhost:2040/mcp/sse"
echo "   Model: anthropic/claude-haiku-4.5"
echo ""

# Run the MCP client with local server
# You can pass a query as the first argument, or it will run in interactive mode
QUERY="${1:-What tools are available?}"

python chat-openrouter-mcp.py \
    --server-url http://localhost:2040/mcp/sse \
    --model anthropic/claude-haiku-4.5 \
    "$QUERY"
