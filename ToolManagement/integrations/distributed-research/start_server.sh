#!/bin/bash

# Startup script for parallel_websearch_server
set -e

echo "🚀 Starting parallel_websearch_server..."

# Check if port 2040 is already in use
if lsof -i:2040 > /dev/null 2>&1; then
    echo "⚠️  Port 2040 is already in use. Killing existing process..."
    lsof -t -i:2040 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

# Check if .env file exists, if not use .env.example
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Using .env.example as reference."
    echo "   Create a .env file with your API keys if needed."
fi

# Set reload mode for development (single worker)
export RELOAD=true

# Start the server
echo "✅ Starting server on http://localhost:2040"
echo "   MCP HTTP endpoint: http://localhost:2040/mcp     (Streamable HTTP)"
echo "   MCP SSE endpoint:  http://localhost:2040/mcp/sse (POST replies to /mcp/sse/messages/)"
echo "   Press Ctrl+C to stop"
echo ""

uv run python -m parallel_websearch_server.main
