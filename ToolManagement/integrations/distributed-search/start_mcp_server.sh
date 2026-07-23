#!/bin/bash

# Startup script for FastAPI MCP Chat Server

echo "🚀 Starting FastAPI MCP Chat Server"
echo "===================================="
echo ""

# Check for required environment variables
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo "⚠️  WARNING: OPENROUTER_API_KEY not set"
    echo "   Set it with: export OPENROUTER_API_KEY='your-key'"
    echo ""
fi

if [ -z "$JINA_API_KEY" ] && [ -z "$MCP_SERVER_URLS" ]; then
    echo "⚠️  WARNING: No MCP servers configured"
    echo "   Set JINA_API_KEY or MCP_SERVER_URLS"
    echo ""
    echo "   Examples:"
    echo "   export JINA_API_KEY='your-jina-key'"
    echo "   export MCP_SERVER_URLS='https://mcp.example.com/sse'"
    echo ""
fi

# Display configuration
echo "📋 Configuration:"
echo "   OPENROUTER_API_KEY: ${OPENROUTER_API_KEY:+✓ Set}"
echo "   JINA_API_KEY: ${JINA_API_KEY:+✓ Set}"
echo "   JINA_MCP_URL: ${JINA_MCP_URL:-https://mcp.jina.ai/sse}"
echo "   MCP_SERVER_URLS: ${MCP_SERVER_URLS:-(none)}"
echo ""

# Check if Python packages are installed
echo "🔍 Checking dependencies..."
python3 -c "import fastapi" 2>/dev/null || {
    echo "❌ fastapi not found. Install with:"
    echo "   pip install fastapi uvicorn httpx mcp rich pydantic"
    exit 1
}

python3 -c "import mcp" 2>/dev/null || {
    echo "❌ mcp not found. Install with:"
    echo "   pip install mcp"
    exit 1
}

echo "✓ Dependencies OK"
echo ""

# Start the server
echo "🎯 Starting server on http://localhost:2046"
echo "📖 API docs will be at http://localhost:2046/docs"
echo ""
echo "Press Ctrl+C to stop"
echo ""

python3 fastapi_mcp_chat.py
