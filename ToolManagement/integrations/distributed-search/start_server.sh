#!/bin/bash

# FastAPI MCP Chat Server Startup Script

set -e

echo "🚀 FastAPI MCP Chat Server"
echo "=========================="
echo ""

# Check environment variables
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo "❌ ERROR: OPENROUTER_API_KEY is required"
    echo ""
    echo "Set it with:"
    echo "  export OPENROUTER_API_KEY='your-openrouter-api-key'"
    echo ""
    echo "Get your key at: https://openrouter.ai/keys"
    exit 1
fi

# Check for MCP configuration
if [ -z "$JINA_API_KEY" ] && [ -z "$MCP_SERVER_URLS" ]; then
    echo "⚠️  WARNING: No MCP servers configured"
    echo ""
    echo "To use MCP tools, set one of:"
    echo "  export JINA_API_KEY='your-jina-api-key'"
    echo "  export MCP_SERVER_URLS='https://mcp.example.com/sse'"
    echo ""
    echo "Server will start but tools will not be available."
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Display configuration
echo "📋 Configuration:"
echo "   Port: 2046"
echo "   OPENROUTER_API_KEY: ✓ Set"
if [ -n "$JINA_API_KEY" ]; then
    echo "   JINA_API_KEY: ✓ Set"
    echo "   JINA_MCP_URL: ${JINA_MCP_URL:-https://mcp.jina.ai/sse}"
fi
if [ -n "$MCP_SERVER_URLS" ]; then
    echo "   MCP_SERVER_URLS: $MCP_SERVER_URLS"
fi
echo ""

# Check Python and dependencies
echo "🔍 Checking Python environment..."

if ! command -v python3 &> /dev/null; then
    echo "❌ ERROR: python3 not found"
    exit 1
fi

# Check required packages
REQUIRED_PACKAGES=("fastapi" "uvicorn" "httpx" "mcp" "pydantic" "rich")
MISSING_PACKAGES=()

for package in "${REQUIRED_PACKAGES[@]}"; do
    if ! python3 -c "import $package" 2>/dev/null; then
        MISSING_PACKAGES+=("$package")
    fi
done

if [ ${#MISSING_PACKAGES[@]} -ne 0 ]; then
    echo "❌ Missing packages: ${MISSING_PACKAGES[*]}"
    echo ""
    echo "Install with:"
    echo "  pip install ${MISSING_PACKAGES[*]}"
    echo ""
    echo "Or install all requirements:"
    echo "  pip install -r requirements.txt"
    exit 1
fi

echo "✓ All dependencies installed"
echo ""

# Start the server
echo "🎯 Starting server..."
echo ""
echo "  🌐 Server: http://localhost:2046"
echo "  📖 API Docs: http://localhost:2046/docs"
echo "  🔧 Tools Endpoint: http://localhost:2046/tools"
echo ""
echo "Press Ctrl+C to stop"
echo ""
echo "─────────────────────────────────────────"
echo ""

# Run the FastAPI server
exec python3 fastapi_mcp_chat.py
