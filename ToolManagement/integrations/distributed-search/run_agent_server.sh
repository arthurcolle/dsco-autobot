#!/bin/bash
# Agent Server Startup Script
# Convenient wrapper to launch the agent server with proper configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    Agent Server - Intelligent /v1/interactions Endpoint${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Check for required API keys
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo -e "${RED}ERROR: OPENROUTER_API_KEY not set${NC}"
    echo "Please set it in your environment:"
    echo "  export OPENROUTER_API_KEY=your_key_here"
    exit 1
fi

if [ -z "$JINA_API_KEY" ]; then
    echo -e "${RED}ERROR: JINA_API_KEY not set${NC}"
    echo "Please set it in your environment:"
    echo "  export JINA_API_KEY=your_key_here"
    exit 1
fi

echo -e "${GREEN}✓${NC} API keys configured"

# Set defaults if not already set
export MAX_ITERATIONS=${MAX_ITERATIONS:-15}
export CONTEXT_COMPRESSION_THRESHOLD=${CONTEXT_COMPRESSION_THRESHOLD:-50}
export SESSION_TTL_SECONDS=${SESSION_TTL_SECONDS:-7200}
export DEFAULT_MODEL=${DEFAULT_MODEL:-anthropic/claude-sonnet-4-5}

echo -e "${GREEN}✓${NC} Configuration loaded"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: python3 not found${NC}"
    echo "Please install Python 3.8 or later"
    exit 1
fi

# Check if required packages are installed
echo "Checking dependencies..."
python3 -c "import fastapi, httpx, mcp, uvicorn" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠${NC}  Some dependencies missing. Installing..."
    pip install fastapi httpx mcp uvicorn python-dotenv 2>&1 | grep -v "Requirement already satisfied" || true
    echo -e "${GREEN}✓${NC} Dependencies installed"
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Starting Agent Server...${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Run the server
python3 agent_server.py
