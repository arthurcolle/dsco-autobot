#!/bin/bash
# Setup Modal secrets for Jina MCP Server

set -e

echo "Setting up Modal secrets for Jina MCP Server..."

# Check if JINA_API_KEY is set
if [ -z "$JINA_API_KEY" ]; then
    echo "Enter your JINA_API_KEY:"
    read -s JINA_API_KEY
fi

# Generate a random API token for OAuth
API_TOKEN=$(openssl rand -hex 32)

echo ""
echo "Creating Modal secret 'jina-mcp-secrets'..."

modal secret create jina-mcp-secrets \
    JINA_API_KEY="$JINA_API_KEY" \
    API_TOKENS="$API_TOKEN" \
    JINA_MCP_URL="https://mcp.jina.ai/sse"

echo ""
echo "Secret created!"
echo ""
echo "Your API token for authentication:"
echo "$API_TOKEN"
echo ""
echo "Save this token - you'll need it to authenticate requests."
echo ""
echo "Deploy with:"
echo "  modal deploy modal_server.py"
echo ""
echo "Test locally with:"
echo "  modal serve modal_server.py"
