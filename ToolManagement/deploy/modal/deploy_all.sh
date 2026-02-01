#!/bin/bash
# Deploy all native MCP servers to Modal
# They will self-register with tools.distributed.systems on startup

set -e

echo "Deploying Native MCP Servers to Modal..."
echo ""

# Deploy main registry first (so MCPs can register with it)
echo "=== Deploying Tool Registry (tools.distributed.systems) ==="
modal deploy deploy/modal/modal_app.py
echo ""

# Deploy each native MCP - they self-register on startup
echo "=== Deploying Distributed Search MCP ==="
modal deploy deploy/modal/search_mcp.py

echo ""
echo "=== Deploying Distributed Research MCP ==="
modal deploy deploy/modal/research_mcp.py

echo ""
echo "=== Deploying Distributed Publish MCP ==="
modal deploy deploy/modal/publish_mcp.py

echo ""
echo "=== Deploying Distributed Memory MCP ==="
modal deploy deploy/modal/memory_mcp.py

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Endpoints:"
echo "  Registry:  https://tools.distributed.systems"
echo "  Search:    https://arthurcolle--distributed-search-mcp-web.modal.run"
echo "  Research:  https://arthurcolle--distributed-research-mcp-web.modal.run"
echo "  Publish:   https://arthurcolle--distributed-publish-mcp-web.modal.run"
echo "  Memory:    https://arthurcolle--distributed-memory-mcp-web.modal.run"
echo ""
echo "Verify registration:"
echo "  curl https://tools.distributed.systems/api/v1/services"
echo ""
echo "Execute tools via gateway:"
echo '  curl -X POST https://tools.distributed.systems/api/v1/gateway/execute \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '\''{"tool": "search:search", "inputs": {"query": "test"}}'\'''
