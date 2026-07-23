#!/bin/bash
# Deploy the registry and ALL native MCP backends to Modal
# Workers self-register with tools.distributed.systems on startup

set -e

echo "Deploying Tool Registry + all native MCP backends to Modal..."
echo ""

# Deploy main registry first (so MCPs can register with it)
echo "=== Deploying Tool Registry (tools.distributed.systems) ==="
modal deploy deploy/modal/modal_app.py
echo ""

WORKERS=(
    search_mcp
    research_mcp
    publish_mcp
    memory_mcp
    video_qa_mcp
    document_qa_mcp
    unit_mcp
    finance_mcp
    integration_mcp
    timeline_mcp
)

for worker in "${WORKERS[@]}"; do
    echo "=== Deploying $worker ==="
    modal deploy "deploy/modal/${worker}.py"
    echo ""
done

echo "=== Deployment Complete ==="
echo ""
echo "Endpoints:"
echo "  Registry:     https://tools.distributed.systems"
echo "  Search:       https://arthurcolle--distributed-search-mcp-web.modal.run"
echo "  Research:     https://arthurcolle--distributed-research-mcp-web.modal.run"
echo "  Publish:      https://arthurcolle--distributed-publish-mcp-web.modal.run"
echo "  Memory:       https://arthurcolle--distributed-memory-mcp-web.modal.run"
echo "  Video QA:     https://arthurcolle--distributed-video-qa-web.modal.run"
echo "  Document QA:  https://arthurcolle--distributed-document-qa-mcp-web.modal.run"
echo "  Unit:         https://arthurcolle--distributed-unit-web.modal.run"
echo "  Finance:      https://arthurcolle--distributed-finance-web.modal.run"
echo "  Integration:  https://arthurcolle--distributed-integration-web.modal.run"
echo ""
echo "Verify all backends:"
echo "  for u in distributed-search-mcp-web distributed-research-mcp-web \\"
echo "           distributed-publish-mcp-web distributed-memory-mcp-web \\"
echo "           distributed-video-qa-web distributed-document-qa-mcp-web \\"
echo "           distributed-unit-web distributed-finance-web distributed-integration-web; do"
echo '    echo "$u: $(curl -s -o /dev/null -w "%{http_code}" https://arthurcolle--$u.modal.run/health)"'
echo "  done"
echo ""
echo "Execute tools via gateway:"
echo '  curl -X POST https://tools.distributed.systems/api/v1/gateway/execute \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '\''{"tool": "search:search", "inputs": {"query": "test"}}'\'''
