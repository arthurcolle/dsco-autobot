#!/bin/bash
set -e

# Bootstrap Script
# Sets up the graph database with initial constraints, roles, permissions, and environments

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Graph Management Bootstrap${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if services are running
echo -e "${GREEN}✓${NC} Checking services..."
if ! curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠${NC}  Graph Management API is not running"
    echo "    Start it with: ./start_local.sh"
    exit 1
fi

echo -e "${GREEN}✓${NC} Graph Management API is running"

# Get current model status
echo ""
echo -e "${BLUE}Current Model Status:${NC}"
STATUS=$(curl -s http://localhost:8000/v1/model/status 2>/dev/null || echo '{}')
echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"

echo ""
read -p "Proceed with bootstrap? This will create constraints, roles, and permissions. (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Bootstrap cancelled"
    exit 0
fi

# Bootstrap the model
echo ""
echo -e "${GREEN}▶${NC}  Bootstrapping graph model..."
RESPONSE=$(curl -s -X POST http://localhost:8000/v1/model/bootstrap \
    -H "Content-Type: application/json" \
    -d '{
        "org_id": "acme",
        "org_name": "Acme Corp",
        "seed_rbac": true,
        "seed_envs": true
    }')

echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

# Get updated status
echo ""
echo -e "${GREEN}✓${NC} Bootstrap complete!"
echo ""
echo -e "${BLUE}Updated Model Status:${NC}"
STATUS=$(curl -s http://localhost:8000/v1/model/status)
echo "$STATUS" | python3 -m json.tool

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Bootstrap Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓${NC} Constraints applied"
echo -e "${GREEN}✓${NC} Roles created (developer, readonly, service, etc.)"
echo -e "${GREEN}✓${NC} Permissions created (schemas:read, query:write, etc.)"
echo -e "${GREEN}✓${NC} Environments created (dev, stage, prod)"
echo -e "${GREEN}✓${NC} Organization created: acme"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "  1. Test the API: curl http://localhost:8000/v1/schema"
echo "  2. View API docs: http://localhost:8000/docs"
echo "  3. Create teams/projects via API"
echo "  4. Register node/relationship schemas"
echo ""
