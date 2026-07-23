#!/bin/bash
set -e

# Local Development Startup Script
# Starts both Layer 1 (Neo4j Wrapper) and Layer 2 (Graph Management API)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Graph Management Local Startup${NC}"
echo -e "${BLUE}========================================${NC}"

# Load environment
if [ -f .env ]; then
    echo -e "${GREEN}✓${NC} Loading .env"
    export $(cat .env | grep -v '^#' | xargs)
else
    echo -e "${YELLOW}⚠${NC}  No .env found, using .env.local defaults"
    if [ -f .env.local ]; then
        export $(cat .env.local | grep -v '^#' | xargs)
    fi
fi

# Check Python version
echo -e "${GREEN}✓${NC} Checking Python..."
python3 --version

# Check dependencies
echo -e "${GREEN}✓${NC} Checking dependencies..."
if ! python3 -c "import fastapi, neo4j, pydantic" 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC}  Missing dependencies. Installing..."
    pip install -r requirements.txt
fi

# Check Neo4j connectivity
echo -e "${GREEN}✓${NC} Testing Neo4j connection at ${NEO4J_URI}..."
if python3 -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('${NEO4J_URI}', auth=('${NEO4J_USER}', '${NEO4J_PASSWORD}'))
driver.verify_connectivity()
driver.close()
print('Neo4j connection successful')
" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Neo4j connection successful"
else
    echo -e "${YELLOW}⚠${NC}  Could not connect to Neo4j at ${NEO4J_URI}"
    echo "    Make sure Neo4j Desktop is running"
    echo "    User: ${NEO4J_USER}"
    echo "    Database: ${NEO4J_DATABASE}"
    read -p "    Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Starting Services${NC}"
echo -e "${BLUE}========================================${NC}"

# Create logs directory
mkdir -p logs

# Start Layer 1: Neo4j Wrapper
echo -e "${GREEN}▶${NC}  Starting Layer 1: Neo4j Wrapper (port 8001)..."
python3 local_neo4j_wrapper.py > logs/neo4j_wrapper.log 2>&1 &
NEO4J_WRAPPER_PID=$!
echo -e "   PID: ${NEO4J_WRAPPER_PID}"

# Wait for Layer 1 to be ready
echo -e "${YELLOW}⏳${NC} Waiting for Neo4j Wrapper to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:8001/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Neo4j Wrapper is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${YELLOW}⚠${NC}  Neo4j Wrapper did not start in time"
        echo "    Check logs/neo4j_wrapper.log for errors"
        kill $NEO4J_WRAPPER_PID 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Start Layer 2: Graph Management API
echo -e "${GREEN}▶${NC}  Starting Layer 2: Graph Management API (port 8000)..."
python3 -m uvicorn graph_management_service:api --host 0.0.0.0 --port 8000 --reload > logs/graph_management.log 2>&1 &
GRAPH_MGMT_PID=$!
echo -e "   PID: ${GRAPH_MGMT_PID}"

# Wait for Layer 2 to be ready
echo -e "${YELLOW}⏳${NC} Waiting for Graph Management API to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Graph Management API is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${YELLOW}⚠${NC}  Graph Management API did not start in time"
        echo "    Check logs/graph_management.log for errors"
        kill $NEO4J_WRAPPER_PID 2>/dev/null || true
        kill $GRAPH_MGMT_PID 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Services Running${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓${NC} Neo4j Wrapper:       http://localhost:8001"
echo -e "${GREEN}✓${NC} Graph Management:    http://localhost:8000"
echo -e "${GREEN}✓${NC} API Docs (Swagger):  http://localhost:8000/docs"
echo -e "${GREEN}✓${NC} API Logs:            logs/graph_management.log"
echo -e "${GREEN}✓${NC} Wrapper Logs:        logs/neo4j_wrapper.log"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
echo ""

# Save PIDs for cleanup
echo $NEO4J_WRAPPER_PID > logs/neo4j_wrapper.pid
echo $GRAPH_MGMT_PID > logs/graph_management.pid

# Trap Ctrl+C and cleanup
cleanup() {
    echo ""
    echo -e "${YELLOW}Stopping services...${NC}"
    kill $NEO4J_WRAPPER_PID 2>/dev/null || true
    kill $GRAPH_MGMT_PID 2>/dev/null || true
    rm -f logs/neo4j_wrapper.pid logs/graph_management.pid
    echo -e "${GREEN}✓${NC} All services stopped"
    exit 0
}
trap cleanup INT TERM

# Follow logs
tail -f logs/graph_management.log &
TAIL_PID=$!

# Wait for processes
wait $NEO4J_WRAPPER_PID $GRAPH_MGMT_PID
kill $TAIL_PID 2>/dev/null || true
