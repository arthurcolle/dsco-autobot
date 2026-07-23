#!/bin/bash

# Stop Local Services Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping all Graph Management services...${NC}"

# Stop from PID files
if [ -f logs/neo4j_wrapper.pid ]; then
    PID=$(cat logs/neo4j_wrapper.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID
        echo -e "${GREEN}✓${NC} Stopped Neo4j Wrapper (PID $PID)"
    fi
    rm -f logs/neo4j_wrapper.pid
fi

if [ -f logs/graph_management.pid ]; then
    PID=$(cat logs/graph_management.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID
        echo -e "${GREEN}✓${NC} Stopped Graph Management API (PID $PID)"
    fi
    rm -f logs/graph_management.pid
fi

# Also kill by port
for PORT in 8000 8001; do
    PID=$(lsof -ti tcp:$PORT 2>/dev/null)
    if [ ! -z "$PID" ]; then
        kill $PID 2>/dev/null || true
        echo -e "${GREEN}✓${NC} Stopped process on port $PORT"
    fi
done

echo -e "${GREEN}✓${NC} All services stopped"
