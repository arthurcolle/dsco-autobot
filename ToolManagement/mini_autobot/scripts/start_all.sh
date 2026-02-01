#!/bin/bash
# Start both Graph API and Tool API
# Runs Graph API in background, Tool API in foreground

cd "$(dirname "$0")/.."

export NEO4J_URI="${NEO4J_URI:-neo4j://127.0.0.1:7687}"
export NEO4J_USER="${NEO4J_USER:-neo4j}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-NEO4J2026!}"
export GRAPH_API_URL="${GRAPH_API_URL:-http://localhost:2015}"

echo "Starting Mini Autobot..."
echo "========================"
echo "Neo4j URI: $NEO4J_URI"
echo "Graph API: http://localhost:2015"
echo "Tool API:  http://localhost:2016"
echo ""

# Start Graph API in background
echo "Starting Graph API..."
python -m mini_autobot.graph_api.server &
GRAPH_PID=$!

# Wait for Graph API to be ready
sleep 2

# Start Tool API in foreground
echo "Starting Tool API..."
python -m mini_autobot.tool_api.server

# Cleanup
kill $GRAPH_PID 2>/dev/null
