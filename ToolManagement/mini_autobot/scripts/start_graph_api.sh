#!/bin/bash
# Start the Graph Management API (Neo4j backend)
# Port: 2015

cd "$(dirname "$0")/.."

export NEO4J_URI="${NEO4J_URI:-neo4j://127.0.0.1:7687}"
export NEO4J_USER="${NEO4J_USER:-neo4j}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-NEO4J2026!}"

echo "Starting Graph API on port 2015..."
echo "Neo4j URI: $NEO4J_URI"

python -m mini_autobot.graph_api.server
