#!/bin/bash
# Start the Tool Management API
# Port: 2016

cd "$(dirname "$0")/.."

export GRAPH_API_URL="${GRAPH_API_URL:-http://localhost:2015}"

echo "Starting Tool API on port 2016..."
echo "Graph API URL: $GRAPH_API_URL"

python -m mini_autobot.tool_api.server
