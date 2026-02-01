#!/bin/bash

# Deploy Neo4j to Modal
# Usage: ./deploy_neo4j.sh [serve|deploy]

set -e

MODE="${1:-serve}"

echo "========================================="
echo "Neo4j Modal Deployment"
echo "========================================="
echo ""

# Check if Modal is installed
if ! command -v modal &> /dev/null; then
    echo "Error: Modal CLI is not installed."
    echo "Install it with: pip install modal"
    exit 1
fi

# Check if user is authenticated
if ! modal token show &> /dev/null; then
    echo "Error: Not authenticated with Modal."
    echo "Run: modal setup"
    exit 1
fi

# Set Neo4j password if not already set
if [ -z "$NEO4J_PASSWORD" ]; then
    export NEO4J_PASSWORD="neo4j_password"
    echo "Using default password: neo4j_password"
    echo "Set NEO4J_PASSWORD environment variable to use a custom password."
    echo ""
fi

echo "Mode: $MODE"
echo ""

if [ "$MODE" == "serve" ]; then
    echo "Starting Neo4j in development mode (with live reload)..."
    echo "Press Ctrl+C to stop."
    echo ""
    modal serve neo4j_self_hosted.py
elif [ "$MODE" == "deploy" ]; then
    echo "Deploying Neo4j to production..."
    echo ""

    # Deploy and capture output
    DEPLOY_OUTPUT=$(modal deploy neo4j_self_hosted.py 2>&1)
    echo "$DEPLOY_OUTPUT"

    # Extract URL from output
    URL=$(echo "$DEPLOY_OUTPUT" | grep -oE 'https://[^[:space:]]+modal\.run' | head -1)

    if [ -n "$URL" ]; then
        echo ""
        echo "========================================="
        echo "Deployment Complete!"
        echo "========================================="
        echo ""
        echo "Your Neo4j instance is available at:"
        echo "  $URL"
        echo ""
        echo "Test it with:"
        echo "  curl $URL/health"
        echo "  curl $URL/probe"
        echo ""
        echo "Or run the full test suite:"
        echo "  python test_neo4j_deployment.py $URL"
        echo ""

        # Save URL to file
        echo "$URL" > .neo4j_modal_url
        echo "URL saved to .neo4j_modal_url"
    else
        echo ""
        echo "Warning: Could not extract deployment URL from output."
        echo "Check Modal dashboard for your app URL."
    fi
else
    echo "Error: Invalid mode '$MODE'"
    echo "Usage: ./deploy_neo4j.sh [serve|deploy]"
    exit 1
fi
