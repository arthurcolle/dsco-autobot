#!/bin/bash

# Quick curl tests for FastAPI MCP Chat Server

SERVER_URL="http://localhost:2046"

echo "🧪 Testing FastAPI MCP Chat Server"
echo "==================================="
echo ""

# Test 1: Health check
echo "1️⃣  Health Check"
echo "GET $SERVER_URL/"
echo ""
curl -s $SERVER_URL/ | jq '.' || curl -s $SERVER_URL/
echo ""
echo ""

# Test 2: List tools
echo "2️⃣  List MCP Tools"
echo "GET $SERVER_URL/tools"
echo ""
curl -s $SERVER_URL/tools | jq '.tools[] | {name: .name, description: .description}' || curl -s $SERVER_URL/tools
echo ""
echo ""

# Test 3: Simple chat (first 30 lines of stream)
echo "3️⃣  Simple Chat Stream (first 30 events)"
echo "POST $SERVER_URL/chat"
echo ""
curl -s -N -X POST $SERVER_URL/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Hello! How are you?",
    "stream": true,
    "enable_tools": false,
    "model": "anthropic/claude-3.5-sonnet",
    "temperature": 0.7
  }' | head -30
echo ""
echo ""

# Test 4: Chat with tools (first 50 lines)
echo "4️⃣  Chat with Tools Enabled (first 50 events)"
echo "POST $SERVER_URL/chat"
echo ""
curl -s -N -X POST $SERVER_URL/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Search for information about FastAPI",
    "stream": true,
    "enable_tools": true,
    "model": "anthropic/claude-3.5-sonnet"
  }' | head -50
echo ""
echo ""

# Test 5: List conversations
echo "5️⃣  List Conversations"
echo "GET $SERVER_URL/conversations"
echo ""
curl -s $SERVER_URL/conversations | jq '.' || curl -s $SERVER_URL/conversations
echo ""
echo ""

echo "✅ Tests complete!"
echo ""
echo "To see full stream, run:"
echo "  curl -N -X POST $SERVER_URL/chat -H 'Content-Type: application/json' -d '{\"message\":\"Hello\",\"stream\":true}'"
