#!/bin/bash

# Test curl commands for FastAPI MCP Chat Server
# Make sure server is running on port 2046 first!

SERVER_URL="http://localhost:2046"

echo "🧪 FastAPI MCP Chat - Curl Test Examples"
echo "========================================"
echo ""

# ==================== Test 1: Simple Chat (No Tools) ====================
echo "1️⃣  Simple Chat (No Tools, No Streaming)"
echo "─────────────────────────────────────────"
curl -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Hello! How are you?",
    "stream": false,
    "enable_tools": false
  }' 2>/dev/null | jq '.' || echo "Error: Is the server running?"

echo ""
echo ""

# ==================== Test 2: Simple Streaming Chat ====================
echo "2️⃣  Simple Streaming Chat (First 20 events)"
echo "─────────────────────────────────────────"
curl -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Tell me a short joke",
    "stream": true,
    "enable_tools": false
  }' 2>/dev/null | head -20

echo ""
echo ""

# ==================== Test 3: Weather Query with Tools ====================
echo "3️⃣  Weather Query with MCP Tools (FULL STREAM)"
echo "─────────────────────────────────────────"
echo "This will trigger tool calls if MCP is connected..."
echo ""

curl -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Get temperatures for 5 major cities on the west coast and 3 cities on the east coast of the USA",
    "stream": true,
    "enable_tools": true,
    "model": "anthropic/claude-3.5-sonnet",
    "temperature": 0.7
  }' 2>/dev/null

echo ""
echo ""

# ==================== Test 4: Search Query with Tools ====================
echo "4️⃣  Search Query (First 50 events)"
echo "─────────────────────────────────────────"
curl -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Search for the latest news about FastAPI framework",
    "stream": true,
    "enable_tools": true
  }' 2>/dev/null | head -50

echo ""
echo ""

# ==================== Test 5: Multi-turn Conversation ====================
echo "5️⃣  Multi-turn Conversation"
echo "─────────────────────────────────────────"
echo "First message..."
CONV_ID=$(curl -s -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is 2+2?",
    "stream": true,
    "enable_tools": false
  }' | grep -o '"conversation_id":"[^"]*"' | head -1 | cut -d'"' -f4)

echo "Conversation ID: $CONV_ID"
echo ""
echo "Second message in same conversation..."
curl -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d "{
    \"message\": \"What was my previous question?\",
    \"conversation_id\": \"$CONV_ID\",
    \"stream\": true,
    \"enable_tools\": false
  }" 2>/dev/null | head -30

echo ""
echo ""

# ==================== Test 6: With File Upload ====================
echo "6️⃣  Chat with File Upload (First 40 events)"
echo "─────────────────────────────────────────"

# Create a test file and encode it
TEST_FILE_CONTENT="This is a test document about FastAPI.

FastAPI is a modern, fast (high-performance), web framework for building APIs with Python 3.7+ based on standard Python type hints."

BASE64_CONTENT=$(echo "$TEST_FILE_CONTENT" | base64)

curl -N -X POST "$SERVER_URL/chat" \
  -H "Content-Type: application/json" \
  -d "{
    \"message\": \"Summarize the attached document\",
    \"stream\": true,
    \"enable_tools\": false,
    \"files\": [
      {
        \"name\": \"test_doc.txt\",
        \"type\": \"text/plain\",
        \"size\": ${#TEST_FILE_CONTENT},
        \"data\": \"$BASE64_CONTENT\"
      }
    ]
  }" 2>/dev/null | head -40

echo ""
echo ""

echo "✅ Tests complete!"
echo ""
echo "💡 Tips:"
echo "  - Remove 'head -N' to see full stream"
echo "  - Add '| grep \"message.delta\"' to see only content chunks"
echo "  - Add '| grep \"tool\"' to see only tool events"
echo "  - Use 'jq' for pretty JSON formatting (if available)"
