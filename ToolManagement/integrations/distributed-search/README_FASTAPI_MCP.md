# FastAPI MCP Chat Server

A FastAPI wrapper for the MCP (Model Context Protocol) chat client that provides:
- **SSE Streaming** for real-time responses
- **Multi-turn conversations** with conversation history
- **MCP Tool Integration** - Connects to unbounded MCP servers
- **OpenRouter LLM** integration with streaming
- **Comprehensive Event Streaming** - All events (messages, tool calls, thinking, etc.)

## Architecture

```
┌─────────────┐      SSE Stream      ┌──────────────┐
│   Client    │ ◄─────────────────── │   FastAPI    │
│  (Browser)  │                      │    Server    │
└─────────────┘                      └──────────────┘
                                            │
                                            │ HTTP
                                            ▼
                          ┌──────────────────────────────┐
                          │      OpenRouter API          │
                          │  (Claude, GPT, etc.)         │
                          └──────────────────────────────┘
                                            │
                                            │ Tool Calls
                                            ▼
                          ┌──────────────────────────────┐
                          │      MCP Servers             │
                          │  (Jina, Search, etc.)        │
                          └──────────────────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
pip install fastapi uvicorn httpx mcp rich pydantic
```

### 2. Set Environment Variables

```bash
export OPENROUTER_API_KEY="your-openrouter-key"
export JINA_API_KEY="your-jina-api-key"  # Optional, for Jina MCP

# Optional: Add more MCP servers
export MCP_SERVER_URLS="https://server1.com/sse,https://server2.com/mcp"
```

### 3. Start the Server

```bash
python fastapi_mcp_chat.py
```

Server will start on `http://localhost:2046`

### 4. Test with Client

Interactive mode:
```bash
python test_fastapi_mcp.py
```

Simple test mode:
```bash
python test_fastapi_mcp.py test
```

## API Endpoints

### `GET /`
Health check and server info

**Response:**
```json
{
  "status": "online",
  "service": "MCP Chat FastAPI",
  "version": "1.0.0",
  "mcp_connected": true,
  "tools_available": 42,
  "timestamp": "2025-01-15T12:00:00.000000Z"
}
```

### `GET /tools`
List available MCP tools

**Response:**
```json
{
  "tools": [
    {
      "name": "search",
      "description": "Search the web",
      "input_schema": {...}
    }
  ],
  "count": 42
}
```

### `POST /chat`
Main chat endpoint with streaming

**Request:**
```json
{
  "message": "Search for FastAPI tutorials",
  "conversation_id": "conv_abc123",  // optional
  "model": "anthropic/claude-3.5-sonnet",
  "temperature": 0.7,
  "max_tokens": 4000,
  "stream": true,
  "enable_tools": true,
  "system_prompt": "You are a helpful assistant"  // optional
}
```

**Response:** Server-Sent Events (SSE) stream

### `GET /conversations`
List all active conversations

### `GET /conversations/{id}`
Get conversation history

### `DELETE /conversations/{id}`
Delete a conversation

## Event Types

The SSE stream emits the following event types:

### Session Events
- `session.start` - Chat session begins
- `session.update` - Session state changes
- `session.end` - Session completes

### Message Events
- `message.start` - Assistant starts responding
- `message.delta` - Streaming message chunk
- `message.end` - Message complete

### Tool Events
- `tool.call.start` - Tool call initiated
- `tool.call.args` - Tool arguments parsed
- `tool.call.execute` - Tool execution starts
- `tool.result` - Tool result received
- `tool.call.end` - Tool call complete

### Other Events
- `thinking.start` - Reasoning begins (if enabled)
- `thinking.delta` - Reasoning stream
- `thinking.end` - Reasoning complete
- `error` - Error occurred
- `metadata` - Additional metadata
- `heartbeat` - Keep-alive

## Example Event Stream

```
event: session.start
data: {"event": "session.start", "data": {"message_id": "msg_abc", "tools_available": 42}, ...}

event: message.start
data: {"event": "message.start", "data": {"iteration": 1}, ...}

event: message.delta
data: {"event": "message.delta", "data": {"delta": "Hello", "full_text": "Hello"}, ...}

event: tool.call.start
data: {"event": "tool.call.start", "data": {"tool_name": "search", ...}, ...}

event: tool.result
data: {"event": "tool.result", "data": {"result": "..."}, ...}

event: message.end
data: {"event": "message.end", "data": {"full_text": "...", "has_tool_calls": false}, ...}

event: session.end
data: {"event": "session.end", "data": {"iterations": 2, "total_events": 45}, ...}
```

## Client Example (JavaScript)

```javascript
const eventSource = new EventSource('http://localhost:2046/chat', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    message: "Search for Python tutorials",
    stream: true,
    enable_tools: true
  })
});

eventSource.addEventListener('message.delta', (e) => {
  const data = JSON.parse(e.data);
  console.log('Delta:', data.data.delta);
});

eventSource.addEventListener('tool.call.start', (e) => {
  const data = JSON.parse(e.data);
  console.log('Tool call:', data.data.tool_name);
});

eventSource.addEventListener('session.end', (e) => {
  const data = JSON.parse(e.data);
  console.log('Session ended');
  eventSource.close();
});
```

## Client Example (Python)

```python
import httpx
import json

async def stream_chat(message: str):
    payload = {
        "message": message,
        "stream": True,
        "enable_tools": True
    }

    async with httpx.AsyncClient() as client:
        async with client.stream("POST", "http://localhost:2046/chat", json=payload) as response:
            async for line in response.aiter_lines():
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data = json.loads(line.split(":", 1)[1])

                    if event_type == "message.delta":
                        print(data["data"]["delta"], end="", flush=True)
                    elif event_type == "tool.call.start":
                        print(f"\n🔧 {data['data']['tool_name']}")
```

## Configuration

### MCP Servers

The server connects to MCP servers configured via environment variables:

1. **Jina MCP** (default):
   ```bash
   export JINA_API_KEY="your-key"
   export JINA_MCP_URL="https://mcp.jina.ai/sse"  # optional
   ```

2. **Additional Servers**:
   ```bash
   export MCP_SERVER_URLS="https://server1.com/sse,https://server2.com/mcp"
   ```

### Models

Supported models (via OpenRouter):
- `anthropic/claude-3.5-sonnet` (default)
- `anthropic/claude-3-opus`
- `anthropic/claude-3-haiku`
- `openai/gpt-4`
- `openai/gpt-3.5-turbo`
- And many more via OpenRouter

## Features

### ✅ Implemented
- [x] FastAPI server with SSE streaming
- [x] MCP client integration (single server)
- [x] OpenRouter LLM streaming
- [x] Tool calling with MCP
- [x] Multi-turn conversations
- [x] Conversation history management
- [x] Comprehensive event streaming
- [x] CORS support
- [x] Error handling

### 🚧 TODO
- [ ] Multiple MCP server support (ClientSessionGroup)
- [ ] Non-streaming mode
- [ ] Authentication/API keys
- [ ] Rate limiting
- [ ] Conversation persistence (database)
- [ ] WebSocket alternative to SSE
- [ ] Thinking/reasoning display
- [ ] Token usage tracking
- [ ] Model switching per request

## Differences from Original chat-openrouter-mcp.py

| Feature | Original | FastAPI Wrapper |
|---------|----------|-----------------|
| Interface | CLI/REPL | HTTP API |
| Streaming | Terminal output | SSE |
| Multi-user | No | Yes |
| Conversation persistence | In-memory | In-memory (can add DB) |
| MCP servers | Multiple (ClientSessionGroup) | Single (for now) |
| Tool calls | Yes | Yes |
| Profile management | Yes | No (simplified) |

## Troubleshooting

### "MCP not initialized"
Make sure you have set `JINA_API_KEY` or `MCP_SERVER_URLS` and the server can connect to the MCP endpoints.

### "OpenRouter API error"
Check that `OPENROUTER_API_KEY` is set correctly.

### Tools not being called
- Verify tools are loaded: `curl http://localhost:2046/tools`
- Check `enable_tools: true` in your request
- Some models may not support function calling

### Connection timeout
Increase the timeout in the client:
```python
httpx.AsyncClient(timeout=300.0)  # 5 minutes
```

## Related Files

- `fastapi_mcp_chat.py` - Main FastAPI server
- `test_fastapi_mcp.py` - Python client for testing
- `chat-openrouter-mcp.py` - Original CLI implementation
- `requirements.txt` - Python dependencies

## License

Same as parent project
