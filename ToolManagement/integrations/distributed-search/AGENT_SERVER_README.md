# Agent Server - Intelligent /v1/interactions Endpoint

A powerful FastAPI server that wraps the entire agent interaction process into a single streaming endpoint. Whether you're sending a simple "say ok" or a complex "get weather in 12 cities", this server handles it all intelligently.

## 🌟 Features

### Smart Context Management
- **Automatic Compression**: Context automatically compresses when it exceeds thresholds
- **Summary Generation**: Old messages are summarized to preserve continuity
- **Intelligent Pruning**: Keeps recent messages while compressing older ones

### Adaptive Streaming
- **Simple Requests**: "say ok" → streams tokens directly
- **Complex Flows**: Multi-tool interactions → streams complete flow with all events
- **Real-time Events**: SSE events for every stage of processing

### Tool Orchestration
- **MCP Integration**: Full MCP tool support via Jina AI
- **Parallel Execution**: Independent tools can run in parallel (configurable)
- **Error Handling**: Graceful error handling with detailed feedback

### Session Management
- **Persistent Sessions**: Resume conversations across requests
- **Auto-cleanup**: Expired sessions automatically removed
- **Context Continuity**: Maintains conversation history intelligently

## 🚀 Quick Start

### 1. Set Up Environment

```bash
# Required API keys
export OPENROUTER_API_KEY=your_openrouter_key
export JINA_API_KEY=your_jina_key

# Optional configuration
export MAX_ITERATIONS=15
export CONTEXT_COMPRESSION_THRESHOLD=50
export SESSION_TTL_SECONDS=7200
```

### 2. Start the Server

```bash
python agent_server.py
```

The server will start on `http://localhost:2046` by default.

### 3. Make a Request

#### Simple Text Request
```bash
curl -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "say ok"
  }'
```

#### Complex Multi-Tool Request
```bash
curl -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "search for recent AI papers and summarize the top 3",
    "enable_tools": true,
    "enable_context_compression": true
  }'
```

#### With Session Continuity
```bash
# First request - creates session
curl -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is the weather in San Francisco?"
  }'

# Get session_id from X-Session-ID header, then:
curl -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "How about tomorrow?",
    "session_id": "sess_abc123..."
  }'
```

## 📡 API Reference

### POST /v1/interactions

Create a new interaction with the agent.

**Request Body:**
```json
{
  "message": "your message here",
  "session_id": "optional_session_id",
  "model": "anthropic/claude-sonnet-4-5",
  "temperature": 0.7,
  "max_tokens": 8000,
  "enable_tools": true,
  "enable_context_compression": true,
  "enable_parallel_tools": true,
  "system_prompt": "optional system prompt",
  "max_iterations": 15,
  "metadata": {}
}
```

**Response:** Server-Sent Events (SSE) stream

**Response Headers:**
- `X-Session-ID`: The session ID (new or existing)
- `X-Interaction-ID`: Unique interaction identifier
- `X-Session-New`: "true" if session was just created

### Event Types

The server streams various SSE events to track the interaction:

#### Session Events
- `session.start` - Session initiated (new or resumed)
- `session.resume` - Existing session resumed
- `session.complete` - Session interaction complete

#### Interaction Events
- `interaction.start` - Interaction begins
- `interaction.analyzing` - Analyzing request
- `interaction.complete` - Interaction finished

#### Message Events
- `message.start` - LLM response starting
- `message.chunk` - Text chunk (stream tokens here!)
- `message.complete` - Message finished

#### Tool Events
- `tool.plan` - Tool execution plan created
- `tool.execute.start` - Tool execution begins
- `tool.execute.progress` - Tool execution progress
- `tool.execute.complete` - Tool execution finished
- `tool.execute.error` - Tool execution failed

#### Context Events
- `context.update` - Context updated
- `context.compress` - Context compression started
- `context.summary` - Compression summary available

#### Metrics Events
- `metrics` - General metrics
- `usage` - Token usage information
- `iteration` - Iteration counter

### GET /v1/sessions

List all active sessions.

**Response:**
```json
{
  "sessions": [
    {
      "session_id": "sess_abc123",
      "created_at": "2025-01-14T10:00:00Z",
      "updated_at": "2025-01-14T10:05:00Z",
      "message_count": 10,
      "total_tokens": 5000,
      "model": "anthropic/claude-sonnet-4-5",
      "compressed": false
    }
  ],
  "total": 1
}
```

### GET /v1/sessions/{session_id}

Get details for a specific session.

### DELETE /v1/sessions/{session_id}

Delete a session.

### GET /v1/tools

List available MCP tools.

### GET /health

Health check endpoint.

## 🎯 Usage Examples

### Python Client

```python
import asyncio
import httpx
import json

async def stream_interaction(message: str):
    async with httpx.AsyncClient() as client:
        async with client.stream(
            "POST",
            "http://localhost:2046/v1/interactions",
            json={"message": message},
            headers={"Accept": "text/event-stream"}
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = json.loads(line[6:])
                    event_type = data.get("event")

                    if event_type == "message.chunk":
                        # Stream the text!
                        print(data["data"]["chunk"], end="", flush=True)

                    elif event_type == "tool.execute.start":
                        print(f"\n[Executing tool: {data['data']['tool_name']}]")

# Run it
asyncio.run(stream_interaction("say ok"))
```

### JavaScript/Node.js Client

```javascript
const EventSource = require('eventsource');

function streamInteraction(message) {
  const response = await fetch('http://localhost:2046/v1/interactions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message })
  });

  const reader = response.body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const chunk = decoder.decode(value);
    const lines = chunk.split('\n');

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const data = JSON.parse(line.slice(6));

        if (data.event === 'message.chunk') {
          // Stream the text!
          process.stdout.write(data.data.chunk);
        }
      }
    }
  }
}

streamInteraction('say ok');
```

### cURL with SSE Processing

```bash
# Simple streaming
curl -N -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{"message": "say ok"}' | \
  while IFS= read -r line; do
    echo "$line"
  done
```

## 🔧 Advanced Configuration

### Environment Variables

```bash
# API Keys (required)
OPENROUTER_API_KEY=sk-or-...
JINA_API_KEY=jina_...

# MCP Configuration
JINA_MCP_URL=https://mcp.jina.ai/sse  # default

# Behavior
MAX_ITERATIONS=15                      # max tool iterations
MAX_CONTEXT_MESSAGES=100               # max messages in context
CONTEXT_COMPRESSION_THRESHOLD=50       # compress after N messages
SESSION_TTL_SECONDS=7200              # session timeout (2 hours)

# Model Defaults
DEFAULT_MODEL=anthropic/claude-sonnet-4-5
DEFAULT_TEMPERATURE=0.7
DEFAULT_MAX_TOKENS=8000

# Optional: Turso Database
TURSO_DATABASE_URL=libsql://...
TURSO_AUTH_TOKEN=...
```

### Request-Level Configuration

Override defaults per request:

```json
{
  "message": "your message",
  "model": "anthropic/claude-opus-4",
  "temperature": 0.3,
  "max_tokens": 16000,
  "max_iterations": 20,
  "enable_tools": true,
  "enable_context_compression": true,
  "enable_parallel_tools": false,
  "system_prompt": "You are a helpful assistant..."
}
```

## 🧠 How It Works

### Context Management

1. **Initial State**: New sessions start with empty context
2. **Message Addition**: Each interaction adds user/assistant/tool messages
3. **Threshold Check**: When context exceeds `CONTEXT_COMPRESSION_THRESHOLD`
4. **Compression**: Old messages (except recent N) are summarized
5. **Summary Injection**: Summary added as system message
6. **Pruning**: Compressed messages marked (excluded from LLM context)

### Interaction Flow

```
User Request
    ↓
Session Lookup/Create
    ↓
Context Compression (if needed)
    ↓
Build LLM Context
    ↓
Stream LLM Response ──→ text chunks → client
    ↓
Tool Calls Detected?
    ├─ Yes → Execute Tools → Continue Loop
    └─ No → Done!
```

### Smart Streaming

**Simple Request** ("say ok"):
- Streams directly to client
- Minimal events (session start, message chunks, complete)
- Low latency

**Complex Request** ("get weather in 12 cities"):
- Streams full flow with all events
- Tool planning events
- Tool execution events (start, progress, complete)
- Multiple iterations
- Rich observability

## 📊 Monitoring

### Event Stream Analysis

Track events to understand interaction:

```python
events_count = defaultdict(int)

async for event in stream_interaction(message):
    event_type = event.get("event")
    events_count[event_type] += 1

print(f"Total iterations: {events_count['iteration']}")
print(f"Tools executed: {events_count['tool.execute.complete']}")
print(f"Tokens streamed: {events_count['message.chunk']}")
```

### Metrics Endpoint

```bash
curl http://localhost:2046/health
```

Returns:
```json
{
  "status": "healthy",
  "mcp_connected": true,
  "tools_available": 150,
  "active_sessions": 5,
  "timestamp": "2025-01-14T10:00:00Z"
}
```

## 🐛 Debugging

### Enable Debug Events

Debug events are emitted for troubleshooting:

```json
{
  "event": "debug",
  "data": {
    "message": "Context compressed: 80 → 20 messages",
    "level": "info"
  }
}
```

### Common Issues

**Tools Not Executing:**
- Check `JINA_API_KEY` is set
- Verify MCP connection: `curl http://localhost:2046/v1/tools`
- Ensure `enable_tools: true` in request

**Context Growing Too Large:**
- Lower `CONTEXT_COMPRESSION_THRESHOLD`
- Reduce `MAX_CONTEXT_MESSAGES`
- Enable compression: `enable_context_compression: true`

**Slow Responses:**
- Check `MAX_ITERATIONS` - high values slow down complex flows
- Use `enable_parallel_tools: true` for independent tools
- Consider faster model (e.g., Claude Haiku)

## 🎓 Best Practices

### Session Management
- Reuse sessions for multi-turn conversations
- Delete sessions when conversation ends
- Set appropriate TTL for your use case

### Context Compression
- Enable for long conversations
- Keep recent messages uncompressed (10-20)
- Monitor compression events

### Tool Usage
- Enable parallel execution for independent tools
- Set reasonable `max_iterations` (10-20)
- Handle tool errors gracefully

### Performance
- Use connection pooling for high traffic
- Stream to client immediately (don't buffer)
- Monitor token usage

## 📝 Testing

Run the included test client:

```bash
python test_agent_server.py
```

This will run three example scenarios:
1. Simple text interaction
2. Complex multi-tool interaction
3. Multi-turn conversation with session continuity

## 🤝 Integration Examples

### FastAPI Integration

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import httpx

app = FastAPI()

@app.post("/chat")
async def chat(message: str):
    async def stream():
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                "http://localhost:2046/v1/interactions",
                json={"message": message}
            ) as response:
                async for line in response.aiter_lines():
                    yield line + "\n"

    return StreamingResponse(stream(), media_type="text/event-stream")
```

### Discord Bot Integration

```python
import discord
import httpx
import json

@bot.command()
async def ask(ctx, *, question):
    async with ctx.typing():
        full_response = ""

        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                "http://localhost:2046/v1/interactions",
                json={"message": question}
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data = json.loads(line[6:])
                        if data.get("event") == "message.chunk":
                            full_response += data["data"]["chunk"]

        await ctx.send(full_response)
```

## 📄 License

MIT License - feel free to use in your projects!

## 🙏 Credits

Built on:
- [FastAPI](https://fastapi.tiangolo.com/)
- [MCP (Model Context Protocol)](https://modelcontextprotocol.io/)
- [OpenRouter](https://openrouter.ai/)
- [Jina AI](https://jina.ai/)

---

Made with ❤️ for intelligent agent interactions
