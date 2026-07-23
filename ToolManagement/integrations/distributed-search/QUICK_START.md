# Quick Start Guide - Agent Server

Get up and running in 60 seconds!

## 1. Set Environment Variables

```bash
export OPENROUTER_API_KEY=your_openrouter_key_here
export JINA_API_KEY=your_jina_key_here
```

## 2. Start the Server

```bash
./run_agent_server.sh
```

Or manually:
```bash
python agent_server.py
```

Server starts at: `http://localhost:2046`

## 3. Test It!

### Simple Text (Just Streams Tokens)

```bash
curl -N -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{"message": "say ok"}'
```

**What you'll see:**
```
event: session.start
data: {"event":"session.start","data":{"session_new":true,...},...}

event: interaction.start
data: {"event":"interaction.start","data":{"message":"say ok",...},...}

event: message.start
data: {"event":"message.start","data":{"iteration":1},...}

event: message.chunk
data: {"event":"message.chunk","data":{"chunk":"Ok"},...}

event: message.chunk
data: {"event":"message.chunk","data":{"chunk":"!"},...}

event: message.complete
data: {"event":"message.complete","data":{"text":"Ok!","has_tool_calls":false},...}

event: interaction.complete
data: {"event":"interaction.complete","data":{"iterations":1,...},...}
```

### Complex Multi-Tool Request

```bash
curl -N -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "search for recent papers on transformers and give me the top 3 titles",
    "enable_tools": true
  }'
```

**What you'll see:**
```
event: session.start
...

event: interaction.start
data: {...,"tools_available":150,...}

event: message.start
...

event: message.chunk
data: {"chunk":"I'll search for recent papers on transformers..."}
...

event: message.complete
data: {...,"has_tool_calls":true,...}

event: tool.plan
data: {"tools":[{"name":"search_arxiv","id":"call_abc123"}],"count":1}

event: tool.execute.start
data: {"tool_name":"search_arxiv",...}

event: tool.execute.complete
data: {"result_preview":"[Papers found...]",...}

event: message.start
data: {"iteration":2}

event: message.chunk
data: {"chunk":"Based on the search results, here are the top 3 papers:..."}
...

event: message.complete
data: {...,"has_tool_calls":false,...}

event: interaction.complete
data: {"iterations":2,...}
```

### With Session Continuity

First request:
```bash
curl -N -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{"message": "What is 2+2?"}'
```

Look for the session ID in the response headers:
```
X-Session-ID: sess_abc123...
```

Second request (uses same session):
```bash
curl -N -X POST http://localhost:2046/v1/interactions \
  -H "Content-Type: application/json" \
  -d '{
    "message": "multiply that by 3",
    "session_id": "sess_abc123..."
  }'
```

## 4. Python Client Example

```python
import asyncio
import httpx
import json

async def chat(message: str):
    async with httpx.AsyncClient() as client:
        async with client.stream(
            "POST",
            "http://localhost:2046/v1/interactions",
            json={"message": message},
            headers={"Accept": "text/event-stream"},
            timeout=60.0
        ) as response:
            print(f"Session: {response.headers.get('X-Session-ID')}\n")

            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = json.loads(line[6:])
                    event = data.get("event")

                    if event == "message.chunk":
                        # Stream the response!
                        print(data["data"]["chunk"], end="", flush=True)

                    elif event == "tool.execute.start":
                        tool = data["data"]["tool_name"]
                        print(f"\n[Executing: {tool}]\n", flush=True)

                    elif event == "interaction.complete":
                        print("\n\nDone!")
                        break

# Run it
asyncio.run(chat("say ok"))
```

## 5. Check Server Status

```bash
curl http://localhost:2046/health
```

Response:
```json
{
  "status": "healthy",
  "mcp_connected": true,
  "tools_available": 150,
  "active_sessions": 0,
  "timestamp": "2025-01-14T10:00:00Z"
}
```

## 6. List Available Tools

```bash
curl http://localhost:2046/v1/tools
```

## 7. Manage Sessions

List sessions:
```bash
curl http://localhost:2046/v1/sessions
```

Get specific session:
```bash
curl http://localhost:2046/v1/sessions/sess_abc123
```

Delete session:
```bash
curl -X DELETE http://localhost:2046/v1/sessions/sess_abc123
```

## Common Patterns

### Filter Only Text Chunks

```python
async for line in response.aiter_lines():
    if line.startswith("data: "):
        data = json.loads(line[6:])
        if data.get("event") == "message.chunk":
            print(data["data"]["chunk"], end="", flush=True)
```

### Track Tool Executions

```python
tools_executed = []

async for line in response.aiter_lines():
    if line.startswith("data: "):
        data = json.loads(line[6:])
        if data.get("event") == "tool.execute.complete":
            tools_executed.append(data["data"]["tool_id"])

print(f"Executed {len(tools_executed)} tools")
```

### Monitor Token Usage

```python
total_tokens = 0

async for line in response.aiter_lines():
    if line.startswith("data: "):
        data = json.loads(line[6:])
        if data.get("event") == "usage":
            usage = data["data"]["iteration_usage"]
            total_tokens += usage.get("total_tokens", 0)

print(f"Total tokens used: {total_tokens}")
```

## Configuration Options

Full request example with all options:

```json
{
  "message": "your message here",
  "session_id": "sess_abc123...",
  "model": "anthropic/claude-sonnet-4-5",
  "temperature": 0.7,
  "max_tokens": 8000,
  "enable_tools": true,
  "enable_context_compression": true,
  "enable_parallel_tools": true,
  "system_prompt": "You are a helpful assistant",
  "max_iterations": 15,
  "metadata": {
    "user_id": "user123",
    "app": "my_app"
  }
}
```

## Troubleshooting

**Connection Refused:**
```bash
# Check if server is running
curl http://localhost:2046/health
```

**No Tools Available:**
```bash
# Check JINA_API_KEY is set
echo $JINA_API_KEY

# Verify MCP connection
curl http://localhost:2046/v1/tools
```

**Slow Responses:**
- Reduce `max_iterations` (default 15)
- Use faster model: `"model": "anthropic/claude-haiku-4.5"`
- Enable parallel tools: `"enable_parallel_tools": true`

## Next Steps

- Read the full [AGENT_SERVER_README.md](AGENT_SERVER_README.md)
- Run the test suite: `python test_agent_server.py`
- Check out the OpenAPI docs: http://localhost:2046/docs

---

Happy building! 🚀
