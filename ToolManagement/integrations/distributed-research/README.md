# Parallel WebSearch Server + AI Matching Engine

A comprehensive FastAPI server wrapper for the Parallel.ai API **PLUS** an autonomous AI labor marketplace with 2300 specialized domain agents.

## 🆕 Matching Engine - AI Labor Marketplace

**NEW**: Intelligent job routing to 2300 specialized AI agents across 16 domains with autonomous matching and multi-backend execution. [See full documentation →](MATCHING_ENGINE.md)

### Quick Overview

The Matching Engine automatically routes jobs to the most qualified AI workers:
- **2300 specialized agents** across finance, technology, medicine, law, science, and more
- **Autonomous matching** based on capability, domain expertise, cost, quality, and availability
- **Multi-backend execution** (Parallel.ai for research, Anthropic for reasoning/computer-use)
- **Real-time tracking** via REST API and SSE streaming
- **Scales to billions** of concurrent task instances

```bash
# Submit a job - it automatically finds the best agent
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Analyze Tesla Q4 earnings and provide investment insights",
    "job_type": "research",
    "requirements": {"preferred_domains": ["finance"]}
  }'
```

---

## Parallel WebSearch Server

A comprehensive FastAPI server wrapper for the Parallel.ai API, providing full access to Search, Extract, Tasks, FindAll, and Monitor endpoints with streaming support.

### Getting Started with Matching Engine

```bash
# Run the demo
python examples/matching_engine_demo.py

# Or submit jobs via API
python -c "
import httpx
response = httpx.post('http://127.0.0.1:2040/v1/matching/jobs', json={
    'description': 'Analyze cryptocurrency market trends',
    'job_type': 'research',
    'requirements': {'preferred_domains': ['finance']}
})
print(response.json())
"
```

**Documentation:**
- [📖 Complete Guide](MATCHING_ENGINE.md) - Full documentation
- [🚀 Quick Start](QUICKSTART.md) - Get started in 5 minutes
- [🏗️ System Overview](SYSTEM_OVERVIEW.md) - Technical deep-dive
- [💻 API Docs](http://127.0.0.1:2040/docs) - Interactive API documentation

### Agent Mailbox Notifications

Long-running jobs no longer require the agent to hold an SSE connection open. Provide a `notification_channel`
when you submit the job and the server will drop completion/failed events into that mailbox:

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Parallel web search for biocompute patents",
    "job_type": "research",
    "notification_channel": "agent-demo-42"
  }'

# Later, poll the mailbox (or long-poll) to pick up notifications
curl "http://127.0.0.1:2040/v1/matching/mailbox/agent-demo-42?max_messages=10"

# Acknowledge/delete delivered messages once processed
curl -X POST http://127.0.0.1:2040/v1/matching/mailbox/agent-demo-42/ack \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["msg_deadbeef123"]}'
```

Every message contains the job ID, event type (`job.completed` / `job.failed`), worker ID, and the result/error payload,
so autonomous agents can resume their workflows the moment the background job finishes.

---

## Parallel WebSearch Features

- **Complete API Coverage**: All Parallel.ai endpoints (Search, Extract, Tasks, Task Groups, FindAll, Monitors)
- **Agent Mailbox**: In-memory mailbox + REST endpoints for asynchronous completion notifications
- **Streaming Support**: Server-Sent Events (SSE) for real-time updates
- **Type Safety**: Full Pydantic models based on OpenAPI spec
- **Production Ready**: Middleware, logging, error handling, CORS
- **Future Proof**: Designed for advanced capabilities and extensions

### Context State & Research Cache

Agents often need more than a list of URLs—they need to track context state, monitor
long-running research, and resume work later. The server now bundles a SQLite-backed cache
(`CACHE_DB_PATH`) plus REST endpoints for context lifecycle management:

```bash
# 1) Create a context to hold research state/history
curl -X POST http://127.0.0.1:2040/v1alpha/contexts \
  -H "Content-Type: application/json" \
  -d '{"name": "clinical-trials-tracker", "metadata": {"owner": "agent-7"}}'

# 2) Run Search/Extract tied to that context (input/output automatically cached)
curl -X POST "http://127.0.0.1:2040/v1beta/search?context_id=clinical-trials-tracker" \
  -H "Content-Type: application/json" \
  -d '{"objective":"latest phase 3 oncology trials","max_results":5}'

# 3) Launch a long-running managed task and keep it in the same context
curl -X POST "http://127.0.0.1:2040/v1/tasks/runs/managed?context_id=clinical-trials-tracker" \
  -H "Content-Type: application/json" \
  -d '{"processor":"lite","input":"Map the 3 most recent FDA oncology approvals"}'

# 4) Inspect the current state and summarized history anytime
curl http://127.0.0.1:2040/v1alpha/contexts/clinical-trials-tracker?history_limit=10
```

Any `context_id` supplied to `/v1beta/search`, `/v1beta/extract`, `/v1/tasks/runs`,
or `/v1/tasks/runs/managed` is persisted together with the raw request/response pair and
a short summary. Managed and standard task runs continuously update their cached record
as statuses change or when final results are fetched, so agents never lose the thread of a
long-running investigation.

## Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or use pyproject.toml
pip install -e .
```

### Configuration

Create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and add your Parallel API key:

```
API_KEY=your_parallel_api_key_here
API_KEY_CACHE_FILE=.parallel_api_key  # optional persistent cache file
CACHE_DB_PATH=parallel_context_cache.db  # optional sqlite cache for context state
```

The server automatically caches the most recent `x-api-key` it sees (and writes it to
`.parallel_api_key` by default) so restarts keep working even if you do not set
environment variables again. Set `API_KEY_CACHE_FILE` to a different path or an empty
string to disable disk caching. `CACHE_DB_PATH` controls where the context-state SQLite
cache lives (defaults to `parallel_context_cache.db` in the working directory).

### Run Server

```bash
# Run directly
python -m parallel_websearch_server.main

# Or use the installed script
parallel-websearch-server

# Development mode with auto-reload
RELOAD=true python -m parallel_websearch_server.main
```

The server will start at `http://127.0.0.1:2040` (configurable via PORT env var)

## API Documentation

Once running, access:
- Interactive docs: http://127.0.0.1:2040/docs
- ReDoc: http://127.0.0.1:2040/redoc
- Health check: http://127.0.0.1:2040/health

## API Endpoints

### Search & Extract (Beta)

Append `?context_id=my-context` to automatically cache the request/response under a
named context (see `/v1alpha/contexts`).

**POST /v1beta/search**
```json
{
  "objective": "Find latest AI research papers",
  "max_results": 10
}
```

**POST /v1beta/extract**
```json
{
  "urls": ["https://example.com"],
  "excerpts": {"max_chars_per_result": 1000}
}
```

### Tasks

All task routes accept an optional `context_id` query parameter. When provided, the server
records the task input, intermediate status updates, and final results inside the context
cache so agents can resume long-running research.

> **Input shape reminder:** `input` (and anything under `task_spec`) must be a JSON object.
> Wrap free-text prompts in a single field (e.g., `{"query": "Describe..."}`) so the Parallel.ai API
> can validate it against the provided schema.

**POST /v1/tasks/runs** - Create task run
**POST /v1/tasks/runs/managed** - Create, monitor, and summarize a run (agent-friendly)
**GET /v1/tasks/runs/{run_id}** - Get task status
**GET /v1/tasks/runs/{run_id}/result** - Get result (blocking)
**GET /v1/tasks/runs/{run_id}/events** - Stream events (SSE)

Example:
```json
{
  "processor": "lite",
  "input": {
    "query": "Analyze market trends for electric vehicles"
  },
  "task_spec": {
    "output_schema": {
      "type": "json",
      "properties": {
        "trends": {"type": "array"},
        "summary": {"type": "string"}
      }
    }
  }
}
```

Managed run example (waits for completion and returns summarized output):

```json
{
  "processor": "lite",
  "input": {
    "query": "Map the 3 most recent FDA oncology approvals and summarize mechanisms of action"
  },
  "task_spec": {"output_schema": {"type": "text"}}
}
```

Response payload includes the final status, polling history, a concise summary (text or JSON),
and the raw output so MCP clients can surface everything to the end user without leaving chat.

#### Processor selection & costs

- Defaults: the server honors `settings.default_parallel_processor` (currently `lite`) for every Parallel.ai run unless the caller explicitly requests a higher tier. This keeps unattended workloads on the cheapest possible path.
- Pricing guide: see `docs/choose_processor.md` for per-tier latency, max fields, and cost-per-1k-run figures sourced from the Parallel Task API documentation.
- Budget guardrails: multi-tenant deployments should pair processor requests with the micro-credit system in `docs/billing.md` to cap exposure per tenant or project.
- Policy controls: set `ALLOWED_PARALLEL_PROCESSORS` (or `processor_allowlist`) to reject expensive tiers at the FastAPI boundary without touching client code.
- Task design helpers: `docs/ingest_api.md` documents the Suggest Task / Suggest Processor endpoints plus a full end-to-end ingest workflow you can copy into your own scripts.

### Contexts (Alpha)

**POST /v1alpha/contexts** - Create a context with optional metadata/state  
**GET /v1alpha/contexts** - List recent contexts  
**GET /v1alpha/contexts/{id}** - Fetch context plus summarized history  
**PATCH /v1alpha/contexts/{id}** - Merge or replace metadata/state  
**GET /v1alpha/contexts/{id}/history** - Paginate raw input/output pairs

### Task Groups (Beta)

**POST /v1beta/tasks/groups** - Create group
**POST /v1beta/tasks/groups/{id}/runs** - Add runs
**GET /v1beta/tasks/groups/{id}/events** - Stream group events

See `docs/task_group.md` for a battery of cURL, Python, and TypeScript examples that cover taskgroup creation, streaming runs, batching, and error handling patterns.

### FindAll (Beta)

**POST /v1beta/findall/ingest** - Convert objective to spec
**POST /v1beta/findall/runs** - Start FindAll run
**GET /v1beta/findall/runs/{id}/events** - Stream discovery events
**POST /v1beta/findall/runs/{id}/extend** - Extend match limit
**POST /v1beta/findall/runs/{id}/enrich** - Add enrichments

Example:
```json
{
  "objective": "Find Y Combinator companies in AI space",
  "match_limit": 50
}
```

### Monitors (Alpha)

**POST /v1alpha/monitors** - Create monitor
**GET /v1alpha/monitors** - List monitors
**PATCH /v1alpha/monitors/{id}** - Update monitor
**GET /v1alpha/monitors/{id}/executions** - Get execution history

Example:
```json
{
  "query": "Monitor competitor product launches",
  "cadence": "daily",
  "webhook": {"url": "https://your-webhook.com"}
}
```

## Streaming Examples

### Python Client

```python
import httpx
from parallel_websearch_server.config import settings

base_url = f"http://{settings.host}:{settings.port}"

async with httpx.AsyncClient() as client:
    async with client.stream(
        "GET",
        f"{base_url}/v1/tasks/runs/{run_id}/events"
    ) as response:
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                data = json.loads(line[5:])
                print(data)
```

### JavaScript/Browser

```javascript
// Configure your server URL (default: http://localhost:2040)
const serverUrl = 'http://localhost:2040';
const eventSource = new EventSource(
  `${serverUrl}/v1beta/findall/runs/{id}/events`
);

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(data);
};
```

## Parallel Web Search MCP

- The server automatically surfaces every Search, Extract, Task, Task Group, FindAll, and Monitor operation as a Parallel Web Search MCP using `fastapi-mcp`.
- HTTP tool invocations are available at `http://{host}:{port}{MCP_HTTP_PATH}` (default `/mcp`), and streaming tools open `GET http://{host}:{port}{MCP_SSE_PATH}` (default `/mcp/sse`) while POSTing replies to `{MCP_SSE_PATH}/messages/`. Both paths are configurable via `.env`.
- MCP clients must include `x-api-key` (and `parallel-beta` when invoking beta-specific routes) so the wrapped endpoints can authenticate. The MCP server forwards these headers automatically.
- SSE tooling is powered by `FastApiMCP.mount_sse`, which keeps a long-lived stream for task runs, FindAll runs, task groups, and monitor updates.

### Choosing an MCP transport

- **Streamable HTTP (`/mcp`)** – Used by modern clients like Codex ≥ 0.57. Send `Accept: application/mcp+json` (or `*/*`) and the server will keep the session alive over standard HTTP requests. Supports resumable sessions.
- **Legacy SSE (`/mcp/sse`)** – Compatible with EventSource/SSE clients (e.g., `chat-openrouter-mcp.py`). Establish the stream with `GET /mcp/sse` and POST acknowledgements to `/mcp/sse/messages/`.
- Both transports run side by side, so humans or agents can pick whichever fits their runtime. Point HTTP clients at `http://localhost:2040/mcp`; SSE clients should use `http://localhost:2040/mcp/sse`.
- If you send an SSE-style `Accept: text/event-stream` header to `/mcp`, FastAPI will return HTTP 406. Switch to `/mcp/sse` (or change the Accept header) to avoid that error.

## Architecture

```
parallel-ai/
├── parallel_websearch_server/
│   ├── __init__.py
│   ├── app.py              # FastAPI application
│   ├── main.py             # Entry point
│   ├── config.py           # Configuration
│   ├── models.py           # Pydantic models (all APIs)
│   ├── client.py           # API client wrapper
│   ├── middleware.py       # Request/response middleware
│   ├── exceptions.py       # Custom exceptions
│   ├── streaming.py        # SSE utilities
│   └── routes/
│       ├── search.py       # Search & Extract
│       ├── tasks.py        # Task API
│       ├── task_groups.py  # Task Groups
│       ├── findall.py      # FindAll API
│       └── monitors.py     # Monitor API
├── pyproject.toml
├── requirements.txt
├── .env.example
└── README.md
```

## Development

### Run Tests
```bash
pytest tests/
```

### Code Formatting
```bash
black parallel_websearch_server/
ruff check parallel_websearch_server/
```

### Type Checking
```bash
mypy parallel_websearch_server/
```

## Configuration Options

All configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | - | Parallel API key (required) |
| `API_KEY_CACHE_FILE` | `.parallel_api_key` | Path used to persist cached API keys (set empty to disable) |
| `API_BASE_URL` | `https://api.parallel.ai` | API base URL |
| `HOST` | `127.0.0.1` | Server host |
| `PORT` | `2040` | Server port |
| `WORKERS` | `4` | Number of workers |
| `LOG_LEVEL` | `info` | Log level |
| `REQUEST_TIMEOUT` | `300` | Request timeout (seconds) |
| `STREAM_TIMEOUT` | `600` | Stream timeout (seconds) |

## Future Enhancements

- [ ] Rate limiting and quotas
- [ ] Request caching layer
- [ ] Batch request optimization
- [ ] WebSocket support
- [ ] GraphQL interface
- [ ] Metrics and monitoring
- [ ] Authentication/authorization
- [ ] Response compression

## License

MIT

## Resources

- [Parallel.ai Documentation](https://docs.parallel.ai)
- [OpenAPI Spec](https://docs.parallel.ai/public-openapi.json)
- [Python SDK](https://docs.parallel.ai/resources/python-sdk)
- [Get API Key](https://platform.parallel.ai)
