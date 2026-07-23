# Advanced Architecture Guide

## Overview

This system provides a **hyper-sophisticated** integration layer for Parallel.ai that completely eliminates the need for users to visit external URLs or manually poll for results. Everything is handled automatically with production-grade reliability.

## Core Systems

### 1. Advanced Task Orchestration (`/v1/orchestration`)

**Never returns platform URLs. Always polls and returns results inline.**

Features:
- **Circuit Breaker Pattern**: Fails fast when API is degraded, automatically recovers
- **Adaptive Polling**: Adjusts polling interval based on task progress
- **Exponential Backoff**: Intelligent retry logic with jitter to prevent thundering herd
- **Priority-Based Queuing**: Critical tasks bypass normal queue limits
- **Result Caching**: Identical requests return cached results (configurable TTL)
- **Deduplication**: Concurrent identical requests share a single execution
- **Comprehensive Telemetry**: Full metrics on success rates, timing, costs

#### Architecture

```
Client Request
      ↓
TaskOrchestrator
      ├─→ Circuit Breaker Check
      ├─→ Cache Lookup
      ├─→ Deduplication Check
      ├─→ Submit to Parallel.ai
      ├─→ Adaptive Polling Loop
      │   ├─→ Status Check (with backoff)
      │   ├─→ Progress Callbacks
      │   └─→ Terminal Status Detection
      ├─→ Result Fetch
      ├─→ Cache Store
      └─→ Return Complete Result
```

#### Usage Examples

**Single Task with Orchestration:**
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:2040/v1/orchestration/tasks",
        json={
            "processor": "ultra2x",
            "input": "Comprehensive analysis of Tesla Q4 2024 earnings",
            "priority": "high",
            "timeout": 1800,  # 30 minutes
            "cache_key": "tesla-q4-2024",  # Optional: for caching
            "context_id": "research-session-123"  # Optional: state persistence
        }
    )

    result = response.json()
    print(f"Status: {result['status']}")
    print(f"Output: {result['output_text']}")
    print(f"Duration: {result['elapsed_seconds']}s")
    print(f"Cost: ${result['estimated_cost']}")
    print(f"Polls: {result['poll_count']}")
```

**Batch Parallel Execution:**
```python
response = await client.post(
    "http://localhost:2040/v1/orchestration/tasks/batch",
    json={
        "tasks": [
            {
                "processor": "lite",
                "input": "Latest AI safety research",
                "priority": "normal"
            },
            {
                "processor": "core",
                "input": "Climate tech unicorns 2025",
                "priority": "high"
            },
            {
                "processor": "pro",
                "input": "Quantum computing breakthroughs",
                "priority": "normal"
            }
        ],
        "fail_fast": False
    }
)

results = response.json()
print(f"Completed: {results['successful']}/{results['total']}")
```

**MCP Integration:**
```typescript
// Sophisticated MCP tools that NEVER return URLs
server.tool("parallel_research", async (args) => {
  const response = await fetch(
    "http://localhost:2040/v1/orchestration/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        processor: args.processor || "lite",
        input: args.input,
        priority: args.priority || "normal",
        timeout: 900
      })
    }
  );

  const result = await response.json();

  // Results are ALWAYS inline - never platform URLs
  return {
    content: [{
      type: "text",
      text: result.output_text || JSON.stringify(result.output_json, null, 2)
    }],
    isError: result.status !== "completed"
  };
});
```

### 2. OpenAPI Schema Synchronization

**Automatically stays synchronized with Parallel.ai's latest API changes.**

Features:
- **Real-time Schema Fetching**: ETags-based efficient fetching from `public-openapi.json`
- **Intelligent Diffing**: Detects breaking vs non-breaking changes
- **Change Notifications**: Alerts on API updates
- **Automatic Cache Management**: SHA256 checksums, local persistence
- **Version Tracking**: Monitors API version changes

#### Architecture

```
SchemaSyncService (Background Loop)
      ↓
OpenAPIFetcher
      ├─→ Fetch with ETag
      ├─→ Calculate SHA256
      └─→ Cache to Disk
      ↓
SchemaDiffer
      ├─→ Compare Schemas
      ├─→ Detect Changes
      │   ├─→ New Endpoints
      │   ├─→ Removed Endpoints
      │   ├─→ Parameter Changes
      │   ├─→ Model Changes
      │   └─→ Security Changes
      ├─→ Classify Severity
      │   ├─→ BREAKING
      │   ├─→ MAJOR
      │   ├─→ MINOR
      │   └─→ PATCH
      └─→ Generate Report
      ↓
Notification Callbacks
```

#### Usage Examples

**Start Sync Service:**
```python
from parallel_websearch_server.schema_sync import SchemaSyncService, SyncConfig

def on_schema_change(event):
    print(f"Schema updated: {event.old_version} → {event.new_version}")
    print(f"Changes: {event.diff_report.total_changes}")

    if event.diff_report.has_breaking_changes():
        print("⚠️  BREAKING CHANGES DETECTED!")
        for change in event.diff_report.get_changes_by_severity("breaking"):
            print(f"  - {change.description}")

config = SyncConfig(
    check_interval_seconds=1800,  # Check every 30 minutes
    auto_sync=True,
    notify_on_breaking_changes_only=False
)

service = SchemaSyncService(
    config=config,
    on_change_callback=on_schema_change
)

await service.start()
```

**Manual Schema Check:**
```python
from parallel_websearch_server.schema_sync import OpenAPIFetcher, SchemaDiffer

fetcher = OpenAPIFetcher()

# Fetch latest
schema, metadata = await fetcher.fetch_schema(force=True)

print(f"Version: {metadata.version}")
print(f"Endpoints: {metadata.endpoint_count}")
print(f"Models: {metadata.model_count}")
print(f"Size: {metadata.size_bytes} bytes")
print(f"Checksum: {metadata.checksum}")

# Compare with previous version (if cached)
# differ = SchemaDiffer()
# diff_report = differ.diff_schemas(old_schema, schema)
```

### 3. Webhook System with HMAC Verification

**Receives real-time notifications from Parallel.ai without polling.**

Features:
- **HMAC-SHA256 Signature Verification**: Industry-standard security
- **Timestamp Validation**: Prevents replay attacks (5-minute window)
- **Event Routing**: Type-based handler registration
- **Multiple Event Types**: FindAll candidates, run completion, task events
- **Error Handling**: Graceful degradation, comprehensive logging

#### Supported Events

**FindAll Events:**
- `findall.candidate.generated` - New candidate discovered
- `findall.candidate.matched` - Candidate matched all conditions
- `findall.candidate.unmatched` - Candidate failed to match
- `findall.candidate.enriched` - Enrichment data extracted
- `findall.run.completed` - Run finished successfully
- `findall.run.cancelled` - Run was cancelled
- `findall.run.failed` - Run failed with error

**Task Events:**
- `task.run.completed` - Task completed successfully
- `task.run.failed` - Task failed
- `task.run.cancelled` - Task was cancelled

#### Architecture

```
Parallel.ai Webhook
      ↓
POST /webhooks/findall or /webhooks/tasks
      ↓
WebhookVerifier
      ├─→ Extract Signature
      ├─→ Validate Timestamp
      ├─→ Compute HMAC-SHA256
      └─→ Constant-Time Comparison
      ↓
Event Parser
      ├─→ Validate Event Type
      └─→ Parse into Model
      ↓
WebhookHandler
      ├─→ Global Handlers (all events)
      └─→ Type-Specific Handlers
      ↓
Custom Business Logic
```

#### Usage Examples

**Register Custom Handlers:**
```python
from parallel_websearch_server.webhooks import WebhookHandler, WebhookEventType

handler = WebhookHandler()

@handler.register(WebhookEventType.FINDALL_CANDIDATE_MATCHED)
def on_match(event):
    candidate = event.data
    print(f"✓ Match found: {candidate.name}")

    # Save to database
    db.candidates.insert({
        "id": candidate.candidate_id,
        "name": candidate.name,
        "url": candidate.url,
        "matched_at": event.timestamp
    })

    # Send notification
    slack.send_message(
        channel="#research",
        text=f"New candidate matched: {candidate.name}"
    )

@handler.register(WebhookEventType.FINDALL_RUN_COMPLETED)
def on_run_complete(event):
    run = event.data
    metrics = run.status.metrics

    print(f"Run {run.findall_id} completed:")
    print(f"  - Generated: {metrics['generated_candidates_count']}")
    print(f"  - Matched: {metrics['matched_candidates_count']}")

    # Trigger downstream processing
    process_results(run.findall_id)

@handler.register()  # Global handler for all events
def log_all_events(event):
    logger.info(f"Webhook received: {event.type} at {event.timestamp}")
```

**Configure Webhook in API Request:**
```python
# When creating a FindAll run
response = await client.post(
    "https://api.parallel.ai/v1beta/findall/runs",
    headers={
        "x-api-key": API_KEY,
        "parallel-beta": "findall-2025-09-15"
    },
    json={
        "objective": "Find portfolio companies of Khosla Ventures",
        "entity_type": "companies",
        "match_limit": 20,
        "processor": "base",
        "webhook": {
            "url": "https://your-domain.com/webhooks/findall",
            "event_types": [
                "findall.candidate.matched",
                "findall.run.completed"
            ]
        }
    }
)
```

**Verify Webhooks in Tests:**
```python
from parallel_websearch_server.webhooks.verification import WebhookVerifier

# Generate test webhook
verifier = WebhookVerifier(secret="your-webhook-secret")

payload = json.dumps({
    "type": "findall.candidate.matched",
    "timestamp": "2025-11-12T10:00:00Z",
    "data": {...}
}).encode()

headers = verifier.generate_headers(payload)

# Verify
is_valid = verifier.verify_signature(
    payload,
    headers["X-Parallel-Signature"],
    headers["X-Parallel-Timestamp"]
)
```

## Configuration

**Environment Variables:**
```bash
# API Configuration
API_KEY=your_parallel_api_key
API_BASE_URL=https://api.parallel.ai
REQUEST_TIMEOUT=300

# Orchestration
MAX_CONCURRENT_TASKS=20
CIRCUIT_BREAKER_ENABLED=true
CACHE_RESULTS=true

# Schema Sync
SCHEMA_SYNC_ENABLED=true
SCHEMA_CHECK_INTERVAL=1800  # 30 minutes

# Webhooks
WEBHOOK_SECRET=your_webhook_secret
WEBHOOK_SIGNATURE_VERIFICATION=true

# Server
HOST=127.0.0.1
PORT=2040
LOG_LEVEL=INFO
```

## Production Deployment

### 1. High Availability Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  parallel-websearch:
    image: parallel-websearch:latest
    replicas: 3
    environment:
      - API_KEY=${PARALLEL_API_KEY}
      - WEBHOOK_SECRET=${WEBHOOK_SECRET}
      - MAX_CONCURRENT_TASKS=50
      - CACHE_DB_PATH=/data/cache.db
    volumes:
      - parallel-cache:/data
    ports:
      - "2040:2040"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:2040/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  parallel-cache:
```

### 2. Monitoring & Observability

**Prometheus Metrics:**
```python
# Add to your monitoring system
from prometheus_client import Counter, Histogram, Gauge

task_requests = Counter(
    'orchestrator_task_requests_total',
    'Total task requests',
    ['processor', 'priority', 'status']
)

task_duration = Histogram(
    'orchestrator_task_duration_seconds',
    'Task execution duration',
    ['processor']
)

circuit_breaker_state = Gauge(
    'orchestrator_circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=open, 2=half-open)'
)
```

**Health Checks:**
```bash
# Orchestrator health
curl http://localhost:2040/v1/orchestration/health

# Webhook handler status
curl http://localhost:2040/webhooks/status

# Schema sync status
curl http://localhost:2040/v1/schema/status  # TODO: Implement
```

### 3. Security Best Practices

1. **Always verify webhook signatures in production**
2. **Use environment variables for secrets, never hardcode**
3. **Enable circuit breaker to prevent cascading failures**
4. **Set reasonable timeouts based on processor tier**
5. **Monitor circuit breaker state - alerts on OPEN**
6. **Rate limit webhook endpoints**
7. **Use HTTPS for all webhook URLs**
8. **Rotate webhook secrets periodically**

## Performance Tuning

### Orchestrator Configuration

```python
config = OrchestrationConfig(
    # Polling
    initial_poll_interval=1.0,  # Start fast
    max_poll_interval=10.0,      # Cap at 10s
    adaptive_polling=True,        # Adjust based on progress

    # Timeouts (match to processor)
    default_timeout=900,          # 15 min for most tasks
    max_timeout=3600,             # 1 hour for ultra2x

    # Retries
    max_retries=3,
    retry_strategy=RetryStrategy.EXPONENTIAL,
    retry_jitter=True,            # Prevent thundering herd

    # Circuit Breaker
    circuit_breaker_enabled=True,
    circuit_breaker_threshold=5,  # Failures before opening
    circuit_breaker_timeout=60,   # Seconds before retry

    # Concurrency
    max_concurrent_tasks=20,      # Parallel executions
    max_queue_size=100,           # Queued tasks

    # Caching
    cache_results=True,
    cache_ttl=3600,               # 1 hour
    deduplicate_tasks=True        # Merge identical requests
)
```

### Processor Selection Guide

| Processor | Use Case | Typical Duration | Recommended Timeout | Cost/1k |
|-----------|----------|------------------|---------------------|---------|
| lite | Quick lookups, simple queries | 10-30s | 120s (2 min) | $6 |
| base | Standard research | 30-90s | 300s (5 min) | $12 |
| core | Detailed analysis | 1-3 min | 600s (10 min) | $30 |
| pro | Complex research | 2-5 min | 900s (15 min) | $60 |
| ultra | Deep investigation | 3-10 min | 1800s (30 min) | $90 |
| ultra2x | **Most comprehensive** | 5-25 min | 3600s (60 min) | $150 |

## API Reference Summary

### Orchestration Endpoints
- `POST /v1/orchestration/tasks` - Execute single task with full orchestration
- `POST /v1/orchestration/tasks/batch` - Execute multiple tasks in parallel
- `GET /v1/orchestration/stats` - Get orchestrator statistics
- `POST /v1/orchestration/circuit-breaker/reset` - Reset circuit breaker
- `GET /v1/orchestration/health` - Health check

### Webhook Endpoints
- `POST /webhooks/findall` - Receive FindAll webhooks
- `POST /webhooks/tasks` - Receive Task webhooks
- `GET /webhooks/status` - Get handler status

### Schema Sync (Programmatic)
- `OpenAPIFetcher.fetch_schema()` - Fetch latest schema
- `SchemaDiffer.diff_schemas()` - Compare schemas
- `SchemaSyncService.start()` - Start background sync

## MCP Integration

The orchestrated endpoints are designed specifically for MCP clients:

```typescript
// mcp_server.ts
import { Server } from "@modelcontextprotocol/sdk/server/index.js";

const server = new Server({
  name: "parallel-research",
  version: "1.0.0"
});

// Sophisticated tool - NEVER returns URLs
server.tool("research_task", async (args) => {
  const response = await fetch("http://localhost:2040/v1/orchestration/tasks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      processor: args.processor || "lite",
      input: args.query,
      priority: args.priority || "normal",
      timeout: 900,
      cache_key: args.cache_key,
      context_id: args.context_id
    })
  });

  const result = await response.json();

  // Results are ALWAYS returned inline
  return {
    content: [{
      type: "text",
      text: `# Research Results\n\n${result.output_text}\n\n---\n**Metadata:**\n- Duration: ${result.elapsed_seconds}s\n- Cost: $${result.estimated_cost}\n- Run ID: ${result.run_id}`
    }]
  };
});
```

## Conclusion

This architecture provides **production-grade reliability** with:

✅ **Zero Manual Polling** - Everything is automatic
✅ **Zero Platform URLs** - Results always returned inline
✅ **Automatic API Synchronization** - Stays up-to-date with Parallel.ai
✅ **Enterprise-Grade Reliability** - Circuit breakers, retries, caching
✅ **Real-Time Notifications** - Webhook support with HMAC verification
✅ **Comprehensive Observability** - Full metrics and health checks
✅ **MCP-Ready** - Perfect for AI agents and assistants

This is how a proper integration should work.
