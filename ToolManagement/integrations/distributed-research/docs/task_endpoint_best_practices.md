# Task Endpoint Best Practices

## Overview

The Parallel WebSearch Server provides two primary endpoints for creating task runs:

1. **Basic endpoint**: `POST /v1/tasks/runs` - Creates a task and returns immediately
2. **Managed endpoint**: `POST /v1/tasks/runs/managed` - Creates a task, polls until completion, and returns the final result

## When to Use Each Endpoint

### Use the Managed Endpoint (`/v1/tasks/runs/managed`) When:

✅ **Building AI agents or MCP clients** - The managed endpoint handles all polling automatically
✅ **You want results without manual intervention** - No need for users to visit external URLs or check status pages
✅ **You need a simple, synchronous workflow** - One request, one response with final results
✅ **You want automatic timeout handling** - Configurable timeout with graceful failure
✅ **You need status change tracking** - Returns full history of status transitions
✅ **Context persistence is important** - Works seamlessly with `context_id` parameter

**Example Use Cases:**
- MCP tools that need to return task results to users
- AI agents performing research or data extraction
- Automated workflows that need reliable task completion
- Scripts that should wait for task completion before proceeding

### Use the Basic Endpoint (`/v1/tasks/runs`) When:

⚙️ **You need fine-grained control** - Handling polling/streaming yourself
⚙️ **Building custom UIs** - Managing task state in your application
⚙️ **Fire-and-forget tasks** - Don't need to wait for results
⚙️ **Custom retry logic** - Implementing your own failure handling
⚙️ **Integration with existing task queues** - Your system already handles async tasks

**Example Use Cases:**
- Custom dashboards showing multiple task statuses
- Applications with their own task queue system
- Background jobs that notify via webhooks
- Systems that need custom polling intervals or retry logic

## Managed Endpoint Deep Dive

### Input Format Reminder

> `input` and `task_spec` must now be JSON objects that align with your task schema. If your prompt is plain
> text, wrap it in a descriptive field such as `{"query": "Explain the economic impact of AI"}` so
> the server can validate and route it correctly.

### Configuration Parameters

```bash
POST /v1/tasks/runs/managed?poll_interval_seconds=2.0&timeout_seconds=900&context_id=my-context
```

**Parameters:**
- `poll_interval_seconds` (0.5 - 15.0) - How often to check task status (default: 2.0)
- `timeout_seconds` (30 - 7200) - Max wait time before timeout (default: 900 = 15 minutes)
- `context_id` - Optional context for state persistence

### Response Format

The managed endpoint returns a `ManagedTaskRunResponse` with:

```json
{
  "run_id": "trun_abc123",
  "final_status": "completed",
  "summary": "Successfully analyzed market trends...",
  "output_text": "Full analysis text...",
  "output_json": null,
  "elapsed_seconds": 234.5,
  "poll_count": 117,
  "status_updates": [
    {"status": "queued", "timestamp": "2025-11-12T10:00:00Z"},
    {"status": "running", "timestamp": "2025-11-12T10:00:05Z"},
    {"status": "completed", "timestamp": "2025-11-12T10:03:54Z"}
  ],
  "run": { /* Full TaskRun metadata */ },
  "error": null
}
```

### Polling Behavior

The managed endpoint:

1. **Creates the task** via internal API call to Parallel.ai
2. **Polls every N seconds** (configurable) checking for status changes
3. **Tracks all status transitions** (queued → running → completed/failed)
4. **Fetches results** when task reaches terminal status
5. **Returns comprehensive response** with results, timing, and history
6. **Times out gracefully** if task exceeds timeout_seconds

### Error Handling

The managed endpoint handles several error scenarios:

- **Timeout**: Returns HTTP 504 with elapsed time and last known status
- **Task failure**: Returns completed response with error details in the `error` field
- **API errors**: Propagates upstream errors with context
- **Cancelled tasks**: Returns status with cancellation details

## Common Anti-Patterns to Avoid

### ❌ Don't Send Users to Platform URLs

**Bad:**
```python
# Create task
task = await client.post("/v1/tasks/runs", json={...})
run_id = task["run_id"]

# Tell user to check manually
print(f"Visit https://platform.parallel.ai/view/task-run/{run_id} to check progress")
```

**Good:**
```python
# Use managed endpoint - returns results automatically
result = await client.post("/v1/tasks/runs/managed", json={...})
print(f"Results: {result['output_text']}")
```

### ❌ Don't Implement Manual Polling When You Don't Need To

**Bad:**
```python
# Manual polling loop
task = await client.post("/v1/tasks/runs", json={...})
run_id = task["run_id"]

while True:
    status = await client.get(f"/v1/tasks/runs/{run_id}")
    if status["status"] in ["completed", "failed"]:
        break
    await asyncio.sleep(2)

result = await client.get(f"/v1/tasks/runs/{run_id}/result")
```

**Good:**
```python
# Let the server handle polling
result = await client.post("/v1/tasks/runs/managed", json={...})
```

### ❌ Don't Block Users Waiting for External Status Checks

**Bad:**
```
Assistant: I've started your research task (ID: trun_abc123).
It's running on the ultra2x processor and should complete in 5-25 minutes.

Visit https://platform.parallel.ai/view/task-run/trun_abc123 to check real-time progress.

The URL is behind authentication, so you'll need to be logged into your Parallel account.
```

**Good:**
```
Assistant: I've completed your research task. Here are the findings:
[Full results displayed inline]

The analysis took 4.5 minutes and processed 150 sources.
```

## Integration Examples

### Python with httpx

```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:2040/v1/tasks/runs/managed",
        params={
            "poll_interval_seconds": 2.0,
            "timeout_seconds": 600,
            "context_id": "research-session-123"
        },
        json={
            "processor": "lite",
            "input": {
                "query": "Analyze Tesla Q4 earnings"
            },
            "task_spec": {
                "output_schema": {"type": "text"}
            }
        },
        headers={"parallel-beta": "search-extract-2025-10-10"},
        timeout=700.0  # Client timeout slightly higher than server timeout
    )

    result = response.json()
    if result["final_status"] == "completed":
        print(f"Analysis: {result['output_text']}")
    else:
        print(f"Task failed: {result['error']}")
```

### cURL

```bash
curl -X POST "http://localhost:2040/v1/tasks/runs/managed?poll_interval_seconds=2&timeout_seconds=600" \
  -H "Content-Type: application/json" \
  -H "parallel-beta: search-extract-2025-10-10" \
-d '{
    "processor": "lite",
    "input": {
      "query": "Find Y Combinator companies in AI space"
    },
    "task_spec": {
      "output_schema": {
        "type": "json",
        "properties": {
          "companies": {"type": "array"}
        }
      }
    }
  }'
```

### MCP Tool Usage

When exposing tasks via MCP, always use the managed endpoint:

```typescript
// In your MCP server
server.tool("parallel_task", async (args) => {
  const response = await fetch(
    `${baseUrl}/v1/tasks/runs/managed?timeout_seconds=900`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "parallel-beta": "search-extract-2025-10-10"
      },
      body: JSON.stringify({
        processor: args.processor || "lite",
        input: args.input,
        task_spec: args.task_spec
      })
    }
  );

  const result = await response.json();

  // Return results directly to user
  return {
    content: [{
      type: "text",
      text: result.output_text || JSON.stringify(result.output_json, null, 2)
    }]
  };
});
```

## Performance Considerations

### Poll Interval Selection

- **Fast tasks (< 1 min)**: `poll_interval_seconds=0.5` - Minimize latency
- **Medium tasks (1-5 min)**: `poll_interval_seconds=2.0` - Default, good balance
- **Long tasks (5+ min)**: `poll_interval_seconds=5.0` - Reduce API load

### Timeout Selection

Match timeout to processor tier expectations:

| Processor | Typical Duration | Recommended Timeout |
|-----------|------------------|---------------------|
| lite      | 10-30 sec       | 120 sec (2 min)     |
| base      | 30-90 sec       | 300 sec (5 min)     |
| core      | 1-3 min         | 600 sec (10 min)    |
| pro       | 2-5 min         | 900 sec (15 min)    |
| ultra     | 3-10 min        | 1800 sec (30 min)   |
| ultra2x   | 5-25 min        | 3600 sec (60 min)   |

## Migration Guide

### Migrating from Basic to Managed Endpoint

**Before:**
```python
# Old: Create + manual poll + fetch result
task = await create_task(processor="lite", input="...")
run_id = task.run_id

while True:
    status = await get_task_status(run_id)
    if status.status in ["completed", "failed"]:
        break
    await asyncio.sleep(2)

result = await get_task_result(run_id)
return result.output
```

**After:**
```python
# New: Single managed call
result = await create_managed_task(
    processor="lite",
    input="...",
    poll_interval_seconds=2.0,
    timeout_seconds=600
)
return result.output_text or result.output_json
```

## Summary

- **Default to managed endpoint** for AI agents and MCP clients
- **Use basic endpoint** only when you need custom control
- **Never send users** to platform URLs - return results inline
- **Configure timeouts** based on processor tier
- **Use context_id** for state persistence across requests

For more examples, see:
- `/examples/streaming_task.py` - Shows basic endpoint with event streaming
- `/examples/managed_task.py` - Shows managed endpoint usage (recommended)
- `/docs/choose_processor.md` - Processor selection guide
- `README.md` - Complete API documentation
