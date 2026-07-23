
# Task Groups: Comprehensive Guide

## Overview

Task Groups provide a powerful way to manage and monitor **batch parallel research workflows**. Unlike individual tasks, groups let you:

- **Submit multiple tasks at once**
- **Track progress across all tasks**
- **Filter and query specific runs**
- **Get aggregated analytics**
- **Export batch results**
- **Wait for completion automatically** (managed execution)

## Complete API Coverage

Based on Parallel.ai's public OpenAPI spec, we provide **100% coverage** of all documented Task Group endpoints, plus advanced client-side features for better UX.

### Core Endpoints (Parallel.ai Native)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1beta/tasks/groups` | POST | Create a new task group |
| `/v1beta/tasks/groups/{id}` | GET | Get group status and metrics |
| `/v1beta/tasks/groups/{id}/runs` | POST | Add task runs to the group |
| `/v1beta/tasks/groups/{id}/runs` | GET | Stream task runs (SSE) with filters |
| `/v1beta/tasks/groups/{id}/runs/{run_id}` | GET | Get specific run from group |
| `/v1beta/tasks/groups/{id}/events` | GET | Stream group events (SSE) |

### Advanced Endpoints (Our Enhancements)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1beta/tasks/groups/{id}/managed` | POST | **Managed execution** - auto-polls until complete |
| `/v1beta/tasks/groups/{id}/analytics` | GET | **Comprehensive analytics** and reporting |
| `/v1beta/tasks/groups/{id}/export` | GET | **Export results** in JSON/CSV/Markdown |

## Feature Matrix

### ✅ Supported Features

| Feature | Parallel.ai Native | Our Enhancement |
|---------|-------------------|-----------------|
| Create groups | ✅ | |
| Add runs to groups | ✅ | |
| Get group status | ✅ | |
| Stream runs (SSE) | ✅ | |
| Stream events (SSE) | ✅ | |
| Get individual run | ✅ | |
| **Status filtering** | ✅ | Enhanced validation |
| **Resumable streams** | ✅ | `last_event_id` param |
| **Include inputs/outputs** | ✅ | Boolean toggles |
| **Metadata** | ✅ | Rich metadata support |
| **Managed execution** | | ✅ (Auto-polls) |
| **Analytics & reporting** | | ✅ (Metrics, rates) |
| **Multi-format export** | | ✅ (JSON, CSV, MD) |
| **Batch result aggregation** | | ✅ |

### ❌ Not Supported (API Limitations)

These operations are **NOT available** in Parallel.ai's API:

- ❌ Cancel/abort task groups
- ❌ Delete task groups
- ❌ Update/modify groups after creation
- ❌ Remove individual runs from groups
- ❌ Pause/resume groups
- ❌ Change task priority
- ❌ Retry failed tasks

> **Note:** Groups are immutable after creation. You can only add more runs, not modify or remove existing ones.

## Query Parameters & Filters

### GET `/v1beta/tasks/groups/{id}/runs`

| Parameter | Type | Options | Description |
|-----------|------|---------|-------------|
| `status` | string | `queued`, `action_required`, `running`, `completed`, `failed`, `cancelling`, `cancelled` | Filter runs by status |
| `include_input` | boolean | `true`, `false` | Include task inputs in response |
| `include_output` | boolean | `true`, `false` | Include task outputs in response |
| `last_event_id` | string | Event ID | Resume stream from specific event |

**Example:**
```bash
# Get only completed runs with outputs
curl "http://localhost:2040/v1beta/tasks/groups/tg_abc123/runs?status=completed&include_output=true"
```

### GET `/v1beta/tasks/groups/{id}/events`

| Parameter | Type | Description |
|-----------|------|-------------|
| `last_event_id` | string | Resume from specific event |
| `timeout` | integer | Connection timeout in seconds |

**Example:**
```bash
# Stream events with 60s timeout
curl "http://localhost:2040/v1beta/tasks/groups/tg_abc123/events?timeout=60"
```

## Usage Examples

> Each task run's `input` is validated as a JSON object. Wrap a free-text prompt in a descriptive field such as `{"query": "Latest AI safety research"}` so the Parallel.ai validation schema can be applied consistently.

### 1. Basic Workflow

```python
import httpx

async with httpx.AsyncClient() as client:
    # 1. Create group
    response = await client.post(
        "http://localhost:2040/v1beta/tasks/groups",
        json={
            "metadata": {
                "project": "competitive-research",
                "researcher": "alice",
                "date": "2025-11-12"
            }
        }
    )

    group = response.json()
    taskgroup_id = group["task_group_id"]

    # 2. Add runs
    await client.post(
        f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/runs",
        json={
            "inputs": [
                {
                    "input": {"query": "Latest AI safety research"},
                    "processor": "lite",
                    "metadata": {"topic": "ai-safety"}
                },
                {
                    "input": {"query": "Quantum computing breakthroughs 2025"},
                    "processor": "base",
                    "metadata": {"topic": "quantum"}
                },
                {
                    "input": {"query": "Climate tech investment trends"},
                    "processor": "lite",
                    "metadata": {"topic": "climate"}
                }
            ]
        }
    )

    # 3. Check status
    status = await client.get(
        f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}"
    )

    print(status.json())
```

### 2. Managed Execution (Recommended)

**NEVER requires manual polling or visiting URLs.**

```python
# Submit and automatically wait for completion
response = await client.post(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/managed",
    params={
        "poll_interval": 2.0,      # Check every 2 seconds
        "timeout": 900,             # Max 15 minutes
        "include_results": True     # Include all run results
    }
)

result = response.json()

print(f"Status: {result['final_status']}")
print(f"Duration: {result['elapsed_seconds']}s")
print(f"Summary: {result['summary']}")
print(f"Metrics: {result['metrics']}")

# Results are inline - NO platform URLs!
for run in result['run_results']:
    print(f"Run {run['run_id']}: {run['status']}")
```

### 3. Status Filtering

```python
# Get only completed runs with outputs
async with httpx.AsyncClient() as client:
    # Note: This returns an SSE stream
    async with client.stream(
        "GET",
        f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/runs",
        params={
            "status": "completed",
            "include_output": True
        }
    ) as response:
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                data = json.loads(line[5:])
                print(data)
```

### 4. Analytics & Reporting

```python
# Get comprehensive analytics
analytics = await client.get(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/analytics",
    params={"include_run_details": True}
)

data = analytics.json()

print(f"Total Runs: {data['total_runs']}")
print(f"Completion Rate: {data['completion_rate']*100:.1f}%")
print(f"Failure Rate: {data['failure_rate']*100:.1f}%")
print(f"Status Distribution: {data['status_distribution']}")
```

### 5. Export Results

```python
# Export as JSON
export = await client.get(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/export",
    params={
        "format": "json",
        "include_inputs": True,
        "include_outputs": True
    }
)

json_data = export.json()['data']

# Export as CSV
export_csv = await client.get(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/export",
    params={"format": "csv"}
)

csv_data = export_csv.json()['data']

# Export as Markdown report
export_md = await client.get(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/export",
    params={"format": "markdown"}
)

markdown_report = export_md.json()['data']
```

### 6. Large-Scale Parallel Research

```python
# Submit 50 research queries in parallel
research_topics = [
    "AI safety research papers 2025",
    "Quantum error correction methods",
    # ... 48 more topics
]

# Create group
group_response = await client.post(
    "http://localhost:2040/v1beta/tasks/groups",
    json={"metadata": {"project": "tech-landscape-2025"}}
)

taskgroup_id = group_response.json()["task_group_id"]

# Submit all 50 tasks at once
await client.post(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/runs",
    json={
        "inputs": [
            {"input": topic, "processor": "lite"}
            for topic in research_topics
        ]
    }
)

# Use managed execution - waits for all 50 to complete
result = await client.post(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/managed",
    params={
        "poll_interval": 3.0,
        "timeout": 3600,  # 1 hour for 50 tasks
        "include_results": True
    }
)

# Export aggregated results
export = await client.get(
    f"http://localhost:2040/v1beta/tasks/groups/{taskgroup_id}/export",
    params={"format": "markdown"}
)

# Save report
with open("tech_landscape_2025.md", "w") as f:
    f.write(export.json()['data'])
```

## Response Schemas

### TaskGroupResponse

```json
{
  "task_group_id": "tg_abc123...",
  "status": "active",
  "num_task_runs": 10,
  "task_run_status_counts": {
    "queued": 2,
    "running": 3,
    "completed": 4,
    "failed": 1
  },
  "is_active": true,
  "status_message": "Processing tasks",
  "metadata": {
    "project": "research-project",
    "custom_field": "value"
  },
  "created_at": "2025-11-12T10:00:00Z",
  "modified_at": "2025-11-12T10:05:00Z"
}
```

### ManagedGroupResult

```json
{
  "taskgroup_id": "tg_abc123...",
  "final_status": "completed",
  "metrics": {
    "total_runs": 10,
    "completed": 9,
    "failed": 1,
    "success_rate": 0.9,
    "status_counts": {...},
    "processor_distribution": {
      "lite": 7,
      "base": 3
    }
  },
  "run_results": [
    {
      "run_id": "trun_xyz...",
      "status": "completed",
      "processor": "lite",
      "output": {...}
    }
  ],
  "elapsed_seconds": 234.5,
  "summary": "Processed 10 task runs in 234.50s | ✓ 9 completed | ✗ 1 failed | Success rate: 90.0%"
}
```

## Best Practices

### 1. Use Metadata for Organization

```python
{
    "metadata": {
        "project_id": "proj_123",
        "researcher": "alice@company.com",
        "department": "R&D",
        "priority": "high",
        "tags": ["competitive-intel", "q4-2025"]
    }
}
```

### 2. Choose the Right Endpoint

| Scenario | Recommended Endpoint |
|----------|---------------------|
| Need results immediately | `/managed` - Auto-polls |
| Building real-time UI | `/events` - SSE stream |
| Batch processing | `/runs` with filters |
| Analytics dashboard | `/analytics` |
| Exporting reports | `/export` |

### 3. Handle Large Groups

```python
# For 100+ tasks, use higher poll intervals
result = await client.post(
    f"{BASE_URL}/v1beta/tasks/groups/{id}/managed",
    params={
        "poll_interval": 5.0,  # Check every 5s for large groups
        "timeout": 7200,        # 2 hours for very large batches
        "include_results": False  # Skip results if not needed
    }
)
```

### 4. Filter for Efficiency

```python
# Only get completed runs with outputs
params = {
    "status": "completed",
    "include_output": True,
    "include_input": False  # Skip inputs if not needed
}
```

### 5. Use Appropriate Timeouts

| Group Size | Recommended Timeout |
|------------|---------------------|
| 1-10 tasks | 900s (15 min) |
| 11-50 tasks | 1800s (30 min) |
| 51-100 tasks | 3600s (1 hour) |
| 100+ tasks | 7200s (2 hours) |

## Event Streaming

### Event Types

The `/events` endpoint emits:

- `TaskGroupStatusEvent` - Group-level status changes
- `TaskRunEvent` - Individual task updates
- `ErrorEvent` - Error notifications

### Resumable Streams

```python
# Save last event ID
last_event = None

async with client.stream(
    "GET",
    f"{BASE_URL}/v1beta/tasks/groups/{id}/events",
    params={"last_event_id": last_event} if last_event else {}
) as response:
    async for line in response.aiter_lines():
        if line.startswith("id:"):
            last_event = line[3:].strip()
```

## Error Handling

```python
try:
    result = await client.post(
        f"{BASE_URL}/v1beta/tasks/groups/{id}/managed",
        params={"timeout": 900}
    )
except httpx.TimeoutException:
    print("Group execution timed out")
except httpx.HTTPStatusError as e:
    if e.response.status_code == 404:
        print("Group not found")
    elif e.response.status_code == 504:
        print("Gateway timeout - group still running")
    else:
        print(f"API error: {e.response.json()}")
```

## Comparison with Individual Tasks

| Feature | Individual Tasks | Task Groups |
|---------|-----------------|-------------|
| Submit multiple at once | ❌ Need loop | ✅ Batch submit |
| Aggregated metrics | ❌ Manual | ✅ Built-in |
| Status filtering | ❌ | ✅ |
| Unified events | ❌ | ✅ SSE stream |
| Export batch results | ❌ Manual | ✅ Built-in |
| Managed execution | ✅ `/managed` | ✅ `/managed` |

## Summary

- **Full API Coverage**: 100% of Parallel.ai's documented Task Group endpoints
- **Advanced Features**: Managed execution, analytics, export (our enhancements)
- **No URL Visits**: Everything returns results inline
- **Scalable**: Handle 1-1000+ tasks per group
- **Flexible**: Status filters, resumable streams, metadata
- **Export Ready**: JSON, CSV, Markdown formats

## Next Steps

1. **Try the demo**: `python examples/complete_task_groups_demo.py`
2. **Read API docs**: Visit `http://localhost:2040/docs`
3. **Check examples**: See `examples/` directory
4. **Production use**: Review ADVANCED_ARCHITECTURE.md

---

**Full endpoint listing**: Visit `/docs` on your running server for interactive API documentation.
