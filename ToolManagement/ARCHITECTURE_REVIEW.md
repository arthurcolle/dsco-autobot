# Architecture Review: Long-Running Background Process Support

**Date**: 2026-01-20
**Status**: Critical Issues Identified

## Executive Summary

The current architecture has **blocking patterns** that prevent efficient long-running background processes. While individual operations can be fast, the system cannot scale to handle many concurrent long-running tasks efficiently.

---

## Current Architecture Analysis

### 1. **MCP Server Layer** (`mcp_server.py`)

**Issues:**
- ❌ **Hard-coded short timeouts**: 30s (tool execution), 60s (workflows)
- ❌ **No connection pooling**: Creates new `httpx.AsyncClient` per request
- ❌ **Synchronous waiting**: All meta-tool calls block waiting for HTTP responses
- ❌ **No background job pattern**: Tools execute inline, blocking the request

```python
# Line 555: Execute tool - blocks for up to 30 seconds
response = await client.post(
    "http://localhost:2016/api/v1/gateway/execute",
    json={"tool_name": tool_args.get("tool_name"), "arguments": tool_args.get("arguments", {})},
    timeout=30.0  # ❌ Blocks for 30s
)
```

**Impact:**
- Cannot handle tasks that take >30 seconds
- Each blocked request consumes a connection/worker
- No way to track long-running job status

---

### 2. **Gateway Layer** (`gateway_routes.py`)

**Issues:**
- ❌ **Synchronous execution model**: `execute_tool()` waits for completion
- ❌ **No job queue integration**: Doesn't use background workers properly
- ❌ **Fixed timeout in Tools class**: 30s default timeout
- ❌ **No webhook/callback mechanism**

```python
# Line 179: Tool execution blocks until complete
async def execute_tool(tool_name: str, inputs: Dict[str, Any], ...):
    # ... routes and executes, waiting for result
    result = await local_tool.execute(**inputs)  # ❌ Blocks
    return {"result": result, "cached": False}
```

**Good:**
- ✅ Has task queue infrastructure (`DurableTaskQueue`)
- ✅ Has streaming endpoints (SSE) for progress updates
- ✅ Supports caching and routing

**Missing:**
- Background job submission endpoint (`POST /gateway/jobs/submit`)
- Job status polling endpoint (`GET /gateway/jobs/{job_id}`)
- Job cancellation (`DELETE /gateway/jobs/{job_id}`)
- Webhook callbacks when jobs complete

---

### 3. **Task Queue** (`task_queue.py`)

**Issues:**
- ❌ **Single worker thread**: `_process_queue()` loop processes one task at a time
- ❌ **No worker pool**: `max_concurrent` limits total queue, not per-backend
- ❌ **Polling-based**: No event-driven task pickup
- ❌ **HTTP timeouts**: Backend requests use task timeout (default 300s) but httpx will timeout first

```python
# Line 646: Single-threaded task processing
async def _process_queue(self):
    while not self._shutdown:
        task = self._get_next_task()  # Gets ONE task
        if not task:
            await asyncio.sleep(1)  # ❌ Wastes time polling
            continue
        backend = self.get_available_backend()
        await self._execute_task(task, backend)  # ❌ Waits for completion
```

**Good:**
- ✅ Persistent SQLite storage (survives restarts)
- ✅ Retry logic with exponential backoff
- ✅ Priority queue support
- ✅ Admin operations (cancel, reprioritize)
- ✅ Health checking for backends

**Missing:**
- Worker pool (should have N concurrent workers)
- Event-driven task dispatch (use `asyncio.Queue`)
- Per-backend concurrency limits
- Circuit breaker for failing backends

---

### 4. **Compose/Workflow Layer** (`compose_patterns.py`)

**Issues:**
- ❌ **Sequential workflows block**: Each step waits for previous
- ❌ **No intermediate persistence**: If pipeline fails mid-way, all lost
- ❌ **Parallel execution uses `gather`**: Limited to request lifetime

```python
# Line 235: Sequential steps block each other
for i, tool_name in enumerate(request.tools):
    result = await _execute_tool_direct(tool_name, mapped_args)  # ❌ Blocks
    current_output = result
```

**Good:**
- ✅ SSE streaming for real-time progress
- ✅ Smart parameter mapping between tools
- ✅ Parallel fork pattern

**Missing:**
- Workflow persistence (save intermediate state)
- Workflow resumption (restart from failure point)
- Async workflow submission (return job ID immediately)

---

### 5. **Tools API** (`tools_api.py`)

**Issues:**
- ❌ **HTTP client timeout**: 30s default
- ❌ **No connection pooling**: Creates client per execution
- ❌ **Snippet execution is synchronous**: Blocks event loop

```python
# Line 328: Short timeout
def __init__(self, ..., timeout: int = 30):
    self.timeout = timeout  # ❌ 30 second limit

# Line 345: No connection reuse
async def _get_client(self) -> httpx.AsyncClient:
    if self._client is None or self._client.is_closed:
        self._client = httpx.AsyncClient(timeout=self.timeout)
```

---

## Critical Architectural Problems

### Problem 1: **No True Background Job Pattern**

**Current Flow:**
```
Client → MCP Server → Gateway → Tool Execution (wait) → Response
         ↓ (blocks until complete)
```

**Should Be:**
```
Client → MCP Server → Gateway → Enqueue Job → Return Job ID
                                      ↓
                               Background Worker
                                      ↓
                               Poll Status or Webhook
```

### Problem 2: **Connection Pool Starvation**

- Every long-running request holds a connection
- New `httpx.AsyncClient()` per request (no pooling)
- No limits on concurrent connections to backends

### Problem 3: **Timeout Cascade**

```
MCP Server (30s) → Gateway (30s) → Backend Tool (???)
           ↓              ↓
        Times out    Times out
```

If a tool takes 40 seconds:
1. Backend completes successfully
2. Gateway times out at 30s
3. MCP server times out at 30s
4. Client gets error despite success

### Problem 4: **No Circuit Breaker**

- Failing backend gets hammered with requests
- No exponential backoff for backend failures
- Health checks run periodically but don't prevent routing to unhealthy backends

---

## Recommended Architectural Changes

### 1. **Add Background Job API Pattern**

#### New Endpoints (add to `gateway_routes.py`):

```python
@router.post("/jobs/submit")
async def submit_job(request: ExecuteRequest, client_id: str = Depends(get_client_id)):
    """Submit a tool for background execution. Returns immediately with job ID."""
    queue = get_task_queue()
    task = queue.enqueue(
        name=request.tool,
        input_data=request.inputs,
        priority=TaskPriority.NORMAL,
        timeout_seconds=request.options.get("timeout", 3600),  # 1 hour default
        metadata={"client_id": client_id, "submitted_at": datetime.utcnow().isoformat()}
    )
    return {
        "job_id": task.id,
        "status": "queued",
        "submitted_at": task.created_at.isoformat()
    }

@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll job status."""
    queue = get_task_queue()
    task = queue.get_task(job_id)
    if not task:
        raise HTTPException(404, "Job not found")
    return {
        "job_id": task.id,
        "status": task.status.value,
        "result": task.output_data,
        "error": task.error,
        "created_at": task.created_at.isoformat(),
        "completed_at": task.completed_at.isoformat() if task.completed_at else None
    }

@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running/queued job."""
    queue = get_task_queue()
    cancelled = queue.cancel_task(job_id, "Cancelled by user")
    return {"cancelled": cancelled}

@router.post("/jobs/{job_id}/webhook")
async def set_job_webhook(job_id: str, webhook_url: str):
    """Set webhook to be called when job completes."""
    # Store webhook URL in task metadata
    queue = get_task_queue()
    task = queue.get_task(job_id)
    if task:
        task.metadata["webhook_url"] = webhook_url
        queue.update_task(task)
        return {"webhook_set": True}
    raise HTTPException(404, "Job not found")
```

#### Update Task Queue to Support Webhooks:

```python
# In task_queue.py, after task completes:
async def _execute_task(self, task: Task, backend: MCPBackend):
    try:
        # ... existing execution code ...
        task.status = TaskStatus.COMPLETED

        # Call webhook if configured
        webhook_url = task.metadata.get("webhook_url")
        if webhook_url:
            await self._call_webhook(webhook_url, task)

    finally:
        # ... cleanup ...

async def _call_webhook(self, url: str, task: Task):
    """Notify webhook about task completion."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={
                "job_id": task.id,
                "status": task.status.value,
                "result": task.output_data,
                "error": task.error,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None
            })
    except Exception as e:
        logger.error(f"Webhook call failed for {url}: {e}")
```

---

### 2. **Implement Worker Pool in Task Queue**

**Replace single worker with pool:**

```python
class DurableTaskQueue:
    def __init__(self, ..., num_workers: int = 10):
        self.num_workers = num_workers
        self._workers: List[asyncio.Task] = []
        self._task_queue: asyncio.Queue = asyncio.Queue()

    async def start(self):
        """Start worker pool."""
        self._shutdown = False

        # Start workers
        for i in range(self.num_workers):
            worker = asyncio.create_task(self._worker(i))
            self._workers.append(worker)

        # Start task feeder
        self._feeder_task = asyncio.create_task(self._feed_tasks())
        self._health_task = asyncio.create_task(self._health_check_loop())

        logger.info(f"Task queue started with {self.num_workers} workers")

    async def _feed_tasks(self):
        """Feed tasks from DB into async queue."""
        while not self._shutdown:
            try:
                task = self._get_next_task_from_db()
                if task:
                    await self._task_queue.put(task)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Task feeder error: {e}")
                await asyncio.sleep(5)

    async def _worker(self, worker_id: int):
        """Worker that processes tasks from queue."""
        logger.info(f"Worker {worker_id} started")
        while not self._shutdown:
            try:
                # Get task from async queue (non-blocking)
                task = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)

                # Find available backend
                backend = self.get_available_backend()
                if not backend:
                    # Put task back and wait
                    await self._task_queue.put(task)
                    await asyncio.sleep(5)
                    continue

                # Execute task
                await self._execute_task(task, backend)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
```

**Benefits:**
- ✅ N workers process tasks concurrently
- ✅ Event-driven (no polling sleep)
- ✅ Scales to handle many long-running tasks
- ✅ Worker crash doesn't stop queue

---

### 3. **Add Connection Pooling**

**Update Tools API:**

```python
class Tools:
    def __init__(self, ..., timeout: int = 3600, max_connections: int = 100):
        self.timeout = timeout

        # Use persistent client with connection limits
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=10.0),
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=max_connections,
                keepalive_expiry=30.0
            )
        )
```

**Update MCP Server:**

```python
class MCPServer:
    def __init__(self, tool_storage: Dict[str, Any]):
        # ... existing code ...

        # Shared HTTP client with connection pooling
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0),
            limits=httpx.Limits(
                max_keepalive_connections=50,
                max_connections=200
            )
        )

    async def _handle_tools_call(self, params: Dict[str, Any], session_id: str):
        # Reuse self._http_client instead of creating new clients
        async with self._http_client as client:
            # ... execute requests ...
```

---

### 4. **Increase Timeouts for Long-Running Tasks**

**Configurable timeouts:**

```python
# In mcp_server.py
async def _handle_tools_call(self, params: Dict[str, Any], session_id: str):
    tool_name = params.get("name")
    tool_args = params.get("arguments", {})

    # Get timeout from tool metadata or default
    timeout = tool_args.pop("_timeout", 3600)  # 1 hour default

    if tool_name == "execute_tool":
        response = await client.post(
            "http://localhost:2016/api/v1/gateway/execute",
            json={...},
            timeout=timeout  # Use configurable timeout
        )
```

---

### 5. **Add Circuit Breaker Pattern**

```python
from datetime import datetime, timedelta
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failures detected, blocking requests
    HALF_OPEN = "half_open"  # Testing if backend recovered

@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    timeout_duration: int = 60  # seconds
    success_threshold: int = 2

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker closed (recovered)")
        else:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened (failures: {self.failure_count})")

    def can_execute(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.timeout_duration:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker half-open (testing recovery)")
                    return True
            return False

        # HALF_OPEN: allow requests to test recovery
        return True

# Add to MCPBackend:
@dataclass
class MCPBackend:
    # ... existing fields ...
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)

# Use in task execution:
async def _execute_task(self, task: Task, backend: MCPBackend):
    if not backend.circuit_breaker.can_execute():
        raise Exception(f"Backend {backend.name} circuit breaker is OPEN")

    try:
        # ... execute task ...
        backend.circuit_breaker.record_success()
    except Exception as e:
        backend.circuit_breaker.record_failure()
        raise
```

---

## Implementation Priority

### Phase 1: Critical (Do First)
1. ✅ **Fix error handling in MCP server** (DONE)
2. **Add background job API** (`/jobs/submit`, `/jobs/{id}`)
3. **Implement worker pool** in task queue
4. **Add connection pooling** to HTTP clients

### Phase 2: Important
5. **Increase configurable timeouts**
6. **Add circuit breaker** to backends
7. **Webhook callbacks** for job completion

### Phase 3: Nice to Have
8. Workflow persistence and resumption
9. Job result caching
10. Job scheduling (cron-like)

---

## Testing Long-Running Processes

### Test Case 1: Submit Long Job
```bash
# Submit a job that takes 5 minutes
curl -X POST http://localhost:2016/gateway/jobs/submit \
  -H "Content-Type: application/json" \
  -d '{
    "tool": "long_running_task",
    "inputs": {"duration_seconds": 300},
    "options": {"timeout": 600}
  }'

# Response: {"job_id": "task_abc123", "status": "queued"}
```

### Test Case 2: Poll Job Status
```bash
curl http://localhost:2016/gateway/jobs/task_abc123

# Response while running:
# {"job_id": "task_abc123", "status": "running", "result": null}

# Response when complete:
# {"job_id": "task_abc123", "status": "completed", "result": {...}}
```

### Test Case 3: Many Concurrent Jobs
```python
import asyncio
import httpx

async def submit_jobs(count=100):
    async with httpx.AsyncClient() as client:
        tasks = [
            client.post("http://localhost:2016/gateway/jobs/submit", json={
                "tool": "process_data",
                "inputs": {"item_id": i},
                "options": {"timeout": 1800}
            })
            for i in range(count)
        ]
        responses = await asyncio.gather(*tasks)
        print(f"Submitted {len(responses)} jobs")

asyncio.run(submit_jobs(100))
```

---

## Summary

### Current State
- ❌ Synchronous execution blocks connections
- ❌ Hard-coded short timeouts
- ❌ Single-threaded task processing
- ❌ No true background job pattern

### After Changes
- ✅ Async job submission (returns immediately)
- ✅ Worker pool handles N concurrent tasks
- ✅ Connection pooling prevents starvation
- ✅ Configurable timeouts (up to hours)
- ✅ Circuit breaker protects failing backends
- ✅ Webhook callbacks for completion

### Performance Impact
- **Current**: Max ~10 concurrent requests (limited by blocking)
- **After**: Max ~1000+ concurrent jobs (limited by worker pool + backend capacity)
- **Latency**: Job submission <50ms (vs 30s+ for synchronous)
