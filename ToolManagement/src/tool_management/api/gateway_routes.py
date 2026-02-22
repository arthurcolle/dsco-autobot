"""
Unified Gateway API Routes.

The main entry point for the Tool Management Gateway.
Combines all sophisticated features:
- Tool discovery and execution
- Pipelines and composition
- Intelligent routing
- Caching
- Rate limiting
- Streaming
- Batch execution
- Metrics
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..registry import get_registry
from ..queue import get_task_queue
from ..contracts import ToolInputBase, ToolOutputBase, BudgetEnforcer, ensure_base_fields, UsageMetrics

# Core components
from ..core.pipeline import Pipeline, PipelineStep, PipelineExecutor, StepType
from ..core.router import get_router, RoutingStrategy, BackendStatus
from ..core.cache import get_cache, CachePolicy
from ..core.rate_limit import get_quota_manager, ClientQuota, RateLimitResult
from ..core.metrics import get_metrics_collector, RequestTrace
from ..core.streaming import get_streaming_executor, EventType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gateway", tags=["gateway"])


# =============================================================================
# Request/Response Models
# =============================================================================

class ExecuteRequest(BaseModel):
    """
    Request to execute a tool.

    Uses `input` for a single execution (the common case).
    The input is a Pydantic-style object with typed attributes.

    All tools support these base budget fields in `input`:
        - max_tokens: int (default 4096) - Maximum tokens for response
        - timeout_ms: int (default 30000) - Timeout in milliseconds
        - max_cost_cents: int (optional) - Maximum cost in cents
        - max_retries: int (default 3) - Maximum retry attempts
        - max_iterations: int (optional) - Max iterations for iterative tools
        - priority: str (default "normal") - Execution priority
        - mode: str (default "sync") - Execution mode
        - use_cache: bool (default True) - Whether to use cache
    """
    tool: str = Field(..., description="Tool name or namespace:tool")
    input: Dict[str, Any] = Field(default_factory=dict, description="Tool input object")
    options: Optional[Dict[str, Any]] = Field(default=None, description="Execution options (deprecated, use input)")


class ExecuteResponse(BaseModel):
    """Response from tool execution."""
    execution_id: str
    tool: str
    status: str
    result: Any = None
    error: Optional[str] = None
    duration_ms: float
    cached: bool = False
    backend: Optional[str] = None
    trace_id: Optional[str] = None

    # Budget tracking
    usage: Optional[Dict[str, Any]] = Field(default=None, description="Resource usage metrics")
    budget_warnings: List[str] = Field(default_factory=list, description="Budget warning messages")


class BatchToolCall(BaseModel):
    """A single tool call within a batch."""
    tool: str = Field(..., description="Tool name or namespace:tool")
    input: Dict[str, Any] = Field(default_factory=dict, description="Tool input object")


class BatchExecuteRequest(BaseModel):
    """
    Request to execute multiple tools.

    Uses `inputs` as a list of input objects, one per tool call.
    For heterogeneous batch (different tools), use `calls`.
    For homogeneous batch (same tool, different inputs), use `tool` + `inputs`.
    """
    # Heterogeneous batch: different tools
    calls: Optional[List[BatchToolCall]] = Field(default=None, description="Tool calls to execute")

    # Homogeneous batch: same tool, multiple inputs
    tool: Optional[str] = Field(default=None, description="Tool name for homogeneous batch")
    inputs: Optional[List[Dict[str, Any]]] = Field(default=None, description="List of input objects")

    parallel: bool = Field(default=True, description="Execute in parallel")
    fail_fast: bool = Field(default=False, description="Stop on first error")

    def get_calls(self) -> List[BatchToolCall]:
        """Normalize to list of BatchToolCall."""
        if self.calls:
            return self.calls
        if self.tool and self.inputs:
            return [BatchToolCall(tool=self.tool, input=inp) for inp in self.inputs]
        return []


class BatchExecuteResponse(BaseModel):
    """Response from batch execution."""
    results: List[ExecuteResponse]
    total_duration_ms: float
    success_count: int
    error_count: int


class PipelineRequest(BaseModel):
    """Request to execute a pipeline."""
    name: Optional[str] = None
    steps: List[Dict[str, Any]] = Field(..., description="Pipeline steps")
    inputs: Dict[str, Any] = Field(default_factory=dict)


class PipelineResponse(BaseModel):
    """Response from pipeline execution."""
    execution_id: str
    pipeline_name: str
    status: str
    output: Any = None
    step_results: Dict[str, Any] = Field(default_factory=dict)
    duration_ms: float


class StreamExecuteRequest(BaseModel):
    """Request for streaming execution."""
    tool: str
    input: Dict[str, Any] = Field(default_factory=dict)


class SearchRequest(BaseModel):
    """Request to search for tools."""
    query: str = Field(..., description="Natural language query")
    limit: int = Field(default=10, ge=1, le=50)
    backends: Optional[List[str]] = None
    categories: Optional[List[str]] = None


class QuotaRequest(BaseModel):
    """Request to set client quota."""
    client_id: str
    requests_per_second: int = 10
    requests_per_minute: int = 100
    requests_per_hour: int = 1000
    max_concurrent: int = 10


# =============================================================================
# Dependencies
# =============================================================================

async def get_client_id(request: Request) -> str:
    """Extract client ID from request."""
    # Check header first
    client_id = request.headers.get("X-Client-ID")
    if client_id:
        return client_id

    # Fall back to API key
    api_key = request.headers.get("Authorization", "").replace("Bearer ", "")
    if api_key:
        return f"key:{api_key[:8]}"

    # Fall back to IP
    return f"ip:{request.client.host}" if request.client else "anonymous"


async def check_rate_limit(
    request: Request,
    client_id: str = Depends(get_client_id),
) -> RateLimitResult:
    """Check rate limits for request."""
    quota_manager = get_quota_manager()
    tool_name = None

    # Try to extract tool name from request body
    try:
        body = await request.json()
        tool_name = body.get("tool")
    except Exception:
        pass

    result = quota_manager.check_and_consume(client_id, tool_name)

    if not result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "reason": result.reason,
                "retry_after": result.retry_after,
                "limit": result.limit,
                "remaining": result.remaining,
            },
            headers={"Retry-After": str(int(result.retry_after))},
        )

    return result


# =============================================================================
# Tool Executor
# =============================================================================

async def execute_tool(
    tool_name: str,
    inputs: Dict[str, Any],
    client_id: str = None,
    use_cache: bool = True,
    progress_callback=None,
) -> Dict[str, Any]:
    """
    Execute a tool with full gateway features.

    Includes:
    - Budget enforcement (tokens, time, cost)
    - Caching
    - Routing
    - Metrics
    - Error handling

    All inputs are merged with base contract defaults to ensure
    budget fields (max_tokens, timeout_ms, etc.) are always present.
    """
    # Ensure all base contract fields are present
    full_inputs = ensure_base_fields(inputs)
    contract_input = ToolInputBase.model_validate(full_inputs)

    # Create budget enforcer
    enforcer = BudgetEnforcer(contract_input)

    # Override use_cache from contract if specified
    use_cache = contract_input.use_cache if contract_input.use_cache is not None else use_cache

    # Separate contract fields from tool-specific inputs
    contract_fields = {
        'max_tokens', 'timeout_ms', 'max_cost_cents', 'priority', 'mode',
        'max_retries', 'max_iterations', 'first_chunk_timeout_ms',
        'use_cache', 'cache_ttl_seconds', 'trace_id', 'span_id', 'metadata'
    }
    tool_inputs = {k: v for k, v in inputs.items() if k not in contract_fields}

    cache = get_cache()
    intelligent_router = get_router()
    metrics = get_metrics_collector()
    registry = get_registry()
    queue = get_task_queue()

    # Start trace
    trace = metrics.start_trace(tool_name=tool_name, client_id=client_id)

    try:
        # Check cache (use tool_inputs for cache key, not contract fields)
        if use_cache:
            cached_result = cache.get(tool_name, tool_inputs)
            if cached_result is not None:
                trace.complete(status="success")
                metrics.end_trace(trace, status="success")
                return {
                    "result": cached_result,
                    "cached": True,
                    "trace_id": trace.trace_id,
                }

        # Parse tool name for backend hint
        backend_hint = None
        actual_tool = tool_name
        if "::" in tool_name:
            backend_hint, actual_tool = tool_name.split("::", 1)

        # Route to backend
        routing_span = trace.start_span("routing")
        decision = intelligent_router.route(
            tool_name=actual_tool,
            prefer_backend=backend_hint,
        )
        trace.end_span(routing_span)

        backend_id = decision.backend_id if decision else None

        # Try local registry first
        local_tool = registry.get_by_name(actual_tool)

        if local_tool:
            # Execute locally with tool-specific inputs only
            exec_span = trace.start_span("execute_local")
            result = await local_tool.execute(**tool_inputs)
            trace.end_span(exec_span)
        elif backend_id:
            # Execute via queue/backend with full inputs (backend handles contract)
            exec_span = trace.start_span("execute_remote")
            task = queue.enqueue(
                name=actual_tool,
                input_data=full_inputs,  # Remote backends get full input with contract
                backend=backend_id,
            )
            # Wait for completion (simplified - real impl would poll)
            await asyncio.sleep(0.1)
            completed_task = queue.get_task(task.id)
            result = completed_task.output_data if completed_task else None
            trace.end_span(exec_span)

            # Record routing metrics
            if decision:
                intelligent_router.release_connection(
                    backend_id,
                    success=True,
                    latency_ms=trace.duration_ms,
                )
        else:
            raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")

        # Cache result
        if use_cache:
            cache.set(tool_name, tool_inputs, result)

        # Track time usage
        enforcer.add_time_ms(trace.duration_ms if hasattr(trace, 'duration_ms') else enforcer.elapsed_ms())

        # Complete trace
        trace.complete(status="success")
        metrics.end_trace(trace, status="success", backend_id=backend_id)

        # Get usage metrics
        usage = enforcer.get_usage()

        return {
            "result": result,
            "cached": False,
            "backend": backend_id,
            "trace_id": trace.trace_id,
            "usage": usage.model_dump(),
            "budget_warnings": enforcer.get_warnings(),
        }

    except HTTPException:
        raise
    except Exception as e:
        trace.complete(status="error", error=str(e))
        metrics.end_trace(trace, status="error", error=str(e))

        # Include usage even on error
        usage = enforcer.get_usage()
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "usage": usage.model_dump(),
            }
        )


# =============================================================================
# Gateway Endpoints
# =============================================================================

@router.get("")
async def gateway_info():
    """Get gateway information and capabilities."""
    registry = get_registry()
    intelligent_router = get_router()
    cache = get_cache()
    metrics = get_metrics_collector()

    return {
        "name": "Tool Management Gateway",
        "version": "2.0.0",
        "capabilities": [
            "tool_execution",
            "pipelines",
            "batch_execution",
            "streaming",
            "caching",
            "rate_limiting",
            "intelligent_routing",
            "metrics",
        ],
        "stats": {
            "tools_registered": len(registry),
            "cache": cache.get_stats(),
            "routing": intelligent_router.get_stats(),
            "metrics": metrics.get_summary(),
        },
    }


@router.post("/execute", response_model=ExecuteResponse)
async def execute(
    request: ExecuteRequest,
    client_id: str = Depends(get_client_id),
    _rate_limit: RateLimitResult = Depends(check_rate_limit),
):
    """
    Execute a single tool.

    Features:
    - Automatic backend routing
    - Result caching
    - Rate limiting
    - Metrics collection
    """
    start_time = time.time()
    quota_manager = get_quota_manager()

    try:
        options = request.options or {}
        result = await execute_tool(
            tool_name=request.tool,
            inputs=request.input,
            client_id=client_id,
            use_cache=options.get("cache", True),
        )

        return ExecuteResponse(
            execution_id=result.get("trace_id", ""),
            tool=request.tool,
            status="success",
            result=result["result"],
            duration_ms=(time.time() - start_time) * 1000,
            cached=result.get("cached", False),
            backend=result.get("backend"),
            trace_id=result.get("trace_id"),
        )

    except HTTPException:
        raise
    except Exception as e:
        return ExecuteResponse(
            execution_id="",
            tool=request.tool,
            status="error",
            error=str(e),
            duration_ms=(time.time() - start_time) * 1000,
        )
    finally:
        quota_manager.release(client_id)


@router.post("/batch", response_model=BatchExecuteResponse)
async def batch_execute(
    request: BatchExecuteRequest,
    client_id: str = Depends(get_client_id),
):
    """
    Execute multiple tools in batch.

    Options:
    - parallel: Execute all at once (default)
    - fail_fast: Stop on first error
    """
    start_time = time.time()
    results = []
    quota_manager = get_quota_manager()

    calls = request.get_calls()

    if request.parallel:
        # Parallel execution
        tasks = [
            execute_tool(
                tool_name=call.tool,
                inputs=call.input,
                client_id=client_id,
            )
            for call in calls
        ]

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for call, outcome in zip(calls, outcomes):
            if isinstance(outcome, Exception):
                results.append(ExecuteResponse(
                    execution_id="",
                    tool=call.tool,
                    status="error",
                    error=str(outcome),
                    duration_ms=0,
                ))
                if request.fail_fast:
                    break
            else:
                results.append(ExecuteResponse(
                    execution_id=outcome.get("trace_id", ""),
                    tool=call.tool,
                    status="success",
                    result=outcome["result"],
                    duration_ms=0,
                    cached=outcome.get("cached", False),
                    backend=outcome.get("backend"),
                ))
    else:
        # Sequential execution
        for call in calls:
            call_start = time.time()
            try:
                outcome = await execute_tool(
                    tool_name=call.tool,
                    inputs=call.input,
                    client_id=client_id,
                )
                results.append(ExecuteResponse(
                    execution_id=outcome.get("trace_id", ""),
                    tool=call.tool,
                    status="success",
                    result=outcome["result"],
                    duration_ms=(time.time() - call_start) * 1000,
                    cached=outcome.get("cached", False),
                    backend=outcome.get("backend"),
                ))
            except Exception as e:
                results.append(ExecuteResponse(
                    execution_id="",
                    tool=call.tool,
                    status="error",
                    error=str(e),
                    duration_ms=(time.time() - call_start) * 1000,
                ))
                if request.fail_fast:
                    break

    success_count = sum(1 for r in results if r.status == "success")

    return BatchExecuteResponse(
        results=results,
        total_duration_ms=(time.time() - start_time) * 1000,
        success_count=success_count,
        error_count=len(results) - success_count,
    )


@router.post("/pipeline", response_model=PipelineResponse)
async def execute_pipeline(
    request: PipelineRequest,
    client_id: str = Depends(get_client_id),
):
    """
    Execute a tool pipeline.

    Supports:
    - Sequential steps
    - Parallel fan-out
    - Data transformation
    - Conditional branching
    - Error handling
    """
    start_time = time.time()

    # Build pipeline from request
    pipeline = Pipeline(name=request.name or "ad-hoc")

    logger.info(f"Building pipeline with {len(request.steps)} steps")

    for step_data in request.steps:
        # Create step with explicit ID if provided
        step_id = step_data.get("id")
        step_kwargs = {
            "name": step_data.get("name", ""),
            "type": StepType(step_data.get("type", "tool")),
            "tool_name": step_data.get("tool") or step_data.get("tool_name"),
            "tool_inputs": step_data.get("inputs") or step_data.get("tool_inputs") or {},
            "input_mapping": step_data.get("input_mapping", {}),
            "depends_on": step_data.get("depends_on", []),
            "on_error": step_data.get("on_error", "fail"),
        }

        # Handle explicit ID - must be passed as keyword argument
        if step_id:
            step_kwargs["id"] = step_id

        step = PipelineStep(**step_kwargs)
        pipeline.add_step(step)
        logger.info(f"Added step: id={step.id}, name={step.name}, depends_on={step.depends_on}")

    # Create executor
    executor = PipelineExecutor(
        tool_executor=lambda name, inputs: execute_tool(name, inputs, client_id)
    )

    # Execute
    result = await executor.execute(pipeline, inputs=request.inputs)

    return PipelineResponse(
        execution_id=result["execution_id"],
        pipeline_name=pipeline.name,
        status=result["status"],
        output=result.get("output"),
        step_results=result.get("step_results", {}),
        duration_ms=(time.time() - start_time) * 1000,
    )


@router.post("/stream/{tool}")
async def stream_execute(
    tool: str,
    request: StreamExecuteRequest,
    client_id: str = Depends(get_client_id),
):
    """
    Execute a tool with streaming results via StreamingExecutor.

    Returns Server-Sent Events (SSE) with:
    - Progress updates
    - Partial results
    - Final result
    """
    try:
        from ..core.streaming import get_streaming_executor
        executor = get_streaming_executor()
        execution = await executor.execute(
            tool_name=tool,
            inputs=request.input or {},
            client_id=client_id,
        )
        return StreamingResponse(
            executor.stream_sse(execution.execution_id, include_history=True),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    except ValueError:
        # Fallback: executor not initialized, use simple wrapper
        async def event_generator():
            try:
                import json as _json
                yield f"event: started\ndata: {_json.dumps({'tool': tool})}\n\n"
                result = await execute_tool(
                    tool_name=tool,
                    inputs=request.input,
                    client_id=client_id,
                )
                yield f"event: completed\ndata: {_json.dumps({'result': result['result']})}\n\n"
            except Exception as e:
                import json as _json
                yield f"event: error\ndata: {_json.dumps({'error': str(e)})}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )


@router.get("/stream/{execution_id}/events")
async def get_stream_events(execution_id: str):
    """Stream SSE events for a running execution."""
    try:
        from ..core.streaming import get_streaming_executor
        executor = get_streaming_executor()
        ex = executor.get_execution(execution_id)
        if not ex:
            raise HTTPException(404, f"Execution not found: {execution_id}")
        return StreamingResponse(
            executor.stream_sse(execution_id, include_history=True),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    except ValueError:
        return {"execution_id": execution_id, "events": [], "error": "Streaming executor not initialized"}


# =============================================================================
# Discovery Endpoints
# =============================================================================

@router.get("/tools")
async def list_gateway_tools(
    query: Optional[str] = Query(None),
    backend: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all available tools across all backends."""
    registry = get_registry()
    queue = get_task_queue()

    tools = []

    # Local tools
    for tool in registry.list():
        tools.append({
            "name": tool.name,
            "description": tool.description,
            "backend": "local",
            "category": tool.category,
            "tags": tool.tags,
        })

    # Backend tools
    for backend_info in queue.list_backends():
        for cap in backend_info.capabilities:
            tools.append({
                "name": f"{backend_info.id}::{cap}",
                "description": f"{cap} via {backend_info.name}",
                "backend": backend_info.id,
                "category": "remote",
            })

    # Filter
    if query:
        query_lower = query.lower()
        tools = [t for t in tools if query_lower in t["name"].lower() or query_lower in t.get("description", "").lower()]

    if backend:
        tools = [t for t in tools if t["backend"] == backend]

    return {
        "tools": tools[:limit],
        "total": len(tools),
    }


@router.post("/search")
async def search_tools(request: SearchRequest):
    """
    Semantic search for tools.

    Uses embeddings to find tools matching natural language queries.
    """
    registry = get_registry()

    # Simple keyword search for now
    results = registry.search(query=request.query)

    return {
        "query": request.query,
        "results": [
            {
                "name": t.name,
                "description": t.description,
                "score": 1.0,  # Placeholder
                "backend": "local",
            }
            for t in results[:request.limit]
        ],
    }


# =============================================================================
# Admin/Config Endpoints
# =============================================================================

@router.get("/metrics")
async def get_gateway_metrics():
    """Get gateway metrics."""
    metrics = get_metrics_collector()
    return metrics.get_summary()


@router.get("/metrics/prometheus")
async def get_prometheus_metrics():
    """Get metrics in Prometheus format."""
    metrics = get_metrics_collector()
    return StreamingResponse(
        iter([metrics.get_prometheus_metrics()]),
        media_type="text/plain",
    )


@router.get("/metrics/traces")
async def get_recent_traces(
    limit: int = Query(100, ge=1, le=1000),
    tool: Optional[str] = Query(None),
):
    """Get recent request traces."""
    metrics = get_metrics_collector()
    return {
        "traces": metrics.get_recent_traces(limit=limit, tool_name=tool),
    }


@router.get("/cache/stats")
async def get_cache_stats():
    """Get cache statistics."""
    cache = get_cache()
    return cache.get_stats()


@router.post("/cache/clear")
async def clear_cache(tool: Optional[str] = Query(None)):
    """Clear cache entries."""
    cache = get_cache()
    if tool:
        cache.invalidate(tool)
        return {"cleared": tool}
    else:
        cache.clear()
        return {"cleared": "all"}


@router.get("/routing/stats")
async def get_routing_stats():
    """Get routing statistics."""
    intelligent_router = get_router()
    return intelligent_router.get_stats()


@router.post("/routing/strategy")
async def set_routing_strategy(strategy: str = Query(...)):
    """Set routing strategy."""
    intelligent_router = get_router()
    try:
        intelligent_router.strategy = RoutingStrategy(strategy)
        return {"strategy": strategy}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid strategy: {strategy}")


@router.get("/quotas")
async def list_quotas():
    """List all client quotas."""
    quota_manager = get_quota_manager()
    return quota_manager.get_all_quotas()


@router.post("/quotas")
async def set_quota(request: QuotaRequest):
    """Set quota for a client."""
    quota_manager = get_quota_manager()
    quota = ClientQuota(
        client_id=request.client_id,
        requests_per_second=request.requests_per_second,
        requests_per_minute=request.requests_per_minute,
        requests_per_hour=request.requests_per_hour,
        max_concurrent=request.max_concurrent,
    )
    quota_manager.set_quota(request.client_id, quota)
    return {"status": "ok", "client_id": request.client_id}


@router.post("/backends/{backend_id}/status")
async def set_backend_status(
    backend_id: str,
    status: str = Query(...),
):
    """Manually set backend status."""
    intelligent_router = get_router()
    try:
        intelligent_router.update_status(backend_id, BackendStatus(status))
        return {"backend_id": backend_id, "status": status}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
