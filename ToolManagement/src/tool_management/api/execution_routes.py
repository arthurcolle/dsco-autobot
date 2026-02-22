"""
Streaming-first execution routes.

A single execution contract supports:
- stream: live SSE stream (default)
- sync: collected response
- job: detached/background execution
- auto: latency-aware sync with timeout escalation to job
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .tools_api import ToolCall, ToolResult, execute_tool as execute_single_tool
from ..core.streaming import EventType, get_streaming_executor
from ..persistence import get_tool_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["execution"])


ExecutionMode = Literal["auto", "sync", "stream", "job"]
BatchExecutionMode = Literal["parallel", "sequential"]
ControlAction = Literal["stop", "pause", "resume", "detach", "priority", "narrow"]


class EarlyStopConfig(BaseModel):
    enabled: bool = True
    min_chunks: int = Field(default=2, ge=1)
    max_wait_ms: int = Field(default=5000, ge=100)
    quality_threshold: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    cost_budget: Optional[float] = Field(default=None, ge=0.0)


class ExecuteRequest(BaseModel):
    tool_name: Optional[str] = Field(default=None, description="Tool name or backend:tool")
    tool: Optional[str] = Field(default=None, description="Alias of tool_name")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    execution_mode: ExecutionMode = "auto"
    stream: bool = Field(default=True, description="When true, return SSE stream by default")
    early_stop: Optional[EarlyStopConfig] = None
    timeout_ms: int = Field(default=30000, ge=100, le=600000)
    sync_timeout_ms: int = Field(default=5000, ge=100, le=600000)
    idempotency_key: Optional[str] = None
    priority: Optional[str] = Field(
        default="p1",
        description="Queue priority: p0|p1|p2|p3 (or critical|high|normal|background)",
    )
    dedupe_window_seconds: int = Field(
        default=60,
        ge=0,
        le=3600,
        description="Deduplicate identical job requests in this window",
    )
    dedupe_key: Optional[str] = None
    detach: bool = False
    client_id: Optional[str] = None
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None


class ExecuteResponse(BaseModel):
    status: str
    tool: str
    execution_mode: str
    call_id: Optional[str] = None
    job_id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    latency_ms: float = 0.0
    poll_url: Optional[str] = None
    stream_url: Optional[str] = None
    idempotent_replay: bool = False
    deduplicated: bool = False
    priority: Optional[int] = None
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None


class BatchCall(BaseModel):
    tool_name: Optional[str] = None
    tool: Optional[str] = None
    arguments: Dict[str, Any] = Field(default_factory=dict)
    execution_mode: ExecutionMode = "auto"
    timeout_ms: int = Field(default=30000, ge=100, le=600000)
    sync_timeout_ms: int = Field(default=5000, ge=100, le=600000)
    idempotency_key: Optional[str] = None
    priority: Optional[str] = "p1"
    dedupe_window_seconds: int = Field(default=60, ge=0, le=3600)
    dedupe_key: Optional[str] = None
    stream: bool = False
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None


class BatchExecuteRequest(BaseModel):
    calls: List[BatchCall]
    execution: BatchExecutionMode = "parallel"


class BatchExecuteResponse(BaseModel):
    results: List[ExecuteResponse]
    total: int
    successful: int
    failed: int
    execution: BatchExecutionMode


class ControlRequest(BaseModel):
    action: ControlAction
    level: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None


def _resolve_tool_name(tool_name: Optional[str], tool: Optional[str]) -> str:
    resolved = tool_name or tool
    if not resolved:
        raise HTTPException(status_code=400, detail="tool_name (or tool) is required")
    return resolved


def _resolve_trace_id(trace_id: Optional[str]) -> str:
    return trace_id or f"tr_{uuid4().hex}"


def _structured_error(
    code: str,
    message: str,
    *,
    category: str = "transient",
    retryable: bool = False,
    retry_after_ms: Optional[int] = None,
    backend: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "code": code,
        "message": message,
        "category": category,
        "retryable": retryable,
    }
    if retry_after_ms is not None:
        payload["retry_after_ms"] = retry_after_ms
    if backend is not None:
        payload["backend"] = backend
    return payload


def _latency_class_for_tool(tool_ref: str) -> str:
    """
    Classify tool latency from observed metrics.

    fast: <2s, medium: 2-30s, slow: >30s
    """
    store = get_tool_store()
    tool = store.get_tool_by_name(tool_ref)
    if not tool:
        return "medium"
    latency_ms = float(tool.avg_latency_ms or 0.0)
    if latency_ms <= 0:
        return "medium"
    if latency_ms < 2000:
        return "fast"
    if latency_ms <= 30000:
        return "medium"
    return "slow"


def _sse(event_name: str, data: Dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"


def _quality_score(data: Any) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    for key in ("quality", "confidence", "relevance", "score"):
        value = data.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _should_signal_sufficient(
    early_stop: EarlyStopConfig,
    chunk_count: int,
    elapsed_ms: float,
    chunk_data: Any,
) -> bool:
    if elapsed_ms >= early_stop.max_wait_ms:
        return True
    if chunk_count < early_stop.min_chunks:
        return False
    if early_stop.quality_threshold is None:
        return True
    score = _quality_score(chunk_data)
    return score is not None and score >= early_stop.quality_threshold


async def _submit_job(
    tool_name: str,
    arguments: Dict[str, Any],
    timeout_ms: int,
    priority: Optional[str] = None,
    dedupe_window_seconds: int = 0,
    dedupe_key: Optional[str] = None,
    client_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
):
    try:
        executor = get_streaming_executor()
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    execution = await executor.execute(
        tool_name=tool_name,
        inputs={
            **arguments,
            "_timeout": timeout_ms / 1000.0,
            "_trace_id": trace_id,
            "_session_id": session_id,
            "_parent_execution_id": parent_execution_id,
        },
        client_id=client_id,
        trace_id=trace_id,
        session_id=session_id,
        parent_execution_id=parent_execution_id,
        priority=priority,
        dedupe_window_seconds=dedupe_window_seconds,
        dedupe_key=dedupe_key,
    )
    return execution


async def _execute_sync(
    tool_name: str,
    arguments: Dict[str, Any],
    timeout_ms: int,
    idempotency_key: Optional[str],
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
) -> ToolResult:
    return await execute_single_tool(
        ToolCall(
            tool=tool_name,
            arguments=arguments,
            timeout=timeout_ms / 1000.0,
            idempotency_key=idempotency_key,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
    )


def _to_collected_response(
    tool_name: str,
    execution_mode: str,
    result: ToolResult,
    *,
    call_id: Optional[str] = None,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
) -> ExecuteResponse:
    if result.success:
        return ExecuteResponse(
            status="completed",
            tool=tool_name,
            execution_mode=execution_mode,
            call_id=call_id,
            job_id=job_id,
            result=result.result,
            latency_ms=result.latency_ms,
            idempotent_replay=result.idempotent_replay,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
    return ExecuteResponse(
        status="error",
        tool=tool_name,
        execution_mode=execution_mode,
        call_id=call_id,
        job_id=job_id,
        error=_structured_error(
            "TOOL_EXECUTION_FAILED",
            result.error or "Tool execution failed",
            retryable=True,
        ),
        latency_ms=result.latency_ms,
        idempotent_replay=result.idempotent_replay,
        trace_id=trace_id,
        session_id=session_id,
        parent_execution_id=parent_execution_id,
    )


async def _stream_execution(
    tool_name: str,
    call_id: str,
    early_stop: Optional[EarlyStopConfig],
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
):
    try:
        executor = get_streaming_executor()
    except ValueError:
        yield _sse(
            "error",
            {"error": _structured_error("STREAMING_UNAVAILABLE", "Streaming executor not initialized")},
        )
        return

    started_at = time.time()
    chunk_count = 0
    signaled = False
    yield _sse(
        "meta",
        {
            "call_id": call_id,
            "tool": tool_name,
            "started_at": started_at,
            "trace_id": trace_id,
            "session_id": session_id,
            "parent_execution_id": parent_execution_id,
        },
    )

    async for event in executor.stream_events(call_id, include_history=True):
        if event.type == EventType.STARTED:
            continue

        if event.type == EventType.PARTIAL:
            payload = {
                "index": chunk_count,
                "type": "result",
                "data": event.data,
                "progress": ((event.progress or 0.0) / 100.0),
            }
            yield _sse("chunk", payload)
            chunk_count += 1

            if early_stop and early_stop.enabled and not signaled:
                elapsed_ms = (time.time() - started_at) * 1000
                if _should_signal_sufficient(early_stop, chunk_count, elapsed_ms, event.data):
                    signaled = True
                    yield _sse(
                        "signal",
                        {
                            "type": "sufficient",
                            "reason": "early_stop threshold reached",
                            "can_stop": True,
                            "chunk_count": chunk_count,
                            "elapsed_ms": elapsed_ms,
                        },
                    )
            continue

        if event.type == EventType.PROGRESS:
            yield _sse(
                "signal",
                {
                    "type": "progress",
                    "progress": ((event.progress or 0.0) / 100.0),
                    "message": event.message,
                },
            )
            continue

        if event.type == EventType.SIGNAL:
            payload = event.data if isinstance(event.data, dict) else {"payload": event.data}
            if event.message and "message" not in payload:
                payload["message"] = event.message
            yield _sse("signal", payload)
            continue

        if event.type in (EventType.PAUSED, EventType.RESUMED, EventType.DETACHED):
            yield _sse("signal", {"type": event.type.value, "message": event.message})
            continue

        if event.type == EventType.HEARTBEAT:
            elapsed_ms = (time.time() - started_at) * 1000
            yield _sse(
                "heartbeat",
                {
                    "call_id": call_id,
                    "elapsed_ms": elapsed_ms,
                    "progress": ((event.progress or 0.0) / 100.0),
                },
            )
            continue

        if event.type == EventType.ERROR:
            yield _sse(
                "error",
                {
                    "error": _structured_error(
                        "BACKEND_ERROR",
                        event.error or event.message or "Execution failed",
                        retryable=True,
                    )
                },
            )
            continue

        if event.type in (EventType.COMPLETED, EventType.CANCELLED):
            duration_ms = (time.time() - started_at) * 1000
            yield _sse(
                "done",
                {
                    "call_id": call_id,
                    "total_chunks": chunk_count,
                    "duration_ms": duration_ms,
                    "stopped_early": event.type == EventType.CANCELLED,
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "parent_execution_id": parent_execution_id,
                },
            )
            break


@router.post("/execute")
async def execute(request: ExecuteRequest):
    tool_name = _resolve_tool_name(request.tool_name, request.tool)
    trace_id = _resolve_trace_id(request.trace_id)
    session_id = request.session_id
    parent_execution_id = request.parent_execution_id
    requested_mode: ExecutionMode = request.execution_mode
    if request.stream:
        requested_mode = "stream"

    if requested_mode == "stream":
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        execution_trace_id = execution.trace_id or trace_id
        execution_session_id = execution.session_id or session_id
        execution_parent_id = execution.parent_execution_id or parent_execution_id
        deduplicated = execution.queued_at is not None and execution.queued_at < request_started_at
        if request.detach:
            executor = get_streaming_executor()
            await executor.detach_execution(execution.execution_id)
            return ExecuteResponse(
                status="running_background",
                tool=tool_name,
                execution_mode="job",
                call_id=execution.execution_id,
                job_id=execution.execution_id,
                deduplicated=deduplicated,
                priority=execution.queue_priority,
                poll_url=f"/api/v1/jobs/{execution.execution_id}",
                stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
                trace_id=execution_trace_id,
                session_id=execution_session_id,
                parent_execution_id=execution_parent_id,
            )

        return StreamingResponse(
            _stream_execution(
                tool_name,
                execution.execution_id,
                request.early_stop,
                trace_id=execution_trace_id,
                session_id=execution_session_id,
                parent_execution_id=execution_parent_id,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if requested_mode == "job":
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        execution_trace_id = execution.trace_id or trace_id
        execution_session_id = execution.session_id or session_id
        execution_parent_id = execution.parent_execution_id or parent_execution_id
        deduplicated = execution.queued_at is not None and execution.queued_at < request_started_at
        return ExecuteResponse(
            status="submitted",
            tool=tool_name,
            execution_mode="job",
            call_id=execution.execution_id,
            job_id=execution.execution_id,
            deduplicated=deduplicated,
            priority=execution.queue_priority,
            poll_url=f"/api/v1/jobs/{execution.execution_id}",
            stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
            trace_id=execution_trace_id,
            session_id=execution_session_id,
            parent_execution_id=execution_parent_id,
        )

    if requested_mode == "sync":
        result = await _execute_sync(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            idempotency_key=request.idempotency_key,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        return _to_collected_response(
            tool_name,
            "sync",
            result,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )

    # auto mode: choose from measured latency class, then attempt sync with timeout escalation.
    latency_class = _latency_class_for_tool(tool_name)
    if latency_class == "slow":
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        execution_trace_id = execution.trace_id or trace_id
        execution_session_id = execution.session_id or session_id
        execution_parent_id = execution.parent_execution_id or parent_execution_id
        deduplicated = execution.queued_at is not None and execution.queued_at < request_started_at
        return ExecuteResponse(
            status="submitted",
            tool=tool_name,
            execution_mode="job",
            call_id=execution.execution_id,
            job_id=execution.execution_id,
            deduplicated=deduplicated,
            priority=execution.queue_priority,
            poll_url=f"/api/v1/jobs/{execution.execution_id}",
            stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
            trace_id=execution_trace_id,
            session_id=execution_session_id,
            parent_execution_id=execution_parent_id,
        )

    try:
        result = await asyncio.wait_for(
            _execute_sync(
                tool_name=tool_name,
                arguments=request.arguments,
                timeout_ms=request.timeout_ms,
                idempotency_key=request.idempotency_key,
                trace_id=trace_id,
                session_id=session_id,
                parent_execution_id=parent_execution_id,
            ),
            timeout=request.sync_timeout_ms / 1000.0,
        )
        return _to_collected_response(
            tool_name,
            "sync",
            result,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
    except asyncio.TimeoutError:
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        execution_trace_id = execution.trace_id or trace_id
        execution_session_id = execution.session_id or session_id
        execution_parent_id = execution.parent_execution_id or parent_execution_id
        deduplicated = execution.queued_at is not None and execution.queued_at < request_started_at
        return ExecuteResponse(
            status="submitted",
            tool=tool_name,
            execution_mode="job",
            call_id=execution.execution_id,
            job_id=execution.execution_id,
            deduplicated=deduplicated,
            priority=execution.queue_priority,
            poll_url=f"/api/v1/jobs/{execution.execution_id}",
            stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
            trace_id=execution_trace_id,
            session_id=execution_session_id,
            parent_execution_id=execution_parent_id,
            error=_structured_error(
                "SYNC_TIMEOUT_ESCALATED",
                f"Sync execution exceeded {request.sync_timeout_ms}ms and was promoted to job",
                retryable=False,
            ),
        )


@router.post("/calls/{call_id}/control")
async def control_call(call_id: str, request: ControlRequest):
    try:
        executor = get_streaming_executor()
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    execution = executor.get_execution(call_id)
    if not execution:
        raise HTTPException(status_code=404, detail=f"Call not found: {call_id}")

    action = request.action
    if action == "stop":
        cancelled = executor.cancel_execution(call_id)
        return {"call_id": call_id, "action": action, "accepted": cancelled}

    if action == "pause":
        paused = await executor.pause_execution(call_id)
        return {"call_id": call_id, "action": action, "accepted": paused}

    if action == "resume":
        resumed = await executor.resume_execution(call_id)
        return {"call_id": call_id, "action": action, "accepted": resumed}

    if action == "detach":
        detached = await executor.detach_execution(call_id)
        return {
            "call_id": call_id,
            "action": action,
            "accepted": detached,
            "job_id": call_id,
            "status": "running_background" if detached else execution.status,
            "poll_url": f"/api/v1/jobs/{call_id}",
            "stream_url": f"/api/v1/jobs/{call_id}/stream",
            "trace_id": execution.trace_id,
            "session_id": execution.session_id,
            "parent_execution_id": execution.parent_execution_id,
        }

    if action == "priority":
        level = request.level or "normal"
        reprioritized = executor.reprioritize_execution(call_id, level)
        executor.update_control_state(call_id, {"priority": level})
        await executor.emit_signal(call_id, "priority_updated", {"level": level})
        return {"call_id": call_id, "action": action, "accepted": reprioritized, "level": level}

    if action == "narrow":
        filter_payload = request.filter or {}
        executor.update_control_state(call_id, {"narrow_filter": filter_payload})
        await executor.emit_signal(call_id, "narrow_applied", {"filter": filter_payload})
        return {"call_id": call_id, "action": action, "accepted": True, "filter": filter_payload}

    raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")


@router.post("/execute/batch", response_model=BatchExecuteResponse)
async def execute_batch(request: BatchExecuteRequest):
    if request.execution not in ("parallel", "sequential"):
        raise HTTPException(status_code=400, detail="execution must be parallel or sequential")

    if request.execution == "parallel":
        tasks = []
        for call in request.calls:
            if call.stream:
                raise HTTPException(status_code=400, detail="Batch calls do not support stream=true")
            tasks.append(
                execute(
                    ExecuteRequest(
                        tool_name=call.tool_name,
                        tool=call.tool,
                        arguments=call.arguments,
                        execution_mode=call.execution_mode,
                        stream=False,
                        timeout_ms=call.timeout_ms,
                        sync_timeout_ms=call.sync_timeout_ms,
                        idempotency_key=call.idempotency_key,
                        priority=call.priority,
                        dedupe_window_seconds=call.dedupe_window_seconds,
                        dedupe_key=call.dedupe_key,
                        trace_id=call.trace_id,
                        session_id=call.session_id,
                        parent_execution_id=call.parent_execution_id,
                    )
                )
            )
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        results: List[ExecuteResponse] = []
        for call, raw in zip(request.calls, raw_results):
            tool_name = _resolve_tool_name(call.tool_name, call.tool)
            if isinstance(raw, Exception):
                results.append(
                    ExecuteResponse(
                        status="error",
                        tool=tool_name,
                        execution_mode=call.execution_mode,
                        error=_structured_error(
                            "BATCH_CALL_FAILED",
                            str(raw),
                            retryable=True,
                        ),
                        trace_id=call.trace_id,
                        session_id=call.session_id,
                        parent_execution_id=call.parent_execution_id,
                    )
                )
                continue
            if isinstance(raw, ExecuteResponse):
                results.append(raw)
            else:
                results.append(ExecuteResponse.model_validate(raw))
    else:
        results: List[ExecuteResponse] = []
        for call in request.calls:
            if call.stream:
                raise HTTPException(status_code=400, detail="Batch calls do not support stream=true")
            tool_name = _resolve_tool_name(call.tool_name, call.tool)
            try:
                raw = await execute(
                    ExecuteRequest(
                        tool_name=call.tool_name,
                        tool=call.tool,
                        arguments=call.arguments,
                        execution_mode=call.execution_mode,
                        stream=False,
                        timeout_ms=call.timeout_ms,
                        sync_timeout_ms=call.sync_timeout_ms,
                        idempotency_key=call.idempotency_key,
                        priority=call.priority,
                        dedupe_window_seconds=call.dedupe_window_seconds,
                        dedupe_key=call.dedupe_key,
                        trace_id=call.trace_id,
                        session_id=call.session_id,
                        parent_execution_id=call.parent_execution_id,
                    )
                )
                if isinstance(raw, ExecuteResponse):
                    results.append(raw)
                else:
                    results.append(ExecuteResponse.model_validate(raw))
            except Exception as exc:
                results.append(
                    ExecuteResponse(
                        status="error",
                        tool=tool_name,
                        execution_mode=call.execution_mode,
                        error=_structured_error(
                            "BATCH_CALL_FAILED",
                            str(exc),
                            retryable=True,
                        ),
                        trace_id=call.trace_id,
                        session_id=call.session_id,
                        parent_execution_id=call.parent_execution_id,
                    )
                )

    successful = sum(1 for r in results if r.status in {"completed", "submitted", "running_background"})
    return BatchExecuteResponse(
        results=results,
        total=len(results),
        successful=successful,
        failed=len(results) - successful,
        execution=request.execution,
    )
