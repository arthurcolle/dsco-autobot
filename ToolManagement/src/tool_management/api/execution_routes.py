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

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .tools_api import ToolCall, ToolResult, execute_tool as execute_single_tool
from ..core import PolicyConstraints, choose_execution_mode
from ..core.streaming import EventType, get_streaming_executor
from ..persistence import get_execution_store, get_tool_store

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


class PolicyConstraintsConfig(BaseModel):
    latency_budget_ms: Optional[float] = Field(default=None, ge=0.0)
    cost_budget: Optional[float] = Field(default=None, ge=0.0)
    reliability_target: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    prefer_streaming: bool = False
    allow_sync: bool = True
    allow_stream: bool = True
    allow_job: bool = True


class ExecuteRequest(BaseModel):
    tool_name: Optional[str] = Field(default=None, description="Tool name or backend:tool")
    tool: Optional[str] = Field(default=None, description="Alias of tool_name")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    execution_mode: ExecutionMode = "auto"
    stream: bool = Field(default=True, description="When true, return SSE stream by default")
    early_stop: Optional[EarlyStopConfig] = None
    policy_constraints: Optional[PolicyConstraintsConfig] = None
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
    callback_url: Optional[str] = None
    callback_events: List[str] = Field(
        default_factory=lambda: ["complete", "error"],
        description="Webhook callback events: progress|complete|error|cancelled|all",
    )
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
    callback_url: Optional[str] = None
    callback_events: List[str] = Field(default_factory=list)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None
    policy_decision: Optional[Dict[str, Any]] = None


class BatchCall(BaseModel):
    tool_name: Optional[str] = None
    tool: Optional[str] = None
    arguments: Dict[str, Any] = Field(default_factory=dict)
    execution_mode: ExecutionMode = "auto"
    policy_constraints: Optional[PolicyConstraintsConfig] = None
    timeout_ms: int = Field(default=30000, ge=100, le=600000)
    sync_timeout_ms: int = Field(default=5000, ge=100, le=600000)
    idempotency_key: Optional[str] = None
    priority: Optional[str] = "p1"
    dedupe_window_seconds: int = Field(default=60, ge=0, le=3600)
    dedupe_key: Optional[str] = None
    callback_url: Optional[str] = None
    callback_events: List[str] = Field(default_factory=list)
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


class PolicyDecisionQueryResponse(BaseModel):
    count: int
    decisions: List[Dict[str, Any]]


def _resolve_tool_name(tool_name: Optional[str], tool: Optional[str]) -> str:
    resolved = tool_name or tool
    if not resolved:
        raise HTTPException(status_code=400, detail="tool_name (or tool) is required")
    return resolved


def _resolve_trace_id(trace_id: Optional[str]) -> str:
    return trace_id or f"tr_{uuid4().hex}"


def _policy_constraints_from_request(request: ExecuteRequest) -> PolicyConstraints:
    if request.policy_constraints is None:
        return PolicyConstraints()
    payload = request.policy_constraints
    return PolicyConstraints(
        latency_budget_ms=payload.latency_budget_ms,
        cost_budget=payload.cost_budget,
        reliability_target=payload.reliability_target,
        prefer_streaming=payload.prefer_streaming,
        allow_sync=payload.allow_sync,
        allow_stream=payload.allow_stream,
        allow_job=payload.allow_job,
    )


def _record_policy_decision(
    *,
    decision_payload: Dict[str, Any],
    trace_id: Optional[str],
    session_id: Optional[str],
    tool_name: str,
):
    try:
        store = get_execution_store()
        constraints = decision_payload.get("constraints") or {}
        store.append_policy_decision(
            decision_id=decision_payload["decision_id"],
            trace_id=trace_id,
            session_id=session_id,
            tool_name=tool_name,
            requested_mode=decision_payload.get("requested_mode"),
            selected_mode=decision_payload.get("selected_mode"),
            backend_id=decision_payload.get("backend_id"),
            latency_class=decision_payload.get("latency_class"),
            confidence=float(decision_payload.get("confidence") or 0.0),
            latency_budget_ms=constraints.get("latency_budget_ms"),
            cost_budget=constraints.get("cost_budget"),
            reliability_target=constraints.get("reliability_target"),
            scores=decision_payload.get("scores") or {},
            reasons=decision_payload.get("reasons") or [],
            constraints=constraints,
            model_version=decision_payload.get("model_version"),
        )
    except Exception as exc:
        logger.warning(f"Failed to persist policy decision ({tool_name}): {exc}")


def _update_policy_decision(
    *,
    decision_payload: Optional[Dict[str, Any]],
    execution_id: Optional[str] = None,
    selected_mode: Optional[str] = None,
    reason: Optional[str] = None,
):
    if not decision_payload:
        return
    decision_id = decision_payload.get("decision_id")
    if not decision_id:
        return
    try:
        store = get_execution_store()
        if execution_id:
            store.update_policy_decision_execution_id(decision_id, execution_id)
        if selected_mode or reason:
            reasons = list(decision_payload.get("reasons") or [])
            if reason:
                reasons.append(reason)
            if selected_mode:
                decision_payload["selected_mode"] = selected_mode
            decision_payload["reasons"] = reasons
            store.append_policy_decision(
                decision_id=decision_id,
                selected_mode=decision_payload.get("selected_mode"),
                reasons=reasons,
            )
    except Exception as exc:
        logger.warning(f"Failed to update policy decision ({decision_id}): {exc}")


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
    callback_url: Optional[str] = None,
    callback_events: Optional[List[str]] = None,
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
        callback_url=callback_url,
        callback_events=callback_events,
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
    policy_decision: Optional[Dict[str, Any]] = None,
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
            policy_decision=policy_decision,
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
        policy_decision=policy_decision,
    )


async def _stream_execution(
    tool_name: str,
    call_id: str,
    early_stop: Optional[EarlyStopConfig],
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
    policy_decision: Optional[Dict[str, Any]] = None,
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
            "policy_decision": policy_decision,
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
                "event_id": event.event_id,
                "replayed": event.replayed,
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
            payload: Dict[str, Any] = {
                "type": "progress",
                "progress": ((event.progress or 0.0) / 100.0),
                "message": event.message,
                "event_id": event.event_id,
                "replayed": event.replayed,
            }
            if isinstance(event.data, dict):
                payload.update(event.data)
            yield _sse(
                "signal",
                payload,
            )
            continue

        if event.type == EventType.SIGNAL:
            payload = event.data if isinstance(event.data, dict) else {"payload": event.data}
            if event.message and "message" not in payload:
                payload["message"] = event.message
            payload["event_id"] = event.event_id
            payload["replayed"] = event.replayed
            yield _sse("signal", payload)
            continue

        if event.type in (EventType.PAUSED, EventType.RESUMED, EventType.DETACHED):
            yield _sse(
                "signal",
                {
                    "type": event.type.value,
                    "message": event.message,
                    "event_id": event.event_id,
                    "replayed": event.replayed,
                },
            )
            continue

        if event.type == EventType.HEARTBEAT:
            elapsed_ms = (time.time() - started_at) * 1000
            yield _sse(
                "heartbeat",
                {
                    "call_id": call_id,
                    "elapsed_ms": elapsed_ms,
                    "progress": ((event.progress or 0.0) / 100.0),
                    "event_id": event.event_id,
                    "replayed": event.replayed,
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
                    ),
                    "event_id": event.event_id,
                    "replayed": event.replayed,
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
                    "event_id": event.event_id,
                    "replayed": event.replayed,
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
    constraints = _policy_constraints_from_request(request)
    policy_decision = choose_execution_mode(
        tool_name=tool_name,
        requested_mode=requested_mode,
        tool_store=get_tool_store(),
        constraints=constraints,
    ).to_dict()
    _record_policy_decision(
        decision_payload=policy_decision,
        trace_id=trace_id,
        session_id=session_id,
        tool_name=tool_name,
    )

    selected_mode = str(policy_decision.get("selected_mode") or "sync")
    auto_requested = request.execution_mode == "auto" and not request.stream

    # Non-streaming clients cannot consume SSE directly; degrade stream to job handle.
    if selected_mode == "stream" and not request.stream:
        selected_mode = "job"
        _update_policy_decision(
            decision_payload=policy_decision,
            selected_mode="job",
            reason="stream_selected_but_client_requested_collected_response",
        )

    async def submit_job_response(*, status: str = "submitted", error: Optional[Dict[str, Any]] = None) -> ExecuteResponse:
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            callback_url=request.callback_url,
            callback_events=request.callback_events,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        _update_policy_decision(
            decision_payload=policy_decision,
            execution_id=execution.execution_id,
        )
        execution_trace_id = execution.trace_id or trace_id
        execution_session_id = execution.session_id or session_id
        execution_parent_id = execution.parent_execution_id or parent_execution_id
        deduplicated = execution.queued_at is not None and execution.queued_at < request_started_at
        return ExecuteResponse(
            status=status,
            tool=tool_name,
            execution_mode="job",
            call_id=execution.execution_id,
            job_id=execution.execution_id,
            deduplicated=deduplicated,
            priority=execution.queue_priority,
            poll_url=f"/api/v1/jobs/{execution.execution_id}",
            stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
            callback_url=request.callback_url,
            callback_events=request.callback_events,
            trace_id=execution_trace_id,
            session_id=execution_session_id,
            parent_execution_id=execution_parent_id,
            error=error,
            policy_decision=policy_decision,
        )

    if selected_mode == "stream":
        request_started_at = time.time()
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            callback_url=request.callback_url,
            callback_events=request.callback_events,
            client_id=request.client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
        )
        _update_policy_decision(
            decision_payload=policy_decision,
            execution_id=execution.execution_id,
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
                callback_url=request.callback_url,
                callback_events=request.callback_events,
                trace_id=execution_trace_id,
                session_id=execution_session_id,
                parent_execution_id=execution_parent_id,
                policy_decision=policy_decision,
            )

        return StreamingResponse(
            _stream_execution(
                tool_name,
                execution.execution_id,
                request.early_stop,
                trace_id=execution_trace_id,
                session_id=execution_session_id,
                parent_execution_id=execution_parent_id,
                policy_decision=policy_decision,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if selected_mode == "job":
        return await submit_job_response(status="submitted")

    # Sync mode (explicit or policy-selected). Auto mode gets timeout escalation.
    if auto_requested:
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
                policy_decision=policy_decision,
            )
        except asyncio.TimeoutError:
            _update_policy_decision(
                decision_payload=policy_decision,
                selected_mode="job",
                reason="sync_timeout_promoted_to_job",
            )
            return await submit_job_response(
                status="submitted",
                error=_structured_error(
                    "SYNC_TIMEOUT_ESCALATED",
                    f"Sync execution exceeded {request.sync_timeout_ms}ms and was promoted to job",
                    retryable=False,
                ),
            )

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
        policy_decision=policy_decision,
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
        started = time.time()
        cancelled = executor.cancel_execution(call_id)
        return {
            "call_id": call_id,
            "action": action,
            "accepted": cancelled,
            "requested_at": started,
            "target_ack_sla_ms": 500,
        }

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


@router.get("/policy-decisions", response_model=PolicyDecisionQueryResponse)
async def list_policy_decisions(
    hours: int = Query(default=24, ge=1, le=720),
    tool_name: Optional[str] = Query(default=None),
    trace_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    store = get_execution_store()
    decisions = store.list_policy_decisions(
        hours=hours,
        tool_name=tool_name,
        trace_id=trace_id,
        limit=limit,
    )
    return {"count": len(decisions), "decisions": decisions}


@router.get("/policy-decisions/analytics")
async def get_policy_decision_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=500, ge=1, le=5000),
):
    store = get_execution_store()
    decisions = store.list_policy_decisions(hours=hours, limit=limit)
    mode_mix: Dict[str, int] = {}
    latency_mix: Dict[str, int] = {}
    tool_mix: Dict[str, int] = {}
    avg_confidence = 0.0

    for decision in decisions:
        selected = str(decision.get("selected_mode") or "unknown")
        latency_class = str(decision.get("latency_class") or "unknown")
        tool = str(decision.get("tool_name") or "unknown")
        mode_mix[selected] = mode_mix.get(selected, 0) + 1
        latency_mix[latency_class] = latency_mix.get(latency_class, 0) + 1
        tool_mix[tool] = tool_mix.get(tool, 0) + 1
        avg_confidence += float(decision.get("confidence") or 0.0)

    total = len(decisions)
    top_tools = sorted(tool_mix.items(), key=lambda item: item[1], reverse=True)[:25]
    return {
        "window_hours": hours,
        "count": total,
        "avg_confidence": (avg_confidence / total) if total else 0.0,
        "selected_mode_mix": mode_mix,
        "latency_class_mix": latency_mix,
        "top_tools": [{"tool_name": name, "decisions": count} for name, count in top_tools],
    }


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
                        policy_constraints=call.policy_constraints,
                        stream=False,
                        timeout_ms=call.timeout_ms,
                        sync_timeout_ms=call.sync_timeout_ms,
                        idempotency_key=call.idempotency_key,
                        priority=call.priority,
                        dedupe_window_seconds=call.dedupe_window_seconds,
                        dedupe_key=call.dedupe_key,
                        callback_url=call.callback_url,
                        callback_events=call.callback_events,
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
                        policy_constraints=call.policy_constraints,
                        stream=False,
                        timeout_ms=call.timeout_ms,
                        sync_timeout_ms=call.sync_timeout_ms,
                        idempotency_key=call.idempotency_key,
                        priority=call.priority,
                        dedupe_window_seconds=call.dedupe_window_seconds,
                        dedupe_key=call.dedupe_key,
                        callback_url=call.callback_url,
                        callback_events=call.callback_events,
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
