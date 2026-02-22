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
import hashlib
import json
import logging
import os
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
BatchOverflowPolicy = Literal["reject", "truncate"]
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
    convergence_key: Optional[str] = Field(
        default=None,
        description="Optional semantic coalescing key to converge similar job submissions",
    )
    fanout_budget: Optional[int] = Field(
        default=None,
        ge=1,
        le=100000,
        description="Optional max in-flight jobs for this fanout scope",
    )
    group_reducer: Optional[str] = Field(
        default=None,
        description="Reducer strategy for convergence groups (append_unique|append|latest|top_k|windowed|score_weighted)",
    )
    group_reducer_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional reducer configuration (k, window_seconds, key_field, default_weight)",
    )
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
    coalesced: bool = False
    coalesced_requests: int = 0
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
    convergence_key: Optional[str] = None
    fanout_budget: Optional[int] = Field(default=None, ge=1, le=100000)
    group_reducer: Optional[str] = None
    group_reducer_config: Dict[str, Any] = Field(default_factory=dict)
    callback_url: Optional[str] = None
    callback_events: List[str] = Field(default_factory=list)
    stream: bool = False
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None


class BatchExecuteRequest(BaseModel):
    calls: List[BatchCall]
    execution: BatchExecutionMode = "parallel"
    max_parallel: Optional[int] = Field(
        default=None,
        ge=1,
        le=512,
        description="Maximum in-flight calls when execution=parallel",
    )
    dedupe_calls: bool = Field(
        default=True,
        description="When true, deduplicate equivalent calls before dispatch and fan results back in order",
    )
    dedupe_on_tool: bool = Field(
        default=True,
        description="Include tool name in batch dedupe fingerprint; false dedupes across tools by args/mode only",
    )
    max_canonical_calls: Optional[int] = Field(
        default=None,
        ge=1,
        le=10000,
        description="Optional cap on unique canonical calls after preflight dedupe",
    )
    overflow_policy: BatchOverflowPolicy = Field(
        default="reject",
        description="Behavior when canonical calls exceed max_canonical_calls (reject|truncate)",
    )
    auto_follow_up: bool = Field(
        default=True,
        description="Automatically persist follow-up actions when batch fanout/reintegration risk is detected",
    )
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


class BatchExecuteResponse(BaseModel):
    results: List[ExecuteResponse]
    total: int
    successful: int
    failed: int
    execution: BatchExecutionMode
    batch_plan: Dict[str, Any] = Field(default_factory=dict)
    follow_up_actions: List[Dict[str, Any]] = Field(default_factory=list)


class ControlRequest(BaseModel):
    action: ControlAction
    level: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None


class PolicyDecisionQueryResponse(BaseModel):
    count: int
    decisions: List[Dict[str, Any]]


class ExecutionGroupStateModel(BaseModel):
    convergence_key: str
    tool_name: str
    reducer: str
    chunk_count: int = 0
    execution_count: int = 0
    last_execution_id: Optional[str] = None
    last_trace_id: Optional[str] = None
    last_session_id: Optional[str] = None
    created_at: float
    updated_at: float
    state: Dict[str, Any] = Field(default_factory=dict)


class ExecutionGroupListResponse(BaseModel):
    count: int
    groups: List[ExecutionGroupStateModel]


class ExecutionGroupCompactionRequest(BaseModel):
    max_items: Optional[int] = Field(default=None, ge=1, le=500000)
    max_age_seconds: Optional[float] = Field(default=None, gt=0.0)


class ExecutionGroupBulkCompactionRequest(BaseModel):
    limit: int = Field(default=500, ge=1, le=10000)
    max_items: Optional[int] = Field(default=None, ge=1, le=500000)
    max_age_seconds: Optional[float] = Field(default=None, gt=0.0)


class ExecutionGroupCleanupRequest(BaseModel):
    max_age_seconds: int = Field(default=86400, ge=1, le=315360000)


class FollowUpActionModel(BaseModel):
    follow_up_id: str
    action_type: str
    title: str
    description: Optional[str] = None
    priority: str
    status: str
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    dedupe_key: Optional[str] = None
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    due_at: Optional[float] = None
    completed_at: Optional[float] = None
    created_at: float
    updated_at: float


class FollowUpActionListResponse(BaseModel):
    count: int
    actions: List[FollowUpActionModel]


class FollowUpGenerateRequest(BaseModel):
    hours: int = Field(default=24, ge=1, le=720)
    limit: int = Field(default=50, ge=1, le=1000)
    merge_pressure_threshold: float = Field(default=2.0, ge=0.0)
    items_threshold: int = Field(default=200, ge=1, le=500000)
    max_items_target: int = Field(default=120, ge=1, le=500000)
    include_compaction_actions: bool = True
    include_reducer_actions: bool = True
    include_fanout_actions: bool = True
    auto_persist: bool = True
    dedupe_window_seconds: int = Field(default=3600, ge=0, le=604800)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


class FollowUpStatusUpdateRequest(BaseModel):
    status: str = Field(
        default="completed",
        description="open|in_progress|completed|dismissed|failed",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


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


def _normalize_execution_group_row(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row)
    payload.pop("state_json", None)
    payload["convergence_key"] = str(payload.get("convergence_key") or "")
    payload["tool_name"] = str(payload.get("tool_name") or "unknown")
    payload["reducer"] = str(payload.get("reducer") or "append_unique")
    payload["chunk_count"] = int(payload.get("chunk_count") or 0)
    payload["execution_count"] = int(payload.get("execution_count") or 0)
    payload["created_at"] = float(payload.get("created_at") or 0.0)
    payload["updated_at"] = float(payload.get("updated_at") or 0.0)
    state = payload.get("state")
    payload["state"] = state if isinstance(state, dict) else {}
    return payload


def _execution_group_items_count(row: Dict[str, Any]) -> int:
    state = row.get("state") if isinstance(row.get("state"), dict) else {}
    items = state.get("items") if isinstance(state.get("items"), list) else []
    return len(items)


def _normalize_follow_up_action_row(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row or {})
    payload["follow_up_id"] = str(payload.get("follow_up_id") or "")
    payload["action_type"] = str(payload.get("action_type") or "")
    payload["title"] = str(payload.get("title") or "")
    payload["description"] = payload.get("description")
    payload["priority"] = str(payload.get("priority") or "normal")
    payload["status"] = str(payload.get("status") or "open")
    payload["source_type"] = payload.get("source_type")
    payload["source_id"] = payload.get("source_id")
    payload["dedupe_key"] = payload.get("dedupe_key")
    payload["trace_id"] = payload.get("trace_id")
    payload["session_id"] = payload.get("session_id")
    payload["payload"] = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    payload["metadata"] = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    payload["due_at"] = payload.get("due_at")
    payload["completed_at"] = payload.get("completed_at")
    payload["created_at"] = float(payload.get("created_at") or 0.0)
    payload["updated_at"] = float(payload.get("updated_at") or payload["created_at"] or 0.0)
    return payload


def _batch_call_fingerprint(call: BatchCall, *, dedupe_on_tool: bool) -> str:
    tool_key = _resolve_tool_name(call.tool_name, call.tool) if dedupe_on_tool else "__any_tool__"
    policy_payload: Dict[str, Any] = {}
    if call.policy_constraints is not None:
        policy_payload = call.policy_constraints.model_dump(exclude_none=True)
    payload = {
        "tool": tool_key,
        "arguments": call.arguments,
        "execution_mode": call.execution_mode,
        "policy_constraints": policy_payload,
        "timeout_ms": call.timeout_ms,
        "sync_timeout_ms": call.sync_timeout_ms,
        "priority": call.priority,
        "dedupe_window_seconds": call.dedupe_window_seconds,
        "dedupe_key": call.dedupe_key,
        "convergence_key": call.convergence_key,
        "fanout_budget": call.fanout_budget,
        "group_reducer": call.group_reducer,
        "group_reducer_config": call.group_reducer_config,
        "callback_url": call.callback_url,
        "callback_events": call.callback_events,
        "trace_id": call.trace_id,
        "session_id": call.session_id,
        "parent_execution_id": call.parent_execution_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _clone_execute_response(response: ExecuteResponse, *, deduplicated: bool = False) -> ExecuteResponse:
    payload = response.model_dump()
    if deduplicated:
        payload["deduplicated"] = True
    return ExecuteResponse.model_validate(payload)


def _build_execution_group_follow_up_candidates(
    *,
    rows: List[Dict[str, Any]],
    merge_pressure_threshold: float,
    items_threshold: int,
    max_items_target: int,
    include_compaction_actions: bool,
    include_reducer_actions: bool,
    include_fanout_actions: bool,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _normalize_execution_group_row(row)
        key = str(normalized.get("convergence_key") or "").strip()
        if not key:
            continue
        tool_name = str(normalized.get("tool_name") or "unknown")
        chunk_count = max(0, int(normalized.get("chunk_count") or 0))
        execution_count = max(1, int(normalized.get("execution_count") or 0))
        items_count = _execution_group_items_count(normalized)
        merge_pressure = float(chunk_count) / float(execution_count)
        pressure_score = merge_pressure * (1.0 + (items_count / max(1.0, float(items_threshold))))

        if include_compaction_actions and merge_pressure >= merge_pressure_threshold:
            recommended_max_items = max(25, min(int(max_items_target), max(25, int(items_count * 0.6) or 25)))
            candidates.append(
                {
                    "action_type": "compact_execution_group",
                    "title": f"Compact execution group {key}",
                    "description": (
                        f"Merge pressure is {merge_pressure:.2f} with {items_count} retained items. "
                        "Compact this convergence group to reduce reintegration cost."
                    ),
                    "priority": "high" if merge_pressure >= (merge_pressure_threshold * 2.0) else "normal",
                    "source_type": "execution_group",
                    "source_id": key,
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "payload": {
                        "convergence_key": key,
                        "tool_name": tool_name,
                        "max_items": recommended_max_items,
                        "max_age_seconds": 86400,
                        "merge_pressure": merge_pressure,
                        "items_count": items_count,
                    },
                    "metadata": {
                        "generator": "execution_groups",
                        "score": pressure_score,
                    },
                    "dedupe_key": f"fu:compact:{key}",
                    "score": pressure_score + 3.0,
                }
            )

        if include_reducer_actions and items_count >= items_threshold:
            candidates.append(
                {
                    "action_type": "set_group_reducer_top_k",
                    "title": f"Switch reducer for {key} to top_k",
                    "description": (
                        f"Execution group has {items_count} items. Switching to top_k can bound "
                        "reintegration while preserving highest-scoring chunks."
                    ),
                    "priority": "high" if items_count >= (items_threshold * 2) else "normal",
                    "source_type": "execution_group",
                    "source_id": key,
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "payload": {
                        "convergence_key": key,
                        "tool_name": tool_name,
                        "group_reducer": "top_k",
                        "group_reducer_config": {"k": max(25, min(int(max_items_target), items_threshold))},
                        "items_count": items_count,
                        "merge_pressure": merge_pressure,
                    },
                    "metadata": {
                        "generator": "execution_groups",
                        "score": pressure_score,
                    },
                    "dedupe_key": f"fu:reducer_top_k:{key}",
                    "score": pressure_score + 2.0,
                }
            )

        if include_fanout_actions and merge_pressure >= (merge_pressure_threshold * 1.5):
            recommended_budget = max(2, min(64, int(max_items_target / 2)))
            candidates.append(
                {
                    "action_type": "tighten_fanout_budget",
                    "title": f"Tighten fanout budget for {tool_name}",
                    "description": (
                        f"Merge pressure {merge_pressure:.2f} indicates excessive parallel fanout. "
                        "Apply a lower fanout budget to reduce downstream reintegration."
                    ),
                    "priority": "critical" if merge_pressure >= (merge_pressure_threshold * 3.0) else "high",
                    "source_type": "execution_group",
                    "source_id": key,
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "payload": {
                        "tool_name": tool_name,
                        "convergence_key": key,
                        "recommended_fanout_budget": recommended_budget,
                        "merge_pressure": merge_pressure,
                    },
                    "metadata": {
                        "generator": "execution_groups",
                        "score": pressure_score,
                    },
                    "dedupe_key": f"fu:fanout_budget:{tool_name}:{key}",
                    "score": pressure_score + 1.0,
                }
            )

    candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    for candidate in candidates:
        candidate.pop("score", None)
    return candidates


def _persist_batch_follow_up_actions(
    *,
    trace_id: str,
    session_id: Optional[str],
    request: BatchExecuteRequest,
    batch_plan: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not request.auto_follow_up:
        return []
    store = get_execution_store()
    actions: List[Dict[str, Any]] = []
    now = time.time()
    total_calls = int(batch_plan.get("total_calls") or 0)
    canonical_calls = int(batch_plan.get("canonical_calls") or 0)
    deduped_calls = int(batch_plan.get("deduped_calls") or 0)
    overflowed = int(batch_plan.get("overflowed_canonical_calls") or 0)

    # Trigger durability-driven follow-ups only for materially large batches.
    if total_calls < 25:
        return []

    if not request.dedupe_calls and total_calls >= 50:
        created = store.append_follow_up_action(
            action_type="enable_batch_preflight_dedupe",
            title="Enable preflight dedupe for large batch executions",
            description=(
                f"Batch had {total_calls} calls with dedupe disabled. "
                "Enable dedupe_calls to prevent unnecessary fanout."
            ),
            priority="high",
            source_type="batch_execution",
            source_id=trace_id,
            trace_id=trace_id,
            session_id=session_id,
            payload={
                "dedupe_calls": True,
                "dedupe_on_tool": True,
                "total_calls": total_calls,
            },
            metadata={"generator": "execute_batch", "generated_at": now},
            dedupe_key=f"fu:batch:enable_dedupe:{trace_id}",
            dedupe_window_seconds=3600,
        )
        actions.append(_normalize_follow_up_action_row(created))

    if canonical_calls >= 120:
        recommended_cap = max(32, min(256, int(canonical_calls * 0.5)))
        created = store.append_follow_up_action(
            action_type="set_batch_canonical_cap",
            title="Set batch canonical cap to bound reintegration",
            description=(
                f"Canonical fanout reached {canonical_calls} unique calls. "
                "Configure max_canonical_calls to avoid costly reintegration spikes."
            ),
            priority="critical" if canonical_calls >= 200 else "high",
            source_type="batch_execution",
            source_id=trace_id,
            trace_id=trace_id,
            session_id=session_id,
            payload={
                "recommended_max_canonical_calls": recommended_cap,
                "overflow_policy": "truncate",
                "canonical_calls": canonical_calls,
                "total_calls": total_calls,
            },
            metadata={"generator": "execute_batch", "generated_at": now},
            dedupe_key=f"fu:batch:canonical_cap:{trace_id}",
            dedupe_window_seconds=3600,
        )
        actions.append(_normalize_follow_up_action_row(created))

    if overflowed > 0:
        created = store.append_follow_up_action(
            action_type="resegment_batch_inputs",
            title="Resegment oversized batch payload",
            description=(
                f"{overflowed} canonical calls overflowed this batch cap. "
                "Split workloads into smaller segments with convergence keys."
            ),
            priority="high",
            source_type="batch_execution",
            source_id=trace_id,
            trace_id=trace_id,
            session_id=session_id,
            payload={
                "overflowed_canonical_calls": overflowed,
                "canonical_calls": canonical_calls,
                "total_calls": total_calls,
                "deduped_calls": deduped_calls,
            },
            metadata={"generator": "execute_batch", "generated_at": now},
            dedupe_key=f"fu:batch:resegment:{trace_id}",
            dedupe_window_seconds=3600,
        )
        actions.append(_normalize_follow_up_action_row(created))

    return actions


async def _submit_job(
    tool_name: str,
    arguments: Dict[str, Any],
    timeout_ms: int,
    priority: Optional[str] = None,
    dedupe_window_seconds: int = 0,
    dedupe_key: Optional[str] = None,
    convergence_key: Optional[str] = None,
    fanout_budget: Optional[int] = None,
    group_reducer: Optional[str] = None,
    group_reducer_config: Optional[Dict[str, Any]] = None,
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

    try:
        execution = await executor.execute(
            tool_name=tool_name,
            inputs={
                **arguments,
                "_timeout": timeout_ms / 1000.0,
                "_trace_id": trace_id,
                "_session_id": session_id,
                "_parent_execution_id": parent_execution_id,
                "_convergence_key": convergence_key,
                "_fanout_budget": fanout_budget,
                "_group_reducer": group_reducer,
                "_group_reducer_config": group_reducer_config or {},
            },
            client_id=client_id,
            trace_id=trace_id,
            session_id=session_id,
            parent_execution_id=parent_execution_id,
            priority=priority,
            dedupe_window_seconds=dedupe_window_seconds,
            dedupe_key=dedupe_key,
            convergence_key=convergence_key,
            fanout_budget=fanout_budget,
            group_reducer=group_reducer,
            group_reducer_config=group_reducer_config or {},
            callback_url=callback_url,
            callback_events=callback_events,
        )
    except RuntimeError as exc:
        message = str(exc)
        if message.startswith("FANOUT_LIMIT_EXCEEDED:"):
            raise HTTPException(
                status_code=429,
                detail=_structured_error(
                    "FANOUT_LIMIT_EXCEEDED",
                    message,
                    category="resource_limit",
                    retryable=True,
                    retry_after_ms=2000,
                ),
            ) from exc
        if message.startswith("QUEUE_BACKPRESSURE:"):
            raise HTTPException(
                status_code=429,
                detail=_structured_error(
                    "QUEUE_BACKPRESSURE",
                    message,
                    category="resource_limit",
                    retryable=True,
                    retry_after_ms=1000,
                ),
            ) from exc
        if message.startswith("INFLIGHT_LIMIT_EXCEEDED:"):
            raise HTTPException(
                status_code=429,
                detail=_structured_error(
                    "INFLIGHT_LIMIT_EXCEEDED",
                    message,
                    category="resource_limit",
                    retryable=True,
                    retry_after_ms=1000,
                ),
            ) from exc
        raise
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
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            convergence_key=request.convergence_key,
            fanout_budget=request.fanout_budget,
            group_reducer=request.group_reducer,
            group_reducer_config=request.group_reducer_config,
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
        control_state = execution.control_state if isinstance(getattr(execution, "control_state", None), dict) else {}
        submit_count = int(control_state.get("submit_count", 1))
        deduplicated = submit_count > 1
        coalesced_requests = int(control_state.get("coalesced_requests", 0))
        return ExecuteResponse(
            status=status,
            tool=tool_name,
            execution_mode="job",
            call_id=execution.execution_id,
            job_id=execution.execution_id,
            deduplicated=deduplicated,
            coalesced=coalesced_requests > 0,
            coalesced_requests=coalesced_requests,
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
        execution = await _submit_job(
            tool_name=tool_name,
            arguments=request.arguments,
            timeout_ms=request.timeout_ms,
            priority=request.priority,
            dedupe_window_seconds=request.dedupe_window_seconds,
            dedupe_key=request.dedupe_key,
            convergence_key=request.convergence_key,
            fanout_budget=request.fanout_budget,
            group_reducer=request.group_reducer,
            group_reducer_config=request.group_reducer_config,
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
        control_state = execution.control_state if isinstance(getattr(execution, "control_state", None), dict) else {}
        submit_count = int(control_state.get("submit_count", 1))
        deduplicated = submit_count > 1
        coalesced_requests = int(control_state.get("coalesced_requests", 0))
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
                coalesced=coalesced_requests > 0,
                coalesced_requests=coalesced_requests,
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


@router.get("/execution-groups", response_model=ExecutionGroupListResponse)
async def list_execution_groups(
    tool_name: Optional[str] = Query(default=None),
    updated_since: Optional[float] = Query(default=None, ge=0.0),
    limit: int = Query(default=100, ge=1, le=2000),
):
    store = get_execution_store()
    rows = store.list_execution_group_states(
        tool_name=tool_name,
        updated_since=updated_since,
        limit=limit,
    )
    groups = [ExecutionGroupStateModel.model_validate(_normalize_execution_group_row(row)) for row in rows]
    return {"count": len(groups), "groups": groups}


@router.get("/execution-groups/analytics")
async def get_execution_group_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=500, ge=1, le=10000),
):
    store = get_execution_store()
    since_ts = time.time() - (max(1, int(hours)) * 3600.0)
    rows = [
        _normalize_execution_group_row(row)
        for row in store.list_execution_group_states(updated_since=since_ts, limit=limit)
    ]
    total_groups = len(rows)
    total_chunks = sum(int(row.get("chunk_count") or 0) for row in rows)
    total_executions = sum(int(row.get("execution_count") or 0) for row in rows)
    total_items = sum(_execution_group_items_count(row) for row in rows)
    avg_items = (float(total_items) / total_groups) if total_groups else 0.0

    hot_groups = sorted(
        rows,
        key=lambda row: (float(row.get("updated_at") or 0.0), int(row.get("chunk_count") or 0)),
        reverse=True,
    )[:25]
    merge_pressure = []
    for row in rows:
        execution_count = max(1, int(row.get("execution_count") or 0))
        pressure = float(row.get("chunk_count") or 0) / float(execution_count)
        merge_pressure.append(
            {
                "convergence_key": row.get("convergence_key"),
                "tool_name": row.get("tool_name"),
                "chunk_count": int(row.get("chunk_count") or 0),
                "execution_count": int(row.get("execution_count") or 0),
                "items_count": _execution_group_items_count(row),
                "merge_pressure": pressure,
                "updated_at": float(row.get("updated_at") or 0.0),
            }
        )
    merge_pressure.sort(key=lambda item: item["merge_pressure"], reverse=True)
    return {
        "window_hours": hours,
        "count": total_groups,
        "totals": {
            "groups": total_groups,
            "chunk_updates": total_chunks,
            "executions": total_executions,
            "items": total_items,
            "avg_items_per_group": avg_items,
        },
        "hot_groups": hot_groups,
        "merge_pressure": merge_pressure[:25],
    }


@router.post("/execution-groups/compact")
async def compact_execution_groups(request: ExecutionGroupBulkCompactionRequest):
    store = get_execution_store()
    result = store.compact_execution_group_states(
        limit=request.limit,
        max_items=request.max_items,
        max_age_seconds=request.max_age_seconds,
    )
    return {
        "limit": request.limit,
        "max_items": request.max_items,
        "max_age_seconds": request.max_age_seconds,
        **result,
    }


@router.post("/execution-groups/cleanup")
async def cleanup_execution_groups(request: ExecutionGroupCleanupRequest):
    store = get_execution_store()
    removed = store.cleanup_old_execution_group_states(max_age_seconds=request.max_age_seconds)
    return {
        "max_age_seconds": request.max_age_seconds,
        "removed": removed,
    }


@router.post("/execution-groups/{convergence_key}/compact", response_model=ExecutionGroupStateModel)
async def compact_execution_group(
    convergence_key: str,
    request: ExecutionGroupCompactionRequest,
):
    store = get_execution_store()
    row = store.compact_execution_group_state(
        convergence_key=convergence_key,
        max_items=request.max_items,
        max_age_seconds=request.max_age_seconds,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Execution group not found: {convergence_key}")
    return ExecutionGroupStateModel.model_validate(_normalize_execution_group_row(row))


@router.get("/execution-groups/{convergence_key}", response_model=ExecutionGroupStateModel)
async def get_execution_group(convergence_key: str):
    store = get_execution_store()
    row = store.get_execution_group_state(convergence_key)
    if not row:
        raise HTTPException(status_code=404, detail=f"Execution group not found: {convergence_key}")
    return ExecutionGroupStateModel.model_validate(_normalize_execution_group_row(row))


@router.delete("/execution-groups/{convergence_key}")
async def delete_execution_group(convergence_key: str):
    store = get_execution_store()
    deleted = store.delete_execution_group_state(convergence_key)
    return {"convergence_key": convergence_key, "deleted": bool(deleted)}


@router.get("/follow-ups", response_model=FollowUpActionListResponse)
async def list_follow_up_actions(
    status: Optional[str] = Query(default=None),
    priority: Optional[str] = Query(default=None),
    action_type: Optional[str] = Query(default=None),
    source_type: Optional[str] = Query(default=None),
    trace_id: Optional[str] = Query(default=None),
    session_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    store = get_execution_store()
    rows = store.list_follow_up_actions(
        status=status,
        priority=priority,
        action_type=action_type,
        source_type=source_type,
        trace_id=trace_id,
        session_id=session_id,
        limit=limit,
    )
    actions = [FollowUpActionModel.model_validate(_normalize_follow_up_action_row(row)) for row in rows]
    return {"count": len(actions), "actions": actions}


@router.get("/follow-ups/analytics")
async def get_follow_up_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=25, ge=1, le=500),
):
    store = get_execution_store()
    return store.get_follow_up_action_analytics(hours=hours, limit=limit)


@router.post("/follow-ups/generate")
async def generate_follow_up_actions(request: FollowUpGenerateRequest):
    store = get_execution_store()
    since_ts = time.time() - (max(1, int(request.hours)) * 3600.0)
    rows = store.list_execution_group_states(
        updated_since=since_ts,
        limit=max(request.limit * 10, request.limit),
    )
    candidates = _build_execution_group_follow_up_candidates(
        rows=rows,
        merge_pressure_threshold=request.merge_pressure_threshold,
        items_threshold=request.items_threshold,
        max_items_target=request.max_items_target,
        include_compaction_actions=request.include_compaction_actions,
        include_reducer_actions=request.include_reducer_actions,
        include_fanout_actions=request.include_fanout_actions,
        trace_id=request.trace_id,
        session_id=request.session_id,
    )[: request.limit]

    if not request.auto_persist:
        return {
            "window_hours": request.hours,
            "count": len(candidates),
            "persisted": 0,
            "actions": candidates,
        }

    persisted: List[Dict[str, Any]] = []
    for candidate in candidates:
        created = store.append_follow_up_action(
            action_type=str(candidate.get("action_type") or "follow_up"),
            title=str(candidate.get("title") or "Follow-up action"),
            description=candidate.get("description"),
            priority=str(candidate.get("priority") or "normal"),
            source_type=str(candidate.get("source_type") or "execution_group"),
            source_id=candidate.get("source_id"),
            payload=candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {},
            metadata=candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {},
            trace_id=request.trace_id,
            session_id=request.session_id,
            dedupe_key=str(candidate.get("dedupe_key") or "") or None,
            dedupe_window_seconds=request.dedupe_window_seconds,
        )
        persisted.append(_normalize_follow_up_action_row(created))

    return {
        "window_hours": request.hours,
        "count": len(candidates),
        "persisted": len(persisted),
        "actions": persisted,
    }


@router.get("/follow-ups/{follow_up_id}", response_model=FollowUpActionModel)
async def get_follow_up_action(follow_up_id: str):
    store = get_execution_store()
    row = store.get_follow_up_action(follow_up_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Follow-up action not found: {follow_up_id}")
    return FollowUpActionModel.model_validate(_normalize_follow_up_action_row(row))


@router.post("/follow-ups/{follow_up_id}/status")
async def update_follow_up_action_status(
    follow_up_id: str,
    request: FollowUpStatusUpdateRequest,
):
    store = get_execution_store()
    updated = store.update_follow_up_action_status(
        follow_up_id=follow_up_id,
        status=request.status,
        metadata=request.metadata,
    )
    if not updated:
        raise HTTPException(status_code=404, detail=f"Follow-up action not found: {follow_up_id}")
    return {
        "status": "updated",
        "action": FollowUpActionModel.model_validate(_normalize_follow_up_action_row(updated)),
    }


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
    max_calls = max(1, int(os.getenv("EXECUTE_BATCH_MAX_CALLS", "256")))
    if len(request.calls) > max_calls:
        raise HTTPException(
            status_code=400,
            detail=f"Batch request too large: {len(request.calls)} calls (max {max_calls})",
        )

    for call in request.calls:
        if call.stream:
            raise HTTPException(status_code=400, detail="Batch calls do not support stream=true")

    if not request.calls:
        return BatchExecuteResponse(
            results=[],
            total=0,
            successful=0,
            failed=0,
            execution=request.execution,
            batch_plan={
                "trace_id": _resolve_trace_id(request.trace_id),
                "session_id": request.session_id,
                "total_calls": 0,
                "canonical_calls": 0,
                "deduped_calls": 0,
                "overflowed_canonical_calls": 0,
                "executed_canonical_calls": 0,
                "dedupe_calls": request.dedupe_calls,
                "dedupe_on_tool": request.dedupe_on_tool,
            },
            follow_up_actions=[],
        )

    batch_trace_id = _resolve_trace_id(request.trace_id or request.calls[0].trace_id)
    batch_session_id = request.session_id or request.calls[0].session_id

    canonical_calls: List[BatchCall] = []
    call_to_canonical: List[int] = []
    canonical_first_logical: Dict[int, int] = {}
    canonical_use_counts: Dict[int, int] = {}
    if request.dedupe_calls:
        canonical_by_fingerprint: Dict[str, int] = {}
        for logical_idx, call in enumerate(request.calls):
            fingerprint = _batch_call_fingerprint(call, dedupe_on_tool=request.dedupe_on_tool)
            canonical_idx = canonical_by_fingerprint.get(fingerprint)
            if canonical_idx is None:
                canonical_idx = len(canonical_calls)
                canonical_by_fingerprint[fingerprint] = canonical_idx
                canonical_calls.append(call)
                canonical_first_logical[canonical_idx] = logical_idx
                canonical_use_counts[canonical_idx] = 0
            call_to_canonical.append(canonical_idx)
            canonical_use_counts[canonical_idx] = canonical_use_counts.get(canonical_idx, 0) + 1
    else:
        canonical_calls = list(request.calls)
        call_to_canonical = list(range(len(request.calls)))
        canonical_first_logical = {idx: idx for idx in range(len(canonical_calls))}
        canonical_use_counts = {idx: 1 for idx in range(len(canonical_calls))}

    overflowed_canonical_indices: set[int] = set()
    executable_canonical_indices = list(range(len(canonical_calls)))
    if request.max_canonical_calls is not None and len(canonical_calls) > int(request.max_canonical_calls):
        overflowed_canonical_indices = set(range(int(request.max_canonical_calls), len(canonical_calls)))
        if request.overflow_policy == "reject":
            batch_plan = {
                "trace_id": batch_trace_id,
                "session_id": batch_session_id,
                "total_calls": len(request.calls),
                "canonical_calls": len(canonical_calls),
                "deduped_calls": len(request.calls) - len(canonical_calls),
                "max_canonical_calls": request.max_canonical_calls,
                "overflow_policy": request.overflow_policy,
                "overflowed_canonical_calls": len(overflowed_canonical_indices),
                "executed_canonical_calls": 0,
                "dedupe_calls": request.dedupe_calls,
                "dedupe_on_tool": request.dedupe_on_tool,
            }
            _persist_batch_follow_up_actions(
                trace_id=batch_trace_id,
                session_id=batch_session_id,
                request=request,
                batch_plan=batch_plan,
            )
            raise HTTPException(
                status_code=429,
                detail=_structured_error(
                    "BATCH_CANONICAL_LIMIT_EXCEEDED",
                    (
                        f"Canonical batch fanout {len(canonical_calls)} exceeds max_canonical_calls="
                        f"{request.max_canonical_calls}"
                    ),
                    category="resource_limit",
                    retryable=True,
                    retry_after_ms=1000,
                ),
            )
        executable_canonical_indices = list(range(int(request.max_canonical_calls)))

    async def _run_call(call: BatchCall):
        return await execute(
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
                convergence_key=call.convergence_key,
                fanout_budget=call.fanout_budget,
                group_reducer=call.group_reducer,
                group_reducer_config=call.group_reducer_config,
                callback_url=call.callback_url,
                callback_events=call.callback_events,
                trace_id=call.trace_id or batch_trace_id,
                session_id=call.session_id or batch_session_id,
                parent_execution_id=call.parent_execution_id,
            )
        )

    def _normalize_batch_result(call: BatchCall, raw: Any) -> ExecuteResponse:
        tool_name = _resolve_tool_name(call.tool_name, call.tool)
        trace_id = call.trace_id or batch_trace_id
        session_id = call.session_id or batch_session_id
        if isinstance(raw, Exception):
            return ExecuteResponse(
                status="error",
                tool=tool_name,
                execution_mode=call.execution_mode,
                error=_structured_error(
                    "BATCH_CALL_FAILED",
                    str(raw),
                    retryable=True,
                ),
                trace_id=trace_id,
                session_id=session_id,
                parent_execution_id=call.parent_execution_id,
            )
        if isinstance(raw, ExecuteResponse):
            normalized = _clone_execute_response(raw)
        else:
            normalized = ExecuteResponse.model_validate(raw)
        if normalized.trace_id is None:
            normalized.trace_id = trace_id
        if normalized.session_id is None:
            normalized.session_id = session_id
        if normalized.parent_execution_id is None:
            normalized.parent_execution_id = call.parent_execution_id
        return normalized

    canonical_results: Dict[int, ExecuteResponse] = {}
    if request.execution == "parallel":
        default_parallel = max(1, int(os.getenv("EXECUTE_BATCH_PARALLEL_LIMIT", "16")))
        parallel_limit = int(request.max_parallel or default_parallel)
        parallel_limit = max(1, min(parallel_limit, max(1, len(executable_canonical_indices))))
        semaphore = asyncio.Semaphore(parallel_limit)

        async def _run_indexed(canonical_idx: int, call: BatchCall):
            async with semaphore:
                try:
                    raw = await _run_call(call)
                except Exception as exc:
                    raw = exc
                return canonical_idx, raw

        tasks = [
            asyncio.create_task(_run_indexed(canonical_idx, canonical_calls[canonical_idx]))
            for canonical_idx in executable_canonical_indices
        ]
        raw_results = await asyncio.gather(*tasks)
        for canonical_idx, raw in raw_results:
            canonical_results[canonical_idx] = _normalize_batch_result(canonical_calls[canonical_idx], raw)
    else:
        for canonical_idx in executable_canonical_indices:
            call = canonical_calls[canonical_idx]
            try:
                raw = await _run_call(call)
            except Exception as exc:
                raw = exc
            canonical_results[canonical_idx] = _normalize_batch_result(call, raw)

    results: List[ExecuteResponse] = []
    for logical_idx, call in enumerate(request.calls):
        tool_name = _resolve_tool_name(call.tool_name, call.tool)
        canonical_idx = call_to_canonical[logical_idx]
        if canonical_idx in overflowed_canonical_indices:
            results.append(
                ExecuteResponse(
                    status="skipped",
                    tool=tool_name,
                    execution_mode=call.execution_mode,
                    error=_structured_error(
                        "BATCH_CANONICAL_LIMIT_OVERFLOW",
                        (
                            f"Call skipped due to canonical cap (max_canonical_calls={request.max_canonical_calls}, "
                            f"overflow_policy={request.overflow_policy})"
                        ),
                        category="resource_limit",
                        retryable=False,
                    ),
                    trace_id=call.trace_id or batch_trace_id,
                    session_id=call.session_id or batch_session_id,
                    parent_execution_id=call.parent_execution_id,
                )
            )
            continue

        canonical_result = canonical_results.get(canonical_idx)
        if not canonical_result:
            results.append(
                ExecuteResponse(
                    status="error",
                    tool=tool_name,
                    execution_mode=call.execution_mode,
                    error=_structured_error(
                        "BATCH_CALL_FAILED",
                        "Missing canonical batch result",
                        retryable=True,
                    ),
                    trace_id=call.trace_id or batch_trace_id,
                    session_id=call.session_id or batch_session_id,
                    parent_execution_id=call.parent_execution_id,
                )
            )
            continue

        deduped_logical = (
            request.dedupe_calls
            and canonical_use_counts.get(canonical_idx, 1) > 1
            and logical_idx != canonical_first_logical.get(canonical_idx, logical_idx)
        )
        cloned = _clone_execute_response(canonical_result, deduplicated=deduped_logical or canonical_result.deduplicated)
        cloned.trace_id = call.trace_id or cloned.trace_id or batch_trace_id
        cloned.session_id = call.session_id or cloned.session_id or batch_session_id
        cloned.parent_execution_id = call.parent_execution_id or cloned.parent_execution_id
        results.append(cloned)

    batch_plan = {
        "trace_id": batch_trace_id,
        "session_id": batch_session_id,
        "total_calls": len(request.calls),
        "canonical_calls": len(canonical_calls),
        "deduped_calls": len(request.calls) - len(canonical_calls),
        "max_canonical_calls": request.max_canonical_calls,
        "overflow_policy": request.overflow_policy,
        "overflowed_canonical_calls": len(overflowed_canonical_indices),
        "executed_canonical_calls": len(executable_canonical_indices),
        "dedupe_calls": request.dedupe_calls,
        "dedupe_on_tool": request.dedupe_on_tool,
    }
    follow_up_actions = _persist_batch_follow_up_actions(
        trace_id=batch_trace_id,
        session_id=batch_session_id,
        request=request,
        batch_plan=batch_plan,
    )

    successful = sum(1 for r in results if r.status in {"completed", "submitted", "running_background"})
    return BatchExecuteResponse(
        results=results,
        total=len(results),
        successful=successful,
        failed=len(results) - successful,
        execution=request.execution,
        batch_plan=batch_plan,
        follow_up_actions=follow_up_actions,
    )
