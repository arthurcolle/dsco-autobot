"""
Execution policy engine for adaptive mode selection.

Selects sync/stream/job based on durable telemetry + caller constraints.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4


ExecutionMode = Literal["auto", "sync", "stream", "job"]
ResolvedMode = Literal["sync", "stream", "job"]


@dataclass
class PolicyConstraints:
    latency_budget_ms: Optional[float] = None
    cost_budget: Optional[float] = None
    reliability_target: Optional[float] = None
    prefer_streaming: bool = False
    allow_sync: bool = True
    allow_stream: bool = True
    allow_job: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "latency_budget_ms": self.latency_budget_ms,
            "cost_budget": self.cost_budget,
            "reliability_target": self.reliability_target,
            "prefer_streaming": self.prefer_streaming,
            "allow_sync": self.allow_sync,
            "allow_stream": self.allow_stream,
            "allow_job": self.allow_job,
        }


@dataclass
class PolicyDecision:
    decision_id: str
    selected_mode: ResolvedMode
    requested_mode: ExecutionMode
    latency_class: str
    backend_id: Optional[str]
    scores: Dict[str, float] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)
    confidence: float = 0.0
    constraints: Dict[str, Any] = field(default_factory=dict)
    model_version: str = "policy_v1"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "selected_mode": self.selected_mode,
            "requested_mode": self.requested_mode,
            "latency_class": self.latency_class,
            "backend_id": self.backend_id,
            "scores": self.scores,
            "reasons": self.reasons,
            "confidence": self.confidence,
            "constraints": self.constraints,
            "model_version": self.model_version,
        }


def _latency_class(p95_latency_ms: float) -> str:
    if p95_latency_ms <= 0:
        return "unknown"
    if p95_latency_ms < 2000:
        return "fast"
    if p95_latency_ms <= 30000:
        return "medium"
    return "slow"


def choose_execution_mode(
    *,
    tool_name: str,
    requested_mode: ExecutionMode,
    tool_store: Any,
    constraints: Optional[PolicyConstraints] = None,
) -> PolicyDecision:
    constraints = constraints or PolicyConstraints()
    decision_id = f"pd_{uuid4().hex}"

    backend_id: Optional[str] = None
    metrics: Dict[str, Any] = {}
    try:
        backend_id, _backend_url, _resolved_tool_name = tool_store.resolve_tool(tool_name)
    except Exception:
        backend_id = None
    try:
        metrics = tool_store.get_tool_latency_metrics(tool_name, hours=24) or {}
    except Exception:
        metrics = {}

    p95_latency_ms = float(metrics.get("p95_latency_ms") or 0.0)
    success_rate = float(metrics.get("success_rate") or 0.0)
    call_count = int(metrics.get("calls") or 0)
    latency_class = _latency_class(p95_latency_ms)

    # Fixed requested mode wins, policy only annotates.
    if requested_mode in {"sync", "stream", "job"}:
        return PolicyDecision(
            decision_id=decision_id,
            selected_mode=requested_mode,  # type: ignore[arg-type]
            requested_mode=requested_mode,
            latency_class=latency_class,
            backend_id=backend_id,
            scores={requested_mode: 1.0},
            reasons=["caller_selected_mode"],
            confidence=1.0,
            constraints=constraints.to_dict(),
        )

    scores: Dict[str, float] = {"sync": 0.0, "stream": 0.0, "job": 0.0}
    reasons: List[str] = []

    # Latency-class priors.
    if latency_class == "fast":
        scores["sync"] += 2.0
        scores["stream"] += 0.8
        scores["job"] -= 0.6
        reasons.append("latency_class_fast_prefers_sync")
    elif latency_class == "medium":
        scores["stream"] += 2.0
        scores["sync"] += 0.6
        scores["job"] += 0.5
        reasons.append("latency_class_medium_prefers_stream")
    elif latency_class == "slow":
        scores["job"] += 2.2
        scores["stream"] += 1.0
        scores["sync"] -= 1.8
        reasons.append("latency_class_slow_prefers_job")
    else:
        # Unknown history: pick stream-biased adaptive middle ground.
        scores["stream"] += 1.2
        scores["sync"] += 0.8
        reasons.append("latency_unknown_bias_stream_sync")

    # Caller constraints.
    latency_budget_ms = constraints.latency_budget_ms
    if latency_budget_ms and latency_budget_ms > 0:
        if p95_latency_ms > 0 and p95_latency_ms <= latency_budget_ms:
            scores["sync"] += 1.2
            reasons.append("meets_latency_budget_sync_bonus")
        elif p95_latency_ms > 0 and p95_latency_ms > latency_budget_ms:
            scores["sync"] -= 1.4
            scores["stream"] += 0.7
            scores["job"] += 0.9
            reasons.append("breaches_latency_budget_penalize_sync")

    reliability_target = constraints.reliability_target
    if reliability_target is not None and 0.0 <= reliability_target <= 1.0:
        if call_count >= 3 and success_rate < reliability_target:
            scores["sync"] -= 1.0
            scores["stream"] += 0.5
            scores["job"] += 0.8
            reasons.append("reliability_below_target_avoid_sync")
        elif call_count >= 3 and success_rate >= reliability_target:
            scores["sync"] += 0.3
            reasons.append("reliability_above_target_sync_ok")

    if constraints.prefer_streaming:
        scores["stream"] += 1.0
        reasons.append("prefer_streaming_hint")

    if constraints.cost_budget is not None:
        # Rough heuristic: smaller budgets avoid scheduler overhead.
        if constraints.cost_budget <= 0.5:
            scores["sync"] += 0.4
            scores["job"] -= 0.5
            reasons.append("tight_cost_budget_bias_sync")
        elif constraints.cost_budget >= 2.0:
            scores["job"] += 0.2
            reasons.append("relaxed_cost_budget_allows_job")

    if not constraints.allow_sync:
        scores["sync"] = -1e9
        reasons.append("sync_disallowed_by_policy")
    if not constraints.allow_stream:
        scores["stream"] = -1e9
        reasons.append("stream_disallowed_by_policy")
    if not constraints.allow_job:
        scores["job"] = -1e9
        reasons.append("job_disallowed_by_policy")

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    selected_mode = ranked[0][0] if ranked else "sync"
    if selected_mode not in {"sync", "stream", "job"}:
        selected_mode = "sync"

    top = ranked[0][1] if ranked else 0.0
    second = ranked[1][1] if len(ranked) > 1 else top
    separation = max(0.0, float(top - second))
    history_weight = min(1.0, call_count / 20.0)
    confidence = min(1.0, 0.4 + (separation / 3.0) + (0.4 * history_weight))

    return PolicyDecision(
        decision_id=decision_id,
        selected_mode=selected_mode,  # type: ignore[arg-type]
        requested_mode="auto",
        latency_class=latency_class,
        backend_id=backend_id,
        scores={k: float(v) for k, v in scores.items()},
        reasons=reasons,
        confidence=confidence,
        constraints=constraints.to_dict(),
    )
