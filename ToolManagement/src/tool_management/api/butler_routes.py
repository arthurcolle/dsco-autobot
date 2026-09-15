"""Declarative Life OS / private-butler blueprint planning.

This module deliberately stops at planning.  Tool Management already owns the
canonical execution, admission, persistence, replay, and approval surfaces.  A
blueprint describes the structure those surfaces should execute without
creating a second, less-governed execution path.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Sequence

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/butler", tags=["butler-blueprints"])


class LifeArea(str, Enum):
    WORK = "work"
    HOME = "home"
    TRAVEL = "travel"
    MONEY = "money"
    HEALTH = "health"
    PEOPLE = "people"


class InitiativeMode(str, Enum):
    ON_CALL = "oncall"
    ATTENTIVE = "attentive"
    PROACTIVE = "proactive"
    STEWARD = "steward"


class FollowThrough(str, Enum):
    LIGHT = "light"
    STEADY = "steady"
    EXACTING = "exacting"


class Authority(str, Enum):
    SEE = "see"
    PREPARE = "prepare"
    ASK = "ask"
    ACT = "act"
    SPEND_TO_LIMIT = "spend_to_limit"


class Cadence(str, Enum):
    ON_DEMAND = "on_demand"
    EVENT_DRIVEN = "event_driven"
    DAILY = "daily"
    WEEKLY = "weekly"
    CONTINUOUS = "continuous"


class EffectClass(str, Enum):
    READ = "read"
    PREPARE = "prepare"
    ACT = "act"
    SPEND = "spend"


class ButlerConstraints(BaseModel):
    """Global planning and House Rule constraints."""

    max_steps: int = Field(default=40, ge=1, le=100)
    max_parallelism: int = Field(default=6, ge=1, le=32)
    max_spend_cents: Optional[int] = Field(default=None, ge=0)
    require_approval_for_irreversible: bool = True
    minimum_tool_confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class StructuralDesire(BaseModel):
    """One user-declared outcome and the authority granted to pursue it."""

    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{1,63}$")
    area: LifeArea
    outcome: str = Field(min_length=3, max_length=1000)
    priority: int = Field(default=3, ge=1, le=5, description="1 is highest")
    cadence: Cadence = Cadence.ON_DEMAND
    authority: Authority = Authority.ASK
    depends_on: List[str] = Field(default_factory=list, max_length=30)
    preferred_tools: List[str] = Field(default_factory=list, max_length=20)
    inputs: Dict[str, Any] = Field(default_factory=dict)
    irreversible: bool = False
    estimated_spend_cents: Optional[int] = Field(default=None, ge=0)

    @field_validator("depends_on", "preferred_tools")
    @classmethod
    def _deduplicate_lists(cls, value: List[str]) -> List[str]:
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("values must be unique")
        return cleaned


class ButlerBlueprintRequest(BaseModel):
    """Declarative request for a constrained butler structure and workflow."""

    name: str = Field(default="My Life Desk", min_length=2, max_length=120)
    objective: str = Field(min_length=3, max_length=2000)
    initiative: InitiativeMode = InitiativeMode.ATTENTIVE
    follow_through: FollowThrough = FollowThrough.STEADY
    desires: List[StructuralDesire] = Field(min_length=1, max_length=100)
    constraints: ButlerConstraints = Field(default_factory=ButlerConstraints)
    context: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_structure(self) -> "ButlerBlueprintRequest":
        ids = [desire.id for desire in self.desires]
        if len(ids) != len(set(ids)):
            raise ValueError("desire ids must be unique")
        if len(ids) > self.constraints.max_steps:
            raise ValueError("desire count exceeds constraints.max_steps")

        known = set(ids)
        for desire in self.desires:
            if desire.id in desire.depends_on:
                raise ValueError(f"desire '{desire.id}' cannot depend on itself")
            unknown = sorted(set(desire.depends_on) - known)
            if unknown:
                raise ValueError(
                    f"desire '{desire.id}' has unknown dependencies: {', '.join(unknown)}"
                )

        # Kahn validation keeps malformed dependency graphs out of the planner.
        indegree = {desire.id: len(desire.depends_on) for desire in self.desires}
        children: Dict[str, List[str]] = defaultdict(list)
        for desire in self.desires:
            for dependency in desire.depends_on:
                children[dependency].append(desire.id)
        ready = sorted(node_id for node_id, degree in indegree.items() if degree == 0)
        visited = 0
        while ready:
            node_id = ready.pop(0)
            visited += 1
            for child in sorted(children[node_id]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        if visited != len(ids):
            cyclic = sorted(node_id for node_id, degree in indegree.items() if degree > 0)
            raise ValueError(f"desire dependencies contain a cycle: {', '.join(cyclic)}")
        return self


class ToolCandidate(BaseModel):
    """Normalized candidate returned by any discovery implementation."""

    tool_name: str
    backend: str = ""
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""
    inputs_template: Dict[str, Any] = Field(default_factory=dict)

    @property
    def tool_ref(self) -> str:
        if self.backend and self.backend not in {"registry", "local", "local-registry"}:
            return f"{self.backend}:{self.tool_name}"
        return self.tool_name


class CandidateResolver(Protocol):
    async def resolve(
        self,
        desire: StructuralDesire,
        intent: str,
        context: Dict[str, Any],
        limit: int,
    ) -> Sequence[ToolCandidate]: ...


class OrchestrationCandidateResolver:
    """Adapter over the existing semantic ContextAwareRecommender."""

    async def resolve(
        self,
        desire: StructuralDesire,
        intent: str,
        context: Dict[str, Any],
        limit: int,
    ) -> Sequence[ToolCandidate]:
        # Lazy import keeps this planner contract usable without loading the
        # embedding stack during API/schema import and unit tests.
        from .advanced_orchestration import (
            WorkflowContext,
            WorkflowIntent,
            get_orchestrator,
        )

        available_data = dict(context)
        available_data.update(desire.inputs)
        recommendations = await get_orchestrator().recommender.recommend_workflow(
            WorkflowContext(
                intent=WorkflowIntent(intent),
                user_query=desire.outcome,
                available_data=available_data,
            ),
            max_steps=limit,
        )
        return [
            ToolCandidate(
                tool_name=item.tool_name,
                backend=item.backend,
                confidence=float(item.confidence),
                reasoning=item.reasoning,
                inputs_template=dict(item.inputs_template or {}),
            )
            for item in recommendations
        ]


class ApprovalGate(BaseModel):
    type: str = "agent_approval"
    reason: str
    default_action: str = "abort"


class ButlerWorkflowStep(BaseModel):
    id: str
    area: LifeArea
    outcome: str
    cadence: Cadence
    authority: Authority
    depends_on: List[str]
    tool: str
    backend: str
    arguments_template: Dict[str, Any]
    confidence: float
    selection_reason: str
    effect: EffectClass
    approval_gate: Optional[ApprovalGate] = None
    irreversible: bool = False
    estimated_spend_cents: Optional[int] = None


class WorkflowStage(BaseModel):
    index: int
    execution: str
    step_ids: List[str]


class ButlerComponent(BaseModel):
    id: str
    name: str
    area: LifeArea
    responsibility_ids: List[str]
    initiative: InitiativeMode
    follow_through: FollowThrough
    authorities: List[Authority]


class UnresolvedDesire(BaseModel):
    desire_id: str
    reason: str
    detail: str


class OptimizationSummary(BaseModel):
    method: str
    objective_score: float
    candidates_considered: int
    selected_steps: int
    unresolved_desires: int
    stage_count: int
    selection_basis: List[str]


class ExecutionContract(BaseModel):
    mode: str = "plan_only"
    direct_execution_supported: bool = False
    tool_execute_endpoint: str = "/api/v1/tools/execute"
    batch_plan_endpoint: str = "/api/v1/execute/batch/plan"
    batch_execute_endpoint: str = "/api/v1/execute/batch"
    composition_definition_endpoint: str = "/api/v1/compose/workflows"
    composition_execute_endpoint: str = "/api/v1/compose/execute"
    note: str = (
        "Execute through the canonical Tool Management surfaces after resolving "
        "templates, dependency outputs, and approval gates."
    )


class ButlerBlueprintResponse(BaseModel):
    blueprint_id: str
    version: str = "1.0.0"
    generated_at: datetime
    name: str
    objective: str
    initiative: InitiativeMode
    follow_through: FollowThrough
    components: List[ButlerComponent]
    steps: List[ButlerWorkflowStep]
    stages: List[WorkflowStage]
    unresolved: List[UnresolvedDesire]
    optimization: OptimizationSummary
    execution: ExecutionContract = Field(default_factory=ExecutionContract)


class IntentCompileRequest(BaseModel):
    """Compact natural-language entry point into the structural planner."""

    intent: str = Field(min_length=3, max_length=2000)
    name: str = Field(default="My Life Desk", min_length=2, max_length=120)
    initiative: InitiativeMode = InitiativeMode.ATTENTIVE
    follow_through: FollowThrough = FollowThrough.STEADY
    areas: List[LifeArea] = Field(default_factory=list, max_length=6)
    cadence: Cadence = Cadence.ON_DEMAND
    authority: Authority = Authority.ASK
    preferred_tools: List[str] = Field(default_factory=list, max_length=20)
    context: Dict[str, Any] = Field(default_factory=dict)
    constraints: ButlerConstraints = Field(default_factory=ButlerConstraints)

    @field_validator("areas", "preferred_tools")
    @classmethod
    def _unique_values(cls, value: list) -> list:
        if len(value) != len(set(value)):
            raise ValueError("values must be unique")
        return value


class IntentCompileResponse(BaseModel):
    normalized_intent: str
    inferred_areas: List[LifeArea]
    structural_request: ButlerBlueprintRequest
    blueprint: ButlerBlueprintResponse


class SubcomponentCatalogItem(BaseModel):
    id: str
    area: LifeArea
    name: str
    accepts: List[str]
    emits: List[str]
    realtime_surfaces: List[str]


class SubcomponentResolution(BaseModel):
    name: str
    objective: str
    components: List[ButlerComponent]
    contract: Dict[str, str]


_INTENT_PATTERNS = (
    ("publish", re.compile(r"\b(publish|post|broadcast|release)\b", re.I)),
    ("notify", re.compile(r"\b(notify|message|email|send|tell|alert|remind)\b", re.I)),
    ("remember", re.compile(r"\b(remember|record|store|retain|log)\b", re.I)),
    ("research", re.compile(r"\b(find|search|research|discover|look up|verify)\b", re.I)),
    ("analyze", re.compile(r"\b(analy[sz]e|compare|assess|monitor|review|summarize)\b", re.I)),
)

_EFFECT_PATTERNS = (
    (
        EffectClass.SPEND,
        re.compile(r"(^|[_:\-])(pay|purchase|buy|book|reserve|order|trade)([_:\-]|$)", re.I),
    ),
    (
        EffectClass.PREPARE,
        re.compile(r"(^|[_:\-])(draft|prepare|preview|plan|compose)([_:\-]|$)", re.I),
    ),
    (
        EffectClass.READ,
        re.compile(
            r"(^|[_:\-])(get|list|search|find|read|fetch|retrieve|query|analyze|inspect|check|monitor|status|lookup|verify)([_:\-]|$)",
            re.I,
        ),
    ),
)

_AREA_NAMES = {
    LifeArea.WORK: "Work & ventures",
    LifeArea.HOME: "Home & estate",
    LifeArea.TRAVEL: "Travel",
    LifeArea.MONEY: "Money & assets",
    LifeArea.HEALTH: "Health & training",
    LifeArea.PEOPLE: "People",
}

_AREA_PATTERNS = {
    LifeArea.WORK: re.compile(
        r"\b(work|board|company|business|client|investor|meeting|project|research)\b",
        re.I,
    ),
    LifeArea.HOME: re.compile(
        r"\b(home|house|property|estate|delivery|maintenance|dinner|staff)\b",
        re.I,
    ),
    LifeArea.TRAVEL: re.compile(
        r"\b(travel|trip|flight|hotel|driver|arrival|departure|itinerary|aspen)\b",
        re.I,
    ),
    LifeArea.MONEY: re.compile(
        r"\b(money|finance|asset|bill|spend|budget|portfolio|payment|price)\b",
        re.I,
    ),
    LifeArea.HEALTH: re.compile(
        r"\b(health|fitness|training|workout|diet|sleep|recovery|fruit)\b",
        re.I,
    ),
    LifeArea.PEOPLE: re.compile(
        r"\b(people|family|friend|relationship|birthday|guest|contact|message)\b",
        re.I,
    ),
}


def _resolve_components(request: ButlerBlueprintRequest) -> List[ButlerComponent]:
    """Build stable life-area subcomponents without performing tool discovery."""
    components: List[ButlerComponent] = []
    for area in LifeArea:
        area_desires = [desire for desire in request.desires if desire.area == area]
        if not area_desires:
            continue
        components.append(
            ButlerComponent(
                id=f"{area.value}-desk",
                name=_AREA_NAMES[area],
                area=area,
                responsibility_ids=[desire.id for desire in area_desires],
                initiative=request.initiative,
                follow_through=request.follow_through,
                authorities=sorted(
                    {desire.authority for desire in area_desires},
                    key=lambda authority: list(Authority).index(authority),
                ),
            )
        )
    return components


def _infer_life_areas(intent: str) -> List[LifeArea]:
    inferred = [area for area in LifeArea if _AREA_PATTERNS[area].search(intent)]
    return inferred or [LifeArea.WORK]


def _intent_to_structural_request(request: IntentCompileRequest) -> ButlerBlueprintRequest:
    areas = request.areas or _infer_life_areas(request.intent)
    desires = [
        StructuralDesire(
            id=f"{area.value}_outcome",
            area=area,
            outcome=request.intent,
            priority=index + 1 if index < 4 else 5,
            cadence=request.cadence,
            authority=request.authority,
            preferred_tools=list(request.preferred_tools),
            inputs={"intent": request.intent, "life_area": area.value},
        )
        for index, area in enumerate(areas)
    ]
    return ButlerBlueprintRequest(
        name=request.name,
        objective=request.intent,
        initiative=request.initiative,
        follow_through=request.follow_through,
        desires=desires,
        constraints=request.constraints,
        context=request.context,
    )


def _infer_intent(desire: StructuralDesire) -> str:
    for intent, pattern in _INTENT_PATTERNS:
        if pattern.search(desire.outcome):
            return intent
    if desire.area in {LifeArea.MONEY, LifeArea.HEALTH}:
        return "analyze"
    return "transform"


def _classify_effect(tool_ref: str) -> EffectClass:
    for effect, pattern in _EFFECT_PATTERNS:
        if pattern.search(tool_ref):
            return effect
    # Unknown verbs are treated as actions.  Read-only authority must be earned
    # by an explicitly recognizable read operation, never inferred by absence.
    return EffectClass.ACT


def _tool_matches_preference(candidate: ToolCandidate, preferences: Sequence[str]) -> bool:
    if not preferences:
        return True
    names = {candidate.tool_name.lower(), candidate.tool_ref.lower()}
    return any(preference.lower() in names for preference in preferences)


def _gate_for(
    desire: StructuralDesire,
    effect: EffectClass,
    constraints: ButlerConstraints,
) -> tuple[bool, Optional[ApprovalGate], Optional[str]]:
    """Return (admissible, gate, rejection reason)."""

    if effect == EffectClass.READ:
        if desire.irreversible and constraints.require_approval_for_irreversible:
            return True, ApprovalGate(reason="irreversible action requires approval"), None
        return True, None, None
    if effect == EffectClass.PREPARE:
        if desire.authority == Authority.SEE:
            return False, None, "authority 'see' permits read-only tools"
        if desire.irreversible and constraints.require_approval_for_irreversible:
            return True, ApprovalGate(reason="irreversible action requires approval"), None
        return True, None, None
    if effect == EffectClass.ACT:
        if desire.authority in {Authority.SEE, Authority.PREPARE}:
            return False, None, f"authority '{desire.authority.value}' does not permit acting"
        if desire.irreversible and constraints.require_approval_for_irreversible:
            return True, ApprovalGate(reason="irreversible action requires approval"), None
        if desire.authority == Authority.ASK:
            return True, ApprovalGate(reason="House Rule requires approval before acting"), None
        return True, None, None

    # Spending is allowed unattended only when the desire grants spend authority,
    # both amounts are known, and the estimate is inside the global limit.
    if desire.authority in {Authority.SEE, Authority.PREPARE}:
        return False, None, f"authority '{desire.authority.value}' does not permit spending"
    if desire.irreversible and constraints.require_approval_for_irreversible:
        return True, ApprovalGate(reason="irreversible action requires approval"), None
    within_limit = (
        desire.authority == Authority.SPEND_TO_LIMIT
        and desire.estimated_spend_cents is not None
        and constraints.max_spend_cents is not None
        and desire.estimated_spend_cents <= constraints.max_spend_cents
    )
    if within_limit:
        return True, None, None
    return True, ApprovalGate(reason="spending is unknown or outside unattended authority"), None


def _topological_stages(
    desires: Sequence[StructuralDesire],
    included_ids: set[str],
    max_parallelism: int,
) -> tuple[List[str], List[List[str]]]:
    by_id = {desire.id: desire for desire in desires if desire.id in included_ids}
    indegree = {
        desire_id: len([dep for dep in desire.depends_on if dep in included_ids])
        for desire_id, desire in by_id.items()
    }
    children: Dict[str, List[str]] = defaultdict(list)
    for desire in by_id.values():
        for dependency in desire.depends_on:
            if dependency in included_ids:
                children[dependency].append(desire.id)

    order: List[str] = []
    stages: List[List[str]] = []
    while indegree:
        ready = sorted(
            (node_id for node_id, degree in indegree.items() if degree == 0),
            key=lambda node_id: (by_id[node_id].priority, node_id),
        )
        if not ready:  # Defensive: request validation already rejects cycles.
            raise ValueError("selected workflow contains a dependency cycle")
        for offset in range(0, len(ready), max_parallelism):
            chunk = ready[offset : offset + max_parallelism]
            stages.append(chunk)
            order.extend(chunk)
        for node_id in ready:
            indegree.pop(node_id)
            for child in children[node_id]:
                if child in indegree:
                    indegree[child] -= 1
    return order, stages


class ButlerBlueprintPlanner:
    """Deterministic best-feasible planner over discovered tool candidates."""

    def __init__(self, resolver: Optional[CandidateResolver] = None):
        self.resolver = resolver or OrchestrationCandidateResolver()

    async def _resolve_candidates(
        self,
        request: ButlerBlueprintRequest,
    ) -> tuple[Dict[str, List[ToolCandidate]], Dict[str, str]]:
        candidates: Dict[str, List[ToolCandidate]] = {}
        failures: Dict[str, str] = {}

        async def resolve_one(desire: StructuralDesire) -> None:
            try:
                resolved = await self.resolver.resolve(
                    desire,
                    _infer_intent(desire),
                    request.context,
                    10,
                )
                unique: Dict[str, ToolCandidate] = {}
                for item in resolved:
                    current = unique.get(item.tool_ref)
                    if current is None or item.confidence > current.confidence:
                        unique[item.tool_ref] = item
                candidates[desire.id] = sorted(
                    unique.values(),
                    key=lambda item: (-item.confidence, item.tool_ref),
                )
            except Exception as exc:  # Discovery failure should remain visible, not destroy the plan.
                logger.warning("Butler tool discovery failed for %s: %s", desire.id, exc)
                candidates[desire.id] = []
                failures[desire.id] = str(exc)

        # The first lookup completes the recommender's lazy index before the
        # remaining independent lookups fan out.
        first, *remaining = request.desires
        await resolve_one(first)
        if remaining:
            semaphore = asyncio.Semaphore(min(8, request.constraints.max_parallelism))

            async def bounded(desire: StructuralDesire) -> None:
                async with semaphore:
                    await resolve_one(desire)

            await asyncio.gather(*(bounded(desire) for desire in remaining))
        return candidates, failures

    async def plan(self, request: ButlerBlueprintRequest) -> ButlerBlueprintResponse:
        candidates_by_desire, discovery_failures = await self._resolve_candidates(request)
        by_id = {desire.id: desire for desire in request.desires}
        all_ids = set(by_id)
        full_order, _ = _topological_stages(
            request.desires,
            all_ids,
            request.constraints.max_parallelism,
        )

        selected: Dict[str, ButlerWorkflowStep] = {}
        unresolved: List[UnresolvedDesire] = []
        candidate_count = sum(len(items) for items in candidates_by_desire.values())

        for desire_id in full_order:
            desire = by_id[desire_id]
            blocked_dependencies = [dep for dep in desire.depends_on if dep not in selected]
            if blocked_dependencies:
                unresolved.append(
                    UnresolvedDesire(
                        desire_id=desire.id,
                        reason="dependency_unresolved",
                        detail=f"unresolved dependencies: {', '.join(blocked_dependencies)}",
                    )
                )
                continue

            raw_candidates = candidates_by_desire.get(desire.id, [])
            preferred_matches = [
                item
                for item in raw_candidates
                if _tool_matches_preference(item, desire.preferred_tools)
            ]
            matching = [
                item
                for item in preferred_matches
                if item.confidence >= request.constraints.minimum_tool_confidence
            ]

            feasible: List[tuple[ToolCandidate, EffectClass, Optional[ApprovalGate]]] = []
            rejection_reasons: List[str] = []
            for item in matching:
                effect = _classify_effect(item.tool_ref)
                admissible, gate, rejection = _gate_for(desire, effect, request.constraints)
                if admissible:
                    feasible.append((item, effect, gate))
                elif rejection:
                    rejection_reasons.append(rejection)

            if not feasible:
                if discovery_failures.get(desire.id):
                    reason = "discovery_failed"
                    detail = discovery_failures[desire.id]
                elif desire.preferred_tools and raw_candidates and not preferred_matches:
                    reason = "preferred_tool_unavailable"
                    detail = "none of the discovered tools matched preferred_tools"
                elif preferred_matches and not matching:
                    reason = "confidence_below_threshold"
                    detail = "matching tools were below constraints.minimum_tool_confidence"
                elif matching and rejection_reasons:
                    reason = "authority_insufficient"
                    detail = sorted(set(rejection_reasons))[0]
                else:
                    reason = "no_tool_candidate"
                    detail = "no discovered tool satisfied confidence and House Rule constraints"
                unresolved.append(
                    UnresolvedDesire(desire_id=desire.id, reason=reason, detail=detail)
                )
                continue

            # Confidence is primary; lower-effect tools win deterministic ties.
            effect_rank = {
                EffectClass.READ: 0,
                EffectClass.PREPARE: 1,
                EffectClass.ACT: 2,
                EffectClass.SPEND: 3,
            }
            candidate, effect, gate = sorted(
                feasible,
                key=lambda row: (-row[0].confidence, effect_rank[row[1]], row[0].tool_ref),
            )[0]
            arguments = dict(candidate.inputs_template)
            arguments.update(desire.inputs)
            selected[desire.id] = ButlerWorkflowStep(
                id=desire.id,
                area=desire.area,
                outcome=desire.outcome,
                cadence=desire.cadence,
                authority=desire.authority,
                depends_on=list(desire.depends_on),
                tool=candidate.tool_ref,
                backend=candidate.backend,
                arguments_template=arguments,
                confidence=candidate.confidence,
                selection_reason=candidate.reasoning or "highest-confidence feasible candidate",
                effect=effect,
                approval_gate=gate,
                irreversible=desire.irreversible,
                estimated_spend_cents=desire.estimated_spend_cents,
            )

        selected_ids = set(selected)
        selected_order, raw_stages = _topological_stages(
            request.desires,
            selected_ids,
            request.constraints.max_parallelism,
        )
        steps = [selected[step_id] for step_id in selected_order]
        stages = [
            WorkflowStage(
                index=index,
                execution="parallel" if len(step_ids) > 1 else "sequential",
                step_ids=step_ids,
            )
            for index, step_ids in enumerate(raw_stages)
        ]

        components = _resolve_components(request)

        total_weight = sum(6 - by_id[step.id].priority for step in steps)
        weighted_confidence = sum(
            (6 - by_id[step.id].priority) * step.confidence for step in steps
        )
        objective_score = weighted_confidence / total_weight if total_weight else 0.0

        stable_payload = {
            "request": request.model_dump(mode="json"),
            "components": [component.model_dump(mode="json") for component in components],
            "steps": [step.model_dump(mode="json") for step in steps],
            "stages": [stage.model_dump(mode="json") for stage in stages],
            "unresolved": [item.model_dump(mode="json") for item in unresolved],
        }
        digest = hashlib.sha256(
            json.dumps(stable_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:20]

        return ButlerBlueprintResponse(
            blueprint_id=f"butler_bp_{digest}",
            generated_at=datetime.now(timezone.utc),
            name=request.name,
            objective=request.objective,
            initiative=request.initiative,
            follow_through=request.follow_through,
            components=components,
            steps=steps,
            stages=stages,
            unresolved=unresolved,
            optimization=OptimizationSummary(
                method="deterministic_best_feasible_candidate",
                objective_score=round(objective_score, 6),
                candidates_considered=candidate_count,
                selected_steps=len(steps),
                unresolved_desires=len(unresolved),
                stage_count=len(stages),
                selection_basis=[
                    "semantic confidence",
                    "explicit tool preference",
                    "dependency feasibility",
                    "House Rule authority",
                    "spend and irreversible-action constraints",
                    "priority-aware bounded parallelism",
                ],
            ),
        )


_planner = ButlerBlueprintPlanner()


@router.post("/blueprints/plan", response_model=ButlerBlueprintResponse)
async def plan_butler_blueprint(request: ButlerBlueprintRequest) -> ButlerBlueprintResponse:
    """Compile structural desires into a safe, inspectable butler blueprint."""

    return await _planner.plan(request)


@router.get("/subcomponents/catalog", response_model=List[SubcomponentCatalogItem])
async def list_butler_subcomponents() -> List[SubcomponentCatalogItem]:
    """Describe the stable subcomponents that a user structure can activate."""
    return [
        SubcomponentCatalogItem(
            id=f"{area.value}-desk",
            area=area,
            name=_AREA_NAMES[area],
            accepts=["structural_desire", "context", "house_rules", "dependencies"],
            emits=["intent", "workflow_step", "artifact", "decision"],
            realtime_surfaces=["call", "events", "widget", "webhook"],
        )
        for area in LifeArea
    ]


@router.post("/subcomponents/resolve", response_model=SubcomponentResolution)
async def resolve_butler_subcomponents(
    request: ButlerBlueprintRequest,
) -> SubcomponentResolution:
    """Resolve user desires into stable life-area components without executing tools."""
    return SubcomponentResolution(
        name=request.name,
        objective=request.objective,
        components=_resolve_components(request),
        contract={
            "intent_compile": "/api/v1/butler/intents/compile",
            "workflow_compile": "/api/v1/butler/workflows/compile",
            "blueprint_plan": "/api/v1/butler/blueprints/plan",
        },
    )


@router.post("/intents/compile", response_model=IntentCompileResponse)
async def compile_butler_intent(request: IntentCompileRequest) -> IntentCompileResponse:
    """Infer life areas, then compile a natural-language intent into a safe blueprint."""
    structural_request = _intent_to_structural_request(request)
    blueprint = await _planner.plan(structural_request)
    return IntentCompileResponse(
        normalized_intent=" ".join(request.intent.split()),
        inferred_areas=[desire.area for desire in structural_request.desires],
        structural_request=structural_request,
        blueprint=blueprint,
    )


@router.post("/workflows/compile", response_model=ButlerBlueprintResponse)
async def compile_butler_workflow(
    request: ButlerBlueprintRequest,
) -> ButlerBlueprintResponse:
    """Compile explicit structural desires into the canonical workflow handoff."""
    return await _planner.plan(request)
