"""Budgets router for budget and quota management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions
from ..schemas import (
    BudgetUpsertRequest,
    BudgetResponse,
    BudgetStatusResponse,
)
from .dependencies import (
    neo4j_query_one,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(prefix="/v1/budgets", tags=["budgets"])


@router.post("/upsert", response_model=BudgetResponse)
async def upsert_budget(
    req: BudgetUpsertRequest,
    _rbac: None = Depends(require_permissions_effective(["budgets:write"])),
    _abac: None = Depends(enforce_abac("budgets:write")),
):
    """Create or update a budget for a given scope (org, project, env)."""
    bid = f"{req.scope_type}:{req.scope_id}:{req.period}"
    rec = await neo4j_query_one(
        """
        MERGE (b:Budget {id: $id})
        ON CREATE SET b.scope_type=$scope_type, b.scope_id=$scope_id, b.period=$period, b.limit=toFloat($limit), b.created_at=datetime()
        ON MATCH SET b.scope_type=coalesce($scope_type, b.scope_type), b.scope_id=coalesce($scope_id, b.scope_id), b.period=coalesce($period, b.period), b.limit=toFloat($limit), b.updated_at=datetime()
        RETURN b
        """,
        {"id": bid, "scope_type": req.scope_type, "scope_id": req.scope_id, "period": req.period, "limit": req.limit},
    )
    # Link to scope
    if req.scope_type == "org":
        await neo4j_query_one("MATCH (x:Org {id: $sid}), (b:Budget {id: $bid}) MERGE (x)-[:HAS_BUDGET]->(b) RETURN 1 AS ok", {"sid": req.scope_id, "bid": bid})
    elif req.scope_type == "project":
        await neo4j_query_one("MATCH (x:Project {id: $sid}), (b:Budget {id: $bid}) MERGE (x)-[:HAS_BUDGET]->(b) RETURN 1 AS ok", {"sid": req.scope_id, "bid": bid})
    elif req.scope_type == "env":
        await neo4j_query_one("MATCH (x:Environment {id: $sid}), (b:Budget {id: $bid}) MERGE (x)-[:HAS_BUDGET]->(b) RETURN 1 AS ok", {"sid": req.scope_id, "bid": bid})
    b = rec["b"]["properties"]
    return BudgetResponse(id=b.get("id"), scope_type=b.get("scope_type"), scope_id=b.get("scope_id"), period=b.get("period"), limit=float(b.get("limit", 0.0)))


@router.get("/{scope_type}/{scope_id}/status", response_model=BudgetStatusResponse)
async def budget_status(scope_type: str, scope_id: str, _rbac: None = Depends(require_permissions_effective(["budgets:read"]))):
    """Get the current budget status and usage for a scope."""
    # Find budget (prefer monthly; fallback to rolling30d)
    rec = await neo4j_query_one("MATCH (b:Budget) WHERE b.scope_type=$st AND b.scope_id=$sid RETURN b ORDER BY CASE b.period WHEN 'monthly' THEN 0 ELSE 1 END LIMIT 1", {"st": scope_type, "sid": scope_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Budget not found")
    b = rec["b"]["properties"]
    period = b.get("period", "monthly")

    # Sum usage for last 30 days as a proxy for both monthly and rolling30d
    cutoff_query = "datetime() - duration('P30D')"
    if scope_type == "org":
        q = f"MATCH (:Org {{id: $sid}})-[:HAS_RUN]->(r:Run) WHERE r.created_at >= {cutoff_query} RETURN coalesce(sum(toFloat(r.cost)), 0.0) AS usage"
    elif scope_type == "project":
        q = f"MATCH (:Project {{id: $sid}})-[:HAS_RUN]->(r:Run) WHERE r.created_at >= {cutoff_query} RETURN coalesce(sum(toFloat(r.cost)), 0.0) AS usage"
    else:  # env
        q = f"MATCH (r:Run)-[:IN_ENV]->(:Environment {{id: $sid}}) WHERE r.created_at >= {cutoff_query} RETURN coalesce(sum(toFloat(r.cost)), 0.0) AS usage"

    usage_rec = await neo4j_query_one(q, {"sid": scope_id})
    usage = float((usage_rec.get("usage") if usage_rec else 0.0) or 0.0)
    limit_val = float(b.get("limit", 0.0))
    remaining = max(0.0, limit_val - usage)

    return BudgetStatusResponse(id=b.get("id"), usage=usage, limit=limit_val, remaining=remaining, window=period)
