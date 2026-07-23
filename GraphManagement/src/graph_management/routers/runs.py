"""Runs router for tracking and managing execution runs."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, Principal
from ..schemas import (
    RunCreateRequest,
    RunEventRequest,
    RunFinishRequest,
    RunResponse,
)
from .dependencies import (
    neo4j_query_one,
)

router = APIRouter(prefix="/v1/runs", tags=["runs"])


@router.post("", response_model=RunResponse)
async def create_run(req: RunCreateRequest, _: Principal = Depends(require_permissions(["runs:write"]))):
    """Create a new run and attach it to env/org/project/actor."""
    # Create run and attach to env/org/project/actor
    await neo4j_query_one(
        """
        MERGE (r:Run {id: $id})
        ON CREATE SET r.env_id=$env_id, r.status='running', r.created_at=datetime(), r.updated_at=datetime(), r.inputs=$inputs
        ON MATCH SET r.env_id=coalesce($env_id, r.env_id), r.updated_at=datetime(), r.inputs=coalesce($inputs, r.inputs)
        WITH r
        MATCH (e:Environment {id: $env_id})
        MERGE (r)-[:IN_ENV]->(e)
        """,
        {"id": req.id, "env_id": req.env_id, "inputs": req.inputs},
    )
    # Attach actor
    if req.actor_type == "user":
        await neo4j_query_one(
            "MATCH (r:Run {id: $id}) MERGE (u:User {sub: $pid}) ON CREATE SET u.created_at=datetime() MERGE (r)-[:BY]->(u) RETURN 1 AS ok",
            {"id": req.id, "pid": req.actor_id},
        )
    else:
        await neo4j_query_one(
            "MATCH (r:Run {id: $id}) MERGE (a:Agent {id: $pid}) ON CREATE SET a.created_at=datetime() MERGE (r)-[:BY]->(a) RETURN 1 AS ok",
            {"id": req.id, "pid": req.actor_id},
        )
    # Org/project linkage if provided
    if req.org_id:
        await neo4j_query_one(
            "MATCH (o:Org {id: $org_id}), (r:Run {id: $id}) MERGE (o)-[:HAS_RUN]->(r) RETURN 1 AS ok",
            {"org_id": req.org_id, "id": req.id},
        )
    if req.project_id:
        await neo4j_query_one(
            "MATCH (p:Project {id: $pid}), (r:Run {id: $id}) MERGE (p)-[:HAS_RUN]->(r) RETURN 1 AS ok",
            {"pid": req.project_id, "id": req.id},
        )
    # Return
    rec = await neo4j_query_one("MATCH (r:Run {id: $id}) RETURN r", {"id": req.id})
    r = rec["r"]["properties"]
    return RunResponse(id=r.get("id"), env_id=r.get("env_id"), status=r.get("status"), actor_type=req.actor_type, actor_id=req.actor_id, created_at=r.get("created_at"), updated_at=r.get("updated_at"))


@router.post("/{run_id}/event", response_model=RunResponse)
async def add_run_event(run_id: str, req: RunEventRequest, _: Principal = Depends(require_permissions(["runs:write"]))):
    """Add an event to a running execution."""
    await neo4j_query_one(
        """
        MATCH (r:Run {id: $id})
        CREATE (ev:Event {id: toString(randomUUID()), type: $type, payload: $payload, created_at: datetime()})
        MERGE (r)-[:HAS_EVENT]->(ev)
        SET r.updated_at = datetime()
        RETURN r
        """,
        {"id": run_id, "type": req.type, "payload": req.payload},
    )
    rec = await neo4j_query_one("MATCH (r:Run {id: $id}) RETURN r", {"id": run_id})
    r = rec["r"]["properties"]
    return RunResponse(id=r.get("id"), env_id=r.get("env_id"), status=r.get("status"), actor_type="", actor_id="", created_at=r.get("created_at"), updated_at=r.get("updated_at"))


@router.post("/{run_id}/finish", response_model=RunResponse)
async def finish_run(run_id: str, req: RunFinishRequest, _: Principal = Depends(require_permissions(["runs:write"]))):
    """Mark a run as finished with final status and outputs."""
    cost = None
    try:
        if isinstance(req.outputs, dict) and "cost" in req.outputs:
            val = req.outputs.get("cost")
            cost = float(val) if val is not None else None
    except Exception:
        cost = None
    rec = await neo4j_query_one(
        "MATCH (r:Run {id: $id}) SET r.status=$status, r.updated_at=datetime(), r.outputs=$outputs, r.cost=coalesce(toFloat($cost), r.cost) RETURN r",
        {"id": run_id, "status": req.status, "outputs": req.outputs, "cost": cost},
    )
    r = rec["r"]["properties"]
    return RunResponse(id=r.get("id"), env_id=r.get("env_id"), status=r.get("status"), actor_type="", actor_id="", created_at=r.get("created_at"), updated_at=r.get("updated_at"))


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(run_id: str, _: Principal = Depends(require_permissions(["runs:read"]))):
    """Get a specific run by ID."""
    rec = await neo4j_query_one("MATCH (r:Run {id: $id}) RETURN r", {"id": run_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Run not found")
    r = rec["r"]["properties"]
    return RunResponse(id=r.get("id"), env_id=r.get("env_id"), status=r.get("status"), actor_type="", actor_id="", created_at=r.get("created_at"), updated_at=r.get("updated_at"))
