"""Agents router for agent registration and management."""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException

from ..security import get_principal, Principal
from ..schemas import (
    AgentRegisterRequest,
    AgentResponse,
    AgentListResponse,
)
from .dependencies import (
    neo4j_query_one,
    neo4j_query_all,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(prefix="/v1", tags=["agents"])


@router.post("/agents", response_model=AgentResponse)
async def register_agent(
    req: AgentRegisterRequest,
    principal: Principal = Depends(get_principal),
    _rbac: None = Depends(require_permissions_effective(["agents:write"])),
    _abac: None = Depends(enforce_abac("agents:write")),
):
    """Register or update an agent."""
    org_id = req.org_id or principal.org
    rec = await neo4j_query_one(
        """
        MERGE (a:Agent {id: $id})
        ON CREATE SET a.name=$name, a.capabilities=$capabilities, a.created_at=datetime()
        ON MATCH SET a.name=coalesce($name, a.name), a.capabilities=coalesce($capabilities, a.capabilities), a.updated_at=datetime()
        WITH a, $org_id AS oid, $project_id AS pid, $team_id AS tid
        CALL {
            WITH a, oid
            WITH a, oid WHERE oid IS NOT NULL
            MATCH (o:Org {id: oid})
            MERGE (o)-[:HAS_AGENT]->(a)
        }
        CALL {
            WITH a, pid
            WITH a, pid WHERE pid IS NOT NULL
            MATCH (p:Project {id: pid})
            MERGE (a)-[:ASSIGNED_TO]->(p)
        }
        CALL {
            WITH a, tid
            WITH a, tid WHERE tid IS NOT NULL
            MATCH (t:Team {id: tid})
            MERGE (a)-[:ASSIGNED_TO]->(t)
        }
        RETURN a
        """,
        {
            "id": req.id,
            "name": req.name,
            "capabilities": req.capabilities,
            "project_id": req.project_id,
            "team_id": req.team_id,
            "org_id": org_id,
        },
    )
    a = rec["a"]["properties"]
    return AgentResponse(id=a.get("id"), name=a.get("name"), capabilities=a.get("capabilities") or [])


@router.get("/orgs/{org_id}/agents", response_model=AgentListResponse)
async def list_agents(org_id: str, _rbac: None = Depends(require_permissions_effective(["agents:read"], org_param="org_id"))):
    """List all agents in an organization."""
    rows = await neo4j_query_all("MATCH (:Org {id: $org_id})-[:HAS_AGENT]->(a:Agent) RETURN a", {"org_id": org_id})
    agents: List[AgentResponse] = []
    for r in rows:
        a = r["a"]["properties"]
        agents.append(AgentResponse(id=a.get("id"), name=a.get("name"), capabilities=a.get("capabilities") or []))
    return AgentListResponse(agents=agents)


@router.get("/agents/{agent_id}", response_model=AgentResponse)
async def get_agent(agent_id: str, _rbac: None = Depends(require_permissions_effective(["agents:read"]))):
    """Get a specific agent by ID."""
    rec = await neo4j_query_one("MATCH (a:Agent {id: $id}) RETURN a", {"id": agent_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Agent not found")
    a = rec["a"]["properties"]
    return AgentResponse(id=a.get("id"), name=a.get("name"), capabilities=a.get("capabilities") or [])
