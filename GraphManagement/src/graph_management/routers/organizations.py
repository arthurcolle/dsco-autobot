"""Organizations router for teams, projects, and organizational context."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    TeamCreateRequest,
    TeamResponse,
    TeamListResponse,
    ProjectCreateRequest,
    ProjectResponse,
    ProjectListResponse,
    AuthEffectiveResponse,
)
from .dependencies import (
    neo4j_query_one,
    neo4j_query_all,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(prefix="/v1", tags=["organizations"])


@router.get("/auth/effective", response_model=AuthEffectiveResponse)
async def auth_effective(
    sub: Optional[str] = None,
    org_id: Optional[str] = None,
    project_id: Optional[str] = None,
    principal: Principal = Depends(get_principal)
):
    """Get effective roles and permissions for a user in a given context."""
    # Allow specifying a different subject if caller has orgs:read; else use self
    if sub is None:
        sub = principal.sub
    # Build scope list for filtering HAS_ROLE.scope
    scopes: List[str] = []
    if org_id:
        scopes.append(f"org:{org_id}")
    if project_id:
        scopes.append(f"project:{project_id}")
    # Collect roles and permissions
    query = (
        "MATCH (u:User {sub: $sub})-[hr:HAS_ROLE]->(r:Role) "
        "WHERE hr.scope IS NULL OR size($scopes) = 0 OR hr.scope IN $scopes "
        "OPTIONAL MATCH (r)-[:GRANTS]->(p:Permission) "
        "RETURN collect(distinct r.name) AS roles, collect(distinct p.name) AS perms"
    )
    rec = await neo4j_query_one(query, {"sub": sub, "scopes": scopes})
    roles = (rec.get("roles") if rec else []) or []
    perms = (rec.get("perms") if rec else []) or []
    # Fallback: derive from env roles if graph has none
    if not roles:
        roles = principal.roles or []
    return AuthEffectiveResponse(roles=roles, permissions=perms)


# --- Teams ---

@router.post("/orgs/{org_id}/teams", response_model=TeamResponse)
async def create_team(
    org_id: str,
    req: TeamCreateRequest,
    _rbac: None = Depends(require_permissions_effective(["teams:write"], org_param="org_id")),
    _abac: None = Depends(enforce_abac("teams:write", org_param="org_id")),
):
    """Create or update a team within an organization."""
    rec = await neo4j_query_one(
        """
        MERGE (t:Team {id: $id})
        ON CREATE SET t.name=$name, t.description=$description, t.created_at=datetime()
        ON MATCH SET t.name=coalesce($name, t.name), t.description=coalesce($description, t.description), t.updated_at=datetime()
        WITH t
        MATCH (o:Org {id: $org_id})
        MERGE (o)-[:HAS_TEAM]->(t)
        RETURN t
        """,
        {"id": req.id, "name": req.name, "description": req.description, "org_id": org_id},
    )
    t = rec["t"]["properties"]
    return TeamResponse(id=t.get("id"), name=t.get("name"), description=t.get("description"))


@router.get("/orgs/{org_id}/teams", response_model=TeamListResponse)
async def list_teams(org_id: str, _rbac: None = Depends(require_permissions_effective(["teams:read"], org_param="org_id"))):
    """List all teams in an organization."""
    rows = await neo4j_query_all(
        "MATCH (:Org {id: $org_id})-[:HAS_TEAM]->(t:Team) RETURN t",
        {"org_id": org_id},
    )
    teams: List[TeamResponse] = []
    for r in rows:
        t = r["t"]["properties"]
        teams.append(TeamResponse(id=t.get("id"), name=t.get("name"), description=t.get("description")))
    return TeamListResponse(teams=teams)


@router.get("/teams/{team_id}", response_model=TeamResponse)
async def get_team(team_id: str, _: Principal = Depends(require_permissions(["teams:read"]))):
    """Get a specific team by ID."""
    rec = await neo4j_query_one("MATCH (t:Team {id: $id}) RETURN t", {"id": team_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Team not found")
    t = rec["t"]["properties"]
    return TeamResponse(id=t.get("id"), name=t.get("name"), description=t.get("description"))


# --- Projects ---

@router.post("/orgs/{org_id}/projects", response_model=ProjectResponse)
async def create_project(
    org_id: str,
    req: ProjectCreateRequest,
    _rbac: None = Depends(require_permissions_effective(["projects:write"], org_param="org_id")),
    _abac: None = Depends(enforce_abac("projects:write", org_param="org_id")),
):
    """Create or update a project within an organization."""
    rec = await neo4j_query_one(
        """
        MERGE (p:Project {id: $id})
        ON CREATE SET p.name=$name, p.description=$description, p.created_at=datetime()
        ON MATCH SET p.name=coalesce($name, p.name), p.description=coalesce($description, p.description), p.updated_at=datetime()
        WITH p
        MATCH (o:Org {id: $org_id})
        MERGE (o)-[:HAS_PROJECT]->(p)
        WITH p
        UNWIND coalesce($env_ids, []) AS env_id
        MATCH (e:Environment {id: env_id})
        MERGE (p)-[:DEPLOYS_TO]->(e)
        RETURN p
        """,
        {"id": req.id, "name": req.name, "description": req.description, "env_ids": req.env_ids or [], "org_id": org_id},
    )
    p = rec["p"]["properties"]
    return ProjectResponse(id=p.get("id"), name=p.get("name"), description=p.get("description"))


@router.get("/orgs/{org_id}/projects", response_model=ProjectListResponse)
async def list_projects(org_id: str, _rbac: None = Depends(require_permissions_effective(["projects:read"], org_param="org_id"))):
    """List all projects in an organization."""
    rows = await neo4j_query_all(
        "MATCH (:Org {id: $org_id})-[:HAS_PROJECT]->(p:Project) RETURN p",
        {"org_id": org_id},
    )
    projects: List[ProjectResponse] = []
    for r in rows:
        p = r["p"]["properties"]
        projects.append(ProjectResponse(id=p.get("id"), name=p.get("name"), description=p.get("description")))
    return ProjectListResponse(projects=projects)


@router.get("/projects/{project_id}", response_model=ProjectResponse)
async def get_project(project_id: str, _rbac: None = Depends(require_permissions_effective(["projects:read"]))):
    """Get a specific project by ID."""
    rec = await neo4j_query_one("MATCH (p:Project {id: $id}) RETURN p", {"id": project_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Project not found")
    p = rec["p"]["properties"]
    return ProjectResponse(id=p.get("id"), name=p.get("name"), description=p.get("description"))
