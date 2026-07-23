"""Roles router for role assignment and management."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends

from ..schemas import (
    RoleAssignmentRequest,
    RoleAssignmentResponse,
)
from .dependencies import (
    neo4j_query_one,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(prefix="/v1/roles", tags=["roles"])


@router.post("/assign", response_model=RoleAssignmentResponse)
async def assign_role(
    req: RoleAssignmentRequest,
    _rbac: None = Depends(require_permissions_effective(["orgs:write"])),
    _abac: None = Depends(enforce_abac("orgs:write")),
):
    """Assign a role to a user or agent within a given scope."""
    scope: Optional[str] = None
    if req.project_id:
        scope = f"project:{req.project_id}"
    elif req.org_id:
        scope = f"org:{req.org_id}"

    if req.principal_type == "user":
        await neo4j_query_one(
            """
            MERGE (u:User {sub: $pid}) ON CREATE SET u.created_at=datetime()
            MERGE (r:Role {name: $role})
            MERGE (u)-[hr:HAS_ROLE]->(r)
            SET hr.scope = $scope
            RETURN 1 AS ok
            """,
            {"pid": req.principal_id, "role": req.role, "scope": scope},
        )
    else:
        await neo4j_query_one(
            """
            MERGE (a:Agent {id: $pid}) ON CREATE SET a.created_at=datetime()
            MERGE (r:Role {name: $role})
            MERGE (a)-[hr:HAS_ROLE]->(r)
            SET hr.scope = $scope
            RETURN 1 AS ok
            """,
            {"pid": req.principal_id, "role": req.role, "scope": scope},
        )

    return RoleAssignmentResponse(principal_type=req.principal_type, principal_id=req.principal_id, role=req.role, scope=scope)
