"""Model router for bootstrap and status endpoints."""

from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, Depends

from ..security import require_permissions, Principal
from ..schemas import (
    BootstrapRequest,
    BootstrapResponse,
    ModelStatusResponse,
    QueryRequest,
)
from .dependencies import (
    post as backend_post,
    get as backend_get,
    neo4j_query_one,
    CONSTRAINT_QUERIES,
    ATOMIC_PERMISSIONS,
    ROLE_GRANTS,
    expand_grants,
)

router = APIRouter(prefix="/v1/model", tags=["model"])


@router.post("/bootstrap", response_model=BootstrapResponse)
async def bootstrap_model(req: BootstrapRequest, _: Principal = Depends(require_permissions(["policy:write"]))):
    """Bootstrap the graph database with constraints, roles, permissions, and environments."""
    # 1) Apply constraints/indexes via auto-commit queries
    applied = 0
    for q in CONSTRAINT_QUERIES:
        try:
            await backend_post("/v1/query", QueryRequest(query=q).dict())
            applied += 1
        except Exception:
            # Ignore individual failures; continue to next
            pass

    # 2) Ensure Org
    await backend_post(
        "/v1/query",
        QueryRequest(
            query=(
                "MERGE (o:Org {id: $org_id}) "
                "ON CREATE SET o.name=$org_name, o.created_at=datetime() "
                "ON MATCH SET o.name=coalesce($org_name, o.name)"
            ),
            parameters={"org_id": req.org_id, "org_name": req.org_name},
        ).dict(),
    )

    envs_created = 0
    if req.seed_envs:
        envs = [
            {"id": "dev", "name": "Development", "order": 1},
            {"id": "stage", "name": "Staging", "order": 2},
            {"id": "prod", "name": "Production", "order": 3},
        ]
        await backend_post(
            "/v1/query",
            QueryRequest(
                query=(
                    "UNWIND $envs AS e "
                    "MERGE (env:Environment {id: e.id}) "
                    "ON CREATE SET env.name=e.name, env.created_at=datetime() "
                    "SET env.name=coalesce(e.name, env.name) "
                    "WITH env, e "
                    "MATCH (o:Org {id: $org_id}) "
                    "MERGE (o)-[rel:HAS_ENV]->(env) "
                    "SET rel.order = e.order"
                ),
                parameters={"envs": envs, "org_id": req.org_id},
            ).dict(),
        )
        # Count env relationships present
        count_envs = await backend_post(
            "/v1/query",
            QueryRequest(
                query=(
                    "MATCH (:Org {id: $org_id})-[:HAS_ENV]->(env:Environment) "
                    "WHERE env.id IN $ids RETURN count(env) AS n"
                ),
                parameters={"org_id": req.org_id, "ids": ["dev", "stage", "prod"]},
                strategy="one",
            ).dict(),
        )
        envs_created = int((count_envs.get("result") or {}).get("n", 0))

    roles_created = 0
    perms_created = 0
    if req.seed_rbac:
        # Create Permission nodes
        await backend_post(
            "/v1/query",
            QueryRequest(
                query=(
                    "UNWIND $perms AS name "
                    "MERGE (:Permission {name: name})"
                ),
                parameters={"perms": ATOMIC_PERMISSIONS},
            ).dict(),
        )
        # Create Role nodes
        role_names = sorted(list(ROLE_GRANTS.keys()))
        await backend_post(
            "/v1/query",
            QueryRequest(
                query=(
                    "UNWIND $roles AS name "
                    "MERGE (:Role {name: name})"
                ),
                parameters={"roles": role_names},
            ).dict(),
        )
        # Grant permissions per role (expanded, no wildcards)
        expanded = expand_grants(ROLE_GRANTS, ATOMIC_PERMISSIONS)
        grants: List[Dict[str, str]] = []
        for role, perms in expanded.items():
            for p in perms:
                grants.append({"role": role, "perm": p})
        if grants:
            await backend_post(
                "/v1/query",
                QueryRequest(
                    query=(
                        "UNWIND $grants AS g "
                        "MATCH (r:Role {name: g.role}) "
                        "MATCH (p:Permission {name: g.perm}) "
                        "MERGE (r)-[:GRANTS]->(p)"
                    ),
                    parameters={"grants": grants},
                ).dict(),
            )
        # Return counts present
        out_roles = await backend_post(
            "/v1/query",
            QueryRequest(
                query=("MATCH (r:Role) WHERE r.name IN $roles RETURN count(r) AS n"),
                parameters={"roles": role_names},
                strategy="one",
            ).dict(),
        )
        roles_created = int((out_roles.get("result") or {}).get("n", 0))
        out_perms = await backend_post(
            "/v1/query",
            QueryRequest(
                query=("MATCH (p:Permission) WHERE p.name IN $perms RETURN count(p) AS n"),
                parameters={"perms": ATOMIC_PERMISSIONS},
                strategy="one",
            ).dict(),
        )
        perms_created = int((out_perms.get("result") or {}).get("n", 0))

    # 3) Ensure a basic global policy with default approvals if not present
    await neo4j_query_one(
        """
        MERGE (p:Policy {id: 'global'})
        ON CREATE SET p.mode = 'observe', p.approvals = {
            add_node_schema: 1,
            add_relationship_schema: 1,
            add_property: 1
        }, p.created_at = datetime()
        RETURN p
        """,
        {},
    )

    return BootstrapResponse(
        constraints_applied=applied,
        permissions_created=perms_created,
        roles_created=roles_created,
        environments_created=envs_created,
        org_id=req.org_id,
    )


@router.get("/status", response_model=ModelStatusResponse)
async def model_status(_: Principal = Depends(require_permissions(["policy:read"]))):
    """Get the current status of the graph model including constraints, indexes, roles, etc."""
    cons = await backend_get("/v1/constraints")
    idxs = await backend_get("/v1/indexes")
    roles = await backend_post(
        "/v1/query",
        QueryRequest(query="MATCH (r:Role) RETURN count(r) AS n", strategy="one").dict(),
    )
    perms = await backend_post(
        "/v1/query",
        QueryRequest(query="MATCH (p:Permission) RETURN count(p) AS n", strategy="one").dict(),
    )
    envs = await backend_post(
        "/v1/query",
        QueryRequest(query="MATCH (:Org)-[:HAS_ENV]->(:Environment) RETURN count(*) AS n", strategy="one").dict(),
    )
    return ModelStatusResponse(
        constraints=len(cons.get("constraints", [])),
        indexes=len(idxs.get("indexes", [])),
        roles=int((roles.get("result") or {}).get("n", 0)),
        permissions=int((perms.get("result") or {}).get("n", 0)),
        environments=int((envs.get("result") or {}).get("n", 0)),
    )
