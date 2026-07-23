"""Policies router for ABAC policy management and evaluation endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    PolicyResponse,
    PolicyUpsertRequest,
    PolicyDetailResponse,
    ABACEvalRequest,
    ABACEvalResponse,
)
from .dependencies import (
    neo4j_query_one,
    policy_id,
    action_matches,
    collect_policies_for_eval,
)

router = APIRouter(prefix="/v1", tags=["policies"])


# --- New policy endpoints (structured) ---

@router.post("/policies/upsert", response_model=PolicyDetailResponse)
async def upsert_policy(req: PolicyUpsertRequest, _: Principal = Depends(require_permissions(["policy:write"]))):
    """Create or update a policy for a given scope (global, org, project, env)."""
    pid = policy_id(req.scope_type, req.scope_id)
    rec = await neo4j_query_one(
        """
        MERGE (p:Policy {id: $id})
        ON CREATE SET p.created_at = datetime()
        SET p.mode = coalesce($mode, p.mode, 'observe'), p.approvals = coalesce($approvals, p.approvals, {}), p.rules = coalesce($rules, p.rules, [])
        RETURN p
        """,
        {"id": pid, "mode": req.mode, "approvals": req.approvals or {}, "rules": req.rules or []},
    )
    # Link policy to scope if not global
    if req.scope_type != "global" and req.scope_id:
        if req.scope_type == "org":
            await neo4j_query_one(
                "MATCH (x:Org {id: $sid}), (p:Policy {id: $pid}) MERGE (x)-[:HAS_POLICY]->(p) RETURN 1 AS ok",
                {"sid": req.scope_id, "pid": pid},
            )
        elif req.scope_type == "env":
            await neo4j_query_one(
                "MATCH (x:Environment {id: $sid}), (p:Policy {id: $pid}) MERGE (x)-[:HAS_POLICY]->(p) RETURN 1 AS ok",
                {"sid": req.scope_id, "pid": pid},
            )
        elif req.scope_type == "project":
            await neo4j_query_one(
                "MATCH (x:Project {id: $sid}), (p:Policy {id: $pid}) MERGE (x)-[:HAS_POLICY]->(p) RETURN 1 AS ok",
                {"sid": req.scope_id, "pid": pid},
            )
    p = rec["p"]["properties"]
    return PolicyDetailResponse(scope_type=req.scope_type, scope_id=req.scope_id, mode=p.get("mode", "observe"), approvals=p.get("approvals") or {}, rules=p.get("rules") or [])


@router.get("/policies/global", response_model=PolicyDetailResponse)
async def get_global_policy(_: Principal = Depends(require_permissions(["policy:read"]))):
    """Get the global policy configuration."""
    rec = await neo4j_query_one("MATCH (p:Policy {id: 'global'}) RETURN p", {})
    if not rec:
        return PolicyDetailResponse(scope_type="global", scope_id=None, mode="observe", approvals={}, rules=[])
    p = rec["p"]["properties"]
    return PolicyDetailResponse(scope_type="global", scope_id=None, mode=p.get("mode", "observe"), approvals=p.get("approvals") or {}, rules=p.get("rules") or [])


@router.get("/policies/{scope_type}/{scope_id}", response_model=PolicyDetailResponse)
async def get_scoped_policy(scope_type: str, scope_id: str, _: Principal = Depends(require_permissions(["policy:read"]))):
    """Get a policy for a specific scope (org, project, or env)."""
    pid = policy_id(scope_type, scope_id)
    rec = await neo4j_query_one("MATCH (p:Policy {id: $id}) RETURN p", {"id": pid})
    if not rec:
        raise HTTPException(status_code=404, detail="Policy not found")
    p = rec["p"]["properties"]
    return PolicyDetailResponse(scope_type=scope_type, scope_id=scope_id, mode=p.get("mode", "observe"), approvals=p.get("approvals") or {}, rules=p.get("rules") or [])


@router.post("/policies/eval", response_model=ABACEvalResponse)
async def eval_policy(req: ABACEvalRequest, principal: Principal = Depends(get_principal)):
    """Evaluate ABAC policies for a given action and context."""
    policies = await collect_policies_for_eval(req.env_id, req.project_id, principal.org)
    roles = set(principal.roles or [])

    # Deny first
    for pol in policies:
        for rule in (pol.get("rules") or []):
            if rule.get("effect") != "deny":
                continue
            if not action_matches(rule.get("actions") or [], req.action):
                continue
            if rule.get("roles") and not roles.intersection(set(rule.get("roles"))):
                continue
            if rule.get("env_ids") and req.env_id not in set(rule.get("env_ids")):
                continue
            if rule.get("projects") and req.project_id not in set(rule.get("projects")):
                continue
            return ABACEvalResponse(decision="deny", matched=rule, scope_applied=pol.get("id"))

    # Allow second
    for pol in policies:
        for rule in (pol.get("rules") or []):
            if rule.get("effect") != "allow":
                continue
            if not action_matches(rule.get("actions") or [], req.action):
                continue
            if rule.get("roles") and not roles.intersection(set(rule.get("roles"))):
                continue
            if rule.get("env_ids") and req.env_id not in set(rule.get("env_ids")):
                continue
            if rule.get("projects") and req.project_id not in set(rule.get("projects")):
                continue
            return ABACEvalResponse(decision="allow", matched=rule, scope_applied=pol.get("id"))

    # Default: allow in observe/warn, deny in enforce
    # Find highest-precedence mode
    mode = None
    for pol in policies:
        if pol.get("mode"):
            mode = pol.get("mode")
            break
    if mode == "enforce":
        return ABACEvalResponse(decision="deny", matched=None, scope_applied=None)
    return ABACEvalResponse(decision="allow", matched=None, scope_applied=None)


# --- Legacy simple policy endpoints ---

@router.get("/policy", response_model=PolicyResponse)
async def get_policy():
    """Get the global policy mode (legacy endpoint)."""
    rec = await neo4j_query_one("MATCH (p:Policy {id: 'global'}) RETURN p", {})
    if not rec:
        return PolicyResponse(mode="observe")
    mode = rec["p"]["properties"].get("mode", "observe")
    return PolicyResponse(mode=mode)


@router.post("/policy", response_model=PolicyResponse)
async def set_policy(policy: PolicyResponse):
    """Set the global policy mode (legacy endpoint)."""
    rec = await neo4j_query_one(
        "MERGE (p:Policy {id: 'global'}) SET p.mode=$mode RETURN p",
        {"mode": policy.mode},
    )
    mode = rec["p"]["properties"].get("mode", "observe")
    return PolicyResponse(mode=mode)
