"""Shared dependencies, helpers, and utilities for Graph Management API routers."""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional
from time import monotonic

from fastapi import HTTPException, Depends, Request
import httpx

from ..security import require_permissions, get_principal, Principal, has_permission
from ..schemas import ToolResponse

# --- Logging configuration ---
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, _LOG_LEVEL, logging.INFO))
logger = logging.getLogger("graph_management_api")
DEBUG_ERRORS = os.environ.get("DEBUG_ERRORS", "0") in {"1", "true", "True", "yes", "on"}
MAX_TOOL_NAME_LENGTH = 64

# --- Caching ---
_EFFECTIVE_CACHE: Dict[str, Dict[str, Any]] = {}
_EFFECTIVE_TTL_SEC = 30.0

# --- Model Constraints ---
CONSTRAINT_QUERIES: List[str] = [
    # Core org/user/rbac
    "CREATE CONSTRAINT org_id_unique IF NOT EXISTS FOR (o:Org) REQUIRE o.id IS UNIQUE",
    "CREATE CONSTRAINT user_sub_unique IF NOT EXISTS FOR (u:User) REQUIRE u.sub IS UNIQUE",
    "CREATE CONSTRAINT role_name_unique IF NOT EXISTS FOR (r:Role) REQUIRE r.name IS UNIQUE",
    "CREATE CONSTRAINT perm_name_unique IF NOT EXISTS FOR (p:Permission) REQUIRE p.name IS UNIQUE",
    "CREATE CONSTRAINT env_id_unique IF NOT EXISTS FOR (e:Environment) REQUIRE e.id IS UNIQUE",
    # Registry / policy
    "CREATE CONSTRAINT rns_name_unique IF NOT EXISTS FOR (ns:RegisteredNodeSchema) REQUIRE ns.name IS UNIQUE",
    "CREATE CONSTRAINT rrs_type_unique IF NOT EXISTS FOR (rs:RegisteredRelationshipSchema) REQUIRE rs.type IS UNIQUE",
    "CREATE CONSTRAINT proposal_id_unique IF NOT EXISTS FOR (sp:SchemaProposal) REQUIRE sp.id IS UNIQUE",
    "CREATE CONSTRAINT policy_id_unique IF NOT EXISTS FOR (pol:Policy) REQUIRE pol.id IS UNIQUE",
    # Helpful index for version lookups
    "CREATE INDEX schema_version_version_idx IF NOT EXISTS FOR (v:SchemaVersion) ON (v.version)",
    # Future model core
    "CREATE CONSTRAINT team_id_unique IF NOT EXISTS FOR (t:Team) REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT project_id_unique IF NOT EXISTS FOR (p:Project) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT tool_id_unique IF NOT EXISTS FOR (t:Tool) REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT dataset_id_unique IF NOT EXISTS FOR (d:Dataset) REQUIRE d.id IS UNIQUE",
    "CREATE CONSTRAINT secret_id_unique IF NOT EXISTS FOR (s:Secret) REQUIRE s.id IS UNIQUE",
    "CREATE CONSTRAINT agent_id_unique IF NOT EXISTS FOR (a:Agent) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT run_id_unique IF NOT EXISTS FOR (r:Run) REQUIRE r.id IS UNIQUE",
    "CREATE CONSTRAINT datacontract_id_unique IF NOT EXISTS FOR (dc:DataContract) REQUIRE dc.id IS UNIQUE",
    "CREATE CONSTRAINT tag_name_unique IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE",
    "CREATE CONSTRAINT budget_id_unique IF NOT EXISTS FOR (b:Budget) REQUIRE b.id IS UNIQUE",
    "CREATE CONSTRAINT incident_id_unique IF NOT EXISTS FOR (i:Incident) REQUIRE i.id IS UNIQUE",
    "CREATE CONSTRAINT risk_id_unique IF NOT EXISTS FOR (r:Risk) REQUIRE r.id IS UNIQUE",
    "CREATE CONSTRAINT event_id_unique IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE",
]

ATOMIC_PERMISSIONS: List[str] = [
    "schemas:read",
    "schemas:write",
    "proposals:create",
    "proposals:read",
    "proposals:review",
    "proposals:apply",
    "policy:read",
    "policy:write",
    "query:read",
    "query:write",
    # Identity & org management
    "orgs:read",
    "orgs:write",
    "teams:read",
    "teams:write",
    "projects:read",
    "projects:write",
    # Resources
    "tools:read",
    "tools:write",
    "datasets:read",
    "datasets:write",
    "secrets:read",
    "secrets:write",
    "agents:read",
    "agents:write",
    # Runtime
    "runs:read",
    "runs:write",
    # Budgets & events
    "budgets:read",
    "budgets:write",
    "events:read",
]

ROLE_GRANTS: Dict[str, List[str]] = {
    "org_admin": ["*"],
    "schema_admin": ["schemas:*", "proposals:*", "policy:*", "query:*"],
    "schema_approver": ["schemas:read", "proposals:review", "proposals:apply"],
    "schema_reviewer": ["schemas:read", "proposals:review"],
    "developer": ["schemas:read", "proposals:create", "query:read", "query:write"],
    "readonly": ["schemas:read", "query:read"],
    "service": ["schemas:read", "query:*"],
}


# --- Helper Functions ---

def policy_id(scope_type: str, scope_id: Optional[str]) -> str:
    """Generate a policy ID from scope type and ID."""
    if scope_type == "global" or not scope_id:
        return "global"
    return f"{scope_type}:{scope_id}"


def backend_url() -> str:
    """Get the backend Neo4j service URL from environment."""
    url = os.getenv("BACKEND_URL")
    if not url:
        raise RuntimeError(
            "BACKEND_URL is not set. Point it at the Neo4j service base URL (e.g., https://<app>.modal.run)."
        )
    return url.rstrip("/")


def ws_backend_url() -> str:
    """Get WebSocket backend URL."""
    url = backend_url()
    return url.replace("https://", "wss://").replace("http://", "ws://")


def cache_key(sub: str, org_id: Optional[str], project_id: Optional[str]) -> str:
    """Generate cache key for effective permissions."""
    return f"{sub}|{org_id or ''}|{project_id or ''}"


def perm_match_needed(needed: str, have: set[str]) -> bool:
    """Check if a needed permission matches any in the have set."""
    if "*" in have:
        return True
    if needed in have:
        return True
    cat = needed.split(":", 1)[0] + ":*"
    return cat in have


def normalize_tool_name(raw_name: Optional[Any], tool_id: Optional[str] = None) -> str:
    """
    Ensure tool names abide by the MCP 64-character limit.
    Existing records may predate the restriction, so truncate as needed.
    """
    if raw_name is None:
        return ""
    name = str(raw_name)
    if len(name) <= MAX_TOOL_NAME_LENGTH:
        return name
    logger.warning(
        "Tool %s name length %d exceeds %d; truncating for response.",
        tool_id or "<unknown>",
        len(name),
        MAX_TOOL_NAME_LENGTH,
    )
    return name[:MAX_TOOL_NAME_LENGTH]


def tool_response_from_props(props: Dict[str, Any]) -> ToolResponse:
    """Create a ToolResponse from Neo4j properties."""
    return ToolResponse(
        id=props.get("id"),
        name=normalize_tool_name(props.get("name"), props.get("id")),
        kind=props.get("kind"),
        provider=props.get("provider"),
        metadata=props.get("metadata") or {},
    )


def sanitize_label(value: str) -> str:
    """Allow only simple Neo4j label/type identifiers (alnum and underscore)."""
    import re

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise HTTPException(status_code=400, detail=f"Invalid label/type: {value!r}")
    return value


def required_props(spec: Dict[str, Any]) -> list[str]:
    """Extract required property names from a schema spec."""
    return [p.get("name") for p in (spec.get("properties") or []) if p.get("required")]


def unique_keys(spec: Dict[str, Any]) -> list[str]:
    """Extract unique key property names from a schema spec."""
    return [p.get("name") for p in (spec.get("properties") or []) if p.get("unique")]


def is_write_cypher(q: str) -> bool:
    """Heuristically detect write operations in Cypher."""
    upper = q.upper()
    write_keywords = [
        "CREATE ",
        "MERGE ",
        "DELETE",
        "DETACH DELETE",
        "SET ",
        "REMOVE ",
        "DROP ",
        "CREATE INDEX",
        "CREATE CONSTRAINT",
    ]
    if any(k in upper for k in write_keywords):
        return True
    # SHOW/RETURN/CALL db.schema/db.labels/etc. treated as read
    read_prefixes = ["RETURN", "MATCH", "CALL", "SHOW", "WITH"]
    if any(upper.startswith(p) for p in read_prefixes):
        return False
    # Default to write for safety
    return True


def expand_grants(role_grants: Dict[str, List[str]], atomic_perms: List[str]) -> Dict[str, List[str]]:
    """Expand role grant patterns to full permission lists."""
    cats = {}
    for p in atomic_perms:
        cat = p.split(":", 1)[0]
        cats.setdefault(cat, []).append(p)

    expanded: Dict[str, List[str]] = {}
    for role, patterns in role_grants.items():
        perms: List[str] = []
        for pat in patterns:
            if pat == "*":
                perms = list(atomic_perms)
                break
            if pat.endswith(":*"):
                cat = pat.split(":", 1)[0]
                perms.extend(cats.get(cat, []))
            else:
                if pat in atomic_perms:
                    perms.append(pat)
        # de-dup
        expanded[role] = sorted(set(perms))
    return expanded


def action_matches(patterns: List[str], action: str) -> bool:
    """Check if an action matches any pattern."""
    for pat in patterns:
        if pat == "*":
            return True
        if pat == action:
            return True
        if pat.endswith(":*") and action.startswith(pat[:-1]):
            return True
    return False


def approval_threshold_for(change_type: str, org_id: Optional[str]) -> int:
    """Get approval threshold for a change type (synchronous)."""
    # In real usage, might query DB; for now return default
    return 1


# --- Async Helpers ---

async def post(path: str, json: Dict[str, Any]) -> Dict[str, Any]:
    """POST to backend Neo4j service."""
    url = f"{backend_url()}{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=json)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def get(path: str) -> Dict[str, Any]:
    """GET from backend Neo4j service."""
    url = f"{backend_url()}{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def neo4j_query_one(query: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Execute a Neo4j query and return first result or None."""
    resp = await post("/v1/query", {"query": query, "parameters": params})
    rows = resp.get("records", [])
    return rows[0] if rows else None


async def neo4j_query_all(query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute a Neo4j query and return all results."""
    resp = await post("/v1/query", {"query": query, "parameters": params})
    return resp.get("records", [])


async def neo4j_tx(statements: List[Dict[str, Any]]) -> List[Any]:
    """Execute a Neo4j transaction with multiple statements."""
    resp = await post("/v1/tx", {"statements": statements})
    return resp.get("results", [])


async def latest_node_spec(name: str) -> Dict[str, Any]:
    """Get the latest schema spec for a node type."""
    rec = await neo4j_query_one(
        """
        MATCH (:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion)
        RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1
        """,
        {"name": name},
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Node schema not found")
    vprops = rec["v"]["properties"]
    return vprops.get("spec", {})


async def latest_rel_spec(rtype: str) -> Dict[str, Any]:
    """Get the latest schema spec for a relationship type."""
    rec = await neo4j_query_one(
        """
        MATCH (:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion)
        RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1
        """,
        {"type": rtype},
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Relationship schema not found")
    vprops = rec["v"]["properties"]
    return vprops.get("spec", {})


async def consume_capability(
    principal: Principal,
    *,
    kind: str,
    target_label: str | None = None,
    relationship_type: str | None = None,
) -> None:
    """Verify the principal has a valid capability and consume one unit from its daily budget."""
    params: Dict[str, Any] = {
        "sub": principal.sub,
        "kind": kind,
        "label": target_label,
        "rtype": relationship_type,
    }
    rec = await neo4j_query_one(
        """
        MATCH (c:CapabilityToken {sub: $sub, kind: $kind})
        WHERE ($label IS NULL OR c.target_label = $label)
          AND ($rtype IS NULL OR c.relationship_type = $rtype)
          AND (c.expires_at IS NULL OR c.expires_at > datetime())
        WITH c, date() AS today
        SET c.used_today = CASE WHEN c.day_key <> toString(today) THEN 0 ELSE coalesce(c.used_today, 0) END,
            c.day_key   = toString(today)
        WITH c
        WHERE coalesce(c.budget_per_day, 0) <= 0 OR coalesce(c.used_today, 0) < c.budget_per_day
        SET c.used_today = CASE WHEN coalesce(c.budget_per_day, 0) <= 0 THEN c.used_today ELSE coalesce(c.used_today, 0) + 1 END,
            c.updated_at = datetime()
        RETURN c
        """,
        params,
    )
    if not rec:
        raise HTTPException(status_code=403, detail="Missing capability or budget exhausted for requested operation")


async def effective_roles_perms(principal: Principal, org_id: Optional[str], project_id: Optional[str]) -> tuple[list[str], list[str]]:
    """Get effective roles and permissions for a principal with caching."""
    ck = cache_key(principal.sub, org_id, project_id)
    now = monotonic()
    cached = _EFFECTIVE_CACHE.get(ck)
    if cached and (now - cached.get("ts", 0.0) < _EFFECTIVE_TTL_SEC):
        return cached.get("roles", []), cached.get("perms", [])

    scopes: list[str] = []
    if org_id:
        scopes.append(f"org:{org_id}")
    if project_id:
        scopes.append(f"project:{project_id}")
    query = (
        "MATCH (u:User {sub: $sub})-[hr:HAS_ROLE]->(r:Role) "
        "WHERE hr.scope IS NULL OR size($scopes) = 0 OR hr.scope IN $scopes "
        "OPTIONAL MATCH (r)-[:GRANTS]->(p:Permission) "
        "RETURN collect(distinct r.name) AS roles, collect(distinct p.name) AS perms"
    )
    rec = await neo4j_query_one(query, {"sub": principal.sub, "scopes": scopes})
    roles: list[str] = (rec.get("roles") if rec else []) or []
    perms: list[str] = (rec.get("perms") if rec else []) or []
    if not roles:
        roles = principal.roles or []
        # Fallback expansion from static grants
        expanded = expand_grants(ROLE_GRANTS, ATOMIC_PERMISSIONS)
        have: set[str] = set()
        for r in roles:
            have.update(expanded.get(r, []))
        perms = sorted(have)

    _EFFECTIVE_CACHE[ck] = {"roles": roles, "perms": perms, "ts": now}
    return roles, perms


async def record_policy_decision(
    action: str,
    decision: str,
    principal: Principal,
    scope: Optional[str] = None,
    reason: Optional[str] = None,
    resource: Optional[Dict[str, Any]] = None,
) -> None:
    """Record a policy decision to the audit log."""
    try:
        await neo4j_query_one(
            """
            CREATE (e:Event {
                id: toString(randomUUID()),
                type: 'policy_decision',
                ts: datetime(),
                action: $action,
                decision: $decision,
                principal: $principal,
                scope: $scope,
                reason: $reason,
                resource: $resource
            }) RETURN e
            """,
            {
                "action": action,
                "decision": decision,
                "principal": {"sub": principal.sub, "roles": principal.roles, "org": principal.org},
                "scope": scope,
                "reason": reason,
                "resource": resource or {},
            },
        )
    except Exception:
        # Non-blocking
        pass


async def env_allows_write(env_id: str) -> bool:
    """Check if an environment allows write operations."""
    try:
        rec = await neo4j_query_one("MATCH (e:Environment {id: $id}) RETURN coalesce(e.allow_query_write, true) AS ok", {"id": env_id})
        if rec and "ok" in rec:
            return bool(rec.get("ok"))
    except Exception:
        return True
    return True


async def collect_policies_for_eval(env_id: Optional[str], project_id: Optional[str], org_id: Optional[str]) -> List[Dict[str, Any]]:
    """Collect all applicable policies for ABAC evaluation."""
    # This is a placeholder - in the real implementation it queries the DB
    # to collect policies from global, org, project, and env scopes
    policies = []

    # Query for policies
    query = """
    MATCH (p:Policy)
    WHERE p.scope = 'global'
       OR (p.scope STARTS WITH 'org:' AND p.scope = $org_scope)
       OR (p.scope STARTS WITH 'project:' AND p.scope = $project_scope)
       OR (p.scope STARTS WITH 'env:' AND p.scope = $env_scope)
    RETURN p
    ORDER BY
        CASE p.scope
            WHEN 'global' THEN 4
            WHEN $env_scope THEN 1
            WHEN $project_scope THEN 2
            WHEN $org_scope THEN 3
            ELSE 5
        END
    """

    params = {
        "org_scope": f"org:{org_id}" if org_id else "",
        "project_scope": f"project:{project_id}" if project_id else "",
        "env_scope": f"env:{env_id}" if env_id else "",
    }

    try:
        rows = await neo4j_query_all(query, params)
        for row in rows:
            if "p" in row and "properties" in row["p"]:
                props = row["p"]["properties"]
                policies.append({
                    "id": props.get("id"),
                    "scope": props.get("scope"),
                    "mode": props.get("mode"),
                    "rules": props.get("rules", []),
                })
    except Exception:
        pass

    return policies


async def approval_threshold_for_async(change_type: str, org_id: Optional[str]) -> int:
    """Get approval threshold for a change type (async version)."""
    # Could query org-specific settings from DB
    # For now, return default
    return approval_threshold_for(change_type, org_id)


# --- FastAPI Dependencies ---

def require_permissions_effective(required: list[str], org_param: str | None = "org_id", project_param: str | None = "project_id"):
    """Dependency to require effective permissions based on graph-derived roles."""
    async def _dep(request: Request, principal: Principal = Depends(get_principal)) -> None:
        org_id = (request.path_params.get(org_param) if org_param else None) or request.headers.get("X-Org-Id") or principal.org
        project_id = (request.path_params.get(project_param) if project_param else None) or request.query_params.get("project_id")
        _roles, perms = await effective_roles_perms(principal, org_id, project_id)
        have = set(perms)
        for need in required:
            if not perm_match_needed(need, have):
                await record_policy_decision(action=need, decision="deny", principal=principal, scope=(f"org:{org_id}" if org_id else None), reason="missing effective permission")
                raise HTTPException(status_code=403, detail=f"Missing permission: {need}")
    return _dep


def enforce_abac(action: str, org_param: str | None = "org_id", project_param: str | None = "project_id", env_param: str | None = "env_id"):
    """Dependency to enforce ABAC policies (deny-first evaluation)."""
    async def _dep(request: Request, principal: Principal = Depends(get_principal)) -> None:
        org_id = (request.path_params.get(org_param) if org_param else None) or request.headers.get("X-Org-Id") or principal.org
        project_id = (request.path_params.get(project_param) if project_param else None) or request.query_params.get("project_id")
        env_id = (request.path_params.get(env_param) if env_param else None) or request.headers.get("X-Env-Id")
        policies = await collect_policies_for_eval(env_id, project_id, org_id)
        roles = set(principal.roles or [])
        # Deny overrides
        for pol in policies:
            for rule in (pol.get("rules") or []):
                if rule.get("effect") != "deny":
                    continue
                if not action_matches(rule.get("actions") or [], action):
                    continue
                if rule.get("roles") and not roles.intersection(set(rule.get("roles"))):
                    continue
                if rule.get("env_ids") and env_id not in set(rule.get("env_ids")):
                    continue
                if rule.get("projects") and project_id not in set(rule.get("projects")):
                    continue
                await record_policy_decision(action=action, decision="deny", principal=principal, scope=pol.get("id"), reason="abac deny matched")
                raise HTTPException(status_code=403, detail="ABAC policy denied request")
        # Allow if any allow rule hits
        for pol in policies:
            for rule in (pol.get("rules") or []):
                if rule.get("effect") != "allow":
                    continue
                if not action_matches(rule.get("actions") or [], action):
                    continue
                if rule.get("roles") and not roles.intersection(set(rule.get("roles"))):
                    continue
                if rule.get("env_ids") and env_id not in set(rule.get("env_ids")):
                    continue
                if rule.get("projects") and project_id not in set(rule.get("projects")):
                    continue
                await record_policy_decision(action=action, decision="allow", principal=principal, scope=pol.get("id"), reason="abac allow matched")
                return None
        # Default according to highest-precedence mode
        mode = None
        for pol in policies:
            if pol.get("mode"):
                mode = pol.get("mode")
                break
        if mode == "enforce":
            await record_policy_decision(action=action, decision="deny", principal=principal, scope=(f"org:{org_id}" if org_id else None), reason="abac default deny")
            raise HTTPException(status_code=403, detail="ABAC policy denied request")
        await record_policy_decision(action=action, decision="allow", principal=principal, scope=(f"org:{org_id}" if org_id else None), reason="abac default allow")
        return None
    return _dep
