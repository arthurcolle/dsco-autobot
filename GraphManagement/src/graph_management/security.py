from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List, Set, Dict, Any, Optional, Callable, Mapping

from fastapi import Depends, HTTPException, Request


@dataclass
class Principal:
    sub: str
    roles: List[str]
    org: Optional[str] = None
    scopes: List[str] = None


def _auth_mode() -> str:
    # Modes: disabled (default), api_key, jwt
    if os.getenv("AUTH_API_KEYS"):
        return "api_key"
    if os.getenv("AUTH_JWT_SECRET"):
        return "jwt"
    return os.getenv("AUTH_MODE", "disabled")


def _load_api_keys() -> Dict[str, Dict[str, Any]]:
    val = os.getenv("AUTH_API_KEYS")
    if not val:
        return {}
    try:
        return json.loads(val)
    except Exception:
        return {}


def _api_key_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """
    Extract an API key from supported headers.

    Preference order:
    1. Explicit X-API-Key header (legacy support)
    2. Authorization: Bearer <key>
    """
    api_key = headers.get("X-API-Key")
    if api_key:
        return api_key.strip()
    authz = headers.get("Authorization", "")
    if authz:
        scheme, _, value = authz.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    return None


async def get_principal(request: Request) -> Principal:
    return principal_from_headers(request.headers)


def principal_from_headers(headers: Mapping[str, str]) -> Principal:
    mode = _auth_mode()
    if mode == "disabled":
        # Dev default: everything allowed with developer role
        return Principal(sub="anonymous", roles=["developer"], org=headers.get("X-Org-Id"))

    if mode == "api_key":
        api_key = _api_key_from_headers(headers)
        mapping = _load_api_keys()
        if not api_key:
            raise HTTPException(status_code=401, detail="Missing API key (use Authorization: Bearer <key> or X-API-Key header)")
        if api_key not in mapping:
            raise HTTPException(status_code=401, detail="Invalid API key")
        entry = mapping[api_key]
        return Principal(
            sub=str(entry.get("sub", api_key)),
            roles=list(entry.get("roles", [])),
            org=str(entry.get("org", headers.get("X-Org-Id"))) if entry.get("org") or headers.get("X-Org-Id") else None,
            scopes=list(entry.get("scopes", [])),
        )

    if mode == "jwt":
        import jwt  # PyJWT
        authz = headers.get("Authorization", "")
        if not authz.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing Bearer token")
        token = authz.split(" ", 1)[1]
        secret = os.getenv("AUTH_JWT_SECRET")
        try:
            payload = jwt.decode(token, secret, algorithms=["HS256"], options={"verify_aud": False})
        except Exception as exc:
            raise HTTPException(status_code=401, detail=f"Invalid token: {exc}")
        roles = payload.get("roles") or payload.get("role") or []
        if isinstance(roles, str):
            roles = [roles]
        scopes = payload.get("scope") or payload.get("scopes") or []
        if isinstance(scopes, str):
            scopes = scopes.split()
        return Principal(
            sub=str(payload.get("sub", "unknown")),
            roles=list(roles),
            org=str(payload.get("org") or headers.get("X-Org-Id")) if (payload.get("org") or headers.get("X-Org-Id")) else None,
            scopes=list(scopes),
        )

    # Fallback
    return Principal(sub="anonymous", roles=["developer"], org=headers.get("X-Org-Id"))


_ROLE_PERMISSIONS: Dict[str, List[str]] = {
    "org_admin": ["*"],
    # Broad administrative role across schema/governance/resources
    "schema_admin": [
        "schemas:*",
        "proposals:*",
        "policy:*",
        "query:*",
        "orgs:*",
        "teams:*",
        "projects:*",
        "tools:*",
        "datasets:*",
        "secrets:*",
        "agents:*",
        "runs:*",
        "budgets:*",
        "events:*",
    ],
    # Governance roles
    "schema_approver": ["schemas:read", "proposals:review", "proposals:apply"],
    "schema_reviewer": ["schemas:read", "proposals:review"],
    # Developer defaults: read schemas/resources, create proposals, run queries and runs
    "developer": [
        "schemas:read",
        "proposals:create",
        "query:read",
        "query:write",
        "tools:read",
        "datasets:read",
        "runs:write",
        "runs:read",
        "events:read",
    ],
    # Read-only users
    "readonly": [
        "schemas:read",
        "query:read",
        "tools:read",
        "datasets:read",
        "runs:read",
        "events:read",
    ],
    # Service workloads (agents/services)
    "service": [
        "schemas:read",
        "query:*",
        "tools:read",
        "datasets:read",
        "runs:*",
    ],
}


def _expand_permissions(roles: List[str]) -> Set[str]:
    perms: Set[str] = set()
    for r in roles:
        perms.update(_ROLE_PERMISSIONS.get(r, []))
    return perms


def _perm_match(needed: str, have: Set[str]) -> bool:
    if "*" in have:
        return True
    if needed in have:
        return True
    # wildcards category:* match
    cat = needed.split(":", 1)[0] + ":*"
    if cat in have:
        return True
    return False


def require_permissions(required: List[str]) -> Callable[[Principal], None]:
    async def _dep(principal: Principal = Depends(get_principal)) -> None:
        have = _expand_permissions(principal.roles)
        for need in required:
            if not _perm_match(need, have):
                raise HTTPException(status_code=403, detail=f"Missing permission: {need}")
    return _dep


def has_permission(principal: Principal, permission: str) -> bool:
    have = _expand_permissions(principal.roles)
    return _perm_match(permission, have)
