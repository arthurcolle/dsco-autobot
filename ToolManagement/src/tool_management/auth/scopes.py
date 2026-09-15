"""
scopes.py — Single source of truth for subdomain audiences + scope vocabulary.

Every ToolManagement subdomain service imports its auth config from here
(AUTH_UNIFIED.md T05). No hand-rolled per-service scope lists.

Audience names are the subdomain labels; scope names are transport-level
grants enforced by tool_management.auth.unified middleware/dependencies.
Tool-level capability constraints remain the warrant system's job
(auth/warrants.py) — scopes gate the service, warrants gate the tool.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet, Tuple

# ── Scope vocabulary ─────────────────────────────────────────────────────────
# Identity (OIDC-standard)
SCOPE_OPENID = "openid"
SCOPE_PROFILE = "profile"
SCOPE_EMAIL = "email"

# Fabric/service scopes (transport-level)
SCOPE_MCP = "mcp"                # MCP protocol sessions
SCOPE_TOOLS = "tools"            # registry read/search/execute
SCOPE_TOOLS_ADMIN = "tools:admin"  # register/update/delete tools
SCOPE_SEARCH = "search"
SCOPE_RESEARCH = "research"
SCOPE_PUBLISH = "publish"        # external publication (operator identity required)
SCOPE_MEMORY = "memory"
SCOPE_SWARM = "swarm"
SCOPE_TIMELINE = "timeline"
SCOPE_VIDEO_QA = "video_qa"
SCOPE_DOC_QA = "doc_qa"
SCOPE_INTEGRATION = "integration"
SCOPE_FINANCE = "finance"        # never poolable; tenant BYOK identity must hold
SCOPE_UNIT = "unit"

COMMON_IDENTITY_SCOPES = frozenset({SCOPE_OPENID, SCOPE_PROFILE, SCOPE_EMAIL})


# ── Subdomain registry ───────────────────────────────────────────────────────
@dataclass(frozen=True)
class Subdomain:
    """One service's transport-auth contract."""
    audience: str                       # 'aud' claim value; subdomain label
    required_scopes: FrozenSet[str]     # transport scopes the middleware demands
    public_paths: FrozenSet[str] = field(default=frozenset({
        "/health", "/ready", "/healthz", "/readyz", "/docs", "/redoc", "/openapi.json",
    }))
    public_prefixes: Tuple[str, ...] = ("/oauth/", "/.well-known/")
    operator_identity_required: bool = False   # e.g. publish: only human/operator tokens
    notes: str = ""


REGISTRY: dict[str, Subdomain] = {
    sub.audience: sub
    for sub in [
        Subdomain(
            "unit_fabric",
            frozenset({SCOPE_UNIT, SCOPE_TOOLS}),
            notes="T06 pilot for enforce rollout.",
        ),
        Subdomain(
            "timeline",
            frozenset({SCOPE_TIMELINE, SCOPE_TOOLS}),
            notes="T07.",
        ),
        Subdomain(
            "video_qa",
            frozenset({SCOPE_VIDEO_QA, SCOPE_TOOLS}),
            notes="T08 (with doc_qa).",
        ),
        Subdomain(
            "doc_qa",
            frozenset({SCOPE_DOC_QA, SCOPE_TOOLS}),
            notes="T08 (with video_qa).",
        ),
        Subdomain(
            "finance_fabric",
            frozenset({SCOPE_FINANCE, SCOPE_TOOLS}),
            notes="T09 — tenant BYOK paths live here; shadow review must cover resolved-credential calls.",
        ),
        Subdomain(
            "integration_fabric",
            frozenset({SCOPE_INTEGRATION, SCOPE_TOOLS}),
            notes="T10 — weather/econ live branches.",
        ),
        Subdomain(
            "search",
            frozenset({SCOPE_SEARCH, SCOPE_TOOLS}),
            notes="T11 — bulk live read traffic.",
        ),
        Subdomain(
            "research",
            frozenset({SCOPE_RESEARCH, SCOPE_TOOLS}),
            notes="T11 — bulk live read traffic.",
        ),
        Subdomain(
            "publish",
            frozenset({SCOPE_PUBLISH, SCOPE_TOOLS}),
            operator_identity_required=True,
            notes="T12 — external publication: operator identity required, never agent identity.",
        ),
        Subdomain(
            "memory",
            frozenset({SCOPE_MEMORY, SCOPE_TOOLS}),
            notes="T13 — internal state plane.",
        ),
        Subdomain(
            "swarm",
            frozenset({SCOPE_SWARM, SCOPE_TOOLS}),
            notes="T13 — internal state plane.",
        ),
        Subdomain(
            "tools",
            frozenset({SCOPE_TOOLS}),
            notes="T14 — the registry + IdP host; protected LAST (after all other flips).",
        ),
        Subdomain(
            "registry",
            frozenset({SCOPE_TOOLS}),
            notes="Registry supervisor service; unified under the tools audience vocabulary (T14 companion).",
        ),
    ]
}


def get(audience: str) -> Subdomain:
    if audience not in REGISTRY:
        raise KeyError(f"unknown subdomain audience {audience!r} — register it in scopes.py first")
    return REGISTRY[audience]


# Service-name derivation often yields a `distributed_`-prefixed audience
# (e.g. SERVICE_NAME "Distributed Search MCP" -> "distributed_search"); alias
# it back to the canonical registry key rather than forcing per-service env
# overrides.
_ALIASES = {
    "distributed_search": "search",
    "distributed_research": "research",
    "distributed_publish": "publish",
    "distributed_memory": "memory",
    "distributed_swarm": "swarm",
    "distributed_timeline": "timeline",
    "distributed_video_qa": "video_qa",
    "distributed_document_qa": "doc_qa",
    "document_qa": "doc_qa",
    # Derived from live SERVICE_NAMEs (observed at runtime 2026-08-31):
    "distributed_unit": "unit_fabric",
    "unit": "unit_fabric",
    "video_q&a": "video_qa",           # '&' appears verbatim in some SERVICE_NAMEs
    "distributed_video_q&a": "video_qa",
    "distributed_integration": "integration_fabric",
    "distributed_finance": "finance_fabric",
    "integration": "integration_fabric",
    "finance": "finance_fabric",
}


def resolve(audience: str) -> Subdomain:
    """Alias-tolerant lookup: exact key, then known alias, else KeyError."""
    if audience in REGISTRY:
        return REGISTRY[audience]
    alias = _ALIASES.get(audience)
    if alias:
        return REGISTRY[alias]
    raise KeyError(
        f"unknown subdomain audience {audience!r} — register it in scopes.py or map it in _ALIASES"
    )


def audience_names() -> list[str]:
    return sorted(REGISTRY)


__all__ = [
    "Subdomain", "REGISTRY", "get", "audience_names", "COMMON_IDENTITY_SCOPES",
    "SCOPE_OPENID", "SCOPE_PROFILE", "SCOPE_EMAIL", "SCOPE_MCP", "SCOPE_TOOLS",
    "SCOPE_TOOLS_ADMIN", "SCOPE_SEARCH", "SCOPE_RESEARCH", "SCOPE_PUBLISH",
    "SCOPE_MEMORY", "SCOPE_SWARM", "SCOPE_TIMELINE", "SCOPE_VIDEO_QA",
    "SCOPE_DOC_QA", "SCOPE_INTEGRATION", "SCOPE_FINANCE", "SCOPE_UNIT",
]
