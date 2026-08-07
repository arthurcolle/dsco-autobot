"""Usage-based payment enforcement for the Gateway (and any metered surface).

Policy, given a paying principal (user_id):
  * active subscription  -> allowed, metered for invoicing, no balance debit
  * prepaid credits       -> allowed iff balance covers the price; debited up front,
                             refunded if the call then fails
  * neither               -> denied (HTTP 402)

Enforcement is OFF unless USAGE_BILLING_ENFORCE is truthy, so existing behavior and
tests are unchanged by default. Unexpected billing errors fail OPEN (serve the call,
log it) — a billing bug must never take the revenue surface down; an insufficient
balance is a clean deny, not an error.
"""
from __future__ import annotations

import inspect
import logging
import os
from typing import Any, Dict, Optional

from fastapi import Request

logger = logging.getLogger(__name__)

INTERNAL = "__internal__"  # operator / service caller: exempt from charge
_TRUTHY = {"1", "true", "yes", "on"}


def enforce_enabled() -> bool:
    return os.environ.get("USAGE_BILLING_ENFORCE", "").strip().lower() in _TRUTHY


def _model_tool_prefixes() -> tuple:
    extra = os.environ.get("USAGE_MODEL_PREFIXES", "").strip()
    base = ("model:", "model::", "checkpoint:", "infer:", "ckpt:")
    return base + tuple(p for p in extra.split(",") if p)


def classify(tool: str) -> tuple[str, str]:
    """Map a gateway tool name to (billing_kind, ref). Checkpointed-model calls are
    priced as 'model_inference' (per-model ref); everything else as 'gateway_call'."""
    t = tool or ""
    if t.startswith(_model_tool_prefixes()):
        return "model_inference", t
    return "gateway_call", t


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def billing_principal(request: Request) -> Optional[str]:
    """FastAPI dependency: resolve the paying principal (a billing user_id).

    Prefers the Tools-API identity resolver (maps sk-user tokens / OAuth to a
    customer user_id). Falls back to an explicit X-User-ID / X-Client-ID header.
    Returns INTERNAL for operator/service callers, None if unresolved."""
    if not enforce_enabled():
        return None
    try:
        from ..api.tools_api import resolve_execution_identity  # lazy: avoid import cycle

        ident = await _maybe_await(resolve_execution_identity(request))
        if isinstance(ident, dict):
            kind = ident.get("kind")
            tenant = ident.get("tenant_id")
            if kind == "customer" and tenant:
                return str(tenant)
            if kind in ("operator", "service"):
                return INTERNAL
    except Exception:  # resolver absent / different signature / no auth — fall back
        pass
    uid = request.headers.get("X-User-ID") or request.headers.get("X-Client-ID")
    if uid:
        return str(uid)
    bearer = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if bearer.startswith("sk-user-"):
        return bearer
    return None


def charge(store: Any, principal: Optional[str], tool: str) -> Dict[str, Any]:
    """Authorize + charge one metered gateway call. Returns a dict with at least
    `allowed`; on credit charges includes `charged_cents`/`ref` for a later refund."""
    if not enforce_enabled():
        return {"allowed": True, "enforced": False}
    if principal is None:
        return {"allowed": False, "reason": "auth_required"}
    if principal == INTERNAL:
        return {"allowed": True, "via": "internal", "charged_cents": 0.0}
    kind, ref = classify(tool)
    try:
        if store.has_active_entitlement(principal):
            store.record_usage(principal, kind, ref)
            return {"allowed": True, "via": "subscription", "charged_cents": 0.0}
        r = store.try_charge(principal, kind, ref)
        if r["allowed"]:
            return {
                "allowed": True, "via": "credits", "kind": kind, "ref": ref,
                "charged_cents": r["price_cents"], "balance_cents": r["balance_cents"],
            }
        return {
            "allowed": False, "reason": "insufficient_credits",
            "price_cents": r["price_cents"], "balance_cents": r["balance_cents"],
        }
    except Exception as e:  # never take the serving surface down on a billing bug
        logger.warning("usage billing failed open for %s: %s", principal, e)
        return {"allowed": True, "via": "error_open", "charged_cents": 0.0}


def refund(store: Any, principal: Optional[str], charge_result: Dict[str, Any], tool: str) -> None:
    """Return credits for a charged call that then failed."""
    try:
        if (
            principal and principal != INTERNAL
            and charge_result.get("via") == "credits"
            and float(charge_result.get("charged_cents", 0)) > 0
        ):
            store.refund(principal, float(charge_result["charged_cents"]), charge_result.get("ref", tool))
    except Exception as e:
        logger.warning("usage billing refund failed for %s: %s", principal, e)
