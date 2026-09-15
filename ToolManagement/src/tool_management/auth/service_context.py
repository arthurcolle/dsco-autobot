"""Signed, short-lived identity context for calls from Tools to native workers.

Customer access tokens terminate at tools.distributed.systems.  Native workers
receive only the minimum account/principal/trace claims they need, protected by
a domain-separated MAC derived from the existing backend service secret.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any, Dict, Optional


CONTEXT_VERSION = 1
MAX_CONTEXT_TTL_SECONDS = 300
DEFAULT_CONTEXT_TTL_SECONDS = 120
_KEY_LABEL = b"dsco-tool-execution-context-v1"


class ServiceContextError(ValueError):
    """The delegated execution context is missing, invalid, or expired."""


def _canonical(claims: Dict[str, Any]) -> bytes:
    return json.dumps(claims, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _signing_key(secret: str) -> bytes:
    if not secret:
        raise ServiceContextError("service context signing secret is unavailable")
    return hmac.new(secret.encode("utf-8"), _KEY_LABEL, hashlib.sha256).digest()


def sign_service_context(
    secret: str,
    *,
    audience: str,
    account_id: Optional[str],
    principal_id: Optional[str],
    issuer: Optional[str],
    identity_kind: str,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    ttl_seconds: int = DEFAULT_CONTEXT_TTL_SECONDS,
    now: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a signed context safe to place in an internal tool call."""
    if not audience:
        raise ServiceContextError("service context audience is required")
    ttl = max(1, min(int(ttl_seconds), MAX_CONTEXT_TTL_SECONDS))
    issued_at = int(time.time() if now is None else now)
    claims: Dict[str, Any] = {
        "v": CONTEXT_VERSION,
        "aud": str(audience),
        "account_id": str(account_id) if account_id else None,
        "principal_id": str(principal_id) if principal_id else None,
        "iss": str(issuer) if issuer else None,
        "kind": str(identity_kind or "unknown"),
        "trace_id": str(trace_id) if trace_id else None,
        "session_id": str(session_id) if session_id else None,
        "invocation_id": str(uuid.uuid4()),
        "iat": issued_at,
        "exp": issued_at + ttl,
    }
    signature = hmac.new(_signing_key(secret), _canonical(claims), hashlib.sha256).hexdigest()
    return {"claims": claims, "signature": signature}


def verify_service_context(
    envelope: Any,
    secret: str,
    *,
    audience: str,
    now: Optional[int] = None,
    clock_skew_seconds: int = 30,
) -> Dict[str, Any]:
    """Verify and return normalized claims for one native-worker invocation."""
    if not isinstance(envelope, dict):
        raise ServiceContextError("signed service context is required")
    claims = envelope.get("claims")
    signature = envelope.get("signature")
    if not isinstance(claims, dict) or not isinstance(signature, str):
        raise ServiceContextError("malformed service context")
    expected = hmac.new(_signing_key(secret), _canonical(claims), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise ServiceContextError("invalid service context signature")
    if claims.get("v") != CONTEXT_VERSION:
        raise ServiceContextError("unsupported service context version")
    if claims.get("aud") != audience:
        raise ServiceContextError("service context audience mismatch")
    try:
        issued_at = int(claims["iat"])
        expires_at = int(claims["exp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ServiceContextError("invalid service context timestamps") from exc
    current = int(time.time() if now is None else now)
    skew = max(0, int(clock_skew_seconds))
    if issued_at > current + skew:
        raise ServiceContextError("service context is not active")
    if expires_at < current - skew:
        raise ServiceContextError("service context expired")
    if expires_at <= issued_at or expires_at - issued_at > MAX_CONTEXT_TTL_SECONDS:
        raise ServiceContextError("invalid service context lifetime")
    if not isinstance(claims.get("invocation_id"), str) or not claims["invocation_id"]:
        raise ServiceContextError("service context invocation ID is required")
    if str(claims.get("kind") or "").startswith("customer") and not claims.get("account_id"):
        raise ServiceContextError("customer service context requires an account")
    return dict(claims)


def context_scope_id(claims: Optional[Dict[str, Any]]) -> str:
    """Return the durable tenant partition for a verified execution context."""
    if claims and claims.get("account_id"):
        return str(claims["account_id"])
    return "__system__"
