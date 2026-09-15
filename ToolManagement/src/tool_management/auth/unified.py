"""
unified.py — Shared token verification + FastAPI auth middleware for every
ToolManagement subdomain service (tools, search, research, publish, memory,
swarm, timeline, fabrics, ...).

Design goal: ZITADEL at auth.distributed.systems is the sole human identity
authority. Product services validate its asymmetric access tokens locally and
bind the human principal to a separate customer/account context. The previous
ToolManagement issuer remains a temporary, explicit migration lane only.

Token formats accepted (in order):
  1. Canonical ZITADEL JWT: RS256, exact issuer, cached JWKS, expiry and
     per-product audience enforcement.
  2. Legacy oauth.py token:  base64url(json claims) + "." + <16-hex HMAC-SHA256>
     — verified locally against TOKEN_SECRET. Claims keys: c (client), s
     (scope), sub, em. No aud/exp/iss: audience check is SKIPPED for legacy
     tokens only while UNIFIED_ALLOW_LEGACY=1.
  3. Unified v2 token:        base64url(json claims) + "." + <32-hex HMAC-SHA256>
     — full digest, standard claims iss/aud/exp/iat, plus terse compat keys.
  4. Standard JWT (HS256): migration-only when UNIFIED_ALLOW_HS256=1.

Revocation: if UNIFIED_INTROSPECTION_URL is set, opaque/legacy tokens are also
checked against the oauth.py durable revocation list via RFC 7662-style
introspection with a small TTL cache. Loopback exemptions are explicit, never
implicit.

Usage:
    from tool_management.auth.unified import AuthMiddleware, require_auth
    app.add_middleware(AuthMiddleware, audience="search", required_scopes={"tools"})
    # or per-route:  @app.get("/x", dependencies=[Depends(require_auth)])

    # non-FastAPI servers (native MCP over HTTP):
    claims = await verify_request(authorization_header, audience="search")
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Optional, Tuple

logger = logging.getLogger("dsco.unified_auth")

# ── Configuration ────────────────────────────────────────────────────────────
TOKEN_SECRET = os.getenv("TOKEN_SECRET", "")  # must match oauth.py issuer
SERVER_PASSWORD = os.getenv("MCP_PASSWORD", "")
ENVIRONMENT = os.getenv("ENV", "development").lower()
CANONICAL_ISSUER = os.getenv(
    "UNIFIED_ISSUER_URL", "https://auth.distributed.systems"
).rstrip("/")
JWKS_URL = os.getenv("UNIFIED_JWKS_URL", f"{CANONICAL_ISSUER}/oauth/v2/keys")
ALLOW_LEGACY = os.getenv(
    "UNIFIED_ALLOW_LEGACY", "0" if ENVIRONMENT == "production" else "1"
) == "1"
ALLOW_HS256 = os.getenv(
    "UNIFIED_ALLOW_HS256", "0" if ENVIRONMENT == "production" else "1"
) == "1"
LEGACY_SUBJECT_IS_ACCOUNT = os.getenv(
    "UNIFIED_LEGACY_SUBJECT_IS_ACCOUNT", "0"
) == "1"
ALLOWED_AUDIENCES = frozenset(
    value.strip()
    for value in os.getenv("UNIFIED_ALLOWED_AUDIENCES", "").split(",")
    if value.strip()
)
PLATFORM_ADMIN_ROLES = frozenset(
    value.strip()
    for value in os.getenv(
        "UNIFIED_PLATFORM_ADMIN_ROLES", "platform.creator,platform.admin"
    ).split(",")
    if value.strip()
)
INTROSPECTION_URL = os.getenv(
    "UNIFIED_INTROSPECTION_URL", ""
)  # e.g. https://tools.distributed.systems/oauth/introspect
INTROSPECTION_TTL = int(os.getenv("UNIFIED_INTROSPECTION_TTL", "60"))
DEFAULT_CLOCK_SKEW = 60  # seconds allowed on exp/nbf checks


def _effective_secret() -> str:
    """Return the migration HMAC secret; derivation is development-only."""
    if TOKEN_SECRET:
        return TOKEN_SECRET
    if ENVIRONMENT == "production":
        return ""
    return hashlib.sha256((SERVER_PASSWORD or "password2026!").encode("utf-8")).hexdigest()


# ── Claims normalization ─────────────────────────────────────────────────────
def _b64url_decode(part: str) -> bytes:
    pad = "=" * (-len(part) % 4)
    return base64.urlsafe_b64decode(part + pad)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


@dataclass
class AuthClaims:
    """Normalized identity across every accepted token format."""
    subject: Optional[str]      # 'sub' (user id) or None for client-only tokens
    client_id: str              # 'c' legacy or 'azp'/'client_id'
    email: Optional[str]
    scopes: FrozenSet[str]
    audience: Optional[str]     # first normalized 'aud'; use audiences for policy
    issuer: Optional[str]
    expires_at: Optional[float]
    issued_at: Optional[float]
    token_format: str           # 'zitadel_jwt' | 'legacy' | 'unified_v2' | 'jwt'
    revoked: bool = False
    audiences: FrozenSet[str] = field(default_factory=frozenset)
    account_ids: FrozenSet[str] = field(default_factory=frozenset)
    selected_account_id: Optional[str] = None
    roles: FrozenSet[str] = field(default_factory=frozenset)
    email_verified: bool = False

    def has_scopes(self, required: FrozenSet[str]) -> bool:
        return required.issubset(self.scopes)

    @property
    def is_platform_admin(self) -> bool:
        return bool(self.roles & PLATFORM_ADMIN_ROLES)


def _string_set(value: Any) -> set[str]:
    if isinstance(value, str):
        return {item for item in value.replace(",", " ").split() if item}
    if isinstance(value, (list, tuple, set, frozenset)):
        return {str(item) for item in value if str(item)}
    return set()


def _extract_roles(raw_claims: Dict[str, Any]) -> FrozenSet[str]:
    roles = set()
    for key in ("dsco:roles", "roles", "role"):
        roles.update(_string_set(raw_claims.get(key)))
    # ZITADEL's project-role claim is keyed by the assigned role name.
    zitadel_roles = raw_claims.get("urn:zitadel:iam:org:project:roles")
    if isinstance(zitadel_roles, dict):
        roles.update(str(role) for role in zitadel_roles)
    return frozenset(roles)


def _extract_accounts(raw_claims: Dict[str, Any], fmt: str) -> tuple[FrozenSet[str], Optional[str]]:
    account_ids: set[str] = set()
    for key in ("dsco:account_ids", "account_ids", "customer_ids"):
        account_ids.update(_string_set(raw_claims.get(key)))
    for key in (
        "urn:zitadel:iam:user:resourceowner:id",
        "urn:zitadel:iam:org:id",
    ):
        value = raw_claims.get(key)
        if value:
            account_ids.add(str(value))

    selected = next(
        (
            str(raw_claims[key])
            for key in ("dsco:account_id", "account_id", "customer_id")
            if raw_claims.get(key)
        ),
        None,
    )
    if selected:
        account_ids.add(selected)
    if fmt in {"legacy", "unified_v2"} and LEGACY_SUBJECT_IS_ACCOUNT:
        subject = raw_claims.get("sub")
        if subject:
            account_ids.add(str(subject))
            selected = selected or str(subject)
    return frozenset(account_ids), selected


def _normalize(raw_claims: Dict[str, Any], fmt: str) -> AuthClaims:
    scope = raw_claims.get("s") or raw_claims.get("scope") or "mcp"
    if isinstance(scope, str):
        scopes = frozenset(scope.split())
    else:
        scopes = frozenset(scope)
    exp_raw = raw_claims.get("exp")
    exp = float(exp_raw) if exp_raw is not None else (
        raw_claims.get("t", 0) + float(os.getenv("UNIFIED_LEGACY_TTL", str(86400 * 30)))
    )
    aud_raw = raw_claims.get("aud")
    audiences = frozenset(_string_set(aud_raw))
    account_ids, selected_account_id = _extract_accounts(raw_claims, fmt)
    return AuthClaims(
        subject=raw_claims.get("sub"),
        client_id=str(raw_claims.get("c") or raw_claims.get("client_id") or raw_claims.get("azp") or "unknown"),
        email=raw_claims.get("em") or raw_claims.get("email"),
        scopes=scopes,
        audience=next(iter(audiences), None),
        issuer=raw_claims.get("iss"),
        expires_at=exp,
        issued_at=raw_claims.get("iat"),
        token_format=fmt,
        audiences=audiences,
        account_ids=account_ids,
        selected_account_id=selected_account_id,
        roles=_extract_roles(raw_claims),
        email_verified=raw_claims.get("email_verified") is True,
    )


# ── Local verification ───────────────────────────────────────────────────────
def _verify_legacy_or_v2(token: str, secret: str) -> Optional[Tuple[Dict[str, Any], str]]:
    """oauth.py shape: payload_b64 + '.' + hexdigest (16 or 32 hex chars).
    The 16-char variant is legacy (64-bit MAC); 32-char is unified v2."""
    try:
        payload_b64, sig = token.rsplit(".", 1)
        if len(sig) not in (16, 32):
            return None
        payload = json.loads(_b64url_decode(payload_b64))
        expected = hmac.new(
            secret.encode(), json.dumps(payload, separators=(",", ":")).encode(), hashlib.sha256
        ).hexdigest()[: len(sig)]
        # oauth.py signs json.dumps(payload).encode() — default separators.
        expected_compat = hmac.new(
            secret.encode(), json.dumps(payload).encode(), hashlib.sha256
        ).hexdigest()[: len(sig)]
        if hmac.compare_digest(sig, expected) or hmac.compare_digest(sig, expected_compat):
            fmt = "unified_v2" if len(sig) == 32 else "legacy"
            return payload, fmt
        return None
    except Exception:
        return None


def _verify_jwt_hs256(token: str, secret: str) -> Optional[Tuple[Dict[str, Any], str]]:
    try:
        h_b64, p_b64, s_b64 = token.split(".")
        header = json.loads(_b64url_decode(h_b64))
        if header.get("alg") != "HS256":
            return None
        signing_input = f"{h_b64}.{p_b64}".encode()
        if not hmac.compare_digest(
            _b64url_decode(s_b64), hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
        ):
            return None
        return json.loads(_b64url_decode(p_b64)), "jwt"
    except Exception:
        return None


_jwks_client = None
_jwks_lock = threading.Lock()


def _get_jwks_client():
    """Create the cached canonical-issuer JWKS client lazily."""
    global _jwks_client
    with _jwks_lock:
        if _jwks_client is None:
            import jwt

            _jwks_client = jwt.PyJWKClient(
                JWKS_URL,
                cache_keys=True,
                lifespan=int(os.getenv("UNIFIED_JWKS_TTL", "600")),
            )
        return _jwks_client


def _verify_zitadel_jwt(token: str) -> Optional[Tuple[Dict[str, Any], str]]:
    """Verify a canonical RS256 token against auth.distributed.systems."""
    try:
        import jwt

        header = jwt.get_unverified_header(token)
        if header.get("alg") != "RS256" or not header.get("kid"):
            return None
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            issuer=CANONICAL_ISSUER,
            options={
                "verify_aud": False,  # enforced below against service policy
                "require": ["iss", "sub", "exp", "iat"],
            },
            leeway=DEFAULT_CLOCK_SKEW,
        )
        return claims, "zitadel_jwt"
    except Exception as exc:
        logger.debug("canonical JWT verification failed: %s", type(exc).__name__)
        return None


def verify_token(token: str) -> Optional[AuthClaims]:
    """Verify signature only (no audience/expiry policy). Use verify_request
    for full enforcement, or call this when you need claims without a policy."""
    if not token:
        return None

    # Canonical asymmetric identity is always attempted first and never falls
    # back to trusting an unverified JWT payload.
    if token.count(".") == 2:
        canonical = _verify_zitadel_jwt(token)
        if canonical:
            return _normalize(canonical[0], canonical[1])

    secret = _effective_secret()
    verifiers = []
    if ALLOW_LEGACY and secret:
        verifiers.append(_verify_legacy_or_v2)
    if ALLOW_HS256 and secret:
        verifiers.append(_verify_jwt_hs256)
    for fn in verifiers:
        result = fn(token, secret)
        if result:
            return _normalize(result[0], result[1])
    return None


def select_account_id(claims: AuthClaims, requested_account_id: Optional[str] = None) -> str:
    """Select a customer account only from the token-authorized account set.

    A human subject is never itself a customer ID. Multi-account principals
    must choose one explicitly with X-DSCO-Account-ID.
    """
    requested = (requested_account_id or "").strip() or None
    if requested:
        if requested not in claims.account_ids:
            raise PermissionError("account_not_authorized")
        return requested
    if claims.selected_account_id:
        if claims.selected_account_id not in claims.account_ids:
            raise PermissionError("account_not_authorized")
        return claims.selected_account_id
    if len(claims.account_ids) == 1:
        return next(iter(claims.account_ids))
    if not claims.account_ids:
        raise PermissionError("account_context_missing")
    raise PermissionError("account_selection_required")


def enforce_audience(claims: AuthClaims, audience: str) -> None:
    """Apply the same service-audience policy in sync and async call sites."""
    accepted_audiences = {audience, *ALLOWED_AUDIENCES}
    if claims.audiences and claims.audiences.isdisjoint(accepted_audiences):
        raise PermissionError(
            f"audience_mismatch: token aud={sorted(claims.audiences)!r} service={audience!r}"
        )
    if claims.token_format == "zitadel_jwt" and not claims.audiences:
        raise PermissionError("audience_missing")


# ── Remote introspection (RFC 7662-style, backed by oauth.py revocation) ─────
introspect_cache: Dict[str, Tuple[float, bool]] = {}


async def introspect_revoked(token: str, httpx_client=None) -> bool:
    """Returns True if the token is durably revoked at the issuer."""
    if not INTROSPECTION_URL:
        return False
    now = time.time()
    key = hashlib.sha256(token.encode()).hexdigest()[:32]
    cached = introspect_cache.get(key)
    if cached and now < cached[0]:
        return cached[1]
    try:
        if httpx_client is None:
            import httpx  # already in ToolManagement requirements
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                INTROSPECTION_URL, json={"token": token}, headers={"Content-Type": "application/json"}
            )
            if resp.status_code != 200:
                logger.warning("introspection endpoint returned %s; failing open (revocation cache-bypass)", resp.status_code)
                return False
            revoked = bool(resp.json().get("revoked", False))
        introspect_cache[key] = (now + INTROSPECTION_TTL, revoked)
        return revoked
    except Exception as exc:  # network down — fail open with logged warning
        logger.warning("introspection unreachable (%s); allowing token", exc)
        return False


# ── Policy enforcement ───────────────────────────────────────────────────────
async def verify_request(
    authorization_header: Optional[str],
    audience: Optional[str] = None,
    required_scopes: Optional[FrozenSet[str]] = None,
    check_revocation: bool = True,
    clock_skew: int = DEFAULT_CLOCK_SKEW,
) -> AuthClaims:
    """Full enforcement: signature -> revocation -> expiry -> audience -> scopes.
    Raises PermissionError with a machine-readable reason on failure."""
    if not authorization_header or not authorization_header.lower().startswith("bearer "):
        raise PermissionError("missing_bearer_token")
    token = authorization_header[7:].strip()
    claims = verify_token(token)
    if claims is None:
        raise PermissionError("invalid_token_signature")
    if (
        check_revocation
        and claims.token_format != "zitadel_jwt"
        and await introspect_revoked(token)
    ):
        raise PermissionError("token_revoked")
    now = time.time()
    if claims.expires_at and now > claims.expires_at + clock_skew:
        raise PermissionError("token_expired")
    if audience:
        enforce_audience(claims, audience)
    if required_scopes and not claims.has_scopes(required_scopes):
        raise PermissionError(f"insufficient_scope: need={sorted(required_scopes)} have={sorted(claims.scopes)}")
    return claims


# ── FastAPI integration ──────────────────────────────────────────────────────
PUBLIC_PATHS = frozenset(
    {"/health", "/ready", "/healthz", "/readyz", "/docs", "/redoc", "/openapi.json"}
)


@dataclass
class AuthMiddlewareConfig:
    audience: str                                    # subdomain/service name
    required_scopes: FrozenSet[str] = field(default_factory=lambda: frozenset({"tools"}))
    public_paths: FrozenSet[str] = PUBLIC_PATHS
    public_prefixes: Tuple[str, ...] = ("/oauth/",)  # auth endpoints themselves
    exempt_loopback: bool = False                    # opt-in via UNIFIED_LOOPBACK_TRUST=1
    # (single-host deployments reach the public surface over loopback too —
    #  blanket loopback trust makes enforcement a no-op there; internal
    #  fabric-to-fabric calls should use minted service tokens instead)
    enforce: bool = True                             # shadow mode toggle for rollout


def make_auth_middleware(config: AuthMiddlewareConfig):
    """Returns an ASGI/http-middleware factory usable by every subdomain service.

    Rollout: deploy with enforce=False first (log-only 'shadow' decisions),
    then flip enforce=True per subdomain — the 401 challenge shape follows the
    MCP OAuth resource-server convention so MCP clients re-auth correctly.
    """
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse
    from starlette.requests import Request

    class UnifiedAuthMiddleware(BaseHTTPMiddleware):
        def __init__(self, app):
            super().__init__(app)

        async def dispatch(self, request: Request, call_next):
            path = request.url.path
            if path in config.public_paths or any(path.startswith(p) for p in config.public_prefixes):
                return await call_next(request)
            if (
                config.exempt_loopback
                and os.getenv("UNIFIED_LOOPBACK_TRUST", "") == "1"
                and request.client
                and request.client.host in ("127.0.0.1", "::1")
            ):
                # Internal fabric traffic is trusted; record identity when present.
                hdr = request.headers.get("authorization")
                if hdr:
                    claims = verify_token(hdr[7:] if hdr.lower().startswith("bearer ") else "")
                    if claims:
                        request.state.auth_claims = claims
                return await call_next(request)
            hdr = request.headers.get("authorization")
            try:
                claims = await verify_request(hdr, audience=config.audience, required_scopes=config.required_scopes)
            except PermissionError as exc:
                if not config.enforce:
                    logger.info("[shadow auth %s] would-deny %s: %s", config.audience, path, exc)
                    return await call_next(request)
                if str(exc) in ("missing_bearer_token", "token_expired"):
                    # MCP-spec challenge: tells clients where to re-authenticate.
                    issuer = CANONICAL_ISSUER
                    meta = f"{issuer}/.well-known/oauth-protected-resource"
                    return JSONResponse(
                        status_code=401,
                        headers={
                            "WWW-Authenticate": f'Bearer resource_metadata="{meta}", scope="{" ".join(sorted(config.required_scopes))}"'
                        },
                        content={"error": "unauthorized", "detail": str(exc)},
                    )
                return JSONResponse(status_code=403, content={"error": "forbidden", "detail": str(exc)})
            request.state.auth_claims = claims
            return await call_next(request)

    return UnifiedAuthMiddleware


# Convenience dependency for per-route protection:
async def require_auth(
    audience: Optional[str] = None, required_scopes: Optional[FrozenSet[str]] = None
):
    from fastapi import HTTPException, Request, status

    async def dependency(request: Request):
        hdr = request.headers.get("authorization")
        try:
            claims = await verify_request(hdr, audience=audience, required_scopes=required_scopes)
        except PermissionError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        request.state.auth_claims = claims
        return claims

    return dependency


# ── Issuance helper (for services that mint service-to-service tokens) ──────
def mint_service_token(
    client_id: str,
    audience: str,
    scopes: FrozenSet[str],
    ttl: int = 3600,
    subject: Optional[str] = None,
) -> str:
    """Unified v2 token: 32-hex HMAC, standard claims + compat keys."""
    now = int(time.time())
    claims = {
        "c": client_id,
        "s": " ".join(sorted(scopes)),
        "sub": subject,
        "em": None,
        # HMAC service tokens are migration tokens issued by ToolManagement;
        # they must never impersonate the canonical ZITADEL issuer.
        "iss": os.getenv(
            "UNIFIED_LEGACY_ISSUER_URL", "https://tools.distributed.systems"
        ),
        "aud": audience,
        "iat": now,
        "exp": now + ttl,
        "v": 2,
    }
    payload = json.dumps(claims, separators=(",", ":")).encode()
    sig = hmac.new(_effective_secret().encode(), payload, hashlib.sha256).hexdigest()[:32]  # 32 hex
    return _b64url_encode(payload) + "." + sig
