"""
Sign in with Distributed — OAuth 2.0 / OIDC identity provider.

Serves two audiences with one authorization server:
- MCP clients (Claude iOS/Desktop) connecting to this API (RFC 9728 + 8414 +
  7591, authorization code + PKCE) — unchanged, still accepts the operator
  password.
- First-party and third-party apps offering "Sign in with Distributed":
  email/password login against real user accounts, consent screen, user-bound
  access tokens, OIDC id_token + /oauth/userinfo, refresh tokens, and durable
  revocation (RFC 7009).

Clients, consent grants, refresh tokens, and revocations persist in DuckDB
(persistence/oauth_store.py); access tokens are stateless HMAC-signed and
checked against the durable revocation list.
"""

import hashlib
import hmac
import base64
import html
import json
import time
import os
import secrets
import logging
from typing import Dict, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Request, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["auth"])

# ── Config ──────────────────────────────────────────────────────────────────
# T02 (AUTH_UNIFIED.md): fail hard in production when no explicit secret is
# configured; the sha256(MCP_PASSWORD) derivation remains dev-only.
_ENV = os.getenv("ENV", "development")
SERVER_PASSWORD = os.getenv(
    "MCP_PASSWORD", "" if _ENV == "production" else "password2026!"
)
TOKEN_SECRET = os.getenv("TOKEN_SECRET", "")
if _ENV == "production":
    if not TOKEN_SECRET:
        raise RuntimeError(
            "AUTH CONFIG ERROR: TOKEN_SECRET must be set explicitly in production "
            "(AUTH_UNIFIED.md T02 — derived-from-password secrets are disabled)."
        )
    if SERVER_PASSWORD == "password2026!":
        raise RuntimeError(
            "AUTH CONFIG ERROR: MCP_PASSWORD is the published default; refusing to "
            "boot in production with the default operator password."
        )
else:
    TOKEN_SECRET = TOKEN_SECRET or hashlib.sha256(
        (SERVER_PASSWORD or "password2026!").encode("utf-8")
    ).hexdigest()
TOKEN_TTL = 86400 * 30  # 30 days
REFRESH_TOKEN_TTL = 86400 * 90
DEFAULT_BEARER_TOKEN = os.getenv("DEFAULT_BEARER_TOKEN", "")
SUPPORTED_SCOPES = ["openid", "profile", "email", "mcp", "tools"]

# In-memory: short-lived auth codes only (5 min TTL); everything durable is in
# persistence/oauth_store.py.
_auth_codes: Dict[str, dict] = {}


def _oauth_store():
    from .persistence.oauth_store import get_oauth_store

    return get_oauth_store()


def _user_store():
    from .persistence.user_store import get_user_store

    return get_user_store()


def _base_url(request: Request) -> str:
    """Derive the public base URL from the request."""
    configured = os.getenv("API_BASE_URL")
    if configured:
        return configured.rstrip("/")
    proto = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("host", "localhost")
    return f"{proto}://{host}"


# ── Tokens ──────────────────────────────────────────────────────────────────

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _make_token(client_id: str, user: Optional[dict] = None, scope: str = "mcp") -> str:
    """Create a signed access token, optionally bound to a user identity.

    T03 (AUTH_UNIFIED.md): new issuances carry unified-v2 claims — iss, aud,
    exp, iat, v:2 — alongside the legacy terse keys, and a full 32-hex HMAC
    (vs the legacy 16-hex MAC). _decode_token accepts both lengths, so existing
    client tokens keep verifying until their 30d TTL expires them naturally.
    """
    now = time.time()
    claims = {"c": client_id, "t": now, "s": scope}
    if user:
        claims["sub"] = user["user_id"]
        claims["em"] = user.get("email")
    claims.update({
        # This is the retiring ToolManagement issuer, not the canonical
        # ZITADEL authority at auth.distributed.systems.
        "iss": os.getenv(
            "UNIFIED_LEGACY_ISSUER_URL", "https://tools.distributed.systems"
        ),
        "aud": os.getenv("UNIFIED_DEFAULT_AUD", "tools"),
        "iat": int(now),
        "exp": int(now) + TOKEN_TTL,
        "v": 2,
    })
    payload = json.dumps(claims).encode()
    sig = hmac.new(TOKEN_SECRET.encode(), payload, hashlib.sha256).hexdigest()[:32]
    return base64.urlsafe_b64encode(payload).decode() + "." + sig


def _decode_token(token: str) -> Optional[dict]:
    """Verify signature + expiry and return the token claims, else None.

    Accepts both the legacy 16-hex MAC and the unified-v2 32-hex MAC, and
    honours standard `exp` when present (legacy `t + TOKEN_TTL` otherwise).
    """
    try:
        payload_b64, sig = token.rsplit(".", 1)
        if len(sig) not in (16, 32):
            return None
        payload = base64.urlsafe_b64decode(payload_b64)
        expected_sig = hmac.new(TOKEN_SECRET.encode(), payload, hashlib.sha256).hexdigest()[: len(sig)]
        if not hmac.compare_digest(sig, expected_sig):
            return None
        data = json.loads(payload)
        exp = data.get("exp")
        issued = data.get("t", 0)
        if exp is not None and float(exp) <= time.time():
            return None
        if issued + TOKEN_TTL <= time.time():
            return None
        return data
    except Exception:
        return None


def get_token_identity(token: str) -> Optional[dict]:
    """Full identity for a bearer token: {client_id, user_id?, email?, scope}.

    Returns None for invalid/revoked tokens. Static/operator tokens yield
    {client_id: 'operator'} with no user_id.
    """
    if not token:
        return None
    if DEFAULT_BEARER_TOKEN and hmac.compare_digest(token, DEFAULT_BEARER_TOKEN):
        return {"client_id": "operator", "scope": "mcp tools"}

    # Canonical human identity: validate RS256, issuer, expiry and JWKS before
    # exposing any claim. Customer/account selection is enforced separately by
    # the request boundary because one principal may belong to many accounts.
    if token.count(".") == 2:
        try:
            from .auth.unified import verify_token as verify_unified_token

            canonical = verify_unified_token(token)
        except Exception:
            canonical = None
        if canonical and canonical.token_format == "zitadel_jwt":
            account_id = canonical.selected_account_id
            if not account_id and len(canonical.account_ids) == 1:
                account_id = next(iter(canonical.account_ids))
            return {
                "client_id": canonical.client_id,
                "user_id": canonical.subject,
                "principal_id": canonical.subject,
                "email": canonical.email,
                "email_verified": canonical.email_verified,
                "scope": " ".join(sorted(canonical.scopes)),
                "issuer": canonical.issuer,
                "token_format": canonical.token_format,
                "account_id": account_id,
                "customer_id": account_id,
                "account_ids": sorted(canonical.account_ids),
                "roles": sorted(canonical.roles),
                "is_platform_admin": canonical.is_platform_admin,
            }
    data = _decode_token(token)
    if not data:
        return None
    try:
        if _oauth_store().is_revoked(token):
            return None
        # User-bound tokens die when the user revokes the app's grant.
        if data.get("sub") and data.get("c") not in ("password-client", "operator"):
            if not _oauth_store().has_grant(data["sub"], data["c"]):
                return None
    except Exception as e:
        logger.debug(f"OAuth store check skipped: {e}")
    return {
        "client_id": data.get("c"),
        "user_id": data.get("sub"),
        "email": data.get("em"),
        "scope": data.get("s", "mcp"),
        # unified-v2 additive fields (absent on legacy tokens; callers using
        # only the legacy keys are unaffected).
        **({k: data[k] for k in ("aud", "iss", "exp") if k in data}),
    }


def validate_token(token: str) -> bool:
    """Check if a bearer token is valid."""
    return get_token_identity(token) is not None


def validate_server_password(password: str) -> bool:
    """Accept the configured server password, or the static bearer token if set."""
    if not password:
        return False
    if hmac.compare_digest(password, SERVER_PASSWORD):
        return True
    if DEFAULT_BEARER_TOKEN and hmac.compare_digest(password, DEFAULT_BEARER_TOKEN):
        return True
    return False


def _jwt_hs256(claims: dict, secret: str) -> str:
    """Minimal HS256 JWT for OIDC id_tokens."""
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url(json.dumps(claims).encode())
    signing_input = f"{header}.{body}".encode()
    sig = _b64url(hmac.new(secret.encode(), signing_input, hashlib.sha256).digest())
    return f"{header}.{body}.{sig}"


# ── Auth helpers for MCP routes ────────────────────────────────────────────

def require_bearer(request: Request) -> Optional[str]:
    """
    Check Authorization header for a valid Bearer token.

    Returns the token string if valid, or None if missing/invalid.
    MCP routes use this to decide whether to return 401.
    """
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:]
        if validate_token(token):
            return token
        # Per-user API keys issued at signup are first-class bearer tokens.
        if token.startswith("sk-user-"):
            try:
                if _user_store().get_by_api_key(token):
                    return token
            except Exception:
                pass
    return None


def make_401_response(request: Request) -> JSONResponse:
    """Build a 401 response with WWW-Authenticate pointing to resource metadata."""
    base = _base_url(request)
    resource_metadata_url = f"{base}/.well-known/oauth-protected-resource"
    return JSONResponse(
        status_code=401,
        content={"error": "unauthorized", "error_description": "Bearer token required"},
        headers={
            "WWW-Authenticate": f'Bearer resource_metadata="{resource_metadata_url}", scope="mcp"',
        },
    )


# ── Discovery ───────────────────────────────────────────────────────────────

@router.get("/.well-known/oauth-protected-resource")
async def protected_resource_metadata(request: Request):
    """RFC 9728 — Protected Resource Metadata. Points clients to the authorization server."""
    base = _base_url(request)
    return {
        "resource": base,
        "authorization_servers": [base],
        "scopes_supported": SUPPORTED_SCOPES,
        "bearer_methods_supported": ["header"],
    }


def _as_metadata(base: str) -> dict:
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "userinfo_endpoint": f"{base}/oauth/userinfo",
        "revocation_endpoint": f"{base}/oauth/revoke",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
        "scopes_supported": SUPPORTED_SCOPES,
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["HS256"],
        "claims_supported": ["sub", "email", "name", "iss", "aud", "exp", "iat"],
    }


@router.get("/.well-known/oauth-authorization-server")
async def oauth_metadata(request: Request):
    return _as_metadata(_base_url(request))


@router.get("/.well-known/openid-configuration")
async def openid_configuration(request: Request):
    """OIDC discovery — lets any property add 'Sign in with Distributed' with a standard OIDC client."""
    return _as_metadata(_base_url(request))


# ── Dynamic Client Registration (RFC 7591) ──────────────────────────────────

@router.post("/oauth/register")
async def register_client(request: Request):
    body = await request.json()
    client_id = secrets.token_hex(16)
    auth_method = str(body.get("token_endpoint_auth_method") or "client_secret_post")
    if auth_method not in {"none", "client_secret_post"}:
        return JSONResponse(
            {"error": "invalid_client_metadata", "error_description": "unsupported token_endpoint_auth_method"},
            status_code=400,
        )
    client_secret = None if auth_method == "none" else secrets.token_hex(32)
    redirect_uris = body.get("redirect_uris", [])
    client_name = body.get("client_name", "MCP Client")

    _oauth_store().save_client(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "name": client_name,
            "redirect_uris": redirect_uris,
            "logo_uri": body.get("logo_uri"),
            "client_uri": body.get("client_uri"),
            "created_at": time.time(),
            "token_endpoint_auth_method": auth_method,
        }
    )
    logger.info(f"Registered OAuth client: {client_name} ({client_id})")

    registration = {
        "client_id": client_id,
        "client_id_issued_at": int(time.time()),
        "redirect_uris": redirect_uris,
        "client_name": client_name,
        "token_endpoint_auth_method": auth_method,
    }
    if client_secret:
        registration["client_secret"] = client_secret
    return registration


# ── Authorization Endpoint ──────────────────────────────────────────────────

_SCOPE_DESCRIPTIONS = {
    "openid": "Confirm your identity",
    "profile": "See your name",
    "email": "See your email address",
    "mcp": "Connect as an MCP client",
    "tools": "Execute tools with your connected integrations",
}


@router.get("/oauth/authorize")
async def authorize_form(
    request: Request,
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    response_type: str = Query("code"),
    state: str = Query(""),
    code_challenge: str = Query(""),
    code_challenge_method: str = Query("S256"),
    scope: str = Query("mcp"),
    mode: str = Query("login"),
):
    """Sign in with Distributed — login + consent in one step."""
    base = _base_url(request)
    client = _oauth_store().get_client(client_id)
    if not client:
        raise HTTPException(status_code=400, detail="Unknown OAuth client")
    if redirect_uri not in set(client.get("redirect_uris") or []):
        raise HTTPException(status_code=400, detail="redirect_uri is not registered")
    if response_type != "code":
        raise HTTPException(status_code=400, detail="Only response_type=code is supported")
    if client.get("token_endpoint_auth_method") == "none":
        if not code_challenge or code_challenge_method != "S256":
            raise HTTPException(status_code=400, detail="Public clients require PKCE S256")
    app_name = html.escape((client or {}).get("name") or client_id[:16])
    scope_items = "".join(
        f"<li>{html.escape(_SCOPE_DESCRIPTIONS.get(s, s))}</li>"
        for s in scope.split()
    ) or "<li>Connect to the API</li>"

    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in with Distributed</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #0a0a0a; color: #e0e0e0;
         display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
  .card {{ background: #1a1a1a; border: 1px solid #333; border-radius: 12px; padding: 2rem;
           width: 100%; max-width: 380px; box-shadow: 0 4px 24px rgba(0,0,0,0.5); }}
  h2 {{ margin: 0 0 0.25rem; font-size: 1.25rem; }}
  .app {{ color: #5b8def; }}
  p {{ color: #888; font-size: 0.875rem; margin: 0 0 1rem; }}
  ul {{ color: #aaa; font-size: 0.85rem; margin: 0 0 1.25rem; padding-left: 1.2rem; }}
  input {{ width: 100%; padding: 0.75rem; border: 1px solid #333; border-radius: 8px;
           background: #111; color: #fff; font-size: 1rem; box-sizing: border-box; margin-bottom: 0.9rem; }}
  input:focus {{ outline: none; border-color: #5b8def; }}
  button {{ width: 100%; padding: 0.75rem; border: none; border-radius: 8px; background: #5b8def;
            color: #fff; font-size: 1rem; cursor: pointer; font-weight: 600; }}
  button:hover {{ background: #4a7de0; }}
  .foot {{ margin-top: 1rem; font-size: 0.8rem; color: #666; text-align: center; }}
  .foot a {{ color: #5b8def; text-decoration: none; }}
</style></head><body>
<div class="card">
  <h2>Sign in with <span class="app">Distributed</span></h2>
  <p><strong>{app_name}</strong> is requesting access to:</p>
  <ul>{scope_items}</ul>
  <form method="POST" action="{base}/oauth/authorize">
    <input type="hidden" name="client_id" value="{html.escape(client_id)}">
    <input type="hidden" name="redirect_uri" value="{html.escape(redirect_uri)}">
    <input type="hidden" name="state" value="{html.escape(state)}">
    <input type="hidden" name="code_challenge" value="{html.escape(code_challenge)}">
    <input type="hidden" name="code_challenge_method" value="{html.escape(code_challenge_method)}">
    <input type="hidden" name="scope" value="{html.escape(scope)}">
    <input type="hidden" id="mode" name="mode" value="{html.escape(mode)}">
    <input type="text" id="display_name" name="display_name" placeholder="Display name" style="display:none">
    <input type="email" name="email" placeholder="Email (leave blank for operator password)" autofocus>
    <input type="password" name="password" placeholder="Password" required>
    <button id="submit" type="submit">Sign in &amp; approve</button>
  </form>
  <div class="foot"><a href="{base}/account" target="_blank">Account</a> ·
  <a href="#" onclick="setMode('signup');return false">Create account</a> ·
  Your credentials never reach {app_name}.</div>
</div>
<script>
function setMode(mode) {{
  document.getElementById('mode').value = mode;
  document.getElementById('display_name').style.display = mode === 'signup' ? 'block' : 'none';
  document.getElementById('submit').textContent = mode === 'signup' ? 'Join Distributed & continue' : 'Sign in & approve';
}}
setMode({json.dumps('signup' if mode == 'signup' else 'login')});
</script>
</body></html>""")


@router.post("/oauth/authorize")
async def authorize_submit(
    client_id: str = Form(...),
    redirect_uri: str = Form(...),
    password: str = Form(...),
    email: str = Form(""),
    state: str = Form(""),
    code_challenge: str = Form(""),
    code_challenge_method: str = Form("S256"),
    scope: str = Form("mcp"),
    mode: str = Form("login"),
    display_name: str = Form(""),
):
    """Authenticate (user account or operator password), record consent, issue code."""
    user: Optional[dict] = None
    email = (email or "").strip()

    client = _oauth_store().get_client(client_id)
    if not client:
        raise HTTPException(status_code=400, detail="Unknown OAuth client")
    if redirect_uri not in set(client.get("redirect_uris") or []):
        raise HTTPException(status_code=400, detail="redirect_uri is not registered")
    if client.get("token_endpoint_auth_method") == "none":
        if not code_challenge or code_challenge_method != "S256":
            raise HTTPException(status_code=400, detail="Public clients require PKCE S256")

    if mode == "signup":
        try:
            user = _user_store().create_user(email, password, display_name=display_name or None)
            authorized = bool(user)
        except ValueError:
            authorized = False
    elif email:
        user = _user_store().authenticate(email, password)
        if not user:
            authorized = False
        else:
            authorized = True
    else:
        authorized = validate_server_password(password)

    if not authorized:
        return HTMLResponse(
            "<html><body style='font-family:system-ui;background:#0a0a0a;color:#e0e0e0;"
            "display:flex;justify-content:center;align-items:center;height:100vh;margin:0'>"
            "<div style='text-align:center'><h2>Sign-in failed</h2>"
            "<p>Wrong email/password or operator password.</p>"
            "<p><a href='javascript:history.back()' style='color:#5b8def'>Try again</a></p>"
            "</div></body></html>",
            status_code=401,
        )

    code = secrets.token_urlsafe(32)
    _auth_codes[code] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "scope": scope,
        "user": (
            {"user_id": user["user_id"], "email": user["email"],
             "name": user.get("display_name")}
            if user else None
        ),
        "expires": time.time() + 300,
    }

    if user:
        try:
            _oauth_store().record_grant(user["user_id"], client_id, scope)
        except Exception as e:
            logger.warning(f"Failed to record grant: {e}")

    params = {"code": code}
    if state:
        params["state"] = state

    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        url=f"{redirect_uri}{sep}{urlencode(params)}",
        status_code=302,
    )


# ── Token Endpoint ──────────────────────────────────────────────────────────

async def _parse_body(request: Request) -> dict:
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        return await request.json()
    form = await request.form()
    return dict(form)


def _authenticate_token_client(body: dict) -> tuple[Optional[dict], Optional[JSONResponse]]:
    client_id = str(body.get("client_id") or "")
    client = _oauth_store().get_client(client_id) if client_id else None
    if not client:
        return None, JSONResponse({"error": "invalid_client"}, status_code=401)
    method = client.get("token_endpoint_auth_method") or (
        "client_secret_post" if client.get("client_secret") else "none"
    )
    if method == "client_secret_post":
        supplied = str(body.get("client_secret") or "")
        expected = str(client.get("client_secret") or "")
        if not supplied or not expected or not hmac.compare_digest(supplied, expected):
            return None, JSONResponse({"error": "invalid_client"}, status_code=401)
    elif method != "none":
        return None, JSONResponse({"error": "invalid_client"}, status_code=401)
    return client, None


def _token_response(base: str, client_id: str, user: Optional[dict], scope: str) -> dict:
    access_token = _make_token(client_id, user=user, scope=scope)
    out = {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": TOKEN_TTL,
        "scope": scope,
    }

    refresh = "rt_" + secrets.token_urlsafe(32)
    _oauth_store().save_refresh_token(
        refresh, client_id, (user or {}).get("user_id"), scope, REFRESH_TOKEN_TTL
    )
    out["refresh_token"] = refresh

    if user and "openid" in scope.split():
        client = _oauth_store().get_client(client_id)
        signing_secret = (client or {}).get("client_secret") or TOKEN_SECRET
        now = int(time.time())
        out["id_token"] = _jwt_hs256(
            {
                "iss": base,
                "aud": client_id,
                "sub": user["user_id"],
                "email": user.get("email"),
                "name": user.get("name"),
                "iat": now,
                "exp": now + TOKEN_TTL,
            },
            signing_secret,
        )
    return out


@router.post("/oauth/token")
async def token_exchange(request: Request):
    """Exchange an authorization code or refresh token for tokens."""
    body = await _parse_body(request)
    grant_type = body.get("grant_type")

    if grant_type == "refresh_token":
        client, client_error = _authenticate_token_client(body)
        if client_error:
            return client_error
        record = _oauth_store().consume_refresh_token(
            body.get("refresh_token", ""), expected_client_id=client["client_id"]
        )
        if not record:
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "Invalid or expired refresh token"},
                status_code=400,
            )
        user = None
        if record["user_id"]:
            u = _user_store().get_by_id(record["user_id"])
            if u:
                user = {"user_id": u["user_id"], "email": u["email"],
                        "name": u.get("display_name")}
        return _token_response(
            _base_url(request), record["client_id"], user, record["scope"]
        )

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    code = body.get("code")
    client_id = body.get("client_id")
    code_verifier = body.get("code_verifier", "")

    if not code or code not in _auth_codes:
        return JSONResponse({"error": "invalid_grant", "error_description": "Invalid or expired code"}, status_code=400)

    auth_info = _auth_codes[code]

    if not client_id or client_id != auth_info.get("client_id"):
        return JSONResponse({"error": "invalid_grant", "error_description": "Authorization code client mismatch"}, status_code=400)
    client, client_error = _authenticate_token_client(body)
    if client_error:
        return client_error
    redirect_uri = body.get("redirect_uri")
    if redirect_uri and redirect_uri != auth_info.get("redirect_uri"):
        return JSONResponse({"error": "invalid_grant", "error_description": "Authorization code redirect mismatch"}, status_code=400)

    if auth_info["expires"] < time.time():
        _auth_codes.pop(code, None)
        return JSONResponse({"error": "invalid_grant", "error_description": "Code expired"}, status_code=400)

    # Verify PKCE
    if auth_info["code_challenge"]:
        if auth_info["code_challenge_method"] == "S256":
            digest = hashlib.sha256(code_verifier.encode()).digest()
            computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        else:
            computed = code_verifier

        if not hmac.compare_digest(computed, auth_info["code_challenge"]):
            return JSONResponse({"error": "invalid_grant", "error_description": "PKCE verification failed"}, status_code=400)

    _auth_codes.pop(code, None)

    return _token_response(
        _base_url(request),
        client["client_id"],
        auth_info.get("user"),
        auth_info.get("scope", "mcp"),
    )


@router.post("/oauth/token/password")
async def token_from_password(request: Request):
    """
    Exchange server password for a bearer token (non-standard helper endpoint).

    Useful for smoke tests and service-to-service bootstrapping without full OAuth redirects.
    """
    body = await _parse_body(request)
    password = (body.get("password") or "").strip()
    client_id = (body.get("client_id") or "password-client").strip() or "password-client"

    if not validate_server_password(password):
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid password"},
            status_code=401,
        )

    access_token = _make_token(client_id)
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": TOKEN_TTL,
        "scope": "mcp",
    }


# ── OIDC userinfo + revocation ─────────────────────────────────────────────

@router.get("/oauth/userinfo")
async def userinfo(request: Request):
    """OIDC UserInfo — identity claims for the presented access token."""
    auth_header = request.headers.get("authorization", "")
    token = auth_header[7:] if auth_header.lower().startswith("bearer ") else ""
    identity = get_token_identity(token)

    # sk-user API keys are also accepted so first-party apps can share code paths.
    if not identity and token.startswith("sk-user-"):
        u = _user_store().get_by_api_key(token)
        if u:
            identity = {"user_id": u["user_id"], "email": u["email"],
                        "client_id": "api-key", "scope": "openid profile email tools"}

    if not identity or not identity.get("user_id"):
        raise HTTPException(status_code=401, detail="A user-bound bearer token is required")

    u = _user_store().get_by_id(identity["user_id"])
    if not u:
        raise HTTPException(status_code=401, detail="Unknown user")
    return {
        "sub": u["user_id"],
        "email": u["email"],
        "email_verified": False,
        "name": u.get("display_name"),
    }


@router.post("/oauth/revoke")
async def revoke(request: Request):
    """RFC 7009 — revoke an access or refresh token. Always returns 200."""
    body = await _parse_body(request)
    token = body.get("token", "")
    if token:
        try:
            _oauth_store().revoke_token(token)
        except Exception as e:
            logger.warning(f"Revocation store write failed: {e}")
    return JSONResponse({"revoked": True})


@router.post("/oauth/introspect")
async def introspect(request: Request):
    """RFC 7662-style token introspection (AUTH_UNIFIED.md T04).

    Backed by the durable DuckDB revocation list — no signature check here by
    design: every subdomain service calls this with an opaque token it already
    signature-verified locally (see tool_management.auth.unified), and we must
    not fail an introspection because of local-secret drift.

    Never logs or echoes the presented token.
    """
    body = await _parse_body(request)
    token = body.get("token", "")
    if not token:
        return JSONResponse({"active": False})
    try:
        revoked = _oauth_store().is_revoked(token)
    except Exception as e:
        logger.warning(f"Introspection store read failed: {e}")
        revoked = False  # fail open, like unified.py's introspection path
    if revoked:
        return JSONResponse({"active": False, "revoked": True})
    identity = _decode_token(token)
    if not identity:
        # Signature/expiry validity is the caller's job, but stale/expired
        # tokens should not be reported as active.
        return JSONResponse({"active": False, "stale_or_invalid": True})
    return JSONResponse({
        "active": True,
        "revoked": False,
        "scope": identity.get("s", "mcp"),
        "client_id": identity.get("c"),
        **({"sub": identity["sub"]} if identity.get("sub") else {}),
        **({"aud": identity["aud"]} if identity.get("aud") else {}),
    })
