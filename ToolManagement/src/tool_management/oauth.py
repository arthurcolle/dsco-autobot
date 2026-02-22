"""
OAuth 2.0 for MCP remote server authentication (RFC 9728 + RFC 8414).

Implements the full flow Claude iOS/Desktop needs to connect:
- /.well-known/oauth-protected-resource   (RFC 9728 resource metadata)
- /.well-known/oauth-authorization-server  (RFC 8414 AS discovery)
- /oauth/register  (RFC 7591 dynamic client registration)
- /oauth/authorize  (authorization code + PKCE)
- /oauth/token  (code → access token exchange)
"""

import hashlib
import hmac
import base64
import json
import time
import os
import secrets
import logging
from typing import Dict, Optional
from urllib.parse import urlencode, parse_qs, urlparse

from fastapi import APIRouter, Request, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["auth"])

# ── Config ──────────────────────────────────────────────────────────────────
SERVER_PASSWORD = os.getenv("MCP_PASSWORD", "password2026!")
# Keep token signatures stable across container restarts when TOKEN_SECRET is not set.
TOKEN_SECRET = os.getenv("TOKEN_SECRET") or hashlib.sha256(
    SERVER_PASSWORD.encode("utf-8")
).hexdigest()
TOKEN_TTL = 86400 * 30  # 30 days
DEFAULT_BEARER_TOKEN = os.getenv("DEFAULT_BEARER_TOKEN", "")

# In-memory stores (fine for single-instance Modal deployment)
_auth_codes: Dict[str, dict] = {}   # code → {client_id, redirect_uri, code_challenge, expires}
_clients: Dict[str, dict] = {}      # client_id → {client_secret, redirect_uris, name}
_tokens: Dict[str, dict] = {}       # access_token → {client_id, expires}


def _base_url(request: Request) -> str:
    """Derive the public base URL from the request."""
    # Prefer explicit config, fall back to request headers
    configured = os.getenv("API_BASE_URL")
    if configured:
        return configured.rstrip("/")
    proto = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("host", "localhost")
    return f"{proto}://{host}"


def _make_token(client_id: str) -> str:
    """Create a signed access token."""
    payload = json.dumps({"c": client_id, "t": time.time()}).encode()
    sig = hmac.new(TOKEN_SECRET.encode(), payload, hashlib.sha256).hexdigest()[:16]
    token = base64.urlsafe_b64encode(payload).decode() + "." + sig
    _tokens[token] = {"client_id": client_id, "expires": time.time() + TOKEN_TTL}
    return token


def validate_token(token: str) -> bool:
    """Check if a bearer token is valid."""
    if not token:
        return False

    # Optional static token for smoke tests / probes.
    if DEFAULT_BEARER_TOKEN and hmac.compare_digest(token, DEFAULT_BEARER_TOKEN):
        return True

    info = _tokens.get(token)
    if info and info["expires"] > time.time():
        return True
    # Also verify signature for tokens we may have lost from memory (restart)
    try:
        payload_b64, sig = token.rsplit(".", 1)
        payload = base64.urlsafe_b64decode(payload_b64)
        expected_sig = hmac.new(TOKEN_SECRET.encode(), payload, hashlib.sha256).hexdigest()[:16]
        if hmac.compare_digest(sig, expected_sig):
            data = json.loads(payload)
            if data.get("t", 0) + TOKEN_TTL > time.time():
                _tokens[token] = {"client_id": data["c"], "expires": data["t"] + TOKEN_TTL}
                return True
    except Exception:
        pass
    return False


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
        "scopes_supported": ["mcp"],
        "bearer_methods_supported": ["header"],
    }


@router.get("/.well-known/oauth-authorization-server")
async def oauth_metadata(request: Request):
    base = _base_url(request)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
        "scopes_supported": ["mcp"],
    }


# ── Dynamic Client Registration (RFC 7591) ──────────────────────────────────

@router.post("/oauth/register")
async def register_client(request: Request):
    body = await request.json()
    client_id = secrets.token_hex(16)
    client_secret = secrets.token_hex(32)
    redirect_uris = body.get("redirect_uris", [])
    client_name = body.get("client_name", "MCP Client")

    _clients[client_id] = {
        "client_secret": client_secret,
        "redirect_uris": redirect_uris,
        "name": client_name,
    }
    logger.info(f"Registered OAuth client: {client_name} ({client_id})")

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uris": redirect_uris,
        "client_name": client_name,
        "token_endpoint_auth_method": "client_secret_post",
    }


# ── Authorization Endpoint ──────────────────────────────────────────────────

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
):
    """Show a simple password form."""
    base = _base_url(request)
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Tool Management API — Sign In</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #0a0a0a; color: #e0e0e0;
         display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
  .card {{ background: #1a1a1a; border: 1px solid #333; border-radius: 12px; padding: 2rem;
           width: 100%; max-width: 360px; box-shadow: 0 4px 24px rgba(0,0,0,0.5); }}
  h2 {{ margin: 0 0 0.5rem; font-size: 1.25rem; }}
  p {{ color: #888; font-size: 0.875rem; margin: 0 0 1.5rem; }}
  input {{ width: 100%; padding: 0.75rem; border: 1px solid #333; border-radius: 8px;
           background: #111; color: #fff; font-size: 1rem; box-sizing: border-box; margin-bottom: 1rem; }}
  input:focus {{ outline: none; border-color: #5b8def; }}
  button {{ width: 100%; padding: 0.75rem; border: none; border-radius: 8px; background: #5b8def;
            color: #fff; font-size: 1rem; cursor: pointer; font-weight: 600; }}
  button:hover {{ background: #4a7de0; }}
  .error {{ color: #ef4444; font-size: 0.875rem; display: none; margin-bottom: 0.5rem; }}
</style></head><body>
<div class="card">
  <h2>Tool Management API</h2>
  <p>Enter the server password to connect.</p>
  <form method="POST" action="{base}/oauth/authorize">
    <input type="hidden" name="client_id" value="{client_id}">
    <input type="hidden" name="redirect_uri" value="{redirect_uri}">
    <input type="hidden" name="state" value="{state}">
    <input type="hidden" name="code_challenge" value="{code_challenge}">
    <input type="hidden" name="code_challenge_method" value="{code_challenge_method}">
    <input type="hidden" name="scope" value="{scope}">
    <input type="password" name="password" placeholder="Password" autofocus required>
    <button type="submit">Connect</button>
  </form>
</div></body></html>""")


@router.post("/oauth/authorize")
async def authorize_submit(
    client_id: str = Form(...),
    redirect_uri: str = Form(...),
    password: str = Form(...),
    state: str = Form(""),
    code_challenge: str = Form(""),
    code_challenge_method: str = Form("S256"),
    scope: str = Form("mcp"),
):
    """Validate password, issue auth code, redirect back."""
    if not hmac.compare_digest(password, SERVER_PASSWORD):
        return HTMLResponse(
            "<html><body style='font-family:system-ui;background:#0a0a0a;color:#e0e0e0;"
            "display:flex;justify-content:center;align-items:center;height:100vh;margin:0'>"
            "<div style='text-align:center'><h2>Wrong password</h2>"
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
        "expires": time.time() + 300,
    }

    params = {"code": code}
    if state:
        params["state"] = state

    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        url=f"{redirect_uri}{sep}{urlencode(params)}",
        status_code=302,
    )


# ── Token Endpoint ──────────────────────────────────────────────────────────

@router.post("/oauth/token")
async def token_exchange(request: Request):
    """Exchange authorization code for access token."""
    # Accept both form-encoded and JSON
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        body = await request.json()
    else:
        form = await request.form()
        body = dict(form)

    grant_type = body.get("grant_type")
    code = body.get("code")
    client_id = body.get("client_id")
    code_verifier = body.get("code_verifier", "")

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    if not code or code not in _auth_codes:
        return JSONResponse({"error": "invalid_grant", "error_description": "Invalid or expired code"}, status_code=400)

    auth_info = _auth_codes.pop(code)

    if auth_info["expires"] < time.time():
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

    access_token = _make_token(client_id or auth_info["client_id"])

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": TOKEN_TTL,
        "scope": "mcp",
    }
