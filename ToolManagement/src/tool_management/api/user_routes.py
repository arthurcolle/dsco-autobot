"""
User accounts + per-user integration connections.

Signup/login issue per-user API keys accepted by the global bearer gate.
Connections associate a user's own credentials (API keys or OAuth tokens)
with integration providers; tool execution resolves them per user and
returns auth_required hints (with a hosted /connect popup URL) when a
provider isn't connected yet.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ..integrations.provider_catalog import PROVIDERS, provider_for_tool
from ..persistence.user_store import get_user_store
from ..services.tenant_credentials import _default_store as get_tenant_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["users"])
connect_page_router = APIRouter(tags=["users"])

_ = provider_for_tool  # re-exported for the execution path


def public_base_url() -> str:
    return os.environ.get("API_BASE_URL", "https://tools.distributed.systems").rstrip("/")


def connect_url_for(provider: str) -> str:
    return f"{public_base_url()}/connect?provider={provider}"


def resolve_user_from_request(request: Request) -> Optional[Dict[str, Any]]:
    """Return the user behind the bearer token — sk-user-* API keys and
    user-bound "Sign in with Distributed" OAuth tokens both resolve."""
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:]
    if token.startswith("sk-user-"):
        return get_user_store().get_by_api_key(token)
    try:
        from ..oauth import get_token_identity

        identity = get_token_identity(token)
        if identity and identity.get("user_id"):
            return get_user_store().get_by_id(identity["user_id"])
    except Exception:
        pass
    return None


def _require_user(request: Request) -> Dict[str, Any]:
    user = resolve_user_from_request(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="A user API key (sk-user-...) is required. Sign up at POST /api/v1/users/signup.",
        )
    return user


# ==================== Accounts ====================

class SignupRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    display_name: Optional[str] = None


class LoginRequest(BaseModel):
    email: str
    password: str


@router.post("/users/signup", status_code=201)
async def signup(body: SignupRequest):
    """Create an account. Returns the user's API key — store it; it is the bearer token."""
    try:
        user = get_user_store().create_user(body.email, body.password, body.display_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        **user,
        "usage": "Send 'Authorization: Bearer <api_key>' on every request. "
                 "Connect integrations at POST /api/v1/me/connections or the /connect page.",
    }


@router.post("/users/login")
async def login(body: LoginRequest):
    user = get_user_store().authenticate(body.email, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return user


@router.get("/me")
async def me(request: Request):
    user = _require_user(request)
    connected = get_tenant_store().list_providers(user["user_id"])
    return {**user, "api_key": None, "connections": connected}


@router.post("/me/rotate-key")
async def rotate_key(request: Request):
    user = _require_user(request)
    new_key = get_user_store().rotate_api_key(user["user_id"])
    return {"user_id": user["user_id"], "api_key": new_key}


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8)


@router.post("/me/change-password")
async def change_password(request: Request, body: ChangePasswordRequest):
    user = _require_user(request)
    try:
        ok = get_user_store().change_password(
            user["user_id"], body.current_password, body.new_password
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not ok:
        raise HTTPException(status_code=401, detail="Current password is incorrect")
    return {"changed": True}


@router.get("/admin/users")
async def admin_list_users(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Operator-only user listing — user API keys and OAuth user tokens are rejected."""
    auth_header = request.headers.get("authorization", "")
    token = auth_header[7:] if auth_header.lower().startswith("bearer ") else ""
    from ..oauth import get_token_identity

    identity = get_token_identity(token)
    is_operator = bool(identity) and not identity.get("user_id") and identity.get(
        "client_id"
    ) in ("operator", "password-client")
    if not is_operator:
        raise HTTPException(status_code=403, detail="Operator token required")

    store = get_user_store()
    return {
        "total": store.count(),
        "users": store.list_users(limit=limit, offset=offset),
    }


# ==================== Authorized apps (OAuth grants) ====================

@router.get("/me/apps")
async def list_authorized_apps(request: Request):
    """Apps the user has approved via Sign in with Distributed."""
    user = _require_user(request)
    from ..persistence.oauth_store import get_oauth_store

    return {
        "user_id": user["user_id"],
        "apps": get_oauth_store().list_grants(user["user_id"]),
    }


@router.delete("/me/apps/{client_id}")
async def revoke_authorized_app(request: Request, client_id: str):
    """Revoke an app's access: kills its grant, refresh tokens, and any
    outstanding user-bound access tokens for that app."""
    user = _require_user(request)
    from ..persistence.oauth_store import get_oauth_store

    removed = get_oauth_store().revoke_grant(user["user_id"], client_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"No grant for app {client_id}")
    return {"revoked": True, "client_id": client_id}


# ==================== Provider catalog ====================

@router.get("/connect/providers")
async def list_connectable_providers(
    request: Request,
    q: Optional[str] = Query(None, description="Filter by name/category substring"),
    limit: int = Query(200, ge=1, le=500),
):
    """All integration providers a user can connect credentials for."""
    user = resolve_user_from_request(request)
    connected = set(
        get_tenant_store().list_providers(user["user_id"]) if user else []
    )
    addons = set()
    if user:
        try:
            from ..persistence.billing_store import get_billing_store

            addons = get_billing_store().active_addon_providers(user["user_id"])
        except Exception:
            pass

    items: List[Dict[str, Any]] = []
    for name, meta in sorted(PROVIDERS.items()):
        if q:
            needle = q.lower()
            if (
                needle not in name.lower()
                and needle not in meta["display_name"].lower()
                and not any(needle in c for c in meta["categories"])
            ):
                continue
        items.append(
            {
                "provider": name,
                "display_name": meta["display_name"],
                "logo_url": meta["logo_url"],
                "categories": meta["categories"],
                "auth_methods": (["oauth"] if meta["oauth"] else []) + ["api_key", "addon"],
                "live_capable": meta["live_capable"],
                "connected": name in connected,
                "addon_active": name in addons,
                "addon_monthly_cents": meta["addon_monthly_cents"],
                "connect_url": connect_url_for(name),
            }
        )
        if len(items) >= limit:
            break
    return {"count": len(items), "providers": items}


# ==================== Connections ====================

class ConnectionRequest(BaseModel):
    provider: str
    credential: str = Field(min_length=1, description="API key / token for the provider")
    label: Optional[str] = None


@router.get("/me/connections")
async def list_connections(request: Request):
    user = _require_user(request)
    store = get_tenant_store()
    providers = store.list_providers(user["user_id"])
    out = []
    for p in providers:
        meta = store.get_metadata(user["user_id"], p)
        out.append(
            {
                "provider": p,
                "label": getattr(meta, "label", None) if meta else None,
                "created_at": getattr(meta, "created_at", None) if meta else None,
            }
        )
    return {"user_id": user["user_id"], "count": len(out), "connections": out}


@router.post("/me/connections", status_code=201)
async def add_connection(request: Request, body: ConnectionRequest):
    user = _require_user(request)
    provider = body.provider.strip().lower()
    if provider not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider: {provider}. See /api/v1/connect/providers.",
        )
    ok = get_tenant_store().set_secret(
        user["user_id"], provider, body.credential, label=body.label
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to store credential")
    return {
        "connected": True,
        "provider": provider,
        "user_id": user["user_id"],
        "note": "This credential is now used automatically when you execute this provider's tools.",
    }


@router.delete("/me/connections/{provider}")
async def delete_connection(request: Request, provider: str):
    user = _require_user(request)
    removed = get_tenant_store().delete_secret(user["user_id"], provider.lower())
    if not removed:
        raise HTTPException(status_code=404, detail=f"No connection for {provider}")
    return {"disconnected": True, "provider": provider.lower()}


@router.post("/me/connections/{provider}/oauth/start")
async def start_oauth(request: Request, provider: str):
    """Begin a user-scoped OAuth flow. Open auth_url in a browser/popup; the
    callback stores the token against this user automatically."""
    user = _require_user(request)
    provider = provider.strip().lower()

    from ..integrations.codex_apps_oauth import KNOWN_PROVIDERS, get_codex_apps_oauth
    from .codex_apps_routes import _get_oauth_client_creds

    if provider not in KNOWN_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=f"{provider} does not support OAuth here — connect an API key instead "
                   f"(POST /api/v1/me/connections). OAuth providers: {sorted(KNOWN_PROVIDERS)}",
        )
    client_id, _client_secret = _get_oauth_client_creds(provider)
    if not client_id:
        raise HTTPException(
            status_code=409,
            detail=f"OAuth client credentials not configured for {provider}. "
                   f"An operator must set them via POST /api/v1/admin/codex-oauth/clients/{provider}.",
        )

    oauth = get_codex_apps_oauth()
    auth_url, state, err = oauth.build_provider_authorization_url(
        provider_name=provider, client_id=client_id, user_id=user["user_id"]
    )
    if err:
        raise HTTPException(status_code=500, detail=err)
    return {"auth_url": auth_url, "state": state, "provider": provider}


# ==================== Hosted connect page (popup target) ====================

_CONNECT_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Connect integration — tools.distributed.systems</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, system-ui, sans-serif; max-width: 460px;
         margin: 40px auto; padding: 0 20px; line-height: 1.5; }
  h1 { font-size: 1.2rem; }
  label { display: block; margin-top: 14px; font-weight: 600; font-size: .85rem; }
  input { width: 100%; padding: 9px 10px; margin-top: 4px; border: 1px solid #8884;
          border-radius: 8px; font-size: .95rem; background: transparent; color: inherit; }
  button { margin-top: 18px; width: 100%; padding: 10px; border: 0; border-radius: 8px;
           background: #4a5ae8; color: #fff; font-size: .95rem; cursor: pointer; }
  button.secondary { background: transparent; color: #4a5ae8; border: 1px solid #4a5ae8; }
  #status { margin-top: 14px; font-size: .9rem; white-space: pre-wrap; }
  .ok { color: #2e9e5b; } .err { color: #d64545; }
  small { opacity: .7 }
</style>
</head>
<body>
<h1 id="title"><img id="plogo" style="display:none;width:28px;height:28px;border-radius:7px;
background:#fff;padding:2px;vertical-align:-6px;margin-right:8px">Connect integration</h1>
<p><small>Credentials are encrypted and linked to your account only. They are used
solely when <em>you</em> execute this provider's tools.</small></p>

<label>Provider</label>
<input id="provider" placeholder="e.g. github, openweather, notion">

<label>Your account API key <small>(from signup — sk-user-...)</small></label>
<input id="userkey" placeholder="sk-user-...">

<label>Provider credential / API key</label>
<input id="credential" placeholder="paste the provider API key or token">

<button onclick="saveKey()">Save credential</button>
<button class="secondary" onclick="startOAuth()">Connect with OAuth instead</button>
<div id="status"></div>

<script>
const qs = new URLSearchParams(location.search);
const providerInput = document.getElementById('provider');
const userkeyInput = document.getElementById('userkey');
const statusEl = document.getElementById('status');
if (qs.get('provider')) {
  const prov = qs.get('provider');
  providerInput.value = prov;
  fetch('/api/v1/connect/providers?q=' + encodeURIComponent(prov) + '&limit=50')
    .then(r => r.json()).then(d => {
      const hit = (d.providers || []).find(p => p.provider === prov);
      if (!hit) return;
      document.getElementById('title').lastChild.textContent = 'Connect ' + hit.display_name;
      const img = document.getElementById('plogo');
      img.src = hit.logo_url; img.style.display = 'inline';
    }).catch(() => {
      document.getElementById('title').lastChild.textContent = 'Connect ' + prov;
    });
}
userkeyInput.value = localStorage.getItem('dsco_user_key') || '';

function setStatus(msg, ok) {
  statusEl.textContent = msg;
  statusEl.className = ok ? 'ok' : 'err';
}
function headers() {
  localStorage.setItem('dsco_user_key', userkeyInput.value.trim());
  return {'content-type': 'application/json',
          'authorization': 'Bearer ' + userkeyInput.value.trim()};
}
async function saveKey() {
  const provider = providerInput.value.trim().toLowerCase();
  const credential = document.getElementById('credential').value.trim();
  if (!provider || !credential || !userkeyInput.value.trim()) {
    return setStatus('Provider, account key, and credential are all required.', false);
  }
  const r = await fetch('/api/v1/me/connections', {
    method: 'POST', headers: headers(),
    body: JSON.stringify({provider, credential})
  });
  const d = await r.json();
  if (r.ok) {
    setStatus('Connected ' + provider + '. You can close this window and retry the tool.', true);
    if (window.opener) setTimeout(() => window.close(), 1500);
  } else {
    setStatus(d.detail || JSON.stringify(d), false);
  }
}
async function startOAuth() {
  const provider = providerInput.value.trim().toLowerCase();
  if (!provider || !userkeyInput.value.trim()) {
    return setStatus('Provider and account key are required.', false);
  }
  const r = await fetch('/api/v1/me/connections/' + provider + '/oauth/start',
                        {method: 'POST', headers: headers()});
  const d = await r.json();
  if (r.ok && d.auth_url) {
    setStatus('Opening ' + provider + ' authorization...', true);
    location.href = d.auth_url;
  } else {
    setStatus(d.detail || JSON.stringify(d), false);
  }
}
</script>
</body>
</html>"""


@connect_page_router.get("/connect", response_class=HTMLResponse, include_in_schema=False)
async def connect_page():
    return HTMLResponse(_CONNECT_PAGE)


_ACCOUNT_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Distributed — Account</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0a0a0a; color: #e0e0e0;
         max-width: 680px; margin: 0 auto; padding: 32px 20px; line-height: 1.5; }
  h1 { font-size: 1.3rem; } h1 span { color: #5b8def; }
  h2 { font-size: .95rem; margin: 0 0 10px; color: #aaa; text-transform: uppercase;
       letter-spacing: .05em; }
  .card { background: #161616; border: 1px solid #2a2a2a; border-radius: 12px;
          padding: 18px 20px; margin-bottom: 16px; }
  input, select { width: 100%; padding: 9px 10px; margin: 4px 0 10px; border: 1px solid #333;
          border-radius: 8px; font-size: .95rem; background: #111; color: #fff;
          box-sizing: border-box; }
  button { padding: 8px 14px; border: 0; border-radius: 8px; background: #5b8def;
           color: #fff; font-size: .9rem; cursor: pointer; margin-right: 8px; }
  button.ghost { background: transparent; color: #5b8def; border: 1px solid #35508f; }
  button.danger { background: transparent; color: #d64545; border: 1px solid #6b2a2a; }
  table { width: 100%; border-collapse: collapse; font-size: .9rem; }
  td, th { padding: 6px 4px; text-align: left; border-bottom: 1px solid #222; }
  code { background: #111; padding: 2px 6px; border-radius: 6px; font-size: .85rem; }
  .muted { color: #777; font-size: .85rem; }
  .ok { color: #2e9e5b; } .err { color: #d64545; }
  #status { min-height: 1.2em; font-size: .9rem; margin-bottom: 10px; }
  .row { display: flex; gap: 10px; } .row > * { flex: 1; }
  a { color: #5b8def; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
          gap: 10px; margin-top: 10px; }
  .tile { background: #101010; border: 1px solid #262626; border-radius: 10px;
          padding: 12px; text-align: center; }
  .tile img { width: 36px; height: 36px; border-radius: 8px; background: #fff;
              padding: 3px; object-fit: contain; }
  .tile .pname { font-size: .85rem; font-weight: 600; margin: 6px 0 2px;
                 white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .tile .price { font-size: .75rem; color: #888; margin-bottom: 8px; }
  .tile button { width: 100%; margin: 2px 0 0; padding: 5px; font-size: .75rem; }
  .tile.on { border-color: #2e9e5b55; }
  .chip { display: inline-flex; align-items: center; gap: 6px; background: #101010;
          border: 1px solid #2e9e5b55; border-radius: 20px; padding: 4px 12px 4px 6px;
          margin: 3px 4px 3px 0; font-size: .85rem; }
  .chip img { width: 20px; height: 20px; border-radius: 5px; background: #fff;
              padding: 1px; }
  .chip .x { cursor: pointer; color: #d64545; margin-left: 4px; }
</style>
</head>
<body>
<h1><span>Distributed</span> account</h1>
<div id="status"></div>

<div id="authView">
  <div class="card">
    <h2>Sign in</h2>
    <input id="li_email" type="email" placeholder="Email">
    <input id="li_password" type="password" placeholder="Password">
    <button onclick="login()">Sign in</button>
  </div>
  <div class="card">
    <h2>Create account</h2>
    <input id="su_email" type="email" placeholder="Email">
    <input id="su_name" placeholder="Display name (optional)">
    <input id="su_password" type="password" placeholder="Password (8+ chars)">
    <button onclick="signup()">Sign up</button>
  </div>
</div>

<div id="dashView" style="display:none">
  <div class="card">
    <h2>Profile</h2>
    <div id="profile"></div>
    <div style="margin-top:10px">
      <button class="ghost" onclick="signout()">Sign out</button>
    </div>
  </div>

  <div class="card">
    <h2>API key</h2>
    <div><code id="apikey"></code>
      <button class="ghost" onclick="copyKey()">Copy</button>
      <button class="danger" onclick="rotateKey()">Rotate</button></div>
    <p class="muted">This key is your bearer token for the API and MCP surface.</p>
    <h2 style="margin-top:14px">Change password</h2>
    <div class="row">
      <input id="cp_current" type="password" placeholder="Current password">
      <input id="cp_new" type="password" placeholder="New password">
    </div>
    <button onclick="changePassword()">Change password</button>
  </div>

  <div class="card">
    <h2>Billing</h2>
    <div id="billing"></div>
    <div id="usage" class="muted" style="margin-top:8px"></div>
  </div>

  <div class="card">
    <h2>Resources</h2>
    <table id="resources"></table>
    <div class="row" style="margin-top:10px">
      <select id="r_type"></select>
      <input id="r_name" placeholder="resource name">
    </div>
    <button onclick="createResource()">Provision</button>
  </div>

  <div class="card">
    <h2>Integrations</h2>
    <div id="myIntegrations"></div>
    <input id="p_search" placeholder="Search 172 integrations — google, asana, weather…"
           oninput="renderGallery()" style="margin-top:12px">
    <div id="gallery" class="grid"></div>
    <p class="muted">Bring your own API key (free), or enable as a managed add-on —
    no key needed, billed monthly.</p>
  </div>

  <div class="card">
    <h2>Authorized apps (Sign in with Distributed)</h2>
    <table id="apps"></table>
    <p class="muted">Apps you've approved via OAuth. Revoking kills their tokens immediately.</p>
  </div>
</div>

<script>
// One-time sign-in via URL fragment (#key=sk-user-...) — the fragment never
// reaches the server; it is stored locally and stripped from the URL.
if (location.hash.startsWith('#key=')) {
  localStorage.setItem('dsco_user_key', location.hash.slice(5));
  history.replaceState(null, '', location.pathname);
}
const st = document.getElementById('status');
function setStatus(m, ok) { st.textContent = m; st.className = ok ? 'ok' : 'err';
                            if (ok) setTimeout(() => { st.textContent = ''; }, 4000); }
function key() { return localStorage.getItem('dsco_user_key') || ''; }
function hdrs() { return {'content-type': 'application/json',
                          'authorization': 'Bearer ' + key()}; }
async function api(path, opts) {
  const r = await fetch(path, Object.assign({headers: hdrs()}, opts));
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.detail || JSON.stringify(d));
  return d;
}

async function signup() {
  try {
    const d = await api('/api/v1/users/signup', {method: 'POST', body: JSON.stringify({
      email: v('su_email'), password: v('su_password'), display_name: v('su_name') || null })});
    localStorage.setItem('dsco_user_key', d.api_key);
    setStatus('Account created', true); render();
  } catch (e) { setStatus(e.message, false); }
}
async function login() {
  try {
    const d = await api('/api/v1/users/login', {method: 'POST', body: JSON.stringify({
      email: v('li_email'), password: v('li_password') })});
    localStorage.setItem('dsco_user_key', d.api_key);
    setStatus('Signed in', true); render();
  } catch (e) { setStatus(e.message, false); }
}
function signout() { localStorage.removeItem('dsco_user_key'); render(); }
function v(id) { return document.getElementById(id).value.trim(); }

async function render() {
  const authed = !!key();
  document.getElementById('authView').style.display = authed ? 'none' : '';
  document.getElementById('dashView').style.display = authed ? '' : 'none';
  if (!authed) return;
  try {
    const me = await api('/api/v1/me');
    document.getElementById('profile').innerHTML =
      `<b>${me.display_name || ''}</b> &lt;${me.email}&gt;<br>` +
      `<span class="muted">${me.user_id} · joined ${String(me.created_at).slice(0,10)}</span>`;
    const k = key();
    document.getElementById('apikey').textContent =
      k.slice(0, 12) + '…' + k.slice(-4);

    ALL_PROVIDERS = (await api('/api/v1/connect/providers?limit=500')).providers;
    const mine = ALL_PROVIDERS.filter(p => p.connected || p.addon_active);
    document.getElementById('myIntegrations').innerHTML = mine.length
      ? mine.map(p =>
          `<span class="chip"><img src="${p.logo_url}" onerror="this.style.display='none'">` +
          `${p.display_name}` +
          (p.addon_active ? ' <span class="muted">add-on</span>' : '') +
          `<span class="x" onclick="${p.addon_active
              ? `disableAddon('${p.provider}')` : `delConnection('${p.provider}')`}">✕</span></span>`
        ).join('')
      : '<span class="muted">Nothing connected yet — pick from below.</span>';
    renderGallery();

    const apps = await api('/api/v1/me/apps');
    document.getElementById('apps').innerHTML =
      apps.apps.map(a =>
        `<tr><td><b>${a.app_name || a.client_id.slice(0,12)}</b></td>` +
        `<td class="muted">${a.scope}</td>` +
        `<td style="text-align:right"><button class="danger" ` +
        `onclick="revokeApp('${a.client_id}')">Revoke</button></td></tr>`
      ).join('') || '<tr><td class="muted">No authorized apps.</td></tr>';

    const bill = await api('/api/v1/me/billing');
    document.getElementById('billing').innerHTML = bill.billing_ready
      ? `Card on file: <b>${bill.card.brand} ····${bill.card.last4}</b>` +
        (bill.card.dev_mode ? ' <span class="muted">(dev mode — no Stripe key configured)</span>' : '') +
        ` · Month to date: <b>$${(bill.month_to_date.total_cents / 100).toFixed(4)}</b>`
      : `<span class="muted">No payment method.</span> ` +
        `<button onclick="addCard()">Add card</button>`;
    document.getElementById('usage').innerHTML =
      bill.month_to_date.lines.map(l =>
        `${l.kind}: ${l.quantity} → $${(l.amount_cents / 100).toFixed(4)}`).join(' · ') +
      (bill.month_to_date.top_tools.length
        ? '<br>Top tools: ' + bill.month_to_date.top_tools.map(t =>
            `${t.tool} (${t.calls})`).join(', ')
        : '');

    const res = await api('/api/v1/me/resources');
    document.getElementById('resources').innerHTML =
      res.resources.map(r =>
        `<tr><td><b>${r.name}</b></td><td class="muted">${r.type}</td>` +
        `<td class="muted">${r.status}</td>` +
        `<td style="text-align:right"><button class="danger" ` +
        `onclick="destroyResource('${r.resource_id}')">Destroy</button></td></tr>`
      ).join('') || '<tr><td class="muted">No resources provisioned.</td></tr>';
    if (!document.getElementById('r_type').children.length) {
      const t = await api('/api/v1/resources/types');
      document.getElementById('r_type').innerHTML =
        Object.keys(t.types).map(k => `<option value="${k}">${k}</option>`).join('');
    }
  } catch (e) {
    if (String(e.message).includes('API key')) signout();
    else setStatus(e.message, false);
  }
}

async function rotateKey() {
  try {
    const d = await api('/api/v1/me/rotate-key', {method: 'POST'});
    localStorage.setItem('dsco_user_key', d.api_key);
    setStatus('API key rotated — old key is dead', true); render();
  } catch (e) { setStatus(e.message, false); }
}
function copyKey() { navigator.clipboard.writeText(key()); setStatus('Copied', true); }
async function changePassword() {
  try {
    await api('/api/v1/me/change-password', {method: 'POST', body: JSON.stringify({
      current_password: v('cp_current'), new_password: v('cp_new')})});
    setStatus('Password changed', true);
  } catch (e) { setStatus(e.message, false); }
}
let ALL_PROVIDERS = [];
function renderGallery() {
  const q = (document.getElementById('p_search').value || '').toLowerCase();
  const list = ALL_PROVIDERS.filter(p =>
    !q || p.provider.includes(q) || p.display_name.toLowerCase().includes(q) ||
    p.categories.some(c => c.includes(q))
  ).slice(0, q ? 60 : 24);
  document.getElementById('gallery').innerHTML = list.map(p => `
    <div class="tile ${p.connected || p.addon_active ? 'on' : ''}">
      <img src="${p.logo_url}" loading="lazy"
           onerror="this.outerHTML='<div style=\\'width:36px;height:36px;border-radius:8px;background:#28304d;color:#5b8def;display:inline-flex;align-items:center;justify-content:center;font-weight:700\\'>${p.display_name[0]}</div>'">
      <div class="pname" title="${p.display_name}">${p.display_name}</div>
      <div class="price">${p.categories[0]}${p.addon_monthly_cents != null
        ? ' · $' + (p.addon_monthly_cents / 100).toFixed(2) + '/mo add-on' : ''}</div>
      ${p.connected
        ? `<button class="danger" onclick="delConnection('${p.provider}')">Disconnect</button>`
        : p.addon_active
          ? `<button class="danger" onclick="disableAddon('${p.provider}')">Disable add-on</button>`
          : `<button class="ghost" onclick="connectKey('${p.provider}')">Use my key</button>
             <button onclick="enableAddon('${p.provider}')">Add-on $${(p.addon_monthly_cents / 100).toFixed(2)}/mo</button>`}
    </div>`).join('');
}
async function connectKey(provider) {
  const credential = prompt('Paste your ' + provider + ' API key / token:');
  if (!credential) return;
  try {
    await api('/api/v1/me/connections', {method: 'POST', body: JSON.stringify({
      provider, credential})});
    setStatus('Connected ' + provider + ' with your key', true); render();
  } catch (e) { setStatus(e.message, false); }
}
async function enableAddon(provider) {
  try {
    const d = await api('/api/v1/me/addons/' + provider, {method: 'POST'});
    setStatus(d.note || ('Enabled ' + provider + ' add-on'), true); render();
  } catch (e) { setStatus(e.message, false); }
}
async function disableAddon(provider) {
  try { await api('/api/v1/me/addons/' + provider, {method: 'DELETE'});
        setStatus('Disabled ' + provider + ' add-on', true); render(); }
  catch (e) { setStatus(e.message, false); }
}
async function delConnection(p) {
  try { await api('/api/v1/me/connections/' + p, {method: 'DELETE'});
        setStatus('Disconnected ' + p, true); render(); }
  catch (e) { setStatus(e.message, false); }
}
async function revokeApp(cid) {
  try { await api('/api/v1/me/apps/' + cid, {method: 'DELETE'});
        setStatus('App access revoked', true); render(); }
  catch (e) { setStatus(e.message, false); }
}
async function addCard() {
  try {
    const d = await api('/api/v1/me/billing/add-card', {method: 'POST'});
    if (d.checkout_url) { location.href = d.checkout_url; return; }
    setStatus(d.note || 'Card added', true); render();
  } catch (e) { setStatus(e.message, false); }
}
async function createResource() {
  try {
    const d = await api('/api/v1/me/resources', {method: 'POST', body: JSON.stringify({
      type: v('r_type'), name: v('r_name')})});
    setStatus('Provisioned ' + d.type + ' "' + d.name + '"', true); render();
  } catch (e) { setStatus(e.message, false); }
}
async function destroyResource(rid) {
  if (!confirm('Destroy this resource?')) return;
  try { await api('/api/v1/me/resources/' + rid, {method: 'DELETE'});
        setStatus('Resource destroyed', true); render(); }
  catch (e) { setStatus(e.message, false); }
}
render();
</script>
</body>
</html>"""


@connect_page_router.get("/account", response_class=HTMLResponse, include_in_schema=False)
async def account_page():
    return HTMLResponse(_ACCOUNT_PAGE)
