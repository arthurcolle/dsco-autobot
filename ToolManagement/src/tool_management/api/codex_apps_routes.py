"""FastAPI routes for Codex Apps connector wrapper descriptors."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..integrations.codex_apps_adapter import get_codex_apps_adapter
from ..integrations.codex_apps_runtime import get_codex_apps_runtime
from ..integrations.codex_apps_oauth import get_codex_apps_oauth

router = APIRouter(prefix="/api/v1/codex-apps", tags=["codex-apps"])


@router.get("/summary")
async def codex_apps_summary():
    """Return counts and cache health for the local Codex Apps directory snapshot."""
    return get_codex_apps_adapter().summary()


@router.get("/connectors")
async def list_codex_app_connectors(
    q: str = Query("", description="Search name, description, or connector id"),
    category: str = Query("", description="Category filter, e.g. DEVELOPER_TOOLS"),
    label: str = Query("", description="Label filter: interactive, consequential, retrievable, sync"),
    distribution_channel: str = Query("", description="Distribution channel filter"),
    accessible: Optional[bool] = Query(None, description="Filter by live accessibility"),
    curated_only: bool = Query(False, description="Only connectors referenced by local plugin manifests"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List normalized connector catalog entries."""
    return get_codex_apps_adapter().query(
        q=q,
        category=category,
        label=label,
        distribution_channel=distribution_channel,
        accessible=accessible,
        curated_only=curated_only,
        limit=limit,
        offset=offset,
    )


@router.get("/connectors/{connector_key}")
async def get_codex_app_connector(connector_key: str):
    """Get a connector by exact id, name, or curated slug."""
    connector = get_codex_apps_adapter().get(connector_key)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_key}' not found")
    return connector.to_dict()


@router.get("/wrappers")
async def list_codex_app_wrappers(
    q: str = Query("", description="Search name, description, or connector id"),
    category: str = Query("", description="Category filter"),
    label: str = Query("", description="Label filter"),
    distribution_channel: str = Query("", description="Distribution channel filter"),
    accessible: Optional[bool] = Query(None, description="Filter by live accessibility"),
    curated_only: bool = Query(False, description="Only connectors referenced by local plugin manifests"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return small wrapper descriptors for matching connectors."""
    return get_codex_apps_adapter().wrappers(
        q=q,
        category=category,
        label=label,
        distribution_channel=distribution_channel,
        accessible=accessible,
        curated_only=curated_only,
        limit=limit,
        offset=offset,
    )


@router.get("/wrappers/{connector_key}")
async def get_codex_app_wrapper(connector_key: str):
    """Build one wrapper descriptor by exact connector id, name, or curated slug."""
    try:
        return get_codex_apps_adapter().build_wrapper(connector_key).to_dict()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_key}' not found")


@router.get("/bundles")
async def list_codex_app_bundles():
    """List commercial wrapper bundles such as engineering, GTM, finance, and governance."""
    return {"bundles": get_codex_apps_adapter().bundles()}


@router.get("/bundles/{bundle_id}")
async def get_codex_app_bundle(bundle_id: str):
    """Get one commercial wrapper bundle."""
    try:
        return get_codex_apps_adapter().bundle(bundle_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bundle '{bundle_id}' not found")


@router.get("/stale-curated-pointers")
async def stale_curated_pointers():
    """Return plugin manifest connector ids that no longer resolve in the directory cache."""
    pointers = get_codex_apps_adapter().stale_curated_pointers()
    return {"count": len(pointers), "pointers": [p.to_dict() for p in pointers]}


@router.post("/connectors/{connector_key}/activate")
async def activate_codex_app_connector(connector_key: str):
    """Register a Codex Apps connector as an MCP registry server backend."""
    runtime = get_codex_apps_runtime()
    result = await runtime.activate_connector(connector_key)
    if not result.activated:
        raise HTTPException(
            status_code=400 if result.error and "not found" in result.error.lower() else 500,
            detail=result.error or "Activation failed",
        )
    return runtime._activation_to_dict(result)


@router.post("/connectors/{connector_key}/deactivate")
async def deactivate_codex_app_connector(connector_key: str):
    """Unregister a Codex Apps connector from the MCP registry."""
    runtime = get_codex_apps_runtime()
    result = await runtime.deactivate_connector(connector_key)
    if not result["deactivated"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Deactivation failed"))
    return result


@router.get("/bridge/status")
async def codex_apps_bridge_status():
    """Get the shared virtual bridge process status."""
    return get_codex_apps_runtime().bridge_status()


@router.post("/bundles/{bundle_id}/activate")
async def activate_codex_app_bundle(bundle_id: str):
    """Activate all resolved connectors in a commercial bundle."""
    runtime = get_codex_apps_runtime()
    result = await runtime.activate_bundle(bundle_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/activated")
async def list_activated_codex_app_connectors():
    """List connectors currently registered as MCP servers."""
    return {"activated": get_codex_apps_runtime().list_activated()}


# =============================================================================
# OAuth callback (public — browser redirects here)
# =============================================================================

callback_router = APIRouter(tags=["codex-apps-oauth"])


@callback_router.get("/api/v1/codex-oauth/callback")
async def codex_oauth_callback(
    state: str = Query(...),
    code: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    error_description: Optional[str] = Query(None),
):
    """OAuth 2.0 callback handler. Exchanges code for tokens and stores them."""
    if error:
        return {
            "success": False,
            "error": error,
            "error_description": error_description,
        }
    if not code:
        return {"success": False, "error": "Missing authorization code"}

    oauth = get_codex_apps_oauth()
    # We don't know the provider yet from the callback alone; it's in state.
    state_data = oauth._state.get(state)
    if not state_data:
        return {"success": False, "error": "Invalid or expired state"}

    provider_name = state_data.get("provider")
    client_id, client_secret = _get_oauth_client_creds(provider_name)
    if not client_id or not client_secret:
        return {"success": False, "error": f"OAuth client credentials not configured for {provider_name}"}

    result = await oauth.exchange_code(state, code, client_id, client_secret)
    return result


def _get_oauth_client_creds(provider: str) -> tuple[str, str]:
    env_prefix = provider.upper().replace("-", "_")
    client_id = __import__("os").getenv(f"{env_prefix}_CLIENT_ID", "")
    client_secret = __import__("os").getenv(f"{env_prefix}_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        from ..integrations.codex_apps_vault import get_codex_apps_vault
        vault = get_codex_apps_vault()
        scope = f"_oauth_clients/{provider}"
        client_id = vault.get_secret(scope, "client_id") or ""
        client_secret = vault.get_secret(scope, "client_secret") or ""
    return client_id, client_secret
