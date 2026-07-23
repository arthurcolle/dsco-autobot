"""
Codex Apps OAuth — generic OAuth 2.0 handler for catalog connectors.

Provides:
- A registry of known OAuth providers with authorize/token/scope metadata.
- Authorization URL generation with PKCE and state.
- Token exchange via code.
- Token refresh.
- Secure storage of access/refresh tokens via CodexAppsVault.

For providers not in the known registry, callers can supply endpoints explicitly
or fall back to API-key auth.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, parse_qs, urlparse

import httpx

from .codex_apps_adapter import CodexAppsCatalogAdapter, get_codex_apps_adapter
from .codex_apps_vault import get_codex_apps_vault

logger = logging.getLogger(__name__)

OAUTH_STATE_FILE = Path(os.getenv("DATA_DIR", "data")) / "codex_oauth_state.json"


@dataclass
class OAuthProviderConfig:
    name: str
    auth_url: str
    token_url: str
    scopes: List[str]
    scope_separator: str = " "
    pkce: bool = True
    token_auth_method: str = "client_secret_post"  # or client_secret_basic
    extra_auth_params: Dict[str, str] = field(default_factory=dict)


KNOWN_PROVIDERS: Dict[str, OAuthProviderConfig] = {
    "github": OAuthProviderConfig(
        name="GitHub",
        auth_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        scopes=["repo", "read:user", "user:email"],
        scope_separator=" ",
        pkce=False,
    ),
    "slack": OAuthProviderConfig(
        name="Slack",
        auth_url="https://slack.com/oauth/v2/authorize",
        token_url="https://slack.com/api/oauth.v2.access",
        scopes=["chat:write", "users:read", "channels:read"],
        scope_separator=",",
        pkce=False,
        extra_auth_params={"user_scope": "users:read"},
    ),
    "notion": OAuthProviderConfig(
        name="Notion",
        auth_url="https://api.notion.com/v1/oauth/authorize",
        token_url="https://api.notion.com/v1/oauth/token",
        scopes=["read_content", "insert_content"],
        token_auth_method="client_secret_basic",
    ),
    "hubspot": OAuthProviderConfig(
        name="HubSpot",
        auth_url="https://app.hubspot.com/oauth/authorize",
        token_url="https://api.hubapi.com/oauth/v1/token",
        scopes=["crm.objects.contacts.read", "crm.objects.contacts.write"],
        pkce=False,
    ),
    "stripe": OAuthProviderConfig(
        name="Stripe",
        auth_url="https://connect.stripe.com/oauth/authorize",
        token_url="https://connect.stripe.com/oauth/token",
        scopes=["read_write"],
        pkce=False,
    ),
    "google": OAuthProviderConfig(
        name="Google",
        auth_url="https://accounts.google.com/o/oauth2/v2/auth",
        token_url="https://oauth2.googleapis.com/token",
        scopes=["openid", "email", "profile"],
        pkce=True,
        extra_auth_params={"access_type": "offline", "prompt": "consent"},
    ),
    "microsoft": OAuthProviderConfig(
        name="Microsoft",
        auth_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
        scopes=["openid", "email", "profile", "User.Read"],
        pkce=True,
    ),
}


@dataclass
class OAuthState:
    connector_id: str
    provider: str
    redirect_uri: str
    pkce_code_verifier: Optional[str] = None
    created_at: float = field(default_factory=time.time)


class CodexAppsOAuth:
    """Generic OAuth 2.0 coordinator for Codex Apps connectors."""

    def __init__(
        self,
        adapter: Optional[CodexAppsCatalogAdapter] = None,
        state_file: Optional[Path] = None,
        callback_base_url: Optional[str] = None,
    ):
        self.adapter = adapter or get_codex_apps_adapter()
        self.state_file = state_file or OAUTH_STATE_FILE
        api_base = os.getenv("API_BASE_URL", "")
        default_callback = api_base if api_base else "http://127.0.0.1:8000"
        self.callback_base_url = callback_base_url or os.getenv("CODEX_OAUTH_CALLBACK_URL", default_callback)
        self._state: Dict[str, Dict[str, Any]] = {}
        self._load_state()

    # -------------------------------------------------------------------------
    # State persistence (CSRF/state params)
    # -------------------------------------------------------------------------
    def _load_state(self) -> None:
        if self.state_file.exists():
            try:
                self._state = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"Could not load OAuth state: {e}")
                self._state = {}

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        # Prune state older than 10 minutes
        cutoff = time.time() - 600
        pruned = {k: v for k, v in self._state.items() if v.get("created_at", 0) > cutoff}
        self.state_file.write_text(json.dumps(pruned, indent=2, default=str), encoding="utf-8")

    # -------------------------------------------------------------------------
    # Provider resolution
    # -------------------------------------------------------------------------
    @staticmethod
    def _detect_provider(connector: Any) -> Optional[str]:
        text = f"{connector.name} {connector.description} {connector.categories}".lower()
        for provider in KNOWN_PROVIDERS:
            if provider in text:
                return provider
        return None

    def get_provider_config(self, connector_id: str) -> Tuple[Optional[OAuthProviderConfig], Optional[str]]:
        connector = self.adapter.get(connector_id)
        if not connector:
            return None, f"Connector not found: {connector_id}"
        provider = self._detect_provider(connector)
        if not provider:
            return None, f"No known OAuth provider for {connector.name}; supply auth_url/token_url explicitly"
        return KNOWN_PROVIDERS[provider], None

    # -------------------------------------------------------------------------
    # PKCE helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _generate_pkce() -> Tuple[str, str]:
        verifier = base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8").rstrip("=")
        challenge = base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode()).digest()
        ).decode("utf-8").rstrip("=")
        return verifier, challenge

    # -------------------------------------------------------------------------
    # Authorization URL
    # -------------------------------------------------------------------------
    def build_authorization_url(
        self,
        connector_id: str,
        client_id: str,
        scopes: Optional[List[str]] = None,
        redirect_path: str = "/api/v1/codex-oauth/callback",
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Return (authorization_url, state_token, error)."""
        connector = self.adapter.get(connector_id)
        if not connector:
            return None, None, f"Connector not found: {connector_id}"

        provider_name = self._detect_provider(connector)
        if not provider_name:
            return None, None, f"No known OAuth provider for {connector.name}"
        config = KNOWN_PROVIDERS[provider_name]

        state_token = secrets.token_urlsafe(32)
        redirect_uri = f"{self.callback_base_url.rstrip('/')}{redirect_path}"

        params: Dict[str, str] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state_token,
            "scope": config.scope_separator.join(scopes or config.scopes),
        }
        params.update(config.extra_auth_params)

        code_verifier: Optional[str] = None
        if config.pkce:
            code_verifier, challenge = self._generate_pkce()
            params["code_challenge"] = challenge
            params["code_challenge_method"] = "S256"

        self._state[state_token] = {
            "connector_id": connector.id,
            "provider": provider_name,
            "redirect_uri": redirect_uri,
            "pkce_code_verifier": code_verifier,
            "created_at": time.time(),
        }
        self._save_state()

        auth_url = f"{config.auth_url}?{urlencode(params)}"
        return auth_url, state_token, None

    def build_provider_authorization_url(
        self,
        provider_name: str,
        client_id: str,
        user_id: str,
        scopes: Optional[List[str]] = None,
        redirect_path: str = "/api/v1/codex-oauth/callback",
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Start a user-scoped OAuth flow for a provider without a codex connector.

        Tokens land in a user-scoped vault slot and the tenant credential store,
        so tool execution can resolve them per user.
        """
        config = KNOWN_PROVIDERS.get(provider_name)
        if not config:
            return None, None, f"No known OAuth provider: {provider_name}"

        state_token = secrets.token_urlsafe(32)
        redirect_uri = f"{self.callback_base_url.rstrip('/')}{redirect_path}"

        params: Dict[str, str] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state_token,
            "scope": config.scope_separator.join(scopes or config.scopes),
        }
        params.update(config.extra_auth_params)

        code_verifier: Optional[str] = None
        if config.pkce:
            code_verifier, challenge = self._generate_pkce()
            params["code_challenge"] = challenge
            params["code_challenge_method"] = "S256"

        self._state[state_token] = {
            "connector_id": f"user:{user_id}:{provider_name}",
            "provider": provider_name,
            "user_id": user_id,
            "redirect_uri": redirect_uri,
            "pkce_code_verifier": code_verifier,
            "created_at": time.time(),
        }
        self._save_state()

        auth_url = f"{config.auth_url}?{urlencode(params)}"
        return auth_url, state_token, None

    # -------------------------------------------------------------------------
    # Callback / token exchange
    # -------------------------------------------------------------------------
    async def exchange_code(
        self,
        state_token: str,
        code: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        state = self._state.pop(state_token, None)
        if not state:
            return {"success": False, "error": "Invalid or expired state"}
        self._save_state()

        connector_id = state["connector_id"]
        provider_name = state["provider"]
        redirect_uri = state["redirect_uri"]
        config = KNOWN_PROVIDERS[provider_name]

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if config.pkce and state.get("pkce_code_verifier"):
            data["code_verifier"] = state["pkce_code_verifier"]

        headers = {"Accept": "application/json"}
        if config.token_auth_method == "client_secret_basic":
            basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            headers["Authorization"] = f"Basic {basic}"
            data.pop("client_id", None)
            data.pop("client_secret", None)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(config.token_url, data=data, headers=headers, timeout=60.0)
                # Slack/GitHub can return form-encoded or JSON
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type:
                    token_data = response.json()
                else:
                    token_data = {k: v[0] for k, v in parse_qs(response.text).items()}
            except Exception as e:
                return {"success": False, "error": f"Token exchange request failed: {e}"}

        if response.status_code >= 400 or "error" in token_data:
            return {"success": False, "error": token_data.get("error_description", token_data.get("error", "Token exchange failed")), "provider_response": token_data}

        # Store tokens
        vault = get_codex_apps_vault()
        vault.set_secret(connector_id, "token", token_data.get("access_token", ""))
        if "refresh_token" in token_data:
            vault.set_secret(connector_id, "refresh_token", token_data["refresh_token"])
        if "expires_in" in token_data:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(token_data["expires_in"]))
            vault.set_secret(connector_id, "token_expires_at", expires_at.isoformat())

        # User-scoped flows also land in the tenant credential store so tool
        # execution resolves the token per user.
        user_id = state.get("user_id")
        if user_id and token_data.get("access_token"):
            try:
                from ..services.tenant_credentials import _default_store

                _default_store().set_secret(
                    user_id,
                    provider_name,
                    token_data["access_token"],
                    label=f"oauth:{provider_name}",
                )
            except Exception as e:
                logger.warning(f"Failed to store user-scoped token for {user_id}/{provider_name}: {e}")

        return {
            "success": True,
            "connector_id": connector_id,
            "provider": provider_name,
            "user_id": user_id,
            "scope": token_data.get("scope"),
            "token_type": token_data.get("token_type"),
        }

    # -------------------------------------------------------------------------
    # Token refresh
    # -------------------------------------------------------------------------
    async def refresh_token(
        self,
        connector_id: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        connector = self.adapter.get(connector_id)
        if not connector:
            return {"success": False, "error": f"Connector not found: {connector_id}"}
        provider_name = self._detect_provider(connector)
        if not provider_name:
            return {"success": False, "error": "No known OAuth provider for this connector"}
        config = KNOWN_PROVIDERS[provider_name]

        vault = get_codex_apps_vault()
        refresh = vault.get_secret(connector_id, "refresh_token")
        if not refresh:
            return {"success": False, "error": "No refresh token stored"}

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = {"Accept": "application/json"}
        if config.token_auth_method == "client_secret_basic":
            basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            headers["Authorization"] = f"Basic {basic}"
            data.pop("client_id", None)
            data.pop("client_secret", None)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(config.token_url, data=data, headers=headers, timeout=60.0)
                token_data = response.json()
            except Exception as e:
                return {"success": False, "error": f"Refresh request failed: {e}"}

        if response.status_code >= 400 or "error" in token_data:
            return {"success": False, "error": token_data.get("error_description", token_data.get("error", "Refresh failed"))}

        vault.set_secret(connector_id, "token", token_data.get("access_token", ""))
        if "refresh_token" in token_data:
            vault.set_secret(connector_id, "refresh_token", token_data["refresh_token"])
        if "expires_in" in token_data:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(token_data["expires_in"]))
            vault.set_secret(connector_id, "token_expires_at", expires_at.isoformat())

        return {"success": True, "connector_id": connector_id, "provider": provider_name}


# Singleton
_oauth: Optional[CodexAppsOAuth] = None


def get_codex_apps_oauth() -> CodexAppsOAuth:
    global _oauth
    if _oauth is None:
        _oauth = CodexAppsOAuth()
    return _oauth
