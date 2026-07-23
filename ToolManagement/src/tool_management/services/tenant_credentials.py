"""
Tenant Credentials — multi-tenant credential resolution for the fabric layer.

Generalizes the single-tenant `CodexAppsVault` pattern (macOS Keychain /
Fernet-encrypted file, scoped by `(scope, key)`) to a tenant-scoped vault
plus a single resolution choke point that every fabric handler (finance,
integration, and future fabrics) can call instead of reading `os.getenv(...)`
directly.

Design goals:
- Backward compatible: when no tenant credential exists, behavior is
  identical to today (falls through to the operator's own env var / pooled
  managed-tier key). Nothing that currently works stops working.
- Single choke point: fabrics call `resolve_credential(tenant_id, provider,
  env_fallback_var)` instead of hardcoding `os.getenv(...)` per provider.
  This lets a tenant's own key flip a provider from sandbox to live without
  touching per-provider dispatch code.
- Values are never returned by any admin/listing API — only existence and
  metadata. Same non-negotiable as CodexAppsVault.
- Storage backend is swappable: local dev keeps the CodexAppsVault-style
  Keychain/Fernet-file backend; a hosted deployment (Modal) should inject an
  EnvelopeEncryptedStore (Postgres/SQLite + KMS-derived key) instead. Both
  implement the same TenantCredentialStore protocol.

This module does NOT change any currently-running dispatch code. Wiring it
into `native/finance_fabric/server.py` and `native/integration_fabric/server.py`
is a separate, follow-up patch (see TENANT_CREDENTIALS_WIRING.md) because
those files hold module-level constants computed once at import time
(e.g. `ALPACA_LIVE = bool(os.getenv(...))`) — making the live/sandbox check
tenant-aware requires moving that check into the request path, which needs
test execution to verify safely.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_TENANT_VAULT_FILE = DATA_DIR / "tenant_credentials.enc"

# Reserved tenant_id for the operator's own pooled/managed-tier credentials.
# Never persisted through the tenant store — this id means "use env fallback".
OPERATOR_TENANT_ID = "__operator__"


# =============================================================================
# MODELS
# =============================================================================

@dataclass
class CredentialMetadata:
    """Non-secret metadata about a stored tenant credential."""
    tenant_id: str
    provider: str
    created_at: str
    updated_at: str
    label: Optional[str] = None
    # Never populated with the actual secret value.


# =============================================================================
# STORE PROTOCOL
# =============================================================================

class TenantCredentialStore(ABC):
    """
    Abstract tenant-scoped credential store.

    Scope key is always (tenant_id, provider) — e.g. ("acct_9f2a", "alpaca").
    Implementations must never expose raw secret values through any listing
    or metadata call; only `get_secret` (used internally by the resolver)
    returns the actual value.
    """

    @abstractmethod
    def set_secret(self, tenant_id: str, provider: str, value: str, label: Optional[str] = None) -> bool:
        ...

    @abstractmethod
    def get_secret(self, tenant_id: str, provider: str) -> Optional[str]:
        ...

    @abstractmethod
    def has_secret(self, tenant_id: str, provider: str) -> bool:
        ...

    @abstractmethod
    def delete_secret(self, tenant_id: str, provider: str) -> bool:
        ...

    @abstractmethod
    def list_providers(self, tenant_id: str) -> List[str]:
        """Provider names the tenant has connected — no secret values."""
        ...

    @abstractmethod
    def get_metadata(self, tenant_id: str, provider: str) -> Optional[CredentialMetadata]:
        ...


# =============================================================================
# LOCAL DEV BACKEND — encrypted file, same pattern as CodexAppsVault
# =============================================================================

class FileTenantCredentialStore(TenantCredentialStore):
    """
    Encrypted-file tenant credential store for local dev / single-instance
    deployments. Not appropriate for multi-instance hosted deployment
    without a shared volume — use a database-backed store there instead.
    """

    def __init__(self, vault_file: Optional[Path] = None, master_key_env: str = "TENANT_VAULT_MASTER_KEY"):
        self.vault_file = vault_file or DEFAULT_TENANT_VAULT_FILE
        self._fernet: Optional["Fernet"] = None
        self._init_crypto(master_key_env)

    def _init_crypto(self, master_key_env: str) -> None:
        if not CRYPTO_AVAILABLE:
            logger.warning("cryptography not installed; tenant vault will store plaintext (insecure)")
            return
        master = os.getenv(master_key_env, "")
        if not master:
            logger.warning(
                f"{master_key_env} not set; deriving a machine-specific fallback key. "
                f"Set {master_key_env} explicitly in any shared/production deployment."
            )
            try:
                import getpass
                login = getpass.getuser()
            except Exception:
                login = os.getenv("USER") or os.getenv("LOGNAME") or "container"
            seed = f"{login}-{os.uname().nodename}-tenant-credentials".encode()
            master = base64.urlsafe_b64encode(hashlib.sha256(seed).digest()).decode()
        key = base64.urlsafe_b64encode(hashlib.sha256(master.encode()).digest())
        self._fernet = Fernet(key)

    def _load(self) -> Dict[str, Any]:
        if not self.vault_file.exists():
            return {}
        raw = self.vault_file.read_bytes()
        if not raw:
            return {}
        if self._fernet:
            try:
                return json.loads(self._fernet.decrypt(raw))
            except Exception as e:
                logger.error(f"Could not decrypt tenant vault: {e}")
                return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _save(self, data: Dict[str, Any]) -> None:
        self.vault_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, indent=2).encode("utf-8")
        if self._fernet:
            payload = self._fernet.encrypt(payload)
        self.vault_file.write_bytes(payload)

    @staticmethod
    def _key(tenant_id: str, provider: str) -> str:
        return f"{tenant_id}:{provider}"

    def set_secret(self, tenant_id: str, provider: str, value: str, label: Optional[str] = None) -> bool:
        data = self._load()
        now = datetime.now(timezone.utc).isoformat()
        entry_key = self._key(tenant_id, provider)
        existing = data.get(entry_key, {})
        data[entry_key] = {
            "value": value,
            "label": label if label is not None else existing.get("label"),
            "created_at": existing.get("created_at", now),
            "updated_at": now,
        }
        self._save(data)
        return True

    def get_secret(self, tenant_id: str, provider: str) -> Optional[str]:
        entry = self._load().get(self._key(tenant_id, provider))
        return entry.get("value") if entry else None

    def has_secret(self, tenant_id: str, provider: str) -> bool:
        return self.get_secret(tenant_id, provider) is not None

    def delete_secret(self, tenant_id: str, provider: str) -> bool:
        data = self._load()
        entry_key = self._key(tenant_id, provider)
        if entry_key in data:
            del data[entry_key]
            self._save(data)
            return True
        return False

    def list_providers(self, tenant_id: str) -> List[str]:
        prefix = f"{tenant_id}:"
        providers = []
        for entry_key in self._load():
            if entry_key.startswith(prefix):
                providers.append(entry_key[len(prefix):])
        return sorted(providers)

    def get_metadata(self, tenant_id: str, provider: str) -> Optional[CredentialMetadata]:
        entry = self._load().get(self._key(tenant_id, provider))
        if not entry:
            return None
        return CredentialMetadata(
            tenant_id=tenant_id,
            provider=provider,
            created_at=entry.get("created_at", ""),
            updated_at=entry.get("updated_at", ""),
            label=entry.get("label"),
        )


# =============================================================================
# IN-MEMORY BACKEND — for tests
# =============================================================================

class InMemoryTenantCredentialStore(TenantCredentialStore):
    """Pure in-memory store for unit tests. No persistence, no crypto overhead."""

    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _key(tenant_id: str, provider: str) -> str:
        return f"{tenant_id}:{provider}"

    def set_secret(self, tenant_id: str, provider: str, value: str, label: Optional[str] = None) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        entry_key = self._key(tenant_id, provider)
        existing = self._data.get(entry_key, {})
        self._data[entry_key] = {
            "value": value,
            "label": label if label is not None else existing.get("label"),
            "created_at": existing.get("created_at", now),
            "updated_at": now,
        }
        return True

    def get_secret(self, tenant_id: str, provider: str) -> Optional[str]:
        entry = self._data.get(self._key(tenant_id, provider))
        return entry.get("value") if entry else None

    def has_secret(self, tenant_id: str, provider: str) -> bool:
        return self.get_secret(tenant_id, provider) is not None

    def delete_secret(self, tenant_id: str, provider: str) -> bool:
        entry_key = self._key(tenant_id, provider)
        if entry_key in self._data:
            del self._data[entry_key]
            return True
        return False

    def list_providers(self, tenant_id: str) -> List[str]:
        prefix = f"{tenant_id}:"
        return sorted(k[len(prefix):] for k in self._data if k.startswith(prefix))

    def get_metadata(self, tenant_id: str, provider: str) -> Optional[CredentialMetadata]:
        entry = self._data.get(self._key(tenant_id, provider))
        if not entry:
            return None
        return CredentialMetadata(
            tenant_id=tenant_id,
            provider=provider,
            created_at=entry.get("created_at", ""),
            updated_at=entry.get("updated_at", ""),
            label=entry.get("label"),
        )


# =============================================================================
# RESOLUTION — the single choke point fabrics should call
# =============================================================================

class CredentialResolver:
    """
    Resolves the effective credential for (tenant_id, provider) at call time.

    Resolution order:
    1. Tenant's own BYOK credential, if connected (real per-user key —
       tenant bears any provider cost/rate-limit for their own calls).
    2. Operator's pooled/managed-tier env var fallback, if the caller
       supplies one AND the provider/category is allow-listed for pooling
       (callers should only pass an env fallback for cheap, capped,
       read-only categories per the D-tier design — see
       TENANT_CREDENTIALS_WIRING.md).
    3. None -> caller falls through to sandbox mode (today's behavior,
       unchanged).
    """

    def __init__(self, store: Optional[TenantCredentialStore] = None):
        self.store = store or _default_store()

    def resolve(
        self,
        tenant_id: Optional[str],
        provider: str,
        env_fallback_var: Optional[str] = None,
        allow_pooled_fallback: bool = False,
    ) -> Optional[str]:
        """Return the effective credential string, or None to stay in sandbox."""
        if tenant_id and tenant_id != OPERATOR_TENANT_ID:
            tenant_secret = self.store.get_secret(tenant_id, provider)
            if tenant_secret:
                return tenant_secret

        if allow_pooled_fallback and env_fallback_var:
            return os.getenv(env_fallback_var) or None

        return None

    def is_live(
        self,
        tenant_id: Optional[str],
        provider: str,
        env_fallback_var: Optional[str] = None,
        allow_pooled_fallback: bool = False,
    ) -> bool:
        return self.resolve(tenant_id, provider, env_fallback_var, allow_pooled_fallback) is not None


_default_store_instance: Optional[TenantCredentialStore] = None


def _default_store() -> TenantCredentialStore:
    global _default_store_instance
    if _default_store_instance is None:
        _default_store_instance = FileTenantCredentialStore()
    return _default_store_instance


_default_resolver: Optional[CredentialResolver] = None


def get_credential_resolver() -> CredentialResolver:
    global _default_resolver
    if _default_resolver is None:
        _default_resolver = CredentialResolver()
    return _default_resolver


def resolve_credential(
    tenant_id: Optional[str],
    provider: str,
    env_fallback_var: Optional[str] = None,
    allow_pooled_fallback: bool = False,
) -> Optional[str]:
    """Module-level convenience wrapper around the default resolver."""
    return get_credential_resolver().resolve(
        tenant_id, provider, env_fallback_var, allow_pooled_fallback
    )
