"""
Unified Secret Store for Tool Management.

Provides a single resolution path for credentials (API keys, tokens) so that
tools never need a secret pasted into chat or passed as a visible tool
argument. Resolution order for ``resolve(name)``:

    1. macOS Keychain (via the ``security`` CLI) under this store's service.
    2. The current process environment (``os.environ``).
    3. The project ``.env`` file (parsed, never ``exec``'d).

Backends mirror :mod:`tool_management.integrations.codex_apps_vault`: Keychain
is primary on macOS; a Fernet-encrypted JSON file is the fallback elsewhere.
Values are never surfaced in plaintext by management tools — only metadata,
existence checks, and masked previews. Tools that need a secret activate it by
name (``activate`` exports it into ``os.environ`` for the process) or call
``resolve_secret`` / ``bearer_header_from_secret`` directly.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

_Fernet: Any = None
try:
    from cryptography.fernet import Fernet as _Fernet  # type: ignore[import-not-found]
    _CRYPTO_AVAILABLE = True
except Exception:  # pragma: no cover - cryptography is a hard dep, but be safe
    _CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_FALLBACK_FILE = DATA_DIR / "secret_store.enc"
DEFAULT_KEYCHAIN_SERVICE = "dsco.toolmanagement.secrets"
DEFAULT_SCOPE = "env"
DEFAULT_DOTENV = Path(os.getenv("DSCO_DOTENV", ".env"))


def redact(value: Optional[str], keep: int = 4) -> str:
    """Return a masked preview of a secret (e.g. ``sk-or-v1…95f5``)."""
    if not value:
        return ""
    if len(value) <= keep * 2:
        return "***"
    return f"{value[:keep]}…{value[-keep:]}"


class SecretStore:
    """Secure, env-aware credential store with keychain and file backends."""

    def __init__(
        self,
        service: str = DEFAULT_KEYCHAIN_SERVICE,
        fallback_file: Optional[Path] = None,
        dotenv_path: Optional[Path] = None,
    ) -> None:
        self.service = service
        self.fallback_file = fallback_file or DEFAULT_FALLBACK_FILE
        self.dotenv_path = dotenv_path or DEFAULT_DOTENV
        self._use_keychain = self._keychain_available()
        self._fernet: Optional[Any] = None
        self._dotenv_cache: Optional[Dict[str, str]] = None
        if not self._use_keychain:
            self._init_fallback()

    # ------------------------------------------------------------------
    # Backend detection / setup
    # ------------------------------------------------------------------
    @staticmethod
    def _keychain_available() -> bool:
        return shutil.which("security") is not None

    def _init_fallback(self) -> None:
        if not _CRYPTO_AVAILABLE:
            logger.warning("cryptography not installed; secret store fallback is plaintext (insecure)")
            return
        master = os.getenv("SECRET_STORE_MASTER_KEY") or os.getenv("CODEX_VAULT_MASTER_KEY") or ""
        if not master:
            logger.warning(
                "SECRET_STORE_MASTER_KEY not set; deriving a machine-specific fallback key. "
                "Set SECRET_STORE_MASTER_KEY in production for stable encrypted access."
            )
            seed = f"{os.getlogin()}-{os.uname().nodename}-{self.service}".encode()
            master = base64.urlsafe_b64encode(hashlib.sha256(seed).digest()).decode()
        key = base64.urlsafe_b64encode(hashlib.sha256(master.encode()).digest())
        self._fernet = _Fernet(key)

    @property
    def backend(self) -> str:
        return "keychain" if self._use_keychain else ("fernet-file" if self._fernet else "plaintext-file")

    # ------------------------------------------------------------------
    # Scope/key helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _account(scope: str, key: str) -> str:
        return f"{scope}:{key}"

    # ------------------------------------------------------------------
    # Keychain backend
    # ------------------------------------------------------------------
    def _set_keychain(self, scope: str, key: str, value: str) -> bool:
        account = self._account(scope, key)
        try:
            self._delete_keychain(scope, key)  # idempotent overwrite
            subprocess.run(
                ["security", "add-generic-password", "-s", self.service, "-a", account, "-w", value],
                check=True, capture_output=True,
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Keychain set failed for %s/%s: %s", scope, key, e.stderr.decode(errors="replace"))
            return False

    def _get_keychain(self, scope: str, key: str) -> Optional[str]:
        account = self._account(scope, key)
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", self.service, "-a", account, "-w"],
                check=True, capture_output=True, text=True,
            )
            return result.stdout.rstrip("\n")
        except subprocess.CalledProcessError:
            return None

    def _delete_keychain(self, scope: str, key: str) -> bool:
        account = self._account(scope, key)
        try:
            subprocess.run(
                ["security", "delete-generic-password", "-s", self.service, "-a", account],
                check=True, capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def _list_keychain_accounts(self) -> List[str]:
        """Return ``scope:key`` accounts stored under this store's service."""
        try:
            result = subprocess.run(
                ["security", "dump-keychain"], check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.warning("Could not dump keychain: %s", e)
            return []

        def _field(block: str, key: str) -> Optional[str]:
            marker = f'"{key}"<blob>="'
            start = block.find(marker)
            if start == -1:
                return None
            tail = block[start + len(marker):]
            return tail[: tail.find('"')]

        # Each keychain entry begins with a ``keychain:`` header line.
        accounts: List[str] = []
        for block in result.stdout.split("keychain:")[1:]:
            if _field(block, "svce") != self.service:
                continue
            acct = _field(block, "acct")
            if acct and ":" in acct:
                accounts.append(acct)
        return accounts

    # ------------------------------------------------------------------
    # Fernet file backend
    # ------------------------------------------------------------------
    def _load_fallback(self) -> Dict[str, Dict[str, str]]:
        if not self.fallback_file.exists():
            return {}
        raw = self.fallback_file.read_bytes()
        if not raw:
            return {}
        if self._fernet:
            try:
                return json.loads(self._fernet.decrypt(raw))
            except Exception as e:
                logger.error("Could not decrypt secret store fallback: %s", e)
                return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _save_fallback(self, data: Dict[str, Dict[str, str]]) -> None:
        self.fallback_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, indent=2).encode("utf-8")
        if self._fernet:
            payload = self._fernet.encrypt(payload)
        self.fallback_file.write_bytes(payload)

    # ------------------------------------------------------------------
    # Public store API (explicit scope)
    # ------------------------------------------------------------------
    def set_secret(self, scope: str, key: str, value: str) -> bool:
        if self._use_keychain:
            return self._set_keychain(scope, key, value)
        data = self._load_fallback()
        data.setdefault(scope, {})[key] = value
        self._save_fallback(data)
        return True

    def get_secret(self, scope: str, key: str) -> Optional[str]:
        if self._use_keychain:
            return self._get_keychain(scope, key)
        return self._load_fallback().get(scope, {}).get(key)

    def has_secret(self, scope: str, key: str) -> bool:
        return self.get_secret(scope, key) is not None

    def delete_secret(self, scope: str, key: str) -> bool:
        if self._use_keychain:
            return self._delete_keychain(scope, key)
        data = self._load_fallback()
        if scope in data and key in data[scope]:
            del data[scope][key]
            if not data[scope]:
                del data[scope]
            self._save_fallback(data)
            return True
        return False

    def list_scopes(self) -> List[str]:
        if self._use_keychain:
            scopes = {a.split(":", 1)[0] for a in self._list_keychain_accounts() if ":" in a}
            return sorted(scopes)
        return sorted(self._load_fallback().keys())

    def list_keys(self, scope: str) -> List[str]:
        if self._use_keychain:
            prefix = f"{scope}:"
            return sorted(a[len(prefix):] for a in self._list_keychain_accounts() if a.startswith(prefix))
        return sorted(self._load_fallback().get(scope, {}).keys())

    # ------------------------------------------------------------------
    # Env-aware resolution (the point of this module)
    # ------------------------------------------------------------------
    def _load_dotenv(self) -> Dict[str, str]:
        if self._dotenv_cache is not None:
            return self._dotenv_cache
        env: Dict[str, str] = {}
        path = self.dotenv_path
        if not path.exists():
            self._dotenv_cache = env
            return env
        try:
            for raw in path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k:
                    env[k] = v
        except Exception as e:
            logger.warning("Could not parse %s: %s", path, e)
        self._dotenv_cache = env
        return env

    def resolve(self, name: str, scope: str = DEFAULT_SCOPE) -> Optional[str]:
        """Resolve a secret by name: keychain -> process env -> .env file."""
        value, _ = self.resolve_with_source(name, scope=scope)
        return value

    def resolve_with_source(self, name: str, scope: str = DEFAULT_SCOPE):
        """Resolve a secret, returning ``(value, source)``.

        ``source`` is one of ``keychain``, ``env``, ``dotenv`` or ``None``.
        """
        if not name:
            return None, None
        kc = self.get_secret(scope, name)
        if kc:
            return kc, "keychain"
        if name in os.environ:
            return os.environ[name], "env"
        dv = self._load_dotenv().get(name)
        if dv is not None:
            return dv, "dotenv"
        return None, None

    def activate(self, name: str, scope: str = DEFAULT_SCOPE) -> Optional[str]:
        """Resolve a secret and export it into ``os.environ`` for the process.

        Downstream tools that read ``os.environ[name]`` (the common case) then
        work without the secret ever being surfaced to the model.
        """
        value = self.resolve(name, scope=scope)
        if value is not None:
            os.environ[name] = value
        return value

    def status(self) -> Dict[str, object]:
        scopes = self.list_scopes()
        return {
            "backend": self.backend,
            "service": self.service,
            "scope_count": len(scopes),
            "scopes": scopes,
        }


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------
_store: Optional[SecretStore] = None


def get_secret_store() -> SecretStore:
    global _store
    if _store is None:
        _store = SecretStore()
    return _store
