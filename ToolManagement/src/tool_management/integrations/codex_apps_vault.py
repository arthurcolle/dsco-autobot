"""
Codex Apps Vault — secure credential storage for connector tokens and keys.

Primary backend: macOS Keychain (via the `security` CLI).
Fallback backend: encrypted JSON file using Fernet from a master key.

Values are never returned by admin APIs; only metadata and existence checks.
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

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
FALLBACK_VAULT_FILE = DATA_DIR / "codex_apps_vault.enc"
KEYCHAIN_SERVICE = "dsco.toolmanagement.codex.apps"


class CodexAppsVault:
    """Secure storage for Codex Apps connector credentials."""

    def __init__(self, service: str = KEYCHAIN_SERVICE, fallback_file: Optional[Path] = None):
        self.service = service
        self.fallback_file = fallback_file or FALLBACK_VAULT_FILE
        self._use_keychain = self._keychain_available()
        self._fernet: Optional[Fernet] = None
        if not self._use_keychain:
            self._init_fallback()

    # -------------------------------------------------------------------------
    # Backend detection
    # -------------------------------------------------------------------------
    @staticmethod
    def _keychain_available() -> bool:
        return shutil.which("security") is not None

    def _init_fallback(self) -> None:
        if not CRYPTO_AVAILABLE:
            logger.warning("cryptography not installed; vault will store plaintext (insecure)")
            return
        master = os.getenv("CODEX_VAULT_MASTER_KEY", "")
        if not master:
            # In production this must be set explicitly. We derive a fallback key
            # from machine-specific data only for local development; it will NOT
            # be stable across containers or OS reinstalls.
            logger.warning(
                "CODEX_VAULT_MASTER_KEY not set; deriving a machine-specific fallback key. "
                "Set CODEX_VAULT_MASTER_KEY in production for stable encrypted vault access."
            )
            # os.getlogin() raises OSError in containers (no controlling TTY);
            # getpass falls back to env vars and pwd instead.
            try:
                import getpass
                login = getpass.getuser()
            except Exception:
                login = os.getenv("USER") or os.getenv("LOGNAME") or "container"
            seed = f"{login}-{os.uname().nodename}-{self.service}".encode()
            master = base64.urlsafe_b64encode(hashlib.sha256(seed).digest()).decode()
        key = base64.urlsafe_b64encode(hashlib.sha256(master.encode()).digest())
        self._fernet = Fernet(key)

    # -------------------------------------------------------------------------
    # Scope/key helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _account(scope: str, key: str) -> str:
        return f"{scope}:{key}"

    # -------------------------------------------------------------------------
    # Keychain backends
    # -------------------------------------------------------------------------
    def _set_keychain(self, scope: str, key: str, value: str) -> bool:
        account = self._account(scope, key)
        try:
            # Update if exists: delete then add
            self._delete_keychain(scope, key)
            subprocess.run(
                ["security", "add-generic-password", "-s", self.service, "-a", account, "-w", value],
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Keychain set failed for {scope}/{key}: {e.stderr.decode(errors='replace')}")
            return False

    def _get_keychain(self, scope: str, key: str) -> Optional[str]:
        account = self._account(scope, key)
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", self.service, "-a", account, "-w"],
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.rstrip("\n")
        except subprocess.CalledProcessError:
            return None

    def _delete_keychain(self, scope: str, key: str) -> bool:
        account = self._account(scope, key)
        try:
            subprocess.run(
                ["security", "delete-generic-password", "-s", self.service, "-a", account],
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def _list_keychain_accounts(self) -> List[str]:
        try:
            result = subprocess.run(
                ["security", "dump-keychain"],
                check=True,
                capture_output=True,
                text=True,
            )
            accounts = []
            for line in result.stdout.splitlines():
                if f'"svce"<blob>="{self.service}"' in line:
                    # The account is on a nearby line
                    pass
                if '0x00000007 <blob>=' in line or '"acct"<blob>=' in line:
                    # Extract account string
                    start = line.find('"acct"<blob>="')
                    if start != -1:
                        account = line[start + len('"acct"<blob>="'):].rstrip('"')
                        accounts.append(account)
            return accounts
        except subprocess.CalledProcessError as e:
            logger.warning(f"Could not list keychain: {e}")
            return []

    # -------------------------------------------------------------------------
    # Fallback file backend
    # -------------------------------------------------------------------------
    def _load_fallback(self) -> Dict[str, Any]:
        if not self.fallback_file.exists():
            return {}
        raw = self.fallback_file.read_bytes()
        if not raw:
            return {}
        if self._fernet:
            try:
                decrypted = self._fernet.decrypt(raw)
                return json.loads(decrypted)
            except Exception as e:
                logger.error(f"Could not decrypt fallback vault: {e}")
                return {}
        # plaintext fallback
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _save_fallback(self, data: Dict[str, Any]) -> None:
        self.fallback_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, indent=2).encode("utf-8")
        if self._fernet:
            payload = self._fernet.encrypt(payload)
        self.fallback_file.write_bytes(payload)

    def _set_fallback(self, scope: str, key: str, value: str) -> bool:
        data = self._load_fallback()
        data.setdefault(scope, {})[key] = value
        self._save_fallback(data)
        return True

    def _get_fallback(self, scope: str, key: str) -> Optional[str]:
        return self._load_fallback().get(scope, {}).get(key)

    def _delete_fallback(self, scope: str, key: str) -> bool:
        data = self._load_fallback()
        if scope in data and key in data[scope]:
            del data[scope][key]
            if not data[scope]:
                del data[scope]
            self._save_fallback(data)
            return True
        return False

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def set_secret(self, scope: str, key: str, value: str) -> bool:
        if self._use_keychain:
            return self._set_keychain(scope, key, value)
        return self._set_fallback(scope, key, value)

    def get_secret(self, scope: str, key: str) -> Optional[str]:
        if self._use_keychain:
            return self._get_keychain(scope, key)
        return self._get_fallback(scope, key)

    def has_secret(self, scope: str, key: str) -> bool:
        return self.get_secret(scope, key) is not None

    def delete_secret(self, scope: str, key: str) -> bool:
        if self._use_keychain:
            return self._delete_keychain(scope, key)
        return self._delete_fallback(scope, key)

    def list_scopes(self) -> List[str]:
        """Return scope names that have at least one stored secret."""
        if self._use_keychain:
            accounts = self._list_keychain_accounts()
            scopes = set()
            for account in accounts:
                if ":" in account:
                    scopes.add(account.split(":", 1)[0])
            return sorted(scopes)
        return sorted(self._load_fallback().keys())

    def list_keys(self, scope: str) -> List[str]:
        if self._use_keychain:
            prefix = f"{scope}:"
            keys = []
            for account in self._list_keychain_accounts():
                if account.startswith(prefix):
                    keys.append(account[len(prefix):])
            return sorted(keys)
        return sorted(self._load_fallback().get(scope, {}).keys())


# Singleton
_vault: Optional[CodexAppsVault] = None


def get_codex_apps_vault() -> CodexAppsVault:
    global _vault
    if _vault is None:
        _vault = CodexAppsVault()
    return _vault
