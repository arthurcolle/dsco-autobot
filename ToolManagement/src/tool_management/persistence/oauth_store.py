"""
Durable OAuth/OIDC state for the "Sign in with Distributed" identity provider.

Client registrations, user consent grants, refresh tokens, and revocations all
live in DuckDB on DATA_DIR (the /data Modal volume) — losing revocations or
clients on container cycles is a security bug, so none of this is in-memory.
"""

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class OAuthStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "oauth.duckdb")
        self._lock = threading.Lock()
        # Small hot caches so bearer validation doesn't hit DuckDB per request.
        self._revoked_cache: Dict[str, float] = {}
        self._grant_cache: Dict[str, float] = {}
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS oauth_clients (
                    client_id VARCHAR PRIMARY KEY,
                    client_secret VARCHAR,
                    name VARCHAR,
                    redirect_uris VARCHAR,
                    logo_uri VARCHAR,
                    client_uri VARCHAR,
                    created_at DOUBLE,
                    token_endpoint_auth_method VARCHAR
                )
                """
            )
            con.execute(
                "ALTER TABLE oauth_clients ADD COLUMN IF NOT EXISTS token_endpoint_auth_method VARCHAR"
            )
            con.execute(
                """
                UPDATE oauth_clients
                SET token_endpoint_auth_method = CASE
                    WHEN client_secret IS NULL OR client_secret = '' THEN 'none'
                    ELSE 'client_secret_post'
                END
                WHERE token_endpoint_auth_method IS NULL OR token_endpoint_auth_method = ''
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS oauth_grants (
                    user_id VARCHAR,
                    client_id VARCHAR,
                    scope VARCHAR,
                    created_at DOUBLE,
                    PRIMARY KEY (user_id, client_id)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
                    token_hash VARCHAR PRIMARY KEY,
                    client_id VARCHAR,
                    user_id VARCHAR,
                    scope VARCHAR,
                    expires_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS oauth_revocations (
                    token_hash VARCHAR PRIMARY KEY,
                    revoked_at DOUBLE
                )
                """
            )

    # ==================== Clients ====================

    def save_client(self, client: Dict[str, Any]) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO oauth_clients (
                    client_id, client_secret, name, redirect_uris, logo_uri,
                    client_uri, created_at, token_endpoint_auth_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    client["client_id"],
                    client.get("client_secret"),
                    client.get("name"),
                    json.dumps(client.get("redirect_uris") or []),
                    client.get("logo_uri"),
                    client.get("client_uri"),
                    client.get("created_at", time.time()),
                    client.get("token_endpoint_auth_method")
                    or ("client_secret_post" if client.get("client_secret") else "none"),
                ],
            )

    def get_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT client_id, client_secret, name, redirect_uris, logo_uri, client_uri, created_at, token_endpoint_auth_method "
                "FROM oauth_clients WHERE client_id = ?",
                [client_id],
            ).fetchone()
        if not row:
            return None
        return {
            "client_id": row[0],
            "client_secret": row[1],
            "name": row[2],
            "redirect_uris": json.loads(row[3] or "[]"),
            "logo_uri": row[4],
            "client_uri": row[5],
            "created_at": row[6],
            "token_endpoint_auth_method": row[7],
        }

    # ==================== Grants (consent) ====================

    def record_grant(self, user_id: str, client_id: str, scope: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO oauth_grants VALUES (?, ?, ?, ?)",
                [user_id, client_id, scope, time.time()],
            )
        self._grant_cache[f"{user_id}:{client_id}"] = time.time()

    def has_grant(self, user_id: str, client_id: str) -> bool:
        key = f"{user_id}:{client_id}"
        if self._grant_cache.get(key, 0) > time.time() - 60:
            return True
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM oauth_grants WHERE user_id = ? AND client_id = ?",
                [user_id, client_id],
            ).fetchone()
        if row:
            self._grant_cache[key] = time.time()
        return bool(row)

    def list_grants(self, user_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                """
                SELECT g.client_id, g.scope, g.created_at, c.name, c.client_uri
                FROM oauth_grants g LEFT JOIN oauth_clients c USING (client_id)
                WHERE g.user_id = ? ORDER BY g.created_at DESC
                """,
                [user_id],
            ).fetchall()
        return [
            {
                "client_id": r[0],
                "scope": r[1],
                "granted_at": r[2],
                "app_name": r[3],
                "app_uri": r[4],
            }
            for r in rows
        ]

    def revoke_grant(self, user_id: str, client_id: str) -> bool:
        with self._lock, self._connect() as con:
            existed = con.execute(
                "SELECT 1 FROM oauth_grants WHERE user_id = ? AND client_id = ?",
                [user_id, client_id],
            ).fetchone()
            con.execute(
                "DELETE FROM oauth_grants WHERE user_id = ? AND client_id = ?",
                [user_id, client_id],
            )
            con.execute(
                "DELETE FROM oauth_refresh_tokens WHERE user_id = ? AND client_id = ?",
                [user_id, client_id],
            )
        self._grant_cache.pop(f"{user_id}:{client_id}", None)
        return bool(existed)

    # ==================== Refresh tokens ====================

    def save_refresh_token(
        self, token: str, client_id: str, user_id: Optional[str], scope: str, ttl: float
    ) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO oauth_refresh_tokens VALUES (?, ?, ?, ?, ?)",
                [_hash_token(token), client_id, user_id, scope, time.time() + ttl],
            )

    def consume_refresh_token(
        self, token: str, expected_client_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Single-use: return the record and rotate it out."""
        h = _hash_token(token)
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT client_id, user_id, scope, expires_at FROM oauth_refresh_tokens WHERE token_hash = ?",
                [h],
            ).fetchone()
            if not row:
                return None
            if expected_client_id and row[0] != expected_client_id:
                return None
            if row[3] < time.time():
                con.execute("DELETE FROM oauth_refresh_tokens WHERE token_hash = ?", [h])
                return None
            con.execute("DELETE FROM oauth_refresh_tokens WHERE token_hash = ?", [h])
        return {"client_id": row[0], "user_id": row[1], "scope": row[2]}

    # ==================== Revocations ====================

    def revoke_token(self, token: str) -> None:
        h = _hash_token(token)
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO oauth_revocations VALUES (?, ?)", [h, time.time()]
            )
            # A revoke call may target a refresh token — clear that too.
            con.execute("DELETE FROM oauth_refresh_tokens WHERE token_hash = ?", [h])
        self._revoked_cache[h] = time.time()

    def is_revoked(self, token: str) -> bool:
        h = _hash_token(token)
        if h in self._revoked_cache:
            return True
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM oauth_revocations WHERE token_hash = ?", [h]
            ).fetchone()
        if row:
            self._revoked_cache[h] = time.time()
        return bool(row)


_oauth_store: Optional[OAuthStore] = None
_store_lock = threading.Lock()


def get_oauth_store() -> OAuthStore:
    global _oauth_store
    with _store_lock:
        if _oauth_store is None:
            _oauth_store = OAuthStore()
        return _oauth_store
