"""Durable, one-time consent requests for tenant Substack connections.

The store deliberately persists only a hash of the bearer-like authorization
token.  The Substack SID is never written here; after consent it goes directly
to the encrypted tenant credential vault.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional


class PublishAuthorizationStore:
    """SQLite-backed authorization state shared by web/API workers."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _init_db(self) -> None:
        with self._init_lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS publish_authorizations (
                    request_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    publication TEXT NOT NULL,
                    label TEXT,
                    callback_url TEXT,
                    token_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    completed_at REAL,
                    last_webhook_event TEXT,
                    last_webhook_status_code INTEGER,
                    last_webhook_success INTEGER,
                    last_webhook_error TEXT,
                    webhook_attempts INTEGER NOT NULL DEFAULT 0,
                    last_webhook_at REAL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_publish_authorizations_tenant_created
                ON publish_authorizations (tenant_id, created_at DESC)
                """
            )

    @staticmethod
    def hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        return dict(row) if row is not None else None

    def create(
        self,
        *,
        request_id: str,
        tenant_id: str,
        publication: str,
        label: Optional[str],
        callback_url: Optional[str],
        token_hash: str,
        expires_at: float,
    ) -> Dict[str, Any]:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO publish_authorizations (
                    request_id, tenant_id, publication, label, callback_url,
                    token_hash, status, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    request_id,
                    tenant_id,
                    publication,
                    label,
                    callback_url,
                    token_hash,
                    now,
                    expires_at,
                ),
            )
        record = self.get(request_id)
        assert record is not None
        return record

    def get(self, request_id: str, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        where = "request_id = ?"
        values: list[Any] = [request_id]
        if tenant_id is not None:
            where += " AND tenant_id = ?"
            values.append(tenant_id)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT * FROM publish_authorizations WHERE {where}", values
            ).fetchone()
        record = self._row(row)
        if record and record["status"] == "pending" and record["expires_at"] <= time.time():
            self.expire(request_id)
            record["status"] = "expired"
        return record

    def latest_for_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM publish_authorizations
                WHERE tenant_id = ? ORDER BY created_at DESC LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
        record = self._row(row)
        if record and record["status"] == "pending" and record["expires_at"] <= time.time():
            self.expire(record["request_id"])
            record["status"] = "expired"
        return record

    def claim(self, request_id: str, presented_token: str) -> Dict[str, Any]:
        """Atomically reserve a pending request for credential storage."""
        presented_hash = self.hash_token(presented_token)
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM publish_authorizations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise KeyError("invalid")
            record = dict(row)
            if record["expires_at"] <= now and record["status"] == "pending":
                connection.execute(
                    "UPDATE publish_authorizations SET status = 'expired' WHERE request_id = ?",
                    (request_id,),
                )
                raise TimeoutError("expired")
            if record["status"] != "pending":
                raise RuntimeError(str(record["status"]))
            # compare_digest prevents a token-prefix timing oracle.
            import hmac

            if not hmac.compare_digest(str(record["token_hash"]), presented_hash):
                raise KeyError("invalid")
            updated = connection.execute(
                """
                UPDATE publish_authorizations SET status = 'authorizing'
                WHERE request_id = ? AND status = 'pending'
                """,
                (request_id,),
            )
            if updated.rowcount != 1:
                raise RuntimeError("authorizing")
        record["status"] = "authorizing"
        return record

    def release_claim(self, request_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE publish_authorizations SET status = 'pending'
                WHERE request_id = ? AND status = 'authorizing'
                """,
                (request_id,),
            )

    def authorize(self, request_id: str) -> Dict[str, Any]:
        now = time.time()
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE publish_authorizations
                SET status = 'authorized', completed_at = ?
                WHERE request_id = ? AND status = 'authorizing'
                """,
                (now, request_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("authorization claim was lost")
        record = self.get(request_id)
        assert record is not None
        return record

    def cancel(self, request_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE publish_authorizations
                SET status = 'cancelled', completed_at = ?
                WHERE request_id = ? AND tenant_id = ? AND status = 'pending'
                """,
                (time.time(), request_id, tenant_id),
            )
        if updated.rowcount != 1:
            return None
        return self.get(request_id, tenant_id)

    def cancel_pending_for_tenant(self, tenant_id: str) -> int:
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE publish_authorizations
                SET status = 'cancelled', completed_at = ?
                WHERE tenant_id = ? AND status = 'pending'
                """,
                (time.time(), tenant_id),
            )
        return int(updated.rowcount)

    def expire(self, request_id: str) -> bool:
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE publish_authorizations SET status = 'expired'
                WHERE request_id = ? AND status = 'pending' AND expires_at <= ?
                """,
                (request_id, time.time()),
            )
        return updated.rowcount == 1

    def record_webhook_delivery(
        self,
        request_id: str,
        *,
        event: str,
        status_code: Optional[int],
        success: bool,
        error: Optional[str],
    ) -> None:
        safe_error = str(error or "")[:500] or None
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE publish_authorizations
                SET last_webhook_event = ?, last_webhook_status_code = ?,
                    last_webhook_success = ?, last_webhook_error = ?,
                    webhook_attempts = webhook_attempts + 1,
                    last_webhook_at = ?
                WHERE request_id = ?
                """,
                (
                    event,
                    status_code,
                    1 if success else 0,
                    safe_error,
                    time.time(),
                    request_id,
                ),
            )


_store_lock = threading.Lock()
_store: Optional[PublishAuthorizationStore] = None
_store_path: Optional[Path] = None


def get_publish_authorization_store() -> PublishAuthorizationStore:
    global _store, _store_path
    configured = os.getenv("PUBLISH_AUTHORIZATION_DB_PATH")
    path = Path(configured) if configured else Path(os.getenv("DATA_DIR", "data")) / "publish_authorizations.db"
    with _store_lock:
        if _store is None or _store_path != path:
            _store = PublishAuthorizationStore(path)
            _store_path = path
        return _store


def public_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return authorization metadata with all ownership and secret fields removed."""
    return {
        "request_id": record["request_id"],
        "publication": record["publication"],
        "label": record.get("label"),
        "status": record["status"],
        "created_at": record["created_at"],
        "expires_at": record["expires_at"],
        "completed_at": record.get("completed_at"),
        "webhook": {
            "configured": bool(record.get("callback_url")),
            "last_event": record.get("last_webhook_event"),
            "last_status_code": record.get("last_webhook_status_code"),
            "last_success": (
                None
                if record.get("last_webhook_success") is None
                else bool(record.get("last_webhook_success"))
            ),
            "attempts": int(record.get("webhook_attempts") or 0),
            "last_attempt_at": record.get("last_webhook_at"),
        },
    }
