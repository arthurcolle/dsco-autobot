"""Durable, tenant-scoped RuntimeSpec build requests.

Only an already validated RuntimeSpec and non-secret status metadata are kept
here.  Activation leases, signer material, provider credentials, and binaries
are intentionally outside this store.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4


class RuntimeBuildStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "runtime_builds.sqlite3")
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_build_requests (
                    request_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    runtime_spec_sha256 TEXT NOT NULL,
                    runtime_spec_json TEXT NOT NULL,
                    organization_id TEXT,
                    deployment_id TEXT,
                    capsule_sha256 TEXT,
                    policy_sha256 TEXT,
                    source_sha256 TEXT,
                    status TEXT NOT NULL,
                    approval_state TEXT NOT NULL,
                    blocker_code TEXT,
                    artifact_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    approved_at REAL,
                    UNIQUE(tenant_id, idempotency_key)
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_runtime_builds_tenant_updated ON runtime_build_requests(tenant_id, updated_at DESC)")
            for name in ("organization_id", "deployment_id", "capsule_sha256", "policy_sha256", "source_sha256"):
                try:
                    con.execute(f"ALTER TABLE runtime_build_requests ADD COLUMN {name} TEXT")
                except sqlite3.OperationalError:
                    pass
            con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_builds_deployment "
                "ON runtime_build_requests(tenant_id, deployment_id) WHERE deployment_id IS NOT NULL"
            )

    @staticmethod
    def _row(row: sqlite3.Row | None) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        value = dict(row)
        value["runtime_spec"] = json.loads(value.pop("runtime_spec_json"))
        value["artifact"] = json.loads(value.pop("artifact_json")) if value.get("artifact_json") else None
        identity_fields = ("organization_id", "deployment_id", "capsule_sha256", "policy_sha256", "source_sha256")
        value["entity_binding"] = (
            {field: value.get(field) for field in identity_fields}
            if value.get("deployment_id")
            else None
        )
        return value

    def create_or_get(
        self, *, tenant_id: str, idempotency_key: str, payload_sha256: str,
        runtime_spec: dict[str, Any], runtime_spec_sha256: str, blocker_code: str | None,
        entity_binding: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = time.time()
        request_id = "rb_" + uuid4().hex
        encoded = json.dumps(runtime_spec, sort_keys=True, separators=(",", ":"))
        with self._lock, self._connect() as con:
            existing = con.execute(
                "SELECT * FROM runtime_build_requests WHERE tenant_id = ? AND idempotency_key = ?",
                (tenant_id, idempotency_key),
            ).fetchone()
            if existing:
                row = self._row(existing)
                if row and row["payload_sha256"] != payload_sha256:
                    raise ValueError("idempotency_key_reused_with_different_payload")
                return row, True  # type: ignore[return-value]
            con.execute(
                """INSERT INTO runtime_build_requests
                (request_id, tenant_id, idempotency_key, payload_sha256, runtime_spec_sha256,
                 runtime_spec_json, organization_id, deployment_id, capsule_sha256, policy_sha256,
                 source_sha256, status, approval_state, blocker_code, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_approval', 'required', ?, ?, ?)""",
                (request_id, tenant_id, idempotency_key, payload_sha256, runtime_spec_sha256,
                 encoded,
                 entity_binding.get("organization_id") if entity_binding else None,
                 entity_binding.get("deployment_id") if entity_binding else None,
                 entity_binding.get("capsule_sha256") if entity_binding else None,
                 entity_binding.get("policy_sha256") if entity_binding else None,
                 entity_binding.get("source_sha256") if entity_binding else None,
                 blocker_code, now, now),
            )
            row = con.execute("SELECT * FROM runtime_build_requests WHERE request_id = ?", (request_id,)).fetchone()
        return self._row(row), False  # type: ignore[return-value]

    def get(self, request_id: str, tenant_id: str) -> Optional[dict[str, Any]]:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT * FROM runtime_build_requests WHERE request_id = ? AND tenant_id = ?",
                (request_id, tenant_id),
            ).fetchone()
        return self._row(row)

    def list(self, tenant_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                "SELECT * FROM runtime_build_requests WHERE tenant_id = ? ORDER BY updated_at DESC LIMIT ?",
                (tenant_id, limit),
            ).fetchall()
        decoded = [self._row(row) for row in rows]
        return [row for row in decoded if row is not None]

    def approve(self, request_id: str, tenant_id: str, blocker_code: str | None) -> Optional[dict[str, Any]]:
        now = time.time()
        status = "blocked" if blocker_code else "queued"
        with self._lock, self._connect() as con:
            result = con.execute(
                """UPDATE runtime_build_requests
                   SET status = ?, approval_state = 'approved', blocker_code = ?, approved_at = ?, updated_at = ?
                   WHERE request_id = ? AND tenant_id = ? AND approval_state = 'required'""",
                (status, blocker_code, now, now, request_id, tenant_id),
            )
            if result.rowcount == 0:
                row = con.execute(
                    "SELECT * FROM runtime_build_requests WHERE request_id = ? AND tenant_id = ?",
                    (request_id, tenant_id),
                ).fetchone()
                return self._row(row)
            row = con.execute("SELECT * FROM runtime_build_requests WHERE request_id = ?", (request_id,)).fetchone()
        return self._row(row)


_store: RuntimeBuildStore | None = None
_store_lock = threading.Lock()


def get_runtime_build_store() -> RuntimeBuildStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = RuntimeBuildStore()
        return _store
