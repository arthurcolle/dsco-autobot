"""
Durable execution/event persistence for streaming jobs.

Stores execution lifecycle and event streams on disk so job state survives restarts.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "executions.db"


class ExecutionStore:
    """Durable SQLite-backed store for executions and events."""

    def __init__(self, db_path: str = str(DB_PATH)):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self):
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    migration_name TEXT PRIMARY KEY,
                    applied_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    tool_name TEXT NOT NULL,
                    client_id TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    parent_execution_id TEXT,
                    status TEXT NOT NULL,
                    progress REAL DEFAULT 0,
                    queue_priority INTEGER DEFAULT 0,
                    dedupe_key TEXT,
                    queued_at REAL,
                    started_at REAL,
                    completed_at REAL,
                    last_message TEXT,
                    error TEXT,
                    result_json TEXT,
                    control_state_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    trace_id TEXT,
                    session_id TEXT,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    progress REAL,
                    message TEXT,
                    data_json TEXT,
                    error TEXT,
                    FOREIGN KEY (execution_id) REFERENCES executions(execution_id)
                );

                CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(status);
                CREATE INDEX IF NOT EXISTS idx_executions_dedupe ON executions(dedupe_key);
                CREATE INDEX IF NOT EXISTS idx_executions_updated ON executions(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_events_execution ON execution_events(execution_id);
                CREATE INDEX IF NOT EXISTS idx_events_ts ON execution_events(timestamp);

                CREATE TABLE IF NOT EXISTS workflow_executions (
                    execution_id TEXT PRIMARY KEY,
                    workflow_id TEXT,
                    workflow_name TEXT NOT NULL,
                    workflow_kind TEXT NOT NULL,
                    workflow_type TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    parent_execution_id TEXT,
                    replay_of_execution_id TEXT,
                    idempotency_key TEXT,
                    request_hash TEXT,
                    status TEXT NOT NULL,
                    inputs_json TEXT,
                    output_json TEXT,
                    error TEXT,
                    started_at REAL,
                    completed_at REAL,
                    duration_ms REAL,
                    metadata_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workflow_definitions (
                    workflow_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    tags_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workflow_steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    step_key TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    step_index INTEGER,
                    tool_name TEXT,
                    mode TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    is_side_effect INTEGER DEFAULT 0,
                    side_effect_key TEXT,
                    status TEXT NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    duration_ms REAL,
                    retries INTEGER DEFAULT 0,
                    input_json TEXT,
                    output_json TEXT,
                    error TEXT,
                    metadata_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE (execution_id, step_key),
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                );

                CREATE TABLE IF NOT EXISTS workflow_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    step_key TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    message TEXT,
                    data_json TEXT,
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                );

                CREATE TABLE IF NOT EXISTS workflow_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    step_key TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    artifact_type TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    payload_json TEXT,
                    payload_size_bytes INTEGER DEFAULT 0,
                    source TEXT,
                    metadata_json TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (execution_id, step_key, artifact_type, content_hash),
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                );

                CREATE TABLE IF NOT EXISTS workflow_side_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    step_key TEXT,
                    trace_id TEXT,
                    session_id TEXT,
                    tool_name TEXT,
                    side_effect_key TEXT,
                    idempotency_key TEXT,
                    request_hash TEXT,
                    request_json TEXT,
                    response_json TEXT,
                    status TEXT NOT NULL,
                    error TEXT,
                    replay_policy TEXT,
                    replay_of_execution_id TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (execution_id, step_key, side_effect_key),
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_kind ON workflow_executions(workflow_kind);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_updated ON workflow_executions(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_trace ON workflow_executions(trace_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_session ON workflow_executions(session_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_parent ON workflow_executions(parent_execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_replay_of ON workflow_executions(replay_of_execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_idempotency ON workflow_executions(idempotency_key);
                CREATE INDEX IF NOT EXISTS idx_workflow_definitions_name ON workflow_definitions(workflow_name);
                CREATE INDEX IF NOT EXISTS idx_workflow_definitions_updated ON workflow_definitions(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_workflow_steps_execution ON workflow_steps(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_steps_status ON workflow_steps(status);
                CREATE INDEX IF NOT EXISTS idx_workflow_steps_side_effect ON workflow_steps(is_side_effect, step_key);
                CREATE INDEX IF NOT EXISTS idx_workflow_events_execution ON workflow_events(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_events_ts ON workflow_events(timestamp);
                CREATE INDEX IF NOT EXISTS idx_workflow_artifacts_execution ON workflow_artifacts(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_artifacts_created ON workflow_artifacts(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_execution ON workflow_side_effects(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_trace ON workflow_side_effects(trace_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_created ON workflow_side_effects(created_at DESC);
                """
            )

            # Backfill/migrate older databases that were created before these columns existed.
            self._ensure_column(conn, "executions", "trace_id", "TEXT")
            self._ensure_column(conn, "executions", "session_id", "TEXT")
            self._ensure_column(conn, "executions", "parent_execution_id", "TEXT")
            self._ensure_column(conn, "execution_events", "trace_id", "TEXT")
            self._ensure_column(conn, "execution_events", "session_id", "TEXT")
            self._ensure_column(conn, "workflow_executions", "trace_id", "TEXT")
            self._ensure_column(conn, "workflow_executions", "session_id", "TEXT")
            self._ensure_column(conn, "workflow_executions", "parent_execution_id", "TEXT")
            self._ensure_column(conn, "workflow_executions", "replay_of_execution_id", "TEXT")
            self._ensure_column(conn, "workflow_executions", "idempotency_key", "TEXT")
            self._ensure_column(conn, "workflow_executions", "request_hash", "TEXT")
            self._ensure_column(conn, "workflow_steps", "trace_id", "TEXT")
            self._ensure_column(conn, "workflow_steps", "session_id", "TEXT")
            self._ensure_column(conn, "workflow_steps", "is_side_effect", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "workflow_steps", "side_effect_key", "TEXT")
            self._ensure_column(conn, "workflow_events", "trace_id", "TEXT")
            self._ensure_column(conn, "workflow_events", "session_id", "TEXT")
            self._run_migrations(conn)

    def _migration_applied(self, conn: sqlite3.Connection, migration_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM schema_migrations
            WHERE migration_name = ?
            LIMIT 1
            """,
            (migration_name,),
        ).fetchone()
        return bool(row)

    def _mark_migration_applied(self, conn: sqlite3.Connection, migration_name: str):
        conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (migration_name, applied_at)
            VALUES (?, ?)
            """,
            (migration_name, time.time()),
        )

    def _run_migrations(self, conn: sqlite3.Connection):
        # All migrations must be idempotent and safe to run multiple times.
        migrations = [
            ("2026_02_22_side_effect_journal", self._migration_side_effect_journal),
        ]
        for migration_name, migration_fn in migrations:
            if self._migration_applied(conn, migration_name):
                continue
            migration_fn(conn)
            self._mark_migration_applied(conn, migration_name)

    def _migration_side_effect_journal(self, conn: sqlite3.Connection):
        self._ensure_column(conn, "workflow_steps", "is_side_effect", "INTEGER DEFAULT 0")
        self._ensure_column(conn, "workflow_steps", "side_effect_key", "TEXT")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS workflow_side_effects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                step_key TEXT,
                trace_id TEXT,
                session_id TEXT,
                tool_name TEXT,
                side_effect_key TEXT,
                idempotency_key TEXT,
                request_hash TEXT,
                request_json TEXT,
                response_json TEXT,
                status TEXT NOT NULL,
                error TEXT,
                replay_policy TEXT,
                replay_of_execution_id TEXT,
                created_at REAL NOT NULL,
                UNIQUE (execution_id, step_key, side_effect_key),
                FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
            );

            CREATE INDEX IF NOT EXISTS idx_workflow_executions_parent ON workflow_executions(parent_execution_id);
            CREATE INDEX IF NOT EXISTS idx_workflow_executions_replay_of ON workflow_executions(replay_of_execution_id);
            CREATE INDEX IF NOT EXISTS idx_workflow_steps_side_effect ON workflow_steps(is_side_effect, step_key);
            CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_execution ON workflow_side_effects(execution_id);
            CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_trace ON workflow_side_effects(trace_id);
            CREATE INDEX IF NOT EXISTS idx_workflow_side_effects_created ON workflow_side_effects(created_at DESC);
            """
        )

    @staticmethod
    def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"] == column for row in rows)

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, column_type: str):
        if self._column_exists(conn, table, column):
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def upsert_execution(
        self,
        *,
        execution_id: str,
        tool_name: str,
        client_id: Optional[str],
        trace_id: Optional[str],
        session_id: Optional[str],
        parent_execution_id: Optional[str],
        status: str,
        progress: float,
        queue_priority: int,
        dedupe_key: Optional[str],
        queued_at: Optional[float],
        started_at: Optional[float],
        completed_at: Optional[float],
        last_message: Optional[str] = None,
        error: Optional[str] = None,
        result: Optional[Any] = None,
        control_state: Optional[Dict[str, Any]] = None,
    ):
        now = time.time()
        result_json = json.dumps(result, default=str) if result is not None else None
        control_json = json.dumps(control_state or {}, default=str)

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO executions (
                        execution_id, tool_name, client_id, trace_id, session_id, parent_execution_id, status, progress,
                        queue_priority, dedupe_key, queued_at, started_at, completed_at,
                        last_message, error, result_json, control_state_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id) DO UPDATE SET
                        tool_name = excluded.tool_name,
                        client_id = excluded.client_id,
                        trace_id = COALESCE(excluded.trace_id, executions.trace_id),
                        session_id = COALESCE(excluded.session_id, executions.session_id),
                        parent_execution_id = COALESCE(excluded.parent_execution_id, executions.parent_execution_id),
                        status = excluded.status,
                        progress = excluded.progress,
                        queue_priority = excluded.queue_priority,
                        dedupe_key = excluded.dedupe_key,
                        queued_at = excluded.queued_at,
                        started_at = excluded.started_at,
                        completed_at = excluded.completed_at,
                        last_message = COALESCE(excluded.last_message, executions.last_message),
                        error = COALESCE(excluded.error, executions.error),
                        result_json = COALESCE(excluded.result_json, executions.result_json),
                        control_state_json = excluded.control_state_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        execution_id,
                        tool_name,
                        client_id,
                        trace_id,
                        session_id,
                        parent_execution_id,
                        status,
                        progress,
                        int(queue_priority),
                        dedupe_key,
                        queued_at,
                        started_at,
                        completed_at,
                        last_message,
                        error,
                        result_json,
                        control_json,
                        now,
                        now,
                    ),
                )

    def append_event(
        self,
        *,
        execution_id: str,
        trace_id: Optional[str],
        session_id: Optional[str],
        event_type: str,
        timestamp: float,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        data: Any = None,
        error: Optional[str] = None,
    ):
        data_json = json.dumps(data, default=str) if data is not None else None
        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO execution_events (
                        execution_id, trace_id, session_id, event_type, timestamp, progress, message, data_json, error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        trace_id,
                        session_id,
                        event_type,
                        timestamp,
                        progress,
                        message,
                        data_json,
                        error,
                    ),
                )

    def get_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            if data.get("control_state_json"):
                data["control_state"] = json.loads(data["control_state_json"])
            else:
                data["control_state"] = {}
            if data.get("result_json"):
                data["result"] = json.loads(data["result_json"])
            else:
                data["result"] = None
            return data

    def get_events(self, execution_id: str, limit: int = 5000) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM execution_events
                WHERE execution_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (execution_id, limit),
            ).fetchall()
            events: List[Dict[str, Any]] = []
            for row in rows:
                d = dict(row)
                if d.get("data_json"):
                    d["data"] = json.loads(d["data_json"])
                else:
                    d["data"] = None
                events.append(d)
            return events

    def list_executions(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        query = "SELECT * FROM executions"
        params: List[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY COALESCE(queued_at, created_at) DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            out: List[Dict[str, Any]] = []
            for row in rows:
                d = dict(row)
                d["control_state"] = json.loads(d["control_state_json"]) if d.get("control_state_json") else {}
                out.append(d)
            return out

    def find_recent_by_dedupe_key(
        self,
        dedupe_key: str,
        since_ts: float,
    ) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT execution_id, status, queued_at
                FROM executions
                WHERE dedupe_key = ?
                  AND COALESCE(queued_at, created_at) >= ?
                  AND status IN ('queued', 'running', 'completed')
                ORDER BY COALESCE(queued_at, created_at) DESC
                LIMIT 1
                """,
                (dedupe_key, since_ts),
            ).fetchone()
            return dict(row) if row else None

    def cleanup_old(self, max_age_seconds: int = 3600) -> int:
        cutoff = time.time() - max_age_seconds
        with self._lock:
            with self._conn() as conn:
                rows = conn.execute(
                    """
                    SELECT execution_id
                    FROM executions
                    WHERE completed_at IS NOT NULL
                      AND completed_at < ?
                    """,
                    (cutoff,),
                ).fetchall()
                execution_ids = [row["execution_id"] for row in rows]
                if not execution_ids:
                    return 0
                placeholders = ", ".join("?" for _ in execution_ids)
                conn.execute(
                    f"DELETE FROM execution_events WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                conn.execute(
                    f"DELETE FROM executions WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                return len(execution_ids)

    def get_usage_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Aggregate durable execution metrics for the requested lookback window."""
        since_ts = time.time() - max(1, int(hours)) * 3600
        with self._conn() as conn:
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                       SUM(CASE WHEN status = 'error' OR status = 'failed' THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                FROM executions
                WHERE COALESCE(queued_at, created_at) >= ?
                """,
                (since_ts,),
            ).fetchone()

            tool_rows = conn.execute(
                """
                SELECT tool_name,
                       COUNT(*) AS calls,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                       AVG(progress) AS avg_progress
                FROM executions
                WHERE COALESCE(queued_at, created_at) >= ?
                GROUP BY tool_name
                ORDER BY calls DESC
                LIMIT 25
                """,
                (since_ts,),
            ).fetchall()

            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM executions
                WHERE COALESCE(queued_at, created_at) >= ?
                GROUP BY status
                """,
                (since_ts,),
            ).fetchall()

            event_rows = conn.execute(
                """
                SELECT event_type, COUNT(*) AS count
                FROM execution_events
                WHERE timestamp >= ?
                GROUP BY event_type
                ORDER BY count DESC
                """,
                (since_ts,),
            ).fetchall()

        total = dict(total_row) if total_row else {}
        return {
            "window_hours": max(1, int(hours)),
            "totals": {
                "executions": int(total.get("total", 0) or 0),
                "completed": int(total.get("completed", 0) or 0),
                "failed": int(total.get("failed", 0) or 0),
                "cancelled": int(total.get("cancelled", 0) or 0),
            },
            "by_status": {row["status"]: int(row["count"]) for row in status_rows},
            "by_tool": [
                {
                    "tool_name": row["tool_name"],
                    "calls": int(row["calls"]),
                    "completed": int(row["completed"] or 0),
                    "avg_progress": float(row["avg_progress"] or 0.0),
                }
                for row in tool_rows
            ],
            "event_volume": {row["event_type"]: int(row["count"]) for row in event_rows},
        }

    # Workflow durability / lineage APIs.

    @staticmethod
    def _safe_json_loads(value: Optional[str], default: Any) -> Any:
        if not value:
            return default
        try:
            return json.loads(value)
        except Exception:
            return default

    @staticmethod
    def _int_env(name: str, default: int, minimum: int = 1) -> int:
        raw = os.getenv(name, str(default)).strip()
        try:
            value = int(raw)
        except Exception:
            value = default
        return max(minimum, value)

    @classmethod
    def _compact_payload_for_storage(
        cls,
        payload: Any,
        *,
        max_bytes: int,
    ) -> Dict[str, Any]:
        raw_json = json.dumps(payload, default=str) if payload is not None else "null"
        raw_bytes = len(raw_json.encode("utf-8"))
        raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        if raw_bytes <= max_bytes:
            return {
                "json": raw_json,
                "original_size_bytes": raw_bytes,
                "stored_size_bytes": raw_bytes,
                "content_hash": raw_hash,
                "truncated": False,
            }

        envelope = {
            "_truncated": True,
            "original_size_bytes": raw_bytes,
            "original_sha256": raw_hash,
            "preview": raw_json[:max_bytes],
        }
        compact_json = json.dumps(envelope, default=str)
        compact_bytes = len(compact_json.encode("utf-8"))
        return {
            "json": compact_json,
            "original_size_bytes": raw_bytes,
            "stored_size_bytes": compact_bytes,
            "content_hash": raw_hash,
            "truncated": True,
        }

    def upsert_workflow_definition(
        self,
        *,
        workflow_id: str,
        workflow_name: str,
        definition: Dict[str, Any],
        tags: Optional[List[str]] = None,
    ):
        now = time.time()
        definition_json = json.dumps(definition, default=str)
        tags_json = json.dumps(tags or [], default=str)
        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO workflow_definitions (
                        workflow_id, workflow_name, definition_json, tags_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(workflow_id) DO UPDATE SET
                        workflow_name = excluded.workflow_name,
                        definition_json = excluded.definition_json,
                        tags_json = excluded.tags_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        workflow_id,
                        workflow_name,
                        definition_json,
                        tags_json,
                        now,
                        now,
                    ),
                )

    def get_workflow_definition(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workflow_definitions
                WHERE workflow_id = ?
                """,
                (workflow_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            data["definition"] = self._safe_json_loads(data.get("definition_json"), {})
            data["tags"] = self._safe_json_loads(data.get("tags_json"), [])
            return data

    def list_workflow_definitions(self, limit: int = 500) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workflow_definitions
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

        definitions: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["definition"] = self._safe_json_loads(data.get("definition_json"), {})
            data["tags"] = self._safe_json_loads(data.get("tags_json"), [])
            definitions.append(data)
        return definitions

    def upsert_workflow_execution(
        self,
        *,
        execution_id: str,
        workflow_id: Optional[str],
        workflow_name: str,
        workflow_kind: str,
        workflow_type: Optional[str],
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        parent_execution_id: Optional[str] = None,
        replay_of_execution_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        request_hash: Optional[str] = None,
        status: str,
        inputs: Any = None,
        output: Any = None,
        error: Optional[str] = None,
        started_at: Optional[float] = None,
        completed_at: Optional[float] = None,
        duration_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        now = time.time()
        inputs_json = json.dumps(inputs, default=str) if inputs is not None else None
        output_json = json.dumps(output, default=str) if output is not None else None
        metadata_json = json.dumps(metadata or {}, default=str)

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO workflow_executions (
                        execution_id, workflow_id, workflow_name, workflow_kind, workflow_type,
                        trace_id, session_id, parent_execution_id, replay_of_execution_id,
                        idempotency_key, request_hash,
                        status, inputs_json, output_json, error,
                        started_at, completed_at, duration_ms, metadata_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id) DO UPDATE SET
                        workflow_id = COALESCE(excluded.workflow_id, workflow_executions.workflow_id),
                        workflow_name = COALESCE(excluded.workflow_name, workflow_executions.workflow_name),
                        workflow_kind = COALESCE(excluded.workflow_kind, workflow_executions.workflow_kind),
                        workflow_type = COALESCE(excluded.workflow_type, workflow_executions.workflow_type),
                        trace_id = COALESCE(excluded.trace_id, workflow_executions.trace_id),
                        session_id = COALESCE(excluded.session_id, workflow_executions.session_id),
                        parent_execution_id = COALESCE(excluded.parent_execution_id, workflow_executions.parent_execution_id),
                        replay_of_execution_id = COALESCE(excluded.replay_of_execution_id, workflow_executions.replay_of_execution_id),
                        idempotency_key = COALESCE(excluded.idempotency_key, workflow_executions.idempotency_key),
                        request_hash = COALESCE(excluded.request_hash, workflow_executions.request_hash),
                        status = excluded.status,
                        inputs_json = COALESCE(excluded.inputs_json, workflow_executions.inputs_json),
                        output_json = COALESCE(excluded.output_json, workflow_executions.output_json),
                        error = COALESCE(excluded.error, workflow_executions.error),
                        started_at = COALESCE(excluded.started_at, workflow_executions.started_at),
                        completed_at = COALESCE(excluded.completed_at, workflow_executions.completed_at),
                        duration_ms = COALESCE(excluded.duration_ms, workflow_executions.duration_ms),
                        metadata_json = COALESCE(excluded.metadata_json, workflow_executions.metadata_json),
                        updated_at = excluded.updated_at
                    """,
                    (
                        execution_id,
                        workflow_id,
                        workflow_name,
                        workflow_kind,
                        workflow_type,
                        trace_id,
                        session_id,
                        parent_execution_id,
                        replay_of_execution_id,
                        idempotency_key,
                        request_hash,
                        status,
                        inputs_json,
                        output_json,
                        error,
                        started_at,
                        completed_at,
                        duration_ms,
                        metadata_json,
                        now,
                        now,
                    ),
                )

    def upsert_workflow_step(
        self,
        *,
        execution_id: str,
        step_key: str,
        step_name: str,
        step_index: Optional[int],
        tool_name: Optional[str],
        mode: Optional[str],
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        is_side_effect: Optional[bool] = None,
        side_effect_key: Optional[str] = None,
        status: str,
        input_data: Any = None,
        output_data: Any = None,
        error: Optional[str] = None,
        started_at: Optional[float] = None,
        completed_at: Optional[float] = None,
        duration_ms: Optional[float] = None,
        retries: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        now = time.time()
        input_json = json.dumps(input_data, default=str) if input_data is not None else None
        output_json = json.dumps(output_data, default=str) if output_data is not None else None
        metadata_json = json.dumps(metadata or {}, default=str)
        is_side_effect_value: Optional[int]
        if is_side_effect is True:
            is_side_effect_value = 1
        elif is_side_effect is False:
            is_side_effect_value = 0
        else:
            is_side_effect_value = None

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO workflow_steps (
                        execution_id, step_key, step_name, step_index, tool_name, mode,
                        trace_id, session_id, is_side_effect, side_effect_key, status,
                        started_at, completed_at, duration_ms, retries,
                        input_json, output_json, error, metadata_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id, step_key) DO UPDATE SET
                        step_name = COALESCE(excluded.step_name, workflow_steps.step_name),
                        step_index = COALESCE(excluded.step_index, workflow_steps.step_index),
                        tool_name = COALESCE(excluded.tool_name, workflow_steps.tool_name),
                        mode = COALESCE(excluded.mode, workflow_steps.mode),
                        trace_id = COALESCE(excluded.trace_id, workflow_steps.trace_id),
                        session_id = COALESCE(excluded.session_id, workflow_steps.session_id),
                        is_side_effect = COALESCE(excluded.is_side_effect, workflow_steps.is_side_effect),
                        side_effect_key = COALESCE(excluded.side_effect_key, workflow_steps.side_effect_key),
                        status = excluded.status,
                        started_at = COALESCE(excluded.started_at, workflow_steps.started_at),
                        completed_at = COALESCE(excluded.completed_at, workflow_steps.completed_at),
                        duration_ms = COALESCE(excluded.duration_ms, workflow_steps.duration_ms),
                        retries = COALESCE(excluded.retries, workflow_steps.retries),
                        input_json = COALESCE(excluded.input_json, workflow_steps.input_json),
                        output_json = COALESCE(excluded.output_json, workflow_steps.output_json),
                        error = COALESCE(excluded.error, workflow_steps.error),
                        metadata_json = COALESCE(excluded.metadata_json, workflow_steps.metadata_json),
                        updated_at = excluded.updated_at
                    """,
                    (
                        execution_id,
                        step_key,
                        step_name,
                        step_index,
                        tool_name,
                        mode,
                        trace_id,
                        session_id,
                        is_side_effect_value,
                        side_effect_key,
                        status,
                        started_at,
                        completed_at,
                        duration_ms,
                        int(retries or 0),
                        input_json,
                        output_json,
                        error,
                        metadata_json,
                        now,
                        now,
                    ),
                )

    def append_workflow_event(
        self,
        *,
        execution_id: str,
        event_type: str,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        step_key: Optional[str] = None,
        message: Optional[str] = None,
        data: Any = None,
    ):
        event_ts = timestamp if timestamp is not None else time.time()
        data_json = json.dumps(data, default=str) if data is not None else None

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO workflow_events (
                        execution_id, step_key, trace_id, session_id, event_type, timestamp, message, data_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        step_key,
                        trace_id,
                        session_id,
                        event_type,
                        event_ts,
                        message,
                        data_json,
                    ),
                )

    def append_workflow_artifact(
        self,
        *,
        execution_id: str,
        step_key: Optional[str],
        artifact_type: str,
        payload: Any,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        source: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        allowed_types = {
            part.strip()
            for part in os.getenv("WORKFLOW_ARTIFACT_TYPES", "input,output,error").split(",")
            if part.strip()
        }
        if allowed_types and artifact_type not in allowed_types:
            return None

        max_bytes = self._int_env("WORKFLOW_ARTIFACT_MAX_PAYLOAD_BYTES", 262_144, minimum=256)
        compact_payload = self._compact_payload_for_storage(payload, max_bytes=max_bytes)
        payload_json = compact_payload["json"]
        payload_size = int(compact_payload["stored_size_bytes"])
        content_hash = str(compact_payload["content_hash"])
        created_at = time.time()
        artifact_metadata = dict(metadata or {})
        if compact_payload["truncated"]:
            artifact_metadata.update(
                {
                    "truncated": True,
                    "original_size_bytes": int(compact_payload["original_size_bytes"]),
                    "stored_size_bytes": payload_size,
                    "max_payload_bytes": max_bytes,
                }
            )
        metadata_json = json.dumps(artifact_metadata, default=str)

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO workflow_artifacts (
                        execution_id, step_key, trace_id, session_id,
                        artifact_type, content_hash, payload_json, payload_size_bytes,
                        source, metadata_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        step_key,
                        trace_id,
                        session_id,
                        artifact_type,
                        content_hash,
                        payload_json,
                        payload_size,
                        source,
                        metadata_json,
                        created_at,
                    ),
                )

        return {
            "execution_id": execution_id,
            "step_key": step_key,
            "artifact_type": artifact_type,
            "content_hash": content_hash,
            "payload_size_bytes": payload_size,
            "source": source,
            "created_at": created_at,
            "metadata": artifact_metadata,
            "truncated": bool(compact_payload["truncated"]),
            "original_size_bytes": int(compact_payload["original_size_bytes"]),
        }

    def append_workflow_side_effect(
        self,
        *,
        execution_id: str,
        step_key: Optional[str],
        tool_name: Optional[str],
        side_effect_key: str,
        status: str,
        request_data: Any = None,
        response_data: Any = None,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        request_hash: Optional[str] = None,
        error: Optional[str] = None,
        replay_policy: Optional[str] = None,
        replay_of_execution_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        created_at = time.time()
        max_bytes = self._int_env("WORKFLOW_SIDE_EFFECT_MAX_PAYLOAD_BYTES", 131_072, minimum=256)
        compact_request = self._compact_payload_for_storage(request_data, max_bytes=max_bytes)
        compact_response = self._compact_payload_for_storage(response_data, max_bytes=max_bytes)
        request_json = compact_request["json"] if request_data is not None else None
        response_json = compact_response["json"] if response_data is not None else None
        if not request_hash and request_data is not None:
            request_hash = str(compact_request["content_hash"])

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO workflow_side_effects (
                        execution_id, step_key, trace_id, session_id, tool_name,
                        side_effect_key, idempotency_key, request_hash,
                        request_json, response_json, status, error,
                        replay_policy, replay_of_execution_id, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        step_key,
                        trace_id,
                        session_id,
                        tool_name,
                        side_effect_key,
                        idempotency_key,
                        request_hash,
                        request_json,
                        response_json,
                        status,
                        error,
                        replay_policy,
                        replay_of_execution_id,
                        created_at,
                    ),
                )

        return {
            "execution_id": execution_id,
            "step_key": step_key,
            "tool_name": tool_name,
            "side_effect_key": side_effect_key,
            "status": status,
            "trace_id": trace_id,
            "session_id": session_id,
            "idempotency_key": idempotency_key,
            "request_hash": request_hash,
            "error": error,
            "replay_policy": replay_policy,
            "replay_of_execution_id": replay_of_execution_id,
            "created_at": created_at,
            "request_truncated": bool(compact_request["truncated"]),
            "response_truncated": bool(compact_response["truncated"]),
            "request_original_size_bytes": int(compact_request["original_size_bytes"]),
            "response_original_size_bytes": int(compact_response["original_size_bytes"]),
        }

    def list_workflow_side_effects(
        self,
        *,
        execution_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        step_key: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM workflow_side_effects"
        clauses: List[str] = []
        params: List[Any] = []
        if execution_id:
            clauses.append("execution_id = ?")
            params.append(execution_id)
        if trace_id:
            clauses.append("trace_id = ?")
            params.append(trace_id)
        if step_key:
            clauses.append("step_key = ?")
            params.append(step_key)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id ASC LIMIT ?"
        params.append(max(1, int(limit)))

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        effects: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["request"] = self._safe_json_loads(data.get("request_json"), None)
            data["response"] = self._safe_json_loads(data.get("response_json"), None)
            effects.append(data)
        return effects

    def list_workflow_artifacts(
        self,
        execution_id: str,
        *,
        step_key: Optional[str] = None,
        artifact_type: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM workflow_artifacts WHERE execution_id = ?"
        params: List[Any] = [execution_id]
        if step_key:
            query += " AND step_key = ?"
            params.append(step_key)
        if artifact_type:
            query += " AND artifact_type = ?"
            params.append(artifact_type)
        query += " ORDER BY id ASC LIMIT ?"
        params.append(max(1, int(limit)))

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        artifacts: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["payload"] = self._safe_json_loads(data.get("payload_json"), None)
            data["metadata"] = self._safe_json_loads(data.get("metadata_json"), {})
            artifacts.append(data)
        return artifacts

    def find_recent_workflow_by_idempotency(
        self,
        *,
        workflow_kind: str,
        workflow_id: Optional[str],
        idempotency_key: str,
        since_ts: float,
    ) -> Optional[Dict[str, Any]]:
        clauses = [
            "workflow_kind = ?",
            "idempotency_key = ?",
            "COALESCE(started_at, created_at) >= ?",
        ]
        params: List[Any] = [workflow_kind, idempotency_key, since_ts]
        if workflow_id:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)

        with self._conn() as conn:
            row = conn.execute(
                f"""
                SELECT *
                FROM workflow_executions
                WHERE {' AND '.join(clauses)}
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["inputs"] = self._safe_json_loads(data.get("inputs_json"), {})
        data["output"] = self._safe_json_loads(data.get("output_json"), None)
        data["metadata"] = self._safe_json_loads(data.get("metadata_json"), {})
        return data

    def list_workflow_executions(
        self,
        *,
        status: Optional[str] = None,
        workflow_kind: Optional[str] = None,
        workflow_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM workflow_executions"
        clauses: List[str] = []
        params: List[Any] = []

        if status:
            clauses.append("status = ?")
            params.append(status)
        if workflow_kind:
            clauses.append("workflow_kind = ?")
            params.append(workflow_kind)
        if workflow_id:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)
        if trace_id:
            clauses.append("trace_id = ?")
            params.append(trace_id)
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)

        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        query += " ORDER BY COALESCE(started_at, created_at) DESC LIMIT ?"
        params.append(max(1, int(limit)))

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        executions: List[Dict[str, Any]] = []
        for row in rows:
            execution = dict(row)
            execution["inputs"] = self._safe_json_loads(execution.get("inputs_json"), {})
            execution["output"] = self._safe_json_loads(execution.get("output_json"), None)
            execution["metadata"] = self._safe_json_loads(execution.get("metadata_json"), {})
            executions.append(execution)
        return executions

    def get_workflow_execution(
        self,
        execution_id: str,
        *,
        include_steps: bool = True,
        include_events: bool = False,
        include_artifacts: bool = False,
        include_side_effects: bool = False,
        event_limit: int = 200,
        artifact_limit: int = 1000,
        side_effect_limit: int = 1000,
    ) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workflow_executions
                WHERE execution_id = ?
                """,
                (execution_id,),
            ).fetchone()
            if not row:
                return None

            execution = dict(row)
            execution["inputs"] = self._safe_json_loads(execution.get("inputs_json"), {})
            execution["output"] = self._safe_json_loads(execution.get("output_json"), None)
            execution["metadata"] = self._safe_json_loads(execution.get("metadata_json"), {})

            if include_steps:
                step_rows = conn.execute(
                    """
                    SELECT *
                    FROM workflow_steps
                    WHERE execution_id = ?
                    ORDER BY COALESCE(step_index, id) ASC, id ASC
                    """,
                    (execution_id,),
                ).fetchall()
                steps: List[Dict[str, Any]] = []
                for step_row in step_rows:
                    step = dict(step_row)
                    step["input"] = self._safe_json_loads(step.get("input_json"), None)
                    step["output"] = self._safe_json_loads(step.get("output_json"), None)
                    step["metadata"] = self._safe_json_loads(step.get("metadata_json"), {})
                    step["is_side_effect"] = bool(step.get("is_side_effect") or 0)
                    steps.append(step)
                execution["steps"] = steps

            if include_events:
                event_rows = conn.execute(
                    """
                    SELECT *
                    FROM workflow_events
                    WHERE execution_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (execution_id, max(1, int(event_limit))),
                ).fetchall()
                events: List[Dict[str, Any]] = []
                for event_row in reversed(event_rows):
                    event = dict(event_row)
                    event["data"] = self._safe_json_loads(event.get("data_json"), None)
                    events.append(event)
                execution["events"] = events

            if include_artifacts:
                artifact_rows = conn.execute(
                    """
                    SELECT *
                    FROM workflow_artifacts
                    WHERE execution_id = ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (execution_id, max(1, int(artifact_limit))),
                ).fetchall()
                artifacts: List[Dict[str, Any]] = []
                for artifact_row in artifact_rows:
                    artifact = dict(artifact_row)
                    artifact["payload"] = self._safe_json_loads(artifact.get("payload_json"), None)
                    artifact["metadata"] = self._safe_json_loads(artifact.get("metadata_json"), {})
                    artifacts.append(artifact)
                execution["artifacts"] = artifacts

            if include_side_effects:
                side_effect_rows = conn.execute(
                    """
                    SELECT *
                    FROM workflow_side_effects
                    WHERE execution_id = ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (execution_id, max(1, int(side_effect_limit))),
                ).fetchall()
                side_effects: List[Dict[str, Any]] = []
                for side_effect_row in side_effect_rows:
                    side_effect = dict(side_effect_row)
                    side_effect["request"] = self._safe_json_loads(side_effect.get("request_json"), None)
                    side_effect["response"] = self._safe_json_loads(side_effect.get("response_json"), None)
                    side_effects.append(side_effect)
                execution["side_effects"] = side_effects

            return execution

    def get_workflow_analytics(self, hours: int = 24) -> Dict[str, Any]:
        since_ts = time.time() - max(1, int(hours)) * 3600
        with self._conn() as conn:
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                       AVG(duration_ms) AS avg_duration_ms
                FROM workflow_executions
                WHERE COALESCE(started_at, created_at) >= ?
                """,
                (since_ts,),
            ).fetchone()

            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM workflow_executions
                WHERE COALESCE(started_at, created_at) >= ?
                GROUP BY status
                ORDER BY count DESC
                """,
                (since_ts,),
            ).fetchall()

            kind_rows = conn.execute(
                """
                SELECT workflow_kind, COUNT(*) AS count
                FROM workflow_executions
                WHERE COALESCE(started_at, created_at) >= ?
                GROUP BY workflow_kind
                ORDER BY count DESC
                """,
                (since_ts,),
            ).fetchall()

            workflow_rows = conn.execute(
                """
                SELECT workflow_name,
                       workflow_kind,
                       COUNT(*) AS runs,
                       AVG(duration_ms) AS avg_duration_ms,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed
                FROM workflow_executions
                WHERE COALESCE(started_at, created_at) >= ?
                GROUP BY workflow_name, workflow_kind
                ORDER BY runs DESC
                LIMIT 25
                """,
                (since_ts,),
            ).fetchall()

            step_rows = conn.execute(
                """
                SELECT mode, status, COUNT(*) AS count
                FROM workflow_steps
                WHERE COALESCE(started_at, created_at) >= ?
                GROUP BY mode, status
                ORDER BY count DESC
                """,
                (since_ts,),
            ).fetchall()

        total = dict(total_row) if total_row else {}
        return {
            "window_hours": max(1, int(hours)),
            "totals": {
                "executions": int(total.get("total", 0) or 0),
                "completed": int(total.get("completed", 0) or 0),
                "failed": int(total.get("failed", 0) or 0),
                "avg_duration_ms": float(total.get("avg_duration_ms", 0.0) or 0.0),
            },
            "status_mix": {row["status"]: int(row["count"]) for row in status_rows},
            "kind_mix": {row["workflow_kind"]: int(row["count"]) for row in kind_rows},
            "top_workflows": [
                {
                    "workflow_name": row["workflow_name"],
                    "workflow_kind": row["workflow_kind"],
                    "runs": int(row["runs"]),
                    "completed": int(row["completed"] or 0),
                    "avg_duration_ms": float(row["avg_duration_ms"] or 0.0),
                }
                for row in workflow_rows
            ],
            "step_mix": [
                {
                    "mode": row["mode"],
                    "status": row["status"],
                    "count": int(row["count"]),
                }
                for row in step_rows
            ],
        }

    def get_workflow_chain_analytics(
        self,
        *,
        hours: int = 24,
        limit: int = 20,
        min_chain_length: int = 2,
    ) -> Dict[str, Any]:
        since_ts = time.time() - max(1, int(hours)) * 3600
        min_chain_length = max(1, int(min_chain_length))
        limit = max(1, int(limit))

        with self._conn() as conn:
            step_rows = conn.execute(
                """
                SELECT execution_id,
                       COALESCE(NULLIF(tool_name, ''), step_name) AS node,
                       COALESCE(step_index, id) AS step_order,
                       status
                FROM workflow_steps
                WHERE COALESCE(started_at, created_at) >= ?
                ORDER BY execution_id ASC, step_order ASC, id ASC
                """,
                (since_ts,),
            ).fetchall()

            execution_rows = conn.execute(
                """
                SELECT execution_id, status, duration_ms
                FROM workflow_executions
                WHERE COALESCE(started_at, created_at) >= ?
                """,
                (since_ts,),
            ).fetchall()

            hotspot_rows = conn.execute(
                """
                SELECT COALESCE(NULLIF(tool_name, ''), step_name) AS step,
                       mode,
                       COUNT(*) AS failures
                FROM workflow_steps
                WHERE status = 'failed'
                  AND COALESCE(started_at, created_at) >= ?
                GROUP BY step, mode
                ORDER BY failures DESC
                LIMIT 25
                """,
                (since_ts,),
            ).fetchall()

        chains_by_execution: Dict[str, List[str]] = {}
        for row in step_rows:
            ex_id = row["execution_id"]
            node = row["node"] or "unknown_step"
            chains_by_execution.setdefault(ex_id, []).append(node)

        execution_status: Dict[str, str] = {
            row["execution_id"]: row["status"] or "unknown"
            for row in execution_rows
        }
        execution_duration: Dict[str, float] = {
            row["execution_id"]: float(row["duration_ms"] or 0.0)
            for row in execution_rows
        }

        chain_agg: Dict[str, Dict[str, Any]] = {}
        for ex_id, nodes in chains_by_execution.items():
            if len(nodes) < min_chain_length:
                continue
            chain_key = " -> ".join(nodes)
            bucket = chain_agg.setdefault(
                chain_key,
                {
                    "chain": chain_key,
                    "steps": nodes,
                    "runs": 0,
                    "completed": 0,
                    "failed": 0,
                    "avg_duration_ms": 0.0,
                    "_durations": [],
                },
            )
            bucket["runs"] += 1
            status = execution_status.get(ex_id)
            if status == "completed":
                bucket["completed"] += 1
            elif status == "failed":
                bucket["failed"] += 1
            bucket["_durations"].append(execution_duration.get(ex_id, 0.0))

        chains: List[Dict[str, Any]] = []
        for bucket in chain_agg.values():
            durations = bucket.pop("_durations", [])
            bucket["avg_duration_ms"] = float(sum(durations) / max(1, len(durations)))
            chains.append(bucket)
        chains.sort(key=lambda item: (-int(item["runs"]), item["chain"]))

        return {
            "window_hours": max(1, int(hours)),
            "chains": chains[:limit],
            "failure_hotspots": [
                {
                    "step": row["step"],
                    "mode": row["mode"],
                    "failures": int(row["failures"]),
                }
                for row in hotspot_rows
            ],
        }

    def get_workflow_lineage(
        self,
        execution_id: str,
        *,
        depth: int = 4,
    ) -> Dict[str, Any]:
        depth = max(1, int(depth))
        with self._conn() as conn:
            root_row = conn.execute(
                """
                SELECT execution_id, workflow_id, workflow_name, workflow_kind, status,
                       trace_id, session_id, parent_execution_id, replay_of_execution_id,
                       started_at, completed_at
                FROM workflow_executions
                WHERE execution_id = ?
                """,
                (execution_id,),
            ).fetchone()
            if not root_row:
                return {"root_execution_id": execution_id, "nodes": [], "edges": []}

            nodes: Dict[str, Dict[str, Any]] = {}
            edges: Set[str] = set()
            queue: List[tuple[str, int]] = [(execution_id, 0)]
            visited: Set[str] = set()

            while queue:
                current_id, level = queue.pop(0)
                if current_id in visited or level > depth:
                    continue
                visited.add(current_id)

                row = conn.execute(
                    """
                    SELECT execution_id, workflow_id, workflow_name, workflow_kind, status,
                           trace_id, session_id, parent_execution_id, replay_of_execution_id,
                           started_at, completed_at
                    FROM workflow_executions
                    WHERE execution_id = ?
                    """,
                    (current_id,),
                ).fetchone()
                if not row:
                    continue

                node = dict(row)
                node["depth"] = level
                nodes[current_id] = node

                parent_ids = [
                    row["parent_execution_id"],
                    row["replay_of_execution_id"],
                ]
                for parent_id in parent_ids:
                    if not parent_id:
                        continue
                    edge_key = f"{parent_id}->{current_id}"
                    edges.add(edge_key)
                    queue.append((parent_id, level + 1))

                child_rows = conn.execute(
                    """
                    SELECT execution_id
                    FROM workflow_executions
                    WHERE parent_execution_id = ? OR replay_of_execution_id = ?
                    """,
                    (current_id, current_id),
                ).fetchall()
                for child in child_rows:
                    child_id = child["execution_id"]
                    edge_key = f"{current_id}->{child_id}"
                    edges.add(edge_key)
                    queue.append((child_id, level + 1))

        edge_records: List[Dict[str, Any]] = []
        for edge in sorted(edges):
            src, dst = edge.split("->", 1)
            edge_type = "parent"
            dst_node = nodes.get(dst, {})
            if dst_node.get("replay_of_execution_id") == src:
                edge_type = "replay_of"
            elif dst_node.get("parent_execution_id") == src:
                edge_type = "parent"
            edge_records.append({"from": src, "to": dst, "type": edge_type})

        node_records = list(nodes.values())
        node_records.sort(key=lambda item: (item.get("depth", 0), item.get("started_at") or 0))
        return {
            "root_execution_id": execution_id,
            "trace_id": root_row["trace_id"],
            "session_id": root_row["session_id"],
            "depth": depth,
            "nodes": node_records,
            "edges": edge_records,
        }

    def get_trace_workflow_lineage(
        self,
        trace_id: str,
        *,
        limit: int = 500,
    ) -> Dict[str, Any]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT execution_id, workflow_id, workflow_name, workflow_kind, status,
                       trace_id, session_id, parent_execution_id, replay_of_execution_id,
                       started_at, completed_at
                FROM workflow_executions
                WHERE trace_id = ?
                ORDER BY COALESCE(started_at, created_at) ASC
                LIMIT ?
                """,
                (trace_id, max(1, int(limit))),
            ).fetchall()

        nodes = [dict(row) for row in rows]
        node_ids = {row["execution_id"] for row in rows}
        edges: List[Dict[str, Any]] = []
        for row in rows:
            current_id = row["execution_id"]
            if row["parent_execution_id"] and row["parent_execution_id"] in node_ids:
                edges.append(
                    {
                        "from": row["parent_execution_id"],
                        "to": current_id,
                        "type": "parent",
                    }
                )
            if row["replay_of_execution_id"] and row["replay_of_execution_id"] in node_ids:
                edges.append(
                    {
                        "from": row["replay_of_execution_id"],
                        "to": current_id,
                        "type": "replay_of",
                    }
                )

        return {
            "trace_id": trace_id,
            "count": len(nodes),
            "nodes": nodes,
            "edges": edges,
        }

    def get_storage_stats(self) -> Dict[str, Any]:
        with self._conn() as conn:
            table_counts = {}
            tables = [
                "executions",
                "execution_events",
                "workflow_executions",
                "workflow_steps",
                "workflow_events",
                "workflow_artifacts",
                "workflow_side_effects",
                "workflow_definitions",
            ]
            for table in tables:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                table_counts[table] = int(row["count"] if row else 0)

            execution_payload_bytes = conn.execute(
                """
                SELECT
                    COALESCE((SELECT SUM(LENGTH(result_json)) FROM executions), 0) AS execution_result_bytes,
                    COALESCE((SELECT SUM(LENGTH(data_json)) FROM execution_events), 0) AS execution_event_bytes
                """
            ).fetchone()

            workflow_payload_bytes = conn.execute(
                """
                SELECT
                    COALESCE((SELECT SUM(LENGTH(inputs_json)) FROM workflow_executions), 0) AS workflow_input_bytes,
                    COALESCE((SELECT SUM(LENGTH(output_json)) FROM workflow_executions), 0) AS workflow_output_bytes,
                    COALESCE((SELECT SUM(LENGTH(payload_json)) FROM workflow_artifacts), 0) AS artifact_payload_bytes,
                    COALESCE((SELECT SUM(LENGTH(request_json)) FROM workflow_side_effects), 0) AS side_effect_request_bytes,
                    COALESCE((SELECT SUM(LENGTH(response_json)) FROM workflow_side_effects), 0) AS side_effect_response_bytes
                """
            ).fetchone()

        db_size_bytes = 0
        wal_size_bytes = 0
        try:
            db_size_bytes = int(self.db_path.stat().st_size)
        except Exception:
            pass
        wal_path = Path(f"{self.db_path}-wal")
        try:
            if wal_path.exists():
                wal_size_bytes = int(wal_path.stat().st_size)
        except Exception:
            pass

        return {
            "db_path": str(self.db_path),
            "db_size_bytes": db_size_bytes,
            "wal_size_bytes": wal_size_bytes,
            "table_counts": table_counts,
            "payload_bytes": {
                "execution_result_bytes": int(execution_payload_bytes["execution_result_bytes"] or 0),
                "execution_event_bytes": int(execution_payload_bytes["execution_event_bytes"] or 0),
                "workflow_input_bytes": int(workflow_payload_bytes["workflow_input_bytes"] or 0),
                "workflow_output_bytes": int(workflow_payload_bytes["workflow_output_bytes"] or 0),
                "artifact_payload_bytes": int(workflow_payload_bytes["artifact_payload_bytes"] or 0),
                "side_effect_request_bytes": int(workflow_payload_bytes["side_effect_request_bytes"] or 0),
                "side_effect_response_bytes": int(workflow_payload_bytes["side_effect_response_bytes"] or 0),
            },
        }

    def cleanup_old_workflows(self, max_age_seconds: int = 86400) -> int:
        cutoff = time.time() - max(1, int(max_age_seconds))
        with self._lock:
            with self._conn() as conn:
                rows = conn.execute(
                    """
                    SELECT execution_id
                    FROM workflow_executions
                    WHERE completed_at IS NOT NULL
                      AND completed_at < ?
                    """,
                    (cutoff,),
                ).fetchall()
                execution_ids = [row["execution_id"] for row in rows]
                if not execution_ids:
                    return 0
                placeholders = ", ".join("?" for _ in execution_ids)
                conn.execute(
                    f"DELETE FROM workflow_events WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                conn.execute(
                    f"DELETE FROM workflow_steps WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                conn.execute(
                    f"DELETE FROM workflow_artifacts WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                conn.execute(
                    f"DELETE FROM workflow_side_effects WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                conn.execute(
                    f"DELETE FROM workflow_executions WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                return len(execution_ids)

    def cleanup_old_workflow_artifacts(self, max_age_seconds: int = 86400) -> int:
        cutoff = time.time() - max(1, int(max_age_seconds))
        with self._lock:
            with self._conn() as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM workflow_artifacts
                    WHERE created_at < ?
                    """,
                    (cutoff,),
                ).fetchone()
                count = int(row["cnt"] if row else 0)
                if count <= 0:
                    return 0
                conn.execute(
                    """
                    DELETE FROM workflow_artifacts
                    WHERE created_at < ?
                    """,
                    (cutoff,),
                )
                return count

    def cleanup_old_workflow_side_effects(self, max_age_seconds: int = 86400) -> int:
        cutoff = time.time() - max(1, int(max_age_seconds))
        with self._lock:
            with self._conn() as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM workflow_side_effects
                    WHERE created_at < ?
                    """,
                    (cutoff,),
                ).fetchone()
                count = int(row["cnt"] if row else 0)
                if count <= 0:
                    return 0
                conn.execute(
                    """
                    DELETE FROM workflow_side_effects
                    WHERE created_at < ?
                    """,
                    (cutoff,),
                )
                return count

    def cleanup_old_workflow_definitions(self, max_age_seconds: int = 0) -> int:
        if max_age_seconds <= 0:
            return 0
        cutoff = time.time() - int(max_age_seconds)
        with self._lock:
            with self._conn() as conn:
                rows = conn.execute(
                    """
                    SELECT workflow_id
                    FROM workflow_definitions
                    WHERE updated_at < ?
                    """,
                    (cutoff,),
                ).fetchall()
                workflow_ids = [row["workflow_id"] for row in rows]
                if not workflow_ids:
                    return 0
                placeholders = ", ".join("?" for _ in workflow_ids)
                conn.execute(
                    f"DELETE FROM workflow_definitions WHERE workflow_id IN ({placeholders})",
                    workflow_ids,
                )
                return len(workflow_ids)


_execution_store: Optional[ExecutionStore] = None


def get_execution_store() -> ExecutionStore:
    global _execution_store
    if _execution_store is None:
        _execution_store = ExecutionStore()
    return _execution_store
