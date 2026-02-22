"""
Durable execution/event persistence for streaming jobs.

Stores execution lifecycle and event streams on disk so job state survives restarts.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional


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
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    tool_name TEXT NOT NULL,
                    client_id TEXT,
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

                CREATE TABLE IF NOT EXISTS workflow_steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    step_key TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    step_index INTEGER,
                    tool_name TEXT,
                    mode TEXT,
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
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    message TEXT,
                    data_json TEXT,
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_kind ON workflow_executions(workflow_kind);
                CREATE INDEX IF NOT EXISTS idx_workflow_executions_updated ON workflow_executions(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_workflow_steps_execution ON workflow_steps(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_steps_status ON workflow_steps(status);
                CREATE INDEX IF NOT EXISTS idx_workflow_events_execution ON workflow_events(execution_id);
                CREATE INDEX IF NOT EXISTS idx_workflow_events_ts ON workflow_events(timestamp);
                """
            )

    def upsert_execution(
        self,
        *,
        execution_id: str,
        tool_name: str,
        client_id: Optional[str],
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
                        execution_id, tool_name, client_id, status, progress,
                        queue_priority, dedupe_key, queued_at, started_at, completed_at,
                        last_message, error, result_json, control_state_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id) DO UPDATE SET
                        tool_name = excluded.tool_name,
                        client_id = excluded.client_id,
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
                        execution_id, event_type, timestamp, progress, message, data_json, error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
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

    def upsert_workflow_execution(
        self,
        *,
        execution_id: str,
        workflow_id: Optional[str],
        workflow_name: str,
        workflow_kind: str,
        workflow_type: Optional[str],
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
                        status, inputs_json, output_json, error,
                        started_at, completed_at, duration_ms, metadata_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id) DO UPDATE SET
                        workflow_id = COALESCE(excluded.workflow_id, workflow_executions.workflow_id),
                        workflow_name = COALESCE(excluded.workflow_name, workflow_executions.workflow_name),
                        workflow_kind = COALESCE(excluded.workflow_kind, workflow_executions.workflow_kind),
                        workflow_type = COALESCE(excluded.workflow_type, workflow_executions.workflow_type),
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

        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO workflow_steps (
                        execution_id, step_key, step_name, step_index, tool_name, mode, status,
                        started_at, completed_at, duration_ms, retries,
                        input_json, output_json, error, metadata_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(execution_id, step_key) DO UPDATE SET
                        step_name = COALESCE(excluded.step_name, workflow_steps.step_name),
                        step_index = COALESCE(excluded.step_index, workflow_steps.step_index),
                        tool_name = COALESCE(excluded.tool_name, workflow_steps.tool_name),
                        mode = COALESCE(excluded.mode, workflow_steps.mode),
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
                        execution_id, step_key, event_type, timestamp, message, data_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        step_key,
                        event_type,
                        event_ts,
                        message,
                        data_json,
                    ),
                )

    def list_workflow_executions(
        self,
        *,
        status: Optional[str] = None,
        workflow_kind: Optional[str] = None,
        workflow_id: Optional[str] = None,
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
        event_limit: int = 200,
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
                    f"DELETE FROM workflow_executions WHERE execution_id IN ({placeholders})",
                    execution_ids,
                )
                return len(execution_ids)


_execution_store: Optional[ExecutionStore] = None


def get_execution_store() -> ExecutionStore:
    global _execution_store
    if _execution_store is None:
        _execution_store = ExecutionStore()
    return _execution_store
