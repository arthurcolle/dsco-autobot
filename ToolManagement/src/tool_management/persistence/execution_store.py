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


_execution_store: Optional[ExecutionStore] = None


def get_execution_store() -> ExecutionStore:
    global _execution_store
    if _execution_store is None:
        _execution_store = ExecutionStore()
    return _execution_store
