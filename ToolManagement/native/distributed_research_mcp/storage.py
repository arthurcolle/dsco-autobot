"""
SQLite Storage for Research MCP - Durable task and result persistence.

Provides:
- Task storage with full result data
- Citation tracking
- Query by status, date, content
- Usage analytics
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class StoredTask:
    """A task stored in the database."""
    id: str
    parallel_id: Optional[str]
    processor: str
    input: str
    status: str
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]
    citations_count: int = 0


class ResearchStorage:
    """
    SQLite storage for Research MCP tasks and results.

    Schema:
    - tasks: Core task metadata
    - results: Full JSON results from Parallel AI
    - citations: Extracted citations for search/reference
    """

    def __init__(self, db_path: str = "data/research_tasks.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_conn() as conn:
            conn.executescript("""
                -- Tasks table
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    parallel_id TEXT,
                    processor TEXT NOT NULL DEFAULT 'lite',
                    input TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL
                );

                -- Full results (JSON blob)
                CREATE TABLE IF NOT EXISTS results (
                    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
                    output_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                -- Extracted citations for search
                CREATE TABLE IF NOT EXISTS citations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    title TEXT,
                    url TEXT,
                    excerpt TEXT,
                    created_at TEXT NOT NULL
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_parallel_id ON tasks(parallel_id);
                CREATE INDEX IF NOT EXISTS idx_citations_task ON citations(task_id);
                CREATE INDEX IF NOT EXISTS idx_citations_url ON citations(url);

                -- Usage stats
                CREATE TABLE IF NOT EXISTS usage_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    processor TEXT NOT NULL,
                    task_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    total_citations INTEGER DEFAULT 0,
                    UNIQUE(date, processor)
                );
            """)
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")

    # =========================================================================
    # TASK OPERATIONS
    # =========================================================================

    def create_task(
        self,
        task_id: str,
        input_text: str,
        processor: str = "lite",
        parallel_id: Optional[str] = None,
    ) -> StoredTask:
        """Create a new task."""
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO tasks (id, parallel_id, processor, input, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """, (task_id, parallel_id, processor, input_text, now, now))
            conn.commit()

        return StoredTask(
            id=task_id,
            parallel_id=parallel_id,
            processor=processor,
            input=input_text,
            status="pending",
            result=None,
            error=None,
            created_at=datetime.fromisoformat(now),
            completed_at=None,
        )

    def update_task_status(
        self,
        task_id: str,
        status: str,
        parallel_id: Optional[str] = None,
        error: Optional[str] = None,
    ):
        """Update task status."""
        now = datetime.now(timezone.utc).isoformat()
        completed_at = now if status in ("completed", "failed", "cancelled") else None

        with self._get_conn() as conn:
            if parallel_id:
                conn.execute("""
                    UPDATE tasks SET status = ?, parallel_id = ?, error = ?,
                           completed_at = ?, updated_at = ?
                    WHERE id = ?
                """, (status, parallel_id, error, completed_at, now, task_id))
            else:
                conn.execute("""
                    UPDATE tasks SET status = ?, error = ?, completed_at = ?, updated_at = ?
                    WHERE id = ?
                """, (status, error, completed_at, now, task_id))
            conn.commit()

    def store_result(
        self,
        task_id: str,
        result: Dict[str, Any],
    ):
        """Store the full result and extract citations."""
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            # Store full result
            conn.execute("""
                INSERT OR REPLACE INTO results (task_id, output_json, created_at)
                VALUES (?, ?, ?)
            """, (task_id, json.dumps(result, default=str), now))

            # Extract and store citations
            output = result.get("output", {})
            basis = output.get("basis", [])
            citations_stored = 0

            for item in basis:
                for citation in item.get("citations", []):
                    conn.execute("""
                        INSERT INTO citations (task_id, title, url, excerpt, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        task_id,
                        citation.get("title"),
                        citation.get("url"),
                        json.dumps(citation.get("excerpts", [])),
                        now
                    ))
                    citations_stored += 1

            # Update usage stats before committing
            self._update_usage_stats(conn, task_id, citations_stored)

            conn.commit()

            logger.debug(f"Stored result for {task_id} with {citations_stored} citations")

    def _update_usage_stats(self, conn, task_id: str, citations_count: int):
        """Update usage statistics."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Get task info
        row = conn.execute(
            "SELECT processor, status FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()

        if row:
            processor = row["processor"]
            status = row["status"]

            conn.execute("""
                INSERT INTO usage_stats (date, processor, task_count, success_count, error_count, total_citations)
                VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT(date, processor) DO UPDATE SET
                    task_count = task_count + 1,
                    success_count = success_count + excluded.success_count,
                    error_count = error_count + excluded.error_count,
                    total_citations = total_citations + excluded.total_citations
            """, (
                today,
                processor,
                1 if status == "completed" else 0,
                1 if status == "failed" else 0,
                citations_count
            ))

    def get_task(self, task_id: str) -> Optional[StoredTask]:
        """Get a task by ID."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT t.*,
                       (SELECT COUNT(*) FROM citations WHERE task_id = t.id) as citations_count
                FROM tasks t WHERE t.id = ?
            """, (task_id,)).fetchone()

            if not row:
                return None

            # Get result if exists
            result_row = conn.execute(
                "SELECT output_json FROM results WHERE task_id = ?", (task_id,)
            ).fetchone()

            result = json.loads(result_row["output_json"]) if result_row else None

            return StoredTask(
                id=row["id"],
                parallel_id=row["parallel_id"],
                processor=row["processor"],
                input=row["input"],
                status=row["status"],
                result=result,
                error=row["error"],
                created_at=datetime.fromisoformat(row["created_at"]),
                completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                citations_count=row["citations_count"],
            )

    def get_task_by_parallel_id(self, parallel_id: str) -> Optional[StoredTask]:
        """Get a task by Parallel AI run ID."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT id FROM tasks WHERE parallel_id = ?", (parallel_id,)
            ).fetchone()

            if row:
                return self.get_task(row["id"])
            return None

    def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "created_at DESC",
    ) -> Tuple[List[StoredTask], int]:
        """List tasks with optional filtering."""
        with self._get_conn() as conn:
            # Build query
            where_clause = ""
            params: List[Any] = []

            if status and status != "all":
                where_clause = "WHERE status = ?"
                params.append(status)

            # Get total count
            count_row = conn.execute(
                f"SELECT COUNT(*) as total FROM tasks {where_clause}", params
            ).fetchone()
            total = count_row["total"]

            # Get tasks
            params.extend([limit, offset])
            rows = conn.execute(f"""
                SELECT t.*,
                       (SELECT COUNT(*) FROM citations WHERE task_id = t.id) as citations_count
                FROM tasks t
                {where_clause}
                ORDER BY {order_by}
                LIMIT ? OFFSET ?
            """, params).fetchall()

            tasks = []
            for row in rows:
                tasks.append(StoredTask(
                    id=row["id"],
                    parallel_id=row["parallel_id"],
                    processor=row["processor"],
                    input=row["input"],
                    status=row["status"],
                    result=None,  # Don't load full results for list
                    error=row["error"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                    citations_count=row["citations_count"],
                ))

            return tasks, total

    # =========================================================================
    # CITATION OPERATIONS
    # =========================================================================

    def get_citations(
        self,
        task_id: Optional[str] = None,
        url_pattern: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get citations with optional filtering."""
        with self._get_conn() as conn:
            where_clauses = []
            params: List[Any] = []

            if task_id:
                where_clauses.append("task_id = ?")
                params.append(task_id)

            if url_pattern:
                where_clauses.append("url LIKE ?")
                params.append(f"%{url_pattern}%")

            where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            params.append(limit)

            rows = conn.execute(f"""
                SELECT c.*, t.input as task_input
                FROM citations c
                JOIN tasks t ON c.task_id = t.id
                {where_str}
                ORDER BY c.created_at DESC
                LIMIT ?
            """, params).fetchall()

            return [
                {
                    "id": row["id"],
                    "task_id": row["task_id"],
                    "title": row["title"],
                    "url": row["url"],
                    "excerpt": json.loads(row["excerpt"]) if row["excerpt"] else [],
                    "task_input": row["task_input"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

    def search_citations(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search citations by title or URL."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT c.*, t.input as task_input
                FROM citations c
                JOIN tasks t ON c.task_id = t.id
                WHERE c.title LIKE ? OR c.url LIKE ? OR c.excerpt LIKE ?
                ORDER BY c.created_at DESC
                LIMIT ?
            """, (f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()

            return [
                {
                    "id": row["id"],
                    "task_id": row["task_id"],
                    "title": row["title"],
                    "url": row["url"],
                    "excerpt": json.loads(row["excerpt"]) if row["excerpt"] else [],
                    "task_input": row["task_input"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

    # =========================================================================
    # ANALYTICS
    # =========================================================================

    def get_usage_stats(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get usage statistics."""
        with self._get_conn() as conn:
            where_clauses = []
            params: List[Any] = []

            if start_date:
                where_clauses.append("date >= ?")
                params.append(start_date)

            if end_date:
                where_clauses.append("date <= ?")
                params.append(end_date)

            where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            # Aggregate stats
            row = conn.execute(f"""
                SELECT
                    COALESCE(SUM(task_count), 0) as total_tasks,
                    COALESCE(SUM(success_count), 0) as total_success,
                    COALESCE(SUM(error_count), 0) as total_errors,
                    COALESCE(SUM(total_citations), 0) as total_citations
                FROM usage_stats
                {where_str}
            """, params).fetchone()

            # By processor
            processor_rows = conn.execute(f"""
                SELECT processor,
                       SUM(task_count) as tasks,
                       SUM(success_count) as success,
                       SUM(error_count) as errors
                FROM usage_stats
                {where_str}
                GROUP BY processor
            """, params).fetchall()

            # Daily breakdown (last 7 days)
            daily_rows = conn.execute("""
                SELECT date, SUM(task_count) as tasks
                FROM usage_stats
                GROUP BY date
                ORDER BY date DESC
                LIMIT 7
            """).fetchall()

            return {
                "total_tasks": row["total_tasks"],
                "total_success": row["total_success"],
                "total_errors": row["total_errors"],
                "total_citations": row["total_citations"],
                "success_rate": row["total_success"] / max(1, row["total_tasks"]),
                "by_processor": {
                    r["processor"]: {
                        "tasks": r["tasks"],
                        "success": r["success"],
                        "errors": r["errors"]
                    }
                    for r in processor_rows
                },
                "daily": [
                    {"date": r["date"], "tasks": r["tasks"]}
                    for r in daily_rows
                ],
            }

    def get_total_counts(self) -> Dict[str, int]:
        """Get quick counts."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT
                    (SELECT COUNT(*) FROM tasks) as tasks,
                    (SELECT COUNT(*) FROM tasks WHERE status = 'completed') as completed,
                    (SELECT COUNT(*) FROM citations) as citations
            """).fetchone()

            return {
                "tasks": row["tasks"],
                "completed": row["completed"],
                "citations": row["citations"],
            }
