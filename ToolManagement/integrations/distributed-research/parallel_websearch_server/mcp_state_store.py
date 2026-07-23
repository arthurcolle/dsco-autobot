"""
MCP State Store - Persistent storage for task and group IDs across conversations.
This ensures we never lose track of created resources.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from threading import Lock
import logging

logger = logging.getLogger(__name__)


class MCPStateStore:
    """
    SQLite-backed store for maintaining MCP tool state.
    Stores task IDs, group IDs, and other important state that needs to persist.
    """

    def __init__(self, db_path: str = None):
        """Initialize the state store."""
        if db_path is None:
            db_path = Path.home() / ".parallel_mcp" / "state.db"
        else:
            db_path = Path(db_path)

        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_runs (
                    task_run_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processor TEXT,
                    input_data TEXT,
                    task_spec TEXT,
                    status TEXT DEFAULT 'created',
                    output_data TEXT,
                    metadata TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_groups (
                    taskgroup_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'created',
                    total_runs INTEGER DEFAULT 0,
                    completed_runs INTEGER DEFAULT 0,
                    failed_runs INTEGER DEFAULT 0,
                    metadata TEXT,
                    run_ids TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS search_history (
                    search_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    objective TEXT,
                    max_results INTEGER,
                    results TEXT,
                    status TEXT DEFAULT 'completed'
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS monitors (
                    monitor_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    name TEXT,
                    objective TEXT,
                    schedule TEXT,
                    status TEXT DEFAULT 'active',
                    metadata TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            conn.close()
            logger.info(f"MCP state database initialized at {self.db_path}")

    def save_task_run(self, task_run_id: str, processor: str, input_data: Any,
                      task_spec: Any = None, status: str = "created",
                      output_data: Any = None, metadata: Dict = None) -> None:
        """Save or update a task run."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            now = datetime.utcnow().isoformat()

            conn.execute("""
                INSERT OR REPLACE INTO task_runs
                (task_run_id, processor, input_data, task_spec, status,
                 output_data, metadata, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task_run_id,
                processor,
                json.dumps(input_data) if not isinstance(input_data, str) else input_data,
                json.dumps(task_spec) if task_spec else None,
                status,
                json.dumps(output_data) if output_data else None,
                json.dumps(metadata) if metadata else None,
                now
            ))
            conn.commit()
            conn.close()
            logger.debug(f"Saved task run {task_run_id} with status {status}")

    def save_task_group(self, taskgroup_id: str, status: str = "created",
                        total_runs: int = 0, completed_runs: int = 0,
                        failed_runs: int = 0, metadata: Dict = None,
                        run_ids: List[str] = None) -> None:
        """Save or update a task group."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            now = datetime.utcnow().isoformat()

            conn.execute("""
                INSERT OR REPLACE INTO task_groups
                (taskgroup_id, status, total_runs, completed_runs, failed_runs,
                 metadata, run_ids, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                taskgroup_id,
                status,
                total_runs,
                completed_runs,
                failed_runs,
                json.dumps(metadata) if metadata else None,
                json.dumps(run_ids) if run_ids else None,
                now
            ))
            conn.commit()
            conn.close()
            logger.debug(f"Saved task group {taskgroup_id} with status {status}")

    def get_task_run(self, task_run_id: str) -> Optional[Dict]:
        """Get a task run by ID."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM task_runs WHERE task_run_id = ?",
                (task_run_id,)
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                result = dict(row)
                # Parse JSON fields
                for field in ['input_data', 'task_spec', 'output_data', 'metadata']:
                    if result.get(field):
                        try:
                            result[field] = json.loads(result[field])
                        except:
                            pass
                return result
            return None

    def get_task_group(self, taskgroup_id: str) -> Optional[Dict]:
        """Get a task group by ID."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM task_groups WHERE taskgroup_id = ?",
                (taskgroup_id,)
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                result = dict(row)
                # Parse JSON fields
                for field in ['metadata', 'run_ids']:
                    if result.get(field):
                        try:
                            result[field] = json.loads(result[field])
                        except:
                            pass
                return result
            return None

    def list_recent_task_runs(self, limit: int = 10) -> List[Dict]:
        """List recent task runs."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT task_run_id, created_at, processor, status, last_updated
                FROM task_runs
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]

    def list_recent_task_groups(self, limit: int = 10) -> List[Dict]:
        """List recent task groups."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT taskgroup_id, created_at, status, total_runs,
                       completed_runs, failed_runs, last_updated
                FROM task_groups
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]

    def save_search(self, objective: str, max_results: int,
                    results: Any = None, status: str = "completed") -> int:
        """Save a search query and results."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.execute("""
                INSERT INTO search_history (objective, max_results, results, status)
                VALUES (?, ?, ?, ?)
            """, (
                objective,
                max_results,
                json.dumps(results) if results else None,
                status
            ))
            search_id = cursor.lastrowid
            conn.commit()
            conn.close()
            logger.debug(f"Saved search {search_id}: {objective[:50]}...")
            return search_id

    def get_recent_searches(self, limit: int = 10) -> List[Dict]:
        """Get recent search history."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT search_id, created_at, objective, max_results, status
                FROM search_history
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]

    def cleanup_old_records(self, days_to_keep: int = 30) -> None:
        """Clean up old records to prevent database bloat."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            cutoff = datetime.utcnow().isoformat()

            for table in ['task_runs', 'task_groups', 'search_history']:
                conn.execute(f"""
                    DELETE FROM {table}
                    WHERE created_at < datetime('now', '-{days_to_keep} days')
                """)

            conn.commit()
            conn.close()
            logger.info(f"Cleaned up records older than {days_to_keep} days")


# Global singleton instance
_mcp_state_store = None


def get_mcp_state_store() -> MCPStateStore:
    """Get or create the global MCP state store instance."""
    global _mcp_state_store
    if _mcp_state_store is None:
        _mcp_state_store = MCPStateStore()
    return _mcp_state_store