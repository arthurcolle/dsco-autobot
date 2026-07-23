"""Turso database storage for Parallel AI API data persistence.

Stores task runs, FindAll runs, chat completions, search/extract results
so they can be recovered if conversation context is lost.
"""

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import libsql_experimental as libsql

# Database configuration
TURSO_DB_URL = os.environ.get(
    "TURSO_DB_URL",
    "libsql://parallel-ai-logs-arthurcolle.aws-us-east-1.turso.io"
)
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")


class TursoStore:
    """Persistent storage for Parallel AI API data.

    Uses thread-local connections for thread safety with asyncio.to_thread().
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        auth_token: Optional[str] = None,
    ):
        self.db_url = db_url or TURSO_DB_URL
        self.auth_token = auth_token or TURSO_AUTH_TOKEN
        self._local = threading.local()

    def connect(self) -> None:
        """Establish database connection for current thread."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = libsql.connect(
                database=self.db_url,
                auth_token=self.auth_token,
            )

    def close(self) -> None:
        """Close database connection for current thread."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    def _get_conn(self):
        """Get thread-local connection, creating if needed."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self.connect()
        return self._local.conn

    def _json_dumps(self, obj: Any) -> Optional[str]:
        """Safely serialize to JSON."""
        if obj is None:
            return None
        return json.dumps(obj, default=str)

    # =========================================================================
    # Task Runs
    # =========================================================================

    def save_task_run(
        self,
        run_id: str,
        processor: str,
        status: str,
        input_text: Optional[str] = None,
        input_json: Optional[Any] = None,  # Any JSON-serializable type
        task_spec: Optional[Dict] = None,
        output_text: Optional[str] = None,
        output_json: Optional[Any] = None,  # Any JSON-serializable type
        metadata: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """Store or update a task run."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO task_runs (
                run_id, processor, status, input_text, input_json, task_spec,
                output_text, output_json, metadata, created_at, modified_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                status = excluded.status,
                output_text = COALESCE(excluded.output_text, task_runs.output_text),
                output_json = COALESCE(excluded.output_json, task_runs.output_json),
                modified_at = excluded.modified_at,
                completed_at = CASE WHEN excluded.status IN ('completed', 'failed', 'cancelled')
                    THEN excluded.modified_at ELSE task_runs.completed_at END,
                error = COALESCE(excluded.error, task_runs.error)
            """,
            (
                run_id, processor, status, input_text,
                self._json_dumps(input_json), self._json_dumps(task_spec),
                output_text, self._json_dumps(output_json),
                self._json_dumps(metadata), now, now, error
            ),
        )
        conn.commit()

    def get_task_run(self, run_id: str) -> Optional[Dict]:
        """Retrieve a task run by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result, conn.execute("SELECT * FROM task_runs LIMIT 0").description)
        return None

    def list_task_runs(
        self,
        status: Optional[str] = None,
        processor: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List task runs with optional filters."""
        conn = self._get_conn()
        query = "SELECT * FROM task_runs WHERE 1=1"
        params = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if processor:
            query += " AND processor = ?"
            params.append(processor)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        desc = conn.execute("SELECT * FROM task_runs LIMIT 0").description
        return [self._row_to_dict(row, desc) for row in results]

    def get_recent_task_ids(self, limit: int = 20) -> List[str]:
        """Get most recent task run IDs."""
        conn = self._get_conn()
        results = conn.execute(
            "SELECT run_id FROM task_runs ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [row[0] for row in results]

    # =========================================================================
    # FindAll Runs
    # =========================================================================

    def save_findall_run(
        self,
        findall_id: str,
        objective: str,
        status: str,
        entity_type: Optional[str] = None,
        generator: Optional[str] = None,
        match_limit: Optional[int] = None,
        candidates_found: int = 0,
        candidates_matched: int = 0,
        termination_reason: Optional[str] = None,
        metadata: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """Store or update a FindAll run."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO findall_runs (
                findall_id, objective, entity_type, generator, match_limit,
                status, candidates_found, candidates_matched, termination_reason,
                metadata, created_at, modified_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(findall_id) DO UPDATE SET
                status = excluded.status,
                candidates_found = excluded.candidates_found,
                candidates_matched = excluded.candidates_matched,
                termination_reason = COALESCE(excluded.termination_reason, findall_runs.termination_reason),
                modified_at = excluded.modified_at,
                completed_at = CASE WHEN excluded.status IN ('completed', 'failed', 'cancelled')
                    THEN excluded.modified_at ELSE findall_runs.completed_at END,
                error = COALESCE(excluded.error, findall_runs.error)
            """,
            (
                findall_id, objective, entity_type, generator, match_limit,
                status, candidates_found, candidates_matched, termination_reason,
                self._json_dumps(metadata), now, now, error
            ),
        )
        conn.commit()

    def get_findall_run(self, findall_id: str) -> Optional[Dict]:
        """Retrieve a FindAll run by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM findall_runs WHERE findall_id = ?", (findall_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result, conn.execute("SELECT * FROM findall_runs LIMIT 0").description)
        return None

    def list_findall_runs(
        self,
        status: Optional[str] = None,
        entity_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List FindAll runs with optional filters."""
        conn = self._get_conn()
        query = "SELECT * FROM findall_runs WHERE 1=1"
        params = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        desc = conn.execute("SELECT * FROM findall_runs LIMIT 0").description
        return [self._row_to_dict(row, desc) for row in results]

    # =========================================================================
    # Chat Completions
    # =========================================================================

    def save_chat_completion(
        self,
        model: str,
        messages: List[Dict],
        completion_id: Optional[str] = None,
        response: Optional[str] = None,
        usage_prompt_tokens: Optional[int] = None,
        usage_completion_tokens: Optional[int] = None,
        usage_total_tokens: Optional[int] = None,
        duration_ms: Optional[int] = None,
        error: Optional[str] = None,
    ) -> int:
        """Store a chat completion. Returns the row ID."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        cursor = conn.execute(
            """
            INSERT INTO chat_completions (
                completion_id, model, messages, response,
                usage_prompt_tokens, usage_completion_tokens, usage_total_tokens,
                created_at, duration_ms, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                completion_id, model, self._json_dumps(messages), response,
                usage_prompt_tokens, usage_completion_tokens, usage_total_tokens,
                now, duration_ms, error
            ),
        )
        conn.commit()
        return cursor.lastrowid

    def get_chat_completion(self, completion_id: str) -> Optional[Dict]:
        """Retrieve a chat completion by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM chat_completions WHERE completion_id = ?", (completion_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result, conn.execute("SELECT * FROM chat_completions LIMIT 0").description)
        return None

    def list_chat_completions(self, limit: int = 50) -> List[Dict]:
        """List recent chat completions."""
        conn = self._get_conn()
        results = conn.execute(
            "SELECT * FROM chat_completions ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        desc = conn.execute("SELECT * FROM chat_completions LIMIT 0").description
        return [self._row_to_dict(row, desc) for row in results]

    # =========================================================================
    # Search Requests
    # =========================================================================

    def save_search_request(
        self,
        objective: str,
        request_id: Optional[str] = None,
        search_queries: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        results_count: Optional[int] = None,
        results: Optional[List[Dict]] = None,
        duration_ms: Optional[int] = None,
        error: Optional[str] = None,
    ) -> int:
        """Store a search request. Returns the row ID."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        cursor = conn.execute(
            """
            INSERT INTO search_requests (
                request_id, objective, search_queries, max_results,
                results_count, results, created_at, duration_ms, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id, objective, self._json_dumps(search_queries),
                max_results, results_count, self._json_dumps(results),
                now, duration_ms, error
            ),
        )
        conn.commit()
        return cursor.lastrowid

    def list_search_requests(self, limit: int = 50) -> List[Dict]:
        """List recent search requests."""
        conn = self._get_conn()
        results = conn.execute(
            "SELECT * FROM search_requests ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        desc = conn.execute("SELECT * FROM search_requests LIMIT 0").description
        return [self._row_to_dict(row, desc) for row in results]

    # =========================================================================
    # Extract Requests
    # =========================================================================

    def save_extract_request(
        self,
        urls: List[str],
        extract_id: Optional[str] = None,
        objective: Optional[str] = None,
        results_count: Optional[int] = None,
        errors_count: Optional[int] = None,
        results: Optional[List[Dict]] = None,
        duration_ms: Optional[int] = None,
        error: Optional[str] = None,
    ) -> int:
        """Store an extract request. Returns the row ID."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        cursor = conn.execute(
            """
            INSERT INTO extract_requests (
                extract_id, urls, objective, results_count, errors_count,
                results, created_at, duration_ms, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                extract_id, self._json_dumps(urls), objective,
                results_count, errors_count, self._json_dumps(results),
                now, duration_ms, error
            ),
        )
        conn.commit()
        return cursor.lastrowid

    def list_extract_requests(self, limit: int = 50) -> List[Dict]:
        """List recent extract requests."""
        conn = self._get_conn()
        results = conn.execute(
            "SELECT * FROM extract_requests ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        desc = conn.execute("SELECT * FROM extract_requests LIMIT 0").description
        return [self._row_to_dict(row, desc) for row in results]

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _row_to_dict(self, row, description) -> Dict:
        """Convert a database row to a dictionary."""
        if row is None:
            return None
        columns = [col[0] for col in description]
        result = dict(zip(columns, row))
        # Parse JSON fields
        for key in ['input_json', 'output_json', 'task_spec', 'metadata',
                    'messages', 'results', 'search_queries', 'urls']:
            if key in result and result[key]:
                try:
                    result[key] = json.loads(result[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        return result

    def search_all(self, query: str, limit: int = 20) -> Dict[str, List[Dict]]:
        """Search across all tables for a query string."""
        conn = self._get_conn()
        results = {}

        # Search task runs
        task_results = conn.execute(
            """SELECT * FROM task_runs
               WHERE run_id LIKE ? OR input_text LIKE ? OR output_text LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        desc = conn.execute("SELECT * FROM task_runs LIMIT 0").description
        results['task_runs'] = [self._row_to_dict(row, desc) for row in task_results]

        # Search FindAll runs
        findall_results = conn.execute(
            """SELECT * FROM findall_runs
               WHERE findall_id LIKE ? OR objective LIKE ? OR entity_type LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        desc = conn.execute("SELECT * FROM findall_runs LIMIT 0").description
        results['findall_runs'] = [self._row_to_dict(row, desc) for row in findall_results]

        # Search chat completions
        chat_results = conn.execute(
            """SELECT * FROM chat_completions
               WHERE completion_id LIKE ? OR messages LIKE ? OR response LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        desc = conn.execute("SELECT * FROM chat_completions LIMIT 0").description
        results['chat_completions'] = [self._row_to_dict(row, desc) for row in chat_results]

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        conn = self._get_conn()
        stats = {}

        for table in ['task_runs', 'findall_runs', 'chat_completions',
                      'search_requests', 'extract_requests']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = {'count': count}

        return stats


# Global instance
_store: Optional[TursoStore] = None


def get_turso_store() -> TursoStore:
    """Get or create the global TursoStore instance."""
    global _store
    if _store is None:
        _store = TursoStore()
        _store.connect()
    return _store
