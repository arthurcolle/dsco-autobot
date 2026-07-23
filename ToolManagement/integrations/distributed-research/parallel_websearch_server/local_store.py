"""Local SQLite + DuckDB storage for Parallel AI API data persistence.

SQLite handles transactional writes (task runs, searches, etc.).
DuckDB provides fast analytics queries when needed.

This is a fully local solution with no external auth required.

Storage location: ~/.parallel-ai/
    - parallel_ai.db: Main SQLite database (WAL mode for concurrency)
    - parallel_ai_analytics.duckdb: DuckDB for fast analytics
    - backups/: Automatic backups directory

Features:
    - Thread-safe via thread-local connections
    - Automatic schema migrations
    - Full-text search on text fields
    - Export/import to JSON/Parquet
    - Automatic backups before destructive operations
    - DuckDB sync for analytical queries
"""

import json
import os
import shutil
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)

# Optional DuckDB for analytics
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

# Default database paths
DEFAULT_DB_DIR = Path.home() / ".parallel-ai"
DEFAULT_SQLITE_PATH = DEFAULT_DB_DIR / "parallel_ai.db"
DEFAULT_DUCKDB_PATH = DEFAULT_DB_DIR / "parallel_ai_analytics.duckdb"
DEFAULT_BACKUP_DIR = DEFAULT_DB_DIR / "backups"

# Schema version for migrations
SCHEMA_VERSION = 5


def _ensure_db_dir():
    """Ensure the database directory exists."""
    DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)


class LocalStore:
    """Local persistent storage for Parallel AI API data.

    Uses SQLite for reliable transactional storage and optionally
    DuckDB for fast analytical queries.

    Thread-safe via thread-local connections.

    Example usage:
        store = get_local_store()

        # Save a task run
        store.save_task_run(
            run_id="trun_abc123",
            processor="core",
            status="completed",
            input_text="What is Python?",
            output_text="Python is a programming language..."
        )

        # Query with filters
        completed_tasks = store.list_task_runs(status="completed", limit=10)

        # Full-text search
        results = store.search_all("Python programming")

        # Analytics (requires DuckDB)
        stats = store.get_task_analytics(days=30)
    """

    def __init__(
        self,
        sqlite_path: Optional[str] = None,
        duckdb_path: Optional[str] = None,
    ):
        """Initialize the local store.

        Args:
            sqlite_path: Path to SQLite database. Defaults to ~/.parallel-ai/parallel_ai.db
            duckdb_path: Path to DuckDB database. Defaults to ~/.parallel-ai/parallel_ai_analytics.duckdb
        """
        _ensure_db_dir()
        self.sqlite_path = sqlite_path or str(DEFAULT_SQLITE_PATH)
        self.duckdb_path = duckdb_path or str(DEFAULT_DUCKDB_PATH)
        self._local = threading.local()
        self._initialized = False
        self._schema_version = 0

    def connect(self) -> None:
        """Establish database connections for current thread."""
        if not hasattr(self._local, 'sqlite_conn') or self._local.sqlite_conn is None:
            self._local.sqlite_conn = sqlite3.connect(
                self.sqlite_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._local.sqlite_conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrency
            self._local.sqlite_conn.execute("PRAGMA journal_mode=WAL")
            self._local.sqlite_conn.execute("PRAGMA synchronous=NORMAL")
            self._local.sqlite_conn.execute("PRAGMA foreign_keys=ON")
            self._local.sqlite_conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

        if HAS_DUCKDB and (not hasattr(self._local, 'duck_conn') or self._local.duck_conn is None):
            self._local.duck_conn = duckdb.connect(self.duckdb_path)

        if not self._initialized:
            self._init_schema()
            self._run_migrations()
            self._create_indexes()
            self._initialized = True

    def close(self) -> None:
        """Close database connections for current thread."""
        if hasattr(self._local, 'sqlite_conn') and self._local.sqlite_conn:
            self._local.sqlite_conn.close()
            self._local.sqlite_conn = None
        if HAS_DUCKDB and hasattr(self._local, 'duck_conn') and self._local.duck_conn:
            self._local.duck_conn.close()
            self._local.duck_conn = None

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local SQLite connection, creating if needed."""
        if not hasattr(self._local, 'sqlite_conn') or self._local.sqlite_conn is None:
            self.connect()
        return self._local.sqlite_conn

    def _get_duck(self):
        """Get thread-local DuckDB connection if available."""
        if not HAS_DUCKDB:
            return None
        if not hasattr(self._local, 'duck_conn') or self._local.duck_conn is None:
            self.connect()
        return self._local.duck_conn

    # =========================================================================
    # Schema Management
    # =========================================================================

    def _init_schema(self) -> None:
        """Initialize database schema."""
        conn = self._get_conn()

        # Schema version tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_info (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # Task runs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                run_id TEXT PRIMARY KEY,
                processor TEXT NOT NULL,
                status TEXT NOT NULL,
                input_text TEXT,
                input_json TEXT,
                task_spec TEXT,
                output_text TEXT,
                output_json TEXT,
                metadata TEXT,
                error TEXT,
                taskgroup_id TEXT,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                completed_at TEXT,
                duration_ms INTEGER
            )
        """)

        # Task groups table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_groups (
                taskgroup_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                metadata TEXT,
                num_task_runs INTEGER DEFAULT 0,
                completed_runs INTEGER DEFAULT 0,
                failed_runs INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL
            )
        """)

        # FindAll runs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS findall_runs (
                findall_id TEXT PRIMARY KEY,
                objective TEXT NOT NULL,
                entity_type TEXT,
                generator TEXT,
                match_limit INTEGER,
                status TEXT NOT NULL,
                candidates_found INTEGER DEFAULT 0,
                candidates_matched INTEGER DEFAULT 0,
                termination_reason TEXT,
                metadata TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                completed_at TEXT
            )
        """)

        # FindAll candidates table (for storing individual results)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS findall_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                findall_id TEXT NOT NULL,
                candidate_data TEXT NOT NULL,
                match_score REAL,
                is_matched INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (findall_id) REFERENCES findall_runs(findall_id)
            )
        """)

        # Monitors table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitors (
                monitor_id TEXT PRIMARY KEY,
                name TEXT,
                url TEXT NOT NULL,
                schedule TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                last_run_at TEXT,
                next_run_at TEXT,
                config TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL
            )
        """)

        # Monitor runs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitor_runs (
                run_id TEXT PRIMARY KEY,
                monitor_id TEXT NOT NULL,
                status TEXT NOT NULL,
                changes_detected INTEGER DEFAULT 0,
                snapshot TEXT,
                diff TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY (monitor_id) REFERENCES monitors(monitor_id)
            )
        """)

        # Chat completions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_completions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                completion_id TEXT UNIQUE,
                model TEXT NOT NULL,
                messages TEXT NOT NULL,
                response TEXT,
                usage_prompt_tokens INTEGER,
                usage_completion_tokens INTEGER,
                usage_total_tokens INTEGER,
                duration_ms INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Search requests table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS search_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT UNIQUE,
                objective TEXT NOT NULL,
                search_queries TEXT,
                max_results INTEGER,
                results_count INTEGER,
                results TEXT,
                duration_ms INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Extract requests table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS extract_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extract_id TEXT UNIQUE,
                urls TEXT NOT NULL,
                objective TEXT,
                results_count INTEGER,
                errors_count INTEGER,
                results TEXT,
                duration_ms INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # API usage tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                method TEXT NOT NULL,
                status_code INTEGER,
                duration_ms INTEGER,
                request_size INTEGER,
                response_size INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Events table for persistent event storage (30 day rolling)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                event_type TEXT NOT NULL,
                resource_id TEXT,
                resource_type TEXT,
                data TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_resource ON events(resource_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)")

        # =====================================================================
        # API Audit Log - Complete request/response capture
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT UNIQUE NOT NULL,

                -- Request details
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                query_params TEXT,
                request_headers TEXT,
                request_body TEXT,
                request_body_size INTEGER,
                client_ip TEXT,
                user_agent TEXT,

                -- Response details
                status_code INTEGER,
                response_headers TEXT,
                response_body TEXT,
                response_body_size INTEGER,

                -- Correlation
                upstream_request_id TEXT,
                context_id TEXT,
                run_id TEXT,
                taskgroup_id TEXT,

                -- Timing
                started_at TEXT NOT NULL,
                completed_at TEXT,
                duration_ms INTEGER,
                upstream_duration_ms INTEGER,

                -- Error capture
                error_type TEXT,
                error_message TEXT,
                error_traceback TEXT,

                -- Classification
                operation_type TEXT,
                was_successful INTEGER DEFAULT 1,
                was_stored INTEGER DEFAULT 0
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_request_id ON api_audit_log(request_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_path ON api_audit_log(path)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_started ON api_audit_log(started_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_status ON api_audit_log(status_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_context ON api_audit_log(context_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_run ON api_audit_log(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_error ON api_audit_log(error_type)")

        # =====================================================================
        # Storage Failures - Queue for failed writes needing retry
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS storage_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                failure_id TEXT UNIQUE NOT NULL,

                -- What failed
                operation_type TEXT NOT NULL,
                target_table TEXT NOT NULL,
                payload TEXT NOT NULL,

                -- Error details
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                error_traceback TEXT,

                -- Retry tracking
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 5,
                next_retry_at TEXT,
                last_retry_at TEXT,

                -- Status
                status TEXT NOT NULL DEFAULT 'pending',
                resolved_at TEXT,
                resolution_type TEXT,

                -- Timestamps
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_failures_status ON storage_failures(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_failures_retry ON storage_failures(next_retry_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_failures_operation ON storage_failures(operation_type)")

        # =====================================================================
        # Upstream API Responses - Raw response storage for debugging
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS upstream_responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,

                -- Upstream request
                upstream_url TEXT NOT NULL,
                upstream_method TEXT NOT NULL,
                upstream_headers TEXT,
                upstream_body TEXT,

                -- Upstream response
                upstream_status INTEGER,
                upstream_response_headers TEXT,
                upstream_response_body TEXT,

                -- Timing
                started_at TEXT NOT NULL,
                completed_at TEXT,
                duration_ms INTEGER,

                -- Error capture
                error_type TEXT,
                error_message TEXT,

                FOREIGN KEY (request_id) REFERENCES api_audit_log(request_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upstream_request ON upstream_responses(request_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upstream_status ON upstream_responses(upstream_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upstream_error ON upstream_responses(error_type)")

        # Full-text search virtual tables
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS task_runs_fts USING fts5(
                run_id, input_text, output_text, error,
                content='task_runs',
                content_rowid='rowid'
            )
        """)

        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS search_requests_fts USING fts5(
                objective, results,
                content='search_requests',
                content_rowid='id'
            )
        """)

        # Triggers to keep FTS in sync
        conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS task_runs_ai AFTER INSERT ON task_runs BEGIN
                INSERT INTO task_runs_fts(rowid, run_id, input_text, output_text, error)
                VALUES (NEW.rowid, NEW.run_id, NEW.input_text, NEW.output_text, NEW.error);
            END;

            CREATE TRIGGER IF NOT EXISTS task_runs_ad AFTER DELETE ON task_runs BEGIN
                INSERT INTO task_runs_fts(task_runs_fts, rowid, run_id, input_text, output_text, error)
                VALUES ('delete', OLD.rowid, OLD.run_id, OLD.input_text, OLD.output_text, OLD.error);
            END;

            CREATE TRIGGER IF NOT EXISTS task_runs_au AFTER UPDATE ON task_runs BEGIN
                INSERT INTO task_runs_fts(task_runs_fts, rowid, run_id, input_text, output_text, error)
                VALUES ('delete', OLD.rowid, OLD.run_id, OLD.input_text, OLD.output_text, OLD.error);
                INSERT INTO task_runs_fts(rowid, run_id, input_text, output_text, error)
                VALUES (NEW.rowid, NEW.run_id, NEW.input_text, NEW.output_text, NEW.error);
            END;
        """)

        # Set schema version
        conn.execute(
            "INSERT OR REPLACE INTO schema_info (key, value) VALUES ('version', ?)",
            (str(SCHEMA_VERSION),)
        )

        conn.commit()

    def _run_migrations(self) -> None:
        """Run any pending schema migrations."""
        conn = self._get_conn()

        # Get current schema version
        try:
            result = conn.execute(
                "SELECT value FROM schema_info WHERE key = 'version'"
            ).fetchone()
            current_version = int(result[0]) if result else 0
        except sqlite3.OperationalError:
            current_version = 0

        if current_version < SCHEMA_VERSION:
            logger.info(f"Migrating schema from v{current_version} to v{SCHEMA_VERSION}")

            # Migration v1 -> v2: Add taskgroup_id, duration_ms to task_runs
            self._add_column_if_not_exists(conn, "task_runs", "taskgroup_id", "TEXT")
            self._add_column_if_not_exists(conn, "task_runs", "duration_ms", "INTEGER")

            # Add missing columns to other tables that may have been created before
            self._add_column_if_not_exists(conn, "search_requests", "duration_ms", "INTEGER")
            self._add_column_if_not_exists(conn, "extract_requests", "duration_ms", "INTEGER")

            # Migration v2 -> v3: Add events table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    event_type TEXT NOT NULL,
                    resource_id TEXT,
                    resource_type TEXT,
                    data TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_resource ON events(resource_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)")

            # Migration v3 -> v4: Add audit log, storage failures, upstream responses
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT UNIQUE NOT NULL,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    query_params TEXT,
                    request_headers TEXT,
                    request_body TEXT,
                    request_body_size INTEGER,
                    client_ip TEXT,
                    user_agent TEXT,
                    status_code INTEGER,
                    response_headers TEXT,
                    response_body TEXT,
                    response_body_size INTEGER,
                    upstream_request_id TEXT,
                    context_id TEXT,
                    run_id TEXT,
                    taskgroup_id TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    duration_ms INTEGER,
                    upstream_duration_ms INTEGER,
                    error_type TEXT,
                    error_message TEXT,
                    error_traceback TEXT,
                    operation_type TEXT,
                    was_successful INTEGER DEFAULT 1,
                    was_stored INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS storage_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    failure_id TEXT UNIQUE NOT NULL,
                    operation_type TEXT NOT NULL,
                    target_table TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    error_traceback TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 5,
                    next_retry_at TEXT,
                    last_retry_at TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved_at TEXT,
                    resolution_type TEXT,
                    created_at TEXT NOT NULL,
                    modified_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS upstream_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    upstream_url TEXT NOT NULL,
                    upstream_method TEXT NOT NULL,
                    upstream_headers TEXT,
                    upstream_body TEXT,
                    upstream_status INTEGER,
                    upstream_response_headers TEXT,
                    upstream_response_body TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    duration_ms INTEGER,
                    error_type TEXT,
                    error_message TEXT
                )
            """)

            # Migration v4 -> v5: Add token tracking columns to audit log
            self._add_column_if_not_exists(conn, "api_audit_log", "request_tokens", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "response_tokens", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "total_tokens", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "request_bytes", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "response_bytes", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "total_bytes", "INTEGER")
            self._add_column_if_not_exists(conn, "api_audit_log", "token_model", "TEXT")
            self._add_column_if_not_exists(conn, "api_audit_log", "size_bucket", "TEXT")

            conn.execute(
                "INSERT OR REPLACE INTO schema_info (key, value) VALUES ('version', ?)",
                (str(SCHEMA_VERSION),)
            )
            conn.commit()

    def _add_column_if_not_exists(self, conn: sqlite3.Connection, table: str, column: str, col_type: str) -> bool:
        """Add a column to a table if it doesn't exist.

        Args:
            conn: SQLite connection
            table: Table name
            column: Column name to add
            col_type: SQLite column type (TEXT, INTEGER, REAL, etc.)

        Returns:
            True if column was added, False if it already existed
        """
        try:
            # Check if column exists
            cursor = conn.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            if column not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                logger.info(f"Added column {column} to {table}")
                return True
            return False
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not add column {column} to {table}: {e}")
            return False

    def _create_indexes(self) -> None:
        """Create indexes for common queries (called after migrations)."""
        conn = self._get_conn()

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_task_runs_processor ON task_runs(processor)",
            "CREATE INDEX IF NOT EXISTS idx_task_runs_created ON task_runs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_task_runs_taskgroup ON task_runs(taskgroup_id)",
            "CREATE INDEX IF NOT EXISTS idx_task_groups_status ON task_groups(status)",
            "CREATE INDEX IF NOT EXISTS idx_findall_status ON findall_runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_findall_created ON findall_runs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_findall_candidates_findall ON findall_candidates(findall_id)",
            "CREATE INDEX IF NOT EXISTS idx_monitors_status ON monitors(status)",
            "CREATE INDEX IF NOT EXISTS idx_monitor_runs_monitor ON monitor_runs(monitor_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_created ON chat_completions(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_search_created ON search_requests(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_extract_created ON extract_requests(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_api_usage_endpoint ON api_usage(endpoint)",
            "CREATE INDEX IF NOT EXISTS idx_api_usage_created ON api_usage(created_at)",
        ]
        for idx_sql in indexes:
            try:
                conn.execute(idx_sql)
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not create index: {e}")

        conn.commit()

    # =========================================================================
    # JSON Helpers
    # =========================================================================

    def _json_dumps(self, obj: Any) -> Optional[str]:
        """Safely serialize to JSON."""
        if obj is None:
            return None
        return json.dumps(obj, default=str, ensure_ascii=False)

    def _json_loads(self, s: Optional[str]) -> Any:
        """Safely deserialize from JSON."""
        if s is None:
            return None
        try:
            return json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return s

    def _row_to_dict(self, row: sqlite3.Row) -> Dict:
        """Convert a database row to a dictionary with JSON parsing."""
        result = dict(row)
        # Parse JSON fields
        json_fields = [
            'input_json', 'output_json', 'task_spec', 'metadata', 'config',
            'messages', 'results', 'search_queries', 'urls', 'candidate_data',
            'snapshot', 'diff'
        ]
        for key in json_fields:
            if key in result and result[key]:
                result[key] = self._json_loads(result[key])
        return result

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
        taskgroup_id: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        """Store or update a task run.

        Args:
            run_id: Unique task run identifier (e.g., "trun_abc123")
            processor: Processor type (lite, base, core, pro, ultra, etc.)
            status: Current status (queued, running, completed, failed, cancelled)
            input_text: Plain text input (for text-based tasks)
            input_json: Structured JSON input
            task_spec: Task specification/configuration
            output_text: Plain text output
            output_json: Structured JSON output
            metadata: Additional metadata
            error: Error message if failed
            taskgroup_id: Parent task group ID if part of a group
            duration_ms: Execution duration in milliseconds
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO task_runs (
                run_id, processor, status, input_text, input_json, task_spec,
                output_text, output_json, metadata, taskgroup_id, created_at,
                modified_at, error, duration_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                status = excluded.status,
                output_text = COALESCE(excluded.output_text, task_runs.output_text),
                output_json = COALESCE(excluded.output_json, task_runs.output_json),
                modified_at = excluded.modified_at,
                completed_at = CASE WHEN excluded.status IN ('completed', 'failed', 'cancelled')
                    THEN excluded.modified_at ELSE task_runs.completed_at END,
                error = COALESCE(excluded.error, task_runs.error),
                duration_ms = COALESCE(excluded.duration_ms, task_runs.duration_ms)
            """,
            (
                run_id, processor, status, input_text,
                self._json_dumps(input_json), self._json_dumps(task_spec),
                output_text, self._json_dumps(output_json),
                self._json_dumps(metadata), taskgroup_id, now, now, error, duration_ms
            ),
        )
        conn.commit()

    def get_task_run(self, run_id: str) -> Optional[Dict]:
        """Retrieve a task run by ID.

        Args:
            run_id: The task run ID to retrieve

        Returns:
            Task run data as a dictionary, or None if not found
        """
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result)
        return None

    def list_task_runs(
        self,
        status: Optional[str] = None,
        processor: Optional[str] = None,
        taskgroup_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict]:
        """List task runs with optional filters.

        Args:
            status: Filter by status (queued, running, completed, failed, cancelled)
            processor: Filter by processor type
            taskgroup_id: Filter by task group
            since: Only include runs created after this time
            until: Only include runs created before this time
            limit: Maximum number of results (default 50)
            offset: Number of results to skip (for pagination)

        Returns:
            List of task run dictionaries
        """
        conn = self._get_conn()
        query = "SELECT * FROM task_runs WHERE 1=1"
        params: List[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if processor:
            query += " AND processor = ?"
            params.append(processor)
        if taskgroup_id:
            query += " AND taskgroup_id = ?"
            params.append(taskgroup_id)
        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())
        if until:
            query += " AND created_at <= ?"
            params.append(until.isoformat())

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

    def get_recent_task_ids(self, limit: int = 20) -> List[str]:
        """Get most recent task run IDs.

        Args:
            limit: Maximum number of IDs to return

        Returns:
            List of task run IDs, most recent first
        """
        conn = self._get_conn()
        results = conn.execute(
            "SELECT run_id FROM task_runs ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [row[0] for row in results]

    def delete_task_run(self, run_id: str) -> bool:
        """Delete a task run by ID.

        Args:
            run_id: The task run ID to delete

        Returns:
            True if deleted, False if not found
        """
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM task_runs WHERE run_id = ?", (run_id,))
        conn.commit()
        return cursor.rowcount > 0

    def update_task_status(
        self,
        run_id: str,
        status: str,
        error: Optional[str] = None,
        output_text: Optional[str] = None,
        output_json: Optional[Any] = None,  # Any JSON-serializable type
    ) -> bool:
        """Update task run status.

        Args:
            run_id: The task run ID
            status: New status
            error: Error message if failed
            output_text: Output text to set
            output_json: Output JSON to set

        Returns:
            True if updated, False if not found
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        cursor = conn.execute(
            """
            UPDATE task_runs SET
                status = ?,
                modified_at = ?,
                completed_at = CASE WHEN ? IN ('completed', 'failed', 'cancelled')
                    THEN ? ELSE completed_at END,
                error = COALESCE(?, error),
                output_text = COALESCE(?, output_text),
                output_json = COALESCE(?, output_json)
            WHERE run_id = ?
            """,
            (status, now, status, now, error, output_text,
             self._json_dumps(output_json), run_id)
        )
        conn.commit()
        return cursor.rowcount > 0

    # =========================================================================
    # Task Groups
    # =========================================================================

    def save_task_group(
        self,
        taskgroup_id: str,
        status: str = "active",
        metadata: Optional[Dict] = None,
        num_task_runs: int = 0,
        completed_runs: int = 0,
        failed_runs: int = 0,
    ) -> None:
        """Store or update a task group.

        Args:
            taskgroup_id: Unique task group identifier
            status: Group status (active, inactive, completed)
            metadata: Additional metadata
            num_task_runs: Total number of runs in the group
            completed_runs: Number of completed runs
            failed_runs: Number of failed runs
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO task_groups (
                taskgroup_id, status, metadata, num_task_runs,
                completed_runs, failed_runs, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(taskgroup_id) DO UPDATE SET
                status = excluded.status,
                metadata = COALESCE(excluded.metadata, task_groups.metadata),
                num_task_runs = excluded.num_task_runs,
                completed_runs = excluded.completed_runs,
                failed_runs = excluded.failed_runs,
                modified_at = excluded.modified_at
            """,
            (taskgroup_id, status, self._json_dumps(metadata),
             num_task_runs, completed_runs, failed_runs, now, now)
        )
        conn.commit()

    def get_task_group(self, taskgroup_id: str) -> Optional[Dict]:
        """Retrieve a task group by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM task_groups WHERE taskgroup_id = ?", (taskgroup_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result)
        return None

    def list_task_groups(
        self,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List task groups with optional filters."""
        conn = self._get_conn()
        query = "SELECT * FROM task_groups WHERE 1=1"
        params: List[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

    def get_task_group_runs(self, taskgroup_id: str) -> List[Dict]:
        """Get all task runs belonging to a task group."""
        return self.list_task_runs(taskgroup_id=taskgroup_id, limit=1000)

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
        """Store or update a FindAll run.

        Args:
            findall_id: Unique FindAll run identifier
            objective: The search objective
            status: Current status
            entity_type: Type of entities being searched for
            generator: Generator type used
            match_limit: Maximum matches requested
            candidates_found: Number of candidates found
            candidates_matched: Number of candidates that matched criteria
            termination_reason: Why the run ended (if applicable)
            metadata: Additional metadata
            error: Error message if failed
        """
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

    def save_findall_candidate(
        self,
        findall_id: str,
        candidate_data: Dict,
        match_score: Optional[float] = None,
        is_matched: bool = False,
    ) -> int:
        """Save a FindAll candidate result.

        Args:
            findall_id: Parent FindAll run ID
            candidate_data: The candidate data
            match_score: Match score if available
            is_matched: Whether this candidate matched criteria

        Returns:
            The candidate row ID
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        cursor = conn.execute(
            """
            INSERT INTO findall_candidates (
                findall_id, candidate_data, match_score, is_matched, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (findall_id, self._json_dumps(candidate_data), match_score,
             1 if is_matched else 0, now)
        )
        conn.commit()
        return cursor.lastrowid

    def get_findall_run(self, findall_id: str) -> Optional[Dict]:
        """Retrieve a FindAll run by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM findall_runs WHERE findall_id = ?", (findall_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result)
        return None

    def get_findall_candidates(
        self,
        findall_id: str,
        matched_only: bool = False,
        limit: int = 100,
    ) -> List[Dict]:
        """Get candidates for a FindAll run.

        Args:
            findall_id: The FindAll run ID
            matched_only: Only return matched candidates
            limit: Maximum results

        Returns:
            List of candidate dictionaries
        """
        conn = self._get_conn()
        query = "SELECT * FROM findall_candidates WHERE findall_id = ?"
        params: List[Any] = [findall_id]

        if matched_only:
            query += " AND is_matched = 1"

        query += " ORDER BY match_score DESC NULLS LAST LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

    def list_findall_runs(
        self,
        status: Optional[str] = None,
        entity_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List FindAll runs with optional filters."""
        conn = self._get_conn()
        query = "SELECT * FROM findall_runs WHERE 1=1"
        params: List[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

    # =========================================================================
    # Monitors
    # =========================================================================

    def save_monitor(
        self,
        monitor_id: str,
        url: str,
        name: Optional[str] = None,
        schedule: Optional[str] = None,
        status: str = "active",
        config: Optional[Dict] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        """Store or update a monitor.

        Args:
            monitor_id: Unique monitor identifier
            url: URL to monitor
            name: Human-readable name
            schedule: Cron-like schedule
            status: Monitor status (active, paused, stopped)
            config: Monitor configuration
            metadata: Additional metadata
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO monitors (
                monitor_id, name, url, schedule, status, config, metadata,
                created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(monitor_id) DO UPDATE SET
                name = COALESCE(excluded.name, monitors.name),
                url = excluded.url,
                schedule = COALESCE(excluded.schedule, monitors.schedule),
                status = excluded.status,
                config = COALESCE(excluded.config, monitors.config),
                metadata = COALESCE(excluded.metadata, monitors.metadata),
                modified_at = excluded.modified_at
            """,
            (monitor_id, name, url, schedule, status,
             self._json_dumps(config), self._json_dumps(metadata), now, now)
        )
        conn.commit()

    def save_monitor_run(
        self,
        run_id: str,
        monitor_id: str,
        status: str,
        changes_detected: int = 0,
        snapshot: Optional[Dict] = None,
        diff: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """Save a monitor run result."""
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO monitor_runs (
                run_id, monitor_id, status, changes_detected, snapshot, diff,
                error, created_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, monitor_id, status, changes_detected,
             self._json_dumps(snapshot), self._json_dumps(diff),
             error, now, now if status in ('completed', 'failed') else None)
        )

        # Update monitor last_run_at
        conn.execute(
            "UPDATE monitors SET last_run_at = ?, modified_at = ? WHERE monitor_id = ?",
            (now, now, monitor_id)
        )
        conn.commit()

    def get_monitor(self, monitor_id: str) -> Optional[Dict]:
        """Retrieve a monitor by ID."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM monitors WHERE monitor_id = ?", (monitor_id,)
        ).fetchone()
        if result:
            return self._row_to_dict(result)
        return None

    def list_monitors(self, status: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """List monitors with optional status filter."""
        conn = self._get_conn()
        query = "SELECT * FROM monitors WHERE 1=1"
        params: List[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

    def get_monitor_runs(self, monitor_id: str, limit: int = 20) -> List[Dict]:
        """Get recent runs for a monitor."""
        conn = self._get_conn()
        results = conn.execute(
            """SELECT * FROM monitor_runs WHERE monitor_id = ?
               ORDER BY created_at DESC LIMIT ?""",
            (monitor_id, limit)
        ).fetchall()
        return [self._row_to_dict(row) for row in results]

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
        """Store a chat completion.

        Args:
            model: Model used for completion
            messages: Input messages
            completion_id: Optional completion ID from API
            response: Generated response text
            usage_prompt_tokens: Prompt token count
            usage_completion_tokens: Completion token count
            usage_total_tokens: Total token count
            duration_ms: Request duration
            error: Error message if failed

        Returns:
            The row ID of the saved completion
        """
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
            return self._row_to_dict(result)
        return None

    def list_chat_completions(
        self,
        model: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List recent chat completions."""
        conn = self._get_conn()
        query = "SELECT * FROM chat_completions WHERE 1=1"
        params: List[Any] = []

        if model:
            query += " AND model = ?"
            params.append(model)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in results]

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
        """Store a search request.

        Args:
            objective: Search objective
            request_id: Optional request ID
            search_queries: Generated search queries
            max_results: Maximum results requested
            results_count: Actual results count
            results: Search results
            duration_ms: Request duration
            error: Error message if failed

        Returns:
            The row ID
        """
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
        return [self._row_to_dict(row) for row in results]

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
        """Store an extract request.

        Args:
            urls: URLs to extract from
            extract_id: Optional request ID
            objective: Extraction objective
            results_count: Successful extractions
            errors_count: Failed extractions
            results: Extraction results
            duration_ms: Request duration
            error: Error message if failed

        Returns:
            The row ID
        """
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
        return [self._row_to_dict(row) for row in results]

    # =========================================================================
    # API Usage Tracking
    # =========================================================================

    def log_api_usage(
        self,
        endpoint: str,
        method: str,
        status_code: Optional[int] = None,
        duration_ms: Optional[int] = None,
        request_size: Optional[int] = None,
        response_size: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """Log an API request for usage tracking.

        Args:
            endpoint: API endpoint path
            method: HTTP method
            status_code: Response status code
            duration_ms: Request duration
            request_size: Request body size in bytes
            response_size: Response body size in bytes
            error: Error message if failed
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO api_usage (
                endpoint, method, status_code, duration_ms,
                request_size, response_size, error, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (endpoint, method, status_code, duration_ms,
             request_size, response_size, error, now)
        )
        conn.commit()

    def get_api_usage_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get API usage statistics for the past N days.

        Args:
            days: Number of days to include

        Returns:
            Dictionary with usage statistics
        """
        conn = self._get_conn()
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()

        # Total requests
        total = conn.execute(
            "SELECT COUNT(*) FROM api_usage WHERE created_at >= ?", (since,)
        ).fetchone()[0]

        # By endpoint
        by_endpoint = conn.execute(
            """SELECT endpoint, COUNT(*) as count, AVG(duration_ms) as avg_duration
               FROM api_usage WHERE created_at >= ?
               GROUP BY endpoint ORDER BY count DESC""",
            (since,)
        ).fetchall()

        # Error rate
        errors = conn.execute(
            "SELECT COUNT(*) FROM api_usage WHERE created_at >= ? AND error IS NOT NULL",
            (since,)
        ).fetchone()[0]

        return {
            'period_days': days,
            'total_requests': total,
            'error_count': errors,
            'error_rate': round(errors / total * 100, 2) if total > 0 else 0,
            'by_endpoint': [dict(row) for row in by_endpoint],
        }

    # =========================================================================
    # Search and Analytics
    # =========================================================================

    def search_all(self, query: str, limit: int = 20) -> Dict[str, List[Dict]]:
        """Search across all tables for a query string.

        Uses full-text search where available, falls back to LIKE queries.

        Args:
            query: Search query
            limit: Maximum results per table

        Returns:
            Dictionary with results grouped by table
        """
        conn = self._get_conn()
        results = {}

        # Full-text search on task runs
        try:
            task_results = conn.execute(
                """SELECT task_runs.* FROM task_runs
                   JOIN task_runs_fts ON task_runs.rowid = task_runs_fts.rowid
                   WHERE task_runs_fts MATCH ?
                   ORDER BY rank LIMIT ?""",
                (query, limit)
            ).fetchall()
        except sqlite3.OperationalError:
            # Fallback to LIKE if FTS fails
            task_results = conn.execute(
                """SELECT * FROM task_runs
                   WHERE run_id LIKE ? OR input_text LIKE ? OR output_text LIKE ?
                   ORDER BY created_at DESC LIMIT ?""",
                (f"%{query}%", f"%{query}%", f"%{query}%", limit)
            ).fetchall()
        results['task_runs'] = [self._row_to_dict(row) for row in task_results]

        # Search FindAll runs
        findall_results = conn.execute(
            """SELECT * FROM findall_runs
               WHERE findall_id LIKE ? OR objective LIKE ? OR entity_type LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        results['findall_runs'] = [self._row_to_dict(row) for row in findall_results]

        # Search chat completions
        chat_results = conn.execute(
            """SELECT * FROM chat_completions
               WHERE completion_id LIKE ? OR messages LIKE ? OR response LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        results['chat_completions'] = [self._row_to_dict(row) for row in chat_results]

        # Search search requests
        search_results = conn.execute(
            """SELECT * FROM search_requests
               WHERE objective LIKE ? OR results LIKE ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", limit)
        ).fetchall()
        results['search_requests'] = [self._row_to_dict(row) for row in search_results]

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics.

        Returns:
            Dictionary with counts, sizes, and status breakdowns
        """
        conn = self._get_conn()
        stats: Dict[str, Any] = {}

        # Table counts
        for table in ['task_runs', 'task_groups', 'findall_runs', 'monitors',
                      'chat_completions', 'search_requests', 'extract_requests']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = {'count': count}

        # Task status breakdown
        status_counts = conn.execute(
            """SELECT status, COUNT(*) as count FROM task_runs
               GROUP BY status ORDER BY count DESC"""
        ).fetchall()
        stats['task_status_breakdown'] = {row[0]: row[1] for row in status_counts}

        # Processor breakdown
        processor_counts = conn.execute(
            """SELECT processor, COUNT(*) as count FROM task_runs
               GROUP BY processor ORDER BY count DESC"""
        ).fetchall()
        stats['processor_breakdown'] = {row[0]: row[1] for row in processor_counts}

        # Recent activity (last 24h)
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        stats['last_24h'] = {
            'task_runs': conn.execute(
                "SELECT COUNT(*) FROM task_runs WHERE created_at >= ?", (yesterday,)
            ).fetchone()[0],
            'searches': conn.execute(
                "SELECT COUNT(*) FROM search_requests WHERE created_at >= ?", (yesterday,)
            ).fetchone()[0],
            'extracts': conn.execute(
                "SELECT COUNT(*) FROM extract_requests WHERE created_at >= ?", (yesterday,)
            ).fetchone()[0],
        }

        # Database file sizes
        if os.path.exists(self.sqlite_path):
            stats['sqlite_size_mb'] = round(
                os.path.getsize(self.sqlite_path) / (1024 * 1024), 2
            )
        if HAS_DUCKDB and os.path.exists(self.duckdb_path):
            stats['duckdb_size_mb'] = round(
                os.path.getsize(self.duckdb_path) / (1024 * 1024), 2
            )

        stats['duckdb_available'] = HAS_DUCKDB
        stats['schema_version'] = SCHEMA_VERSION

        return stats

    def get_task_analytics(self, days: int = 30) -> Dict[str, Any]:
        """Get task run analytics for the past N days.

        Args:
            days: Number of days to analyze

        Returns:
            Analytics data including trends, success rates, etc.
        """
        conn = self._get_conn()
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()

        # Daily counts
        daily_counts = conn.execute(
            """SELECT DATE(created_at) as date, COUNT(*) as count,
                      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
               FROM task_runs WHERE created_at >= ?
               GROUP BY DATE(created_at) ORDER BY date""",
            (since,)
        ).fetchall()

        # Average duration by processor
        avg_duration = conn.execute(
            """SELECT processor, AVG(duration_ms) as avg_ms, COUNT(*) as count
               FROM task_runs
               WHERE created_at >= ? AND duration_ms IS NOT NULL
               GROUP BY processor ORDER BY avg_ms DESC""",
            (since,)
        ).fetchall()

        # Success rate
        totals = conn.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
               FROM task_runs WHERE created_at >= ?""",
            (since,)
        ).fetchone()

        total_count = totals[0] or 0
        completed_count = totals[1] or 0

        return {
            'period_days': days,
            'total_runs': total_count,
            'completed_runs': completed_count,
            'failed_runs': totals[2] or 0,
            'success_rate': round(completed_count / total_count * 100, 2) if total_count > 0 else 0,
            'daily_counts': [dict(row) for row in daily_counts],
            'avg_duration_by_processor': [dict(row) for row in avg_duration],
        }

    # =========================================================================
    # Event Storage (30 day rolling)
    # =========================================================================

    def save_event(
        self,
        event_id: str,
        event_type: str,
        data: Dict[str, Any],
        resource_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        created_at: Optional[str] = None,
    ) -> None:
        """Save an event to persistent storage.

        Args:
            event_id: Unique event identifier
            event_type: Type of event (e.g., 'task_run.created')
            data: Event payload data
            resource_id: Optional resource ID this event relates to
            resource_type: Optional resource type (task_run, task_group, etc.)
            created_at: Optional timestamp (defaults to now)
        """
        conn = self._get_conn()
        now = created_at or datetime.utcnow().isoformat()

        conn.execute(
            """INSERT OR REPLACE INTO events
               (event_id, event_type, resource_id, resource_type, data, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (event_id, event_type, resource_id, resource_type, self._json_dumps(data), now)
        )
        conn.commit()

    def list_events(
        self,
        event_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict]:
        """List events with optional filters.

        Args:
            event_type: Filter by event type
            resource_id: Filter by resource ID
            since: Only include events after this time
            until: Only include events before this time
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of event dictionaries
        """
        conn = self._get_conn()
        query = "SELECT * FROM events WHERE 1=1"
        params = []

        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        if resource_id:
            query += " AND resource_id = ?"
            params.append(resource_id)
        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())
        if until:
            query += " AND created_at <= ?"
            params.append(until.isoformat())

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        results = conn.execute(query, params).fetchall()
        events = []
        for row in results:
            event = self._row_to_dict(row)
            if event.get('data'):
                event['data'] = json.loads(event['data'])
            events.append(event)
        return events

    def cleanup_old_events(self, days: int = 30) -> int:
        """Delete events older than N days.

        Args:
            days: Number of days to keep events

        Returns:
            Number of events deleted
        """
        conn = self._get_conn()
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        result = conn.execute(
            "DELETE FROM events WHERE created_at < ?",
            (cutoff,)
        )
        conn.commit()
        return result.rowcount

    def get_event_count(self, since: Optional[datetime] = None) -> int:
        """Get count of events, optionally since a timestamp."""
        conn = self._get_conn()
        if since:
            result = conn.execute(
                "SELECT COUNT(*) FROM events WHERE created_at >= ?",
                (since.isoformat(),)
            ).fetchone()
        else:
            result = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return result[0] if result else 0

    # =========================================================================
    # Export and Import
    # =========================================================================

    def export_to_json(self, output_path: str, tables: Optional[List[str]] = None) -> Dict[str, int]:
        """Export data to JSON file.

        Args:
            output_path: Path to output JSON file
            tables: List of tables to export (defaults to all)

        Returns:
            Dictionary with row counts per table
        """
        conn = self._get_conn()
        all_tables = ['task_runs', 'task_groups', 'findall_runs', 'monitors',
                      'chat_completions', 'search_requests', 'extract_requests']
        tables = tables or all_tables

        data = {}
        counts = {}

        for table in tables:
            if table not in all_tables:
                continue
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            data[table] = [self._row_to_dict(row) for row in rows]
            counts[table] = len(rows)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Exported {sum(counts.values())} rows to {output_path}")
        return counts

    def import_from_json(self, input_path: str) -> Dict[str, int]:
        """Import data from JSON file.

        Args:
            input_path: Path to input JSON file

        Returns:
            Dictionary with imported row counts per table
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        counts = {}

        if 'task_runs' in data:
            for row in data['task_runs']:
                self.save_task_run(**row)
            counts['task_runs'] = len(data['task_runs'])

        if 'task_groups' in data:
            for row in data['task_groups']:
                self.save_task_group(**row)
            counts['task_groups'] = len(data['task_groups'])

        if 'findall_runs' in data:
            for row in data['findall_runs']:
                self.save_findall_run(**row)
            counts['findall_runs'] = len(data['findall_runs'])

        logger.info(f"Imported {sum(counts.values())} rows from {input_path}")
        return counts

    def export_to_parquet(self, output_dir: str) -> Dict[str, str]:
        """Export data to Parquet files (requires DuckDB).

        Args:
            output_dir: Directory for output files

        Returns:
            Dictionary mapping table names to file paths
        """
        if not HAS_DUCKDB:
            raise RuntimeError("DuckDB required for Parquet export")

        self.sync_to_duckdb()
        duck = self._get_duck()
        os.makedirs(output_dir, exist_ok=True)

        files = {}
        tables = ['task_runs', 'task_groups', 'findall_runs', 'monitors',
                  'chat_completions', 'search_requests', 'extract_requests']

        for table in tables:
            path = os.path.join(output_dir, f"{table}.parquet")
            duck.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
            files[table] = path

        logger.info(f"Exported to Parquet in {output_dir}")
        return files

    # =========================================================================
    # Backup and Maintenance
    # =========================================================================

    def backup(self, backup_name: Optional[str] = None) -> str:
        """Create a backup of the SQLite database.

        Args:
            backup_name: Optional name for backup (defaults to timestamp)

        Returns:
            Path to backup file
        """
        if backup_name is None:
            backup_name = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        backup_path = DEFAULT_BACKUP_DIR / f"parallel_ai_{backup_name}.db"
        shutil.copy2(self.sqlite_path, backup_path)

        logger.info(f"Created backup: {backup_path}")
        return str(backup_path)

    def restore(self, backup_path: str) -> None:
        """Restore from a backup file.

        Args:
            backup_path: Path to backup file
        """
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup not found: {backup_path}")

        # Close current connection
        self.close()

        # Create safety backup of current state
        safety_backup = self.backup("pre_restore")
        logger.info(f"Created safety backup: {safety_backup}")

        # Restore
        shutil.copy2(backup_path, self.sqlite_path)
        logger.info(f"Restored from: {backup_path}")

        # Reconnect
        self._initialized = False
        self.connect()

    def list_backups(self) -> List[Dict[str, Any]]:
        """List available backups.

        Returns:
            List of backup info dictionaries
        """
        backups = []
        for f in sorted(DEFAULT_BACKUP_DIR.glob("parallel_ai_*.db"), reverse=True):
            stat = f.stat()
            backups.append({
                'name': f.name,
                'path': str(f),
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'created_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
        return backups

    def vacuum(self) -> Dict[str, float]:
        """Reclaim unused space and optimize database.

        Returns:
            Size before and after in MB
        """
        size_before = os.path.getsize(self.sqlite_path) / (1024 * 1024)

        conn = self._get_conn()
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        conn.commit()

        size_after = os.path.getsize(self.sqlite_path) / (1024 * 1024)

        return {
            'size_before_mb': round(size_before, 2),
            'size_after_mb': round(size_after, 2),
            'reclaimed_mb': round(size_before - size_after, 2),
        }

    def cleanup_old_data(
        self,
        days: int = 90,
        dry_run: bool = True,
    ) -> Dict[str, int]:
        """Delete data older than N days.

        Args:
            days: Delete data older than this many days
            dry_run: If True, only report what would be deleted

        Returns:
            Dictionary with deletion counts per table
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        conn = self._get_conn()

        tables = {
            'task_runs': 'created_at',
            'findall_runs': 'created_at',
            'chat_completions': 'created_at',
            'search_requests': 'created_at',
            'extract_requests': 'created_at',
            'api_usage': 'created_at',
        }

        counts = {}
        for table, date_col in tables.items():
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {date_col} < ?",
                (cutoff,)
            ).fetchone()[0]
            counts[table] = count

            if not dry_run and count > 0:
                conn.execute(f"DELETE FROM {table} WHERE {date_col} < ?", (cutoff,))

        if not dry_run:
            conn.commit()
            logger.info(f"Deleted {sum(counts.values())} old records")

        return counts

    # =========================================================================
    # DuckDB Analytics
    # =========================================================================

    def sync_to_duckdb(self) -> None:
        """Sync SQLite data to DuckDB for analytics."""
        if not HAS_DUCKDB:
            return

        duck = self._get_duck()
        if duck is None:
            return

        # Attach SQLite and copy tables
        duck.execute(f"ATTACH '{self.sqlite_path}' AS sqlite_db (TYPE SQLITE)")

        for table in ['task_runs', 'task_groups', 'findall_runs', 'monitors',
                      'chat_completions', 'search_requests', 'extract_requests']:
            duck.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM sqlite_db.{table}
            """)

        duck.execute("DETACH sqlite_db")
        logger.info("Synced data to DuckDB")

    def analytics_query(self, sql: str) -> List[Dict]:
        """Run an analytics query on DuckDB.

        Args:
            sql: SQL query to execute

        Returns:
            Query results as list of dictionaries
        """
        if not HAS_DUCKDB:
            raise RuntimeError("DuckDB not installed. Install with: pip install duckdb")

        duck = self._get_duck()
        if duck is None:
            raise RuntimeError("DuckDB connection not available")

        result = duck.execute(sql).fetchdf()
        return result.to_dict(orient='records')

    # =========================================================================
    # Audit Log Methods
    # =========================================================================

    def save_audit_log(
        self,
        request_id: str,
        method: str,
        path: str,
        started_at: str,
        query_params: Optional[Dict] = None,
        request_headers: Optional[Dict] = None,
        request_body: Optional[Any] = None,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        status_code: Optional[int] = None,
        response_headers: Optional[Dict] = None,
        response_body: Optional[Any] = None,
        completed_at: Optional[str] = None,
        duration_ms: Optional[int] = None,
        upstream_request_id: Optional[str] = None,
        upstream_duration_ms: Optional[int] = None,
        context_id: Optional[str] = None,
        run_id: Optional[str] = None,
        taskgroup_id: Optional[str] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        operation_type: Optional[str] = None,
        was_successful: bool = True,
        was_stored: bool = False,
        request_tokens: Optional[int] = None,
        response_tokens: Optional[int] = None,
        total_tokens: Optional[int] = None,
        request_bytes: Optional[int] = None,
        response_bytes: Optional[int] = None,
        total_bytes: Optional[int] = None,
        token_model: Optional[str] = None,
        size_bucket: Optional[str] = None,
    ) -> bool:
        """Save a complete audit log entry for a request/response.

        This captures everything about an API request for debugging and analysis.

        Args:
            request_id: Unique request identifier (X-Request-ID)
            method: HTTP method (GET, POST, etc.)
            path: Request path
            started_at: ISO timestamp when request started
            query_params: Query string parameters
            request_headers: Request headers (sensitive headers filtered)
            request_body: Request body (JSON or text)
            client_ip: Client IP address
            user_agent: User-Agent header
            status_code: HTTP response status code
            response_headers: Response headers
            response_body: Response body (JSON or text)
            completed_at: ISO timestamp when request completed
            duration_ms: Total request duration in milliseconds
            upstream_request_id: Request ID from upstream API (Parallel)
            upstream_duration_ms: Time spent waiting for upstream
            context_id: Conversation context ID if provided
            run_id: Task run ID if this is a task operation
            taskgroup_id: Task group ID if part of a group
            error_type: Type of error if request failed
            error_message: Error message if request failed
            error_traceback: Full traceback if available
            operation_type: Classification (task.run, search, extract, etc.)
            was_successful: Whether the request succeeded
            was_stored: Whether data was successfully persisted

        Returns:
            True if saved successfully, False otherwise
        """
        conn = self._get_conn()

        # Calculate body sizes
        request_body_str = self._json_dumps(request_body) if request_body else None
        response_body_str = self._json_dumps(response_body) if response_body else None
        request_size = len(request_body_str) if request_body_str else 0
        response_size = len(response_body_str) if response_body_str else 0

        # Filter sensitive headers
        safe_request_headers = self._filter_sensitive_headers(request_headers)
        safe_response_headers = self._filter_sensitive_headers(response_headers)

        try:
            conn.execute(
                """
                INSERT INTO api_audit_log (
                    request_id, method, path, query_params, request_headers,
                    request_body, request_body_size, client_ip, user_agent,
                    status_code, response_headers, response_body, response_body_size,
                    upstream_request_id, context_id, run_id, taskgroup_id,
                    started_at, completed_at, duration_ms, upstream_duration_ms,
                    error_type, error_message, error_traceback,
                    operation_type, was_successful, was_stored,
                    request_tokens, response_tokens, total_tokens,
                    request_bytes, response_bytes, total_bytes,
                    token_model, size_bucket
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    status_code = COALESCE(excluded.status_code, api_audit_log.status_code),
                    response_headers = COALESCE(excluded.response_headers, api_audit_log.response_headers),
                    response_body = COALESCE(excluded.response_body, api_audit_log.response_body),
                    response_body_size = COALESCE(excluded.response_body_size, api_audit_log.response_body_size),
                    completed_at = COALESCE(excluded.completed_at, api_audit_log.completed_at),
                    duration_ms = COALESCE(excluded.duration_ms, api_audit_log.duration_ms),
                    error_type = COALESCE(excluded.error_type, api_audit_log.error_type),
                    error_message = COALESCE(excluded.error_message, api_audit_log.error_message),
                    error_traceback = COALESCE(excluded.error_traceback, api_audit_log.error_traceback),
                    was_successful = excluded.was_successful,
                    was_stored = excluded.was_stored,
                    response_tokens = COALESCE(excluded.response_tokens, api_audit_log.response_tokens),
                    total_tokens = COALESCE(excluded.total_tokens, api_audit_log.total_tokens),
                    response_bytes = COALESCE(excluded.response_bytes, api_audit_log.response_bytes),
                    total_bytes = COALESCE(excluded.total_bytes, api_audit_log.total_bytes),
                    size_bucket = COALESCE(excluded.size_bucket, api_audit_log.size_bucket)
                """,
                (
                    request_id, method, path,
                    self._json_dumps(query_params),
                    self._json_dumps(safe_request_headers),
                    request_body_str, request_size,
                    client_ip, user_agent,
                    status_code,
                    self._json_dumps(safe_response_headers),
                    response_body_str, response_size,
                    upstream_request_id, context_id, run_id, taskgroup_id,
                    started_at, completed_at, duration_ms, upstream_duration_ms,
                    error_type, error_message, error_traceback,
                    operation_type, 1 if was_successful else 0, 1 if was_stored else 0,
                    request_tokens, response_tokens, total_tokens,
                    request_bytes, response_bytes, total_bytes,
                    token_model, size_bucket
                )
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save audit log for {request_id}: {e}")
            return False

    def _filter_sensitive_headers(self, headers: Optional[Dict]) -> Optional[Dict]:
        """Remove sensitive headers from a header dict."""
        if not headers:
            return None

        sensitive_keys = {
            'authorization', 'x-api-key', 'api-key', 'cookie', 'set-cookie',
            'x-auth-token', 'x-access-token', 'proxy-authorization',
            'www-authenticate', 'x-csrf-token'
        }
        return {
            k: '[REDACTED]' if k.lower() in sensitive_keys else v
            for k, v in headers.items()
        }

    def save_upstream_response(
        self,
        request_id: str,
        upstream_url: str,
        upstream_method: str,
        started_at: str,
        upstream_headers: Optional[Dict] = None,
        upstream_body: Optional[Any] = None,
        upstream_status: Optional[int] = None,
        upstream_response_headers: Optional[Dict] = None,
        upstream_response_body: Optional[Any] = None,
        completed_at: Optional[str] = None,
        duration_ms: Optional[int] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Save an upstream API request/response for debugging.

        Args:
            request_id: Parent request ID from api_audit_log
            upstream_url: URL called
            upstream_method: HTTP method used
            started_at: ISO timestamp when upstream call started
            upstream_headers: Headers sent to upstream
            upstream_body: Body sent to upstream
            upstream_status: Status code received
            upstream_response_headers: Headers received
            upstream_response_body: Body received
            completed_at: ISO timestamp when upstream call completed
            duration_ms: Duration of upstream call
            error_type: Error type if failed
            error_message: Error message if failed

        Returns:
            True if saved successfully
        """
        conn = self._get_conn()

        try:
            conn.execute(
                """
                INSERT INTO upstream_responses (
                    request_id, upstream_url, upstream_method, upstream_headers,
                    upstream_body, upstream_status, upstream_response_headers,
                    upstream_response_body, started_at, completed_at, duration_ms,
                    error_type, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id, upstream_url, upstream_method,
                    self._json_dumps(self._filter_sensitive_headers(upstream_headers)),
                    self._json_dumps(upstream_body),
                    upstream_status,
                    self._json_dumps(upstream_response_headers),
                    self._json_dumps(upstream_response_body),
                    started_at, completed_at, duration_ms,
                    error_type, error_message
                )
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save upstream response for {request_id}: {e}")
            return False

    # =========================================================================
    # Storage Failure Queue Methods
    # =========================================================================

    def record_storage_failure(
        self,
        operation_type: str,
        target_table: str,
        payload: Dict[str, Any],
        error: Exception,
        max_retries: int = 5,
    ) -> str:
        """Record a storage failure for later retry.

        When a storage operation fails, call this to queue it for retry.
        The failure will be retried with exponential backoff.

        Args:
            operation_type: Type of operation (save_task_run, save_search, etc.)
            target_table: Table that was being written to
            payload: The data that failed to save
            error: The exception that occurred
            max_retries: Maximum retry attempts

        Returns:
            The failure_id for tracking
        """
        import traceback
        import uuid

        conn = self._get_conn()
        now = datetime.utcnow()
        failure_id = f"fail_{uuid.uuid4().hex[:16]}"

        # Calculate next retry with exponential backoff (start at 1 minute)
        next_retry = now + timedelta(minutes=1)

        try:
            conn.execute(
                """
                INSERT INTO storage_failures (
                    failure_id, operation_type, target_table, payload,
                    error_type, error_message, error_traceback,
                    max_retries, next_retry_at, status,
                    created_at, modified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    failure_id, operation_type, target_table,
                    self._json_dumps(payload),
                    type(error).__name__,
                    str(error),
                    traceback.format_exc(),
                    max_retries,
                    next_retry.isoformat(),
                    now.isoformat(),
                    now.isoformat()
                )
            )
            conn.commit()
            logger.warning(
                f"Storage failure recorded: {failure_id} for {operation_type} "
                f"on {target_table} - will retry at {next_retry.isoformat()}"
            )
            return failure_id
        except Exception as e:
            # If we can't even record the failure, log it as critical
            logger.critical(
                f"CRITICAL: Failed to record storage failure AND save to {target_table}: "
                f"Original error: {error}, Recording error: {e}, "
                f"Payload: {self._json_dumps(payload)[:500]}"
            )
            return ""

    def get_pending_failures(
        self,
        limit: int = 100,
        ready_only: bool = True,
    ) -> List[Dict]:
        """Get pending storage failures that need retry.

        Args:
            limit: Maximum failures to return
            ready_only: Only return failures ready for retry (next_retry_at <= now)

        Returns:
            List of failure records
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        if ready_only:
            rows = conn.execute(
                """
                SELECT * FROM storage_failures
                WHERE status = 'pending'
                  AND next_retry_at <= ?
                  AND retry_count < max_retries
                ORDER BY next_retry_at ASC
                LIMIT ?
                """,
                (now, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM storage_failures
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,)
            ).fetchall()

        return [dict(row) for row in rows]

    def retry_storage_operation(self, failure_id: str) -> bool:
        """Attempt to retry a failed storage operation.

        Args:
            failure_id: The failure to retry

        Returns:
            True if retry succeeded, False otherwise
        """
        conn = self._get_conn()
        now = datetime.utcnow()

        # Get the failure record
        row = conn.execute(
            "SELECT * FROM storage_failures WHERE failure_id = ?",
            (failure_id,)
        ).fetchone()

        if not row:
            logger.warning(f"Failure {failure_id} not found")
            return False

        failure = dict(row)
        operation_type = failure['operation_type']
        payload = self._json_loads(failure['payload'])
        retry_count = failure['retry_count'] + 1

        try:
            # Attempt the operation based on type
            if operation_type == 'save_task_run':
                self.save_task_run(**payload)
            elif operation_type == 'save_search_request':
                self.save_search_request(**payload)
            elif operation_type == 'save_extract_request':
                self.save_extract_request(**payload)
            elif operation_type == 'save_findall_run':
                self.save_findall_run(**payload)
            elif operation_type == 'save_task_group':
                self.save_task_group(**payload)
            else:
                logger.error(f"Unknown operation type: {operation_type}")
                return False

            # Success! Mark as resolved
            conn.execute(
                """
                UPDATE storage_failures SET
                    status = 'resolved',
                    resolution_type = 'retry_success',
                    resolved_at = ?,
                    retry_count = ?,
                    last_retry_at = ?,
                    modified_at = ?
                WHERE failure_id = ?
                """,
                (now.isoformat(), retry_count, now.isoformat(), now.isoformat(), failure_id)
            )
            conn.commit()
            logger.info(f"Successfully retried {failure_id} after {retry_count} attempts")
            return True

        except Exception as e:
            # Calculate next retry with exponential backoff
            backoff_minutes = min(2 ** retry_count, 60)  # Max 1 hour
            next_retry = now + timedelta(minutes=backoff_minutes)

            if retry_count >= failure['max_retries']:
                # Max retries exceeded
                conn.execute(
                    """
                    UPDATE storage_failures SET
                        status = 'failed',
                        resolution_type = 'max_retries_exceeded',
                        resolved_at = ?,
                        retry_count = ?,
                        last_retry_at = ?,
                        error_message = ?,
                        modified_at = ?
                    WHERE failure_id = ?
                    """,
                    (now.isoformat(), retry_count, now.isoformat(), str(e), now.isoformat(), failure_id)
                )
                logger.error(f"Max retries exceeded for {failure_id}: {e}")
            else:
                # Schedule next retry
                conn.execute(
                    """
                    UPDATE storage_failures SET
                        retry_count = ?,
                        last_retry_at = ?,
                        next_retry_at = ?,
                        error_message = ?,
                        modified_at = ?
                    WHERE failure_id = ?
                    """,
                    (retry_count, now.isoformat(), next_retry.isoformat(), str(e), now.isoformat(), failure_id)
                )
                logger.warning(f"Retry {retry_count} failed for {failure_id}, next retry at {next_retry}")

            conn.commit()
            return False

    def process_failure_queue(self, batch_size: int = 10) -> Dict[str, int]:
        """Process pending storage failures.

        Call this periodically (e.g., every minute) to retry failed operations.

        Args:
            batch_size: Number of failures to process per call

        Returns:
            Dict with success_count, failure_count, skipped_count
        """
        failures = self.get_pending_failures(limit=batch_size, ready_only=True)

        results = {'success_count': 0, 'failure_count': 0, 'skipped_count': 0}

        for failure in failures:
            try:
                if self.retry_storage_operation(failure['failure_id']):
                    results['success_count'] += 1
                else:
                    results['failure_count'] += 1
            except Exception as e:
                logger.error(f"Error processing failure {failure['failure_id']}: {e}")
                results['skipped_count'] += 1

        if results['success_count'] > 0 or results['failure_count'] > 0:
            logger.info(
                f"Failure queue processed: {results['success_count']} succeeded, "
                f"{results['failure_count']} failed, {results['skipped_count']} skipped"
            )

        return results

    def get_failure_stats(self) -> Dict[str, Any]:
        """Get statistics about storage failures.

        Returns:
            Dict with counts by status, operation type, etc.
        """
        conn = self._get_conn()

        stats = {
            'pending': 0,
            'resolved': 0,
            'failed': 0,
            'by_operation': {},
            'by_table': {},
            'oldest_pending': None,
            'avg_retry_count': 0,
        }

        # Count by status
        rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM storage_failures GROUP BY status"
        ).fetchall()
        for row in rows:
            stats[row['status']] = row['cnt']

        # Count by operation
        rows = conn.execute(
            "SELECT operation_type, COUNT(*) as cnt FROM storage_failures WHERE status = 'pending' GROUP BY operation_type"
        ).fetchall()
        stats['by_operation'] = {row['operation_type']: row['cnt'] for row in rows}

        # Count by table
        rows = conn.execute(
            "SELECT target_table, COUNT(*) as cnt FROM storage_failures WHERE status = 'pending' GROUP BY target_table"
        ).fetchall()
        stats['by_table'] = {row['target_table']: row['cnt'] for row in rows}

        # Oldest pending
        row = conn.execute(
            "SELECT created_at FROM storage_failures WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if row:
            stats['oldest_pending'] = row['created_at']

        # Average retry count for pending
        row = conn.execute(
            "SELECT AVG(retry_count) as avg FROM storage_failures WHERE status = 'pending'"
        ).fetchone()
        stats['avg_retry_count'] = round(row['avg'] or 0, 2)

        return stats

    # =========================================================================
    # Safe Storage Wrappers
    # =========================================================================

    def safe_save_task_run(self, **kwargs) -> Tuple[bool, Optional[str]]:
        """Save a task run with automatic failure queueing.

        Same parameters as save_task_run, but returns a tuple of
        (success, failure_id) where failure_id is set if the save failed
        and was queued for retry.

        Returns:
            Tuple of (success: bool, failure_id: Optional[str])
        """
        try:
            self.save_task_run(**kwargs)
            return True, None
        except Exception as e:
            failure_id = self.record_storage_failure(
                operation_type='save_task_run',
                target_table='task_runs',
                payload=kwargs,
                error=e
            )
            return False, failure_id

    def safe_save_search_request(self, **kwargs) -> Tuple[bool, Optional[str]]:
        """Save a search request with automatic failure queueing."""
        try:
            self.save_search_request(**kwargs)
            return True, None
        except Exception as e:
            failure_id = self.record_storage_failure(
                operation_type='save_search_request',
                target_table='search_requests',
                payload=kwargs,
                error=e
            )
            return False, failure_id

    def safe_save_extract_request(self, **kwargs) -> Tuple[bool, Optional[str]]:
        """Save an extract request with automatic failure queueing."""
        try:
            self.save_extract_request(**kwargs)
            return True, None
        except Exception as e:
            failure_id = self.record_storage_failure(
                operation_type='save_extract_request',
                target_table='extract_requests',
                payload=kwargs,
                error=e
            )
            return False, failure_id

    def safe_save_task_group(self, **kwargs) -> Tuple[bool, Optional[str]]:
        """Save a task group with automatic failure queueing."""
        try:
            self.save_task_group(**kwargs)
            return True, None
        except Exception as e:
            failure_id = self.record_storage_failure(
                operation_type='save_task_group',
                target_table='task_groups',
                payload=kwargs,
                error=e
            )
            return False, failure_id

    # =========================================================================
    # Event Persistence
    # =========================================================================

    def save_event(
        self,
        event_id: str,
        event_type: str,
        resource_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        data: Optional[Dict] = None,
    ) -> bool:
        """Save an event to persistent storage.

        Events are kept for 30 days for auditing and debugging.

        Args:
            event_id: Unique event identifier
            event_type: Type of event (task.created, search.completed, etc.)
            resource_id: Related resource ID (run_id, search_id, etc.)
            resource_type: Type of resource (task_run, search, etc.)
            data: Event payload data

        Returns:
            True if saved successfully
        """
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()

        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO events (
                    event_id, event_type, resource_id, resource_type, data, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, event_type, resource_id, resource_type, self._json_dumps(data), now)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save event {event_id}: {e}")
            return False

    def get_events(
        self,
        event_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Get events with optional filters.

        Args:
            event_type: Filter by event type
            resource_id: Filter by resource ID
            since: Only events after this ISO timestamp
            limit: Maximum events to return

        Returns:
            List of event records
        """
        conn = self._get_conn()
        conditions = []
        params = []

        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type)
        if resource_id:
            conditions.append("resource_id = ?")
            params.append(resource_id)
        if since:
            conditions.append("created_at >= ?")
            params.append(since)

        where = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT * FROM events
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params
        ).fetchall()

        results = []
        for row in rows:
            record = dict(row)
            record['data'] = self._json_loads(record.get('data'))
            results.append(record)
        return results

    def cleanup_old_events(self, days: int = 30) -> int:
        """Delete events older than specified days.

        Args:
            days: Age threshold in days

        Returns:
            Number of events deleted
        """
        conn = self._get_conn()
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        cursor = conn.execute(
            "DELETE FROM events WHERE created_at < ?",
            (cutoff,)
        )
        conn.commit()

        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} events older than {days} days")
        return deleted

    def cleanup_old_audit_logs(self, days: int = 90) -> int:
        """Delete audit logs older than specified days.

        Args:
            days: Age threshold in days

        Returns:
            Number of logs deleted
        """
        conn = self._get_conn()
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        # Delete upstream responses first (foreign key)
        conn.execute(
            """
            DELETE FROM upstream_responses
            WHERE request_id IN (
                SELECT request_id FROM api_audit_log WHERE started_at < ?
            )
            """,
            (cutoff,)
        )

        cursor = conn.execute(
            "DELETE FROM api_audit_log WHERE started_at < ?",
            (cutoff,)
        )
        conn.commit()

        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} audit logs older than {days} days")
        return deleted


# =============================================================================
# Global Instance
# =============================================================================

_store: Optional[LocalStore] = None


def get_local_store() -> LocalStore:
    """Get or create the global LocalStore instance.

    Returns:
        The global LocalStore instance
    """
    global _store
    if _store is None:
        _store = LocalStore()
        _store.connect()
    return _store


def reset_local_store() -> None:
    """Reset the global LocalStore instance (useful for testing)."""
    global _store
    if _store is not None:
        _store.close()
        _store = None
