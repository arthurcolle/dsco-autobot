"""
Persistent Tool Store - DuckDB-backed durable tool registration.

Tools are persisted with their backend mappings, schemas, and metadata.
No more in-memory-only registration that disappears on restart.
"""

import os
import json
import shutil
import time
import random
import threading
import uuid
import duckdb
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)

# Use /data in Modal (ephemeral but works), otherwise local data dir
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Could not create DATA_DIR {DATA_DIR}: {e}")
    # Fallback to /tmp if data dir fails
    DATA_DIR = Path("/tmp/tool_management_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "tools.duckdb"
logger.info(f"ToolStore will use database at {DB_PATH}")


@dataclass
class RegisteredTool:
    """A persistently registered tool."""
    tool_id: str
    name: str
    backend_id: str
    backend_url: str
    description: str = ""
    input_schema: Optional[Dict] = None
    output_schema: Optional[Dict] = None
    tags: Optional[List[str]] = None
    category: str = "general"
    version: str = "1.0.0"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    call_count: int = 0
    error_count: int = 0
    avg_latency_ms: float = 0.0

    def __post_init__(self):
        if self.input_schema is None:
            self.input_schema = {}
        if self.output_schema is None:
            self.output_schema = {}
        if self.tags is None:
            self.tags = []
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
        if self.updated_at is None:
            self.updated_at = self.created_at


_REG_FIELDS = set(RegisteredTool.__dataclass_fields__.keys())
def _reg_from_dict(data):
    """Build a RegisteredTool from a row dict, ignoring extra columns (e.g. the
    Postgres `fts` tsvector) that are not dataclass fields."""
    return RegisteredTool(**{k: v for k, v in data.items() if k in _REG_FIELDS})


def _json_safe_value(value):
    """DuckDB returns TIMESTAMP columns as datetime objects; JSON callers need ISO strings."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def category_candidates(category: Optional[str]) -> List[str]:
    """
    Expand a category filter into the naming variants used across backends.

    Backends register categories inconsistently ("unit", "unit-fabric",
    "distributed-unit", "distributed-unit-mcp"). Callers pass any of these;
    match all of them so filtering behaves the same everywhere.
    """
    c = (category or "").strip().lower()
    if not c:
        return []
    stem = c
    if stem.startswith("distributed-"):
        stem = stem[len("distributed-"):]
    for suffix in ("-mcp", "-fabric"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    stem = stem.strip("-") or c
    return sorted({
        c,
        stem,
        f"{stem}-mcp",
        f"{stem}-fabric",
        f"distributed-{stem}",
        f"distributed-{stem}-mcp",
    })


def _category_where(category: str, params: List[Any]) -> str:
    """Build a WHERE fragment matching category variants against category or backend_id.

    Also matches the integration-fabric naming convention so a category filter
    works even for tools registered before per-tool taxonomy existed:
      - primitives  ``{provider}_{category}_{op}``  -> name LIKE '%_weather_%'
      - canonical   ``{category}__{op}``            -> name LIKE 'weather__%'
      - description ``... in {category}`` / ``[canonical:{category}]``
      - tags array contains the bare category stem
    """
    cands = category_candidates(category)
    placeholders = ", ".join("?" for _ in cands)
    params.extend(cands)  # lower(category) IN (...)
    params.extend(cands)  # lower(backend_id) IN (...)

    stem = (category or "").strip().lower()
    if stem.startswith("distributed-"):
        stem = stem[len("distributed-"):]
    for suffix in ("-mcp", "-fabric"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    stem = stem.strip("-")

    extra = ""
    if stem:
        # name-pattern + description + tags fallbacks (repairs legacy rows)
        params.extend([
            f"%\\_{stem}\\_%",     # provider_{cat}_op
            f"{stem}\\_\\_%",       # {cat}__op canonical
            f"% in {stem}",         # "...(kind) in {cat}" descriptions
            f"[canonical:{stem}]%", # canonical description prefix
            f'%"{stem}"%',          # tags JSON contains the stem
        ])
        extra = (
            " OR lower(name) LIKE ? ESCAPE '\\'"
            " OR lower(name) LIKE ? ESCAPE '\\'"
            " OR lower(description) LIKE ?"
            " OR lower(description) LIKE ?"
            " OR lower(tags::VARCHAR) LIKE ?"
        )

    return (
        f"(lower(category) IN ({placeholders}) "
        f"OR lower(backend_id) IN ({placeholders}){extra})"
    )


class ToolStore:
    """
    Durable tool storage with DuckDB.

    Provides:
    - Persistent tool registration
    - Backend-to-tool mappings
    - Tool lookup by name, backend, or category
    - Usage statistics tracking
    """

    _instance = None
    _conn = None
    _lock = threading.RLock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path = str(DB_PATH)
        self._init_schema()
        self._initialized = True
        logger.info(f"ToolStore initialized at {self.db_path}")

    @staticmethod
    def _pg_attach_url() -> Optional[str]:
        """Neon Postgres URL for the shared catalog, or None to use local DuckDB.
        Strips channel_binding, which DuckDB's postgres attach rejects."""
        url = os.getenv("POSTGRES_URL", "").strip()
        if not url.startswith("postgresql://") or url.startswith("postgresql://redis"):
            return None
        for frag in ("&channel_binding=require", "channel_binding=require&", "?channel_binding=require"):
            url = url.replace(frag, "?" if frag.startswith("?") else "")
        return url

    @contextmanager
    def _get_conn(self, read_only: bool = False):
        # ONE long-lived connection per process, serialized by a lock.
        #
        # When POSTGRES_URL is set the catalog lives in Neon Postgres: each
        # container runs its OWN in-memory DuckDB attached to the shared Neon
        # database (registry schema), so there is no single-writer file lock and
        # the registry scales horizontally across containers. Without it we fall
        # back to a local DuckDB file (single-container dev).
        with ToolStore._lock:
            if ToolStore._conn is None:
                pg = self._pg_attach_url()
                if pg:
                    try:
                        c = duckdb.connect(":memory:")
                        c.execute("INSTALL postgres; LOAD postgres;")
                        c.execute(f"ATTACH '{pg}' AS pg (TYPE postgres)")
                        c.execute("CREATE SCHEMA IF NOT EXISTS pg.registry")
                        c.execute("USE pg.registry")
                        ToolStore._conn = c
                        logger.info("ToolStore: attached to Neon Postgres (registry schema) — horizontally scalable")
                    except Exception as exc:  # noqa: BLE001
                        logger.error("ToolStore: Neon attach failed (%s); falling back to local DuckDB", exc)
                        ToolStore._conn = duckdb.connect(self.db_path)
                else:
                    ToolStore._conn = duckdb.connect(self.db_path)
            yield ToolStore._conn

    def _close_conn(self):
        """Close the shared connection (e.g. before deleting DB files)."""
        with ToolStore._lock:
            if ToolStore._conn is not None:
                try:
                    ToolStore._conn.close()
                except Exception:
                    pass
                ToolStore._conn = None

    def _init_schema(self):
        """Initialize database schema, recovering from WAL corruption if needed."""
        try:
            self._create_tables()
        except Exception as e:
            if "WAL" in str(e) or "Conflict" in str(e):
                logger.warning(f"DuckDB WAL corrupted, deleting WAL and retrying: {e}")
                self._close_conn()
                import pathlib
                wal_path = pathlib.Path(self.db_path + ".wal")
                if wal_path.exists():
                    backup = f"{self.db_path}.wal.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    shutil.copy2(str(wal_path), backup)
                    logger.warning(f"WAL backed up to {backup}")
                    wal_path.unlink()
                try:
                    self._create_tables()
                except Exception:
                    logger.warning("DB still broken after WAL removal, recreating from scratch")
                    self._close_conn()
                    db = pathlib.Path(self.db_path)
                    if db.exists():
                        db_backup = f"{self.db_path}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        shutil.copy2(str(db), db_backup)
                        logger.warning(f"DB backed up to {db_backup}")
                        db.unlink()
                    self._create_tables()
            else:
                raise

    def _create_tables(self):
        # On Postgres (Neon) the registry.* tables are created natively out of
        # band — DuckDB's postgres attach can't forward CREATE SEQUENCE and some
        # DDL — so skip the DuckDB DDL entirely and use the pre-created schema.
        if self._pg_attach_url():
            logger.info("ToolStore: using pre-created Neon registry schema (DuckDB DDL skipped)")
            return
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tools (
                    tool_id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    backend_id VARCHAR NOT NULL,
                    backend_url VARCHAR NOT NULL,
                    description VARCHAR DEFAULT '',
                    input_schema JSON,
                    output_schema JSON,
                    tags JSON DEFAULT '[]',
                    category VARCHAR DEFAULT 'general',
                    version VARCHAR DEFAULT '1.0.0',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    call_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    avg_latency_ms DOUBLE DEFAULT 0.0
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS backends (
                    backend_id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    url VARCHAR NOT NULL,
                    health_endpoint VARCHAR DEFAULT '/health',
                    mcp_endpoint VARCHAR DEFAULT '/mcp',
                    capabilities JSON DEFAULT '[]',
                    is_healthy BOOLEAN DEFAULT true,
                    last_health_check TIMESTAMP,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tool_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS integration_feedback (
                    feedback_id VARCHAR PRIMARY KEY,
                    backend_id VARCHAR NOT NULL,
                    tool_name VARCHAR,
                    outcome VARCHAR,
                    category VARCHAR DEFAULT 'other',
                    rating INTEGER,
                    message VARCHAR NOT NULL,
                    suggested_fix VARCHAR,
                    context JSON,
                    session_id VARCHAR,
                    trace_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_backend ON integration_feedback(backend_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_tool ON integration_feedback(tool_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_created ON integration_feedback(created_at)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    idempotency_key VARCHAR NOT NULL,
                    tool_id VARCHAR NOT NULL,
                    request_hash VARCHAR NOT NULL,
                    response_json JSON,
                    success BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    PRIMARY KEY (idempotency_key, tool_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tool_call_history (
                    id BIGINT PRIMARY KEY,
                    tool_id VARCHAR NOT NULL,
                    backend_id VARCHAR NOT NULL,
                    success BOOLEAN NOT NULL,
                    latency_ms DOUBLE DEFAULT 0.0,
                    execution_mode VARCHAR DEFAULT 'sync',
                    trace_id VARCHAR,
                    session_id VARCHAR,
                    error_code VARCHAR,
                    error_message VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS tool_call_history_id_seq START 1
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tool_dependencies (
                    source_tool_id VARCHAR NOT NULL,
                    target_tool_id VARCHAR NOT NULL,
                    dependency_type VARCHAR DEFAULT 'explicit',
                    confidence DOUBLE DEFAULT 1.0,
                    call_count INTEGER DEFAULT 0,
                    metadata JSON DEFAULT '{}',
                    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_tool_id, target_tool_id, dependency_type)
                )
            """)

            # Indexes for fast lookup
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_name ON tools(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_backend ON tools(backend_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_category ON tools(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_records(expires_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_call_history_tool ON tool_call_history(tool_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_call_history_created ON tool_call_history(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_call_history_success ON tool_call_history(success)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_dependencies_source ON tool_dependencies(source_tool_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_dependencies_target ON tool_dependencies(target_tool_id)")

    # ==================== Backend Operations ====================

    def register_backend(
        self,
        backend_id: str,
        name: str,
        url: str,
        health_endpoint: str = "/health",
        mcp_endpoint: str = "/mcp",
        capabilities: Optional[List[str]] = None
    ) -> Dict:
        """Register or update a backend service."""
        capabilities = capabilities if capabilities is not None else []

        try:
            now = datetime.utcnow().isoformat()
            with self._get_conn() as conn:
                # delete-then-insert upsert: the DuckDB->Postgres attach turns a
                # parametrized UPDATE into UPDATE...FROM a varchar temp table,
                # which fails against the json `capabilities` column. INSERT binds
                # json.dumps(...) cleanly. The tool_count contention that made
                # this path conflict was removed from register_tool, so a plain
                # delete+insert is both correct and cheap here.
                conn.execute("DELETE FROM backends WHERE backend_id = ?", [backend_id])
                conn.execute("""
                    INSERT INTO backends (backend_id, name, url, health_endpoint, mcp_endpoint, capabilities, last_heartbeat)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [backend_id, name, url, health_endpoint, mcp_endpoint, json.dumps(capabilities), now])
            logger.info(f"Registered backend: {backend_id} at {url}")
            return {"backend_id": backend_id, "url": url, "status": "registered"}
        except Exception as e:
            logger.error(f"Failed to register backend {backend_id}: {e}")
            raise

    def get_backend(self, backend_id: str) -> Optional[Dict]:
        """Get backend by ID."""
        with self._get_conn(read_only=True) as conn:
            result = conn.execute(
                "SELECT * FROM backends WHERE backend_id = ?", [backend_id]
            ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))
        return None

    def list_backends(self) -> List[Dict]:
        """List all registered backends."""
        # tool_count is derived here via JOIN rather than maintained as a
        # column: nothing else writes backends.tool_count, so a stored value
        # would silently rot to 0 (observed: list_tool_categories reporting
        # total_tools=0 against a fully-populated store).
        with self._get_conn(read_only=True) as conn:
            results = conn.execute("""
                SELECT b.*, COALESCE(t.cnt, 0) AS tool_count
                FROM backends b
                LEFT JOIN (
                    SELECT backend_id, COUNT(*) AS cnt
                    FROM tools
                    GROUP BY backend_id
                ) t ON t.backend_id = b.backend_id
                ORDER BY b.name
            """).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in results]

    def update_backend_health(self, backend_id: str, is_healthy: bool):
        """Update backend health status."""
        now = datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE backends
                SET is_healthy = ?, last_health_check = ?
                WHERE backend_id = ?
            """, [is_healthy, now, backend_id])

    # ==================== Tool Operations ====================

    def register_tool(self, tool: RegisteredTool) -> Dict:
        """Register or update a tool."""
        now = datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            # DuckDB forbids ON CONFLICT DO UPDATE on indexed columns
            # (name, backend_id, category all carry indexes), so emulate the
            # upsert with delete-then-insert inside a transaction.
            existing_created = conn.execute(
                "SELECT created_at FROM tools WHERE tool_id = ?", [tool.tool_id]
            ).fetchone()
            created_at = (existing_created[0] if existing_created else None) or tool.created_at or now

            conn.execute("DELETE FROM tools WHERE tool_id = ?", [tool.tool_id])
            conn.execute("""
                INSERT INTO tools (
                    tool_id, name, backend_id, backend_url, description,
                    input_schema, output_schema, tags, category, version,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                tool.tool_id, tool.name, tool.backend_id, tool.backend_url,
                tool.description, json.dumps(tool.input_schema), json.dumps(tool.output_schema),
                json.dumps(tool.tags), tool.category, tool.version,
                created_at, now
            ])

            # Tool count is derived for status/reporting. Avoid updating the
            # shared backends row for every announced tool: concurrent backend
            # announcements otherwise serialize/conflict on Neon and stall MCP.

        return {"tool_id": tool.tool_id, "status": "registered"}

    def register_tools_batch(self, tools: List[RegisteredTool]) -> Dict:
        """Register multiple tools in a batch."""
        registered = 0
        for tool in tools:
            self.register_tool(tool)
            registered += 1

        logger.info(f"Batch registered {registered} tools")
        return {"registered": registered}

    def get_tool(self, tool_id: str) -> Optional[RegisteredTool]:
        """Get tool by ID."""
        with self._get_conn(read_only=True) as conn:
            result = conn.execute(
                "SELECT * FROM tools WHERE tool_id = ?", [tool_id]
            ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                data = dict(zip(columns, result))
                data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                data['tags'] = json.loads(data['tags']) if data['tags'] else []
                return _reg_from_dict(data)
        return None

    def get_tool_by_name(self, name: str, backend_id: Optional[str] = None) -> Optional[RegisteredTool]:
        """Get tool by name, optionally filtered by backend."""
        with self._get_conn(read_only=True) as conn:
            if backend_id is None and ":" in name:
                # Accept fully-qualified names (backend:tool) as first-class lookup keys.
                exact = conn.execute(
                    "SELECT * FROM tools WHERE tool_id = ? ORDER BY updated_at DESC LIMIT 1", [name]
                ).fetchone()
                if exact:
                    columns = [desc[0] for desc in conn.description]
                    data = dict(zip(columns, exact))
                    data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                    data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                    data['tags'] = json.loads(data['tags']) if data['tags'] else []
                    return _reg_from_dict(data)

                backend_id, name = name.split(":", 1)

            if backend_id:
                result = conn.execute(
                    "SELECT * FROM tools WHERE name = ? AND backend_id = ?", [name, backend_id]
                ).fetchone()
            else:
                result = conn.execute(
                    "SELECT * FROM tools WHERE name = ? ORDER BY updated_at DESC LIMIT 1", [name]
                ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                data = dict(zip(columns, result))
                data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                data['tags'] = json.loads(data['tags']) if data['tags'] else []
                return _reg_from_dict(data)
        return None

    def list_tools(self, backend_id: Optional[str] = None, category: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[RegisteredTool]:
        """List tools with optional filters and pagination."""
        query = "SELECT * FROM tools WHERE 1=1"
        params = []

        if backend_id:
            query += " AND backend_id = ?"
            params.append(backend_id)
        if category:
            query += " AND " + _category_where(category, params)

        query += f" ORDER BY name LIMIT {limit} OFFSET {offset}"

        with self._get_conn(read_only=True) as conn:
            results = conn.execute(query, params).fetchall()
            columns = [desc[0] for desc in conn.description]

            tools = []
            for row in results:
                data = dict(zip(columns, row))
                data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                data['tags'] = json.loads(data['tags']) if data['tags'] else []
                tools.append(_reg_from_dict(data))

            return tools

    def count_tools(self, backend_id: Optional[str] = None, category: Optional[str] = None) -> int:
        """Count tools matching filters."""
        query = "SELECT COUNT(*) FROM tools WHERE 1=1"
        params = []

        if backend_id:
            query += " AND backend_id = ?"
            params.append(backend_id)
        if category:
            query += " AND " + _category_where(category, params)

        with self._get_conn(read_only=True) as conn:
            return conn.execute(query, params).fetchone()[0]

    def list_tools_by_backend(self, backend_id: str) -> List[RegisteredTool]:
        """List all tools for a specific backend."""
        return self.list_tools(backend_id=backend_id, limit=1000)

    def prune_backend_tools(self, backend_id: str, keep_tool_ids: List[str]) -> int:
        """Delete this backend's tools that are NOT in keep_tool_ids.

        Called on announce so renamed/removed tools don't linger in the
        catalog and shadow other backends' names. Returns rows removed.
        """
        keep = set(keep_tool_ids)
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT tool_id FROM tools WHERE backend_id = ?", [backend_id]
            ).fetchall()
            stale = [r[0] for r in rows if r[0] not in keep]
            for tool_id in stale:
                conn.execute("DELETE FROM tools WHERE tool_id = ?", [tool_id])
            # tool_count is derived lazily by status queries; do not contend on
            # the shared backend row during announcement pruning.
        return len(stale)

    def find_backend_for_tool(self, tool_name: str) -> Optional[str]:
        """Find which backend provides a tool by name."""
        tool = self.get_tool_by_name(tool_name)
        return tool.backend_id if tool else None

    def resolve_tool(self, tool_ref: str) -> tuple[str, str, str]:
        """
        Resolve a tool reference to (backend_id, backend_url, tool_name).

        Accepts:
        - "backend:tool" format
        - Plain tool name (looks up backend)

        Returns (backend_id, backend_url, tool_name) or raises ValueError.
        """
        if ":" in tool_ref:
            backend_id, tool_name = tool_ref.split(":", 1)
            backend = self.get_backend(backend_id)
            if not backend:
                raise ValueError(f"Backend not found: {backend_id}")
            return backend_id, backend['url'], tool_name
        else:
            tool = self.get_tool_by_name(tool_ref)
            if not tool:
                raise ValueError(f"Tool not found: {tool_ref}")
            return tool.backend_id, tool.backend_url, tool.name

    # ==================== Advanced Queries ====================

    def catalog_version(self) -> str:
        """
        Cheap catalog fingerprint for cache keys / ETags: COUNT(*) plus the most
        recent updated_at. Changes whenever tools are added, removed, or updated,
        so callers can cache derived views (e.g. the MCP tools/list pages) and
        invalidate atomically when the catalog moves. Never raises — a failure
        degrades to a unique-ish token so callers simply skip the cache.
        """
        try:
            with self._get_conn(read_only=True) as conn:
                row = conn.execute(
                    "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM tools"
                ).fetchone()
            count = row[0] if row else 0
            newest = row[1] if row and len(row) > 1 else ""
            return f"{count}:{newest}"
        except Exception as exc:  # noqa: BLE001
            logger.debug("catalog_version failed (%s); using volatile token", exc)
            import time as _t
            return f"volatile:{_t.time()}"

    def query_tools(
        self,
        search: Optional[str] = None,
        backend_id: Optional[str] = None,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        sort_by: str = "name",
        sort_order: str = "asc",
        limit: int = 100,
        offset: int = 0,
        columns: Optional[List[str]] = None,
        match_any: bool = False,
        unique_names: bool = False,
        include_total: bool = True,
    ) -> tuple[List[Dict], int]:
        """
        Advanced tool query with text search, sorting, tag filtering, and column projection.

        Args:
            search: Text search across name and description (ILIKE)
            match_any: For multi-word searches, match tools containing ANY word
                (default requires ALL words)
            backend_id: Filter by backend
            category: Filter by category
            tags: Filter tools that have ANY of these tags
            sort_by: Column to sort by (name, call_count, updated_at, avg_latency_ms, error_count)
            sort_order: asc or desc
            limit: Page size
            offset: Page offset
            columns: Which columns to return (None = all). Use for compact responses.
            unique_names: Return at most one row per public tool name, selecting
                the most recently updated row. Useful for MCP's name-keyed surface.
            include_total: Whether to run the matching COUNT query. Set false for
                callers that only need a bounded result page.

        Returns:
            (rows as dicts, total count matching filters)
        """
        allowed_sort = {"name", "call_count", "updated_at", "avg_latency_ms", "error_count", "created_at", "category"}
        if sort_by not in allowed_sort:
            sort_by = "name"
        if sort_order not in ("asc", "desc"):
            sort_order = "asc"

        where_clauses = ["1=1"]
        params = []

        if search:
            # Split multi-word queries so "memory belief" matches tools with both words
            words = search.strip().split()
            if len(words) == 1:
                where_clauses.append("(name ILIKE ? OR description ILIKE ?)")
                params.extend([f"%{words[0]}%", f"%{words[0]}%"])
            else:
                word_clauses = []
                for word in words:
                    word_clauses.append("(name ILIKE ? OR description ILIKE ?)")
                    params.extend([f"%{word}%", f"%{word}%"])
                joiner = " OR " if match_any else " AND "
                where_clauses.append(f"({joiner.join(word_clauses)})")
        if backend_id:
            where_clauses.append("backend_id = ?")
            params.append(backend_id)
        if category:
            where_clauses.append(_category_where(category, params))
        if tags:
            # DuckDB JSON: check if tags array contains any of the given tags
            tag_conditions = []
            for tag in tags:
                tag_conditions.append("list_contains(tags::VARCHAR[], ?)")
                params.append(tag)
            where_clauses.append(f"({' OR '.join(tag_conditions)})")

        where_sql = " AND ".join(where_clauses)

        with self._get_conn(read_only=True) as conn:
            # Count. MCP exposes tools by public name, so its catalog query can
            # request unique_names and count the same logical rows it pages. A
            # caller that only needs a bounded page can skip this full scan.
            if include_total:
                if unique_names:
                    count_sql = f"""
                        SELECT COUNT(*) FROM (
                            SELECT name, ROW_NUMBER() OVER (
                                PARTITION BY name ORDER BY updated_at DESC, tool_id DESC
                            ) AS rn
                            FROM tools WHERE {where_sql}
                        ) dedup WHERE rn = 1
                    """
                else:
                    count_sql = f"SELECT COUNT(*) FROM tools WHERE {where_sql}"
                count_result = conn.execute(count_sql, params).fetchone()
                total = count_result[0] if count_result else 0
            else:
                total = -1

            # Select
            if columns:
                safe_cols = [c for c in columns if c in {
                    "tool_id", "name", "backend_id", "backend_url", "description",
                    "input_schema", "output_schema", "tags", "category", "version",
                    "created_at", "updated_at", "call_count", "error_count", "avg_latency_ms"
                }]
                if not safe_cols:
                    safe_cols = ["tool_id", "name", "description", "backend_id"]
                select_sql = ", ".join(safe_cols)
            else:
                select_sql = "*"
                safe_cols = None

            if unique_names:
                # Pick the newest row for each public name before applying the
                # stable name ordering and page window. This makes OFFSET cursors
                # operate on logical MCP tools, not duplicate backend rows.
                query = f"""
                    SELECT {select_sql} FROM (
                        SELECT {select_sql}, ROW_NUMBER() OVER (
                            PARTITION BY name ORDER BY updated_at DESC, tool_id DESC
                        ) AS _dsco_name_rank
                        FROM tools WHERE {where_sql}
                    ) dedup
                    WHERE _dsco_name_rank = 1
                    ORDER BY {sort_by} {sort_order}
                    LIMIT {limit} OFFSET {offset}
                """
            else:
                query = f"SELECT {select_sql} FROM tools WHERE {where_sql} ORDER BY {sort_by} {sort_order} LIMIT {limit} OFFSET {offset}"
            results = conn.execute(query, params).fetchall()
            col_names = [desc[0] for desc in conn.description]

            rows = []
            for row in results:
                d = {k: _json_safe_value(v) for k, v in zip(col_names, row)}
                # Deserialize JSON columns if present
                for jcol in ("input_schema", "output_schema", "tags"):
                    if jcol in d and d[jcol] is not None:
                        if isinstance(d[jcol], str):
                            d[jcol] = json.loads(d[jcol])
                rows.append(d)

            return rows, total

    def list_unique_tools_page(
        self,
        limit: int = 200,
        offset: int = 0,
        columns: Optional[List[str]] = None,
    ) -> tuple[List[Dict], bool]:
        """Return a bounded, newest-row-per-name catalog page.

        Unlike ``query_tools(unique_names=True)``, this avoids both COUNT(*) and
        the windowed full-table query on every request. It is intentionally
        specialized for MCP's immutable-ish, name-keyed discovery surface and
        returns ``has_more`` by fetching one sentinel row beyond the page.
        """
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))
        safe = [c for c in (columns or []) if c in {
            "tool_id", "name", "backend_id", "backend_url", "description",
            "input_schema", "output_schema", "tags", "category", "version",
            "created_at", "updated_at", "call_count", "error_count", "avg_latency_ms"
        }]
        if not safe:
            safe = ["tool_id", "name", "description", "backend_id"]
        select_sql = ", ".join(safe)
        with self._get_conn(read_only=True) as conn:
            query = f"""
                SELECT {select_sql} FROM (
                    SELECT {select_sql}, ROW_NUMBER() OVER (
                        PARTITION BY name ORDER BY updated_at DESC, tool_id DESC
                    ) AS _dsco_name_rank
                    FROM tools
                ) dedup
                WHERE _dsco_name_rank = 1
                ORDER BY name ASC
                LIMIT {limit + 1} OFFSET {offset}
            """
            results = conn.execute(query).fetchall()
            names = [d[0] for d in conn.description]
        has_more = len(results) > limit
        rows = []
        for row in results[:limit]:
            d = {k: _json_safe_value(v) for k, v in zip(names, row)}
            for jcol in ("input_schema", "output_schema", "tags"):
                if jcol in d and isinstance(d[jcol], str):
                    d[jcol] = json.loads(d[jcol])
            rows.append(d)
        return rows, has_more

    def list_unique_tools_snapshot(
        self,
        columns: Optional[List[str]] = None,
    ) -> List[Dict]:
        """Load the newest row for every public tool name in one read."""
        safe = [c for c in (columns or []) if c in {
            "tool_id", "name", "backend_id", "backend_url", "description",
            "input_schema", "output_schema", "tags", "category", "version",
            "created_at", "updated_at", "call_count", "error_count", "avg_latency_ms"
        }]
        if not safe:
            safe = ["tool_id", "name", "description", "backend_id"]
        select_sql = ", ".join(safe)
        with self._get_conn(read_only=True) as conn:
            query = f"""
                SELECT {select_sql} FROM (
                    SELECT {select_sql}, ROW_NUMBER() OVER (
                        PARTITION BY name ORDER BY updated_at DESC, tool_id DESC
                    ) AS _dsco_name_rank
                    FROM tools
                ) dedup
                WHERE _dsco_name_rank = 1
                ORDER BY name ASC
            """
            results = conn.execute(query).fetchall()
            names = [d[0] for d in conn.description]
        rows = []
        for row in results:
            d = {k: _json_safe_value(v) for k, v in zip(names, row)}
            for jcol in ("input_schema", "output_schema", "tags"):
                if jcol in d and isinstance(d[jcol], str):
                    d[jcol] = json.loads(d[jcol])
            rows.append(d)
        return rows

    def get_tools_batch(self, names: List[str]) -> List[Dict]:
        """
        Fetch multiple tools by name in one query.
        Returns compact tool dicts with full schemas.
        """
        if not names:
            return []
        qualified = [n for n in names if ":" in n]
        plain = [n for n in names if ":" not in n]
        where_parts = []
        params: List[str] = []
        if plain:
            placeholders = ", ".join("?" for _ in plain)
            where_parts.append(f"name IN ({placeholders})")
            params.extend(plain)
        if qualified:
            placeholders = ", ".join("?" for _ in qualified)
            where_parts.append(f"tool_id IN ({placeholders})")
            params.extend(qualified)
        if not where_parts:
            return []
        where_sql = " OR ".join(where_parts)
        with self._get_conn(read_only=True) as conn:
            results = conn.execute(
                f"SELECT * FROM tools WHERE {where_sql} ORDER BY name",
                params
            ).fetchall()
            col_names = [desc[0] for desc in conn.description]
            rows = []
            for row in results:
                d = dict(zip(col_names, row))
                for jcol in ("input_schema", "output_schema", "tags"):
                    if jcol in d and d[jcol] is not None:
                        if isinstance(d[jcol], str):
                            d[jcol] = json.loads(d[jcol])
                rows.append(d)
            return rows

    # ==================== Idempotency ====================

    def get_idempotent_response(
        self,
        idempotency_key: str,
        tool_id: str,
        request_hash: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a previously stored idempotent response.

        If a record exists and request_hash mismatches, raises ValueError.
        """
        with self._get_conn(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT request_hash, response_json, success, expires_at
                FROM idempotency_records
                WHERE idempotency_key = ? AND tool_id = ?
                """,
                [idempotency_key, tool_id],
            ).fetchone()
            if not row:
                return None

            stored_hash, response_json, success, expires_at = row
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            if expires_at and datetime.utcnow() > expires_at:
                return None
            if request_hash and stored_hash != request_hash:
                raise ValueError("Idempotency key reused with different arguments")

            response = json.loads(response_json) if isinstance(response_json, str) else response_json
            return {
                "success": bool(success),
                "response": response,
            }

    def save_idempotent_response(
        self,
        idempotency_key: str,
        tool_id: str,
        request_hash: str,
        response: Dict[str, Any],
        success: bool = True,
        ttl_seconds: int = 86_400,
    ) -> bool:
        """
        Persist an idempotent response.

        Returns True when inserted, False when an equivalent record already exists.
        """
        now = datetime.utcnow()
        expires_at = datetime.utcfromtimestamp(now.timestamp() + max(0, ttl_seconds))
        response_json = json.dumps(response)

        with self._get_conn() as conn:
            existing = conn.execute(
                """
                SELECT request_hash
                FROM idempotency_records
                WHERE idempotency_key = ? AND tool_id = ?
                """,
                [idempotency_key, tool_id],
            ).fetchone()
            if existing:
                if existing[0] != request_hash:
                    raise ValueError("Idempotency key reused with different arguments")
                return False

            conn.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key, tool_id, request_hash, response_json, success, created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    idempotency_key,
                    tool_id,
                    request_hash,
                    response_json,
                    success,
                    now,
                    expires_at,
                ],
            )
        return True

    def cleanup_idempotency_records(self) -> int:
        """Delete expired idempotency records and return removed count."""
        now = datetime.utcnow()
        with self._get_conn() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM idempotency_records WHERE expires_at IS NOT NULL AND expires_at < ?",
                [now],
            ).fetchone()[0]
            conn.execute(
                "DELETE FROM idempotency_records WHERE expires_at IS NOT NULL AND expires_at < ?",
                [now],
            )
            return int(count)

    def get_distinct_categories(self) -> List[Dict]:
        """Get distinct categories with tool counts."""
        with self._get_conn(read_only=True) as conn:
            results = conn.execute("""
                SELECT category, COUNT(*) as tool_count,
                       SUM(call_count) as total_calls
                FROM tools
                GROUP BY category
                ORDER BY tool_count DESC
            """).fetchall()
            return [
                {"category": r[0], "tool_count": r[1], "total_calls": r[2] or 0}
                for r in results
            ]

    def get_popular_tools(self, limit: int = 10) -> List[Dict]:
        """Get most-used tools by call count."""
        with self._get_conn(read_only=True) as conn:
            results = conn.execute("""
                SELECT tool_id, name, backend_id, description,
                       call_count, avg_latency_ms, error_count
                FROM tools
                WHERE call_count > 0
                ORDER BY call_count DESC
                LIMIT ?
            """, [limit]).fetchall()
            col_names = [desc[0] for desc in conn.description]
            return [dict(zip(col_names, row)) for row in results]

    def summarize_tool_schema(self, tool_id: str) -> Optional[Dict]:
        """
        Return a compact schema summary: parameter names, types, and required flags.
        Much smaller than the full JSON schema — good for context-limited agents.
        """
        tool = self.get_tool(tool_id)
        if not tool:
            return None

        input_schema = tool.input_schema or {}
        props = input_schema.get("properties", {})
        required = set(input_schema.get("required", []))

        params = []
        for pname, pschema in props.items():
            params.append({
                "name": pname,
                "type": pschema.get("type", "any"),
                "required": pname in required,
                "description": pschema.get("description", "")[:80],
            })

        return {
            "tool_id": tool.tool_id,
            "name": tool.name,
            "description": tool.description[:200],
            "backend": tool.backend_id,
            "parameters": params,
            "signature": _build_signature(tool.name, props, required),
        }

    # ==================== Rich Analytics & Dependencies ====================

    def resolve_tool_id(self, tool_ref: str) -> Optional[str]:
        """Resolve a tool reference (name or backend:name) to canonical tool_id."""
        if not tool_ref:
            return None
        if ":" in tool_ref:
            tool = self.get_tool_by_name(tool_ref)
            if tool:
                return tool.tool_id
            return None
        tool = self.get_tool_by_name(tool_ref)
        return tool.tool_id if tool else None

    @staticmethod
    def _since_timestamp(hours: int) -> datetime:
        hours = max(1, int(hours))
        return datetime.utcfromtimestamp(time.time() - (hours * 3600))

    def get_tool_latency_metrics(self, tool_ref: str, hours: int = 24) -> Optional[Dict[str, Any]]:
        """Get percentile latency and reliability metrics from durable call history."""
        tool_id = self.resolve_tool_id(tool_ref)
        if not tool_id:
            return None

        since_ts = self._since_timestamp(hours)
        with self._get_conn(read_only=True) as conn:
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS calls,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS successes,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failures,
                    AVG(latency_ms) AS avg_latency_ms,
                    MIN(latency_ms) AS min_latency_ms,
                    MAX(latency_ms) AS max_latency_ms,
                    quantile_cont(latency_ms, 0.50) AS p50_latency_ms,
                    quantile_cont(latency_ms, 0.95) AS p95_latency_ms,
                    quantile_cont(latency_ms, 0.99) AS p99_latency_ms,
                    MAX(created_at) AS last_call_at
                FROM tool_call_history
                WHERE tool_id = ? AND created_at >= ?
                """,
                [tool_id, since_ts],
            ).fetchone()
            last_error = conn.execute(
                """
                SELECT error_code, error_message, created_at
                FROM tool_call_history
                WHERE tool_id = ? AND NOT success
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [tool_id],
            ).fetchone()

            tool_row = conn.execute(
                """
                SELECT name, backend_id, call_count, error_count, avg_latency_ms
                FROM tools
                WHERE tool_id = ?
                """,
                [tool_id],
            ).fetchone()

        calls = int(summary[0] or 0) if summary else 0
        successes = int(summary[1] or 0) if summary else 0
        failures = int(summary[2] or 0) if summary else 0
        all_time_calls = int(tool_row[2] or 0) if tool_row else 0
        all_time_errors = int(tool_row[3] or 0) if tool_row else 0

        response: Dict[str, Any] = {
            "tool_id": tool_id,
            "name": tool_row[0] if tool_row else tool_ref,
            "backend_id": tool_row[1] if tool_row else None,
            "window_hours": max(1, int(hours)),
            "calls": calls,
            "successes": successes,
            "failures": failures,
            "success_rate": (float(successes) / calls) if calls else 0.0,
            "error_rate": (float(failures) / calls) if calls else 0.0,
            "avg_latency_ms": float(summary[3] or 0.0) if summary else 0.0,
            "min_latency_ms": float(summary[4] or 0.0) if summary else 0.0,
            "max_latency_ms": float(summary[5] or 0.0) if summary else 0.0,
            "p50_latency_ms": float(summary[6] or 0.0) if summary else 0.0,
            "p95_latency_ms": float(summary[7] or 0.0) if summary else 0.0,
            "p99_latency_ms": float(summary[8] or 0.0) if summary else 0.0,
            "last_call_at": str(summary[9]) if summary and summary[9] else None,
            "all_time": {
                "calls": all_time_calls,
                "errors": all_time_errors,
                "avg_latency_ms": float(tool_row[4] or 0.0) if tool_row else 0.0,
                "error_rate": (float(all_time_errors) / all_time_calls) if all_time_calls else 0.0,
            },
        }

        if last_error:
            response["last_error"] = {
                "code": last_error[0],
                "message": last_error[1],
                "at": str(last_error[2]) if last_error[2] else None,
            }
        else:
            response["last_error"] = None
        return response

    def get_tool_usage_timeseries(
        self,
        *,
        hours: int = 24,
        bucket_minutes: int = 60,
        tool_ref: Optional[str] = None,
        limit: int = 2000,
    ) -> Dict[str, Any]:
        """Get bucketed call volume/latency series for charting and anomaly detection."""
        since_ts = self._since_timestamp(hours)
        bucket_seconds = max(60, int(bucket_minutes) * 60)
        max_rows = max(1, int(limit))

        where_sql = "created_at >= ?"
        params: List[Any] = [bucket_seconds, bucket_seconds, since_ts]
        resolved_tool_id = None
        if tool_ref:
            resolved_tool_id = self.resolve_tool_id(tool_ref)
            if not resolved_tool_id:
                return {
                    "window_hours": max(1, int(hours)),
                    "bucket_minutes": max(1, int(bucket_minutes)),
                    "tool_id": None,
                    "series": [],
                }
            where_sql += " AND tool_id = ?"
            params.append(resolved_tool_id)

        params.append(max_rows)
        with self._get_conn(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT
                    to_timestamp(floor(epoch(created_at) / ?) * ?) AS bucket_start,
                    COUNT(*) AS calls,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS successes,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failures,
                    AVG(latency_ms) AS avg_latency_ms,
                    quantile_cont(latency_ms, 0.95) AS p95_latency_ms
                FROM tool_call_history
                WHERE {where_sql}
                GROUP BY bucket_start
                ORDER BY bucket_start ASC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return {
            "window_hours": max(1, int(hours)),
            "bucket_minutes": max(1, int(bucket_minutes)),
            "tool_id": resolved_tool_id,
            "series": [
                {
                    "bucket_start": str(row[0]),
                    "calls": int(row[1] or 0),
                    "successes": int(row[2] or 0),
                    "failures": int(row[3] or 0),
                    "success_rate": (float(row[2] or 0) / float(row[1])) if row[1] else 0.0,
                    "avg_latency_ms": float(row[4] or 0.0),
                    "p95_latency_ms": float(row[5] or 0.0),
                }
                for row in rows
            ],
        }

    def get_failure_modes(
        self,
        *,
        hours: int = 24,
        tool_ref: Optional[str] = None,
        limit: int = 25,
    ) -> Dict[str, Any]:
        """Get top failure classes for debugging and reliability work."""
        since_ts = self._since_timestamp(hours)
        max_rows = max(1, int(limit))
        where_sql = "created_at >= ? AND NOT success"
        params: List[Any] = [since_ts]
        resolved_tool_id = None
        if tool_ref:
            resolved_tool_id = self.resolve_tool_id(tool_ref)
            if not resolved_tool_id:
                return {
                    "window_hours": max(1, int(hours)),
                    "tool_id": None,
                    "modes": [],
                }
            where_sql += " AND tool_id = ?"
            params.append(resolved_tool_id)
        params.append(max_rows)

        with self._get_conn(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT
                    COALESCE(error_code, 'unknown') AS error_code,
                    COUNT(*) AS failures,
                    MAX(created_at) AS last_seen_at
                FROM tool_call_history
                WHERE {where_sql}
                GROUP BY error_code
                ORDER BY failures DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return {
            "window_hours": max(1, int(hours)),
            "tool_id": resolved_tool_id,
            "modes": [
                {
                    "error_code": row[0],
                    "failures": int(row[1] or 0),
                    "last_seen_at": str(row[2]) if row[2] else None,
                }
                for row in rows
            ],
        }

    def get_backend_latency_metrics(self, *, hours: int = 24) -> Dict[str, Dict[str, Any]]:
        """Get backend-level reliability/latency metrics from durable call history."""
        since_ts = self._since_timestamp(hours)
        with self._get_conn(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT
                    backend_id,
                    COUNT(*) AS calls,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS successes,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failures,
                    AVG(latency_ms) AS avg_latency_ms,
                    quantile_cont(latency_ms, 0.50) AS p50_latency_ms,
                    quantile_cont(latency_ms, 0.95) AS p95_latency_ms,
                    quantile_cont(latency_ms, 0.99) AS p99_latency_ms,
                    MAX(created_at) AS last_call_at
                FROM tool_call_history
                WHERE created_at >= ?
                GROUP BY backend_id
                ORDER BY calls DESC
                """,
                [since_ts],
            ).fetchall()

        metrics: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            calls = int(row[1] or 0)
            failures = int(row[3] or 0)
            metrics[row[0]] = {
                "window_hours": max(1, int(hours)),
                "calls": calls,
                "successes": int(row[2] or 0),
                "failures": failures,
                "error_rate": (float(failures) / calls) if calls else 0.0,
                "avg_latency_ms": float(row[4] or 0.0),
                "p50_latency_ms": float(row[5] or 0.0),
                "p95_latency_ms": float(row[6] or 0.0),
                "p99_latency_ms": float(row[7] or 0.0),
                "last_call_at": str(row[8]) if row[8] else None,
            }
        return metrics

    def upsert_tool_dependency(
        self,
        *,
        source_tool_ref: str,
        target_tool_ref: str,
        dependency_type: str = "explicit",
        confidence: float = 1.0,
        call_count: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Insert/update a dependency edge between two tools."""
        source_tool_id = self.resolve_tool_id(source_tool_ref)
        target_tool_id = self.resolve_tool_id(target_tool_ref)
        if not source_tool_id:
            raise ValueError(f"Source tool not found: {source_tool_ref}")
        if not target_tool_id:
            raise ValueError(f"Target tool not found: {target_tool_ref}")

        now = datetime.utcnow().isoformat()
        dep_type = (dependency_type or "explicit").strip().lower()
        confidence = min(1.0, max(0.0, float(confidence)))
        increment = max(1, int(call_count))
        payload = json.dumps(metadata or {})

        with self._get_conn() as conn:
            # Read-modify-write upsert (attach-safe; no ON CONFLICT). Merge with
            # any existing edge: max confidence, summed call_count, kept first_seen.
            existing = conn.execute(
                "SELECT confidence, call_count, first_seen_at FROM tool_dependencies "
                "WHERE source_tool_id=? AND target_tool_id=? AND dependency_type=?",
                [source_tool_id, target_tool_id, dep_type],
            ).fetchone()
            merged_conf = max(confidence, float(existing[0])) if existing else confidence
            merged_calls = (int(existing[1]) + increment) if existing else increment
            first_seen = existing[2] if existing else now
            conn.execute(
                "DELETE FROM tool_dependencies WHERE source_tool_id=? AND target_tool_id=? AND dependency_type=?",
                [source_tool_id, target_tool_id, dep_type],
            )
            conn.execute(
                """
                INSERT INTO tool_dependencies (
                    source_tool_id, target_tool_id, dependency_type, confidence,
                    call_count, metadata, first_seen_at, last_seen_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [source_tool_id, target_tool_id, dep_type, merged_conf,
                 merged_calls, payload, first_seen, now, now, now],
            )

        return {
            "source_tool_id": source_tool_id,
            "target_tool_id": target_tool_id,
            "dependency_type": dep_type,
            "confidence": confidence,
            "incremented_by": increment,
        }

    def list_tool_dependencies(
        self,
        *,
        tool_ref: Optional[str] = None,
        dependency_type: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """List dependency edges with optional filters."""
        where = ["1=1"]
        params: List[Any] = []

        tool_id = None
        if tool_ref:
            tool_id = self.resolve_tool_id(tool_ref)
            if not tool_id:
                return []
            where.append("(d.source_tool_id = ? OR d.target_tool_id = ?)")
            params.extend([tool_id, tool_id])

        if dependency_type:
            where.append("d.dependency_type = ?")
            params.append(dependency_type.strip().lower())

        min_confidence = min(1.0, max(0.0, float(min_confidence)))
        where.append("d.confidence >= ?")
        params.append(min_confidence)
        params.append(max(1, int(limit)))

        where_sql = " AND ".join(where)
        with self._get_conn(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT
                    d.source_tool_id,
                    COALESCE(src.name, d.source_tool_id) AS source_name,
                    d.target_tool_id,
                    COALESCE(dst.name, d.target_tool_id) AS target_name,
                    d.dependency_type,
                    d.confidence,
                    d.call_count,
                    d.first_seen_at,
                    d.last_seen_at,
                    d.metadata
                FROM tool_dependencies d
                LEFT JOIN tools src ON src.tool_id = d.source_tool_id
                LEFT JOIN tools dst ON dst.tool_id = d.target_tool_id
                WHERE {where_sql}
                ORDER BY d.call_count DESC, d.confidence DESC, d.last_seen_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        dependencies: List[Dict[str, Any]] = []
        for row in rows:
            metadata = row[9]
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {"raw": metadata}
            dependencies.append(
                {
                    "source_tool_id": row[0],
                    "source_name": row[1],
                    "target_tool_id": row[2],
                    "target_name": row[3],
                    "dependency_type": row[4],
                    "confidence": float(row[5] or 0.0),
                    "call_count": int(row[6] or 0),
                    "first_seen_at": str(row[7]) if row[7] else None,
                    "last_seen_at": str(row[8]) if row[8] else None,
                    "metadata": metadata if isinstance(metadata, dict) else {},
                }
            )
        return dependencies


    # ==================== Statistics ====================

    def record_call(
        self,
        tool_id: str,
        latency_ms: float,
        success: bool,
        *,
        execution_mode: str = "sync",
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        """Record a tool call for statistics and durable time-series analytics."""
        tool = self.get_tool(tool_id)
        backend_id = tool.backend_id if tool else (tool_id.split(":", 1)[0] if ":" in tool_id else "unknown")
        now = datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            if success:
                conn.execute("""
                    UPDATE tools SET
                        call_count = call_count + 1,
                        avg_latency_ms = (avg_latency_ms * call_count + ?) / (call_count + 1)
                    WHERE tool_id = ?
                """, [latency_ms, tool_id])
            else:
                conn.execute("""
                    UPDATE tools SET
                        call_count = call_count + 1,
                        error_count = error_count + 1
                    WHERE tool_id = ?
                """, [tool_id])

            # Python-generated monotonic id (µs epoch + small rand) instead of a
            # DB sequence — DuckDB's postgres attach can't nextval() a PG
            # sequence, and this works identically on the DuckDB-file fallback.
            call_id = int(time.time() * 1_000_000) * 1000 + random.randint(0, 999)
            conn.execute(
                """
                INSERT INTO tool_call_history (
                    id, tool_id, backend_id, success, latency_ms, execution_mode,
                    trace_id, session_id, error_code, error_message, created_at
                )
                VALUES (
                    ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    call_id,
                    tool_id,
                    backend_id,
                    bool(success),
                    float(latency_ms or 0.0),
                    execution_mode or "sync",
                    trace_id,
                    session_id,
                    error_code,
                    (error_message or "")[:1000] if error_message else None,
                    now,
                ],
            )

    _stats_cache: Optional[Dict] = None
    _stats_cache_ts: float = 0.0

    def get_stats(self) -> Dict:
        """Overall statistics, TTL-cached so cheap callers (e.g. /health) serve
        process-local counts instead of hitting Neon per request. The periodic
        refresh doubles as a Neon keepalive (prevents compute autosuspend)."""
        ttl = float(os.getenv("REGISTRY_STATS_TTL_SECONDS", "45"))
        now = time.time()
        if ToolStore._stats_cache is not None and (now - ToolStore._stats_cache_ts) < ttl:
            return ToolStore._stats_cache
        stats = self._compute_stats()
        ToolStore._stats_cache = stats
        ToolStore._stats_cache_ts = now
        return stats

    def _compute_stats(self) -> Dict:
        with self._get_conn(read_only=True) as conn:
            tool_count = conn.execute("SELECT COUNT(*) FROM tools").fetchone()[0]
            backend_count = conn.execute("SELECT COUNT(*) FROM backends").fetchone()[0]
            healthy_backends = conn.execute("SELECT COUNT(*) FROM backends WHERE is_healthy = true").fetchone()[0]
            total_calls = conn.execute("SELECT SUM(call_count) FROM tools").fetchone()[0] or 0
            total_errors = conn.execute("SELECT SUM(error_count) FROM tools").fetchone()[0] or 0
            total_call_history = conn.execute("SELECT COUNT(*) FROM tool_call_history").fetchone()[0] or 0
            dependency_edges = conn.execute("SELECT COUNT(*) FROM tool_dependencies").fetchone()[0] or 0

            return {
                "tool_count": tool_count,
                "backend_count": backend_count,
                "healthy_backends": healthy_backends,
                "total_calls": total_calls,
                "total_errors": total_errors,
                "error_rate": total_errors / total_calls if total_calls > 0 else 0,
                "total_call_history_rows": int(total_call_history),
                "dependency_edges": int(dependency_edges),
            }

    # ==================== Integration Feedback ====================

    def record_feedback(
        self,
        *,
        backend_id: str,
        message: str,
        tool_name: Optional[str] = None,
        outcome: Optional[str] = None,
        category: str = "other",
        rating: Optional[int] = None,
        suggested_fix: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record AI-agent feedback about a tool/integration for later review."""
        feedback_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO integration_feedback (
                    feedback_id, backend_id, tool_name, outcome, category, rating,
                    message, suggested_fix, context, session_id, trace_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    feedback_id, backend_id, tool_name, outcome, category or "other", rating,
                    message[:5000], suggested_fix, json.dumps(context) if context else None,
                    session_id, trace_id, now,
                ],
            )
        return {"feedback_id": feedback_id, "status": "recorded", "created_at": now}

    def query_feedback(
        self,
        *,
        backend_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[List[Dict[str, Any]], int]:
        where = ["1=1"]
        params: List[Any] = []
        if backend_id:
            where.append("backend_id = ?"); params.append(backend_id)
        if tool_name:
            where.append("tool_name = ?"); params.append(tool_name)
        if category:
            where.append("category = ?"); params.append(category)
        where_sql = " AND ".join(where)

        with self._get_conn(read_only=True) as conn:
            total = conn.execute(
                f"SELECT COUNT(*) FROM integration_feedback WHERE {where_sql}", params
            ).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT feedback_id, backend_id, tool_name, outcome, category, rating,
                       message, suggested_fix, context, session_id, trace_id, created_at
                FROM integration_feedback WHERE {where_sql}
                ORDER BY created_at DESC LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
            cols = [d[0] for d in conn.description]

        items = []
        for row in rows:
            d = {k: _json_safe_value(v) for k, v in zip(cols, row)}
            if d.get("context"):
                d["context"] = json.loads(d["context"]) if isinstance(d["context"], str) else d["context"]
            items.append(d)
        return items, int(total)

    def feedback_stats(self) -> Dict[str, Any]:
        with self._get_conn(read_only=True) as conn:
            total = conn.execute("SELECT COUNT(*) FROM integration_feedback").fetchone()[0]
            by_backend = dict(conn.execute(
                "SELECT backend_id, COUNT(*) FROM integration_feedback GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall())
            by_category = dict(conn.execute(
                "SELECT category, COUNT(*) FROM integration_feedback GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall())
            by_outcome = dict(conn.execute(
                "SELECT outcome, COUNT(*) FROM integration_feedback WHERE outcome IS NOT NULL GROUP BY 1"
            ).fetchall())
            avg_rating = conn.execute(
                "SELECT AVG(rating) FROM integration_feedback WHERE rating IS NOT NULL"
            ).fetchone()[0]
        return {
            "total_feedback": int(total),
            "by_backend": by_backend,
            "by_category": by_category,
            "by_outcome": by_outcome,
            "avg_rating": round(float(avg_rating), 2) if avg_rating is not None else None,
        }


def _build_signature(name: str, props: Dict, required: set) -> str:
    """Build a function-like signature string for a tool."""
    parts = []
    for pname, pschema in props.items():
        ptype = pschema.get("type", "any")
        if pname in required:
            parts.append(f"{pname}: {ptype}")
        else:
            default = pschema.get("default", "...")
            parts.append(f"{pname}: {ptype} = {default}")
    return f"{name}({', '.join(parts)})"


# Global singleton
_tool_store: Optional[ToolStore] = None


def get_tool_store() -> ToolStore:
    """Get the global tool store instance."""
    global _tool_store
    if _tool_store is None:
        _tool_store = ToolStore()
    return _tool_store
