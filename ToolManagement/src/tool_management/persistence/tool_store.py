"""
Persistent Tool Store - DuckDB-backed durable tool registration.

Tools are persisted with their backend mappings, schemas, and metadata.
No more in-memory-only registration that disappears on restart.
"""

import os
import json
import duckdb
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
    input_schema: Dict = None
    output_schema: Dict = None
    tags: List[str] = None
    category: str = "general"
    version: str = "1.0.0"
    created_at: str = None
    updated_at: str = None
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

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_schema(self):
        """Initialize database schema."""
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

            # Indexes for fast lookup
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_name ON tools(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_backend ON tools(backend_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tools_category ON tools(category)")

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
                conn.execute("""
                    INSERT INTO backends (backend_id, name, url, health_endpoint, mcp_endpoint, capabilities, last_heartbeat)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (backend_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        url = EXCLUDED.url,
                        health_endpoint = EXCLUDED.health_endpoint,
                        mcp_endpoint = EXCLUDED.mcp_endpoint,
                        capabilities = EXCLUDED.capabilities,
                        last_heartbeat = EXCLUDED.last_heartbeat
                """, [backend_id, name, url, health_endpoint, mcp_endpoint, json.dumps(capabilities), now])
            logger.info(f"Registered backend: {backend_id} at {url}")
            return {"backend_id": backend_id, "url": url, "status": "registered"}
        except Exception as e:
            logger.error(f"Failed to register backend {backend_id}: {e}")
            raise

    def get_backend(self, backend_id: str) -> Optional[Dict]:
        """Get backend by ID."""
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT * FROM backends WHERE backend_id = ?", [backend_id]
            ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))
        return None

    def list_backends(self) -> List[Dict]:
        """List all registered backends."""
        with self._get_conn() as conn:
            results = conn.execute("SELECT * FROM backends ORDER BY name").fetchall()
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
            conn.execute("""
                INSERT INTO tools (
                    tool_id, name, backend_id, backend_url, description,
                    input_schema, output_schema, tags, category, version,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tool_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    backend_id = EXCLUDED.backend_id,
                    backend_url = EXCLUDED.backend_url,
                    description = EXCLUDED.description,
                    input_schema = EXCLUDED.input_schema,
                    output_schema = EXCLUDED.output_schema,
                    tags = EXCLUDED.tags,
                    category = EXCLUDED.category,
                    version = EXCLUDED.version,
                    updated_at = EXCLUDED.updated_at
            """, [
                tool.tool_id, tool.name, tool.backend_id, tool.backend_url,
                tool.description, json.dumps(tool.input_schema), json.dumps(tool.output_schema),
                json.dumps(tool.tags), tool.category, tool.version,
                tool.created_at or now, now
            ])

            # Update backend tool count
            conn.execute("""
                UPDATE backends SET tool_count = (
                    SELECT COUNT(*) FROM tools WHERE backend_id = ?
                ) WHERE backend_id = ?
            """, [tool.backend_id, tool.backend_id])

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
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT * FROM tools WHERE tool_id = ?", [tool_id]
            ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                data = dict(zip(columns, result))
                data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                data['tags'] = json.loads(data['tags']) if data['tags'] else []
                return RegisteredTool(**data)
        return None

    def get_tool_by_name(self, name: str, backend_id: str = None) -> Optional[RegisteredTool]:
        """Get tool by name, optionally filtered by backend."""
        with self._get_conn() as conn:
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
                return RegisteredTool(**data)
        return None

    def list_tools(self, backend_id: str = None, category: str = None, limit: int = 100) -> List[RegisteredTool]:
        """List tools with optional filters."""
        query = "SELECT * FROM tools WHERE 1=1"
        params = []

        if backend_id:
            query += " AND backend_id = ?"
            params.append(backend_id)
        if category:
            query += " AND category = ?"
            params.append(category)

        query += f" ORDER BY name LIMIT {limit}"

        with self._get_conn() as conn:
            results = conn.execute(query, params).fetchall()
            columns = [desc[0] for desc in conn.description]

            tools = []
            for row in results:
                data = dict(zip(columns, row))
                data['input_schema'] = json.loads(data['input_schema']) if data['input_schema'] else {}
                data['output_schema'] = json.loads(data['output_schema']) if data['output_schema'] else {}
                data['tags'] = json.loads(data['tags']) if data['tags'] else []
                tools.append(RegisteredTool(**data))

            return tools

    def list_tools_by_backend(self, backend_id: str) -> List[RegisteredTool]:
        """List all tools for a specific backend."""
        return self.list_tools(backend_id=backend_id, limit=1000)

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

    # ==================== Statistics ====================

    def record_call(self, tool_id: str, latency_ms: float, success: bool):
        """Record a tool call for statistics."""
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

    def get_stats(self) -> Dict:
        """Get overall statistics."""
        with self._get_conn() as conn:
            tool_count = conn.execute("SELECT COUNT(*) FROM tools").fetchone()[0]
            backend_count = conn.execute("SELECT COUNT(*) FROM backends").fetchone()[0]
            healthy_backends = conn.execute("SELECT COUNT(*) FROM backends WHERE is_healthy = true").fetchone()[0]
            total_calls = conn.execute("SELECT SUM(call_count) FROM tools").fetchone()[0] or 0
            total_errors = conn.execute("SELECT SUM(error_count) FROM tools").fetchone()[0] or 0

            return {
                "tool_count": tool_count,
                "backend_count": backend_count,
                "healthy_backends": healthy_backends,
                "total_calls": total_calls,
                "total_errors": total_errors,
                "error_rate": total_errors / total_calls if total_calls > 0 else 0
            }


# Global singleton
_tool_store: Optional[ToolStore] = None


def get_tool_store() -> ToolStore:
    """Get the global tool store instance."""
    global _tool_store
    if _tool_store is None:
        _tool_store = ToolStore()
    return _tool_store
