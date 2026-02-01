"""
Tool Context Manager - Smart loading and eviction of tool schemas.

Manages tool schemas across multiple MCP backends with:
- Schema caching with TTL
- Token budget tracking
- LRU eviction policy
- Semantic clustering for related tools
- Usage-based prioritization
"""

from __future__ import annotations

import json
import time
import sqlite3
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from collections import OrderedDict
import threading

logger = logging.getLogger(__name__)

# Singleton instance
_context_manager: Optional["ToolContextManager"] = None
_lock = threading.Lock()


class ThermalState(Enum):
    """Tool thermal state based on usage patterns."""
    FROZEN = "frozen"      # Never used, candidate for eviction
    COLD = "cold"          # Not used recently
    COOL = "cool"          # Used occasionally
    WARM = "warm"          # Used recently
    HOT = "hot"            # Frequently used
    ACTIVE = "active"      # Currently in use


@dataclass
class ToolSchema:
    """Cached tool schema with metadata."""
    name: str
    backend_id: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Optional[Dict[str, Any]] = None

    # Metadata
    group: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    estimated_tokens: int = 0

    # Usage tracking
    use_count: int = 0
    last_used: Optional[datetime] = None
    thermal_state: ThermalState = ThermalState.FROZEN

    # Caching
    cached_at: datetime = field(default_factory=datetime.utcnow)
    schema_hash: str = ""

    def __post_init__(self):
        if not self.schema_hash:
            self.schema_hash = self._compute_hash()
        if self.estimated_tokens == 0:
            self.estimated_tokens = self._estimate_tokens()

    def _compute_hash(self) -> str:
        """Compute hash of schema for change detection."""
        content = json.dumps({
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _estimate_tokens(self) -> int:
        """Estimate token count for this schema."""
        # Rough estimate: ~4 chars per token
        content = json.dumps({
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        })
        return len(content) // 4

    def record_use(self):
        """Record a tool usage."""
        self.use_count += 1
        self.last_used = datetime.utcnow()
        self._update_thermal_state()

    def _update_thermal_state(self):
        """Update thermal state based on usage."""
        if self.last_used is None:
            self.thermal_state = ThermalState.FROZEN
            return

        age = datetime.utcnow() - self.last_used

        if age < timedelta(minutes=5):
            self.thermal_state = ThermalState.HOT
        elif age < timedelta(minutes=30):
            self.thermal_state = ThermalState.WARM
        elif age < timedelta(hours=2):
            self.thermal_state = ThermalState.COOL
        elif age < timedelta(hours=24):
            self.thermal_state = ThermalState.COLD
        else:
            self.thermal_state = ThermalState.FROZEN

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "backend_id": self.backend_id,
            "description": self.description,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "group": self.group,
            "tags": self.tags,
            "estimated_tokens": self.estimated_tokens,
            "use_count": self.use_count,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "thermal_state": self.thermal_state.value,
            "cached_at": self.cached_at.isoformat(),
            "schema_hash": self.schema_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ToolSchema":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            backend_id=data["backend_id"],
            description=data["description"],
            input_schema=data["input_schema"],
            output_schema=data.get("output_schema"),
            group=data.get("group"),
            tags=data.get("tags", []),
            estimated_tokens=data.get("estimated_tokens", 0),
            use_count=data.get("use_count", 0),
            last_used=datetime.fromisoformat(data["last_used"]) if data.get("last_used") else None,
            thermal_state=ThermalState(data.get("thermal_state", "frozen")),
            cached_at=datetime.fromisoformat(data["cached_at"]) if data.get("cached_at") else datetime.utcnow(),
            schema_hash=data.get("schema_hash", ""),
        )

    def to_mcp_tool(self) -> Dict[str, Any]:
        """Convert to MCP tool format for client consumption."""
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }


@dataclass
class ContextBudget:
    """Token budget for tool context."""
    total_tokens: int = 30000
    reserved_tokens: int = 5000  # Reserved for system/user messages
    tool_budget: int = 0

    # Current usage
    loaded_tokens: int = 0
    loaded_tools: int = 0

    def __post_init__(self):
        self.tool_budget = self.total_tokens - self.reserved_tokens

    def can_load(self, tool: ToolSchema) -> bool:
        """Check if tool can be loaded within budget."""
        return (self.loaded_tokens + tool.estimated_tokens) <= self.tool_budget

    def load(self, tool: ToolSchema):
        """Mark tool as loaded."""
        self.loaded_tokens += tool.estimated_tokens
        self.loaded_tools += 1

    def unload(self, tool: ToolSchema):
        """Mark tool as unloaded."""
        self.loaded_tokens = max(0, self.loaded_tokens - tool.estimated_tokens)
        self.loaded_tools = max(0, self.loaded_tools - 1)

    def utilization(self) -> float:
        """Get budget utilization percentage."""
        return self.loaded_tokens / self.tool_budget if self.tool_budget > 0 else 0

    def remaining(self) -> int:
        """Get remaining token budget."""
        return self.tool_budget - self.loaded_tokens


class ToolContextManager:
    """
    Manages tool schemas with smart loading and eviction.

    Features:
    - Persistent schema cache in SQLite
    - Token budget management
    - LRU eviction with thermal state consideration
    - Backend synchronization
    """

    def __init__(
        self,
        db_path: str = "tool_context.db",
        budget: Optional[ContextBudget] = None,
    ):
        self.db_path = db_path
        self.budget = budget or ContextBudget()

        # In-memory caches
        self._schemas: Dict[str, ToolSchema] = {}  # key: backend_id:tool_name
        self._loaded: OrderedDict[str, ToolSchema] = OrderedDict()  # LRU order
        self._groups: Dict[str, Set[str]] = {}  # group -> tool keys
        self._backends: Dict[str, Set[str]] = {}  # backend_id -> tool keys

        self._init_db()
        self._load_from_db()

    def _init_db(self):
        """Initialize SQLite database."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tool_schemas (
                    key TEXT PRIMARY KEY,
                    backend_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    data JSON NOT NULL,
                    schema_hash TEXT,
                    cached_at TEXT,
                    use_count INTEGER DEFAULT 0,
                    last_used TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_backend ON tool_schemas(backend_id);
                CREATE INDEX IF NOT EXISTS idx_name ON tool_schemas(name);
                CREATE INDEX IF NOT EXISTS idx_use_count ON tool_schemas(use_count DESC);

                CREATE TABLE IF NOT EXISTS tool_groups (
                    group_name TEXT NOT NULL,
                    tool_key TEXT NOT NULL,
                    PRIMARY KEY (group_name, tool_key)
                );

                CREATE TABLE IF NOT EXISTS context_sessions (
                    session_id TEXT PRIMARY KEY,
                    loaded_tools JSON,
                    budget_used INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                );
            """)
            conn.commit()
        finally:
            conn.close()

    def _load_from_db(self):
        """Load cached schemas from database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT key, data FROM tool_schemas")
            for key, data_json in cursor:
                try:
                    data = json.loads(data_json)
                    schema = ToolSchema.from_dict(data)
                    self._schemas[key] = schema

                    # Index by backend
                    if schema.backend_id not in self._backends:
                        self._backends[schema.backend_id] = set()
                    self._backends[schema.backend_id].add(key)

                    # Index by group
                    if schema.group:
                        if schema.group not in self._groups:
                            self._groups[schema.group] = set()
                        self._groups[schema.group].add(key)
                except Exception as e:
                    logger.warning(f"Failed to load schema {key}: {e}")

            logger.info(f"Loaded {len(self._schemas)} tool schemas from cache")
        finally:
            conn.close()

    def _save_schema(self, schema: ToolSchema):
        """Save schema to database."""
        key = f"{schema.backend_id}:{schema.name}"
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO tool_schemas
                (key, backend_id, name, data, schema_hash, cached_at, use_count, last_used)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key,
                schema.backend_id,
                schema.name,
                json.dumps(schema.to_dict()),
                schema.schema_hash,
                schema.cached_at.isoformat(),
                schema.use_count,
                schema.last_used.isoformat() if schema.last_used else None,
            ))
            conn.commit()
        finally:
            conn.close()

    def register_tool(
        self,
        backend_id: str,
        name: str,
        description: str,
        input_schema: Dict[str, Any],
        output_schema: Optional[Dict[str, Any]] = None,
        group: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> ToolSchema:
        """Register a tool schema."""
        key = f"{backend_id}:{name}"

        schema = ToolSchema(
            name=name,
            backend_id=backend_id,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            group=group,
            tags=tags or [],
        )

        self._schemas[key] = schema

        # Index
        if backend_id not in self._backends:
            self._backends[backend_id] = set()
        self._backends[backend_id].add(key)

        if group:
            if group not in self._groups:
                self._groups[group] = set()
            self._groups[group].add(key)

        # Persist
        self._save_schema(schema)

        return schema

    def register_tools_bulk(
        self,
        backend_id: str,
        tools: List[Dict[str, Any]],
        group: Optional[str] = None,
    ) -> int:
        """Register multiple tools at once."""
        count = 0
        for tool in tools:
            try:
                self.register_tool(
                    backend_id=backend_id,
                    name=tool.get("name", tool.get("operationId", "")),
                    description=tool.get("description", tool.get("summary", "")),
                    input_schema=tool.get("inputSchema", tool.get("input_schema", tool.get("parameters", {}))),
                    output_schema=tool.get("outputSchema", tool.get("output_schema")),
                    group=group or tool.get("group"),
                    tags=tool.get("tags", []),
                )
                count += 1
            except Exception as e:
                logger.warning(f"Failed to register tool {tool.get('name')}: {e}")

        logger.info(f"Registered {count} tools for backend {backend_id}")
        return count

    def get_schema(self, backend_id: str, name: str) -> Optional[ToolSchema]:
        """Get a specific tool schema."""
        key = f"{backend_id}:{name}"
        return self._schemas.get(key)

    def load_tool(self, backend_id: str, name: str) -> Optional[ToolSchema]:
        """Load a tool into active context."""
        key = f"{backend_id}:{name}"
        schema = self._schemas.get(key)

        if not schema:
            return None

        # Check if already loaded
        if key in self._loaded:
            # Move to end (most recently used)
            self._loaded.move_to_end(key)
            return schema

        # Check budget
        if not self.budget.can_load(schema):
            # Need to evict
            self._evict_for_space(schema.estimated_tokens)

        if self.budget.can_load(schema):
            self._loaded[key] = schema
            self.budget.load(schema)
            schema.thermal_state = ThermalState.ACTIVE
            return schema

        return None

    def unload_tool(self, backend_id: str, name: str) -> bool:
        """Unload a tool from active context."""
        key = f"{backend_id}:{name}"

        if key in self._loaded:
            schema = self._loaded.pop(key)
            self.budget.unload(schema)
            schema._update_thermal_state()
            return True

        return False

    def record_tool_use(self, backend_id: str, name: str):
        """Record that a tool was used."""
        key = f"{backend_id}:{name}"
        schema = self._schemas.get(key)

        if schema:
            schema.record_use()
            self._save_schema(schema)

            # Move to end in loaded order
            if key in self._loaded:
                self._loaded.move_to_end(key)

    def _evict_for_space(self, needed_tokens: int):
        """Evict tools to make space for new ones."""
        freed = 0
        to_evict = []

        # Evict coldest tools first (from front of OrderedDict = LRU)
        for key, schema in list(self._loaded.items()):
            if freed >= needed_tokens:
                break

            # Don't evict hot/active tools unless necessary
            if schema.thermal_state in (ThermalState.HOT, ThermalState.ACTIVE):
                continue

            to_evict.append(key)
            freed += schema.estimated_tokens

        # If still not enough, evict warmer tools
        if freed < needed_tokens:
            for key, schema in list(self._loaded.items()):
                if freed >= needed_tokens:
                    break
                if key not in to_evict:
                    to_evict.append(key)
                    freed += schema.estimated_tokens

        # Perform eviction
        for key in to_evict:
            schema = self._loaded.pop(key)
            self.budget.unload(schema)
            schema._update_thermal_state()
            logger.debug(f"Evicted tool {key} (thermal: {schema.thermal_state.value})")

    def load_group(self, group: str) -> List[ToolSchema]:
        """Load all tools in a group."""
        loaded = []
        keys = self._groups.get(group, set())

        for key in keys:
            schema = self._schemas.get(key)
            if schema:
                backend_id, name = key.split(":", 1)
                result = self.load_tool(backend_id, name)
                if result:
                    loaded.append(result)

        return loaded

    def load_backend(self, backend_id: str, limit: Optional[int] = None) -> List[ToolSchema]:
        """Load tools from a specific backend."""
        loaded = []
        keys = self._backends.get(backend_id, set())

        # Sort by use_count (most used first)
        sorted_keys = sorted(
            keys,
            key=lambda k: self._schemas[k].use_count if k in self._schemas else 0,
            reverse=True
        )

        if limit:
            sorted_keys = sorted_keys[:limit]

        for key in sorted_keys:
            schema = self._schemas.get(key)
            if schema:
                result = self.load_tool(backend_id, schema.name)
                if result:
                    loaded.append(result)

        return loaded

    def get_loaded_tools(self) -> List[ToolSchema]:
        """Get all currently loaded tools."""
        return list(self._loaded.values())

    def get_loaded_as_mcp(self) -> List[Dict[str, Any]]:
        """Get loaded tools in MCP format."""
        return [schema.to_mcp_tool() for schema in self._loaded.values()]

    def search_tools(
        self,
        query: str,
        backend_id: Optional[str] = None,
        group: Optional[str] = None,
        limit: int = 10,
    ) -> List[ToolSchema]:
        """Simple text search for tools."""
        query_lower = query.lower()
        results = []

        candidates = self._schemas.values()

        if backend_id:
            keys = self._backends.get(backend_id, set())
            candidates = [self._schemas[k] for k in keys if k in self._schemas]

        if group:
            keys = self._groups.get(group, set())
            candidates = [s for s in candidates if f"{s.backend_id}:{s.name}" in keys]

        for schema in candidates:
            score = 0

            # Name match
            if query_lower in schema.name.lower():
                score += 10

            # Description match
            if query_lower in schema.description.lower():
                score += 5

            # Tag match
            for tag in schema.tags:
                if query_lower in tag.lower():
                    score += 3

            # Boost by usage
            score += min(schema.use_count, 5)

            if score > 0:
                results.append((score, schema))

        # Sort by score
        results.sort(key=lambda x: x[0], reverse=True)

        return [schema for _, schema in results[:limit]]

    def get_stats(self) -> Dict[str, Any]:
        """Get context manager statistics."""
        return {
            "total_schemas": len(self._schemas),
            "loaded_tools": len(self._loaded),
            "backends": {
                bid: len(keys) for bid, keys in self._backends.items()
            },
            "groups": {
                g: len(keys) for g, keys in self._groups.items()
            },
            "budget": {
                "total": self.budget.total_tokens,
                "tool_budget": self.budget.tool_budget,
                "loaded_tokens": self.budget.loaded_tokens,
                "utilization": f"{self.budget.utilization():.1%}",
                "remaining": self.budget.remaining(),
            },
            "thermal_distribution": self._get_thermal_distribution(),
        }

    def _get_thermal_distribution(self) -> Dict[str, int]:
        """Get distribution of thermal states."""
        dist = {state.value: 0 for state in ThermalState}
        for schema in self._schemas.values():
            schema._update_thermal_state()
            dist[schema.thermal_state.value] += 1
        return dist

    def clear_loaded(self):
        """Clear all loaded tools."""
        self._loaded.clear()
        self.budget.loaded_tokens = 0
        self.budget.loaded_tools = 0

    def clear_cache(self, backend_id: Optional[str] = None):
        """Clear cached schemas."""
        conn = sqlite3.connect(self.db_path)
        try:
            if backend_id:
                conn.execute("DELETE FROM tool_schemas WHERE backend_id = ?", (backend_id,))
                # Clear in-memory
                keys = list(self._backends.get(backend_id, set()))
                for key in keys:
                    self._schemas.pop(key, None)
                    self._loaded.pop(key, None)
                self._backends.pop(backend_id, None)
            else:
                conn.execute("DELETE FROM tool_schemas")
                self._schemas.clear()
                self._loaded.clear()
                self._backends.clear()
                self._groups.clear()

            conn.commit()
        finally:
            conn.close()


def get_tool_context_manager(
    db_path: Optional[str] = None,
    budget: Optional[ContextBudget] = None,
) -> ToolContextManager:
    """Get or create singleton ToolContextManager."""
    global _context_manager

    with _lock:
        if _context_manager is None:
            _context_manager = ToolContextManager(
                db_path=db_path or "data/tool_context.db",
                budget=budget,
            )
        return _context_manager
