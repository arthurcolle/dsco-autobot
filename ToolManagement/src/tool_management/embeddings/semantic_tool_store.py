"""
Semantic Tool Store - Scale to millions of tools with intelligent retrieval

Uses embeddings and vector search to:
1. Store tool schemas with their embeddings
2. Retrieve relevant tools semantically
3. Manage context window intelligently (load/evict)
4. Enable single MCP client to access millions of tools
"""
from __future__ import annotations

import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import sqlite3
import json
from pathlib import Path

from .openrouter_embeddings import get_embedding_service, cosine_similarity

logger = logging.getLogger(__name__)

# Use DATA_DIR from environment (set to /data on Modal volume)
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = Path("/tmp/semantic_tool_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ToolEmbedding:
    """Tool with its embedding"""
    tool_name: str
    tool_data: Dict[str, Any]
    embedding: np.ndarray
    backend: Optional[str] = None
    last_accessed: Optional[datetime] = None
    access_count: int = 0


@dataclass
class ContextWindow:
    """Manages active tools in MCP context"""
    max_tools: int = 50  # Maximum tools to expose at once
    active_tools: Dict[str, ToolEmbedding] = field(default_factory=dict)
    access_scores: Dict[str, float] = field(default_factory=dict)

    def can_add(self) -> bool:
        """Check if we can add more tools"""
        return len(self.active_tools) < self.max_tools

    def should_evict(self) -> bool:
        """Check if we should evict tools"""
        return len(self.active_tools) >= self.max_tools

    def get_eviction_candidate(self) -> Optional[str]:
        """Get tool to evict based on access patterns"""
        if not self.active_tools:
            return None

        # Evict least recently used with lowest score
        candidates = sorted(
            self.active_tools.keys(),
            key=lambda name: (
                self.access_scores.get(name, 0),
                self.active_tools[name].last_accessed or datetime.min
            )
        )

        return candidates[0] if candidates else None


class SemanticToolStore:
    """
    Semantic tool store with intelligent context management.

    Scales to millions of tools by:
    1. Storing all tools with embeddings in SQLite
    2. Dynamically loading only relevant tools into MCP context
    3. Evicting unused tools from context
    4. Using semantic search for tool discovery
    """

    def __init__(self, db_path: Optional[str] = None, max_context_tools: int = 50):
        if db_path is None:
            db_path = str(DATA_DIR / "tool_embeddings.db")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"SemanticToolStore using database at {self.db_path}")

        self.embedding_service = get_embedding_service()
        self.context = ContextWindow(max_tools=max_context_tools)

        self._init_db()

    def _init_db(self):
        """Initialize the tool embeddings database"""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tool_embeddings (
                tool_name TEXT PRIMARY KEY,
                tool_data TEXT NOT NULL,
                embedding BLOB NOT NULL,
                backend TEXT,
                last_accessed TEXT,
                access_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_backend
            ON tool_embeddings(backend)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_access
            ON tool_embeddings(access_count DESC, last_accessed DESC)
        """)

        conn.commit()
        conn.close()

    async def index_tool(self, tool: Dict[str, Any], backend: Optional[str] = None):
        """
        Index a tool with its embedding.

        Args:
            tool: Tool schema dict (name, description, inputSchema, outputSchema, etc.)
            backend: Backend ID this tool comes from
        """
        tool_name = tool.get("name")
        if not tool_name:
            logger.warning("Tool missing name, skipping indexing")
            return

        try:
            # Generate embedding
            embedding = await self.embedding_service.embed_tool_schema(tool)

            # Store in database
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("""
                INSERT OR REPLACE INTO tool_embeddings
                (tool_name, tool_data, embedding, backend, last_accessed, access_count)
                VALUES (?, ?, ?, ?, ?, COALESCE(
                    (SELECT access_count FROM tool_embeddings WHERE tool_name = ?),
                    0
                ))
            """, (
                tool_name,
                json.dumps(tool),
                embedding.tobytes(),
                backend,
                datetime.utcnow().isoformat(),
                tool_name
            ))
            conn.commit()
            conn.close()

            logger.debug(f"Indexed tool: {tool_name}")

        except Exception as e:
            logger.error(f"Failed to index tool {tool_name}: {e}")

    async def index_tools_batch(self, tools: List[Dict[str, Any]], backend: Optional[str] = None):
        """Batch index multiple tools - embeddings are batched for efficiency"""
        if not tools:
            return

        try:
            # Convert all tools to text representations
            tool_texts = []
            tool_names = []
            for tool in tools:
                tool_name = tool.get("name")
                if not tool_name:
                    logger.warning("Tool missing name, skipping indexing")
                    continue
                tool_names.append(tool_name)
                tool_texts.append(self.embedding_service._tool_to_text(tool))

            if not tool_texts:
                return

            # Batch generate all embeddings in one call
            logger.debug(f"Generating embeddings for {len(tool_texts)} tools in batch...")
            embeddings = await self.embedding_service.embed(tool_texts)

            # Store all embeddings in database
            conn = sqlite3.connect(str(self.db_path))
            for tool, tool_name, embedding in zip(tools, tool_names, embeddings):
                if embedding is None or len(embedding) == 0:
                    continue

                conn.execute("""
                    INSERT OR REPLACE INTO tool_embeddings
                    (tool_name, tool_data, embedding, backend, last_accessed, access_count)
                    VALUES (?, ?, ?, ?, ?, COALESCE(
                        (SELECT access_count FROM tool_embeddings WHERE tool_name = ?),
                        0
                    ))
                """, (
                    tool_name,
                    json.dumps(tool),
                    embedding.tobytes(),
                    backend,
                    datetime.utcnow().isoformat(),
                    tool_name
                ))

            conn.commit()
            conn.close()

            logger.info(f"Batch indexed {len(tool_names)} tools from {backend or 'unknown'}")

        except Exception as e:
            logger.error(f"Failed to batch index tools: {e}")
            # Fallback to individual indexing if batch fails
            tasks = [self.index_tool(tool, backend) for tool in tools]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def search_tools(
        self,
        query: str,
        limit: int = 10,
        backend_filter: Optional[str] = None,
        min_similarity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for tools.

        Args:
            query: Natural language query describing desired tool
            limit: Maximum number of results
            backend_filter: Only search tools from this backend
            min_similarity: Minimum cosine similarity threshold

        Returns:
            List of tool dicts with similarity scores
        """
        # Generate query embedding
        query_embedding = await self.embedding_service.embed([query])
        query_embedding = query_embedding[0]

        # Search database
        conn = sqlite3.connect(str(self.db_path))

        if backend_filter:
            cursor = conn.execute("""
                SELECT tool_name, tool_data, embedding, backend, access_count
                FROM tool_embeddings
                WHERE backend = ?
            """, (backend_filter,))
        else:
            cursor = conn.execute("""
                SELECT tool_name, tool_data, embedding, backend, access_count
                FROM tool_embeddings
            """)

        results = []
        for row in cursor:
            tool_name, tool_data_json, embedding_bytes, backend, access_count = row

            # Reconstruct embedding
            embedding = np.frombuffer(embedding_bytes, dtype=np.float64)

            # Calculate similarity
            similarity = cosine_similarity(query_embedding, embedding)

            if similarity >= min_similarity:
                tool_data = json.loads(tool_data_json)
                results.append({
                    "tool": tool_data,
                    "similarity": float(similarity),
                    "backend": backend,
                    "access_count": access_count
                })

        conn.close()

        # Sort by similarity and access count
        results.sort(key=lambda x: (x["similarity"], x["access_count"]), reverse=True)

        return results[:limit]

    async def get_relevant_tools(
        self,
        query: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get most relevant tools for current context.

        If query provided, uses semantic search.
        Otherwise, returns most frequently accessed tools.
        """
        if query:
            results = await self.search_tools(query, limit=limit)
            return [r["tool"] for r in results]
        else:
            # Return most accessed tools
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.execute("""
                SELECT tool_data
                FROM tool_embeddings
                ORDER BY access_count DESC, last_accessed DESC
                LIMIT ?
            """, (limit,))

            tools = [json.loads(row[0]) for row in cursor]
            conn.close()

            return tools

    def load_into_context(self, tool_name: str, tool_data: Dict[str, Any]):
        """Load a tool into the active MCP context"""
        # Check if we need to evict first
        if self.context.should_evict():
            evict_name = self.context.get_eviction_candidate()
            if evict_name:
                self.evict_from_context(evict_name)

        # Load tool
        self.context.active_tools[tool_name] = ToolEmbedding(
            tool_name=tool_name,
            tool_data=tool_data,
            embedding=np.array([]),  # Don't need embedding in context
            last_accessed=datetime.utcnow(),
            access_count=1
        )

        self.context.access_scores[tool_name] = 1.0

    def evict_from_context(self, tool_name: str):
        """Evict a tool from the active context"""
        if tool_name in self.context.active_tools:
            del self.context.active_tools[tool_name]
        if tool_name in self.context.access_scores:
            del self.context.access_scores[tool_name]

        logger.debug(f"Evicted tool from context: {tool_name}")

    def mark_accessed(self, tool_name: str):
        """Mark a tool as accessed (for LRU tracking)"""
        if tool_name in self.context.active_tools:
            self.context.active_tools[tool_name].last_accessed = datetime.utcnow()
            self.context.active_tools[tool_name].access_count += 1
            self.context.access_scores[tool_name] = self.context.access_scores.get(tool_name, 0) + 1.0

        # Update database
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            UPDATE tool_embeddings
            SET last_accessed = ?, access_count = access_count + 1
            WHERE tool_name = ?
        """, (datetime.utcnow().isoformat(), tool_name))
        conn.commit()
        conn.close()

    def get_active_tools(self) -> List[Dict[str, Any]]:
        """Get currently active tools in context"""
        return [
            tool_emb.tool_data
            for tool_emb in self.context.active_tools.values()
        ]

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the tool store"""
        conn = sqlite3.connect(str(self.db_path))

        # Total tools
        total = conn.execute("SELECT COUNT(*) FROM tool_embeddings").fetchone()[0]

        # Backends
        backends = conn.execute("""
            SELECT backend, COUNT(*) as count
            FROM tool_embeddings
            GROUP BY backend
        """).fetchall()

        # Most accessed
        top_tools = conn.execute("""
            SELECT tool_name, access_count
            FROM tool_embeddings
            ORDER BY access_count DESC
            LIMIT 10
        """).fetchall()

        conn.close()

        return {
            "total_tools": total,
            "active_in_context": len(self.context.active_tools),
            "max_context_size": self.context.max_tools,
            "backends": {backend: count for backend, count in backends},
            "top_accessed": {name: count for name, count in top_tools}
        }


# Global semantic tool store
_tool_store: Optional[SemanticToolStore] = None


def get_tool_store(max_context_tools: int = 50) -> SemanticToolStore:
    """Get or create the global semantic tool store"""
    global _tool_store

    if _tool_store is None:
        _tool_store = SemanticToolStore(max_context_tools=max_context_tools)

    return _tool_store
