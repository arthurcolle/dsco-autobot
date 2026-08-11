"""
Semantic Tool Store - Scale to millions of tools with intelligent retrieval

Uses embeddings and vector search to:
1. Store tool schemas with their embeddings
2. Retrieve relevant tools semantically
3. Manage context window intelligently (load/evict)
4. Enable single MCP client to access millions of tools

Performance: in-memory embedding matrix for vectorized cosine similarity.
"""
from __future__ import annotations

import os
import asyncio
import logging
import threading
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import sqlite3
import json
from pathlib import Path

from .openrouter_embeddings import get_embedding_service

logger = logging.getLogger(__name__)

# Use DATA_DIR from environment (set to /data on Modal volume)
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = Path("/tmp/semantic_tool_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _embedder_space_suffix(embedding_service) -> str:
    """Return a filename-safe suffix namespacing a vector DB by embedder+model.

    Vectors produced by different embedding models occupy incompatible spaces
    and must never be compared, so each embedder gets its own database file.
    The legacy Jina v3 space keeps the historic empty suffix (the existing
    ``tool_embeddings.db``) so current deployments do not silently lose their
    already-indexed corpus.
    """
    import re as _re

    name = str(getattr(embedding_service, "name", "") or "")
    model = str(getattr(getattr(embedding_service, "config", None), "model", "") or "")
    identity = name or model
    if not identity:
        return ""
    if identity == "jina" and (not model or "jina-embeddings-v3" in model):
        # Historic default space -> keep existing DB filename.
        return ""
    slug = _re.sub(r"[^a-z0-9]+", "_", identity.lower()).strip("_")
    return f"_{slug}" if slug else ""


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

    Performance optimizations:
    - In-memory embedding matrix: all embeddings loaded into a single numpy array
    - Vectorized cosine similarity: single np.dot() call instead of per-row
    - Persistent SQLite connection with WAL mode
    - Skip-existing check in batch indexing
    - Tool metadata kept in parallel lists for O(1) lookup by index
    """

    def __init__(self, db_path: Optional[str] = None, max_context_tools: int = 50):
        self.embedding_service = get_embedding_service()
        if db_path is None:
            # Namespace the vector DB by embedder+model: vectors from different
            # embedding models live in incompatible spaces and must never be
            # compared. Derive a stable suffix from the active embedder.
            db_path = str(DATA_DIR / f"tool_embeddings{_embedder_space_suffix(self.embedding_service)}.db")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"SemanticToolStore using database at {self.db_path}")

        self.context = ContextWindow(max_tools=max_context_tools)

        # In-memory embedding matrix (loaded lazily)
        self._matrix: Optional[np.ndarray] = None          # shape (N, dim), float32
        self._matrix_norms: Optional[np.ndarray] = None    # shape (N,), precomputed
        self._tool_names: List[str] = []                    # parallel to matrix rows
        self._tool_data: List[Dict[str, Any]] = []          # parallel to matrix rows
        self._backends: List[Optional[str]] = []            # parallel to matrix rows
        self._access_counts: List[int] = []                 # parallel to matrix rows
        self._name_to_idx: Dict[str, int] = {}              # tool_name -> row index
        self._matrix_dirty = True                           # reload flag

        # Persistent connection
        self._conn: Optional[sqlite3.Connection] = None
        self._conn_lock = threading.Lock()

        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create persistent SQLite connection with WAL mode."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA cache_size=10000")
        return self._conn

    @staticmethod
    def _backend_key(backend: Optional[str]) -> str:
        """Normalize the backend column to a non-null string for the composite PK."""
        return backend or ""

    @staticmethod
    def _row_identity(tool_name: str, backend: Optional[str]) -> str:
        """Stable in-memory identity for a (tool_name, backend) row.

        Distinct backends may expose a tool under the same name; the identity
        must therefore include the backend so they never collide.
        """
        return f"{SemanticToolStore._backend_key(backend)}::{tool_name}"

    @staticmethod
    def _enrich_tool_for_embedding(tool: Dict[str, Any], backend: Optional[str]) -> Dict[str, Any]:
        """Return a copy of ``tool`` guaranteed to carry its provider label.

        The embedder turns provider/backend/source/category/tags into embedded
        text, so the label must be present on the dict handed to it even when
        the caller did not enrich the tool upstream.
        """
        if not backend:
            return tool
        annotations = tool.get("annotations") or {}
        has_label = (
            tool.get("provider")
            or tool.get("backend")
            or tool.get("source")
            or annotations.get("backend")
        )
        if has_label:
            return tool
        enriched = dict(tool)
        merged = dict(annotations)
        merged["backend"] = backend
        enriched["annotations"] = merged
        enriched["backend"] = backend
        return enriched

    def _init_db(self):
        """Initialize the tool embeddings database"""
        with self._conn_lock:
            conn = self._get_conn()

            # Detect the legacy name-only primary key. Two backends that expose
            # a tool with the same name must not overwrite each other, so the
            # identity is the (tool_name, backend) pair. Migrate in place when
            # the old single-column PK is present.
            pk_cols = [
                row[1] for row in conn.execute("PRAGMA table_info(tool_embeddings)").fetchall()
                if row[5] > 0
            ]
            if pk_cols == ["tool_name"]:
                logger.info(
                    "Migrating tool_embeddings to composite (tool_name, backend) primary key"
                )
                conn.execute("ALTER TABLE tool_embeddings RENAME TO tool_embeddings_legacy")
                conn.execute("""
                    CREATE TABLE tool_embeddings (
                        tool_name TEXT NOT NULL,
                        tool_data TEXT NOT NULL,
                        embedding BLOB NOT NULL,
                        backend TEXT NOT NULL DEFAULT '',
                        last_accessed TEXT,
                        access_count INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (tool_name, backend)
                    )
                """)
                conn.execute("""
                    INSERT OR REPLACE INTO tool_embeddings
                        (tool_name, tool_data, embedding, backend, last_accessed, access_count, created_at)
                    SELECT tool_name, tool_data, embedding, COALESCE(backend, ''),
                           last_accessed, access_count, created_at
                    FROM tool_embeddings_legacy
                """)
                conn.execute("DROP TABLE tool_embeddings_legacy")
            else:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS tool_embeddings (
                        tool_name TEXT NOT NULL,
                        tool_data TEXT NOT NULL,
                        embedding BLOB NOT NULL,
                        backend TEXT NOT NULL DEFAULT '',
                        last_accessed TEXT,
                        access_count INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (tool_name, backend)
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

    def _load_matrix(self):
        """Load all embeddings from SQLite into a single numpy matrix.

        This is the critical performance optimization: instead of per-row
        np.frombuffer + cosine_similarity on every search, we load once
        and do a single vectorized np.dot().
        """
        with self._conn_lock:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT tool_name, tool_data, embedding, backend, access_count
                FROM tool_embeddings
            """)
            rows = cursor.fetchall()

        if not rows:
            self._matrix = None
            self._matrix_norms = None
            self._tool_names = []
            self._tool_data = []
            self._backends = []
            self._access_counts = []
            self._name_to_idx = {}
            self._matrix_dirty = False
            return

        names = []
        data = []
        backends = []
        counts = []
        embeddings = []

        for tool_name, tool_data_json, embedding_bytes, backend, access_count in rows:
            emb = np.frombuffer(embedding_bytes, dtype=np.float64).astype(np.float32)
            embeddings.append(emb)
            names.append(tool_name)
            data.append(json.loads(tool_data_json))
            backends.append(backend)
            counts.append(access_count or 0)

        # Stack into matrix and precompute norms
        self._matrix = np.vstack(embeddings)  # (N, dim)
        # Replace any nan/inf in embeddings with 0
        self._matrix = np.nan_to_num(self._matrix, nan=0.0, posinf=0.0, neginf=0.0)
        self._matrix_norms = np.linalg.norm(self._matrix, axis=1)  # (N,)
        # Avoid division by zero
        self._matrix_norms = np.where(self._matrix_norms == 0, 1e-10, self._matrix_norms)
        self._tool_names = names
        self._tool_data = data
        self._backends = backends
        self._access_counts = counts
        # In-memory identity mirrors the (tool_name, backend) composite PK so
        # same-named tools from distinct backends never collide.
        self._name_to_idx = {
            self._row_identity(name, backend): i
            for i, (name, backend) in enumerate(zip(names, backends))
        }
        self._matrix_dirty = False

        logger.info(f"Loaded {len(names)} tool embeddings into in-memory matrix ({self._matrix.shape})")

    def _ensure_matrix(self):
        """Ensure the in-memory matrix is loaded and up to date."""
        if self._matrix_dirty or self._matrix is None:
            self._load_matrix()

    async def index_tool(self, tool: Dict[str, Any], backend: Optional[str] = None, force: bool = True):
        """Index a single tool with its embedding.

        ``force`` re-embeds even when the row already exists, so enriched text
        actually takes effect (INSERT OR REPLACE semantics already overwrite).
        """
        tool_name = tool.get("name")
        if not tool_name:
            return

        tool = self._enrich_tool_for_embedding(tool, backend)
        backend_key = self._backend_key(backend)

        try:
            embedding = await self.embedding_service.embed_tool_schema(tool)

            with self._conn_lock:
                conn = self._get_conn()
                conn.execute("""
                    INSERT OR REPLACE INTO tool_embeddings
                    (tool_name, tool_data, embedding, backend, last_accessed, access_count)
                    VALUES (?, ?, ?, ?, ?, COALESCE(
                        (SELECT access_count FROM tool_embeddings WHERE tool_name = ? AND backend = ?),
                        0
                    ))
                """, (
                    tool_name,
                    json.dumps(tool),
                    embedding.tobytes(),
                    backend_key,
                    datetime.utcnow().isoformat(),
                    tool_name,
                    backend_key
                ))
                conn.commit()

            self._matrix_dirty = True
            logger.debug(f"Indexed tool: {tool_name} (backend={backend_key})")

        except Exception as e:
            logger.error(f"Failed to index tool {tool_name}: {e}")

    async def index_tools_batch(
        self,
        tools: List[Dict[str, Any]],
        backend: Optional[str] = None,
        force: bool = False,
    ):
        """Batch index multiple tools.

        By default this skips tools that already have an embedding for this
        backend. Pass ``force=True`` to re-embed and overwrite existing rows —
        required whenever the embedded text format changes (e.g. injecting the
        provider label), otherwise the stale embeddings are never refreshed.
        Tool identity is the (tool_name, backend) pair, so two backends that
        expose the same tool name do not collide.
        """
        if not tools:
            return

        backend_key = self._backend_key(backend)

        try:
            # Check which tools already exist in the DB for THIS backend
            all_names = [t.get("name") for t in tools if t.get("name")]
            if not all_names:
                return

            existing: Set[str] = set()
            if not force:
                with self._conn_lock:
                    conn = self._get_conn()
                    placeholders = ",".join("?" for _ in all_names)
                    cursor = conn.execute(
                        f"SELECT tool_name FROM tool_embeddings WHERE backend = ? AND tool_name IN ({placeholders})",
                        [backend_key, *all_names],
                    )
                    existing = {row[0] for row in cursor}

            # Filter to tools we actually need to (re)embed
            new_tools = []
            new_names = []
            new_texts = []
            for tool in tools:
                name = tool.get("name")
                if not name or (not force and name in existing):
                    continue
                enriched = self._enrich_tool_for_embedding(tool, backend)
                new_tools.append(enriched)
                new_names.append(name)
                new_texts.append(self.embedding_service._tool_to_text(enriched))

            if not new_texts:
                logger.debug(f"All {len(all_names)} tools from {backend or 'unknown'} already indexed, skipping")
                return

            logger.info(
                f"Embedding {len(new_texts)} tools (force={force}, skipping {len(existing)} existing) from {backend or 'unknown'}"
            )
            embeddings = await self.embedding_service.embed(new_texts)

            with self._conn_lock:
                conn = self._get_conn()
                for tool, tool_name, embedding in zip(new_tools, new_names, embeddings):
                    if embedding is None or len(embedding) == 0:
                        continue

                    conn.execute("""
                        INSERT OR REPLACE INTO tool_embeddings
                        (tool_name, tool_data, embedding, backend, last_accessed, access_count)
                        VALUES (?, ?, ?, ?, ?, COALESCE(
                            (SELECT access_count FROM tool_embeddings WHERE tool_name = ? AND backend = ?),
                            0
                        ))
                    """, (
                        tool_name,
                        json.dumps(tool),
                        embedding.tobytes(),
                        backend_key,
                        datetime.utcnow().isoformat(),
                        tool_name,
                        backend_key,
                    ))

                conn.commit()

            self._matrix_dirty = True
            logger.info(f"Batch indexed {len(new_names)} tools from {backend or 'unknown'}")

        except Exception as e:
            logger.error(f"Failed to batch index tools: {e}")

    async def search_tools(
        self,
        query: str,
        limit: int = 10,
        backend_filter: Optional[str] = None,
        min_similarity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for tools using vectorized cosine similarity.

        Single np.dot() call against the full embedding matrix instead of
        per-row deserialization + similarity computation.
        """
        self._ensure_matrix()

        if self._matrix is None or len(self._tool_names) == 0:
            return []

        # Get query embedding (cached by JinaEmbeddings._embedding_cache)
        query_emb = await self.embedding_service.embed([query])
        q = query_emb[0].astype(np.float32)
        q_norm = np.linalg.norm(q)
        if q_norm == 0:
            return []

        # Vectorized cosine similarity: dot(matrix, q) / (||matrix_rows|| * ||q||)
        if backend_filter and backend_filter in self._backends:
            # Filter to matching backend indices
            mask = np.array([b == backend_filter for b in self._backends], dtype=bool)
            indices = np.where(mask)[0]
            if len(indices) == 0:
                return []
            sub_matrix = self._matrix[indices]
            sub_norms = self._matrix_norms[indices]  # type: ignore[index]
            with np.errstate(divide='ignore', invalid='ignore', over='ignore'):
                similarities = sub_matrix @ q / (sub_norms * q_norm)
            similarities = np.nan_to_num(similarities, nan=0.0, posinf=0.0, neginf=0.0)
            # Map back to global indices
            top_k = min(limit, len(indices))
            top_k = min(limit, len(similarities))
            top_local = np.argpartition(-similarities, top_k - 1)[:top_k]
            top_local = top_local[np.argsort(-similarities[top_local])]
            top_indices = indices[top_local]
            top_sims = similarities[top_local]
        else:
            with np.errstate(divide='ignore', invalid='ignore', over='ignore'):
                similarities = self._matrix @ q / (self._matrix_norms * q_norm)
            similarities = np.nan_to_num(similarities, nan=0.0, posinf=0.0, neginf=0.0)
            top_k = min(limit, len(similarities))
            top_indices = np.argpartition(-similarities, top_k - 1)[:top_k]
            top_indices = top_indices[np.argsort(-similarities[top_indices])]
            top_sims = similarities[top_indices]

        results = []
        for idx, sim in zip(top_indices, top_sims):
            if sim < min_similarity:
                continue
            results.append({
                "tool": self._tool_data[idx],
                "similarity": float(sim),
                "backend": self._backends[idx],
                "access_count": self._access_counts[idx],
            })

        return results

    async def search_tools_with_names(
        self,
        query: str,
        extra_names: List[str],
        limit: int = 10,
        min_similarity: float = 0.0,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Search tools AND resolve specific tool names in a single pass.

        Returns:
            (ranked_results, name_map) where name_map maps tool_name -> result dict
            for each requested name that exists in the store.

        This eliminates the need for separate search_tools() calls per intent default.
        """
        self._ensure_matrix()

        if self._matrix is None or len(self._tool_names) == 0:
            return [], {}

        # Get query embedding
        query_emb = await self.embedding_service.embed([query])
        q = query_emb[0].astype(np.float32)
        q_norm = np.linalg.norm(q)
        if q_norm == 0:
            return [], {}

        # Vectorized cosine similarity
        similarities = self._matrix @ q / (self._matrix_norms * q_norm)

        # Top-k results
        top_k = min(limit, len(similarities))
        top_indices = np.argpartition(-similarities, top_k - 1)[:top_k]
        top_indices = top_indices[np.argsort(-similarities[top_indices])]

        results = []
        for idx in top_indices:
            sim = float(similarities[idx])
            if sim < min_similarity:
                continue
            results.append({
                "tool": self._tool_data[idx],
                "similarity": sim,
                "backend": self._backends[idx],
                "access_count": self._access_counts[idx],
            })

        # Resolve extra names by direct lookup. A bare name may be provided by
        # several backends; map it to its highest-similarity occurrence.
        name_map: Dict[str, Dict[str, Any]] = {}
        for name in extra_names:
            best_idx: Optional[int] = None
            best_sim = float("-inf")
            for idx, (row_name, row_backend) in enumerate(zip(self._tool_names, self._backends)):
                if row_name != name:
                    continue
                sim = float(similarities[idx])
                if best_idx is None or sim > best_sim:
                    best_idx, best_sim = idx, sim
            if best_idx is not None:
                name_map[name] = {
                    "tool": self._tool_data[best_idx],
                    "similarity": best_sim,
                    "backend": self._backends[best_idx],
                    "access_count": self._access_counts[best_idx],
                }

        return results, name_map

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
            with self._conn_lock:
                conn = self._get_conn()
                cursor = conn.execute("""
                    SELECT tool_data
                    FROM tool_embeddings
                    ORDER BY access_count DESC, last_accessed DESC
                    LIMIT ?
                """, (limit,))

                tools = [json.loads(row[0]) for row in cursor]

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
        with self._conn_lock:
            conn = self._get_conn()
            conn.execute("""
                UPDATE tool_embeddings
                SET last_accessed = ?, access_count = access_count + 1
                WHERE tool_name = ?
            """, (datetime.utcnow().isoformat(), tool_name))
            conn.commit()

    def get_active_tools(self) -> List[Dict[str, Any]]:
        """Get currently active tools in context"""
        return [
            tool_emb.tool_data
            for tool_emb in self.context.active_tools.values()
        ]

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the tool store"""
        with self._conn_lock:
            conn = self._get_conn()

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

        return {
            "total_tools": total,
            "active_in_context": len(self.context.active_tools),
            "max_context_size": self.context.max_tools,
            "backends": {backend: count for backend, count in backends},
            "top_accessed": {name: count for name, count in top_tools},
            "matrix_loaded": self._matrix is not None,
            "matrix_shape": list(self._matrix.shape) if self._matrix is not None else None,
        }


# Global semantic tool store
_tool_store: Optional[SemanticToolStore] = None


def get_tool_store(max_context_tools: int = 50) -> SemanticToolStore:
    """Get or create the global semantic tool store"""
    global _tool_store

    if _tool_store is None:
        _tool_store = SemanticToolStore(max_context_tools=max_context_tools)

    return _tool_store
