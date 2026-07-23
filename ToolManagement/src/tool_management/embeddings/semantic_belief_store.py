"""
Semantic Belief Store - SQLite-backed belief storage with Jina embeddings.

Provides persistent storage for beliefs/facts with semantic search capabilities.
Survives Modal container restarts through SQLite persistence.
"""
from __future__ import annotations

import os
import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

from .openrouter_embeddings import (
    get_embedding_service,
    get_resilient_embedding_service,
    cosine_similarity,
)

logger = logging.getLogger(__name__)

# Use DATA_DIR from environment (set to /data on Modal volume)
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = Path("/tmp/semantic_belief_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Belief:
    """A semantic memory belief/fact with embedding support."""
    id: str
    subject: str
    predicate: str
    object: str
    confidence: float = 1.0
    source: Optional[str] = None
    created_at: Optional[str] = None
    access_count: int = 0
    embedding: Optional[np.ndarray] = None

    def to_text(self) -> str:
        """Convert belief to text for embedding."""
        return f"{self.subject} {self.predicate} {self.object}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding embedding)."""
        return {
            "id": self.id,
            "subject": self.subject,
            "predicate": self.predicate,
            "object": self.object,
            "confidence": self.confidence,
            "source": self.source,
            "created_at": self.created_at,
            "access_count": self.access_count,
        }


class SemanticBeliefStore:
    """
    SQLite-backed belief store with Jina embeddings.

    Features:
    - Persistent storage (survives container restarts)
    - Semantic search using Jina v3 embeddings (1024 dims)
    - Backward-compatible exact-match queries
    - Batch embedding generation for efficiency
    """

    def __init__(self, db_path: Optional[str] = None, embedding_service: Any = None):
        if db_path is None:
            db_path = str(DATA_DIR / "belief_embeddings.db")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"SemanticBeliefStore using database at {self.db_path}")

        # Resilient service: uses Jina when the key works, otherwise falls
        # back to deterministic local hash embeddings (sticky per-process).
        self.embedding_service = embedding_service or get_resilient_embedding_service()
        self._init_db()

    def _active_embedder_tag(self) -> str:
        """Tag naming the embedder the next embed() call will use."""
        return getattr(
            self.embedding_service, "active_name",
            getattr(self.embedding_service, "name", "unknown"),
        )

    async def _embed_tagged(self, texts: List[str]) -> Tuple[np.ndarray, str]:
        """Embed texts and return (vectors, embedder_tag actually used)."""
        embed_tagged = getattr(self.embedding_service, "embed_tagged", None)
        if embed_tagged is not None:
            return await embed_tagged(texts)
        vectors = await self.embedding_service.embed(texts)
        return vectors, self._active_embedder_tag()

    def _init_db(self):
        """Initialize the belief embeddings database."""
        conn = sqlite3.connect(str(self.db_path))

        conn.execute("""
            CREATE TABLE IF NOT EXISTS beliefs (
                id TEXT PRIMARY KEY,
                subject TEXT NOT NULL,
                predicate TEXT NOT NULL,
                object TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                source TEXT,
                embedding BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 0
            )
        """)

        # Indexes for exact-match queries (backward compatibility)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subject ON beliefs(subject)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_predicate ON beliefs(predicate)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_object ON beliefs(object)")

        # Index for access patterns
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_access
            ON beliefs(access_count DESC, created_at DESC)
        """)

        # Migration: tag each stored vector with the embedder that produced it
        # so vectors from different embedding spaces are never compared.
        columns = {row[1] for row in conn.execute("PRAGMA table_info(beliefs)")}
        if "embedder" not in columns:
            conn.execute("ALTER TABLE beliefs ADD COLUMN embedder TEXT")

        conn.commit()
        conn.close()

    def _generate_belief_id(self, subject: str, predicate: str, obj: str) -> str:
        """Generate deterministic ID from triple."""
        triple_key = f"{subject}:{predicate}:{obj}"
        return hashlib.sha256(triple_key.encode()).hexdigest()[:12]

    async def store(
        self,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float = 1.0,
        source: Optional[str] = None,
    ) -> str:
        """
        Store or update a belief with its embedding.

        Args:
            subject: Subject of the belief
            predicate: Relationship/predicate
            obj: Object of the belief
            confidence: Confidence level (0-1)
            source: Source of the belief

        Returns:
            belief_id: The ID of the stored belief
        """
        belief_id = self._generate_belief_id(subject, predicate, obj)
        belief_text = f"{subject} {predicate} {obj}"

        try:
            # Generate embedding (tagged with the embedder that produced it)
            embeddings, embedder_tag = await self._embed_tagged([belief_text])
            embedding = embeddings[0] if len(embeddings) > 0 else None
            embedding_bytes = embedding.tobytes() if embedding is not None else None

            conn = sqlite3.connect(str(self.db_path))

            # Check if belief exists
            existing = conn.execute(
                "SELECT confidence, access_count FROM beliefs WHERE id = ?",
                (belief_id,)
            ).fetchone()

            if existing:
                # Update existing belief (weighted average confidence)
                old_confidence, _ = existing
                new_confidence = (old_confidence + confidence) / 2
                if embedding_bytes is not None:
                    conn.execute("""
                        UPDATE beliefs
                        SET confidence = ?, source = COALESCE(?, source),
                            embedding = ?, embedder = ?, access_count = access_count + 1
                        WHERE id = ?
                    """, (new_confidence, source, embedding_bytes, embedder_tag, belief_id))
                else:
                    conn.execute("""
                        UPDATE beliefs
                        SET confidence = ?, source = COALESCE(?, source),
                            access_count = access_count + 1
                        WHERE id = ?
                    """, (new_confidence, source, belief_id))
            else:
                # Insert new belief
                conn.execute("""
                    INSERT INTO beliefs (id, subject, predicate, object, confidence, source, embedding, embedder, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (belief_id, subject, predicate, obj, confidence, source, embedding_bytes,
                      embedder_tag if embedding_bytes is not None else None,
                      datetime.now(timezone.utc).isoformat()))

            conn.commit()
            conn.close()

            logger.debug(f"Stored belief: {subject} {predicate} {obj}")
            return belief_id

        except Exception as e:
            logger.error(f"Failed to store belief: {e}")
            # Fallback: store without embedding
            return self._store_without_embedding(belief_id, subject, predicate, obj, confidence, source)

    def _store_without_embedding(
        self,
        belief_id: str,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float,
        source: Optional[str],
    ) -> str:
        """Store belief without embedding (fallback)."""
        conn = sqlite3.connect(str(self.db_path))

        conn.execute("""
            INSERT OR REPLACE INTO beliefs (id, subject, predicate, object, confidence, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (belief_id, subject, predicate, obj, confidence, source, datetime.now(timezone.utc).isoformat()))

        conn.commit()
        conn.close()
        return belief_id

    async def store_batch(
        self,
        beliefs: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Batch store multiple beliefs with embeddings.

        Args:
            beliefs: List of dicts with subject, predicate, object, confidence, source

        Returns:
            List of belief IDs
        """
        if not beliefs:
            return []

        try:
            # Generate all belief texts
            belief_texts = []
            belief_ids = []
            for b in beliefs:
                belief_id = self._generate_belief_id(b["subject"], b["predicate"], b["object"])
                belief_ids.append(belief_id)
                belief_texts.append(f"{b['subject']} {b['predicate']} {b['object']}")

            # Batch generate embeddings
            embeddings, embedder_tag = await self._embed_tagged(belief_texts)

            # Store all beliefs
            conn = sqlite3.connect(str(self.db_path))

            for belief, belief_id, embedding in zip(beliefs, belief_ids, embeddings):
                embedding_bytes = embedding.tobytes() if embedding is not None else None

                conn.execute("""
                    INSERT OR REPLACE INTO beliefs
                    (id, subject, predicate, object, confidence, source, embedding, embedder, created_at, access_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(
                        (SELECT access_count FROM beliefs WHERE id = ?),
                        0
                    ))
                """, (
                    belief_id,
                    belief["subject"],
                    belief["predicate"],
                    belief["object"],
                    belief.get("confidence", 1.0),
                    belief.get("source"),
                    embedding_bytes,
                    embedder_tag if embedding_bytes is not None else None,
                    datetime.now(timezone.utc).isoformat(),
                    belief_id
                ))

            conn.commit()
            conn.close()

            logger.info(f"Batch stored {len(beliefs)} beliefs")
            return belief_ids

        except Exception as e:
            logger.error(f"Failed to batch store beliefs: {e}")
            # Fallback to individual storage
            ids = []
            for b in beliefs:
                bid = await self.store(
                    b["subject"], b["predicate"], b["object"],
                    b.get("confidence", 1.0), b.get("source")
                )
                ids.append(bid)
            return ids

    def query(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        obj: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 100,
    ) -> List[Belief]:
        """
        Query beliefs by exact match (backward compatible API).

        Args:
            subject: Subject to match (optional)
            predicate: Predicate to match (optional)
            obj: Object to match (optional)
            min_confidence: Minimum confidence threshold
            limit: Maximum results

        Returns:
            List of matching Belief objects
        """
        conn = sqlite3.connect(str(self.db_path))

        # Build query dynamically
        conditions = ["confidence >= ?"]
        params: List[Any] = [min_confidence]

        if subject:
            conditions.append("subject = ?")
            params.append(subject)
        if predicate:
            conditions.append("predicate = ?")
            params.append(predicate)
        if obj:
            conditions.append("object = ?")
            params.append(obj)

        query = f"""
            SELECT id, subject, predicate, object, confidence, source, created_at, access_count
            FROM beliefs
            WHERE {' AND '.join(conditions)}
            ORDER BY confidence DESC, access_count DESC
            LIMIT ?
        """
        params.append(limit)

        cursor = conn.execute(query, params)

        beliefs = []
        for row in cursor:
            beliefs.append(Belief(
                id=row[0],
                subject=row[1],
                predicate=row[2],
                object=row[3],
                confidence=row[4],
                source=row[5],
                created_at=row[6],
                access_count=row[7],
            ))

        conn.close()
        return beliefs

    async def semantic_search(
        self,
        query: str,
        limit: int = 10,
        min_similarity: float = 0.3,
    ) -> List[Tuple[Belief, float]]:
        """
        Search beliefs by semantic similarity.

        Args:
            query: Natural language query
            limit: Maximum results
            min_similarity: Minimum cosine similarity threshold

        Returns:
            List of (Belief, similarity_score) tuples
        """
        # Generate query embedding (tagged with the embedder actually used)
        query_embeddings, active_tag = await self._embed_tagged([query])
        query_embedding = query_embeddings[0]

        # Re-embed any beliefs whose stored vectors came from a different
        # embedder (or are missing) so we never compare across vector spaces.
        await self._reembed_stale(active_tag)

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.execute("""
            SELECT id, subject, predicate, object, confidence, source, embedding, created_at, access_count
            FROM beliefs
            WHERE embedding IS NOT NULL AND embedder = ?
        """, (active_tag,))

        results = []
        for row in cursor:
            belief_id, subject, predicate, obj, confidence, source, embedding_bytes, created_at, access_count = row

            if embedding_bytes is None:
                continue

            # Reconstruct embedding
            embedding = np.frombuffer(embedding_bytes, dtype=np.float64)
            if embedding.shape != query_embedding.shape:
                continue

            # Calculate similarity
            similarity = cosine_similarity(query_embedding, embedding)

            if similarity >= min_similarity:
                belief = Belief(
                    id=belief_id,
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                    confidence=confidence,
                    source=source,
                    created_at=created_at,
                    access_count=access_count,
                )
                results.append((belief, float(similarity)))

        conn.close()

        # Sort by similarity (weighted by confidence)
        results.sort(key=lambda x: x[1] * (0.5 + 0.5 * x[0].confidence), reverse=True)
        return results[:limit]

    async def find_related(
        self,
        entity: str,
        limit: int = 20,
    ) -> Dict[str, List[Tuple[Belief, float]]]:
        """
        Find beliefs semantically related to an entity.

        Args:
            entity: Entity to find relations for
            limit: Maximum results per category

        Returns:
            Dict with 'semantic' (similar beliefs) and 'exact' (exact matches)
        """
        results = {
            "semantic": [],
            "exact": {
                "as_subject": [],
                "as_object": [],
            }
        }

        # Exact matches
        as_subject = self.query(subject=entity, limit=limit)
        as_object = self.query(obj=entity, limit=limit)
        results["exact"]["as_subject"] = [b.to_dict() for b in as_subject]
        results["exact"]["as_object"] = [b.to_dict() for b in as_object]

        # Semantic search
        semantic_results = await self.semantic_search(entity, limit=limit)
        results["semantic"] = [
            {"belief": b.to_dict(), "similarity": s}
            for b, s in semantic_results
        ]

        return results

    async def _reembed_stale(self, active_tag: str, limit: int = 512) -> int:
        """
        Re-embed beliefs whose vectors are missing or were produced by a
        different embedder than the active one (re-embed on demand).
        """
        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute("""
            SELECT id, subject, predicate, object
            FROM beliefs
            WHERE embedding IS NULL OR embedder IS NULL OR embedder != ?
            LIMIT ?
        """, (active_tag, limit)).fetchall()
        conn.close()

        if not rows:
            return 0

        texts = [f"{s} {p} {o}" for _, s, p, o in rows]
        try:
            embeddings, actual_tag = await self._embed_tagged(texts)
        except Exception as e:
            logger.warning(f"Re-embedding stale beliefs failed: {e}")
            return 0

        conn = sqlite3.connect(str(self.db_path))
        updated = 0
        for (belief_id, _, _, _), embedding in zip(rows, embeddings):
            if embedding is not None:
                conn.execute(
                    "UPDATE beliefs SET embedding = ?, embedder = ? WHERE id = ?",
                    (embedding.tobytes(), actual_tag, belief_id),
                )
                updated += 1
        conn.commit()
        conn.close()

        if updated:
            logger.info(f"Re-embedded {updated} beliefs with embedder '{actual_tag}'")
        return updated

    def mark_accessed(self, belief_id: str):
        """Mark a belief as accessed (for tracking)."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            UPDATE beliefs
            SET access_count = access_count + 1
            WHERE id = ?
        """, (belief_id,))
        conn.commit()
        conn.close()

    def invalidate(self, belief_id: str):
        """Remove a belief from the store."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("DELETE FROM beliefs WHERE id = ?", (belief_id,))
        conn.commit()
        conn.close()

    def get_beliefs_without_embeddings(self, limit: int = 100) -> List[Belief]:
        """Get beliefs that don't have embeddings yet."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.execute("""
            SELECT id, subject, predicate, object, confidence, source, created_at, access_count
            FROM beliefs
            WHERE embedding IS NULL
            LIMIT ?
        """, (limit,))

        beliefs = []
        for row in cursor:
            beliefs.append(Belief(
                id=row[0],
                subject=row[1],
                predicate=row[2],
                object=row[3],
                confidence=row[4],
                source=row[5],
                created_at=row[6],
                access_count=row[7],
            ))

        conn.close()
        return beliefs

    async def backfill_embeddings(self, batch_size: int = 50) -> int:
        """Backfill embeddings for beliefs missing or from a stale embedder."""
        return await self._reembed_stale(self._active_embedder_tag(), limit=batch_size)

    def stats(self) -> Dict[str, Any]:
        """Get statistics about the belief store."""
        conn = sqlite3.connect(str(self.db_path))

        total = conn.execute("SELECT COUNT(*) FROM beliefs").fetchone()[0]
        with_embeddings = conn.execute(
            "SELECT COUNT(*) FROM beliefs WHERE embedding IS NOT NULL"
        ).fetchone()[0]

        # Unique counts
        unique_subjects = conn.execute("SELECT COUNT(DISTINCT subject) FROM beliefs").fetchone()[0]
        unique_predicates = conn.execute("SELECT COUNT(DISTINCT predicate) FROM beliefs").fetchone()[0]
        unique_objects = conn.execute("SELECT COUNT(DISTINCT object) FROM beliefs").fetchone()[0]

        # Average confidence
        avg_confidence = conn.execute("SELECT AVG(confidence) FROM beliefs").fetchone()[0] or 0.0

        # Most accessed
        top_accessed = conn.execute("""
            SELECT subject, predicate, object, access_count
            FROM beliefs
            ORDER BY access_count DESC
            LIMIT 5
        """).fetchall()

        by_embedder = dict(conn.execute("""
            SELECT COALESCE(embedder, 'untagged'), COUNT(*)
            FROM beliefs
            WHERE embedding IS NOT NULL
            GROUP BY embedder
        """).fetchall())

        conn.close()

        return {
            "total_beliefs": total,
            "with_embeddings": with_embeddings,
            "without_embeddings": total - with_embeddings,
            "active_embedder": self._active_embedder_tag(),
            "embeddings_by_embedder": by_embedder,
            "unique_subjects": unique_subjects,
            "unique_predicates": unique_predicates,
            "unique_objects": unique_objects,
            "avg_confidence": float(avg_confidence),
            "top_accessed": [
                {"triple": f"{s} {p} {o}", "access_count": c}
                for s, p, o, c in top_accessed
            ],
        }


# Global semantic belief store
_belief_store: Optional[SemanticBeliefStore] = None


def get_belief_store() -> SemanticBeliefStore:
    """Get or create the global semantic belief store."""
    global _belief_store

    if _belief_store is None:
        _belief_store = SemanticBeliefStore()

    return _belief_store
