"""
Tool Retriever
Semantic search over tool embeddings using Jina v3 + Reranker
"""

import asyncio
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import OrderedDict
import time
import logging

logger = logging.getLogger(__name__)


@dataclass
class RetrievalResult:
    """Result from tool retrieval"""
    tool_id: str
    score: float
    category: str


class LRUCache:
    """Simple LRU cache for query results"""

    def __init__(self, maxsize: int = 1000):
        self.cache = OrderedDict()
        self.maxsize = maxsize

    def get(self, key) -> Optional[any]:
        if key not in self.cache:
            return None

        # Move to end (most recently used)
        self.cache.move_to_end(key)
        return self.cache[key]

    def set(self, key, value, ttl: int = 300):
        """Set with TTL (not implemented, just for API compat)"""
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.maxsize:
                # Remove least recently used
                self.cache.popitem(last=False)

        self.cache[key] = value


class ToolRetriever:
    """Retrieve relevant tools using semantic search + reranking"""

    def __init__(self, embedder):
        self.embedder = embedder
        self.jina_embed_func = None  # Set by caller
        self.jina_rerank_func = None  # Set by caller
        self.cache = LRUCache(maxsize=1000)

    def set_jina_functions(self, embed_func, rerank_func):
        """Set Jina MCP tool functions"""
        self.jina_embed_func = embed_func
        self.jina_rerank_func = rerank_func

    async def retrieve_for_query(
        self,
        query: str,
        top_k: int = 10,
        use_reranking: bool = True,
        category_filter: Optional[str] = None,
        min_score: float = 0.0
    ) -> List[Dict]:
        """
        Main retrieval pipeline:
        1. Embed query (Jina v3)
        2. Vector search (cosine similarity)
        3. Rerank (Jina reranker)

        Returns list of tool metadata dicts
        """

        # Check cache
        cache_key = (query, top_k, use_reranking, category_filter)
        if cached := self.cache.get(cache_key):
            logger.debug(f"Cache hit for query: {query[:50]}")
            return cached

        start_time = time.time()

        # Step 1: Embed query
        query_embedding = await self._embed_query(query)

        # Step 2: Vector search
        candidates = await self._vector_search(
            query_embedding,
            top_k=min(20, top_k * 2) if use_reranking else top_k,
            category_filter=category_filter,
            min_score=min_score
        )

        logger.info(f"Vector search found {len(candidates)} candidates")

        # Step 3: Rerank using Jina reranker
        if use_reranking and len(candidates) > top_k:
            reranked = await self._rerank_tools(query, candidates, top_k)
            result = reranked
        else:
            result = candidates[:top_k]

        # Convert to full tool metadata
        tools = []
        for res in result:
            schema = self.embedder.get_tool_schema(res.tool_id)
            if schema:
                tools.append({
                    'tool_id': res.tool_id,
                    'tool_name': res.tool_id.split(':', 1)[1],
                    'server_id': res.tool_id.split(':', 1)[0],
                    'category': res.category,
                    'score': res.score,
                    'schema': schema
                })

        elapsed_ms = (time.time() - start_time) * 1000
        logger.info(f"Retrieved {len(tools)} tools in {elapsed_ms:.1f}ms")

        # Cache result
        self.cache.set(cache_key, tools)

        return tools

    async def _embed_query(self, query: str) -> np.ndarray:
        """Embed query using Jina embeddings v3"""

        try:
            result = await self.jina_embed_func({
                "inputs": [query],
                "model": "jina-embeddings-v3",
                "task": "retrieval.query",
                "dimensions": 1024,
                "normalize": True
            })

            embedding = np.array(result['data'][0]['embedding'], dtype=np.float32)
            return embedding

        except Exception as e:
            logger.error(f"Error embedding query: {e}")
            # Fallback: return zero vector
            return np.zeros(1024, dtype=np.float32)

    async def _vector_search(
        self,
        query_embedding: np.ndarray,
        top_k: int,
        category_filter: Optional[str] = None,
        min_score: float = 0.0
    ) -> List[RetrievalResult]:
        """
        Cosine similarity search over all tool embeddings.

        Returns sorted list of RetrievalResult by score (descending)
        """

        all_embeddings = self.embedder.get_all_embeddings()

        scores = []

        for tool_id, tool_embedding in all_embeddings.items():
            # Apply category filter
            if category_filter:
                cursor = self.embedder.conn.execute(
                    "SELECT category FROM tool_embeddings WHERE tool_id = ?",
                    (tool_id,)
                )
                row = cursor.fetchone()
                if not row or row[0] != category_filter:
                    continue

            # Cosine similarity (vectors are already normalized)
            score = float(np.dot(query_embedding, tool_embedding))

            if score >= min_score:
                # Get category
                cursor = self.embedder.conn.execute(
                    "SELECT category FROM tool_embeddings WHERE tool_id = ?",
                    (tool_id,)
                )
                row = cursor.fetchone()
                category = row[0] if row else 'utility'

                scores.append(RetrievalResult(
                    tool_id=tool_id,
                    score=score,
                    category=category
                ))

        # Sort by score descending
        scores.sort(key=lambda x: x.score, reverse=True)

        return scores[:top_k]

    async def _rerank_tools(
        self,
        query: str,
        candidates: List[RetrievalResult],
        top_k: int
    ) -> List[RetrievalResult]:
        """
        Rerank candidates using Jina reranker API.

        Uses: sort_by_relevance MCP tool
        """

        # Get tool descriptions for reranking
        documents = []
        for res in candidates:
            cursor = self.embedder.conn.execute("""
                SELECT tool_name, schema_json
                FROM tool_embeddings
                WHERE tool_id = ?
            """, (res.tool_id,))

            row = cursor.fetchone()
            if row:
                tool_name = row[0]
                schema_json = row[1]

                # Create description for reranking
                import json
                schema = json.loads(schema_json)
                desc = f"{tool_name}: {schema.get('description', '')}"
                documents.append(desc)
            else:
                documents.append(res.tool_id)

        try:
            # Call Jina reranker
            result = await self.jina_rerank_func({
                "query": query,
                "documents": documents,
                "top_n": top_k
            })

            # Map reranked results back to candidates
            reranked = []
            for item in result['results']:
                idx = item['index']
                if idx < len(candidates):
                    # Update score with reranking score
                    res = candidates[idx]
                    res.score = item.get('relevance_score', res.score)
                    reranked.append(res)

            logger.info(f"Reranked {len(candidates)} → {len(reranked)} tools")

            return reranked

        except Exception as e:
            logger.error(f"Error reranking tools: {e}")
            # Fallback: return original candidates
            return candidates[:top_k]

    def get_tool_description(self, tool_id: str) -> str:
        """Get human-readable tool description"""

        cursor = self.embedder.conn.execute("""
            SELECT tool_name, schema_json, category
            FROM tool_embeddings
            WHERE tool_id = ?
        """, (tool_id,))

        row = cursor.fetchone()
        if not row:
            return tool_id

        tool_name = row[0]
        schema_json = row[1]
        category = row[2]

        import json
        schema = json.loads(schema_json)
        desc = schema.get('description', 'No description')

        return f"{tool_name} ({category}): {desc}"


class ToolDependencyGraph:
    """Learn and predict tool usage patterns"""

    def __init__(self, embedder):
        self.embedder = embedder
        self.graph = {}  # tool_id -> {next_tool_id: weight}

    async def load_from_db(self):
        """Build graph from historical usage logs"""

        cursor = self.embedder.conn.execute("""
            SELECT tool_sequence
            FROM tool_usage_log
            ORDER BY created_at DESC
            LIMIT 1000
        """)

        import json
        for row in cursor:
            sequence = json.loads(row[0])
            self._add_sequence(sequence)

        logger.info(f"Loaded graph with {len(self.graph)} nodes")

    def _add_sequence(self, sequence: List[str]):
        """Add a sequence to the graph"""

        for i in range(len(sequence) - 1):
            current = sequence[i]
            next_tool = sequence[i + 1]

            if current not in self.graph:
                self.graph[current] = {}

            if next_tool not in self.graph[current]:
                self.graph[current][next_tool] = 0

            self.graph[current][next_tool] += 1

    def predict_next_tools(self, current_tools: List[str], top_k: int = 5) -> List[str]:
        """Predict likely next tools based on current tools"""

        candidates = {}

        for tool in current_tools:
            if tool in self.graph:
                for next_tool, weight in self.graph[tool].items():
                    if next_tool not in candidates:
                        candidates[next_tool] = 0
                    candidates[next_tool] += weight

        # Sort by weight
        sorted_tools = sorted(
            candidates.items(),
            key=lambda x: x[1],
            reverse=True
        )

        return [tool_id for tool_id, _ in sorted_tools[:top_k]]

    async def add_sequence(self, sequence: List[str]):
        """Add new sequence and persist to DB"""

        self._add_sequence(sequence)

        # Sequences are already logged by ToolEmbedder
        # This just updates the in-memory graph
