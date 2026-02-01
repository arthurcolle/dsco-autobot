"""
Embedding-based Tool Index - Semantic search over tool capabilities.

Provides:
- Tool embedding generation (name, description, schema)
- Approximate nearest neighbor search
- Capability clustering
- Query expansion and reranking
"""

from __future__ import annotations
import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Callable
from abc import ABC, abstractmethod
import asyncio


@dataclass
class ToolEmbedding:
    """Embedded representation of a tool."""
    tool_id: str
    tool_name: str
    description: str
    vector: List[float]
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def dimension(self) -> int:
        return len(self.vector)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_id": self.tool_id,
            "tool_name": self.tool_name,
            "description": self.description,
            "vector": self.vector,
            "metadata": self.metadata,
        }


class EmbeddingProvider(ABC):
    """Abstract embedding provider."""

    @abstractmethod
    async def embed(self, text: str) -> List[float]:
        """Generate embedding for text."""
        pass

    @abstractmethod
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        pass

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Embedding dimension."""
        pass


class LocalEmbeddingProvider(EmbeddingProvider):
    """
    Local embedding using simple TF-IDF + dimensionality reduction.

    No external dependencies - works offline.
    """

    def __init__(self, dimension: int = 256, vocabulary_size: int = 10000):
        self._dimension = dimension
        self._vocabulary_size = vocabulary_size
        self._vocab: Dict[str, int] = {}
        self._idf: Dict[str, float] = {}
        self._projection: Optional[List[List[float]]] = None

    @property
    def dimension(self) -> int:
        return self._dimension

    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenization."""
        import re
        text = text.lower()
        tokens = re.findall(r'\b[a-z][a-z0-9_]*\b', text)
        return tokens

    def _hash_project(self, sparse_vec: Dict[int, float]) -> List[float]:
        """Project sparse vector to dense using random projection."""
        result = [0.0] * self._dimension

        for idx, value in sparse_vec.items():
            # Deterministic hash-based projection
            for d in range(self._dimension):
                h = hashlib.md5(f"{idx}:{d}".encode()).digest()
                sign = 1 if h[0] % 2 == 0 else -1
                result[d] += sign * value / math.sqrt(len(sparse_vec))

        # Normalize
        norm = math.sqrt(sum(x * x for x in result))
        if norm > 0:
            result = [x / norm for x in result]

        return result

    async def embed(self, text: str) -> List[float]:
        """Generate embedding using hash-based projection."""
        tokens = self._tokenize(text)
        if not tokens:
            return [0.0] * self._dimension

        # Count term frequencies
        tf: Dict[str, int] = {}
        for token in tokens:
            tf[token] = tf.get(token, 0) + 1

        # Create sparse vector
        sparse: Dict[int, float] = {}
        for token, count in tf.items():
            # Hash token to vocabulary index
            idx = int(hashlib.md5(token.encode()).hexdigest(), 16) % self._vocabulary_size
            tfidf = count * self._idf.get(token, 1.0)
            sparse[idx] = sparse.get(idx, 0) + tfidf

        return self._hash_project(sparse)

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed multiple texts."""
        # Update IDF estimates
        doc_freq: Dict[str, int] = {}
        for text in texts:
            seen = set(self._tokenize(text))
            for token in seen:
                doc_freq[token] = doc_freq.get(token, 0) + 1

        n_docs = len(texts)
        for token, df in doc_freq.items():
            self._idf[token] = math.log((n_docs + 1) / (df + 1)) + 1

        return [await self.embed(text) for text in texts]


class JinaEmbeddingProvider(EmbeddingProvider):
    """Jina AI embedding provider."""

    def __init__(self, api_key: str, model: str = "jina-embeddings-v3"):
        self.api_key = api_key
        self.model = model
        self._dimension = 1024

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed(self, text: str) -> List[float]:
        results = await self.embed_batch([text])
        return results[0]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        import httpx

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.jina.ai/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "input": texts,
                    "normalized": True,
                }
            )
            response.raise_for_status()
            data = response.json()
            return [item["embedding"] for item in data["data"]]


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """OpenAI embedding provider."""

    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.api_key = api_key
        self.model = model
        self._dimension = 1536 if "3-small" in model else 3072

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed(self, text: str) -> List[float]:
        results = await self.embed_batch([text])
        return results[0]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        import httpx

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "input": texts,
                }
            )
            response.raise_for_status()
            data = response.json()
            return [item["embedding"] for item in data["data"]]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def euclidean_distance(a: List[float], b: List[float]) -> float:
    """Compute Euclidean distance between two vectors."""
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


@dataclass
class SearchResult:
    """Result from semantic search."""
    tool_id: str
    tool_name: str
    description: str
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class ToolIndex:
    """
    Semantic index over tools for similarity search.

    Features:
    - Multiple embedding strategies (local, Jina, OpenAI)
    - Approximate nearest neighbor with locality-sensitive hashing
    - Query expansion using tool clusters
    - Hybrid search (semantic + keyword)
    """

    def __init__(
        self,
        provider: Optional[EmbeddingProvider] = None,
        n_hash_tables: int = 10,
        n_hash_bits: int = 8,
    ):
        self.provider = provider or LocalEmbeddingProvider()
        self.embeddings: Dict[str, ToolEmbedding] = {}

        # LSH index for approximate NN
        self.n_hash_tables = n_hash_tables
        self.n_hash_bits = n_hash_bits
        self._hash_tables: List[Dict[int, List[str]]] = [
            {} for _ in range(n_hash_tables)
        ]
        self._random_planes: Optional[List[List[List[float]]]] = None

    def _init_lsh(self, dimension: int):
        """Initialize LSH random hyperplanes."""
        import random
        random.seed(42)  # Reproducible

        self._random_planes = []
        for _ in range(self.n_hash_tables):
            table_planes = []
            for _ in range(self.n_hash_bits):
                plane = [random.gauss(0, 1) for _ in range(dimension)]
                norm = math.sqrt(sum(x * x for x in plane))
                plane = [x / norm for x in plane]
                table_planes.append(plane)
            self._random_planes.append(table_planes)

    def _lsh_hash(self, vector: List[float], table_idx: int) -> int:
        """Compute LSH hash for a vector."""
        if self._random_planes is None:
            self._init_lsh(len(vector))

        planes = self._random_planes[table_idx]
        hash_val = 0
        for i, plane in enumerate(planes):
            dot = sum(v * p for v, p in zip(vector, plane))
            if dot >= 0:
                hash_val |= (1 << i)
        return hash_val

    def _index_embedding(self, emb: ToolEmbedding):
        """Add embedding to LSH index."""
        for table_idx in range(self.n_hash_tables):
            h = self._lsh_hash(emb.vector, table_idx)
            if h not in self._hash_tables[table_idx]:
                self._hash_tables[table_idx][h] = []
            self._hash_tables[table_idx][h].append(emb.tool_id)

    async def add_tool(
        self,
        tool_id: str,
        tool_name: str,
        description: str,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Add a tool to the index."""
        # Build rich text representation
        text_parts = [
            f"Tool: {tool_name}",
            f"Description: {description}",
        ]

        if input_schema:
            input_fields = input_schema.get("properties", {})
            if input_fields:
                text_parts.append(
                    f"Inputs: {', '.join(input_fields.keys())}"
                )

        if output_schema:
            output_fields = output_schema.get("properties", {})
            if output_fields:
                text_parts.append(
                    f"Outputs: {', '.join(output_fields.keys())}"
                )

        text = ". ".join(text_parts)

        # Generate embedding
        vector = await self.provider.embed(text)

        emb = ToolEmbedding(
            tool_id=tool_id,
            tool_name=tool_name,
            description=description,
            vector=vector,
            metadata=metadata or {},
        )

        self.embeddings[tool_id] = emb
        self._index_embedding(emb)

    async def add_tools_batch(
        self,
        tools: List[Dict[str, Any]],
    ):
        """Add multiple tools efficiently."""
        texts = []
        for tool in tools:
            text_parts = [
                f"Tool: {tool['name']}",
                f"Description: {tool.get('description', '')}",
            ]
            if tool.get("input_schema"):
                input_fields = tool["input_schema"].get("properties", {})
                if input_fields:
                    text_parts.append(f"Inputs: {', '.join(input_fields.keys())}")
            texts.append(". ".join(text_parts))

        vectors = await self.provider.embed_batch(texts)

        for tool, vector in zip(tools, vectors):
            emb = ToolEmbedding(
                tool_id=tool["id"],
                tool_name=tool["name"],
                description=tool.get("description", ""),
                vector=vector,
                metadata=tool.get("metadata", {}),
            )
            self.embeddings[tool["id"]] = emb
            self._index_embedding(emb)

    async def search(
        self,
        query: str,
        k: int = 10,
        threshold: float = 0.0,
        filter_fn: Optional[Callable[[ToolEmbedding], bool]] = None,
    ) -> List[SearchResult]:
        """
        Search for tools matching the query.

        Uses LSH for candidate retrieval, then exact ranking.
        """
        query_vector = await self.provider.embed(query)

        # Collect candidates from LSH buckets
        candidates: set = set()
        for table_idx in range(self.n_hash_tables):
            h = self._lsh_hash(query_vector, table_idx)
            bucket = self._hash_tables[table_idx].get(h, [])
            candidates.update(bucket)

        # If LSH gives too few results, fall back to full scan
        if len(candidates) < k * 2:
            candidates = set(self.embeddings.keys())

        # Score candidates
        results = []
        for tool_id in candidates:
            emb = self.embeddings[tool_id]

            if filter_fn and not filter_fn(emb):
                continue

            score = cosine_similarity(query_vector, emb.vector)

            if score >= threshold:
                results.append(SearchResult(
                    tool_id=emb.tool_id,
                    tool_name=emb.tool_name,
                    description=emb.description,
                    score=score,
                    metadata=emb.metadata,
                ))

        # Sort by score
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:k]

    async def search_by_capability(
        self,
        capability: str,
        k: int = 10,
    ) -> List[SearchResult]:
        """Search for tools that provide a specific capability."""
        # Expand capability query
        query = f"Tool that can {capability}. Tool for {capability}."
        return await self.search(query, k)

    async def search_by_input_output(
        self,
        input_type: Optional[str] = None,
        output_type: Optional[str] = None,
        k: int = 10,
    ) -> List[SearchResult]:
        """Search for tools by input/output types."""
        parts = []
        if input_type:
            parts.append(f"Takes {input_type} as input")
        if output_type:
            parts.append(f"Produces {output_type} as output")

        if not parts:
            return []

        query = ". ".join(parts)
        return await self.search(query, k)

    def get_similar_tools(
        self,
        tool_id: str,
        k: int = 5,
    ) -> List[Tuple[str, float]]:
        """Find tools similar to a given tool."""
        if tool_id not in self.embeddings:
            return []

        source = self.embeddings[tool_id]
        similarities = []

        for other_id, other in self.embeddings.items():
            if other_id == tool_id:
                continue
            sim = cosine_similarity(source.vector, other.vector)
            similarities.append((other_id, sim))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:k]

    def cluster_tools(self, n_clusters: int = 10) -> Dict[int, List[str]]:
        """Cluster tools by semantic similarity."""
        if len(self.embeddings) < n_clusters:
            return {0: list(self.embeddings.keys())}

        # Simple k-means clustering
        vectors = [e.vector for e in self.embeddings.values()]
        tool_ids = list(self.embeddings.keys())

        # Initialize centroids
        import random
        random.seed(42)
        centroid_indices = random.sample(range(len(vectors)), n_clusters)
        centroids = [vectors[i][:] for i in centroid_indices]

        # Iterate
        for _ in range(20):
            # Assign points to clusters
            assignments = []
            for vec in vectors:
                dists = [euclidean_distance(vec, c) for c in centroids]
                assignments.append(dists.index(min(dists)))

            # Update centroids
            new_centroids = []
            for cluster_id in range(n_clusters):
                cluster_vecs = [
                    vectors[i] for i, a in enumerate(assignments) if a == cluster_id
                ]
                if cluster_vecs:
                    dim = len(cluster_vecs[0])
                    centroid = [
                        sum(v[d] for v in cluster_vecs) / len(cluster_vecs)
                        for d in range(dim)
                    ]
                    new_centroids.append(centroid)
                else:
                    new_centroids.append(centroids[cluster_id])

            centroids = new_centroids

        # Build result
        clusters: Dict[int, List[str]] = {i: [] for i in range(n_clusters)}
        for tool_id, vec in zip(tool_ids, vectors):
            dists = [euclidean_distance(vec, c) for c in centroids]
            cluster_id = dists.index(min(dists))
            clusters[cluster_id].append(tool_id)

        return clusters

    def save(self, path: str):
        """Save index to file."""
        data = {
            "embeddings": {k: v.to_dict() for k, v in self.embeddings.items()},
            "n_hash_tables": self.n_hash_tables,
            "n_hash_bits": self.n_hash_bits,
        }
        with open(path, "w") as f:
            json.dump(data, f)

    @classmethod
    def load(cls, path: str, provider: Optional[EmbeddingProvider] = None) -> ToolIndex:
        """Load index from file."""
        with open(path) as f:
            data = json.load(f)

        index = cls(
            provider=provider,
            n_hash_tables=data["n_hash_tables"],
            n_hash_bits=data["n_hash_bits"],
        )

        for emb_data in data["embeddings"].values():
            emb = ToolEmbedding(
                tool_id=emb_data["tool_id"],
                tool_name=emb_data["tool_name"],
                description=emb_data["description"],
                vector=emb_data["vector"],
                metadata=emb_data.get("metadata", {}),
            )
            index.embeddings[emb.tool_id] = emb
            index._index_embedding(emb)

        return index
