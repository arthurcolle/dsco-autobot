"""
Jina Embedding Service - Semantic Tool Discovery

Uses Jina AI's embedding API to enable semantic search over tool schemas.
Supports millions of tools through intelligent context management.
"""
from __future__ import annotations

import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np
import httpx

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingConfig:
    """Configuration for Jina embeddings"""
    api_key: str
    model: str = "jina-embeddings-v3"  # Latest Jina model, 1024 dims
    base_url: str = "https://api.jina.ai/v1"
    batch_size: int = 100  # Process up to 100 texts per request
    max_retries: int = 3
    timeout: float = 30.0


class JinaEmbeddings:
    """
    Jina AI embedding service for semantic tool discovery.

    Embeds tool schemas (name, description, input/output schemas)
    for semantic search and retrieval.
    """

    def __init__(self, config: Optional[EmbeddingConfig] = None):
        if config is None:
            api_key = os.getenv("JINA_API_KEY")
            if not api_key:
                raise ValueError("JINA_API_KEY environment variable not set")
            config = EmbeddingConfig(api_key=api_key)

        self.config = config
        self.client = httpx.AsyncClient(timeout=config.timeout)
        self._embedding_cache: Dict[str, np.ndarray] = {}

    async def embed(self, texts: List[str]) -> np.ndarray:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed

        Returns:
            numpy array of shape (len(texts), embedding_dim)
        """
        if not texts:
            return np.array([])

        # Check cache first
        uncached_indices = []
        uncached_texts = []
        embeddings = [None] * len(texts)

        for i, text in enumerate(texts):
            if text in self._embedding_cache:
                embeddings[i] = self._embedding_cache[text]
            else:
                uncached_indices.append(i)
                uncached_texts.append(text)

        # Embed uncached texts
        if uncached_texts:
            new_embeddings = await self._embed_batch(uncached_texts)

            # Update cache and results
            for idx, text, embedding in zip(uncached_indices, uncached_texts, new_embeddings):
                self._embedding_cache[text] = embedding
                embeddings[idx] = embedding

        return np.array(embeddings)

    async def _embed_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Embed a batch of texts via OpenRouter API"""
        all_embeddings = []

        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]
            batch_embeddings = await self._call_api(batch)
            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    async def _call_api(self, texts: List[str]) -> List[np.ndarray]:
        """Call Jina embeddings API"""
        for attempt in range(self.config.max_retries):
            try:
                response = await self.client.post(
                    f"{self.config.base_url}/embeddings",
                    headers={
                        "Authorization": f"Bearer {self.config.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.config.model,
                        "input": texts
                    }
                )

                if response.status_code == 200:
                    data = response.json()
                    embeddings = [
                        np.array(item["embedding"])
                        for item in data["data"]
                    ]
                    return embeddings
                else:
                    logger.error(f"Embedding API error: {response.status_code} - {response.text}")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise Exception(f"Embedding failed: {response.text}")

            except Exception as e:
                logger.error(f"Embedding request failed: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

        raise Exception("Embedding failed after all retries")

    async def embed_tool_schema(self, tool: Dict[str, Any]) -> np.ndarray:
        """
        Embed a tool schema for semantic search.

        Combines name, description, input schema, and output schema
        into a rich semantic representation.
        """
        text = self._tool_to_text(tool)
        embeddings = await self.embed([text])
        return embeddings[0] if len(embeddings) > 0 else np.array([])

    def _tool_to_text(self, tool: Dict[str, Any]) -> str:
        return tool_to_text(tool)

    def get_embedding_dim(self) -> int:
        """Get embedding dimension for the configured model"""
        # Jina embedding dimensions
        if "jina-embeddings-v3" in self.config.model:
            return 1024
        elif "jina-embeddings-v2" in self.config.model:
            return 768
        else:
            return 1024  # Default for Jina v3

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


def tool_to_text(tool: Dict[str, Any]) -> str:
    """
    Convert tool schema to text for embedding.

    Format:
    Tool: {name}
    Provider: {backend}     # strong, repeated provider label for name queries
    Description: {description}
    Inputs: {input_schema}
    Outputs: {output_schema}
    Tags: {tags}
    Category: {category}
    Source: {source}
    """
    parts = []

    # Name
    if "name" in tool:
        parts.append(f"Tool: {tool['name']}")

    # Provider / backend label. This is the fix for provider-name retrieval
    # (discover_tools("cloudmail") -> matched:0): the backend label used to be
    # only a filter column and never entered the embedded text, so provider
    # queries could never semantically match. Resolve it from every place it
    # can live and emit a strong, explicit line.
    provider = _resolve_provider_label(tool)
    if provider:
        parts.append(f"Provider: {provider}")
        parts.append(f"Backend: {provider}")
        parts.append(f"This tool is provided by {provider}.")

    # Description
    if "description" in tool:
        parts.append(f"Description: {tool['description']}")

    # Input schema
    if "inputSchema" in tool or "input_schema" in tool:
        schema = tool.get("inputSchema") or tool.get("input_schema", {})
        if isinstance(schema, dict):
            properties = schema.get("properties", {})
            if properties:
                input_desc = ", ".join(
                    f"{k} ({v.get('type', 'any')})"
                    for k, v in properties.items()
                )
                parts.append(f"Inputs: {input_desc}")

    # Output schema
    if "outputSchema" in tool or "output_schema" in tool:
        schema = tool.get("outputSchema") or tool.get("output_schema", {})
        if isinstance(schema, dict):
            properties = schema.get("properties", {})
            if properties:
                output_desc = ", ".join(
                    f"{k} ({v.get('type', 'any')})"
                    for k, v in properties.items()
                )
                parts.append(f"Outputs: {output_desc}")

    # Tags
    if "tags" in tool and tool["tags"]:
        parts.append(f"Tags: {', '.join(tool['tags'])}")

    # Category
    if "category" in tool:
        parts.append(f"Category: {tool['category']}")

    # Human-facing source / provider display name
    if tool.get("source"):
        parts.append(f"Source: {tool['source']}")

    return "\n".join(parts)


def _resolve_provider_label(tool: Dict[str, Any]) -> str:
    """Resolve the provider/backend label from wherever it may live.

    Prefers the machine-readable backend id (an explicit ``provider``/``backend``
    field or ``annotations.backend``), falling back to a human-facing ``source``
    display name. Returns an empty string when no label is present.
    """
    for key in ("provider", "backend"):
        value = tool.get(key)
        if value:
            return str(value)
    annotations = tool.get("annotations") or {}
    backend = annotations.get("backend")
    if backend:
        return str(backend)
    source = tool.get("source")
    return str(source) if source else ""


class LocalHashEmbeddings:
    """
    Deterministic offline embeddings via feature hashing (words + char trigrams).

    Much weaker than a real embedding model, but keeps semantic search
    functional when JINA_API_KEY is absent instead of dying to keyword-only
    fallback. Vectors are only comparable to other LocalHashEmbeddings
    vectors — the index layer namespaces its DB per embedder so the two
    spaces never mix.
    """

    DIM = 1024
    name = "local-hash-v1"

    def __init__(self):
        self._embedding_cache: Dict[str, np.ndarray] = {}

    @staticmethod
    def _features(text: str) -> List[str]:
        t = text.lower()
        words = [w for w in "".join(c if c.isalnum() else " " for c in t).split() if w]
        grams = []
        for w in words:
            padded = f"#{w}#"
            grams.extend(padded[i:i + 3] for i in range(len(padded) - 2))
        return words + grams

    def _embed_one(self, text: str) -> np.ndarray:
        import hashlib

        vec = np.zeros(self.DIM, dtype=np.float64)
        for feat in self._features(text):
            digest = hashlib.blake2b(feat.encode("utf-8"), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "little") % self.DIM
            sign = 1.0 if digest[4] & 1 else -1.0
            vec[bucket] += sign
        norm = np.linalg.norm(vec)
        return vec / norm if norm > 0 else vec

    async def embed(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.array([])
        out = []
        for text in texts:
            cached = self._embedding_cache.get(text)
            if cached is None:
                cached = self._embed_one(text)
                self._embedding_cache[text] = cached
            out.append(cached)
        return np.array(out)

    async def embed_tool_schema(self, tool: Dict[str, Any]) -> np.ndarray:
        text = tool_to_text(tool)
        embeddings = await self.embed([text])
        return embeddings[0] if len(embeddings) > 0 else np.array([])

    def _tool_to_text(self, tool: Dict[str, Any]) -> str:
        return tool_to_text(tool)

    def get_embedding_dim(self) -> int:
        return self.DIM

    async def close(self):
        pass


class ResilientEmbeddings:
    """
    Wraps a primary embedding provider (Jina when JINA_API_KEY is set) with a
    deterministic LocalHashEmbeddings fallback.

    Unlike get_embedding_service(), this also survives a key that is set but
    broken (e.g. 402 zero-balance): the first primary failure trips a sticky
    switch to the local embedder for the rest of the process lifetime.

    embed_tagged() returns (vectors, embedder_tag) so callers can namespace
    stored vectors and never compare across embedding spaces.
    """

    PRIMARY_NAME = "jina"

    def __init__(self, primary: Optional[Any] = None):
        self._local = LocalHashEmbeddings()
        if primary is None and os.getenv("JINA_API_KEY"):
            try:
                primary = JinaEmbeddings()
            except Exception as e:
                logger.warning(f"Failed to initialize primary embedder: {e}")
                primary = None
        self._primary = primary
        self._primary_failed = False
        self.last_error: Optional[str] = None

    @property
    def active_name(self) -> str:
        """Tag of the embedder that the next embed() call will use."""
        if self._primary is not None and not self._primary_failed:
            return self.PRIMARY_NAME
        return self._local.name

    @property
    def name(self) -> str:
        return self.active_name

    async def embed_tagged(self, texts: List[str]) -> Tuple[np.ndarray, str]:
        """Embed texts, returning (vectors, embedder_tag actually used)."""
        if self._primary is not None and not self._primary_failed:
            try:
                vectors = await self._primary.embed(texts)
                return vectors, self.PRIMARY_NAME
            except Exception as e:
                self.last_error = str(e)
                self._primary_failed = True
                logger.warning(
                    f"Primary embedder failed ({e}); "
                    f"falling back to {self._local.name} for the rest of this process"
                )
        return await self._local.embed(texts), self._local.name

    async def embed(self, texts: List[str]) -> np.ndarray:
        vectors, _ = await self.embed_tagged(texts)
        return vectors

    async def embed_tool_schema(self, tool: Dict[str, Any]) -> np.ndarray:
        embeddings = await self.embed([tool_to_text(tool)])
        return embeddings[0] if len(embeddings) > 0 else np.array([])

    def _tool_to_text(self, tool: Dict[str, Any]) -> str:
        return tool_to_text(tool)

    def get_embedding_dim(self) -> int:
        if self._primary is not None and not self._primary_failed:
            return self._primary.get_embedding_dim()
        return self._local.get_embedding_dim()

    async def close(self):
        if self._primary is not None:
            await self._primary.close()
        await self._local.close()


# Global embedding service
_embedding_service: Optional[Any] = None
_resilient_embedding_service: Optional[ResilientEmbeddings] = None


def get_embedding_service():
    """
    Get or create the global embedding service.

    Prefers Jina (JINA_API_KEY set); falls back to deterministic local
    hashing embeddings so semantic search stays available without a key.
    """
    global _embedding_service

    if _embedding_service is None:
        if os.getenv("JINA_API_KEY"):
            _embedding_service = JinaEmbeddings()
        else:
            logger.warning(
                "JINA_API_KEY not set — using LocalHashEmbeddings fallback for tool search"
            )
            _embedding_service = LocalHashEmbeddings()

    return _embedding_service


def get_resilient_embedding_service() -> ResilientEmbeddings:
    """
    Get or create the global resilient embedding service.

    Tries Jina when JINA_API_KEY is set; on any failure (402, network, ...)
    it permanently falls back to LocalHashEmbeddings for this process.
    """
    global _resilient_embedding_service

    if _resilient_embedding_service is None:
        _resilient_embedding_service = ResilientEmbeddings()

    return _resilient_embedding_service


def embedding_service_is_local() -> bool:
    """True when the active embedding service is the offline hashing fallback."""
    return isinstance(get_embedding_service(), LocalHashEmbeddings)


# Backward compatibility alias
OpenRouterEmbeddings = JinaEmbeddings


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Calculate cosine similarity between two vectors"""
    if len(a.shape) == 1 and len(b.shape) == 1:
        # Single vectors
        dot_product = np.dot(a, b)
        magnitude_a = np.linalg.norm(a)
        magnitude_b = np.linalg.norm(b)
        return float(dot_product / (magnitude_a * magnitude_b))
    else:
        # Batch computation
        dot_product = np.dot(a, b.T)
        magnitude_a = np.linalg.norm(a, axis=1, keepdims=True)
        magnitude_b = np.linalg.norm(b, axis=1, keepdims=True)
        return dot_product / (magnitude_a * magnitude_b.T)
