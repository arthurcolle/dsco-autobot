"""
Jina Embedding Service - Semantic Tool Discovery

Uses Jina AI's embedding API to enable semantic search over tool schemas.
Supports millions of tools through intelligent context management.
"""
from __future__ import annotations

import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
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
        """
        Convert tool schema to text for embedding.

        Format:
        Tool: {name}
        Description: {description}
        Inputs: {input_schema}
        Outputs: {output_schema}
        Tags: {tags}
        Category: {category}
        """
        parts = []

        # Name
        if "name" in tool:
            parts.append(f"Tool: {tool['name']}")

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

        # Annotations
        if "annotations" in tool:
            ann = tool["annotations"]
            if ann.get("backend"):
                parts.append(f"Backend: {ann['backend']}")

        return "\n".join(parts)

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


# Global embedding service
_embedding_service: Optional[JinaEmbeddings] = None


def get_embedding_service() -> JinaEmbeddings:
    """Get or create the global Jina embedding service"""
    global _embedding_service

    if _embedding_service is None:
        _embedding_service = JinaEmbeddings()

    return _embedding_service


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
