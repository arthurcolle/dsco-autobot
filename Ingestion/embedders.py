"""
Text Embedders

Generate vector embeddings for text chunks.
"""

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from .models import Chunk

logger = logging.getLogger(__name__)


@dataclass
class EmbedderConfig:
    """Configuration for embedding generation."""
    model: str = "jina-embeddings-v3"
    dimensions: int = 1024
    batch_size: int = 32
    max_retries: int = 3
    timeout: float = 60.0


class Embedder(ABC):
    """Base class for text embedders."""

    def __init__(self, config: Optional[EmbedderConfig] = None):
        self.config = config or EmbedderConfig()

    @abstractmethod
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors
        """
        pass

    async def embed_chunks(self, chunks: List[Chunk]) -> List[Chunk]:
        """
        Generate embeddings for chunks.

        Args:
            chunks: List of chunks to embed

        Returns:
            Chunks with embeddings added
        """
        if not chunks:
            return chunks

        texts = [c.content for c in chunks]
        embeddings = await self.embed(texts)

        for chunk, embedding in zip(chunks, embeddings):
            chunk.embedding = embedding
            chunk.embedding_model = self.config.model

        return chunks


class JinaEmbedder(Embedder):
    """Generate embeddings using Jina AI API."""

    def __init__(self, config: Optional[EmbedderConfig] = None, api_key: Optional[str] = None):
        super().__init__(config)
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        self.base_url = "https://api.jina.ai/v1/embeddings"

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Jina API."""
        import httpx

        if not self.api_key:
            raise ValueError("JINA_API_KEY environment variable not set")

        all_embeddings: List[List[float]] = []

        # Process in batches
        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]

            for attempt in range(self.config.max_retries):
                try:
                    async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                        response = await client.post(
                            self.base_url,
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "Content-Type": "application/json"
                            },
                            json={
                                "model": self.config.model,
                                "input": batch,
                                "dimensions": self.config.dimensions,
                            }
                        )
                        response.raise_for_status()
                        data = response.json()

                        # Extract embeddings
                        batch_embeddings = [
                            item["embedding"] for item in data["data"]
                        ]
                        all_embeddings.extend(batch_embeddings)
                        break

                except Exception as e:
                    logger.warning(f"Jina embedding attempt {attempt + 1} failed: {e}")
                    if attempt == self.config.max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

        return all_embeddings


class OpenAIEmbedder(Embedder):
    """Generate embeddings using OpenAI API."""

    def __init__(self, config: Optional[EmbedderConfig] = None, api_key: Optional[str] = None):
        config = config or EmbedderConfig(model="text-embedding-3-small", dimensions=1536)
        super().__init__(config)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = "https://api.openai.com/v1/embeddings"

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using OpenAI API."""
        import httpx

        if not self.api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")

        all_embeddings: List[List[float]] = []

        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]

            for attempt in range(self.config.max_retries):
                try:
                    async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                        response = await client.post(
                            self.base_url,
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "Content-Type": "application/json"
                            },
                            json={
                                "model": self.config.model,
                                "input": batch,
                            }
                        )
                        response.raise_for_status()
                        data = response.json()

                        batch_embeddings = [
                            item["embedding"] for item in data["data"]
                        ]
                        all_embeddings.extend(batch_embeddings)
                        break

                except Exception as e:
                    logger.warning(f"OpenAI embedding attempt {attempt + 1} failed: {e}")
                    if attempt == self.config.max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

        return all_embeddings


class CohereEmbedder(Embedder):
    """Generate embeddings using Cohere API."""

    def __init__(self, config: Optional[EmbedderConfig] = None, api_key: Optional[str] = None):
        config = config or EmbedderConfig(model="embed-english-v3.0", dimensions=1024)
        super().__init__(config)
        self.api_key = api_key or os.getenv("COHERE_API_KEY")
        self.base_url = "https://api.cohere.ai/v1/embed"

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Cohere API."""
        import httpx

        if not self.api_key:
            raise ValueError("COHERE_API_KEY environment variable not set")

        all_embeddings: List[List[float]] = []

        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]

            for attempt in range(self.config.max_retries):
                try:
                    async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                        response = await client.post(
                            self.base_url,
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "Content-Type": "application/json"
                            },
                            json={
                                "model": self.config.model,
                                "texts": batch,
                                "input_type": "search_document",
                            }
                        )
                        response.raise_for_status()
                        data = response.json()

                        all_embeddings.extend(data["embeddings"])
                        break

                except Exception as e:
                    logger.warning(f"Cohere embedding attempt {attempt + 1} failed: {e}")
                    if attempt == self.config.max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

        return all_embeddings


class BatchEmbedder(Embedder):
    """
    Batched embedder with rate limiting and retries.

    Wraps another embedder to add batching, rate limiting, and progress tracking.
    """

    def __init__(
        self,
        embedder: Embedder,
        batch_size: int = 32,
        rate_limit: float = 0.1,  # Seconds between batches
    ):
        super().__init__(embedder.config)
        self.embedder = embedder
        self.batch_size = batch_size
        self.rate_limit = rate_limit
        self._processed = 0
        self._total = 0

    @property
    def progress(self) -> float:
        """Get current progress as a fraction."""
        return self._processed / self._total if self._total > 0 else 0.0

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings with batching and rate limiting."""
        self._total = len(texts)
        self._processed = 0

        all_embeddings: List[List[float]] = []

        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]

            batch_embeddings = await self.embedder.embed(batch)
            all_embeddings.extend(batch_embeddings)

            self._processed += len(batch)
            logger.debug(f"Embedded {self._processed}/{self._total} texts")

            # Rate limiting
            if i + self.batch_size < len(texts):
                await asyncio.sleep(self.rate_limit)

        return all_embeddings


class LocalEmbedder(Embedder):
    """
    Local embedding using sentence-transformers.

    Useful for development/testing without API calls.
    """

    def __init__(self, config: Optional[EmbedderConfig] = None, model_name: Optional[str] = None):
        config = config or EmbedderConfig(model="all-MiniLM-L6-v2", dimensions=384)
        super().__init__(config)
        self.model_name = model_name or config.model
        self._model = None

    def _load_model(self):
        """Lazy load the model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name)
                logger.info(f"Loaded local embedding model: {self.model_name}")
            except ImportError:
                raise ImportError("sentence-transformers required for local embeddings")
        return self._model

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings locally."""
        model = self._load_model()

        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        embeddings = await loop.run_in_executor(
            None,
            lambda: model.encode(texts, convert_to_numpy=True).tolist()
        )

        return embeddings


def get_embedder(
    provider: str = "jina",
    config: Optional[EmbedderConfig] = None
) -> Embedder:
    """
    Get an embedder by provider name.

    Args:
        provider: "jina", "openai", "cohere", or "local"
        config: Optional embedder configuration

    Returns:
        Configured embedder instance
    """
    providers: Dict[str, type] = {
        "jina": JinaEmbedder,
        "openai": OpenAIEmbedder,
        "cohere": CohereEmbedder,
        "local": LocalEmbedder,
    }

    embedder_class = providers.get(provider.lower())
    if not embedder_class:
        raise ValueError(f"Unknown embedding provider: {provider}")

    return embedder_class(config)
