"""
MLX Embedding Service - Local embeddings on Apple Silicon

Supports:
- jina-embeddings-v4 (8192 context, multilingual)
- bge-m3 (8192 context, retrieval-optimized)

Running on MLX for M4 Max (48GB unified memory)
"""
from __future__ import annotations

import asyncio
import logging
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import json

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingConfig:
    """Configuration for embedding model"""
    model_name: str = "jinaai/jina-embeddings-v4"
    max_length: int = 8192
    batch_size: int = 32
    device: str = "mps"  # Metal Performance Shaders for Apple Silicon
    use_mlx: bool = True
    cache_dir: Optional[str] = None


class MLXEmbeddings:
    """
    MLX-based embedding service for Apple Silicon.

    Optimized for M4 Max with 48GB unified memory.
    Uses Metal Performance Shaders for GPU acceleration.
    """

    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.model = None
        self._initialized = False

    async def initialize(self):
        """Initialize the embedding model"""
        if self._initialized:
            return

        try:
            # Try to use MLX first
            if self.config.use_mlx:
                await self._init_mlx_model()
            else:
                await self._init_transformers_model()

            self._initialized = True
            logger.info(f"Initialized embedding model: {self.config.model_name}")

        except Exception as e:
            logger.error(f"Failed to initialize embedding model: {e}")
            raise

    async def _init_mlx_model(self):
        """Initialize MLX model for Apple Silicon"""
        try:
            # Try importing MLX
            import mlx.core as mx
            import mlx.nn as nn

            logger.info("Using MLX backend for embeddings")

            # For now, we'll use a placeholder
            # In production, you'd load the actual MLX-converted model
            self.model = MLXEmbeddingModel(self.config)

        except ImportError:
            logger.warning("MLX not available, falling back to transformers")
            await self._init_transformers_model()

    async def _init_transformers_model(self):
        """Fallback to transformers with MPS backend"""
        try:
            from sentence_transformers import SentenceTransformer
            import torch

            logger.info("Using transformers with MPS backend")

            # Load model on MPS (Metal Performance Shaders)
            self.model = SentenceTransformer(
                self.config.model_name,
                device=self.config.device,
                cache_folder=self.config.cache_dir
            )

        except Exception as e:
            logger.error(f"Failed to initialize transformers model: {e}")
            raise

    async def embed(self, texts: List[str]) -> np.ndarray:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed

        Returns:
            numpy array of shape (len(texts), embedding_dim)
        """
        if not self._initialized:
            await self.initialize()

        if not texts:
            return np.array([])

        try:
            # Batch processing for efficiency
            embeddings = []

            for i in range(0, len(texts), self.config.batch_size):
                batch = texts[i:i + self.config.batch_size]

                if isinstance(self.model, MLXEmbeddingModel):
                    batch_embeddings = await self.model.encode(batch)
                else:
                    # sentence-transformers
                    batch_embeddings = self.model.encode(
                        batch,
                        batch_size=self.config.batch_size,
                        show_progress_bar=False
                    )

                embeddings.append(batch_embeddings)

            return np.vstack(embeddings)

        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            raise

    async def embed_single(self, text: str) -> np.ndarray:
        """Embed a single text"""
        result = await self.embed([text])
        return result[0] if len(result) > 0 else np.array([])

    def get_embedding_dim(self) -> int:
        """Get the embedding dimension"""
        if self.config.model_name == "jinaai/jina-embeddings-v4":
            return 1024
        elif "bge-m3" in self.config.model_name:
            return 1024
        else:
            return 768  # Default


class MLXEmbeddingModel:
    """
    MLX-native embedding model implementation.

    This is a placeholder for actual MLX model.
    In production, you'd convert the model to MLX format.
    """

    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.embedding_dim = 1024

        logger.info(f"MLX Model initialized: {config.model_name}")

    async def encode(self, texts: List[str]) -> np.ndarray:
        """
        Encode texts using MLX.

        NOTE: This is a placeholder implementation.
        In production, you'd use actual MLX model weights.
        """
        # For now, return random embeddings
        # In production, this would be actual model inference

        # Simulate async processing
        await asyncio.sleep(0.01)

        # TODO: Replace with actual MLX inference
        # import mlx.core as mx
        # embeddings = self.model(texts)
        # return mx.array(embeddings).to_numpy()

        # Placeholder: random normalized vectors
        embeddings = np.random.randn(len(texts), self.embedding_dim)
        # Normalize to unit length
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

        return embeddings


# Global embedding service instance
_embedding_service: Optional[MLXEmbeddings] = None


def get_embedding_service(
    model_name: str = "jinaai/jina-embeddings-v4",
    use_mlx: bool = True
) -> MLXEmbeddings:
    """Get or create the global embedding service"""
    global _embedding_service

    if _embedding_service is None:
        config = EmbeddingConfig(
            model_name=model_name,
            use_mlx=use_mlx,
            max_length=8192,
            batch_size=32
        )
        _embedding_service = MLXEmbeddings(config)

    return _embedding_service


async def embed_texts(texts: List[str]) -> np.ndarray:
    """Convenience function to embed texts"""
    service = get_embedding_service()
    return await service.embed(texts)
