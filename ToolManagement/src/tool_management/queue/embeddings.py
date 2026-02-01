"""
Local Embeddings Module - Generate vector embeddings using open-source models.

Uses sentence-transformers for high-quality embeddings without API costs.

Models:
- all-MiniLM-L6-v2: Fast, 384 dimensions (recommended for most use cases)
- all-mpnet-base-v2: Better quality, 768 dimensions
- multi-qa-MiniLM-L6-cos-v1: Optimized for semantic search
- paraphrase-multilingual-MiniLM-L12-v2: Multilingual support

Install: pip install sentence-transformers
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import hashlib
import json
from pathlib import Path

logger = logging.getLogger(__name__)

# Try to import sentence-transformers
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logger.warning(
        "sentence-transformers not available. "
        "Install with: pip install sentence-transformers"
    )

# Try numpy for vector operations
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False


@dataclass
class EmbeddingResult:
    """Result of embedding generation."""
    text: str
    embedding: List[float]
    model: str
    dimensions: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text_preview": self.text[:100] + "..." if len(self.text) > 100 else self.text,
            "embedding": self.embedding,
            "model": self.model,
            "dimensions": self.dimensions,
            "metadata": self.metadata,
        }


@dataclass
class EmbeddingModelInfo:
    """Information about an embedding model."""
    name: str
    dimensions: int
    description: str
    max_seq_length: int
    speed: str  # "fast", "medium", "slow"
    quality: str  # "good", "better", "best"


# Available models
EMBEDDING_MODELS: Dict[str, EmbeddingModelInfo] = {
    "all-MiniLM-L6-v2": EmbeddingModelInfo(
        name="all-MiniLM-L6-v2",
        dimensions=384,
        description="Fast general-purpose model, good balance of speed and quality",
        max_seq_length=256,
        speed="fast",
        quality="good",
    ),
    "all-mpnet-base-v2": EmbeddingModelInfo(
        name="all-mpnet-base-v2",
        dimensions=768,
        description="Higher quality embeddings, slower but more accurate",
        max_seq_length=384,
        speed="medium",
        quality="best",
    ),
    "multi-qa-MiniLM-L6-cos-v1": EmbeddingModelInfo(
        name="multi-qa-MiniLM-L6-cos-v1",
        dimensions=384,
        description="Optimized for semantic search and question answering",
        max_seq_length=512,
        speed="fast",
        quality="good",
    ),
    "paraphrase-multilingual-MiniLM-L12-v2": EmbeddingModelInfo(
        name="paraphrase-multilingual-MiniLM-L12-v2",
        dimensions=384,
        description="Multilingual support for 50+ languages",
        max_seq_length=128,
        speed="medium",
        quality="good",
    ),
    "BAAI/bge-small-en-v1.5": EmbeddingModelInfo(
        name="BAAI/bge-small-en-v1.5",
        dimensions=384,
        description="BGE small model, excellent quality for its size",
        max_seq_length=512,
        speed="fast",
        quality="better",
    ),
    "BAAI/bge-base-en-v1.5": EmbeddingModelInfo(
        name="BAAI/bge-base-en-v1.5",
        dimensions=768,
        description="BGE base model, state-of-the-art quality",
        max_seq_length=512,
        speed="medium",
        quality="best",
    ),
}

DEFAULT_MODEL = "all-MiniLM-L6-v2"


class EmbeddingGenerator:
    """
    Generate embeddings using sentence-transformers.

    Supports batching, caching, and multiple models.
    """

    _model_cache: Dict[str, Any] = {}  # Class-level model cache

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        cache_dir: Optional[str] = None,
        device: Optional[str] = None,
        normalize: bool = True,
    ):
        """
        Initialize the embedding generator.

        Args:
            model_name: Name of the sentence-transformers model
            cache_dir: Directory to cache model files
            device: Device to run on ('cpu', 'cuda', 'mps')
            normalize: Whether to L2 normalize embeddings
        """
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.device = device
        self.normalize = normalize
        self._model: Optional[Any] = None

        # Get model info
        self.model_info = EMBEDDING_MODELS.get(model_name)
        if self.model_info:
            self.dimensions = self.model_info.dimensions
        else:
            self.dimensions = None  # Will be set when model loads

    def _load_model(self) -> Any:
        """Load or retrieve cached model."""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError(
                "sentence-transformers is required. "
                "Install with: pip install sentence-transformers"
            )

        # Check class-level cache first
        if self.model_name in self._model_cache:
            return self._model_cache[self.model_name]

        logger.info(f"Loading embedding model: {self.model_name}")

        model = SentenceTransformer(
            self.model_name,
            cache_folder=self.cache_dir,
            device=self.device,
        )

        # Cache the model
        self._model_cache[self.model_name] = model

        # Update dimensions if not known
        if self.dimensions is None:
            self.dimensions = model.get_sentence_embedding_dimension()

        return model

    @property
    def model(self) -> Any:
        """Lazy load the model."""
        if self._model is None:
            self._model = self._load_model()
        return self._model

    def embed(self, text: str) -> EmbeddingResult:
        """
        Generate embedding for a single text.

        Args:
            text: Text to embed

        Returns:
            EmbeddingResult with embedding vector
        """
        embedding = self.model.encode(
            text,
            normalize_embeddings=self.normalize,
            show_progress_bar=False,
        )

        return EmbeddingResult(
            text=text,
            embedding=embedding.tolist(),
            model=self.model_name,
            dimensions=len(embedding),
        )

    def embed_batch(
        self,
        texts: List[str],
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> List[EmbeddingResult]:
        """
        Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed
            batch_size: Batch size for encoding
            show_progress: Whether to show progress bar

        Returns:
            List of EmbeddingResult objects
        """
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            normalize_embeddings=self.normalize,
            show_progress_bar=show_progress,
        )

        return [
            EmbeddingResult(
                text=text,
                embedding=emb.tolist(),
                model=self.model_name,
                dimensions=len(emb),
            )
            for text, emb in zip(texts, embeddings)
        ]

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Simple interface to get just the embedding vectors.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        embeddings = self.model.encode(
            texts,
            normalize_embeddings=self.normalize,
            show_progress_bar=False,
        )
        return [emb.tolist() for emb in embeddings]

    @staticmethod
    def cosine_similarity(a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        if not NUMPY_AVAILABLE:
            # Fallback without numpy
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = sum(x * x for x in a) ** 0.5
            norm_b = sum(x * x for x in b) ** 0.5
            return dot / (norm_a * norm_b + 1e-8)

        a_np = np.array(a)
        b_np = np.array(b)
        return float(np.dot(a_np, b_np) / (np.linalg.norm(a_np) * np.linalg.norm(b_np) + 1e-8))

    @staticmethod
    def find_most_similar(
        query_embedding: List[float],
        embeddings: List[List[float]],
        top_k: int = 5,
    ) -> List[tuple]:
        """
        Find most similar embeddings to a query.

        Args:
            query_embedding: Query vector
            embeddings: List of candidate vectors
            top_k: Number of results to return

        Returns:
            List of (index, similarity_score) tuples
        """
        similarities = []
        for i, emb in enumerate(embeddings):
            sim = EmbeddingGenerator.cosine_similarity(query_embedding, emb)
            similarities.append((i, sim))

        # Sort by similarity descending
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]


class CachedEmbeddingGenerator(EmbeddingGenerator):
    """
    Embedding generator with disk-based caching.

    Caches embeddings to avoid recomputing for the same text.
    """

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        cache_dir: Optional[str] = None,
        embedding_cache_path: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(model_name, cache_dir, **kwargs)

        # Set up embedding cache
        if embedding_cache_path:
            self.embedding_cache_path = Path(embedding_cache_path)
        else:
            self.embedding_cache_path = Path.home() / ".tool_management" / "embedding_cache.json"

        self.embedding_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._embedding_cache: Dict[str, List[float]] = self._load_cache()

    def _load_cache(self) -> Dict[str, List[float]]:
        """Load embedding cache from disk."""
        if self.embedding_cache_path.exists():
            try:
                with open(self.embedding_cache_path) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load embedding cache: {e}")
        return {}

    def _save_cache(self):
        """Save embedding cache to disk."""
        try:
            with open(self.embedding_cache_path, "w") as f:
                json.dump(self._embedding_cache, f)
        except Exception as e:
            logger.warning(f"Failed to save embedding cache: {e}")

    def _cache_key(self, text: str) -> str:
        """Generate cache key for text."""
        return hashlib.md5(f"{self.model_name}:{text}".encode()).hexdigest()

    def embed(self, text: str) -> EmbeddingResult:
        """Generate embedding with caching."""
        cache_key = self._cache_key(text)

        if cache_key in self._embedding_cache:
            embedding = self._embedding_cache[cache_key]
            return EmbeddingResult(
                text=text,
                embedding=embedding,
                model=self.model_name,
                dimensions=len(embedding),
                metadata={"cached": True},
            )

        result = super().embed(text)
        self._embedding_cache[cache_key] = result.embedding
        self._save_cache()

        return result

    def embed_batch(
        self,
        texts: List[str],
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> List[EmbeddingResult]:
        """Generate embeddings with caching."""
        results = []
        texts_to_embed = []
        text_indices = []

        # Check cache first
        for i, text in enumerate(texts):
            cache_key = self._cache_key(text)
            if cache_key in self._embedding_cache:
                results.append(EmbeddingResult(
                    text=text,
                    embedding=self._embedding_cache[cache_key],
                    model=self.model_name,
                    dimensions=len(self._embedding_cache[cache_key]),
                    metadata={"cached": True},
                ))
            else:
                texts_to_embed.append(text)
                text_indices.append(i)
                results.append(None)  # Placeholder

        # Embed uncached texts
        if texts_to_embed:
            new_results = super().embed_batch(texts_to_embed, batch_size, show_progress)

            for idx, result in zip(text_indices, new_results):
                results[idx] = result
                cache_key = self._cache_key(result.text)
                self._embedding_cache[cache_key] = result.embedding

            self._save_cache()

        return results


# =============================================================================
# Convenience Functions
# =============================================================================

# Global generator instance (lazy loaded)
_default_generator: Optional[EmbeddingGenerator] = None


def get_embedding_generator(
    model_name: str = DEFAULT_MODEL,
    cached: bool = False,
) -> EmbeddingGenerator:
    """
    Get an embedding generator instance.

    Args:
        model_name: Model to use
        cached: Whether to use caching

    Returns:
        EmbeddingGenerator instance
    """
    if cached:
        return CachedEmbeddingGenerator(model_name)
    return EmbeddingGenerator(model_name)


def embed_text(text: str, model_name: str = DEFAULT_MODEL) -> List[float]:
    """
    Convenience function to embed a single text.

    Args:
        text: Text to embed
        model_name: Model to use

    Returns:
        Embedding vector
    """
    global _default_generator
    if _default_generator is None or _default_generator.model_name != model_name:
        _default_generator = EmbeddingGenerator(model_name)
    return _default_generator.embed(text).embedding


def embed_texts(texts: List[str], model_name: str = DEFAULT_MODEL) -> List[List[float]]:
    """
    Convenience function to embed multiple texts.

    Args:
        texts: List of texts to embed
        model_name: Model to use

    Returns:
        List of embedding vectors
    """
    global _default_generator
    if _default_generator is None or _default_generator.model_name != model_name:
        _default_generator = EmbeddingGenerator(model_name)
    return _default_generator.embed_texts(texts)


def list_available_models() -> List[Dict[str, Any]]:
    """List available embedding models with their info."""
    return [
        {
            "name": info.name,
            "dimensions": info.dimensions,
            "description": info.description,
            "max_seq_length": info.max_seq_length,
            "speed": info.speed,
            "quality": info.quality,
        }
        for info in EMBEDDING_MODELS.values()
    ]
