"""
Base embedding provider interface.
"""

from abc import ABC, abstractmethod
from typing import List, Union


class EmbeddingProvider(ABC):
    """Base class for embedding providers."""

    @abstractmethod
    def embed(self, text: str, model: str, dim: int) -> List[float]:
        """
        Generate embedding for a single text.

        Args:
            text: Input text
            model: Model identifier
            dim: Expected dimension

        Returns:
            Embedding vector
        """
        pass

    def embed_batch(self, texts: List[str], model: str, dim: int) -> List[List[float]]:
        """
        Generate embeddings for multiple texts.

        Default implementation calls embed() sequentially.
        Override for batched implementations.

        Args:
            texts: List of input texts
            model: Model identifier
            dim: Expected dimension

        Returns:
            List of embedding vectors
        """
        return [self.embed(text, model, dim) for text in texts]

    def __call__(self, text: Union[str, List[str]], model: str, dim: int) -> Union[List[float], List[List[float]]]:
        """
        Callable interface for compatibility.

        Args:
            text: Single text or list of texts
            model: Model identifier
            dim: Expected dimension

        Returns:
            Single vector or list of vectors
        """
        if isinstance(text, list):
            return self.embed_batch(text, model, dim)
        return self.embed(text, model, dim)
