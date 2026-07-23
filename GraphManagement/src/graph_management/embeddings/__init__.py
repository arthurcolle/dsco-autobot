"""
Embedding providers for graph overlay system.

Supports multiple providers:
- Jina AI (recommended)
- Custom implementations
"""

from .base import EmbeddingProvider
from .jina_provider import JinaProvider

__all__ = [
    "EmbeddingProvider",
    "JinaProvider",
]
