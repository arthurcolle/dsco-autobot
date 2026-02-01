"""
Embeddings module for semantic tool discovery.

Provides:
- OpenRouter embedding service
- Semantic tool store with intelligent context management
- Vector search for millions of tools
"""

from .openrouter_embeddings import (
    OpenRouterEmbeddings,
    get_embedding_service,
    cosine_similarity
)

from .semantic_tool_store import (
    SemanticToolStore,
    get_tool_store,
    ToolEmbedding,
    ContextWindow
)

__all__ = [
    "OpenRouterEmbeddings",
    "get_embedding_service",
    "cosine_similarity",
    "SemanticToolStore",
    "get_tool_store",
    "ToolEmbedding",
    "ContextWindow"
]
