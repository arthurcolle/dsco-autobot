"""
Embeddings module for semantic tool discovery and belief storage.

Provides:
- OpenRouter/Jina embedding service
- Semantic tool store with intelligent context management
- Semantic belief store with persistence and semantic search
- Vector search for millions of tools and beliefs
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

from .semantic_belief_store import (
    SemanticBeliefStore,
    get_belief_store,
    Belief
)

__all__ = [
    "OpenRouterEmbeddings",
    "get_embedding_service",
    "cosine_similarity",
    "SemanticToolStore",
    "get_tool_store",
    "ToolEmbedding",
    "ContextWindow",
    "SemanticBeliefStore",
    "get_belief_store",
    "Belief"
]
