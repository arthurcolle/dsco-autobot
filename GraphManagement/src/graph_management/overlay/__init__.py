"""
Graph overlay system for sophisticated memory objects with embeddings,
relationships, calculated fields, and policies.

This module provides the @graph.object decorator and BasedModel ORM layer
for creating graph-native Pydantic models.
"""

from .specs import (
    EmbeddingSpec,
    EmbeddingSource,
    AutoLinkSimilar,
    RelationSpec,
    FieldSpec,
    CalculatedFieldSpec,
    MemoryPolicies,
    SimilarityFn,
    Direction,
    Cardinality,
    RetentionPolicy,
)
from .decorator import object as graph_object
from .based_model import BasedModel

__all__ = [
    # Decorator
    "graph_object",
    "BasedModel",
    # Specifications
    "EmbeddingSpec",
    "EmbeddingSource",
    "AutoLinkSimilar",
    "RelationSpec",
    "FieldSpec",
    "CalculatedFieldSpec",
    "MemoryPolicies",
    # Enums
    "SimilarityFn",
    "Direction",
    "Cardinality",
    "RetentionPolicy",
]
