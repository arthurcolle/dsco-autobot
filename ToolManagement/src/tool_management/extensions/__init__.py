"""
Graph Management Extensions.

Provides vector indexing, schema management, and agent coordination.
"""

from .graph import (
    VectorIndexRequest,
    SchemaWithEmbeddingRequest,
    HierarchicalSchemaRequest,
    EmbeddingCallbackRegistration,
    ChangeDetectionRequest,
    AgentRegistrationRequest,
    AgentCoordinationRequest,
    GraphManagementExtensions,
)

__all__ = [
    "VectorIndexRequest",
    "SchemaWithEmbeddingRequest",
    "HierarchicalSchemaRequest",
    "EmbeddingCallbackRegistration",
    "ChangeDetectionRequest",
    "AgentRegistrationRequest",
    "AgentCoordinationRequest",
    "GraphManagementExtensions",
]
