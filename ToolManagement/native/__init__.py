"""
Native MCP Servers - First-class MCP implementations.

These servers provide native MCP protocol support without external dependencies,
establishing our core distributed systems capabilities.

Base Infrastructure (native/base.py):
- NativeMCPBase: Foundation for all native MCPs
- MCPMesh: Cross-MCP communication with service discovery
- EmbeddingMixin: Adds embedding capabilities to any MCP
- CapabilityType: Standard capability types
- CircuitBreaker: Health monitoring for endpoints
- ResultCache: LRU cache with TTL

Servers:
- distributed_search_mcp (4001): Web search, content extraction, embeddings
- distributed_research_mcp (4002): Deep research, task execution, parallel processing
- distributed_publish_mcp (4003): Webhooks, events, content syndication
- distributed_memory_mcp (4004): Cognitive memory architecture

v2 Servers (built on NativeMCPBase):
- All servers have been migrated to use NativeMCPBase for:
  * Cross-MCP mesh communication
  * Automatic service discovery
  * Circuit breaker pattern
  * Result caching
  * Standardized capabilities

Media/Document Servers:
- distributed_video_qa (4005): Video ingestion, Q&A, search, transcripts
- distributed_document_qa (4006): PDF ingestion, Q&A, semantic search, tables/figures
"""

# Base infrastructure
from .base import (
    NativeMCPBase,
    MCPMesh,
    EmbeddingMixin,
    CapabilityType,
    Capability,
    MCPEndpoint,
    CircuitBreaker,
    CircuitState,
    ResultCache,
    cosine_similarity,
    euclidean_distance,
)

# Sibling MCP servers are imported DEFENSIVELY: a missing optional dependency in
# one server must NOT break importing another. Each bundle deploys with only its
# own deps (e.g. distributed-unit must not require publish's substack_client).
import logging as _logging
_lg = _logging.getLogger(__name__)
def _opt(modpath, *names):
    try:
        m = __import__(f"{__name__}.{modpath}", fromlist=list(names))
        for n in names:
            if hasattr(m, n):
                globals()[n] = getattr(m, n)
    except Exception as e:
        _lg.debug("native: skipped optional import %s (%s)", modpath, e)

_opt("distributed_search_mcp", "DistributedSearchMCP")
_opt("distributed_research_mcp", "DistributedResearchMCP")
_opt("distributed_publish_mcp", "DistributedPublishMCP")
_opt("distributed_memory_mcp", "DistributedMemoryMCP")
_opt("distributed_search_mcp.server_v2", "DistributedSearchMCPv2")
_opt("distributed_research_mcp.server_v2", "DistributedResearchMCPv2")
_opt("distributed_publish_mcp.server_v2", "DistributedPublishMCPv2")
_opt("distributed_memory_mcp.server_v2", "DistributedMemoryMCPv2")
_opt("distributed_video_qa", "DistributedVideoQAMCP")
_opt("distributed_document_qa", "DistributedDocumentQAMCP")

__all__ = [
    # Base infrastructure
    "NativeMCPBase",
    "MCPMesh",
    "EmbeddingMixin",
    "CapabilityType",
    "Capability",
    "MCPEndpoint",
    "CircuitBreaker",
    "CircuitState",
    "ResultCache",
    "cosine_similarity",
    "euclidean_distance",
    # v1 servers (legacy)
    "DistributedSearchMCP",
    "DistributedResearchMCP",
    "DistributedPublishMCP",
    "DistributedMemoryMCP",
    # v2 servers (NativeMCPBase)
    "DistributedSearchMCPv2",
    "DistributedResearchMCPv2",
    "DistributedPublishMCPv2",
    "DistributedMemoryMCPv2",
    # media/document servers
    "DistributedVideoQAMCP",
    "DistributedDocumentQAMCP",
]
