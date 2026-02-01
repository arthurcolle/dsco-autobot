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

# Original servers (v1)
from .distributed_search_mcp import DistributedSearchMCP
from .distributed_research_mcp import DistributedResearchMCP
from .distributed_publish_mcp import DistributedPublishMCP
from .distributed_memory_mcp import DistributedMemoryMCP

# v2 servers (built on NativeMCPBase)
from .distributed_search_mcp.server_v2 import DistributedSearchMCPv2
from .distributed_research_mcp.server_v2 import DistributedResearchMCPv2
from .distributed_publish_mcp.server_v2 import DistributedPublishMCPv2
from .distributed_memory_mcp.server_v2 import DistributedMemoryMCPv2

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
]
