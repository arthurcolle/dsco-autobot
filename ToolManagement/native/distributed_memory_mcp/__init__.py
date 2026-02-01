"""
Distributed Memory MCP - Cognitive memory architecture for AI workloads.

Port: 4004

Memory Types:
- Working Memory: Fast, ephemeral attention buffer
- Episodic Memory: Timestamped event sequences
- Semantic Memory: Factual knowledge graph
- Procedural Memory: Executable skills and patterns

This MCP provides the foundation for context-aware, learning AI systems.

v2: Migrated to NativeMCPBase with cross-MCP mesh communication and semantic search.
"""

from .server import DistributedMemoryMCP
from .server_v2 import DistributedMemoryMCPv2

__all__ = ["DistributedMemoryMCP", "DistributedMemoryMCPv2"]
