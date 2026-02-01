"""
Distributed Search MCP - Native MCP server for web search and content extraction.

Built on Jina AI capabilities with native MCP protocol support.

v2: Migrated to NativeMCPBase with cross-MCP mesh communication.
"""

from .server import DistributedSearchMCP, create_server, app
from .server_v2 import DistributedSearchMCPv2

__all__ = ["DistributedSearchMCP", "DistributedSearchMCPv2", "create_server", "app"]
