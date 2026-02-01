"""
Distributed Research MCP - Native MCP server for deep research and parallel task execution.

Built on Parallel AI capabilities with native MCP protocol support.

v2: Migrated to NativeMCPBase with cross-MCP mesh communication and embedding support.
"""

# Use lazy imports to avoid double-module issues when running with python -m
def __getattr__(name):
    """Lazy import to prevent module duplication when run with python -m."""
    if name == "DistributedResearchMCP":
        from .server import DistributedResearchMCP
        return DistributedResearchMCP
    elif name == "create_server":
        from .server import create_server
        return create_server
    elif name == "app":
        from .server import app
        return app
    elif name == "DistributedResearchMCPv2":
        from .server_v2 import DistributedResearchMCPv2
        return DistributedResearchMCPv2
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["DistributedResearchMCP", "DistributedResearchMCPv2", "create_server", "app"]
