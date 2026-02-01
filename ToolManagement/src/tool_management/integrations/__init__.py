"""MCP Server Integrations - Adapters for external tool providers."""

from .mcp_adapter import MCPAdapter, MCPServerConfig
from .jina_adapter import JinaAdapter
from .parallel_adapter import ParallelAdapter
from .integration_server import IntegrationServer, create_integration_server

# Extended MCP and Jina support
from .mcp_registry import MCPRegistry, get_registry as get_mcp_registry
from .mcp_server import MCPServer, MCPWebSocketHandler
from .jina_provider import JinaProvider, get_jina

__all__ = [
    # Core adapters
    "MCPAdapter",
    "MCPServerConfig",
    "JinaAdapter",
    "ParallelAdapter",
    "IntegrationServer",
    "create_integration_server",
    # Extended MCP
    "MCPRegistry",
    "get_mcp_registry",
    "MCPServer",
    "MCPWebSocketHandler",
    # Extended Jina
    "JinaProvider",
    "get_jina",
]
