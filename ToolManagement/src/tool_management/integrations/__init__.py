"""MCP Server Integrations - Adapters for external tool providers."""

from .mcp_adapter import MCPAdapter, MCPServerConfig
from .jina_adapter import JinaAdapter
from .parallel_adapter import ParallelAdapter
from .integration_server import IntegrationServer, create_integration_server

# Extended MCP and Jina support
from .mcp_registry import MCPRegistry, get_registry as get_mcp_registry
from .mcp_server import MCPServer, MCPWebSocketHandler
from .jina_provider import JinaProvider, get_jina
from .codex_apps_adapter import CodexAppsCatalogAdapter, get_codex_apps_adapter
from .codex_apps_runtime import CodexAppsRuntime, get_codex_apps_runtime
from .codex_apps_vault import CodexAppsVault, get_codex_apps_vault
from .codex_apps_oauth import CodexAppsOAuth, get_codex_apps_oauth

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
    # Codex Apps
    "CodexAppsCatalogAdapter",
    "get_codex_apps_adapter",
    "CodexAppsRuntime",
    "get_codex_apps_runtime",
    "CodexAppsVault",
    "get_codex_apps_vault",
    "CodexAppsOAuth",
    "get_codex_apps_oauth",
]
