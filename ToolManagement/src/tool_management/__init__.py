"""
Tool Management - A unified system for registering, discovering, and executing tools.

Includes:
- Core tool models and registry
- Enhanced @tools.endpoint() and @tools.snippet() decorators
- MCP integrations for Jina AI, Parallel AI, and more
- White-label service wrappers with durable storage
- DSPy pipeline-based compound tools
- Autonomous agent infrastructure
- Tool discovery engine
- Tool mesh and interconnectivity
- Graph management extensions
- HATEOAS utilities
"""

from .models.tool import Tool, ToolInput, ToolOutput, ToolMetadata
from .registry.registry import ToolRegistry, tool, get_registry
from .tools_api import (
    Tools,
    ToolType,
    ToolVisibility,
    ToolDefinition,
    ToolInputSchema,
    ToolOutputSchema,
    ToolAction,
    ActionType,
)

__version__ = "1.0.0"

__all__ = [
    # Core models
    "Tool",
    "ToolInput",
    "ToolOutput",
    "ToolMetadata",
    # Simple registry
    "ToolRegistry",
    "tool",
    "get_registry",
    # Enhanced Tools API
    "Tools",
    "ToolType",
    "ToolVisibility",
    "ToolDefinition",
    "ToolInputSchema",
    "ToolOutputSchema",
    "ToolAction",
    "ActionType",
]

# Lazy imports for integrations (to avoid circular imports)
def __getattr__(name):
    # Integration adapters
    if name in ("IntegrationServer", "JinaAdapter", "ParallelAdapter", "MCPAdapter", "create_integration_server",
                "MCPRegistry", "MCPServer", "JinaProvider"):
        from .integrations import (
            IntegrationServer, JinaAdapter, ParallelAdapter, MCPAdapter,
            MCPRegistry, MCPServer, JinaProvider
        )
        from .integrations.integration_server import create_integration_server
        return {
            "IntegrationServer": IntegrationServer,
            "JinaAdapter": JinaAdapter,
            "ParallelAdapter": ParallelAdapter,
            "MCPAdapter": MCPAdapter,
            "create_integration_server": create_integration_server,
            "MCPRegistry": MCPRegistry,
            "MCPServer": MCPServer,
            "JinaProvider": JinaProvider,
        }[name]

    # Compound tools
    if name in ("CompoundToolRegistry", "get_compound_registry"):
        from .compound import CompoundToolRegistry, get_compound_registry
        return {
            "CompoundToolRegistry": CompoundToolRegistry,
            "get_compound_registry": get_compound_registry,
        }[name]

    # Agents
    if name in ("AgentCapability", "AgentState", "AgentOrchestrator", "EmbeddingManager"):
        from .agents import AgentCapability, AgentState, AgentOrchestrator, EmbeddingManager
        return {
            "AgentCapability": AgentCapability,
            "AgentState": AgentState,
            "AgentOrchestrator": AgentOrchestrator,
            "EmbeddingManager": EmbeddingManager,
        }[name]

    # Discovery
    if name in ("AdvancedToolDiscoveryEngine", "ToolRecommendation", "setup_auto_discovery"):
        from .discovery import AdvancedToolDiscoveryEngine, ToolRecommendation, setup_auto_discovery
        return {
            "AdvancedToolDiscoveryEngine": AdvancedToolDiscoveryEngine,
            "ToolRecommendation": ToolRecommendation,
            "setup_auto_discovery": setup_auto_discovery,
        }[name]

    # Mesh
    if name in ("InterconnectivityManager", "ToolFederation", "EventBus", "ToolChain"):
        from .mesh import InterconnectivityManager, ToolFederation, EventBus, ToolChain
        return {
            "InterconnectivityManager": InterconnectivityManager,
            "ToolFederation": ToolFederation,
            "EventBus": EventBus,
            "ToolChain": ToolChain,
        }[name]

    # Extensions
    if name in ("GraphManagementExtensions",):
        from .extensions import GraphManagementExtensions
        return GraphManagementExtensions

    # Utils
    if name in ("HATEOASBuilder", "HALResource", "LinkRelation"):
        from .utils import HATEOASBuilder, HALResource, LinkRelation
        return {
            "HATEOASBuilder": HATEOASBuilder,
            "HALResource": HALResource,
            "LinkRelation": LinkRelation,
        }[name]

    # Services
    if name in ("GraphManagementClient", "ToolManagementAPI"):
        from .services import GraphManagementClient, ToolManagementAPI
        return {
            "GraphManagementClient": GraphManagementClient,
            "ToolManagementAPI": ToolManagementAPI,
        }[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
