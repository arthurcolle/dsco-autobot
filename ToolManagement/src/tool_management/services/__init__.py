"""
White-label Tool Services.

Deployable services that wrap vendor integrations with:
- Durable request/response storage
- Vendor abstraction (clients see your brand)
- Usage tracking for billing
- Central registry integration
- Agent management hooks
- Contextualized toolkits for agent personas
- Graph management client
- Tool management API client
"""

from .durable_store import DurableStore, ToolRequest, ToolResponse, RequestStatus
from .service_wrapper import ServiceWrapper, ServiceConfig, EconomicPolicy
from .registry_client import RegistryClient, RegisteredTool, InMemoryRegistry
from .billing_hooks import (
    BillingHook,
    BillingManager,
    AgentHook,
    AgentContext,
    UsageRecord,
    NoOpBillingHook,
    InMemoryBillingHook,
    WebhookBillingHook,
)
from .toolkit import (
    Toolkit,
    ToolBinding,
    ToolkitTool,
    ToolkitManager,
    AgentToolkitConfig,
    create_research_toolkit,
    create_data_toolkit,
    create_task_toolkit,
    create_full_toolkit,
)
from .graph_client import GraphManagementClient
from .tool_client import ToolManagementAPI

__all__ = [
    # Storage
    "DurableStore",
    "ToolRequest",
    "ToolResponse",
    "RequestStatus",
    # Service wrapper
    "ServiceWrapper",
    "ServiceConfig",
    "EconomicPolicy",
    # Registry
    "RegistryClient",
    "RegisteredTool",
    "InMemoryRegistry",
    # Billing
    "BillingHook",
    "BillingManager",
    "AgentHook",
    "AgentContext",
    "UsageRecord",
    "NoOpBillingHook",
    "InMemoryBillingHook",
    "WebhookBillingHook",
    # Toolkits
    "Toolkit",
    "ToolBinding",
    "ToolkitTool",
    "ToolkitManager",
    "AgentToolkitConfig",
    "create_research_toolkit",
    "create_data_toolkit",
    "create_task_toolkit",
    "create_full_toolkit",
    # Clients
    "GraphManagementClient",
    "ToolManagementAPI",
]
