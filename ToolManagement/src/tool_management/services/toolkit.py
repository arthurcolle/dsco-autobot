"""
Toolkit - Agent-specific tool configurations.

Provides:
- Mix and match tools from multiple services
- Rename tools per agent/persona
- Contextualized toolkits for agent personas
- Tool access control per agent

Example:
    # Research Agent gets a curated toolkit
    research_toolkit = Toolkit(
        name="Research Tools",
        tools={
            "search": ToolBinding(service="search-api", tool="web_search"),
            "read": ToolBinding(service="search-api", tool="read_url"),
            "deep_search": ToolBinding(service="advanced-search-api", tool="search"),
            "extract": ToolBinding(service="advanced-search-api", tool="extract"),
        }
    )

    # The agent sees: search, read, deep_search, extract
    # No vendor details, no service complexity
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# =============================================================================
# TOOL BINDING
# =============================================================================

class ToolBinding(BaseModel):
    """Binds an agent-facing tool name to a service tool."""
    # Source (internal)
    service_id: str
    tool_name: str  # Original tool name in service

    # Presentation (what agent sees)
    alias: Optional[str] = None  # Renamed tool name
    description_override: Optional[str] = None  # Custom description

    # Behavior
    enabled: bool = True
    require_confirmation: bool = False  # Require human approval

    # Cost
    cost_multiplier: float = 1.0  # For premium features

    @property
    def display_name(self) -> str:
        return self.alias or self.tool_name

    @property
    def tool_id(self) -> str:
        return f"{self.service_id}:{self.tool_name}"


class ToolkitTool(BaseModel):
    """Tool as presented to an agent."""
    name: str  # What the agent calls it
    description: str
    input_schema: Dict[str, Any]

    # Internal routing
    service_id: str
    original_name: str

    # Metadata
    category: Optional[str] = None
    tags: List[str] = []


# =============================================================================
# TOOLKIT
# =============================================================================

class Toolkit(BaseModel):
    """
    A curated set of tools for an agent persona.

    Toolkits let you:
    - Combine tools from multiple services (Jina + Parallel)
    - Rename tools to fit the agent's context
    - Override descriptions for better agent understanding
    - Control access and costs

    Example personas:
    - Research Agent: search, read, deep_search, extract, verify
    - Data Agent: extract, find_all, monitor, store
    - Content Agent: search, read, summarize, fact_check
    """
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""

    # Tool bindings: agent_tool_name -> binding
    tools: Dict[str, ToolBinding] = Field(default_factory=dict)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Access control
    organization_id: Optional[str] = None
    public: bool = False  # Available to all agents in org

    def add_tool(
        self,
        agent_name: str,
        service_id: str,
        tool_name: str,
        description_override: Optional[str] = None,
    ) -> "Toolkit":
        """Add a tool to the toolkit."""
        self.tools[agent_name] = ToolBinding(
            service_id=service_id,
            tool_name=tool_name,
            alias=agent_name,
            description_override=description_override,
        )
        self.updated_at = datetime.utcnow()
        return self

    def remove_tool(self, agent_name: str) -> "Toolkit":
        """Remove a tool from the toolkit."""
        self.tools.pop(agent_name, None)
        self.updated_at = datetime.utcnow()
        return self

    def get_binding(self, agent_tool_name: str) -> Optional[ToolBinding]:
        """Get the binding for an agent-facing tool name."""
        return self.tools.get(agent_tool_name)

    def list_tools(self) -> List[str]:
        """List all agent-facing tool names."""
        return [name for name, binding in self.tools.items() if binding.enabled]

    def resolve(self, agent_tool_name: str) -> Optional[tuple[str, str]]:
        """Resolve agent tool name to (service_id, tool_name)."""
        binding = self.tools.get(agent_tool_name)
        if binding and binding.enabled:
            return (binding.service_id, binding.tool_name)
        return None


# =============================================================================
# TOOLKIT TEMPLATES
# =============================================================================

def create_research_toolkit() -> Toolkit:
    """
    Pre-configured toolkit for research agents.

    Combines Search API and Advanced Search API tools with
    intuitive names for research workflows.
    """
    return Toolkit(
        name="Research Toolkit",
        description="Tools for web research, fact-checking, and information extraction",
        tools={
            # From Search API (our wrapped Jina)
            "search": ToolBinding(
                service_id="search-api",
                tool_name="search",  # Our name for web_search
                alias="search",
                description_override="Search the web for information",
            ),
            "read": ToolBinding(
                service_id="search-api",
                tool_name="read",  # Our name for read_url
                alias="read",
                description_override="Read and extract content from a URL",
            ),
            "verify": ToolBinding(
                service_id="search-api",
                tool_name="verify",  # Our name for fact_check
                alias="verify",
                description_override="Verify claims and check facts",
            ),
            # From Advanced Search API (our wrapped Parallel)
            "deep_search": ToolBinding(
                service_id="advanced-search-api",
                tool_name="deep_search",  # Our name for search
                alias="deep_search",
                description_override="Perform deep, comprehensive web search with autonomous browsing",
            ),
            "extract": ToolBinding(
                service_id="advanced-search-api",
                tool_name="extract_data",  # Our name for extract
                alias="extract",
                description_override="Extract structured data from web pages",
            ),
        },
    )


def create_data_toolkit() -> Toolkit:
    """
    Pre-configured toolkit for data extraction agents.

    Focused on extracting, monitoring, and processing data
    from web sources.
    """
    return Toolkit(
        name="Data Toolkit",
        description="Tools for data extraction, monitoring, and batch processing",
        tools={
            # From Search API
            "read": ToolBinding(
                service_id="search-api",
                tool_name="read",
                alias="read",
                description_override="Read content from any URL",
            ),
            "embed": ToolBinding(
                service_id="search-api",
                tool_name="embed",
                alias="embed",
                description_override="Generate embeddings for semantic search",
            ),
            # From Advanced Search API
            "extract": ToolBinding(
                service_id="advanced-search-api",
                tool_name="extract_data",
                alias="extract",
                description_override="Extract structured data from any URL",
            ),
            "find_all": ToolBinding(
                service_id="advanced-search-api",
                tool_name="find_all",
                alias="find_all",
                description_override="Find all matching items across sources",
            ),
            "monitor": ToolBinding(
                service_id="advanced-search-api",
                tool_name="create_monitor",
                alias="monitor",
                description_override="Set up monitoring for data changes",
            ),
        },
    )


def create_task_toolkit() -> Toolkit:
    """
    Pre-configured toolkit for autonomous task agents.

    Enables agents to spawn and orchestrate long-running tasks.
    """
    return Toolkit(
        name="Task Toolkit",
        description="Tools for autonomous task execution and orchestration",
        tools={
            # From Advanced Search API (task capabilities)
            "run_task": ToolBinding(
                service_id="advanced-search-api",
                tool_name="run_task",
                alias="run_task",
                description_override="Execute an autonomous task with full browser capabilities",
            ),
            "create_task_group": ToolBinding(
                service_id="advanced-search-api",
                tool_name="create_task_group",
                alias="create_task_group",
                description_override="Create a group of related tasks for batch execution",
            ),
            "list_tasks": ToolBinding(
                service_id="advanced-search-api",
                tool_name="list_tasks",
                alias="list_tasks",
                description_override="List all tasks with their status and results",
            ),
            # From Search API for quick lookups
            "search": ToolBinding(
                service_id="search-api",
                tool_name="search",
                alias="search",
                description_override="Quick web search for context",
            ),
            "read": ToolBinding(
                service_id="search-api",
                tool_name="read",
                alias="read",
                description_override="Read a specific URL",
            ),
        },
    )


def create_full_toolkit() -> Toolkit:
    """
    Complete toolkit with all available tools.

    For power users and advanced agents that need everything.
    """
    return Toolkit(
        name="Full Toolkit",
        description="Complete access to all Search API and Advanced Search API tools",
        tools={
            # Search API (Jina-based)
            "search": ToolBinding(service_id="search-api", tool_name="search"),
            "read": ToolBinding(service_id="search-api", tool_name="read"),
            "verify": ToolBinding(service_id="search-api", tool_name="verify"),
            "rerank": ToolBinding(service_id="search-api", tool_name="rerank"),
            "embed": ToolBinding(service_id="search-api", tool_name="embed"),

            # Advanced Search API (Parallel-based)
            "deep_search": ToolBinding(service_id="advanced-search-api", tool_name="deep_search"),
            "extract": ToolBinding(service_id="advanced-search-api", tool_name="extract_data"),
            "find_all": ToolBinding(service_id="advanced-search-api", tool_name="find_all"),
            "run_task": ToolBinding(service_id="advanced-search-api", tool_name="run_task"),
            "create_task_group": ToolBinding(service_id="advanced-search-api", tool_name="create_task_group"),
            "list_tasks": ToolBinding(service_id="advanced-search-api", tool_name="list_tasks"),
            "monitor": ToolBinding(service_id="advanced-search-api", tool_name="create_monitor"),
        },
    )


# =============================================================================
# TOOLKIT MANAGER
# =============================================================================

class ToolkitManager:
    """
    Manages toolkits and resolves tool calls.

    Provides:
    - Toolkit CRUD
    - Tool resolution (agent name -> service/tool)
    - Schema generation for agents
    """

    def __init__(self):
        self.toolkits: Dict[str, Toolkit] = {}
        self._service_schemas: Dict[str, Dict[str, Dict]] = {}  # service -> tool -> schema

    def register_toolkit(self, toolkit: Toolkit):
        """Register a toolkit."""
        self.toolkits[toolkit.id] = toolkit
        logger.info(f"Registered toolkit: {toolkit.name} ({len(toolkit.tools)} tools)")

    def get_toolkit(self, toolkit_id: str) -> Optional[Toolkit]:
        """Get toolkit by ID."""
        return self.toolkits.get(toolkit_id)

    def list_toolkits(
        self,
        organization_id: Optional[str] = None,
    ) -> List[Toolkit]:
        """List available toolkits."""
        toolkits = list(self.toolkits.values())
        if organization_id:
            toolkits = [
                t for t in toolkits
                if t.public or t.organization_id == organization_id
            ]
        return toolkits

    def register_service_schemas(
        self,
        service_id: str,
        tools: Dict[str, Dict[str, Any]],
    ):
        """Register tool schemas from a service."""
        self._service_schemas[service_id] = tools

    def get_toolkit_tools(
        self,
        toolkit_id: str,
    ) -> List[ToolkitTool]:
        """Get all tools in a toolkit with full schemas."""
        toolkit = self.toolkits.get(toolkit_id)
        if not toolkit:
            return []

        tools = []
        for agent_name, binding in toolkit.tools.items():
            if not binding.enabled:
                continue

            # Get original schema
            service_tools = self._service_schemas.get(binding.service_id, {})
            original_schema = service_tools.get(binding.tool_name, {})

            tools.append(ToolkitTool(
                name=agent_name,
                description=binding.description_override or original_schema.get("description", ""),
                input_schema=original_schema.get("input_schema", {}),
                service_id=binding.service_id,
                original_name=binding.tool_name,
                category=original_schema.get("category"),
                tags=original_schema.get("tags", []),
            ))

        return tools

    def resolve_tool(
        self,
        toolkit_id: str,
        agent_tool_name: str,
    ) -> Optional[tuple[str, str]]:
        """
        Resolve an agent's tool call to the actual service/tool.

        Returns (service_id, tool_name) or None if not found.
        """
        toolkit = self.toolkits.get(toolkit_id)
        if not toolkit:
            return None
        return toolkit.resolve(agent_tool_name)


# =============================================================================
# AGENT TOOLKIT ASSIGNMENT
# =============================================================================

class AgentToolkitConfig(BaseModel):
    """Configuration linking an agent to its toolkit."""
    agent_id: str
    toolkit_id: str

    # Overrides at agent level
    disabled_tools: Set[str] = set()  # Tools to disable for this agent
    tool_overrides: Dict[str, ToolBinding] = {}  # Additional customization

    # Limits
    max_tool_calls_per_session: Optional[int] = None
    max_cost_per_session_cents: Optional[int] = None

    def get_effective_tools(self, toolkit: Toolkit) -> Dict[str, ToolBinding]:
        """Get tools available to this agent."""
        tools = {}
        for name, binding in toolkit.tools.items():
            if name in self.disabled_tools:
                continue
            if name in self.tool_overrides:
                tools[name] = self.tool_overrides[name]
            else:
                tools[name] = binding
        return tools
