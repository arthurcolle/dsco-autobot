"""
Base MCP Adapter - Common functionality for MCP server integration.

Provides:
- Tool discovery from MCP servers
- Automatic registration with our tool registry
- Tool execution proxying
- SSE and HTTP transport support
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type
from enum import Enum

import httpx
from pydantic import BaseModel, Field

from ..tools_api import (
    Tools,
    ToolDefinition,
    ToolType,
    ToolVisibility,
    ToolInputSchema,
    ToolOutputSchema,
    ToolAction,
    ActionType,
)

logger = logging.getLogger(__name__)


class MCPTransport(str, Enum):
    """MCP transport types."""
    SSE = "sse"
    HTTP = "http"
    WEBSOCKET = "websocket"
    STDIO = "stdio"


@dataclass
class MCPServerConfig:
    """Configuration for an MCP server."""
    name: str
    url: str
    transport: MCPTransport = MCPTransport.SSE
    api_key: Optional[str] = None
    api_key_header: str = "Authorization"
    api_key_prefix: str = "Bearer"
    timeout: int = 30
    tags: List[str] = field(default_factory=list)
    category: str = "mcp"
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPTool:
    """Discovered MCP tool."""
    name: str
    description: str
    input_schema: Dict[str, Any]
    server_name: str
    server_url: str


class MCPAdapter:
    """
    Base adapter for MCP servers.

    Handles:
    - Connecting to MCP servers
    - Discovering available tools
    - Registering tools with our registry
    - Proxying tool execution
    """

    def __init__(
        self,
        config: MCPServerConfig,
        tools_registry: Optional[Tools] = None,
    ):
        self.config = config
        self.tools_registry = tools_registry
        self._client: Optional[httpx.AsyncClient] = None
        self._discovered_tools: Dict[str, MCPTool] = {}
        self._connected = False

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def url(self) -> str:
        return self.config.url

    async def connect(self) -> bool:
        """Connect to the MCP server and discover tools."""
        try:
            self._client = httpx.AsyncClient(timeout=self.config.timeout)

            # Build headers
            headers = {}
            if self.config.api_key:
                if self.config.api_key_prefix:
                    headers[self.config.api_key_header] = f"{self.config.api_key_prefix} {self.config.api_key}"
                else:
                    headers[self.config.api_key_header] = self.config.api_key

            self._headers = headers

            # Discover tools
            await self._discover_tools()
            self._connected = True

            logger.info(f"Connected to MCP server: {self.name} ({len(self._discovered_tools)} tools)")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to MCP server {self.name}: {e}")
            return False

    async def disconnect(self):
        """Disconnect from the MCP server."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False

    async def _discover_tools(self):
        """Discover tools from the MCP server."""
        # Default implementation - subclasses can override
        try:
            # Try to get tools list
            response = await self._client.get(
                f"{self.config.url}/tools",
                headers=self._headers
            )

            if response.status_code == 200:
                data = response.json()
                tools = data.get("tools", data) if isinstance(data, dict) else data

                for tool_data in tools:
                    tool = MCPTool(
                        name=tool_data.get("name"),
                        description=tool_data.get("description", ""),
                        input_schema=tool_data.get("inputSchema", tool_data.get("input_schema", {})),
                        server_name=self.name,
                        server_url=self.config.url,
                    )
                    self._discovered_tools[tool.name] = tool

        except Exception as e:
            logger.warning(f"Could not discover tools from {self.name}: {e}")

    def get_discovered_tools(self) -> List[MCPTool]:
        """Get all discovered tools."""
        return list(self._discovered_tools.values())

    async def register_tools(self) -> List[ToolDefinition]:
        """Register discovered tools with the tools registry."""
        if not self.tools_registry:
            logger.warning("No tools registry configured")
            return []

        registered = []

        for mcp_tool in self._discovered_tools.values():
            try:
                tool_def = self._mcp_tool_to_definition(mcp_tool)

                # Store in registry
                self.tools_registry._tools[tool_def.id] = tool_def

                # Create execution handler
                handler = self._create_execution_handler(mcp_tool)
                self.tools_registry._handlers[tool_def.id] = handler

                registered.append(tool_def)
                logger.info(f"Registered MCP tool: {mcp_tool.name} from {self.name}")

            except Exception as e:
                logger.error(f"Failed to register tool {mcp_tool.name}: {e}")

        return registered

    def _mcp_tool_to_definition(self, mcp_tool: MCPTool) -> ToolDefinition:
        """Convert an MCP tool to our ToolDefinition."""
        import hashlib

        # Generate ID
        tool_id = f"mcp_{self.name}_{mcp_tool.name}_{hashlib.md5(mcp_tool.name.encode()).hexdigest()[:8]}"

        # Build input schema
        input_schema = ToolInputSchema(**mcp_tool.input_schema) if mcp_tool.input_schema else ToolInputSchema()

        # Build action
        action = ToolAction(
            type=ActionType.MCP,
            mcp={
                "server": self.name,
                "server_url": self.config.url,
                "tool_name": mcp_tool.name,
                "transport": self.config.transport.value,
            }
        )

        return ToolDefinition(
            id=tool_id,
            name=mcp_tool.name,
            description=mcp_tool.description,
            tool_type=ToolType.MCP,
            input_schema=input_schema,
            output_schema=ToolOutputSchema(),
            action=action,
            tags=self.config.tags + ["mcp", self.name],
            category=self.config.category,
            visibility=ToolVisibility.PUBLIC,
            service_name=self.name,
            service_url=self.config.url,
            metadata=self.config.metadata,
        )

    def _create_execution_handler(self, mcp_tool: MCPTool) -> Callable:
        """Create an execution handler for an MCP tool."""
        async def handler(**inputs) -> Any:
            return await self.execute_tool(mcp_tool.name, inputs)
        return handler

    async def execute_tool(self, tool_name: str, inputs: Dict[str, Any]) -> Any:
        """Execute an MCP tool."""
        if not self._connected:
            raise RuntimeError(f"Not connected to MCP server: {self.name}")

        mcp_tool = self._discovered_tools.get(tool_name)
        if not mcp_tool:
            raise ValueError(f"Tool not found: {tool_name}")

        # Default execution via HTTP POST
        try:
            response = await self._client.post(
                f"{self.config.url}/tools/{tool_name}/execute",
                json={"arguments": inputs},
                headers=self._headers,
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise RuntimeError(f"Tool execution failed: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Failed to execute tool {tool_name}: {e}")
            raise

    async def list_tools_mcp_format(self) -> List[Dict[str, Any]]:
        """Get tools in MCP format."""
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema,
            }
            for t in self._discovered_tools.values()
        ]

    def __repr__(self):
        status = "connected" if self._connected else "disconnected"
        return f"MCPAdapter({self.name}, {status}, {len(self._discovered_tools)} tools)"
