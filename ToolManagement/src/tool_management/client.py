"""
Tool Management Service Client.

Used by native MCPs and external services to register tools with the central registry.
Tools registered via this client become globally available at tools.distributed.systems.

Input Convention:
    - `input: Dict` - single execution input (Pydantic-style object)
    - `inputs: List[Dict]` - batch execution (list of input objects)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, TypeVar
from functools import wraps

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ToolSchema(BaseModel):
    """Schema for a registered tool."""
    name: str
    description: str
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Optional[Dict[str, Any]] = None


class ToolRegistration(BaseModel):
    """Tool registration payload."""
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Optional[Dict[str, Any]] = None
    provider_id: str
    provider_url: str
    execute_endpoint: str = "/execute"
    mcp_endpoint: str = "/mcp"
    namespace: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ServiceRegistration(BaseModel):
    """Service registration payload."""
    service_id: str
    name: str
    base_url: str
    service_type: str = "mcp"
    version: str = "1.0.0"
    capabilities: List[str] = Field(default_factory=list)
    tool_names: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


@dataclass
class RegisteredTool:
    """A tool registered with the service."""
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Callable
    output_schema: Optional[Dict[str, Any]] = None
    namespace: Optional[str] = None
    tags: List[str] = field(default_factory=list)


class ToolManagementClient:
    """
    Client for registering tools with the Tool Management service.

    Usage:
        client = ToolManagementClient(
            registry_url="https://tools.distributed.systems",
            provider_id="my-service",
            provider_url="https://my-service.modal.run"
        )

        @client.tool(
            name="search",
            description="Search the web",
            input_schema={...}
        )
        async def search(query: str) -> dict:
            ...

        # Register all tools on startup
        await client.register_all()
    """

    def __init__(
        self,
        registry_url: str = "https://tools.distributed.systems",
        provider_id: Optional[str] = None,
        provider_url: Optional[str] = None,
        namespace: Optional[str] = None,
        auto_register: bool = True,
    ):
        self.registry_url = registry_url.rstrip("/")
        self.provider_id = provider_id or os.getenv("SERVICE_ID", "unknown")
        self.provider_url = provider_url or os.getenv("SERVICE_URL", "")
        self.namespace = namespace
        self.auto_register = auto_register

        self._tools: Dict[str, RegisteredTool] = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._registered = False

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    def tool(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ):
        """
        Decorator to register a function as a tool.

        @client.tool(
            name="search",
            description="Search the web for information",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["query"]
            }
        )
        async def search(query: str) -> dict:
            ...
        """
        def decorator(func: Callable) -> Callable:
            tool_name = name or func.__name__
            tool_desc = description or func.__doc__ or f"Execute {tool_name}"

            # Try to infer schema from type hints if not provided
            schema = input_schema
            if schema is None:
                schema = self._infer_input_schema(func)

            self._tools[tool_name] = RegisteredTool(
                name=tool_name,
                description=tool_desc,
                input_schema=schema,
                handler=func,
                output_schema=output_schema,
                namespace=self.namespace,
                tags=tags or [],
            )

            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            return wrapper
        return decorator

    def _infer_input_schema(self, func: Callable) -> Dict[str, Any]:
        """Infer JSON schema from function type hints."""
        import inspect
        from typing import get_type_hints

        schema = {
            "type": "object",
            "properties": {},
            "required": []
        }

        try:
            hints = get_type_hints(func)
            sig = inspect.signature(func)

            for param_name, param in sig.parameters.items():
                if param_name in ("self", "cls"):
                    continue

                param_type = hints.get(param_name, str)
                json_type = self._python_type_to_json(param_type)

                schema["properties"][param_name] = {
                    "type": json_type,
                    "description": f"Parameter {param_name}"
                }

                if param.default is inspect.Parameter.empty:
                    schema["required"].append(param_name)

        except Exception as e:
            logger.debug(f"Could not infer schema: {e}")

        return schema

    def _python_type_to_json(self, python_type) -> str:
        """Convert Python type to JSON schema type."""
        type_map = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            list: "array",
            dict: "object",
        }
        return type_map.get(python_type, "string")

    async def register_service(self) -> bool:
        """Register this service with the registry."""
        if not self.provider_url:
            logger.warning("No provider_url set, skipping service registration")
            return False

        client = await self._get_client()

        registration = ServiceRegistration(
            service_id=self.provider_id,
            name=self.provider_id.replace("-", " ").title(),
            base_url=self.provider_url,
            service_type="mcp",
            capabilities=[t.name for t in self._tools.values()],
            tool_names=list(self._tools.keys()),
            metadata={
                "namespace": self.namespace,
                "registered_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        try:
            response = await client.post(
                f"{self.registry_url}/api/v1/services",
                json=registration.model_dump(),
            )

            if response.status_code in (200, 201):
                logger.info(f"Registered service {self.provider_id} with registry")
                return True
            else:
                logger.warning(f"Service registration returned {response.status_code}: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to register service: {e}")
            return False

    async def register_tool(self, tool: RegisteredTool) -> bool:
        """Register a single tool with the registry."""
        if not self.provider_url:
            logger.warning("No provider_url set, skipping tool registration")
            return False

        client = await self._get_client()

        full_name = f"{self.namespace}:{tool.name}" if self.namespace else tool.name

        registration = ToolRegistration(
            name=full_name,
            description=tool.description,
            input_schema=tool.input_schema,
            output_schema=tool.output_schema,
            provider_id=self.provider_id,
            provider_url=self.provider_url,
            namespace=self.namespace,
            tags=tool.tags,
            metadata={
                "registered_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        try:
            response = await client.post(
                f"{self.registry_url}/api/v1/tools",
                json=registration.model_dump(),
            )

            if response.status_code in (200, 201):
                logger.info(f"Registered tool {full_name}")
                return True
            else:
                logger.warning(f"Tool registration returned {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Failed to register tool {tool.name}: {e}")
            return False

    async def register_all(self) -> Dict[str, bool]:
        """Register service and all tools with the registry."""
        results = {"service": await self.register_service()}

        for name, tool in self._tools.items():
            results[name] = await self.register_tool(tool)

        self._registered = True
        return results

    async def deregister(self) -> bool:
        """Deregister this service from the registry."""
        client = await self._get_client()

        try:
            response = await client.delete(
                f"{self.registry_url}/api/v1/services/{self.provider_id}"
            )
            return response.status_code in (200, 204)
        except Exception as e:
            logger.error(f"Failed to deregister: {e}")
            return False

    async def execute(self, tool_name: str, inputs: Dict[str, Any]) -> Any:
        """Execute a registered tool locally."""
        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")
        return await tool.handler(**inputs)

    def get_tools(self) -> List[Dict[str, Any]]:
        """Get list of registered tools in MCP format."""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            }
            for tool in self._tools.values()
        ]

    def get_tool_handlers(self) -> Dict[str, Callable]:
        """Get mapping of tool names to handlers."""
        return {name: tool.handler for name, tool in self._tools.items()}


# Convenience function to create a client
def create_client(
    registry_url: str = "https://tools.distributed.systems",
    provider_id: Optional[str] = None,
    provider_url: Optional[str] = None,
    namespace: Optional[str] = None,
) -> ToolManagementClient:
    """Create a Tool Management client."""
    return ToolManagementClient(
        registry_url=registry_url,
        provider_id=provider_id,
        provider_url=provider_url,
        namespace=namespace,
    )
