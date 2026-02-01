"""
Core tool models for the Tool Management system.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from pydantic import BaseModel, Field
import uuid
import inspect


class ToolVisibility(str, Enum):
    """Tool visibility levels."""
    PUBLIC = "public"
    PRIVATE = "private"
    ORGANIZATION = "organization"


class ToolStatus(str, Enum):
    """Tool operational status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    DISABLED = "disabled"
    DRAFT = "draft"


class ToolInput(BaseModel):
    """Schema for tool input parameters."""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="Parameter type (string, number, boolean, object, array)")
    description: str = Field("", description="Parameter description")
    required: bool = Field(True, description="Whether the parameter is required")
    default: Optional[Any] = Field(None, description="Default value if not required")
    enum: Optional[List[Any]] = Field(None, description="Allowed values")

    class Config:
        extra = "allow"


class ToolOutput(BaseModel):
    """Schema for tool output."""
    type: str = Field("object", description="Output type")
    description: str = Field("", description="Output description")
    schema_: Optional[Dict[str, Any]] = Field(None, alias="schema", description="JSON schema for output")

    class Config:
        extra = "allow"
        populate_by_name = True


class ToolMetadata(BaseModel):
    """Extended metadata for a tool."""
    author: Optional[str] = None
    license: Optional[str] = None
    homepage: Optional[str] = None
    repository: Optional[str] = None
    documentation_url: Optional[str] = None
    pricing_tier: Optional[str] = None
    rate_limit: Optional[Dict[str, Any]] = None
    timeout_ms: int = Field(30000, description="Default timeout in milliseconds")
    retry_policy: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class Tool(BaseModel):
    """
    Complete tool definition.

    A Tool represents a callable operation that can be discovered,
    registered, and executed through the Tool Management system.
    """

    # Identity
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique tool identifier")
    name: str = Field(..., description="Tool name (must be unique within scope)")
    description: str = Field(..., description="What the tool does")
    version: str = Field("1.0.0", description="Semantic version")

    # Schema
    inputs: List[ToolInput] = Field(default_factory=list, description="Input parameters")
    output: Optional[ToolOutput] = Field(None, description="Output schema")

    # Categorization
    tags: List[str] = Field(default_factory=list, description="Tags for discovery")
    category: Optional[str] = Field(None, description="Primary category")

    # Ownership
    user_id: Optional[str] = Field(None, description="Owner user ID")
    organization_id: Optional[str] = Field(None, description="Owning organization")
    toolkit_id: Optional[str] = Field(None, description="Parent toolkit/collection")

    # Access control
    visibility: ToolVisibility = Field(ToolVisibility.PRIVATE, description="Visibility level")
    status: ToolStatus = Field(ToolStatus.ACTIVE, description="Operational status")

    # Execution
    service_url: Optional[str] = Field(None, description="Service URL for remote tools")
    endpoint: Optional[str] = Field(None, description="API endpoint path")
    handler: Optional[str] = Field(None, description="Local handler function path")

    # Metadata
    metadata: ToolMetadata = Field(default_factory=ToolMetadata)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Stats
    usage_count: int = Field(0, description="Execution count")
    rating: Optional[float] = Field(None, ge=0, le=5, description="Average rating")

    # Internal
    _callable: Optional[Callable] = None

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert inputs to JSON Schema format."""
        properties = {}
        required = []

        for inp in self.inputs:
            prop = {
                "type": inp.type,
                "description": inp.description,
            }
            if inp.enum:
                prop["enum"] = inp.enum
            if inp.default is not None:
                prop["default"] = inp.default
            properties[inp.name] = prop

            if inp.required:
                required.append(inp.name)

        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }

    def to_mcp_format(self) -> Dict[str, Any]:
        """Convert to MCP tool format."""
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.to_json_schema(),
        }

    @classmethod
    def from_function(
        cls,
        func: Callable,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        **kwargs
    ) -> "Tool":
        """
        Create a Tool from a Python function using introspection.

        Args:
            func: The function to wrap as a tool
            name: Override function name
            description: Override docstring
            tags: Tags for categorization
            category: Primary category
            **kwargs: Additional Tool fields
        """
        sig = inspect.signature(func)
        hints = func.__annotations__

        inputs = []
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue

            param_type = "string"
            if param_name in hints:
                hint = hints[param_name]
                param_type = _python_type_to_json(hint)

            inputs.append(ToolInput(
                name=param_name,
                type=param_type,
                required=param.default is inspect.Parameter.empty,
                default=None if param.default is inspect.Parameter.empty else param.default,
            ))

        # Parse output type
        output = None
        if "return" in hints:
            output = ToolOutput(
                type=_python_type_to_json(hints["return"]),
                description="Function return value",
            )

        tool = cls(
            name=name or func.__name__,
            description=description or func.__doc__ or f"Tool: {func.__name__}",
            inputs=inputs,
            output=output,
            tags=tags or [],
            category=category,
            handler=f"{func.__module__}.{func.__qualname__}",
            **kwargs
        )
        tool._callable = func
        return tool

    async def execute(self, **input_data) -> Any:
        """Execute the tool with given inputs."""
        # Local callable execution
        if self._callable:
            result = self._callable(**input_data)
            if inspect.iscoroutine(result):
                return await result
            return result

        # Remote HTTP execution
        if self.service_url and self.endpoint:
            return await self._execute_remote(**input_data)

        # Handler path resolution
        if self.handler:
            return await self._execute_from_handler(**input_data)

        raise NotImplementedError("Tool has no callable handler")

    async def _execute_remote(self, **input_data) -> Any:
        """Execute tool via remote HTTP call."""
        import httpx

        if not self.service_url or not self.endpoint:
            raise ValueError("service_url and endpoint are required for remote execution")

        url = f"{self.service_url.rstrip('/')}{self.endpoint}"
        # Handle both dict and ToolMetadata object for timeout
        if self.metadata:
            if isinstance(self.metadata, dict):
                timeout = self.metadata.get("timeout_ms", 30000) / 1000
            else:
                timeout = getattr(self.metadata, "timeout_ms", 30000) / 1000
        else:
            timeout = 30

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    url,
                    json=input_data,
                    headers={"Content-Type": "application/json"},
                    timeout=timeout
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                return {
                    "error": str(e),
                    "status_code": e.response.status_code
                }
            except Exception as e:
                return {"error": str(e)}

    async def _execute_from_handler(self, **input_data) -> Any:
        """Resolve and execute handler from module path."""
        import importlib

        if not self.handler:
            raise ValueError("No handler path specified")

        parts = self.handler.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid handler path: {self.handler}")

        module_path, func_name = parts

        try:
            module = importlib.import_module(module_path)
            handler = getattr(module, func_name)
            self._callable = handler

            result = handler(**input_data)
            if inspect.iscoroutine(result):
                return await result
            return result
        except ImportError as e:
            raise ImportError(f"Could not import handler module: {module_path}") from e
        except AttributeError as e:
            raise AttributeError(f"Handler function not found: {func_name}") from e


def _python_type_to_json(py_type) -> str:
    """Convert Python type hint to JSON Schema type."""
    type_map = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
        type(None): "null",
    }

    origin = getattr(py_type, "__origin__", None)
    if origin is list:
        return "array"
    if origin is dict:
        return "object"
    if origin is Union:
        args = py_type.__args__
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _python_type_to_json(non_none[0])

    return type_map.get(py_type, "string")
