"""
Enhanced Tool Management API with @tools.endpoint() and @tools.snippet() decorators.

Supports:
- Pydantic input/output schemas with automatic extraction
- FastAPI endpoint registration
- Code snippet storage and execution
- Graph API integration for persistence
- MCP tool format export
- Semantic search via embeddings
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import textwrap
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .core.http_client import HTTPClientPool
from .core.timeout_config import TimeoutConfig

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


# =============================================================================
# ENUMS AND TYPES
# =============================================================================

class ToolType(str, Enum):
    """Type of tool registration."""
    ENDPOINT = "endpoint"
    SNIPPET = "snippet"
    FUNCTION = "function"
    MCP = "mcp"


class ToolVisibility(str, Enum):
    """Tool visibility levels."""
    PUBLIC = "public"
    PRIVATE = "private"
    ORGANIZATION = "organization"


class ActionType(str, Enum):
    """Type of tool action."""
    HTTP = "http"
    GRPC = "grpc"
    WEBSOCKET = "websocket"
    LAMBDA = "lambda"
    CODE = "code"
    MCP = "mcp"


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class ToolInputSchema(BaseModel):
    """Input schema for a tool."""
    type: str = "object"
    properties: Dict[str, Any] = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)

    class Config:
        extra = "allow"


class ToolOutputSchema(BaseModel):
    """Output schema for a tool."""
    type: str = "object"
    properties: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        extra = "allow"


class ToolAction(BaseModel):
    """Action configuration for tool execution."""
    type: ActionType = ActionType.HTTP
    http: Optional[Dict[str, Any]] = None
    code: Optional[str] = None
    mcp: Optional[Dict[str, Any]] = None


class ToolDefinition(BaseModel):
    """Complete tool definition."""
    id: str
    name: str
    description: str
    tool_type: ToolType = ToolType.FUNCTION

    # Schemas
    input_schema: ToolInputSchema = Field(default_factory=ToolInputSchema)
    output_schema: ToolOutputSchema = Field(default_factory=ToolOutputSchema)

    # Pydantic models (stored as schema)
    input_model_name: Optional[str] = None
    output_model_name: Optional[str] = None

    # Action
    action: ToolAction = Field(default_factory=ToolAction)

    # Metadata
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    visibility: ToolVisibility = ToolVisibility.PRIVATE
    version: str = "1.0.0"

    # Ownership
    user_id: Optional[str] = None
    organization_id: Optional[str] = None
    service_name: Optional[str] = None
    service_url: Optional[str] = None
    endpoint: Optional[str] = None

    # Code snippet (for snippet tools)
    code: Optional[str] = None
    code_hash: Optional[str] = None

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Stats
    usage_count: int = 0

    class Config:
        extra = "allow"


# =============================================================================
# SCHEMA EXTRACTION
# =============================================================================

def extract_pydantic_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    """Extract JSON schema from a Pydantic model."""
    try:
        # Pydantic v2
        return model.model_json_schema()
    except AttributeError:
        # Pydantic v1
        return model.schema()


def extract_function_schemas(func: Callable) -> tuple[ToolInputSchema, ToolOutputSchema, Optional[str], Optional[str]]:
    """
    Extract input and output schemas from a function's type hints.

    Returns:
        Tuple of (input_schema, output_schema, input_model_name, output_model_name)
    """
    sig = inspect.signature(func)
    hints = get_type_hints(func) if hasattr(func, '__annotations__') else {}

    input_properties = {}
    required = []
    input_model_name = None
    output_model_name = None

    for param_name, param in sig.parameters.items():
        if param_name in ("self", "cls"):
            continue

        annotation = hints.get(param_name, param.annotation)

        # Check if it's a Pydantic model (even if named 'request' or 'response')
        if annotation is not inspect.Parameter.empty and isinstance(annotation, type) and issubclass(annotation, BaseModel):
            # Use the Pydantic model's schema directly
            schema = extract_pydantic_schema(annotation)
            input_model_name = annotation.__name__

            # Also extract output schema if return type is Pydantic
            return_hint = hints.get("return")
            out_schema = ToolOutputSchema()
            if return_hint and isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
                out_schema = ToolOutputSchema(**extract_pydantic_schema(return_hint))
                output_model_name = return_hint.__name__

            return (
                ToolInputSchema(**schema),
                out_schema,
                input_model_name,
                output_model_name
            )

        # Extract individual parameter
        prop_schema = _python_type_to_schema(annotation)
        prop_schema["description"] = ""

        # Check for Field with description
        if param.default is not inspect.Parameter.empty:
            if hasattr(param.default, "description"):
                prop_schema["description"] = param.default.description
            if not isinstance(param.default, type):
                prop_schema["default"] = param.default
        else:
            required.append(param_name)

        input_properties[param_name] = prop_schema

    # Extract output schema
    output_schema = ToolOutputSchema()
    return_hint = hints.get("return")

    if return_hint:
        if isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
            output_schema = ToolOutputSchema(**extract_pydantic_schema(return_hint))
            output_model_name = return_hint.__name__
        else:
            output_schema = ToolOutputSchema(**_python_type_to_schema(return_hint))

    input_schema = ToolInputSchema(
        type="object",
        properties=input_properties,
        required=required
    )

    return input_schema, output_schema, input_model_name, output_model_name


def _python_type_to_schema(py_type) -> Dict[str, Any]:
    """Convert Python type to JSON Schema."""
    if py_type is None or py_type is type(None):
        return {"type": "null"}

    type_map = {
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
        list: {"type": "array"},
        dict: {"type": "object"},
        bytes: {"type": "string", "format": "binary"},
    }

    # Handle basic types
    if py_type in type_map:
        return type_map[py_type]

    # Handle Optional, List, Dict from typing
    origin = getattr(py_type, "__origin__", None)
    args = getattr(py_type, "__args__", ())

    if origin is Union:
        # Handle Optional[X] = Union[X, None]
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return _python_type_to_schema(non_none_args[0])
        return {"oneOf": [_python_type_to_schema(a) for a in non_none_args]}

    if origin is list:
        items_schema = _python_type_to_schema(args[0]) if args else {}
        return {"type": "array", "items": items_schema}

    if origin is dict:
        return {"type": "object"}

    # Default
    return {"type": "string"}


# =============================================================================
# TOOLS CLASS - MAIN API
# =============================================================================

class Tools:
    """
    Tool registration and management API.

    Usage:
        tools = Tools(
            service_url="http://localhost:8000",
            tool_api_url="http://tool-api.internal",
            graph_api_url="https://graph.brainchain.cloud"
        )

        # Register an endpoint
        @tools.endpoint(tags=["math"])
        async def add(request: AddRequest) -> AddResponse:
            return AddResponse(result=request.a + request.b)

        # Register a code snippet
        @tools.snippet(
            name="fetch_weather",
            description="Fetch weather for a city",
            input_schema={"city": "string"}
        )
        async def weather_snippet(city: str) -> dict:
            '''
            import httpx
            response = await httpx.get(f"https://api.weather.com/{city}")
            return response.json()
            '''
    """

    def __init__(
        self,
        service_name: str = "default",
        service_url: str = "http://localhost:8000",
        tool_api_url: Optional[str] = None,
        graph_api_url: Optional[str] = None,
        timeout: int = 30,
        http_pool: Optional[HTTPClientPool] = None,
        timeout_config: Optional[TimeoutConfig] = None,
    ):
        self.service_name = service_name
        self.service_url = service_url.rstrip("/")
        self.tool_api_url = tool_api_url.rstrip("/") if tool_api_url else None
        self.graph_api_url = graph_api_url.rstrip("/") if graph_api_url else None
        self.timeout = timeout

        # HTTP client pool and timeout config
        self._http_pool = http_pool
        self._timeout_config = timeout_config or TimeoutConfig(read=timeout, task_timeout=timeout)

        # Storage
        self._tools: Dict[str, ToolDefinition] = {}
        self._handlers: Dict[str, Callable] = {}
        self._snippets: Dict[str, str] = {}
        self._pending_registration: List[ToolDefinition] = []

    async def close(self):
        """Close is now a no-op since we use pooled clients."""
        pass  # Pooled clients are managed by HTTPClientPool

    def _get_client_from_pool(self) -> httpx.AsyncClient:
        """Get HTTP client from pool or create fallback client."""
        if self._http_pool:
            return self._http_pool.get_client_sync('default')
        else:
            # Fallback: create inline client for backward compatibility
            return httpx.AsyncClient(timeout=self.timeout)

    # =========================================================================
    # DECORATORS
    # =========================================================================

    def endpoint(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        visibility: ToolVisibility = ToolVisibility.PRIVATE,
        version: str = "1.0.0",
        method: str = "POST",
        path: Optional[str] = None,
        **kwargs
    ) -> Callable[[F], F]:
        """
        Decorator to register a FastAPI endpoint as a tool.

        Args:
            name: Tool name (defaults to function name)
            description: Tool description (defaults to docstring)
            tags: Tags for categorization
            category: Primary category
            visibility: Access level
            version: Semantic version
            method: HTTP method
            path: API path (defaults to /{name})

        Usage:
            @tools.endpoint(tags=["math"], category="calculation")
            async def add(request: AddRequest) -> AddResponse:
                '''Add two numbers together.'''
                return AddResponse(result=request.a + request.b)
        """
        def decorator(func: F) -> F:
            tool_name = name or func.__name__
            tool_description = description or (func.__doc__ or "").strip() or f"Endpoint: {tool_name}"
            tool_path = path or f"/{tool_name}"

            # Extract schemas from function signature
            input_schema, output_schema, input_model, output_model = extract_function_schemas(func)

            # Generate tool ID
            tool_id = self._generate_tool_id(tool_name)

            # Create action
            action = ToolAction(
                type=ActionType.HTTP,
                http={
                    "method": method,
                    "url": f"{self.service_url}{tool_path}",
                    "headers": {"Content-Type": "application/json"},
                }
            )

            # Create tool definition
            tool = ToolDefinition(
                id=tool_id,
                name=tool_name,
                description=tool_description,
                tool_type=ToolType.ENDPOINT,
                input_schema=input_schema,
                output_schema=output_schema,
                input_model_name=input_model,
                output_model_name=output_model,
                action=action,
                tags=tags or [],
                category=category,
                visibility=visibility,
                version=version,
                service_name=self.service_name,
                service_url=self.service_url,
                endpoint=tool_path,
                **kwargs
            )

            # Store tool and handler
            self._tools[tool_id] = tool
            self._handlers[tool_id] = func
            self._pending_registration.append(tool)

            logger.info(f"Registered endpoint tool: {tool_name} -> {tool_path}")

            # Mark function as FastAPI endpoint
            @wraps(func)
            async def wrapper(*args, **kw):
                return await func(*args, **kw) if asyncio.iscoroutinefunction(func) else func(*args, **kw)

            wrapper._tool = tool
            wrapper._tool_id = tool_id
            wrapper.is_tool_endpoint = True

            return wrapper

        return decorator

    def snippet(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        visibility: ToolVisibility = ToolVisibility.PRIVATE,
        version: str = "1.0.0",
        **kwargs
    ) -> Callable[[F], F]:
        """
        Decorator to register a code snippet as a tool.

        The function's docstring is treated as the executable code.
        The function body can provide example usage or be empty.

        Args:
            name: Tool name (defaults to function name)
            description: Tool description
            input_schema: JSON schema for inputs (or auto-extracted)
            output_schema: JSON schema for outputs
            tags: Tags for categorization
            category: Primary category

        Usage:
            @tools.snippet(
                name="fetch_data",
                description="Fetch data from an API",
                input_schema={"url": {"type": "string"}}
            )
            def fetch_snippet(url: str) -> dict:
                '''
                import httpx
                response = httpx.get(url)
                return response.json()
                '''
                pass  # Body not executed
        """
        def decorator(func: F) -> F:
            tool_name = name or func.__name__

            # Extract code from docstring
            code = textwrap.dedent(func.__doc__ or "").strip()
            code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]

            # Build description
            tool_description = description or f"Code snippet: {tool_name}"

            # Get schemas
            if input_schema:
                inp_schema = ToolInputSchema(**input_schema) if isinstance(input_schema, dict) else input_schema
            else:
                inp_schema, _, input_model, _ = extract_function_schemas(func)

            out_schema = ToolOutputSchema(**output_schema) if output_schema else ToolOutputSchema()

            # Generate ID
            tool_id = self._generate_tool_id(tool_name, prefix="snippet")

            # Create action
            action = ToolAction(
                type=ActionType.CODE,
                code=code
            )

            # Create tool definition
            tool = ToolDefinition(
                id=tool_id,
                name=tool_name,
                description=tool_description,
                tool_type=ToolType.SNIPPET,
                input_schema=inp_schema,
                output_schema=out_schema,
                action=action,
                tags=tags or [],
                category=category,
                visibility=visibility,
                version=version,
                service_name=self.service_name,
                code=code,
                code_hash=code_hash,
                **kwargs
            )

            # Store
            self._tools[tool_id] = tool
            self._snippets[tool_id] = code
            self._pending_registration.append(tool)

            logger.info(f"Registered snippet tool: {tool_name} (hash: {code_hash})")

            @wraps(func)
            def wrapper(*args, **kw):
                # Snippet execution happens via execute_snippet
                return func(*args, **kw)

            wrapper._tool = tool
            wrapper._tool_id = tool_id
            wrapper.is_tool_snippet = True

            return wrapper

        return decorator

    def function(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        **kwargs
    ) -> Callable[[F], F]:
        """
        Decorator to register a plain function as a tool.

        Usage:
            @tools.function(tags=["utility"])
            def calculate(a: int, b: int) -> int:
                '''Calculate sum of two numbers.'''
                return a + b
        """
        def decorator(func: F) -> F:
            tool_name = name or func.__name__
            tool_description = description or (func.__doc__ or "").strip() or f"Function: {tool_name}"

            input_schema, output_schema, input_model, output_model = extract_function_schemas(func)

            tool_id = self._generate_tool_id(tool_name, prefix="func")

            tool = ToolDefinition(
                id=tool_id,
                name=tool_name,
                description=tool_description,
                tool_type=ToolType.FUNCTION,
                input_schema=input_schema,
                output_schema=output_schema,
                input_model_name=input_model,
                output_model_name=output_model,
                action=ToolAction(type=ActionType.LAMBDA),
                tags=tags or [],
                category=category,
                service_name=self.service_name,
                **kwargs
            )

            self._tools[tool_id] = tool
            self._handlers[tool_id] = func
            self._pending_registration.append(tool)

            logger.info(f"Registered function tool: {tool_name}")

            # Preserve sync/async nature of the original function
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def wrapper(*args, **kw):
                    return await func(*args, **kw)
            else:
                @wraps(func)
                def wrapper(*args, **kw):
                    return func(*args, **kw)

            wrapper._tool = tool
            wrapper._tool_id = tool_id

            return wrapper

        return decorator

    # =========================================================================
    # TOOL MANAGEMENT
    # =========================================================================

    def _generate_tool_id(self, name: str, prefix: str = "tool") -> str:
        """Generate unique tool ID."""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        hash_input = f"{self.service_name}:{name}:{timestamp}"
        short_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        return f"{prefix}_{name}_{short_hash}"

    def get(self, tool_id: str) -> Optional[ToolDefinition]:
        """Get a tool by ID."""
        return self._tools.get(tool_id)

    def get_by_name(self, name: str) -> Optional[ToolDefinition]:
        """Get a tool by name."""
        for tool in self._tools.values():
            if tool.name == name:
                return tool
        return None

    def list_tools(self) -> List[ToolDefinition]:
        """List all registered tools."""
        return list(self._tools.values())

    def search(
        self,
        query: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        tool_type: Optional[ToolType] = None,
    ) -> List[ToolDefinition]:
        """Search for tools matching criteria."""
        results = list(self._tools.values())

        if query:
            q = query.lower()
            results = [t for t in results if q in t.name.lower() or q in t.description.lower()]

        if tags:
            results = [t for t in results if any(tag in t.tags for tag in tags)]

        if category:
            results = [t for t in results if t.category == category]

        if tool_type:
            results = [t for t in results if t.tool_type == tool_type]

        return results

    # =========================================================================
    # EXECUTION
    # =========================================================================

    async def execute(self, tool_name: str, **inputs) -> Any:
        """Execute a tool by name."""
        tool = self.get_by_name(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")

        tool.usage_count += 1
        tool.updated_at = datetime.utcnow()

        # Snippet execution
        if tool.tool_type == ToolType.SNIPPET:
            return await self.execute_snippet(tool.id, inputs)

        # Local handler execution
        handler = self._handlers.get(tool.id)
        if handler:
            if asyncio.iscoroutinefunction(handler):
                return await handler(**inputs)
            return handler(**inputs)

        # Remote HTTP execution
        if tool.action.type == ActionType.HTTP and tool.action.http:
            return await self._execute_http_tool(tool, inputs)

        # MCP tool execution
        if tool.action.type == ActionType.MCP and tool.action.mcp:
            return await self._execute_mcp_tool(tool, inputs)

        # Remote service execution (fallback for tools with service_url)
        if tool.service_url and tool.endpoint:
            return await self._execute_remote_tool(tool, inputs)

        raise NotImplementedError(f"No handler for tool: {tool_name}")

    async def _execute_http_tool(self, tool: ToolDefinition, inputs: Dict[str, Any]) -> Any:
        """Execute a tool via HTTP request."""
        http_config = tool.action.http
        if not http_config:
            raise ValueError(f"Tool {tool.name} has no HTTP configuration")

        client = self._get_client_from_pool()

        method = http_config.get("method", "POST").upper()
        url = http_config.get("url")
        headers = http_config.get("headers", {})
        timeout = http_config.get("timeout", self._timeout_config.read)

        if not url:
            raise ValueError(f"Tool {tool.name} has no URL configured")

        try:
            if method == "GET":
                response = await client.get(url, params=inputs, headers=headers, timeout=timeout)
            elif method == "POST":
                response = await client.post(url, json=inputs, headers=headers, timeout=timeout)
            elif method == "PUT":
                response = await client.put(url, json=inputs, headers=headers, timeout=timeout)
            elif method == "PATCH":
                response = await client.patch(url, json=inputs, headers=headers, timeout=timeout)
            elif method == "DELETE":
                response = await client.delete(url, params=inputs, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            # Try to parse as JSON, fallback to text
            try:
                return response.json()
            except Exception:
                return {"text": response.text, "status_code": response.status_code}

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error executing tool {tool.name}: {e}")
            return {
                "error": str(e),
                "status_code": e.response.status_code,
                "response": e.response.text[:500] if e.response.text else None
            }
        except httpx.RequestError as e:
            logger.error(f"Request error executing tool {tool.name}: {e}")
            return {"error": str(e), "type": "request_error"}

    async def _execute_mcp_tool(self, tool: ToolDefinition, inputs: Dict[str, Any]) -> Any:
        """Execute a tool via MCP protocol."""
        mcp_config = tool.action.mcp
        if not mcp_config:
            raise ValueError(f"Tool {tool.name} has no MCP configuration")

        server_url = mcp_config.get("server_url")
        tool_name = mcp_config.get("tool_name", tool.name)

        if not server_url:
            raise ValueError(f"Tool {tool.name} has no MCP server URL configured")

        client = self._get_client_from_pool()

        # MCP JSON-RPC 2.0 request
        request_id = str(hash(f"{tool_name}:{datetime.utcnow().timestamp()}"))
        mcp_request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": inputs
            }
        }

        try:
            response = await client.post(
                server_url,
                json=mcp_request,
                headers={"Content-Type": "application/json"},
                timeout=mcp_config.get("timeout", 60)
            )
            response.raise_for_status()

            result = response.json()

            if "error" in result:
                return {
                    "error": result["error"].get("message", "Unknown MCP error"),
                    "code": result["error"].get("code"),
                    "data": result["error"].get("data")
                }

            return result.get("result", result)

        except Exception as e:
            logger.error(f"MCP error executing tool {tool.name}: {e}")
            return {"error": str(e), "type": "mcp_error"}

    async def _execute_remote_tool(self, tool: ToolDefinition, inputs: Dict[str, Any]) -> Any:
        """Execute a tool on a remote service."""
        client = self._get_client_from_pool()

        url = f"{tool.service_url.rstrip('/')}{tool.endpoint}"

        try:
            response = await client.post(
                url,
                json=inputs,
                headers={"Content-Type": "application/json"},
                timeout=tool.metadata.get("timeout_ms", 30000) / 1000 if hasattr(tool, 'metadata') else 30
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Remote execution error for {tool.name}: {e}")
            return {
                "error": str(e),
                "status_code": e.response.status_code
            }
        except Exception as e:
            logger.error(f"Error executing remote tool {tool.name}: {e}")
            return {"error": str(e)}

    async def execute_snippet(self, tool_id: str, inputs: Dict[str, Any]) -> Any:
        """
        Execute a code snippet tool with sandboxed execution.

        Supports both sync and async snippets. For async snippets, the code should
        define an async function called 'main' or set 'result' to a coroutine.
        """
        code = self._snippets.get(tool_id)
        if not code:
            raise ValueError(f"Snippet not found: {tool_id}")

        # Create sandboxed execution context with limited builtins
        safe_builtins = {
            # Safe operations
            "len": len,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
            "set": set,
            "tuple": tuple,
            "range": range,
            "enumerate": enumerate,
            "zip": zip,
            "map": map,
            "filter": filter,
            "sorted": sorted,
            "reversed": reversed,
            "min": min,
            "max": max,
            "sum": sum,
            "abs": abs,
            "round": round,
            "isinstance": isinstance,
            "hasattr": hasattr,
            "getattr": getattr,
            "setattr": setattr,
            "type": type,
            "print": print,
            "None": None,
            "True": True,
            "False": False,
            # JSON handling
            "json": json,
            # Async support
            "asyncio": asyncio,
        }

        exec_globals = {
            "__builtins__": safe_builtins,
            "input_data": inputs,
            "inputs": inputs,
            **inputs,  # Make inputs directly accessible
        }

        # Allow httpx for network requests in snippets
        try:
            exec_globals["httpx"] = httpx
        except Exception:
            pass

        # Execute the snippet
        try:
            exec(code, exec_globals)

            # Check for async main function
            if "main" in exec_globals and asyncio.iscoroutinefunction(exec_globals["main"]):
                result = await exec_globals["main"]()
                return result

            # Check if result is a coroutine
            result = exec_globals.get("result", exec_globals.get("output", None))
            if asyncio.iscoroutine(result):
                return await result

            return result

        except SyntaxError as e:
            logger.error(f"Snippet syntax error: {e}")
            raise ValueError(f"Invalid snippet syntax: {e}")
        except NameError as e:
            logger.error(f"Snippet name error (likely using restricted builtin): {e}")
            raise ValueError(f"Restricted operation in snippet: {e}")
        except Exception as e:
            logger.error(f"Snippet execution failed: {e}")
            raise RuntimeError(f"Snippet execution failed: {e}")

    # =========================================================================
    # EXPORT FORMATS
    # =========================================================================

    def to_mcp_tools(self) -> List[Dict[str, Any]]:
        """Export all tools in MCP format."""
        return [self._to_mcp_format(t) for t in self._tools.values()]

    def _to_mcp_format(self, tool: ToolDefinition) -> Dict[str, Any]:
        """Convert a tool to MCP format."""
        return {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": tool.input_schema.model_dump(),
        }

    def to_openai_tools(self) -> List[Dict[str, Any]]:
        """Export all tools in OpenAI function calling format."""
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema.model_dump(),
                }
            }
            for t in self._tools.values()
        ]

    def to_anthropic_tools(self) -> List[Dict[str, Any]]:
        """Export all tools in Anthropic tool use format."""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema.model_dump(),
            }
            for t in self._tools.values()
        ]

    # =========================================================================
    # REGISTRATION WITH EXTERNAL APIS
    # =========================================================================

    async def register_all(self):
        """Register all pending tools with external APIs."""
        if not self._pending_registration:
            logger.info("No pending tools to register")
            return

        for tool in self._pending_registration:
            try:
                if self.tool_api_url:
                    await self._register_with_tool_api(tool)
                if self.graph_api_url:
                    await self._register_with_graph_api(tool)
            except Exception as e:
                logger.error(f"Failed to register tool {tool.name}: {e}")

        self._pending_registration.clear()

    async def _register_with_tool_api(self, tool: ToolDefinition):
        """Register tool with Tool Management API."""
        client = self._get_client_from_pool()

        payload = {
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.input_schema.model_dump(),
            "output_schema": tool.output_schema.model_dump(),
            "action": {
                "type": tool.action.type.value,
                "http": tool.action.http,
            } if tool.action.http else {"type": "code", "code": tool.code},
            "tags": tool.tags,
            "category": tool.category,
            "service_url": tool.service_url,
            "endpoint": tool.endpoint,
            "metadata": {
                "tool_type": tool.tool_type.value,
                "version": tool.version,
            }
        }

        response = await client.post(f"{self.tool_api_url}/tools", json=payload)

        if response.status_code in (200, 201):
            logger.info(f"Registered {tool.name} with Tool API")
        else:
            logger.warning(f"Tool API registration failed: {response.status_code}")

    async def _register_with_graph_api(self, tool: ToolDefinition):
        """Register tool with Graph API."""
        client = self._get_client_from_pool()

        # Create tool node
        node_data = {
            "label": "Tool",
            "properties": {
                "tool_id": tool.id,
                "name": tool.name,
                "description": tool.description,
                "tool_type": tool.tool_type.value,
                "input_schema": json.dumps(tool.input_schema.model_dump()),
                "output_schema": json.dumps(tool.output_schema.model_dump()),
                "tags": tool.tags,
                "category": tool.category,
                "version": tool.version,
                "service_name": tool.service_name,
                "created_at": tool.created_at.isoformat(),
            }
        }

        if tool.code:
            node_data["properties"]["code"] = tool.code
            node_data["properties"]["code_hash"] = tool.code_hash

        response = await client.post(
            f"{self.graph_api_url}/api/v1/node-with-metadata",
            json=node_data
        )

        if response.status_code in (200, 201):
            logger.info(f"Registered {tool.name} with Graph API")
        else:
            logger.warning(f"Graph API registration failed: {response.status_code}")

    # =========================================================================
    # FASTAPI INTEGRATION
    # =========================================================================

    def setup_fastapi(self, app: FastAPI):
        """
        Setup FastAPI integration with automatic tool registration.

        Usage:
            app = FastAPI()
            tools.setup_fastapi(app)
        """
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup: register tools
            await self.register_all()
            yield
            # Shutdown: cleanup
            await self.close()

        app.router.lifespan_context = lifespan

        # Add tools info endpoint
        @app.get("/tools")
        async def list_tools():
            return {"tools": [t.model_dump() for t in self._tools.values()]}

        @app.get("/tools/mcp")
        async def get_mcp_tools():
            return {"tools": self.to_mcp_tools()}

        @app.get("/tools/{tool_name}")
        async def get_tool(tool_name: str):
            tool = self.get_by_name(tool_name)
            if not tool:
                return {"error": f"Tool not found: {tool_name}"}
            return tool.model_dump()
