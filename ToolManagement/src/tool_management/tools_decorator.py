"""
Enhanced Tool Decorator System with Full Pydantic + Streaming Support.

Features:
- Auto-inspection of Pydantic input/output schemas
- Support for async generators / SSE streams
- Proper FastAPI route binding
- Namespace support for tool organization
- MCP-compatible tool export

Usage:
    from tool_management.tools_decorator import tools, ToolRegistry

    # Simple endpoint with Pydantic schemas
    @tools.endpoint(namespace="math", name="add", description="Add two numbers")
    @app.post("/add")
    async def add_endpoint(request: AddRequest) -> AddResponse:
        return AddResponse(result=request.a + request.b)

    # Streaming endpoint
    @tools.endpoint(namespace="ai", name="generate", stream=True)
    @app.post("/generate")
    async def generate_stream(request: GenerateRequest) -> AsyncGenerator[str, None]:
        async for chunk in llm.stream(request.prompt):
            yield chunk

    # Get MCP-compatible tools
    mcp_tools = tools.to_mcp()
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import sys
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Generator
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
    Generic,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field, create_model

logger = logging.getLogger(__name__)

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


# =============================================================================
# TYPE DETECTION
# =============================================================================

def is_async_generator_type(tp) -> bool:
    """Check if type is AsyncGenerator or AsyncIterator."""
    origin = get_origin(tp)
    if origin is None:
        return False
    return origin in (AsyncGenerator, AsyncIterator) or (
        hasattr(tp, "__origin__") and tp.__origin__ in (AsyncGenerator, AsyncIterator)
    )


def is_streaming_response(tp) -> bool:
    """Check if type is a streaming response type."""
    if tp is StreamingResponse:
        return True
    origin = get_origin(tp)
    return origin in (AsyncGenerator, AsyncIterator, Generator)


def is_pydantic_model(tp) -> bool:
    """Check if type is a Pydantic model."""
    try:
        return isinstance(tp, type) and issubclass(tp, BaseModel)
    except TypeError:
        return False


def get_inner_type(tp) -> Optional[type]:
    """Extract inner type from generic (e.g., AsyncGenerator[str, None] -> str)."""
    args = get_args(tp)
    return args[0] if args else None


# =============================================================================
# SCHEMA EXTRACTION
# =============================================================================

def pydantic_to_json_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    """Extract JSON schema from Pydantic model (v1 and v2 compatible)."""
    try:
        return model.model_json_schema()
    except AttributeError:
        return model.schema()


def python_type_to_json_schema(tp) -> Dict[str, Any]:
    """Convert Python type annotation to JSON Schema."""
    if tp is None or tp is type(None):
        return {"type": "null"}

    # Check for Pydantic model
    if is_pydantic_model(tp):
        return pydantic_to_json_schema(tp)

    # Basic type mapping
    type_map = {
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
        bytes: {"type": "string", "format": "binary"},
        list: {"type": "array"},
        dict: {"type": "object"},
        Any: {},
    }

    if tp in type_map:
        return type_map[tp]

    origin = get_origin(tp)
    args = get_args(tp)

    # Optional[X] = Union[X, None]
    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            schema = python_type_to_json_schema(non_none[0])
            schema["nullable"] = True
            return schema
        return {"oneOf": [python_type_to_json_schema(a) for a in non_none]}

    # List[X]
    if origin is list:
        items = python_type_to_json_schema(args[0]) if args else {}
        return {"type": "array", "items": items}

    # Dict[K, V]
    if origin is dict:
        return {"type": "object"}

    # Literal["a", "b"]
    if origin is Literal:
        return {"type": "string", "enum": list(args)}

    # AsyncGenerator[T, None] -> stream of T
    if is_async_generator_type(tp):
        inner = get_inner_type(tp)
        return {
            "type": "stream",
            "items": python_type_to_json_schema(inner) if inner else {"type": "string"},
            "format": "event-stream",
        }

    return {"type": "string"}


def extract_endpoint_schemas(func: Callable) -> Tuple[
    Dict[str, Any],  # input_schema
    Dict[str, Any],  # output_schema
    Optional[Type[BaseModel]],  # input_model
    Optional[Type[BaseModel]],  # output_model
    bool,  # is_streaming
]:
    """
    Extract input/output schemas from a function signature.

    Handles:
    - Single Pydantic model parameter (request body)
    - Multiple typed parameters
    - Pydantic model return type
    - AsyncGenerator return type (streaming)

    Returns:
        (input_schema, output_schema, input_model, output_model, is_streaming)
    """
    sig = inspect.signature(func)

    # Get type hints, handling forward references
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = func.__annotations__.copy() if hasattr(func, "__annotations__") else {}

    input_model: Optional[Type[BaseModel]] = None
    output_model: Optional[Type[BaseModel]] = None
    is_streaming = False

    input_schema = {"type": "object", "properties": {}, "required": []}
    output_schema = {"type": "object", "properties": {}}

    # Analyze parameters
    for param_name, param in sig.parameters.items():
        # Skip special params
        if param_name in ("self", "cls", "request", "response"):
            continue

        annotation = hints.get(param_name, param.annotation)
        if annotation is inspect.Parameter.empty:
            continue

        # Check if single Pydantic model (request body pattern)
        if is_pydantic_model(annotation):
            input_model = annotation
            input_schema = pydantic_to_json_schema(annotation)
            break

        # Build schema from individual params
        prop_schema = python_type_to_json_schema(annotation)

        # Handle default values
        if param.default is not inspect.Parameter.empty:
            if hasattr(param.default, "description"):
                prop_schema["description"] = param.default.description
            if not isinstance(param.default, type):
                prop_schema["default"] = param.default
        else:
            input_schema["required"].append(param_name)

        input_schema["properties"][param_name] = prop_schema

    # Analyze return type
    return_type = hints.get("return")
    if return_type:
        # Check for streaming
        if is_streaming_response(return_type) or is_async_generator_type(return_type):
            is_streaming = True
            inner_type = get_inner_type(return_type)
            if inner_type and is_pydantic_model(inner_type):
                output_model = inner_type
                output_schema = {
                    "type": "stream",
                    "items": pydantic_to_json_schema(inner_type),
                    "format": "event-stream",
                }
            else:
                output_schema = {
                    "type": "stream",
                    "items": python_type_to_json_schema(inner_type) if inner_type else {"type": "string"},
                    "format": "event-stream",
                }
        elif is_pydantic_model(return_type):
            output_model = return_type
            output_schema = pydantic_to_json_schema(return_type)
        else:
            output_schema = python_type_to_json_schema(return_type)

    return input_schema, output_schema, input_model, output_model, is_streaming


# =============================================================================
# TOOL DEFINITION
# =============================================================================

class ToolType(str, Enum):
    ENDPOINT = "endpoint"
    FUNCTION = "function"
    SNIPPET = "snippet"
    MCP = "mcp"
    STREAM = "stream"


@dataclass
class ToolSpec:
    """Complete tool specification with schemas and metadata."""
    id: str
    name: str
    namespace: str
    description: str
    tool_type: ToolType

    # Schemas (JSON Schema format)
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]

    # Model references
    input_model: Optional[Type[BaseModel]] = None
    output_model: Optional[Type[BaseModel]] = None

    # Execution
    handler: Optional[Callable] = None
    endpoint: Optional[str] = None
    method: str = "POST"
    is_streaming: bool = False

    # Metadata
    tags: List[str] = field(default_factory=list)
    version: str = "1.0.0"
    created_at: datetime = field(default_factory=datetime.utcnow)

    # Service info
    service_url: Optional[str] = None

    @property
    def full_name(self) -> str:
        """Namespaced tool name."""
        return f"{self.namespace}.{self.name}" if self.namespace else self.name

    def to_mcp(self) -> Dict[str, Any]:
        """Export as MCP tool format."""
        return {
            "name": self.full_name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }

    def to_openai(self) -> Dict[str, Any]:
        """Export as OpenAI function format."""
        return {
            "type": "function",
            "function": {
                "name": self.full_name,
                "description": self.description,
                "parameters": self.input_schema,
            }
        }

    def to_anthropic(self) -> Dict[str, Any]:
        """Export as Anthropic tool format."""
        return {
            "name": self.full_name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


# =============================================================================
# TOOL REGISTRY
# =============================================================================

class ToolRegistry:
    """
    Central registry for tools with namespace support.

    Usage:
        tools = ToolRegistry(service_url="http://localhost:8000")

        @tools.endpoint(namespace="math", name="add")
        @app.post("/api/math/add")
        async def add(request: AddRequest) -> AddResponse:
            ...
    """

    def __init__(
        self,
        service_url: str = "http://localhost:8000",
        default_namespace: str = "default",
    ):
        self.service_url = service_url.rstrip("/")
        self.default_namespace = default_namespace

        self._tools: Dict[str, ToolSpec] = {}
        self._by_namespace: Dict[str, List[str]] = {}
        self._handlers: Dict[str, Callable] = {}
        self._routes: Dict[str, APIRoute] = {}

    def endpoint(
        self,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        version: str = "1.0.0",
        stream: bool = False,
    ) -> Callable[[F], F]:
        """
        Decorator to register a FastAPI endpoint as a tool.

        Automatically extracts:
        - Input schema from Pydantic request model
        - Output schema from Pydantic response model
        - Streaming capability from AsyncGenerator return type

        Args:
            namespace: Tool namespace (e.g., "math", "ai")
            name: Tool name (defaults to function name)
            description: Tool description (defaults to docstring)
            tags: Additional tags
            version: Semantic version
            stream: Force streaming mode

        Example:
            @tools.endpoint(namespace="math", name="add", description="Add numbers")
            @app.post("/api/math/add")
            async def add_numbers(request: AddRequest) -> AddResponse:
                return AddResponse(result=request.a + request.b)
        """
        def decorator(func: F) -> F:
            nonlocal stream

            tool_name = name or func.__name__
            tool_namespace = namespace or self.default_namespace
            tool_description = description or inspect.getdoc(func) or f"Tool: {tool_name}"

            # Extract schemas
            input_schema, output_schema, input_model, output_model, is_streaming = extract_endpoint_schemas(func)

            # Override streaming if specified
            if stream:
                is_streaming = True

            # Generate ID
            tool_id = f"{tool_namespace}.{tool_name}"

            # Infer endpoint path from function (if decorated with FastAPI route)
            endpoint_path = None
            if hasattr(func, "__self__") and hasattr(func.__self__, "path"):
                endpoint_path = func.__self__.path

            # Create tool spec
            tool = ToolSpec(
                id=tool_id,
                name=tool_name,
                namespace=tool_namespace,
                description=tool_description,
                tool_type=ToolType.STREAM if is_streaming else ToolType.ENDPOINT,
                input_schema=input_schema,
                output_schema=output_schema,
                input_model=input_model,
                output_model=output_model,
                handler=func,
                endpoint=endpoint_path,
                method="POST",
                is_streaming=is_streaming,
                tags=tags or [],
                version=version,
                service_url=self.service_url,
            )

            # Register
            self._tools[tool_id] = tool
            self._handlers[tool_id] = func

            if tool_namespace not in self._by_namespace:
                self._by_namespace[tool_namespace] = []
            self._by_namespace[tool_namespace].append(tool_id)

            logger.info(
                f"Registered tool: {tool_id} "
                f"(input: {input_model.__name__ if input_model else 'params'}, "
                f"output: {output_model.__name__ if output_model else 'dict'}, "
                f"streaming: {is_streaming})"
            )

            # Attach metadata to function
            @wraps(func)
            async def wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            wrapper._tool_spec = tool
            wrapper._tool_id = tool_id
            wrapper.is_tool = True

            return wrapper

        return decorator

    def function(
        self,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Callable[[F], F]:
        """
        Decorator for plain functions (not HTTP endpoints).

        Example:
            @tools.function(namespace="util", name="hash")
            def compute_hash(data: str) -> str:
                return hashlib.sha256(data.encode()).hexdigest()
        """
        def decorator(func: F) -> F:
            tool_name = name or func.__name__
            tool_namespace = namespace or self.default_namespace
            tool_description = description or inspect.getdoc(func) or f"Function: {tool_name}"

            input_schema, output_schema, input_model, output_model, _ = extract_endpoint_schemas(func)

            tool_id = f"{tool_namespace}.{tool_name}"

            tool = ToolSpec(
                id=tool_id,
                name=tool_name,
                namespace=tool_namespace,
                description=tool_description,
                tool_type=ToolType.FUNCTION,
                input_schema=input_schema,
                output_schema=output_schema,
                input_model=input_model,
                output_model=output_model,
                handler=func,
                tags=tags or [],
            )

            self._tools[tool_id] = tool
            self._handlers[tool_id] = func

            if tool_namespace not in self._by_namespace:
                self._by_namespace[tool_namespace] = []
            self._by_namespace[tool_namespace].append(tool_id)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper._tool_spec = tool
            wrapper._tool_id = tool_id
            wrapper.is_tool = True

            return wrapper

        return decorator

    # =========================================================================
    # EXECUTION
    # =========================================================================

    async def execute(
        self,
        tool_id: str,
        inputs: Dict[str, Any],
        stream: bool = False,
    ) -> Union[Any, AsyncGenerator[Any, None]]:
        """
        Execute a tool by ID.

        Args:
            tool_id: Full tool ID (namespace.name) or just name
            inputs: Input parameters
            stream: Return streaming response if available

        Returns:
            Tool result or async generator for streaming
        """
        tool = self.get(tool_id)
        if not tool:
            # Try without namespace
            for tid, t in self._tools.items():
                if t.name == tool_id:
                    tool = t
                    break

        if not tool:
            raise ValueError(f"Tool not found: {tool_id}")

        handler = self._handlers.get(tool.id)
        if not handler:
            raise ValueError(f"No handler for tool: {tool_id}")

        # Prepare input
        if tool.input_model:
            # Validate and construct Pydantic model
            request_obj = tool.input_model(**inputs)

            if asyncio.iscoroutinefunction(handler):
                result = await handler(request_obj)
            else:
                result = handler(request_obj)
        else:
            # Pass as kwargs
            if asyncio.iscoroutinefunction(handler):
                result = await handler(**inputs)
            else:
                result = handler(**inputs)

        # Handle streaming
        if tool.is_streaming and stream:
            if inspect.isasyncgen(result):
                return result
            elif inspect.isgenerator(result):
                async def async_gen():
                    for item in result:
                        yield item
                return async_gen()

        # Convert Pydantic model to dict
        if isinstance(result, BaseModel):
            return result.model_dump()

        return result

    async def execute_stream(
        self,
        tool_id: str,
        inputs: Dict[str, Any],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Execute a tool and yield SSE-formatted events.

        Yields events in format:
            {"event": "data", "data": <chunk>}
            {"event": "done", "data": null}
        """
        tool = self.get(tool_id)
        if not tool:
            yield {"event": "error", "data": {"error": f"Tool not found: {tool_id}"}}
            return

        try:
            result = await self.execute(tool_id, inputs, stream=True)

            if inspect.isasyncgen(result):
                async for chunk in result:
                    if isinstance(chunk, BaseModel):
                        yield {"event": "data", "data": chunk.model_dump()}
                    elif isinstance(chunk, dict):
                        yield {"event": "data", "data": chunk}
                    else:
                        yield {"event": "data", "data": str(chunk)}
            else:
                # Single result
                if isinstance(result, BaseModel):
                    yield {"event": "data", "data": result.model_dump()}
                else:
                    yield {"event": "data", "data": result}

            yield {"event": "done", "data": None}

        except Exception as e:
            logger.exception(f"Error executing tool {tool_id}")
            yield {"event": "error", "data": {"error": str(e)}}

    # =========================================================================
    # LOOKUP
    # =========================================================================

    def get(self, tool_id: str) -> Optional[ToolSpec]:
        """Get tool by ID."""
        return self._tools.get(tool_id)

    def get_by_name(self, name: str, namespace: Optional[str] = None) -> Optional[ToolSpec]:
        """Get tool by name, optionally scoped to namespace."""
        if namespace:
            tool_id = f"{namespace}.{name}"
            return self._tools.get(tool_id)

        for tool in self._tools.values():
            if tool.name == name:
                return tool
        return None

    def list_tools(self, namespace: Optional[str] = None) -> List[ToolSpec]:
        """List all tools, optionally filtered by namespace."""
        if namespace:
            tool_ids = self._by_namespace.get(namespace, [])
            return [self._tools[tid] for tid in tool_ids]
        return list(self._tools.values())

    def list_namespaces(self) -> List[str]:
        """List all registered namespaces."""
        return list(self._by_namespace.keys())

    # =========================================================================
    # EXPORT
    # =========================================================================

    def to_mcp(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Export tools in MCP format."""
        tools = self.list_tools(namespace)
        return [t.to_mcp() for t in tools]

    def to_openai(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Export tools in OpenAI function format."""
        tools = self.list_tools(namespace)
        return [t.to_openai() for t in tools]

    def to_anthropic(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Export tools in Anthropic format."""
        tools = self.list_tools(namespace)
        return [t.to_anthropic() for t in tools]

    def to_json_schema(self, namespace: Optional[str] = None) -> Dict[str, Any]:
        """Export all tool schemas as JSON Schema."""
        tools = self.list_tools(namespace)
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Tool Registry",
            "type": "object",
            "properties": {
                t.full_name: {
                    "description": t.description,
                    "input": t.input_schema,
                    "output": t.output_schema,
                    "streaming": t.is_streaming,
                }
                for t in tools
            }
        }

    # =========================================================================
    # FASTAPI INTEGRATION
    # =========================================================================

    def bind_routes(self, app: FastAPI, prefix: str = "/tools"):
        """
        Bind tool management routes to FastAPI app.

        Adds:
            GET  {prefix}/                - List all tools
            GET  {prefix}/{namespace}     - List tools in namespace
            GET  {prefix}/{namespace}/{name}  - Get tool details
            POST {prefix}/{namespace}/{name}/execute  - Execute tool
            POST {prefix}/{namespace}/{name}/stream   - Execute with streaming
        """
        from fastapi import HTTPException
        from fastapi.responses import JSONResponse
        from sse_starlette.sse import EventSourceResponse

        @app.get(f"{prefix}/")
        async def list_all_tools():
            """List all registered tools."""
            return {
                "tools": [t.to_mcp() for t in self._tools.values()],
                "namespaces": self.list_namespaces(),
                "count": len(self._tools),
            }

        @app.get(f"{prefix}/mcp")
        async def get_mcp_tools():
            """Get tools in MCP format."""
            return {"tools": self.to_mcp()}

        @app.get(f"{prefix}/openai")
        async def get_openai_tools():
            """Get tools in OpenAI format."""
            return {"tools": self.to_openai()}

        @app.get(f"{prefix}/{{namespace}}")
        async def list_namespace_tools(namespace: str):
            """List tools in a namespace."""
            tools = self.list_tools(namespace)
            if not tools:
                raise HTTPException(404, f"Namespace not found: {namespace}")
            return {"tools": [t.to_mcp() for t in tools]}

        @app.get(f"{prefix}/{{namespace}}/{{name}}")
        async def get_tool_details(namespace: str, name: str):
            """Get tool details."""
            tool = self.get_by_name(name, namespace)
            if not tool:
                raise HTTPException(404, f"Tool not found: {namespace}.{name}")
            return {
                "id": tool.id,
                "name": tool.name,
                "namespace": tool.namespace,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "output_schema": tool.output_schema,
                "streaming": tool.is_streaming,
                "tags": tool.tags,
                "version": tool.version,
            }

        @app.post(f"{prefix}/{{namespace}}/{{name}}/execute")
        async def execute_tool(namespace: str, name: str, request: Request):
            """Execute a tool."""
            tool = self.get_by_name(name, namespace)
            if not tool:
                raise HTTPException(404, f"Tool not found: {namespace}.{name}")

            try:
                inputs = await request.json()
                result = await self.execute(tool.id, inputs)
                return {"result": result, "tool": tool.full_name}
            except Exception as e:
                logger.exception(f"Error executing {tool.full_name}")
                raise HTTPException(500, str(e))

        @app.post(f"{prefix}/{{namespace}}/{{name}}/stream")
        async def stream_tool(namespace: str, name: str, request: Request):
            """Execute a tool with streaming response."""
            tool = self.get_by_name(name, namespace)
            if not tool:
                raise HTTPException(404, f"Tool not found: {namespace}.{name}")

            inputs = await request.json()

            async def event_generator():
                async for event in self.execute_stream(tool.id, inputs):
                    yield {
                        "event": event["event"],
                        "data": json.dumps(event["data"]),
                    }

            return EventSourceResponse(event_generator())

        logger.info(f"Bound tool routes to {prefix}/")


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

# Default global registry
tools = ToolRegistry()


def get_tools() -> ToolRegistry:
    """Get the global tool registry."""
    return tools


def create_registry(service_url: str = "http://localhost:8000") -> ToolRegistry:
    """Create a new tool registry."""
    return ToolRegistry(service_url=service_url)
