"""
Tool Management Client with decorators for registering tools.

Usage:
    from mini_autobot.client import ToolManagementClient

    tools = ToolManagementClient(
        tool_api_url="http://localhost:2016",
        service_name="MyService",
        service_url="http://localhost:8000"
    )

    @tools.endpoint(description="Get user by ID")
    async def get_user(input: GetUserInput) -> UserOutput:
        ...

    @tools.function(description="Calculate sum")
    def calculate_sum(input: CalcInput) -> CalcOutput:
        ...

    @tools.snippet(language="javascript", description="Format date")
    def format_date_js():
        '''
        function formatDate(d) { return new Date(d).toISOString(); }
        '''

    # On startup
    await tools.register_tools()
"""
import inspect
import asyncio
import logging
from typing import Callable, Dict, Any, List, Optional
from functools import wraps

import httpx
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_json_schema(model) -> Dict[str, Any]:
    """Generate JSON schema from Pydantic model."""
    if model and isinstance(model, type) and issubclass(model, BaseModel):
        return model.model_json_schema()
    return {}


class ToolManagementClient:
    """
    Client for services to register and manage their tools via decorators.

    Provides decorators:
    - @tools.endpoint() - For FastAPI endpoints
    - @tools.function() - For standalone functions
    - @tools.snippet() - For code snippets (JS, SQL, etc.)
    - @tools.task() - For long-running tasks
    """

    def __init__(
        self,
        tool_api_url: str,
        service_name: str = "DefaultService",
        service_url: str = "http://localhost:8000",
        debug: bool = False
    ):
        """
        Initialize the client.

        Args:
            tool_api_url: URL of the Tool Management API
            service_name: Name of your service
            service_url: Base URL of your service endpoints
            debug: Enable debug logging
        """
        self.base_url = tool_api_url.rstrip('/')
        self.service_name = service_name
        self.service_url = service_url.rstrip('/')
        self.debug = debug
        self.tools_to_register: List[Dict[str, Any]] = []
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    # =========================================================================
    # Decorators
    # =========================================================================

    def endpoint(
        self,
        description: str = "",
        path: Optional[str] = None,
        method: str = "POST"
    ):
        """
        Decorator for FastAPI endpoints.

        Infers input/output schemas from type annotations.

        Args:
            description: Tool description
            path: API path (defaults to /{function_name})
            method: HTTP method
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            # Find input model
            input_model = None
            for param in params:
                if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
                    input_model = param.annotation
                    break

            output_model = sig.return_annotation
            name = func.__name__
            endpoint_path = path or f"/{name}"

            tool_schema = {
                "name": name,
                "description": (description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                "input_schema": generate_json_schema(input_model),
                "output_schema": generate_json_schema(output_model),
                "action": {
                    "type": "http",
                    "http": {
                        "method": method,
                        "url": f"{self.service_url}{endpoint_path}",
                        "headers": {"Content-Type": "application/json"}
                    }
                },
                "output": {"type": "json", "content": ""}
            }

            self.tools_to_register.append(tool_schema)
            logger.debug(f"Registered endpoint: {name}")

            @wraps(func)
            async def wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            wrapper.tool_schema = tool_schema
            return wrapper
        return decorator

    def function(self, description: str = "", name: Optional[str] = None):
        """
        Decorator for standalone functions.

        Args:
            description: Tool description
            name: Override function name
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
                    input_model = param.annotation
                    break

            output_model = sig.return_annotation
            func_name = name or func.__name__

            tool_schema = {
                "name": func_name,
                "description": (description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                "input_schema": generate_json_schema(input_model),
                "output_schema": generate_json_schema(output_model),
                "action": {
                    "type": "http",
                    "http": {
                        "method": "POST",
                        "url": f"{self.service_url}/{func_name}",
                        "headers": {"Content-Type": "application/json"}
                    }
                },
                "output": {"type": "json", "content": ""}
            }

            self.tools_to_register.append(tool_schema)
            logger.debug(f"Registered function: {func_name}")

            @wraps(func)
            async def wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            wrapper.tool_schema = tool_schema
            wrapper._original_func = func
            return wrapper
        return decorator

    def snippet(
        self,
        language: str,
        description: str = "",
        name: Optional[str] = None,
        runtime: Optional[str] = None
    ):
        """
        Decorator for code snippets (JavaScript, SQL, Python, etc.).

        The code is extracted from the function's docstring.

        Args:
            language: Programming language
            description: Tool description
            name: Override name
            runtime: Runtime environment
        """
        runtimes = {
            'javascript': 'node', 'js': 'node', 'typescript': 'node',
            'python': 'python3', 'py': 'python3',
            'sql': 'postgres', 'mysql': 'mysql',
            'bash': 'bash', 'sh': 'sh',
        }

        def decorator(func: Callable):
            code = inspect.getdoc(func) or ""
            snippet_name = name or func.__name__
            inferred_runtime = runtime or runtimes.get(language.lower(), language.lower())

            tool_schema = {
                "name": snippet_name,
                "description": description or f"{language} snippet: {snippet_name}",
                "input_schema": {},
                "output_schema": {"type": "string"},
                "is_adhoc": True,
                "runtime": language.lower(),
                "code": code.strip(),
                "action": {
                    "type": language.lower(),
                    language.lower(): {
                        "code": code.strip(),
                        "function_name": snippet_name
                    }
                },
                "output": {"type": "json", "content": ""}
            }

            self.tools_to_register.append(tool_schema)
            logger.debug(f"Registered snippet: {snippet_name} [{language}]")

            @wraps(func)
            def wrapper(*args, **kwargs):
                return code.strip()

            wrapper.tool_schema = tool_schema
            wrapper.source_code = code.strip()
            return wrapper
        return decorator

    def task(
        self,
        description: str = "",
        name: Optional[str] = None,
        timeout: Optional[int] = None
    ):
        """
        Decorator for long-running tasks with streaming support.

        Args:
            description: Tool description
            name: Override name
            timeout: Execution timeout in seconds
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
                    input_model = param.annotation
                    break

            output_model = sig.return_annotation
            func_name = name or func.__name__
            is_async_gen = inspect.isasyncgenfunction(func)
            is_gen = inspect.isgeneratorfunction(func)

            tool_schema = {
                "name": func_name,
                "description": (description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                "input_schema": generate_json_schema(input_model),
                "output_schema": generate_json_schema(output_model),
                "action": {
                    "type": "http",
                    "http": {
                        "method": "POST",
                        "url": f"{self.service_url}/{func_name}",
                        "headers": {"Content-Type": "application/json"}
                    }
                },
                "output": {"type": "json", "content": ""},
                "metadata": {
                    "is_task": True,
                    "streaming": is_async_gen or is_gen,
                    "timeout": timeout
                }
            }

            self.tools_to_register.append(tool_schema)
            logger.debug(f"Registered task: {func_name}")

            if is_async_gen or is_gen:
                @wraps(func)
                async def gen_wrapper(*args, **kwargs):
                    if is_async_gen:
                        async for item in func(*args, **kwargs):
                            yield item
                    else:
                        for item in func(*args, **kwargs):
                            yield item
                gen_wrapper.tool_schema = tool_schema
                return gen_wrapper
            else:
                @wraps(func)
                async def wrapper(*args, **kwargs):
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    return func(*args, **kwargs)
                wrapper.tool_schema = tool_schema
                return wrapper
        return decorator

    # =========================================================================
    # Registration
    # =========================================================================

    async def register_tools(self) -> None:
        """Register all decorated tools with the Tool Management API."""
        client = await self._get_client()
        existing = await self._get_existing_tools()
        existing_names = {t["name"]: t for t in existing}

        registered = 0
        updated = 0
        skipped = 0

        for tool_schema in self.tools_to_register:
            name = tool_schema["name"]
            try:
                if name in existing_names:
                    existing_tool = existing_names[name]
                    if self._schemas_match(existing_tool, tool_schema):
                        logger.info(f"Tool '{name}' already exists with matching schema")
                        skipped += 1
                        continue
                    else:
                        logger.info(f"Updating tool '{name}'")
                        await client.put(
                            f"{self.base_url}/tools/{existing_tool['id']}",
                            json=tool_schema
                        )
                        updated += 1
                else:
                    logger.info(f"Creating tool '{name}'")
                    await client.post(f"{self.base_url}/tools", json=tool_schema)
                    registered += 1
            except Exception as e:
                logger.error(f"Failed to register tool '{name}': {e}")

        logger.info(f"Tool registration complete: {registered} created, {updated} updated, {skipped} skipped")

    async def _get_existing_tools(self) -> List[Dict[str, Any]]:
        """Get existing tools from API."""
        client = await self._get_client()
        try:
            response = await client.get(f"{self.base_url}/tools")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Could not fetch existing tools: {e}")
            return []

    def _schemas_match(self, existing: Dict[str, Any], new: Dict[str, Any]) -> bool:
        """Check if two tool schemas match."""
        keys = ["name", "description", "input_schema", "output_schema"]
        return all(existing.get(k) == new.get(k) for k in keys)

    # =========================================================================
    # API Methods
    # =========================================================================

    async def get_tools(self) -> List[Dict[str, Any]]:
        """Get all tools from the API."""
        client = await self._get_client()
        response = await client.get(f"{self.base_url}/tools")
        response.raise_for_status()
        return response.json()

    async def get_tool(self, tool_id: str) -> Dict[str, Any]:
        """Get a tool by ID."""
        client = await self._get_client()
        response = await client.get(f"{self.base_url}/tools/{tool_id}")
        response.raise_for_status()
        return response.json()

    async def get_tool_by_name(self, name: str) -> Dict[str, Any]:
        """Get a tool by name."""
        client = await self._get_client()
        response = await client.get(f"{self.base_url}/tools/name/{name}")
        response.raise_for_status()
        return response.json()

    async def execute_tool(
        self,
        tool_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a tool."""
        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/execute_tool",
            json={
                "tool_id": tool_id,
                "tool_name": tool_name,
                "input_data": input_data or {}
            }
        )
        response.raise_for_status()
        return response.json()

    async def search_tools(self, prompt: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search for tools."""
        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/tools/search",
            json={"prompt": prompt, "limit": limit}
        )
        response.raise_for_status()
        return response.json()
