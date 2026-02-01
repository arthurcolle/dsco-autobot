"""
Tool Management API v2 - Uses Graph Management API for persistence.

Clean separation of concerns:
- Graph Management API handles all Neo4j operations
- Tool Management API handles tool-specific logic, decorators, and execution
"""
import os
import json
import logging
import inspect
import asyncio
from typing import Callable, Type, Dict, Any, List, Optional, Literal
from datetime import datetime, timezone
from functools import wraps
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from graph_api_client import GraphAPIClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

class JavaScriptCode(BaseModel):
    code: str
    function_name: str


class PythonCode(BaseModel):
    code: str
    function_name: str


class HTTPAction(BaseModel):
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
    url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None


class ToolAction(BaseModel):
    type: Literal["javascript", "http", "python"]
    javascript: Optional[JavaScriptCode] = None
    http: Optional[HTTPAction] = None
    python: Optional[PythonCode] = None


class ToolOutput(BaseModel):
    type: Literal["ai", "markdown", "html", "json"]
    content: str = ""


class UserSetting(BaseModel):
    name: str
    label: str
    type: str = "text"
    description: Optional[str] = None
    required: bool = False
    placeholder: Optional[str] = None
    values: Optional[List[str]] = None


class ToolCreate(BaseModel):
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    action: ToolAction
    output: ToolOutput
    user_settings: Optional[List[UserSetting]] = None
    code: Optional[str] = None
    is_adhoc: bool = False
    runtime: Optional[Literal["python", "javascript"]] = None


class ToolResponse(BaseModel):
    id: str
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    action: ToolAction
    output: ToolOutput
    version: str = "1.0.0"
    user_settings: Optional[List[UserSetting]] = None


class ToolExecutionRequest(BaseModel):
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None
    input_data: Dict[str, Any]
    user_settings: Dict[str, Any] = Field(default_factory=dict)


class ToolExecutionResponse(BaseModel):
    tool_id: str
    tool_name: str
    output_data: Any
    execution_time_ms: int


class ToolSearchRequest(BaseModel):
    prompt: str
    limit: int = 5
    for_model: Literal["anthropic", "openai", "gpt-4o"] = "anthropic"


# =============================================================================
# Tool Management Service
# =============================================================================

class ToolManagementService:
    """
    Service for managing tools via Graph Management API.

    Handles:
    - Tool CRUD operations
    - Tool execution (HTTP, Python, JavaScript)
    - Embedding generation for semantic search
    """

    def __init__(
        self,
        graph_api_url: str = "http://localhost:2015",
        embedding_api_url: Optional[str] = None
    ):
        self.graph = GraphAPIClient(graph_api_url)
        self.embedding_api_url = embedding_api_url
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self) -> None:
        """Close all connections."""
        await self.graph.close()
        if self._http_client:
            await self._http_client.aclose()

    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text using embedding API."""
        if not self.embedding_api_url:
            return None

        try:
            client = await self._get_http_client()
            response = await client.post(
                f"{self.embedding_api_url}/generate_embeddings",
                json={"texts": [text]}
            )
            response.raise_for_status()
            result = response.json()
            return result["embeddings"][0]["embedding"]
        except Exception as e:
            logger.warning(f"Failed to generate embedding: {e}")
            return None

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    async def create_tool(self, tool: ToolCreate) -> ToolResponse:
        """Create a new tool."""
        # Generate embeddings
        tool_text = f"{tool.name}: {tool.description}"
        embedding = await self.generate_embedding(tool_text)

        # Get next version
        version = await self._get_next_version(tool.name)

        # Build action dict
        action_dict = {
            "type": tool.action.type,
            "javascript": tool.action.javascript.model_dump() if tool.action.javascript else None,
            "http": tool.action.http.model_dump() if tool.action.http else None,
            "python": tool.action.python.model_dump() if tool.action.python else None
        }

        # Build output dict
        output_dict = {
            "type": tool.output.type,
            "content": tool.output.content
        }

        # Create via Graph API
        result = await self.graph.create_tool_with_schema(
            name=tool.name,
            description=tool.description,
            input_schema=tool.input_schema,
            output_schema=tool.output_schema,
            action=action_dict,
            output=output_dict,
            embedding=embedding,
            user_settings=[s.model_dump() for s in tool.user_settings] if tool.user_settings else None,
            version=version
        )

        logger.info(f"Created tool: {tool.name} (ID: {result['tool_id']}, Version: {version})")

        return ToolResponse(
            id=result["tool_id"],
            name=tool.name,
            description=tool.description,
            input_schema=tool.input_schema,
            output_schema=tool.output_schema,
            action=tool.action,
            output=tool.output,
            version=version,
            user_settings=tool.user_settings
        )

    async def get_tool(self, tool_id: str) -> Optional[ToolResponse]:
        """Get a tool by ID."""
        result = await self.graph.get_tool_full(tool_id)
        if not result or not result.get("tool"):
            return None
        return self._parse_tool_result(result)

    async def get_tool_by_name(self, name: str) -> Optional[ToolResponse]:
        """Get a tool by name."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {name: $name})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
            ORDER BY t.version DESC
            LIMIT 1
        """, {"name": name}, strategy="single")

        if not result or not result.get("result"):
            return None

        r = result["result"]
        return self._parse_tool_result({
            "tool": r.get("t"),
            "schema": r.get("s"),
            "action": r.get("a"),
            "output": r.get("o")
        })

    async def list_tools(self, limit: int = 100) -> List[ToolResponse]:
        """List all tools."""
        results = await self.graph.list_tools(limit)
        return [self._parse_tool_result(r) for r in results if r.get("tool")]

    async def update_tool(self, tool_id: str, tool: ToolCreate) -> ToolResponse:
        """Update an existing tool."""
        # Delete old tool
        await self.graph.delete_tool_full(tool_id)
        # Create new version
        return await self.create_tool(tool)

    async def delete_tool(self, tool_id: str) -> bool:
        """Delete a tool."""
        return await self.graph.delete_tool_full(tool_id)

    # =========================================================================
    # Search
    # =========================================================================

    async def search_tools(self, prompt: str, limit: int = 5) -> List[ToolResponse]:
        """Search tools by semantic similarity."""
        embedding = await self.generate_embedding(prompt)

        if embedding:
            results = await self.graph.vector_search("Tool", embedding, limit)
            tool_ids = [r["node"]["id"] for r in results]
        else:
            # Fallback to text search
            results = await self.graph.full_text_search("Tool", prompt, limit)
            tool_ids = [r["node"]["id"] for r in results]

        tools = []
        for tool_id in tool_ids:
            tool = await self.get_tool(tool_id)
            if tool:
                tools.append(tool)

        return tools

    # =========================================================================
    # Execution
    # =========================================================================

    async def execute_tool(self, request: ToolExecutionRequest) -> ToolExecutionResponse:
        """Execute a tool."""
        start_time = datetime.now(timezone.utc)

        # Get the tool
        if request.tool_id:
            tool = await self.get_tool(request.tool_id)
        elif request.tool_name:
            tool = await self.get_tool_by_name(request.tool_name)
        else:
            raise HTTPException(status_code=400, detail="Either tool_id or tool_name required")

        if not tool:
            raise HTTPException(status_code=404, detail="Tool not found")

        # Execute based on action type
        action = tool.action
        output_data: Any = None

        if action.type == "http" and action.http:
            output_data = await self._execute_http(action.http, request.input_data)
        elif action.type == "python" and action.python:
            output_data = await self._execute_python(action.python, request.input_data)
        elif action.type == "javascript" and action.javascript:
            output_data = await self._execute_javascript(action.javascript, request.input_data)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported action type: {action.type}")

        end_time = datetime.now(timezone.utc)
        execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

        return ToolExecutionResponse(
            tool_id=tool.id,
            tool_name=tool.name,
            output_data=output_data,
            execution_time_ms=execution_time_ms
        )

    async def _execute_http(self, http: HTTPAction, input_data: Dict[str, Any]) -> Any:
        """Execute an HTTP action."""
        client = await self._get_http_client()

        method = http.method.upper()
        if method in ["POST", "PUT", "PATCH"]:
            body = {**(http.body or {}), **input_data}
            response = await client.request(method, http.url, json=body, headers=http.headers)
        else:
            response = await client.request(method, http.url, params=input_data, headers=http.headers)

        response.raise_for_status()
        return response.json()

    async def _execute_python(self, python: PythonCode, input_data: Dict[str, Any]) -> Any:
        """Execute Python code in sandbox."""
        # TODO: Implement sandboxed Python execution
        raise HTTPException(status_code=501, detail="Python execution not implemented")

    async def _execute_javascript(self, js: JavaScriptCode, input_data: Dict[str, Any]) -> Any:
        """Execute JavaScript code."""
        # TODO: Implement JavaScript execution
        raise HTTPException(status_code=501, detail="JavaScript execution not implemented")

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _get_next_version(self, tool_name: str) -> str:
        """Get the next version number for a tool."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {name: $name})
            RETURN max(t.version) as latest
        """, {"name": tool_name}, strategy="single")

        if result and result.get("result") and result["result"].get("latest"):
            latest = result["result"]["latest"]
            major, minor, patch = map(int, latest.split('.'))
            return f"{major}.{minor}.{patch + 1}"
        return "1.0.0"

    def _parse_tool_result(self, result: Dict[str, Any]) -> ToolResponse:
        """Parse a tool result from Graph API into ToolResponse."""
        t = result.get("tool", {})
        s = result.get("schema", {})
        a = result.get("action", {})
        o = result.get("output", {})

        # Parse JSON strings
        input_schema = s.get("input_schema", "{}")
        output_schema = s.get("output_schema", "{}")
        if isinstance(input_schema, str):
            input_schema = json.loads(input_schema)
        if isinstance(output_schema, str):
            output_schema = json.loads(output_schema)

        # Parse action
        action_type = a.get("type", "http")
        action = ToolAction(
            type=action_type,
            http=HTTPAction(**json.loads(a["http"])) if a.get("http") else None,
            python=PythonCode(**json.loads(a["python"])) if a.get("python") else None,
            javascript=JavaScriptCode(**json.loads(a["javascript"])) if a.get("javascript") else None
        )

        # Parse output
        output = ToolOutput(
            type=o.get("type", "json"),
            content=o.get("content", "")
        )

        # Parse user settings
        user_settings = None
        if t.get("user_settings"):
            settings_data = t["user_settings"]
            if isinstance(settings_data, str):
                settings_data = json.loads(settings_data)
            user_settings = [UserSetting(**s) for s in settings_data]

        return ToolResponse(
            id=t.get("id", ""),
            name=t.get("name", ""),
            description=t.get("description", ""),
            input_schema=input_schema,
            output_schema=output_schema,
            action=action,
            output=output,
            version=t.get("version", "1.0.0"),
            user_settings=user_settings
        )


# =============================================================================
# Tool Management Client (for services to register tools)
# =============================================================================

class ToolManagementClient:
    """
    Client for services to register and manage their tools.

    Usage:
        tools = ToolManagementClient(
            tool_api_url="http://localhost:2016",
            service_name="MyService",
            service_url="http://localhost:8000"
        )

        @tools.endpoint(description="Get user by ID")
        async def get_user(input: GetUserInput) -> UserOutput:
            ...

        # On startup
        await tools.register_tools()
    """

    def __init__(
        self,
        tool_api_url: str,
        service_name: str = "DefaultService",
        service_url: str = "http://localhost:8000"
    ):
        self.base_url = tool_api_url.rstrip('/')
        self.service_name = service_name
        self.service_url = service_url.rstrip('/')
        self.tools_to_register: List[Dict[str, Any]] = []
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self) -> None:
        if self._http_client:
            await self._http_client.aclose()

    # =========================================================================
    # Decorators
    # =========================================================================

    def endpoint(
        self,
        description: str = "",
        path: Optional[str] = None,
        method: str = "POST"
    ):
        """Decorator for FastAPI endpoints."""
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

            # Build schema
            input_schema = input_model.model_json_schema() if input_model else {}
            output_schema = output_model.model_json_schema() if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {}

            tool_schema = {
                "name": name,
                "description": (description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                "input_schema": input_schema,
                "output_schema": output_schema,
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

            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            wrapper.tool_schema = tool_schema
            return wrapper
        return decorator

    def function(self, description: str = "", name: Optional[str] = None):
        """Decorator for standalone functions."""
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

            input_schema = input_model.model_json_schema() if input_model else {}
            output_schema = output_model.model_json_schema() if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {}

            tool_schema = {
                "name": func_name,
                "description": (description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                "input_schema": input_schema,
                "output_schema": output_schema,
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

            @wraps(func)
            async def wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            wrapper.tool_schema = tool_schema
            return wrapper
        return decorator

    def snippet(
        self,
        language: str,
        description: str = "",
        name: Optional[str] = None
    ):
        """Decorator for code snippets."""
        def decorator(func: Callable):
            code = inspect.getdoc(func) or ""
            snippet_name = name or func.__name__

            tool_schema = {
                "name": snippet_name,
                "description": description or f"{language} snippet",
                "input_schema": {},
                "output_schema": {"type": "string"},
                "action": {
                    "type": language.lower(),
                    language.lower(): {"code": code.strip(), "function_name": snippet_name}
                },
                "output": {"type": "json", "content": ""}
            }

            self.tools_to_register.append(tool_schema)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return code.strip()

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

        for tool_schema in self.tools_to_register:
            name = tool_schema["name"]
            try:
                if name in existing_names:
                    existing = existing_names[name]
                    if self._schemas_match(existing, tool_schema):
                        logger.info(f"Tool '{name}' already exists with matching schema")
                        continue
                    else:
                        logger.info(f"Updating tool '{name}'")
                        await client.put(f"{self.base_url}/tools/{existing['id']}", json=tool_schema)
                else:
                    logger.info(f"Creating tool '{name}'")
                    await client.post(f"{self.base_url}/tools", json=tool_schema)
            except Exception as e:
                logger.error(f"Failed to register tool '{name}': {e}")

    async def _get_existing_tools(self) -> List[Dict[str, Any]]:
        """Get existing tools from API."""
        client = await self._get_client()
        try:
            response = await client.get(f"{self.base_url}/tools")
            response.raise_for_status()
            return response.json()
        except Exception:
            return []

    def _schemas_match(self, existing: Dict[str, Any], new: Dict[str, Any]) -> bool:
        """Check if two tool schemas match."""
        keys = ["name", "description", "input_schema", "output_schema", "action"]
        return all(existing.get(k) == new.get(k) for k in keys)


# =============================================================================
# FastAPI Application
# =============================================================================

_service: Optional[ToolManagementService] = None


def get_service() -> ToolManagementService:
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return _service


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _service
    _service = ToolManagementService(
        graph_api_url=os.getenv("GRAPH_API_URL", "http://localhost:2015"),
        embedding_api_url=os.getenv("EMBEDDING_API_URL")
    )
    yield
    if _service:
        await _service.close()


app = FastAPI(
    title="Tool Management API",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check(service: ToolManagementService = Depends(get_service)):
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/tools", response_model=ToolResponse)
async def create_tool(tool: ToolCreate, service: ToolManagementService = Depends(get_service)):
    return await service.create_tool(tool)


@app.get("/tools", response_model=List[ToolResponse])
async def list_tools(limit: int = 100, service: ToolManagementService = Depends(get_service)):
    return await service.list_tools(limit)


@app.get("/tools/{tool_id}", response_model=ToolResponse)
async def get_tool(tool_id: str, service: ToolManagementService = Depends(get_service)):
    tool = await service.get_tool(tool_id)
    if not tool:
        raise HTTPException(status_code=404, detail="Tool not found")
    return tool


@app.get("/tools/name/{name}", response_model=ToolResponse)
async def get_tool_by_name(name: str, service: ToolManagementService = Depends(get_service)):
    tool = await service.get_tool_by_name(name)
    if not tool:
        raise HTTPException(status_code=404, detail="Tool not found")
    return tool


@app.put("/tools/{tool_id}", response_model=ToolResponse)
async def update_tool(tool_id: str, tool: ToolCreate, service: ToolManagementService = Depends(get_service)):
    return await service.update_tool(tool_id, tool)


@app.delete("/tools/{tool_id}")
async def delete_tool(tool_id: str, service: ToolManagementService = Depends(get_service)):
    if await service.delete_tool(tool_id):
        return {"deleted": True, "tool_id": tool_id}
    raise HTTPException(status_code=404, detail="Tool not found")


@app.post("/tools/search", response_model=List[ToolResponse])
async def search_tools(request: ToolSearchRequest, service: ToolManagementService = Depends(get_service)):
    return await service.search_tools(request.prompt, request.limit)


@app.post("/execute_tool", response_model=ToolExecutionResponse)
async def execute_tool(request: ToolExecutionRequest, service: ToolManagementService = Depends(get_service)):
    return await service.execute_tool(request)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "2016")))
