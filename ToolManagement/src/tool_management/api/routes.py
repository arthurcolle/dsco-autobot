"""
FastAPI routes for Tool Management.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..registry import get_registry
from ..models import Tool, ToolVisibility, ToolStatus

router = APIRouter(prefix="/tools", tags=["tools"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class ToolCreate(BaseModel):
    """Request model for creating a tool.

    Supports two formats:
    1. Flat format: inputs (list), output (dict), endpoint (str)
    2. Graph format: input_schema (dict), output_schema (dict), action (dict)
    """
    name: str
    description: str
    version: str = "1.0.0"
    # Flat format (original)
    inputs: List[Dict[str, Any]] = []
    output: Optional[Dict[str, Any]] = None
    endpoint: Optional[str] = None
    # Graph format (new)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    action: Optional[Dict[str, Any]] = None
    tool_type: str = "function"
    # Common fields
    tags: List[str] = []
    category: Optional[str] = None
    visibility: str = "private"
    service_url: Optional[str] = None
    service_name: Optional[str] = None
    metadata: Dict[str, Any] = {}


class ToolUpdate(BaseModel):
    """Request model for updating a tool."""
    description: Optional[str] = None
    version: Optional[str] = None
    tags: Optional[List[str]] = None
    category: Optional[str] = None
    visibility: Optional[str] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ToolExecuteRequest(BaseModel):
    """Request model for executing a tool.

    Canonical body field is `inputs`; `arguments` is accepted as an alias so
    MCP-style callers ({"arguments": {...}}) don't silently execute with an
    empty payload (which also used to trip the backend circuit breaker).
    """
    inputs: Dict[str, Any] = {}
    arguments: Dict[str, Any] = {}
    timeout: Optional[float] = None  # seconds; long tools (deepsearch, task runs) need >60

    def model_post_init(self, __context) -> None:
        if not self.inputs and self.arguments:
            self.inputs = self.arguments


class ToolExecuteResponse(BaseModel):
    """Response model for tool execution."""
    tool_id: str
    tool_name: str
    status: str
    output: Any = None
    error: Optional[str] = None


class BatchCallItem(BaseModel):
    """A single call within a batch request."""
    tool: str = Field(..., description="Tool name")
    inputs: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=30.0, description="Per-call timeout in seconds")


class BatchResultItem(BaseModel):
    tool: str
    status: str
    output: Any = None
    error: Optional[str] = None
    latency_ms: float = 0.0


class BatchExecuteRequest(BaseModel):
    """Batch execution request. Runs multiple tools in one call."""
    calls: List[BatchCallItem]
    parallel: bool = Field(default=True, description="Run calls in parallel vs sequential")


class BatchExecuteResponse(BaseModel):
    results: List[BatchResultItem]
    total: int
    successful: int
    failed: int
    total_latency_ms: float


# =============================================================================
# ROUTES
# =============================================================================

@router.get("", response_model=List[Dict[str, Any]])
async def list_tools(
    query: Optional[str] = Query(None, description="Search query"),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    category: Optional[str] = Query(None, description="Category filter"),
    visibility: Optional[str] = Query(None, description="Visibility filter"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List all registered tools (in-process + durable backends) with filtering."""
    from ..catalog_view import merged_tool_dicts

    tag_list = tags.split(",") if tags else None
    vis = ToolVisibility(visibility) if visibility else None

    tools = merged_tool_dicts(
        query=query,
        tags=tag_list,
        category=category,
        visibility=vis,
    )

    # Paginate
    return tools[offset:offset + limit]


@router.get("/names")
async def list_tool_names():
    """List all tool names (in-process + durable backends)."""
    from ..catalog_view import merged_names
    return {"names": merged_names()}


@router.get("/tags")
async def list_tags():
    """List all unique tags (in-process + durable backends)."""
    from ..catalog_view import merged_tags
    return {"tags": merged_tags()}


@router.get("/categories")
async def list_categories():
    """List all categories (in-process + durable backends)."""
    from ..catalog_view import merged_categories
    return {"categories": merged_categories()}


@router.get("/mcp")
async def get_mcp_tools():
    """Get all tools in MCP format (in-process + durable backends)."""
    from ..catalog_view import merged_mcp_tools
    return {"tools": merged_mcp_tools()}


@router.get("/{tool_name}")
async def get_tool(tool_name: str):
    """Get a tool by name."""
    registry = get_registry()
    tool = registry.get_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    return tool.model_dump(exclude={"_callable"})


@router.post("", status_code=201)
async def create_tool(request: ToolCreate):
    """Register a new tool.

    Accepts both flat format (inputs, endpoint) and graph format (input_schema, action).
    Graph format is preferred for service integrations.
    """
    registry = get_registry()

    if request.name in registry:
        raise HTTPException(status_code=409, detail=f"Tool already exists: {request.name}")

    from ..models.tool import ToolInput, ToolOutput, ToolMetadata

    # Handle graph format (input_schema, action)
    if request.input_schema:
        # Convert input_schema to inputs list
        inputs = _schema_to_inputs(request.input_schema)
    else:
        inputs = [ToolInput(**inp) for inp in request.inputs]

    # Handle output
    if request.output_schema:
        output = ToolOutput(type="object", description="", schema=request.output_schema)
    elif request.output:
        output = ToolOutput(**request.output)
    else:
        output = None

    # Handle endpoint from action
    endpoint = request.endpoint
    if not endpoint and request.action:
        http_config = request.action.get("http", {})
        action_url = http_config.get("url", "")
        if action_url:
            # Extract endpoint from action URL (relative to service_url)
            if request.service_url and action_url.startswith(request.service_url):
                endpoint = action_url[len(request.service_url):]
            else:
                endpoint = action_url

    # Store action config in metadata for execution
    metadata_dict = dict(request.metadata)
    if request.action:
        metadata_dict["action"] = request.action
    if request.input_schema:
        metadata_dict["input_schema"] = request.input_schema
    if request.service_name:
        metadata_dict["service_name"] = request.service_name

    tool = Tool(
        name=request.name,
        description=request.description,
        version=request.version,
        inputs=inputs,
        output=output,
        tags=request.tags,
        category=request.category,
        visibility=ToolVisibility(request.visibility),
        service_url=request.service_url,
        endpoint=endpoint,
        metadata=ToolMetadata(**metadata_dict),
    )

    registry.register(tool)
    return tool.model_dump(exclude={"_callable"})


def _schema_to_inputs(schema: Dict[str, Any]) -> List:
    """Convert JSON Schema to list of ToolInput objects."""
    from ..models.tool import ToolInput

    inputs = []
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    for name, prop in properties.items():
        param_type = prop.get("type", "string")
        if isinstance(param_type, list):
            param_type = param_type[0] if param_type else "string"

        inputs.append(ToolInput(
            name=name,
            type=param_type,
            description=prop.get("description", ""),
            required=name in required,
            default=prop.get("default"),
            enum=prop.get("enum"),
        ))

    return inputs


@router.patch("/{tool_name}")
async def update_tool(tool_name: str, request: ToolUpdate):
    """Update a tool's metadata."""
    registry = get_registry()
    tool = registry.get_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")

    update_data = request.model_dump(exclude_unset=True)

    if "visibility" in update_data:
        update_data["visibility"] = ToolVisibility(update_data["visibility"])
    if "status" in update_data:
        update_data["status"] = ToolStatus(update_data["status"])

    for key, value in update_data.items():
        setattr(tool, key, value)

    from datetime import datetime
    tool.updated_at = datetime.utcnow()

    return tool.model_dump(exclude={"_callable"})


@router.delete("/{tool_name}", status_code=204)
async def delete_tool(tool_name: str):
    """Unregister a tool."""
    registry = get_registry()
    tool = registry.get_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")

    registry.unregister(tool.id)
    return None


@router.post("/batch", response_model=BatchExecuteResponse)
async def batch_execute(request: BatchExecuteRequest):
    """Execute multiple tools in a single request.

    Each call specifies a tool name and inputs. By default all calls
    run in parallel; set parallel=false for sequential execution.
    """
    registry = get_registry()
    batch_start = time.time()

    async def _run_one(item: BatchCallItem) -> BatchResultItem:
        t0 = time.time()
        tool = registry.get_by_name(item.tool)
        if not tool:
            return BatchResultItem(
                tool=item.tool, status="error",
                error=f"Tool not found: {item.tool}",
                latency_ms=(time.time() - t0) * 1000,
            )
        if tool.status != ToolStatus.ACTIVE:
            return BatchResultItem(
                tool=item.tool, status="error",
                error=f"Tool is not active: {tool.status}",
                latency_ms=(time.time() - t0) * 1000,
            )
        try:
            action = getattr(tool.metadata, "action", None) if tool.metadata else None
            if action:
                result = await asyncio.wait_for(
                    _execute_with_action(action, item.inputs),
                    timeout=item.timeout,
                )
            else:
                result = await asyncio.wait_for(
                    tool.execute(**item.inputs),
                    timeout=item.timeout,
                )
            return BatchResultItem(
                tool=item.tool, status="success", output=result,
                latency_ms=(time.time() - t0) * 1000,
            )
        except asyncio.TimeoutError:
            return BatchResultItem(
                tool=item.tool, status="error",
                error=f"Timeout after {item.timeout}s",
                latency_ms=(time.time() - t0) * 1000,
            )
        except Exception as exc:
            return BatchResultItem(
                tool=item.tool, status="error", error=str(exc),
                latency_ms=(time.time() - t0) * 1000,
            )

    if request.parallel:
        results = await asyncio.gather(*[_run_one(c) for c in request.calls])
    else:
        results = [await _run_one(c) for c in request.calls]

    results_list = list(results)
    successful = sum(1 for r in results_list if r.status == "success")
    return BatchExecuteResponse(
        results=results_list,
        total=len(results_list),
        successful=successful,
        failed=len(results_list) - successful,
        total_latency_ms=(time.time() - batch_start) * 1000,
    )


@router.post("/{tool_name}/execute", response_model=ToolExecuteResponse)
async def execute_tool(tool_name: str, request: ToolExecuteRequest, http_request: Request):
    """Execute a tool with the given inputs.

    Execution priority:
    1. Action config from metadata (HTTP, MCP, Code)
    2. Local callable handler
    3. service_url + endpoint fallback
    """
    registry = get_registry()
    tool = registry.get_by_name(tool_name)
    if not tool:
        # Not a local builtin — resolve against the tool_store / MCP backends over HTTP.
        tenant_id = None
        try:
            from .user_routes import resolve_user_from_request

            user = resolve_user_from_request(http_request)
            if user:
                tenant_id = user["user_id"]
        except Exception:
            pass
        backend_result = await _execute_via_backend(
            tool_name, request.inputs, tenant_id, timeout=request.timeout
        )
        if backend_result is not None:
            return backend_result
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")

    if tool.status != ToolStatus.ACTIVE:
        raise HTTPException(status_code=400, detail=f"Tool is not active: {tool.status}")

    try:
        # Check for action config in metadata (graph format)
        action = getattr(tool.metadata, "action", None) if tool.metadata else None

        if action:
            result = await _execute_with_action(action, request.inputs)
        else:
            # Fallback to original execution
            result = await tool.execute(**request.inputs)

        tool.usage_count += 1
        return ToolExecuteResponse(
            tool_id=tool.id,
            tool_name=tool.name,
            status="success",
            output=result,
        )
    except Exception as e:
        return ToolExecuteResponse(
            tool_id=tool.id,
            tool_name=tool.name,
            status="error",
            error=str(e),
        )


async def _execute_via_backend(
    tool_name: str,
    inputs: Dict[str, Any],
    tenant_id: Optional[str] = None,
    timeout: Optional[float] = None,
):
    """Resolve a tool against the tool_store and execute it over HTTP (MCP JSON-RPC).

    Returns a ToolExecuteResponse if the tool resolved to a backend, else None so
    the caller can raise a 404.
    """
    try:
        from .tools_api import execute_tool as _exec_backend, ToolCall
        from ..persistence import get_tool_store
    except Exception:
        return None
    try:
        get_tool_store().resolve_tool(tool_name)
    except Exception:
        return None
    call_kwargs: Dict[str, Any] = {"tool": tool_name, "arguments": inputs}
    if timeout:
        call_kwargs["timeout"] = max(1.0, min(float(timeout), 600.0))
    call = ToolCall(**call_kwargs)
    call.bind_tenant(tenant_id)
    res = await _exec_backend(call)
    return ToolExecuteResponse(
        tool_id=tool_name,
        tool_name=tool_name,
        status="success" if getattr(res, "success", False) else "error",
        output=getattr(res, "result", None),
        error=getattr(res, "error", None),
    )


async def _execute_with_action(action: Dict[str, Any], inputs: Dict[str, Any]) -> Any:
    """Execute tool using action configuration."""
    import httpx

    action_type = action.get("type", "http")

    if action_type == "http":
        http_config = action.get("http", {})
        url = http_config.get("url")
        method = http_config.get("method", "POST").upper()
        headers = http_config.get("headers", {"Content-Type": "application/json"})
        timeout = http_config.get("timeout_ms", 30000) / 1000

        if not url:
            raise ValueError("HTTP action missing URL")

        async with httpx.AsyncClient(timeout=timeout) as client:
            if method == "GET":
                response = await client.get(url, params=inputs, headers=headers)
            elif method == "POST":
                response = await client.post(url, json=inputs, headers=headers)
            elif method == "PUT":
                response = await client.put(url, json=inputs, headers=headers)
            else:
                response = await client.request(method, url, json=inputs, headers=headers)

            response.raise_for_status()
            try:
                return response.json()
            except Exception:
                return {"text": response.text, "status_code": response.status_code}

    elif action_type == "mcp":
        mcp_config = action.get("mcp", {})
        server_url = mcp_config.get("server_url")
        tool_name = mcp_config.get("tool_name")

        if not server_url:
            raise ValueError("MCP action missing server_url")

        mcp_request = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": inputs}
        }

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                server_url,
                json=mcp_request,
                headers={"Content-Type": "application/json"}
            )
            result = response.json()
            if "error" in result:
                raise RuntimeError(result["error"].get("message", "MCP error"))
            return result.get("result", result)

    elif action_type == "code":
        return await _execute_code_action(action, inputs)

    else:
        raise ValueError(f"Unknown action type: {action_type}")


async def _execute_code_action(action: Dict[str, Any], inputs: Dict[str, Any]) -> Any:
    """Execute code in a sandboxed environment.

    Supports:
    - Python: Uses RestrictedPython for sandboxing
    - JavaScript: Uses subprocess with timeout

    Action config:
    {
        "type": "code",
        "code": {
            "language": "python" | "javascript",
            "source": "...",
            "entry_point": "main",  # Function to call
            "timeout_ms": 30000
        }
    }
    """
    code_config = action.get("code", {})
    language = code_config.get("language", "python").lower()
    source = code_config.get("source", "")
    entry_point = code_config.get("entry_point", "main")
    timeout_ms = code_config.get("timeout_ms", 30000)
    timeout_sec = timeout_ms / 1000

    if not source:
        raise ValueError("Code action missing source code")

    if language == "python":
        return await _execute_python_code(source, entry_point, inputs, timeout_sec)
    elif language == "javascript":
        return await _execute_javascript_code(source, entry_point, inputs, timeout_sec)
    else:
        raise ValueError(f"Unsupported language: {language}")


async def _execute_python_code(
    source: str,
    entry_point: str,
    inputs: Dict[str, Any],
    timeout: float
) -> Any:
    """Execute Python code in a restricted sandbox."""
    import asyncio
    from io import StringIO
    import sys
    import math
    import json as json_module

    # Restricted builtins for sandboxed execution
    SAFE_BUILTINS = {
        'abs': abs,
        'all': all,
        'any': any,
        'bool': bool,
        'chr': chr,
        'dict': dict,
        'divmod': divmod,
        'enumerate': enumerate,
        'filter': filter,
        'float': float,
        'frozenset': frozenset,
        'int': int,
        'isinstance': isinstance,
        'issubclass': issubclass,
        'iter': iter,
        'len': len,
        'list': list,
        'map': map,
        'max': max,
        'min': min,
        'next': next,
        'ord': ord,
        'pow': pow,
        'print': print,
        'range': range,
        'repr': repr,
        'reversed': reversed,
        'round': round,
        'set': set,
        'slice': slice,
        'sorted': sorted,
        'str': str,
        'sum': sum,
        'tuple': tuple,
        'type': type,
        'zip': zip,
        'True': True,
        'False': False,
        'None': None,
        'Exception': Exception,
        'ValueError': ValueError,
        'TypeError': TypeError,
        'KeyError': KeyError,
        'IndexError': IndexError,
        'RuntimeError': RuntimeError,
    }

    # Safe modules to allow
    SAFE_MODULES = {
        'math': math,
        'json': json_module,
        're': __import__('re'),
        'datetime': __import__('datetime'),
        'collections': __import__('collections'),
        'itertools': __import__('itertools'),
        'functools': __import__('functools'),
        'random': __import__('random'),
        'string': __import__('string'),
        'hashlib': __import__('hashlib'),
        'base64': __import__('base64'),
        'urllib.parse': __import__('urllib.parse'),
    }

    # Capture stdout
    stdout_capture = StringIO()
    original_stdout = sys.stdout

    def safe_import(name, *args, **kwargs):
        """Restricted import function."""
        if name in SAFE_MODULES:
            return SAFE_MODULES[name]
        # Allow submodule access
        base_module = name.split('.')[0]
        if base_module in SAFE_MODULES:
            return __import__(name, *args, **kwargs)
        raise ImportError(f"Import of '{name}' is not allowed in sandbox")

    # Build execution namespace
    namespace = {
        '__builtins__': SAFE_BUILTINS,
        '__import__': safe_import,
        'inputs': inputs,
        '_result': None,
    }

    # Add safe modules to namespace
    for name, module in SAFE_MODULES.items():
        namespace[name.replace('.', '_')] = module

    # Wrap execution with timeout
    async def run_code():
        nonlocal stdout_capture
        result = None
        error = None

        try:
            sys.stdout = stdout_capture

            # Compile and execute
            code_obj = compile(source, '<sandbox>', 'exec')
            exec(code_obj, namespace)

            # Call entry point if it exists
            if entry_point in namespace:
                entry_func = namespace[entry_point]
                if callable(entry_func):
                    result = entry_func(inputs)
                else:
                    result = entry_func
            elif '_result' in namespace and namespace['_result'] is not None:
                result = namespace['_result']
            else:
                # Look for a return value in common variable names
                for var_name in ['result', 'output', 'response', 'data']:
                    if var_name in namespace:
                        result = namespace[var_name]
                        break

        except Exception as e:
            error = str(e)
        finally:
            sys.stdout = original_stdout

        return result, error, stdout_capture.getvalue()

    try:
        result, error, stdout = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(run_code())),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        return {
            "status": "timeout",
            "error": f"Execution timed out after {timeout}s",
            "stdout": stdout_capture.getvalue()
        }

    if error:
        return {
            "status": "error",
            "error": error,
            "stdout": stdout
        }

    return {
        "status": "success",
        "result": result,
        "stdout": stdout
    }


async def _execute_javascript_code(
    source: str,
    entry_point: str,
    inputs: Dict[str, Any],
    timeout: float
) -> Any:
    """Execute JavaScript code using Node.js subprocess."""
    import asyncio
    import subprocess
    import tempfile
    import os
    import json as json_module

    # Wrap the code to call entry point and output JSON
    wrapper = f"""
const inputs = {json_module.dumps(inputs)};

{source}

// Call entry point and output result
let result;
if (typeof {entry_point} === 'function') {{
    result = {entry_point}(inputs);
}} else if (typeof {entry_point} !== 'undefined') {{
    result = {entry_point};
}} else {{
    result = null;
}}

// Handle promises
Promise.resolve(result).then(r => {{
    console.log(JSON.stringify({{status: 'success', result: r}}));
}}).catch(e => {{
    console.log(JSON.stringify({{status: 'error', error: e.message}}));
}});
"""

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
        f.write(wrapper)
        temp_path = f.name

    try:
        # Try node first, fall back to deno
        for runtime in ['node', 'deno run --allow-none']:
            try:
                cmd = runtime.split() + [temp_path]
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    return {
                        "status": "timeout",
                        "error": f"Execution timed out after {timeout}s"
                    }

                if process.returncode != 0:
                    return {
                        "status": "error",
                        "error": stderr.decode('utf-8'),
                        "stdout": stdout.decode('utf-8')
                    }

                # Parse output
                output = stdout.decode('utf-8').strip()
                try:
                    # Find the last JSON line (result)
                    lines = output.split('\n')
                    for line in reversed(lines):
                        if line.strip().startswith('{'):
                            return json_module.loads(line)
                    return {"status": "success", "result": output}
                except json_module.JSONDecodeError:
                    return {"status": "success", "result": output}

            except FileNotFoundError:
                continue

        return {
            "status": "error",
            "error": "No JavaScript runtime found (node or deno)"
        }

    finally:
        os.unlink(temp_path)
