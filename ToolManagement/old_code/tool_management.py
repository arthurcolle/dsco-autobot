from typing import Callable, Type, Dict, Any, Union, List, Optional, AsyncIterator, TypeVar
from pydantic import BaseModel, Field
from functools import wraps
import inspect
import asyncio
import httpx
import logging
import uuid
import json
from datetime import datetime, timezone
from enum import Enum
from fastapi.responses import StreamingResponse
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

T = TypeVar('T')


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    STREAMING = "streaming"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StreamEvent(BaseModel):
    event_type: str = "data"
    data: Any
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0
    task_id: Optional[str] = None


class TaskState(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    progress: float = 0.0
    message: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    events: List[StreamEvent] = []


class TaskManager:
    """Manages long-running tasks and their state."""

    def __init__(self):
        self._tasks: Dict[str, TaskState] = {}
        self._task_futures: Dict[str, asyncio.Task] = {}

    def create_task(self, task_id: Optional[str] = None) -> TaskState:
        task_id = task_id or str(uuid.uuid4())
        state = TaskState(task_id=task_id)
        self._tasks[task_id] = state
        return state

    def get_task(self, task_id: str) -> Optional[TaskState]:
        return self._tasks.get(task_id)

    def update_task(
        self,
        task_id: str,
        status: Optional[TaskStatus] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        result: Optional[Any] = None,
        error: Optional[str] = None
    ) -> Optional[TaskState]:
        state = self._tasks.get(task_id)
        if not state:
            return None
        if status:
            state.status = status
            if status == TaskStatus.RUNNING and not state.started_at:
                state.started_at = datetime.now(timezone.utc)
            elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                state.completed_at = datetime.now(timezone.utc)
        if progress is not None:
            state.progress = min(max(progress, 0.0), 100.0)
        if message:
            state.message = message
        if result is not None:
            state.result = result
        if error:
            state.error = error
        return state

    def add_event(self, task_id: str, event: StreamEvent) -> None:
        state = self._tasks.get(task_id)
        if state:
            event.sequence = len(state.events)
            event.task_id = task_id
            state.events.append(event)

    def cancel_task(self, task_id: str) -> bool:
        if task_id in self._task_futures:
            self._task_futures[task_id].cancel()
            self.update_task(task_id, status=TaskStatus.CANCELLED)
            return True
        return False

    def register_future(self, task_id: str, future: asyncio.Task) -> None:
        self._task_futures[task_id] = future


# Global task manager instance
_task_manager = TaskManager()


def get_task_manager() -> TaskManager:
    return _task_manager


def generate_json_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    return model.model_json_schema()

class ToolManagementAPI:
    def __init__(
        self, 
        tool_api_url: str, 
        service_name: str = "Your Service Name", 
        service_url: str = "http://yourserviceapi.internal",
        timeout: int = 30
    ):
        """
        Initializes the ToolManagementAPI client.

        Args:
            tool_api_url (str): Base URL of the Tool Management API.
            service_name (str, optional): Name of the service. Defaults to "Your Service Name".
            service_url (str, optional): Internal service URL for tool actions. Defaults to "http://yourserviceapi.internal".
            timeout (int, optional): Timeout for HTTP requests in seconds. Defaults to 30.
        """
        self.base_url = tool_api_url.rstrip('/')
        self.service_name = service_name
        self.service_url = service_url.rstrip('/')
        self.tool_mappings = {}  # Placeholder for future use
        self.tools_to_register = []
        self.timeout = timeout  # Timeout for HTTP requests

    async def health_check(self) -> Dict[str, Any]:
        """
        Performs a health check on the Tool Management API.

        Returns:
            Dict[str, Any]: JSON response from the health endpoint.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()

    async def get_tools(self) -> List[Dict[str, Any]]:
        """
        Retrieves all registered tools.

        Returns:
            List[Dict[str, Any]]: List of tool details.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/tools")
            response.raise_for_status()
            return response.json()

    async def create_tool(self, tool: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new tool.

        Args:
            tool (Dict[str, Any]): Tool details.

        Returns:
            Dict[str, Any]: Created tool details.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/tools", json=tool)
            response.raise_for_status()
            return response.json()

    async def get_tool(self, tool_id: str) -> Dict[str, Any]:
        """
        Retrieves a specific tool by its ID.

        Args:
            tool_id (str): ID of the tool.

        Returns:
            Dict[str, Any]: Tool details.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()
        
    async def get_tool_by_name(self, tool_name: str) -> Dict[str, Any]:
        """
        Retrieves a specific tool by its name.

        Args:
            tool_name (str): Name of the tool.

        Returns:
            Dict[str, Any]: Tool details.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/tools/name/{tool_name}")
            response.raise_for_status()
            return response.json()

    async def update_tool(self, tool_id: str, tool: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates an existing tool.

        Args:
            tool_id (str): ID of the tool to update.
            tool (Dict[str, Any]): Updated tool details.

        Returns:
            Dict[str, Any]: Updated tool details.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.put(f"{self.base_url}/tools/{tool_id}", json=tool)
            response.raise_for_status()
            return response.json()

    async def delete_tool(self, tool_id: str) -> Dict[str, Any]:
        """
        Deletes a specific tool by its ID.

        Args:
            tool_id (str): ID of the tool to delete.

        Returns:
            Dict[str, Any]: Response from the delete operation.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.delete(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()

    # Start of Selection
    async def execute_single_tool(self, tool_execution_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a single tool with the provided input data.

        Args:
            tool_execution_request (Dict[str, Any]): Execution request details.

        Returns:
            Dict[str, Any]: Execution response.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/execute_tool", json=tool_execution_request)
            response.raise_for_status()
            return response.json()
        
    async def search_tools(
        self, 
        prompt: str, 
        node_label: str = "Tool", 
        limit: int = 5, 
        for_model: str = "gpt-4o"
    ) -> List[Dict[str, Any]]:
        """
        Searches for tools using vector similarity based on the provided prompt.

        Args:
            prompt (str): The search prompt.
            node_label (str, optional): The node label to search. Defaults to "Tool".
            limit (int, optional): Maximum number of results to return. Defaults to 5.
            for_model (str, optional): The model to format the results for. Defaults to "anthropic".

        Returns:
            List[Dict[str, Any]]: List of tool information dictionaries.
        """
        search_payload = {
            "prompt": prompt,
            "node_label": node_label,
            "limit": limit,
            "for_model": for_model
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/tools/search", params=search_payload)
            response.raise_for_status()
            return response.json()

    async def execute_tools_sequential(self, sequential_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes a sequence of tools, passing the output from one to the next.

        Args:
            sequential_execution_request (Dict[str, Any]): Sequential execution request details.

        Returns:
            List[Dict[str, Any]]: List of execution responses.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/execute_tools_sequential", json=sequential_execution_request)
            response.raise_for_status()
            return response.json()

    async def execute_tools_parallel(self, parallel_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes multiple tools in parallel.

        Args:
            parallel_execution_request (Dict[str, Any]): Parallel execution request details.

        Returns:
            List[Dict[str, Any]]: List of execution responses.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/execute_tools_parallel", json=parallel_execution_request)
            response.raise_for_status()
            return response.json()

    def endpoint(self, description: str = ""):
        """
        Decorator for FastAPI endpoints. Infers HTTP method, path, and schemas from FastAPI route metadata.

        Usage:
            @app.post("/users", response_model=UserResponse)
            @tools.endpoint(description="Create a new user")
            async def create_user(user: UserCreate) -> UserResponse:
                ...
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            # Extract FastAPI route metadata via metaprogramming
            http_method = getattr(func, '_method', None) or self._infer_http_method(func)
            route_path = getattr(func, '_path', None) or self._infer_route_path(func)
            response_model = getattr(func, '_response_model', None) or sig.return_annotation

            # Find input model from parameters (skip Request, Response, Depends, etc.)
            input_model = None
            for param in params:
                ann = param.annotation
                if ann == inspect.Parameter.empty:
                    continue
                if isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            name = func.__name__
            tool_description = (description + "\n\n" + func.__doc__.strip()) if func.__doc__ else description

            input_schema = generate_json_schema(input_model) if input_model else {"type": "object"}
            output_schema = generate_json_schema(response_model) if response_model and isinstance(response_model, type) and issubclass(response_model, BaseModel) else {"type": "object"}

            tool_schema = {
                "name": name,
                "description": tool_description,
                "type": "endpoint",
                "input_schema": input_schema,
                "output_schema": output_schema,
                "action": {
                    "type": "http",
                    "http": {
                        "method": http_method or "POST",
                        "url": f"{self.service_url}{route_path or f'/{name}'}",
                        "headers": {"Content-Type": "application/json"},
                        "body": {}
                    }
                },
                "metadata": {
                    "framework": "fastapi",
                    "inferred_method": http_method,
                    "inferred_path": route_path
                }
            }

            logger.info(f"Registering endpoint: {name} [{http_method or 'POST'}] {route_path or f'/{name}'}")

            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            wrapper.tool_schema = tool_schema
            self.tools_to_register.append(tool_schema)
            return wrapper
        return decorator

    def function(self, description: str = "", name: Optional[str] = None):
        """
        Decorator for standalone functions with Pydantic input/output models.

        Usage:
            @tools.function(description="Calculate sum of numbers")
            def calculate_sum(input: CalculateInput) -> CalculateOutput:
                return CalculateOutput(result=sum(input.numbers))
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            # Extract input model from first Pydantic parameter
            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            output_model = sig.return_annotation
            func_name = name or func.__name__
            tool_description = (description + "\n\n" + func.__doc__.strip()) if func.__doc__ else description

            input_schema = generate_json_schema(input_model) if input_model else {"type": "object"}
            output_schema = generate_json_schema(output_model) if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {"type": "object"}

            tool_schema = {
                "name": func_name,
                "description": tool_description,
                "type": "function",
                "input_schema": input_schema,
                "output_schema": output_schema,
                "action": {
                    "type": "function",
                    "function": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "is_async": asyncio.iscoroutinefunction(func)
                    }
                }
            }

            logger.info(f"Registering function: {func_name}")

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            async_wrapper.tool_schema = tool_schema
            async_wrapper._original_func = func
            self.tools_to_register.append(tool_schema)
            return async_wrapper
        return decorator

    def snippet(
        self,
        language: str,
        description: str = "",
        name: Optional[str] = None,
        runtime: Optional[str] = None,
        dependencies: Optional[List[str]] = None
    ):
        """
        Decorator for code snippets in various languages (JavaScript, Python, SQL, etc.).

        Usage:
            @tools.snippet(language="javascript", description="Format a date string")
            def format_date():
                '''
                function formatDate(date) {
                    return new Date(date).toLocaleDateString('en-US');
                }
                '''

            @tools.snippet(language="sql", description="Get active users")
            def get_active_users():
                '''
                SELECT * FROM users WHERE status = 'active' ORDER BY created_at DESC;
                '''
        """
        def decorator(func: Callable):
            # Extract code from docstring or function body
            code = inspect.getdoc(func) or ""
            if not code:
                # Try to get from source
                try:
                    source = inspect.getsource(func)
                    # Extract string literals from function body
                    import ast
                    tree = ast.parse(source)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Constant) and isinstance(node.value, str):
                            code = node.value
                            break
                except Exception:
                    pass

            snippet_name = name or func.__name__

            # Infer runtime from language if not specified
            inferred_runtime = runtime or self._infer_runtime(language)

            tool_schema = {
                "name": snippet_name,
                "description": description or f"{language} snippet: {snippet_name}",
                "type": "snippet",
                "input_schema": {"type": "object", "properties": {}, "required": []},
                "output_schema": {"type": "string"},
                "action": {
                    "type": "code",
                    "code": {
                        "language": language.lower(),
                        "runtime": inferred_runtime,
                        "source": code.strip(),
                        "dependencies": dependencies or []
                    }
                },
                "metadata": {
                    "language": language,
                    "runtime": inferred_runtime,
                    "dependencies": dependencies or []
                }
            }

            logger.info(f"Registering snippet: {snippet_name} [{language}]")

            @wraps(func)
            def wrapper(*args, **kwargs):
                return code.strip()

            wrapper.tool_schema = tool_schema
            wrapper.source_code = code.strip()
            self.tools_to_register.append(tool_schema)
            return wrapper
        return decorator

    def _infer_http_method(self, func: Callable) -> Optional[str]:
        """Infer HTTP method from FastAPI route decorator metadata."""
        # Check common FastAPI route attributes
        for method in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD']:
            if hasattr(func, f'_{method.lower()}'):
                return method
        # Check __wrapped__ chain for route info
        current = func
        while hasattr(current, '__wrapped__'):
            current = current.__wrapped__
            if hasattr(current, '_method'):
                return current._method
        # Infer from function name conventions
        name = func.__name__.lower()
        if name.startswith(('get_', 'list_', 'fetch_', 'retrieve_')):
            return 'GET'
        elif name.startswith(('create_', 'add_', 'post_')):
            return 'POST'
        elif name.startswith(('update_', 'modify_', 'put_')):
            return 'PUT'
        elif name.startswith(('delete_', 'remove_')):
            return 'DELETE'
        elif name.startswith(('patch_', 'partial_')):
            return 'PATCH'
        return None

    def _infer_route_path(self, func: Callable) -> Optional[str]:
        """Infer route path from FastAPI route decorator metadata."""
        # Check for path attribute
        if hasattr(func, '_path'):
            return func._path
        if hasattr(func, 'path'):
            return func.path
        # Check __wrapped__ chain
        current = func
        while hasattr(current, '__wrapped__'):
            current = current.__wrapped__
            if hasattr(current, '_path'):
                return current._path
        return None

    def _infer_runtime(self, language: str) -> str:
        """Infer runtime environment from language."""
        runtimes = {
            'javascript': 'node',
            'js': 'node',
            'typescript': 'node',
            'ts': 'node',
            'python': 'python3',
            'py': 'python3',
            'ruby': 'ruby',
            'rb': 'ruby',
            'go': 'go',
            'golang': 'go',
            'rust': 'cargo',
            'rs': 'cargo',
            'sql': 'postgres',
            'mysql': 'mysql',
            'postgres': 'postgres',
            'postgresql': 'postgres',
            'bash': 'bash',
            'sh': 'sh',
            'shell': 'bash',
            'r': 'Rscript',
            'julia': 'julia',
            'php': 'php',
            'perl': 'perl',
            'lua': 'lua',
            'swift': 'swift',
            'kotlin': 'kotlin',
            'java': 'java',
            'scala': 'scala',
            'clojure': 'clojure',
            'elixir': 'elixir',
            'erlang': 'erlang',
            'haskell': 'ghc',
            'c': 'gcc',
            'cpp': 'g++',
            'c++': 'g++',
            'csharp': 'dotnet',
            'c#': 'dotnet',
            'fsharp': 'dotnet',
            'f#': 'dotnet'
        }
        return runtimes.get(language.lower(), language.lower())

    def task(
        self,
        description: str = "",
        name: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: int = 0
    ):
        """
        Decorator for long-running tasks with progress tracking and streaming support.

        Usage:
            @tools.task(description="Process large dataset", timeout=3600)
            async def process_dataset(input: DatasetInput) -> ProcessResult:
                for i, chunk in enumerate(input.chunks):
                    yield StreamEvent(event_type="progress", data={"chunk": i, "total": len(input.chunks)})
                    await process_chunk(chunk)
                return ProcessResult(status="complete")
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            output_model = sig.return_annotation
            is_async_gen = inspect.isasyncgenfunction(func)
            is_gen = inspect.isgeneratorfunction(func)
            func_name = name or func.__name__
            tool_description = (description + "\n\n" + func.__doc__.strip()) if func.__doc__ else description

            input_schema = generate_json_schema(input_model) if input_model else {"type": "object"}
            output_schema = generate_json_schema(output_model) if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {"type": "object"}

            tool_schema = {
                "name": func_name,
                "description": tool_description,
                "type": "task",
                "input_schema": input_schema,
                "output_schema": output_schema,
                "streaming": is_async_gen or is_gen,
                "action": {
                    "type": "task",
                    "task": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "is_async": asyncio.iscoroutinefunction(func) or is_async_gen,
                        "is_generator": is_async_gen or is_gen,
                        "timeout": timeout,
                        "retries": retries
                    }
                },
                "metadata": {
                    "timeout": timeout,
                    "retries": retries,
                    "supports_streaming": is_async_gen or is_gen
                }
            }

            logger.info(f"Registering task: {func_name} [streaming={is_async_gen or is_gen}]")

            @wraps(func)
            async def wrapper(*args, **kwargs) -> Union[TaskState, AsyncIterator[StreamEvent]]:
                task_manager = get_task_manager()
                task_state = task_manager.create_task()
                task_id = task_state.task_id

                async def run_task():
                    try:
                        task_manager.update_task(task_id, status=TaskStatus.RUNNING)

                        if is_async_gen:
                            task_manager.update_task(task_id, status=TaskStatus.STREAMING)
                            result = None
                            async for event in func(*args, **kwargs):
                                if isinstance(event, StreamEvent):
                                    task_manager.add_event(task_id, event)
                                    if event.event_type == "progress" and isinstance(event.data, dict):
                                        progress = event.data.get("progress", event.data.get("percent"))
                                        if progress is not None:
                                            task_manager.update_task(task_id, progress=progress)
                                else:
                                    result = event
                            task_manager.update_task(task_id, status=TaskStatus.COMPLETED, result=result, progress=100.0)
                        elif is_gen:
                            task_manager.update_task(task_id, status=TaskStatus.STREAMING)
                            result = None
                            for event in func(*args, **kwargs):
                                if isinstance(event, StreamEvent):
                                    task_manager.add_event(task_id, event)
                                else:
                                    result = event
                                await asyncio.sleep(0)  # Yield control
                            task_manager.update_task(task_id, status=TaskStatus.COMPLETED, result=result, progress=100.0)
                        elif asyncio.iscoroutinefunction(func):
                            result = await func(*args, **kwargs)
                            task_manager.update_task(task_id, status=TaskStatus.COMPLETED, result=result, progress=100.0)
                        else:
                            result = func(*args, **kwargs)
                            task_manager.update_task(task_id, status=TaskStatus.COMPLETED, result=result, progress=100.0)

                    except asyncio.CancelledError:
                        task_manager.update_task(task_id, status=TaskStatus.CANCELLED)
                        raise
                    except Exception as e:
                        task_manager.update_task(task_id, status=TaskStatus.FAILED, error=str(e))
                        logger.exception(f"Task {task_id} failed: {e}")
                        raise

                future = asyncio.create_task(run_task())
                task_manager.register_future(task_id, future)

                return task_state

            wrapper.tool_schema = tool_schema  # type: ignore
            wrapper._original_func = func  # type: ignore
            wrapper._task_manager = get_task_manager  # type: ignore
            self.tools_to_register.append(tool_schema)
            return wrapper
        return decorator

    def stream(
        self,
        description: str = "",
        name: Optional[str] = None,
        chunk_type: str = "text"
    ):
        """
        Decorator for streaming responses (SSE, chunked data, etc.).

        Usage:
            @tools.stream(description="Stream log output", chunk_type="text")
            async def stream_logs(input: LogStreamInput) -> AsyncIterator[str]:
                async for line in tail_logs(input.file_path):
                    yield line

            @tools.stream(description="Stream JSON events", chunk_type="json")
            async def stream_events(input: EventInput) -> AsyncIterator[dict]:
                async for event in event_source:
                    yield {"event": event.type, "data": event.data}
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            func_name = name or func.__name__
            tool_description = (description + "\n\n" + func.__doc__.strip()) if func.__doc__ else description
            is_async_gen = inspect.isasyncgenfunction(func)

            input_schema = generate_json_schema(input_model) if input_model else {"type": "object"}

            tool_schema = {
                "name": func_name,
                "description": tool_description,
                "type": "stream",
                "input_schema": input_schema,
                "output_schema": {"type": "string", "format": "stream"},
                "streaming": True,
                "action": {
                    "type": "stream",
                    "stream": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "chunk_type": chunk_type,
                        "is_async": is_async_gen
                    }
                },
                "metadata": {
                    "chunk_type": chunk_type,
                    "content_type": "text/event-stream" if chunk_type == "sse" else "application/x-ndjson" if chunk_type == "json" else "text/plain"
                }
            }

            logger.info(f"Registering stream: {func_name} [chunk_type={chunk_type}]")

            @wraps(func)
            async def wrapper(*args, **kwargs) -> StreamingResponse:
                async def generate():
                    try:
                        if is_async_gen:
                            async for chunk in func(*args, **kwargs):
                                if chunk_type == "json":
                                    yield json.dumps(chunk if isinstance(chunk, dict) else {"data": chunk}) + "\n"
                                elif chunk_type == "sse":
                                    data = json.dumps(chunk) if isinstance(chunk, dict) else str(chunk)
                                    yield f"data: {data}\n\n"
                                else:
                                    yield str(chunk)
                        else:
                            for chunk in func(*args, **kwargs):
                                if chunk_type == "json":
                                    yield json.dumps(chunk if isinstance(chunk, dict) else {"data": chunk}) + "\n"
                                elif chunk_type == "sse":
                                    data = json.dumps(chunk) if isinstance(chunk, dict) else str(chunk)
                                    yield f"data: {data}\n\n"
                                else:
                                    yield str(chunk)
                                await asyncio.sleep(0)
                    except Exception as e:
                        logger.exception(f"Stream error: {e}")
                        if chunk_type == "sse":
                            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                        elif chunk_type == "json":
                            yield json.dumps({"error": str(e)}) + "\n"
                        raise

                content_type = tool_schema["metadata"]["content_type"]
                return StreamingResponse(generate(), media_type=content_type)

            wrapper.tool_schema = tool_schema  # type: ignore
            wrapper._original_func = func  # type: ignore
            self.tools_to_register.append(tool_schema)
            return wrapper
        return decorator

    async def register_tools(self):
        """
        Registers or updates tools with the Tool Management API based on the collected tool schemas.
        Also handles dynamic loading of tools from the filesystem.
        """
        # First load any tools from the filesystem
        await self.load_tools_from_filesystem()
        
        existing_tools = await self.get_tools()
        existing_tool_names = {tool['name']: tool for tool in existing_tools}

        for tool_schema in self.tools_to_register:
            tool_name = tool_schema['name']
            try:
                if tool_name in existing_tool_names:
                    existing_tool = existing_tool_names[tool_name]
                    if self._schemas_match(existing_tool, tool_schema):
                        logger.info(f"Tool '{tool_name}' already exists with matching schema. Reusing.")
                        continue
                    else:
                        logger.info(f"Tool '{tool_name}' exists but schemas don't match. Updating.")
                        response = await self.update_tool(existing_tool['id'], tool_schema)
                else:
                    logger.info(f"Attempting to register new tool: {tool_name}")
                    response = await self.create_tool(tool_schema)
                
                logger.info(f"Tool '{tool_name}' registered/updated successfully")
                logger.debug(f"Registration/update response: {response}")
            except httpx.HTTPError as http_err:
                logger.error(f"HTTP error occurred while registering/updating tool '{tool_name}': {str(http_err)}")
                if http_err.response:
                    logger.error(f"Response status: {http_err.response.status_code}")
                    logger.error(f"Response content: {http_err.response.text}")
            except Exception as e:
                logger.error(f"Failed to register/update tool '{tool_name}': {str(e)}")
                logger.error(f"Tool schema that failed: {tool_schema}")

    async def load_tools_from_filesystem(self):
        """
        Dynamically loads tools from the filesystem.
        Looks for Python files in the tools directory and registers any functions decorated with @endpoint.
        """
        import os
        import importlib.util
        import sys
        
        # Define the tools directory - you can make this configurable
        tools_dir = os.path.join(os.path.dirname(__file__), "tools")
        
        # Create tools directory if it doesn't exist
        os.makedirs(tools_dir, exist_ok=True)
        
        # Add tools directory to Python path
        if tools_dir not in sys.path:
            sys.path.append(tools_dir)
        
        # Scan for Python files in the tools directory
        for filename in os.listdir(tools_dir):
            if filename.endswith('.py'):
                try:
                    module_name = filename[:-3]  # Remove .py extension
                    module_path = os.path.join(tools_dir, filename)
                    
                    # Load the module
                    spec = importlib.util.spec_from_file_location(module_name, module_path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        
                        # Look for functions decorated with @endpoint
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if hasattr(attr, 'tool_schema'):
                                tool_schema = getattr(attr, 'tool_schema')
                                if tool_schema not in self.tools_to_register:
                                    self.tools_to_register.append(tool_schema)
                                    logger.info(f"Loaded tool '{tool_schema['name']}' from {filename}")
                
                except Exception as e:
                    logger.error(f"Error loading tool from {filename}: {str(e)}")

    def _schemas_match(self, existing_tool: Dict[str, Any], new_tool_schema: Dict[str, Any]) -> bool:
        """
        Compares relevant parts of the existing tool schema with the new tool schema.

        Args:
            existing_tool (Dict[str, Any]): Existing tool details.
            new_tool_schema (Dict[str, Any]): New tool schema to compare.

        Returns:
            bool: True if schemas match, False otherwise.
        """
        keys_to_compare = ['name', 'description', 'input_schema', 'output_schema', 'action']
        for key in keys_to_compare:
            existing_value = existing_tool.get(key)
            new_value = new_tool_schema.get(key)
            if existing_value != new_value:
                logger.debug(f"Mismatch found in key '{key}': existing='{existing_value}' vs new='{new_value}'")
                return False
        return True

    def setup_fastapi(self, include_task_routes: bool = True) -> FastAPI:
        """
        Sets up FastAPI application with lifespan events and optional task management routes.

        Args:
            include_task_routes: Whether to include task management endpoints.

        Returns:
            FastAPI: Configured FastAPI application.
        """
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await self.register_tools()
            yield

        app = FastAPI(lifespan=lifespan)

        if include_task_routes:
            self._register_task_routes(app)

        return app

    def _register_task_routes(self, app: FastAPI) -> None:
        """Register task management endpoints."""
        from fastapi import HTTPException

        @app.get("/tasks/{task_id}", response_model=TaskState)
        async def get_task_status(task_id: str) -> TaskState:
            """Get the current status of a task."""
            task_manager = get_task_manager()
            state = task_manager.get_task(task_id)
            if not state:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            return state

        @app.get("/tasks/{task_id}/events")
        async def get_task_events(task_id: str, since: int = 0) -> List[StreamEvent]:
            """Get events from a task, optionally filtered by sequence number."""
            task_manager = get_task_manager()
            state = task_manager.get_task(task_id)
            if not state:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            return [e for e in state.events if e.sequence >= since]

        @app.get("/tasks/{task_id}/stream")
        async def stream_task_events(task_id: str) -> StreamingResponse:
            """Stream task events as SSE."""
            task_manager = get_task_manager()
            state = task_manager.get_task(task_id)
            if not state:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

            async def event_stream():
                last_seq = 0
                while True:
                    current_state = task_manager.get_task(task_id)
                    if not current_state:
                        break

                    # Send new events
                    for event in current_state.events[last_seq:]:
                        yield f"event: {event.event_type}\ndata: {json.dumps(event.model_dump(), default=str)}\n\n"
                        last_seq = event.sequence + 1

                    # Send status updates
                    yield f"event: status\ndata: {json.dumps({'status': current_state.status, 'progress': current_state.progress})}\n\n"

                    if current_state.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                        yield f"event: done\ndata: {json.dumps({'status': current_state.status, 'result': current_state.result, 'error': current_state.error}, default=str)}\n\n"
                        break

                    await asyncio.sleep(0.1)

            return StreamingResponse(event_stream(), media_type="text/event-stream")

        @app.post("/tasks/{task_id}/cancel")
        async def cancel_task(task_id: str) -> Dict[str, Any]:
            """Cancel a running task."""
            task_manager = get_task_manager()
            if task_manager.cancel_task(task_id):
                return {"status": "cancelled", "task_id": task_id}
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found or not cancellable")

        @app.get("/tasks")
        async def list_tasks(status: Optional[str] = None) -> List[TaskState]:
            """List all tasks, optionally filtered by status."""
            task_manager = get_task_manager()
            tasks = list(task_manager._tasks.values())
            if status:
                tasks = [t for t in tasks if t.status == status]
            return tasks
