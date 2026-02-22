from typing import Callable, Type, Dict, Any, Union, Coroutine, List, Optional
from pydantic import BaseModel, Field
from functools import wraps
import inspect
import asyncio
import httpx
import logging
import contextlib
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi import Form, File, UploadFile, Request, FastAPI
from fastapi.params import Depends

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_json_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    return model.schema()

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
        # Shared persistent httpx client for connection pooling
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )

    async def close(self):
        """Close the shared httpx client."""
        await self._client.aclose()

    async def health_check(self) -> Dict[str, Any]:
        """
        Performs a health check on the Tool Management API.

        Returns:
            Dict[str, Any]: JSON response from the health endpoint.
        """
        response = await self._client.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

    async def get_tools(self) -> List[Dict[str, Any]]:
        """
        Retrieves all registered tools.

        Returns:
            List[Dict[str, Any]]: List of tool details.
        """
        response = await self._client.get(f"{self.base_url}/tools")
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
        response = await self._client.post(f"{self.base_url}/tools", json=tool)
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
        response = await self._client.get(f"{self.base_url}/tools/{tool_id}")
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
        response = await self._client.get(f"{self.base_url}/tools/name/{tool_name}")
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
        response = await self._client.put(f"{self.base_url}/tools/{tool_id}", json=tool)
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
        response = await self._client.delete(f"{self.base_url}/tools/{tool_id}")
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
        response = await self._client.post(f"{self.base_url}/execute_tool", json=tool_execution_request)
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
        response = await self._client.post(f"{self.base_url}/tools/search", params=search_payload)
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
        response = await self._client.post(f"{self.base_url}/execute_tools_sequential", json=sequential_execution_request)
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
        response = await self._client.post(f"{self.base_url}/execute_tools_parallel", json=parallel_execution_request)
        response.raise_for_status()
        return response.json()

    def tool_wrapper(
        self, 
        description: str = "", 
        service_name: Optional[str] = None, 
        endpoint: Optional[str] = None
    ):
        """
        Decorator to register a function as a tool.

        Args:
            description (str, optional): Description of the tool. Defaults to "".
            service_name (Optional[str], optional): Service name override. Defaults to None.
            endpoint (Optional[str], optional): API endpoint override. Defaults to None.

        Returns:
            Callable: Decorated function.
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())
            
            # Check if the function is a FastAPI endpoint by looking for 'is_fastapi_endpoint' attribute
            is_fastapi_endpoint = getattr(func, 'is_fastapi_endpoint', False)
            
            if is_fastapi_endpoint:
                input_model = next(
                    (param.annotation for param in params if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel)),
                    None
                )
                output_model = getattr(func, 'response_model', None)
            else:
                input_model = params[0].annotation if params else None
                output_model = sig.return_annotation
            
            name = func.__name__
            tool_description = description + "\n\n" + (func.__doc__.strip() if func.__doc__ else "")
            
            input_schema = generate_json_schema(input_model) if input_model and isinstance(input_model, type) and issubclass(input_model, BaseModel) else {"type": "object"}
            output_schema = generate_json_schema(output_model) if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {"type": "object"}
            
            tool_schema = {
                "name": name,
                "description": tool_description,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "action": {
                    "type": "http",
                    "http": {
                        "method": "POST",
                        "url": f"{self.service_url}{endpoint or f'/{name}'}",
                        "headers": {
                            "Content-Type": "application/json"
                        },
                        "body": {}
                    }
                },
                "output": {
                    "type": "ai",
                    "content": ""
                },
                "user_settings": []
            }
            
            logger.info(f"Registering tool: {name}")
            logger.debug(f"Tool schema: {tool_schema}")
            
            @wraps(func)
            async def wrapper(*args, **kwargs):
                if is_fastapi_endpoint:
                    result = await func(*args, **kwargs)
                    if isinstance(result, output_model):
                        return result
                    else:
                        return output_model(**result)
                else:
                    async def dispatch(input_data: Any):
                        tool_execution_request = {
                            "tool_id": name,
                            "input_data": input_data.dict() if hasattr(input_data, 'dict') else input_data
                        }
                        result = await self.execute_single_tool(tool_execution_request)
                        return result['output'] if 'output' in result else result
                    return dispatch
            
            wrapper.tool_schema = tool_schema
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
        Looks for Python files in the tools directory and registers any functions decorated with @tool_wrapper.
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
                        
                        # Look for functions decorated with @tool_wrapper
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

    def setup_fastapi(self) -> FastAPI:
        """
        Sets up FastAPI application with the required lifespan events.

        Returns:
            FastAPI: Configured FastAPI application.
        """
        app = FastAPI()

        @app.on_event("startup")
        async def startup_event():
            await self.register_tools()

        # Additional route registrations can be added here if necessary

        return app
