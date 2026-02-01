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

allowed_models = [BaseModel, StreamingResponse, Coroutine, Form, File, UploadFile, Depends, Field, Request]

class ToolManagementAPI:
    def __init__(self, tool_api_url: str, service_name: str = "Your Service Name", service_url: str = "http://yourserviceapi.internal"):
        self.base_url = tool_api_url
        self.service_name = service_name
        self.service_url = service_url
        self.tool_mappings = {}  # New dictionary to store tool mappings
        self.tools_to_register = []

    async def health_check(self) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()

    async def get_tools(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools")
            response.raise_for_status()
            return response.json()

    async def create_tool(self, tool: Dict[str, Any]) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/tools", json=tool)
            response.raise_for_status()
            return response.json()

    async def get_tool(self, tool_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()
        
    async def get_tool_by_name(self, tool_name: str) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools/name/{tool_name}")
            response.raise_for_status()
            return response.json()

    async def update_tool(self, tool_id: str, tool: Dict[str, Any]) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.put(f"{self.base_url}/tools/{tool_id}", json=tool)
            response.raise_for_status()
            return response.json()

    async def delete_tool(self, tool_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()

    # Start of Selection
    async def execute_single_tool(self, tool_execution_request: Dict[str, Any]) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/execute_single_tool", json=tool_execution_request)
            response.raise_for_status()
            return response.json()


    async def execute_tools_sequential(self, sequential_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/execute_tools_sequential", json=sequential_execution_request)
            response.raise_for_status()
            return response.json()

    async def execute_tools_parallel(self, parallel_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/execute_tools_parallel", json=parallel_execution_request)
            response.raise_for_status()
            return response.json()

    def tool_wrapper(self, description: str = "", service_name: Optional[str] = None, endpoint: Optional[str] = None):
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())
            
            # Check if the function is a FastAPI endpoint
            is_fastapi_endpoint = hasattr(func, '__fastapi_route__')
            
            if is_fastapi_endpoint:
                input_model = next((param.annotation for param in params if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel)), None)
                output_model = func.__fastapi_route__.response_model
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
    
    async def endpoint(self, endpoint: str, method: str, input_model: Type[BaseModel], output_model: Type[BaseModel]):
        # For backwards compatibility, delegate to tool_wrapper
        def decorator(func):
            return self.tool_wrapper(
                endpoint=endpoint,
                service_name=None
            )(func)
        return decorator

    async def register_tools(self):
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
            except Exception as e:
                logger.error(f"Failed to register/update tool '{tool_name}': {str(e)}")
                logger.error(f"Tool schema that failed: {tool_schema}")
                if hasattr(e, 'response'):
                    logger.error(f"Response status: {e.response.status_code}")
                    logger.error(f"Response content: {e.response.text}")

    def _schemas_match(self, existing_tool: Dict[str, Any], new_tool_schema: Dict[str, Any]) -> bool:
        # Compare relevant parts of the schema
        keys_to_compare = ['name', 'description', 'input_schema', 'output_schema', 'action']
        return all(existing_tool.get(key) == new_tool_schema.get(key) for key in keys_to_compare)

    def setup_fastapi(self, app: FastAPI):
        @contextlib.asynccontextmanager
        async def lifespan(app: FastAPI):
            await self.register_tools()
            yield

        app.router.lifespan_context = lifespan

    def lifecycle_hook(self):
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Run the original function
                result = await func(*args, **kwargs)
                # After function execution, synchronize tools
                await self.register_tools()
                return result
            return wrapper
        return decorator
