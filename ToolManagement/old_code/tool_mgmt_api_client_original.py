from typing import Callable, Type, Dict, Any, Union, Coroutine, List, Literal
from pydantic import BaseModel, Field
from functools import wraps
import inspect
import asyncio
import httpx
import json
import logging
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi import Form, File, UploadFile, Request
from typing import Optional, Union
from fastapi.params import Depends

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class ToolExecutionError(Exception):
    """Custom exception for errors during tool execution."""
    def __init__(self, message: str, tool_id: str = None, status_code: int = None):
        super().__init__(message)
        self.tool_id = tool_id
        self.status_code = status_code


def generate_json_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    """
    Generate a JSON schema from a Pydantic model.

    Args:
        model (Type[BaseModel]): The Pydantic model to generate the schema from.

    Returns:
        Dict[str, Any]: The JSON schema representation of the model.
    """
    return model.schema()

from typing import Dict, Any, List, Union, Optional                                                                                                                                                                                                                          
from pydantic import BaseModel, Field                                                                                                                                                                                                                                        
from enum import Enum                                                                                                                                                                                                                                                        
from neo4j.time import DateTime, Date, Time, Duration                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                              
class Neo4jSerializableMixin:                                                                                                                                                                                                                                                
    @classmethod                                                                                                                                                                                                                                                             
    def __get_validators__(cls):                                                                                                                                                                                                                                             
        yield cls.validate                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                             
    @classmethod                                                                                                                                                                                                                                                             
    def validate(cls, v):                                                                                                                                                                                                                                                    
        if isinstance(v, (DateTime, Date, Time, Duration)):                                                                                                                                                                                                                  
            return str(v)                                                                                                                                                                                                                                                    
        return v                                                                                                                                                                                                                                                             
                                                                                                                                                                                                                                                                              
class CustomBaseModel(BaseModel, Neo4jSerializableMixin):                                                                                                                                                                                                                    
    class Config:                                                                                                                                                                                                                                                            
        arbitrary_types_allowed = True                                                                                                                                                                                                                                       

from datetime import datetime

class BasedModel(BaseModel):                                                                                                                                                                                                                                                 
    class Config:                                                                                                                                                                                                                                                            
        json_encoders = {                                                                                                                                                                                                                                                    
            datetime: lambda v: v.isoformat(),                                                                                                                                                                                                                               
        }                                                                                                                                                                                                                                                                    
                                                                                                                                                                                                                                                                            
    def dict(self, *args, **kwargs):                                                                                                                                                                                                                                         
        d = super().model_dump(*args, **kwargs)                                                                                                                                                                                                                              
        for k, v in d.items():                                                                                                                                                                                                                                               
            if isinstance(v, datetime):                                                                                                                                                                                                                                      
                d[k] = v.isoformat()                                                                                                                                                                                                                                         
        return d                                                                                                                                                                                                                                                             
                                                                                                                                                                                                                                                                            
    def model_dump(self, *args, **kwargs):                                                                                                                                                                                                                                   
        return self.dict(*args, **kwargs)                                                                                                                                                                                                                                    
                                                                                                                                                                                                                                                                            
class BasedModelEncoder(json.JSONEncoder):                                                                                                                                                                                                                                   
    def default(self, obj):                                                                                                                                                                                                                                                  
        if isinstance(obj, datetime):                                                                                                                                                                                                                                        
            return obj.isoformat()                                                                                                                                                                                                                                           
        elif isinstance(obj, (list, tuple)):                                                                                                                                                                                                                                 
            return [self.default(x) for x in obj]                                                                                                                                                                                                                            
        elif isinstance(obj, BasedModel):                                                                                                                                                                                                                                    
            return obj.dict()                                                                                                                                                                                                                                                
        return super().default(obj)                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                            
class Neo4jSerializableMixin:                                                                                                                                                                                                                                                
    @classmethod                                                                                                                                                                                                                                                             
    def __get_validators__(cls):                                                                                                                                                                                                                                             
        yield cls.validate                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                            
    @classmethod                                                                                                                                                                                                                                                             
    def validate(cls, v):                                                                                                                                                                                                                                                    
        if isinstance(v, (DateTime, Date, Time, Duration)):                                                                                                                                                                                                                  
            return str(v)                                                                                                                                                                                                                                                    
        return v                                                                                                                                                                                                                                                             
                                                                                                                                                                                                                                                                            
class CustomBaseModel(BasedModel, Neo4jSerializableMixin):                                                                                                                                                                                                                   
    class Config:                                                                                                                                                                                                                                                            
        arbitrary_types_allowed = True                                                                                                                                                                                                                                                             

allowed_models = [BasedModel, BaseModel, CustomBaseModel, CustomBaseModel, StreamingResponse, Coroutine, Form, File, UploadFile, Depends, Field, Request]

def adhoc_tool(runtime: Literal["python", "javascript"] = "python"):
    """
    Decorator to mark a function as an ad hoc tool.
    
    Args:
        runtime: The runtime environment ("python" or "javascript")
    """
    def decorator(func):
        func.is_adhoc = True
        func.runtime = runtime
        return func
    return decorator

class ToolManagementAPI:
    """
    A class for managing and interacting with tools through an API.
    """

    def __init__(self, tool_api_url: str, service_name: str = "Your Service Name", service_url: str = "http://yourserviceapi.internal", debug: bool = False):
        """
        Initialize the ToolManagementAPI.

        Args:
            tool_api_url (str): The base URL for the tool management API.
            service_name (str, optional): The name of the service. Defaults to "Your Service Name".
            service_url (str, optional): The URL of the service. Defaults to "http://yourserviceapi.internal".
            debug (bool, optional): Enable debug mode. Defaults to False.
        """
        self.base_url = tool_api_url
        self.service_name = service_name
        self.service_url = service_url
        self.tool_mappings = {}  # New dictionary to store tool mappings
        self.debug = debug

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the tool management API.

        Returns:
            Dict[str, Any]: The health check response.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()

    async def get_tools(self) -> List[Dict[str, Any]]:
        """
        Get all tools from the tool management API.

        Returns:
            List[Dict[str, Any]]: A list of all tools.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools")
            response.raise_for_status()
            return response.json()

    async def create_tool(self, tool: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new tool using the tool management API.

        Args:
            tool (Dict[str, Any]): The tool data to create.
                For ad hoc tools, include:
                - is_adhoc: bool
                - runtime: "python" or "javascript" 
                - code: string of code to execute

        Returns:
            Dict[str, Any]: The created tool data.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/tools", json=tool)
            response.raise_for_status()
            return response.json()

    async def update_user_settings(self, tool_id: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update user settings for a specific tool.

        Args:
            tool_id (str): The ID of the tool to update settings for.
            settings (Dict[str, Any]): The user settings to update.

        Returns:
            Dict[str, Any]: The response data from the update operation.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/tools/{tool_id}/user_settings", json=settings)
            response.raise_for_status()
            return response.json()

    async def get_tool(self, tool_id: str) -> Dict[str, Any]:
        """
        Get a specific tool from the tool management API.

        Args:
            tool_id (str): The ID of the tool to retrieve.

        Returns:
            Dict[str, Any]: The tool data.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()

    async def update_tool(self, tool_id: str, tool: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a specific tool using the tool management API.

        Args:
            tool_id (str): The ID of the tool to update.
            tool (Dict[str, Any]): The updated tool data.

        Returns:
            Dict[str, Any]: The updated tool data.
        """
        async with httpx.AsyncClient() as client:
            response = await client.put(f"{self.base_url}/tools/{tool_id}", json=tool)
            response.raise_for_status()
            return response.json()

    async def delete_tool(self, tool_id: str) -> Dict[str, Any]:
        """
        Delete a specific tool using the tool management API.

        Args:
            tool_id (str): The ID of the tool to delete.

        Returns:
            Dict[str, Any]: The response data from the deletion operation.
        """
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{self.base_url}/tools/{tool_id}")
            response.raise_for_status()
            return response.json()
        
    async def get_tool_by_name(self, tool_name: str) -> Dict[str, Any]:
        """
        Get a specific tool by name from the tool management API.

        Args:
            tool_name (str): The name of the tool to retrieve.

        Returns:
            Dict[str, Any]: The tool data.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/tools?name={tool_name}")
            response.raise_for_status()
            tools = response.json()
            return tools[0] if tools else None
    

    # Start of Selection
    async def execute_single_tool(self, tool_execution_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single tool using the tool management API.

        Args:
            tool_execution_request (Dict[str, Any]): The execution request data.

        Returns:
            Dict[str, Any]: The execution response data.
        """
        tool_id = tool_execution_request.get('tool_id')
        tool_name = tool_execution_request.get('tool_name')
        input_data = tool_execution_request.get('input_data', {})
        user_settings = tool_execution_request.get('user_settings', {})

        # Fetch the tool details
        if tool_id:
            tool = await self.get_tool(tool_id)
        elif tool_name:
            tool = await self.get_tool_by_name(tool_name)
        else:
            raise ValueError("Either 'tool_id' or 'tool_name' must be provided in the tool_execution_request.")

        # Use the tool's action to determine how to execute it
        action = tool.get('action', {})
        action_type = action.get('type')

        if action_type == 'http':
            http_action = action.get('http', {})
            method = http_action.get('method', 'GET').upper()
            url = http_action.get('url')
            headers = http_action.get('headers', {})
            body = http_action.get('body', {})

            # Merge input_data into body or params
            if method in ['POST', 'PUT', 'PATCH']:
                # For these methods, send input_data as JSON body
                body.update(input_data)
                params = None
            else:
                # For GET/DELETE, send input_data as query parameters
                params = input_data
                body = None

            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=body,
                    params=params
                )
                response.raise_for_status()
                return response.json()

        elif action_type == 'python':
            # For Python ad hoc tools, execute the code directly
            python_action = action.get('python', {})
            code = python_action.get('code', '')
            
            # Execute in sandbox
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/execute_tool",
                    json={
                        "tool_id": tool_id,
                        "input_data": input_data,
                        "user_settings": user_settings
                    }
                )
                response.raise_for_status()
                return response.json()

        elif action_type == 'javascript':
            # For JavaScript ad hoc tools
            js_action = action.get('javascript', {})
            code = js_action.get('code', '')
            
            # Execute JavaScript code
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/execute_tool",
                    json={
                        "tool_id": tool_id,
                        "input_data": input_data,
                        "user_settings": user_settings
                    }
                )
                response.raise_for_status()
                return response.json()

        else:
            raise ValueError(f"Unsupported action type: {action_type}")

    async def execute_tools_sequential(self, sequential_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute multiple tools sequentially using the tool management API.

        Args:
            sequential_execution_request (Dict[str, Any]): The sequential execution request data.

        Returns:
            List[Dict[str, Any]]: The list of execution response data for each tool.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/execute_tools_sequential", json=sequential_execution_request)
            response.raise_for_status()
            return response.json()

    async def execute_tools_parallel(self, parallel_execution_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute multiple tools in parallel using the tool management API.

        Args:
            parallel_execution_request (Dict[str, Any]): The parallel execution request data.

        Returns:
            List[Dict[str, Any]]: The list of execution response data for each tool.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/execute_tools_parallel", json=parallel_execution_request)
            response.raise_for_status()
            return response.json()

    def tool_wrapper(self, description: str = "", service_name: Optional[str] = None, endpoint: Optional[str] = None, debug: Optional[bool] = None):
        """
        A decorator for wrapping functions as tools and registering them with the tool management API.

        Args:
            description (str, optional): A description of the tool. Defaults to "".
            service_name (Optional[str], optional): The name of the service. Defaults to None.
            endpoint (Optional[str], optional): The endpoint for the tool. Defaults to None.
            debug (Optional[bool], optional): Enable debug mode for this tool. Defaults to None (uses class-level debug setting).

        Returns:
            Callable: A decorator function.
        """
        debug = self.debug if debug is None else debug
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())
            
            # Check if the function is a FastAPI endpoint
            is_fastapi_endpoint = any(param.name == 'response_model' for param in params)
            
            if not is_fastapi_endpoint and (not params or not (
                isinstance(params[0].annotation, type) and (
                    any(issubclass(params[0].annotation, allowed_model) for allowed_model in allowed_models) or
                    params[0].annotation in allowed_models
                )
            )):
                raise ValueError(f"First parameter of {func.__name__} must be a Pydantic model, Form, File, UploadFile, Depends, Field, or Request")
            
            input_model = params[0].annotation if not is_fastapi_endpoint else None
            output_model = sig.return_annotation if not is_fastapi_endpoint else None
            
            if not is_fastapi_endpoint and not (any(issubclass(output_model, allowed_model) for allowed_model in allowed_models) or output_model in [StreamingResponse, Coroutine, RedirectResponse]):
                raise ValueError(f"Return type of {func.__name__} must be a Pydantic model, StreamingResponse, Coroutine, or RedirectResponse")
            
            @wraps(func)
            async def wrapper(*args, **kwargs):
                name = func.__name__ or endpoint
                tool_description = description + "\n\n" + (func.__doc__.strip() if func.__doc__ else "")
                
                if is_fastapi_endpoint:
                    # For FastAPI endpoints, we need to extract the input and output schemas from the function's parameters
                    input_schema = {}
                    output_schema = {}
                    for param in params:
                        if param.name == 'response_model':
                            output_schema = generate_json_schema(param.default)
                        elif isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
                            input_schema = generate_json_schema(param.annotation)
                else:
                    input_schema = generate_json_schema(input_model) if issubclass(input_model, BaseModel) else {}
                    output_schema = generate_json_schema(output_model) if issubclass(output_model, BaseModel) else {"type": "string", "format": "binary"}
                
                # Determine if this is an ad hoc tool
                is_adhoc = getattr(func, 'is_adhoc', False)
                runtime = getattr(func, 'runtime', None)
                code = inspect.getsource(func) if is_adhoc else None

                tool_schema = {
                    "name": name,
                    "description": tool_description,
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "is_adhoc": is_adhoc,
                    "runtime": runtime,
                    "code": code if is_adhoc else None,
                    "action": {
                        "type": runtime if is_adhoc else "http",
                        "http": {
                            "method": "POST",
                            "url": f"{self.service_url}/{name}"
                        } if not is_adhoc else None,
                        "python": {
                            "code": code
                        } if is_adhoc and runtime == "python" else None,
                        "javascript": {
                            "code": code
                        } if is_adhoc and runtime == "javascript" else None
                    },
                    "output": {
                        "type": "json",
                        "content": ""
                    }
                }
                
                try:
                    await self.create_tool(tool_schema)
                    logger.info(f"Tool with name '{name}' registered")
                except Exception as e:
                    logger.error(f"Failed to register tool '{name}': {str(e)}")
                    raise
                
                async def dispatch(input_data: Any):
                    tool_execution_request = {
                        "tool_id": name,
                        "input_data": input_data.dict() if isinstance(input_data, BaseModel) else input_data
                    }
                    print(f"tool_execution_request: {json.dumps(tool_execution_request,indent=2)}")
                    return await self.execute_single_tool(tool_execution_request)
                
                return dispatch
            return wrapper
        return decorator

# # Create a global instance of ToolManagementAPI
# tool_management_api = ToolManagementAPI("http://localhost:2016")  # Replace with your actual Tool API URL
