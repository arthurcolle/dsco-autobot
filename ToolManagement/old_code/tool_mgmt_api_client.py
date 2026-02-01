from typing import Callable, Type, Dict, Any, Union, Coroutine, List
from pydantic import BaseModel, Field
from functools import wraps
import inspect
import asyncio
import httpx
import logging
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi import Form, File, UploadFile, Request
from typing import Optional, Union
from fastapi.params import Depends

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class ToolManagementAPI:
    """
    A class for managing and interacting with tools through an API.
    """

    def __init__(self, tool_api_url: str, service_name: str = "Your Service Name", service_url: str = "http://yourserviceapi.internal"):
        """
        Initialize the ToolManagementAPI.

        Args:
            tool_api_url (str): The base URL for the tool management API.
            service_name (str, optional): The name of the service. Defaults to "Your Service Name".
            service_url (str, optional): The URL of the service. Defaults to "http://yourserviceapi.internal".
        """
        self.base_url = tool_api_url
        self.service_name = service_name
        self.service_url = service_url
        self.tool_mappings = {}  # New dictionary to store tool mappings

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

        Returns:
            Dict[str, Any]: The created tool data.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/tools", json=tool)
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

    async def execute_single_tool(self, tool_execution_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single tool using the tool management API.

        Args:
            tool_execution_request (Dict[str, Any]): The execution request data.

        Returns:
            Dict[str, Any]: The execution response data.
        """
        tool_id = tool_execution_request['tool_id']
        input_data = tool_execution_request['input_data']
        
        # Fetch the tool details
        tool = await self.get_tool(tool_id)
        
        # Use the tool's service_url and endpoint
        full_url = f"{tool['service_url']}{tool['endpoint']}"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(full_url, json=input_data)
            response.raise_for_status()
            return response.json()

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

    def tool_wrapper(self, description: str = "", service_name: Optional[str] = None, endpoint: Optional[str] = None):
        """
        A decorator for wrapping functions as tools and registering them with the tool management API.

        Args:
            description (str, optional): A description of the tool. Defaults to "".
            service_name (Optional[str], optional): The name of the service. Defaults to None.
            endpoint (Optional[str], optional): The endpoint for the tool. Defaults to None.

        Returns:
            Callable: A decorator function.
        """
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
                
                tool_schema = {
                    "name": name,
                    "description": tool_description,
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "service_url": self.service_url,
                    "endpoint": f"/{name}"
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
                    return await self.execute_single_tool(tool_execution_request)
                
                return dispatch
            return wrapper
        return decorator
