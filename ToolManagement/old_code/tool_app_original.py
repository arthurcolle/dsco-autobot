import os
import json
import asyncio
import httpx
from httpx import AsyncClient
from fastapi import FastAPI, HTTPException, Depends, Body, WebSocket, Request, status
from prometheus_client import make_asgi_app
import logging
from cache import cache_response, rate_limit
import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Initialize Redis with timeout
logger.info("Attempting to initialize Redis connection...")
logger.debug(f"Redis Host: {os.getenv('REDIS_HOST', 'localhost')}")
logger.debug(f"Redis Port: {os.getenv('REDIS_PORT', 6379)}")

try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        socket_timeout=5,  # 5 second timeout
        socket_connect_timeout=5,
        decode_responses=True  # Add this for proper string handling
    )
    # Test the connection
    redis_client.ping()
    logger.info("Redis connection established successfully")
except (RedisConnectionError, RedisTimeoutError) as e:
    logger.error(f"Redis connection failed: {str(e)}", exc_info=True)
    logger.error("Redis will be disabled and caching will not be available")
    redis_client = None
except Exception as e:
    logger.error(f"Unexpected error connecting to Redis: {str(e)}", exc_info=True)
    logger.error("Redis will be disabled and caching will not be available")
    redis_client = None
from metrics import (
    track_request_metrics, track_tool_metrics,
    http_requests_total, tool_executions_total,
    cache_hits, cache_misses
)
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, ValidationError
from typing import List, Dict, Any, Union, Optional, Tuple, Literal
from schemas import (
    ToolValidationRequest, ToolValidationResponse,
    PendingToolsResponse, AgentToolsResponse,
    ToolFormatRequest, UserSettingsUpdateRequest,
    UserSettingsUpdateResponse
)
from enum import Enum

class FormatType(str, Enum):
    anthropic = "anthropic"
    openai = "openai"
from fastapi.middleware.cors import CORSMiddleware
from clients.embed import EmbeddingAPI, EmbeddingRequest
from clients.graph import GraphAPI
import websockets
import yfinance as yf
import uuid
import sys
from sandbox import PythonSandbox
# from engine_api_client import EngineAPIClient

# Initialize FastAPI app
app = FastAPI(
    title="Tool Management API",
    version="1.0.0",
    docs_url=None if os.getenv("ENVIRONMENT") == "production" else "/docs",
    redoc_url=None if os.getenv("ENVIRONMENT") == "production" else "/redoc"
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# WebSocket connection
ws_connection = None
from tool_mgmt_api_client import ToolManagementAPI as ToolManagementAPIClient

# Environment variables for Engine API
# ENGINE_API_HOST = os.getenv("ENGINE_API_HOST", "localhost")
# ENGINE_API_PORT = os.getenv("ENGINE_API_PORT", "2020")
# ENGINE_API_URL = os.getenv("ENGINE_API_URL", f"http://{ENGINE_API_HOST}:{ENGINE_API_PORT}").rstrip("'")
# HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "30"))  # in seconds

# Initialize Engine API Client
# engine_api_client = EngineAPIClient(ENGINE_API_URL)

# Attempt to import Neo4j
from neo4j import GraphDatabase, AsyncGraphDatabase
from typing import Union as Neo4jSession

# Add CORS middleware with safe defaults
origins = os.getenv("CORS_ORIGINS", "*").split(",")
if len(origins) == 1 and origins[0] == "*":
    # If wildcard or no origins specified, use ["*"]
    origins = ["*"]
else:
    # Clean any whitespace from origins
    origins = [origin.strip() for origin in origins]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {request.method} {request.url} - Status: {response.status_code}")
    return response

# Set up logging
import logging
from fastapi.logger import logger as fastapi_logger

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Ensure that FastAPI's logger uses the same configuration
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)

# Define Pydantic models
class JavaScriptCode(BaseModel):
    code: str
    function_name: str

class PythonCode(BaseModel):
    code: str
    function_name: str

class HTTPAction(BaseModel):
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
    url: str
    headers: Optional[Dict[str, str]] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None

class ToolAction(BaseModel):
    type: Literal["javascript", "http", "python"]
    javascript: Optional[JavaScriptCode] = None
    http: Optional[HTTPAction] = None
    python: Optional[PythonCode] = None

class ToolOutput(BaseModel):
    type: Literal["ai", "markdown", "html"]
    content: str

class UserSetting(BaseModel):
    name: str
    label: str
    type: str = "text"
    description: Optional[str] = None
    required: bool = False
    placeholder: Optional[str] = None
    values: Optional[List[str]] = None

class Tool(BaseModel):
    name: str
    description: str
    input_schema: dict
    output_schema: dict
    action: ToolAction
    output: ToolOutput
    user_settings: Optional[List[UserSetting]] = None
    code: Optional[str] = None
    is_adhoc: Optional[bool] = False
    runtime: Optional[Literal["python", "javascript"]] = None
    created_by: Optional[str] = "user"  # Can be "user" or "agent"
    agent_id: Optional[str] = None  # ID of the agent that created the tool
    validation_status: Optional[Literal["pending", "approved", "rejected"]] = "pending"

class ToolResponse(BaseModel):
    id: str
    name: str
    description: str
    input_schema: dict
    output_schema: dict
    action: ToolAction
    output: ToolOutput
    version: str

class ToolExecutionRequest(BaseModel):
    tool_id: str
    input_data: Dict[str, Any]

class ToolExecutionResponse(BaseModel):
    tool_id: str
    output_data: Dict[str, Any]

class SequentialToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    initial_input: Dict[str, Any]
    format_type: Optional[FormatType] = None

class ParallelToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    input_data: Dict[str, Dict[str, Any]]
    format_type: Optional[FormatType] = None

class Neo4jConnection:
    """Manages the Neo4j database connection."""

    def __init__(self):
        """Initialize the Neo4j connection."""
        uri = os.getenv("NEO4J_URI", "neo4j://bolt.n4j.distributed.systems:7687")
        username = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "Backstab2025!")
        
        logger.info(f"Connecting to Neo4j at: {uri}")
        
        # Initialize Neo4j driver with minimal config for direct connection
        self.driver = GraphDatabase.driver(
            uri,
            auth=(username, password),
            encrypted=False,  # Disable encryption for direct bolt connection
            resolver=lambda x: [("bolt.n4j.distributed.systems", 7687)]  # Direct resolver
        )
        
        # Verify connection
        try:
            self.driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise
        
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()

    def get_session(self):
        """Get a Neo4j session."""
        return self.driver.session()

    def execute_query(self, query, params=None):
        """Execute a Cypher query and return results."""
        with self.get_session() as session:
            result = session.run(query, parameters=params or {})
            return list(result)

    async def add_tool(self, name: str, description: str):
        async with self.driver.session() as session:
            result = await session.run(
                "CREATE (t:Tool {name: $name, description: $description}) RETURN t",
                name=name,
                description=description
            )
            return await result.single()

    async def add_tool_schema(self, tool_name: str, input_schema: dict, output_schema: dict):
        async with self.driver.session() as session:
            result = await session.run(
                "CREATE (s:ToolSchema {tool_name: $tool_name, input_schema: $input_schema, output_schema: $output_schema}) RETURN s",
                tool_name=tool_name,
                input_schema=json.dumps(input_schema),
                output_schema=json.dumps(output_schema)
            )
            return await result.single()

    async def add_tool_code(self, tool_name: str, code: bytes):
        async with self.driver.session() as session:
            result = await session.run(
                "CREATE (c:ToolCode {tool_name: $tool_name, code: $code}) RETURN c",
                tool_name=tool_name,
                code=code
            )
            return await result.single()

    async def link_tool_to_schema(self, tool_id: int, schema_id: int, schema_type: str):
        async with self.driver.session() as session:
            relationship_type = "HAS_INPUT_SCHEMA" if schema_type == "input" else "HAS_OUTPUT_SCHEMA"
            result = await session.run(
                f"MATCH (t:Tool), (s:ToolSchema) WHERE ID(t) = $tool_id AND ID(s) = $schema_id CREATE (t)-[r:{relationship_type}]->(s) RETURN r",
                tool_id=tool_id,
                schema_id=schema_id
            )
            return await result.single()

    async def link_tool_to_code(self, tool_id: int, code_id: int):
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (t:Tool), (c:ToolCode) WHERE ID(t) = $tool_id AND ID(c) = $code_id CREATE (t)-[r:HAS_CODE]->(c) RETURN r",
                tool_id=tool_id,
                code_id=code_id
            )
            return await result.single()

    @staticmethod
    def get_stock_data(ticker, interval, start_date, end_date):
        ticker_obj = yf.Ticker(ticker)
        return ticker_obj.history(start=start_date, end=end_date, interval=interval)

def get_db():
    """Dependency to get database session."""
    db = Neo4jConnection()
    try:
        yield db
    finally:
        db.close()

def get_session(db: Neo4jConnection = Depends(get_db)) -> Neo4jSession:
    """Get the Memgraph connection."""
    return db.get_session()

def generate_endpoint_overview():
    """Generate an overview of all endpoints and their schemas."""
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    
    endpoint_overview = []
    for path, path_item in openapi_schema["paths"].items():
        for method, operation in path_item.items():
            endpoint_info = {
                "path": path,
                "method": method.upper(),
                "summary": operation.get("summary", ""),
                "description": operation.get("description", ""),
                "parameters": operation.get("parameters", []),
                "requestBody": operation.get("requestBody", {}),
                "responses": operation.get("responses", {})
            }
            endpoint_overview.append(endpoint_info)
    
    return endpoint_overview

import socket

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Root endpoint displaying IP, health check, and endpoint overview."""
    client_ip = request.client.host if request.client else request.headers.get("X-Forwarded-For", "Unknown")
    try:
        system_ip = socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        system_ip = "Unable to determine"
    health_status = await health_check()
    health_status = health_status["status"]
    endpoint_overview = generate_endpoint_overview()
    
    html_content = f"""
    <html>
        <head>
            <title>{app.title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                h1 {{ color: #333; }}
                h2 {{ color: #666; }}
                pre {{ background-color: #f4f4f4; padding: 10px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>{app.title}</h1>
            <p><strong>Your IP:</strong> {client_ip}</p>
            <p><strong>System IP:</strong> {system_ip}</p>
            <p><strong>Health Check:</strong> {health_status}</p>
            <h2>Endpoint Overview:</h2>
            <pre>{json.dumps(endpoint_overview, indent=2)}</pre>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
@track_request_metrics()
@cache_response(ttl=30) if redis_client else lambda x: x  # Only cache if Redis is available
async def health_check():
    """Health check endpoint with detailed system status."""
    try:
        # Check Redis connection with timeout
        redis_status = "ok" if redis_client and redis_client.ping() else "error"
    except (RedisConnectionError, RedisTimeoutError) as e:
        redis_status = f"error: {str(e)}"
    except Exception as e:
        redis_status = f"error: Unexpected error - {str(e)}"

    # Check Memgraph connection
    try:
        session = get_session()
        memgraph_status = "ok" if session else "error"
    except Exception as e:
        memgraph_status = f"error: {str(e)}"

    # Get system metrics
    import psutil
    system_metrics = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent
    }

    return {
        "status": "healthy",
        "version": app.version,
        "timestamp": datetime.now().isoformat(),
        "dependencies": {
            "memgraph": memgraph_status,
            "redis": redis_status
        },
        "system_metrics": system_metrics,
        "api_metrics": {
            "total_requests": sum(v._value.get() for v in http_requests_total._metrics.values()),
            "total_tool_executions": sum(v._value.get() for v in tool_executions_total._metrics.values()),
            "cache_hit_ratio": sum(v._value.get() for v in cache_hits._metrics.values()) / 
                             (sum(v._value.get() for v in cache_hits._metrics.values()) + 
                              sum(v._value.get() for v in cache_misses._metrics.values()))
                             if (sum(v._value.get() for v in cache_hits._metrics.values()) + 
                                 sum(v._value.get() for v in cache_misses._metrics.values())) > 0 
                             else 0
        }
    }


client = ToolManagementAPIClient(
    os.getenv("TOOL_MANAGEMENT_API_URL", "https://tools-test.brainchain.cloud").rstrip("'"),
    service_name="BrainchainIntegrationAPI",
    service_url=os.getenv("SERVICE_URL", "https://tools.brainchain.cloud"),
    debug=os.getenv("DEBUG", "False").lower() == "true"
)

tool = client.tool_wrapper

class ListToolsRequest(BaseModel):
    pass

class ListToolResponse(BaseModel):
    tools: List[ToolResponse]

# Define a function to be used as a tool
@tool(description="Retrieve all registered tools")
async def get_all_tools(input: ListToolsRequest) -> ListToolResponse:
    """
    Retrieve all registered tools from the Tool Management API.
    
    :param input_data: An empty dictionary (no input required for this tool)
    :return: A list of dictionaries containing tool information
    """
    try:
        tools = await get_tools()  # This calls the existing get_tools function
        return [tool.dict() for tool in tools]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve tools: {str(e)}")


@app.post("/tools", response_model=ToolResponse)
async def create_tool(tool: Tool, agent_id: Optional[str] = None, session: Neo4jSession = Depends(get_session)):
    """Create a new tool."""
    if tool.is_adhoc and tool.code:
        # For ad hoc tools, validate the code using sandbox
        sandbox = PythonSandbox()
        test_result = await sandbox.execute(
            tool.code,
            {"test": "data"},
            {}
        )
        if test_result.error:
            raise HTTPException(
                status_code=400,
                detail=f"Code validation failed: {test_result.error}"
            )
    """Create a new tool."""
    logger.info(f"Received request to create new tool: {tool.name}")
    try:
        # Set tool metadata
        if agent_id:
            tool.created_by = "agent"
            tool.agent_id = agent_id
            tool.validation_status = "pending"
        else:
            tool.created_by = "user"
            tool.validation_status = "approved"

        # Initialize the EmbeddingAPI
        embed_api_url = os.getenv("EMBED_API_URL", "https://https://arthurcolle--embeddings.modal.run")
        if not embed_api_url.startswith(('http://', 'https://')):
            embed_api_url = f"https://{embed_api_url}"
        if embed_api_url == "https://https://arthurcolle--embeddings.modal.run":
            logger.warning("EMBED_API_URL environment variable not set. Using default URL.")
        logger.info(f"Using embedding API URL: {embed_api_url}")
        embedding_api = EmbeddingAPI(embed_api_url)
        
        # Generate embeddings
        tool_text = f"{tool.name} {tool.description}"
        tool_embeddings = await embedding_api.generate_embeddings([tool_text])
        schema_text = f"{json.dumps(tool.input_schema)} {json.dumps(tool.output_schema)}"
        schema_embeddings = await embedding_api.generate_embeddings([schema_text])
        tool_id = str(uuid.uuid4())
        
        # Get the latest version number for this tool name
        result = session.run(
            """
            MATCH (t:Tool {name: $name})
            RETURN max(t.version) as latest_version
            """,
            {'name': tool.name}
        )
        # Get results synchronously
        record = result.single()
        latest_version = record["latest_version"] if record and record["latest_version"] is not None else "0.0.0"
        new_version = increment_version(latest_version)
        
        # Add status property, version, and user settings to Tool node
        # Execute Neo4j query synchronously since driver doesn't support async
        result = session.run(
            """
            CREATE (t:Tool {
                id: $tool_id, 
                name: $name, 
                description: $description, 
                embedding: $tool_embedding, 
                status: $status, 
                version: $version, 
                user_settings: $user_settings,
                created_by: $created_by,
                agent_id: $agent_id,
                validation_status: $validation_status
            })
            CREATE (s:ToolSchema {id: $schema_id, input_schema: $input_schema, output_schema: $output_schema, embedding: $schema_embedding})
            CREATE (a:ToolAction {type: $action_type, javascript: $javascript, http: $http, python: $python})
            CREATE (o:ToolOutput {type: $output_type, content: $output_content})
            CREATE (t)-[:HAS_SCHEMA]->(s)
            CREATE (t)-[:HAS_ACTION]->(a)
            CREATE (t)-[:HAS_OUTPUT]->(o)
            RETURN t, s, a, o
            """,
            {
                'tool_id': tool_id,
                'schema_id': str(uuid.uuid4()),
                'name': tool.name,
                'description': tool.description,
                'created_by': tool.created_by,
                'agent_id': tool.agent_id,
                'validation_status': tool.validation_status,
                'input_schema': json.dumps(tool.input_schema),
                'output_schema': json.dumps(tool.output_schema),
                'tool_embedding': tool_embeddings["embeddings"][0]["embedding"],
                'schema_embedding': schema_embeddings["embeddings"][0]["embedding"],
                'status': "online",
                'version': new_version,
                'user_settings': json.dumps([setting.dict() for setting in tool.user_settings]) if tool.user_settings else None,
                'action_type': tool.action.type,
                'javascript': json.dumps(tool.action.javascript.dict()) if tool.action.javascript else None,
                'http': json.dumps(tool.action.http.dict()) if tool.action.http else None,
                'python': json.dumps(tool.action.python.dict()) if tool.action.python else None,
                'output_type': tool.output.type,
                'output_content': tool.output.content
            }
        )

        record = result.single()
        
        if record:
            tool_node = record["t"]
            schema_node = record["s"]
            action_node = record["a"]
            output_node = record["o"]
            logger.info(f"Tool created successfully: {tool.name} (ID: {tool_node['id']}, Version: {new_version})")
            response = ToolResponse(
                id=tool_node["id"],
                name=tool_node["name"],
                description=tool_node["description"],
                input_schema=json.loads(schema_node["input_schema"]),
                output_schema=json.loads(schema_node["output_schema"]),
                action=ToolAction(
                    type=action_node["type"],
                    javascript=JavaScriptCode(**json.loads(action_node["javascript"])) if action_node["javascript"] else None,
                    http=HTTPAction(**json.loads(action_node["http"])) if action_node["http"] else None,
                    python=PythonCode(**json.loads(action_node["python"])) if action_node["python"] else None
                ),
                output=ToolOutput(
                    type=output_node["type"],
                    content=output_node["content"]
                ),
                version=new_version,
                user_settings=tool.user_settings
            )
            return response
        else:
            raise HTTPException(status_code=500, detail="Failed to create tool")
    except ValidationError as ve:
        logger.error(f"Validation error creating tool: {tool.name}. Error: {str(ve)}", exc_info=True)
        error_details = []
        for error in ve.errors():
            error_details.append(f"Field: {'.'.join(error['loc'])}, Error: {error['msg']}")
        error_message = "Validation error(s):\n" + "\n".join(error_details)
        raise HTTPException(status_code=422, detail=error_message)
    except Exception as e:
        logger.error(f"Error creating tool: {tool.name}. Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create tool: {str(e)}")

def increment_version(version: str) -> str:
    """Increment the version number."""
    major, minor, patch = map(int, version.split('.'))
    return f"{major}.{minor}.{patch + 1}"

from celery import Celery
from celery.schedules import crontab
import httpx

celery_app = Celery('tasks', broker='redis://localhost:6379')

@celery_app.task
async def check_tool_health():
    async with Neo4jSession() as session:
        result = session.run(
            """
            MATCH (t:Tool)
            RETURN t
            """
        )
        async for record in result:
            tool = record["t"]
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{tool['service_url']}/health", timeout=5.0)
                    new_status = "online" if response.status_code == 200 else "offline"
            except Exception as e:
                logger.error(f"Error checking health of tool {tool['id']}: {str(e)}")
                new_status = "offline"
            
            if new_status == "offline":
                session.run(
                    """
                    MATCH (t:Tool {id: $tool_id})
                    DETACH DELETE t
                    """,
                    tool_id=tool["id"]
                )
                logger.info(f"Deleted offline tool: {tool['id']}")
            else:
                session.run(
                    """
                    MATCH (t:Tool {id: $tool_id})
                    SET t.status = $new_status
                    """,
                    tool_id=tool["id"],
                    new_status=new_status
                )

@app.get("/tools", response_model=List[ToolResponse])
async def get_tools(name: Optional[str] = None, id: Optional[str] = None, session: Neo4jSession = Depends(get_session)):
    """Get tools, with optional filtering by name and id."""
    logger.info(f"Fetching tools with name={name}, id={id}")
    query = """
    MATCH (t:Tool)
    OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
    OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
    OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
    """
    conditions = []
    params = {}
    if name:
        conditions.append("t.name = $name")
        params["name"] = name
    if id:
        conditions.append("t.id = $id")
        params["id"] = id
    if conditions:
        query += "\nWHERE " + " AND ".join(conditions)
    query += "\nRETURN t, s, a, o"
    
    # Use synchronous execution since Neo4j driver doesn't support async
    result = session.run(query, params)
    records = list(result)
    tools = []
    for record in records:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        
        # Create a valid ToolResponse even if some relationships are missing
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]) if schema else {},
            output_schema=json.loads(schema["output_schema"]) if schema else {},
            action=ToolAction(
                type=action["type"] if action else "http",
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action and action.get("javascript") else None,
                http=HTTPAction(**json.loads(action["http"])) if action and action.get("http") else None,
                python=PythonCode(**json.loads(action["python"])) if action and action.get("python") else None
            ),
            output=ToolOutput(
                type=output["type"] if output else "markdown",
                content=output["content"] if output else ""
            ),
            version=tool.get("version", "1.0.0")
        )
        tools.append(tool_response)
    logger.info(f"Fetched {len(tools)} tools")
    return tools

# Schedule the health check task to run every minute
celery_app.conf.beat_schedule = {
    'check-tool-health-every-minute': {
        'task': 'app.check_tool_health',
        'schedule': crontab(minute='*'),
    },
}

# Note: The ToolResponse model no longer needs to include the 'status' field

@app.get("/tools", response_model=List[ToolResponse])
async def get_tools(session: Neo4jSession = Depends(get_session)):
    """Get all tools."""
    logger.info("Fetching all tools")
    result = session.run(
        """
        MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        """
    )
    records = await result.fetch()
    tools = []
    result = await session.run(
        """
        MATCH (t:Tool)
        MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        """
    )
    records = await result.fetch()
    for record in records:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None,
                python=PythonCode(**json.loads(action["python"])) if action["python"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool.get("version", "1.0.0")  # Default to "1.0.0" if version is not present
        )
        tools.append(tool_response)
    logger.info(f"Fetched {len(tools)} tools")
    return tools

class GetToolRequest(BaseModel):
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None

@app.get("/tools/{tool_id}", response_model=ToolResponse)
async def get_tool(request: GetToolRequest, session: Neo4jSession = Depends(get_session)):
    """Get a specific tool."""
    logger.info(f"Fetching tool with id: {request.tool_id}")
    query = """
        MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        WHERE t.id = $tool_id OR t.name = $tool_name
        RETURN t, s, a, o
        """
    results = session.execute_and_fetch(
        query,
        tool_id=request.tool_id,
        tool_name=request.tool_name
    )
    records = await results.fetch()
    record = records[0] if records else None
    if record:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        logger.info(f"Tool found: {tool['name']}")
        return ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool.get("version", "1.0.0")  # Add version field with a default value
        )
    else:
        logger.warning(f"Tool not found with id: {request.tool_id}")
        raise HTTPException(status_code=404, detail=f"Tool not found with id: {request.tool_id}")

@app.put("/tools/{tool_id}", response_model=ToolResponse)
async def update_tool(tool_id: str, tool: Tool, session: Neo4jSession = Depends(get_session)):
    """Update a tool."""
    logger.info(f"Updating tool with id: {tool_id}")
    
    # Prepare the Cypher query
    query = """
    MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
    MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
    MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
    WHERE t.id = $tool_id
    SET t.name = $name, t.description = $description,
        s.input_schema = $input_schema, s.output_schema = $output_schema,
        a.type = $action_type, a.javascript = $javascript, a.http = $http,
        o.type = $output_type, o.content = $output_content
    """
    
    # Prepare parameters
    params = {
        "tool_id": tool_id,
        "name": tool.name,
        "description": tool.description,
        "input_schema": json.dumps(tool.input_schema),
        "output_schema": json.dumps(tool.output_schema),
        "action_type": tool.action.type,
        "javascript": json.dumps(tool.action.javascript.dict()) if tool.action.javascript else None,
        "http": json.dumps(tool.action.http.dict()) if tool.action.http else None,
        "output_type": tool.output.type,
        "output_content": tool.output.content
    }
    
    # Add code update if it's present
    if tool.code is not None:
        query += """
        WITH t
        OPTIONAL MATCH (t)-[:HAS_CODE]->(c:ToolCode)
        SET c.code = $code
        """
        params["code"] = tool.code
    
    query += "RETURN t, s, a, o"
    
    result = session.run(query, params)
    record = result.single()
    if record:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        logger.info(f"Tool updated successfully: {tool['name']}")
        return ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool.get("version", "1.0.0")  # Add version field with a default value
        )
    else:
        logger.warning(f"Tool not found for update with id: {tool_id}")
        raise HTTPException(status_code=404, detail="Tool not found")

@app.delete("/tools/{tool_id}")
async def delete_tool(tool_id: str, session: Neo4jSession = Depends(get_session)):
    """Delete a tool."""
    logger.info(f"Deleting tool with id: {tool_id}")
    result = session.run(
            """
            MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
            MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            WHERE t.id = $tool_id
            OPTIONAL MATCH (t)-[:HAS_CODE]->(c:ToolCode)
            DETACH DELETE t, s, a, o, c
            RETURN count(*) as deleted_count
            """,
            tool_id=tool_id
        )
    record = result.single()
    if record and record["deleted_count"] > 0:
        logger.info(f"Tool deleted successfully: {tool_id}")
        return {"message": "Tool deleted successfully"}
    else:
        logger.warning(f"Tool not found for deletion with id: {tool_id}")
        raise HTTPException(status_code=404, detail="Tool not found")

@app.delete("/tools")
async def delete_all_tools(session: Neo4jSession = Depends(get_session)):
    """Delete all tools, schemas, and code."""
    logger.info("Deleting all tools, schemas, and code")
    result = session.run(
            """
            MATCH (t:Tool)
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            OPTIONAL MATCH (t)-[:HAS_CODE]->(c:ToolCode)
            DETACH DELETE t, s, a, o, c
            RETURN count(*) as deleted_count
            """
        )
    record = result.single()
    if record and record["deleted_count"] > 0:
        logger.info(f"All tools, schemas, and code deleted successfully. Count: {record['deleted_count']}")
        return {"message": f"All tools, schemas, and code deleted successfully. Count: {record['deleted_count']}"}
    else:
        logger.warning("No tools found to delete")
        return {"message": "No tools found to delete"}

@app.get("/tools/by_name/{tool_name}", response_model=List[ToolResponse])
async def get_tool_versions(tool_name: str, session: Neo4jSession = Depends(get_session)):
    """Get all versions of a tool by name, including the latest."""
    logger.info(f"Fetching all versions of tool: {tool_name}")
    results = session.execute_and_fetch(
        """
        MATCH (t:Tool {name: $tool_name})-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        ORDER BY t.version DESC
        """,
        {'tool_name': tool_name}
    )
    
    tools = []
    async for record in results:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool["version"]
        )
        tools.append(tool_response)
    
    if not tools:
        logger.warning(f"No tools found with name: {tool_name}")
        raise HTTPException(status_code=404, detail=f"No tools found with name: {tool_name}")
    
    logger.info(f"Fetched {len(tools)} versions of tool: {tool_name}")
    return [tool.model_dump() for tool in tools]

@app.get("/tools/name/{tool_name}", response_model=ToolResponse)
async def get_tool_by_name(tool_name: str, session: Neo4jSession = Depends(get_session)):
    """Get a tool by name."""
    logger.info(f"Fetching tool by name: {tool_name}")
    result = session.run(
        """
        MATCH (t:Tool {name: $tool_name})
        MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        ORDER BY t.version DESC
        LIMIT 1
        """,
        tool_name=tool_name
    )
    
    record = result.single()
    if record:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool["version"]
        )
        logger.info(f"Fetched tool: {tool_name} (Version: {tool['version']})")
        return tool_response
    else:
        logger.warning(f"No tool found with name: {tool_name}")
        raise HTTPException(status_code=404, detail=f"No tool found with name: {tool_name}")

@app.get("/tools/latest/{tool_name}", response_model=ToolResponse)
async def get_latest_tool_version(tool_name: str, session: Neo4jSession = Depends(get_session)):
    """Get the latest version of a tool by name."""
    logger.info(f"Fetching latest version of tool: {tool_name}")
    result = session.run(
        """
        MATCH (t:Tool {name: $tool_name})-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        ORDER BY t.version DESC
        LIMIT 1
        """,
        tool_name=tool_name
    )
    
    record = result.single()
    if record:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool["version"]
        )
        logger.info(f"Fetched latest version of tool: {tool_name} (Version: {tool['version']})")
        return tool_response
    else:
        logger.warning(f"No tool found with name: {tool_name}")
        raise HTTPException(status_code=404, detail=f"No tool found with name: {tool_name}")

async def execute_tool(tool_id: Optional[str] = None, tool_name: Optional[str] = None, input_data: Dict[str, Any] = None, session: Neo4jSession = Depends(get_session), user_settings: Optional[Dict[str, Any]] = None, format_type: Optional[FormatType] = None) -> Dict[str, Any]:
    """Execute a single tool."""
    logger.info(f"Executing tool with id: {tool_id} or name: {tool_name}")
    logger.debug(f"Input data: {input_data}")
    logger.debug(f"User settings: {user_settings}")
    tool = None
    if tool_id:
        result = session.run(
            """
            MATCH (t:Tool {id: $tool_id})
            RETURN t
            """,
            tool_id=tool_id
        )
        record = result.single()
        if record:
            tool = record["t"]
        else:
            logger.error(f"Tool with id '{tool_id}' not found")
            raise HTTPException(status_code=404, detail=f"Tool with id '{tool_id}' not found")
    elif tool_name:
        result = session.run(
            """
            MATCH (t:Tool {name: $tool_name})
            RETURN t
            ORDER BY t.version DESC
            LIMIT 1
            """,
            tool_name=tool_name
        )
        record = result.single()
        if record:
            tool = record["t"]
            tool_id = tool["id"]
        else:
            logger.error(f"Tool with name '{tool_name}' not found")
            raise HTTPException(status_code=404, detail=f"Tool with name '{tool_name}' not found")
    else:
        logger.error("Neither tool_id nor tool_name provided")
        raise HTTPException(status_code=400, detail="Either tool_id or tool_name must be provided")
    
    logger.info(f"Retrieved tool: {tool}")
    
    result = session.run(
        """
        MATCH (t:Tool {id: $tool_id})-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t.name AS name, s.input_schema AS input_schema, s.output_schema AS output_schema,
               a.type AS action_type, 
               CASE WHEN a.javascript IS NOT NULL THEN a.javascript ELSE null END AS javascript,
               CASE WHEN a.http IS NOT NULL THEN a.http ELSE null END AS http,
               CASE WHEN a.python IS NOT NULL THEN a.python ELSE null END AS python,
               o.type AS output_type, o.content AS output_content
        """,
        tool_id=tool_id
    )
    record = result.single()
    if record:
        tool_name = record["name"]
        logger.debug(f"Tool name: {tool_name}")

        input_schema = json.loads(record["input_schema"])
        logger.debug(f"Input schema for tool '{tool_name}': {input_schema}")

        output_schema = json.loads(record["output_schema"])
        logger.debug(f"Output schema for tool '{tool_name}': {output_schema}")

        action_type = record["action_type"]
        logger.info(f"Action type for tool '{tool_name}': {action_type}")

        http = json.loads(record["http"]) if record["http"] else None
        logger.debug(f"HTTP configuration for tool '{tool_name}': {http}")

        output_type = record["output_type"]
        logger.debug(f"Output type for tool '{tool_name}': {output_type}")

        output_content = record["output_content"]
        logger.debug(f"Output content for tool '{tool_name}': {output_content}")
        
        logger.info(f"Successfully parsed all details for tool '{tool_name}'")
        logger.debug(f"Tool '{tool_name}' details: action_type={action_type}, http={http}")
        logger.debug(f"Input schema: {input_schema}")
        logger.debug(f"Output schema: {output_schema}")
        
        try:
            if action_type == "python" and record.get("python"):
                python_code = json.loads(record["python"])
                sandbox = PythonSandbox()
                result = await sandbox.execute(
                    python_code["code"],
                    input_data,
                    user_settings or {}
                )
                if result.error:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Python execution error: {result.error}"
                    )
                output_data = result.output if result.output is not None else {}
                if result.stdout:
                    output_data = {"result": output_data, "stdout": result.stdout}
            elif action_type == "http":
                async with httpx.AsyncClient(timeout=120.0) as client:  # 2 minute timeout
                    method = http["method"].lower()
                    url = replace_template_params(http["url"], input_data)
                    headers = {k: replace_template_params(v, input_data) for k, v in http.get("headers", {}).items()}
                    
                    # Use the input_data as the request body
                    body = input_data
                    
                    logger.info(f"Preparing HTTP request for tool '{tool_name}'")
                    logger.debug(f"HTTP request details: method={method}, url={url}, headers={headers}, body={body}")
                    
                    http_method = getattr(client, method)
                    logger.info(f"Sending HTTP request for tool '{tool_name}'")
                    response = await http_method(url, headers=headers, json=body)
                    
                    logger.info(f"Received HTTP response for tool '{tool_name}'")
                    logger.debug(f"HTTP response status: {response.status_code}")
                    logger.debug(f"HTTP response content: {response.text}")
                    
                    response.raise_for_status()
                    
                    output_data = response.json()
                    logger.info(f"Successfully parsed JSON response for tool '{tool_name}'")
                    logger.debug(f"Parsed output data: {output_data}")
            else:
                logger.error(f"Unsupported action type for tool '{tool_name}': {action_type}")
                raise ValueError(f"Unsupported action type: {action_type}")
            
            logger.info(f"Tool '{tool_name}' ({tool_id}) executed successfully")
            
            if format_type:
                if format_type == FormatType.anthropic:
                    formatted_output = format_for_anthropic([{"name": tool_name, "description": "", "input_schema": input_schema, "output": output_data}])
                elif format_type == FormatType.openai:
                    formatted_output = format_for_openai([{"name": tool_name, "description": "", "input_schema": input_schema, "output": output_data}])
                logger.info(f"Output formatted for {format_type}")
                return formatted_output
            else:
                return output_data
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error executing tool '{tool_name}' ({tool_id}): {str(e)}", exc_info=True)
            logger.error(f"Response content: {e.response.text}")
            error_detail = f"Error executing tool '{tool_name}': {str(e)}. Response content: {e.response.text}"
            raise HTTPException(status_code=e.response.status_code, detail=error_detail)
        except Exception as e:
            logger.error(f"Unexpected error executing tool '{tool_name}' ({tool_id}): {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error executing tool '{tool_name}': {str(e)}")
    else:
        logger.warning(f"Tool not found for execution with id: {tool_id}")
        raise HTTPException(status_code=404, detail=f"Tool with id {tool_id} not found")

def replace_template_params(template: str, params: Dict[str, Any]) -> str:
    """Replace template parameters in a string."""
    for key, value in params.items():
        template = template.replace(f"{{{key}}}", str(value))
    return template

class ToolExecutionRequest(BaseModel):
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None
    input_data: Dict[str, Any]
    format_type: Optional[FormatType] = None

@app.post("/execute_tool", response_model=Dict[str, Any])
async def execute_single_tool(request: ToolExecutionRequest, session: Neo4jSession = Depends(get_session)):
    """
    Execute a single tool with the given input data.
    """
    logger.info(f"Received request to execute tool: {request.tool_id or request.tool_name}")
    logger.debug(f"Input data for tool: {request.input_data}")

    try:
        tool_id = request.tool_id
        tool_name = request.tool_name

        if not tool_id and not tool_name:
            logger.error("Neither tool_id nor tool_name provided")
            raise HTTPException(status_code=400, detail="Either tool_id or tool_name must be provided")

        logger.info(f"Attempting to execute tool: {tool_id or tool_name}")
        result = await execute_tool(
            tool_id=tool_id, 
            tool_name=tool_name, 
            input_data=request.input_data, 
            session=session, 
            format_type=request.format_type
        )
        
        return {"result": result}
        
    except HTTPException as he:
        logger.error(f"HTTP error occurred while executing tool: {str(he)}")
        raise he
    except Exception as e:
        logger.error(f"Unexpected error occurred while executing tool: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    

@app.post("/execute_tools_sequential", response_model=List[ToolExecutionResponse])
async def execute_tools_sequential(request: SequentialToolExecutionRequest, session: Neo4jSession = Depends(get_session)):
    """
    Execute a sequence of tools, passing output from one to the next.

    This endpoint allows for the sequential execution of multiple tools, where the output of one tool
    becomes the input for the next. This is useful for creating tool pipelines or workflows.

    Args:
        request (SequentialToolExecutionRequest): An object containing:
            - tool_ids (List[str]): A list of tool IDs to be executed in sequence.
            - initial_input (Dict[str, Any]): The initial input data for the first tool.
        db (Neo4jConnection): The database connection object.

    Returns:
        List[ToolExecutionResponse]: A list of ToolExecutionResponse objects, each containing:
            - tool_id (str): The ID of the executed tool.
            - output_data (Dict[str, Any]): The output data from the tool execution.

    Example:
        Request:
        {
            "tool_ids": ["1", "2", "3"],
            "initial_input": {"param1": "value1"}
        }

        Response:
        [
            {"tool_id": "1", "output_data": {"result1": "output1"}},
            {"tool_id": "2", "output_data": {"result2": "output2"}},
            {"tool_id": "3", "output_data": {"result3": "output3"}}
        ]

    Raises:
        HTTPException: If there's an error during tool execution or if a tool is not found.
    """
    logger.info(f"Received request to execute tools sequentially: {request.tool_ids}")
    logger.debug(f"Initial input for sequential execution: {request.initial_input}")

    results = []
    current_input = request.initial_input

    for index, tool_id in enumerate(request.tool_ids):
        logger.info(f"Executing tool {tool_id} (Step {index + 1}/{len(request.tool_ids)})")
        logger.debug(f"Input data for tool {tool_id}: {current_input}")

        try:
            output_data = await execute_tool(tool_id, current_input, session=session, user_settings=None, format_type=request.format_type)
            logger.info(f"Tool {tool_id} executed successfully")
            logger.debug(f"Output data from tool {tool_id}: {output_data}")

            results.append(ToolExecutionResponse(tool_id=tool_id, output_data=output_data))
            current_input = output_data

            logger.info(f"Updated input for next tool: {current_input}")
        except HTTPException as e:
            logger.error(f"Error executing tool {tool_id}: {str(e)}")
            raise HTTPException(status_code=e.status_code, detail=f"Error in sequential execution at tool {tool_id}: {e.detail}")
        except Exception as e:
            logger.error(f"Unexpected error executing tool {tool_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Unexpected error in sequential execution at tool {tool_id}: {str(e)}")

    logger.info(f"Sequential execution completed for {len(request.tool_ids)} tools")
    logger.debug(f"Final results of sequential execution: {results}")

    return results

@app.post("/execute_tools_parallel", response_model=List[ToolExecutionResponse])
async def execute_tools_parallel(request: ParallelToolExecutionRequest, session: Neo4jSession = Depends(get_session)):
    """
    Execute multiple tools in parallel.

    This endpoint allows for the simultaneous execution of multiple tools, improving efficiency
    when dealing with independent tasks. It takes a ParallelToolExecutionRequest object, which
    contains a list of tool IDs and their corresponding input data.

    Args:
        request (ParallelToolExecutionRequest): An object containing:
            - tool_ids (List[str]): A list of tool IDs to be executed.
            - input_data (Dict[str, Dict[str, Any]]): A dictionary mapping tool IDs to their input data.
        db (Neo4jConnection): The database connection object.

    Returns:
        List[ToolExecutionResponse]: A list of ToolExecutionResponse objects, each containing:
            - tool_id (str): The ID of the executed tool.
            - output_data (Dict[str, Any]): The output data from the tool execution.

    Example:
        Request:
        {
            "tool_ids": ["1", "2", "3"],
            "input_data": {
                "1": {"param1": "value1"},
                "2": {"param2": "value2"},
                "3": {"param3": "value3"}
            }
        }

        Response:
        [
            {"tool_id": "1", "output_data": {"result1": "output1"}},
            {"tool_id": "2", "output_data": {"result2": "output2"}},
            {"tool_id": "3", "output_data": {"result3": "output3"}}
        ]

    Raises:
        HTTPException: If there's an error during tool execution or if a tool is not found.
    """
    logger.info(f"Received request to execute tools in parallel: {request.tool_ids}")
    logger.debug(f"Input data for parallel execution: {request.input_data}")

    # Create tasks for each tool execution
    tasks = []
    for tool_id in request.tool_ids:
        input_data = request.input_data.get(tool_id, {})
        logger.debug(f"Creating task for tool {tool_id} with input data: {input_data}")
        tasks.append(execute_tool(tool_id, input_data, session=session, user_settings=None, format_type=request.format_type))

    logger.info(f"Starting parallel execution of {len(tasks)} tools")
    try:
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)
        logger.info(f"Parallel execution completed successfully for {len(results)} tools")
    except Exception as e:
        logger.error(f"Error during parallel execution: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during parallel execution: {str(e)}")

    # Prepare the response
    response = []
    for tool_id, output in zip(request.tool_ids, results):
        logger.debug(f"Tool {tool_id} execution result: {output}")
        response.append(ToolExecutionResponse(tool_id=tool_id, output_data=output))

    logger.info(f"Returning results for {len(response)} tools")
    return response

from fastapi.responses import JSONResponse

def format_tools(tools, for_model="anthropic"):
    """Format tools for the specified model."""
    if for_model == "openai":
        return format_for_openai(tools)
    elif for_model == "anthropic":
        return format_for_anthropic(tools)
    else:
        return tools

def format_for_openai(tools):
    """Format tools for OpenAI function calling."""
    openai_functions = []
    for tool in tools:
        openai_function = {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"]
            }
        }
        openai_functions.append(openai_function)
    return openai_functions

def format_for_anthropic(tools):
    """Format tools for Anthropic function calling."""
    anthropic_tools = []
    for tool in tools:
        anthropic_tool = {
            "name": tool["name"],
            "description": tool["description"],
            "input_schema": tool["input_schema"]
        }
        anthropic_tools.append(anthropic_tool)
    return anthropic_tools

@app.post("/tools/search")
async def search_tools(prompt: str = Body(...), node_label: str = Body("Tool"), limit: int = Body(5), for_model: str = Body("anthropic")):
    """
    Search for tools using vector similarity with a given prompt.

    Args:
        prompt (str): The search prompt.
        node_label (str): The node label to search (default: "Tool").
        limit (int): The maximum number of results to return (default: 5).
        for_model (str): The model to format the results for (default: "anthropic").

    Returns:
        List[Dict]: A list of tool information dictionaries formatted for the specified model.
    """
    logger.info(f"Searching tools with prompt: '{prompt}', node_label: '{node_label}', limit: {limit}, for_model: {for_model}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://arthurcolle--embeddings.modal.run/generate_embeddings",
                json={"texts": [prompt], "batch_size": 32, "max_length": 8192},
                headers={"accept": "application/json", "Content-Type": "application/json"}
            )
            response.raise_for_status()
            search_results = response.json()

        logger.info(f"Search completed. Found {len(search_results)} results.")
        logger.debug(f"Search results: {search_results}")

        # Ensure search_results is a list
        if not isinstance(search_results, list):
            logger.warning(f"Unexpected response format. Expected list, got {type(search_results)}")
            return []

        # Process the results
        processed_results = []
        for result in search_results:
            if isinstance(result, dict) and 'node' in result:
                processed_results.append(result['node'])
            else:
                logger.warning(f"Unexpected result format: {result}")

        # Format the results for the specified model
        formatted_results = format_tools(processed_results, for_model)
        logger.info(f"Results formatted for {for_model}")
        logger.debug(f"Formatted results: {formatted_results}")

        return formatted_results
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during tool search: {e.response.status_code} {e.response.reason_phrase}")
        logger.error(f"Response content: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"Error during tool search: {e.response.reason_phrase}")
    except Exception as e:
        logger.error(f"Error during tool search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error during tool search: {str(e)}")

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    if isinstance(exc, RequestValidationError):
        error_details = [f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()]
        error_message = "Validation error(s): " + "; ".join(error_details)
        logger.error(error_message)
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": error_message}
        )
    logger.error(f"HTTP exception: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    global ws_connection
    ws_connection = websocket
    
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Message received: {data}")
            await asyncio.sleep(0.01)  # Changed to asyncio.sleep for non-blocking wait
    except websockets.exceptions.ConnectionClosed:
        ws_connection = None

async def send_heartbeat():
    global ws_connection
    heartbeat_interval = 30  # Default value, you can adjust this as needed
    while True:
        await asyncio.sleep(heartbeat_interval)
        if ws_connection:
            try:
                await ws_connection.send_text("heartbeat")
                logger.info("Heartbeat sent")
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {str(e)}")
                ws_connection = None

# Create a background task for the heartbeat
heartbeat_task = None

@app.on_event("startup")
async def start_heartbeat():
    global heartbeat_task
    heartbeat_task = asyncio.create_task(send_heartbeat())

@app.on_event("shutdown")
async def stop_heartbeat():
    if heartbeat_task:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup initiated")
    
    # Initialize Redis connection status
    if redis_client:
        try:
            redis_client.ping()
            logger.info("Redis connection verified during startup")
        except Exception as e:
            logger.error(f"Redis connection check failed during startup: {str(e)}", exc_info=True)
    else:
        logger.warning("Redis client not available - caching will be disabled")
    
    # Start heartbeat
    logger.info("Starting heartbeat task")
    asyncio.create_task(send_heartbeat())
    
    # Register tools
    logger.info("Starting tool registration process")
    await register_tools()
    
    logger.info("Application startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated")
    
    # Clean up Redis connection if it exists
    if redis_client:
        try:
            redis_client.close()
            logger.info("Redis connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}", exc_info=True)
    
    logger.info("Application shutdown completed")

async def register_tools():
    """Register all tools at startup."""
    for route in app.routes:
        if isinstance(route, APIRoute):
            endpoint = route.endpoint
            if hasattr(endpoint, 'tool_schema'):
                try:
                    response = await client.create_tool(endpoint.tool_schema)
                    if isinstance(response, dict) and 'id' in response:
                        logger.info(f"Tool '{endpoint.__name__}' registered successfully with ID: {response['id']}")
                    else:
                        logger.warning(f"Unexpected response when registering tool '{endpoint.__name__}': {response}")
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error occurred while registering tool '{endpoint.__name__}': {e.response.status_code} {e.response.reason_phrase}")
                    logger.error(f"Response content: {e.response.text}")
                except Exception as e:
                    logger.error(f"Failed to register tool '{endpoint.__name__}' at startup: {str(e)}", exc_info=True)
    
    logger.info("Tool registration process completed")

@app.post("/tools/validate/{tool_id}", response_model=ToolValidationResponse)
async def validate_tool(
    tool_id: str,
    request: ToolValidationRequest,
    session: Neo4jSession = Depends(get_session)
):
    """Validate or reject a tool created by an agent."""
    logger.info(f"Validating tool {tool_id} with action: {request.action}")
    
    result = await session.run(
        """
        MATCH (t:Tool {id: $tool_id})
        WHERE t.created_by = 'agent'
        SET t.validation_status = $status
        RETURN t
        """,
        tool_id=tool_id,
        status=request.action
    )
    
    records = await result.fetch()
    record = records[0] if records else None
    if not record:
        raise HTTPException(status_code=404, detail="Tool not found or not created by an agent")
    
    return ToolValidationResponse(message=f"Tool {request.action}ed successfully")

@app.get("/tools/pending", response_model=PendingToolsResponse)
async def get_pending_tools(session: Neo4jSession = Depends(get_session)):
    """Get all tools pending validation."""
    logger.info("Fetching pending tools")
    
    result = await session.run(
        """
        MATCH (t:Tool {validation_status: 'pending'})
        MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        """
    )
    records = await result.fetch()
    
    tools = []
    for record in records:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        
        tool_dict = {
            "id": tool["id"],
            "name": tool["name"],
            "description": tool["description"],
            "input_schema": json.loads(schema["input_schema"]),
            "output_schema": json.loads(schema["output_schema"]),
            "action": {
                "type": action["type"],
                "javascript": json.loads(action["javascript"]) if action["javascript"] else None,
                "http": json.loads(action["http"]) if action["http"] else None,
                "python": json.loads(action["python"]) if action["python"] else None
            },
            "output": {
                "type": output["type"],
                "content": output["content"]
            },
            "version": tool.get("version", "1.0.0")
        }
        tools.append(tool_dict)
    
    return PendingToolsResponse(tools=tools)

@app.get("/tools/agent/{agent_id}", response_model=AgentToolsResponse)
async def get_agent_tools(agent_id: str, session: Neo4jSession = Depends(get_session)):
    """Get all tools created by a specific agent."""
    logger.info(f"Fetching tools for agent: {agent_id}")
    
    result = await session.run(
        """
        MATCH (t:Tool {agent_id: $agent_id})
        MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
        MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        RETURN t, s, a, o
        """,
        agent_id=agent_id
    )
    
    tools = []
    for record in result:
        tool = record["t"]
        schema = record["s"]
        action = record["a"]
        output = record["o"]
        
        tool_response = ToolResponse(
            id=tool["id"],
            name=tool["name"],
            description=tool["description"],
            input_schema=json.loads(schema["input_schema"]),
            output_schema=json.loads(schema["output_schema"]),
            action=ToolAction(
                type=action["type"],
                javascript=JavaScriptCode(**json.loads(action["javascript"])) if action["javascript"] else None,
                http=HTTPAction(**json.loads(action["http"])) if action["http"] else None,
                python=PythonCode(**json.loads(action["python"])) if action["python"] else None
            ),
            output=ToolOutput(
                type=output["type"],
                content=output["content"]
            ),
            version=tool.get("version", "1.0.0")
        )
        tools.append(tool_response)
    
    return tools

@app.post("/tools/format/{model}")
async def format_tools_for_model(request: ToolFormatRequest):
    """Format tools for the specified model."""
    logger.info(f"Formatting tools for {request.model}")
    formatted_tools = format_tools(request.tools, for_model=request.model)
    logger.debug(f"Formatted tools for {request.model}: {formatted_tools}")
    return JSONResponse(content=formatted_tools)

@app.post("/tools/{tool_id}/user_settings", response_model=UserSettingsUpdateResponse)
async def update_user_settings(
    tool_id: str,
    request: UserSettingsUpdateRequest,
    session: Neo4jSession = Depends(get_session)
):
    """Update user settings for a specific tool."""
    logger.info(f"Updating user settings for tool: {tool_id}")
    result = await session.run(
        """
        MATCH (t:Tool {id: $tool_id})
        SET t.user_settings = $settings
        RETURN t
        """,
        tool_id=tool_id,
        settings=json.dumps(request.settings)
    )
    records = await result.fetch()
    record = records[0] if records else None
    if record:
        logger.info(f"User settings updated for tool: {tool_id}")
        return UserSettingsUpdateResponse(message="User settings updated successfully")
    else:
        logger.warning(f"Tool not found for updating user settings: {tool_id}")
        raise HTTPException(status_code=404, detail="Tool not found")

if __name__ == "__main__":
    import uvicorn
    import socket
    
    def find_available_port(start_port: int, max_attempts: int = 10) -> int:
        for port in range(start_port, start_port + max_attempts):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('0.0.0.0', port))
                    return port
            except OSError:
                continue
        raise RuntimeError(f"Could not find an available port after {max_attempts} attempts")

    try:
        port = find_available_port(2016)
        print(f"Starting server on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    except Exception as e:
        print(f"Failed to start server: {e}")
