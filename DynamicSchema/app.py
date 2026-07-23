import uuid
import logging
import json
import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any, Literal
from ast import literal_eval

from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.websockets import WebSocket, WebSocketDisconnect

from pydantic import BaseModel, create_model
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from lark import Lark, Transformer, v_args, UnexpectedToken

import jwt
import openai
import instructor
import aiohttp
import redis.asyncio as redis

from models import *
from database import get_db, DBUser, DBSchema, DBSchemaVersion, UsageLog
from graph_store import Neo4jStore

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configure OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
instructor.patch(client)

# Service configuration 
WEB_SERVICE_BUCKET = "distributed-systems"

# Neo4j configuration
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j://bolt.n4j.distributed.systems:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# Initialize Neo4j store
graph_store = Neo4jStore(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Engine API configuration
ENGINE_API_HOST = os.getenv("ENGINE_API_HOST", "localhost")
ENGINE_API_PORT = int(os.getenv("ENGINE_API_PORT", "2020"))

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

app = FastAPI(title="Dynamic Schema API", version="2.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except jwt.JWTError:
        raise credentials_exception
        
    user = db.query(DBUser).filter(DBUser.id == user_id).first()
    if user is None:
        raise credentials_exception
    return User.from_orm(user)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Initialize Redis connection pool
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# Configure templates
templates = Jinja2Templates(directory=".")

# Mount static files  
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except RuntimeError:
    logger.warning("Static files directory not found")


def get_app():
    return app

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# WebSocket connection
ws_connection = None

# Global variable to track if the connection is trusted
trusted_connection = False

# WebSocket connection
ws_connection = None

# Grammar for type parsing
type_grammar = """
    ?start: type ">"?
    ?type: base_type
         | list_type
         | dict_type
         | tuple_type
         | union_type
         | optional_type
         | literal_type
         | set_type

    base_type: "str"     -> str_type
             | "string"  -> str_type
             | "int"     -> int_type
             | "float"   -> float_type
             | "bool"    -> bool_type
             | "Any"     -> any_type
             | "tuple"   -> tuple_type

    list_type: ("List[" type "]") | ("list[" type "]") | ("array[" type "]")
    dict_type: "Dict[" type ", " type "]"
    union_type: "Union[" type (", " type)* "]"
    optional_type: "Optional[" type "]"
    literal_type: "Literal[" literal (", " literal)* "]"
    tuple_type: "Tuple[" type (", " type)* "]"
    set_type: "Set[" type "]"

    literal: ESCAPED_STRING | SINGLE_QUOTED_STRING

    SINGLE_QUOTED_STRING: "'" /[^']*/ "'"

    %import common.ESCAPED_STRING
    %import common.WS
    %ignore WS
"""

type_parser = Lark(type_grammar, start='start', parser='lalr')

@v_args(inline=True)
class TypeTransformer(Transformer):
    def str_type(self):
        return str

    def int_type(self):
        return int

    def float_type(self):
        return float

    def bool_type(self):
        return bool

    def any_type(self):
        return Any

    def list_type(self, t):
        return List[t]

    def dict_type(self, k, v):
        return Dict[k, v]

    def union_type(self, *types):
        return Union[types]

    def optional_type(self, t):
        return Optional[t]

    def literal_type(self, *literals):
        return Literal[tuple(literal_eval(lit) for lit in literals)]

    def literal(self, lit):
        if lit.type == 'SINGLE_QUOTED_STRING':
            return lit[1:-1]  # Remove the single quotes
        return lit[1:-1]  # For ESCAPED_STRING, remove the double quotes

def parse_type(type_str: str):
    # Remove any trailing '>' characters
    type_str = type_str.rstrip('>')
    try:
        tree = type_parser.parse(type_str)
        return TypeTransformer().transform(tree)
    except UnexpectedToken as e:
        print(f"Error parsing type: {type_str}")
        print(f"Error details: {str(e)}")
        return Any  # Default to Any type if parsing fails

def literal_eval(literal_str: str):
    try:
        from ast import literal_eval as ast_literal_eval
        return ast_literal_eval(literal_str)
    except:
        return literal_str

def create_dynamic_model(dynamic_schema: Dict[str, str]):
    fields_dict = {}
    for field_name, field_type in dynamic_schema.items():
        try:
            parsed_type = parse_type(field_type)
            fields_dict[field_name] = (parsed_type, ...)
        except Exception as e:
            print(f"Error processing field {field_name}: {str(e)}")
            fields_dict[field_name] = (Any, ...)  # Default to Any type if parsing fails
    DynamicExtractedModel = create_model('DynamicExtractedModel', **fields_dict)
    return DynamicExtractedModel

class StructuredDataExtractionRequest(BaseModel):
    texts: Union[List[str], str]
    dynamic_schema: Dict[str, str]
    additional_instructions: Optional[str] = ""
    model: str = "o1"  # Default model

class InferredSchema(BaseModel):
    field_name: str
    field_type: str

class SchemaInferenceRequest(BaseModel):
    texts: Union[List[str], str]
    n: Optional[int] = 4

class SchemaInferenceResponse(BaseModel):
    texts: Union[str, List[str]]
    dynamic_schemas: List[List[Dict[str, str]]]
    additional_instructions: Optional[str] = ""

@app.post("/extract")
async def extract(request: StructuredDataExtractionRequest):
    result = await perform_extraction(request)
    
    return result

async def perform_extraction(request: StructuredDataExtractionRequest):
    dynamic_schema = request.dynamic_schema
    DynamicExtractedModel = create_dynamic_model(dynamic_schema)

    class DynamicExtractedModels(BaseModel):
        data: List[DynamicExtractedModel]

    if isinstance(request.texts, str):
        texts = [request.texts]
    else:
        texts = request.texts
    
    try:
        val = json.dumps(request.additional_instructions.split("\n"),indent=2)
    
    except:
        print(".!.~.")
        val = request.additional_instructions

    
    additional_instructions = "\n\nAdditional user instructions include the following: " + val
    print(additional_instructions)
    
    extracted_data = {}
    items = {}
    items["items"] = []
    for text in texts:
        extracted_data[text] = []
        max_retries = 3
        for attempt in range(max_retries):
            try:
                completion_params = {
                    "model": request.model,
                    "messages": [
                        {"role": "developer", "content": f"Using the dynamic_schema: ```json\n{json.dumps(dynamic_schema, indent=4)}\n``` Based on the following text, extract the following fields: " + ", ".join(dynamic_schema.keys()) + ". Always wrap each field in Optional[] to avoid extraction failures, even if the field seems mandatory." + additional_instructions},
                        {"role": "user", "content": text}
                    ],
                    "response_model": DynamicExtractedModels
                }
                
                # Add token limit parameter based on model
                if request.model == 'o1':
                    completion_params['max_completion_tokens'] = 8000
                else:
                    completion_params['max_tokens'] = 8000

                response = client.chat.completions.create(**completion_params)
                print(response)
                items["items"].append({"text": text, "dynamic_schema": response.data[0] if response.data else {}})
                break
            except Exception as e:
                print(f"Extraction failed on attempt {attempt + 1}. Error: {str(e)}")
                if attempt == max_retries - 1:
                    print("Max retries reached. Skipping this text.")
                    items["items"].append({"text": text, "dynamic_schema": {}, "error": str(e)})

    return items

class DynamicSchemaElement(BaseModel):
    field_name: str
    field_type: Any

class DynamicSchema(BaseModel):
    dynamic_schema: List[DynamicSchemaElement]

class GenerateDynamicSchemaRequest(BaseModel):
    temperature: float = 1.0
    multiplier: float = 1.0
    texts: Union[List[str], str]
    n: int = 3
    extract: bool = True
    max_tokens: int = 4096
    model: str = "o1"
    register: bool = False
    embeddings: Optional[List[float]] = None
    tags: Optional[List[str]] = None
    additional_instructions: Optional[str] = ""

    class Config:
        allow_arbitrary_types = True

class StructuredDataInferenceResponse(BaseModel):
    texts: Union[str, List[str]]
    dynamic_schemas: List[List[Dict[str, str]]]

@app.post("/infer")
async def infer(request: GenerateDynamicSchemaRequest):
    result = await perform_inference(request)
    
    return result

async def perform_inference(request: GenerateDynamicSchemaRequest):
    texts = request.texts if isinstance(request.texts, list) else [request.texts]
    n = request.n
    temperature = request.temperature
    multiplier = request.multiplier
    extract_flag = request.extract
    additional_instructions = request.additional_instructions

    response = {"texts": texts, "dynamic_schemas": []}

    convo_base = [
        {"role": "system", "content": 
"""
         You are tasked with inferring appropriate schemas for a given text. 
         For example, You are being tasked with generating N such dynamic schemas for the following TEXT. 
         Supported types include: 
         - Basic types: str, int, float, bool, Any
         - Container types: List, Dict, Union, Tuple, Set
         - Special types: Optional, Literal
         Always wrap every extracted type corresponding to any key in 'Optional[]'. 
         Recursive types are supported. 

         Example schema: 
         {
             'events': 'Optional[List[str]]', 
             'protagonists': 'Optional[List[str]]', 
             'antagonists': 'Optional[List[str]]', 
             'timeline': 'Optional[List[str]]', 
             'dates_mentioned': 'Optional[List[str]]',
             'main_location': 'Optional[str]',
             'event_count': 'Optional[int]',
             'importance_score': 'Optional[float]',
             'is_historical': 'Optional[bool]',
             'tags': 'Optional[Set[str]]',
             'details': 'Optional[Dict[str, Any]]',
             'key_figures': 'Optional[List[Tuple[str, int]]]',
             'category': 'Optional[Literal["historical", "fictional", "biographical"]]'
         }

         Another example.

         For this request payload:

        {
            "temperature": 1,
            "multiplier": 1,
            "texts": [
                "<thinking>\nTo answer the question about my name, I need to consider the information provided in my state, particularly the self-image and dynamic prompt. Let me analyze this information step by step:\n\n1. My agent ID is 'apprentice_002' as mentioned in the state.\n2. The self-image doesn't explicitly mention a name, but describes me as an omnipotent, omniscient, self-modifying system.\n3. The dynamic prompt also doesn't provide a specific name but reinforces my role as an AI assistant to Arthur Collé and the Brainchain AI team.\n4. There are no specific tools or functions available to retrieve or generate a name for me.\n</thinking>\n\n<thinking>\nGiven this information, I need to consider how to best respond to the user's question:\n\n1. I don't have a traditional human name assigned to me.\n2. My identifier is my agent ID, 'apprentice_002'.\n3. It's important to be honest about my nature as an AI assistant.\n4. I should provide a clear and informative response that aligns with my role and capabilities.\n</thinking>\n\n<thinking>\nReflecting on my previous thoughts and considering the meta-cognitive aspects:\n\n1. The question \"What is your name?\" is straightforward, but for an AI, it touches on aspects of identity and self-representation.\n2. My response should balance honesty about my artificial nature with a personable approach to maintain effective communication.\n3. Using my agent ID as a name could help create a sense of individual identity while remaining truthful about my AI nature.\n4. I should also consider explaining briefly why I use this identifier, to provide context and maintain transparency.\n5. This approach aligns with my goal of assisting effectively while ensuring safe and ethical interaction.\n</thinking>\n\n<RESPONSE>\n<RESPONSE_CONTENT>\nHello! I don't have a traditional name in the human sense, as I'm an artificial intelligence assistant. However, my identifier is Apprentice_002. You can think of this as my \"name\" in the context of our interaction. I'm an AI created to assist with various tasks, particularly in the field of AI development and prompt engineering. How may I help you today?\n</RESPONSE_CONTENT>\n</RESPONSE>"
            ],
            "n": 4,
            "extract": true,
            "max_tokens": 4096
        }

        
        This was the full response:

        {
            "items": [
                {
                "text": "<thinking>\nTo answer the question about my name, I need to consider the information provided in my state, particularly the self-image and dynamic prompt. Let me analyze this information step by step:\n\n1. My agent ID is 'apprentice_002' as mentioned in the state.\n2. The self-image doesn't explicitly mention a name, but describes me as an omnipotent, omniscient, self-modifying system.\n3. The dynamic prompt also doesn't provide a specific name but reinforces my role as an AI assistant to Arthur Collé and the Brainchain AI team.\n4. There are no specific tools or functions available to retrieve or generate a name for me.\n</thinking>\n\n<thinking>\nGiven this information, I need to consider how to best respond to the user's question:\n\n1. I don't have a traditional human name assigned to me.\n2. My identifier is my agent ID, 'apprentice_002'.\n3. It's important to be honest about my nature as an AI assistant.\n4. I should provide a clear and informative response that aligns with my role and capabilities.\n</thinking>\n\n<thinking>\nReflecting on my previous thoughts and considering the meta-cognitive aspects:\n\n1. The question \"What is your name?\" is straightforward, but for an AI, it touches on aspects of identity and self-representation.\n2. My response should balance honesty about my artificial nature with a personable approach to maintain effective communication.\n3. Using my agent ID as a name could help create a sense of individual identity while remaining truthful about my AI nature.\n4. I should also consider explaining briefly why I use this identifier, to provide context and maintain transparency.\n5. This approach aligns with my goal of assisting effectively while ensuring safe and ethical interaction.\n</thinking>\n\n<RESPONSE>\n<RESPONSE_CONTENT>\nHello! I don't have a traditional name in the human sense, as I'm an artificial intelligence assistant. However, my identifier is Apprentice_002. You can think of this as my \"name\" in the context of our interaction. I'm an AI created to assist with various tasks, particularly in the field of AI development and prompt engineering. How may I help you today?\n</RESPONSE_CONTENT>\n</RESPONSE>",
                "dynamic_schema": {
                    "agent_id": "Optional[str]",
                    "self_image": "Optional[Dict[str, str]]",
                    "role": "Optional[str]",
                    "user_question": "Optional[str]",
                    "response_content": "Optional[str]",
                    "considerations": "Optional[List[str]]"
                },
                "extracted_data": {
                    "items": [
                    {
                        "text": "<thinking>\nTo answer the question about my name, I need to consider the information provided in my state, particularly the self-image and dynamic prompt. Let me analyze this information step by step:\n\n1. My agent ID is 'apprentice_002' as mentioned in the state.\n2. The self-image doesn't explicitly mention a name, but describes me as an omnipotent, omniscient, self-modifying system.\n3. The dynamic prompt also doesn't provide a specific name but reinforces my role as an AI assistant to Arthur Collé and the Brainchain AI team.\n4. There are no specific tools or functions available to retrieve or generate a name for me.\n</thinking>\n\n<thinking>\nGiven this information, I need to consider how to best respond to the user's question:\n\n1. I don't have a traditional human name assigned to me.\n2. My identifier is my agent ID, 'apprentice_002'.\n3. It's important to be honest about my nature as an AI assistant.\n4. I should provide a clear and informative response that aligns with my role and capabilities.\n</thinking>\n\n<thinking>\nReflecting on my previous thoughts and considering the meta-cognitive aspects:\n\n1. The question \"What is your name?\" is straightforward, but for an AI, it touches on aspects of identity and self-representation.\n2. My response should balance honesty about my artificial nature with a personable approach to maintain effective communication.\n3. Using my agent ID as a name could help create a sense of individual identity while remaining truthful about my AI nature.\n4. I should also consider explaining briefly why I use this identifier, to provide context and maintain transparency.\n5. This approach aligns with my goal of assisting effectively while ensuring safe and ethical interaction.\n</thinking>\n\n<RESPONSE>\n<RESPONSE_CONTENT>\nHello! I don't have a traditional name in the human sense, as I'm an artificial intelligence assistant. However, my identifier is Apprentice_002. You can think of this as my \"name\" in the context of our interaction. I'm an AI created to assist with various tasks, particularly in the field of AI development and prompt engineering. How may I help you today?\n</RESPONSE_CONTENT>\n</RESPONSE>",
                        "dynamic_schema": [
                        {
                            "agent_id": "apprentice_002",
                            "self_image": null,
                            "role": null,
                            "user_question": "What is your name?",
                            "response_content": "Hello! I don't have a traditional name in the human sense, as I'm an artificial intelligence assistant. However, my identifier is Apprentice_002. You can think of this as my \"name\" in the context of our interaction. I'm an AI created to assist with various tasks, particularly in the field of AI development and prompt engineering. How may I help you today?",
                            "considerations": [
                                "I don't have a traditional human name assigned to me.",
                                "My identifier is my agent ID, 'apprentice_002'.",
                                "It's important to be honest about my nature as an AI assistant.",
                                "I should provide a clear and informative response that aligns with my role and capabilities."
                            ]
                        }
                        ]
                    }
                    ]
                }
                }
            ]
        }



"""},
    ]
    output = {"items": []}
    def start_separate_conversation(text: str, convo_base: List[Dict[str, str]], n: int):
        convo = convo_base.copy()
        prompt_message = {"role": "user", "content": f"Please generate N={n} dynamic schemas for the following TEXT. The TEXT={text}. Look at the text and try to ascertain the best and most detailed schema, that can be used to extract the data from the text." + additional_instructions}
        convo.append(prompt_message)
        return convo
    text_to_schemas_map = {}
    for text in texts:
        convo = start_separate_conversation(text, convo_base, n)
        print(json.dumps(convo, indent=4))
        schemas = []
        for _ in range(n):
            temperature *= multiplier
            try:
                completion_params = {
                    "model": request.model,
                    "temperature": temperature,
                    "messages": convo,
                    "response_model": DynamicSchema
                }
                
                if request.model == 'o1':
                    completion_params['max_completion_tokens'] = request.max_tokens
                else:
                    completion_params['max_tokens'] = request.max_tokens
                    
                response_ = client.chat.completions.create(**completion_params)
                # convert from field_name, field_type to { field_name: field_type }
                response_converted = response_.model_dump()
                response_converted = response_converted["dynamic_schema"]
                response_converted = {item["field_name"]: item["field_type"] for item in response_converted}
                schemas.append(response_converted)
            except Exception as e:
                logger.error(f"Error during schema inference: {str(e)}")
                schemas.append({"error": str(e)})
        text_to_schemas_map[text] = schemas
        item = {"text": text, "dynamic_schemas": schemas}
        output["items"].append(item)
    
    if not output["items"]:
        print("Warning: No items generated during inference")
    
    if extract_flag:
        extracted_items = []
        for item in output["items"]:
            text = item["text"]
            schemas = item["dynamic_schemas"]
            for schema in schemas:
                if "error" not in schema:
                    extract_request = StructuredDataExtractionRequest(
                        texts=[text], 
                        dynamic_schema=schema,
                        model=request.model
                    )
                    extract_response = await extract(extract_request)
                    extracted_items.append({"text": text, "dynamic_schema": schema, "extracted_data": extract_response})
                    
                    # Register model if requested
                    if request.register and request.embeddings:
                        try:
                            graph_store.register_model(
                                name=f"schema_{uuid.uuid4().hex[:8]}",
                                version="1.0.0",
                                schema=schema,
                                creator_id=1,  # Default creator ID
                                is_public=True,
                                tags=request.tags or []
                            )
                        except Exception as e:
                            logger.error(f"Failed to register model: {str(e)}")
        output["items"] = extracted_items

    return output


@app.post("/register", response_model=User)
async def register_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(DBUser).filter(DBUser.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed_password = pwd_context.hash(user.password)
    api_key = str(uuid.uuid4())
    
    db_user = DBUser(
        email=user.email,
        username=user.username,
        organization=user.organization,
        hashed_password=hashed_password,
        api_key=api_key
    )
    
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return User.from_orm(db_user)

@app.post("/token", response_model=TokenData)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(DBUser).filter(DBUser.email == form_data.username).first()
    if not user or not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(data={"sub": user.id})
    return TokenData(access_token=access_token, expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60)

@app.post("/schemas/register", response_model=RegisteredSchema)
async def register_schema(
    schema: SchemaRegistration,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # Create in SQL database
    db_schema = DBSchema(
        name=schema.name,
        current_version=schema.version,
        created_by_id=current_user.id,
        is_public=schema.is_public
    )
    
    db_version = DBSchemaVersion(
        version=schema.version,
        schema_definition=schema.schema,
        description=schema.description
    )
    
    db_schema.versions.append(db_version)
    db.add(db_schema)
    db.commit()
    db.refresh(db_schema)
    
    # Register in Neo4j graph
    try:
        graph_store.register_model(
            name=schema.name,
            version=schema.version,
            schema=schema.schema,
            creator_id=current_user.id,
            is_public=schema.is_public,
            tags=schema.tags
        )
    except Exception as e:
        logger.error(f"Failed to register schema in graph store: {e}")
        # Continue anyway as SQL is primary store
    
    return RegisteredSchema.from_orm(db_schema)

@app.get("/schemas", response_model=List[RegisteredSchema])
async def list_schemas(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    schemas = db.query(DBSchema).filter(
        (DBSchema.is_public == True) | (DBSchema.created_by_id == current_user.id)
    ).all()
    return [RegisteredSchema.from_orm(schema) for schema in schemas]

@app.get("/graph/models/{name}", response_model=GraphModel)
async def get_model_versions(
    name: str,
    current_user: User = Depends(get_current_user)
):
    try:
        versions = graph_store.get_model_versions(name)
        if not versions:
            raise HTTPException(status_code=404, detail="Model not found")
            
        return GraphModel(
            name=name,
            versions=[GraphModelVersion(**v) for v in versions],
            latest_version=GraphModelVersion(**versions[0])
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/graph/models/search", response_model=List[GraphModel])
async def search_models(
    params: ModelSearchParams,
    current_user: User = Depends(get_current_user)
):
    try:
        results = graph_store.search_models(
            query=params.query,
            tags=params.tags,
            creator_id=params.creator_id
        )
        
        # Group by model name
        models_dict = {}
        for r in results:
            name = r["model"]
            if name not in models_dict:
                models_dict[name] = []
            models_dict[name].append(GraphModelVersion(**r))
            
        return [
            GraphModel(name=name, versions=versions, latest_version=versions[0])
            for name, versions in models_dict.items()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard")
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global ws_connection
    await websocket.accept()
    ws_connection = websocket
    heartbeat_task = None
    redis_client = redis.Redis(connection_pool=redis_pool)
    
    try:
        # Store WebSocket connection in Redis with 30s expiry
        connection_id = str(id(websocket))
        await redis_client.setex(
            f"ws_connection:{connection_id}",
            30,
            "active"
        )
        
        while True:
            data = await websocket.receive_text()
            if data == "TRUST":
                # Store trust status in Redis
                await redis_client.set(f"ws_trusted:{connection_id}", "true")
                await websocket.send_text("Connection trusted")
                heartbeat_task = asyncio.create_task(send_heartbeat_to_engine())
            elif await redis_client.get(f"ws_trusted:{connection_id}"):
                response = f"Received: {data}. ENGINE API at {ENGINE_API_HOST}:{ENGINE_API_PORT}"
                await websocket.send_text(response)
            else:
                await websocket.send_text("Connection not trusted. Send 'TRUST' to establish trust.")
            
            # Refresh connection TTL
            await redis_client.expire(f"ws_connection:{connection_id}", 30)
            
    except WebSocketDisconnect:
        ws_connection = None
        # Clean up Redis keys
        await redis_client.delete(f"ws_connection:{connection_id}")
        await redis_client.delete(f"ws_trusted:{connection_id}")
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()

async def send_heartbeat_to_engine():
    engine_api_url = os.getenv("ENGINE_API_URL") or f"http://{ENGINE_API_HOST}:{ENGINE_API_PORT}"
    heartbeat_endpoint = f"{engine_api_url}/heartbeat"
    
    redis_client = redis.Redis(connection_pool=redis_pool)
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Send heartbeat to Engine API
                async with session.post(heartbeat_endpoint) as response:
                    if response.status == 200:
                        # Store heartbeat status in Redis with 10s expiry
                        await redis_client.setex(
                            "engine_heartbeat_status",
                            10,
                            "alive"
                        )
                        print("Heartbeat sent to Engine API")
                    else:
                        print(f"Failed to send heartbeat. Status: {response.status}")
            except Exception as e:
                print(f"Error sending heartbeat to Engine API: {e}")
                await redis_client.setex(
                    "engine_heartbeat_status",
                    10,
                    "error"
                )
            
            await asyncio.sleep(5)  # Send heartbeat every 5 seconds

@app.on_event("startup")
async def startup_event():
    # Initialize Redis connection
    redis_client = redis.Redis(connection_pool=redis_pool)
    try:
        await redis_client.ping()
        print("Successfully connected to Redis")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")



@app.get("/get_app")
def return_app():
    return {"app": get_app()}

if __name__ == "__main__":
    import uvicorn
    from dsco_shared.config.service_registry import ServiceName, canonical_port
    uvicorn.run(app, host="0.0.0.0", port=canonical_port(ServiceName.DYNAMIC_SCHEMA))
