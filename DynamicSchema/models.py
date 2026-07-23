from typing import List, Dict, Optional, Union, Any, Literal
from pydantic import BaseModel, EmailStr, Field, HttpUrl
from datetime import datetime

class UserBase(BaseModel):
    email: EmailStr
    username: str
    organization: Optional[str] = None

class UserCreate(UserBase):
    password: str

class User(UserBase):
    id: int
    is_active: bool = True
    is_admin: bool = False
    created_at: datetime
    api_key: str

class TokenData(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int

class SchemaRegistration(BaseModel):
    name: str
    version: str
    schema: Dict[str, str]
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_by: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_public: bool = True

class SchemaVersion(BaseModel):
    version: str
    schema: Dict[str, str]
    description: Optional[str] = None
    created_at: datetime

class RegisteredSchema(BaseModel):
    id: int
    name: str
    current_version: str
    versions: List[SchemaVersion]
    created_by: User
    is_public: bool
    tags: List[str]
    usage_count: int = 0
    graph_id: Optional[str] = None

class GraphModelVersion(BaseModel):
    version: str
    schema: Dict[str, Any]
    created_at: datetime
    is_public: bool
    creator_id: int
    tags: List[str] = Field(default_factory=list)

class GraphModel(BaseModel):
    name: str
    versions: List[GraphModelVersion]
    latest_version: GraphModelVersion

class ModelSearchParams(BaseModel):
    query: Optional[str] = None
    tags: Optional[List[str]] = None
    creator_id: Optional[int] = None
