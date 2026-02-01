"""
Pydantic schemas for Tool Management API.
"""
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime


# =============================================================================
# Action Models
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


# =============================================================================
# User Settings
# =============================================================================

class UserSetting(BaseModel):
    name: str
    label: str
    type: str = "text"
    description: Optional[str] = None
    required: bool = False
    placeholder: Optional[str] = None
    values: Optional[List[str]] = None


class UserSettingsUpdateRequest(BaseModel):
    settings: Dict[str, Any]


class UserSettingsUpdateResponse(BaseModel):
    tool_id: str
    settings: Dict[str, Any]
    updated_at: datetime


# =============================================================================
# Tool Models
# =============================================================================

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
    created_by: str = "user"
    agent_id: Optional[str] = None


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
    status: str = "online"
    created_by: str = "user"
    agent_id: Optional[str] = None
    validation_status: str = "approved"


class ToolUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    action: Optional[ToolAction] = None
    output: Optional[ToolOutput] = None
    user_settings: Optional[List[UserSetting]] = None


# =============================================================================
# Execution Models
# =============================================================================

class ToolExecutionRequest(BaseModel):
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None
    input_data: Dict[str, Any]
    user_settings: Dict[str, Any] = Field(default_factory=dict)
    format_type: Optional[Literal["anthropic", "openai"]] = None


class ToolExecutionResponse(BaseModel):
    tool_id: str
    tool_name: Optional[str] = None
    output_data: Any
    execution_time_ms: Optional[int] = None


class SequentialToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    initial_input: Dict[str, Any]
    format_type: Optional[Literal["anthropic", "openai"]] = None


class ParallelToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    input_data: Dict[str, Dict[str, Any]]
    format_type: Optional[Literal["anthropic", "openai"]] = None


# =============================================================================
# Validation Models
# =============================================================================

class ToolValidationRequest(BaseModel):
    tool_id: str
    action: Literal["approve", "reject"]
    reason: Optional[str] = None


class ToolValidationResponse(BaseModel):
    tool_id: str
    validation_status: str
    validated_by: Optional[str] = None
    validated_at: datetime


class PendingToolsResponse(BaseModel):
    tools: List[ToolResponse]
    count: int


class AgentToolsResponse(BaseModel):
    agent_id: str
    tools: List[ToolResponse]
    count: int


# =============================================================================
# Search Models
# =============================================================================

class ToolSearchRequest(BaseModel):
    prompt: str
    limit: int = 5
    for_model: Literal["anthropic", "openai"] = "anthropic"
    node_label: str = "Tool"


class ToolFormatRequest(BaseModel):
    tool_ids: Optional[List[str]] = None
    for_model: Literal["anthropic", "openai"] = "anthropic"


# =============================================================================
# Format Type Enum
# =============================================================================

class FormatType:
    ANTHROPIC = "anthropic"
    OPENAI = "openai"
