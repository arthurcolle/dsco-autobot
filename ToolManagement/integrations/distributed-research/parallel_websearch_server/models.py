"""Comprehensive data models for Parallel.ai API based on OpenAPI spec."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Literal

from pydantic import BaseModel, Field, HttpUrl, ConfigDict, model_validator, field_validator


# ============================================================================
# Common Models
# ============================================================================

class Error(BaseModel):
    """Error model."""
    detail: Optional[str] = None
    message: str
    ref_id: Optional[str] = None


class ErrorResponse(BaseModel):
    """Error response wrapper."""
    error: Error
    type: str = "error"


class ErrorEvent(BaseModel):
    """Error event for streaming."""
    error: Error
    type: str = "error"


class Warning(BaseModel):
    """Warning model."""
    message: str
    detail: Optional[str] = None


class UsageItem(BaseModel):
    """Usage tracking allowing both legacy {key,value} and {name,count} forms."""

    model_config = ConfigDict(extra="allow")

    key: Optional[str] = None
    value: Optional[Union[int, float, str]] = None
    name: Optional[str] = None
    count: Optional[Union[int, float, str]] = None

    @model_validator(mode="after")
    def _normalize_usage_fields(self) -> "UsageItem":
        """Mirror key/value fields across aliases so any upstream shape validates."""
        if self.key is None and self.name is not None:
            self.key = self.name
        if self.name is None and self.key is not None:
            self.name = self.key

        if self.value is None and self.count is not None:
            self.value = self.count
        if self.count is None and self.value is not None:
            self.count = self.value

        if self.key is None:
            raise ValueError("usage item requires either 'key' or 'name'")
        if self.value is None:
            raise ValueError("usage item requires either 'value' or 'count'")
        return self


# ============================================================================
# Search & Extract Models (Beta)
# ============================================================================

class ExcerptSettings(BaseModel):
    """Settings for extracting relevant excerpts/snippets from content.

    Example: {"max_chars_per_result": 500}
    """
    max_chars_per_result: Optional[int] = Field(
        None, ge=1,
        description="Maximum characters per excerpt. Default varies by endpoint."
    )


class FullContentSettings(BaseModel):
    """Settings for extracting full page content.

    Example: {"max_chars_per_result": 10000}
    """
    max_chars_per_result: Optional[int] = Field(
        None, ge=1,
        description="Maximum characters of full content to return per URL."
    )


class FetchPolicy(BaseModel):
    """Controls how URLs are fetched - caching, timeouts, freshness.

    Example: {"max_age_seconds": 3600, "timeout_seconds": 10}
    """
    disable_cache_fallback: Optional[bool] = Field(
        None,
        description="If true, don't use cached content even if fresh fetch fails."
    )
    max_age_seconds: Optional[int] = Field(
        None, ge=0,
        description="Maximum age of cached content in seconds. 0 = always fetch fresh."
    )
    timeout_seconds: Optional[int] = Field(
        None, ge=1, le=30,
        description="HTTP timeout for fetching URLs (1-30 seconds)."
    )


class SearchRequest(BaseModel):
    """Web search request. Returns relevant URLs with excerpts.

    SIMPLE USAGE (recommended):
        {"objective": "Latest AI news December 2024"}

    WITH OPTIONS:
        {
            "objective": "quantum computing breakthroughs",
            "max_results": 20,
            "search_queries": ["quantum computing 2024", "quantum supremacy news"]
        }

    The 'objective' is the main search goal. Optionally provide specific
    'search_queries' to control exact searches performed.
    """
    objective: str = Field(
        ...,
        description="What you want to find. Natural language description of search goal."
    )
    search_queries: Optional[List[str]] = Field(
        None,
        description="Optional specific search queries. If omitted, queries are auto-generated from objective."
    )
    max_results: Optional[Union[int, str]] = Field(
        None,
        ge=1,
        le=100,
        description="Number of results to return (1-100). Default: 10."
    )
    excerpts: Optional[ExcerptSettings] = Field(
        None,
        description="Configure excerpt extraction. Default: enabled with sensible limits."
    )
    source_policy: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional domain filtering: {'allowed_domains': [...]} or {'blocked_domains': [...]}"
    )

    @field_validator("max_results", mode="before")
    @classmethod
    def _normalize_max_results(cls, value: Optional[Union[int, str]]) -> Optional[int]:
        """Allow callers to pass max_results as either an int or stringified int."""
        if value is None or isinstance(value, int):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("max_results string cannot be empty")
            try:
                return int(stripped, 10)
            except ValueError as exc:
                raise ValueError("max_results must be an integer or numeric string") from exc
        raise ValueError("max_results must be either an integer or numeric string")


class WebSearchResult(BaseModel):
    """Single search result."""
    title: str
    url: str
    excerpts: List[str] = Field(default_factory=list)
    publish_date: Optional[str] = None


class SearchResponse(BaseModel):
    """Search response."""
    results: List[WebSearchResult]
    usage: Optional[List[UsageItem]] = None
    warnings: Optional[List[Warning]] = None


class ExtractRequest(BaseModel):
    """Extract content from specific URLs. Use after search to get detailed content.

    SIMPLE USAGE - get excerpts from URLs:
        {
            "urls": ["https://example.com/article1", "https://example.com/article2"],
            "objective": "Extract key findings about climate change"
        }

    GET FULL CONTENT:
        {
            "urls": ["https://example.com/article"],
            "full_content": true
        }

    WITH CUSTOM SETTINGS:
        {
            "urls": ["https://example.com/article"],
            "objective": "Find pricing information",
            "excerpts": {"max_chars_per_result": 1000},
            "fetch_policy": {"timeout_seconds": 15}
        }
    """
    urls: List[str] = Field(
        ..., min_length=1,
        description="List of URLs to extract content from. At least 1 required."
    )
    objective: Optional[str] = Field(
        None,
        description="What to extract. Helps focus on relevant content. E.g., 'pricing details' or 'technical specifications'."
    )
    excerpts: Optional[Union[bool, ExcerptSettings]] = Field(
        None,
        description="Extract relevant snippets. true = defaults, or pass {max_chars_per_result: N}."
    )
    full_content: Optional[Union[bool, FullContentSettings]] = Field(
        None,
        description="Extract full page content. true = defaults, or pass {max_chars_per_result: N}."
    )
    fetch_policy: Optional[FetchPolicy] = Field(
        None,
        description="Control caching and timeouts. E.g., {max_age_seconds: 0} for fresh content."
    )
    search_queries: Optional[List[str]] = Field(
        None,
        description="Optional search terms to highlight in excerpts."
    )

    @field_validator("excerpts", mode="before")
    @classmethod
    def _normalize_excerpts(cls, value: Optional[Union[bool, Dict, ExcerptSettings]]) -> Optional[Union[bool, ExcerptSettings]]:
        """Allow boolean or object for excerpts setting."""
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, ExcerptSettings):
            return value
        if isinstance(value, dict):
            return ExcerptSettings(**value)
        return value

    @field_validator("full_content", mode="before")
    @classmethod
    def _normalize_full_content(cls, value: Optional[Union[bool, Dict, FullContentSettings]]) -> Optional[Union[bool, FullContentSettings]]:
        """Allow boolean or object for full_content setting."""
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, FullContentSettings):
            return value
        if isinstance(value, dict):
            return FullContentSettings(**value)
        return value


class ExtractResult(BaseModel):
    """Single extraction result."""
    url: str
    title: Optional[str] = None
    excerpts: Optional[List[str]] = None
    full_content: Optional[str] = None
    publish_date: Optional[str] = None


class ExtractError(BaseModel):
    """Extraction error for a specific URL."""
    url: str
    error_type: str
    content: Optional[str] = None
    http_status_code: Optional[int] = None


class ExtractResponse(BaseModel):
    """Extract response."""
    extract_id: str
    results: List[ExtractResult]
    errors: Optional[List[ExtractError]] = None
    usage: Optional[List[UsageItem]] = None
    warnings: Optional[List[Warning]] = None


# ============================================================================
# Context & State Management
# ============================================================================


class ContextCreateRequest(BaseModel):
    """Request payload to create a context."""

    context_id: Optional[str] = Field(
        default=None,
        description="Optional identifier supplied by the caller. Random if omitted.",
    )
    name: Optional[str] = Field(default=None, description="Human-friendly label.")
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Free-form tags.")
    state: Optional[Dict[str, Any]] = Field(
        default=None, description="Initial context state the agent wants to persist."
    )


class ContextUpdateRequest(BaseModel):
    """Update context metadata or state."""

    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    state: Optional[Dict[str, Any]] = None
    replace_metadata: bool = Field(
        default=False, description="When true, replace metadata instead of merging."
    )
    replace_state: bool = Field(
        default=False, description="When true, replace state dictionary instead of merging."
    )


class ContextRecord(BaseModel):
    """Stored context metadata and state."""

    context_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    state: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class ContextInteraction(BaseModel):
    """Single cached input/output exchange."""

    interaction_id: int
    context_id: Optional[str] = None
    interaction_type: str
    status: str
    summary: Optional[str] = None
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    output_payload: Optional[Dict[str, Any]] = None
    remote_run_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ContextDetailResponse(BaseModel):
    """Context plus most recent interactions."""

    context: ContextRecord
    history: List[ContextInteraction] = Field(default_factory=list)


# ============================================================================
# Task Models
# ============================================================================

class ProcessorType(str, Enum):
    """Processor tiers - determines speed, capability, and cost.

    RECOMMENDED FOR MOST TASKS:
    - lite: Fastest, cheapest. Good for simple queries. ONLY supports text output (no JSON schema).
    - base: Balanced speed/quality. Supports all output schemas.
    - core: Higher quality. Good for complex research.

    FOR DEMANDING TASKS:
    - pro: Premium quality for difficult tasks.
    - ultra: Maximum capability. Use for very complex multi-step research.

    COST ORDER (low to high): lite < base < core < pro < ultra
    CAPABILITY ORDER: lite < base < core < pro < ultra

    IMPORTANT: 'lite' processor does NOT support auto or JSON output schemas.
    Use {"type": "text"} for lite processor task_spec.
    """

    LITE = "lite"      # Fastest, cheapest. Text output only.
    BASE = "base"      # Balanced. All output types.
    CORE = "core"      # Higher quality research.
    CORE_2X = "core2x" # Extended core processing.
    PRO = "pro"        # Premium quality.
    ULTRA = "ultra"    # Maximum capability.
    ULTRA_2X = "ultra2x"
    ULTRA_4X = "ultra4x"
    ULTRA_8X = "ultra8x"


class TaskRunStatus(str, Enum):
    """Task execution status. Check this to know if task is done.

    ACTIVE STATES (task still running):
    - queued: Waiting to start
    - running: Currently executing
    - action_required: Needs user input (rare)
    - cancelling: Being cancelled

    TERMINAL STATES (task finished):
    - completed: Success! Check output.
    - failed: Error occurred. Check error field.
    - cancelled: Was cancelled by user.
    """
    QUEUED = "queued"
    ACTION_REQUIRED = "action_required"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class SourcePolicy(BaseModel):
    """Control which domains/sources can be used for research.

    ALLOW ONLY SPECIFIC DOMAINS:
        {"allowed_domains": ["reuters.com", "bbc.com", "nytimes.com"]}

    BLOCK CERTAIN DOMAINS:
        {"blocked_domains": ["reddit.com", "twitter.com"]}

    Note: Use allowed_domains OR blocked_domains, not both.
    """
    allowed_domains: Optional[List[str]] = Field(
        None,
        description="Only use these domains. E.g., ['reuters.com', 'bbc.com']"
    )
    blocked_domains: Optional[List[str]] = Field(
        None,
        description="Never use these domains. E.g., ['reddit.com']"
    )


class Webhook(BaseModel):
    """Webhook for receiving task completion notifications.

    Example:
        {
            "url": "https://your-server.com/webhook",
            "headers": {"Authorization": "Bearer your-token"},
            "event_types": ["completed", "failed"]
        }
    """
    url: HttpUrl = Field(..., description="Your webhook endpoint URL")
    headers: Optional[Dict[str, str]] = Field(
        None,
        description="Headers to include in webhook requests (e.g., auth tokens)"
    )
    event_types: Optional[List[str]] = Field(
        None,
        description="Which events to send: 'completed', 'failed', 'progress'"
    )


class JsonSchemaDefinition(BaseModel):
    """The actual JSON schema definition object."""
    type: str = Field("object", description="JSON Schema type, typically 'object'")
    properties: Optional[Dict[str, Any]] = Field(None, description="Property definitions")
    required: Optional[List[str]] = Field(None, description="List of required property names")
    description: Optional[str] = None


class JsonSchema(BaseModel):
    """JSON output schema wrapper. Use for structured JSON output.

    REQUIRED FORMAT for task_spec.output_schema:
        {
            "type": "json",
            "json_schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "key_points": {"type": "array", "items": {"type": "string"}}
                }
            }
        }

    IMPORTANT: The 'json_schema' field is REQUIRED when using JSON output.
    It must contain the actual JSON Schema definition for your expected output.
    """
    type: Literal["json"] = Field("json", description="Must be 'json' for JSON output")
    json_schema: Optional[JsonSchemaDefinition] = Field(
        None,
        description="REQUIRED for input: The JSON Schema definition for expected output structure. Contains 'type', 'properties', etc."
    )
    # Legacy fields for backwards compatibility with response parsing
    properties: Optional[Dict[str, Any]] = Field(None, description="Legacy: use json_schema.properties instead")
    required: Optional[List[str]] = Field(None, description="Legacy: use json_schema.required instead")
    description: Optional[str] = None


class TextSchema(BaseModel):
    """Text output schema. Use this for lite processor."""
    type: Literal["text"] = Field("text", description="Must be 'text' for text output. Required for lite processor.")
    description: Optional[str] = None


class AutoSchema(BaseModel):
    """Auto-generated schema. NOT supported by lite processor - use TextSchema instead."""
    type: Literal["auto"] = Field("auto", description="Must be 'auto'. WARNING: Not supported by lite processor.")


class TaskSpec(BaseModel):
    """Task specification for structured outputs.

    IMPORTANT: For 'lite' processor, output_schema MUST be {"type": "text"}.
    Auto schema is NOT supported by lite processor.

    Example for lite processor:
        task_spec: {"output_schema": {"type": "text"}}

    Example for other processors with JSON output:
        task_spec: {
            "output_schema": {
                "type": "json",
                "json_schema": {"type": "object", "properties": {...}}
            }
        }
    """
    output_schema: Union[JsonSchema, TextSchema, AutoSchema] = Field(
        ...,
        description="Output schema. For lite processor: use {'type': 'text'}. For JSON output: use {'type': 'json', 'json_schema': {...}}. Auto schema NOT supported by lite."
    )
    objective: Optional[str] = Field(None, description="Optional objective/instructions for the task")


class McpServer(BaseModel):
    """MCP server configuration."""
    url: str
    headers: Optional[Dict[str, str]] = None


class McpToolCall(BaseModel):
    """MCP tool call record."""
    tool_name: str
    arguments: Dict[str, Any]
    result: Optional[Any] = None


class TaskRunInput(BaseModel):
    """Task run input for creating a new task.

    IMPORTANT INPUT FORMAT:
    - Without task_spec: input can be a simple string (e.g., "Research quantum computing")
    - With task_spec: input MUST be an object/dict (e.g., {"query": "Research quantum computing"})

    PROCESSOR + OUTPUT SCHEMA COMPATIBILITY:
    - lite processor: Only supports {"type": "text"} output schema. Does NOT support auto schema.
    - Other processors (base, core, pro, ultra): Support text, json, and auto schemas.

    Example (lite processor, no structured output):
        {"processor": "lite", "input": "Explain quantum entanglement"}

    Example (lite processor with task_spec):
        {
            "processor": "lite",
            "input": {"query": "Explain quantum entanglement"},
            "task_spec": {"output_schema": {"type": "text"}}
        }
    """
    processor: ProcessorType = Field(..., description="Processor tier: lite, base, core, core2x, pro, ultra, etc.")
    input: Union[str, Dict[str, Any]] = Field(
        ...,
        description="Task input. String for simple queries, or object {'query': '...'} when using task_spec."
    )
    task_spec: Optional[TaskSpec] = Field(
        None,
        description="Optional task specification. When provided, 'input' must be an object. For lite processor, output_schema must be {'type': 'text'}."
    )
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata to store with the task")
    source_policy: Optional[SourcePolicy] = None
    webhook: Optional[Webhook] = None


class BetaTaskRunInput(TaskRunInput):
    """Beta task run input with additional features like MCP server integration.

    Inherits all fields from TaskRunInput. See TaskRunInput for input format requirements.
    """
    enable_events: Optional[bool] = Field(False, description="Enable SSE event streaming for this task")
    mcp_servers: Optional[List[McpServer]] = Field(None, description="Optional MCP servers for tool execution")


class Citation(BaseModel):
    """Citation for task output."""
    url: Optional[str] = None
    title: Optional[str] = None
    excerpts: Optional[List[str]] = None


class FieldBasis(BaseModel):
    """Research basis for a field."""
    field: Optional[str] = None
    reasoning: Optional[str] = None
    confidence: Optional[Union[float, str]] = Field(None, description="Confidence level: float 0-1 or string 'high'/'medium'/'low'")
    citations: Optional[List[Citation]] = None


class TaskRunTextOutput(BaseModel):
    """Text output from task run."""
    type: str = "text"
    content: Union[str, Dict[str, Any]] = Field(..., description="Text content or dict with 'output' key")
    basis: Optional[List[FieldBasis]] = None

    @field_validator("content", mode="before")
    @classmethod
    def _normalize_content(cls, value: Union[str, Dict[str, Any]]) -> str:
        """Extract content string from dict if needed."""
        if isinstance(value, dict):
            return value.get("output", str(value))
        return value


class TaskRunJsonOutput(BaseModel):
    """JSON output from task run."""
    type: str = "json"
    content: Dict[str, Any]
    output_schema: Optional[JsonSchema] = None
    basis: Optional[List[FieldBasis]] = None


class BetaTaskRunTextOutput(TaskRunTextOutput):
    """Beta text output with additional fields."""
    mcp_tool_calls: Optional[List[McpToolCall]] = None
    beta_fields: Optional[Dict[str, Any]] = None


class BetaTaskRunJsonOutput(TaskRunJsonOutput):
    """Beta JSON output with additional fields."""
    mcp_tool_calls: Optional[List[McpToolCall]] = None
    beta_fields: Optional[Dict[str, Any]] = None


class TaskRunSourceStats(BaseModel):
    """Statistics about sources used."""
    total_sources: int
    unique_domains: int


class TaskRun(BaseModel):
    """Status of a task run.

    Contains metadata about a task run including its current status,
    processor used, and timestamps.
    """
    run_id: str = Field(
        ...,
        description="ID of the task run.",
        examples=["trun_e0083b6aac0544eb8686e8d2a76533d2"]
    )
    status: str = Field(
        ...,
        description="Status of the run: queued, action_required, running, completed, failed, cancelling, cancelled"
    )
    is_active: Optional[bool] = Field(
        None,
        description="Whether the run is currently active (status in {queued, running, cancelling})."
    )
    warnings: Optional[List[Warning]] = Field(
        None,
        description="Warnings for the run, if any."
    )
    error: Optional[Error] = Field(
        None,
        description="Error for the run, present only if status is 'failed'."
    )
    processor: str = Field(
        ...,
        description="Processor used for the run.",
        examples=["base", "lite", "pro"]
    )
    metadata: Optional[Dict[str, Union[str, int, float, bool]]] = Field(
        None,
        description="User-provided metadata stored with the run."
    )
    taskgroup_id: Optional[str] = Field(
        None,
        description="ID of the taskgroup to which the run belongs."
    )
    created_at: Optional[str] = Field(
        None,
        description="Timestamp of the creation of the task, as an RFC 3339 string."
    )
    modified_at: Optional[str] = Field(
        None,
        description="Timestamp of the last modification to the task, as an RFC 3339 string."
    )

    model_config = ConfigDict(extra="allow")


class TaskRunResult(BaseModel):
    """Task run result."""
    run: TaskRun
    output: Optional[Union[TaskRunTextOutput, TaskRunJsonOutput]] = None


class BetaTaskRunResult(BaseModel):
    """Beta task run result."""
    run: TaskRun
    output: Optional[Union[BetaTaskRunTextOutput, BetaTaskRunJsonOutput]] = None


# Task Events
class TaskRunEvent(BaseModel):
    """Base task run event."""
    type: str
    event_id: str
    timestamp: datetime


class TaskRunProgressMessageEvent(TaskRunEvent):
    """Progress message event."""
    type: str = "progress_message"
    data: Dict[str, Any]


class TaskRunProgressStatsEvent(TaskRunEvent):
    """Progress stats event."""
    type: str = "progress_stats"
    data: TaskRunSourceStats


class ManagedTaskStatusUpdate(BaseModel):
    """Status change observed while polling a managed run."""
    status: str = Field(..., description="Status: queued, action_required, running, completed, failed, cancelling, cancelled")
    timestamp: datetime
    detail: Optional[str] = None


class ManagedTaskRunResponse(BaseModel):
    """Response for a managed task run that was polled to completion."""
    run: TaskRun
    run_id: str
    final_status: str = Field(..., description="Final status: queued, action_required, running, completed, failed, cancelling, cancelled")
    summary: str
    status_updates: List[ManagedTaskStatusUpdate]
    elapsed_seconds: float
    poll_count: int
    output_text: Optional[str] = None
    output_json: Optional[Any] = Field(
        default=None,
        description="JSON output from the task. Can be any JSON-serializable type (dict, list, str, number, bool, null)."
    )
    raw_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ============================================================================
# Task Group Models (Beta)
# ============================================================================

class TaskGroupStatus(BaseModel):
    """Status of a task group.

    This is a nested object (NOT an enum) containing:
    - num_task_runs: Number of task runs in the group
    - task_run_status_counts: Count of runs in each status
    - is_active: Whether any runs are currently active
    - status_message: Human-readable status message
    - modified_at: Last status update timestamp
    """
    num_task_runs: int = Field(
        ...,
        description="Number of task runs in the group."
    )
    task_run_status_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="Number of task runs with each status. Keys: queued, action_required, running, completed, failed, cancelling, cancelled"
    )
    is_active: bool = Field(
        ...,
        description="True if at least one run in the group is currently active (status in {queued, running, cancelling})."
    )
    status_message: Optional[str] = Field(
        None,
        description="Human-readable status message for the group."
    )
    modified_at: Optional[str] = Field(
        None,
        description="Timestamp of the last status update to the group, as an RFC 3339 string."
    )

    model_config = ConfigDict(extra="allow")


class CreateTaskGroupRequest(BaseModel):
    """Create a task group to batch multiple tasks together.

    Task groups let you:
    - Submit multiple related tasks at once
    - Track collective progress
    - Get aggregated results

    SIMPLE USAGE:
        {}  # Empty object creates group with auto-generated ID

    WITH METADATA:
        {"metadata": {"project": "quarterly-research", "priority": "high"}}

    After creating a group, add tasks using the add_group_runs endpoint.
    """
    metadata: Optional[Dict[str, Union[str, int, float, bool]]] = Field(
        None,
        description="Optional tags/labels for the group. E.g., {'project': 'research', 'batch': 1}"
    )


class TaskGroupResponse(BaseModel):
    """Response object for a task group, including its status and metadata.

    The status field is a nested TaskGroupStatus object containing:
    - num_task_runs: Total runs in the group
    - task_run_status_counts: Breakdown by status
    - is_active: Whether group has active runs
    - status_message: Human-readable status
    """
    taskgroup_id: str = Field(
        ...,
        description="ID of the group."
    )
    metadata: Optional[Dict[str, Union[str, int, float, bool]]] = Field(
        None,
        description="User-provided metadata stored with the group."
    )
    status: TaskGroupStatus = Field(
        ...,
        description="Status of the group (nested object with run counts and activity state)."
    )
    created_at: Optional[str] = Field(
        None,
        description="Timestamp of the creation of the group, as an RFC 3339 string."
    )

    model_config = ConfigDict(extra="allow")


class TaskGroupRunRequest(BaseModel):
    """Add multiple task runs to an existing task group.

    SIMPLE EXAMPLE - multiple research tasks:
        {
            "runs": [
                {"processor": "lite", "input": "Research renewable energy trends"},
                {"processor": "lite", "input": "Research electric vehicle market"},
                {"processor": "lite", "input": "Research battery technology advances"}
            ]
        }

    WITH STRUCTURED OUTPUT (use base or higher processor):
        {
            "runs": [
                {
                    "processor": "base",
                    "input": {"query": "AI startup funding 2024"},
                    "task_spec": {
                        "output_schema": {
                            "type": "json",
                            "json_schema": {
                                "type": "object",
                                "properties": {
                                    "companies": {"type": "array"},
                                    "total_funding": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            ]
        }
    """
    runs: List[BetaTaskRunInput] = Field(
        ...,
        serialization_alias="inputs",
        description="List of tasks to add. Each follows BetaTaskRunInput format."
    )

    model_config = {"populate_by_name": True}


class TaskGroupRunResponse(BaseModel):
    """Task group run response."""
    run_ids: List[str]


class TaskGroupStatusEvent(BaseModel):
    """Task group status event."""
    type: str = "status"
    event_id: str
    timestamp: datetime
    data: TaskGroupResponse


# ============================================================================
# FindAll Models (Beta)
# ============================================================================

class FindAllRunStatus(str, Enum):
    """Status of a FindAll discovery run.

    ACTIVE: queued, running
    TERMINAL: completed, failed, cancelled
    """
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FindAllGenerator(str, Enum):
    """Generator tier for FindAll - controls search breadth and cost.

    CHOOSE BASED ON YOUR NEEDS:
    - preview: Testing only. Max ~10 matches. Cheapest.
    - base: Broad searches. Good for common entity types.
    - core: Moderate specificity. Good balance of coverage and precision.
    - pro: Rare/specific entities. Most thorough but expensive.

    COST ORDER: preview < base < core < pro
    THOROUGHNESS ORDER: preview < base < core < pro

    Start with 'base' for most use cases, upgrade to 'core' or 'pro' if
    you're not finding enough matches.
    """
    PREVIEW = "preview"  # Testing only, ~10 candidates max
    BASE = "base"        # Broad queries, common entities
    CORE = "core"        # Moderate specificity
    PRO = "pro"          # Rare/specific entities, most thorough


class FindAllTerminationReason(str, Enum):
    """Why a FindAll run stopped. Only 'match_limit_met' can be extended.

    EXTENDABLE (can continue searching):
    - match_limit_met: Found requested number of matches. Call extend to find more.

    NOT EXTENDABLE (search exhausted):
    - low_match_rate: Query too specific, not enough matches exist.
    - candidates_exhausted: Searched everything, no more to find.
    - error_occurred: Something went wrong.
    - timeout: Took too long.
    - user_cancelled: You cancelled it.
    """
    MATCH_LIMIT_MET = "match_limit_met"
    LOW_MATCH_RATE = "low_match_rate"
    CANDIDATES_EXHAUSTED = "candidates_exhausted"
    ERROR_OCCURRED = "error_occurred"
    TIMEOUT = "timeout"
    USER_CANCELLED = "user_cancelled"


class CandidateMatchStatus(str, Enum):
    """Status of each candidate entity found by FindAll.

    - generated: Found but not yet evaluated against match conditions.
    - matched: Passed all match conditions. This is what you want!
    - unmatched: Failed one or more match conditions.
    """
    GENERATED = "generated"
    MATCHED = "matched"
    UNMATCHED = "unmatched"


class InternalMatchCondition(BaseModel):
    """Internal match condition structure."""
    field: str
    operator: str
    value: Any


class InternalFindAllSchema(BaseModel):
    """Internal FindAll schema structure."""
    entity_type: str
    match_conditions: List[InternalMatchCondition]
    output_schema: Optional[JsonSchema] = None


class InternalFindAllCandidateMetrics(BaseModel):
    """Internal candidate metrics."""
    relevance_score: Optional[float] = None


class IngestInput(BaseModel):
    """FindAll ingest input."""
    objective: str


class MatchCondition(BaseModel):
    """A boolean condition that candidates must satisfy to be considered a match.

    Each condition has a name (identifier) and description (what must be true).

    Example conditions for finding AI startups:
        {"name": "is_ai_company", "description": "Company's primary focus is artificial intelligence or machine learning"}
        {"name": "is_startup", "description": "Company was founded within the last 5 years"}
        {"name": "has_funding", "description": "Company has raised venture capital funding"}
    """
    name: str = Field(
        ...,
        description="Short identifier for this condition. E.g., 'is_ai_company', 'has_funding'"
    )
    description: str = Field(
        ...,
        description="What must be true for a match. Be specific! E.g., 'Company was founded after 2020'"
    )


class FindAllEnrichment(BaseModel):
    """Additional data field to extract for each matched entity.

    Enrichments add structured data to your matches beyond just name/URL.

    Example enrichments:
        {"name": "funding_amount", "description": "Total funding raised in USD", "type": "string"}
        {"name": "employee_count", "description": "Approximate number of employees", "type": "number"}
        {"name": "headquarters", "description": "City and country of headquarters", "type": "string"}
    """
    name: str = Field(..., description="Field name in output. E.g., 'funding_amount'")
    description: str = Field(..., description="What to extract. E.g., 'Total funding raised in USD'")
    type: Optional[str] = Field(None, description="Expected type: 'string', 'number', 'boolean', 'array'")


class FindAllRunInput(BaseModel):
    """FindAll: Discover multiple entities matching your criteria across the web.

    Use FindAll when you need to find MANY things (companies, people, products, etc.)
    that match specific conditions. Unlike search, FindAll evaluates each candidate
    against your match_conditions.

    SIMPLE EXAMPLE - Find AI startups:
        {
            "objective": "Find AI startups that have raised Series A funding",
            "entity_type": "startup",
            "generator": "base",
            "match_limit": 20,
            "match_conditions": [
                {"name": "is_ai_focused", "description": "Company builds AI/ML products"},
                {"name": "series_a", "description": "Has raised Series A funding round"}
            ]
        }

    WITH ENRICHMENTS - Get structured data for each match:
        {
            "objective": "Find electric vehicle manufacturers",
            "entity_type": "company",
            "generator": "core",
            "match_limit": 50,
            "match_conditions": [
                {"name": "makes_evs", "description": "Manufactures electric vehicles"},
                {"name": "in_production", "description": "Has vehicles currently in production"}
            ],
            "enrichments": [
                {"name": "headquarters", "description": "Company HQ location"},
                {"name": "flagship_model", "description": "Most popular vehicle model"},
                {"name": "price_range", "description": "Price range of vehicles"}
            ]
        }

    GENERATOR SELECTION:
    - preview: Testing only (max 10 results)
    - base: Common entities, broad searches
    - core: Moderate specificity, good balance
    - pro: Rare/specific entities, most thorough
    """
    objective: str = Field(
        ...,
        description="What you're looking for in natural language. Be specific about the domain and criteria."
    )
    entity_type: str = Field(
        ...,
        description="Category of entity: 'company', 'startup', 'person', 'product', 'organization', etc."
    )
    match_conditions: List[MatchCondition] = Field(
        ...,
        description="Conditions entities must satisfy. Each has 'name' and 'description'. Be specific!"
    )
    generator: Union[FindAllGenerator, str] = Field(
        ...,
        description="Search tier: 'preview' (testing), 'base' (common), 'core' (specific), 'pro' (rare)"
    )
    match_limit: int = Field(
        ..., ge=5, le=1000,
        description="Maximum matches to find (5-1000). Preview caps at 10."
    )
    enrichments: Optional[List[FindAllEnrichment]] = Field(
        None,
        description="Extra data fields to extract for each match. Each has 'name', 'description', optional 'type'."
    )
    exclude_list: Optional[List[str]] = Field(
        None,
        description="Entity names or URLs to skip (e.g., previously found results)"
    )
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional tags for tracking")
    source_policy: Optional[SourcePolicy] = Field(None, description="Domain filtering")
    webhook: Optional[Webhook] = Field(None, description="Webhook for completion notifications")
    enable_events: Optional[bool] = Field(False, description="Enable SSE streaming of progress")
    mcp_servers: Optional[List[McpServer]] = Field(None, description="Optional MCP servers")

    @field_validator("generator", mode="before")
    @classmethod
    def _normalize_generator(cls, value: Union[str, FindAllGenerator]) -> Union[str, FindAllGenerator]:
        """Allow string or enum for generator field."""
        if isinstance(value, str):
            try:
                return FindAllGenerator(value.lower())
            except ValueError:
                return value  # Let pydantic validation handle invalid values
        return value


class FindAllExtendInput(BaseModel):
    """Extend FindAll run."""
    match_limit: int = Field(..., ge=1, le=1000)


class FindAllEnrichInput(BaseModel):
    """Enrich FindAll candidates."""
    enrichment_output_schema: JsonSchema
    processor: Optional[ProcessorType] = None
    enable_events: Optional[bool] = False
    metadata: Optional[Dict[str, Any]] = None
    source_policy: Optional[SourcePolicy] = None
    webhook: Optional[Webhook] = None
    mcp_servers: Optional[List[McpServer]] = None


class FindAllCandidate(BaseModel):
    """FindAll candidate entity."""
    candidate_id: str
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    match_status: Union[CandidateMatchStatus, str] = Field(
        ...,
        description="Status: generated, matched, or unmatched"
    )
    output: Optional[Dict[str, Any]] = Field(
        None,
        description="Extracted field results for this candidate"
    )
    basis: Optional[List[FieldBasis]] = Field(
        None,
        description="Citations and reasoning for the match decision"
    )


class FindAllRun(BaseModel):
    """FindAll run metadata."""
    findall_id: str
    status: FindAllRunStatus
    created_at: datetime
    modified_at: datetime
    objective: str
    entity_type: Optional[str] = None
    generator: Optional[Union[FindAllGenerator, str]] = None
    match_limit: int
    candidates_found: int
    candidates_matched: Optional[int] = None
    termination_reason: Optional[Union[FindAllTerminationReason, str]] = Field(
        None,
        description="Reason the run terminated (if completed/failed/cancelled)"
    )
    is_extendable: Optional[bool] = Field(
        None,
        description="Whether the run can be extended (only if termination_reason is match_limit_met)"
    )
    metadata: Optional[Dict[str, Any]] = None


class FindAllRunResult(BaseModel):
    """FindAll run result."""
    run: FindAllRun
    candidates: List[FindAllCandidate]


class FindAllRunStatusEvent(BaseModel):
    """FindAll run status event."""
    type: str = "run_status"
    event_id: str
    timestamp: datetime
    data: FindAllRun


class FindAllCandidateMatchStatusEvent(BaseModel):
    """FindAll candidate match status event."""
    type: str = "candidate_match_status"
    event_id: str
    timestamp: datetime
    data: FindAllCandidate


class FindAllSchemaUpdatedEvent(BaseModel):
    """FindAll schema updated event."""
    type: str = "schema_updated"
    event_id: str
    timestamp: datetime
    data: Dict[str, Any]


# ============================================================================
# Monitor Models (Alpha)
# ============================================================================

class MonitorCadence(str, Enum):
    """How often a monitor runs its query.

    - hourly: Every hour. Good for fast-moving topics (stock news, breaking events)
    - daily: Once per day. Best for most use cases.
    - weekly: Once per week. Good for slow-moving topics.
    """
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"


class MonitorStatus(str, Enum):
    """Current state of a monitor.

    - active: Running on schedule
    - paused: Temporarily stopped, can be resumed
    - failed: Error occurred, check error details
    """
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"


class MonitorWebhook(BaseModel):
    """Webhook to receive monitor alerts when new information is found.

    Example:
        {"url": "https://your-server.com/alerts", "headers": {"Authorization": "Bearer token"}}
    """
    url: HttpUrl = Field(..., description="Your webhook endpoint URL")
    headers: Optional[Dict[str, str]] = Field(None, description="Auth headers for webhook calls")


class CreateMonitorRequest(BaseModel):
    """Create a monitor to track a topic and get alerted on changes.

    Monitors run your query on a schedule and notify you when new/relevant
    information is found. Great for tracking competitors, news, regulations, etc.

    SIMPLE EXAMPLE - Track a company:
        {
            "query": "OpenAI product announcements and partnerships",
            "cadence": "daily"
        }

    WITH WEBHOOK - Get notified:
        {
            "query": "SEC filings from Tesla",
            "cadence": "daily",
            "webhook": {"url": "https://your-server.com/alerts"},
            "metadata": {"project": "competitor-tracking"}
        }

    HOURLY FOR FAST TOPICS:
        {
            "query": "Bitcoin price movements and major trades",
            "cadence": "hourly"
        }
    """
    query: str = Field(
        ...,
        description="What to monitor. Be specific! E.g., 'OpenAI product launches' not just 'AI news'"
    )
    cadence: MonitorCadence = Field(
        ...,
        description="How often to check: 'hourly', 'daily', or 'weekly'"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional tags for organizing monitors. E.g., {'project': 'research'}"
    )
    webhook: Optional[MonitorWebhook] = Field(
        None,
        description="Webhook to receive alerts. Include 'url' and optional 'headers'."
    )


class UpdateMonitorRequest(BaseModel):
    """Update an existing monitor's settings.

    All fields are optional - only include fields you want to change.

    PAUSE A MONITOR:
        {"status": "paused"}

    CHANGE CADENCE:
        {"cadence": "weekly"}

    UPDATE QUERY:
        {"query": "New search query for this monitor"}
    """
    query: Optional[str] = Field(None, description="New query to monitor")
    cadence: Optional[MonitorCadence] = Field(None, description="New cadence: hourly/daily/weekly")
    status: Optional[MonitorStatus] = Field(None, description="Change status: 'active' or 'paused'")
    webhook: Optional[MonitorWebhook] = Field(None, description="Update webhook settings")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Update metadata tags")


class MonitorResponse(BaseModel):
    """Monitor response."""
    monitor_id: str
    query: str
    cadence: MonitorCadence
    status: MonitorStatus
    created_at: datetime
    modified_at: datetime
    last_execution_at: Optional[datetime] = None
    next_execution_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    webhook: Optional[MonitorWebhook] = None


class MonitorExecutionCompletion(BaseModel):
    """Monitor execution completion."""
    execution_id: str
    monitor_id: str
    timestamp: datetime
    has_material_change: bool
    output: Optional[Dict[str, Any]] = None


class MonitorExecutionError(BaseModel):
    """Monitor execution error."""
    execution_id: str
    monitor_id: str
    timestamp: datetime
    error: Error


class MonitorEventUpdate(BaseModel):
    """Monitor event update."""
    event_type: str
    timestamp: datetime
    data: Union[MonitorExecutionCompletion, MonitorExecutionError]


class MonitorExecutionHistory(BaseModel):
    """Monitor execution history."""
    monitor_id: str
    executions: List[MonitorExecutionCompletion]
    total_executions: int


class MonitorEventDetail(BaseModel):
    """Material change event emitted by a monitor."""
    type: Literal["event"] = "event"
    event_group_id: str
    output: str
    event_date: Optional[str] = None
    source_urls: List[str]


class MonitorEventCompletion(BaseModel):
    """Monitor execution completion event."""
    type: Literal["completion"] = "completion"
    monitor_run_id: str


class MonitorEventErrorEntry(BaseModel):
    """Monitor execution error event."""
    type: Literal["error"] = "error"
    error: str
    id: str
    date: datetime


class MonitorEventGroupResponse(BaseModel):
    """Event group payload returned from monitor event group lookups."""
    events: List[MonitorEventDetail]


class MonitorEventsResponse(BaseModel):
    """Flattened monitor event stream (event/completion/error)."""
    events: List[Union[MonitorEventDetail, MonitorEventCompletion, MonitorEventErrorEntry]]


# ============================================================================
# Documentation Catalog Models
# ============================================================================


class DocResource(BaseModel):
    """Single documentation resource entry."""
    title: str
    url: HttpUrl
    description: Optional[str] = None


class DocSection(BaseModel):
    """Documentation section grouping."""
    name: str
    slug: str
    resources: List[DocResource]


class DocCatalog(BaseModel):
    """Documentation catalog parsed from llms.txt."""
    title: str
    sections: List[DocSection]
    source_path: str
    last_modified: datetime


class DocSectionSummary(BaseModel):
    """Summary of documentation section."""
    name: str
    slug: str
    total_resources: int


class DocResourceWithSection(DocResource):
    """Documentation resource coupled with section metadata."""
    section: str


class DocSearchResponse(BaseModel):
    """Search results for documentation catalog."""
    query: str
    total_results: int
    results: List[DocResourceWithSection]


# ============================================================================
# Chat API Models (OpenAI-compatible)
# ============================================================================

class ChatMessage(BaseModel):
    """Chat message in OpenAI format."""
    role: Literal["system", "user", "assistant"]
    content: str


class ChatResponseFormat(BaseModel):
    """Response format configuration for chat completions."""
    type: Literal["text", "json_object", "json_schema"] = Field(
        "text",
        description="Output format type"
    )
    json_schema: Optional[Dict[str, Any]] = Field(
        None,
        description="JSON schema for structured output (when type is json_schema)"
    )


class ChatCompletionRequest(BaseModel):
    """Chat completion request (OpenAI-compatible).

    Note: temperature and max_tokens are accepted but ignored by Parallel API.
    Use response_format for structured JSON output.
    """
    model: str = Field(
        ...,
        description="Model to use. Use 'speed' for Parallel's chat model."
    )
    messages: List[ChatMessage] = Field(..., min_length=1)
    stream: Optional[bool] = Field(False, description="Whether to stream the response")
    response_format: Optional[ChatResponseFormat] = Field(
        None,
        description="Output format - supports JSON schema for structured responses"
    )
    temperature: Optional[float] = Field(
        None,
        ge=0,
        le=2,
        description="Ignored by Parallel API"
    )
    max_tokens: Optional[int] = Field(
        None,
        ge=1,
        description="Ignored by Parallel API"
    )


class ChatCompletionChoice(BaseModel):
    """Single choice in chat completion response."""
    index: int
    message: ChatMessage
    finish_reason: Optional[str] = None


class ChatCompletionUsage(BaseModel):
    """Token usage in chat completion response."""
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class ChatCompletionResponse(BaseModel):
    """Chat completion response (OpenAI-compatible)."""
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: Optional[ChatCompletionUsage] = None


class ChatCompletionChunkDelta(BaseModel):
    """Delta content in streaming chunk."""
    role: Optional[str] = None
    content: Optional[str] = None


class ChatCompletionChunkChoice(BaseModel):
    """Single choice in streaming chunk."""
    index: int
    delta: ChatCompletionChunkDelta
    finish_reason: Optional[str] = None


class ChatCompletionChunk(BaseModel):
    """Streaming chunk for chat completion."""
    id: str
    object: str = "chat.completion.chunk"
    created: int
    model: str
    choices: List[ChatCompletionChunkChoice]


# ============================================================================
# Health Check
# ============================================================================

class HealthCheckResponse(BaseModel):
    """Health check response."""
    status: str = "healthy"
    version: str = "0.1.0"
    timestamp: datetime


# ============================================================================
# Token Metrics (Context Visibility)
# ============================================================================

class EndpointTokenMetrics(BaseModel):
    """Token metrics for a specific endpoint."""
    endpoint: str
    total_requests: int
    total_tokens: int
    avg_tokens_per_request: float
    avg_input_tokens: float
    avg_output_tokens: float
    p50_tokens: Optional[float] = None
    p95_tokens: Optional[float] = None
    p99_tokens: Optional[float] = None
    min_tokens: Optional[int] = None
    max_tokens: Optional[int] = None


class ProcessorTokenMetrics(BaseModel):
    """Token metrics for a specific processor."""
    processor: str
    total_requests: int
    total_tokens: int
    avg_tokens_per_request: float


class ExpensiveRequest(BaseModel):
    """Details of a high-token-count request."""
    request_id: str
    path: str
    total_tokens: int
    request_tokens: int
    response_tokens: int
    size_bucket: str
    operation_type: Optional[str] = None
    timestamp: str
    duration_ms: Optional[int] = None


class TokenMetricsResponse(BaseModel):
    """Response containing comprehensive token usage metrics."""
    timeframe: str
    total_requests: int
    total_tokens: int
    total_input_tokens: int
    total_output_tokens: int
    avg_tokens_per_request: float
    avg_input_tokens: float
    avg_output_tokens: float
    by_endpoint: Dict[str, EndpointTokenMetrics]
    by_operation_type: Dict[str, Dict[str, Any]]
    by_size_bucket: Dict[str, int]
    expensive_requests: List[ExpensiveRequest]
    token_percentiles: Dict[str, float]


class TokenMetricsQuery(BaseModel):
    """Query parameters for token metrics."""
    timeframe: str = Field(
        default="24h",
        description="Time window: 1h, 6h, 24h, 7d, 30d"
    )
    group_by: str = Field(
        default="endpoint",
        description="Group by: endpoint, operation_type, processor, size_bucket"
    )
    limit: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Limit for grouped results"
    )
    include_expensive: bool = Field(
        default=True,
        description="Include list of most expensive requests"
    )
    expensive_limit: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Number of expensive requests to return"
    )
