"""
Comprehensive Pydantic schemas for MCP endpoints.
These schemas handle all the edge cases and ensure proper serialization/deserialization.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
import json


# ============================================================================
# ENUMS
# ============================================================================

class ProcessorType(str, Enum):
    """Available processors."""
    LITE = "lite"
    TURBO = "turbo"
    PREMIUM = "premium"


class TaskStatus(str, Enum):
    """Task execution status."""
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class GroupStatus(str, Enum):
    """Task group status."""
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class OptimizationStrategy(str, Enum):
    """Task optimization strategies."""
    COST = "cost"
    SPEED = "speed"
    BALANCED = "balanced"
    QUALITY = "quality"
    ADAPTIVE = "adaptive"


class ExportFormat(str, Enum):
    """Export formats."""
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"


# ============================================================================
# SEARCH SCHEMAS
# ============================================================================

class SearchInput(BaseModel):
    """Input schema for search endpoint."""
    objective: str = Field(..., description="Search query or research objective")
    max_results: int = Field(default=10, ge=1, le=100, description="Maximum number of results")

    model_config = {
        "json_schema_extra": {
            "example": {
                "objective": "Latest developments in quantum computing",
                "max_results": 5
            }
        }
    }


class SearchResult(BaseModel):
    """Individual search result."""
    title: str
    url: str
    excerpts: List[str]
    publish_date: Optional[str] = None


class SearchOutput(BaseModel):
    """Output schema for search endpoint."""
    results: List[SearchResult]
    total_results: int
    search_id: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "results": [
                    {
                        "title": "Quantum Computing Breakthrough",
                        "url": "https://example.com/article",
                        "excerpts": ["Scientists achieve...", "The breakthrough..."],
                        "publish_date": "2024-11-19"
                    }
                ],
                "total_results": 1
            }
        }
    }


# ============================================================================
# TASK RUN SCHEMAS
# ============================================================================

class TaskSpecSchema(BaseModel):
    """Task specification for structured outputs."""
    output_schema: Optional[Dict[str, Any]] = Field(None, description="JSON Schema for expected output")
    instructions: Optional[str] = Field(None, description="Additional instructions for the task")


class ManagedTaskRunInput(BaseModel):
    """Input schema for managed task run endpoint."""
    processor: ProcessorType = Field(default=ProcessorType.LITE)
    input: Union[str, Dict[str, Any]] = Field(..., description="Task input - string or structured object")
    task_spec: Optional[TaskSpecSchema] = None
    metadata: Optional[Dict[str, Any]] = None
    poll_interval: float = Field(default=2.0, ge=0.5, le=15.0, description="Polling interval in seconds")
    timeout: int = Field(default=900, ge=30, le=7200, description="Timeout in seconds")

    @field_validator('input', mode='before')
    @classmethod
    def validate_input(cls, v, info):
        """Ensure input format matches task_spec requirements."""
        task_spec = info.data.get('task_spec') if hasattr(info, 'data') else None
        if task_spec and isinstance(v, str):
            # With task_spec, wrap string in object
            return {"query": v}
        elif not task_spec and isinstance(v, dict):
            # Without task_spec, extract string
            return v.get("query", str(v))
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "processor": "lite",
                "input": "Explain quantum entanglement",
                "poll_interval": 2.0,
                "timeout": 300
            }
        }
    }


class ManagedTaskRunOutput(BaseModel):
    """Output schema for managed task run endpoint."""
    task_run_id: str
    status: TaskStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    output: Optional[Union[str, Dict[str, Any]]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "task_run_id": "trun_abc123",
                "status": "completed",
                "created_at": "2024-11-19T20:00:00Z",
                "completed_at": "2024-11-19T20:00:30Z",
                "output": "Quantum entanglement is a phenomenon where..."
            }
        }
    }


# ============================================================================
# TASK GROUP SCHEMAS
# ============================================================================

class CreateGroupInput(BaseModel):
    """Input schema for create group endpoint."""
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @field_validator('metadata', mode='before')
    @classmethod
    def parse_metadata(cls, v):
        """Handle metadata that might be passed as JSON string."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except:
                return {"value": v}
        return v or {}

    model_config = {
        "json_schema_extra": {
            "example": {
                "metadata": {
                    "name": "Research batch",
                    "priority": "high"
                }
            }
        }
    }


class CreateGroupOutput(BaseModel):
    """Output schema for create group endpoint."""
    taskgroup_id: str
    status: GroupStatus = Field(default=GroupStatus.CREATED)
    created_at: datetime
    modified_at: Optional[datetime] = None
    total_runs: int = Field(default=0)
    completed_runs: int = Field(default=0)
    failed_runs: int = Field(default=0)
    metadata: Optional[Dict[str, Any]] = None

    @model_validator(mode='before')
    @classmethod
    def handle_missing_fields(cls, values):
        """Handle missing fields from API response."""
        # Handle status that might be a dict
        if 'status' in values and isinstance(values['status'], dict):
            values['status'] = GroupStatus.RUNNING

        # Ensure modified_at exists
        if 'modified_at' not in values or values['modified_at'] is None:
            values['modified_at'] = values.get('updated_at') or values.get('created_at')

        return values

    model_config = {
        "extra": "allow"  # Allow extra fields from API
    }


class AddGroupRunsInput(BaseModel):
    """Input schema for add runs to group endpoint."""
    runs: List[ManagedTaskRunInput] = Field(..., min_items=1)

    @field_validator('runs', mode='before')
    @classmethod
    def parse_runs(cls, v):
        """Handle runs that might be passed as JSON string."""
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except:
                return [{"processor": "lite", "input": v}]

        if not isinstance(v, list):
            v = [v]

        # Ensure each run is properly formatted
        fixed_runs = []
        for run in v:
            if isinstance(run, str):
                fixed_runs.append({"processor": "lite", "input": run})
            elif isinstance(run, dict):
                # Fix input field if needed
                if 'input' in run:
                    task_spec = run.get('task_spec')
                    input_val = run['input']
                    if task_spec and isinstance(input_val, str):
                        run['input'] = {"query": input_val}
                    elif not task_spec and isinstance(input_val, dict):
                        run['input'] = input_val.get("query", str(input_val))
                fixed_runs.append(run)
            else:
                fixed_runs.append({"processor": "lite", "input": str(run)})

        return fixed_runs

    model_config = {
        "json_schema_extra": {
            "example": {
                "runs": [
                    {"processor": "lite", "input": "Research task 1"},
                    {"processor": "lite", "input": "Research task 2"}
                ]
            }
        }
    }


class AddGroupRunsOutput(BaseModel):
    """Output schema for add runs to group endpoint."""
    run_ids: List[str]
    taskgroup_id: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "run_ids": ["trun_abc123", "trun_def456"],
                "taskgroup_id": "tgrp_xyz789"
            }
        }
    }


class ExecuteManagedGroupInput(BaseModel):
    """Input schema for execute managed group endpoint."""
    taskgroup_id: str = Field(..., description="Task group ID to execute")
    poll_interval: float = Field(default=2.0, ge=0.5, le=15.0)
    timeout: int = Field(default=3600, ge=60, le=7200)
    include_results: bool = Field(default=True)

    model_config = {
        "json_schema_extra": {
            "example": {
                "taskgroup_id": "tgrp_xyz789",
                "poll_interval": 2.0,
                "timeout": 600,
                "include_results": True
            }
        }
    }


class ExecuteManagedGroupOutput(BaseModel):
    """Output schema for execute managed group endpoint."""
    taskgroup_id: str
    status: GroupStatus
    total_runs: int
    completed_runs: int
    failed_runs: int
    results: Optional[List[Dict[str, Any]]] = None
    execution_time: float
    errors: Optional[List[str]] = None


class GetGroupAnalyticsInput(BaseModel):
    """Input schema for get group analytics endpoint."""
    taskgroup_id: str
    include_run_details: bool = Field(default=False)


class GetGroupAnalyticsOutput(BaseModel):
    """Output schema for get group analytics endpoint."""
    taskgroup_id: str
    status: GroupStatus
    total_runs: int
    completed_runs: int
    failed_runs: int
    success_rate: float
    average_execution_time: Optional[float] = None
    processor_distribution: Dict[str, int]
    run_details: Optional[List[Dict[str, Any]]] = None


class ExportGroupResultsInput(BaseModel):
    """Input schema for export group results endpoint."""
    taskgroup_id: str
    format: ExportFormat = Field(default=ExportFormat.JSON)
    include_inputs: bool = Field(default=True)
    include_outputs: bool = Field(default=True)


class ExportGroupResultsOutput(BaseModel):
    """Output schema for export group results endpoint."""
    format: str
    taskgroup_id: str
    data: Union[str, List[Dict[str, Any]], Dict[str, Any]]


class OptimizeAndExecuteGroupInput(BaseModel):
    """Input schema for optimize and execute group endpoint."""
    task_inputs: List[str] = Field(..., min_items=1, description="List of task prompts")
    strategy: OptimizationStrategy = Field(default=OptimizationStrategy.BALANCED)
    deduplicate: bool = Field(default=True)
    auto_retry: bool = Field(default=True)
    metadata: Optional[Dict[str, Any]] = None

    @field_validator('task_inputs', mode='before')
    @classmethod
    def parse_task_inputs(cls, v):
        """Handle task_inputs that might be passed as JSON string."""
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except:
                # Try splitting by commas
                if ',' in v:
                    v = [t.strip() for t in v.split(',')]
                else:
                    v = [v]

        if not isinstance(v, list):
            v = [str(v)]

        return [str(item) for item in v]

    model_config = {
        "json_schema_extra": {
            "example": {
                "task_inputs": [
                    "Research quantum computing",
                    "Analyze AI trends",
                    "Review blockchain technology"
                ],
                "strategy": "balanced",
                "deduplicate": True,
                "auto_retry": True
            }
        }
    }


class OptimizeAndExecuteGroupOutput(BaseModel):
    """Output schema for optimize and execute group endpoint."""
    taskgroup_id: str
    optimization_report: Dict[str, Any]
    execution_plan: List[Dict[str, Any]]
    status: GroupStatus
    total_tasks: int
    deduplicated_tasks: int
    estimated_cost: Optional[float] = None
    estimated_time: Optional[float] = None
    results: Optional[List[Dict[str, Any]]] = None


# ============================================================================
# STATE MANAGEMENT SCHEMAS
# ============================================================================

class GetRecentTasksInput(BaseModel):
    """Input schema for get recent tasks endpoint."""
    limit: int = Field(default=10, ge=1, le=100)


class GetRecentTasksOutput(BaseModel):
    """Output schema for get recent tasks endpoint."""
    tasks: List[Dict[str, Any]]
    total_count: int


class GetRecentGroupsInput(BaseModel):
    """Input schema for get recent groups endpoint."""
    limit: int = Field(default=10, ge=1, le=100)


class GetRecentGroupsOutput(BaseModel):
    """Output schema for get recent groups endpoint."""
    groups: List[Dict[str, Any]]
    total_count: int


class GetTaskStatusInput(BaseModel):
    """Input schema for get task status endpoint."""
    task_run_id: str


class GetTaskStatusOutput(BaseModel):
    """Output schema for get task status endpoint."""
    task_run_id: str
    status: TaskStatus
    processor: str
    input_data: Union[str, Dict[str, Any]]
    output_data: Optional[Union[str, Dict[str, Any]]] = None
    created_at: datetime
    last_updated: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class GetGroupStatusInput(BaseModel):
    """Input schema for get group status endpoint."""
    taskgroup_id: str


class GetGroupStatusOutput(BaseModel):
    """Output schema for get group status endpoint."""
    taskgroup_id: str
    status: GroupStatus
    total_runs: int
    completed_runs: int
    failed_runs: int
    run_ids: Optional[List[str]] = None
    created_at: datetime
    last_updated: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# ERROR SCHEMAS
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str
    detail: Optional[Union[str, Dict[str, Any]]] = None
    status_code: Optional[int] = None
    ref_id: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "error": "Task creation failed",
                "detail": "Invalid processor type",
                "status_code": 400
            }
        }
    }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_response_model(success_model: BaseModel, error_model: BaseModel = ErrorResponse):
    """Create a union response model for success/error cases."""
    return Union[success_model, error_model]