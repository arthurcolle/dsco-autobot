"""
API Data Models

Pydantic models for request/response validation.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime


class TaskType(str, Enum):
    """Task types."""
    CODE = "code"
    SEARCH = "search"
    LLM = "llm"
    CUSTOM = "custom"


class WorkflowPattern(str, Enum):
    """Workflow patterns."""
    FAN_OUT = "fan_out"
    SEQUENTIAL = "sequential"
    CONSENSUS = "consensus"
    PIPELINE = "pipeline"


# Request Models

class SubmitTaskRequest(BaseModel):
    """Request to submit a task."""
    task_type: TaskType
    task_data: Dict[str, Any]
    required_capabilities: List[str] = []
    priority: int = Field(default=5, ge=1, le=10)
    estimated_cost: float = Field(default=1.0, gt=0)
    deadline_seconds: Optional[int] = None

    class Config:
        schema_extra = {
            "example": {
                "task_type": "code",
                "task_data": {"code": "print('hello')", "language": "python"},
                "required_capabilities": ["python"],
                "priority": 5,
                "estimated_cost": 0.5,
            }
        }


class SpawnAgentRequest(BaseModel):
    """Request to spawn a new agent."""
    model: str = Field(default="claude-3.5-sonnet")
    budget: float = Field(default=100.0, gt=0)
    capabilities: Dict[str, float] = Field(
        default_factory=lambda: {"general": 0.5}
    )
    pool_name: str = Field(default="default")

    class Config:
        schema_extra = {
            "example": {
                "model": "claude-3.5-sonnet",
                "budget": 100.0,
                "capabilities": {"python": 0.95, "search": 0.88},
                "pool_name": "default",
            }
        }


class ExecuteWorkflowRequest(BaseModel):
    """Request to execute a multi-agent workflow."""
    pattern: WorkflowPattern
    tasks: List[Dict[str, Any]]
    duration_seconds: float = Field(default=60.0, gt=0)

    class Config:
        schema_extra = {
            "example": {
                "pattern": "fan_out",
                "tasks": [
                    {
                        "task_id": "task-1",
                        "task_type": "code",
                        "task_data": {"code": "x = 1"},
                        "required_capabilities": ["python"],
                    },
                    {
                        "task_id": "task-2",
                        "task_type": "code",
                        "task_data": {"code": "y = 2"},
                        "required_capabilities": ["python"],
                    },
                ],
            }
        }


class HealthCheckRequest(BaseModel):
    """Request health check."""
    agent_id: Optional[str] = None


# Response Models

class TaskResponse(BaseModel):
    """Response for task submission."""
    task_id: str
    status: str
    created_at: datetime
    estimated_completion: Optional[datetime] = None

    class Config:
        schema_extra = {
            "example": {
                "task_id": "task-abc123",
                "status": "pending",
                "created_at": "2025-12-09T12:00:00",
                "estimated_completion": "2025-12-09T12:01:00",
            }
        }


class TaskStatusResponse(BaseModel):
    """Response for task status query."""
    task_id: str
    status: str  # pending, executing, completed, failed
    progress: float = Field(default=0.0, ge=0, le=1.0)
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    cost_incurred: float = 0.0


class AgentResponse(BaseModel):
    """Response for agent information."""
    agent_id: str
    model: str
    status: str  # healthy, degraded, unhealthy, dead
    budget_remaining: float
    tasks_completed: int
    tasks_failed: int
    capabilities: Dict[str, float]
    created_at: datetime
    last_heartbeat: datetime


class PoolStatusResponse(BaseModel):
    """Response for pool status."""
    pool_name: str
    total_agents: int
    healthy_agents: int
    degraded_agents: int
    unhealthy_agents: int
    capacity_utilization: float = Field(default=0.0, ge=0, le=1.0)
    total_budget_remaining: float
    pending_tasks: int
    executing_tasks: int
    avg_capability_confidence: float


class WorkflowStatusResponse(BaseModel):
    """Response for workflow status."""
    workflow_id: str
    pattern: WorkflowPattern
    status: str  # running, completed, failed
    created_at: datetime
    completed_at: Optional[datetime] = None
    total_subtasks: int
    completed_subtasks: int
    failed_subtasks: int
    results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}


class GPUStatusResponse(BaseModel):
    """Response for GPU cluster status."""
    total_gpus: Dict[str, int]
    available_gpus: Dict[str, int]
    pending_tasks: int
    running_tasks: int
    average_wait_seconds: float
    cluster_utilization: float = Field(default=0.0, ge=0, le=1.0)


class SystemStatusResponse(BaseModel):
    """Overall system status."""
    status: str  # operational, degraded, warning, critical
    timestamp: datetime
    agents: PoolStatusResponse
    gpu: GPUStatusResponse
    temporal_healthy: bool
    redis_healthy: bool
    graphsub_healthy: bool
    uptime_seconds: float
    requests_processed: int


class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
    timestamp: datetime

    class Config:
        schema_extra = {
            "example": {
                "error": "NotFound",
                "detail": "Task not found: invalid-id",
                "timestamp": "2025-12-09T12:00:00",
            }
        }
