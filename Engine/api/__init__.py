"""
API Module

FastAPI server and HTTP client for the Agent Orchestration Platform.
"""

from .server import AgentOrchestrationAPI, app
from .client import AgentOrchestrationClient
from .models import (
    TaskType,
    WorkflowPattern,
    SubmitTaskRequest,
    SpawnAgentRequest,
    ExecuteWorkflowRequest,
    TaskResponse,
    TaskStatusResponse,
    AgentResponse,
    PoolStatusResponse,
    WorkflowStatusResponse,
    SystemStatusResponse,
    ErrorResponse,
)

__all__ = [
    "AgentOrchestrationAPI",
    "app",
    "AgentOrchestrationClient",
    "TaskType",
    "WorkflowPattern",
    "SubmitTaskRequest",
    "SpawnAgentRequest",
    "ExecuteWorkflowRequest",
    "TaskResponse",
    "TaskStatusResponse",
    "AgentResponse",
    "PoolStatusResponse",
    "WorkflowStatusResponse",
    "SystemStatusResponse",
    "ErrorResponse",
]
