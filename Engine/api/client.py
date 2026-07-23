"""
HTTP Client for Agent Orchestration API

Provides synchronous client for REST API interaction.
"""

import httpx
from typing import Optional, Dict, List, Any
from urllib.parse import urljoin
import json

from dsco_shared.config.service_registry import ServiceName, resolve

from .models import (
    SubmitTaskRequest,
    SpawnAgentRequest,
    ExecuteWorkflowRequest,
    TaskResponse,
    TaskStatusResponse,
    AgentResponse,
    PoolStatusResponse,
    WorkflowStatusResponse,
    SystemStatusResponse,
    TaskType,
    WorkflowPattern,
)


class AgentOrchestrationClient:
    """HTTP client for Agent Orchestration API."""

    def __init__(self, base_url: str = resolve(ServiceName.ENGINE)):
        """Initialize client with base URL."""
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, timeout=30.0)

    def close(self):
        """Close HTTP client."""
        self.client.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # Task endpoints

    def submit_task(
        self,
        task_type: str,
        task_data: Dict[str, Any],
        required_capabilities: Optional[List[str]] = None,
        priority: int = 5,
        estimated_cost: float = 1.0,
    ) -> TaskResponse:
        """Submit a new task."""
        request = SubmitTaskRequest(
            task_type=TaskType(task_type),
            task_data=task_data,
            required_capabilities=required_capabilities or [],
            priority=priority,
            estimated_cost=estimated_cost,
        )
        response = self.client.post(
            "/api/tasks",
            json=request.model_dump(),
        )
        response.raise_for_status()
        data = response.json()
        return TaskResponse(**data)

    def get_task_status(self, task_id: str) -> TaskStatusResponse:
        """Get task status."""
        response = self.client.get(f"/api/tasks/{task_id}")
        response.raise_for_status()
        data = response.json()
        return TaskStatusResponse(**data)

    def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[TaskStatusResponse]:
        """List all tasks."""
        params = {"limit": limit}
        if status:
            params["status"] = status

        response = self.client.get("/api/tasks", params=params)
        response.raise_for_status()
        data = response.json()
        return [TaskStatusResponse(**task) for task in data]

    # Agent endpoints

    def spawn_agent(
        self,
        model: str = "claude-3.5-sonnet",
        budget: float = 100.0,
        capabilities: Optional[Dict[str, float]] = None,
        pool_name: str = "default",
    ) -> AgentResponse:
        """Spawn a new agent."""
        request = SpawnAgentRequest(
            model=model,
            budget=budget,
            capabilities=capabilities or {"general": 0.5},
            pool_name=pool_name,
        )
        response = self.client.post(
            "/api/agents/spawn",
            json=request.model_dump(),
        )
        response.raise_for_status()
        data = response.json()
        return AgentResponse(**data)

    def get_agent(self, agent_id: str) -> AgentResponse:
        """Get agent details."""
        response = self.client.get(f"/api/agents/{agent_id}")
        response.raise_for_status()
        data = response.json()
        return AgentResponse(**data)

    def list_agents(
        self,
        status: Optional[str] = None,
        pool: Optional[str] = None,
    ) -> List[AgentResponse]:
        """List all agents."""
        params = {}
        if status:
            params["status"] = status
        if pool:
            params["pool"] = pool

        response = self.client.get("/api/agents", params=params)
        response.raise_for_status()
        data = response.json()
        return [AgentResponse(**agent) for agent in data]

    def check_agent_health(self, agent_id: str) -> Dict[str, Any]:
        """Check agent health."""
        response = self.client.post(f"/api/agents/{agent_id}/health")
        response.raise_for_status()
        return response.json()

    def prune_agent(self, agent_id: str) -> Dict[str, Any]:
        """Remove an agent from the pool."""
        response = self.client.delete(f"/api/agents/{agent_id}")
        response.raise_for_status()
        return response.json()

    # Pool endpoints

    def get_pool_status(self, pool_name: str = "default") -> PoolStatusResponse:
        """Get pool status and metrics."""
        params = {"pool_name": pool_name}
        response = self.client.get("/api/pool/status", params=params)
        response.raise_for_status()
        data = response.json()
        return PoolStatusResponse(**data)

    # Workflow endpoints

    def execute_workflow(
        self,
        pattern: str,
        tasks: List[Dict[str, Any]],
        duration_seconds: float = 60.0,
    ) -> WorkflowStatusResponse:
        """Execute a multi-agent workflow."""
        request = ExecuteWorkflowRequest(
            pattern=WorkflowPattern(pattern),
            tasks=tasks,
            duration_seconds=duration_seconds,
        )
        response = self.client.post(
            "/api/workflows",
            json=request.model_dump(),
        )
        response.raise_for_status()
        data = response.json()
        return WorkflowStatusResponse(**data)

    def get_workflow_status(self, workflow_id: str) -> WorkflowStatusResponse:
        """Get workflow status."""
        response = self.client.get(f"/api/workflows/{workflow_id}")
        response.raise_for_status()
        data = response.json()
        return WorkflowStatusResponse(**data)

    # System endpoints

    def get_system_status(self) -> SystemStatusResponse:
        """Get overall system status."""
        response = self.client.get("/api/system/status")
        response.raise_for_status()
        data = response.json()
        return SystemStatusResponse(**data)

    def health_check(self) -> Dict[str, Any]:
        """Check system health."""
        response = self.client.get("/health")
        response.raise_for_status()
        return response.json()
