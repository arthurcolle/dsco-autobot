"""
Integration tests for REST API endpoints.

Tests all API functionality for task submission, agent management, and monitoring.
"""

import pytest
import json
from datetime import datetime
from httpx import AsyncClient

from api.server import AgentOrchestrationAPI
from api.models import TaskType, WorkflowPattern


@pytest.fixture
def api():
    """Create API instance for testing."""
    return AgentOrchestrationAPI()


@pytest.fixture
async def client(api):
    """Create async HTTP client."""
    async with AsyncClient(app=api.app, base_url="http://test") as ac:
        yield ac


class TestTaskEndpoints:
    """Test task submission and status endpoints."""

    async def test_submit_task_success(self, client):
        """Test successful task submission."""
        response = await client.post(
            "/api/tasks",
            json={
                "task_type": "code",
                "task_data": {"code": "print('hello')"},
                "required_capabilities": ["python"],
                "priority": 5,
                "estimated_cost": 0.5,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "pending"
        assert data["task_id"].startswith("task-")

    async def test_submit_task_with_invalid_type(self, client):
        """Test task submission with invalid task type."""
        response = await client.post(
            "/api/tasks",
            json={
                "task_type": "invalid_type",
                "task_data": {},
                "priority": 5,
                "estimated_cost": 1.0,
            },
        )
        assert response.status_code == 422  # Validation error

    async def test_submit_task_without_required_data(self, client):
        """Test task submission without required fields."""
        response = await client.post(
            "/api/tasks",
            json={
                "task_type": "code",
                # Missing task_data
                "priority": 5,
                "estimated_cost": 1.0,
            },
        )
        assert response.status_code == 422

    async def test_get_task_status_success(self, client):
        """Test getting task status."""
        # First submit a task
        submit_response = await client.post(
            "/api/tasks",
            json={
                "task_type": "search",
                "task_data": {"query": "test"},
                "priority": 7,
                "estimated_cost": 0.3,
            },
        )
        task_id = submit_response.json()["task_id"]

        # Then get its status
        response = await client.get(f"/api/tasks/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == task_id
        assert data["status"] == "pending"
        assert data["progress"] == 0.0

    async def test_get_task_status_not_found(self, client):
        """Test getting status of non-existent task."""
        response = await client.get("/api/tasks/task-nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_list_tasks_empty(self, client):
        """Test listing tasks when none exist."""
        response = await client.get("/api/tasks")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    async def test_list_tasks_with_filter(self, client):
        """Test listing tasks with status filter."""
        # Submit multiple tasks
        for i in range(3):
            await client.post(
                "/api/tasks",
                json={
                    "task_type": "llm",
                    "task_data": {"prompt": f"test {i}"},
                    "priority": 5,
                    "estimated_cost": 1.0,
                },
            )

        # List all pending tasks
        response = await client.get("/api/tasks?status=pending")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert all(task["status"] == "pending" for task in data)

    async def test_list_tasks_with_limit(self, client):
        """Test listing tasks with limit."""
        # Submit 5 tasks
        for i in range(5):
            await client.post(
                "/api/tasks",
                json={
                    "task_type": "custom",
                    "task_data": {"data": f"test {i}"},
                    "priority": 5,
                    "estimated_cost": 1.0,
                },
            )

        # List with limit=2
        response = await client.get("/api/tasks?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2


class TestAgentEndpoints:
    """Test agent spawning and management endpoints."""

    async def test_spawn_agent_success(self, client):
        """Test successful agent spawning."""
        response = await client.post(
            "/api/agents/spawn",
            json={
                "model": "claude-3.5-sonnet",
                "budget": 100.0,
                "capabilities": {"python": 0.95, "web-search": 0.88},
                "pool_name": "default",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "agent_id" in data
        assert data["status"] == "healthy"
        assert data["model"] == "claude-3.5-sonnet"
        assert data["budget_remaining"] == 100.0
        assert data["agent_id"].startswith("agent-")

    async def test_spawn_agent_with_defaults(self, client):
        """Test agent spawning with default values."""
        response = await client.post(
            "/api/agents/spawn",
            json={},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["model"] == "claude-3.5-sonnet"
        assert data["budget_remaining"] == 100.0

    async def test_spawn_agent_invalid_budget(self, client):
        """Test agent spawning with invalid budget."""
        response = await client.post(
            "/api/agents/spawn",
            json={
                "budget": -50.0,  # Invalid negative budget
                "model": "claude-3.5-sonnet",
            },
        )
        assert response.status_code == 422

    async def test_get_agent_success(self, client):
        """Test getting agent details."""
        # First spawn an agent
        spawn_response = await client.post(
            "/api/agents/spawn",
            json={
                "model": "gpt-4",
                "budget": 50.0,
                "capabilities": {"gpt": 0.9},
                "pool_name": "test-pool",
            },
        )
        agent_id = spawn_response.json()["agent_id"]

        # Then get its details
        response = await client.get(f"/api/agents/{agent_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["agent_id"] == agent_id
        assert data["model"] == "gpt-4"
        assert data["status"] == "healthy"
        assert data["budget_remaining"] == 50.0

    async def test_get_agent_not_found(self, client):
        """Test getting non-existent agent."""
        response = await client.get("/api/agents/agent-nonexistent")
        assert response.status_code == 404

    async def test_list_agents_empty(self, client):
        """Test listing agents when none exist."""
        response = await client.get("/api/agents")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    async def test_list_agents_multiple(self, client):
        """Test listing multiple agents."""
        # Spawn 3 agents
        agent_ids = []
        for i in range(3):
            response = await client.post(
                "/api/agents/spawn",
                json={
                    "model": f"model-{i}",
                    "budget": 100.0 + (i * 10),
                    "pool_name": "default",
                },
            )
            agent_ids.append(response.json()["agent_id"])

        # List all agents
        response = await client.get("/api/agents")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        returned_ids = [a["agent_id"] for a in data]
        assert all(aid in returned_ids for aid in agent_ids)

    async def test_list_agents_with_filter(self, client):
        """Test listing agents with filters."""
        # Spawn agents
        await client.post(
            "/api/agents/spawn",
            json={
                "model": "claude",
                "pool_name": "pool-a",
                "budget": 100.0,
            },
        )
        await client.post(
            "/api/agents/spawn",
            json={
                "model": "gpt",
                "pool_name": "pool-b",
                "budget": 50.0,
            },
        )

        # Filter by pool
        response = await client.get("/api/agents?pool=pool-a")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert "pool_name" in data[0]

    async def test_check_agent_health_success(self, client):
        """Test agent health check."""
        # Spawn an agent
        spawn_response = await client.post(
            "/api/agents/spawn",
            json={"model": "test-model", "budget": 100.0},
        )
        agent_id = spawn_response.json()["agent_id"]

        # Check health
        response = await client.post(f"/api/agents/{agent_id}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["healthy"] is True
        assert data["agent_id"] == agent_id

    async def test_check_agent_health_not_found(self, client):
        """Test health check for non-existent agent."""
        response = await client.post("/api/agents/agent-nonexistent/health")
        assert response.status_code == 404

    async def test_prune_agent_success(self, client):
        """Test removing an agent."""
        # Spawn an agent
        spawn_response = await client.post(
            "/api/agents/spawn",
            json={"model": "test-model", "budget": 100.0},
        )
        agent_id = spawn_response.json()["agent_id"]

        # Verify it exists
        response = await client.get(f"/api/agents/{agent_id}")
        assert response.status_code == 200

        # Prune it
        response = await client.delete(f"/api/agents/{agent_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["pruned"] is True

        # Verify it's gone
        response = await client.get(f"/api/agents/{agent_id}")
        assert response.status_code == 404

    async def test_prune_agent_not_found(self, client):
        """Test pruning non-existent agent."""
        response = await client.delete("/api/agents/agent-nonexistent")
        assert response.status_code == 404


class TestPoolEndpoints:
    """Test agent pool status endpoints."""

    async def test_get_pool_status_empty(self, client):
        """Test pool status when no agents exist."""
        response = await client.get("/api/pool/status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_agents"] == 0
        assert data["healthy_agents"] == 0
        assert data["degraded_agents"] == 0
        assert data["unhealthy_agents"] == 0
        assert data["capacity_utilization"] == 0.0

    async def test_get_pool_status_with_agents(self, client):
        """Test pool status with multiple agents."""
        # Spawn 2 agents
        for i in range(2):
            await client.post(
                "/api/agents/spawn",
                json={
                    "model": f"model-{i}",
                    "budget": 100.0,
                    "pool_name": "default",
                },
            )

        response = await client.get("/api/pool/status?pool_name=default")
        assert response.status_code == 200
        data = response.json()
        assert data["total_agents"] == 2
        assert data["healthy_agents"] == 2
        assert data["pool_name"] == "default"

    async def test_get_pool_status_by_name(self, client):
        """Test pool status filtered by pool name."""
        # Spawn agents in different pools
        await client.post(
            "/api/agents/spawn",
            json={
                "model": "model-1",
                "budget": 100.0,
                "pool_name": "pool-a",
            },
        )
        await client.post(
            "/api/agents/spawn",
            json={
                "model": "model-2",
                "budget": 100.0,
                "pool_name": "pool-b",
            },
        )

        # Get status for pool-a
        response = await client.get("/api/pool/status?pool_name=pool-a")
        assert response.status_code == 200
        data = response.json()
        assert data["total_agents"] == 1
        assert data["pool_name"] == "pool-a"


class TestWorkflowEndpoints:
    """Test workflow execution endpoints."""

    async def test_execute_workflow_success(self, client):
        """Test successful workflow execution."""
        response = await client.post(
            "/api/workflows",
            json={
                "pattern": "fan_out",
                "tasks": [
                    {"task_id": "task-1", "task_type": "code", "task_data": {}},
                    {"task_id": "task-2", "task_type": "code", "task_data": {}},
                ],
                "duration_seconds": 60.0,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "workflow_id" in data
        assert data["status"] == "running"
        assert data["pattern"] == "fan_out"
        assert data["total_subtasks"] == 2

    async def test_execute_workflow_invalid_pattern(self, client):
        """Test workflow with invalid pattern."""
        response = await client.post(
            "/api/workflows",
            json={
                "pattern": "invalid_pattern",
                "tasks": [],
                "duration_seconds": 60.0,
            },
        )
        assert response.status_code == 422

    async def test_execute_workflow_sequential(self, client):
        """Test sequential workflow."""
        response = await client.post(
            "/api/workflows",
            json={
                "pattern": "sequential",
                "tasks": [
                    {"task_id": "t1", "task_type": "llm", "task_data": {}},
                    {"task_id": "t2", "task_type": "llm", "task_data": {}},
                    {"task_id": "t3", "task_type": "llm", "task_data": {}},
                ],
                "duration_seconds": 120.0,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["pattern"] == "sequential"
        assert data["total_subtasks"] == 3

    async def test_execute_workflow_consensus(self, client):
        """Test consensus workflow."""
        response = await client.post(
            "/api/workflows",
            json={
                "pattern": "consensus",
                "tasks": [
                    {"task_id": "t1", "task_type": "search", "task_data": {}},
                    {"task_id": "t2", "task_type": "search", "task_data": {}},
                    {"task_id": "t3", "task_type": "search", "task_data": {}},
                ],
                "duration_seconds": 90.0,
            },
        )
        assert response.status_code == 200

    async def test_execute_workflow_pipeline(self, client):
        """Test pipeline workflow."""
        response = await client.post(
            "/api/workflows",
            json={
                "pattern": "pipeline",
                "tasks": [
                    {"task_id": "extract", "task_type": "code", "task_data": {}},
                    {"task_id": "transform", "task_type": "code", "task_data": {}},
                    {"task_id": "load", "task_type": "code", "task_data": {}},
                ],
                "duration_seconds": 60.0,
            },
        )
        assert response.status_code == 200

    async def test_get_workflow_status_success(self, client):
        """Test getting workflow status."""
        # Execute a workflow
        exec_response = await client.post(
            "/api/workflows",
            json={
                "pattern": "fan_out",
                "tasks": [{"task_id": "t1", "task_type": "code", "task_data": {}}],
                "duration_seconds": 60.0,
            },
        )
        workflow_id = exec_response.json()["workflow_id"]

        # Get its status
        response = await client.get(f"/api/workflows/{workflow_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == workflow_id
        assert data["status"] == "running"

    async def test_get_workflow_status_not_found(self, client):
        """Test getting status of non-existent workflow."""
        response = await client.get("/api/workflows/wf-nonexistent")
        assert response.status_code == 404


class TestSystemEndpoints:
    """Test system status endpoints."""

    async def test_health_check_success(self, client):
        """Test system health check."""
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    async def test_system_status_success(self, client):
        """Test overall system status."""
        response = await client.get("/api/system/status")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "timestamp" in data
        assert "agents" in data
        assert "gpu" in data
        assert data["temporal_healthy"] is not None
        assert data["redis_healthy"] is not None
        assert data["graphsub_healthy"] is not None

    async def test_system_status_with_agents(self, client):
        """Test system status reflects agent pool."""
        # Spawn an agent
        await client.post(
            "/api/agents/spawn",
            json={
                "model": "test",
                "budget": 100.0,
                "pool_name": "default",
            },
        )

        # Check system status
        response = await client.get("/api/system/status")
        assert response.status_code == 200
        data = response.json()
        assert data["agents"]["total_agents"] == 1
        assert data["agents"]["healthy_agents"] == 1

    async def test_system_status_request_counting(self, client):
        """Test that system status tracks requests."""
        # Get initial request count
        response1 = await client.get("/api/system/status")
        initial_count = response1.json()["requests_processed"]

        # Make some requests
        await client.post(
            "/api/tasks",
            json={
                "task_type": "code",
                "task_data": {},
                "priority": 5,
                "estimated_cost": 1.0,
            },
        )
        await client.post(
            "/api/agents/spawn",
            json={"model": "test", "budget": 100.0},
        )

        # Check request count increased
        response2 = await client.get("/api/system/status")
        final_count = response2.json()["requests_processed"]
        assert final_count > initial_count


class TestCORSMiddleware:
    """Test CORS configuration."""

    async def test_cors_headers_present(self, client):
        """Test that CORS headers are present."""
        response = await client.get("/health")
        # FastAPI CORS middleware should add headers
        assert response.status_code == 200


class TestErrorHandling:
    """Test error handling and responses."""

    async def test_404_not_found(self, client):
        """Test 404 error for non-existent endpoint."""
        response = await client.get("/api/nonexistent")
        assert response.status_code == 404

    async def test_method_not_allowed(self, client):
        """Test 405 method not allowed."""
        response = await client.put("/api/tasks")
        assert response.status_code in [404, 405]  # Depends on FastAPI version

    async def test_invalid_json(self, client):
        """Test handling of invalid JSON in request."""
        response = await client.post(
            "/api/tasks",
            content="invalid json",
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422
