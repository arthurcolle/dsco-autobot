"""
Pytest configuration and fixtures for Engine tests.

Provides:
- Mock clients (GraphSub, Redis, Temporal)
- Test databases (in-memory Redis)
- Common test data factories
"""

import pytest
import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_graphsub_client():
    """Mock GraphSub client for testing."""
    client = AsyncMock()
    client.create_node = AsyncMock(return_value={"id": "node-123", "type": "Agent"})
    client.query = AsyncMock(return_value={"results": []})
    client.create_edge = AsyncMock(return_value={"id": "edge-456"})
    client.deposit_pheromone = AsyncMock(return_value=True)
    return client


@pytest.fixture
async def mock_redis_client():
    """Mock Redis client for testing."""
    client = AsyncMock()
    client.xadd = AsyncMock(return_value="1234567890-0")
    client.xreadgroup = AsyncMock(return_value=[])
    client.publish = AsyncMock(return_value=1)
    client.get = AsyncMock(return_value=None)
    return client


@pytest.fixture
async def mock_temporal_client():
    """Mock Temporal client for testing."""
    client = AsyncMock()
    client.start_workflow = AsyncMock(return_value=MagicMock(id="workflow-789"))
    client.describe_workflow = AsyncMock(return_value={"state": "COMPLETED"})
    return client


@pytest.fixture
def sample_task():
    """Sample task for testing."""
    return {
        "id": "task-123",
        "type": "code_execution",
        "priority": 5,
        "input_data": {
            "code": "print('hello')",
            "language": "python"
        },
        "required_capabilities": ["python", "code_execution"],
        "estimated_duration_seconds": 10,
        "budget_estimate_usd": 0.01
    }


@pytest.fixture
def sample_agent():
    """Sample agent configuration for testing."""
    return {
        "id": "agent-test-1",
        "model": "claude-3.5-sonnet",
        "budget_remaining": 50.00,
        "capabilities": {
            "python": 0.95,
            "code_execution": 0.92,
            "web_search": 0.88
        },
        "status": "idle"
    }
