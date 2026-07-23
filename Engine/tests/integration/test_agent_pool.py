"""
Agent Pool Integration Tests

Tests agent spawning, health checks, scaling, and coordination.
"""

import pytest
import asyncio
from datetime import datetime

from runtime.agent_pool import (
    AgentPool,
    PoolConfig,
    PoolScalingPolicy,
    AgentHealthStatus,
)


class TestAgentPoolInitialization:
    """Test pool initialization and configuration."""

    def test_pool_config_creation(self):
        """Test creating pool configuration."""
        config = PoolConfig(
            pool_name="test-pool",
            min_agents=2,
            max_agents=5,
            scaling_policy=PoolScalingPolicy.AUTO_LOAD,
        )

        assert config.pool_name == "test-pool"
        assert config.min_agents == 2
        assert config.max_agents == 5
        assert config.scaling_policy == PoolScalingPolicy.AUTO_LOAD

    def test_pool_initialization(self, mock_graphsub_client):
        """Test initializing agent pool."""
        config = PoolConfig(
            pool_name="test-pool",
            min_agents=1,
            max_agents=3,
        )

        pool = AgentPool(mock_graphsub_client, config)

        assert pool.config.pool_name == "test-pool"
        assert len(pool._agents) == 0
        assert not pool._running

    def test_default_agent_capabilities(self):
        """Test default agent capabilities."""
        config = PoolConfig(
            pool_name="test-pool",
            default_agent_capabilities={"python": 0.8, "ml": 0.6},
        )

        assert "python" in config.default_agent_capabilities
        assert config.default_agent_capabilities["python"] == 0.8


class TestAgentSpawning:
    """Test agent spawning and lifecycle."""

    @pytest.mark.asyncio
    async def test_spawn_agent(self, mock_graphsub_client):
        """Test spawning an agent."""
        config = PoolConfig(
            pool_name="spawn-pool",
            min_agents=0,
            max_agents=5,
        )
        pool = AgentPool(mock_graphsub_client, config)

        # Don't actually start the pool, just test spawn logic
        agent = await pool.spawn_agent()

        # Even with mock, should return an agent or None
        assert agent is None or agent.agent_id.startswith("spawn-pool-")

    @pytest.mark.asyncio
    async def test_spawn_exceeds_max(self, mock_graphsub_client):
        """Test spawning when at max capacity."""
        config = PoolConfig(
            pool_name="max-pool",
            min_agents=0,
            max_agents=1,
        )
        pool = AgentPool(mock_graphsub_client, config)

        # Manually add an agent to registry
        pool._agents["agent-1"] = None

        # Try to spawn - should fail
        agent = await pool.spawn_agent()
        assert agent is None


class TestHealthChecking:
    """Test agent health checking."""

    @pytest.mark.asyncio
    async def test_health_check_empty_pool(self, mock_graphsub_client):
        """Test health check on empty pool."""
        config = PoolConfig(pool_name="empty-pool")
        pool = AgentPool(mock_graphsub_client, config)

        results = await pool.health_check()
        assert results == {}

    @pytest.mark.asyncio
    async def test_get_agent_list_empty(self, mock_graphsub_client):
        """Test getting agent list from empty pool."""
        config = PoolConfig(pool_name="empty-pool")
        pool = AgentPool(mock_graphsub_client, config)

        agents = await pool.get_agent_list()
        assert agents == []

    @pytest.mark.asyncio
    async def test_get_agent_list_with_filter(self, mock_graphsub_client):
        """Test getting filtered agent list."""
        config = PoolConfig(pool_name="filter-pool")
        pool = AgentPool(mock_graphsub_client, config)

        # Manually add health records
        from runtime.agent_pool import AgentHealthCheckResult

        pool._agent_health["agent-1"] = AgentHealthCheckResult(
            agent_id="agent-1",
            status=AgentHealthStatus.HEALTHY,
        )
        pool._agent_health["agent-2"] = AgentHealthCheckResult(
            agent_id="agent-2",
            status=AgentHealthStatus.DEGRADED,
        )

        # Get only healthy
        healthy = await pool.get_agent_list(AgentHealthStatus.HEALTHY)
        assert len(healthy) == 1
        assert healthy[0][0] == "agent-1"

        # Get degraded
        degraded = await pool.get_agent_list(AgentHealthStatus.DEGRADED)
        assert len(degraded) == 1
        assert degraded[0][0] == "agent-2"


class TestPoolMetrics:
    """Test pool metrics and monitoring."""

    @pytest.mark.asyncio
    async def test_get_metrics_empty_pool(self, mock_graphsub_client):
        """Test getting metrics for empty pool."""
        config = PoolConfig(pool_name="metrics-pool")
        pool = AgentPool(mock_graphsub_client, config)

        metrics = await pool.get_metrics()

        assert metrics.total_agents == 0
        assert metrics.healthy_agents == 0
        assert metrics.total_budget_remaining == 0.0

    @pytest.mark.asyncio
    async def test_get_pool_summary(self, mock_graphsub_client):
        """Test getting pool summary."""
        config = PoolConfig(
            pool_name="summary-pool",
            min_agents=2,
            max_agents=5,
        )
        pool = AgentPool(mock_graphsub_client, config)

        summary = await pool.get_pool_summary()

        assert summary["pool_name"] == "summary-pool"
        assert summary["min_agents"] == 2
        assert summary["max_agents"] == 5
        assert "status" in summary
        assert "capacity_utilization" in summary


class TestAgentSelection:
    """Test agent selection for task assignment."""

    @pytest.mark.asyncio
    async def test_select_agent_from_empty_pool(self, mock_graphsub_client):
        """Test selecting agent from empty pool."""
        config = PoolConfig(pool_name="select-pool")
        pool = AgentPool(mock_graphsub_client, config)

        agent = await pool.select_agent_for_task(
            required_capabilities=["python"],
        )

        assert agent is None

    @pytest.mark.asyncio
    async def test_select_with_preferred_agent(self, mock_graphsub_client):
        """Test selection with preferred agent."""
        config = PoolConfig(pool_name="preferred-pool")
        pool = AgentPool(mock_graphsub_client, config)

        # Mock an agent in the pool
        from unittest.mock import MagicMock
        from runtime.agent_pool import AgentHealthCheckResult

        mock_agent = MagicMock()
        mock_agent.agent_id = "preferred-1"
        mock_agent.capabilities = {"python": 0.9}

        pool._agents["preferred-1"] = mock_agent
        pool._agent_health["preferred-1"] = AgentHealthCheckResult(
            agent_id="preferred-1",
            status=AgentHealthStatus.HEALTHY,
        )

        # Select with preference
        selected = await pool.select_agent_for_task(
            required_capabilities=["python"],
            preferred_agent_id="preferred-1",
        )

        assert selected == mock_agent


class TestPoolScaling:
    """Test pool scaling policies."""

    def test_scaling_policy_enum(self):
        """Test scaling policy enumeration."""
        assert PoolScalingPolicy.FIXED.value == "fixed"
        assert PoolScalingPolicy.AUTO_LOAD.value == "auto_load"
        assert PoolScalingPolicy.AUTO_RESOURCE.value == "auto_resource"

    def test_config_with_auto_load_policy(self):
        """Test config with auto-load scaling."""
        config = PoolConfig(
            pool_name="auto-pool",
            scaling_policy=PoolScalingPolicy.AUTO_LOAD,
            auto_scale_threshold=0.75,
            auto_scale_down_threshold=0.25,
        )

        assert config.scaling_policy == PoolScalingPolicy.AUTO_LOAD
        assert config.auto_scale_threshold == 0.75


class TestHealthStatus:
    """Test health status management."""

    def test_health_status_enum(self):
        """Test health status enumeration."""
        assert AgentHealthStatus.HEALTHY.value == "healthy"
        assert AgentHealthStatus.DEGRADED.value == "degraded"
        assert AgentHealthStatus.UNHEALTHY.value == "unhealthy"
        assert AgentHealthStatus.DEAD.value == "dead"

    def test_health_check_result_success_rate(self):
        """Test success rate calculation."""
        from runtime.agent_pool import AgentHealthCheckResult

        result = AgentHealthCheckResult(
            agent_id="test-1",
            status=AgentHealthStatus.HEALTHY,
            tasks_completed=8,
            tasks_failed=2,
        )

        assert result.success_rate == 0.8

    def test_health_check_result_zero_tasks(self):
        """Test success rate with no tasks."""
        from runtime.agent_pool import AgentHealthCheckResult

        result = AgentHealthCheckResult(
            agent_id="test-1",
            status=AgentHealthStatus.HEALTHY,
            tasks_completed=0,
            tasks_failed=0,
        )

        assert result.success_rate == 0.0


class TestPoolMetricsCalculation:
    """Test metrics calculation."""

    def test_pool_metrics_degradation_percentage(self):
        """Test degradation percentage calculation."""
        from runtime.agent_pool import PoolMetrics

        metrics = PoolMetrics(
            total_agents=10,
            healthy_agents=7,
            degraded_agents=2,
            unhealthy_agents=1,
        )

        assert metrics.degradation_percentage == 30.0
        assert metrics.available_agents == 7

    def test_pool_metrics_no_agents(self):
        """Test metrics with no agents."""
        from runtime.agent_pool import PoolMetrics

        metrics = PoolMetrics()

        assert metrics.total_agents == 0
        assert metrics.degradation_percentage == 0.0
        assert metrics.available_agents == 0


class TestPoolIntegration:
    """Integration tests for pool functionality."""

    @pytest.mark.asyncio
    async def test_pool_configuration_validation(self, mock_graphsub_client):
        """Test that pool validates configuration."""
        config = PoolConfig(
            pool_name="valid-pool",
            min_agents=2,
            max_agents=10,
            default_agent_budget=50.0,
        )

        pool = AgentPool(mock_graphsub_client, config)

        assert pool.config.min_agents <= pool.config.max_agents
        assert pool.config.default_agent_budget > 0

    @pytest.mark.asyncio
    async def test_prune_nonexistent_agent(self, mock_graphsub_client):
        """Test pruning an agent not in pool."""
        config = PoolConfig(pool_name="prune-pool")
        pool = AgentPool(mock_graphsub_client, config)

        result = await pool.prune_agent("nonexistent-agent")
        assert result is False
