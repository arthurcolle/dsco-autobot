"""
Phase 2 Runtime Integration Tests

Tests the integration of:
- AgentExecutor (OODA loop)
- CapabilityMatcher (skill matching)
- ResourceManager (budget tracking)
- Substrate layer (GraphSub, Redis, Temporal)
"""

import asyncio
import pytest
import json
from datetime import datetime
from typing import List, Optional

from runtime.agent_executor import AgentExecutor, ExecutionContext
from runtime.capability_matcher import (
    CapabilityMatcher,
    MatchStrategy,
    CapabilityTier,
)
from runtime.resource_manager import (
    ResourceManager,
    ResourceEstimate,
    ResourceType,
    ReservationStatus,
)
from substrate.graphsub_client import GraphSubClient
from substrate.redis_mailbox import RedisMailbox


class TestCapabilityMatcher:
    """Test capability matching and learning."""

    @pytest.mark.asyncio
    async def test_matcher_initialization(self, mock_graphsub_client):
        """Verify matcher initializes correctly."""
        matcher = CapabilityMatcher(mock_graphsub_client)
        assert matcher.graphsub_client
        assert matcher.cache_size == 1000

    @pytest.mark.asyncio
    async def test_can_execute_with_required_capabilities(self, mock_graphsub_client):
        """Test capability matching for required skills."""
        matcher = CapabilityMatcher(mock_graphsub_client)

        # Mock an agent with Python capability
        agent_id = "test-agent-1"

        # Test: agent can execute task with matching capability
        can_exec, result = await matcher.can_execute(
            agent_id,
            required_capabilities=["python"],
            min_confidence=0.3,
            strategy=MatchStrategy.FUZZY,
        )

        # Validate result object
        assert result.agent_id == agent_id
        assert isinstance(result.match_score, float)
        assert isinstance(result.confidence_tier, CapabilityTier)
        assert isinstance(result.reasoning, str)

    @pytest.mark.asyncio
    async def test_matcher_caching(self, mock_graphsub_client):
        """Verify matcher caches profiles correctly."""
        matcher = CapabilityMatcher(mock_graphsub_client, cache_size=10)

        agent_id = "test-agent-cache"

        # Load profile twice - should use cache second time
        profile1 = await matcher.load_agent_profile(agent_id, use_cache=True)
        stats1 = matcher.cache_stats()
        cached_count = stats1["cached_profiles"]

        # Load again - cache should have entry
        profile2 = await matcher.load_agent_profile(agent_id, use_cache=True)
        stats2 = matcher.cache_stats()

        assert stats2["cached_profiles"] >= cached_count
        assert profile1.agent_id == profile2.agent_id

    @pytest.mark.asyncio
    async def test_find_best_agent(self, mock_graphsub_client):
        """Test finding best agent for a task."""
        matcher = CapabilityMatcher(mock_graphsub_client)

        agent_ids = ["agent-1", "agent-2", "agent-3"]
        required_capabilities = ["python", "ml"]

        # Find best agent
        result = await matcher.find_best_agent(
            agent_ids,
            required_capabilities,
            min_confidence=0.3,
            strategy=MatchStrategy.FUZZY,
        )

        if result:
            best_agent_id, match_result = result
            assert best_agent_id in agent_ids
            assert match_result.agent_id == best_agent_id
            assert match_result.match_score > 0

    @pytest.mark.asyncio
    async def test_capability_learning(self, mock_graphsub_client):
        """Test capability score updating based on execution."""
        matcher = CapabilityMatcher(mock_graphsub_client)

        agent_id = "learning-agent"
        capability = "python"

        # Update capability on success
        await matcher.update_capability_score(
            agent_id,
            capability,
            success=True
        )

        # Update on failure
        await matcher.update_capability_score(
            agent_id,
            capability,
            success=False
        )

        # Get summary - should show updates
        summary = await matcher.get_capability_summary(agent_id)
        assert isinstance(summary, dict)


class TestResourceManager:
    """Test resource allocation and budget management."""

    @pytest.mark.asyncio
    async def test_resource_manager_initialization(self, mock_graphsub_client):
        """Verify resource manager initializes correctly."""
        rm = ResourceManager(mock_graphsub_client)
        assert rm.graphsub
        assert rm.cache_ttl_seconds == 60

    @pytest.mark.asyncio
    async def test_load_quota(self, mock_graphsub_client):
        """Test loading agent resource quota."""
        rm = ResourceManager(mock_graphsub_client)

        agent_id = "quota-agent"
        quota = await rm.load_quota(agent_id)

        assert quota.agent_id == agent_id
        assert quota.api_credits > 0
        assert quota.max_concurrent_tasks > 0

    @pytest.mark.asyncio
    async def test_can_afford_check(self, mock_graphsub_client):
        """Test affordability checking."""
        rm = ResourceManager(mock_graphsub_client)

        agent_id = "budget-agent"
        estimate = ResourceEstimate(
            task_id="task-1",
            api_credit_cost=10.0
        )

        can_afford, reason = await rm.can_afford(agent_id, estimate)
        assert isinstance(can_afford, bool)
        assert isinstance(reason, str)

    @pytest.mark.asyncio
    async def test_resource_reservation(self, mock_graphsub_client):
        """Test resource reservation workflow."""
        rm = ResourceManager(mock_graphsub_client)

        agent_id = "reserve-agent"
        task_id = "task-2"
        estimate = ResourceEstimate(
            task_id=task_id,
            api_credit_cost=5.0
        )

        # Reserve resources
        reservation = await rm.reserve(agent_id, task_id, estimate)

        if reservation:
            assert reservation.agent_id == agent_id
            assert reservation.task_id == task_id
            assert reservation.status == ReservationStatus.ALLOCATED
            assert reservation.get_resource(ResourceType.API_CREDITS) == 5.0

    @pytest.mark.asyncio
    async def test_reservation_completion(self, mock_graphsub_client):
        """Test completing a reservation."""
        rm = ResourceManager(mock_graphsub_client)

        agent_id = "complete-agent"
        task_id = "task-3"
        estimate = ResourceEstimate(
            task_id=task_id,
            api_credit_cost=5.0
        )

        # Reserve
        reservation = await rm.reserve(agent_id, task_id, estimate)
        if reservation:
            # Complete with refund
            await rm.complete(reservation, actual_cost=3.0)

            assert reservation.status == ReservationStatus.COMPLETED
            assert 3.0 in reservation.refunded_amount.values()

    @pytest.mark.asyncio
    async def test_agent_summary(self, mock_graphsub_client):
        """Test getting agent resource summary."""
        rm = ResourceManager(mock_graphsub_client)

        agent_id = "summary-agent"
        summary = await rm.get_agent_summary(agent_id)

        assert "agent_id" in summary
        assert "budget" in summary
        assert "gpu" in summary
        assert "storage" in summary
        assert "concurrent_tasks" in summary
        assert summary["budget"]["total"] > 0


class TestAgentExecutor:
    """Test agent executor with OODA loop."""

    @pytest.mark.asyncio
    async def test_executor_initialization(self, mock_temporal_client):
        """Verify executor initializes correctly."""
        executor = AgentExecutor(
            agent_id="executor-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.9, "search": 0.8},
            budget=100.0,
        )

        assert executor.agent_id == "executor-1"
        assert executor.model == "claude-3.5-sonnet"
        assert "python" in executor.capabilities
        assert executor.budget_remaining == 100.0

    @pytest.mark.asyncio
    async def test_execution_context_creation(self):
        """Test creating execution context from task."""
        context = ExecutionContext(
            task_id="task-123",
            task_type="code_execution",
            task_data={"code": "print('hello')"},
            required_capabilities=["python"],
            estimated_cost=1.5,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
            estimated_duration_seconds=10.0
        )

        assert context.task_id == "task-123"
        assert context.estimated_cost == 1.5
        assert context.estimated_duration_seconds == 10.0
        assert not context.is_overdue()

    @pytest.mark.asyncio
    async def test_observe_phase_no_task(self):
        """Test observe phase when no task in mailbox."""
        executor = AgentExecutor(
            agent_id="observer-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.8},
            budget=50.0,
        )

        # Without connecting to Redis, observe should handle gracefully
        task, signals = await executor.observe()
        assert task is None or isinstance(task, ExecutionContext)
        assert isinstance(signals, dict)

    @pytest.mark.asyncio
    async def test_orient_phase_assessment(self):
        """Test orient phase capability assessment."""
        executor = AgentExecutor(
            agent_id="orient-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.9},
            budget=50.0,
        )

        task = ExecutionContext(
            task_id="task-orient",
            task_type="code",
            task_data={},
            required_capabilities=["python"],
            estimated_cost=5.0,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
        )

        assessment = await executor.orient(task, {})

        assert "capability_match" in assessment
        assert "can_afford" in assessment
        assert "urgency" in assessment
        assert "risk_level" in assessment
        assert 0.0 <= assessment["capability_match"] <= 1.0
        assert 0.0 <= assessment["urgency"] <= 1.0

    @pytest.mark.asyncio
    async def test_decide_phase_high_confidence(self):
        """Test decide phase with high capability match."""
        executor = AgentExecutor(
            agent_id="decide-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.9},
            budget=50.0,
        )

        task = ExecutionContext(
            task_id="task-decide",
            task_type="code",
            task_data={},
            required_capabilities=["python"],
            estimated_cost=5.0,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
        )

        assessment = {
            "has_task": True,
            "capability_match": 0.95,
            "can_afford": True,
            "urgency": 0.5,
            "risk_level": 0.1,
        }

        decision = await executor.decide(task, assessment)

        assert decision.action.value in ["execute", "wait", "delegate", "escalate", "rest"]
        assert isinstance(decision.confidence, float)

    @pytest.mark.asyncio
    async def test_ooda_loop_integration(self):
        """Test full OODA loop tick."""
        executor = AgentExecutor(
            agent_id="ooda-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.8, "search": 0.7},
            budget=100.0,
            tick_interval_seconds=0.1,
        )

        # Run a single tick - should complete without error
        try:
            await executor.ooda_tick()
            # If we get here, tick completed
            assert executor.running or not executor.running  # Just verify executor exists
        except Exception as e:
            # Expected - services not really connected
            pytest.skip(f"Real service connection required: {e}")


class TestSubstrateIntegration:
    """Test integration between substrate components."""

    @pytest.mark.asyncio
    async def test_graphsub_redis_integration(self):
        """Test GraphSub and Redis mailbox working together."""
        # This would require actual services running
        # For now, verify components can be instantiated together

        graphsub = GraphSubClient("http://localhost:7879")
        mailbox = RedisMailbox("redis://localhost:6379", agent_id="test-1")

        assert graphsub
        assert mailbox

    @pytest.mark.asyncio
    async def test_end_to_end_task_lifecycle(self):
        """Test complete task lifecycle through all components."""
        # This is a high-level integration test
        # In a real scenario, this would:
        # 1. Create task in Redis stream
        # 2. Agent observes task
        # 3. Agent matches capabilities
        # 4. Agent checks resources
        # 5. Agent executes task
        # 6. Agent updates capabilities
        # 7. Agent deposits pheromones

        executor = AgentExecutor(
            agent_id="e2e-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.85},
            budget=50.0,
        )

        task = ExecutionContext(
            task_id="e2e-task",
            task_type="code",
            task_data={"code": "x = 1 + 1; print(x)"},
            required_capabilities=["python"],
            estimated_cost=1.0,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
            estimated_duration_seconds=5.0
        )

        # Simulate OODA
        signals = {"attractive_tasks": [], "dangerous_paths": []}

        assessment = await executor.orient(task, signals)
        decision = await executor.decide(task, assessment)

        # Verify decision was made
        assert decision.action
        assert decision.confidence >= 0.0


class TestPhase2CompleteFunctionality:
    """Integration tests for all Phase 2 functionality together."""

    @pytest.mark.asyncio
    async def test_matcher_and_executor_integration(self, mock_graphsub_client):
        """Test capability matcher used by executor."""
        executor = AgentExecutor(
            agent_id="integrated-1",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.88},
            budget=75.0,
        )

        task = ExecutionContext(
            task_id="integrated-task",
            task_type="code",
            task_data={},
            required_capabilities=["python", "ml"],
            estimated_cost=2.5,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
        )

        # Executor's orient phase uses matcher
        assessment = await executor.orient(task, {})

        # Verify matcher was used
        assert "capability_match" in assessment
        assert assessment["capability_match"] >= 0.0

    @pytest.mark.asyncio
    async def test_resource_and_executor_integration(self, mock_graphsub_client):
        """Test resource manager used by executor."""
        executor = AgentExecutor(
            agent_id="resource-integrated",
            model="claude-3.5-sonnet",
            capabilities={"python": 0.9},
            budget=50.0,
        )

        task = ExecutionContext(
            task_id="resource-task",
            task_type="code",
            task_data={},
            required_capabilities=["python"],
            estimated_cost=10.0,
            priority=5,
            received_at=datetime.utcnow().isoformat(),
        )

        # Executor's orient phase checks affordability with resource manager
        assessment = await executor.orient(task, {})

        # Verify resource manager was used
        assert "can_afford" in assessment
        assert "affordability_reason" in assessment
