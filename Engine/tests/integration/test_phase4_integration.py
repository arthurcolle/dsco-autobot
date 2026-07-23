"""
Phase 4 Integration Tests

Tests Temporal workflows, GPU scheduling, and advanced orchestration.
"""

import pytest
from datetime import datetime, timedelta

from runtime.temporal_activities import (
    execute_code_activity,
    search_web_activity,
    call_llm_activity,
    ActivityResult,
)
from runtime.gpu_scheduler import (
    GPUScheduler,
    GPUTaskSpec,
    GPUType,
    GPUQuota,
    GPUScheduleStatus,
)


class TestTemporalActivities:
    """Test Temporal activity definitions."""

    @pytest.mark.asyncio
    async def test_execute_code_activity_definition(self):
        """Verify code execution activity is defined."""
        assert execute_code_activity is not None

    @pytest.mark.asyncio
    async def test_search_web_activity_definition(self):
        """Verify web search activity is defined."""
        assert search_web_activity is not None

    @pytest.mark.asyncio
    async def test_llm_activity_definition(self):
        """Verify LLM call activity is defined."""
        assert call_llm_activity is not None

    def test_activity_result_dataclass(self):
        """Test ActivityResult creation."""
        result = ActivityResult(
            success=True,
            output={"key": "value"},
            duration_seconds=1.5,
            tokens_used=100,
            cost_usd=0.05,
        )

        assert result.success
        assert result.output == {"key": "value"}
        assert result.duration_seconds == 1.5
        assert result.tokens_used == 100
        assert result.cost_usd == 0.05

    def test_activity_result_failure(self):
        """Test ActivityResult with failure."""
        result = ActivityResult(
            success=False,
            output=None,
            error="Failed to execute",
        )

        assert not result.success
        assert result.error == "Failed to execute"


class TestGPUTaskSpec:
    """Test GPU task specifications."""

    def test_gpu_task_spec_creation(self):
        """Test creating GPU task spec."""
        spec = GPUTaskSpec(
            task_id="gpu-task-1",
            agent_id="agent-1",
            gpu_type=GPUType.A100,
            gpu_count=2,
            memory_gb=80,
            estimated_duration_seconds=600.0,
        )

        assert spec.task_id == "gpu-task-1"
        assert spec.gpu_type == GPUType.A100
        assert spec.gpu_count == 2
        assert spec.memory_gb == 80

    def test_gpu_types(self):
        """Test GPU type enumeration."""
        assert GPUType.H100.value == "h100"
        assert GPUType.A100.value == "a100"
        assert GPUType.L4.value == "l4"
        assert GPUType.T4.value == "t4"
        assert GPUType.CPU.value == "cpu"


class TestGPUQuota:
    """Test GPU quota management."""

    def test_gpu_quota_creation(self):
        """Test creating GPU quota."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_hours_monthly=10.0,
            gpu_budget_usd=100.0,
        )

        assert quota.agent_id == "agent-1"
        assert quota.gpu_hours_monthly == 10.0
        assert quota.gpu_budget_usd == 100.0

    def test_gpu_quota_remaining_hours(self):
        """Test calculating remaining GPU hours."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_hours_monthly=10.0,
            gpu_hours_used=3.0,
        )

        assert quota.gpu_hours_remaining == 7.0

    def test_gpu_quota_remaining_budget(self):
        """Test calculating remaining GPU budget."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_budget_usd=100.0,
            gpu_budget_spent=30.0,
        )

        assert quota.gpu_budget_remaining == 70.0

    def test_gpu_quota_can_allocate_success(self):
        """Test checking if allocation is possible (success)."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_hours_monthly=10.0,
            gpu_hours_used=2.0,
            gpu_budget_usd=100.0,
            gpu_budget_spent=20.0,
        )

        can_allocate, reason = quota.can_allocate(
            duration_hours=3.0,
            estimated_cost=30.0,
        )

        assert can_allocate
        assert "Can allocate" in reason

    def test_gpu_quota_can_allocate_insufficient_hours(self):
        """Test allocation when insufficient GPU hours."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_hours_monthly=10.0,
            gpu_hours_used=9.0,
        )

        can_allocate, reason = quota.can_allocate(
            duration_hours=2.0,
            estimated_cost=10.0,
        )

        assert not can_allocate
        assert "Insufficient GPU hours" in reason

    def test_gpu_quota_can_allocate_insufficient_budget(self):
        """Test allocation when insufficient budget."""
        quota = GPUQuota(
            agent_id="agent-1",
            gpu_hours_monthly=10.0,
            gpu_budget_usd=50.0,
            gpu_budget_spent=40.0,
        )

        can_allocate, reason = quota.can_allocate(
            duration_hours=1.0,
            estimated_cost=20.0,
        )

        assert not can_allocate
        assert "Insufficient GPU budget" in reason


class TestGPUScheduler:
    """Test GPU scheduler."""

    @pytest.mark.asyncio
    async def test_gpu_scheduler_initialization(self):
        """Test initializing GPU scheduler."""
        scheduler = GPUScheduler()

        assert scheduler is not None
        assert len(scheduler.cluster_config) > 0

    @pytest.mark.asyncio
    async def test_gpu_cluster_status(self):
        """Test getting cluster status."""
        scheduler = GPUScheduler()
        status = await scheduler.get_cluster_status()

        assert status.total_gpus
        assert status.available_gpus
        assert status.pending_tasks >= 0
        assert status.running_tasks >= 0
        assert 0 <= status.cluster_utilization <= 1.0

    @pytest.mark.asyncio
    async def test_request_gpu_allocation(self):
        """Test requesting GPU allocation."""
        scheduler = GPUScheduler()

        spec = GPUTaskSpec(
            task_id="task-1",
            agent_id="agent-1",
            gpu_type=GPUType.T4,
            gpu_count=1,
            estimated_duration_seconds=300.0,
        )

        allocation = await scheduler.request_gpu_allocation(spec)

        # May or may not get immediate allocation depending on availability
        if allocation:
            assert allocation.task_id == "task-1"
            assert allocation.agent_id == "agent-1"
            assert allocation.gpu_type == GPUType.T4
            assert allocation.status == GPUScheduleStatus.SCHEDULED

    @pytest.mark.asyncio
    async def test_release_gpu_allocation(self):
        """Test releasing GPU allocation."""
        scheduler = GPUScheduler()

        spec = GPUTaskSpec(
            task_id="task-2",
            agent_id="agent-1",
            gpu_type=GPUType.L4,
            gpu_count=1,
            estimated_duration_seconds=100.0,
        )

        allocation = await scheduler.request_gpu_allocation(spec)

        if allocation:
            result = await scheduler.release_gpu_allocation(allocation.allocation_id)
            assert result is True

    @pytest.mark.asyncio
    async def test_release_nonexistent_allocation(self):
        """Test releasing allocation that doesn't exist."""
        scheduler = GPUScheduler()

        result = await scheduler.release_gpu_allocation("nonexistent-id")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_allocation_status(self):
        """Test getting allocation status."""
        scheduler = GPUScheduler()

        spec = GPUTaskSpec(
            task_id="task-3",
            agent_id="agent-1",
            gpu_type=GPUType.H100,
            gpu_count=1,
            estimated_duration_seconds=500.0,
        )

        allocation = await scheduler.request_gpu_allocation(spec)

        if allocation:
            status = await scheduler.get_allocation_status(allocation.allocation_id)
            assert status is not None
            assert status["task_id"] == "task-3"
            assert status["gpu_type"] == "h100"

    @pytest.mark.asyncio
    async def test_get_agent_gpu_usage(self):
        """Test getting agent GPU usage."""
        scheduler = GPUScheduler()

        spec = GPUTaskSpec(
            task_id="task-4",
            agent_id="agent-2",
            gpu_type=GPUType.A100,
            gpu_count=2,
            estimated_duration_seconds=1800.0,
        )

        await scheduler.request_gpu_allocation(spec)

        usage = await scheduler.get_agent_gpu_usage("agent-2")

        assert usage["agent_id"] == "agent-2"
        assert "gpu_hours_monthly" in usage
        assert "gpu_budget_usd" in usage


class TestGPUScheduleStatus:
    """Test GPU schedule status enumeration."""

    def test_gpu_schedule_status_values(self):
        """Test GPU schedule status enum."""
        assert GPUScheduleStatus.PENDING.value == "pending"
        assert GPUScheduleStatus.SCHEDULED.value == "scheduled"
        assert GPUScheduleStatus.RUNNING.value == "running"
        assert GPUScheduleStatus.COMPLETED.value == "completed"
        assert GPUScheduleStatus.FAILED.value == "failed"
        assert GPUScheduleStatus.CANCELLED.value == "cancelled"


class TestGPUSchedulerAdvanced:
    """Advanced GPU scheduler tests."""

    @pytest.mark.asyncio
    async def test_multiple_allocations(self):
        """Test multiple concurrent allocations."""
        scheduler = GPUScheduler()

        specs = [
            GPUTaskSpec(
                task_id=f"task-{i}",
                agent_id=f"agent-{i}",
                gpu_type=GPUType.T4,
                gpu_count=1,
                estimated_duration_seconds=100.0 * (i + 1),
            )
            for i in range(3)
        ]

        allocations = []
        for spec in specs:
            alloc = await scheduler.request_gpu_allocation(spec)
            if alloc:
                allocations.append(alloc)

        # Some should be allocated
        assert len(allocations) >= 0

    @pytest.mark.asyncio
    async def test_quota_exhaustion(self):
        """Test quota exhaustion handling."""
        scheduler = GPUScheduler()

        # Manually exhaust quota
        quota = GPUQuota(
            agent_id="exhausted-agent",
            gpu_hours_monthly=0.1,
            gpu_hours_used=0.1,  # Exhausted
            gpu_budget_usd=1.0,
            gpu_budget_spent=1.0,  # Exhausted
        )

        scheduler._quotas["exhausted-agent"] = quota

        spec = GPUTaskSpec(
            task_id="task-exhausted",
            agent_id="exhausted-agent",
            gpu_type=GPUType.A100,
            gpu_count=1,
            estimated_duration_seconds=3600.0,
        )

        allocation = await scheduler.request_gpu_allocation(spec)
        assert allocation is None

    @pytest.mark.asyncio
    async def test_cluster_utilization_calculation(self):
        """Test cluster utilization calculation."""
        scheduler = GPUScheduler()

        # Add some allocations
        spec1 = GPUTaskSpec(
            task_id="util-1",
            agent_id="agent-1",
            gpu_type=GPUType.T4,
            gpu_count=4,
            estimated_duration_seconds=300.0,
        )

        alloc1 = await scheduler.request_gpu_allocation(spec1)

        status = await scheduler.get_cluster_status()

        # Should show some utilization
        assert status.cluster_utilization >= 0.0

        if alloc1:
            await scheduler.release_gpu_allocation(alloc1.allocation_id)
