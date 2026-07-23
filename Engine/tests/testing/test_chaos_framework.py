"""
Chaos Testing Framework Tests

Tests the chaos framework itself for correctness.
"""

import pytest
import asyncio
from datetime import datetime

from testing.chaos_framework import (
    ChaosScenario,
    ChaosTestFramework,
    FailureInjection,
    FailureType,
    FailureTarget,
    ScenarioBuilder,
    ChaosResult,
)
from testing.jepsen_scenarios import JepsenScenarios


class TestFailureInjection:
    """Test failure injection data structures."""

    def test_failure_injection_creation(self):
        """Test creating failure injection."""
        failure = FailureInjection(
            failure_type=FailureType.NETWORK_PARTITION,
            target=FailureTarget.AGENT,
            target_id="agent-1",
            duration_seconds=5.0,
        )

        assert failure.failure_type == FailureType.NETWORK_PARTITION
        assert failure.target == FailureTarget.AGENT
        assert failure.target_id == "agent-1"
        assert failure.duration_seconds == 5.0

    def test_failure_type_enum(self):
        """Test failure type enumeration."""
        assert FailureType.NETWORK_PARTITION.value == "network_partition"
        assert FailureType.SERVICE_CRASH.value == "service_crash"
        assert FailureType.SLOW_NETWORK.value == "slow_network"
        assert FailureType.DATA_CORRUPTION.value == "data_corruption"

    def test_failure_target_enum(self):
        """Test failure target enumeration."""
        assert FailureTarget.AGENT.value == "agent"
        assert FailureTarget.TEMPORAL.value == "temporal"
        assert FailureTarget.REDIS.value == "redis"
        assert FailureTarget.GRAPHSUB.value == "graphsub"


class TestScenarioBuilder:
    """Test chaos scenario builder."""

    def test_builder_basic(self):
        """Test basic scenario building."""
        scenario = (
            ScenarioBuilder("test-scenario")
            .with_description("Test scenario")
            .add_failure(FailureType.NETWORK_PARTITION, FailureTarget.AGENT)
            .build()
        )

        assert scenario.name == "test-scenario"
        assert scenario.description == "Test scenario"
        assert len(scenario.failures) == 1

    def test_builder_multiple_failures(self):
        """Test building scenario with multiple failures."""
        scenario = (
            ScenarioBuilder("multi-failure")
            .with_description("Multiple failures")
            .add_failure(FailureType.SERVICE_CRASH, FailureTarget.REDIS)
            .add_failure(FailureType.SLOW_NETWORK, FailureTarget.TEMPORAL)
            .add_failure(FailureType.DATA_CORRUPTION, FailureTarget.GRAPHSUB)
            .with_duration(60.0)
            .build()
        )

        assert len(scenario.failures) == 3
        assert scenario.duration_seconds == 60.0

    def test_builder_method_chaining(self):
        """Test builder method chaining."""
        scenario = (
            ScenarioBuilder("chain-test")
            .with_description("Chaining test")
            .with_duration(45.0)
            .add_failure(FailureType.NETWORK_PARTITION, FailureTarget.AGENT, "agent-1")
            .add_failure(FailureType.SLOW_NETWORK, FailureTarget.NETWORK, "n1-to-n2")
            .build()
        )

        assert len(scenario.failures) == 2
        assert scenario.failures[0].target_id == "agent-1"
        assert scenario.failures[1].target_id == "n1-to-n2"


class TestChaosFramework:
    """Test chaos testing framework."""

    def test_framework_initialization(self):
        """Test framework initialization."""
        framework = ChaosTestFramework()

        assert len(framework.failures_active) == 0
        assert len(framework.failure_history) == 0
        assert len(framework.observations) == 0

    def test_should_fail_check(self):
        """Test failure checking."""
        framework = ChaosTestFramework()

        # Add active failure
        failure = FailureInjection(
            failure_type=FailureType.NETWORK_PARTITION,
            target=FailureTarget.AGENT,
            target_id="agent-1",
        )

        framework.failures_active["network_partition:agent-1"] = failure

        # Check if failure is active
        assert framework.should_fail(FailureType.NETWORK_PARTITION, "agent-1")
        assert not framework.should_fail(FailureType.SERVICE_CRASH, "agent-1")

    def test_get_active_failures(self):
        """Test getting active failures."""
        framework = ChaosTestFramework()

        failure1 = FailureInjection(
            failure_type=FailureType.NETWORK_PARTITION,
            target=FailureTarget.AGENT,
            target_id="agent-1",
        )

        failure2 = FailureInjection(
            failure_type=FailureType.SLOW_NETWORK,
            target=FailureTarget.NETWORK,
            target_id="net-1",
        )

        framework.failures_active["failure-1"] = failure1
        framework.failures_active["failure-2"] = failure2

        active = framework.get_active_failures()

        assert len(active) == 2
        assert "failure-1" in active
        assert "failure-2" in active

    @pytest.mark.asyncio
    async def test_run_scenario_basic(self):
        """Test running basic scenario."""
        framework = ChaosTestFramework()

        async def simple_workload():
            """Simple workload that completes quickly."""
            await asyncio.sleep(0.1)
            return {"completed": True}

        scenario = (
            ScenarioBuilder("quick-test")
            .with_description("Quick test")
            .with_duration(1.0)
            .add_failure(FailureType.NETWORK_PARTITION, FailureTarget.AGENT, "agent-1", 0.1)
            .build()
        )

        result = await framework.run_scenario(scenario, simple_workload)

        assert result.scenario_name == "quick-test"
        assert result.failures_injected == 1


class TestJepsenScenarios:
    """Test Jepsen scenario definitions."""

    def test_redis_lazy_fsync_scenario(self):
        """Test Redis lazy fsync scenario."""
        scenario = JepsenScenarios.redis_lazy_fsync_power_failure()

        assert "Lazy Fsync" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].target == FailureTarget.REDIS

    def test_split_brain_scenario(self):
        """Test split-brain scenario."""
        scenario = JepsenScenarios.single_node_crash_network_delay_split_brain()

        assert "Split Brain" in scenario.name
        assert len(scenario.failures) == 2  # Crash + network delay

    def test_file_corruption_scenario(self):
        """Test file corruption scenario."""
        scenario = JepsenScenarios.file_corruption_cascading_loss()

        assert "File Corruption" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].failure_type == FailureType.DATA_CORRUPTION

    def test_async_writes_scenario(self):
        """Test async writes scenario."""
        scenario = JepsenScenarios.async_writes_acknowledged_data_lost()

        assert "Async Writes" in scenario.name
        assert len(scenario.failures) == 1

    def test_concurrent_writes_scenario(self):
        """Test concurrent writes scenario."""
        scenario = JepsenScenarios.concurrent_writes_race_condition()

        assert "Concurrent Writes" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].failure_type == FailureType.CONCURRENT_WRITE

    def test_byzantine_scenario(self):
        """Test Byzantine agent scenario."""
        scenario = JepsenScenarios.byzantine_agent_false_pheromones()

        assert "Byzantine" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].failure_type == FailureType.BYZANTINE

    def test_memory_exhaustion_scenario(self):
        """Test memory exhaustion scenario."""
        scenario = JepsenScenarios.memory_exhaustion_slow_recovery()

        assert "Memory Exhaustion" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].failure_type == FailureType.RESOURCE_EXHAUSTION

    def test_timeout_recovery_scenario(self):
        """Test timeout and recovery scenario."""
        scenario = JepsenScenarios.temporal_workflow_timeout_recovery()

        assert "Timeout" in scenario.name
        assert len(scenario.failures) == 1

    def test_clock_skew_scenario(self):
        """Test clock skew scenario."""
        scenario = JepsenScenarios.clock_skew_pheromone_decay()

        assert "Clock Skew" in scenario.name
        assert len(scenario.failures) == 1
        assert scenario.failures[0].failure_type == FailureType.CLOCK_SKEW


class TestChaosResult:
    """Test chaos result data structure."""

    def test_chaos_result_creation(self):
        """Test creating chaos result."""
        result = ChaosResult(
            scenario_name="test",
            success=True,
            total_duration_seconds=10.0,
            failures_injected=1,
            errors=[],
            recovery_time_seconds={"redis": 0.5},
            data_loss_detected=False,
            state_divergence_detected=False,
            observations=["Test observation"],
        )

        assert result.scenario_name == "test"
        assert result.success
        assert result.failures_injected == 1
        assert not result.data_loss_detected

    def test_chaos_result_with_errors(self):
        """Test chaos result with errors."""
        result = ChaosResult(
            scenario_name="test-fail",
            success=False,
            total_duration_seconds=5.0,
            failures_injected=1,
            errors=["Error 1", "Error 2"],
            recovery_time_seconds={},
            data_loss_detected=True,
            state_divergence_detected=True,
            observations=[],
        )

        assert not result.success
        assert len(result.errors) == 2
        assert result.data_loss_detected


class TestFailureScenarios:
    """Test various failure scenarios."""

    def test_network_partition_scenario(self):
        """Test network partition scenario."""
        scenario = (
            ScenarioBuilder("network-partition")
            .add_failure(
                FailureType.NETWORK_PARTITION,
                FailureTarget.AGENT,
                "agent-1",
                duration_seconds=3.0,
                start_offset=1.0,
            )
            .build()
        )

        assert scenario.failures[0].failure_type == FailureType.NETWORK_PARTITION
        assert scenario.failures[0].duration_seconds == 3.0
        assert scenario.failures[0].start_time_offset_seconds == 1.0

    def test_slow_network_scenario(self):
        """Test slow network scenario."""
        scenario = (
            ScenarioBuilder("slow-network")
            .add_failure(
                FailureType.SLOW_NETWORK,
                FailureTarget.NETWORK,
                "link-1",
                duration_seconds=10.0,
            )
            .build()
        )

        assert scenario.failures[0].failure_type == FailureType.SLOW_NETWORK

    def test_mixed_failure_scenario(self):
        """Test scenario with mixed failure types."""
        scenario = (
            ScenarioBuilder("mixed-failures")
            .add_failure(FailureType.SERVICE_CRASH, FailureTarget.REDIS)
            .add_failure(FailureType.SLOW_NETWORK, FailureTarget.TEMPORAL)
            .add_failure(FailureType.DATA_CORRUPTION, FailureTarget.GRAPHSUB)
            .build()
        )

        assert len(scenario.failures) == 3
        failure_types = [f.failure_type for f in scenario.failures]
        assert FailureType.SERVICE_CRASH in failure_types
        assert FailureType.SLOW_NETWORK in failure_types
        assert FailureType.DATA_CORRUPTION in failure_types
