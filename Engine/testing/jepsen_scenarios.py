"""
Jepsen-Inspired Chaos Test Scenarios

Tests based on Kyle Kingsbury's findings about distributed system failures:
- Lazy fsync → data loss on power failure
- Single node crash + network delay → split-brain
- File corruption → cascading loss
- Async writes → acknowledged data lost
"""

import asyncio
import logging
from typing import Callable, Dict, List, Optional

from .chaos_framework import (
    ChaosScenario,
    ChaosTestFramework,
    FailureInjection,
    FailureType,
    FailureTarget,
    ScenarioBuilder,
)

logger = logging.getLogger(__name__)


class JepsenScenarios:
    """Jepsen-style chaos test scenarios."""

    @staticmethod
    def redis_lazy_fsync_power_failure() -> ChaosScenario:
        """
        Test: Redis with lazy fsync loses data on power failure.

        Scenario:
        1. Agent publishes task to Redis
        2. Agent crashes immediately (power loss)
        3. Redis has not fsynced to disk
        4. Data is lost

        Mitigation: appendfsync:always configured
        """
        return (
            ScenarioBuilder(
                "Redis Lazy Fsync - Power Failure (Jepsen #7564)"
            )
            .with_description(
                "Verify Redis data persists even with sudden power loss. "
                "Should use appendfsync:always to prevent data loss."
            )
            .with_duration(30.0)
            .add_failure(
                FailureType.RESOURCE_EXHAUSTION,
                FailureTarget.REDIS,
                target_id="redis-primary",
                duration_seconds=0.1,  # Immediate crash
                start_offset=2.0,
                severity=1.0,
            )
            .build()
        )

    @staticmethod
    def single_node_crash_network_delay_split_brain() -> ChaosScenario:
        """
        Test: Single node crash + network delay causes split-brain.

        Scenario:
        1. Agent A on primary node performing task
        2. Primary node crashes
        3. Network delay prevents failover detection
        4. Agent thinks it's still leader
        5. Agent B becomes leader elsewhere
        6. Conflicting decisions made

        Mitigation: Temporal checkpoints before acking work
        """
        return (
            ScenarioBuilder(
                "Single Node Crash + Network Delay - Split Brain (Jepsen #7567)"
            )
            .with_description(
                "Verify split-brain is prevented by consensus. "
                "Requires quorum before accepting updates."
            )
            .with_duration(45.0)
            .add_failure(
                FailureType.SERVICE_CRASH,
                FailureTarget.TEMPORAL,
                target_id="temporal-node-1",
                duration_seconds=15.0,
                start_offset=5.0,
                severity=1.0,
            )
            .add_failure(
                FailureType.SLOW_NETWORK,
                FailureTarget.NETWORK,
                target_id="node-1-to-node-2",
                duration_seconds=10.0,
                start_offset=6.0,
                severity=0.8,
            )
            .build()
        )

    @staticmethod
    def file_corruption_cascading_loss() -> ChaosScenario:
        """
        Test: File corruption on one node cascades to cluster loss.

        Scenario:
        1. GraphSub node has bit flip in pheromone data
        2. Data is replicated to other nodes
        3. Corrupted state spreads
        4. Entire cluster state is corrupted

        Mitigation: Checksums on all files
        """
        return (
            ScenarioBuilder(
                "File Corruption - Cascading Loss (Jepsen #7549, #7556)"
            )
            .with_description(
                "Verify file corruption doesn't spread. "
                "Checksums should detect and isolate corruption."
            )
            .with_duration(40.0)
            .add_failure(
                FailureType.DATA_CORRUPTION,
                FailureTarget.GRAPHSUB,
                target_id="graphsub-shard-1",
                duration_seconds=20.0,
                start_offset=3.0,
                severity=0.5,  # Bit flip (not total loss)
            )
            .build()
        )

    @staticmethod
    def async_writes_acknowledged_data_lost() -> ChaosScenario:
        """
        Test: Async writes acknowledged before durability causes loss.

        Scenario:
        1. Agent publishes task, gets ACK from Redis
        2. Data is in Redis memory buffer, not yet fsynced
        3. Redis crashes before fsync
        4. Agent thinks task was submitted, but it's lost

        Mitigation: Durability-before-ack model
        """
        return (
            ScenarioBuilder(
                "Async Writes - Acknowledged Data Lost (Jepsen #7561)"
            )
            .with_description(
                "Verify acknowledged writes are durable. "
                "Must fsync before returning ACK."
            )
            .with_duration(30.0)
            .add_failure(
                FailureType.SERVICE_CRASH,
                FailureTarget.REDIS,
                target_id="redis",
                duration_seconds=0.05,  # Brief crash
                start_offset=2.0,
                severity=1.0,
            )
            .build()
        )

    @staticmethod
    def concurrent_writes_race_condition() -> ChaosScenario:
        """
        Test: Concurrent writes from multiple agents to same capability.

        Scenario:
        1. Agent A and B both update capability score for 'python'
        2. Write A: confidence 0.8
        3. Write B: confidence 0.9
        4. Both writes reach GraphSub simultaneously
        5. Which one wins?

        Mitigation: CRDT merging or versioning
        """
        return (
            ScenarioBuilder(
                "Concurrent Writes - Race Condition"
            )
            .with_description(
                "Verify concurrent updates converge correctly. "
                "Last-writer-wins or merge semantics should apply."
            )
            .with_duration(20.0)
            .add_failure(
                FailureType.CONCURRENT_WRITE,
                FailureTarget.GRAPHSUB,
                target_id="capability-python",
                duration_seconds=5.0,
                start_offset=2.0,
                severity=1.0,
            )
            .build()
        )

    @staticmethod
    def byzantine_agent_false_pheromones() -> ChaosScenario:
        """
        Test: Byzantine (lying) agent deposits false pheromones.

        Scenario:
        1. Agent A deposits 'success' pheromone for dangerous task
        2. Other agents follow pheromone and fail
        3. False signal caused widespread failure

        Mitigation: Signed pheromones, agent reputation
        """
        return (
            ScenarioBuilder(
                "Byzantine Agent - False Pheromones"
            )
            .with_description(
                "Verify false signals don't cause system-wide failure. "
                "Pheromones should be signed and reputation-tracked."
            )
            .with_duration(30.0)
            .add_failure(
                FailureType.BYZANTINE,
                FailureTarget.AGENT,
                target_id="agent-1",
                duration_seconds=10.0,
                start_offset=2.0,
                severity=0.5,
            )
            .build()
        )

    @staticmethod
    def memory_exhaustion_slow_recovery() -> ChaosScenario:
        """
        Test: Memory exhaustion causes slowdown and recovery time.

        Scenario:
        1. GraphSub accumulates pheromones (no sweep)
        2. Memory usage increases
        3. Performance degrades
        4. Eventually OOM
        5. Recovery time from restart

        Mitigation: Pheromone TTL sweeping
        """
        return (
            ScenarioBuilder(
                "Memory Exhaustion - Slow Recovery"
            )
            .with_description(
                "Verify system doesn't OOM from stale pheromones. "
                "TTL-based sweeping should prevent memory growth."
            )
            .with_duration(60.0)
            .add_failure(
                FailureType.RESOURCE_EXHAUSTION,
                FailureTarget.GRAPHSUB,
                target_id="graphsub-memory",
                duration_seconds=30.0,
                start_offset=5.0,
                severity=0.9,
            )
            .build()
        )

    @staticmethod
    def temporal_workflow_timeout_recovery() -> ChaosScenario:
        """
        Test: Task timeout during Temporal workflow and recovery.

        Scenario:
        1. Agent starts task via Temporal workflow
        2. Network partition isolates activity
        3. Workflow waits for heartbeat timeout (~30s)
        4. Activity retried on different agent
        5. Budget refunded after failure

        Mitigation: Timeouts + retry logic + refunds
        """
        return (
            ScenarioBuilder(
                "Temporal Workflow Timeout - Recovery"
            )
            .with_description(
                "Verify task timeout doesn't lose budget. "
                "Failed tasks should refund resources."
            )
            .with_duration(50.0)
            .add_failure(
                FailureType.NETWORK_PARTITION,
                FailureTarget.AGENT,
                target_id="agent-1",
                duration_seconds=5.0,
                start_offset=5.0,
                severity=1.0,
            )
            .build()
        )

    @staticmethod
    def clock_skew_pheromone_decay() -> ChaosScenario:
        """
        Test: Clock skew affects pheromone decay calculations.

        Scenario:
        1. System clock jumps backward by 10 seconds
        2. Pheromones appear to be in the future
        3. Decay calculation goes negative
        4. False pheromone intensity

        Mitigation: Monotonic clocks or version vectors
        """
        return (
            ScenarioBuilder(
                "Clock Skew - Pheromone Decay"
            )
            .with_description(
                "Verify clock skew doesn't break pheromone decay. "
                "Use monotonic clocks or version vectors."
            )
            .with_duration(30.0)
            .add_failure(
                FailureType.CLOCK_SKEW,
                FailureTarget.AGENT,
                target_id="agent-1",
                duration_seconds=15.0,
                start_offset=3.0,
                severity=0.8,
            )
            .build()
        )


async def run_jepsen_test_suite(
    workload_fn: Callable,
    scenario_filter: Optional[List[str]] = None,
) -> Dict[str, Dict]:
    """
    Run full Jepsen test suite.

    Args:
        workload_fn: Async function generating system load
        scenario_filter: Only run scenarios matching these names

    Returns:
        Dict with results for each scenario
    """
    framework = ChaosTestFramework()

    scenarios = [
        JepsenScenarios.redis_lazy_fsync_power_failure(),
        JepsenScenarios.single_node_crash_network_delay_split_brain(),
        JepsenScenarios.file_corruption_cascading_loss(),
        JepsenScenarios.async_writes_acknowledged_data_lost(),
        JepsenScenarios.concurrent_writes_race_condition(),
        JepsenScenarios.byzantine_agent_false_pheromones(),
        JepsenScenarios.memory_exhaustion_slow_recovery(),
        JepsenScenarios.temporal_workflow_timeout_recovery(),
        JepsenScenarios.clock_skew_pheromone_decay(),
    ]

    # Filter scenarios if requested
    if scenario_filter:
        scenarios = [
            s for s in scenarios
            if any(f.lower() in s.name.lower() for f in scenario_filter)
        ]

    results = {}

    logger.info(f"Running {len(scenarios)} Jepsen scenarios")
    logger.info("=" * 70)

    for scenario in scenarios:
        try:
            logger.info(f"\n▶ {scenario.name}")
            logger.info(f"  {scenario.description}")

            result = await framework.run_scenario(scenario, workload_fn)

            results[scenario.name] = {
                "success": result.success,
                "duration": result.total_duration_seconds,
                "failures": result.failures_injected,
                "errors": result.errors,
                "data_loss": result.data_loss_detected,
                "state_divergence": result.state_divergence_detected,
                "observations": result.observations,
            }

            status = "✅ PASS" if result.success else "❌ FAIL"
            logger.info(f"  {status} ({result.total_duration_seconds:.1f}s)")

            if result.errors:
                for error in result.errors:
                    logger.error(f"    Error: {error}")

        except Exception as e:
            logger.error(f"❌ {scenario.name} - Exception: {e}")
            results[scenario.name] = {
                "success": False,
                "error": str(e),
            }

    # Summary
    logger.info("\n" + "=" * 70)
    passed = sum(1 for r in results.values() if r.get("success"))
    failed = len(results) - passed
    logger.info(f"Results: {passed} passed, {failed} failed")

    return results
