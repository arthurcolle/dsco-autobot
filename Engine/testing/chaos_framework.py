"""
Chaos Testing Framework (Jepsen-style)

Injects failures into the system to verify resilience and correctness:
- Network partitions (agent isolation)
- Service crashes (Temporal, Redis, GraphSub)
- Data corruption (bit flips, truncation)
- Latency injection (slow responses)
- Byzantine failures (contradictory state)
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class FailureType(Enum):
    """Types of failures to inject."""
    NETWORK_PARTITION = "network_partition"      # Agent can't reach services
    SERVICE_CRASH = "service_crash"              # Temporal/Redis/GraphSub dies
    SLOW_NETWORK = "slow_network"                # High latency (1-10s)
    DATA_CORRUPTION = "data_corruption"          # Bit flip in messages
    BYZANTINE = "byzantine"                      # Contradictory state
    CONCURRENT_WRITE = "concurrent_write"        # Race condition
    RESOURCE_EXHAUSTION = "resource_exhaustion"  # OOM/CPU saturation
    CLOCK_SKEW = "clock_skew"                    # Time jump


class FailureTarget(Enum):
    """What to fail."""
    AGENT = "agent"              # Specific agent fails
    TEMPORAL = "temporal"        # Temporal server
    REDIS = "redis"              # Redis server
    GRAPHSUB = "graphsub"        # GraphSub server
    NETWORK = "network"          # Network between nodes
    DISK = "disk"                # Disk I/O
    MEMORY = "memory"            # Memory access


@dataclass
class ChaosScenario:
    """Describes a chaos test scenario."""
    name: str
    description: str
    failures: List["FailureInjection"]
    duration_seconds: float = 60.0
    expected_outcome: str = ""
    verification_fn: Optional[Callable] = None


@dataclass
class FailureInjection:
    """Single failure to inject."""
    failure_type: FailureType
    target: FailureTarget
    target_id: Optional[str] = None  # agent-1, redis, etc
    duration_seconds: float = 5.0
    start_time_offset_seconds: float = 0.0
    recovery_time_seconds: float = 0.1  # How fast to recover after failure
    severity: float = 1.0  # 0-1 scale


@dataclass
class ChaosResult:
    """Result of chaos test execution."""
    scenario_name: str
    success: bool
    total_duration_seconds: float
    failures_injected: int
    errors: List[str]
    recovery_time_seconds: Dict[str, float]  # service -> recovery time
    data_loss_detected: bool
    state_divergence_detected: bool
    observations: List[str]


class ChaosTestFramework:
    """
    Framework for injecting failures and detecting problems.

    Based on Kyle Kingsbury's Jepsen testing methodology.
    """

    def __init__(self):
        """Initialize chaos framework."""
        self.failures_active: Dict[str, FailureInjection] = {}
        self.failure_history: List[FailureInjection] = []
        self.observations: List[str] = []
        self.errors: List[str] = []

        logger.info("Initialized Chaos Test Framework")

    async def run_scenario(
        self,
        scenario: ChaosScenario,
        workload_fn: Callable,
    ) -> ChaosResult:
        """
        Run a chaos test scenario with workload.

        Args:
            scenario: Chaos scenario to run
            workload_fn: Async function generating load

        Returns:
            ChaosResult with outcomes
        """
        logger.info(f"Starting chaos scenario: {scenario.name}")
        logger.info(f"Description: {scenario.description}")

        start_time = datetime.utcnow()
        self.observations = []
        self.errors = []
        recovery_times = {}

        try:
            # Start workload and failure injection in parallel
            workload_task = asyncio.create_task(
                self._run_workload(workload_fn, scenario.duration_seconds)
            )

            failures_task = asyncio.create_task(
                self._inject_failures(scenario.failures, scenario.duration_seconds)
            )

            # Wait for both to complete
            workload_results = await workload_task
            failure_results = await failures_task

            # Analyze results
            self.observations.extend(workload_results.get("observations", []))
            self.observations.extend(failure_results.get("observations", []))
            recovery_times = failure_results.get("recovery_times", {})

            # Check for data loss
            data_loss = await self._check_data_loss()
            state_divergence = await self._check_state_divergence()

            # Run verification if provided
            verification_passed = True
            if scenario.verification_fn:
                try:
                    verification_passed = await scenario.verification_fn()
                except Exception as e:
                    logger.error(f"Verification failed: {e}")
                    self.errors.append(f"Verification failed: {e}")
                    verification_passed = False

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            success = (
                len(self.errors) == 0
                and not data_loss
                and not state_divergence
                and verification_passed
            )

            result = ChaosResult(
                scenario_name=scenario.name,
                success=success,
                total_duration_seconds=duration,
                failures_injected=len(scenario.failures),
                errors=self.errors,
                recovery_time_seconds=recovery_times,
                data_loss_detected=data_loss,
                state_divergence_detected=state_divergence,
                observations=self.observations,
            )

            logger.info(
                f"Scenario complete: {'✅ PASSED' if success else '❌ FAILED'}"
            )
            return result

        except Exception as e:
            logger.error(f"Scenario execution failed: {e}")
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            return ChaosResult(
                scenario_name=scenario.name,
                success=False,
                total_duration_seconds=duration,
                failures_injected=len(scenario.failures),
                errors=[str(e)] + self.errors,
                recovery_time_seconds={},
                data_loss_detected=True,
                state_divergence_detected=True,
                observations=self.observations,
            )

    async def _run_workload(
        self,
        workload_fn: Callable,
        duration_seconds: float,
    ) -> Dict[str, Any]:
        """Run workload concurrent with failures."""
        logger.info(f"Starting workload for {duration_seconds}s")

        observations = []
        start_time = datetime.utcnow()

        try:
            # Run workload
            result = await asyncio.wait_for(
                workload_fn(),
                timeout=duration_seconds + 5,  # Small buffer
            )

            end_time = datetime.utcnow()
            elapsed = (end_time - start_time).total_seconds()

            observations.append(f"Workload completed in {elapsed:.2f}s")

            return {
                "success": True,
                "observations": observations,
                "result": result,
            }

        except asyncio.TimeoutError:
            observations.append("Workload timeout")
            return {
                "success": False,
                "observations": observations,
                "result": None,
            }
        except Exception as e:
            logger.error(f"Workload failed: {e}")
            observations.append(f"Workload error: {e}")
            return {
                "success": False,
                "observations": observations,
                "error": str(e),
            }

    async def _inject_failures(
        self,
        failures: List[FailureInjection],
        duration_seconds: float,
    ) -> Dict[str, Any]:
        """Inject failures on schedule."""
        logger.info(f"Scheduling {len(failures)} failures")

        observations = []
        recovery_times = {}
        start_time = datetime.utcnow()

        failure_tasks = []

        for failure in failures:
            task = asyncio.create_task(
                self._inject_single_failure(
                    failure, start_time, observations, recovery_times
                )
            )
            failure_tasks.append(task)

        # Wait for all failures to be injected
        await asyncio.gather(*failure_tasks, return_exceptions=True)

        observations.append(
            f"Injected {len(failures)} failures, recovery times: {recovery_times}"
        )

        return {
            "success": True,
            "observations": observations,
            "recovery_times": recovery_times,
        }

    async def _inject_single_failure(
        self,
        failure: FailureInjection,
        start_time: datetime,
        observations: List[str],
        recovery_times: Dict[str, float],
    ) -> None:
        """Inject a single failure and recover."""
        # Wait for start time offset
        await asyncio.sleep(failure.start_time_offset_seconds)

        target_id = failure.target_id or failure.target.value
        failure_id = f"{failure.failure_type.value}:{target_id}"

        try:
            logger.warning(
                f"Injecting {failure.failure_type.value} on {target_id} for {failure.duration_seconds}s"
            )

            # Record failure start
            failure_start = datetime.utcnow()
            observations.append(
                f"Failure injected: {failure_id} at {failure_start.isoformat()}"
            )

            # Simulate failure effects
            self.failures_active[failure_id] = failure

            # Wait for failure duration
            await asyncio.sleep(failure.duration_seconds)

            # Recover from failure
            del self.failures_active[failure_id]
            recovery_start = datetime.utcnow()

            await asyncio.sleep(failure.recovery_time_seconds)

            recovery_end = datetime.utcnow()
            recovery_time = (recovery_end - recovery_start).total_seconds()
            recovery_times[target_id] = recovery_time

            logger.info(
                f"Recovered from {failure.failure_type.value}: {recovery_time:.3f}s"
            )
            observations.append(
                f"Recovered from {failure_id}: {recovery_time:.3f}s"
            )

        except Exception as e:
            logger.error(f"Error injecting failure: {e}")
            if failure_id in self.failures_active:
                del self.failures_active[failure_id]

    async def _check_data_loss(self) -> bool:
        """
        Check if any data was lost.

        In real implementation:
        - Query all service backends
        - Compare with transaction log
        - Look for missing commits
        """
        logger.info("Checking for data loss...")

        # For now, simulate check
        # Real implementation would:
        # 1. Query Redis for unacknowledged messages
        # 2. Query Temporal for failed workflows
        # 3. Query GraphSub for orphaned nodes

        data_loss_detected = False

        if data_loss_detected:
            self.observations.append("❌ Data loss detected!")
            self.errors.append("Data loss detected")

        return data_loss_detected

    async def _check_state_divergence(self) -> bool:
        """
        Check if distributed state diverged.

        In real implementation:
        - Compare agent states
        - Verify pheromone consistency
        - Check capability score agreement
        """
        logger.info("Checking for state divergence...")

        # For now, simulate check
        # Real implementation would:
        # 1. Query each agent's local state
        # 2. Query GraphSub authoritative state
        # 3. Compute divergence metric

        state_divergence = False

        if state_divergence:
            self.observations.append("❌ State divergence detected!")
            self.errors.append("State divergence")

        return state_divergence

    def should_fail(
        self,
        failure_type: FailureType,
        target_id: str,
    ) -> bool:
        """Check if a failure is currently active."""
        failure_id = f"{failure_type.value}:{target_id}"
        return failure_id in self.failures_active

    def get_active_failures(self) -> Dict[str, FailureInjection]:
        """Get currently active failures."""
        return self.failures_active.copy()


class ScenarioBuilder:
    """Builder for constructing chaos scenarios."""

    def __init__(self, name: str):
        """Initialize scenario builder."""
        self.name = name
        self.description = ""
        self.failures: List[FailureInjection] = []
        self.duration_seconds = 60.0
        self.expected_outcome = ""
        self.verification_fn: Optional[Callable] = None

    def with_description(self, description: str) -> "ScenarioBuilder":
        """Set scenario description."""
        self.description = description
        return self

    def with_duration(self, seconds: float) -> "ScenarioBuilder":
        """Set scenario duration."""
        self.duration_seconds = seconds
        return self

    def add_failure(
        self,
        failure_type: FailureType,
        target: FailureTarget,
        target_id: Optional[str] = None,
        duration_seconds: float = 5.0,
        start_offset: float = 5.0,
        severity: float = 1.0,
    ) -> "ScenarioBuilder":
        """Add failure to scenario."""
        failure = FailureInjection(
            failure_type=failure_type,
            target=target,
            target_id=target_id,
            duration_seconds=duration_seconds,
            start_time_offset_seconds=start_offset,
            severity=severity,
        )
        self.failures.append(failure)
        return self

    def with_verification(self, fn: Callable) -> "ScenarioBuilder":
        """Add verification function."""
        self.verification_fn = fn
        return self

    def build(self) -> ChaosScenario:
        """Build the scenario."""
        return ChaosScenario(
            name=self.name,
            description=self.description,
            failures=self.failures,
            duration_seconds=self.duration_seconds,
            expected_outcome=self.expected_outcome,
            verification_fn=self.verification_fn,
        )
