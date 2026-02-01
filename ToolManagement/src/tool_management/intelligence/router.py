"""
Adaptive Router

Routes tool requests based on execution history, cost/quality/latency tradeoffs,
and real-time health monitoring. No external dependencies required.
"""

from __future__ import annotations
import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any, Dict, List, Optional, Set, Tuple, Callable, Union,
    TYPE_CHECKING
)
from collections import defaultdict
import threading

if TYPE_CHECKING:
    from .memory import ExecutionMemory, ToolStats


class RoutingStrategy(Enum):
    """Strategy for selecting between tool variants."""
    LOWEST_LATENCY = auto()
    LOWEST_COST = auto()
    HIGHEST_SUCCESS = auto()
    BALANCED = auto()
    ROUND_ROBIN = auto()
    RANDOM = auto()
    WEIGHTED = auto()
    EPSILON_GREEDY = auto()  # Explore vs exploit


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = auto()    # Normal operation
    OPEN = auto()      # Failing, reject requests
    HALF_OPEN = auto() # Testing recovery


@dataclass
class CircuitBreaker:
    """Circuit breaker for a single tool."""
    tool_id: str
    failure_threshold: int = 5
    recovery_timeout_s: float = 30.0
    half_open_max_calls: int = 3

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0

    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_success(self) -> None:
        """Record successful execution."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.half_open_max_calls:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
            else:
                self.failure_count = 0

    def record_failure(self) -> None:
        """Record failed execution."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                # Failed during recovery test
                self.state = CircuitState.OPEN
                self.half_open_calls = 0
            elif self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN

    def allow_request(self) -> bool:
        """Check if request should be allowed."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                # Check if recovery timeout elapsed
                if time.time() - self.last_failure_time >= self.recovery_timeout_s:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
                    return True
                return False

            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.half_open_max_calls:
                    self.half_open_calls += 1
                    return True
                return False

            return False

    def get_state(self) -> Dict[str, Any]:
        """Get current state."""
        return {
            "tool_id": self.tool_id,
            "state": self.state.name,
            "failure_count": self.failure_count,
            "last_failure": self.last_failure_time,
        }


@dataclass
class ToolVariant:
    """A variant of a tool (different provider, version, etc.)."""
    variant_id: str
    tool_id: str
    provider: Optional[str] = None
    version: Optional[str] = None
    weight: float = 1.0
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Runtime stats (updated from execution memory)
    avg_latency_ms: float = 0.0
    avg_cost: float = 0.0
    success_rate: float = 1.0
    total_calls: int = 0


@dataclass
class RoutingContext:
    """Context for routing decision."""
    tool_id: str
    input_data: Optional[Any] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    preferences: Dict[str, float] = field(default_factory=dict)
    trace_id: Optional[str] = None

    # Constraints
    max_latency_ms: Optional[float] = None
    max_cost: Optional[float] = None
    min_success_rate: Optional[float] = None

    # Preferences (0-1, higher = more important)
    latency_weight: float = 0.33
    cost_weight: float = 0.33
    reliability_weight: float = 0.34


@dataclass
class RoutingDecision:
    """Result of routing decision."""
    selected_variant: ToolVariant
    reason: str
    alternatives: List[ToolVariant] = field(default_factory=list)
    score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class AdaptiveRouter:
    """
    Routes tool requests to optimal variants based on:
    - Execution history (latency, cost, success rate)
    - Real-time health (circuit breakers)
    - User preferences (cost vs speed vs reliability)
    - A/B testing for exploration
    """

    def __init__(
        self,
        execution_memory: Optional['ExecutionMemory'] = None,
        default_strategy: RoutingStrategy = RoutingStrategy.BALANCED,
        epsilon: float = 0.1,  # Exploration rate
    ):
        self.memory = execution_memory
        self.default_strategy = default_strategy
        self.epsilon = epsilon

        # Tool variants registry
        self._variants: Dict[str, List[ToolVariant]] = defaultdict(list)
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}

        # Round-robin counters
        self._rr_counters: Dict[str, int] = defaultdict(int)

        # Routing stats
        self._routing_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        self._lock = threading.Lock()

    def register_variant(self, variant: ToolVariant) -> None:
        """Register a tool variant."""
        with self._lock:
            self._variants[variant.tool_id].append(variant)
            # Create circuit breaker
            self._circuit_breakers[variant.variant_id] = CircuitBreaker(
                tool_id=variant.variant_id
            )

    def register_variants(self, tool_id: str, variants: List[ToolVariant]) -> None:
        """Register multiple variants for a tool."""
        for v in variants:
            v.tool_id = tool_id
            self.register_variant(v)

    def unregister_variant(self, variant_id: str) -> None:
        """Remove a variant."""
        with self._lock:
            for tool_id, variants in self._variants.items():
                self._variants[tool_id] = [v for v in variants if v.variant_id != variant_id]
            self._circuit_breakers.pop(variant_id, None)

    async def route(
        self,
        context: RoutingContext,
        strategy: Optional[RoutingStrategy] = None,
    ) -> RoutingDecision:
        """
        Route a tool request to the best variant.

        Args:
            context: Routing context with constraints and preferences
            strategy: Override default routing strategy

        Returns:
            RoutingDecision with selected variant
        """
        strategy = strategy or self.default_strategy
        variants = self._get_available_variants(context.tool_id)

        if not variants:
            raise ValueError(f"No variants available for tool: {context.tool_id}")

        # Update variant stats from memory
        await self._update_variant_stats(variants)

        # Filter by constraints
        filtered = self._filter_by_constraints(variants, context)

        if not filtered:
            # Fallback: use all variants if constraints too strict
            filtered = variants

        # Select based on strategy
        if strategy == RoutingStrategy.EPSILON_GREEDY:
            selected, reason = self._epsilon_greedy(filtered, context)
        elif strategy == RoutingStrategy.LOWEST_LATENCY:
            selected, reason = self._select_lowest_latency(filtered)
        elif strategy == RoutingStrategy.LOWEST_COST:
            selected, reason = self._select_lowest_cost(filtered)
        elif strategy == RoutingStrategy.HIGHEST_SUCCESS:
            selected, reason = self._select_highest_success(filtered)
        elif strategy == RoutingStrategy.BALANCED:
            selected, reason = self._select_balanced(filtered, context)
        elif strategy == RoutingStrategy.ROUND_ROBIN:
            selected, reason = self._select_round_robin(filtered, context.tool_id)
        elif strategy == RoutingStrategy.RANDOM:
            selected, reason = self._select_random(filtered)
        elif strategy == RoutingStrategy.WEIGHTED:
            selected, reason = self._select_weighted(filtered)
        else:
            selected, reason = filtered[0], "default"

        # Record routing decision
        self._routing_stats[context.tool_id][selected.variant_id] += 1

        alternatives = [v for v in filtered if v.variant_id != selected.variant_id]

        return RoutingDecision(
            selected_variant=selected,
            reason=reason,
            alternatives=alternatives[:3],
            score=self._compute_variant_score(selected, context),
            metadata={
                "strategy": strategy.name,
                "candidates": len(filtered),
            },
        )

    def _get_available_variants(self, tool_id: str) -> List[ToolVariant]:
        """Get variants that pass circuit breaker check."""
        variants = []
        for v in self._variants.get(tool_id, []):
            cb = self._circuit_breakers.get(v.variant_id)
            if not cb or cb.allow_request():
                variants.append(v)
        return variants

    async def _update_variant_stats(self, variants: List[ToolVariant]) -> None:
        """Update variant stats from execution memory."""
        if not self.memory:
            return

        for v in variants:
            stats = self.memory.get_tool_stats(v.variant_id)
            if stats:
                v.avg_latency_ms = stats.ema_latency_ms
                v.avg_cost = stats.total_cost / max(stats.total_executions, 1)
                v.success_rate = stats.ema_success_rate
                v.total_calls = stats.total_executions

    def _filter_by_constraints(
        self,
        variants: List[ToolVariant],
        context: RoutingContext,
    ) -> List[ToolVariant]:
        """Filter variants by hard constraints."""
        filtered = variants

        if context.max_latency_ms is not None:
            filtered = [v for v in filtered if v.avg_latency_ms <= context.max_latency_ms]

        if context.max_cost is not None:
            filtered = [v for v in filtered if v.avg_cost <= context.max_cost]

        if context.min_success_rate is not None:
            filtered = [v for v in filtered if v.success_rate >= context.min_success_rate]

        return filtered

    def _compute_variant_score(
        self,
        variant: ToolVariant,
        context: RoutingContext,
    ) -> float:
        """Compute weighted score for a variant."""
        # Normalize metrics (lower is better for latency/cost, higher for success)
        latency_score = 1.0 / (1.0 + variant.avg_latency_ms / 1000.0)
        cost_score = 1.0 / (1.0 + variant.avg_cost)
        success_score = variant.success_rate

        return (
            context.latency_weight * latency_score +
            context.cost_weight * cost_score +
            context.reliability_weight * success_score
        )

    def _epsilon_greedy(
        self,
        variants: List[ToolVariant],
        context: RoutingContext,
    ) -> Tuple[ToolVariant, str]:
        """Epsilon-greedy selection for exploration/exploitation."""
        if random.random() < self.epsilon:
            # Explore: random selection
            selected = random.choice(variants)
            return selected, "exploration"
        else:
            # Exploit: best known
            return self._select_balanced(variants, context)

    def _select_lowest_latency(
        self,
        variants: List[ToolVariant],
    ) -> Tuple[ToolVariant, str]:
        """Select variant with lowest latency."""
        # Prefer variants with data, but don't exclude new ones
        with_data = [v for v in variants if v.total_calls > 0]
        if with_data:
            selected = min(with_data, key=lambda v: v.avg_latency_ms)
            return selected, f"lowest_latency:{selected.avg_latency_ms:.0f}ms"
        return variants[0], "no_data_fallback"

    def _select_lowest_cost(
        self,
        variants: List[ToolVariant],
    ) -> Tuple[ToolVariant, str]:
        """Select variant with lowest cost."""
        with_data = [v for v in variants if v.total_calls > 0]
        if with_data:
            selected = min(with_data, key=lambda v: v.avg_cost)
            return selected, f"lowest_cost:${selected.avg_cost:.4f}"
        return variants[0], "no_data_fallback"

    def _select_highest_success(
        self,
        variants: List[ToolVariant],
    ) -> Tuple[ToolVariant, str]:
        """Select variant with highest success rate."""
        with_data = [v for v in variants if v.total_calls > 0]
        if with_data:
            selected = max(with_data, key=lambda v: v.success_rate)
            return selected, f"highest_success:{selected.success_rate:.1%}"
        return variants[0], "no_data_fallback"

    def _select_balanced(
        self,
        variants: List[ToolVariant],
        context: RoutingContext,
    ) -> Tuple[ToolVariant, str]:
        """Select variant with best balanced score."""
        scored = [(v, self._compute_variant_score(v, context)) for v in variants]
        selected, score = max(scored, key=lambda x: x[1])
        return selected, f"balanced_score:{score:.3f}"

    def _select_round_robin(
        self,
        variants: List[ToolVariant],
        tool_id: str,
    ) -> Tuple[ToolVariant, str]:
        """Round-robin selection."""
        with self._lock:
            idx = self._rr_counters[tool_id] % len(variants)
            self._rr_counters[tool_id] += 1
        return variants[idx], f"round_robin:{idx}"

    def _select_random(
        self,
        variants: List[ToolVariant],
    ) -> Tuple[ToolVariant, str]:
        """Random selection."""
        selected = random.choice(variants)
        return selected, "random"

    def _select_weighted(
        self,
        variants: List[ToolVariant],
    ) -> Tuple[ToolVariant, str]:
        """Weighted random selection based on variant weights."""
        total_weight = sum(v.weight for v in variants)
        if total_weight == 0:
            return random.choice(variants), "weighted_fallback"

        r = random.random() * total_weight
        cumulative = 0.0
        for v in variants:
            cumulative += v.weight
            if r <= cumulative:
                return v, f"weighted:{v.weight}"

        return variants[-1], "weighted_fallback"

    def record_execution(
        self,
        variant_id: str,
        success: bool,
        latency_ms: float = 0.0,
        cost: float = 0.0,
    ) -> None:
        """Record execution result for circuit breaker and stats."""
        cb = self._circuit_breakers.get(variant_id)
        if cb:
            if success:
                cb.record_success()
            else:
                cb.record_failure()

    def get_circuit_breaker_state(self, variant_id: str) -> Optional[Dict[str, Any]]:
        """Get circuit breaker state for a variant."""
        cb = self._circuit_breakers.get(variant_id)
        return cb.get_state() if cb else None

    def get_all_circuit_states(self) -> Dict[str, Dict[str, Any]]:
        """Get all circuit breaker states."""
        return {vid: cb.get_state() for vid, cb in self._circuit_breakers.items()}

    def get_routing_stats(self) -> Dict[str, Dict[str, int]]:
        """Get routing statistics."""
        return dict(self._routing_stats)

    def reset_circuit_breaker(self, variant_id: str) -> bool:
        """Manually reset a circuit breaker."""
        cb = self._circuit_breakers.get(variant_id)
        if cb:
            with cb._lock:
                cb.state = CircuitState.CLOSED
                cb.failure_count = 0
                cb.success_count = 0
            return True
        return False


class LoadBalancer:
    """
    Load balancer for distributing requests across tool instances.
    Uses weighted algorithms and health checks.
    """

    def __init__(self, check_interval_s: float = 10.0):
        self._instances: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._health: Dict[str, bool] = {}
        self._weights: Dict[str, float] = {}
        self._check_interval = check_interval_s
        self._last_check: Dict[str, float] = {}
        self._lock = threading.Lock()

    def register_instance(
        self,
        tool_id: str,
        instance_id: str,
        endpoint: str,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a tool instance."""
        with self._lock:
            self._instances[tool_id].append({
                "instance_id": instance_id,
                "endpoint": endpoint,
                "weight": weight,
                "metadata": metadata or {},
            })
            self._health[instance_id] = True
            self._weights[instance_id] = weight

    def unregister_instance(self, instance_id: str) -> None:
        """Remove an instance."""
        with self._lock:
            for tool_id, instances in self._instances.items():
                self._instances[tool_id] = [
                    i for i in instances if i["instance_id"] != instance_id
                ]
            self._health.pop(instance_id, None)
            self._weights.pop(instance_id, None)

    def get_instance(
        self,
        tool_id: str,
        algorithm: str = "weighted_round_robin",
    ) -> Optional[Dict[str, Any]]:
        """Get next instance for a tool."""
        instances = self._get_healthy_instances(tool_id)
        if not instances:
            return None

        if algorithm == "round_robin":
            return self._round_robin(tool_id, instances)
        elif algorithm == "weighted_round_robin":
            return self._weighted_round_robin(instances)
        elif algorithm == "least_connections":
            return self._least_connections(instances)
        elif algorithm == "random":
            return random.choice(instances)
        else:
            return instances[0]

    def _get_healthy_instances(self, tool_id: str) -> List[Dict[str, Any]]:
        """Get healthy instances for a tool."""
        return [
            i for i in self._instances.get(tool_id, [])
            if self._health.get(i["instance_id"], False)
        ]

    def _round_robin(
        self,
        tool_id: str,
        instances: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Simple round-robin selection."""
        with self._lock:
            if not hasattr(self, "_rr_idx"):
                self._rr_idx = {}
            idx = self._rr_idx.get(tool_id, 0) % len(instances)
            self._rr_idx[tool_id] = idx + 1
        return instances[idx]

    def _weighted_round_robin(
        self,
        instances: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Weighted random selection."""
        total = sum(self._weights.get(i["instance_id"], 1.0) for i in instances)
        r = random.random() * total
        cumulative = 0.0
        for inst in instances:
            cumulative += self._weights.get(inst["instance_id"], 1.0)
            if r <= cumulative:
                return inst
        return instances[-1]

    def _least_connections(
        self,
        instances: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Select instance with least active connections."""
        # Would need connection tracking - fallback to weighted
        return self._weighted_round_robin(instances)

    def mark_healthy(self, instance_id: str) -> None:
        """Mark instance as healthy."""
        self._health[instance_id] = True

    def mark_unhealthy(self, instance_id: str) -> None:
        """Mark instance as unhealthy."""
        self._health[instance_id] = False

    def set_weight(self, instance_id: str, weight: float) -> None:
        """Update instance weight."""
        self._weights[instance_id] = weight


class RoutingMiddleware:
    """
    Middleware that integrates adaptive routing into request flow.
    Wraps tool execution with routing, circuit breaking, and stats collection.
    """

    def __init__(
        self,
        router: AdaptiveRouter,
        execution_memory: Optional['ExecutionMemory'] = None,
    ):
        self.router = router
        self.memory = execution_memory

    async def execute(
        self,
        tool_id: str,
        executor: Callable,
        input_data: Any,
        constraints: Optional[Dict[str, Any]] = None,
        preferences: Optional[Dict[str, float]] = None,
    ) -> Tuple[Any, RoutingDecision]:
        """
        Execute tool with adaptive routing.

        Args:
            tool_id: Base tool ID
            executor: Async function that takes (variant_id, input_data) and returns result
            input_data: Input to the tool
            constraints: Hard constraints (max_latency_ms, max_cost, etc.)
            preferences: Soft preferences (latency_weight, cost_weight, etc.)

        Returns:
            Tuple of (result, routing_decision)
        """
        context = RoutingContext(
            tool_id=tool_id,
            input_data=input_data,
            constraints=constraints or {},
        )

        if preferences:
            context.latency_weight = preferences.get("latency", 0.33)
            context.cost_weight = preferences.get("cost", 0.33)
            context.reliability_weight = preferences.get("reliability", 0.34)

        if constraints:
            context.max_latency_ms = constraints.get("max_latency_ms")
            context.max_cost = constraints.get("max_cost")
            context.min_success_rate = constraints.get("min_success_rate")

        # Route
        decision = await self.router.route(context)
        variant = decision.selected_variant

        # Execute with timing
        start_time = time.time()
        success = True
        result = None
        error = None

        try:
            result = await executor(variant.variant_id, input_data)
        except Exception as e:
            success = False
            error = e

        elapsed_ms = (time.time() - start_time) * 1000

        # Record execution
        self.router.record_execution(
            variant.variant_id,
            success=success,
            latency_ms=elapsed_ms,
        )

        # Record in memory if available
        if self.memory:
            from .memory import ExecutionRecord
            record = ExecutionRecord(
                tool_id=variant.variant_id,
                input_hash=str(hash(str(input_data)))[:16],
                output_hash=str(hash(str(result)))[:16] if result else "",
                success=success,
                latency_ms=elapsed_ms,
                error_type=type(error).__name__ if error else None,
            )
            self.memory.record(record)

        if error:
            raise error

        return result, decision

    async def execute_with_fallback(
        self,
        tool_id: str,
        executor: Callable,
        input_data: Any,
        max_retries: int = 2,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, List[RoutingDecision]]:
        """
        Execute with automatic fallback to alternatives on failure.

        Returns:
            Tuple of (result, list_of_decisions_tried)
        """
        decisions = []
        last_error = None
        tried_variants: Set[str] = set()

        for attempt in range(max_retries + 1):
            context = RoutingContext(
                tool_id=tool_id,
                input_data=input_data,
                constraints=constraints or {},
            )

            try:
                decision = await self.router.route(context)

                # Skip if we already tried this variant
                if decision.selected_variant.variant_id in tried_variants:
                    # Try an alternative
                    for alt in decision.alternatives:
                        if alt.variant_id not in tried_variants:
                            decision = RoutingDecision(
                                selected_variant=alt,
                                reason="fallback",
                                alternatives=[],
                            )
                            break
                    else:
                        # No new alternatives
                        break

                decisions.append(decision)
                tried_variants.add(decision.selected_variant.variant_id)

                result, _ = await self.execute(
                    tool_id,
                    executor,
                    input_data,
                    constraints,
                )
                return result, decisions

            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"No available variants for {tool_id}")
