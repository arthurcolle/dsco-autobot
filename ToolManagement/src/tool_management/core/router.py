"""
Intelligent Backend Router.

Routes tool execution requests to the best available backend based on:
- Health status
- Current load
- Latency percentiles
- Error rates
- Cost optimization
- Capability matching
"""

from __future__ import annotations

import asyncio
import time
import random
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import deque
import statistics

logger = logging.getLogger(__name__)


class RoutingStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    LATENCY = "latency"
    COST = "cost"
    ADAPTIVE = "adaptive"  # Combines multiple factors


class BackendStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class LatencyStats:
    """Latency statistics for a backend."""
    samples: deque = field(default_factory=lambda: deque(maxlen=1000))

    def record(self, latency_ms: float):
        self.samples.append(latency_ms)

    @property
    def p50(self) -> float:
        if not self.samples:
            return 0
        sorted_samples = sorted(self.samples)
        idx = len(sorted_samples) // 2
        return sorted_samples[idx]

    @property
    def p95(self) -> float:
        if not self.samples:
            return 0
        sorted_samples = sorted(self.samples)
        idx = int(len(sorted_samples) * 0.95)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

    @property
    def p99(self) -> float:
        if not self.samples:
            return 0
        sorted_samples = sorted(self.samples)
        idx = int(len(sorted_samples) * 0.99)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

    @property
    def avg(self) -> float:
        if not self.samples:
            return 0
        return statistics.mean(self.samples)


@dataclass
class BackendMetrics:
    """Metrics for a single backend."""
    backend_id: str
    status: BackendStatus = BackendStatus.UNKNOWN

    # Connection tracking
    active_connections: int = 0
    max_connections: int = 100
    total_requests: int = 0

    # Success/failure tracking
    success_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0

    # Latency tracking
    latency: LatencyStats = field(default_factory=LatencyStats)

    # Health check
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0

    # Cost (per 1000 requests)
    cost_per_1k: float = 0.0

    # Capabilities
    capabilities: List[str] = field(default_factory=list)
    supported_tools: List[str] = field(default_factory=list)

    # Weight for weighted routing
    weight: int = 100

    # Circuit breaker
    circuit_open: bool = False
    circuit_open_until: Optional[datetime] = None

    @property
    def error_rate(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0
        return self.failure_count / total

    @property
    def load(self) -> float:
        if self.max_connections == 0:
            return 1.0
        return self.active_connections / self.max_connections

    @property
    def is_available(self) -> bool:
        if self.circuit_open:
            if self.circuit_open_until and datetime.utcnow() > self.circuit_open_until:
                self.circuit_open = False
            else:
                return False
        return self.status in (BackendStatus.HEALTHY, BackendStatus.DEGRADED)

    def record_success(self, latency_ms: float):
        self.success_count += 1
        self.total_requests += 1
        self.latency.record(latency_ms)
        self.consecutive_failures = 0

    def record_failure(self, is_timeout: bool = False):
        self.failure_count += 1
        self.total_requests += 1
        self.consecutive_failures += 1
        if is_timeout:
            self.timeout_count += 1

        # Open circuit breaker after 5 consecutive failures
        if self.consecutive_failures >= 5:
            self.circuit_open = True
            self.circuit_open_until = datetime.utcnow() + timedelta(seconds=30)
            logger.warning(f"Circuit breaker opened for {self.backend_id}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend_id": self.backend_id,
            "status": self.status.value,
            "active_connections": self.active_connections,
            "max_connections": self.max_connections,
            "load": f"{self.load:.1%}",
            "total_requests": self.total_requests,
            "error_rate": f"{self.error_rate:.2%}",
            "latency_p50_ms": round(self.latency.p50, 2),
            "latency_p95_ms": round(self.latency.p95, 2),
            "latency_p99_ms": round(self.latency.p99, 2),
            "circuit_open": self.circuit_open,
            "capabilities": self.capabilities,
        }


@dataclass
class RoutingDecision:
    """Result of a routing decision."""
    backend_id: str
    reason: str
    score: float
    alternatives: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class IntelligentRouter:
    """
    Routes requests to backends using intelligent selection.

    Supports multiple strategies:
    - Round robin: Simple rotation
    - Least connections: Route to least loaded backend
    - Weighted: Use configured weights
    - Latency: Route to fastest backend
    - Cost: Route to cheapest backend
    - Adaptive: Combine all factors with scoring
    """

    def __init__(
        self,
        strategy: RoutingStrategy = RoutingStrategy.ADAPTIVE,
        health_check_interval: int = 30,
    ):
        self.strategy = strategy
        self.health_check_interval = health_check_interval

        self._backends: Dict[str, BackendMetrics] = {}
        self._round_robin_index = 0
        self._health_checker: Optional[Callable] = None
        self._running = False

    def register_backend(
        self,
        backend_id: str,
        url: str = None,
        capabilities: List[str] = None,
        max_connections: int = 100,
        weight: int = 100,
        cost_per_1k: float = 0.0,
    ):
        """Register a backend for routing."""
        self._backends[backend_id] = BackendMetrics(
            backend_id=backend_id,
            capabilities=capabilities or [],
            max_connections=max_connections,
            weight=weight,
            cost_per_1k=cost_per_1k,
            status=BackendStatus.UNKNOWN,
        )
        logger.info(f"Registered backend {backend_id} for routing")

    def unregister_backend(self, backend_id: str):
        """Remove a backend from routing."""
        if backend_id in self._backends:
            del self._backends[backend_id]
            logger.info(f"Unregistered backend {backend_id}")

    def update_status(self, backend_id: str, status: BackendStatus):
        """Update backend health status."""
        if backend_id in self._backends:
            self._backends[backend_id].status = status
            self._backends[backend_id].last_health_check = datetime.utcnow()

    def route(
        self,
        tool_name: str = None,
        required_capabilities: List[str] = None,
        prefer_backend: str = None,
        exclude_backends: List[str] = None,
    ) -> Optional[RoutingDecision]:
        """
        Select the best backend for a request.

        Args:
            tool_name: Specific tool being called
            required_capabilities: Capabilities the backend must have
            prefer_backend: Preferred backend (if available)
            exclude_backends: Backends to exclude

        Returns:
            RoutingDecision with selected backend and reasoning
        """
        exclude = set(exclude_backends or [])
        required_caps = set(required_capabilities or [])

        # Filter available backends
        candidates = []
        for backend_id, metrics in self._backends.items():
            if backend_id in exclude:
                continue
            if not metrics.is_available:
                continue
            if required_caps and not required_caps.issubset(set(metrics.capabilities)):
                continue
            if tool_name and metrics.supported_tools and tool_name not in metrics.supported_tools:
                continue
            candidates.append(metrics)

        if not candidates:
            return None

        # Check preferred backend
        if prefer_backend:
            for m in candidates:
                if m.backend_id == prefer_backend:
                    return RoutingDecision(
                        backend_id=prefer_backend,
                        reason="preferred",
                        score=1.0,
                        alternatives=[c.backend_id for c in candidates if c.backend_id != prefer_backend],
                    )

        # Apply routing strategy
        if self.strategy == RoutingStrategy.ROUND_ROBIN:
            return self._route_round_robin(candidates)
        elif self.strategy == RoutingStrategy.LEAST_CONNECTIONS:
            return self._route_least_connections(candidates)
        elif self.strategy == RoutingStrategy.WEIGHTED:
            return self._route_weighted(candidates)
        elif self.strategy == RoutingStrategy.LATENCY:
            return self._route_latency(candidates)
        elif self.strategy == RoutingStrategy.COST:
            return self._route_cost(candidates)
        else:
            return self._route_adaptive(candidates)

    def _route_round_robin(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """Simple round-robin selection."""
        self._round_robin_index = (self._round_robin_index + 1) % len(candidates)
        selected = candidates[self._round_robin_index]
        return RoutingDecision(
            backend_id=selected.backend_id,
            reason="round_robin",
            score=1.0,
            alternatives=[c.backend_id for c in candidates if c.backend_id != selected.backend_id],
        )

    def _route_least_connections(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """Route to backend with least active connections."""
        selected = min(candidates, key=lambda m: m.load)
        return RoutingDecision(
            backend_id=selected.backend_id,
            reason=f"least_connections (load={selected.load:.1%})",
            score=1.0 - selected.load,
            alternatives=[c.backend_id for c in candidates if c.backend_id != selected.backend_id],
        )

    def _route_weighted(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """Route based on configured weights."""
        total_weight = sum(m.weight for m in candidates)
        if total_weight == 0:
            return self._route_round_robin(candidates)

        r = random.randint(0, total_weight - 1)
        cumulative = 0
        for m in candidates:
            cumulative += m.weight
            if r < cumulative:
                return RoutingDecision(
                    backend_id=m.backend_id,
                    reason=f"weighted (weight={m.weight})",
                    score=m.weight / total_weight,
                    alternatives=[c.backend_id for c in candidates if c.backend_id != m.backend_id],
                )

        return self._route_round_robin(candidates)

    def _route_latency(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """Route to backend with lowest latency."""
        # Use p95 latency for selection
        selected = min(candidates, key=lambda m: m.latency.p95 if m.latency.samples else float('inf'))
        return RoutingDecision(
            backend_id=selected.backend_id,
            reason=f"latency (p95={selected.latency.p95:.0f}ms)",
            score=1.0,
            alternatives=[c.backend_id for c in candidates if c.backend_id != selected.backend_id],
            metadata={"latency_p95_ms": selected.latency.p95},
        )

    def _route_cost(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """Route to cheapest backend."""
        selected = min(candidates, key=lambda m: m.cost_per_1k)
        return RoutingDecision(
            backend_id=selected.backend_id,
            reason=f"cost (${selected.cost_per_1k}/1k)",
            score=1.0,
            alternatives=[c.backend_id for c in candidates if c.backend_id != selected.backend_id],
            metadata={"cost_per_1k": selected.cost_per_1k},
        )

    def _route_adaptive(self, candidates: List[BackendMetrics]) -> RoutingDecision:
        """
        Adaptive routing combining multiple factors.

        Score = w1 * (1 - load) + w2 * (1 - error_rate) + w3 * latency_score + w4 * cost_score
        """
        scores = []
        max_latency = max((m.latency.p95 for m in candidates if m.latency.samples), default=1000) or 1000
        max_cost = max((m.cost_per_1k for m in candidates), default=1) or 1

        for m in candidates:
            # Load score (0-1, higher is better = less load)
            load_score = 1.0 - m.load

            # Error score (0-1, higher is better = fewer errors)
            error_score = 1.0 - min(m.error_rate, 1.0)

            # Latency score (0-1, higher is better = lower latency)
            latency_score = 1.0 - (m.latency.p95 / max_latency) if m.latency.samples else 0.5

            # Cost score (0-1, higher is better = lower cost)
            cost_score = 1.0 - (m.cost_per_1k / max_cost)

            # Weighted combination
            total_score = (
                0.3 * load_score +
                0.3 * error_score +
                0.25 * latency_score +
                0.15 * cost_score
            )

            scores.append((m, total_score))

        # Sort by score descending
        scores.sort(key=lambda x: x[1], reverse=True)
        selected, best_score = scores[0]

        return RoutingDecision(
            backend_id=selected.backend_id,
            reason=f"adaptive (score={best_score:.2f})",
            score=best_score,
            alternatives=[s[0].backend_id for s in scores[1:4]],
            metadata={
                "load": selected.load,
                "error_rate": selected.error_rate,
                "latency_p95": selected.latency.p95,
                "cost": selected.cost_per_1k,
            },
        )

    def acquire_connection(self, backend_id: str) -> bool:
        """Acquire a connection slot for a backend."""
        if backend_id not in self._backends:
            return False

        metrics = self._backends[backend_id]
        if metrics.active_connections >= metrics.max_connections:
            return False

        metrics.active_connections += 1
        return True

    def release_connection(self, backend_id: str, success: bool, latency_ms: float = 0):
        """Release a connection and record metrics."""
        if backend_id not in self._backends:
            return

        metrics = self._backends[backend_id]
        metrics.active_connections = max(0, metrics.active_connections - 1)

        if success:
            metrics.record_success(latency_ms)
        else:
            metrics.record_failure()

    def get_metrics(self, backend_id: str = None) -> Dict[str, Any]:
        """Get metrics for one or all backends."""
        if backend_id:
            if backend_id in self._backends:
                return self._backends[backend_id].to_dict()
            return {}

        return {
            bid: m.to_dict()
            for bid, m in self._backends.items()
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate routing statistics."""
        total_requests = sum(m.total_requests for m in self._backends.values())
        total_failures = sum(m.failure_count for m in self._backends.values())
        healthy_count = sum(1 for m in self._backends.values() if m.is_available)

        return {
            "strategy": self.strategy.value,
            "total_backends": len(self._backends),
            "healthy_backends": healthy_count,
            "total_requests": total_requests,
            "total_failures": total_failures,
            "overall_error_rate": f"{total_failures / total_requests:.2%}" if total_requests else "0%",
            "backends": self.get_metrics(),
        }


# Singleton instance
_router: Optional[IntelligentRouter] = None


def get_router() -> IntelligentRouter:
    """Get or create the global router instance."""
    global _router
    if _router is None:
        _router = IntelligentRouter()
    return _router
