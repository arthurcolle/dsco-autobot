"""
Telemetry and Metrics System

Production-grade observability with:
- Counter, Gauge, Histogram, Summary metrics
- Request tracing with correlation IDs
- Performance profiling
- Resource utilization tracking
- Metric aggregation and export
"""

import time
import asyncio
import psutil
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from collections import defaultdict, deque
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Metric types"""
    COUNTER = "counter"  # Monotonically increasing
    GAUGE = "gauge"  # Can go up or down
    HISTOGRAM = "histogram"  # Distribution of values
    SUMMARY = "summary"  # Statistical summary


@dataclass
class MetricValue:
    """Single metric value with timestamp"""
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class HistogramBucket:
    """Histogram bucket"""
    le: float  # Less than or equal to
    count: int = 0


class Metric:
    """Base metric class"""

    def __init__(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        labels: Optional[List[str]] = None
    ):
        self.name = name
        self.metric_type = metric_type
        self.description = description
        self.labels = labels or []
        self.created_at = time.time()


class Counter(Metric):
    """
    Counter metric (monotonically increasing).

    Use for:
    - Request counts
    - Error counts
    - Bytes processed
    """

    def __init__(self, name: str, description: str = "", labels: Optional[List[str]] = None):
        super().__init__(name, MetricType.COUNTER, description, labels)
        self._values: Dict[tuple, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    async def inc(self, amount: float = 1.0, label_values: Optional[Dict[str, str]] = None):
        """Increment counter"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            self._values[label_key] += amount

    async def get(self, label_values: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            return self._values.get(label_key, 0.0)

    def _make_label_key(self, label_values: Dict[str, str]) -> tuple:
        """Create hashable label key"""
        return tuple(sorted(label_values.items()))

    async def get_all(self) -> List[MetricValue]:
        """Get all counter values"""
        async with self._lock:
            return [
                MetricValue(value=v, labels=dict(k))
                for k, v in self._values.items()
            ]


class Gauge(Metric):
    """
    Gauge metric (can increase or decrease).

    Use for:
    - Current queue size
    - Active connections
    - Memory usage
    - CPU utilization
    """

    def __init__(self, name: str, description: str = "", labels: Optional[List[str]] = None):
        super().__init__(name, MetricType.GAUGE, description, labels)
        self._values: Dict[tuple, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    async def set(self, value: float, label_values: Optional[Dict[str, str]] = None):
        """Set gauge value"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            self._values[label_key] = value

    async def inc(self, amount: float = 1.0, label_values: Optional[Dict[str, str]] = None):
        """Increment gauge"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            self._values[label_key] += amount

    async def dec(self, amount: float = 1.0, label_values: Optional[Dict[str, str]] = None):
        """Decrement gauge"""
        await self.inc(-amount, label_values)

    async def get(self, label_values: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            return self._values.get(label_key, 0.0)

    def _make_label_key(self, label_values: Dict[str, str]) -> tuple:
        """Create hashable label key"""
        return tuple(sorted(label_values.items()))

    async def get_all(self) -> List[MetricValue]:
        """Get all gauge values"""
        async with self._lock:
            return [
                MetricValue(value=v, labels=dict(k))
                for k, v in self._values.items()
            ]


class Histogram(Metric):
    """
    Histogram metric (distribution of values).

    Use for:
    - Request duration
    - Response size
    - Batch size
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        buckets: Optional[List[float]] = None,
        labels: Optional[List[str]] = None
    ):
        super().__init__(name, MetricType.HISTOGRAM, description, labels)

        # Default buckets (exponential)
        self.buckets = buckets or [
            0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5,
            0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf')
        ]

        self._bucket_counts: Dict[tuple, List[int]] = defaultdict(
            lambda: [0] * len(self.buckets)
        )
        self._sums: Dict[tuple, float] = defaultdict(float)
        self._counts: Dict[tuple, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def observe(self, value: float, label_values: Optional[Dict[str, str]] = None):
        """Observe a value"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            self._sums[label_key] += value
            self._counts[label_key] += 1

            # Update buckets
            for i, bucket_le in enumerate(self.buckets):
                if value <= bucket_le:
                    self._bucket_counts[label_key][i] += 1

    async def get_stats(self, label_values: Optional[Dict[str, str]] = None) -> Dict:
        """Get histogram statistics"""
        label_key = self._make_label_key(label_values or {})

        async with self._lock:
            count = self._counts.get(label_key, 0)
            sum_value = self._sums.get(label_key, 0.0)
            buckets = self._bucket_counts.get(label_key, [0] * len(self.buckets))

            return {
                "count": count,
                "sum": sum_value,
                "avg": sum_value / count if count > 0 else 0.0,
                "buckets": [
                    {"le": le, "count": c}
                    for le, c in zip(self.buckets, buckets)
                ]
            }

    def _make_label_key(self, label_values: Dict[str, str]) -> tuple:
        """Create hashable label key"""
        return tuple(sorted(label_values.items()))


class Summary(Metric):
    """
    Summary metric (statistical summary over sliding window).

    Use for:
    - Request latency percentiles
    - Throughput rates
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        max_age_seconds: float = 600.0,  # 10 minutes
        quantiles: Optional[List[float]] = None,  # [0.5, 0.9, 0.99]
        labels: Optional[List[str]] = None
    ):
        super().__init__(name, MetricType.SUMMARY, description, labels)

        self.max_age_seconds = max_age_seconds
        self.quantiles = quantiles or [0.5, 0.9, 0.95, 0.99]

        self._observations: Dict[tuple, deque] = defaultdict(
            lambda: deque(maxlen=10000)  # Max 10k observations
        )
        self._lock = asyncio.Lock()

    async def observe(self, value: float, label_values: Optional[Dict[str, str]] = None):
        """Observe a value"""
        label_key = self._make_label_key(label_values or {})
        timestamp = time.time()

        async with self._lock:
            self._observations[label_key].append((timestamp, value))

    async def get_stats(self, label_values: Optional[Dict[str, str]] = None) -> Dict:
        """Get summary statistics"""
        label_key = self._make_label_key(label_values or {})
        now = time.time()
        cutoff = now - self.max_age_seconds

        async with self._lock:
            # Filter to recent observations
            obs = self._observations.get(label_key, deque())
            recent_values = [v for t, v in obs if t > cutoff]

            if not recent_values:
                return {
                    "count": 0,
                    "sum": 0.0,
                    "avg": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "quantiles": {}
                }

            sorted_values = sorted(recent_values)

            return {
                "count": len(recent_values),
                "sum": sum(recent_values),
                "avg": statistics.mean(recent_values),
                "min": min(recent_values),
                "max": max(recent_values),
                "quantiles": {
                    q: statistics.quantiles(sorted_values, n=100)[int(q * 100) - 1]
                    for q in self.quantiles
                } if len(sorted_values) > 1 else {}
            }

    def _make_label_key(self, label_values: Dict[str, str]) -> tuple:
        """Create hashable label key"""
        return tuple(sorted(label_values.items()))


class MetricsRegistry:
    """
    Central metrics registry.

    Manages all metrics and provides collection/export.
    """

    def __init__(self):
        self._metrics: Dict[str, Metric] = {}
        self._lock = asyncio.Lock()

    async def register(self, metric: Metric):
        """Register a metric"""
        async with self._lock:
            if metric.name in self._metrics:
                logger.warning(f"Metric {metric.name} already registered, overwriting")

            self._metrics[metric.name] = metric

    async def get_metric(self, name: str) -> Optional[Metric]:
        """Get metric by name"""
        async with self._lock:
            return self._metrics.get(name)

    async def get_or_create_counter(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None
    ) -> Counter:
        """Get or create counter metric"""
        metric = await self.get_metric(name)

        if metric is None:
            metric = Counter(name, description, labels)
            await self.register(metric)
        elif not isinstance(metric, Counter):
            raise TypeError(f"Metric {name} exists but is not a Counter")

        return metric

    async def get_or_create_gauge(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None
    ) -> Gauge:
        """Get or create gauge metric"""
        metric = await self.get_metric(name)

        if metric is None:
            metric = Gauge(name, description, labels)
            await self.register(metric)
        elif not isinstance(metric, Gauge):
            raise TypeError(f"Metric {name} exists but is not a Gauge")

        return metric

    async def get_or_create_histogram(
        self,
        name: str,
        description: str = "",
        buckets: Optional[List[float]] = None,
        labels: Optional[List[str]] = None
    ) -> Histogram:
        """Get or create histogram metric"""
        metric = await self.get_metric(name)

        if metric is None:
            metric = Histogram(name, description, buckets, labels)
            await self.register(metric)
        elif not isinstance(metric, Histogram):
            raise TypeError(f"Metric {name} exists but is not a Histogram")

        return metric

    async def get_or_create_summary(
        self,
        name: str,
        description: str = "",
        max_age_seconds: float = 600.0,
        quantiles: Optional[List[float]] = None,
        labels: Optional[List[str]] = None
    ) -> Summary:
        """Get or create summary metric"""
        metric = await self.get_metric(name)

        if metric is None:
            metric = Summary(name, description, max_age_seconds, quantiles, labels)
            await self.register(metric)
        elif not isinstance(metric, Summary):
            raise TypeError(f"Metric {name} exists but is not a Summary")

        return metric

    async def collect(self) -> Dict:
        """Collect all metrics"""
        metrics_data = {}

        async with self._lock:
            for name, metric in self._metrics.items():
                if isinstance(metric, (Counter, Gauge)):
                    metrics_data[name] = await metric.get_all()
                elif isinstance(metric, Histogram):
                    metrics_data[name] = await metric.get_stats()
                elif isinstance(metric, Summary):
                    metrics_data[name] = await metric.get_stats()

        return metrics_data


class ResourceMonitor:
    """
    System resource monitoring.

    Tracks:
    - CPU usage
    - Memory usage
    - Disk I/O
    - Network I/O
    """

    def __init__(self, registry: MetricsRegistry):
        self.registry = registry
        self._monitoring = False
        self._monitor_task = None

    async def start(self, interval: float = 5.0):
        """Start resource monitoring"""
        if self._monitoring:
            return

        self._monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop(interval))
        logger.info("Resource monitoring started")

    async def stop(self):
        """Stop resource monitoring"""
        self._monitoring = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Resource monitoring stopped")

    async def _monitor_loop(self, interval: float):
        """Monitor loop"""
        # Get or create gauges
        cpu_gauge = await self.registry.get_or_create_gauge(
            "system_cpu_percent",
            "CPU utilization percentage"
        )
        memory_gauge = await self.registry.get_or_create_gauge(
            "system_memory_percent",
            "Memory utilization percentage"
        )
        memory_bytes_gauge = await self.registry.get_or_create_gauge(
            "system_memory_bytes",
            "Memory usage in bytes"
        )

        while self._monitoring:
            try:
                # CPU
                cpu_percent = psutil.cpu_percent(interval=1)
                await cpu_gauge.set(cpu_percent)

                # Memory
                memory = psutil.virtual_memory()
                await memory_gauge.set(memory.percent)
                await memory_bytes_gauge.set(memory.used)

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                await asyncio.sleep(interval)


# Global registry
_registry: Optional[MetricsRegistry] = None


def get_metrics_registry() -> MetricsRegistry:
    """Get or create global metrics registry"""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry()
    return _registry
