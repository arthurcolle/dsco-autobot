"""
Metrics and Observability.

Provides:
- Request tracing
- Tool execution metrics
- Backend performance tracking
- Real-time dashboards data
"""

from __future__ import annotations

import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from collections import defaultdict, deque
from contextlib import contextmanager
import threading
import statistics

logger = logging.getLogger(__name__)


@dataclass
class RequestTrace:
    """Trace for a single request through the system."""
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_id: Optional[str] = None
    client_id: Optional[str] = None

    # Timing
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    # Request info
    tool_name: Optional[str] = None
    backend_id: Optional[str] = None
    inputs_hash: Optional[str] = None

    # Response info
    status: str = "pending"  # pending, success, error, timeout
    error: Optional[str] = None
    output_size: int = 0

    # Pipeline info
    pipeline_id: Optional[str] = None
    step_id: Optional[str] = None

    # Spans for sub-operations
    spans: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000

    def start_span(self, name: str, metadata: Dict[str, Any] = None) -> Dict:
        """Start a child span."""
        span = {
            "name": name,
            "start_time": time.time(),
            "end_time": None,
            "duration_ms": 0,
            "metadata": metadata or {},
        }
        self.spans.append(span)
        return span

    def end_span(self, span: Dict):
        """End a child span."""
        span["end_time"] = time.time()
        span["duration_ms"] = (span["end_time"] - span["start_time"]) * 1000

    def complete(self, status: str = "success", error: str = None):
        """Mark trace as complete."""
        self.end_time = time.time()
        self.status = status
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "parent_id": self.parent_id,
            "client_id": self.client_id,
            "tool_name": self.tool_name,
            "backend_id": self.backend_id,
            "status": self.status,
            "duration_ms": round(self.duration_ms, 2),
            "error": self.error,
            "spans": [
                {
                    "name": s["name"],
                    "duration_ms": round(s["duration_ms"], 2),
                }
                for s in self.spans
            ],
            "timestamp": datetime.fromtimestamp(self.start_time, tz=timezone.utc).isoformat(),
        }


@dataclass
class ToolMetrics:
    """Aggregated metrics for a single tool."""
    tool_name: str
    total_calls: int = 0
    success_count: int = 0
    error_count: int = 0
    timeout_count: int = 0

    # Latency tracking
    latencies: deque = field(default_factory=lambda: deque(maxlen=1000))

    # Last N errors for debugging
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=10))

    # Per-backend breakdown
    backend_calls: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Time series (last hour, 1-minute buckets)
    hourly_calls: deque = field(default_factory=lambda: deque(maxlen=60))
    last_bucket_time: float = field(default_factory=time.time)

    def record_call(
        self,
        backend_id: str,
        latency_ms: float,
        success: bool,
        error: str = None,
        is_timeout: bool = False,
    ):
        """Record a tool execution."""
        self.total_calls += 1
        self.backend_calls[backend_id] += 1
        self.latencies.append(latency_ms)

        if success:
            self.success_count += 1
        else:
            self.error_count += 1
            if error:
                self.recent_errors.append({
                    "time": time.time(),
                    "error": error[:200],
                    "backend": backend_id,
                })
            if is_timeout:
                self.timeout_count += 1

        # Update hourly buckets
        self._update_hourly_bucket()

    def _update_hourly_bucket(self):
        """Update time-series buckets."""
        now = time.time()
        if now - self.last_bucket_time >= 60:
            self.hourly_calls.append({
                "time": int(now),
                "calls": 1,
            })
            self.last_bucket_time = now
        elif self.hourly_calls:
            self.hourly_calls[-1]["calls"] += 1

    @property
    def error_rate(self) -> float:
        if self.total_calls == 0:
            return 0
        return self.error_count / self.total_calls

    @property
    def avg_latency(self) -> float:
        if not self.latencies:
            return 0
        return statistics.mean(self.latencies)

    @property
    def p95_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "total_calls": self.total_calls,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "timeout_count": self.timeout_count,
            "error_rate": f"{self.error_rate:.2%}",
            "avg_latency_ms": round(self.avg_latency, 2),
            "p95_latency_ms": round(self.p95_latency, 2),
            "backend_calls": dict(self.backend_calls),
            "recent_errors": list(self.recent_errors),
        }


class MetricsCollector:
    """
    Central metrics collection and aggregation.

    Provides:
    - Request tracing
    - Tool metrics aggregation
    - Real-time stats
    - Export for monitoring systems
    """

    def __init__(self, max_traces: int = 10000):
        self.max_traces = max_traces
        self._traces: deque = deque(maxlen=max_traces)
        self._tool_metrics: Dict[str, ToolMetrics] = {}
        self._lock = threading.RLock()

        # Global counters
        self._total_requests = 0
        self._total_errors = 0
        self._start_time = time.time()

        # Active traces
        self._active_traces: Dict[str, RequestTrace] = {}

    def start_trace(
        self,
        tool_name: str = None,
        client_id: str = None,
        parent_id: str = None,
        **kwargs,
    ) -> RequestTrace:
        """Start a new request trace."""
        trace = RequestTrace(
            tool_name=tool_name,
            client_id=client_id,
            parent_id=parent_id,
            **kwargs,
        )

        with self._lock:
            self._active_traces[trace.trace_id] = trace
            self._total_requests += 1

        return trace

    def end_trace(
        self,
        trace: RequestTrace,
        status: str = "success",
        error: str = None,
        backend_id: str = None,
    ):
        """Complete a trace and record metrics."""
        trace.complete(status, error)
        if backend_id:
            trace.backend_id = backend_id

        with self._lock:
            # Remove from active
            self._active_traces.pop(trace.trace_id, None)

            # Add to history
            self._traces.append(trace)

            # Update tool metrics
            if trace.tool_name:
                if trace.tool_name not in self._tool_metrics:
                    self._tool_metrics[trace.tool_name] = ToolMetrics(trace.tool_name)

                self._tool_metrics[trace.tool_name].record_call(
                    backend_id=backend_id or "unknown",
                    latency_ms=trace.duration_ms,
                    success=(status == "success"),
                    error=error,
                    is_timeout=(status == "timeout"),
                )

            # Update global counters
            if status != "success":
                self._total_errors += 1

    @contextmanager
    def trace(self, tool_name: str, client_id: str = None, **kwargs):
        """Context manager for tracing."""
        trace = self.start_trace(tool_name=tool_name, client_id=client_id, **kwargs)
        try:
            yield trace
            self.end_trace(trace, status="success")
        except Exception as e:
            self.end_trace(trace, status="error", error=str(e))
            raise

    def get_tool_metrics(self, tool_name: str = None) -> Dict[str, Any]:
        """Get metrics for one or all tools."""
        with self._lock:
            if tool_name:
                if tool_name in self._tool_metrics:
                    return self._tool_metrics[tool_name].to_dict()
                return {}

            return {
                name: m.to_dict()
                for name, m in self._tool_metrics.items()
            }

    def get_recent_traces(self, limit: int = 100, tool_name: str = None) -> List[Dict]:
        """Get recent traces."""
        with self._lock:
            traces = list(self._traces)[-limit:]
            if tool_name:
                traces = [t for t in traces if t.tool_name == tool_name]
            return [t.to_dict() for t in traces]

    def get_active_traces(self) -> List[Dict]:
        """Get currently active traces."""
        with self._lock:
            return [t.to_dict() for t in self._active_traces.values()]

    def get_summary(self) -> Dict[str, Any]:
        """Get overall metrics summary."""
        with self._lock:
            uptime = time.time() - self._start_time
            rps = self._total_requests / uptime if uptime > 0 else 0

            # Top tools by calls
            top_tools = sorted(
                self._tool_metrics.items(),
                key=lambda x: x[1].total_calls,
                reverse=True,
            )[:10]

            # Error-prone tools
            error_tools = sorted(
                [(n, m) for n, m in self._tool_metrics.items() if m.error_count > 0],
                key=lambda x: x[1].error_rate,
                reverse=True,
            )[:5]

            return {
                "uptime_seconds": round(uptime, 0),
                "total_requests": self._total_requests,
                "total_errors": self._total_errors,
                "error_rate": f"{self._total_errors / self._total_requests:.2%}" if self._total_requests else "0%",
                "requests_per_second": round(rps, 2),
                "active_traces": len(self._active_traces),
                "tools_tracked": len(self._tool_metrics),
                "top_tools": [
                    {"name": n, "calls": m.total_calls}
                    for n, m in top_tools
                ],
                "error_prone_tools": [
                    {"name": n, "error_rate": f"{m.error_rate:.2%}"}
                    for n, m in error_tools
                ],
            }

    def get_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        # Global metrics
        lines.append(f"tool_gateway_requests_total {self._total_requests}")
        lines.append(f"tool_gateway_errors_total {self._total_errors}")
        lines.append(f"tool_gateway_active_traces {len(self._active_traces)}")

        # Per-tool metrics
        for name, m in self._tool_metrics.items():
            safe_name = name.replace("-", "_").replace(":", "_")
            lines.append(f'tool_calls_total{{tool="{safe_name}"}} {m.total_calls}')
            lines.append(f'tool_errors_total{{tool="{safe_name}"}} {m.error_count}')
            lines.append(f'tool_latency_avg_ms{{tool="{safe_name}"}} {m.avg_latency:.2f}')
            lines.append(f'tool_latency_p95_ms{{tool="{safe_name}"}} {m.p95_latency:.2f}')

        return "\n".join(lines)


# Singleton instance
_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create the global metrics collector."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector
