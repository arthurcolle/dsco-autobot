"""
Metrics and Telemetry API Routes

Provides endpoints for accessing metrics, statistics, and performance data.
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
import logging

from parallel_websearch_server.telemetry import get_metrics_registry, ResourceMonitor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/metrics", tags=["metrics"])

# Global resource monitor
_resource_monitor: Optional[ResourceMonitor] = None


def get_resource_monitor() -> ResourceMonitor:
    """Get or create resource monitor"""
    global _resource_monitor
    if _resource_monitor is None:
        registry = get_metrics_registry()
        _resource_monitor = ResourceMonitor(registry)
    return _resource_monitor


@router.get("/")
async def get_all_metrics():
    """
    Get all registered metrics.

    Returns:
        All metrics with their current values
    """
    registry = get_metrics_registry()
    metrics = await registry.collect()

    return {
        "metrics": metrics,
        "timestamp": time.time()
    }


@router.get("/{metric_name}")
async def get_metric(metric_name: str):
    """
    Get specific metric by name.

    Args:
        metric_name: Name of the metric

    Returns:
        Metric data
    """
    registry = get_metrics_registry()
    metric = await registry.get_metric(metric_name)

    if metric is None:
        raise HTTPException(status_code=404, detail=f"Metric not found: {metric_name}")

    # Collect metric data
    if isinstance(metric, (Counter, Gauge)):
        values = await metric.get_all()
        return {
            "name": metric.name,
            "type": metric.metric_type,
            "description": metric.description,
            "values": [
                {"value": v.value, "timestamp": v.timestamp, "labels": v.labels}
                for v in values
            ]
        }
    elif isinstance(metric, (Histogram, Summary)):
        stats = await metric.get_stats()
        return {
            "name": metric.name,
            "type": metric.metric_type,
            "description": metric.description,
            "statistics": stats
        }

    return {"error": "Unknown metric type"}


@router.post("/resource-monitor/start")
async def start_resource_monitoring(interval: float = 5.0):
    """
    Start system resource monitoring.

    Args:
        interval: Monitoring interval in seconds

    Returns:
        Confirmation
    """
    monitor = get_resource_monitor()
    await monitor.start(interval)

    return {
        "monitoring": True,
        "interval": interval
    }


@router.post("/resource-monitor/stop")
async def stop_resource_monitoring():
    """
    Stop system resource monitoring.

    Returns:
        Confirmation
    """
    monitor = get_resource_monitor()
    await monitor.stop()

    return {
        "monitoring": False
    }


@router.get("/resource-monitor/status")
async def get_resource_monitor_status():
    """
    Get resource monitor status.

    Returns:
        Current monitoring status and latest metrics
    """
    monitor = get_resource_monitor()
    registry = get_metrics_registry()

    # Get system metrics
    cpu_metric = await registry.get_metric("system_cpu_percent")
    memory_metric = await registry.get_metric("system_memory_percent")
    memory_bytes_metric = await registry.get_metric("system_memory_bytes")

    cpu_value = 0.0
    memory_value = 0.0
    memory_bytes = 0

    if cpu_metric:
        cpu_value = await cpu_metric.get()
    if memory_metric:
        memory_value = await memory_metric.get()
    if memory_bytes_metric:
        memory_bytes = await memory_bytes_metric.get()

    return {
        "monitoring": monitor._monitoring,
        "cpu_percent": cpu_value,
        "memory_percent": memory_value,
        "memory_bytes": memory_bytes
    }


@router.get("/health")
async def metrics_health():
    """
    Health check for metrics system.

    Returns:
        Health status
    """
    registry = get_metrics_registry()

    async with registry._lock:
        metric_count = len(registry._metrics)

    return {
        "status": "healthy",
        "registered_metrics": metric_count
    }


# Import time for timestamp
import time
# Import metric types for isinstance checks
from parallel_websearch_server.telemetry import Counter, Gauge, Histogram, Summary
