"""
Monitoring and Observability for Agent Swarms.

Provides:
- Real-time metrics collection
- Alert system
- Audit logging
- Performance dashboards
- Anomaly detection
- Health checks

Based on observability best practices:
- Metrics (what's happening)
- Logs (what happened)
- Traces (how it happened)
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import logging
import statistics

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics."""
    COUNTER = "counter"       # Monotonically increasing
    GAUGE = "gauge"           # Can go up or down
    HISTOGRAM = "histogram"   # Distribution of values
    SUMMARY = "summary"       # Similar to histogram


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    """Alert status."""
    FIRING = "firing"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"


class AuditAction(str, Enum):
    """Types of audit actions."""
    # Agent actions
    AGENT_REGISTERED = "agent_registered"
    AGENT_STAKED = "agent_staked"
    AGENT_UNSTAKED = "agent_unstaked"
    AGENT_SLASHED = "agent_slashed"

    # Task actions
    TASK_CREATED = "task_created"
    TASK_ASSIGNED = "task_assigned"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"

    # Financial actions
    PAYMENT_SENT = "payment_sent"
    PAYMENT_RECEIVED = "payment_received"
    FEE_COLLECTED = "fee_collected"
    BUDGET_EXCEEDED = "budget_exceeded"

    # Security actions
    WARRANT_MINTED = "warrant_minted"
    WARRANT_VALIDATED = "warrant_validated"
    WARRANT_REVOKED = "warrant_revoked"
    ACCESS_DENIED = "access_denied"

    # Swarm actions
    SWARM_CREATED = "swarm_created"
    SWARM_FROZEN = "swarm_frozen"
    COALITION_FORMED = "coalition_formed"


@dataclass
class Metric:
    """A single metric."""
    name: str
    type: MetricType
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.value,
            "value": self.value,
            "labels": self.labels,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class Alert:
    """An alert triggered by a condition."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    severity: AlertSeverity = AlertSeverity.WARNING
    status: AlertStatus = AlertStatus.FIRING

    # Trigger info
    condition: str = ""
    threshold: float = 0.0
    actual_value: float = 0.0

    # Context
    swarm_id: Optional[str] = None
    agent_id: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)

    # Timing
    fired_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None

    # Actions taken
    actions: List[str] = field(default_factory=list)

    def resolve(self):
        """Mark alert as resolved."""
        self.status = AlertStatus.RESOLVED
        self.resolved_at = datetime.now(timezone.utc)

    def acknowledge(self, user_id: str):
        """Acknowledge the alert."""
        self.status = AlertStatus.ACKNOWLEDGED
        self.acknowledged_at = datetime.now(timezone.utc)
        self.acknowledged_by = user_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "severity": self.severity.value,
            "status": self.status.value,
            "condition": self.condition,
            "threshold": self.threshold,
            "actual_value": self.actual_value,
            "swarm_id": self.swarm_id,
            "agent_id": self.agent_id,
            "labels": self.labels,
            "fired_at": self.fired_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by,
        }


@dataclass
class AuditEntry:
    """An entry in the audit log."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action: AuditAction = AuditAction.TASK_CREATED
    description: str = ""

    # Actor
    actor_type: str = ""  # "agent", "user", "system"
    actor_id: str = ""

    # Subject
    subject_type: str = ""  # "task", "agent", "wallet", etc.
    subject_id: str = ""

    # Context
    swarm_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Result
    success: bool = True
    error: Optional[str] = None

    # Timing
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Signature (for non-repudiation)
    signature: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "action": self.action.value,
            "description": self.description,
            "actor_type": self.actor_type,
            "actor_id": self.actor_id,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "swarm_id": self.swarm_id,
            "metadata": self.metadata,
            "success": self.success,
            "error": self.error,
            "timestamp": self.timestamp.isoformat(),
        }


class MetricsCollector:
    """
    Collects and aggregates metrics.

    Features:
    - Counter, gauge, histogram support
    - Label-based filtering
    - Time-based aggregation
    - Export to Prometheus format
    """

    def __init__(self):
        # Metric storage
        self.counters: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.gauges: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.histograms: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

        # Time series (recent values)
        self.time_series: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)
        self.max_series_length = 1000

    def _labels_key(self, labels: Dict[str, str]) -> str:
        """Create a key from labels."""
        return json.dumps(labels, sort_keys=True)

    def increment(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter."""
        labels = labels or {}
        key = self._labels_key(labels)
        self.counters[name][key] += value

        # Record in time series
        self._record_time_series(name, self.counters[name][key], labels)

    def gauge_set(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge value."""
        labels = labels or {}
        key = self._labels_key(labels)
        self.gauges[name][key] = value

        self._record_time_series(name, value, labels)

    def observe(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record an observation for a histogram."""
        labels = labels or {}
        key = self._labels_key(labels)
        self.histograms[name][key].append(value)

        # Keep only recent observations
        if len(self.histograms[name][key]) > self.max_series_length:
            self.histograms[name][key] = self.histograms[name][key][-self.max_series_length:]

        self._record_time_series(name, value, labels)

    def _record_time_series(self, name: str, value: float, labels: Dict[str, str]):
        """Record value in time series."""
        key = f"{name}:{self._labels_key(labels)}"
        self.time_series[key].append((datetime.now(timezone.utc), value))

        if len(self.time_series[key]) > self.max_series_length:
            self.time_series[key] = self.time_series[key][-self.max_series_length:]

    def get_counter(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get counter value."""
        labels = labels or {}
        key = self._labels_key(labels)
        return self.counters[name].get(key, 0.0)

    def get_gauge(self, name: str, labels: Dict[str, str] = None) -> Optional[float]:
        """Get gauge value."""
        labels = labels or {}
        key = self._labels_key(labels)
        return self.gauges[name].get(key)

    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None) -> Dict[str, float]:
        """Get histogram statistics."""
        labels = labels or {}
        key = self._labels_key(labels)
        values = self.histograms[name].get(key, [])

        if not values:
            return {}

        return {
            "count": len(values),
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "p95": values[int(len(values) * 0.95)] if len(values) >= 20 else max(values),
            "p99": values[int(len(values) * 0.99)] if len(values) >= 100 else max(values),
        }

    def get_time_series(
        self,
        name: str,
        labels: Dict[str, str] = None,
        since: Optional[datetime] = None,
    ) -> List[Tuple[datetime, float]]:
        """Get time series data."""
        labels = labels or {}
        key = f"{name}:{self._labels_key(labels)}"
        series = self.time_series.get(key, [])

        if since:
            series = [(t, v) for t, v in series if t >= since]

        return series

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        for name, label_values in self.counters.items():
            lines.append(f"# TYPE {name} counter")
            for key, value in label_values.items():
                labels = json.loads(key) if key else {}
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                metric_name = f"{name}{{{label_str}}}" if label_str else name
                lines.append(f"{metric_name} {value}")

        for name, label_values in self.gauges.items():
            lines.append(f"# TYPE {name} gauge")
            for key, value in label_values.items():
                labels = json.loads(key) if key else {}
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                metric_name = f"{name}{{{label_str}}}" if label_str else name
                lines.append(f"{metric_name} {value}")

        return "\n".join(lines)


class AlertManager:
    """
    Manages alerts and notifications.

    Features:
    - Alert rules with conditions
    - Notification routing
    - Alert grouping and deduplication
    - Escalation policies
    """

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector

        # Active alerts
        self.alerts: Dict[str, Alert] = {}

        # Alert rules
        self.rules: List[Dict[str, Any]] = []

        # Notification handlers
        self.handlers: List[Callable[[Alert], None]] = []

        # Alert history
        self.history: List[Alert] = []
        self.max_history = 1000

    def add_rule(
        self,
        name: str,
        metric_name: str,
        condition: str,  # "gt", "lt", "eq", "gte", "lte"
        threshold: float,
        severity: AlertSeverity = AlertSeverity.WARNING,
        labels: Dict[str, str] = None,
        description: str = "",
    ):
        """Add an alert rule."""
        self.rules.append({
            "name": name,
            "metric_name": metric_name,
            "condition": condition,
            "threshold": threshold,
            "severity": severity,
            "labels": labels or {},
            "description": description,
        })

    def register_handler(self, handler: Callable[[Alert], None]):
        """Register a notification handler."""
        self.handlers.append(handler)

    def check_rules(self):
        """Check all alert rules and fire alerts if needed."""
        for rule in self.rules:
            value = self.metrics.get_gauge(
                rule["metric_name"],
                rule.get("labels"),
            )

            if value is None:
                continue

            triggered = False
            condition = rule["condition"]
            threshold = rule["threshold"]

            if condition == "gt" and value > threshold:
                triggered = True
            elif condition == "lt" and value < threshold:
                triggered = True
            elif condition == "gte" and value >= threshold:
                triggered = True
            elif condition == "lte" and value <= threshold:
                triggered = True
            elif condition == "eq" and value == threshold:
                triggered = True

            rule_id = f"{rule['name']}:{json.dumps(rule.get('labels', {}), sort_keys=True)}"

            if triggered and rule_id not in self.alerts:
                alert = Alert(
                    name=rule["name"],
                    description=rule.get("description", ""),
                    severity=rule["severity"],
                    condition=f"{rule['metric_name']} {condition} {threshold}",
                    threshold=threshold,
                    actual_value=value,
                    labels=rule.get("labels", {}),
                )
                self.alerts[rule_id] = alert
                self._fire_alert(alert)

            elif not triggered and rule_id in self.alerts:
                alert = self.alerts.pop(rule_id)
                alert.resolve()
                self.history.append(alert)
                self._notify_handlers(alert)

    def _fire_alert(self, alert: Alert):
        """Fire an alert."""
        logger.warning(f"Alert fired: {alert.name} - {alert.description}")
        self.metrics.increment("swarm_alerts_total", labels={"severity": alert.severity.value})
        self._notify_handlers(alert)

    def _notify_handlers(self, alert: Alert):
        """Notify all registered handlers."""
        for handler in self.handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")

    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active alerts."""
        alerts = list(self.alerts.values())
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts

    def acknowledge_alert(self, alert_id: str, user_id: str) -> bool:
        """Acknowledge an alert."""
        for rule_id, alert in self.alerts.items():
            if alert.id == alert_id:
                alert.acknowledge(user_id)
                return True
        return False


class AuditLogger:
    """
    Audit logging for compliance and forensics.

    Features:
    - Immutable audit trail
    - Signed entries
    - Query capabilities
    - Export for compliance
    """

    def __init__(self):
        self.entries: List[AuditEntry] = []
        self.max_entries = 100000

        # Index by various fields
        self.by_actor: Dict[str, List[str]] = defaultdict(list)
        self.by_subject: Dict[str, List[str]] = defaultdict(list)
        self.by_action: Dict[AuditAction, List[str]] = defaultdict(list)
        self.by_swarm: Dict[str, List[str]] = defaultdict(list)

    def log(
        self,
        action: AuditAction,
        actor_type: str,
        actor_id: str,
        subject_type: str = "",
        subject_id: str = "",
        description: str = "",
        swarm_id: Optional[str] = None,
        metadata: Dict[str, Any] = None,
        success: bool = True,
        error: Optional[str] = None,
    ) -> AuditEntry:
        """Log an audit entry."""
        entry = AuditEntry(
            action=action,
            description=description,
            actor_type=actor_type,
            actor_id=actor_id,
            subject_type=subject_type,
            subject_id=subject_id,
            swarm_id=swarm_id,
            metadata=metadata or {},
            success=success,
            error=error,
        )

        # Sign entry (in production, use proper signing)
        entry.signature = self._sign_entry(entry)

        self.entries.append(entry)

        # Update indexes
        self.by_actor[actor_id].append(entry.id)
        if subject_id:
            self.by_subject[subject_id].append(entry.id)
        self.by_action[action].append(entry.id)
        if swarm_id:
            self.by_swarm[swarm_id].append(entry.id)

        # Trim if needed
        if len(self.entries) > self.max_entries:
            self.entries = self.entries[-self.max_entries:]

        logger.debug(f"Audit: {action.value} by {actor_type}:{actor_id}")
        return entry

    def _sign_entry(self, entry: AuditEntry) -> str:
        """Sign an audit entry (simplified)."""
        import hashlib
        data = json.dumps(entry.to_dict(), sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def query(
        self,
        actor_id: Optional[str] = None,
        subject_id: Optional[str] = None,
        action: Optional[AuditAction] = None,
        swarm_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        """Query audit entries."""
        # Get candidate entry IDs
        candidate_ids: Optional[Set[str]] = None

        if actor_id:
            ids = set(self.by_actor.get(actor_id, []))
            candidate_ids = ids if candidate_ids is None else candidate_ids & ids

        if subject_id:
            ids = set(self.by_subject.get(subject_id, []))
            candidate_ids = ids if candidate_ids is None else candidate_ids & ids

        if action:
            ids = set(self.by_action.get(action, []))
            candidate_ids = ids if candidate_ids is None else candidate_ids & ids

        if swarm_id:
            ids = set(self.by_swarm.get(swarm_id, []))
            candidate_ids = ids if candidate_ids is None else candidate_ids & ids

        # Filter entries
        results = []
        for entry in reversed(self.entries):
            if candidate_ids is not None and entry.id not in candidate_ids:
                continue

            if since and entry.timestamp < since:
                continue

            if until and entry.timestamp > until:
                continue

            results.append(entry)

            if len(results) >= limit:
                break

        return results

    def export(
        self,
        format: str = "json",
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> str:
        """Export audit log."""
        entries = self.query(since=since, until=until, limit=self.max_entries)

        if format == "json":
            return json.dumps([e.to_dict() for e in entries], indent=2)
        elif format == "csv":
            lines = ["id,timestamp,action,actor_type,actor_id,subject_type,subject_id,success"]
            for e in entries:
                lines.append(
                    f"{e.id},{e.timestamp.isoformat()},{e.action.value},"
                    f"{e.actor_type},{e.actor_id},{e.subject_type},{e.subject_id},{e.success}"
                )
            return "\n".join(lines)
        else:
            raise ValueError(f"Unknown format: {format}")


class HealthChecker:
    """
    Health checking for swarm components.

    Checks:
    - Agent availability
    - Wallet connectivity
    - Task queue health
    - Economic system health
    """

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.checks: Dict[str, Callable[[], Dict[str, Any]]] = {}
        self.last_results: Dict[str, Dict[str, Any]] = {}

    def register_check(self, name: str, check_fn: Callable[[], Dict[str, Any]]):
        """Register a health check."""
        self.checks[name] = check_fn

    async def run_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all health checks."""
        results = {}

        for name, check_fn in self.checks.items():
            try:
                start = time.time()
                if asyncio.iscoroutinefunction(check_fn):
                    result = await check_fn()
                else:
                    result = check_fn()
                duration = time.time() - start

                results[name] = {
                    "status": result.get("status", "unknown"),
                    "details": result,
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                # Record metrics
                status_value = 1.0 if result.get("status") == "healthy" else 0.0
                self.metrics.gauge_set(f"health_check_{name}", status_value)
                self.metrics.observe(f"health_check_{name}_duration_ms", duration * 1000)

            except Exception as e:
                results[name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self.metrics.gauge_set(f"health_check_{name}", 0.0)

        self.last_results = results
        return results

    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall health status."""
        if not self.last_results:
            return {"status": "unknown", "message": "No health checks run yet"}

        unhealthy = [
            name for name, result in self.last_results.items()
            if result.get("status") != "healthy"
        ]

        if not unhealthy:
            return {"status": "healthy", "checks": len(self.last_results)}
        else:
            return {
                "status": "unhealthy",
                "unhealthy_checks": unhealthy,
                "total_checks": len(self.last_results),
            }


class SwarmMonitor:
    """
    Unified monitoring for the swarm.

    Combines:
    - Metrics collection
    - Alert management
    - Audit logging
    - Health checking
    """

    def __init__(self):
        self.metrics = MetricsCollector()
        self.alerts = AlertManager(self.metrics)
        self.audit = AuditLogger()
        self.health = HealthChecker(self.metrics)

        # Set up default alert rules
        self._setup_default_rules()

        # Set up default health checks
        self._setup_default_health_checks()

    def _setup_default_rules(self):
        """Set up default alert rules."""
        self.alerts.add_rule(
            name="high_task_failure_rate",
            metric_name="swarm_task_failure_rate",
            condition="gt",
            threshold=0.2,
            severity=AlertSeverity.WARNING,
            description="Task failure rate exceeds 20%",
        )

        self.alerts.add_rule(
            name="low_agent_availability",
            metric_name="swarm_idle_agent_ratio",
            condition="lt",
            threshold=0.1,
            severity=AlertSeverity.WARNING,
            description="Less than 10% of agents are idle",
        )

        self.alerts.add_rule(
            name="budget_exhausted",
            metric_name="swarm_budget_remaining_ratio",
            condition="lt",
            threshold=0.1,
            severity=AlertSeverity.ERROR,
            description="Less than 10% of budget remaining",
        )

        self.alerts.add_rule(
            name="high_risk_score",
            metric_name="swarm_risk_score",
            condition="gt",
            threshold=0.7,
            severity=AlertSeverity.ERROR,
            description="Overall risk score exceeds 0.7",
        )

    def _setup_default_health_checks(self):
        """Set up default health checks."""
        def check_metrics():
            # Check if metrics are being recorded
            counter_count = len(self.metrics.counters)
            gauge_count = len(self.metrics.gauges)

            return {
                "status": "healthy" if counter_count + gauge_count > 0 else "degraded",
                "counters": counter_count,
                "gauges": gauge_count,
            }

        def check_alerts():
            active = len(self.alerts.get_active_alerts())
            critical = len(self.alerts.get_active_alerts(AlertSeverity.CRITICAL))

            status = "healthy"
            if critical > 0:
                status = "critical"
            elif active > 5:
                status = "degraded"

            return {
                "status": status,
                "active_alerts": active,
                "critical_alerts": critical,
            }

        self.health.register_check("metrics", check_metrics)
        self.health.register_check("alerts", check_alerts)

    def record_task_completion(
        self,
        swarm_id: str,
        task_id: str,
        agent_id: str,
        success: bool,
        duration_seconds: float,
        bounty: Decimal,
    ):
        """Record task completion metrics."""
        labels = {"swarm_id": swarm_id}

        self.metrics.increment("swarm_tasks_total", labels=labels)
        if success:
            self.metrics.increment("swarm_tasks_completed", labels=labels)
        else:
            self.metrics.increment("swarm_tasks_failed", labels=labels)

        self.metrics.observe("swarm_task_duration_seconds", duration_seconds, labels=labels)
        self.metrics.observe("swarm_task_bounty", float(bounty), labels=labels)

        # Update failure rate gauge
        completed = self.metrics.get_counter("swarm_tasks_completed", labels)
        failed = self.metrics.get_counter("swarm_tasks_failed", labels)
        total = completed + failed
        if total > 0:
            self.metrics.gauge_set("swarm_task_failure_rate", failed / total, labels=labels)

        # Audit log
        self.audit.log(
            action=AuditAction.TASK_COMPLETED if success else AuditAction.TASK_FAILED,
            actor_type="agent",
            actor_id=agent_id,
            subject_type="task",
            subject_id=task_id,
            swarm_id=swarm_id,
            metadata={"duration": duration_seconds, "bounty": str(bounty)},
            success=success,
        )

    def record_payment(
        self,
        swarm_id: str,
        from_id: str,
        to_id: str,
        amount: Decimal,
        fee: Decimal,
    ):
        """Record payment metrics."""
        labels = {"swarm_id": swarm_id}

        self.metrics.increment("swarm_payments_total", labels=labels)
        self.metrics.observe("swarm_payment_amount", float(amount), labels=labels)
        self.metrics.observe("swarm_payment_fee", float(fee), labels=labels)

        # Track total volume
        current_volume = self.metrics.get_counter("swarm_payment_volume", labels)
        self.metrics.increment("swarm_payment_volume", float(amount), labels=labels)

        # Audit log
        self.audit.log(
            action=AuditAction.PAYMENT_SENT,
            actor_type="agent" if from_id != swarm_id else "swarm",
            actor_id=from_id,
            subject_type="payment",
            subject_id=to_id,
            swarm_id=swarm_id,
            metadata={"amount": str(amount), "fee": str(fee)},
        )

    def get_dashboard(self, swarm_id: str) -> Dict[str, Any]:
        """Get monitoring dashboard data."""
        labels = {"swarm_id": swarm_id}

        return {
            "metrics": {
                "tasks_total": self.metrics.get_counter("swarm_tasks_total", labels),
                "tasks_completed": self.metrics.get_counter("swarm_tasks_completed", labels),
                "tasks_failed": self.metrics.get_counter("swarm_tasks_failed", labels),
                "failure_rate": self.metrics.get_gauge("swarm_task_failure_rate", labels),
                "payment_volume": self.metrics.get_counter("swarm_payment_volume", labels),
                "task_duration_stats": self.metrics.get_histogram_stats("swarm_task_duration_seconds", labels),
            },
            "alerts": [a.to_dict() for a in self.alerts.get_active_alerts()],
            "health": self.health.get_overall_health(),
            "recent_audit": [
                e.to_dict() for e in self.audit.query(swarm_id=swarm_id, limit=20)
            ],
        }


# Global instance
_monitor = SwarmMonitor()


def get_monitor() -> SwarmMonitor:
    return _monitor
