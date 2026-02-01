"""
Execution Memory - Learn from past executions.

Provides:
- Semantic result caching (similar queries hit cache)
- Execution pattern learning (common tool sequences)
- Performance statistics (latency, success rate by context)
- Cost optimization (choose cheaper tools when equivalent)
- Failure prediction (skip tools likely to fail)
"""

from __future__ import annotations
import hashlib
import json
import time
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from enum import Enum
from collections import defaultdict
import asyncio


class ExecutionOutcome(str, Enum):
    """Possible execution outcomes."""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


@dataclass
class ExecutionRecord:
    """Record of a single tool execution."""
    execution_id: str
    tool_id: str
    tool_name: str
    input_hash: str
    input_data: Dict[str, Any]
    output_data: Optional[Any]
    outcome: ExecutionOutcome
    latency_ms: float
    cost: float
    timestamp: datetime
    context: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "tool_id": self.tool_id,
            "tool_name": self.tool_name,
            "input_hash": self.input_hash,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "outcome": self.outcome.value,
            "latency_ms": self.latency_ms,
            "cost": self.cost,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
            "error": self.error,
        }


@dataclass
class ToolStats:
    """Aggregated statistics for a tool."""
    tool_id: str
    total_executions: int = 0
    success_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0
    total_latency_ms: float = 0
    total_cost: float = 0
    last_execution: Optional[datetime] = None
    # Exponential moving averages
    ema_latency_ms: float = 0
    ema_success_rate: float = 1.0

    @property
    def success_rate(self) -> float:
        if self.total_executions == 0:
            return 0.0
        return self.success_count / self.total_executions

    @property
    def avg_latency_ms(self) -> float:
        if self.total_executions == 0:
            return 0.0
        return self.total_latency_ms / self.total_executions

    @property
    def avg_cost(self) -> float:
        if self.total_executions == 0:
            return 0.0
        return self.total_cost / self.total_executions

    def update(self, record: ExecutionRecord, alpha: float = 0.1):
        """Update stats with new execution record."""
        self.total_executions += 1
        self.total_latency_ms += record.latency_ms
        self.total_cost += record.cost
        self.last_execution = record.timestamp

        if record.outcome == ExecutionOutcome.SUCCESS:
            self.success_count += 1
        elif record.outcome == ExecutionOutcome.FAILURE:
            self.failure_count += 1
        elif record.outcome == ExecutionOutcome.TIMEOUT:
            self.timeout_count += 1

        # Update EMAs
        self.ema_latency_ms = alpha * record.latency_ms + (1 - alpha) * self.ema_latency_ms
        success = 1.0 if record.outcome == ExecutionOutcome.SUCCESS else 0.0
        self.ema_success_rate = alpha * success + (1 - alpha) * self.ema_success_rate


@dataclass
class SequencePattern:
    """A learned pattern of tool sequences."""
    sequence: Tuple[str, ...]  # tool_ids in order
    count: int = 0
    avg_total_latency_ms: float = 0
    success_rate: float = 0
    contexts: List[Dict[str, Any]] = field(default_factory=list)

    def update(self, latency_ms: float, success: bool, context: Dict[str, Any]):
        old_count = self.count
        self.count += 1

        # Running average
        self.avg_total_latency_ms = (
            (old_count * self.avg_total_latency_ms + latency_ms) / self.count
        )
        self.success_rate = (
            (old_count * self.success_rate + (1.0 if success else 0.0)) / self.count
        )

        # Keep sample contexts
        if len(self.contexts) < 10:
            self.contexts.append(context)


class SemanticCache:
    """
    Cache that matches on semantic similarity of inputs.

    Unlike exact-match caching, this returns cached results
    for inputs that are semantically similar.
    """

    def __init__(
        self,
        similarity_threshold: float = 0.95,
        max_entries: int = 10000,
        ttl_seconds: int = 3600,
    ):
        self.threshold = similarity_threshold
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds

        self._entries: Dict[str, Dict[str, Any]] = {}
        self._vectors: Dict[str, List[float]] = {}
        self._access_times: Dict[str, float] = {}

    def _compute_input_vector(self, input_data: Dict[str, Any]) -> List[float]:
        """Compute a vector representation of input data."""
        # Simple approach: hash-based projection
        text = json.dumps(input_data, sort_keys=True)

        # Character n-gram features
        n = 3
        features: Dict[int, float] = {}
        for i in range(len(text) - n + 1):
            gram = text[i:i+n]
            idx = int(hashlib.md5(gram.encode()).hexdigest(), 16) % 1024
            features[idx] = features.get(idx, 0) + 1

        # Normalize
        total = sum(features.values())
        if total > 0:
            features = {k: v / total for k, v in features.items()}

        # Convert to dense vector
        vector = [features.get(i, 0) for i in range(1024)]
        return vector

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def get(
        self,
        tool_id: str,
        input_data: Dict[str, Any]
    ) -> Optional[Any]:
        """Get cached result for similar input."""
        now = time.time()
        query_vec = self._compute_input_vector(input_data)

        best_match = None
        best_sim = 0

        # Find most similar entry for this tool
        for key, entry in list(self._entries.items()):
            if not key.startswith(f"{tool_id}:"):
                continue

            # Check TTL
            if now - self._access_times.get(key, 0) > self.ttl_seconds:
                del self._entries[key]
                if key in self._vectors:
                    del self._vectors[key]
                continue

            vec = self._vectors.get(key)
            if vec is None:
                continue

            sim = self._cosine_similarity(query_vec, vec)
            if sim > best_sim and sim >= self.threshold:
                best_sim = sim
                best_match = entry

        if best_match:
            return best_match.get("output")

        return None

    def set(
        self,
        tool_id: str,
        input_data: Dict[str, Any],
        output: Any,
    ):
        """Cache a result."""
        # Evict if at capacity
        if len(self._entries) >= self.max_entries:
            # Remove oldest entries
            sorted_keys = sorted(
                self._access_times.keys(),
                key=lambda k: self._access_times[k]
            )
            for key in sorted_keys[:len(sorted_keys) // 4]:
                del self._entries[key]
                if key in self._vectors:
                    del self._vectors[key]
                del self._access_times[key]

        # Add entry
        input_hash = hashlib.md5(
            json.dumps(input_data, sort_keys=True).encode()
        ).hexdigest()[:16]
        key = f"{tool_id}:{input_hash}"

        self._entries[key] = {
            "input": input_data,
            "output": output,
        }
        self._vectors[key] = self._compute_input_vector(input_data)
        self._access_times[key] = time.time()

    def invalidate(self, tool_id: Optional[str] = None):
        """Invalidate cache entries."""
        if tool_id:
            keys_to_remove = [
                k for k in self._entries.keys()
                if k.startswith(f"{tool_id}:")
            ]
        else:
            keys_to_remove = list(self._entries.keys())

        for key in keys_to_remove:
            del self._entries[key]
            if key in self._vectors:
                del self._vectors[key]
            if key in self._access_times:
                del self._access_times[key]


class ExecutionMemory:
    """
    Central memory system for learning from executions.

    Features:
    - Execution history with queryable records
    - Per-tool statistics with trend detection
    - Sequence pattern learning
    - Semantic caching
    - Failure prediction
    - Cost optimization recommendations
    """

    def __init__(
        self,
        max_history: int = 100000,
        cache_similarity_threshold: float = 0.95,
    ):
        self.max_history = max_history

        # Execution records
        self._records: List[ExecutionRecord] = []
        self._records_by_tool: Dict[str, List[ExecutionRecord]] = defaultdict(list)

        # Tool statistics
        self._tool_stats: Dict[str, ToolStats] = {}

        # Sequence patterns
        self._sequences: Dict[Tuple[str, ...], SequencePattern] = {}
        self._active_sequences: Dict[str, List[str]] = {}  # session_id -> tool_ids

        # Semantic cache
        self.cache = SemanticCache(
            similarity_threshold=cache_similarity_threshold
        )

        # Failure patterns
        self._failure_patterns: Dict[str, Dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )  # tool_id -> error_type -> count

        # Tool equivalence (for cost optimization)
        self._equivalents: Dict[str, Set[str]] = defaultdict(set)

    def record_execution(
        self,
        execution_id: str,
        tool_id: str,
        tool_name: str,
        input_data: Dict[str, Any],
        output_data: Optional[Any],
        outcome: ExecutionOutcome,
        latency_ms: float,
        cost: float = 0.0,
        context: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> ExecutionRecord:
        """Record an execution and update statistics."""
        input_hash = hashlib.md5(
            json.dumps(input_data, sort_keys=True).encode()
        ).hexdigest()[:16]

        record = ExecutionRecord(
            execution_id=execution_id,
            tool_id=tool_id,
            tool_name=tool_name,
            input_hash=input_hash,
            input_data=input_data,
            output_data=output_data,
            outcome=outcome,
            latency_ms=latency_ms,
            cost=cost,
            timestamp=datetime.utcnow(),
            context=context or {},
            error=error,
        )

        # Store record
        self._records.append(record)
        self._records_by_tool[tool_id].append(record)

        # Trim history
        if len(self._records) > self.max_history:
            removed = self._records[:len(self._records) - self.max_history]
            self._records = self._records[-self.max_history:]
            for r in removed:
                if r.tool_id in self._records_by_tool:
                    self._records_by_tool[r.tool_id] = [
                        rec for rec in self._records_by_tool[r.tool_id]
                        if rec.execution_id != r.execution_id
                    ]

        # Update tool stats
        if tool_id not in self._tool_stats:
            self._tool_stats[tool_id] = ToolStats(tool_id=tool_id)
        self._tool_stats[tool_id].update(record)

        # Track failure patterns
        if outcome == ExecutionOutcome.FAILURE and error:
            error_type = error.split(":")[0] if ":" in error else error[:50]
            self._failure_patterns[tool_id][error_type] += 1

        # Track sequences
        if session_id:
            if session_id not in self._active_sequences:
                self._active_sequences[session_id] = []
            self._active_sequences[session_id].append(tool_id)

        # Cache successful results
        if outcome == ExecutionOutcome.SUCCESS and output_data is not None:
            self.cache.set(tool_id, input_data, output_data)

        return record

    def end_session(self, session_id: str):
        """End a session and learn sequence pattern."""
        if session_id not in self._active_sequences:
            return

        tool_ids = self._active_sequences.pop(session_id)
        if len(tool_ids) < 2:
            return

        # Record the sequence pattern
        seq_tuple = tuple(tool_ids)
        if seq_tuple not in self._sequences:
            self._sequences[seq_tuple] = SequencePattern(sequence=seq_tuple)

        # Get session stats
        session_records = [
            r for r in self._records[-1000:]
            if r.context.get("session_id") == session_id
        ]
        total_latency = sum(r.latency_ms for r in session_records)
        success = all(r.outcome == ExecutionOutcome.SUCCESS for r in session_records)

        self._sequences[seq_tuple].update(
            latency_ms=total_latency,
            success=success,
            context={}
        )

    def get_tool_stats(self, tool_id: str) -> Optional[ToolStats]:
        """Get statistics for a tool."""
        return self._tool_stats.get(tool_id)

    def get_cached_result(
        self,
        tool_id: str,
        input_data: Dict[str, Any]
    ) -> Optional[Any]:
        """Get cached result for similar input."""
        return self.cache.get(tool_id, input_data)

    def predict_failure_probability(
        self,
        tool_id: str,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Predict probability that tool will fail."""
        stats = self._tool_stats.get(tool_id)
        if not stats or stats.total_executions < 5:
            return 0.0  # Not enough data

        # Base failure rate
        base_rate = 1 - stats.ema_success_rate

        # Adjust based on recent trend
        recent_records = self._records_by_tool.get(tool_id, [])[-20:]
        if len(recent_records) >= 5:
            recent_failures = sum(
                1 for r in recent_records
                if r.outcome != ExecutionOutcome.SUCCESS
            )
            recent_rate = recent_failures / len(recent_records)

            # Weight recent more heavily
            base_rate = 0.3 * base_rate + 0.7 * recent_rate

        return min(base_rate, 1.0)

    def predict_latency(
        self,
        tool_id: str,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Predict execution latency in ms."""
        stats = self._tool_stats.get(tool_id)
        if not stats or stats.total_executions == 0:
            return 1000.0  # Default 1 second

        # Use EMA for prediction (responds to recent changes)
        return stats.ema_latency_ms

    def suggest_sequence(
        self,
        current_tools: List[str],
        available_tools: Set[str],
        k: int = 3,
    ) -> List[Tuple[str, float]]:
        """Suggest next tools based on learned patterns."""
        if not current_tools:
            # Suggest most common starting tools
            starts: Dict[str, int] = defaultdict(int)
            for seq in self._sequences.values():
                if seq.sequence and seq.sequence[0] in available_tools:
                    starts[seq.sequence[0]] += seq.count

            sorted_starts = sorted(starts.items(), key=lambda x: x[1], reverse=True)
            return [(t, c / sum(starts.values())) for t, c in sorted_starts[:k]]

        # Find sequences that match current prefix
        prefix = tuple(current_tools)
        matches: List[Tuple[str, float]] = []

        for seq_tuple, pattern in self._sequences.items():
            # Check if prefix matches
            if len(seq_tuple) > len(prefix):
                if seq_tuple[:len(prefix)] == prefix:
                    next_tool = seq_tuple[len(prefix)]
                    if next_tool in available_tools:
                        # Score by success rate and frequency
                        score = pattern.success_rate * math.log(pattern.count + 1)
                        matches.append((next_tool, score))

        # Aggregate scores for same tool
        tool_scores: Dict[str, float] = defaultdict(float)
        for tool, score in matches:
            tool_scores[tool] += score

        sorted_tools = sorted(tool_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_tools[:k]

    def find_cheaper_alternative(
        self,
        tool_id: str,
        required_success_rate: float = 0.9,
    ) -> Optional[Tuple[str, float]]:
        """Find a cheaper tool that achieves similar results."""
        if tool_id not in self._equivalents:
            return None

        current_stats = self._tool_stats.get(tool_id)
        if not current_stats:
            return None

        best_alt = None
        best_savings = 0

        for alt_id in self._equivalents[tool_id]:
            alt_stats = self._tool_stats.get(alt_id)
            if not alt_stats:
                continue

            if alt_stats.success_rate < required_success_rate:
                continue

            savings = current_stats.avg_cost - alt_stats.avg_cost
            if savings > best_savings:
                best_alt = alt_id
                best_savings = savings

        if best_alt:
            return (best_alt, best_savings)
        return None

    def register_equivalent_tools(self, tool_ids: List[str]):
        """Register tools as functionally equivalent (for cost optimization)."""
        for t1 in tool_ids:
            for t2 in tool_ids:
                if t1 != t2:
                    self._equivalents[t1].add(t2)

    def get_hot_tools(
        self,
        window_seconds: int = 3600,
        k: int = 10,
    ) -> List[Tuple[str, int]]:
        """Get most frequently used tools in recent window."""
        cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)

        counts: Dict[str, int] = defaultdict(int)
        for record in reversed(self._records):
            if record.timestamp < cutoff:
                break
            counts[record.tool_id] += 1

        sorted_tools = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_tools[:k]

    def get_failing_tools(
        self,
        window_seconds: int = 3600,
        min_executions: int = 5,
    ) -> List[Tuple[str, float]]:
        """Get tools with high failure rates in recent window."""
        cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)

        tool_results: Dict[str, List[bool]] = defaultdict(list)
        for record in reversed(self._records):
            if record.timestamp < cutoff:
                break
            tool_results[record.tool_id].append(
                record.outcome == ExecutionOutcome.SUCCESS
            )

        failing = []
        for tool_id, results in tool_results.items():
            if len(results) >= min_executions:
                fail_rate = 1 - (sum(results) / len(results))
                if fail_rate > 0.1:  # More than 10% failure
                    failing.append((tool_id, fail_rate))

        failing.sort(key=lambda x: x[1], reverse=True)
        return failing

    def export_stats(self) -> Dict[str, Any]:
        """Export all statistics."""
        return {
            "total_executions": len(self._records),
            "tools": {
                tool_id: {
                    "total": stats.total_executions,
                    "success_rate": stats.success_rate,
                    "avg_latency_ms": stats.avg_latency_ms,
                    "avg_cost": stats.avg_cost,
                    "ema_success_rate": stats.ema_success_rate,
                    "ema_latency_ms": stats.ema_latency_ms,
                }
                for tool_id, stats in self._tool_stats.items()
            },
            "patterns": {
                "->".join(seq): {
                    "count": pattern.count,
                    "avg_latency_ms": pattern.avg_total_latency_ms,
                    "success_rate": pattern.success_rate,
                }
                for seq, pattern in self._sequences.items()
            },
            "cache_size": len(self.cache._entries),
        }
