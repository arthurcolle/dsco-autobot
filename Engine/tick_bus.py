"""
Engine API Tick Bus

High-performance tick generation and distribution.

Design goals:
- Sub-millisecond tick jitter
- 100k+ ticks/sec throughput
- Fair distribution across agents
- Priority-aware scheduling
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Callable, Awaitable, Any, Set
from enum import Enum
import heapq
import uuid

from .models import Tick, TickType, Task, Signal, SignalType
from .connection import ConnectionManager, AgentConnection

logger = logging.getLogger(__name__)


# ============================================================================
# Tick Generator
# ============================================================================

class TickGenerator:
    """
    Generates ticks at high frequency with minimal jitter.

    Uses a precision timer loop that aims for sub-millisecond accuracy.
    """

    def __init__(
        self,
        interval_ms: int = 100,
        batch_size: int = 100,
    ):
        self.interval_ms = interval_ms
        self.batch_size = batch_size

        self._running = False
        self._sequence = 0
        self._task: Optional[asyncio.Task] = None

        # Callbacks
        self._on_tick: Optional[Callable[[Tick], Awaitable[None]]] = None

        # Stats
        self._ticks_generated = 0
        self._last_tick_time: Optional[float] = None
        self._jitter_samples: List[float] = []

    async def start(self, on_tick: Callable[[Tick], Awaitable[None]]) -> None:
        """Start generating ticks."""
        if self._running:
            return

        self._running = True
        self._on_tick = on_tick
        self._task = asyncio.create_task(self._tick_loop())

        logger.info(f"TickGenerator started (interval={self.interval_ms}ms)")

    async def stop(self) -> None:
        """Stop generating ticks."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("TickGenerator stopped")

    async def _tick_loop(self) -> None:
        """Main tick generation loop."""
        interval_ns = self.interval_ms * 1_000_000
        next_tick_ns = time.time_ns()

        while self._running:
            try:
                # Wait until next tick time
                now_ns = time.time_ns()
                sleep_ns = next_tick_ns - now_ns

                if sleep_ns > 0:
                    await asyncio.sleep(sleep_ns / 1_000_000_000)

                # Generate tick
                actual_time_ns = time.time_ns()
                tick = Tick(
                    tick_id=str(uuid.uuid4()),
                    tick_type=TickType.POLL,
                    timestamp=datetime.utcnow(),
                    timestamp_ns=actual_time_ns,
                    sequence=self._sequence,
                    deadline_ms=self.interval_ms * 2,  # 2x interval for deadline
                )

                self._sequence += 1
                self._ticks_generated += 1

                # Track jitter
                if self._last_tick_time:
                    jitter = (actual_time_ns - self._last_tick_time - interval_ns) / 1_000_000  # ms
                    self._jitter_samples.append(abs(jitter))
                    if len(self._jitter_samples) > 1000:
                        self._jitter_samples = self._jitter_samples[-1000:]

                self._last_tick_time = actual_time_ns

                # Call handler
                if self._on_tick:
                    await self._on_tick(tick)

                # Schedule next tick
                next_tick_ns += interval_ns

                # If we've fallen behind, catch up
                if next_tick_ns < time.time_ns():
                    next_tick_ns = time.time_ns() + interval_ns

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Tick generation error: {e}")
                await asyncio.sleep(0.1)

    def get_stats(self) -> Dict[str, Any]:
        """Get tick generator statistics."""
        jitter_p50 = 0
        jitter_p99 = 0

        if self._jitter_samples:
            sorted_samples = sorted(self._jitter_samples)
            jitter_p50 = sorted_samples[len(sorted_samples) // 2]
            jitter_p99 = sorted_samples[int(len(sorted_samples) * 0.99)]

        return {
            "ticks_generated": self._ticks_generated,
            "interval_ms": self.interval_ms,
            "sequence": self._sequence,
            "jitter_p50_ms": round(jitter_p50, 3),
            "jitter_p99_ms": round(jitter_p99, 3),
            "running": self._running,
        }


# ============================================================================
# Tick Scheduler
# ============================================================================

@dataclass(order=True)
class ScheduledTick:
    """A tick scheduled for a specific time."""
    scheduled_time_ns: int
    agent_id: str = field(compare=False)
    tick: Tick = field(compare=False)
    priority: int = field(default=5, compare=False)  # 0 = highest


class TickScheduler:
    """
    Schedules ticks for specific agents at specific times.

    Supports:
    - Per-agent tick intervals
    - Priority-based scheduling
    - Task-triggered ticks
    """

    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

        # Scheduled ticks (min-heap by time)
        self._scheduled: List[ScheduledTick] = []

        # Per-agent intervals
        self._agent_intervals: Dict[str, int] = {}  # agent_id -> interval_ms

        # Running state
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Stats
        self._ticks_scheduled = 0
        self._ticks_delivered = 0
        self._ticks_missed = 0

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())

        logger.info("TickScheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("TickScheduler stopped")

    def set_interval(self, agent_id: str, interval_ms: int) -> None:
        """Set tick interval for an agent."""
        self._agent_intervals[agent_id] = interval_ms

        # Schedule first tick
        self._schedule_next_tick(agent_id)

    def remove_agent(self, agent_id: str) -> None:
        """Remove an agent from scheduling."""
        self._agent_intervals.pop(agent_id, None)

    def schedule_tick(
        self,
        agent_id: str,
        tick: Tick,
        delay_ms: int = 0,
        priority: int = 5,
    ) -> None:
        """Schedule a tick for an agent."""
        scheduled_time_ns = time.time_ns() + (delay_ms * 1_000_000)

        scheduled = ScheduledTick(
            scheduled_time_ns=scheduled_time_ns,
            agent_id=agent_id,
            tick=tick,
            priority=priority,
        )

        heapq.heappush(self._scheduled, scheduled)
        self._ticks_scheduled += 1

    def schedule_task_tick(
        self,
        agent_id: str,
        task: Task,
        delay_ms: int = 0,
    ) -> None:
        """Schedule a tick with a task assignment."""
        tick = Tick(
            tick_type=TickType.TASK,
            scheduled_tasks=[task],
            deadline_ms=task.timeout_ms,
        )

        self.schedule_tick(
            agent_id=agent_id,
            tick=tick,
            delay_ms=delay_ms,
            priority=task.priority,
        )

    def _schedule_next_tick(self, agent_id: str) -> None:
        """Schedule the next regular tick for an agent."""
        interval_ms = self._agent_intervals.get(agent_id)
        if not interval_ms:
            return

        tick = Tick(tick_type=TickType.POLL)
        self.schedule_tick(
            agent_id=agent_id,
            tick=tick,
            delay_ms=interval_ms,
        )

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                if not self._scheduled:
                    await asyncio.sleep(0.001)  # 1ms when idle
                    continue

                # Peek at next scheduled tick
                next_tick = self._scheduled[0]
                now_ns = time.time_ns()

                if next_tick.scheduled_time_ns <= now_ns:
                    # Time to deliver
                    heapq.heappop(self._scheduled)

                    # Check if too late (missed deadline)
                    latency_ms = (now_ns - next_tick.scheduled_time_ns) / 1_000_000
                    if latency_ms > next_tick.tick.deadline_ms:
                        logger.warning(
                            f"Tick missed deadline for agent {next_tick.agent_id}: "
                            f"latency={latency_ms:.1f}ms, deadline={next_tick.tick.deadline_ms}ms"
                        )
                        self._ticks_missed += 1
                        continue

                    # Deliver tick
                    conn = self.connection_manager.get(next_tick.agent_id)
                    if conn:
                        await conn.send_tick(next_tick.tick)
                        self._ticks_delivered += 1

                        # Schedule next regular tick
                        if next_tick.tick.tick_type == TickType.POLL:
                            self._schedule_next_tick(next_tick.agent_id)

                else:
                    # Wait until next tick time
                    wait_ns = next_tick.scheduled_time_ns - now_ns
                    wait_ms = wait_ns / 1_000_000
                    await asyncio.sleep(min(wait_ms / 1000, 0.1))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(0.01)

    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "ticks_scheduled": self._ticks_scheduled,
            "ticks_delivered": self._ticks_delivered,
            "ticks_missed": self._ticks_missed,
            "pending_ticks": len(self._scheduled),
            "agents_scheduled": len(self._agent_intervals),
            "running": self._running,
        }


# ============================================================================
# Tick Bus (Redis-backed for distributed deployment)
# ============================================================================

class TickBus:
    """
    Distributed tick bus using Redis Streams.

    For single-instance deployment, use in-memory mode.
    For distributed deployment, use Redis mode.
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        redis_url: Optional[str] = None,
    ):
        self.connection_manager = connection_manager
        self.redis_url = redis_url

        # Components
        self._generator = TickGenerator()
        self._scheduler = TickScheduler(connection_manager)

        # Redis client (optional)
        self._redis = None

        # Running state
        self._running = False

    async def start(self) -> None:
        """Start the tick bus."""
        if self._running:
            return

        self._running = True

        # Connect to Redis if configured
        if self.redis_url:
            try:
                import redis.asyncio as redis
                self._redis = redis.from_url(self.redis_url)
                logger.info(f"Connected to Redis: {self.redis_url}")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis: {e}, using in-memory mode")
                self._redis = None

        # Start components
        await self._generator.start(self._on_tick)
        await self._scheduler.start()

        logger.info("TickBus started")

    async def stop(self) -> None:
        """Stop the tick bus."""
        self._running = False

        await self._generator.stop()
        await self._scheduler.stop()

        if self._redis:
            await self._redis.close()

        logger.info("TickBus stopped")

    async def _on_tick(self, tick: Tick) -> None:
        """Handle a generated tick - broadcast to all agents."""
        # In distributed mode, publish to Redis
        if self._redis:
            import json
            await self._redis.xadd(
                "ticks",
                {"tick": json.dumps(tick.to_dict())},
                maxlen=10000,
            )

        # Broadcast to local connections
        await self.connection_manager.broadcast_tick(tick)

    def schedule_tick(
        self,
        agent_id: str,
        tick: Tick,
        delay_ms: int = 0,
    ) -> None:
        """Schedule a tick for a specific agent."""
        self._scheduler.schedule_tick(agent_id, tick, delay_ms)

    def schedule_task(
        self,
        agent_id: str,
        task: Task,
        delay_ms: int = 0,
    ) -> None:
        """Schedule a task tick for a specific agent."""
        self._scheduler.schedule_task_tick(agent_id, task, delay_ms)

    def set_agent_interval(self, agent_id: str, interval_ms: int) -> None:
        """Set the tick interval for an agent."""
        self._scheduler.set_interval(agent_id, interval_ms)

    def remove_agent(self, agent_id: str) -> None:
        """Remove an agent from the tick bus."""
        self._scheduler.remove_agent(agent_id)

    async def send_signal(self, agent_id: str, signal: Signal) -> bool:
        """Send a signal to an agent."""
        conn = self.connection_manager.get(agent_id)
        if not conn:
            return False

        return await conn.send_signal(signal)

    async def broadcast_signal(self, signal: Signal) -> int:
        """Broadcast a signal to all agents."""
        return await self.connection_manager.broadcast_signal(signal)

    def get_stats(self) -> Dict[str, Any]:
        """Get tick bus statistics."""
        return {
            "generator": self._generator.get_stats(),
            "scheduler": self._scheduler.get_stats(),
            "connections": self.connection_manager.get_stats(),
            "redis_connected": self._redis is not None,
            "running": self._running,
        }


# ============================================================================
# Signal Bus
# ============================================================================

class SignalBus:
    """
    Handles out-of-band signal delivery.

    Signals bypass the tick scheduler for low-latency delivery.
    """

    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

        # Pending signals (for retry)
        self._pending: Dict[str, Signal] = {}

        # Stats
        self._signals_sent = 0
        self._signals_acked = 0
        self._signals_failed = 0

    async def send(self, signal: Signal) -> bool:
        """Send a signal to an agent."""
        conn = self.connection_manager.get(signal.target_agent)
        if not conn:
            self._signals_failed += 1
            return False

        success = await conn.send_signal(signal)
        if success:
            self._signals_sent += 1
            if signal.require_ack:
                self._signals_acked += 1
        else:
            self._signals_failed += 1

        return success

    async def broadcast(
        self,
        signal_type: SignalType,
        payload: Any = None,
        exclude: Optional[Set[str]] = None,
    ) -> int:
        """Broadcast a signal to all agents."""
        count = 0
        exclude = exclude or set()

        for conn in self.connection_manager.get_all():
            if conn.agent_id in exclude:
                continue

            signal = Signal(
                signal_type=signal_type,
                target_agent=conn.agent_id,
                payload=payload,
            )

            if await self.send(signal):
                count += 1

        return count

    async def interrupt(
        self,
        agent_id: str,
        task: Task,
        preempt: bool = True,
    ) -> bool:
        """Interrupt an agent with a new task."""
        signal = Signal(
            signal_type=SignalType.INTERRUPT,
            target_agent=agent_id,
            payload=task.to_dict(),
            preempt=preempt,
            priority=0,  # Highest priority
        )

        return await self.send(signal)

    async def shutdown(
        self,
        agent_id: Optional[str] = None,
        graceful: bool = True,
    ) -> int:
        """Send shutdown signal to agent(s)."""
        if agent_id:
            signal = Signal(
                signal_type=SignalType.SHUTDOWN,
                target_agent=agent_id,
                payload={"graceful": graceful},
            )
            return 1 if await self.send(signal) else 0

        return await self.broadcast(
            SignalType.SHUTDOWN,
            payload={"graceful": graceful},
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get signal bus statistics."""
        return {
            "signals_sent": self._signals_sent,
            "signals_acked": self._signals_acked,
            "signals_failed": self._signals_failed,
            "pending": len(self._pending),
        }
