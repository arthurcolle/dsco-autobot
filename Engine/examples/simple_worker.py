#!/usr/bin/env python3
"""
Simple Worker Agent Example

A basic agent that connects to the Engine and processes tasks.

Usage:
    python simple_worker.py --agent-id worker-1 --engine-url ws://localhost:8080
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sdk import Agent, AgentConfig, ClientConfig
from models import Tick, TickType, Signal, SignalType, Task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class SimpleWorker(Agent):
    """
    A simple worker agent that processes tasks.

    This demonstrates the basic agent pattern:
    1. Connect to Engine
    2. Receive ticks
    3. Process assigned tasks
    4. Return results
    """

    def __init__(self, config: AgentConfig, client_config: ClientConfig = None):
        super().__init__(config, client_config)
        self.processed_count = 0

    async def on_start(self) -> None:
        """Called when the agent starts."""
        logger.info(f"Worker {self.config.agent_id} started")
        logger.info(f"Capabilities: {self.config.capabilities}")

    async def on_stop(self) -> None:
        """Called when the agent stops."""
        logger.info(f"Worker {self.config.agent_id} stopped")
        logger.info(f"Processed {self.processed_count} tasks")

    async def on_tick(self, tick: Tick) -> any:
        """
        Handle incoming ticks.

        Returns the result if there's a task, None otherwise.
        """
        logger.debug(f"Received tick: {tick.tick_type.value} (seq={tick.sequence})")

        if tick.tick_type == TickType.POLL:
            # Regular polling tick - just acknowledge we're alive
            return None

        elif tick.tick_type == TickType.TASK:
            # We have a task to process
            if tick.scheduled_tasks:
                task = tick.scheduled_tasks[0]
                logger.info(f"Processing task: {task.name} ({task.task_id})")

                result = await self.process_task(task)
                self.processed_count += 1

                return result

        elif tick.tick_type == TickType.SYNC:
            # State sync tick - handled automatically by SDK
            pass

        return None

    async def process_task(self, task: Task) -> dict:
        """
        Process a task and return the result.

        Override this in your agent to implement custom logic.
        """
        logger.info(f"Task type: {task.task_type}")
        logger.info(f"Input data: {task.input_data}")

        # Simulate some work
        await asyncio.sleep(0.1)

        # Return result
        return {
            "status": "completed",
            "task_id": task.task_id,
            "agent_id": self.config.agent_id,
            "output": f"Processed by {self.config.agent_id}",
        }

    async def on_signal(self, signal: Signal) -> None:
        """Handle incoming signals."""
        logger.info(f"Received signal: {signal.signal_type.value}")

        if signal.signal_type == SignalType.SHUTDOWN:
            logger.info("Shutdown signal received")
            # SDK will handle graceful shutdown

        elif signal.signal_type == SignalType.CANCEL:
            task_id = signal.payload.get("task_id") if signal.payload else None
            logger.info(f"Cancel signal for task: {task_id}")

        elif signal.signal_type == SignalType.PRIORITY:
            priority = signal.payload.get("priority") if signal.payload else None
            logger.info(f"Priority changed to: {priority}")

    async def on_error(self, error: str) -> None:
        """Handle errors."""
        logger.error(f"Error: {error}")


async def main():
    parser = argparse.ArgumentParser(description="Simple Worker Agent")
    parser.add_argument(
        "--agent-id",
        default="worker-1",
        help="Unique agent identifier",
    )
    parser.add_argument(
        "--agent-type",
        default="worker",
        help="Agent type",
    )
    parser.add_argument(
        "--engine-url",
        default="ws://localhost:8080",
        help="Engine WebSocket URL",
    )
    parser.add_argument(
        "--tick-interval",
        type=int,
        default=1000,
        help="Tick interval in milliseconds",
    )
    parser.add_argument(
        "--capabilities",
        nargs="+",
        default=["generic"],
        help="Agent capabilities",
    )

    args = parser.parse_args()

    # Configure agent
    config = AgentConfig(
        agent_id=args.agent_id,
        agent_type=args.agent_type,
        tick_interval_ms=args.tick_interval,
        capabilities=set(args.capabilities),
    )

    client_config = ClientConfig(
        engine_url=args.engine_url,
        auto_reconnect=True,
    )

    # Create and run agent
    agent = SimpleWorker(config, client_config)

    try:
        await agent.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == "__main__":
    asyncio.run(main())
