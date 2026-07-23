"""
Engine API Temporal Worker

Runs the Temporal workflows and activities for the Engine.

Usage:
    # Start worker
    python -m engine.worker

    # With configuration
    python -m engine.worker \
        --temporal-host localhost:7233 \
        --engine-url http://localhost:8000 \
        --redis-url redis://localhost:6379
"""

import asyncio
import argparse
import logging
import signal
import sys
from dataclasses import dataclass
from typing import Optional

from temporalio.client import Client
from temporalio.worker import Worker

from dsco_shared.config.service_registry import ServiceName, resolve

from .workflows import (
    AgentLifecycleWorkflow,
    TaskExecutionWorkflow,
    MultiAgentCoordinationWorkflow,
    ScheduledWorkWorkflow,
    HealthCheckWorkflow,
)
from .activities import (
    ALL_ACTIVITIES,
    EngineConfig,
    configure as configure_activities,
    ActivityClients,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class WorkerConfig:
    """Configuration for the Temporal worker."""
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "engine-task-queue"

    # Worker settings
    max_concurrent_activities: int = 100
    max_concurrent_workflow_tasks: int = 100

    # Engine API
    engine_api_url: str = resolve(ServiceName.ENGINE)
    tool_gateway_url: str = resolve(ServiceName.TOOL_GATEWAY)

    # Redis (optional)
    redis_url: Optional[str] = None

    # Neo4j (optional)
    neo4j_uri: Optional[str] = None
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    # Logging
    log_level: str = "INFO"


# ============================================================================
# Worker
# ============================================================================

class EngineWorker:
    """
    Temporal worker for Engine workflows and activities.
    """

    def __init__(self, config: WorkerConfig):
        self.config = config
        self._client: Optional[Client] = None
        self._worker: Optional[Worker] = None
        self._running = False

    async def start(self) -> None:
        """Start the worker."""
        logger.info(f"Starting Engine worker...")
        logger.info(f"  Temporal: {self.config.temporal_host}")
        logger.info(f"  Namespace: {self.config.temporal_namespace}")
        logger.info(f"  Task Queue: {self.config.task_queue}")

        # Configure activities with runtime settings
        configure_activities(EngineConfig(
            engine_api_url=self.config.engine_api_url,
            tool_gateway_url=self.config.tool_gateway_url,
            redis_url=self.config.redis_url,
            neo4j_uri=self.config.neo4j_uri,
            neo4j_user=self.config.neo4j_user,
            neo4j_password=self.config.neo4j_password,
        ))

        # Connect to Temporal
        self._client = await Client.connect(
            self.config.temporal_host,
            namespace=self.config.temporal_namespace,
        )

        # Create worker
        self._worker = Worker(
            self._client,
            task_queue=self.config.task_queue,
            workflows=[
                AgentLifecycleWorkflow,
                TaskExecutionWorkflow,
                MultiAgentCoordinationWorkflow,
                ScheduledWorkWorkflow,
                HealthCheckWorkflow,
            ],
            activities=ALL_ACTIVITIES,
            max_concurrent_activities=self.config.max_concurrent_activities,
            max_concurrent_workflow_tasks=self.config.max_concurrent_workflow_tasks,
        )

        self._running = True
        logger.info("Engine worker started")

        # Run worker
        await self._worker.run()

    async def stop(self) -> None:
        """Stop the worker."""
        logger.info("Stopping Engine worker...")
        self._running = False

        # Close activity clients
        await ActivityClients.close()

        logger.info("Engine worker stopped")

    @property
    def client(self) -> Client:
        """Get the Temporal client."""
        if not self._client:
            raise RuntimeError("Worker not started")
        return self._client


# ============================================================================
# Workflow Management API
# ============================================================================

class WorkflowManager:
    """
    High-level API for managing Engine workflows.

    Usage:
        manager = WorkflowManager(temporal_host="localhost:7233")
        await manager.connect()

        # Start agent lifecycle
        handle = await manager.start_agent("agent-1", config)

        # Query status
        status = await manager.get_agent_status("agent-1")

        # Signal agent
        await manager.assign_task("agent-1", task)
    """

    def __init__(
        self,
        temporal_host: str = "localhost:7233",
        namespace: str = "default",
        task_queue: str = "engine-task-queue",
    ):
        self.temporal_host = temporal_host
        self.namespace = namespace
        self.task_queue = task_queue
        self._client: Optional[Client] = None

    async def connect(self) -> None:
        """Connect to Temporal."""
        self._client = await Client.connect(
            self.temporal_host,
            namespace=self.namespace,
        )

    async def close(self) -> None:
        """Close the connection."""
        self._client = None

    @property
    def client(self) -> Client:
        if not self._client:
            raise RuntimeError("Not connected")
        return self._client

    async def __aenter__(self) -> "WorkflowManager":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    # ========================================================================
    # Agent Lifecycle
    # ========================================================================

    async def start_agent(
        self,
        agent_id: str,
        config: dict,
        initial_state: Optional[dict] = None,
    ):
        """Start an agent lifecycle workflow."""
        from .workflows import AgentLifecycleInput

        handle = await self.client.start_workflow(
            AgentLifecycleWorkflow.run,
            AgentLifecycleInput(
                agent_id=agent_id,
                config=config,
                initial_state=initial_state,
            ),
            id=f"agent-lifecycle-{agent_id}",
            task_queue=self.task_queue,
        )
        return handle

    async def get_agent_status(self, agent_id: str) -> str:
        """Query agent status."""
        handle = self.client.get_workflow_handle(f"agent-lifecycle-{agent_id}")
        return await handle.query(AgentLifecycleWorkflow.get_status)

    async def get_agent_stats(self, agent_id: str) -> dict:
        """Query agent statistics."""
        handle = self.client.get_workflow_handle(f"agent-lifecycle-{agent_id}")
        return await handle.query(AgentLifecycleWorkflow.get_stats)

    async def assign_task(self, agent_id: str, task: dict) -> None:
        """Assign a task to an agent."""
        handle = self.client.get_workflow_handle(f"agent-lifecycle-{agent_id}")
        await handle.signal(AgentLifecycleWorkflow.assign_task, task)

    async def interrupt_agent(self, agent_id: str, signal: dict) -> None:
        """Send an interrupt signal to an agent."""
        handle = self.client.get_workflow_handle(f"agent-lifecycle-{agent_id}")
        await handle.signal(AgentLifecycleWorkflow.interrupt, signal)

    async def shutdown_agent(self, agent_id: str, graceful: bool = True) -> None:
        """Shutdown an agent."""
        handle = self.client.get_workflow_handle(f"agent-lifecycle-{agent_id}")
        await handle.signal(AgentLifecycleWorkflow.shutdown, graceful)

    # ========================================================================
    # Task Execution
    # ========================================================================

    async def execute_task(
        self,
        task: dict,
        agent_id: Optional[str] = None,
        timeout_ms: int = 300000,
    ):
        """Execute a task, optionally on a specific agent."""
        from .workflows import TaskExecutionInput

        task_id = task.get("task_id", f"task-{hash(str(task))}")

        handle = await self.client.start_workflow(
            TaskExecutionWorkflow.run,
            TaskExecutionInput(
                task=task,
                agent_id=agent_id or "",
                timeout_ms=timeout_ms,
            ),
            id=f"task-{task_id}",
            task_queue=self.task_queue,
        )
        return handle

    async def get_task_status(self, task_id: str) -> str:
        """Query task status."""
        handle = self.client.get_workflow_handle(f"task-{task_id}")
        return await handle.query(TaskExecutionWorkflow.get_status)

    async def cancel_task(self, task_id: str) -> None:
        """Cancel a task."""
        handle = self.client.get_workflow_handle(f"task-{task_id}")
        await handle.signal(TaskExecutionWorkflow.cancel)

    # ========================================================================
    # Multi-Agent Coordination
    # ========================================================================

    async def coordinate_agents(
        self,
        task: dict,
        agent_pool: list,
        strategy: str = "round_robin",
    ):
        """Start a multi-agent coordination workflow."""
        from .workflows import MultiAgentInput

        handle = await self.client.start_workflow(
            MultiAgentCoordinationWorkflow.run,
            MultiAgentInput(
                task=task,
                agent_pool=agent_pool,
                coordination_strategy=strategy,
            ),
            id=f"coordination-{task.get('task_id', hash(str(task)))}",
            task_queue=self.task_queue,
        )
        return handle

    async def get_coordination_progress(self, coordination_id: str) -> dict:
        """Query coordination progress."""
        handle = self.client.get_workflow_handle(f"coordination-{coordination_id}")
        return await handle.query(MultiAgentCoordinationWorkflow.get_progress)

    # ========================================================================
    # Health Checks
    # ========================================================================

    async def check_health(self, agent_ids: list) -> dict:
        """Run a health check on specified agents."""
        handle = await self.client.start_workflow(
            HealthCheckWorkflow.run,
            agent_ids,
            id=f"health-check-{asyncio.get_event_loop().time()}",
            task_queue=self.task_queue,
        )
        return await handle.result()


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> WorkerConfig:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Engine API Temporal Worker"
    )

    parser.add_argument(
        "--temporal-host",
        default="localhost:7233",
        help="Temporal server address",
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Temporal namespace",
    )
    parser.add_argument(
        "--task-queue",
        default="engine-task-queue",
        help="Temporal task queue name",
    )
    parser.add_argument(
        "--engine-url",
        default=resolve(ServiceName.ENGINE),
        help="Engine API URL",
    )
    parser.add_argument(
        "--tool-gateway-url",
        default=resolve(ServiceName.TOOL_GATEWAY),
        help="Tool Gateway URL",
    )
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Redis URL (optional)",
    )
    parser.add_argument(
        "--max-concurrent-activities",
        type=int,
        default=100,
        help="Max concurrent activities",
    )
    parser.add_argument(
        "--max-concurrent-workflows",
        type=int,
        default=100,
        help="Max concurrent workflow tasks",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level",
    )

    args = parser.parse_args()

    return WorkerConfig(
        temporal_host=args.temporal_host,
        temporal_namespace=args.namespace,
        task_queue=args.task_queue,
        engine_api_url=args.engine_url,
        tool_gateway_url=args.tool_gateway_url,
        redis_url=args.redis_url,
        max_concurrent_activities=args.max_concurrent_activities,
        max_concurrent_workflow_tasks=args.max_concurrent_workflows,
        log_level=args.log_level,
    )


async def main() -> None:
    """Main entry point."""
    config = parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Create worker
    worker = EngineWorker(config)

    # Handle signals
    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    # Run worker
    try:
        worker_task = asyncio.create_task(worker.start())
        await asyncio.wait(
            [worker_task, stop_event.wait()],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
