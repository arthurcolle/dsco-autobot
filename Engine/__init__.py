"""
Engine API - Real-time Agent Substrate

The single connection point for all agents. Built on Temporal for durable
execution with a custom real-time layer for sub-millisecond ticks and signals.

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                         AGENTS                                  │
    │    (macOS app, Claude, custom Python/Swift agents)              │
    └───────────────────────────┬─────────────────────────────────────┘
                                │
                      WebSocket/gRPC stream
                                │
    ┌───────────────────────────┴─────────────────────────────────────┐
    │                      ENGINE API                                 │
    │                                                                 │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
    │  │ Tick Bus    │  │ Signal Bus  │  │ State CRDT  │            │
    │  │ <1ms jitter │  │ <5ms P99    │  │ convergent  │            │
    │  └─────────────┘  └─────────────┘  └─────────────┘            │
    │                                                                 │
    │  ┌──────────────────────────────────────────────────────────┐ │
    │  │               TEMPORAL ORCHESTRATION                      │ │
    │  │  AgentLifecycle │ TaskExecution │ MultiAgentCoord        │ │
    │  └──────────────────────────────────────────────────────────┘ │
    └─────────────────────────────────────────────────────────────────┘

Usage:
    # Server-side: Run the Engine API
    from engine import create_engine_app
    app = create_engine_app()

    # Client-side: Connect an agent
    from engine import EngineClient, AgentConfig

    async with EngineClient(AgentConfig(agent_id="my-agent")) as engine:
        async for message in engine:
            if message.type == "tick":
                result = await do_work(message.tick)
                await engine.send_result(message.tick.tick_id, result)

    # Or use the Agent base class
    from engine import Agent, AgentConfig

    class MyAgent(Agent):
        async def on_tick(self, tick):
            return await self.process(tick)

    agent = MyAgent(AgentConfig(agent_id="my-agent"))
    await agent.run()
"""

# Models
from .models import (
    Tick,
    TickType,
    Signal,
    SignalType,
    AgentStatus,
    AgentState,
    AgentConfig,
    AgentPresence,
    Task,
    TaskStatus,
    StateDelta,
)

# Connection Layer
from .connection import (
    AgentConnection,
    ConnectionManager,
)

# Tick/Signal Bus
from .tick_bus import (
    TickBus,
    TickGenerator,
    TickScheduler,
    SignalBus,
)

# Server
from .server import (
    EngineServer,
    create_engine_app,
)

# Client SDK
from .sdk import (
    EngineClient,
    ClientConfig,
    EngineMessage,
    MessageType,
    Agent,
    run_agent,
)

# Temporal Workflows
from .workflows import (
    AgentLifecycleWorkflow,
    AgentLifecycleInput,
    TaskExecutionWorkflow,
    TaskExecutionInput,
    MultiAgentCoordinationWorkflow,
    MultiAgentInput,
    ScheduledWorkWorkflow,
    ScheduledWorkInput,
    HealthCheckWorkflow,
)

# Temporal Activities
from .activities import (
    ALL_ACTIVITIES,
    EngineConfig,
    configure as configure_activities,
)

# Temporal Worker
from .worker import (
    EngineWorker,
    WorkerConfig,
    WorkflowManager,
)

__all__ = [
    # Models
    "Tick",
    "TickType",
    "Signal",
    "SignalType",
    "AgentStatus",
    "AgentState",
    "AgentConfig",
    "AgentPresence",
    "Task",
    "TaskStatus",
    "StateDelta",
    # Connection
    "AgentConnection",
    "ConnectionManager",
    # Tick Bus
    "TickBus",
    "TickGenerator",
    "TickScheduler",
    "SignalBus",
    # Server
    "EngineServer",
    "create_engine_app",
    # SDK
    "EngineClient",
    "ClientConfig",
    "EngineMessage",
    "MessageType",
    "Agent",
    "run_agent",
    # Workflows
    "AgentLifecycleWorkflow",
    "AgentLifecycleInput",
    "TaskExecutionWorkflow",
    "TaskExecutionInput",
    "MultiAgentCoordinationWorkflow",
    "MultiAgentInput",
    "ScheduledWorkWorkflow",
    "ScheduledWorkInput",
    "HealthCheckWorkflow",
    # Activities
    "ALL_ACTIVITIES",
    "EngineConfig",
    "configure_activities",
    # Worker
    "EngineWorker",
    "WorkerConfig",
    "WorkflowManager",
]

__version__ = "0.1.0"
