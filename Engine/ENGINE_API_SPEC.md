# Engine API Specification

## The Vision

The Engine API is the **single connection point** for all agents. It's not just an API - it's a **real-time agent substrate** built on Temporal that provides:

- **Ticks**: Scheduled heartbeats that drive agent execution
- **Signals**: Real-time interrupts and priority changes
- **State Sync**: CRDT-based convergent state across distributed agents
- **Durable Execution**: Workflows survive crashes, retries are automatic
- **Horizontal Scale**: Thousands of concurrent agents with sub-10ms latency

## Architecture

```
                                    ┌─────────────────────────────────┐
                                    │         AGENTS                  │
                                    │  (macOS app, Claude, custom)    │
                                    └───────────────┬─────────────────┘
                                                    │
                                          WebSocket/gRPC stream
                                                    │
┌───────────────────────────────────────────────────┴─────────────────────────────────────────┐
│                                      ENGINE API                                             │
│                                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐│
│  │                              CONNECTION LAYER                                          ││
│  │                                                                                        ││
│  │  Agent connects once → receives continuous stream of:                                  ││
│  │  • Ticks (work to do)                                                                  ││
│  │  • Heartbeats (you're alive, here's global state delta)                               ││
│  │  • Signals (interrupt current work, priority change, new task)                        ││
│  │  • Presence (other agents online, their status)                                       ││
│  │                                                                                        ││
│  │  Agent sends:                                                                          ││
│  │  • Results (task completed)                                                            ││
│  │  • State mutations (I changed something)                                              ││
│  │  • Tool requests (execute this tool)                                                   ││
│  │  • Checkpoints (save my state)                                                         ││
│  └────────────────────────────────────────────────────────────────────────────────────────┘│
│                                           │                                                 │
│  ┌────────────────────────────────────────┴───────────────────────────────────────────────┐│
│  │                              TEMPORAL ORCHESTRATION                                    ││
│  │                                                                                        ││
│  │  Workflows:                                                                            ││
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐      ││
│  │  │ AgentLifecycle  │ │ TaskExecution   │ │ MultiAgentCoord │ │ ScheduledWork   │      ││
│  │  │                 │ │                 │ │                 │ │                 │      ││
│  │  │ spawn/pause/    │ │ assign task     │ │ fan-out work    │ │ cron triggers   │      ││
│  │  │ resume/kill     │ │ track progress  │ │ aggregate       │ │ time-based      │      ││
│  │  │ state machine   │ │ handle failure  │ │ consensus       │ │ ticks           │      ││
│  │  └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘      ││
│  │                                                                                        ││
│  │  Activities:                                                                           ││
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐      ││
│  │  │ SendTick        │ │ ExecuteTool     │ │ SyncState       │ │ Checkpoint      │      ││
│  │  │ SendSignal      │ │ QueryMemory     │ │ BroadcastDelta  │ │ LoadState       │      ││
│  │  └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘      ││
│  └────────────────────────────────────────────────────────────────────────────────────────┘│
│                                           │                                                 │
│  ┌────────────────────────────────────────┴───────────────────────────────────────────────┐│
│  │                              REALTIME SYNC LAYER                                       ││
│  │                                                                                        ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              ││
│  │  │  Tick Bus    │  │  Signal Bus  │  │  State CRDT  │  │  Presence    │              ││
│  │  │              │  │              │  │              │  │              │              ││
│  │  │  High-freq   │  │  Priority    │  │  Convergent  │  │  Who's       │              ││
│  │  │  scheduler   │  │  interrupts  │  │  merge       │  │  online      │              ││
│  │  │  <1ms jitter │  │  <5ms P99    │  │  no locks    │  │  status      │              ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘              ││
│  │                                                                                        ││
│  │  Implementation options:                                                               ││
│  │  • Redis Streams + Pub/Sub (proven, simple)                                           ││
│  │  • NATS JetStream (higher throughput)                                                 ││
│  │  • Custom Rust (GraphSub integration, max performance)                                ││
│  └────────────────────────────────────────────────────────────────────────────────────────┘│
│                                           │                                                 │
│  ┌────────────────────────────────────────┴───────────────────────────────────────────────┐│
│  │                              SERVICES                                                  ││
│  │                                                                                        ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              ││
│  │  │ Tool Gateway │  │ Memory/State │  │ Trace/Obs    │  │ Auth/Caps    │              ││
│  │  │              │  │              │  │              │  │              │              ││
│  │  │ MCP Registry │  │ Neo4j/       │  │ PostHog      │  │ Capability   │              ││
│  │  │ + Neo4j      │  │ GraphSub     │  │ + custom     │  │ tokens       │              ││
│  │  │ tools        │  │ CRDT state   │  │ spans        │  │ RBAC         │              ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘              ││
│  └────────────────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### 1. Agent Connection

Agent connects ONCE via WebSocket/gRPC and maintains a persistent bidirectional stream:

```python
# Agent-side (simplified)
async with engine.connect(agent_id, capabilities) as stream:
    async for message in stream:
        match message.type:
            case "tick":
                # Time to do work
                result = await do_work(message.payload)
                await stream.send(Result(result))

            case "signal":
                # Interrupt - priority change, new task, etc.
                await handle_signal(message.signal)

            case "state_delta":
                # Global state changed - merge into local
                local_state.merge(message.delta)

            case "heartbeat":
                # Still alive, optionally includes state snapshot
                await stream.send(Ack())
```

### 2. Tick System

Ticks are the **clock** of the system. Every agent receives ticks at configured intervals:

```python
@dataclass
class Tick:
    tick_id: str
    timestamp: datetime
    sequence: int

    # What the agent should do this tick
    scheduled_tasks: List[Task]

    # Global state relevant to this agent
    state_snapshot: Optional[StateSnapshot]

    # Deadline for response
    deadline_ms: int
```

**Tick Types**:
- **Polling Tick**: Regular interval (100ms, 1s, etc.) - "check if there's work"
- **Task Tick**: Specific task assigned - "do this now"
- **Sync Tick**: State synchronization - "here's what changed"

### 3. Signal System

Signals are **out-of-band interrupts** that don't wait for the next tick:

```python
@dataclass
class Signal:
    signal_id: str
    signal_type: SignalType  # INTERRUPT, PRIORITY, CANCEL, REASSIGN
    target_agent: str
    payload: Any

    # For priority signals
    priority: int  # 0 = highest
    preempt: bool  # Cancel current work?

class SignalType(Enum):
    INTERRUPT = "interrupt"      # Stop current, do this instead
    PRIORITY = "priority"        # Change task priority
    CANCEL = "cancel"            # Abort current task
    REASSIGN = "reassign"        # Move task to another agent
    STATE_PUSH = "state_push"    # Immediate state update
    SHUTDOWN = "shutdown"        # Graceful shutdown
```

### 4. State Synchronization (CRDT)

All agents share a **convergent state** that merges automatically:

```python
@dataclass
class AgentState:
    """CRDT-backed agent state - merges without conflicts"""

    # LWW (Last-Writer-Wins) registers
    current_task: LWWRegister[Optional[Task]]
    status: LWWRegister[AgentStatus]

    # G-Counter (grow-only)
    tasks_completed: GCounter

    # OR-Set (add/remove set)
    capabilities: ORSet[str]
    active_tools: ORSet[str]

    # LWW-Map for arbitrary k/v
    metadata: LWWMap[str, Any]

    def merge(self, other: "AgentState") -> "AgentState":
        """Merge two states - always converges"""
        return AgentState(
            current_task=self.current_task.merge(other.current_task),
            status=self.status.merge(other.status),
            tasks_completed=self.tasks_completed.merge(other.tasks_completed),
            capabilities=self.capabilities.merge(other.capabilities),
            active_tools=self.active_tools.merge(other.active_tools),
            metadata=self.metadata.merge(other.metadata),
        )
```

**Why CRDT?**
- No coordination needed between agents
- Network partitions don't cause data loss
- Eventual consistency guaranteed mathematically
- Perfect for distributed agents that may be offline

### 5. Temporal Workflows

Temporal provides **durable execution** - workflows survive crashes:

```python
# workflows.py
from temporalio import workflow
from datetime import timedelta

@workflow.defn
class AgentLifecycleWorkflow:
    """Manages a single agent's lifecycle"""

    def __init__(self):
        self.status = AgentStatus.INITIALIZING
        self.current_task: Optional[Task] = None

    @workflow.run
    async def run(self, agent_id: str, config: AgentConfig):
        # Register agent
        await workflow.execute_activity(
            register_agent,
            agent_id,
            start_to_close_timeout=timedelta(seconds=10)
        )
        self.status = AgentStatus.IDLE

        # Main loop - runs forever until signaled
        while self.status != AgentStatus.TERMINATED:
            # Wait for signal or timeout (tick interval)
            try:
                await workflow.wait_condition(
                    lambda: self.current_task is not None or
                            self.status == AgentStatus.TERMINATED,
                    timeout=timedelta(milliseconds=config.tick_interval_ms)
                )
            except asyncio.TimeoutError:
                # Tick timeout - send tick to agent
                await workflow.execute_activity(
                    send_tick,
                    agent_id,
                    start_to_close_timeout=timedelta(seconds=5)
                )

            # Process task if assigned
            if self.current_task:
                self.status = AgentStatus.WORKING
                result = await workflow.execute_activity(
                    execute_task,
                    agent_id,
                    self.current_task,
                    start_to_close_timeout=timedelta(minutes=5)
                )
                self.current_task = None
                self.status = AgentStatus.IDLE

    @workflow.signal
    async def assign_task(self, task: Task):
        self.current_task = task

    @workflow.signal
    async def interrupt(self, signal: Signal):
        if signal.signal_type == SignalType.SHUTDOWN:
            self.status = AgentStatus.TERMINATED
        elif signal.signal_type == SignalType.CANCEL:
            self.current_task = None

    @workflow.query
    def get_status(self) -> AgentStatus:
        return self.status


@workflow.defn
class MultiAgentCoordinationWorkflow:
    """Coordinates work across multiple agents"""

    @workflow.run
    async def run(self, task: ComplexTask, agent_pool: List[str]):
        # Decompose task
        subtasks = await workflow.execute_activity(
            decompose_task,
            task,
            start_to_close_timeout=timedelta(seconds=30)
        )

        # Fan out to agents
        results = []
        for subtask, agent_id in zip(subtasks, cycle(agent_pool)):
            # Get agent workflow handle and signal it
            handle = workflow.get_external_workflow_handle(
                f"agent-{agent_id}"
            )
            await handle.signal(AgentLifecycleWorkflow.assign_task, subtask)
            results.append(handle)

        # Wait for all results
        completed = await asyncio.gather(*[
            workflow.execute_activity(
                wait_for_result,
                r,
                start_to_close_timeout=timedelta(minutes=10)
            )
            for r in results
        ])

        # Aggregate
        return await workflow.execute_activity(
            aggregate_results,
            completed,
            start_to_close_timeout=timedelta(seconds=30)
        )
```

### 6. Activities (Side Effects)

Activities are the **side effects** - actual work that talks to external systems:

```python
# activities.py
from temporalio import activity

@activity.defn
async def send_tick(agent_id: str) -> None:
    """Send tick to agent via realtime layer"""
    tick = Tick(
        tick_id=generate_id(),
        timestamp=datetime.utcnow(),
        sequence=get_next_sequence(agent_id),
        scheduled_tasks=[],
        deadline_ms=100
    )
    await realtime.publish(f"agent:{agent_id}:ticks", tick)

@activity.defn
async def execute_tool(tool_id: str, params: Dict) -> Any:
    """Execute a tool via the Tool Gateway"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{TOOL_GATEWAY_URL}/unified/execute",
            json={"tool_id": tool_id, "input_data": params}
        )
        return resp.json()

@activity.defn
async def sync_state(agent_id: str, delta: StateDelta) -> None:
    """Sync state delta to GraphSub"""
    await graphsub.apply_delta(agent_id, delta)

@activity.defn
async def checkpoint(agent_id: str, state: AgentState) -> str:
    """Save agent state checkpoint"""
    checkpoint_id = generate_id()
    await graphsub.create_checkpoint(agent_id, checkpoint_id, state)
    return checkpoint_id

@activity.defn
async def load_checkpoint(checkpoint_id: str) -> AgentState:
    """Load agent state from checkpoint"""
    return await graphsub.load_checkpoint(checkpoint_id)
```

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Tick Latency P50 | <5ms | Time from tick generation to agent receipt |
| Tick Latency P99 | <20ms | Even under load |
| Signal Latency P99 | <10ms | Interrupts must be fast |
| Concurrent Agents | 10,000+ | Per Engine instance |
| Tick Throughput | 100,000/sec | Aggregate across all agents |
| State Sync | <50ms | CRDT merge propagation |
| Checkpoint | <100ms | Full agent state save |
| Recovery | <1s | Agent restart from checkpoint |

## Wire Protocol

### WebSocket Messages (Agent ↔ Engine)

```typescript
// Engine → Agent
type EngineMessage =
  | { type: "tick", tick: Tick }
  | { type: "signal", signal: Signal }
  | { type: "state_delta", delta: StateDelta }
  | { type: "heartbeat", timestamp: number }
  | { type: "presence", agents: AgentPresence[] }

// Agent → Engine
type AgentMessage =
  | { type: "result", task_id: string, result: any }
  | { type: "state_mutation", mutation: StateMutation }
  | { type: "tool_request", tool_id: string, params: any }
  | { type: "checkpoint_request" }
  | { type: "ack", message_id: string }
  | { type: "error", message_id: string, error: string }
```

### gRPC Service Definition

```protobuf
syntax = "proto3";
package engine.v1;

service EngineService {
  // Bidirectional stream - main connection
  rpc Connect(stream AgentMessage) returns (stream EngineMessage);

  // Unary RPCs for out-of-band operations
  rpc RegisterAgent(RegisterRequest) returns (RegisterResponse);
  rpc GetAgentStatus(StatusRequest) returns (StatusResponse);
  rpc ListAgents(ListRequest) returns (ListResponse);

  // Admin operations
  rpc AssignTask(AssignTaskRequest) returns (AssignTaskResponse);
  rpc SendSignal(SignalRequest) returns (SignalResponse);
  rpc GetMetrics(MetricsRequest) returns (MetricsResponse);
}

message Tick {
  string tick_id = 1;
  int64 timestamp_ms = 2;
  int64 sequence = 3;
  repeated Task scheduled_tasks = 4;
  bytes state_snapshot = 5;  // Optional, msgpack/protobuf encoded
  int32 deadline_ms = 6;
}

message Signal {
  string signal_id = 1;
  SignalType signal_type = 2;
  string target_agent = 3;
  bytes payload = 4;
  int32 priority = 5;
  bool preempt = 6;
}

enum SignalType {
  SIGNAL_TYPE_UNSPECIFIED = 0;
  SIGNAL_TYPE_INTERRUPT = 1;
  SIGNAL_TYPE_PRIORITY = 2;
  SIGNAL_TYPE_CANCEL = 3;
  SIGNAL_TYPE_REASSIGN = 4;
  SIGNAL_TYPE_STATE_PUSH = 5;
  SIGNAL_TYPE_SHUTDOWN = 6;
}
```

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LOAD BALANCER                               │
│                    (sticky sessions by agent_id)                    │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│  Engine API  │        │  Engine API  │        │  Engine API  │
│  Instance 1  │        │  Instance 2  │        │  Instance N  │
│              │        │              │        │              │
│  WebSocket   │        │  WebSocket   │        │  WebSocket   │
│  handlers    │        │  handlers    │        │  handlers    │
└──────┬───────┘        └──────┬───────┘        └──────┬───────┘
       │                       │                       │
       └───────────────────────┼───────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     TEMPORAL CLUSTER                                │
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
│  │  Frontend   │  │  History    │  │  Matching   │                │
│  │  Service    │  │  Service    │  │  Service    │                │
│  └─────────────┘  └─────────────┘  └─────────────┘                │
│                                                                     │
│  ┌─────────────────────────────────────────────────┐               │
│  │              Temporal Workers                   │               │
│  │  (run workflow/activity code)                   │               │
│  └─────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                               │
       ┌───────────────────────┼───────────────────────┐
       ▼                       ▼                       ▼
┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│    Redis     │        │   Neo4j/     │        │  PostgreSQL  │
│  (realtime)  │        │  GraphSub    │        │  (Temporal)  │
│              │        │  (state)     │        │              │
│  Tick bus    │        │  CRDT store  │        │  Workflow    │
│  Pub/Sub     │        │  Checkpoints │        │  history     │
│  Presence    │        │  Memory      │        │              │
└──────────────┘        └──────────────┘        └──────────────┘
```

## Implementation Phases

### Phase 1: Core Connection Layer
- [ ] WebSocket server with agent connection management
- [ ] Basic tick generation and distribution
- [ ] Heartbeat/presence tracking
- [ ] Simple signal delivery

### Phase 2: Temporal Integration
- [ ] AgentLifecycleWorkflow
- [ ] Task assignment via signals
- [ ] Basic activities (send_tick, execute_tool)
- [ ] Workflow state queries

### Phase 3: State Synchronization
- [ ] CRDT implementation (or use existing library)
- [ ] State delta generation and propagation
- [ ] Merge conflict resolution
- [ ] Integration with GraphSub

### Phase 4: Multi-Agent Coordination
- [ ] MultiAgentCoordinationWorkflow
- [ ] Task decomposition
- [ ] Fan-out/fan-in patterns
- [ ] Result aggregation

### Phase 5: Performance Optimization
- [ ] Redis Streams for tick bus
- [ ] gRPC for high-throughput connections
- [ ] Connection pooling
- [ ] Benchmark suite

### Phase 6: Production Hardening
- [ ] Auth/capability tokens
- [ ] Rate limiting
- [ ] Circuit breakers
- [ ] Observability (traces, metrics)

## Questions to Resolve

1. **Tick bus implementation**: Redis Streams vs NATS vs custom Rust?
2. **CRDT library**: Use `yrs` (Yjs Rust port) or implement custom?
3. **GraphSub integration**: Direct embedding or service boundary?
4. **Agent SDK**: Python first? Then Swift for macOS app?
5. **Deployment**: Modal? Fly.io? Self-hosted k8s?

## References

- [Temporal Concepts](https://docs.temporal.io/concepts)
- [CRDT Literature](https://crdt.tech/)
- [Redis Streams](https://redis.io/docs/data-types/streams/)
- [gRPC Bidirectional Streaming](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc)
