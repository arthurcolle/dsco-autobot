"""
Engine API Temporal Activities

Activities are the side effects - actual work that interacts with external systems.

Categories:
- Agent Management: register, unregister, health checks
- Tick/Signal: send ticks, signals, heartbeats to agents
- State: checkpoint, restore, apply deltas
- Task: find agents, wait for results, notify completion
- Tool Execution: invoke tools via Tool Gateway
"""

import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
import uuid

from temporalio import activity
import httpx

from dsco_shared.config.service_registry import ServiceName, resolve

from .models import (
    Tick,
    TickType,
    Signal,
    SignalType,
    Task,
    TaskStatus,
    AgentStatus,
    AgentState,
    StateDelta,
)

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class EngineConfig:
    """Configuration for activities."""
    engine_api_url: str = resolve(ServiceName.ENGINE)
    tool_gateway_url: str = resolve(ServiceName.TOOL_GATEWAY)
    redis_url: Optional[str] = None
    neo4j_uri: Optional[str] = None
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""


# Global config - set on worker startup
_config = EngineConfig()


def configure(config: EngineConfig) -> None:
    """Configure activities with runtime settings."""
    global _config
    _config = config


# ============================================================================
# Shared Clients
# ============================================================================

class ActivityClients:
    """Shared async clients for activities."""

    _http_client: Optional[httpx.AsyncClient] = None
    _redis_client: Optional[Any] = None
    _pg_pool: Optional[Any] = None
    _neo4j_driver: Optional[Any] = None

    @classmethod
    async def http(cls) -> httpx.AsyncClient:
        if cls._http_client is None:
            cls._http_client = httpx.AsyncClient(timeout=30.0)
        return cls._http_client

    @classmethod
    async def redis(cls):
        if cls._redis_client is None and _config.redis_url:
            import redis.asyncio as redis
            cls._redis_client = redis.from_url(_config.redis_url)
        return cls._redis_client

    @classmethod
    async def postgres(cls):
        """Get PostgreSQL connection pool."""
        if cls._pg_pool is None:
            try:
                import asyncpg
                import os
                pg_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/dsco")
                cls._pg_pool = await asyncpg.create_pool(pg_url, min_size=1, max_size=10)
            except ImportError:
                logger.warning("asyncpg not installed, PostgreSQL storage unavailable")
                return None
            except Exception as e:
                logger.warning(f"Failed to connect to PostgreSQL: {e}")
                return None
        return cls._pg_pool

    @classmethod
    async def neo4j(cls):
        """Get Neo4j driver."""
        if cls._neo4j_driver is None and _config.neo4j_uri:
            try:
                from neo4j import AsyncGraphDatabase
                cls._neo4j_driver = AsyncGraphDatabase.driver(
                    _config.neo4j_uri,
                    auth=(_config.neo4j_user, _config.neo4j_password)
                )
            except ImportError:
                logger.warning("neo4j driver not installed")
                return None
            except Exception as e:
                logger.warning(f"Failed to connect to Neo4j: {e}")
                return None
        return cls._neo4j_driver

    @classmethod
    async def close(cls):
        if cls._http_client:
            await cls._http_client.aclose()
            cls._http_client = None
        if cls._redis_client:
            await cls._redis_client.close()
            cls._redis_client = None
        if cls._pg_pool:
            await cls._pg_pool.close()
            cls._pg_pool = None
        if cls._neo4j_driver:
            await cls._neo4j_driver.close()
            cls._neo4j_driver = None


# ============================================================================
# Persistent Checkpoint Storage
# ============================================================================

class CheckpointStore:
    """
    Persistent storage for agent checkpoints.

    Supports multiple backends:
    - PostgreSQL: Primary persistent storage
    - Neo4j: Graph-based storage for state relationships
    - Redis: Fast cache layer

    Schema (PostgreSQL):
    CREATE TABLE IF NOT EXISTS checkpoints (
        id VARCHAR(255) PRIMARY KEY,
        agent_id VARCHAR(255) NOT NULL,
        state JSONB NOT NULL,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        expires_at TIMESTAMP WITH TIME ZONE,
        INDEX idx_agent_id (agent_id),
        INDEX idx_created_at (created_at)
    );
    """

    @staticmethod
    async def ensure_schema():
        """Ensure database schema exists."""
        pool = await ActivityClients.postgres()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        id VARCHAR(255) PRIMARY KEY,
                        agent_id VARCHAR(255) NOT NULL,
                        state JSONB NOT NULL,
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        expires_at TIMESTAMP WITH TIME ZONE
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_checkpoints_agent_id
                    ON checkpoints(agent_id)
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_checkpoints_created_at
                    ON checkpoints(created_at DESC)
                """)
                logger.info("Checkpoint schema initialized")

    @staticmethod
    async def save(
        checkpoint_id: str,
        agent_id: str,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """
        Save a checkpoint to persistent storage.

        Args:
            checkpoint_id: Unique checkpoint identifier
            agent_id: Agent that created the checkpoint
            state: Agent state to persist
            metadata: Optional metadata (workflow_id, reason, etc.)
            ttl_seconds: Optional time-to-live for the checkpoint

        Returns:
            True if saved successfully
        """
        from datetime import timezone, timedelta

        expires_at = None
        if ttl_seconds:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

        # Save to PostgreSQL
        pool = await ActivityClients.postgres()
        if pool:
            try:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO checkpoints (id, agent_id, state, metadata, expires_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (id) DO UPDATE SET
                            state = $3,
                            metadata = $4,
                            expires_at = $5
                    """, checkpoint_id, agent_id, json.dumps(state),
                       json.dumps(metadata or {}), expires_at)
                logger.info(f"Saved checkpoint {checkpoint_id} to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to save checkpoint to PostgreSQL: {e}")
                return False

        # Save to Neo4j for graph relationships
        driver = await ActivityClients.neo4j()
        if driver:
            try:
                async with driver.session() as session:
                    await session.run("""
                        MERGE (c:Checkpoint {id: $checkpoint_id})
                        SET c.agent_id = $agent_id,
                            c.state = $state,
                            c.metadata = $metadata,
                            c.created_at = datetime(),
                            c.expires_at = $expires_at
                        MERGE (a:Agent {id: $agent_id})
                        MERGE (a)-[:HAS_CHECKPOINT]->(c)
                    """, checkpoint_id=checkpoint_id, agent_id=agent_id,
                       state=json.dumps(state), metadata=json.dumps(metadata or {}),
                       expires_at=expires_at.isoformat() if expires_at else None)
                logger.info(f"Saved checkpoint {checkpoint_id} to Neo4j")
            except Exception as e:
                logger.warning(f"Failed to save checkpoint to Neo4j: {e}")

        return True

    @staticmethod
    async def load(checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Load a checkpoint from persistent storage.

        Args:
            checkpoint_id: Checkpoint to load

        Returns:
            Checkpoint data including state and metadata, or None if not found
        """
        from datetime import timezone

        # Try PostgreSQL first
        pool = await ActivityClients.postgres()
        if pool:
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow("""
                        SELECT id, agent_id, state, metadata, created_at, expires_at
                        FROM checkpoints
                        WHERE id = $1
                          AND (expires_at IS NULL OR expires_at > NOW())
                    """, checkpoint_id)

                    if row:
                        return {
                            "checkpoint_id": row["id"],
                            "agent_id": row["agent_id"],
                            "state": json.loads(row["state"]),
                            "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
                            "created_at": row["created_at"].isoformat(),
                            "expires_at": row["expires_at"].isoformat() if row["expires_at"] else None
                        }
            except Exception as e:
                logger.error(f"Failed to load checkpoint from PostgreSQL: {e}")

        # Fallback to Neo4j
        driver = await ActivityClients.neo4j()
        if driver:
            try:
                async with driver.session() as session:
                    result = await session.run("""
                        MATCH (c:Checkpoint {id: $checkpoint_id})
                        WHERE c.expires_at IS NULL OR datetime(c.expires_at) > datetime()
                        RETURN c
                    """, checkpoint_id=checkpoint_id)
                    record = await result.single()
                    if record:
                        node = record["c"]
                        return {
                            "checkpoint_id": node["id"],
                            "agent_id": node["agent_id"],
                            "state": json.loads(node["state"]),
                            "metadata": json.loads(node.get("metadata", "{}")),
                            "created_at": node.get("created_at"),
                            "expires_at": node.get("expires_at")
                        }
            except Exception as e:
                logger.warning(f"Failed to load checkpoint from Neo4j: {e}")

        return None

    @staticmethod
    async def list_for_agent(
        agent_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        List checkpoints for an agent.

        Args:
            agent_id: Agent to list checkpoints for
            limit: Maximum number of checkpoints to return

        Returns:
            List of checkpoint summaries (id, created_at, metadata)
        """
        checkpoints = []

        pool = await ActivityClients.postgres()
        if pool:
            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT id, created_at, metadata
                        FROM checkpoints
                        WHERE agent_id = $1
                          AND (expires_at IS NULL OR expires_at > NOW())
                        ORDER BY created_at DESC
                        LIMIT $2
                    """, agent_id, limit)

                    for row in rows:
                        checkpoints.append({
                            "checkpoint_id": row["id"],
                            "created_at": row["created_at"].isoformat(),
                            "metadata": json.loads(row["metadata"]) if row["metadata"] else {}
                        })
            except Exception as e:
                logger.error(f"Failed to list checkpoints: {e}")

        return checkpoints

    @staticmethod
    async def delete(checkpoint_id: str) -> bool:
        """Delete a checkpoint."""
        deleted = False

        pool = await ActivityClients.postgres()
        if pool:
            try:
                async with pool.acquire() as conn:
                    result = await conn.execute("""
                        DELETE FROM checkpoints WHERE id = $1
                    """, checkpoint_id)
                    deleted = result.endswith("1")
            except Exception as e:
                logger.error(f"Failed to delete checkpoint from PostgreSQL: {e}")

        driver = await ActivityClients.neo4j()
        if driver:
            try:
                async with driver.session() as session:
                    await session.run("""
                        MATCH (c:Checkpoint {id: $checkpoint_id})
                        DETACH DELETE c
                    """, checkpoint_id=checkpoint_id)
            except Exception as e:
                logger.warning(f"Failed to delete checkpoint from Neo4j: {e}")

        return deleted

    @staticmethod
    async def cleanup_expired() -> int:
        """Clean up expired checkpoints."""
        count = 0

        pool = await ActivityClients.postgres()
        if pool:
            try:
                async with pool.acquire() as conn:
                    result = await conn.execute("""
                        DELETE FROM checkpoints
                        WHERE expires_at IS NOT NULL AND expires_at < NOW()
                    """)
                    # Extract count from result like "DELETE 5"
                    parts = result.split()
                    if len(parts) == 2:
                        count = int(parts[1])
                    logger.info(f"Cleaned up {count} expired checkpoints")
            except Exception as e:
                logger.error(f"Failed to cleanup expired checkpoints: {e}")

        return count


# ============================================================================
# Agent Management Activities
# ============================================================================

@activity.defn
async def register_agent(agent_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Register an agent with the Engine.

    This is called when an agent's lifecycle workflow starts.
    The agent may already be connected via WebSocket.
    """
    activity.logger.info(f"Registering agent: {agent_id}")

    client = await ActivityClients.http()

    try:
        response = await client.post(
            f"{_config.engine_api_url}/agents/register",
            json={
                "agent_id": agent_id,
                "agent_type": config.get("agent_type", "generic"),
                "capabilities": config.get("capabilities", []),
                "tick_interval_ms": config.get("tick_interval_ms", 1000),
                "metadata": config.get("metadata", {}),
            },
        )
        response.raise_for_status()
        result = response.json()

        activity.logger.info(f"Agent {agent_id} registered successfully")
        return result

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:  # Already registered
            activity.logger.info(f"Agent {agent_id} already registered")
            return {"agent_id": agent_id, "status": "already_registered"}
        raise


@activity.defn
async def unregister_agent(agent_id: str) -> bool:
    """Unregister an agent from the Engine."""
    activity.logger.info(f"Unregistering agent: {agent_id}")

    client = await ActivityClients.http()

    try:
        response = await client.delete(
            f"{_config.engine_api_url}/agents/{agent_id}",
            params={"graceful": True},
        )
        response.raise_for_status()
        return True

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:  # Already gone
            return True
        raise


@activity.defn
async def find_available_agent(
    required_capabilities: List[str] = None,
) -> Optional[str]:
    """
    Find an available agent, optionally with required capabilities.

    Returns agent_id or None if no agent available.
    """
    activity.logger.info(
        f"Finding available agent with capabilities: {required_capabilities}"
    )

    client = await ActivityClients.http()

    try:
        response = await client.get(f"{_config.engine_api_url}/agents")
        response.raise_for_status()
        agents = response.json()

        # Filter by status and capabilities
        for agent in agents:
            if agent.get("status") != "idle":
                continue

            if required_capabilities:
                agent_caps = set(agent.get("capabilities", []))
                if not all(cap in agent_caps for cap in required_capabilities):
                    continue

            activity.logger.info(f"Found available agent: {agent.get('agent_id')}")
            return agent.get("agent_id")

        activity.logger.warning("No available agent found")
        return None

    except Exception as e:
        activity.logger.error(f"Error finding agent: {e}")
        raise


# ============================================================================
# Tick/Signal Activities
# ============================================================================

@activity.defn
async def send_tick(agent_id: str, tick_type: str = "poll") -> bool:
    """Send a tick to an agent."""
    activity.logger.debug(f"Sending {tick_type} tick to agent: {agent_id}")

    # Create tick
    tick = Tick(
        tick_id=str(uuid.uuid4()),
        tick_type=TickType(tick_type),
        timestamp=datetime.utcnow(),
        timestamp_ns=time.time_ns(),
        sequence=activity.info().attempt,
        deadline_ms=100,
    )

    # Option 1: Via Redis pub/sub (preferred for low latency)
    redis = await ActivityClients.redis()
    if redis:
        await redis.publish(
            f"agent:{agent_id}:ticks",
            json.dumps(tick.to_dict()),
        )
        return True

    # Option 2: Via HTTP API
    client = await ActivityClients.http()
    try:
        response = await client.post(
            f"{_config.engine_api_url}/ticks/{agent_id}",
            json=tick.to_dict(),
        )
        response.raise_for_status()
        return True
    except Exception as e:
        activity.logger.error(f"Failed to send tick: {e}")
        return False


@activity.defn
async def send_task_tick(agent_id: str, task: Dict[str, Any]) -> bool:
    """Send a tick with task assignment to an agent."""
    activity.logger.info(f"Sending task tick to agent {agent_id}: {task.get('task_id')}")

    tick = Tick(
        tick_id=str(uuid.uuid4()),
        tick_type=TickType.TASK,
        timestamp=datetime.utcnow(),
        timestamp_ns=time.time_ns(),
        sequence=0,
        scheduled_tasks=[Task(**task) if isinstance(task, dict) else task],
        deadline_ms=task.get("timeout_ms", 300000),
    )

    redis = await ActivityClients.redis()
    if redis:
        await redis.publish(
            f"agent:{agent_id}:ticks",
            json.dumps(tick.to_dict()),
        )
        return True

    client = await ActivityClients.http()
    response = await client.post(
        f"{_config.engine_api_url}/ticks/{agent_id}",
        json=tick.to_dict(),
    )
    response.raise_for_status()
    return True


@activity.defn
async def send_signal(agent_id: str, signal: Dict[str, Any]) -> bool:
    """Send a signal to an agent."""
    activity.logger.info(
        f"Sending signal to agent {agent_id}: {signal.get('signal_type')}"
    )

    client = await ActivityClients.http()

    response = await client.post(
        f"{_config.engine_api_url}/signals",
        json={
            "target_agent": agent_id,
            "signal_type": signal.get("signal_type"),
            "payload": signal.get("payload"),
            "preempt": signal.get("preempt", False),
        },
    )
    response.raise_for_status()
    return response.json().get("success", False)


@activity.defn
async def send_heartbeat(agent_id: str) -> bool:
    """Send heartbeat to an agent."""
    redis = await ActivityClients.redis()
    if redis:
        await redis.publish(
            f"agent:{agent_id}:heartbeat",
            json.dumps({
                "type": "heartbeat",
                "timestamp": time.time_ns(),
            }),
        )
        return True

    # Heartbeats via HTTP are less efficient but work
    client = await ActivityClients.http()
    await client.post(
        f"{_config.engine_api_url}/agents/{agent_id}/heartbeat"
    )
    return True


# ============================================================================
# State Activities
# ============================================================================

@activity.defn
async def checkpoint_state(
    agent_id: str,
    state: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
    ttl_seconds: Optional[int] = None
) -> str:
    """
    Save agent state checkpoint to both Redis (cache) and persistent storage.

    Args:
        agent_id: Agent identifier
        state: Agent state to checkpoint
        metadata: Optional metadata (workflow_id, reason, etc.)
        ttl_seconds: Optional time-to-live for the checkpoint

    Returns:
        checkpoint_id
    """
    activity.logger.info(f"Checkpointing state for agent: {agent_id}")

    checkpoint_id = f"ckpt-{agent_id}-{int(time.time())}"

    # Store in Redis for fast access (cache layer)
    redis = await ActivityClients.redis()
    if redis:
        await redis.hset(
            f"agent:{agent_id}:checkpoints",
            checkpoint_id,
            json.dumps(state),
        )
        await redis.set(
            f"agent:{agent_id}:latest_checkpoint",
            checkpoint_id,
        )
        # Set TTL on Redis if specified
        if ttl_seconds:
            await redis.expire(f"agent:{agent_id}:checkpoints", ttl_seconds)

    # Store in persistent storage (PostgreSQL/Neo4j)
    await CheckpointStore.save(
        checkpoint_id=checkpoint_id,
        agent_id=agent_id,
        state=state,
        metadata=metadata or {
            "workflow_run_id": activity.info().workflow_run_id if activity.info() else None,
            "activity_id": activity.info().activity_id if activity.info() else None,
        },
        ttl_seconds=ttl_seconds
    )

    activity.logger.info(f"Created checkpoint: {checkpoint_id}")
    return checkpoint_id


@activity.defn
async def restore_state(agent_id: str, state: Dict[str, Any]) -> bool:
    """Restore agent state from provided state dict."""
    activity.logger.info(f"Restoring state for agent: {agent_id}")

    client = await ActivityClients.http()

    response = await client.put(
        f"{_config.engine_api_url}/agents/{agent_id}/state",
        json=state,
    )
    response.raise_for_status()
    return True


@activity.defn
async def load_checkpoint(checkpoint_id: str) -> Optional[Dict[str, Any]]:
    """
    Load state from checkpoint.

    Tries Redis first (cache), then falls back to persistent storage.
    """
    activity.logger.info(f"Loading checkpoint: {checkpoint_id}")

    # Extract agent_id from checkpoint_id
    parts = checkpoint_id.split("-")
    if len(parts) >= 2:
        agent_id = parts[1]
    else:
        activity.logger.error(f"Invalid checkpoint_id format: {checkpoint_id}")
        return None

    # Try Redis cache first (fastest)
    redis = await ActivityClients.redis()
    if redis:
        state_json = await redis.hget(
            f"agent:{agent_id}:checkpoints",
            checkpoint_id,
        )
        if state_json:
            activity.logger.debug(f"Loaded checkpoint {checkpoint_id} from Redis cache")
            return json.loads(state_json)

    # Fall back to persistent storage
    checkpoint_data = await CheckpointStore.load(checkpoint_id)
    if checkpoint_data:
        state = checkpoint_data.get("state")

        # Populate Redis cache for next time
        if redis and state:
            await redis.hset(
                f"agent:{agent_id}:checkpoints",
                checkpoint_id,
                json.dumps(state),
            )

        activity.logger.debug(f"Loaded checkpoint {checkpoint_id} from persistent storage")
        return state

    activity.logger.warning(f"Checkpoint {checkpoint_id} not found")
    return None


@activity.defn
async def apply_state_delta(agent_id: str, delta: Dict[str, Any]) -> bool:
    """Apply a state delta to an agent."""
    activity.logger.info(f"Applying state delta to agent: {agent_id}")

    client = await ActivityClients.http()

    response = await client.patch(
        f"{_config.engine_api_url}/agents/{agent_id}/state",
        json=delta,
    )
    response.raise_for_status()
    return True


# ============================================================================
# Task Activities
# ============================================================================

@activity.defn
async def wait_for_task_result(
    agent_id: str,
    task_id: str,
    poll_interval_ms: int = 500,
) -> Any:
    """
    Wait for a task to complete and return its result.

    Uses activity heartbeat to prevent timeout during long tasks.
    """
    activity.logger.info(f"Waiting for task result: {task_id} from {agent_id}")

    redis = await ActivityClients.redis()
    client = await ActivityClients.http()

    start_time = time.time()
    max_wait = activity.info().start_to_close_timeout.total_seconds()

    while (time.time() - start_time) < max_wait:
        # Heartbeat to keep activity alive
        activity.heartbeat()

        # Check Redis for result (fast path)
        if redis:
            result = await redis.get(f"task:{task_id}:result")
            if result:
                return json.loads(result)

            error = await redis.get(f"task:{task_id}:error")
            if error:
                raise Exception(json.loads(error))

        # Check via HTTP (slower path)
        try:
            response = await client.get(
                f"{_config.engine_api_url}/tasks/{task_id}"
            )
            if response.status_code == 200:
                task_data = response.json()
                status = task_data.get("status")

                if status == "completed":
                    return task_data.get("result")
                elif status in ["failed", "cancelled"]:
                    raise Exception(task_data.get("error", f"Task {status}"))

        except httpx.HTTPStatusError:
            pass

        await asyncio.sleep(poll_interval_ms / 1000)

    raise TimeoutError(f"Task {task_id} did not complete in time")


@activity.defn
async def notify_task_completed(
    agent_id: str,
    task_id: str,
    result: Any,
) -> bool:
    """Notify that a task completed successfully."""
    activity.logger.info(f"Task completed: {task_id}")

    redis = await ActivityClients.redis()
    if redis:
        await redis.set(
            f"task:{task_id}:result",
            json.dumps(result),
            ex=3600,  # 1 hour TTL
        )
        await redis.publish(
            f"task:{task_id}:status",
            json.dumps({"status": "completed", "result": result}),
        )

    client = await ActivityClients.http()
    await client.patch(
        f"{_config.engine_api_url}/tasks/{task_id}",
        json={"status": "completed", "result": result},
    )
    return True


@activity.defn
async def notify_task_failed(
    agent_id: str,
    task_id: str,
    error: str,
) -> bool:
    """Notify that a task failed."""
    activity.logger.warning(f"Task failed: {task_id} - {error}")

    redis = await ActivityClients.redis()
    if redis:
        await redis.set(
            f"task:{task_id}:error",
            json.dumps(error),
            ex=3600,
        )
        await redis.publish(
            f"task:{task_id}:status",
            json.dumps({"status": "failed", "error": error}),
        )

    client = await ActivityClients.http()
    await client.patch(
        f"{_config.engine_api_url}/tasks/{task_id}",
        json={"status": "failed", "error": error},
    )
    return True


@activity.defn
async def notify_task_cancelled(agent_id: str, task_id: str) -> bool:
    """Notify that a task was cancelled."""
    activity.logger.info(f"Task cancelled: {task_id}")

    client = await ActivityClients.http()
    await client.patch(
        f"{_config.engine_api_url}/tasks/{task_id}",
        json={"status": "cancelled"},
    )
    return True


# ============================================================================
# Multi-Agent Coordination Activities
# ============================================================================

@activity.defn
async def decompose_task(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Decompose a complex task into subtasks.

    This is a placeholder - actual implementation would use:
    - LLM to analyze and split task
    - Domain-specific decomposition rules
    - Historical patterns
    """
    activity.logger.info(f"Decomposing task: {task.get('task_id')}")

    # Simple example: if task has explicit subtasks, use them
    if "subtasks" in task:
        return task["subtasks"]

    # Otherwise, create a single subtask (pass-through)
    return [{
        **task,
        "subtask_id": f"{task.get('task_id')}-sub-0",
        "parent_task_id": task.get("task_id"),
    }]


@activity.defn
async def aggregate_results(
    task: Dict[str, Any],
    results: Dict[str, Any],
    failed: List[str],
) -> Dict[str, Any]:
    """
    Aggregate results from multiple subtasks.

    This is a placeholder - actual implementation would use:
    - Domain-specific aggregation logic
    - LLM to synthesize results
    - Weighted averaging, voting, etc.
    """
    activity.logger.info(
        f"Aggregating {len(results)} results, {len(failed)} failed"
    )

    return {
        "task_id": task.get("task_id"),
        "aggregated_results": results,
        "failed_subtasks": failed,
        "success_rate": len(results) / max(len(results) + len(failed), 1),
    }


# ============================================================================
# Tool Execution Activities
# ============================================================================

@activity.defn
async def execute_tool(
    tool_id: str,
    params: Dict[str, Any],
    agent_id: Optional[str] = None,
) -> Any:
    """
    Execute a tool via the Tool Gateway.

    Supports both internal (Neo4j) and external (MCP) tools.
    """
    activity.logger.info(f"Executing tool: {tool_id}")

    client = await ActivityClients.http()

    response = await client.post(
        f"{_config.tool_gateway_url}/unified/execute",
        json={
            "tool_id": tool_id,
            "input_data": params,
            "agent_id": agent_id,
        },
        timeout=60.0,  # Tools can take time
    )
    response.raise_for_status()
    return response.json()


@activity.defn
async def search_tools(
    query: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Search for tools via the Tool Gateway."""
    activity.logger.info(f"Searching tools: {query}")

    client = await ActivityClients.http()

    response = await client.get(
        f"{_config.tool_gateway_url}/unified/search",
        params={"q": query, "limit": limit},
    )
    response.raise_for_status()
    return response.json().get("tools", [])


# ============================================================================
# Observability Activities
# ============================================================================

@activity.defn
async def emit_trace_event(
    event_type: str,
    agent_id: Optional[str] = None,
    task_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> bool:
    """Emit a trace event for observability."""
    event = {
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "agent_id": agent_id,
        "task_id": task_id,
        "data": data or {},
        "activity_id": activity.info().activity_id,
        "workflow_id": activity.info().workflow_id,
    }

    # Log locally
    activity.logger.info(f"Trace event: {json.dumps(event)}")

    # Send to observability backend
    # TODO: PostHog, Datadog, etc.

    return True


# ============================================================================
# Activity Registry
# ============================================================================

# All activities for worker registration
ALL_ACTIVITIES = [
    # Agent Management
    register_agent,
    unregister_agent,
    find_available_agent,
    # Tick/Signal
    send_tick,
    send_task_tick,
    send_signal,
    send_heartbeat,
    # State
    checkpoint_state,
    restore_state,
    load_checkpoint,
    apply_state_delta,
    # Task
    wait_for_task_result,
    notify_task_completed,
    notify_task_failed,
    notify_task_cancelled,
    # Multi-Agent
    decompose_task,
    aggregate_results,
    # Tools
    execute_tool,
    search_tools,
    # Observability
    emit_trace_event,
]
