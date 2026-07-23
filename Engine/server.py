"""
Engine API Server

The main WebSocket/HTTP server that agents connect to.
"""

import asyncio
import logging
import json
import secrets
import hashlib
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

from .models import (
    Tick,
    Signal,
    SignalType,
    Task,
    TaskStatus,
    AgentConfig,
    AgentStatus,
)
from .connection import ConnectionManager
from .tick_bus import TickBus, SignalBus

logger = logging.getLogger(__name__)


# ============================================================================
# Session Token Management
# ============================================================================

@dataclass
class Session:
    """Represents an authenticated session."""
    token_hash: str
    agent_id: str
    created_at: datetime
    expires_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at

    def refresh(self, ttl_seconds: int = 3600) -> None:
        """Refresh the session expiration."""
        self.last_activity = datetime.now(timezone.utc)
        self.expires_at = self.last_activity + timedelta(seconds=ttl_seconds)


class SessionManager:
    """
    Manages session tokens for agent authentication.

    Features:
    - Secure token generation using secrets.token_urlsafe
    - Token hashing for secure storage
    - Automatic expiration with configurable TTL
    - Session refresh on activity
    - Optional Redis backend for distributed deployments
    """

    def __init__(
        self,
        ttl_seconds: int = 3600,
        max_sessions_per_agent: int = 5,
        redis_client=None
    ):
        self.ttl_seconds = ttl_seconds
        self.max_sessions_per_agent = max_sessions_per_agent
        self.redis_client = redis_client

        # In-memory storage (fallback if Redis not available)
        self._sessions: Dict[str, Session] = {}
        self._agent_sessions: Dict[str, set] = {}  # agent_id -> set of token hashes

        # Start cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the session manager."""
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
        logger.info("Session manager started")

    async def stop(self):
        """Stop the session manager."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Session manager stopped")

    def _hash_token(self, token: str) -> str:
        """Hash a token for secure storage."""
        return hashlib.sha256(token.encode()).hexdigest()

    async def create_session(
        self,
        agent_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new session for an agent.

        Args:
            agent_id: The agent identifier
            metadata: Optional metadata to store with the session

        Returns:
            The session token (unhashed)
        """
        # Generate secure token
        token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(token)

        now = datetime.now(timezone.utc)
        session = Session(
            token_hash=token_hash,
            agent_id=agent_id,
            created_at=now,
            expires_at=now + timedelta(seconds=self.ttl_seconds),
            metadata=metadata or {},
            last_activity=now
        )

        # Enforce max sessions per agent
        if agent_id in self._agent_sessions:
            if len(self._agent_sessions[agent_id]) >= self.max_sessions_per_agent:
                # Remove oldest session
                oldest_hash = min(
                    self._agent_sessions[agent_id],
                    key=lambda h: self._sessions[h].created_at if h in self._sessions else datetime.max
                )
                await self.revoke_session_by_hash(oldest_hash)

        # Store session
        if self.redis_client:
            await self._store_session_redis(session)
        else:
            self._sessions[token_hash] = session
            if agent_id not in self._agent_sessions:
                self._agent_sessions[agent_id] = set()
            self._agent_sessions[agent_id].add(token_hash)

        logger.info(f"Created session for agent {agent_id}")
        return token

    async def validate_token(self, token: str, refresh: bool = True) -> Optional[Session]:
        """
        Validate a session token.

        Args:
            token: The session token to validate
            refresh: Whether to refresh the session on validation

        Returns:
            The Session if valid, None otherwise
        """
        token_hash = self._hash_token(token)

        if self.redis_client:
            session = await self._get_session_redis(token_hash)
        else:
            session = self._sessions.get(token_hash)

        if not session:
            return None

        if session.is_expired:
            await self.revoke_session_by_hash(token_hash)
            return None

        # Refresh session
        if refresh:
            session.refresh(self.ttl_seconds)
            if self.redis_client:
                await self._store_session_redis(session)

        return session

    async def revoke_session(self, token: str) -> bool:
        """Revoke a session by token."""
        token_hash = self._hash_token(token)
        return await self.revoke_session_by_hash(token_hash)

    async def revoke_session_by_hash(self, token_hash: str) -> bool:
        """Revoke a session by token hash."""
        if self.redis_client:
            return await self._revoke_session_redis(token_hash)

        if token_hash in self._sessions:
            session = self._sessions.pop(token_hash)
            if session.agent_id in self._agent_sessions:
                self._agent_sessions[session.agent_id].discard(token_hash)
                if not self._agent_sessions[session.agent_id]:
                    del self._agent_sessions[session.agent_id]
            logger.info(f"Revoked session for agent {session.agent_id}")
            return True
        return False

    async def revoke_all_sessions(self, agent_id: str) -> int:
        """Revoke all sessions for an agent."""
        count = 0
        if agent_id in self._agent_sessions:
            for token_hash in list(self._agent_sessions[agent_id]):
                if await self.revoke_session_by_hash(token_hash):
                    count += 1
        return count

    async def _cleanup_expired_sessions(self):
        """Background task to clean up expired sessions."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                if self.redis_client:
                    # Redis handles expiration automatically with TTL
                    continue

                now = datetime.now(timezone.utc)
                expired = [
                    h for h, s in self._sessions.items()
                    if s.expires_at < now
                ]

                for token_hash in expired:
                    await self.revoke_session_by_hash(token_hash)

                if expired:
                    logger.info(f"Cleaned up {len(expired)} expired sessions")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error cleaning up sessions: {e}")

    async def _store_session_redis(self, session: Session):
        """Store session in Redis."""
        key = f"session:{session.token_hash}"
        data = {
            "token_hash": session.token_hash,
            "agent_id": session.agent_id,
            "created_at": session.created_at.isoformat(),
            "expires_at": session.expires_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "metadata": json.dumps(session.metadata)
        }
        ttl = int((session.expires_at - datetime.now(timezone.utc)).total_seconds())
        await self.redis_client.hset(key, mapping=data)
        await self.redis_client.expire(key, max(ttl, 1))

        # Track agent sessions
        agent_key = f"agent_sessions:{session.agent_id}"
        await self.redis_client.sadd(agent_key, session.token_hash)
        await self.redis_client.expire(agent_key, self.ttl_seconds * 2)

    async def _get_session_redis(self, token_hash: str) -> Optional[Session]:
        """Get session from Redis."""
        key = f"session:{token_hash}"
        data = await self.redis_client.hgetall(key)
        if not data:
            return None

        return Session(
            token_hash=data[b"token_hash"].decode(),
            agent_id=data[b"agent_id"].decode(),
            created_at=datetime.fromisoformat(data[b"created_at"].decode()),
            expires_at=datetime.fromisoformat(data[b"expires_at"].decode()),
            last_activity=datetime.fromisoformat(data[b"last_activity"].decode()),
            metadata=json.loads(data[b"metadata"].decode())
        )

    async def _revoke_session_redis(self, token_hash: str) -> bool:
        """Revoke session from Redis."""
        key = f"session:{token_hash}"
        session = await self._get_session_redis(token_hash)
        if session:
            agent_key = f"agent_sessions:{session.agent_id}"
            await self.redis_client.srem(agent_key, token_hash)
        return bool(await self.redis_client.delete(key))


# Global session manager instance
_session_manager: Optional[SessionManager] = None


def get_session_manager() -> SessionManager:
    """Get the global session manager."""
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager()
    return _session_manager


# Security scheme
security = HTTPBearer(auto_error=False)


async def verify_session(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    x_session_token: Optional[str] = Header(None)
) -> Session:
    """
    Dependency to verify session token.

    Accepts token via:
    - Authorization: Bearer <token>
    - X-Session-Token header
    """
    token = None
    if credentials:
        token = credentials.credentials
    elif x_session_token:
        token = x_session_token

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Session token required"
        )

    session = await get_session_manager().validate_token(token)
    if not session:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired session token"
        )

    return session


# ============================================================================
# Pydantic Models
# ============================================================================

class AgentRegisterRequest(BaseModel):
    """Request to register an agent."""
    agent_id: str
    agent_type: str = "generic"
    capabilities: list[str] = Field(default_factory=list)
    tick_interval_ms: int = 1000
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AgentRegisterResponse(BaseModel):
    """Response after agent registration."""
    agent_id: str
    session_token: str
    tick_interval_ms: int
    engine_version: str = "0.1.0"


class TaskCreateRequest(BaseModel):
    """Request to create a task."""
    name: str
    description: str = ""
    task_type: str = "generic"
    input_data: Dict[str, Any] = Field(default_factory=dict)
    priority: int = 5
    timeout_ms: int = 300000
    target_agent: Optional[str] = None


class TaskResponse(BaseModel):
    """Task information."""
    task_id: str
    name: str
    status: str
    assigned_agent: Optional[str]
    created_at: str


class SignalRequest(BaseModel):
    """Request to send a signal."""
    target_agent: str
    signal_type: str
    payload: Any = None
    preempt: bool = False


class EngineStatsResponse(BaseModel):
    """Engine statistics."""
    connections: int
    ticks_per_second: float
    active_tasks: int
    uptime_seconds: float


# ============================================================================
# Engine Server
# ============================================================================

class EngineServer:
    """
    The Engine API server.

    Provides:
    - WebSocket endpoint for agent connections
    - HTTP API for management
    - Task scheduling
    - Signal delivery
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        tick_interval_ms: int = 100,
    ):
        self.redis_url = redis_url
        self.tick_interval_ms = tick_interval_ms

        # Core components
        self.connection_manager = ConnectionManager()
        self.tick_bus = TickBus(self.connection_manager, redis_url)
        self.signal_bus = SignalBus(self.connection_manager)

        # Task tracking
        self._tasks: Dict[str, Task] = {}
        self._task_queue: asyncio.Queue = asyncio.Queue()

        # Start time
        self._start_time: Optional[datetime] = None

        # FastAPI app
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Create the FastAPI application."""
        app = FastAPI(
            title="Engine API",
            description="Real-time agent substrate with Temporal orchestration",
            version="0.1.0",
        )

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Register routes
        self._register_routes(app)

        # Startup/shutdown
        @app.on_event("startup")
        async def startup():
            await self.start()

        @app.on_event("shutdown")
        async def shutdown():
            await self.stop()

        return app

    def _register_routes(self, app: FastAPI) -> None:
        """Register all routes."""

        # ====================================================================
        # WebSocket Connection
        # ====================================================================

        @app.websocket("/ws/{agent_id}")
        async def websocket_endpoint(
            websocket: WebSocket,
            agent_id: str,
            agent_type: str = Query("generic"),
            tick_interval_ms: int = Query(1000),
        ):
            """
            WebSocket endpoint for agent connections.

            The agent maintains a persistent connection and receives:
            - Ticks (scheduled work)
            - Signals (interrupts)
            - State deltas (sync)
            - Heartbeats (liveness)
            """
            await websocket.accept()

            logger.info(f"Agent {agent_id} connecting via WebSocket")

            # Create config
            config = AgentConfig(
                agent_id=agent_id,
                agent_type=agent_type,
                tick_interval_ms=tick_interval_ms,
            )

            # Register connection
            async def send(message: str):
                await websocket.send_text(message)

            conn = await self.connection_manager.register(
                agent_id=agent_id,
                config=config,
                send_func=send,
            )

            # Set tick interval for this agent
            self.tick_bus.set_agent_interval(agent_id, tick_interval_ms)

            try:
                # Connection loop
                while True:
                    try:
                        message = await websocket.receive_text()
                        await conn.handle_message(message)
                    except WebSocketDisconnect:
                        logger.info(f"Agent {agent_id} disconnected")
                        break

            finally:
                # Cleanup
                self.tick_bus.remove_agent(agent_id)
                await self.connection_manager.unregister(agent_id)

        # ====================================================================
        # Agent Management
        # ====================================================================

        @app.post("/agents/register", response_model=AgentRegisterResponse)
        async def register_agent(request: AgentRegisterRequest):
            """
            Pre-register an agent before WebSocket connection.

            Returns a session token for authentication.
            The token should be included in subsequent requests via:
            - Authorization: Bearer <token>
            - X-Session-Token: <token>
            """
            session_manager = get_session_manager()

            # Create session with agent metadata
            session_token = await session_manager.create_session(
                agent_id=request.agent_id,
                metadata={
                    "agent_type": request.agent_type,
                    "capabilities": request.capabilities,
                    "tick_interval_ms": request.tick_interval_ms,
                    **request.metadata
                }
            )

            logger.info(f"Registered agent {request.agent_id} with session token")

            return AgentRegisterResponse(
                agent_id=request.agent_id,
                session_token=session_token,
                tick_interval_ms=request.tick_interval_ms,
            )

        @app.get("/agents")
        async def list_agents():
            """List all connected agents."""
            presence = self.connection_manager.get_presence()
            return [p.to_dict() for p in presence]

        @app.get("/agents/{agent_id}")
        async def get_agent(agent_id: str):
            """Get agent information."""
            conn = self.connection_manager.get(agent_id)
            if not conn:
                raise HTTPException(status_code=404, detail="Agent not found")

            return {
                "agent_id": agent_id,
                "status": conn.state.status.value,
                "connected": conn.connected,
                "state": conn.state.to_dict(),
                "presence": conn.presence.to_dict(),
            }

        @app.delete("/agents/{agent_id}")
        async def disconnect_agent(agent_id: str, graceful: bool = True):
            """Disconnect an agent."""
            if graceful:
                await self.signal_bus.shutdown(agent_id, graceful=True)
            else:
                await self.connection_manager.unregister(agent_id)

            return {"status": "disconnected", "agent_id": agent_id}

        # ====================================================================
        # Tasks
        # ====================================================================

        @app.post("/tasks", response_model=TaskResponse)
        async def create_task(request: TaskCreateRequest):
            """
            Create a new task.

            If target_agent is specified, assign directly.
            Otherwise, put in queue for next available agent.
            """
            task = Task(
                name=request.name,
                description=request.description,
                task_type=request.task_type,
                input_data=request.input_data,
                priority=request.priority,
                timeout_ms=request.timeout_ms,
            )

            self._tasks[task.task_id] = task

            if request.target_agent:
                # Assign directly
                task.assigned_agent = request.target_agent
                task.assigned_at = datetime.utcnow()
                task.status = TaskStatus.ASSIGNED

                # Schedule task tick
                self.tick_bus.schedule_task(request.target_agent, task)
            else:
                # Add to queue
                await self._task_queue.put(task)

            return TaskResponse(
                task_id=task.task_id,
                name=task.name,
                status=task.status.value,
                assigned_agent=task.assigned_agent,
                created_at=datetime.utcnow().isoformat(),
            )

        @app.get("/tasks")
        async def list_tasks(
            status: Optional[str] = None,
            limit: int = 100,
        ):
            """List tasks."""
            tasks = list(self._tasks.values())

            if status:
                tasks = [t for t in tasks if t.status.value == status]

            return [
                TaskResponse(
                    task_id=t.task_id,
                    name=t.name,
                    status=t.status.value,
                    assigned_agent=t.assigned_agent,
                    created_at=t.assigned_at.isoformat() if t.assigned_at else "",
                )
                for t in tasks[:limit]
            ]

        @app.get("/tasks/{task_id}")
        async def get_task(task_id: str):
            """Get task details."""
            task = self._tasks.get(task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")

            return task.to_dict()

        @app.delete("/tasks/{task_id}")
        async def cancel_task(task_id: str):
            """Cancel a task."""
            task = self._tasks.get(task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")

            if task.assigned_agent:
                # Signal agent to cancel
                signal = Signal(
                    signal_type=SignalType.CANCEL,
                    target_agent=task.assigned_agent,
                    payload={"task_id": task_id},
                )
                await self.signal_bus.send(signal)

            task.status = TaskStatus.CANCELLED
            return {"status": "cancelled", "task_id": task_id}

        # ====================================================================
        # Signals
        # ====================================================================

        @app.post("/signals")
        async def send_signal(request: SignalRequest):
            """Send a signal to an agent."""
            try:
                signal_type = SignalType(request.signal_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid signal type: {request.signal_type}")

            signal = Signal(
                signal_type=signal_type,
                target_agent=request.target_agent,
                payload=request.payload,
                preempt=request.preempt,
            )

            success = await self.signal_bus.send(signal)

            return {
                "success": success,
                "signal_id": signal.signal_id,
            }

        @app.post("/signals/broadcast")
        async def broadcast_signal(
            signal_type: str,
            payload: Any = None,
        ):
            """Broadcast a signal to all agents."""
            try:
                st = SignalType(signal_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid signal type: {signal_type}")

            count = await self.signal_bus.broadcast(st, payload)

            return {"count": count}

        # ====================================================================
        # Statistics
        # ====================================================================

        @app.get("/stats", response_model=EngineStatsResponse)
        async def get_stats():
            """Get engine statistics."""
            uptime = 0.0
            if self._start_time:
                uptime = (datetime.utcnow() - self._start_time).total_seconds()

            tick_stats = self.tick_bus.get_stats()
            generator_stats = tick_stats.get("generator", {})

            # Estimate ticks per second
            ticks_generated = generator_stats.get("ticks_generated", 0)
            tps = ticks_generated / max(uptime, 1)

            active_tasks = sum(
                1 for t in self._tasks.values()
                if t.status in (TaskStatus.ASSIGNED, TaskStatus.RUNNING)
            )

            return EngineStatsResponse(
                connections=self.connection_manager.connection_count,
                ticks_per_second=round(tps, 2),
                active_tasks=active_tasks,
                uptime_seconds=round(uptime, 2),
            )

        @app.get("/stats/detailed")
        async def get_detailed_stats():
            """Get detailed statistics."""
            return {
                "tick_bus": self.tick_bus.get_stats(),
                "signal_bus": self.signal_bus.get_stats(),
                "connections": self.connection_manager.get_stats(),
                "tasks": {
                    "total": len(self._tasks),
                    "by_status": {
                        status.value: sum(
                            1 for t in self._tasks.values()
                            if t.status == status
                        )
                        for status in TaskStatus
                    },
                },
            }

        # ====================================================================
        # Health
        # ====================================================================

        @app.get("/health")
        async def health():
            """Health check."""
            return {
                "status": "healthy",
                "connections": self.connection_manager.connection_count,
                "uptime": (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            }

        @app.get("/")
        async def root():
            """API info."""
            return {
                "name": "Engine API",
                "version": "0.1.0",
                "description": "Real-time agent substrate",
                "endpoints": {
                    "websocket": "/ws/{agent_id}",
                    "agents": "/agents",
                    "tasks": "/tasks",
                    "signals": "/signals",
                    "stats": "/stats",
                },
            }

    async def start(self) -> None:
        """Start the engine."""
        self._start_time = datetime.now(timezone.utc)

        # Start session manager
        await get_session_manager().start()

        await self.connection_manager.start()
        await self.tick_bus.start()

        # Start task assignment loop
        asyncio.create_task(self._task_assignment_loop())

        logger.info("Engine started")

    async def stop(self) -> None:
        """Stop the engine."""
        await self.tick_bus.stop()
        await self.connection_manager.stop()

        # Stop session manager
        await get_session_manager().stop()

        logger.info("Engine stopped")

    async def _task_assignment_loop(self) -> None:
        """Background loop to assign queued tasks to available agents."""
        while True:
            try:
                task = await self._task_queue.get()

                # Find an available agent
                available = [
                    conn for conn in self.connection_manager.get_all()
                    if conn.state.status == AgentStatus.IDLE
                ]

                if available:
                    # Sort by tasks completed (load balancing)
                    available.sort(key=lambda c: c.state.tasks_completed)
                    agent = available[0]

                    # Assign
                    task.assigned_agent = agent.agent_id
                    task.assigned_at = datetime.utcnow()
                    task.status = TaskStatus.ASSIGNED

                    # Schedule task tick
                    self.tick_bus.schedule_task(agent.agent_id, task)

                    logger.info(f"Assigned task {task.task_id} to agent {agent.agent_id}")
                else:
                    # No available agents, re-queue with delay
                    await asyncio.sleep(0.1)
                    await self._task_queue.put(task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Task assignment error: {e}")
                await asyncio.sleep(1)


# ============================================================================
# Factory
# ============================================================================

def create_engine_app(
    redis_url: Optional[str] = None,
    tick_interval_ms: int = 100,
) -> FastAPI:
    """Create an Engine API FastAPI application."""
    server = EngineServer(
        redis_url=redis_url,
        tick_interval_ms=tick_interval_ms,
    )
    return server.app


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    from dsco_shared.config.service_registry import ServiceName, canonical_port

    app = create_engine_app()
    uvicorn.run(app, host="0.0.0.0", port=canonical_port(ServiceName.ENGINE))
