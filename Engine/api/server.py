"""
FastAPI Server for Agent Orchestration Platform

REST API endpoints for task submission, agent management, and monitoring.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import uuid
import asyncio

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .models import (
    SubmitTaskRequest,
    SpawnAgentRequest,
    ExecuteWorkflowRequest,
    HealthCheckRequest,
    TaskResponse,
    TaskStatusResponse,
    AgentResponse,
    PoolStatusResponse,
    WorkflowStatusResponse,
    GPUStatusResponse,
    SystemStatusResponse,
    ErrorResponse,
    TaskType,
    WorkflowPattern,
)

logger = logging.getLogger(__name__)


class AgentOrchestrationAPI:
    """FastAPI application for agent orchestration."""

    def __init__(self, agent_pool=None, workflow_engine=None, graphsub_client=None, redis_mailbox=None):
        """Initialize API with optional runtime components."""
        self.app = FastAPI(
            title="Agent Orchestration Platform",
            description="REST API for multi-agent task execution and monitoring",
            version="1.0.0",
        )

        # Runtime components (optional, allows fallback to in-memory)
        self.agent_pool = agent_pool
        self.workflow_engine = workflow_engine
        self.graphsub_client = graphsub_client
        self.redis_mailbox = redis_mailbox

        # In-memory storage (fallback for development)
        self.tasks: Dict[str, Dict] = {}
        self.agents: Dict[str, Dict] = {}
        self.workflows: Dict[str, Dict] = {}
        self.start_time = datetime.utcnow()
        self.request_count = 0

        # Setup CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup all API routes."""

        # Health check
        @self.app.get("/health")
        async def health_check():
            """System health check."""
            return {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
            }

        # Task endpoints
        @self.app.post("/api/tasks", response_model=TaskResponse)
        async def submit_task(request: SubmitTaskRequest):
            """Submit a new task."""
            self.request_count += 1
            task_id = f"task-{uuid.uuid4().hex[:8]}"

            task = {
                "task_id": task_id,
                "type": request.task_type.value,
                "data": request.task_data,
                "required_capabilities": request.required_capabilities,
                "priority": request.priority,
                "estimated_cost": request.estimated_cost,
                "status": "pending",
                "created_at": datetime.utcnow(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
            }

            self.tasks[task_id] = task

            # Publish to Redis mailbox if available
            if self.redis_mailbox:
                try:
                    await self.redis_mailbox.publish_task(task_id, task)
                except Exception as e:
                    logger.warning(f"Failed to publish task to Redis: {e}")

            logger.info(f"Submitted task: {task_id}")

            return TaskResponse(
                task_id=task_id,
                status="pending",
                created_at=task["created_at"],
                estimated_completion=datetime.utcnow() + timedelta(minutes=1),
            )

        @self.app.get("/api/tasks/{task_id}", response_model=TaskStatusResponse)
        async def get_task_status(task_id: str):
            """Get task status."""
            if task_id not in self.tasks:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

            task = self.tasks[task_id]

            return TaskStatusResponse(
                task_id=task_id,
                status=task["status"],
                progress=0.0,  # Would calculate from execution
                created_at=task["created_at"],
                started_at=task["started_at"],
                completed_at=task["completed_at"],
                result=task["result"],
                error=task["error"],
                cost_incurred=0.0,  # Would track actual cost
            )

        @self.app.get("/api/tasks", response_model=List[TaskStatusResponse])
        async def list_tasks(
            status: Optional[str] = Query(None),
            limit: int = Query(100, ge=1, le=1000),
        ):
            """List all tasks with optional filtering."""
            tasks = list(self.tasks.values())

            if status:
                tasks = [t for t in tasks if t["status"] == status]

            tasks = sorted(tasks, key=lambda t: t["created_at"], reverse=True)
            tasks = tasks[:limit]

            return [
                TaskStatusResponse(
                    task_id=t["task_id"],
                    status=t["status"],
                    progress=0.0,
                    created_at=t["created_at"],
                    started_at=t["started_at"],
                    completed_at=t["completed_at"],
                    result=t["result"],
                    error=t["error"],
                )
                for t in tasks
            ]

        # Agent endpoints
        @self.app.post("/api/agents/spawn", response_model=AgentResponse)
        async def spawn_agent(request: SpawnAgentRequest):
            """Spawn a new agent."""
            self.request_count += 1
            agent_id = f"agent-{uuid.uuid4().hex[:8]}"

            agent = {
                "agent_id": agent_id,
                "model": request.model,
                "pool_name": request.pool_name,
                "status": "healthy",
                "budget_remaining": request.budget,
                "budget_spent": 0.0,
                "tasks_completed": 0,
                "tasks_failed": 0,
                "capabilities": request.capabilities,
                "created_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
            }

            self.agents[agent_id] = agent

            # Spawn agent in pool if available
            if self.agent_pool:
                try:
                    await self.agent_pool.spawn_agent(
                        agent_id=agent_id,
                        model=request.model,
                        budget=request.budget,
                        capabilities=request.capabilities,
                        pool_name=request.pool_name,
                    )
                except Exception as e:
                    logger.warning(f"Failed to spawn agent in pool: {e}")

            logger.info(f"Spawned agent: {agent_id}")

            return AgentResponse(
                agent_id=agent_id,
                model=request.model,
                status="healthy",
                budget_remaining=request.budget,
                tasks_completed=0,
                tasks_failed=0,
                capabilities=request.capabilities,
                created_at=agent["created_at"],
                last_heartbeat=agent["last_heartbeat"],
            )

        @self.app.get("/api/agents/{agent_id}", response_model=AgentResponse)
        async def get_agent(agent_id: str):
            """Get agent details."""
            if agent_id not in self.agents:
                raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

            agent = self.agents[agent_id]

            return AgentResponse(
                agent_id=agent_id,
                model=agent["model"],
                status=agent["status"],
                budget_remaining=agent["budget_remaining"],
                tasks_completed=agent["tasks_completed"],
                tasks_failed=agent["tasks_failed"],
                capabilities=agent["capabilities"],
                created_at=agent["created_at"],
                last_heartbeat=agent["last_heartbeat"],
            )

        @self.app.get("/api/agents", response_model=List[AgentResponse])
        async def list_agents(
            status: Optional[str] = Query(None),
            pool: Optional[str] = Query(None),
        ):
            """List all agents."""
            agents = list(self.agents.values())

            if status:
                agents = [a for a in agents if a["status"] == status]

            if pool:
                agents = [a for a in agents if a["pool_name"] == pool]

            return [
                AgentResponse(
                    agent_id=a["agent_id"],
                    model=a["model"],
                    status=a["status"],
                    budget_remaining=a["budget_remaining"],
                    tasks_completed=a["tasks_completed"],
                    tasks_failed=a["tasks_failed"],
                    capabilities=a["capabilities"],
                    created_at=a["created_at"],
                    last_heartbeat=a["last_heartbeat"],
                )
                for a in agents
            ]

        @self.app.post("/api/agents/{agent_id}/health")
        async def check_agent_health(agent_id: str):
            """Check agent health."""
            if agent_id not in self.agents:
                raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

            agent = self.agents[agent_id]
            agent["last_heartbeat"] = datetime.utcnow()

            return {"agent_id": agent_id, "status": agent["status"], "healthy": True}

        @self.app.delete("/api/agents/{agent_id}")
        async def prune_agent(agent_id: str):
            """Remove an agent from the pool."""
            if agent_id not in self.agents:
                raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

            del self.agents[agent_id]
            logger.info(f"Pruned agent: {agent_id}")

            return {"agent_id": agent_id, "pruned": True}

        # Pool endpoints
        @self.app.get("/api/pool/status", response_model=PoolStatusResponse)
        async def get_pool_status(pool_name: str = Query("default")):
            """Get pool status and metrics."""
            agents = [
                a for a in self.agents.values()
                if a["pool_name"] == pool_name
            ]

            healthy = sum(1 for a in agents if a["status"] == "healthy")
            degraded = sum(1 for a in agents if a["status"] == "degraded")
            unhealthy = sum(1 for a in agents if a["status"] == "unhealthy")

            total_budget = sum(a["budget_remaining"] for a in agents)
            avg_confidence = 0.0
            if agents:
                total_confidence = 0.0
                count = 0
                for a in agents:
                    for conf in a["capabilities"].values():
                        total_confidence += conf
                        count += 1
                avg_confidence = total_confidence / count if count > 0 else 0.0

            pending_tasks = sum(
                1 for t in self.tasks.values() if t["status"] == "pending"
            )
            executing_tasks = sum(
                1 for t in self.tasks.values() if t["status"] == "executing"
            )

            utilization = len(agents) / 10.0 if len(agents) > 0 else 0.0

            return PoolStatusResponse(
                pool_name=pool_name,
                total_agents=len(agents),
                healthy_agents=healthy,
                degraded_agents=degraded,
                unhealthy_agents=unhealthy,
                capacity_utilization=min(utilization, 1.0),
                total_budget_remaining=total_budget,
                pending_tasks=pending_tasks,
                executing_tasks=executing_tasks,
                avg_capability_confidence=avg_confidence,
            )

        # Workflow endpoints
        @self.app.post("/api/workflows", response_model=WorkflowStatusResponse)
        async def execute_workflow(request: ExecuteWorkflowRequest):
            """Execute a multi-agent workflow."""
            self.request_count += 1
            workflow_id = f"wf-{uuid.uuid4().hex[:8]}"

            workflow = {
                "workflow_id": workflow_id,
                "pattern": request.pattern.value,
                "tasks": request.tasks,
                "status": "running",
                "created_at": datetime.utcnow(),
                "completed_at": None,
                "results": {},
                "errors": {},
            }

            self.workflows[workflow_id] = workflow

            # Execute workflow in engine if available
            if self.workflow_engine:
                try:
                    # Start workflow execution asynchronously (fire and forget)
                    asyncio.create_task(
                        self.workflow_engine.execute_workflow(
                            workflow_id=workflow_id,
                            pattern=request.pattern.value,
                            tasks=request.tasks,
                            duration_seconds=request.duration_seconds,
                        )
                    )
                except Exception as e:
                    logger.warning(f"Failed to execute workflow in engine: {e}")

            logger.info(f"Started workflow: {workflow_id} (pattern={request.pattern.value})")

            return WorkflowStatusResponse(
                workflow_id=workflow_id,
                pattern=request.pattern,
                status="running",
                created_at=workflow["created_at"],
                completed_at=None,
                total_subtasks=len(request.tasks),
                completed_subtasks=0,
                failed_subtasks=0,
            )

        @self.app.get("/api/workflows/{workflow_id}", response_model=WorkflowStatusResponse)
        async def get_workflow_status(workflow_id: str):
            """Get workflow status."""
            if workflow_id not in self.workflows:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

            wf = self.workflows[workflow_id]

            return WorkflowStatusResponse(
                workflow_id=workflow_id,
                pattern=WorkflowPattern(wf["pattern"]),
                status=wf["status"],
                created_at=wf["created_at"],
                completed_at=wf["completed_at"],
                total_subtasks=len(wf["tasks"]),
                completed_subtasks=len(wf["results"]),
                failed_subtasks=len(wf["errors"]),
                results=wf["results"],
                errors=wf["errors"],
            )

        # System endpoints
        @self.app.get("/api/system/status", response_model=SystemStatusResponse)
        async def system_status():
            """Get overall system status."""
            uptime = (datetime.utcnow() - self.start_time).total_seconds()

            agents = list(self.agents.values())
            healthy_agents = sum(1 for a in agents if a["status"] == "healthy")
            degraded_agents = sum(1 for a in agents if a["status"] == "degraded")
            unhealthy_agents = sum(1 for a in agents if a["status"] == "unhealthy")

            total_budget = sum(a["budget_remaining"] for a in agents)
            avg_confidence = 0.0
            if agents:
                total_confidence = 0.0
                count = 0
                for a in agents:
                    for conf in a["capabilities"].values():
                        total_confidence += conf
                        count += 1
                avg_confidence = total_confidence / count if count > 0 else 0.0

            # Determine overall status
            if degraded_agents > 0 or unhealthy_agents > 0:
                status = "degraded"
            elif healthy_agents == 0:
                status = "warning"
            else:
                status = "operational"

            pool_status = PoolStatusResponse(
                pool_name="default",
                total_agents=len(agents),
                healthy_agents=healthy_agents,
                degraded_agents=degraded_agents,
                unhealthy_agents=unhealthy_agents,
                capacity_utilization=0.0,
                total_budget_remaining=total_budget,
                pending_tasks=sum(1 for t in self.tasks.values() if t["status"] == "pending"),
                executing_tasks=sum(1 for t in self.tasks.values() if t["status"] == "executing"),
                avg_capability_confidence=avg_confidence,
            )

            gpu_status = GPUStatusResponse(
                total_gpus={"h100": 4, "a100": 8, "l4": 16, "t4": 32},
                available_gpus={"h100": 4, "a100": 8, "l4": 16, "t4": 32},
                pending_tasks=0,
                running_tasks=0,
                average_wait_seconds=0.0,
                cluster_utilization=0.0,
            )

            return SystemStatusResponse(
                status=status,
                timestamp=datetime.utcnow(),
                agents=pool_status,
                gpu=gpu_status,
                temporal_healthy=True,
                redis_healthy=True,
                graphsub_healthy=True,
                uptime_seconds=uptime,
                requests_processed=self.request_count,
            )

        # Error handler
        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request, exc):
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "error": exc.detail,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

    def get_app(self):
        """Get FastAPI application."""
        return self.app


# Create API instance
api = AgentOrchestrationAPI()
app = api.get_app()

# ── Edge Agent Routes (dsco-cli ↔ autobot integration) ─────────────────────
from .edge_routes import router as edge_router
app.include_router(edge_router)
