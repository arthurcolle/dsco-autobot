"""
Swarm API Routes with DuckDB Persistence.

All data is stored in DuckDB on Modal volumes for durability.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field

from ..swarm.persistence import get_swarm_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/swarm", tags=["swarm"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class CreateSwarmRequest(BaseModel):
    name: str = Field(..., description="Swarm name")
    host_id: str = Field(..., description="Host/designer ID")
    host_wallet: str = Field(..., description="Host wallet address")
    host_fee_rate: float = Field(default=0.05, description="Host fee rate (0-1)")
    min_stake: float = Field(default=10, description="Minimum stake to join")


class SpawnAgentRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    name: str = Field(..., description="Agent name")
    capabilities: List[str] = Field(..., description="Agent capabilities")
    role: str = Field(default="worker", description="Agent role")
    initial_stake: float = Field(default=0, description="Initial stake amount")


class SubmitTaskRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    task_type: str = Field(..., description="Task type")
    description: str = Field(..., description="Task description")
    input_data: Dict[str, Any] = Field(default_factory=dict)
    bounty: float = Field(default=10, description="Task bounty")
    required_capabilities: List[str] = Field(default_factory=list)
    priority: str = Field(default="normal")
    deadline_hours: Optional[float] = None


class AssignTaskRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    task_id: str = Field(..., description="Task ID")
    agent_id: str = Field(..., description="Agent ID")


class CompleteTaskRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    task_id: str = Field(..., description="Task ID")
    output: Dict[str, Any] = Field(..., description="Task output")
    success: bool = Field(default=True)


# =============================================================================
# ROOT ENDPOINT
# =============================================================================

@router.get("")
@router.get("/")
async def swarm_root():
    """Swarm API root - list endpoints."""
    return {
        "service": "Swarm Economy API",
        "version": "2.5.0",
        "persistence": "DuckDB",
        "endpoints": {
            "list_swarms": "GET /api/v1/swarm/list",
            "create_swarm": "POST /api/v1/swarm/create",
            "get_swarm": "GET /api/v1/swarm/{swarm_id}",
            "get_stats": "GET /api/v1/swarm/{swarm_id}/stats",
            "spawn_agent": "POST /api/v1/swarm/agents/spawn",
            "list_agents": "GET /api/v1/swarm/{swarm_id}/agents",
            "submit_task": "POST /api/v1/swarm/tasks/submit",
            "assign_task": "POST /api/v1/swarm/tasks/assign",
            "complete_task": "POST /api/v1/swarm/tasks/complete",
            "list_tasks": "GET /api/v1/swarm/{swarm_id}/tasks",
            "get_earnings": "GET /api/v1/swarm/{swarm_id}/earnings",
            "collect_earnings": "POST /api/v1/swarm/{swarm_id}/collect",
        },
    }


# =============================================================================
# SWARM MANAGEMENT
# =============================================================================

@router.post("/create")
async def create_swarm(request: CreateSwarmRequest):
    """Create a new swarm."""
    db = get_swarm_db()

    swarm_id = str(uuid.uuid4())
    swarm = db.create_swarm(
        swarm_id=swarm_id,
        name=request.name,
        host_id=request.host_id,
        host_wallet=request.host_wallet,
        host_fee_rate=request.host_fee_rate,
        min_stake=request.min_stake,
        config={"version": "2.5.0"},
    )

    logger.info(f"Created swarm: {request.name} ({swarm_id})")

    return {
        "swarm_id": swarm_id,
        "name": request.name,
        "host_id": request.host_id,
        "host_fee_rate": request.host_fee_rate,
        "min_stake": request.min_stake,
    }


@router.get("/list")
async def list_swarms():
    """List all swarms."""
    db = get_swarm_db()
    swarms = db.list_swarms()

    return {
        "swarms": [
            {
                "id": s["id"],
                "name": s["name"],
                "host_id": s["host_id"],
                "total_tasks_completed": s.get("total_tasks_completed", 0),
                "total_payments_processed": s.get("total_payments_processed", 0),
                "created_at": s["created_at"].isoformat() if s.get("created_at") else None,
            }
            for s in swarms
        ]
    }


@router.get("/{swarm_id}")
async def get_swarm(swarm_id: str):
    """Get swarm details."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    agents = db.list_agents(swarm_id)
    tasks = db.list_tasks(swarm_id)
    pending = [t for t in tasks if t.get("status") == "pending"]

    return {
        **swarm,
        "agent_count": len(agents),
        "pending_tasks": len(pending),
        "total_tasks": len(tasks),
    }


@router.get("/{swarm_id}/stats")
async def get_swarm_stats(swarm_id: str):
    """Get swarm statistics."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    return db.get_swarm_stats(swarm_id)


@router.delete("/{swarm_id}")
async def delete_swarm(swarm_id: str):
    """Delete a swarm and all related data."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    db.delete_swarm(swarm_id)
    logger.info(f"Deleted swarm: {swarm_id}")

    return {"deleted": True, "swarm_id": swarm_id}


# =============================================================================
# AGENT MANAGEMENT
# =============================================================================

@router.post("/agents/spawn")
async def spawn_agent(request: SpawnAgentRequest):
    """Spawn a new agent in a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(request.swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Check minimum stake
    if request.initial_stake < swarm.get("min_stake", 0):
        raise HTTPException(
            400,
            f"Stake too low. Minimum: {swarm.get('min_stake', 0)}"
        )

    agent_id = str(uuid.uuid4())
    wallet_id = str(uuid.uuid4())  # Simplified wallet for now

    agent = db.create_agent(
        agent_id=agent_id,
        swarm_id=request.swarm_id,
        name=request.name,
        role=request.role,
        capabilities=request.capabilities,
        wallet_id=wallet_id,
        stake_amount=request.initial_stake,
        metadata={"version": "2.5.0"},
    )

    # Create stake record if staking
    if request.initial_stake > 0:
        stake_id = str(uuid.uuid4())
        db.create_stake(
            stake_id=stake_id,
            swarm_id=request.swarm_id,
            agent_id=agent_id,
            amount=request.initial_stake,
        )

    logger.info(f"Spawned agent: {request.name} ({agent_id}) in swarm {request.swarm_id}")

    return agent


@router.get("/{swarm_id}/agents")
async def list_agents(swarm_id: str):
    """List agents in a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    agents = db.list_agents(swarm_id)
    return {"agents": agents}


@router.get("/{swarm_id}/agents/{agent_id}")
async def get_agent(swarm_id: str, agent_id: str):
    """Get agent details."""
    db = get_swarm_db()
    agent = db.get_agent(agent_id)

    if not agent or agent.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Agent not found")

    return agent


# =============================================================================
# TASK MANAGEMENT
# =============================================================================

@router.post("/tasks/submit")
async def submit_task(request: SubmitTaskRequest):
    """Submit a task to the swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(request.swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    deadline = None
    if request.deadline_hours:
        deadline = datetime.now(timezone.utc) + timedelta(hours=request.deadline_hours)

    task_id = str(uuid.uuid4())
    task = db.create_task(
        task_id=task_id,
        swarm_id=request.swarm_id,
        task_type=request.task_type,
        description=request.description,
        bounty=request.bounty,
        priority=request.priority,
        input_data=request.input_data,
        required_capabilities=request.required_capabilities,
        deadline=deadline,
    )

    logger.info(f"Task submitted: {request.task_type} ({task_id}) - bounty: ${request.bounty}")

    return task


@router.get("/{swarm_id}/tasks")
async def list_tasks(swarm_id: str, status: Optional[str] = None):
    """List tasks in a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    tasks = db.list_tasks(swarm_id, status=status)
    return {"tasks": tasks}


@router.get("/{swarm_id}/tasks/{task_id}")
async def get_task(swarm_id: str, task_id: str):
    """Get task details."""
    db = get_swarm_db()
    task = db.get_task(task_id)

    if not task or task.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Task not found")

    return task


@router.post("/tasks/assign")
async def assign_task(request: AssignTaskRequest):
    """Assign a task to an agent."""
    db = get_swarm_db()

    # Verify swarm exists
    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Verify task exists and is pending
    task = db.get_task(request.task_id)
    if not task or task.get("swarm_id") != request.swarm_id:
        raise HTTPException(404, "Task not found")

    if task.get("status") != "pending":
        raise HTTPException(400, f"Task not pending: {task.get('status')}")

    # Verify agent exists and is idle
    agent = db.get_agent(request.agent_id)
    if not agent or agent.get("swarm_id") != request.swarm_id:
        raise HTTPException(404, "Agent not found")

    if agent.get("status") != "idle":
        raise HTTPException(400, f"Agent not idle: {agent.get('status')}")

    # Check capabilities
    required = set(task.get("required_capabilities") or [])
    has = set(agent.get("capabilities") or [])
    missing = required - has
    if missing:
        raise HTTPException(400, f"Agent missing capabilities: {missing}")

    db.assign_task(request.task_id, request.agent_id)
    logger.info(f"Assigned task {request.task_id} to agent {request.agent_id}")

    return db.get_task(request.task_id)


@router.post("/tasks/complete")
async def complete_task(request: CompleteTaskRequest):
    """Complete a task."""
    db = get_swarm_db()

    # Verify swarm
    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Verify task
    task = db.get_task(request.task_id)
    if not task or task.get("swarm_id") != request.swarm_id:
        raise HTTPException(404, "Task not found")

    if task.get("status") not in ["claimed", "in_progress"]:
        raise HTTPException(400, f"Task not in progress: {task.get('status')}")

    if not task.get("assigned_to"):
        raise HTTPException(400, "Task not assigned to any agent")

    # Complete the task
    db.complete_task(request.task_id, request.output, success=request.success)

    # If successful, record transaction with host fee
    if request.success:
        bounty = task.get("bounty", 0)
        fee_rate = swarm.get("host_fee_rate", 0.05)
        fee = bounty * fee_rate

        tx_id = str(uuid.uuid4())
        db.create_transaction(
            tx_id=tx_id,
            swarm_id=request.swarm_id,
            from_agent=swarm["host_id"],  # Treasury
            to_agent=task["assigned_to"],
            amount=bounty,
            fee=fee,
            purpose=f"Task completion: {task.get('type')}",
        )

        logger.info(
            f"Task {request.task_id} completed - "
            f"paid ${bounty}, fee ${fee:.2f}"
        )

    return db.get_task(request.task_id)


# =============================================================================
# ECONOMICS
# =============================================================================

@router.get("/{swarm_id}/earnings")
async def get_earnings(swarm_id: str):
    """Get host earnings from the swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    earnings = db.get_host_earnings(swarm_id)

    return {
        "swarm_id": swarm_id,
        "host_id": swarm["host_id"],
        "host_fee_rate": swarm.get("host_fee_rate", 0.05),
        **earnings,
    }


@router.post("/{swarm_id}/collect")
async def collect_earnings(swarm_id: str):
    """Collect pending host earnings."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    collected = db.collect_earnings(swarm_id)
    logger.info(f"Collected ${collected:.2f} from swarm {swarm_id}")

    return {
        "swarm_id": swarm_id,
        "collected": collected,
        "earnings": db.get_host_earnings(swarm_id),
    }


@router.get("/{swarm_id}/transactions")
async def list_transactions(swarm_id: str, limit: int = 100):
    """List transactions for a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    txs = db.list_transactions(swarm_id, limit=limit)
    return {"transactions": txs}


# =============================================================================
# QUICK START
# =============================================================================

@router.post("/quick")
async def quick_swarm(body: Dict[str, Any] = Body(...)):
    """
    Quick helper to create swarm, spawn agents, and submit tasks in one call.

    Request body:
    {
        "name": "My Swarm",
        "host_id": "alice",
        "host_wallet": "0x...",
        "agents": [
            {"name": "researcher", "capabilities": ["search"], "stake": 50},
            {"name": "coder", "capabilities": ["code"], "stake": 50}
        ],
        "tasks": [
            {"type": "search", "description": "Find AI papers", "bounty": 10},
            {"type": "code", "description": "Summarize findings", "bounty": 20}
        ]
    }
    """
    db = get_swarm_db()

    # Create swarm
    swarm_id = str(uuid.uuid4())
    db.create_swarm(
        swarm_id=swarm_id,
        name=body.get("name", "Quick Swarm"),
        host_id=body["host_id"],
        host_wallet=body["host_wallet"],
        host_fee_rate=body.get("host_fee_rate", 0.05),
        min_stake=body.get("min_stake", 0),
    )

    # Spawn agents
    agent_ids = []
    for agent_spec in body.get("agents", []):
        agent_id = str(uuid.uuid4())
        db.create_agent(
            agent_id=agent_id,
            swarm_id=swarm_id,
            name=agent_spec["name"],
            role=agent_spec.get("role", "worker"),
            capabilities=agent_spec.get("capabilities", []),
            stake_amount=agent_spec.get("stake", 0),
        )

        if agent_spec.get("stake", 0) > 0:
            db.create_stake(
                stake_id=str(uuid.uuid4()),
                swarm_id=swarm_id,
                agent_id=agent_id,
                amount=agent_spec["stake"],
            )

        agent_ids.append(agent_id)

    # Submit tasks
    task_ids = []
    for task_spec in body.get("tasks", []):
        task_id = str(uuid.uuid4())
        db.create_task(
            task_id=task_id,
            swarm_id=swarm_id,
            task_type=task_spec["type"],
            description=task_spec["description"],
            bounty=task_spec.get("bounty", 10),
            priority=task_spec.get("priority", "normal"),
            required_capabilities=task_spec.get("capabilities", []),
        )
        task_ids.append(task_id)

    logger.info(f"Quick swarm created: {swarm_id} with {len(agent_ids)} agents, {len(task_ids)} tasks")

    return {
        "swarm_id": swarm_id,
        "agents": agent_ids,
        "tasks": task_ids,
        "stats": db.get_swarm_stats(swarm_id),
    }
