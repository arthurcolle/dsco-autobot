"""
Swarm API Routes with DuckDB Persistence and MCP Integration.

All data is stored in DuckDB on Modal volumes for durability.
Supports MCP task execution, workflows, and skill learning.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field

from ..swarm.persistence import get_swarm_db
from ..swarm.swarm_mcp_bridge import get_swarm_mcp_bridge
from ..swarm.mcp_task_types import MCPTaskType, is_mcp_task_type, MCP_BACKEND_CAPABILITIES
from ..swarm.agent_capabilities import AgentMCPProfile, create_specialized_profile, create_generalist_profile

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


# MCP Integration Models

class ExecuteMCPTaskRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    task_type: str = Field(..., description="MCP task type (e.g., search_web, research_deep)")
    input_data: Dict[str, Any] = Field(default_factory=dict, description="Input data for MCP tool")
    bounty: float = Field(default=10, description="Task bounty")
    agent_id: Optional[str] = Field(default=None, description="Specific agent to execute")


class ExecuteWorkflowRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    workflow_type: str = Field(default="sequential", description="sequential or parallel")
    steps: List[Dict[str, Any]] = Field(..., description="Workflow steps")
    agent_id: Optional[str] = Field(default=None, description="Agent to execute workflow")


class WorkflowStep(BaseModel):
    task_type: str = Field(..., description="MCP task type")
    input_data: Dict[str, Any] = Field(default_factory=dict, description="Input (can use $prev)")


class SpecializeAgentRequest(BaseModel):
    backend_id: str = Field(..., description="MCP backend to specialize in")
    tools: List[str] = Field(..., description="Tools to enable")
    initial_proficiency: float = Field(default=0.5, ge=0.0, le=1.0)


class SubscribeEventsRequest(BaseModel):
    webhook_url: Optional[str] = Field(default=None, description="Webhook URL for events")
    events: List[str] = Field(..., description="Event types to subscribe to")
    filter_criteria: Optional[Dict[str, Any]] = Field(default=None)


class ExecuteLLMTaskRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    task_description: str = Field(..., description="Natural language task description")
    context: Optional[Dict[str, Any]] = Field(default=None, description="Additional context")
    model: Optional[str] = Field(default=None, description="Model override (default: claude-sonnet-4)")
    agent_id: Optional[str] = Field(default=None, description="Agent to execute")


class SmartRouteRequest(BaseModel):
    swarm_id: str = Field(..., description="Swarm ID")
    query: str = Field(..., description="Natural language query to route")
    agent_id: Optional[str] = Field(default=None, description="Agent to execute")


# =============================================================================
# ROOT ENDPOINT
# =============================================================================

@router.get("")
@router.get("/")
async def swarm_root():
    """Swarm API root - list endpoints."""
    return {
        "service": "Swarm Economy API",
        "version": "3.0.0",
        "persistence": "DuckDB",
        "features": ["MCP Integration", "Skill Learning", "Event Coordination"],
        "endpoints": {
            # Core endpoints
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
            # MCP Integration endpoints
            "execute_mcp_task": "POST /api/v1/swarm/tasks/execute-mcp",
            "execute_workflow": "POST /api/v1/swarm/workflows/execute",
            "list_workflows": "GET /api/v1/swarm/{swarm_id}/workflows",
            "get_mcp_profile": "GET /api/v1/swarm/{swarm_id}/agents/{agent_id}/mcp-profile",
            "specialize_agent": "POST /api/v1/swarm/{swarm_id}/agents/{agent_id}/specialize",
            "list_skills": "GET /api/v1/swarm/{swarm_id}/skills",
            "subscribe_events": "POST /api/v1/swarm/{swarm_id}/events/subscribe",
            "mcp_health": "GET /api/v1/swarm/{swarm_id}/mcp-health",
            # LLM-powered endpoints
            "execute_llm_task": "POST /api/v1/swarm/tasks/execute-llm",
            "smart_route": "POST /api/v1/swarm/tasks/smart-route",
            "chat": "POST /api/v1/swarm/chat",
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


# =============================================================================
# MCP INTEGRATION
# =============================================================================

@router.post("/tasks/execute-mcp")
async def execute_mcp_task(request: ExecuteMCPTaskRequest):
    """
    Execute a task via MCP backend.

    Routes the task to the appropriate MCP backend based on task_type.
    Supports: search_web, search_read, research_deep, memory_store, etc.
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    # Verify swarm exists
    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Validate task type
    if not is_mcp_task_type(request.task_type):
        valid_types = [t.value for t in MCPTaskType]
        raise HTTPException(400, f"Invalid MCP task type. Valid types: {valid_types}")

    # Create task record
    task_id = str(uuid.uuid4())
    backend_id, tool = MCP_BACKEND_CAPABILITIES.get(request.task_type, (None, None))

    # Get backend/tool from routing
    from ..swarm.mcp_task_types import MCP_TASK_ROUTING
    mcp_type = MCPTaskType(request.task_type)
    backend_id, tool = MCP_TASK_ROUTING[mcp_type]

    db.create_task(
        task_id=task_id,
        swarm_id=request.swarm_id,
        task_type=request.task_type,
        description=f"MCP task: {request.task_type}",
        bounty=request.bounty,
        input_data=request.input_data,
    )

    # Update task with MCP metadata
    db.update_task(task_id, mcp_backend=backend_id, mcp_tool=tool)

    # Execute via bridge
    result = await bridge.execute_mcp_task(
        task_type=request.task_type,
        input_data=request.input_data,
        agent_id=request.agent_id,
        swarm_id=request.swarm_id,
    )

    # Update task with result
    if result.get("success"):
        db.complete_task(task_id, result, success=True)

        # Check for skill learning opportunity
        validation_score = result.get("validation_score", 0.7)
        if validation_score >= 0.8 and request.agent_id:
            skill_id = await bridge.maybe_learn_skill(
                task_type=request.task_type,
                input_data=request.input_data,
                result=result.get("result", {}),
                validation_score=validation_score,
                agent_id=request.agent_id,
                swarm_id=request.swarm_id,
            )
            if skill_id:
                db.update_task(task_id, skill_pattern_id=skill_id)
                result["skill_learned"] = skill_id
    else:
        db.complete_task(task_id, {"error": result.get("error")}, success=False)

    return {
        "task_id": task_id,
        **result,
    }


@router.post("/tasks/execute-mcp-async")
async def execute_mcp_task_async(request: ExecuteMCPTaskRequest):
    """
    Execute a long-running MCP task asynchronously via the Jobs API.

    Returns immediately with a job_id. Use GET /api/v1/jobs/{job_id} to poll.
    Falls back to synchronous execution if the Jobs API is unavailable.
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    if not is_mcp_task_type(request.task_type):
        valid_types = [t.value for t in MCPTaskType]
        raise HTTPException(400, f"Invalid MCP task type. Valid types: {valid_types}")

    task_id = str(uuid.uuid4())
    from ..swarm.mcp_task_types import MCP_TASK_ROUTING
    mcp_type = MCPTaskType(request.task_type)
    backend_id, tool = MCP_TASK_ROUTING[mcp_type]

    db.create_task(
        task_id=task_id,
        swarm_id=request.swarm_id,
        task_type=request.task_type,
        description=f"Async MCP task: {request.task_type}",
        bounty=request.bounty,
        input_data=request.input_data,
    )
    db.update_task(task_id, mcp_backend=backend_id, mcp_tool=tool)

    result = await bridge.execute_mcp_task_async(
        task_type=request.task_type,
        input_data=request.input_data,
        agent_id=request.agent_id,
        swarm_id=request.swarm_id,
    )

    if result.get("success"):
        db.complete_task(task_id, result, success=True)
    else:
        db.complete_task(task_id, {"error": result.get("error")}, success=False)

    return {
        "task_id": task_id,
        **result,
    }


@router.post("/workflows/execute")
async def execute_workflow(request: ExecuteWorkflowRequest):
    """
    Execute a multi-step MCP workflow.

    Workflows can be:
    - sequential: Each step uses $prev to reference previous output
    - parallel: All steps execute simultaneously

    Example:
    {
        "swarm_id": "...",
        "workflow_type": "sequential",
        "steps": [
            {"task_type": "research_deep", "input_data": {"query": "AI agents"}},
            {"task_type": "memory_store", "input_data": {"data": "$prev"}},
            {"task_type": "publish_event", "input_data": {"event": "research_complete"}}
        ]
    }
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    # Verify swarm
    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Validate steps
    for i, step in enumerate(request.steps):
        if "task_type" not in step:
            raise HTTPException(400, f"Step {i} missing task_type")
        if not is_mcp_task_type(step["task_type"]):
            raise HTTPException(400, f"Step {i} has invalid task_type: {step['task_type']}")

    # Create workflow record
    workflow_id = str(uuid.uuid4())
    db.create_workflow(
        workflow_id=workflow_id,
        swarm_id=request.swarm_id,
        steps=request.steps,
        workflow_type=request.workflow_type,
        agent_id=request.agent_id,
    )

    # Execute workflow
    result = await bridge.execute_workflow(
        steps=request.steps,
        swarm_id=request.swarm_id,
        workflow_type=request.workflow_type,
        agent_id=request.agent_id,
    )

    # Update workflow record
    status = "completed" if result.get("success") else "failed"
    db.update_workflow(
        workflow_id=workflow_id,
        status=status,
        results=result.get("results", []),
        steps_completed=result.get("steps_completed", 0),
        completed_at=datetime.now(timezone.utc),
    )

    return {
        "workflow_id": workflow_id,
        **result,
    }


@router.get("/{swarm_id}/workflows")
async def list_workflows(swarm_id: str, status: Optional[str] = None):
    """List workflows in a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    workflows = db.list_workflows(swarm_id, status=status)
    return {"workflows": workflows}


@router.get("/{swarm_id}/workflows/{workflow_id}")
async def get_workflow(swarm_id: str, workflow_id: str):
    """Get workflow details."""
    db = get_swarm_db()
    workflow = db.get_workflow(workflow_id)

    if not workflow or workflow.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Workflow not found")

    return workflow


# =============================================================================
# AGENT MCP PROFILES
# =============================================================================

@router.get("/{swarm_id}/agents/{agent_id}/mcp-profile")
async def get_agent_mcp_profile(swarm_id: str, agent_id: str):
    """Get an agent's MCP capability profile."""
    db = get_swarm_db()

    agent = db.get_agent(agent_id)
    if not agent or agent.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Agent not found")

    profile = db.get_agent_mcp_profile(agent_id)

    return {
        "agent_id": agent_id,
        "agent_name": agent.get("name"),
        "mcp_profile": profile or {},
    }


@router.post("/{swarm_id}/agents/{agent_id}/specialize")
async def specialize_agent(swarm_id: str, agent_id: str, request: SpecializeAgentRequest):
    """
    Specialize an agent for specific MCP backend capabilities.

    This enables the agent to execute tasks on the specified backend
    and tracks their proficiency over time.
    """
    db = get_swarm_db()

    agent = db.get_agent(agent_id)
    if not agent or agent.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Agent not found")

    # Validate backend
    if request.backend_id not in MCP_BACKEND_CAPABILITIES:
        valid_backends = list(MCP_BACKEND_CAPABILITIES.keys())
        raise HTTPException(400, f"Invalid backend. Valid backends: {valid_backends}")

    # Create or update MCP profile
    existing_profile = db.get_agent_mcp_profile(agent_id) or {"agent_id": agent_id, "specializations": []}

    # Check if already specialized
    specializations = existing_profile.get("specializations", [])
    found = False
    for spec in specializations:
        if spec.get("backend_id") == request.backend_id:
            spec["tools"] = list(set(spec.get("tools", []) + request.tools))
            spec["proficiency"] = request.initial_proficiency
            found = True
            break

    if not found:
        specializations.append({
            "backend_id": request.backend_id,
            "tools": request.tools,
            "proficiency": request.initial_proficiency,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_rewards": 0.0,
        })

    existing_profile["specializations"] = specializations
    existing_profile["primary_backend"] = request.backend_id

    db.update_agent_mcp_profile(agent_id, existing_profile)

    logger.info(f"Specialized agent {agent_id} for {request.backend_id}")

    return {
        "agent_id": agent_id,
        "backend_id": request.backend_id,
        "tools": request.tools,
        "proficiency": request.initial_proficiency,
        "mcp_profile": existing_profile,
    }


# =============================================================================
# SKILLS
# =============================================================================

@router.get("/{swarm_id}/skills")
async def list_skills(swarm_id: str, skill_type: Optional[str] = None):
    """
    List learned skills in a swarm.

    Skills are patterns captured from successful high-scoring task executions
    and stored in the Memory MCP.
    """
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    skills = db.list_skills(swarm_id, skill_type=skill_type)

    return {
        "swarm_id": swarm_id,
        "skills": skills,
        "count": len(skills),
    }


@router.get("/{swarm_id}/skills/{skill_id}")
async def get_skill(swarm_id: str, skill_id: str):
    """Get skill details."""
    db = get_swarm_db()
    skill = db.get_skill(skill_id)

    if not skill or skill.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Skill not found")

    return skill


# =============================================================================
# EVENT SUBSCRIPTIONS
# =============================================================================

@router.post("/{swarm_id}/events/subscribe")
async def subscribe_events(swarm_id: str, request: SubscribeEventsRequest):
    """
    Subscribe to swarm events via webhook.

    Events include:
    - swarm.task.completed
    - swarm.task.failed
    - swarm.agent.idle
    - swarm.agent.skill_learned
    - swarm.coordination.work_available
    - swarm.workflow.completed
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Create subscription in database
    subscription_id = str(uuid.uuid4())
    db.create_subscription(
        subscription_id=subscription_id,
        swarm_id=swarm_id,
        event_types=request.events,
        webhook_url=request.webhook_url,
        filter_criteria=request.filter_criteria,
    )

    # Register with bridge for local delivery
    if request.webhook_url:
        bridge.subscribe(
            swarm_id=swarm_id,
            event_types=request.events,
            webhook_url=request.webhook_url,
            subscriber_id=subscription_id,
        )

    logger.info(f"Created event subscription {subscription_id} for {request.events}")

    return {
        "subscription_id": subscription_id,
        "swarm_id": swarm_id,
        "events": request.events,
        "webhook_url": request.webhook_url,
    }


@router.get("/{swarm_id}/events/subscriptions")
async def list_subscriptions(swarm_id: str, active_only: bool = True):
    """List event subscriptions for a swarm."""
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    subscriptions = db.list_subscriptions(swarm_id, active_only=active_only)
    return {"subscriptions": subscriptions}


@router.delete("/{swarm_id}/events/subscriptions/{subscription_id}")
async def unsubscribe_events(swarm_id: str, subscription_id: str):
    """Delete an event subscription."""
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    sub = db.get_subscription(subscription_id)
    if not sub or sub.get("swarm_id") != swarm_id:
        raise HTTPException(404, "Subscription not found")

    db.delete_subscription(subscription_id)
    bridge.unsubscribe(subscription_id)

    return {"deleted": True, "subscription_id": subscription_id}


# =============================================================================
# MCP HEALTH & BACKENDS
# =============================================================================

@router.get("/{swarm_id}/mcp-health")
async def get_mcp_health(swarm_id: str):
    """
    Get health status of all MCP backends.

    Returns connectivity and available tools for each backend.
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    health = await bridge.get_backend_health()

    # Add backend info
    backends_info = []
    for backend_id, status in health.items():
        info = MCP_BACKEND_CAPABILITIES.get(backend_id, {})
        backends_info.append({
            "backend_id": backend_id,
            "name": info.get("name", backend_id),
            "description": info.get("description", ""),
            **status,
        })

    all_healthy = all(b.get("healthy") for b in backends_info)

    return {
        "swarm_id": swarm_id,
        "status": "healthy" if all_healthy else "degraded",
        "backends": backends_info,
        "total_backends": len(backends_info),
        "healthy_backends": sum(1 for b in backends_info if b.get("healthy")),
    }


@router.get("/{swarm_id}/mcp-backends")
async def list_mcp_backends(swarm_id: str):
    """
    List available MCP backends and their capabilities.
    """
    db = get_swarm_db()
    swarm = db.get_swarm(swarm_id)

    if not swarm:
        raise HTTPException(404, "Swarm not found")

    backends = []
    for backend_id, info in MCP_BACKEND_CAPABILITIES.items():
        backends.append({
            "backend_id": backend_id,
            "name": info.get("name"),
            "description": info.get("description"),
            "tools": info.get("tools", []),
            "task_types": [t.value for t in info.get("task_types", [])],
        })

    return {
        "swarm_id": swarm_id,
        "backends": backends,
    }


# =============================================================================
# LLM-POWERED ENDPOINTS (OpenRouter)
# =============================================================================

@router.post("/tasks/execute-llm")
async def execute_llm_task(request: ExecuteLLMTaskRequest):
    """
    Execute a task using LLM (OpenRouter).

    Uses natural language to describe the task. The LLM processes it
    and returns a structured result.

    Models available:
    - anthropic/claude-sonnet-4 (default)
    - anthropic/claude-opus-4
    - openai/gpt-4o
    - google/gemini-2.0-flash
    - and more via OpenRouter
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Execute LLM task
    result = await bridge.execute_llm_task(
        task_description=request.task_description,
        context=request.context,
        model=request.model,
        agent_id=request.agent_id,
        swarm_id=request.swarm_id,
    )

    # Create task record
    task_id = str(uuid.uuid4())
    db.create_task(
        task_id=task_id,
        swarm_id=request.swarm_id,
        task_type="llm_task",
        description=request.task_description[:500],
        bounty=0,
        input_data={"description": request.task_description, "context": request.context, "model": request.model},
    )

    if result.get("success"):
        db.complete_task(task_id, {"llm_result": result.get("result")}, success=True)
    else:
        db.complete_task(task_id, {"error": result.get("error")}, success=False)

    return {
        "task_id": task_id,
        **result,
    }


@router.post("/tasks/smart-route")
async def smart_route_task(request: SmartRouteRequest):
    """
    Intelligently route a natural language query to appropriate tools.

    The LLM analyzes the query and determines:
    1. Which MCP tools/backends to use
    2. What order to execute them
    3. What parameters to pass

    Example:
    {
        "swarm_id": "...",
        "query": "Search for recent AI papers and store the findings in memory"
    }

    This will automatically route to:
    1. distributed-search-mcp:search
    2. distributed-memory-mcp:store
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(request.swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    result = await bridge.smart_route_and_execute(
        query=request.query,
        swarm_id=request.swarm_id,
        agent_id=request.agent_id,
    )

    return result


@router.post("/chat")
async def swarm_chat(swarm_id: str = Body(...), message: str = Body(...), model: Optional[str] = Body(default=None)):
    """
    Simple chat endpoint for swarm interaction.

    Send a message and get an LLM response. Good for testing
    or simple conversational interactions with agents.
    """
    db = get_swarm_db()
    bridge = get_swarm_mcp_bridge()

    swarm = db.get_swarm(swarm_id)
    if not swarm:
        raise HTTPException(404, "Swarm not found")

    # Direct chat via OpenRouter
    messages = [{"role": "user", "content": message}]
    result = await bridge.openrouter.chat(messages, model=model)

    response = result.get("choices", [{}])[0].get("message", {}).get("content", "")
    usage = result.get("usage", {})

    return {
        "swarm_id": swarm_id,
        "response": response,
        "model": model or bridge.openrouter.default_model,
        "usage": usage,
    }
