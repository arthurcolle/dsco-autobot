"""
Edge Agent Routes — Registration, heartbeat, tool proxy, swarm deploy, fleet sync.

These endpoints connect dsco-cli edge agents to the cloud orchestration platform.
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from dataclasses import dataclass, field
from fastapi import APIRouter, Request, HTTPException, Header
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ── Models ──────────────────────────────────────────────────────────────────

class EdgeRegisterRequest(BaseModel):
    """Edge agent registration."""
    agent_id: str
    host: str
    bridge_peer: Optional[str] = None
    capabilities: List[str] = []
    tools: List[str] = []
    graphsub_endpoint: Optional[str] = None
    version: str = "2.0"


class EdgeHeartbeatRequest(BaseModel):
    """Edge agent heartbeat."""
    agent_id: str
    status: str = "idle"  # idle|executing|swarming|error
    uptime_seconds: int = 0
    tasks_completed: int = 0
    load: float = 0.0


class EdgeToolProxyRequest(BaseModel):
    """Proxy a cloud-only tool from edge agent."""
    agent_id: str
    tool: str
    params: Dict[str, Any] = {}
    timeout_seconds: int = 60


class SwarmDeployRequest(BaseModel):
    """Deploy a swarm topology across edge agents."""
    topology: str = "fanout"  # fanout|fanin|debate|pipeline|tournament|stigmergic
    agents: List[Dict[str, Any]] = []  # [{agent_id, role}]
    task: str = ""
    swarm_id: Optional[str] = None
    timeout_seconds: int = 300


class BridgeFleetRequest(BaseModel):
    """Sync bridge fleet peers."""
    fleet_name: str = "bridge"
    peers: List[Dict[str, Any]] = []  # [{name, host, addr, agent_id}]


# ── State ───────────────────────────────────────────────────────────────────

@dataclass
class EdgeAgent:
    """Registered edge agent."""
    agent_id: str
    host: str
    bridge_peer: Optional[str]
    capabilities: List[str]
    tools: List[str]
    graphsub_endpoint: Optional[str]
    version: str
    registered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "idle"
    uptime_seconds: int = 0
    tasks_completed: int = 0
    load: float = 0.0
    session_token: str = ""


class EdgeRegistry:
    """Registry of connected edge agents."""

    def __init__(self):
        self.agents: Dict[str, EdgeAgent] = {}
        self._token_counter = 0

    def register(self, req: EdgeRegisterRequest) -> tuple[str, str]:
        """Register an edge agent. Returns (agent_id, session_token)."""
        import secrets
        token = secrets.token_urlsafe(32)

        agent = EdgeAgent(
            agent_id=req.agent_id,
            host=req.host,
            bridge_peer=req.bridge_peer,
            capabilities=req.capabilities,
            tools=req.tools,
            graphsub_endpoint=req.graphsub_endpoint,
            version=req.version,
            session_token=token,
        )
        self.agents[req.agent_id] = agent
        logger.info(f"Edge agent registered: {req.agent_id} @ {req.host}")
        return req.agent_id, token

    def heartbeat(self, req: EdgeHeartbeatRequest) -> bool:
        """Update edge agent heartbeat."""
        agent = self.agents.get(req.agent_id)
        if not agent:
            return False
        agent.last_heartbeat = datetime.now(timezone.utc)
        agent.status = req.status
        agent.uptime_seconds = req.uptime_seconds
        agent.tasks_completed = req.tasks_completed
        agent.load = req.load
        return True

    def deregister(self, agent_id: str) -> bool:
        """Remove an registered edge agent."""
        if agent_id in self.agents:
            del self.agents[agent_id]
            return True
        return False

    def get(self, agent_id: str) -> Optional[EdgeAgent]:
        return self.agents.get(agent_id)

    def list_agents(self) -> List[EdgeAgent]:
        return list(self.agents.values())

    def verify_token(self, agent_id: str, token: str) -> bool:
        agent = self.agents.get(agent_id)
        if not agent:
            return False
        return agent.session_token == token

    def cleanup_stale(self, max_age_seconds: int = 120):
        """Remove agents that haven't sent heartbeat."""
        now = datetime.now(timezone.utc)
        stale = []
        for aid, agent in self.agents.items():
            age = (now - agent.last_heartbeat).total_seconds()
            if age > max_age_seconds:
                stale.append(aid)
        for aid in stale:
            del self.agents[aid]
            logger.info(f"Edge agent expired (no heartbeat): {aid}")


# Global registry
edge_registry = EdgeRegistry()


# ── Router ──────────────────────────────────────────────────────────────────

router = APIRouter(tags=["edge"])


@router.post("/edge/register")
async def register_edge_agent(req: EdgeRegisterRequest):
    """Register an edge dsco-cli agent."""
    agent_id, token = edge_registry.register(req)
    return {
        "ok": True,
        "agent_id": agent_id,
        "session_token": token,
        "expires_at": (datetime.now(timezone.utc).replace(microsecond=0)).isoformat(),
    }


@router.post("/edge/heartbeat")
async def edge_heartbeat(req: EdgeHeartbeatRequest, authorization: str = Header(None)):
    """Update edge agent heartbeat."""
    if not edge_registry.heartbeat(req):
        raise HTTPException(status_code=404, detail="agent not registered")
    # Generate any pending actions
    actions = []
    agent = edge_registry.get(req.agent_id)
    if agent and req.status == "idle":
        actions.append("ready_for_task")
    return {"ok": True, "actions": actions}


@router.post("/edge/tool_proxy")
async def edge_tool_proxy(req: EdgeToolProxyRequest, authorization: str = Header(None)):
    """Proxy a cloud-only tool execution for an edge agent."""
    agent = edge_registry.get(req.agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="agent not registered")

    # Cloud-only tools that can be proxied
    CLOUD_TOOLS = {
        "temporal_workflow": "temporal",
        "redis_pubsub": "redis",
        "neo4j_query": "neo4j",
        "graphsub_admin": "graphsub",
        "embed_document": "ingestion",
        "schema_generate": "dynamic_schema",
    }

    handler = CLOUD_TOOLS.get(req.tool)
    if not handler:
        raise HTTPException(status_code=400, detail=f"unknown cloud tool: {req.tool}")

    # TODO: route to appropriate service
    # For now, return ack
    return {
        "ok": True,
        "tool": req.tool,
        "handler": handler,
        "result": {"status": "queued"},
    }


@router.post("/swarm/deploy")
async def deploy_swarm(req: SwarmDeployRequest, authorization: str = Header(None)):
    """Deploy a swarm topology across registered edge agents."""
    agents = []
    for a in req.agents:
        agent_id = a.get("agent_id")
        edge_agent = edge_registry.get(agent_id)
        if edge_agent:
            agents.append({
                "agent_id": agent_id,
                "host": edge_agent.host,
                "role": a.get("role", "worker"),
                "capabilities": edge_agent.capabilities,
                "status": edge_agent.status,
            })
        else:
            logger.warning(f"swarm deploy: agent {agent_id} not registered")

    if not agents:
        raise HTTPException(status_code=400, detail="no registered agents available for swarm")

    swarm_id = req.swarm_id or f"swarm-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Register swarm on GraphSub if available
    # TODO: wire to graphsub_client

    return {
        "ok": True,
        "swarm_id": swarm_id,
        "deployed_to": [a["agent_id"] for a in agents],
        "agents": agents,
    }


@router.post("/bridge/fleet")
async def sync_bridge_fleet(req: BridgeFleetRequest):
    """Sync bridge fleet peers to the cloud registry."""
    registered = 0
    for peer in req.peers:
        # Register each peer as a known host
        # TODO: store in GraphSub as fleet topology nodes
        registered += 1

    return {"ok": True, "registered": registered, "fleet_name": req.fleet_name}


@router.get("/edge/agents")
async def list_edge_agents():
    """List all registered edge agents."""
    agents = edge_registry.list_agents()
    return {
        "ok": True,
        "count": len(agents),
        "agents": [
            {
                "agent_id": a.agent_id,
                "host": a.host,
                "status": a.status,
                "capabilities": a.capabilities,
                "tools": a.tools,
                "bridge_peer": a.bridge_peer,
                "uptime_seconds": a.uptime_seconds,
                "tasks_completed": a.tasks_completed,
                "load": a.load,
                "last_heartbeat": a.last_heartbeat.isoformat(),
            }
            for a in agents
        ],
    }


@router.delete("/edge/{agent_id}")
async def deregister_edge_agent(agent_id: str):
    """Deregister an edge agent."""
    if edge_registry.deregister(agent_id):
        return {"ok": True}
    raise HTTPException(status_code=404, detail="agent not found")
