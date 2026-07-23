"""
GraphSub Client - MCP wrapper for stigmergic graph database

Provides high-level API for agents to interact with GraphSub:
- Agent state management (nodes, edges)
- Capability registration and matching
- Stigmergic coordination (pheromone trails)
- CRDT state synchronization

GraphSub is the distributed state store for agent swarms.
"""

import asyncio
import json
import logging
from typing import Optional, Dict, List, Any, AsyncIterator
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


@dataclass
class AgentNode:
    """Agent node in GraphSub."""
    id: str
    model: str
    status: str  # idle, executing, waiting, failed
    budget_remaining: float
    capabilities: Dict[str, float]  # skill -> confidence level
    created_at: str
    heartbeat_last: str


@dataclass
class TaskNode:
    """Task node in GraphSub."""
    id: str
    type: str
    priority: int
    status: str  # pending, executing, completed, failed
    required_capabilities: List[str]
    estimated_cost_usd: float
    created_at: str


@dataclass
class PheromoneTrail:
    """Stigmergic pheromone trail configuration."""
    signal_type: str  # "progress", "attraction", "warning", etc.
    intensity: float  # 0.0 to 1.0
    decay_rate: float  # exponential decay rate
    ttl_seconds: int  # time to live before expiration


class GraphSubClient:
    """
    High-level client for GraphSub interaction.

    Handles:
    - HTTP requests to GraphSub REST API
    - MCP tool invocation for graph operations
    - State caching and synchronization
    - Pheromone management
    """

    def __init__(self, graphsub_url: str = "http://localhost:7879"):
        """
        Initialize GraphSub client.

        Args:
            graphsub_url: Base URL of GraphSub server (default: http://localhost:7879)
        """
        self.graphsub_url = graphsub_url
        self.http_client = httpx.AsyncClient(
            base_url=graphsub_url,
            timeout=30.0
        )
        self._node_cache: Dict[str, Any] = {}
        self._capability_cache: Dict[str, Dict[str, float]] = {}

    async def health_check(self) -> bool:
        """Check if GraphSub server is healthy."""
        try:
            response = await self.http_client.get("/health")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"GraphSub health check failed: {e}")
            return False

    async def create_agent_node(
        self,
        agent_id: str,
        model: str,
        budget_remaining: float,
        capabilities: Dict[str, float]
    ) -> AgentNode:
        """
        Create an agent node in GraphSub.

        Args:
            agent_id: Unique agent identifier
            model: LLM model name (e.g., "claude-3.5-sonnet")
            budget_remaining: Remaining API credit budget
            capabilities: Dict of skill -> confidence level

        Returns:
            AgentNode with created node ID
        """
        now = datetime.utcnow().isoformat()

        agent_data = {
            "id": agent_id,
            "type": "Agent",
            "payload": json.dumps({
                "model": model,
                "status": "idle",
                "budget_remaining": budget_remaining,
                "capabilities": capabilities,
                "created_at": now,
                "heartbeat_last": now
            })
        }

        try:
            response = await self.http_client.post(
                "/api/v1/shards/0/nodes",
                json=agent_data
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Created agent node: {agent_id}")

            node = AgentNode(
                id=result.get("id", agent_id),
                model=model,
                status="idle",
                budget_remaining=budget_remaining,
                capabilities=capabilities,
                created_at=now,
                heartbeat_last=now
            )

            # Cache the node
            self._node_cache[agent_id] = asdict(node)
            self._capability_cache[agent_id] = capabilities

            return node
        except httpx.HTTPError as e:
            logger.error(f"Failed to create agent node: {e}")
            raise

    async def create_task_node(
        self,
        task_id: str,
        task_type: str,
        priority: int,
        required_capabilities: List[str],
        estimated_cost_usd: float = 0.0
    ) -> TaskNode:
        """
        Create a task node in GraphSub.

        Args:
            task_id: Unique task identifier
            task_type: Type of task (e.g., "code_execution", "summarization")
            priority: Priority level (1-10)
            required_capabilities: List of required agent capabilities
            estimated_cost_usd: Estimated cost in USD

        Returns:
            TaskNode with created node ID
        """
        now = datetime.utcnow().isoformat()

        task_data = {
            "id": task_id,
            "type": "Task",
            "payload": json.dumps({
                "task_type": task_type,
                "priority": priority,
                "status": "pending",
                "required_capabilities": required_capabilities,
                "estimated_cost_usd": estimated_cost_usd,
                "created_at": now
            })
        }

        try:
            response = await self.http_client.post(
                "/api/v1/shards/0/nodes",
                json=task_data
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Created task node: {task_id}")

            node = TaskNode(
                id=result.get("id", task_id),
                type=task_type,
                priority=priority,
                status="pending",
                required_capabilities=required_capabilities,
                estimated_cost_usd=estimated_cost_usd,
                created_at=now
            )

            self._node_cache[task_id] = asdict(node)
            return node
        except httpx.HTTPError as e:
            logger.error(f"Failed to create task node: {e}")
            raise

    async def create_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        weight: float = 1.0
    ) -> Dict[str, Any]:
        """
        Create an edge between two nodes.

        Args:
            source_id: Source node ID
            target_id: Target node ID
            relation_type: Relation type (e.g., "HAS_CAPABILITY", "ASSIGNED_TO")
            weight: Edge weight (default 1.0)

        Returns:
            Edge metadata
        """
        edge_data = {
            "source": source_id,
            "target": target_id,
            "relation": relation_type,
            "weight": weight
        }

        try:
            response = await self.http_client.post(
                "/api/v1/shards/0/edges",
                json=edge_data
            )
            response.raise_for_status()
            result = response.json()
            logger.debug(f"Created edge: {source_id} -[{relation_type}]-> {target_id}")
            return result
        except httpx.HTTPError as e:
            logger.error(f"Failed to create edge: {e}")
            raise

    async def deposit_pheromone(
        self,
        source_id: str,
        target_id: str,
        trail_config: PheromoneTrail
    ) -> bool:
        """
        Deposit a stigmergic pheromone trail.

        Args:
            source_id: Agent depositing the pheromone
            target_id: Target node (task, location, etc.)
            trail_config: Pheromone configuration (signal type, intensity, decay)

        Returns:
            True if pheromone deposited successfully
        """
        pheromone_data = {
            "source": source_id,
            "target": target_id,
            "signal_type": trail_config.signal_type,
            "intensity": trail_config.intensity,
            "decay_rate": trail_config.decay_rate,
            "ttl_seconds": trail_config.ttl_seconds
        }

        try:
            response = await self.http_client.post(
                "/api/v1/stigmergy/edge",
                json=pheromone_data
            )
            response.raise_for_status()
            logger.debug(
                f"Deposited pheromone: {source_id} "
                f"[{trail_config.signal_type}:{trail_config.intensity}]"
            )
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to deposit pheromone: {e}")
            return False

    async def get_agent_capabilities(
        self,
        agent_id: str
    ) -> Dict[str, float]:
        """
        Get agent capabilities and confidence levels.

        Uses Cypher query to fetch capabilities from GraphSub.

        Args:
            agent_id: Agent ID

        Returns:
            Dict of skill -> confidence level
        """
        # Check cache first
        if agent_id in self._capability_cache:
            return self._capability_cache[agent_id]

        query = f"""
        MATCH (a:Agent {{id: '{agent_id}'}})-[:HAS_CAPABILITY {{level: $min_level}}]->(c:Capability)
        RETURN c.name, c.level
        """

        try:
            response = await self.http_client.post(
                "/cypher",
                json={"query": query, "parameters": {"min_level": 0.5}}
            )
            response.raise_for_status()
            result = response.json()

            capabilities = {}
            for record in result.get("results", []):
                if len(record) >= 2:
                    skill_name = record[0]
                    confidence = record[1]
                    capabilities[skill_name] = confidence

            # Cache the result
            self._capability_cache[agent_id] = capabilities
            logger.debug(f"Fetched {len(capabilities)} capabilities for agent {agent_id}")
            return capabilities
        except httpx.HTTPError as e:
            logger.error(f"Failed to get agent capabilities: {e}")
            return {}

    async def query(
        self,
        cypher_query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[List[Any]]:
        """
        Execute a Cypher query on GraphSub.

        Args:
            cypher_query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result rows
        """
        try:
            response = await self.http_client.post(
                "/cypher",
                json={
                    "query": cypher_query,
                    "parameters": parameters or {}
                }
            )
            response.raise_for_status()
            result = response.json()
            return result.get("results", [])
        except httpx.HTTPError as e:
            logger.error(f"Cypher query failed: {e}")
            raise

    async def update_agent_status(
        self,
        agent_id: str,
        status: str,
        budget_remaining: Optional[float] = None
    ) -> bool:
        """
        Update agent status in GraphSub.

        Args:
            agent_id: Agent ID
            status: New status (idle, executing, waiting, failed)
            budget_remaining: Updated budget (optional)

        Returns:
            True if updated successfully
        """
        updates = {"status": status}
        if budget_remaining is not None:
            updates["budget_remaining"] = budget_remaining

        update_query = f"""
        MATCH (a:Agent {{id: '{agent_id}'}})
        SET a.status = '{status}'
        """
        if budget_remaining is not None:
            update_query += f", a.budget_remaining = {budget_remaining}"

        try:
            await self.query(update_query)
            logger.debug(f"Updated agent {agent_id} status to {status}")

            # Update cache
            if agent_id in self._node_cache:
                self._node_cache[agent_id]["status"] = status
                if budget_remaining is not None:
                    self._node_cache[agent_id]["budget_remaining"] = budget_remaining

            return True
        except Exception as e:
            logger.error(f"Failed to update agent status: {e}")
            return False

    async def get_pheromone_strength(
        self,
        target_id: str,
        signal_type: Optional[str] = None
    ) -> float:
        """
        Get current pheromone strength at a target node.

        Args:
            target_id: Target node ID
            signal_type: Filter by signal type (optional)

        Returns:
            Aggregated pheromone intensity (0.0 to 10.0+)
        """
        query = f"""
        MATCH (a:Agent)-[p:PHEROMONE {{signal_type: '{signal_type}'}}]->(t:Node {{id: '{target_id}'}})
        RETURN SUM(p.intensity)
        """

        try:
            result = await self.query(query)
            if result and result[0]:
                return float(result[0][0] or 0.0)
            return 0.0
        except Exception as e:
            logger.error(f"Failed to get pheromone strength: {e}")
            return 0.0

    async def scan_high_pheromone_targets(
        self,
        signal_type: str,
        agent_id: Optional[str] = None,
        limit: int = 10
    ) -> List[tuple]:
        """
        Scan for targets with high pheromone concentration.

        Used for emergent task discovery and load balancing.

        Args:
            signal_type: Pheromone signal type to scan
            agent_id: Optional - exclude pheromones from this agent
            limit: Maximum number of results

        Returns:
            List of (target_id, intensity) tuples, sorted by intensity desc
        """
        query = f"""
        MATCH (a:Agent)-[p:PHEROMONE {{signal_type: '{signal_type}'}}]->(target)
        RETURN target.id, SUM(p.intensity)
        ORDER BY SUM(p.intensity) DESC
        LIMIT {limit}
        """

        if agent_id:
            query = query.replace(
                "MATCH (a:Agent)",
                f"MATCH (a:Agent) WHERE a.id != '{agent_id}'"
            )

        try:
            results = await self.query(query)
            return [(row[0], float(row[1])) for row in results if row]
        except Exception as e:
            logger.error(f"Failed to scan pheromones: {e}")
            return []

    async def close(self):
        """Close the HTTP client connection."""
        await self.http_client.aclose()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
