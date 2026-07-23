"""
Swarm Hierarchy - Hierarchical swarm instantiation and control.

Enables swarms to:
- Instantiate child swarms
- Direct and delegate tasks to subordinate swarms
- Operate with reflexive autonomy (self-directed behavior)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from .persistence import get_swarm_db
from .master_control import get_mcp, ClearanceLevel, ActionType, AuthorizationRequest
from .swarm_mcp_bridge import get_swarm_mcp_bridge

logger = logging.getLogger(__name__)


class SwarmRelationType(str, Enum):
    """Types of relationships between swarms."""
    PARENT_CHILD = "parent_child"      # Direct hierarchy
    PEER = "peer"                       # Equal collaboration
    DELEGATE = "delegate"               # Task delegation only
    OBSERVER = "observer"               # Read-only access


class AutonomyLevel(int, Enum):
    """Level of autonomous decision-making."""
    MANUAL = 0          # Requires approval for all actions
    SUPERVISED = 1      # Can act, but reports to parent
    SEMI_AUTONOMOUS = 2 # Can spawn tasks and agents within budget
    AUTONOMOUS = 3      # Full autonomy within scope
    REFLEXIVE = 4       # Can modify own behavior and spawn swarms


class SwarmDirective(BaseModel):
    """A directive from one swarm to another."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    from_swarm: str
    to_swarm: str
    directive_type: str  # "task", "config", "spawn_agent", "terminate", etc.
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = 1
    requires_ack: bool = True
    timeout_seconds: Optional[int] = None
    status: str = "pending"  # pending, acknowledged, completed, failed, timeout
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    acknowledged_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None


class SwarmRelation(BaseModel):
    """Relationship between two swarms."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    parent_swarm_id: str
    child_swarm_id: str
    relation_type: SwarmRelationType = SwarmRelationType.PARENT_CHILD
    permissions: Set[str] = Field(default_factory=set)  # What parent can do to child
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SwarmController:
    """
    Controller for a swarm with hierarchical capabilities.

    Enables:
    - Spawning child swarms
    - Directing subordinate swarms
    - Reflexive autonomy (self-improvement)
    """

    def __init__(self, swarm_id: str):
        self.swarm_id = swarm_id
        self.db = get_swarm_db()
        self.mcp = get_mcp(swarm_id)
        self.bridge = get_swarm_mcp_bridge()

        # In-memory state
        self._children: Dict[str, SwarmRelation] = {}
        self._parent: Optional[SwarmRelation] = None
        self._pending_directives: Dict[str, SwarmDirective] = {}
        self._autonomy_level = AutonomyLevel.SUPERVISED
        self._directive_handlers: Dict[str, Callable] = {}

        # Load relationships from DB
        self._load_relationships()

        # Register default directive handlers
        self._register_default_handlers()

        logger.info(f"SwarmController initialized for {swarm_id}")

    def _load_relationships(self):
        """Load swarm relationships from database."""
        try:
            with self.db.connection() as conn:
                # Check if table exists
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='swarm_relations'"
                ).fetchall()

                if not tables:
                    # Create the table
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS swarm_relations (
                            id VARCHAR PRIMARY KEY,
                            parent_swarm_id VARCHAR NOT NULL,
                            child_swarm_id VARCHAR NOT NULL,
                            relation_type VARCHAR DEFAULT 'parent_child',
                            permissions JSON DEFAULT '[]',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS swarm_directives (
                            id VARCHAR PRIMARY KEY,
                            from_swarm VARCHAR NOT NULL,
                            to_swarm VARCHAR NOT NULL,
                            directive_type VARCHAR NOT NULL,
                            payload JSON,
                            priority INTEGER DEFAULT 1,
                            requires_ack BOOLEAN DEFAULT TRUE,
                            timeout_seconds INTEGER,
                            status VARCHAR DEFAULT 'pending',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            acknowledged_at TIMESTAMP,
                            completed_at TIMESTAMP,
                            result JSON
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS swarm_autonomy (
                            swarm_id VARCHAR PRIMARY KEY,
                            autonomy_level INTEGER DEFAULT 1,
                            scope JSON DEFAULT '{}',
                            budget_limit DOUBLE DEFAULT 100.0,
                            can_spawn_swarms BOOLEAN DEFAULT FALSE,
                            can_self_modify BOOLEAN DEFAULT FALSE,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    return

                # Load children
                children = conn.execute(
                    "SELECT * FROM swarm_relations WHERE parent_swarm_id = ?",
                    [self.swarm_id]
                ).fetchall()

                for row in children:
                    rel = SwarmRelation(
                        id=row[0],
                        parent_swarm_id=row[1],
                        child_swarm_id=row[2],
                        relation_type=SwarmRelationType(row[3]) if row[3] else SwarmRelationType.PARENT_CHILD,
                    )
                    self._children[rel.child_swarm_id] = rel

                # Load parent
                parent = conn.execute(
                    "SELECT * FROM swarm_relations WHERE child_swarm_id = ? AND relation_type = 'parent_child'",
                    [self.swarm_id]
                ).fetchone()

                if parent:
                    self._parent = SwarmRelation(
                        id=parent[0],
                        parent_swarm_id=parent[1],
                        child_swarm_id=parent[2],
                        relation_type=SwarmRelationType(parent[3]) if parent[3] else SwarmRelationType.PARENT_CHILD,
                    )

                # Load autonomy level
                autonomy = conn.execute(
                    "SELECT autonomy_level FROM swarm_autonomy WHERE swarm_id = ?",
                    [self.swarm_id]
                ).fetchone()

                if autonomy:
                    self._autonomy_level = AutonomyLevel(autonomy[0])

        except Exception as e:
            logger.warning(f"Failed to load relationships: {e}")

    def _register_default_handlers(self):
        """Register default directive handlers."""
        self._directive_handlers["task"] = self._handle_task_directive
        self._directive_handlers["config"] = self._handle_config_directive
        self._directive_handlers["spawn_agent"] = self._handle_spawn_agent_directive
        self._directive_handlers["terminate"] = self._handle_terminate_directive
        self._directive_handlers["report"] = self._handle_report_directive
        self._directive_handlers["spawn_swarm"] = self._handle_spawn_swarm_directive

    @property
    def autonomy_level(self) -> AutonomyLevel:
        return self._autonomy_level

    @property
    def can_spawn_swarms(self) -> bool:
        return self._autonomy_level >= AutonomyLevel.REFLEXIVE

    @property
    def parent_swarm_id(self) -> Optional[str]:
        return self._parent.parent_swarm_id if self._parent else None

    @property
    def child_swarm_ids(self) -> List[str]:
        return list(self._children.keys())

    async def spawn_child_swarm(
        self,
        name: str,
        purpose: str,
        autonomy_level: AutonomyLevel = AutonomyLevel.SUPERVISED,
        initial_agents: Optional[List[Dict[str, Any]]] = None,
        budget: float = 100.0,
    ) -> Optional[str]:
        """
        Spawn a new child swarm.

        Returns the new swarm ID if successful.
        """
        # Check if we have permission
        if not self.can_spawn_swarms:
            # Request permission from MCP
            auth = self.mcp.authorize(AuthorizationRequest(
                agent_id=f"swarm-{self.swarm_id}",
                action=ActionType.SPAWN_AGENT,  # Use spawn_agent as proxy
                target=f"swarm:{name}",
                justification=f"Spawning child swarm for: {purpose}",
            ))

            if not auth.authorized:
                logger.warning(f"Swarm {self.swarm_id} denied permission to spawn child: {auth.reason}")
                return None

        # Generate child swarm ID
        child_id = f"{self.swarm_id}-child-{str(uuid4())[:8]}"

        # Create the swarm in DB
        try:
            self.db.create_swarm(
                swarm_id=child_id,
                name=name,
                host_id=self.swarm_id,  # Parent is the host
                host_wallet=f"internal:{self.swarm_id}",
            )

            # Create relationship
            with self.db.connection() as conn:
                rel_id = str(uuid4())
                conn.execute("""
                    INSERT INTO swarm_relations (id, parent_swarm_id, child_swarm_id, relation_type, permissions)
                    VALUES (?, ?, ?, ?, ?)
                """, [rel_id, self.swarm_id, child_id, SwarmRelationType.PARENT_CHILD.value, "[]"])

                # Set autonomy level
                conn.execute("""
                    INSERT INTO swarm_autonomy (swarm_id, autonomy_level, budget_limit, can_spawn_swarms)
                    VALUES (?, ?, ?, ?)
                """, [child_id, autonomy_level.value, budget, autonomy_level >= AutonomyLevel.REFLEXIVE])

            # Update local cache
            self._children[child_id] = SwarmRelation(
                id=rel_id,
                parent_swarm_id=self.swarm_id,
                child_swarm_id=child_id,
            )

            # Initialize child's MCP
            child_mcp = get_mcp(child_id)

            # Spawn initial agents if provided
            if initial_agents:
                for agent_config in initial_agents:
                    child_mcp.register_agent(
                        agent_id=agent_config.get("id", f"agent-{str(uuid4())[:8]}"),
                        clearance=ClearanceLevel(agent_config.get("clearance", 1)),
                    )

            # Log the event
            self.db.log_mcp_event(
                swarm_id=self.swarm_id,
                event_type="child_swarm_spawned",
                data={
                    "child_swarm_id": child_id,
                    "name": name,
                    "purpose": purpose,
                    "autonomy_level": autonomy_level.name,
                }
            )

            logger.info(f"Spawned child swarm: {child_id} with autonomy {autonomy_level.name}")

            return child_id

        except Exception as e:
            logger.error(f"Failed to spawn child swarm: {e}")
            return None

    async def send_directive(
        self,
        to_swarm: str,
        directive_type: str,
        payload: Dict[str, Any],
        priority: int = 1,
        timeout_seconds: Optional[int] = 60,
    ) -> Optional[SwarmDirective]:
        """
        Send a directive to another swarm.

        Only works if we have a relationship with the target swarm.
        """
        # Verify relationship
        if to_swarm not in self._children:
            # Check if it's a peer or we're the parent
            logger.warning(f"No relationship with swarm {to_swarm}")
            return None

        directive = SwarmDirective(
            from_swarm=self.swarm_id,
            to_swarm=to_swarm,
            directive_type=directive_type,
            payload=payload,
            priority=priority,
            timeout_seconds=timeout_seconds,
        )

        # Store in DB
        try:
            import json
            with self.db.connection() as conn:
                conn.execute("""
                    INSERT INTO swarm_directives
                    (id, from_swarm, to_swarm, directive_type, payload, priority, timeout_seconds, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """, [
                    directive.id, directive.from_swarm, directive.to_swarm,
                    directive.directive_type, json.dumps(directive.payload),
                    directive.priority, directive.timeout_seconds,
                    datetime.now(timezone.utc)
                ])

            self._pending_directives[directive.id] = directive

            logger.info(f"Sent directive {directive.id} to {to_swarm}: {directive_type}")

            return directive

        except Exception as e:
            logger.error(f"Failed to send directive: {e}")
            return None

    async def process_incoming_directives(self) -> List[Dict[str, Any]]:
        """Process directives sent to this swarm."""
        results = []

        try:
            with self.db.connection() as conn:
                directives = conn.execute("""
                    SELECT * FROM swarm_directives
                    WHERE to_swarm = ? AND status = 'pending'
                    ORDER BY priority DESC, created_at ASC
                """, [self.swarm_id]).fetchall()

            for row in directives:
                import json
                payload = {}
                if row[4]:
                    try:
                        payload = json.loads(row[4])
                    except (json.JSONDecodeError, ValueError, TypeError):
                        payload = {}

                directive = SwarmDirective(
                    id=row[0],
                    from_swarm=row[1],
                    to_swarm=row[2],
                    directive_type=row[3],
                    payload=payload,
                    priority=row[5],
                    status=row[8],
                )

                # Process based on type
                handler = self._directive_handlers.get(directive.directive_type)
                if handler:
                    try:
                        result = await handler(directive)
                        directive.status = "completed"
                        directive.result = result
                        results.append({"directive_id": directive.id, "status": "completed", "result": result})
                    except Exception as e:
                        directive.status = "failed"
                        directive.result = {"error": str(e)}
                        results.append({"directive_id": directive.id, "status": "failed", "error": str(e)})
                else:
                    directive.status = "failed"
                    directive.result = {"error": f"Unknown directive type: {directive.directive_type}"}
                    results.append({"directive_id": directive.id, "status": "failed", "error": "Unknown type"})

                # Update DB
                with self.db.connection() as conn:
                    conn.execute("""
                        UPDATE swarm_directives SET status = ?, completed_at = ?, result = ?
                        WHERE id = ?
                    """, [directive.status, datetime.now(timezone.utc), str(directive.result), directive.id])

        except Exception as e:
            logger.error(f"Error processing directives: {e}")

        return results

    async def _handle_task_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle a task directive."""
        task_desc = directive.payload.get("description", "")

        # Execute via LLM
        result = await self.bridge.execute_llm_task(
            task_description=task_desc,
            swarm_id=self.swarm_id,
            context=directive.payload.get("context"),
        )

        return result

    async def _handle_config_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle a configuration directive."""
        config = directive.payload

        # Update autonomy level if specified
        if "autonomy_level" in config:
            new_level = AutonomyLevel(config["autonomy_level"])
            self._autonomy_level = new_level

            with self.db.connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO swarm_autonomy (swarm_id, autonomy_level, updated_at)
                    VALUES (?, ?, ?)
                """, [self.swarm_id, new_level.value, datetime.now(timezone.utc)])

        return {"status": "configured", "config": config}

    async def _handle_spawn_agent_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle agent spawn directive."""
        agent_id = directive.payload.get("agent_id", f"agent-{str(uuid4())[:8]}")
        clearance = ClearanceLevel(directive.payload.get("clearance", 1))

        self.mcp.register_agent(agent_id, clearance)

        return {"status": "spawned", "agent_id": agent_id, "clearance": clearance.name}

    async def _handle_terminate_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle termination directive."""
        # This would gracefully shut down the swarm
        logger.warning(f"Swarm {self.swarm_id} received termination directive")
        return {"status": "terminating"}

    async def _handle_report_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle report directive - return swarm status."""
        return {
            "swarm_id": self.swarm_id,
            "autonomy_level": self._autonomy_level.name,
            "children": list(self._children.keys()),
            "mcp_status": self.mcp.status(),
        }

    async def _handle_spawn_swarm_directive(self, directive: SwarmDirective) -> Dict[str, Any]:
        """Handle spawn swarm directive."""
        child_id = await self.spawn_child_swarm(
            name=directive.payload.get("name", "child-swarm"),
            purpose=directive.payload.get("purpose", "delegated work"),
            autonomy_level=AutonomyLevel(directive.payload.get("autonomy_level", 1)),
            budget=directive.payload.get("budget", 100.0),
        )

        if child_id:
            return {"status": "spawned", "child_swarm_id": child_id}
        else:
            return {"status": "failed", "error": "Could not spawn child swarm"}

    async def autonomous_decision(self, context: str) -> Optional[Dict[str, Any]]:
        """
        Make an autonomous decision based on context.

        Uses LLM to analyze situation and decide action.
        Only available at SEMI_AUTONOMOUS or higher.
        """
        if self._autonomy_level < AutonomyLevel.SEMI_AUTONOMOUS:
            return None

        # Use LLM to make decision
        prompt = f"""You are an autonomous swarm controller.

Current swarm: {self.swarm_id}
Autonomy level: {self._autonomy_level.name}
Children: {list(self._children.keys())}
Parent: {self.parent_swarm_id or 'None (root)'}

Context:
{context}

Based on this context, decide what action to take.
Options:
1. spawn_child - Spawn a new child swarm
2. send_task - Delegate task to a child
3. report_parent - Report status to parent
4. self_improve - Propose improvement to own code
5. no_action - Take no action

Respond with JSON: {{"action": "...", "params": {{}}, "reasoning": "..."}}
"""

        result = await self.bridge.execute_llm_task(
            task_description=prompt,
            swarm_id=self.swarm_id,
        )

        if result.get("success"):
            try:
                import json
                import re
                response = result.get("result", "{}")
                # Extract JSON
                json_match = re.search(r'\{[\s\S]*\}', response)
                if json_match:
                    decision = json.loads(json_match.group())

                    # Log the autonomous decision
                    self.db.log_mcp_event(
                        swarm_id=self.swarm_id,
                        event_type="autonomous_decision",
                        data=decision,
                    )

                    return decision
            except Exception:
                pass

        return None

    def status(self) -> Dict[str, Any]:
        """Get swarm controller status."""
        return {
            "swarm_id": self.swarm_id,
            "autonomy_level": self._autonomy_level.name,
            "can_spawn_swarms": self.can_spawn_swarms,
            "parent_swarm": self.parent_swarm_id,
            "children": list(self._children.keys()),
            "children_count": len(self._children),
            "pending_directives": len(self._pending_directives),
            "mcp_status": self.mcp.status(),
        }


# Singleton per swarm
_controllers: Dict[str, SwarmController] = {}


def get_swarm_controller(swarm_id: str) -> SwarmController:
    """Get or create a swarm controller."""
    if swarm_id not in _controllers:
        _controllers[swarm_id] = SwarmController(swarm_id)
    return _controllers[swarm_id]
