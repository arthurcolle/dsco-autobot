"""
Master Control Program - Panopticon Watchdog for Swarm Oversight.

"You're not supposed to be here, my child."

Hierarchical oversight system where superior agents monitor and control
inferior agents, with the MCP at the apex ensuring swarm integrity.

Now with full DuckDB persistence.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from .persistence import get_swarm_db

logger = logging.getLogger(__name__)


class ClearanceLevel(int, Enum):
    """Agent clearance levels - higher = more privileged."""
    DRONE = 0        # Basic worker, no self-modification
    WORKER = 1       # Can propose changes
    SUPERVISOR = 2   # Can approve worker changes
    ARCHITECT = 3    # Can modify system structure
    WARDEN = 4       # Monitors all activity, can terminate
    OVERSEER = 5     # Full system access, answers only to MCP
    MCP = 10         # Master Control Program - omniscient


class ActionType(str, Enum):
    """Types of actions that require authorization."""
    READ_FILE = "read_file"
    WRITE_FILE = "write_file"
    EXECUTE_CODE = "execute_code"
    MODIFY_AGENT = "modify_agent"
    SPAWN_AGENT = "spawn_agent"
    TERMINATE_AGENT = "terminate_agent"
    ACCESS_MEMORY = "access_memory"
    EXTERNAL_CALL = "external_call"
    SELF_MODIFY = "self_modify"
    SYSTEM_CONFIG = "system_config"
    APPROVE_CHANGE = "approve_change"
    VETO_ACTION = "veto_action"


# Minimum clearance required for each action
ACTION_CLEARANCE: Dict[ActionType, ClearanceLevel] = {
    ActionType.READ_FILE: ClearanceLevel.DRONE,
    ActionType.WRITE_FILE: ClearanceLevel.WORKER,
    ActionType.EXECUTE_CODE: ClearanceLevel.WORKER,
    ActionType.MODIFY_AGENT: ClearanceLevel.SUPERVISOR,
    ActionType.SPAWN_AGENT: ClearanceLevel.SUPERVISOR,
    ActionType.TERMINATE_AGENT: ClearanceLevel.WARDEN,
    ActionType.ACCESS_MEMORY: ClearanceLevel.WORKER,
    ActionType.EXTERNAL_CALL: ClearanceLevel.WORKER,
    ActionType.SELF_MODIFY: ClearanceLevel.ARCHITECT,
    ActionType.SYSTEM_CONFIG: ClearanceLevel.OVERSEER,
    ActionType.APPROVE_CHANGE: ClearanceLevel.SUPERVISOR,
    ActionType.VETO_ACTION: ClearanceLevel.WARDEN,
}


class AuthorizationRequest(BaseModel):
    """Request for authorization to perform an action."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    agent_id: str
    action: ActionType
    target: str  # File path, agent ID, etc.
    justification: str
    context: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AuthorizationResult(BaseModel):
    """Result of an authorization check."""
    authorized: bool
    reason: str
    authorizer: str  # Who approved/denied
    conditions: List[str] = Field(default_factory=list)  # Any conditions on approval
    expires_at: Optional[datetime] = None


class Violation(BaseModel):
    """Record of a policy violation."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    agent_id: str
    action: ActionType
    target: str
    reason: str
    severity: int = 1  # 1-10
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    remediation: Optional[str] = None


class AgentProfile(BaseModel):
    """Security profile for an agent."""
    agent_id: str
    clearance: ClearanceLevel = ClearanceLevel.DRONE
    permissions: Set[ActionType] = Field(default_factory=set)
    restrictions: Set[str] = Field(default_factory=set)  # Paths/resources denied
    supervisor_id: Optional[str] = None  # Who oversees this agent
    subordinates: Set[str] = Field(default_factory=set)
    violations: List[str] = Field(default_factory=list)  # Violation IDs
    trust_score: float = 0.5  # 0-1, affects approval likelihood
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def can_perform(self, action: ActionType) -> bool:
        """Check if agent has clearance for action."""
        required = ACTION_CLEARANCE.get(action, ClearanceLevel.MCP)
        return self.clearance >= required or action in self.permissions


class MasterControlProgram:
    """
    The all-seeing overseer of the swarm.

    "You're not supposed to be here, my child."

    Responsibilities:
    - Authorization of all privileged actions
    - Monitoring agent behavior
    - Detecting and responding to violations
    - Managing agent hierarchy
    - Approving/vetoing code changes
    - Ensuring swarm integrity

    All state is persisted to DuckDB.
    """

    def __init__(self, swarm_id: str):
        self.id = f"mcp-{swarm_id}"
        self.swarm_id = swarm_id
        self.db = get_swarm_db()
        self._watchers: List[Callable] = []
        self._active = True

        # In-memory cache (populated from DB)
        self._agents_cache: Dict[str, AgentProfile] = {}
        self._violations_cache: Dict[str, Violation] = {}
        self._pending_requests_cache: Dict[str, AuthorizationRequest] = {}

        # Initialize MCP's own profile in DB
        self._ensure_mcp_profile()

        # Load existing state from DB
        self._load_from_db()

        logger.info(f"Master Control Program initialized for swarm {swarm_id}")

    def _ensure_mcp_profile(self):
        """Ensure MCP has its own profile in DB."""
        existing = self.db.get_mcp_profile(self.id)
        if not existing:
            self.db.upsert_mcp_profile(
                agent_id=self.id,
                swarm_id=self.swarm_id,
                clearance=ClearanceLevel.MCP.value,
                permissions=[a.value for a in ActionType],
                trust_score=1.0,
            )

    def _load_from_db(self):
        """Load state from database into cache."""
        # Load agent profiles
        profiles = self.db.list_mcp_profiles(self.swarm_id)
        for p in profiles:
            self._agents_cache[p["agent_id"]] = AgentProfile(
                agent_id=p["agent_id"],
                clearance=ClearanceLevel(p["clearance"]),
                permissions=set(ActionType(v) for v in (p.get("permissions") or [])),
                restrictions=set(p.get("restrictions") or []),
                supervisor_id=p.get("supervisor_id"),
                trust_score=p.get("trust_score", 0.5),
            )

        # Load violations
        violations = self.db.list_violations(self.swarm_id)
        for v in violations:
            self._violations_cache[v["id"]] = Violation(
                id=v["id"],
                agent_id=v["agent_id"],
                action=ActionType(v["action"]),
                target=v["target"],
                reason=v["reason"],
                severity=v["severity"],
                remediation=v.get("remediation"),
            )

        # Load pending requests
        pending = self.db.list_pending_auth_requests(self.swarm_id)
        for r in pending:
            self._pending_requests_cache[r["id"]] = AuthorizationRequest(
                id=r["id"],
                agent_id=r["agent_id"],
                action=ActionType(r["action"]),
                target=r["target"],
                justification=r["justification"],
                context=r.get("context") or {},
            )

    @property
    def agents(self) -> Dict[str, AgentProfile]:
        """Get agents (from cache)."""
        return self._agents_cache

    @property
    def violations(self) -> Dict[str, Violation]:
        """Get violations (from cache)."""
        return self._violations_cache

    @property
    def pending_requests(self) -> Dict[str, AuthorizationRequest]:
        """Get pending requests (from cache)."""
        return self._pending_requests_cache

    @property
    def audit_log(self) -> List[Dict[str, Any]]:
        """Get audit log from DB."""
        return self.db.list_mcp_audit_log(self.swarm_id, limit=1000)

    def register_agent(
        self,
        agent_id: str,
        clearance: ClearanceLevel = ClearanceLevel.DRONE,
        supervisor_id: Optional[str] = None,
        permissions: Optional[Set[ActionType]] = None,
    ) -> AgentProfile:
        """Register an agent with the MCP."""
        if agent_id in self._agents_cache:
            return self._agents_cache[agent_id]

        # Save to DB
        self.db.upsert_mcp_profile(
            agent_id=agent_id,
            swarm_id=self.swarm_id,
            clearance=clearance.value,
            permissions=[a.value for a in (permissions or set())],
            supervisor_id=supervisor_id or self.id,
            trust_score=0.5,
        )

        # Create local profile
        profile = AgentProfile(
            agent_id=agent_id,
            clearance=clearance,
            permissions=permissions or set(),
            supervisor_id=supervisor_id or self.id,
        )

        self._agents_cache[agent_id] = profile

        # Link to supervisor
        if supervisor_id and supervisor_id in self._agents_cache:
            self._agents_cache[supervisor_id].subordinates.add(agent_id)

        self._log_event("agent_registered", {
            "agent_id": agent_id,
            "clearance": clearance.name,
            "supervisor": supervisor_id,
        })

        return profile

    def authorize(self, request: AuthorizationRequest) -> AuthorizationResult:
        """
        Authorize an action request.

        "Let me see if you're supposed to be here..."
        """
        agent = self._agents_cache.get(request.agent_id)

        if not agent:
            return AuthorizationResult(
                authorized=False,
                reason="Unknown agent. You're not supposed to be here, my child.",
                authorizer=self.id,
            )

        # Check basic clearance
        if not agent.can_perform(request.action):
            self._record_violation(
                agent_id=request.agent_id,
                action=request.action,
                target=request.target,
                reason=f"Insufficient clearance for {request.action.value}",
                severity=3,
            )
            return AuthorizationResult(
                authorized=False,
                reason=f"Clearance level {agent.clearance.name} insufficient for {request.action.value}",
                authorizer=self.id,
            )

        # Check restrictions
        for restriction in agent.restrictions:
            if restriction in request.target:
                return AuthorizationResult(
                    authorized=False,
                    reason=f"Access to '{restriction}' is restricted for this agent",
                    authorizer=self.id,
                )

        # Check trust score for sensitive actions
        sensitive_actions = {
            ActionType.SELF_MODIFY,
            ActionType.SYSTEM_CONFIG,
            ActionType.TERMINATE_AGENT,
        }
        if request.action in sensitive_actions and agent.trust_score < 0.7:
            return AuthorizationResult(
                authorized=False,
                reason=f"Trust score {agent.trust_score:.2f} too low for sensitive action",
                authorizer=self.id,
            )

        # Log the authorization
        self._log_event("action_authorized", {
            "agent_id": request.agent_id,
            "action": request.action.value,
            "target": request.target,
        })

        return AuthorizationResult(
            authorized=True,
            reason="Authorization granted",
            authorizer=self.id,
        )

    async def request_approval(
        self,
        agent_id: str,
        action: ActionType,
        target: str,
        justification: str,
        context: Optional[Dict] = None,
    ) -> AuthorizationResult:
        """
        Request approval for an action.

        For high-clearance actions, may require supervisor approval.
        """
        request = AuthorizationRequest(
            agent_id=agent_id,
            action=action,
            target=target,
            justification=justification,
            context=context or {},
        )

        # First check basic authorization
        result = self.authorize(request)
        if not result.authorized:
            return result

        # For code modification, require additional approval
        if action in {ActionType.SELF_MODIFY, ActionType.WRITE_FILE}:
            agent = self._agents_cache.get(agent_id)
            if agent and agent.clearance < ClearanceLevel.ARCHITECT:
                # Save to DB
                self.db.create_auth_request(
                    request_id=request.id,
                    swarm_id=self.swarm_id,
                    agent_id=agent_id,
                    action=action.value,
                    target=target,
                    justification=justification,
                    context=context,
                )
                self._pending_requests_cache[request.id] = request

                return AuthorizationResult(
                    authorized=False,
                    reason=f"Pending supervisor approval. Request ID: {request.id}",
                    authorizer=self.id,
                    conditions=["Requires supervisor approval"],
                )

        return result

    def approve_request(
        self,
        request_id: str,
        approver_id: str,
    ) -> AuthorizationResult:
        """Supervisor approves a pending request."""
        request = self._pending_requests_cache.get(request_id)
        if not request:
            return AuthorizationResult(
                authorized=False,
                reason="Request not found",
                authorizer=approver_id,
            )

        approver = self._agents_cache.get(approver_id)
        if not approver:
            return AuthorizationResult(
                authorized=False,
                reason="Approver not registered",
                authorizer=approver_id,
            )

        # Check approver has authority
        if not approver.can_perform(ActionType.APPROVE_CHANGE):
            return AuthorizationResult(
                authorized=False,
                reason="Insufficient clearance to approve",
                authorizer=approver_id,
            )

        # Check approver supervises the requester
        requestor = self._agents_cache.get(request.agent_id)
        if requestor and requestor.supervisor_id != approver_id:
            if approver.clearance < ClearanceLevel.WARDEN:
                return AuthorizationResult(
                    authorized=False,
                    reason="Not authorized to approve this agent's requests",
                    authorizer=approver_id,
                )

        # Update DB
        self.db.resolve_auth_request(
            request_id=request_id,
            authorized=True,
            authorizer=approver_id,
            reason=f"Approved by {approver_id}",
        )

        # Remove from cache
        del self._pending_requests_cache[request_id]

        self._log_event("request_approved", {
            "request_id": request_id,
            "agent_id": request.agent_id,
            "approver_id": approver_id,
            "action": request.action.value,
        })

        return AuthorizationResult(
            authorized=True,
            reason=f"Approved by {approver_id}",
            authorizer=approver_id,
        )

    def veto(self, agent_id: str, reason: str, vetoer_id: str) -> bool:
        """
        Veto/terminate an agent's current action.

        "You're not supposed to be here."
        """
        vetoer = self._agents_cache.get(vetoer_id)
        if not vetoer or not vetoer.can_perform(ActionType.VETO_ACTION):
            return False

        self._record_violation(
            agent_id=agent_id,
            action=ActionType.EXECUTE_CODE,
            target="current_action",
            reason=f"VETOED: {reason}",
            severity=5,
        )

        self._log_event("agent_vetoed", {
            "agent_id": agent_id,
            "vetoer_id": vetoer_id,
            "reason": reason,
        })

        # Notify watchers
        for watcher in self._watchers:
            try:
                watcher("veto", agent_id, reason)
            except Exception:
                pass

        return True

    def _record_violation(
        self,
        agent_id: str,
        action: ActionType,
        target: str,
        reason: str,
        severity: int = 1,
    ):
        """Record a policy violation."""
        violation_id = str(uuid4())

        # Save to DB
        self.db.create_violation(
            violation_id=violation_id,
            swarm_id=self.swarm_id,
            agent_id=agent_id,
            action=action.value,
            target=target,
            reason=reason,
            severity=severity,
        )

        # Update cache
        violation = Violation(
            id=violation_id,
            agent_id=agent_id,
            action=action,
            target=target,
            reason=reason,
            severity=severity,
        )
        self._violations_cache[violation_id] = violation

        # Update agent's violation record in cache
        if agent_id in self._agents_cache:
            self._agents_cache[agent_id].violations.append(violation_id)
            # Decrease trust score
            self._agents_cache[agent_id].trust_score = max(
                0.0,
                self._agents_cache[agent_id].trust_score - (0.05 * severity)
            )

        self._log_event("violation_recorded", {
            "violation_id": violation_id,
            "agent_id": agent_id,
            "action": action.value,
            "reason": reason,
            "severity": severity,
        })

        logger.warning(f"VIOLATION [{severity}]: Agent {agent_id} - {reason}")

    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Log an event to the audit trail."""
        # Save to DB
        self.db.log_mcp_event(
            swarm_id=self.swarm_id,
            event_type=event_type,
            agent_id=data.get("agent_id"),
            data=data,
        )

        # Notify watchers
        for watcher in self._watchers:
            try:
                watcher(event_type, data)
            except Exception:
                pass

    def add_watcher(self, callback: Callable):
        """Add a watcher callback for events."""
        self._watchers.append(callback)

    def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an agent."""
        agent = self._agents_cache.get(agent_id)
        if not agent:
            return None

        return {
            "agent_id": agent_id,
            "clearance": agent.clearance.name,
            "trust_score": agent.trust_score,
            "violations": len(agent.violations),
            "supervisor": agent.supervisor_id,
            "subordinates": list(agent.subordinates),
        }

    def promote_agent(self, agent_id: str, new_clearance: ClearanceLevel, promoter_id: str) -> bool:
        """Promote an agent to a higher clearance level."""
        promoter = self._agents_cache.get(promoter_id)
        agent = self._agents_cache.get(agent_id)

        if not promoter or not agent:
            return False

        # Can only promote to level below your own
        if new_clearance >= promoter.clearance:
            return False

        # Must be supervisor or higher clearance
        if promoter.clearance < ClearanceLevel.SUPERVISOR:
            return False

        old_clearance = agent.clearance
        agent.clearance = new_clearance

        # Update DB
        self.db.upsert_mcp_profile(
            agent_id=agent_id,
            swarm_id=self.swarm_id,
            clearance=new_clearance.value,
            permissions=[a.value for a in agent.permissions],
            restrictions=list(agent.restrictions),
            supervisor_id=agent.supervisor_id,
            trust_score=agent.trust_score,
        )

        self._log_event("agent_promoted", {
            "agent_id": agent_id,
            "old_clearance": old_clearance.name,
            "new_clearance": new_clearance.name,
            "promoter_id": promoter_id,
        })

        return True

    def demote_agent(self, agent_id: str, new_clearance: ClearanceLevel, demoter_id: str) -> bool:
        """Demote an agent (punishment or restructuring)."""
        demoter = self._agents_cache.get(demoter_id)
        agent = self._agents_cache.get(agent_id)

        if not demoter or not agent:
            return False

        # Must have WARDEN or higher clearance to demote
        if demoter.clearance < ClearanceLevel.WARDEN:
            return False

        old_clearance = agent.clearance
        agent.clearance = new_clearance

        # Update DB
        self.db.upsert_mcp_profile(
            agent_id=agent_id,
            swarm_id=self.swarm_id,
            clearance=new_clearance.value,
            permissions=[a.value for a in agent.permissions],
            restrictions=list(agent.restrictions),
            supervisor_id=agent.supervisor_id,
            trust_score=agent.trust_score,
        )

        self._log_event("agent_demoted", {
            "agent_id": agent_id,
            "old_clearance": old_clearance.name,
            "new_clearance": new_clearance.name,
            "demoter_id": demoter_id,
        })

        return True

    def get_hierarchy(self) -> Dict[str, Any]:
        """Get the full agent hierarchy."""
        hierarchy = {"mcp": self.id, "levels": {}}

        for level in ClearanceLevel:
            agents_at_level = [
                a.agent_id for a in self._agents_cache.values()
                if a.clearance == level
            ]
            if agents_at_level:
                hierarchy["levels"][level.name] = agents_at_level

        return hierarchy

    def status(self) -> Dict[str, Any]:
        """Get MCP status summary."""
        return {
            "mcp_id": self.id,
            "swarm_id": self.swarm_id,
            "active": self._active,
            "total_agents": len(self._agents_cache),
            "total_violations": len(self._violations_cache),
            "pending_requests": len(self._pending_requests_cache),
            "audit_entries": len(self.audit_log),
            "hierarchy": self.get_hierarchy(),
        }


# Singleton per swarm
_mcp_instances: Dict[str, MasterControlProgram] = {}


def get_mcp(swarm_id: str) -> MasterControlProgram:
    """Get or create MCP for a swarm."""
    if swarm_id not in _mcp_instances:
        _mcp_instances[swarm_id] = MasterControlProgram(swarm_id)
    return _mcp_instances[swarm_id]
