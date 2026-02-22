"""
Agent MCP Capabilities for Swarm Economy.

Defines agent specializations and proficiencies with MCP backends.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MCPCapability(BaseModel):
    """An agent's capability with a specific MCP backend."""

    backend_id: str = Field(..., description="MCP backend ID (e.g., 'distributed-search-mcp')")
    tools: List[str] = Field(default_factory=list, description="Tools the agent can use")
    proficiency: float = Field(default=0.5, ge=0.0, le=1.0, description="Skill level 0-1")
    tasks_completed: int = Field(default=0, ge=0, description="Successful tasks with this backend")
    tasks_failed: int = Field(default=0, ge=0, description="Failed tasks with this backend")
    total_rewards: float = Field(default=0.0, description="Total rewards earned from this backend")
    last_used: Optional[datetime] = Field(default=None, description="Last time this capability was used")

    def update_proficiency(self, success: bool, task_reward: float = 0.0):
        """Update proficiency based on task outcome."""
        if success:
            self.tasks_completed += 1
            self.total_rewards += task_reward
            # Increase proficiency on success (diminishing returns)
            improvement = 0.05 * (1 - self.proficiency)
            self.proficiency = min(1.0, self.proficiency + improvement)
        else:
            self.tasks_failed += 1
            # Decrease proficiency on failure
            penalty = 0.02 * self.proficiency
            self.proficiency = max(0.1, self.proficiency - penalty)

        self.last_used = datetime.now(timezone.utc)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.tasks_completed + self.tasks_failed
        if total == 0:
            return 0.5  # Default assumption
        return self.tasks_completed / total

    @property
    def score(self) -> float:
        """Combined score for agent matching."""
        return self.proficiency * (0.5 + 0.5 * self.success_rate)


class AgentMCPProfile(BaseModel):
    """Complete MCP capability profile for an agent."""

    agent_id: str = Field(..., description="Agent ID")
    specializations: List[MCPCapability] = Field(
        default_factory=list,
        description="Agent's MCP backend specializations",
    )
    primary_backend: Optional[str] = Field(
        default=None,
        description="Agent's primary/preferred backend",
    )
    learning_enabled: bool = Field(
        default=True,
        description="Whether agent can learn new skills",
    )
    skills_learned: List[str] = Field(
        default_factory=list,
        description="Skill pattern IDs learned by this agent",
    )
    total_mcp_tasks: int = Field(default=0, description="Total MCP tasks executed")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def get_capability(self, backend_id: str) -> Optional[MCPCapability]:
        """Get capability for a specific backend."""
        for cap in self.specializations:
            if cap.backend_id == backend_id:
                return cap
        return None

    def add_capability(self, backend_id: str, tools: List[str]) -> MCPCapability:
        """Add or update a capability."""
        existing = self.get_capability(backend_id)
        if existing:
            # Update tools list
            existing.tools = list(set(existing.tools + tools))
            return existing

        # Create new capability
        cap = MCPCapability(backend_id=backend_id, tools=tools)
        self.specializations.append(cap)
        return cap

    def remove_capability(self, backend_id: str) -> bool:
        """Remove a capability."""
        for i, cap in enumerate(self.specializations):
            if cap.backend_id == backend_id:
                self.specializations.pop(i)
                if self.primary_backend == backend_id:
                    self.primary_backend = None
                return True
        return False

    def set_primary(self, backend_id: str) -> bool:
        """Set a backend as primary specialization."""
        if self.get_capability(backend_id):
            self.primary_backend = backend_id
            return True
        return False

    def record_task(self, backend_id: str, success: bool, reward: float = 0.0):
        """Record a task execution result."""
        cap = self.get_capability(backend_id)
        if cap:
            cap.update_proficiency(success, reward)
            self.total_mcp_tasks += 1
            self.updated_at = datetime.now(timezone.utc)

    def learn_skill(self, skill_id: str):
        """Record a learned skill."""
        if skill_id not in self.skills_learned:
            self.skills_learned.append(skill_id)
            self.updated_at = datetime.now(timezone.utc)

    def get_best_backend_for_tools(self, required_tools: List[str]) -> Optional[MCPCapability]:
        """Find the best capability matching required tools."""
        matching = []
        for cap in self.specializations:
            if all(tool in cap.tools for tool in required_tools):
                matching.append(cap)

        if not matching:
            return None

        # Sort by score (proficiency * success_rate)
        matching.sort(key=lambda c: c.score, reverse=True)
        return matching[0]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "agent_id": self.agent_id,
            "specializations": [
                {
                    "backend_id": c.backend_id,
                    "tools": c.tools,
                    "proficiency": c.proficiency,
                    "tasks_completed": c.tasks_completed,
                    "tasks_failed": c.tasks_failed,
                    "total_rewards": c.total_rewards,
                    "last_used": c.last_used.isoformat() if c.last_used else None,
                }
                for c in self.specializations
            ],
            "primary_backend": self.primary_backend,
            "learning_enabled": self.learning_enabled,
            "skills_learned": self.skills_learned,
            "total_mcp_tasks": self.total_mcp_tasks,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentMCPProfile":
        """Create from dictionary."""
        specializations = []
        for s in data.get("specializations", []):
            cap = MCPCapability(
                backend_id=s["backend_id"],
                tools=s.get("tools", []),
                proficiency=s.get("proficiency", 0.5),
                tasks_completed=s.get("tasks_completed", 0),
                tasks_failed=s.get("tasks_failed", 0),
                total_rewards=s.get("total_rewards", 0.0),
                last_used=datetime.fromisoformat(s["last_used"]) if s.get("last_used") else None,
            )
            specializations.append(cap)

        return cls(
            agent_id=data["agent_id"],
            specializations=specializations,
            primary_backend=data.get("primary_backend"),
            learning_enabled=data.get("learning_enabled", True),
            skills_learned=data.get("skills_learned", []),
            total_mcp_tasks=data.get("total_mcp_tasks", 0),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(timezone.utc),
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else datetime.now(timezone.utc),
        )


def create_specialized_profile(
    agent_id: str,
    backend_id: str,
    tools: List[str],
    initial_proficiency: float = 0.5,
) -> AgentMCPProfile:
    """Create a profile specialized for a single backend."""
    profile = AgentMCPProfile(agent_id=agent_id)
    cap = profile.add_capability(backend_id, tools)
    cap.proficiency = initial_proficiency
    profile.set_primary(backend_id)
    return profile


def create_generalist_profile(agent_id: str) -> AgentMCPProfile:
    """Create a generalist profile with all backends."""
    from .mcp_task_types import MCP_BACKEND_CAPABILITIES

    profile = AgentMCPProfile(agent_id=agent_id)
    for backend_id, info in MCP_BACKEND_CAPABILITIES.items():
        profile.add_capability(backend_id, info["tools"])

    return profile
