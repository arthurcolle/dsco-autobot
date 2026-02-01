"""
Project Management and Task Decomposition.

Enables:
- Complex project breakdown into subtasks
- Dependency tracking
- Milestone management
- Progress monitoring
- Resource allocation
- Risk assessment

Based on:
- Hierarchical Task Network (HTN) planning
- Work Breakdown Structure (WBS)
- Critical Path Method (CPM)
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class ProjectStatus(str, Enum):
    """Status of a project."""
    DRAFT = "draft"
    PLANNING = "planning"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskDependencyType(str, Enum):
    """Types of task dependencies."""
    FINISH_TO_START = "fs"   # B starts after A finishes
    START_TO_START = "ss"    # B starts when A starts
    FINISH_TO_FINISH = "ff"  # B finishes when A finishes
    START_TO_FINISH = "sf"   # B finishes when A starts


class MilestoneStatus(str, Enum):
    """Status of a milestone."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    ACHIEVED = "achieved"
    MISSED = "missed"


@dataclass
class TaskDependency:
    """A dependency between two tasks."""
    from_task_id: str
    to_task_id: str
    type: TaskDependencyType = TaskDependencyType.FINISH_TO_START
    lag: int = 0  # Additional delay in seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_task_id": self.from_task_id,
            "to_task_id": self.to_task_id,
            "type": self.type.value,
            "lag": self.lag,
        }


@dataclass
class Milestone:
    """A project milestone."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""

    # Requirements
    required_task_ids: List[str] = field(default_factory=list)
    deliverables: List[str] = field(default_factory=list)

    # Timing
    target_date: Optional[datetime] = None
    achieved_date: Optional[datetime] = None

    # Status
    status: MilestoneStatus = MilestoneStatus.PENDING

    # Reward
    bonus_on_time: Decimal = Decimal("0")
    penalty_late: Decimal = Decimal("0")

    def check_achieved(self, completed_task_ids: Set[str]) -> bool:
        """Check if milestone is achieved."""
        if all(tid in completed_task_ids for tid in self.required_task_ids):
            self.achieved_date = datetime.now(timezone.utc)

            if self.target_date:
                if self.achieved_date <= self.target_date:
                    self.status = MilestoneStatus.ACHIEVED
                else:
                    self.status = MilestoneStatus.MISSED
            else:
                self.status = MilestoneStatus.ACHIEVED

            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "required_task_ids": self.required_task_ids,
            "deliverables": self.deliverables,
            "target_date": self.target_date.isoformat() if self.target_date else None,
            "achieved_date": self.achieved_date.isoformat() if self.achieved_date else None,
            "status": self.status.value,
            "bonus_on_time": str(self.bonus_on_time),
            "penalty_late": str(self.penalty_late),
        }


@dataclass
class ResourceRequirement:
    """Resources required for a task."""
    capability: str
    quantity: int = 1
    min_reputation: float = 0.5
    min_stake: Decimal = Decimal("0")


@dataclass
class Risk:
    """A project risk."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""

    # Assessment
    probability: float = 0.5  # 0-1
    impact: float = 0.5       # 0-1

    # Mitigation
    mitigation_strategy: str = ""
    contingency_plan: str = ""

    # Status
    status: str = "identified"  # identified, mitigated, occurred, closed
    occurred_at: Optional[datetime] = None

    @property
    def risk_score(self) -> float:
        """Calculate risk score (probability * impact)."""
        return self.probability * self.impact

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "probability": self.probability,
            "impact": self.impact,
            "risk_score": self.risk_score,
            "mitigation_strategy": self.mitigation_strategy,
            "status": self.status,
        }


@dataclass
class WorkBreakdownItem:
    """An item in the Work Breakdown Structure."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    code: str = ""  # WBS code like "1.2.3"
    name: str = ""
    description: str = ""

    # Hierarchy
    parent_id: Optional[str] = None
    children_ids: List[str] = field(default_factory=list)
    level: int = 0

    # If this is a work package (leaf node)
    is_work_package: bool = False
    task_ids: List[str] = field(default_factory=list)

    # Estimates
    estimated_effort_hours: float = 0.0
    estimated_cost: Decimal = Decimal("0")

    # Actuals
    actual_effort_hours: float = 0.0
    actual_cost: Decimal = Decimal("0")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "code": self.code,
            "name": self.name,
            "description": self.description,
            "parent_id": self.parent_id,
            "children_ids": self.children_ids,
            "level": self.level,
            "is_work_package": self.is_work_package,
            "task_ids": self.task_ids,
            "estimated_effort_hours": self.estimated_effort_hours,
            "estimated_cost": str(self.estimated_cost),
            "actual_effort_hours": self.actual_effort_hours,
            "actual_cost": str(self.actual_cost),
        }


@dataclass
class Project:
    """
    A complex project managed by the swarm.

    Projects are broken down into:
    - Work Breakdown Structure (WBS)
    - Tasks with dependencies
    - Milestones
    - Resources
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    goal: str = ""

    # Ownership
    owner_id: str = ""  # Host/designer
    swarm_id: Optional[str] = None

    # Status
    status: ProjectStatus = ProjectStatus.DRAFT
    progress: float = 0.0  # 0-100

    # Work Breakdown Structure
    wbs_items: Dict[str, WorkBreakdownItem] = field(default_factory=dict)
    wbs_root_id: Optional[str] = None

    # Tasks
    task_ids: List[str] = field(default_factory=list)
    dependencies: List[TaskDependency] = field(default_factory=list)

    # Milestones
    milestones: Dict[str, Milestone] = field(default_factory=dict)

    # Risks
    risks: Dict[str, Risk] = field(default_factory=dict)

    # Budget
    budget: Decimal = Decimal("0")
    spent: Decimal = Decimal("0")

    # Timeline
    planned_start: Optional[datetime] = None
    planned_end: Optional[datetime] = None
    actual_start: Optional[datetime] = None
    actual_end: Optional[datetime] = None

    # Team
    team_agent_ids: Set[str] = field(default_factory=set)

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: List[str] = field(default_factory=list)

    def update_progress(self, completed_task_ids: Set[str]):
        """Update project progress based on completed tasks."""
        if not self.task_ids:
            return

        completed = len([t for t in self.task_ids if t in completed_task_ids])
        self.progress = (completed / len(self.task_ids)) * 100
        self.updated_at = datetime.now(timezone.utc)

        # Check milestones
        for milestone in self.milestones.values():
            if milestone.status == MilestoneStatus.PENDING:
                milestone.check_achieved(completed_task_ids)

    def get_critical_path(self) -> List[str]:
        """
        Calculate the critical path through tasks.

        Uses topological sort with longest path.
        """
        if not self.task_ids or not self.dependencies:
            return self.task_ids

        # Build adjacency list
        graph: Dict[str, List[str]] = defaultdict(list)
        for dep in self.dependencies:
            graph[dep.from_task_id].append(dep.to_task_id)

        # Find tasks with no predecessors
        has_predecessor = set()
        for dep in self.dependencies:
            has_predecessor.add(dep.to_task_id)

        start_tasks = [t for t in self.task_ids if t not in has_predecessor]

        # BFS to find longest path
        distances: Dict[str, int] = {t: 0 for t in self.task_ids}
        predecessors: Dict[str, Optional[str]] = {t: None for t in self.task_ids}

        queue = list(start_tasks)
        visited = set()

        while queue:
            task_id = queue.pop(0)
            if task_id in visited:
                continue
            visited.add(task_id)

            for next_task in graph[task_id]:
                new_dist = distances[task_id] + 1
                if new_dist > distances[next_task]:
                    distances[next_task] = new_dist
                    predecessors[next_task] = task_id
                queue.append(next_task)

        # Find task with longest path
        end_task = max(distances.keys(), key=lambda t: distances[t])

        # Reconstruct path
        path = []
        current = end_task
        while current:
            path.append(current)
            current = predecessors[current]

        return list(reversed(path))

    def add_wbs_item(
        self,
        name: str,
        parent_id: Optional[str] = None,
        description: str = "",
        is_work_package: bool = False,
    ) -> WorkBreakdownItem:
        """Add an item to the WBS."""
        # Calculate code and level
        if parent_id and parent_id in self.wbs_items:
            parent = self.wbs_items[parent_id]
            level = parent.level + 1
            sibling_count = len(parent.children_ids) + 1
            code = f"{parent.code}.{sibling_count}"
        else:
            # Root level
            level = 0
            code = str(len([w for w in self.wbs_items.values() if w.level == 0]) + 1)

        item = WorkBreakdownItem(
            code=code,
            name=name,
            description=description,
            parent_id=parent_id,
            level=level,
            is_work_package=is_work_package,
        )

        self.wbs_items[item.id] = item

        if parent_id and parent_id in self.wbs_items:
            self.wbs_items[parent_id].children_ids.append(item.id)

        if level == 0 and not self.wbs_root_id:
            self.wbs_root_id = item.id

        return item

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "goal": self.goal,
            "owner_id": self.owner_id,
            "swarm_id": self.swarm_id,
            "status": self.status.value,
            "progress": self.progress,
            "task_count": len(self.task_ids),
            "milestone_count": len(self.milestones),
            "risk_count": len(self.risks),
            "budget": str(self.budget),
            "spent": str(self.spent),
            "planned_start": self.planned_start.isoformat() if self.planned_start else None,
            "planned_end": self.planned_end.isoformat() if self.planned_end else None,
            "actual_start": self.actual_start.isoformat() if self.actual_start else None,
            "actual_end": self.actual_end.isoformat() if self.actual_end else None,
            "team_size": len(self.team_agent_ids),
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class TaskDecomposer:
    """
    Decomposes complex tasks into subtasks.

    Uses patterns:
    - Goal decomposition (what -> how)
    - Temporal decomposition (when)
    - Resource decomposition (who)
    """

    def __init__(self):
        # Decomposition rules
        self.rules: Dict[str, List[Dict[str, Any]]] = {
            "research": [
                {"type": "search", "capabilities": ["search"], "effort_pct": 0.3},
                {"type": "analyze", "capabilities": ["analyze"], "effort_pct": 0.4},
                {"type": "summarize", "capabilities": ["summarize"], "effort_pct": 0.3},
            ],
            "develop": [
                {"type": "design", "capabilities": ["design"], "effort_pct": 0.2},
                {"type": "code", "capabilities": ["code"], "effort_pct": 0.5},
                {"type": "test", "capabilities": ["test"], "effort_pct": 0.2},
                {"type": "document", "capabilities": ["write"], "effort_pct": 0.1},
            ],
            "deploy": [
                {"type": "build", "capabilities": ["build"], "effort_pct": 0.3},
                {"type": "test", "capabilities": ["test"], "effort_pct": 0.3},
                {"type": "deploy", "capabilities": ["deploy"], "effort_pct": 0.3},
                {"type": "verify", "capabilities": ["verify"], "effort_pct": 0.1},
            ],
            "analyze": [
                {"type": "gather", "capabilities": ["search"], "effort_pct": 0.3},
                {"type": "process", "capabilities": ["analyze"], "effort_pct": 0.4},
                {"type": "report", "capabilities": ["write"], "effort_pct": 0.3},
            ],
        }

    def decompose(
        self,
        task_type: str,
        goal: str,
        budget: Decimal,
        deadline: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Decompose a high-level task into subtasks.

        Returns list of subtask specifications.
        """
        rules = self.rules.get(task_type, [])
        if not rules:
            # No decomposition rule, return single task
            return [{
                "type": task_type,
                "description": goal,
                "bounty": budget,
                "capabilities": [],
                "deadline": deadline,
            }]

        subtasks = []
        for i, rule in enumerate(rules):
            bounty = budget * Decimal(str(rule["effort_pct"]))

            subtask_deadline = None
            if deadline:
                # Distribute deadline proportionally
                total_effort = sum(r["effort_pct"] for r in rules[:i+1])
                subtask_deadline = datetime.now(timezone.utc) + (
                    (deadline - datetime.now(timezone.utc)) * total_effort
                )

            subtasks.append({
                "type": rule["type"],
                "description": f"{rule['type'].title()}: {goal}",
                "bounty": bounty,
                "capabilities": rule["capabilities"],
                "deadline": subtask_deadline,
                "order": i,
            })

        return subtasks

    def create_dependencies(
        self,
        subtasks: List[Dict[str, Any]],
    ) -> List[TaskDependency]:
        """Create dependencies between subtasks (sequential by default)."""
        dependencies = []

        for i in range(1, len(subtasks)):
            dependencies.append(TaskDependency(
                from_task_id=subtasks[i-1].get("id", str(i-1)),
                to_task_id=subtasks[i].get("id", str(i)),
                type=TaskDependencyType.FINISH_TO_START,
            ))

        return dependencies


class ProjectManager:
    """
    Manages projects for the swarm.

    Features:
    - Project lifecycle management
    - Resource allocation
    - Progress tracking
    - Risk management
    """

    def __init__(self):
        self.projects: Dict[str, Project] = {}
        self.decomposer = TaskDecomposer()

        # Project templates
        self.templates: Dict[str, Dict[str, Any]] = {}

    def create_project(
        self,
        name: str,
        description: str,
        goal: str,
        owner_id: str,
        budget: Decimal,
        deadline: Optional[datetime] = None,
    ) -> Project:
        """Create a new project."""
        project = Project(
            name=name,
            description=description,
            goal=goal,
            owner_id=owner_id,
            budget=budget,
            planned_end=deadline,
        )

        # Add root WBS item
        project.add_wbs_item(name)

        self.projects[project.id] = project

        logger.info(f"Created project: {name} ({project.id})")
        return project

    def decompose_project(
        self,
        project_id: str,
        high_level_tasks: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Decompose project into detailed tasks.

        Takes high-level task descriptions and breaks them down.
        """
        project = self.projects.get(project_id)
        if not project:
            return []

        all_subtasks = []
        budget_per_task = project.budget / len(high_level_tasks) if high_level_tasks else Decimal("0")

        for hl_task in high_level_tasks:
            task_type = hl_task.get("type", "generic")
            goal = hl_task.get("goal", "")
            task_budget = Decimal(str(hl_task.get("budget", budget_per_task)))

            subtasks = self.decomposer.decompose(
                task_type=task_type,
                goal=goal,
                budget=task_budget,
                deadline=project.planned_end,
            )

            # Add WBS entry for this task
            wbs_item = project.add_wbs_item(
                name=goal,
                parent_id=project.wbs_root_id,
                is_work_package=True,
            )

            for subtask in subtasks:
                subtask["wbs_id"] = wbs_item.id
                subtask["parent_goal"] = goal
                all_subtasks.append(subtask)

        return all_subtasks

    def add_milestone(
        self,
        project_id: str,
        name: str,
        required_task_ids: List[str],
        target_date: Optional[datetime] = None,
        bonus: Decimal = Decimal("0"),
        penalty: Decimal = Decimal("0"),
    ) -> Optional[Milestone]:
        """Add a milestone to a project."""
        project = self.projects.get(project_id)
        if not project:
            return None

        milestone = Milestone(
            name=name,
            required_task_ids=required_task_ids,
            target_date=target_date,
            bonus_on_time=bonus,
            penalty_late=penalty,
        )

        project.milestones[milestone.id] = milestone
        return milestone

    def add_risk(
        self,
        project_id: str,
        name: str,
        description: str,
        probability: float,
        impact: float,
        mitigation: str = "",
    ) -> Optional[Risk]:
        """Add a risk to a project."""
        project = self.projects.get(project_id)
        if not project:
            return None

        risk = Risk(
            name=name,
            description=description,
            probability=probability,
            impact=impact,
            mitigation_strategy=mitigation,
        )

        project.risks[risk.id] = risk
        return risk

    def start_project(self, project_id: str) -> bool:
        """Start a project."""
        project = self.projects.get(project_id)
        if not project:
            return False

        project.status = ProjectStatus.ACTIVE
        project.actual_start = datetime.now(timezone.utc)
        project.updated_at = datetime.now(timezone.utc)

        logger.info(f"Started project: {project.name}")
        return True

    def update_project_progress(
        self,
        project_id: str,
        completed_task_ids: Set[str],
        spent: Decimal = Decimal("0"),
    ):
        """Update project progress."""
        project = self.projects.get(project_id)
        if not project:
            return

        project.update_progress(completed_task_ids)
        project.spent += spent

        # Check for completion
        if project.progress >= 100:
            project.status = ProjectStatus.COMPLETED
            project.actual_end = datetime.now(timezone.utc)
            logger.info(f"Project completed: {project.name}")

    def get_project_dashboard(self, project_id: str) -> Dict[str, Any]:
        """Get a dashboard view of project status."""
        project = self.projects.get(project_id)
        if not project:
            return {}

        # Count milestones by status
        milestone_counts = defaultdict(int)
        for m in project.milestones.values():
            milestone_counts[m.status.value] += 1

        # Count risks by score
        high_risks = len([r for r in project.risks.values() if r.risk_score > 0.5])

        # Budget status
        budget_used_pct = (project.spent / project.budget * 100) if project.budget > 0 else 0

        # Schedule status
        schedule_status = "on_track"
        if project.planned_end and project.actual_start:
            elapsed = (datetime.now(timezone.utc) - project.actual_start).total_seconds()
            planned = (project.planned_end - project.actual_start).total_seconds()
            expected_progress = (elapsed / planned) * 100 if planned > 0 else 0

            if project.progress < expected_progress - 10:
                schedule_status = "behind"
            elif project.progress > expected_progress + 10:
                schedule_status = "ahead"

        return {
            "project": project.to_dict(),
            "metrics": {
                "progress": project.progress,
                "budget_used_pct": float(budget_used_pct),
                "schedule_status": schedule_status,
                "high_risks": high_risks,
                "milestones": dict(milestone_counts),
            },
            "critical_path": project.get_critical_path(),
            "wbs_summary": {
                "total_items": len(project.wbs_items),
                "work_packages": len([w for w in project.wbs_items.values() if w.is_work_package]),
            },
        }

    def list_projects(self, status: Optional[ProjectStatus] = None) -> List[Dict[str, Any]]:
        """List all projects, optionally filtered by status."""
        projects = list(self.projects.values())
        if status:
            projects = [p for p in projects if p.status == status]

        return [p.to_dict() for p in projects]


# Global instance
_project_manager = ProjectManager()


def get_project_manager() -> ProjectManager:
    return _project_manager
