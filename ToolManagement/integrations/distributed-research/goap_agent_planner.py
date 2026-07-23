#!/usr/bin/env python3
"""
GOAP META-PLANNER FOR AUTONOMOUS AGENT DEVELOPMENT

Goal-Oriented Action Planning (GOAP) system that plans how agents
should learn, evolve, and develop themselves.

This is the metacognitive layer - the system that thinks about thinking,
plans about planning, and develops the developers.

Architecture:
┌─────────────────────────────────────────────────────────────────────┐
│                        META-COGNITIVE LAYER                          │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                      WORLD MODEL                                 ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        ││
│  │  │ Agent    │  │ Code     │  │ Knowledge│  │ Capability│        ││
│  │  │ States   │  │ Patterns │  │ Graph    │  │ Map      │        ││
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘        ││
│  └─────────────────────────────────────────────────────────────────┘│
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                      GOAL HIERARCHY                              ││
│  │  ┌──────────────────────────────────────────────────────────┐   ││
│  │  │ ULTIMATE: Autonomous self-improving coding system        │   ││
│  │  ├──────────────────────────────────────────────────────────┤   ││
│  │  │ STRATEGIC: Learn patterns → Generate code → Verify → Fix │   ││
│  │  ├──────────────────────────────────────────────────────────┤   ││
│  │  │ TACTICAL: Read file, Extract pattern, Write code, Test   │   ││
│  │  ├──────────────────────────────────────────────────────────┤   ││
│  │  │ OPERATIONAL: Execute tool, Parse result, Update state    │   ││
│  │  └──────────────────────────────────────────────────────────┘   ││
│  └─────────────────────────────────────────────────────────────────┘│
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                      GOAP PLANNER                                ││
│  │  Actions → Preconditions → Effects → Cost → Plan                ││
│  └─────────────────────────────────────────────────────────────────┘│
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                      EXECUTION ENGINE                            ││
│  │  Plan → Execute → Observe → Learn → Replan                      ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘

What the swarm ACTUALLY learns:
- Syntactic patterns (AST structures)
- Function signatures and types
- Class hierarchies
- Import dependencies
- Naming conventions

What it SHOULD learn (this system enables):
- Semantic code understanding
- Design pattern recognition
- Problem decomposition strategies
- Error recovery patterns
- Self-improvement trajectories
- Meta-learning: learning HOW to learn better
"""

from __future__ import annotations
import json
import heapq
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Callable, Dict, FrozenSet, List, Optional,
    Set, Tuple, TypeVar, Generic, Iterator
)
from datetime import datetime
import copy

# =============================================================================
# WORLD STATE REPRESENTATION
# =============================================================================

class StateKey(Enum):
    """Keys for world state properties."""
    # Agent capabilities
    HAS_FILE_READ = auto()
    HAS_FILE_WRITE = auto()
    HAS_CODE_EXEC = auto()
    HAS_LLM_ACCESS = auto()
    HAS_KNOWLEDGE_GRAPH = auto()

    # Learning state
    PATTERNS_LEARNED = auto()
    FUNCTIONS_KNOWN = auto()
    CLASSES_KNOWN = auto()
    IMPORTS_KNOWN = auto()

    # Code understanding
    UNDERSTANDS_SYNTAX = auto()
    UNDERSTANDS_SEMANTICS = auto()
    UNDERSTANDS_PATTERNS = auto()
    UNDERSTANDS_ARCHITECTURE = auto()

    # Generation capabilities
    CAN_GENERATE_FUNCTIONS = auto()
    CAN_GENERATE_CLASSES = auto()
    CAN_GENERATE_MODULES = auto()
    CAN_GENERATE_SYSTEMS = auto()

    # Verification capabilities
    CAN_SYNTAX_CHECK = auto()
    CAN_TYPE_CHECK = auto()
    CAN_RUN_TESTS = auto()
    CAN_VERIFY_BEHAVIOR = auto()

    # Self-improvement
    CAN_IDENTIFY_GAPS = auto()
    CAN_IMPROVE_SELF = auto()
    CAN_TEACH_OTHERS = auto()

    # Meta capabilities
    HAS_GOAL_STACK = auto()
    CAN_PLAN = auto()
    CAN_REPLAN = auto()
    CAN_REFLECT = auto()


@dataclass(frozen=True)
class WorldState:
    """
    Immutable representation of the world state.
    Used for GOAP planning - states are compared to find paths.
    """
    properties: FrozenSet[Tuple[StateKey, Any]]

    @classmethod
    def empty(cls) -> 'WorldState':
        return cls(frozenset())

    @classmethod
    def from_dict(cls, d: Dict[StateKey, Any]) -> 'WorldState':
        return cls(frozenset(d.items()))

    def to_dict(self) -> Dict[StateKey, Any]:
        return dict(self.properties)

    def get(self, key: StateKey, default: Any = None) -> Any:
        for k, v in self.properties:
            if k == key:
                return v
        return default

    def with_update(self, updates: Dict[StateKey, Any]) -> 'WorldState':
        """Return new state with updates applied."""
        current = dict(self.properties)
        current.update(updates)
        return WorldState(frozenset(current.items()))

    def satisfies(self, requirements: Dict[StateKey, Any]) -> bool:
        """Check if this state satisfies all requirements."""
        current = self.to_dict()
        for key, required_value in requirements.items():
            if key not in current:
                return False
            actual = current[key]
            if isinstance(required_value, (int, float)):
                if actual < required_value:
                    return False
            elif actual != required_value:
                return False
        return True

    def distance_to(self, goal: Dict[StateKey, Any]) -> int:
        """Heuristic distance to goal state."""
        current = self.to_dict()
        distance = 0
        for key, goal_value in goal.items():
            if key not in current:
                distance += 10
            else:
                actual = current[key]
                if isinstance(goal_value, (int, float)):
                    distance += max(0, goal_value - actual)
                elif actual != goal_value:
                    distance += 5
        return distance

    def __hash__(self):
        return hash(self.properties)

    def __eq__(self, other):
        if not isinstance(other, WorldState):
            return False
        return self.properties == other.properties


# =============================================================================
# GOAP ACTIONS
# =============================================================================

@dataclass
class GOAPAction:
    """
    An action in the GOAP system.

    Each action has:
    - Preconditions: What must be true to execute
    - Effects: What becomes true after execution
    - Cost: How expensive the action is
    - Execute: The actual implementation
    """
    name: str
    preconditions: Dict[StateKey, Any]
    effects: Dict[StateKey, Any]
    cost: float = 1.0
    execute: Optional[Callable[[WorldState], Tuple[bool, WorldState]]] = None
    description: str = ""

    def is_valid(self, state: WorldState) -> bool:
        """Check if action can be executed in given state."""
        return state.satisfies(self.preconditions)

    def apply(self, state: WorldState) -> WorldState:
        """Apply action effects to state (for planning)."""
        return state.with_update(self.effects)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, GOAPAction) and self.name == other.name


# =============================================================================
# GOAP PLANNER - A* SEARCH
# =============================================================================

@dataclass(order=True)
class PlanNode:
    """Node in the planning search tree."""
    f_score: float
    g_score: float = field(compare=False)
    state: WorldState = field(compare=False)
    action: Optional[GOAPAction] = field(compare=False)
    parent: Optional['PlanNode'] = field(compare=False)


class GOAPPlanner:
    """
    Goal-Oriented Action Planner using A* search.

    Finds the optimal sequence of actions to transform
    the current world state into a goal state.
    """

    def __init__(self, actions: List[GOAPAction], max_iterations: int = 10000):
        self.actions = actions
        self.max_iterations = max_iterations

    def plan(self, start: WorldState,
             goal: Dict[StateKey, Any]) -> Optional[List[GOAPAction]]:
        """
        Find a plan from start state to goal state.
        Returns list of actions or None if no plan found.
        """
        # Priority queue: (f_score, node)
        open_set: List[PlanNode] = []
        closed_set: Set[WorldState] = set()

        start_node = PlanNode(
            f_score=start.distance_to(goal),
            g_score=0,
            state=start,
            action=None,
            parent=None
        )
        heapq.heappush(open_set, start_node)

        iterations = 0

        while open_set and iterations < self.max_iterations:
            iterations += 1

            current = heapq.heappop(open_set)

            # Goal check
            if current.state.satisfies(goal):
                return self._reconstruct_plan(current)

            if current.state in closed_set:
                continue
            closed_set.add(current.state)

            # Expand neighbors
            for action in self.actions:
                if not action.is_valid(current.state):
                    continue

                new_state = action.apply(current.state)
                if new_state in closed_set:
                    continue

                g_score = current.g_score + action.cost
                h_score = new_state.distance_to(goal)
                f_score = g_score + h_score

                new_node = PlanNode(
                    f_score=f_score,
                    g_score=g_score,
                    state=new_state,
                    action=action,
                    parent=current
                )
                heapq.heappush(open_set, new_node)

        return None  # No plan found

    def _reconstruct_plan(self, node: PlanNode) -> List[GOAPAction]:
        """Reconstruct plan from goal node to start."""
        plan = []
        current = node
        while current.action is not None:
            plan.append(current.action)
            current = current.parent
        plan.reverse()
        return plan


# =============================================================================
# HIERARCHICAL GOAL SYSTEM
# =============================================================================

class GoalPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class Goal:
    """A goal in the hierarchical goal system."""
    id: str
    name: str
    description: str
    conditions: Dict[StateKey, Any]
    priority: GoalPriority = GoalPriority.MEDIUM
    parent_id: Optional[str] = None
    subgoals: List[str] = field(default_factory=list)
    deadline: Optional[float] = None

    # Dynamic properties
    progress: float = 0.0
    attempts: int = 0
    last_plan: Optional[List[str]] = None

    def is_satisfied(self, state: WorldState) -> bool:
        return state.satisfies(self.conditions)


class GoalHierarchy:
    """
    Manages hierarchical goals for the agent.

    Goals form a tree:
    - Ultimate goals (long-term vision)
    - Strategic goals (what to achieve)
    - Tactical goals (how to achieve)
    - Operational goals (immediate actions)
    """

    def __init__(self):
        self.goals: Dict[str, Goal] = {}
        self.active_goal: Optional[str] = None
        self.goal_stack: List[str] = []

    def add_goal(self, goal: Goal):
        """Add a goal to the hierarchy."""
        self.goals[goal.id] = goal
        if goal.parent_id and goal.parent_id in self.goals:
            self.goals[goal.parent_id].subgoals.append(goal.id)

    def get_active_goal(self) -> Optional[Goal]:
        """Get the current active goal."""
        if self.active_goal:
            return self.goals.get(self.active_goal)
        return None

    def select_next_goal(self, state: WorldState) -> Optional[Goal]:
        """Select the next goal to pursue based on state and priorities."""
        candidates = []

        for goal in self.goals.values():
            if goal.is_satisfied(state):
                continue

            # Check if parent is satisfied or doesn't exist
            if goal.parent_id:
                parent = self.goals.get(goal.parent_id)
                if parent and not parent.is_satisfied(state):
                    continue

            # Check if all subgoals are satisfied
            subgoals_satisfied = all(
                self.goals[sg].is_satisfied(state)
                for sg in goal.subgoals
                if sg in self.goals
            )

            if not subgoals_satisfied:
                continue

            candidates.append(goal)

        if not candidates:
            return None

        # Sort by priority, then by progress (prefer nearly complete)
        candidates.sort(key=lambda g: (g.priority.value, -g.progress))

        selected = candidates[0]
        self.active_goal = selected.id
        self.goal_stack.append(selected.id)

        return selected

    def mark_completed(self, goal_id: str):
        """Mark a goal as completed."""
        if goal_id in self.goals:
            self.goals[goal_id].progress = 1.0
        if self.active_goal == goal_id:
            self.active_goal = None
        if goal_id in self.goal_stack:
            self.goal_stack.remove(goal_id)

    def get_goal_tree(self) -> Dict:
        """Get hierarchical representation of goals."""
        def build_tree(goal_id: str) -> Dict:
            goal = self.goals[goal_id]
            return {
                'id': goal.id,
                'name': goal.name,
                'priority': goal.priority.name,
                'progress': goal.progress,
                'satisfied': goal.is_satisfied if hasattr(goal, '_last_state') else None,
                'subgoals': [build_tree(sg) for sg in goal.subgoals if sg in self.goals]
            }

        roots = [g for g in self.goals.values() if not g.parent_id]
        return {'goals': [build_tree(r.id) for r in roots]}


# =============================================================================
# AGENT DEVELOPMENT ACTIONS
# =============================================================================

def create_agent_development_actions() -> List[GOAPAction]:
    """Create the action library for agent self-development."""

    return [
        # === LEARNING ACTIONS ===
        GOAPAction(
            name="digest_codebase",
            description="Digest source code files into knowledge graph",
            preconditions={
                StateKey.HAS_FILE_READ: True,
                StateKey.HAS_KNOWLEDGE_GRAPH: True
            },
            effects={
                StateKey.PATTERNS_LEARNED: 100,
                StateKey.UNDERSTANDS_SYNTAX: True
            },
            cost=5.0
        ),

        GOAPAction(
            name="extract_function_patterns",
            description="Extract function signature patterns from knowledge",
            preconditions={
                StateKey.PATTERNS_LEARNED: 50,
                StateKey.UNDERSTANDS_SYNTAX: True
            },
            effects={
                StateKey.FUNCTIONS_KNOWN: 100,
                StateKey.CAN_GENERATE_FUNCTIONS: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="extract_class_patterns",
            description="Extract class structure patterns from knowledge",
            preconditions={
                StateKey.PATTERNS_LEARNED: 50,
                StateKey.UNDERSTANDS_SYNTAX: True
            },
            effects={
                StateKey.CLASSES_KNOWN: 50,
                StateKey.CAN_GENERATE_CLASSES: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="learn_design_patterns",
            description="Identify design patterns in codebase",
            preconditions={
                StateKey.CLASSES_KNOWN: 30,
                StateKey.FUNCTIONS_KNOWN: 50
            },
            effects={
                StateKey.UNDERSTANDS_PATTERNS: True
            },
            cost=5.0
        ),

        GOAPAction(
            name="extract_import_patterns",
            description="Extract import patterns from code",
            preconditions={
                StateKey.PATTERNS_LEARNED: 30,
                StateKey.UNDERSTANDS_SYNTAX: True
            },
            effects={
                StateKey.IMPORTS_KNOWN: 100
            },
            cost=2.0
        ),

        GOAPAction(
            name="learn_architecture",
            description="Understand system architecture from code structure",
            preconditions={
                StateKey.UNDERSTANDS_PATTERNS: True,
                StateKey.IMPORTS_KNOWN: 50
            },
            effects={
                StateKey.UNDERSTANDS_ARCHITECTURE: True,
                StateKey.CAN_GENERATE_MODULES: True
            },
            cost=8.0
        ),

        # === GENERATION ACTIONS ===
        GOAPAction(
            name="enable_function_generation",
            description="Enable function code generation",
            preconditions={
                StateKey.FUNCTIONS_KNOWN: 20,
                StateKey.HAS_LLM_ACCESS: True
            },
            effects={
                StateKey.CAN_GENERATE_FUNCTIONS: True
            },
            cost=1.0
        ),

        GOAPAction(
            name="enable_class_generation",
            description="Enable class code generation",
            preconditions={
                StateKey.CLASSES_KNOWN: 10,
                StateKey.CAN_GENERATE_FUNCTIONS: True
            },
            effects={
                StateKey.CAN_GENERATE_CLASSES: True
            },
            cost=2.0
        ),

        GOAPAction(
            name="enable_module_generation",
            description="Enable full module generation",
            preconditions={
                StateKey.CAN_GENERATE_CLASSES: True,
                StateKey.UNDERSTANDS_ARCHITECTURE: True
            },
            effects={
                StateKey.CAN_GENERATE_MODULES: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="enable_system_generation",
            description="Enable multi-module system generation",
            preconditions={
                StateKey.CAN_GENERATE_MODULES: True,
                StateKey.UNDERSTANDS_ARCHITECTURE: True,
                StateKey.CAN_VERIFY_BEHAVIOR: True
            },
            effects={
                StateKey.CAN_GENERATE_SYSTEMS: True
            },
            cost=5.0
        ),

        # === VERIFICATION ACTIONS ===
        GOAPAction(
            name="enable_syntax_checking",
            description="Enable Python syntax validation",
            preconditions={
                StateKey.HAS_CODE_EXEC: True
            },
            effects={
                StateKey.CAN_SYNTAX_CHECK: True
            },
            cost=1.0
        ),

        GOAPAction(
            name="enable_type_checking",
            description="Enable type hint validation",
            preconditions={
                StateKey.CAN_SYNTAX_CHECK: True,
                StateKey.HAS_CODE_EXEC: True
            },
            effects={
                StateKey.CAN_TYPE_CHECK: True
            },
            cost=2.0
        ),

        GOAPAction(
            name="enable_test_running",
            description="Enable automated test execution",
            preconditions={
                StateKey.CAN_SYNTAX_CHECK: True,
                StateKey.HAS_CODE_EXEC: True,
                StateKey.HAS_FILE_WRITE: True
            },
            effects={
                StateKey.CAN_RUN_TESTS: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="enable_behavior_verification",
            description="Enable runtime behavior verification",
            preconditions={
                StateKey.CAN_RUN_TESTS: True,
                StateKey.CAN_TYPE_CHECK: True
            },
            effects={
                StateKey.CAN_VERIFY_BEHAVIOR: True
            },
            cost=4.0
        ),

        # === SELF-IMPROVEMENT ACTIONS ===
        GOAPAction(
            name="enable_gap_identification",
            description="Enable identification of knowledge gaps",
            preconditions={
                StateKey.UNDERSTANDS_PATTERNS: True,
                StateKey.CAN_REFLECT: True
            },
            effects={
                StateKey.CAN_IDENTIFY_GAPS: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="enable_self_improvement",
            description="Enable self-modification of code",
            preconditions={
                StateKey.CAN_IDENTIFY_GAPS: True,
                StateKey.CAN_GENERATE_MODULES: True,
                StateKey.CAN_VERIFY_BEHAVIOR: True
            },
            effects={
                StateKey.CAN_IMPROVE_SELF: True
            },
            cost=10.0
        ),

        GOAPAction(
            name="enable_teaching",
            description="Enable teaching other agents",
            preconditions={
                StateKey.CAN_IMPROVE_SELF: True,
                StateKey.UNDERSTANDS_ARCHITECTURE: True
            },
            effects={
                StateKey.CAN_TEACH_OTHERS: True
            },
            cost=5.0
        ),

        # === META ACTIONS ===
        GOAPAction(
            name="enable_planning",
            description="Enable goal-oriented planning",
            preconditions={
                StateKey.HAS_GOAL_STACK: True
            },
            effects={
                StateKey.CAN_PLAN: True
            },
            cost=2.0
        ),

        GOAPAction(
            name="enable_replanning",
            description="Enable dynamic replanning on failure",
            preconditions={
                StateKey.CAN_PLAN: True,
                StateKey.CAN_REFLECT: True
            },
            effects={
                StateKey.CAN_REPLAN: True
            },
            cost=3.0
        ),

        GOAPAction(
            name="enable_reflection",
            description="Enable self-reflection on actions",
            preconditions={
                StateKey.HAS_LLM_ACCESS: True
            },
            effects={
                StateKey.CAN_REFLECT: True
            },
            cost=2.0
        ),

        # === BOOTSTRAP ACTIONS ===
        GOAPAction(
            name="initialize_file_access",
            description="Initialize file system access",
            preconditions={},
            effects={
                StateKey.HAS_FILE_READ: True,
                StateKey.HAS_FILE_WRITE: True
            },
            cost=1.0
        ),

        GOAPAction(
            name="initialize_code_execution",
            description="Initialize code execution capability",
            preconditions={},
            effects={
                StateKey.HAS_CODE_EXEC: True
            },
            cost=1.0
        ),

        GOAPAction(
            name="initialize_llm_access",
            description="Initialize LLM API access",
            preconditions={},
            effects={
                StateKey.HAS_LLM_ACCESS: True
            },
            cost=1.0
        ),

        GOAPAction(
            name="initialize_knowledge_graph",
            description="Initialize knowledge graph storage",
            preconditions={
                StateKey.HAS_FILE_WRITE: True
            },
            effects={
                StateKey.HAS_KNOWLEDGE_GRAPH: True
            },
            cost=2.0
        ),

        GOAPAction(
            name="initialize_goal_system",
            description="Initialize hierarchical goal system",
            preconditions={},
            effects={
                StateKey.HAS_GOAL_STACK: True
            },
            cost=1.0
        ),
    ]


# =============================================================================
# META-PLANNER: PLANS FOR AGENT DEVELOPMENT
# =============================================================================

class AgentDevelopmentPlanner:
    """
    High-level planner for agent self-development.

    This is the metacognitive layer that:
    1. Assesses current agent capabilities
    2. Identifies development goals
    3. Plans sequences of actions to achieve goals
    4. Monitors and adjusts plans during execution
    """

    def __init__(self):
        self.actions = create_agent_development_actions()
        self.planner = GOAPPlanner(self.actions)
        self.goal_hierarchy = GoalHierarchy()
        self.current_state = WorldState.empty()

        # Initialize goal hierarchy
        self._initialize_goals()

    def _initialize_goals(self):
        """Set up the hierarchical goal structure."""

        # Ultimate goal
        self.goal_hierarchy.add_goal(Goal(
            id="ultimate",
            name="Autonomous Self-Improving Coding System",
            description="A system that can learn, code, verify, and improve itself",
            conditions={
                StateKey.CAN_IMPROVE_SELF: True,
                StateKey.CAN_TEACH_OTHERS: True,
                StateKey.CAN_GENERATE_SYSTEMS: True
            },
            priority=GoalPriority.BACKGROUND
        ))

        # Strategic goals
        self.goal_hierarchy.add_goal(Goal(
            id="learn_codebase",
            name="Learn Codebase Patterns",
            description="Digest and understand the codebase",
            conditions={
                StateKey.UNDERSTANDS_PATTERNS: True,
                StateKey.UNDERSTANDS_ARCHITECTURE: True
            },
            priority=GoalPriority.HIGH,
            parent_id="ultimate"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="generate_code",
            name="Code Generation Capability",
            description="Ability to generate correct code",
            conditions={
                StateKey.CAN_GENERATE_MODULES: True
            },
            priority=GoalPriority.HIGH,
            parent_id="ultimate"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="verify_code",
            name="Code Verification Capability",
            description="Ability to verify code correctness",
            conditions={
                StateKey.CAN_VERIFY_BEHAVIOR: True
            },
            priority=GoalPriority.HIGH,
            parent_id="ultimate"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="self_improve",
            name="Self-Improvement Capability",
            description="Ability to improve own code",
            conditions={
                StateKey.CAN_IMPROVE_SELF: True
            },
            priority=GoalPriority.MEDIUM,
            parent_id="ultimate"
        ))

        # Tactical goals
        self.goal_hierarchy.add_goal(Goal(
            id="init_systems",
            name="Initialize Core Systems",
            description="Set up basic capabilities",
            conditions={
                StateKey.HAS_FILE_READ: True,
                StateKey.HAS_FILE_WRITE: True,
                StateKey.HAS_CODE_EXEC: True,
                StateKey.HAS_LLM_ACCESS: True,
                StateKey.HAS_KNOWLEDGE_GRAPH: True,
                StateKey.HAS_GOAL_STACK: True
            },
            priority=GoalPriority.CRITICAL,
            parent_id="learn_codebase"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="learn_patterns",
            name="Learn Code Patterns",
            description="Extract patterns from codebase",
            conditions={
                StateKey.PATTERNS_LEARNED: 100,
                StateKey.FUNCTIONS_KNOWN: 100,
                StateKey.CLASSES_KNOWN: 50
            },
            priority=GoalPriority.HIGH,
            parent_id="learn_codebase"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="enable_generation",
            name="Enable Code Generation",
            description="Set up generation capabilities",
            conditions={
                StateKey.CAN_GENERATE_FUNCTIONS: True,
                StateKey.CAN_GENERATE_CLASSES: True
            },
            priority=GoalPriority.HIGH,
            parent_id="generate_code"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="enable_verification",
            name="Enable Verification",
            description="Set up verification capabilities",
            conditions={
                StateKey.CAN_SYNTAX_CHECK: True,
                StateKey.CAN_TYPE_CHECK: True,
                StateKey.CAN_RUN_TESTS: True
            },
            priority=GoalPriority.HIGH,
            parent_id="verify_code"
        ))

        self.goal_hierarchy.add_goal(Goal(
            id="enable_meta",
            name="Enable Metacognition",
            description="Set up reflection and planning",
            conditions={
                StateKey.CAN_PLAN: True,
                StateKey.CAN_REPLAN: True,
                StateKey.CAN_REFLECT: True
            },
            priority=GoalPriority.MEDIUM,
            parent_id="self_improve"
        ))

    def assess_current_state(self, capabilities: Dict[str, bool]) -> WorldState:
        """Assess current agent state from capabilities."""
        state_dict = {}

        # Map capabilities to state keys
        capability_map = {
            'file_read': StateKey.HAS_FILE_READ,
            'file_write': StateKey.HAS_FILE_WRITE,
            'code_exec': StateKey.HAS_CODE_EXEC,
            'llm_access': StateKey.HAS_LLM_ACCESS,
            'knowledge_graph': StateKey.HAS_KNOWLEDGE_GRAPH,
            'patterns_learned': StateKey.PATTERNS_LEARNED,
            'functions_known': StateKey.FUNCTIONS_KNOWN,
            'classes_known': StateKey.CLASSES_KNOWN,
        }

        for cap, key in capability_map.items():
            if cap in capabilities:
                state_dict[key] = capabilities[cap]

        self.current_state = WorldState.from_dict(state_dict)
        return self.current_state

    def get_next_goal(self) -> Optional[Goal]:
        """Get the next goal to pursue."""
        return self.goal_hierarchy.select_next_goal(self.current_state)

    def plan_for_goal(self, goal: Goal) -> Optional[List[GOAPAction]]:
        """Create a plan to achieve a goal."""
        return self.planner.plan(self.current_state, goal.conditions)

    def get_development_plan(self) -> Dict:
        """Get the full development plan from current state to ultimate goal."""
        ultimate = self.goal_hierarchy.goals.get("ultimate")
        if not ultimate:
            return {"error": "No ultimate goal defined"}

        plan = self.planner.plan(self.current_state, ultimate.conditions)

        if not plan:
            return {"error": "No plan found to ultimate goal"}

        return {
            "current_state": {k.name: v for k, v in self.current_state.to_dict().items()},
            "ultimate_goal": ultimate.conditions,
            "plan": [
                {
                    "action": a.name,
                    "description": a.description,
                    "cost": a.cost,
                    "effects": {k.name: v for k, v in a.effects.items()}
                }
                for a in plan
            ],
            "total_cost": sum(a.cost for a in plan),
            "steps": len(plan)
        }

    def explain_plan(self, plan: List[GOAPAction]) -> str:
        """Generate human-readable explanation of plan."""
        lines = ["AGENT DEVELOPMENT PLAN", "=" * 50, ""]

        state = self.current_state
        for i, action in enumerate(plan, 1):
            lines.append(f"Step {i}: {action.name}")
            lines.append(f"  {action.description}")
            lines.append(f"  Cost: {action.cost}")
            lines.append(f"  Enables: {', '.join(k.name for k in action.effects.keys())}")
            lines.append("")
            state = action.apply(state)

        lines.append(f"Total steps: {len(plan)}")
        lines.append(f"Total cost: {sum(a.cost for a in plan)}")

        return "\n".join(lines)


# =============================================================================
# EXECUTION ENGINE
# =============================================================================

class PlanExecutor:
    """
    Executes GOAP plans and handles failures.

    Implements:
    - Step-by-step execution
    - Failure detection
    - Replanning on failure
    - Progress tracking
    """

    def __init__(self, planner: AgentDevelopmentPlanner):
        self.planner = planner
        self.execution_history: List[Dict] = []
        self.current_plan: Optional[List[GOAPAction]] = None
        self.plan_index: int = 0

    def execute_plan(self, plan: List[GOAPAction]) -> bool:
        """Execute a full plan."""
        self.current_plan = plan
        self.plan_index = 0

        for action in plan:
            success = self.execute_action(action)
            if not success:
                return False
            self.plan_index += 1

        return True

    def execute_action(self, action: GOAPAction) -> bool:
        """Execute a single action."""
        record = {
            "action": action.name,
            "started_at": datetime.now().isoformat(),
            "preconditions": {k.name: v for k, v in action.preconditions.items()},
        }

        # Check preconditions
        if not action.is_valid(self.planner.current_state):
            record["status"] = "failed"
            record["error"] = "Preconditions not met"
            self.execution_history.append(record)
            return False

        # Execute if implementation exists
        if action.execute:
            try:
                success, new_state = action.execute(self.planner.current_state)
                if success:
                    self.planner.current_state = new_state
                else:
                    record["status"] = "failed"
                    record["error"] = "Execution returned failure"
                    self.execution_history.append(record)
                    return False
            except Exception as e:
                record["status"] = "failed"
                record["error"] = str(e)
                self.execution_history.append(record)
                return False
        else:
            # Simulate execution by applying effects
            self.planner.current_state = action.apply(self.planner.current_state)

        record["status"] = "success"
        record["completed_at"] = datetime.now().isoformat()
        record["new_state"] = {k.name: v for k, v in self.planner.current_state.to_dict().items()}
        self.execution_history.append(record)

        return True

    def replan(self, goal: Goal) -> Optional[List[GOAPAction]]:
        """Create new plan from current state."""
        return self.planner.plan_for_goal(goal)


# =============================================================================
# MAIN INTERFACE
# =============================================================================

def create_meta_planner() -> AgentDevelopmentPlanner:
    """Create a configured meta-planner."""
    return AgentDevelopmentPlanner()


def plan_agent_development(current_capabilities: Dict[str, Any]) -> Dict:
    """
    Create a development plan for an agent given its current capabilities.

    Returns a structured plan with steps to achieve full autonomy.
    """
    planner = create_meta_planner()
    planner.assess_current_state(current_capabilities)
    return planner.get_development_plan()


def main():
    """Demo the GOAP meta-planner."""
    print("GOAP META-PLANNER FOR AGENT DEVELOPMENT")
    print("=" * 60)
    print()

    # Create planner
    planner = create_meta_planner()

    # Simulate current state (new agent with basic capabilities)
    initial_capabilities = {
        'file_read': True,
        'file_write': True,
        'code_exec': True,
        'llm_access': True,
        'knowledge_graph': False,
        'patterns_learned': 0,
        'functions_known': 0,
        'classes_known': 0,
    }

    state = planner.assess_current_state(initial_capabilities)

    print("CURRENT STATE:")
    for key, value in state.to_dict().items():
        print(f"  {key.name}: {value}")
    print()

    # Get development plan
    plan_result = planner.get_development_plan()

    if "error" in plan_result:
        print(f"Error: {plan_result['error']}")
        return

    print("DEVELOPMENT PLAN TO AUTONOMOUS SELF-IMPROVEMENT:")
    print("-" * 60)

    for i, step in enumerate(plan_result['plan'], 1):
        print(f"\n{i}. {step['action']}")
        print(f"   {step['description']}")
        print(f"   Cost: {step['cost']}")
        effects = ', '.join(step['effects'].keys())
        print(f"   Enables: {effects}")

    print()
    print(f"Total steps: {plan_result['steps']}")
    print(f"Total cost: {plan_result['total_cost']}")

    print()
    print("GOAL HIERARCHY:")
    print("-" * 60)
    tree = planner.goal_hierarchy.get_goal_tree()

    def print_tree(goals, indent=0):
        for g in goals:
            print("  " * indent + f"• {g['name']} [{g['priority']}]")
            if g['subgoals']:
                print_tree(g['subgoals'], indent + 1)

    print_tree(tree['goals'])


if __name__ == "__main__":
    main()
