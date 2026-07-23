"""GraphSub: Reflexive world model graph schema for agent execution.

Everything is a node. Edges are nodes. Types reference types.
Provides execution semantics layer for "graph as execution medium" agents.
"""

import hashlib
import struct
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, auto
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import json

# =============================================================================
# BINARY LAYOUT - Cache-aligned structures for nanosecond traversal
# =============================================================================

class NodeType(IntEnum):
    """Node type discriminator (fits in 1 byte)."""
    # Physical grounding (0x00-0x1F)
    PERSON = 0x00
    ORGANIZATION = 0x01
    GROUP = 0x02
    LOCATION = 0x03
    FACILITY = 0x04
    OBJECT = 0x05
    SUBSTANCE = 0x06
    ORGANISM = 0x07

    # Abstract grounding (0x20-0x3F)
    CONCEPT = 0x20
    CATEGORY = 0x21
    PROPERTY = 0x22
    METRIC = 0x23
    PATTERN = 0x24
    PROCESS = 0x25

    # Temporal grounding (0x40-0x5F)
    EVENT = 0x40
    STATE = 0x41
    INTERVAL = 0x42
    INSTANT = 0x43
    SCHEDULE = 0x44
    DEADLINE = 0x45

    # Symbolic grounding (0x60-0x7F)
    PRODUCT = 0x60
    WORK = 0x61
    LAW = 0x62
    LANGUAGE = 0x63
    PROTOCOL = 0x64
    SPECIFICATION = 0x65
    IDENTIFIER = 0x66

    # Quantitative grounding (0x80-0x9F)
    QUANTITY = 0x80
    MONEY = 0x81
    DURATION = 0x82
    DISTANCE = 0x83
    PROBABILITY = 0x84
    RATIO = 0x85

    # Mathematical structure (0xA0-0xBF)
    VARIABLE = 0xA0
    CONSTANT = 0xA1
    EXPRESSION = 0xA2
    EQUATION = 0xA3
    FORMULA = 0xA4
    THRESHOLD = 0xA5

    # Execution semantics (0xC0-0xDF)
    ACTION = 0xC0
    GOAL = 0xC1
    FRAME = 0xC2
    PLAN = 0xC3
    CAPABILITY = 0xC4
    CONSTRAINT = 0xC5
    OUTCOME = 0xC6
    RESOURCE = 0xC7

    # Epistemic (0xE0-0xEF)
    CLAIM = 0xE0
    EVIDENCE = 0xE1
    UNCERTAINTY = 0xE2
    BELIEF = 0xE3

    # Meta/reflexive (0xF0-0xFF)
    TYPE_NODE = 0xF0
    RELATION_NODE = 0xF1
    SCHEMA_NODE = 0xF2
    FRAME_NODE = 0xF3
    SUMMARY_NODE = 0xF4
    INDEX_NODE = 0xF5
    SHARD_NODE = 0xF6


class EdgeType(IntEnum):
    """Edge type discriminator (fits in 1 byte)."""
    # Compositional (0x00-0x0F)
    PART_OF = 0x00
    CONTAINS = 0x01
    MEMBER_OF = 0x02
    INSTANCE_OF = 0x03
    SUBTYPE_OF = 0x04

    # Causal (0x10-0x1F)
    CAUSES = 0x10
    ENABLES = 0x11
    PREVENTS = 0x12
    TRIGGERS = 0x13
    INHIBITS = 0x14
    MODULATES = 0x15

    # Temporal - Allen algebra (0x20-0x2F)
    PRECEDES = 0x20
    OVERLAPS = 0x21
    DURING = 0x22
    STARTS = 0x23
    FINISHES = 0x24
    MEETS = 0x25
    EQUALS_TEMPORAL = 0x26

    # Spatial (0x30-0x3F)
    LOCATED_IN = 0x30
    NEAR = 0x31
    ADJACENT_TO = 0x32
    CONTAINS_SPATIALLY = 0x33
    PATH_BETWEEN = 0x34

    # Epistemic (0x40-0x4F)
    SUPPORTS = 0x40
    CONTRADICTS = 0x41
    ELABORATES = 0x42
    SUMMARIZES = 0x43
    CITES = 0x44
    INFERRED_FROM = 0x45

    # Agentive (0x50-0x5F)
    OWNS = 0x50
    CONTROLS = 0x51
    AUTHORED = 0x52
    RESPONSIBLE_FOR = 0x53
    DELEGATED_TO = 0x54

    # Execution (0x60-0x6F)
    REQUIRES = 0x60
    PRODUCES = 0x61
    SATISFIES = 0x62
    USES = 0x63
    DEPENDS_ON = 0x64
    ACHIEVES = 0x65
    BLOCKS = 0x66

    # Mathematical (0x70-0x7F)
    DIVIDED_BY = 0x70
    MULTIPLIED_BY = 0x71
    ADDED_TO = 0x72
    EQUALS = 0x73
    GREATER_THAN = 0x74
    LESS_THAN = 0x75
    HAS_VARIABLE = 0x76
    HAS_COEFFICIENT = 0x77

    # Meta/cross-layer (0xF0-0xFF)
    REFERENCED_BY = 0xF0
    CONSTRAINS = 0xF1
    DERIVED_FROM = 0xF2
    ANALOGOUS_TO = 0xF3
    CO_OCCURS = 0xFE
    RELATED_TO = 0xFF


@dataclass
class BinaryNodeHeader:
    """64-byte cache-aligned node header.

    Layout:
    - bytes 0-7:   node_id (uint64)
    - byte 8:      node_type (NodeType enum)
    - byte 9:      flags (bitfield)
    - bytes 10-11: confidence (float16)
    - bytes 12-15: mention_count (uint32)
    - bytes 16-23: created_ts (uint64 - unix micros)
    - bytes 24-31: updated_ts (uint64 - unix micros)
    - bytes 32-39: payload_offset (uint64 - offset to variable payload)
    - bytes 40-47: payload_size (uint64 - size of payload)
    - bytes 48-55: first_edge_offset (uint64 - linked list of edges)
    - bytes 56-63: reserved (padding for alignment)
    """
    node_id: int
    node_type: NodeType
    flags: int = 0
    confidence: float = 1.0
    mention_count: int = 1
    created_ts: int = 0
    updated_ts: int = 0
    payload_offset: int = 0
    payload_size: int = 0
    first_edge_offset: int = 0

    STRUCT_FORMAT = "<QBBHIQQQQQxxxxxxxx"  # 64 bytes total (8 padding bytes)

    def pack(self) -> bytes:
        conf_half = int(self.confidence * 65535)
        return struct.pack(
            self.STRUCT_FORMAT,
            self.node_id,
            self.node_type.value,
            self.flags,
            conf_half,
            self.mention_count,
            self.created_ts,
            self.updated_ts,
            self.payload_offset,
            self.payload_size,
            self.first_edge_offset,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "BinaryNodeHeader":
        values = struct.unpack(cls.STRUCT_FORMAT, data[:64])
        return cls(
            node_id=values[0],
            node_type=NodeType(values[1]),
            flags=values[2],
            confidence=values[3] / 65535.0,
            mention_count=values[4],
            created_ts=values[5],
            updated_ts=values[6],
            payload_offset=values[7],
            payload_size=values[8],
            first_edge_offset=values[9],
        )


@dataclass
class BinaryEdgeHeader:
    """32-byte cache-aligned edge header.

    Layout:
    - bytes 0-7:   source_id (uint64)
    - bytes 8-15:  target_id (uint64)
    - byte 16:     edge_type (EdgeType enum)
    - byte 17:     flags (bitfield)
    - bytes 18-19: weight (float16)
    - bytes 20-23: next_edge_offset (uint32 - linked list)
    - bytes 24-27: properties_offset (uint32 - to properties blob)
    - bytes 28-31: reserved (padding)
    """
    source_id: int
    target_id: int
    edge_type: EdgeType
    flags: int = 0
    weight: float = 1.0
    next_edge_offset: int = 0
    properties_offset: int = 0

    STRUCT_FORMAT = "<QQBBHIIxxxx"  # 32 bytes total (4 padding bytes)

    def pack(self) -> bytes:
        weight_half = int(self.weight * 65535)
        return struct.pack(
            self.STRUCT_FORMAT,
            self.source_id,
            self.target_id,
            self.edge_type.value,
            self.flags,
            weight_half,
            self.next_edge_offset,
            self.properties_offset,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "BinaryEdgeHeader":
        values = struct.unpack(cls.STRUCT_FORMAT, data[:32])
        return cls(
            source_id=values[0],
            target_id=values[1],
            edge_type=EdgeType(values[2]),
            flags=values[3],
            weight=values[4] / 65535.0,
            next_edge_offset=values[5],
            properties_offset=values[6],
        )


# =============================================================================
# EXECUTION SEMANTICS LAYER - Actions, Goals, Frames, States
# =============================================================================

@dataclass
class ActionPayload:
    """Executable action payload.

    Actions are verbs grounded as executable operations with:
    - Preconditions: What must be true before execution
    - Effects: What becomes true after execution
    - Parameters: Bound variables for the action
    - Constraints: Invariants during execution
    """
    verb: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    preconditions: List[str] = field(default_factory=list)
    effects: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    cost: float = 1.0
    duration_estimate: Optional[float] = None
    reversible: bool = False
    idempotent: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "action",
            "verb": self.verb,
            "parameters": self.parameters,
            "preconditions": self.preconditions,
            "effects": self.effects,
            "constraints": self.constraints,
            "cost": self.cost,
            "duration_estimate": self.duration_estimate,
            "reversible": self.reversible,
            "idempotent": self.idempotent,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ActionPayload":
        return cls(
            verb=d.get("verb", ""),
            parameters=d.get("parameters", {}),
            preconditions=d.get("preconditions", []),
            effects=d.get("effects", []),
            constraints=d.get("constraints", []),
            cost=d.get("cost", 1.0),
            duration_estimate=d.get("duration_estimate"),
            reversible=d.get("reversible", False),
            idempotent=d.get("idempotent", False),
        )


@dataclass
class GoalPayload:
    """Goal structure for agent planning.

    Goals represent desired states with:
    - Target state: The condition to achieve
    - Priority: Relative importance
    - Deadline: Optional time constraint
    - Subgoals: Hierarchical decomposition
    - Success criteria: How to verify achievement
    """
    description: str
    target_state: Dict[str, Any] = field(default_factory=dict)
    priority: float = 0.5
    deadline: Optional[str] = None
    subgoal_ids: List[str] = field(default_factory=list)
    success_criteria: List[str] = field(default_factory=list)
    failure_conditions: List[str] = field(default_factory=list)
    reward: float = 1.0
    status: str = "pending"  # pending, active, achieved, failed, abandoned

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "goal",
            "description": self.description,
            "target_state": self.target_state,
            "priority": self.priority,
            "deadline": self.deadline,
            "subgoal_ids": self.subgoal_ids,
            "success_criteria": self.success_criteria,
            "failure_conditions": self.failure_conditions,
            "reward": self.reward,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GoalPayload":
        return cls(
            description=d.get("description", ""),
            target_state=d.get("target_state", {}),
            priority=d.get("priority", 0.5),
            deadline=d.get("deadline"),
            subgoal_ids=d.get("subgoal_ids", []),
            success_criteria=d.get("success_criteria", []),
            failure_conditions=d.get("failure_conditions", []),
            reward=d.get("reward", 1.0),
            status=d.get("status", "pending"),
        )


@dataclass
class FramePayload:
    """Execution context frame.

    Frames provide context for interpretation:
    - Active entities: What's in scope
    - Active relations: Which connections matter
    - Attention weights: What to focus on
    - Temporal bounds: Time window of relevance
    - Causal chain: How we got here
    """
    name: str
    active_entity_ids: Set[str] = field(default_factory=set)
    active_relation_ids: Set[str] = field(default_factory=set)
    attention_weights: Dict[str, float] = field(default_factory=dict)
    temporal_start: Optional[str] = None
    temporal_end: Optional[str] = None
    causal_chain: List[str] = field(default_factory=list)
    parent_frame_id: Optional[str] = None
    child_frame_ids: List[str] = field(default_factory=list)
    compression_level: int = 0  # 0=full detail, higher=more compressed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "frame",
            "name": self.name,
            "active_entity_ids": list(self.active_entity_ids),
            "active_relation_ids": list(self.active_relation_ids),
            "attention_weights": self.attention_weights,
            "temporal_start": self.temporal_start,
            "temporal_end": self.temporal_end,
            "causal_chain": self.causal_chain,
            "parent_frame_id": self.parent_frame_id,
            "child_frame_ids": self.child_frame_ids,
            "compression_level": self.compression_level,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FramePayload":
        return cls(
            name=d.get("name", ""),
            active_entity_ids=set(d.get("active_entity_ids", [])),
            active_relation_ids=set(d.get("active_relation_ids", [])),
            attention_weights=d.get("attention_weights", {}),
            temporal_start=d.get("temporal_start"),
            temporal_end=d.get("temporal_end"),
            causal_chain=d.get("causal_chain", []),
            parent_frame_id=d.get("parent_frame_id"),
            child_frame_ids=d.get("child_frame_ids", []),
            compression_level=d.get("compression_level", 0),
        )


@dataclass
class StatePayload:
    """World state snapshot.

    States capture configuration at a point in time:
    - Variable bindings: Current values
    - Active constraints: What must hold
    - Changed entities: What's different from parent state
    - Entropy: Uncertainty measure
    """
    state_id: str
    variable_bindings: Dict[str, Any] = field(default_factory=dict)
    active_constraints: List[str] = field(default_factory=list)
    changed_entity_ids: Set[str] = field(default_factory=set)
    parent_state_id: Optional[str] = None
    timestamp: Optional[str] = None
    entropy: float = 0.0
    is_terminal: bool = False
    is_goal_state: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "state",
            "state_id": self.state_id,
            "variable_bindings": self.variable_bindings,
            "active_constraints": self.active_constraints,
            "changed_entity_ids": list(self.changed_entity_ids),
            "parent_state_id": self.parent_state_id,
            "timestamp": self.timestamp,
            "entropy": self.entropy,
            "is_terminal": self.is_terminal,
            "is_goal_state": self.is_goal_state,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StatePayload":
        return cls(
            state_id=d.get("state_id", ""),
            variable_bindings=d.get("variable_bindings", {}),
            active_constraints=d.get("active_constraints", []),
            changed_entity_ids=set(d.get("changed_entity_ids", [])),
            parent_state_id=d.get("parent_state_id"),
            timestamp=d.get("timestamp"),
            entropy=d.get("entropy", 0.0),
            is_terminal=d.get("is_terminal", False),
            is_goal_state=d.get("is_goal_state", False),
        )


@dataclass
class PlanPayload:
    """Execution plan - sequence of actions to achieve goals.

    Plans connect actions to goals:
    - Action sequence: Ordered steps
    - Goal references: What this plan achieves
    - Estimated cost: Total resource requirement
    - Success probability: Likelihood of completion
    """
    plan_id: str
    action_ids: List[str] = field(default_factory=list)
    goal_ids: List[str] = field(default_factory=list)
    estimated_cost: float = 0.0
    estimated_duration: float = 0.0
    success_probability: float = 1.0
    status: str = "draft"  # draft, active, completed, failed, cancelled
    current_step: int = 0
    rollback_plan_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "plan",
            "plan_id": self.plan_id,
            "action_ids": self.action_ids,
            "goal_ids": self.goal_ids,
            "estimated_cost": self.estimated_cost,
            "estimated_duration": self.estimated_duration,
            "success_probability": self.success_probability,
            "status": self.status,
            "current_step": self.current_step,
            "rollback_plan_id": self.rollback_plan_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PlanPayload":
        return cls(
            plan_id=d.get("plan_id", ""),
            action_ids=d.get("action_ids", []),
            goal_ids=d.get("goal_ids", []),
            estimated_cost=d.get("estimated_cost", 0.0),
            estimated_duration=d.get("estimated_duration", 0.0),
            success_probability=d.get("success_probability", 1.0),
            status=d.get("status", "draft"),
            current_step=d.get("current_step", 0),
            rollback_plan_id=d.get("rollback_plan_id"),
        )


# =============================================================================
# EPISTEMIC LAYER - Claims, Evidence, Uncertainty
# =============================================================================

class EvidenceType(IntEnum):
    """Types of evidence supporting claims."""
    DIRECT_OBSERVATION = auto()
    TESTIMONY = auto()
    DOCUMENT = auto()
    INFERENCE = auto()
    SIMULATION = auto()
    EXPERT_OPINION = auto()
    STATISTICAL = auto()
    EXPERIMENTAL = auto()


@dataclass
class ClaimPayload:
    """Epistemic claim with provenance tracking.

    Claims are assertions with:
    - Proposition: What is claimed
    - Confidence: How certain
    - Evidence: What supports it
    - Source: Where it came from
    - Temporal validity: When it's true
    """
    proposition: str
    confidence: float = 0.5
    evidence_ids: List[str] = field(default_factory=list)
    source: Optional[str] = None
    source_type: Optional[str] = None  # document, observation, inference, etc.
    valid_from: Optional[str] = None
    valid_until: Optional[str] = None
    contradicted_by: List[str] = field(default_factory=list)
    supported_by: List[str] = field(default_factory=list)
    inference_chain: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "claim",
            "proposition": self.proposition,
            "confidence": self.confidence,
            "evidence_ids": self.evidence_ids,
            "source": self.source,
            "source_type": self.source_type,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "contradicted_by": self.contradicted_by,
            "supported_by": self.supported_by,
            "inference_chain": self.inference_chain,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ClaimPayload":
        return cls(
            proposition=d.get("proposition", ""),
            confidence=d.get("confidence", 0.5),
            evidence_ids=d.get("evidence_ids", []),
            source=d.get("source"),
            source_type=d.get("source_type"),
            valid_from=d.get("valid_from"),
            valid_until=d.get("valid_until"),
            contradicted_by=d.get("contradicted_by", []),
            supported_by=d.get("supported_by", []),
            inference_chain=d.get("inference_chain", []),
        )


@dataclass
class EvidencePayload:
    """Evidence supporting or contradicting claims.

    Evidence has:
    - Type: Kind of evidence (observation, document, inference)
    - Content: The actual evidence
    - Strength: How strong the support
    - Reliability: How trustworthy the source
    """
    evidence_type: EvidenceType
    content: str
    strength: float = 0.5  # 0=weak, 1=strong
    reliability: float = 0.5  # 0=unreliable, 1=highly reliable
    source_doc_id: Optional[str] = None
    extraction_method: Optional[str] = None
    timestamp: Optional[str] = None
    verifiable: bool = False
    verified: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "evidence",
            "evidence_type": self.evidence_type.name,
            "content": self.content,
            "strength": self.strength,
            "reliability": self.reliability,
            "source_doc_id": self.source_doc_id,
            "extraction_method": self.extraction_method,
            "timestamp": self.timestamp,
            "verifiable": self.verifiable,
            "verified": self.verified,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EvidencePayload":
        return cls(
            evidence_type=EvidenceType[d.get("evidence_type", "INFERENCE")],
            content=d.get("content", ""),
            strength=d.get("strength", 0.5),
            reliability=d.get("reliability", 0.5),
            source_doc_id=d.get("source_doc_id"),
            extraction_method=d.get("extraction_method"),
            timestamp=d.get("timestamp"),
            verifiable=d.get("verifiable", False),
            verified=d.get("verified", False),
            metadata=d.get("metadata", {}),
        )


@dataclass
class UncertaintyPayload:
    """Explicit uncertainty representation.

    Captures what we don't know:
    - Unknown entity: Missing information about entity
    - Unknown relation: Missing relationship information
    - Ambiguity: Multiple valid interpretations
    - Confidence intervals: Ranges of possible values
    """
    uncertainty_type: str  # missing_entity, missing_relation, ambiguous, range
    description: str
    related_entity_ids: List[str] = field(default_factory=list)
    possible_values: List[Any] = field(default_factory=list)
    confidence_interval: Optional[Tuple[float, float]] = None
    distribution_type: Optional[str] = None  # uniform, gaussian, etc.
    resolution_hints: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "uncertainty",
            "uncertainty_type": self.uncertainty_type,
            "description": self.description,
            "related_entity_ids": self.related_entity_ids,
            "possible_values": self.possible_values,
            "confidence_interval": self.confidence_interval,
            "distribution_type": self.distribution_type,
            "resolution_hints": self.resolution_hints,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "UncertaintyPayload":
        interval = d.get("confidence_interval")
        if interval and isinstance(interval, list):
            interval = tuple(interval)
        return cls(
            uncertainty_type=d.get("uncertainty_type", ""),
            description=d.get("description", ""),
            related_entity_ids=d.get("related_entity_ids", []),
            possible_values=d.get("possible_values", []),
            confidence_interval=interval,
            distribution_type=d.get("distribution_type"),
            resolution_hints=d.get("resolution_hints", []),
        )


# =============================================================================
# META-TAG SYSTEM - Cross-layer references
# =============================================================================

@dataclass
class MetaTag:
    """Cross-layer reference tag.

    Meta-tags connect nodes across layers:
    - referenced_by: What other nodes reference this
    - constrains: What constraints this imposes
    - derived_from: Source nodes this was derived from
    - analogous_to: Similar nodes in different contexts
    """
    tag_type: str  # referenced_by, constrains, derived_from, analogous_to
    source_node_id: str
    target_node_id: str
    strength: float = 1.0
    context: Optional[str] = None
    bidirectional: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tag_type": self.tag_type,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "strength": self.strength,
            "context": self.context,
            "bidirectional": self.bidirectional,
        }


# =============================================================================
# CONTEXT WINDOWING - Hierarchical compression for 20M token window
# =============================================================================

@dataclass
class ContextWindow:
    """Hierarchical context window for attention management.

    Manages attention across compression levels:
    - Level 0: Full detail, current focus (8K tokens)
    - Level 1: Recent context, summarized (16K tokens)
    - Level 2: Session context, compressed (32K tokens)
    - Level 3: Persistent memory, highly compressed (64K tokens)
    - Level 4: Archive, maximally compressed (128K tokens)

    Total: ~250K tokens in hierarchical structure representing ~20M tokens of original content.
    """
    level: int
    tokens_budget: int
    compression_ratio: float
    entity_ids: List[str] = field(default_factory=list)
    relation_ids: List[str] = field(default_factory=list)
    summary_node_id: Optional[str] = None
    lru_order: List[str] = field(default_factory=list)  # Most recent first

    LEVEL_CONFIG = {
        0: {"name": "immediate", "tokens": 8000, "compression": 1.0},
        1: {"name": "recent", "tokens": 16000, "compression": 0.5},
        2: {"name": "session", "tokens": 32000, "compression": 0.25},
        3: {"name": "persistent", "tokens": 64000, "compression": 0.1},
        4: {"name": "archive", "tokens": 128000, "compression": 0.02},
    }

    def should_evict(self, current_tokens: int) -> bool:
        return current_tokens > self.tokens_budget

    def evict_lru(self) -> Optional[str]:
        if self.lru_order:
            return self.lru_order.pop()
        return None

    def access(self, entity_id: str):
        if entity_id in self.lru_order:
            self.lru_order.remove(entity_id)
        self.lru_order.insert(0, entity_id)


# =============================================================================
# GRAPH NODE - Unified node structure
# =============================================================================

@dataclass
class GraphNode:
    """Unified graph node combining header and payload.

    Everything is a node - entities, actions, goals, claims, etc.
    The payload field contains type-specific data.
    """
    node_id: str
    node_type: NodeType
    text: str  # Canonical name/text
    properties: Dict[str, Any] = field(default_factory=dict)
    payload: Optional[Union[
        ActionPayload, GoalPayload, FramePayload, StatePayload,
        PlanPayload, ClaimPayload, EvidencePayload, UncertaintyPayload
    ]] = None
    meta_tags: List[MetaTag] = field(default_factory=list)
    confidence: float = 1.0
    mention_count: int = 1
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    grounding_category: str = "abstract"
    source_docs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.name,
            "text": self.text,
            "properties": self.properties,
            "payload": self.payload.to_dict() if self.payload else None,
            "meta_tags": [t.to_dict() for t in self.meta_tags],
            "confidence": self.confidence,
            "mention_count": self.mention_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "grounding_category": self.grounding_category,
            "source_docs": self.source_docs,
        }

    def to_binary_header(self) -> BinaryNodeHeader:
        return BinaryNodeHeader(
            node_id=int(hashlib.sha256(self.node_id.encode()).hexdigest()[:16], 16),
            node_type=self.node_type,
            confidence=self.confidence,
            mention_count=self.mention_count,
        )


@dataclass
class GraphEdge:
    """Unified graph edge.

    Edges connect nodes with typed relationships.
    Edges can also be nodes (reflexive property).
    """
    edge_id: str
    source_id: str
    target_id: str
    edge_type: EdgeType
    properties: Dict[str, Any] = field(default_factory=dict)
    weight: float = 1.0
    confidence: float = 1.0
    context: str = ""
    source_docs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type.name,
            "properties": self.properties,
            "weight": self.weight,
            "confidence": self.confidence,
            "context": self.context,
            "source_docs": self.source_docs,
        }

    def to_binary_header(self) -> BinaryEdgeHeader:
        return BinaryEdgeHeader(
            source_id=int(hashlib.sha256(self.source_id.encode()).hexdigest()[:16], 16),
            target_id=int(hashlib.sha256(self.target_id.encode()).hexdigest()[:16], 16),
            edge_type=self.edge_type,
            weight=self.weight,
        )


# =============================================================================
# TYPE MAPPINGS - String to enum conversions
# =============================================================================

NODE_TYPE_MAP = {
    # Physical
    "person": NodeType.PERSON,
    "organization": NodeType.ORGANIZATION,
    "group": NodeType.GROUP,
    "location": NodeType.LOCATION,
    "facility": NodeType.FACILITY,
    "object": NodeType.OBJECT,
    "substance": NodeType.SUBSTANCE,
    "organism": NodeType.ORGANISM,
    # Abstract
    "concept": NodeType.CONCEPT,
    "category": NodeType.CATEGORY,
    "property": NodeType.PROPERTY,
    "metric": NodeType.METRIC,
    "pattern": NodeType.PATTERN,
    "process": NodeType.PROCESS,
    # Temporal
    "event": NodeType.EVENT,
    "state": NodeType.STATE,
    "interval": NodeType.INTERVAL,
    "instant": NodeType.INSTANT,
    "schedule": NodeType.SCHEDULE,
    "deadline": NodeType.DEADLINE,
    # Symbolic
    "product": NodeType.PRODUCT,
    "work": NodeType.WORK,
    "law": NodeType.LAW,
    "language": NodeType.LANGUAGE,
    "protocol": NodeType.PROTOCOL,
    "specification": NodeType.SPECIFICATION,
    "identifier": NodeType.IDENTIFIER,
    # Quantitative
    "quantity": NodeType.QUANTITY,
    "money": NodeType.MONEY,
    "duration": NodeType.DURATION,
    "distance": NodeType.DISTANCE,
    "probability": NodeType.PROBABILITY,
    "ratio": NodeType.RATIO,
    # Mathematical
    "variable": NodeType.VARIABLE,
    "constant": NodeType.CONSTANT,
    "expression": NodeType.EXPRESSION,
    "equation": NodeType.EQUATION,
    "formula": NodeType.FORMULA,
    "threshold": NodeType.THRESHOLD,
    # Execution
    "action": NodeType.ACTION,
    "goal": NodeType.GOAL,
    "frame": NodeType.FRAME,
    "plan": NodeType.PLAN,
    "capability": NodeType.CAPABILITY,
    "constraint": NodeType.CONSTRAINT,
    "outcome": NodeType.OUTCOME,
    "resource": NodeType.RESOURCE,
    # Epistemic
    "claim": NodeType.CLAIM,
    "evidence": NodeType.EVIDENCE,
    "uncertainty": NodeType.UNCERTAINTY,
    "belief": NodeType.BELIEF,
    # Meta
    "type_node": NodeType.TYPE_NODE,
    "relation_node": NodeType.RELATION_NODE,
    "schema_node": NodeType.SCHEMA_NODE,
    "frame_node": NodeType.FRAME_NODE,
    "summary_node": NodeType.SUMMARY_NODE,
    "index_node": NodeType.INDEX_NODE,
    "shard_node": NodeType.SHARD_NODE,
}

EDGE_TYPE_MAP = {
    # Compositional
    "part_of": EdgeType.PART_OF,
    "contains": EdgeType.CONTAINS,
    "member_of": EdgeType.MEMBER_OF,
    "instance_of": EdgeType.INSTANCE_OF,
    "subtype_of": EdgeType.SUBTYPE_OF,
    # Causal
    "causes": EdgeType.CAUSES,
    "enables": EdgeType.ENABLES,
    "prevents": EdgeType.PREVENTS,
    "triggers": EdgeType.TRIGGERS,
    "inhibits": EdgeType.INHIBITS,
    "modulates": EdgeType.MODULATES,
    # Temporal
    "precedes": EdgeType.PRECEDES,
    "overlaps": EdgeType.OVERLAPS,
    "during": EdgeType.DURING,
    "starts": EdgeType.STARTS,
    "finishes": EdgeType.FINISHES,
    "meets": EdgeType.MEETS,
    # Spatial
    "located_in": EdgeType.LOCATED_IN,
    "near": EdgeType.NEAR,
    "adjacent_to": EdgeType.ADJACENT_TO,
    "contains_spatially": EdgeType.CONTAINS_SPATIALLY,
    "path_between": EdgeType.PATH_BETWEEN,
    # Epistemic
    "supports": EdgeType.SUPPORTS,
    "contradicts": EdgeType.CONTRADICTS,
    "elaborates": EdgeType.ELABORATES,
    "summarizes": EdgeType.SUMMARIZES,
    "cites": EdgeType.CITES,
    "inferred_from": EdgeType.INFERRED_FROM,
    # Agentive
    "owns": EdgeType.OWNS,
    "controls": EdgeType.CONTROLS,
    "authored": EdgeType.AUTHORED,
    "responsible_for": EdgeType.RESPONSIBLE_FOR,
    "delegated_to": EdgeType.DELEGATED_TO,
    # Execution
    "requires": EdgeType.REQUIRES,
    "produces": EdgeType.PRODUCES,
    "satisfies": EdgeType.SATISFIES,
    "uses": EdgeType.USES,
    "depends_on": EdgeType.DEPENDS_ON,
    "achieves": EdgeType.ACHIEVES,
    "blocks": EdgeType.BLOCKS,
    # Mathematical
    "divided_by": EdgeType.DIVIDED_BY,
    "multiplied_by": EdgeType.MULTIPLIED_BY,
    "added_to": EdgeType.ADDED_TO,
    "equals": EdgeType.EQUALS,
    "greater_than": EdgeType.GREATER_THAN,
    "less_than": EdgeType.LESS_THAN,
    "has_variable": EdgeType.HAS_VARIABLE,
    "has_coefficient": EdgeType.HAS_COEFFICIENT,
    # Meta
    "referenced_by": EdgeType.REFERENCED_BY,
    "constrains": EdgeType.CONSTRAINS,
    "derived_from": EdgeType.DERIVED_FROM,
    "analogous_to": EdgeType.ANALOGOUS_TO,
    "co_occurs": EdgeType.CO_OCCURS,
    "related_to": EdgeType.RELATED_TO,
}

GROUNDING_CATEGORY_MAP = {
    NodeType.PERSON: "physical",
    NodeType.ORGANIZATION: "physical",
    NodeType.GROUP: "physical",
    NodeType.LOCATION: "physical",
    NodeType.FACILITY: "physical",
    NodeType.OBJECT: "physical",
    NodeType.SUBSTANCE: "physical",
    NodeType.ORGANISM: "physical",
    NodeType.CONCEPT: "abstract",
    NodeType.CATEGORY: "abstract",
    NodeType.PROPERTY: "abstract",
    NodeType.METRIC: "abstract",
    NodeType.PATTERN: "abstract",
    NodeType.PROCESS: "abstract",
    NodeType.EVENT: "temporal",
    NodeType.STATE: "temporal",
    NodeType.INTERVAL: "temporal",
    NodeType.INSTANT: "temporal",
    NodeType.SCHEDULE: "temporal",
    NodeType.DEADLINE: "temporal",
    NodeType.PRODUCT: "symbolic",
    NodeType.WORK: "symbolic",
    NodeType.LAW: "symbolic",
    NodeType.LANGUAGE: "symbolic",
    NodeType.PROTOCOL: "symbolic",
    NodeType.SPECIFICATION: "symbolic",
    NodeType.IDENTIFIER: "symbolic",
    NodeType.QUANTITY: "quantitative",
    NodeType.MONEY: "quantitative",
    NodeType.DURATION: "quantitative",
    NodeType.DISTANCE: "quantitative",
    NodeType.PROBABILITY: "quantitative",
    NodeType.RATIO: "quantitative",
    NodeType.VARIABLE: "quantitative",
    NodeType.CONSTANT: "quantitative",
    NodeType.EXPRESSION: "quantitative",
    NodeType.EQUATION: "quantitative",
    NodeType.FORMULA: "quantitative",
    NodeType.THRESHOLD: "quantitative",
    NodeType.ACTION: "execution",
    NodeType.GOAL: "execution",
    NodeType.FRAME: "execution",
    NodeType.PLAN: "execution",
    NodeType.CAPABILITY: "execution",
    NodeType.CONSTRAINT: "execution",
    NodeType.OUTCOME: "execution",
    NodeType.RESOURCE: "execution",
    NodeType.CLAIM: "epistemic",
    NodeType.EVIDENCE: "epistemic",
    NodeType.UNCERTAINTY: "epistemic",
    NodeType.BELIEF: "epistemic",
    NodeType.TYPE_NODE: "meta",
    NodeType.RELATION_NODE: "meta",
    NodeType.SCHEMA_NODE: "meta",
    NodeType.FRAME_NODE: "meta",
    NodeType.SUMMARY_NODE: "meta",
    NodeType.INDEX_NODE: "meta",
    NodeType.SHARD_NODE: "meta",
}


def get_node_type(type_str: str) -> NodeType:
    """Convert string type to NodeType enum."""
    return NODE_TYPE_MAP.get(type_str.lower(), NodeType.CONCEPT)


def get_edge_type(type_str: str) -> EdgeType:
    """Convert string type to EdgeType enum."""
    return EDGE_TYPE_MAP.get(type_str.lower(), EdgeType.RELATED_TO)


def get_grounding_category(node_type: NodeType) -> str:
    """Get grounding category for a node type."""
    return GROUNDING_CATEGORY_MAP.get(node_type, "abstract")
