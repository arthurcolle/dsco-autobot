"""
Distributed Memory MCP Server - Cognitive Memory Architecture.

Port: 4004

Implements a multi-tier memory system inspired by human cognition:
- Working Memory: LRU cache with attention decay for active context
- Episodic Memory: Time-indexed event log with similarity search
- Semantic Memory: Entity-relationship graph for factual knowledge
- Procedural Memory: Executable skill patterns

Tools:
- focus: Prime working memory with relevant context
- expand_context: Expand working memory with related items
- record_episode: Store an episode in episodic memory
- recall_episodes: Retrieve episodes by similarity or time
- store_belief: Add/update a belief in semantic memory
- query_beliefs: Query semantic memory with inference
- learn_skill: Store a procedural pattern
- apply_skill: Execute a learned skill
- consolidate: Move items between memory tiers
- forget: Explicit forgetting with decay
- search_all: Cross-memory unified search
- get_memory_stats: Memory usage and statistics
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

# Internal client for cross-MCP calls
try:
    from native.internal_client import get_internal_client, InternalMCPClient
except ImportError:
    # Fallback for when running as standalone
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from internal_client import get_internal_client, InternalMCPClient

# Semantic belief store with embeddings
try:
    from tool_management.embeddings import SemanticBeliefStore, get_belief_store
    SEMANTIC_BELIEFS_AVAILABLE = True
except ImportError:
    SEMANTIC_BELIEFS_AVAILABLE = False
    SemanticBeliefStore = None  # type: ignore

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# VECTOR MATH UTILITIES
# =============================================================================

def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# =============================================================================
# DATA MODELS
# =============================================================================

class MemoryTier(str, Enum):
    WORKING = "working"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    PROCEDURAL = "procedural"


@dataclass
class MemoryItem:
    """Base memory item with common fields."""
    id: str
    content: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    importance: float = 0.5
    decay_rate: float = 0.1
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self):
        """Update access timestamp and count."""
        self.accessed_at = time.time()
        self.access_count += 1

    def get_activation(self, current_time: Optional[float] = None) -> float:
        """Calculate activation level based on recency and frequency."""
        t = current_time or time.time()
        recency = math.exp(-self.decay_rate * (t - self.accessed_at))
        frequency = math.log(1 + self.access_count)
        return self.importance * recency * (1 + 0.1 * frequency)


@dataclass
class Episode:
    """An episodic memory entry."""
    id: str
    event_type: str
    content: Any
    timestamp: float
    context: Dict[str, Any] = field(default_factory=dict)
    participants: List[str] = field(default_factory=list)
    location: Optional[str] = None
    outcome: Optional[str] = None
    emotional_valence: float = 0.0  # -1 to 1
    importance: float = 0.5
    embedding: Optional[List[float]] = None


@dataclass
class Belief:
    """A semantic memory belief/fact."""
    id: str
    subject: str
    predicate: str
    object: str
    confidence: float = 1.0
    source: Optional[str] = None
    valid_from: Optional[float] = None
    valid_until: Optional[float] = None
    evidence: List[str] = field(default_factory=list)
    contradicts: List[str] = field(default_factory=list)


@dataclass
class Skill:
    """A procedural memory skill."""
    id: str
    name: str
    description: str
    pattern: Dict[str, Any]  # The executable pattern
    preconditions: List[str] = field(default_factory=list)
    postconditions: List[str] = field(default_factory=list)
    success_count: int = 0
    failure_count: int = 0
    avg_execution_time: float = 0.0
    last_used: Optional[float] = None


# =============================================================================
# MEMORY STORES
# =============================================================================

class WorkingMemory:
    """
    Fast, capacity-limited attention buffer.
    Uses LRU eviction with activation-based prioritization.
    """

    def __init__(self, capacity: int = 100, decay_rate: float = 0.05):
        self.capacity = capacity
        self.decay_rate = decay_rate
        self._items: OrderedDict[str, MemoryItem] = OrderedDict()
        self._focus_items: Set[str] = set()  # Items currently in focus

    def add(self, content: Any, importance: float = 0.5, tags: List[str] = None) -> str:
        """Add item to working memory."""
        item_id = str(uuid.uuid4())[:8]
        now = time.time()

        item = MemoryItem(
            id=item_id,
            content=content,
            created_at=now,
            accessed_at=now,
            importance=importance,
            decay_rate=self.decay_rate,
            tags=tags or [],
        )

        # Evict if at capacity
        while len(self._items) >= self.capacity:
            self._evict_lowest_activation()

        self._items[item_id] = item
        return item_id

    def get(self, item_id: str) -> Optional[MemoryItem]:
        """Get and touch an item."""
        if item_id in self._items:
            self._items[item_id].touch()
            self._items.move_to_end(item_id)
            return self._items[item_id]
        return None

    def focus(self, item_ids: List[str]):
        """Bring items into focus, boosting their importance."""
        self._focus_items = set(item_ids)
        for item_id in item_ids:
            if item_id in self._items:
                self._items[item_id].importance = min(1.0, self._items[item_id].importance + 0.2)
                self._items[item_id].touch()

    def get_context(self, limit: int = 10) -> List[MemoryItem]:
        """Get most activated items as current context."""
        now = time.time()
        items = list(self._items.values())
        items.sort(key=lambda x: x.get_activation(now), reverse=True)
        return items[:limit]

    def search(self, query: str, limit: int = 10) -> List[MemoryItem]:
        """Simple text search in working memory."""
        results = []
        query_lower = query.lower()
        for item in self._items.values():
            content_str = str(item.content).lower()
            if query_lower in content_str:
                item.touch()
                results.append(item)
        return sorted(results, key=lambda x: x.get_activation(), reverse=True)[:limit]

    def _evict_lowest_activation(self):
        """Evict the item with lowest activation."""
        if not self._items:
            return

        now = time.time()
        min_item = min(self._items.values(), key=lambda x: x.get_activation(now))

        # Don't evict focused items unless we must
        if min_item.id in self._focus_items and len(self._items) > len(self._focus_items):
            non_focus = [i for i in self._items.values() if i.id not in self._focus_items]
            if non_focus:
                min_item = min(non_focus, key=lambda x: x.get_activation(now))

        del self._items[min_item.id]
        self._focus_items.discard(min_item.id)

    def clear(self):
        """Clear working memory."""
        self._items.clear()
        self._focus_items.clear()

    def stats(self) -> Dict[str, Any]:
        return {
            "size": len(self._items),
            "capacity": self.capacity,
            "focused_count": len(self._focus_items),
            "avg_activation": sum(i.get_activation() for i in self._items.values()) / max(1, len(self._items)),
        }


class EpisodicMemory:
    """
    Time-indexed event memory with similarity search.
    Stores episodes as they occur, allows temporal and similarity-based retrieval.
    """

    def __init__(self, max_episodes: int = 10000):
        self.max_episodes = max_episodes
        self._episodes: List[Episode] = []
        self._by_type: Dict[str, List[str]] = {}  # event_type -> episode_ids
        self._by_participant: Dict[str, List[str]] = {}  # participant -> episode_ids

    def record(
        self,
        event_type: str,
        content: Any,
        context: Dict[str, Any] = None,
        participants: List[str] = None,
        location: str = None,
        outcome: str = None,
        emotional_valence: float = 0.0,
        importance: float = 0.5,
    ) -> str:
        """Record a new episode."""
        episode_id = str(uuid.uuid4())[:12]

        episode = Episode(
            id=episode_id,
            event_type=event_type,
            content=content,
            timestamp=time.time(),
            context=context or {},
            participants=participants or [],
            location=location,
            outcome=outcome,
            emotional_valence=emotional_valence,
            importance=importance,
        )

        self._episodes.append(episode)

        # Index by type
        if event_type not in self._by_type:
            self._by_type[event_type] = []
        self._by_type[event_type].append(episode_id)

        # Index by participant
        for p in episode.participants:
            if p not in self._by_participant:
                self._by_participant[p] = []
            self._by_participant[p].append(episode_id)

        # Prune if over limit
        while len(self._episodes) > self.max_episodes:
            self._prune_oldest()

        return episode_id

    def recall_by_time(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 50,
    ) -> List[Episode]:
        """Recall episodes within a time range."""
        results = []
        for ep in reversed(self._episodes):  # Most recent first
            if start_time and ep.timestamp < start_time:
                continue
            if end_time and ep.timestamp > end_time:
                continue
            results.append(ep)
            if len(results) >= limit:
                break
        return results

    def recall_by_type(self, event_type: str, limit: int = 50) -> List[Episode]:
        """Recall episodes of a specific type."""
        episode_ids = self._by_type.get(event_type, [])
        return [self._get_by_id(eid) for eid in episode_ids[-limit:] if self._get_by_id(eid)]

    def recall_by_participant(self, participant: str, limit: int = 50) -> List[Episode]:
        """Recall episodes involving a participant."""
        episode_ids = self._by_participant.get(participant, [])
        return [self._get_by_id(eid) for eid in episode_ids[-limit:] if self._get_by_id(eid)]

    def recall_by_similarity_text(self, query: str, limit: int = 10) -> List[Episode]:
        """Fallback text-based similarity search (no embeddings)."""
        query_lower = query.lower()
        scored = []

        for ep in self._episodes:
            content_str = str(ep.content).lower()
            # Simple scoring: word overlap
            query_words = set(query_lower.split())
            content_words = set(content_str.split())
            overlap = len(query_words & content_words)
            if overlap > 0:
                score = overlap / len(query_words) * ep.importance
                scored.append((score, ep))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [ep for _, ep in scored[:limit]]

    def recall_by_embedding(
        self,
        query_embedding: List[float],
        limit: int = 10,
        min_similarity: float = 0.3,
    ) -> List[Tuple[Episode, float]]:
        """Recall episodes by embedding similarity."""
        scored = []

        for ep in self._episodes:
            if ep.embedding:
                sim = cosine_similarity(query_embedding, ep.embedding)
                if sim >= min_similarity:
                    # Weight by importance
                    score = sim * (0.5 + 0.5 * ep.importance)
                    scored.append((score, ep))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [(ep, score) for score, ep in scored[:limit]]

    def get_episodes_without_embeddings(self, limit: int = 100) -> List[Episode]:
        """Get episodes that don't have embeddings yet."""
        return [ep for ep in self._episodes if ep.embedding is None][:limit]

    def set_embedding(self, episode_id: str, embedding: List[float]):
        """Set embedding for an episode."""
        ep = self._get_by_id(episode_id)
        if ep:
            ep.embedding = embedding

    def _get_by_id(self, episode_id: str) -> Optional[Episode]:
        """Get episode by ID."""
        for ep in self._episodes:
            if ep.id == episode_id:
                return ep
        return None

    def _prune_oldest(self):
        """Remove oldest, lowest importance episode."""
        if not self._episodes:
            return

        # Find lowest importance among oldest 10%
        oldest = self._episodes[:max(1, len(self._episodes) // 10)]
        to_remove = min(oldest, key=lambda x: x.importance)

        self._episodes.remove(to_remove)

        # Clean up indices
        if to_remove.event_type in self._by_type:
            self._by_type[to_remove.event_type] = [
                eid for eid in self._by_type[to_remove.event_type] if eid != to_remove.id
            ]
        for p in to_remove.participants:
            if p in self._by_participant:
                self._by_participant[p] = [
                    eid for eid in self._by_participant[p] if eid != to_remove.id
                ]

    def stats(self) -> Dict[str, Any]:
        return {
            "total_episodes": len(self._episodes),
            "max_episodes": self.max_episodes,
            "event_types": len(self._by_type),
            "unique_participants": len(self._by_participant),
        }


class SemanticMemory:
    """
    Knowledge graph for factual beliefs.
    Stores subject-predicate-object triples with confidence and provenance.
    """

    def __init__(self):
        self._beliefs: Dict[str, Belief] = {}
        self._by_subject: Dict[str, List[str]] = {}
        self._by_predicate: Dict[str, List[str]] = {}
        self._by_object: Dict[str, List[str]] = {}

    def store(
        self,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float = 1.0,
        source: str = None,
        evidence: List[str] = None,
    ) -> str:
        """Store or update a belief."""
        # Create deterministic ID from triple
        triple_key = f"{subject}:{predicate}:{obj}"
        belief_id = hashlib.sha256(triple_key.encode()).hexdigest()[:12]

        # Check for existing belief
        if belief_id in self._beliefs:
            existing = self._beliefs[belief_id]
            # Update confidence (weighted average)
            existing.confidence = (existing.confidence + confidence) / 2
            if source and source not in (existing.evidence or []):
                existing.evidence.append(source)
            return belief_id

        belief = Belief(
            id=belief_id,
            subject=subject,
            predicate=predicate,
            object=obj,
            confidence=confidence,
            source=source,
            valid_from=time.time(),
            evidence=evidence or ([source] if source else []),
        )

        self._beliefs[belief_id] = belief

        # Index
        for idx, key in [(self._by_subject, subject), (self._by_predicate, predicate), (self._by_object, obj)]:
            if key not in idx:
                idx[key] = []
            idx[key].append(belief_id)

        return belief_id

    def query(
        self,
        subject: str = None,
        predicate: str = None,
        obj: str = None,
        min_confidence: float = 0.0,
    ) -> List[Belief]:
        """Query beliefs matching the pattern."""
        # Find candidate belief IDs
        candidates = None

        if subject and subject in self._by_subject:
            candidates = set(self._by_subject[subject])
        if predicate and predicate in self._by_predicate:
            pred_set = set(self._by_predicate[predicate])
            candidates = candidates & pred_set if candidates else pred_set
        if obj and obj in self._by_object:
            obj_set = set(self._by_object[obj])
            candidates = candidates & obj_set if candidates else obj_set

        if candidates is None:
            candidates = set(self._beliefs.keys())

        # Filter and return
        results = []
        now = time.time()
        for bid in candidates:
            belief = self._beliefs.get(bid)
            if not belief:
                continue
            if belief.confidence < min_confidence:
                continue
            if belief.valid_until and belief.valid_until < now:
                continue
            results.append(belief)

        return sorted(results, key=lambda x: x.confidence, reverse=True)

    def infer(self, subject: str, predicate: str) -> List[Tuple[str, float]]:
        """
        Simple inference: find objects that match subject+predicate,
        or transitively through shared predicates.
        """
        # Direct matches
        direct = self.query(subject=subject, predicate=predicate)
        results = [(b.object, b.confidence) for b in direct]

        # Simple transitive inference for 'is_a' predicate
        if predicate == "is_a":
            # Find what subject is_a, then what those things are_a
            for parent, conf in results[:]:
                grandparents = self.query(subject=parent, predicate="is_a")
                for gp in grandparents:
                    # Reduce confidence for transitive inference
                    results.append((gp.object, conf * gp.confidence * 0.8))

        return results

    def get_related(self, entity: str, limit: int = 20) -> Dict[str, List[Belief]]:
        """Get all beliefs related to an entity."""
        results = {
            "as_subject": self.query(subject=entity)[:limit],
            "as_object": self.query(obj=entity)[:limit],
        }
        return results

    def invalidate(self, belief_id: str, reason: str = None):
        """Mark a belief as no longer valid."""
        if belief_id in self._beliefs:
            self._beliefs[belief_id].valid_until = time.time()
            if reason:
                self._beliefs[belief_id].contradicts.append(reason)

    def stats(self) -> Dict[str, Any]:
        now = time.time()
        valid_beliefs = [b for b in self._beliefs.values() if not b.valid_until or b.valid_until > now]
        return {
            "total_beliefs": len(self._beliefs),
            "valid_beliefs": len(valid_beliefs),
            "unique_subjects": len(self._by_subject),
            "unique_predicates": len(self._by_predicate),
            "unique_objects": len(self._by_object),
            "avg_confidence": sum(b.confidence for b in valid_beliefs) / max(1, len(valid_beliefs)),
        }


class ProceduralMemory:
    """
    Stores executable skill patterns.
    Skills are learned from successful executions and can be applied in similar contexts.
    """

    def __init__(self):
        self._skills: Dict[str, Skill] = {}
        self._by_tag: Dict[str, List[str]] = {}

    def learn(
        self,
        name: str,
        description: str,
        pattern: Dict[str, Any],
        preconditions: List[str] = None,
        postconditions: List[str] = None,
        tags: List[str] = None,
    ) -> str:
        """Learn a new skill or update existing."""
        skill_id = hashlib.sha256(name.encode()).hexdigest()[:12]

        skill = Skill(
            id=skill_id,
            name=name,
            description=description,
            pattern=pattern,
            preconditions=preconditions or [],
            postconditions=postconditions or [],
        )

        self._skills[skill_id] = skill

        # Index by tags
        for tag in (tags or []):
            if tag not in self._by_tag:
                self._by_tag[tag] = []
            if skill_id not in self._by_tag[tag]:
                self._by_tag[tag].append(skill_id)

        return skill_id

    def get(self, skill_id: str) -> Optional[Skill]:
        """Get a skill by ID."""
        return self._skills.get(skill_id)

    def find_by_name(self, name: str) -> Optional[Skill]:
        """Find skill by name."""
        skill_id = hashlib.sha256(name.encode()).hexdigest()[:12]
        return self._skills.get(skill_id)

    def find_applicable(self, context: Dict[str, Any], tags: List[str] = None) -> List[Skill]:
        """Find skills whose preconditions might match the context."""
        candidates = []

        # Filter by tags if provided
        if tags:
            skill_ids = set()
            for tag in tags:
                skill_ids.update(self._by_tag.get(tag, []))
            candidates = [self._skills[sid] for sid in skill_ids if sid in self._skills]
        else:
            candidates = list(self._skills.values())

        # Sort by success rate
        def success_rate(s: Skill) -> float:
            total = s.success_count + s.failure_count
            if total == 0:
                return 0.5
            return s.success_count / total

        return sorted(candidates, key=success_rate, reverse=True)

    def record_execution(self, skill_id: str, success: bool, execution_time: float):
        """Record skill execution result."""
        if skill_id not in self._skills:
            return

        skill = self._skills[skill_id]
        skill.last_used = time.time()

        if success:
            skill.success_count += 1
        else:
            skill.failure_count += 1

        # Update average execution time
        total = skill.success_count + skill.failure_count
        skill.avg_execution_time = (
            (skill.avg_execution_time * (total - 1) + execution_time) / total
        )

    def stats(self) -> Dict[str, Any]:
        skills = list(self._skills.values())
        return {
            "total_skills": len(skills),
            "total_executions": sum(s.success_count + s.failure_count for s in skills),
            "avg_success_rate": sum(
                s.success_count / max(1, s.success_count + s.failure_count) for s in skills
            ) / max(1, len(skills)),
        }


# =============================================================================
# MCP SERVER
# =============================================================================

class DistributedMemoryMCP:
    """
    Native MCP server providing cognitive memory capabilities.
    """

    SERVICE_ID = "distributed-memory-mcp"
    SERVICE_NAME = "Distributed Memory MCP"
    VERSION = "1.0.0"
    MCP_VERSION = "2024-11-05"

    TOOLS = [
        {
            "name": "focus",
            "description": "Prime working memory with relevant context. Brings specified items into focus, boosting their activation.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "item_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Item IDs to focus on"
                    },
                    "content": {
                        "type": "string",
                        "description": "New content to add to working memory and focus on"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance level (0-1)",
                        "default": 0.7
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags for the new content"
                    }
                }
            }
        },
        {
            "name": "expand_context",
            "description": "Expand working memory context by retrieving related items from all memory tiers.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to expand context around"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum items to retrieve",
                        "default": 10
                    },
                    "include_episodic": {
                        "type": "boolean",
                        "description": "Include episodes",
                        "default": True
                    },
                    "include_semantic": {
                        "type": "boolean",
                        "description": "Include beliefs",
                        "default": True
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "record_episode",
            "description": "Record an event/episode in episodic memory.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "event_type": {
                        "type": "string",
                        "description": "Type of event (e.g., 'task_completed', 'error', 'interaction')"
                    },
                    "content": {
                        "type": "object",
                        "description": "Event content/details"
                    },
                    "context": {
                        "type": "object",
                        "description": "Contextual information"
                    },
                    "participants": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Entities involved"
                    },
                    "outcome": {
                        "type": "string",
                        "description": "Outcome of the event"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance (0-1)",
                        "default": 0.5
                    }
                },
                "required": ["event_type", "content"]
            }
        },
        {
            "name": "recall_episodes",
            "description": "Recall episodes from episodic memory by various criteria.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Similarity search query"
                    },
                    "event_type": {
                        "type": "string",
                        "description": "Filter by event type"
                    },
                    "participant": {
                        "type": "string",
                        "description": "Filter by participant"
                    },
                    "start_time": {
                        "type": "number",
                        "description": "Start of time range (unix timestamp)"
                    },
                    "end_time": {
                        "type": "number",
                        "description": "End of time range (unix timestamp)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum episodes to return",
                        "default": 20
                    }
                }
            }
        },
        {
            "name": "store_belief",
            "description": "Store a belief/fact in semantic memory as a subject-predicate-object triple.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {
                        "type": "string",
                        "description": "Subject of the belief"
                    },
                    "predicate": {
                        "type": "string",
                        "description": "Relationship/predicate"
                    },
                    "object": {
                        "type": "string",
                        "description": "Object of the belief"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "Confidence level (0-1)",
                        "default": 1.0
                    },
                    "source": {
                        "type": "string",
                        "description": "Source of the belief"
                    }
                },
                "required": ["subject", "predicate", "object"]
            }
        },
        {
            "name": "query_beliefs",
            "description": "Query semantic memory for beliefs matching a pattern.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {
                        "type": "string",
                        "description": "Subject to match (optional)"
                    },
                    "predicate": {
                        "type": "string",
                        "description": "Predicate to match (optional)"
                    },
                    "object": {
                        "type": "string",
                        "description": "Object to match (optional)"
                    },
                    "min_confidence": {
                        "type": "number",
                        "description": "Minimum confidence threshold",
                        "default": 0.0
                    },
                    "infer": {
                        "type": "boolean",
                        "description": "Enable inference for transitive relationships",
                        "default": False
                    }
                }
            }
        },
        {
            "name": "learn_skill",
            "description": "Store a procedural pattern/skill that can be applied later.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Skill name"
                    },
                    "description": {
                        "type": "string",
                        "description": "What the skill does"
                    },
                    "pattern": {
                        "type": "object",
                        "description": "The executable pattern (steps, parameters, etc.)"
                    },
                    "preconditions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Conditions that must be true to apply"
                    },
                    "postconditions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Conditions that will be true after"
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags for categorization"
                    }
                },
                "required": ["name", "description", "pattern"]
            }
        },
        {
            "name": "apply_skill",
            "description": "Retrieve and potentially execute a learned skill.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Skill name to apply"
                    },
                    "skill_id": {
                        "type": "string",
                        "description": "Skill ID (alternative to name)"
                    },
                    "context": {
                        "type": "object",
                        "description": "Current context for skill application"
                    },
                    "execute": {
                        "type": "boolean",
                        "description": "Whether to actually execute the skill's pattern steps via cross-MCP calls",
                        "default": False
                    },
                    "record_result": {
                        "type": "boolean",
                        "description": "Whether to record execution result",
                        "default": True
                    },
                    "success": {
                        "type": "boolean",
                        "description": "If recording, whether execution succeeded"
                    },
                    "execution_time": {
                        "type": "number",
                        "description": "If recording, execution time in seconds"
                    }
                }
            }
        },
        {
            "name": "consolidate",
            "description": "Consolidate memory: move important working memory items to long-term storage, prune old items.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "working_to_episodic": {
                        "type": "boolean",
                        "description": "Save important working memory as episodes",
                        "default": True
                    },
                    "importance_threshold": {
                        "type": "number",
                        "description": "Minimum importance to consolidate",
                        "default": 0.6
                    },
                    "prune_working": {
                        "type": "boolean",
                        "description": "Clear low-activation items from working memory",
                        "default": True
                    }
                }
            }
        },
        {
            "name": "forget",
            "description": "Explicitly forget/decay items from memory.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tier": {
                        "type": "string",
                        "enum": ["working", "episodic", "semantic", "procedural"],
                        "description": "Memory tier to forget from"
                    },
                    "id": {
                        "type": "string",
                        "description": "Single item ID to forget (alternative to item_ids)"
                    },
                    "item_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific item IDs to forget"
                    },
                    "older_than": {
                        "type": "number",
                        "description": "Forget items older than this (seconds ago)"
                    },
                    "below_importance": {
                        "type": "number",
                        "description": "Forget items below this importance"
                    }
                },
                "required": ["tier"]
            }
        },
        {
            "name": "search_all",
            "description": "Unified search across all memory tiers.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "tiers": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["working", "episodic", "semantic", "procedural"]},
                        "description": "Tiers to search (default: all)"
                    },
                    "limit_per_tier": {
                        "type": "integer",
                        "description": "Max results per tier",
                        "default": 5
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "get_memory_stats",
            "description": "Get statistics about memory usage across all tiers.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "semantic_search",
            "description": "Search memory using semantic embeddings from Search MCP. Finds conceptually similar content even without exact word matches.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query"
                    },
                    "tiers": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["episodic", "working"]},
                        "description": "Memory tiers to search (default: all with embeddings)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results",
                        "default": 10
                    },
                    "min_similarity": {
                        "type": "number",
                        "description": "Minimum cosine similarity threshold (0-1)",
                        "default": 0.3
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "_connectivity_test",
            "description": "Test connectivity to the Memory MCP server.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "semantic_search_beliefs",
            "description": "Search beliefs using semantic embeddings. Finds conceptually similar facts even without exact word matches. Uses Jina v3 embeddings for high-quality semantic understanding.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query to search for related beliefs"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results",
                        "default": 10
                    },
                    "min_similarity": {
                        "type": "number",
                        "description": "Minimum cosine similarity threshold (0-1)",
                        "default": 0.3
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "find_related_beliefs",
            "description": "Find all beliefs related to an entity, both exact matches and semantically similar.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "entity": {
                        "type": "string",
                        "description": "Entity to find related beliefs for"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results per category",
                        "default": 20
                    }
                },
                "required": ["entity"]
            }
        }
    ]

    def __init__(self, port: int = 4004, data_dir: str = None):
        self.port = port
        self.data_dir = Path(data_dir) if data_dir else Path("data/memory")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize memory stores
        self.working = WorkingMemory(capacity=100)
        self.episodic = EpisodicMemory(max_episodes=10000)
        self.semantic = SemanticMemory()  # In-memory for backward compat
        self.procedural = ProceduralMemory()

        # Semantic belief store with embeddings (persistent)
        self._semantic_belief_store: Optional[SemanticBeliefStore] = None
        if SEMANTIC_BELIEFS_AVAILABLE:
            try:
                self._semantic_belief_store = get_belief_store()
                logger.info("SemanticBeliefStore initialized with persistence and embeddings")
            except Exception as e:
                logger.warning(f"Failed to initialize SemanticBeliefStore: {e}")

        # Internal MCP client for cross-MCP calls (embeddings, etc.)
        self._mcp_client: Optional[InternalMCPClient] = None
        self._embeddings_available = False

        # FastAPI app
        self.app = self._create_app()

    async def _ensure_mcp_client(self) -> InternalMCPClient:
        """Ensure the internal MCP client is initialized."""
        if self._mcp_client is None:
            self._mcp_client = get_internal_client(
                source_mcp_id=self.SERVICE_ID,
                registry_url=os.getenv("REGISTRY_URL", "http://localhost:2016"),
            )
            # Check if embeddings are available
            await self._check_embedding_availability()
        return self._mcp_client

    async def _check_embedding_availability(self):
        """Check if embedding service is available."""
        if self._mcp_client:
            endpoints = await self._mcp_client.find_by_capability("embed")
            self._embeddings_available = len(endpoints) > 0
            if self._embeddings_available:
                logger.info("Embeddings available via Search MCP")
            else:
                logger.warning("Embeddings not available, falling back to text similarity")

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for text using Search MCP."""
        if not self._embeddings_available:
            return None

        client = await self._ensure_mcp_client()
        return await client.get_embedding(text)

    async def _get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts."""
        if not self._embeddings_available:
            return [None] * len(texts)

        client = await self._ensure_mcp_client()
        return await client.get_embeddings_batch(texts)

    async def _ensure_episode_embeddings(self, limit: int = 50):
        """Background task to compute embeddings for episodes without them."""
        if not self._embeddings_available:
            return

        episodes = self.episodic.get_episodes_without_embeddings(limit=limit)
        if not episodes:
            return

        # Get texts to embed
        texts = [str(ep.content) for ep in episodes]
        embeddings = await self._get_embeddings_batch(texts)

        # Update episodes with embeddings
        for ep, embedding in zip(episodes, embeddings):
            if embedding:
                self.episodic.set_embedding(ep.id, embedding)

        logger.debug(f"Computed embeddings for {sum(1 for e in embeddings if e)} episodes")

    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title=self.SERVICE_NAME,
            description="Cognitive memory architecture MCP",
            version=self.VERSION,
        )

        @app.on_event("startup")
        async def seed_memory():
            """Seed working memory with a boot episode."""
            self.episodic.record(
                event_type="system_boot",
                content={"message": "Memory MCP server started", "version": self.VERSION},
                importance=0.3,
            )
            self.working.add(
                content=f"Memory MCP v{self.VERSION} started",
                importance=0.3,
                tags=["system", "boot"],
            )
            logger.info("Seeded working memory with boot episode")

        @app.on_event("startup")
        async def announce():
            """Announce this server and its tools to Tool Management, then heartbeat every 60s."""
            import httpx
            import asyncio

            registry_url = os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
            service_url = os.getenv("SERVICE_URL", "https://arthurcolle--distributed-memory-mcp-web.modal.run")

            async def _do_announce():
                async with httpx.AsyncClient(timeout=15.0) as client:
                    response = await client.post(
                        f"{registry_url}/api/v1/tools/announce",
                        json={
                            "backend_id": self.SERVICE_ID,
                            "name": self.SERVICE_NAME,
                            "url": service_url,
                            "capabilities": [t["name"] for t in self.TOOLS],
                            "tools": self.TOOLS,
                        },
                        timeout=15.0,
                    )
                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"Announced {result.get('registered', 0)} memory tools to {registry_url}")
                    else:
                        logger.warning(f"Announce returned {response.status_code}: {response.text[:200]}")

            # Initial announce
            try:
                await _do_announce()
            except Exception as e:
                logger.warning(f"Could not announce to registry: {e}")

            # Background heartbeat loop
            async def _heartbeat_loop():
                await asyncio.sleep(30)
                while True:
                    try:
                        await _do_announce()
                        logger.debug("Heartbeat: re-announced to registry")
                    except Exception as e:
                        logger.warning(f"Heartbeat failed: {e}")
                    await asyncio.sleep(60)

            asyncio.create_task(_heartbeat_loop())

        @app.get("/")
        async def root():
            return {
                "service": self.SERVICE_NAME,
                "version": self.VERSION,
                "mcp_version": self.MCP_VERSION,
                "status": "running",
                "port": self.port,
            }

        @app.get("/health")
        async def health():
            return {"status": "healthy", "service": self.SERVICE_ID}

        @app.post("/mcp")
        async def mcp_handler(request: Request):
            body = await request.json()
            return await self._handle_mcp_request(body)

        @app.get("/tools")
        async def list_tools():
            return {"tools": self.TOOLS}

        @app.get("/mcp/sse")
        @app.get("/sse")
        async def sse_endpoint(request: Request):
            """Server-Sent Events endpoint for MCP streaming."""
            from fastapi.responses import StreamingResponse
            import asyncio

            async def event_generator():
                session_id = str(uuid.uuid4())
                yield f"data: {json.dumps({'type': 'connected', 'session_id': session_id})}\n\n"

                while True:
                    if await request.is_disconnected():
                        break
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': time.time()})}\n\n"
                    await asyncio.sleep(30)

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            )

        @app.post("/mcp/sse")
        @app.post("/sse")
        async def sse_post_endpoint(request: Request):
            """Handle MCP JSON-RPC POST requests via SSE URL."""
            body = await request.json()
            return await self._handle_mcp_request(body)

        return app

    async def _handle_mcp_request(self, body: Dict[str, Any]) -> JSONResponse:
        """Handle MCP JSON-RPC requests."""
        method = body.get("method", "")
        params = body.get("params", {})
        request_id = body.get("id")

        try:
            if method == "initialize":
                result = {
                    "protocolVersion": self.MCP_VERSION,
                    "serverInfo": {
                        "name": self.SERVICE_NAME,
                        "version": self.VERSION,
                    },
                    "capabilities": {
                        "tools": {"listChanged": True},
                    },
                }
            elif method == "tools/list":
                result = {"tools": [t for t in self.TOOLS if not t["name"].startswith("_")]}
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await self._execute_tool(tool_name, arguments)
            else:
                return JSONResponse({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Unknown method: {method}"},
                })

            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            })

        except Exception as e:
            logger.error(f"MCP error: {e}")
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(e)},
            })

    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool by name."""
        handlers = {
            "focus": self._tool_focus,
            "expand_context": self._tool_expand_context,
            "record_episode": self._tool_record_episode,
            "recall_episodes": self._tool_recall_episodes,
            "store_belief": self._tool_store_belief,
            "query_beliefs": self._tool_query_beliefs,
            "learn_skill": self._tool_learn_skill,
            "apply_skill": self._tool_apply_skill,
            "consolidate": self._tool_consolidate,
            "forget": self._tool_forget,
            "search_all": self._tool_search_all,
            "semantic_search": self._tool_semantic_search,
            "get_memory_stats": self._tool_get_memory_stats,
            "_connectivity_test": self._tool_connectivity_test,
            "semantic_search_beliefs": self._tool_semantic_search_beliefs,
            "find_related_beliefs": self._tool_find_related_beliefs,
        }

        handler = handlers.get(tool_name)
        if not handler:
            return {"content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}], "isError": True}

        result = await handler(arguments)

        # Apply output guard: enforce size limits, cap links, capture URLs
        if OUTPUT_GUARD_AVAILABLE:
            result = guard_mcp_content(
                result,
                tool_name=tool_name,
                source_context={"server": self.SERVICE_ID},
            )

        return result

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    async def _tool_focus(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Focus working memory on specific items or new content."""
        item_ids = args.get("item_ids", [])
        content = args.get("content")
        importance = args.get("importance", 0.7)
        tags = args.get("tags", [])

        focused_ids = []

        # Add new content if provided
        if content:
            new_id = self.working.add(content, importance=importance, tags=tags)
            item_ids.append(new_id)
            focused_ids.append(new_id)

        # Focus on specified items
        if item_ids:
            self.working.focus(item_ids)
            focused_ids.extend(item_ids)

        context = self.working.get_context(limit=10)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "focused_ids": focused_ids,
                    "context_size": len(context),
                    "current_context": [
                        {"id": item.id, "content": item.content[:200] if isinstance(item.content, str) else str(item.content)[:200]}
                        for item in context[:5]
                    ]
                }, indent=2)
            }]
        }

    async def _tool_expand_context(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Expand context by searching all memory tiers."""
        query = args["query"]
        limit = args.get("limit", 10)
        include_episodic = args.get("include_episodic", True)
        include_semantic = args.get("include_semantic", True)
        used_embeddings = False

        results = {
            "working": [],
            "episodic": [],
            "semantic": [],
        }

        # Search working memory
        working_items = self.working.search(query, limit=limit)
        results["working"] = [
            {"id": i.id, "content": i.content, "activation": i.get_activation()}
            for i in working_items
        ]

        # Search episodic memory with embeddings if available
        if include_episodic:
            episodes = []
            await self._ensure_mcp_client()

            if self._embeddings_available:
                query_embedding = await self._get_embedding(query)
                if query_embedding:
                    await self._ensure_episode_embeddings(limit=100)
                    ep_results = self.episodic.recall_by_embedding(query_embedding, limit=limit)
                    episodes = [ep for ep, _ in ep_results]
                    used_embeddings = True

            if not episodes:
                episodes = self.episodic.recall_by_similarity_text(query, limit=limit)

            results["episodic"] = [
                {"id": e.id, "type": e.event_type, "content": e.content, "timestamp": e.timestamp}
                for e in episodes
            ]

        # Search semantic memory
        if include_semantic:
            # Try to extract subject from query
            words = query.split()
            for word in words:
                beliefs = self.semantic.query(subject=word)
                beliefs.extend(self.semantic.query(obj=word))
                results["semantic"].extend([
                    {"id": b.id, "triple": f"{b.subject} {b.predicate} {b.object}", "confidence": b.confidence}
                    for b in beliefs[:limit]
                ])

        # Add top results to working memory
        for item in working_items[:3]:
            item.touch()

        # Inject top episodic results into working memory so it's not always empty
        for ep_entry in results.get("episodic", [])[:3]:
            self.working.add(
                content=ep_entry.get("content", ""),
                importance=0.5,
                tags=["expanded_context", ep_entry.get("type", "episode")],
            )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "query": query,
                    "results": results,
                    "total_found": sum(len(v) for v in results.values())
                }, indent=2)
            }]
        }

    async def _tool_record_episode(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Record an episode to episodic memory."""
        event_type = args.get("event_type")
        if not event_type:
            return {
                "content": [{"type": "text", "text": json.dumps({"error": "event_type is required"})}],
                "isError": True,
            }
        content = args.get("content")
        if content is None:
            return {
                "content": [{"type": "text", "text": json.dumps({"error": "content is required"})}],
                "isError": True,
            }
        episode_id = self.episodic.record(
            event_type=event_type,
            content=content,
            context=args.get("context"),
            participants=args.get("participants"),
            outcome=args.get("outcome"),
            importance=args.get("importance", 0.5),
        )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "recorded": True,
                    "episode_id": episode_id,
                    "event_type": args["event_type"],
                    "total_episodes": len(self.episodic._episodes)
                }, indent=2)
            }]
        }

    async def _tool_recall_episodes(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Recall episodes from episodic memory."""
        limit = args.get("limit", 20)
        episodes = []
        similarity_scores = {}
        used_embeddings = False

        if args.get("query"):
            query = args["query"]

            # Try embedding-based similarity first
            await self._ensure_mcp_client()
            if self._embeddings_available:
                query_embedding = await self._get_embedding(query)
                if query_embedding:
                    # Also ensure episodes have embeddings
                    await self._ensure_episode_embeddings(limit=100)

                    results = self.episodic.recall_by_embedding(
                        query_embedding, limit=limit, min_similarity=0.3
                    )
                    episodes = [ep for ep, score in results]
                    similarity_scores = {ep.id: score for ep, score in results}
                    used_embeddings = True

            # Fallback to text similarity
            if not episodes:
                episodes = self.episodic.recall_by_similarity_text(query, limit=limit)

        elif args.get("event_type"):
            episodes = self.episodic.recall_by_type(args["event_type"], limit=limit)
        elif args.get("participant"):
            episodes = self.episodic.recall_by_participant(args["participant"], limit=limit)
        else:
            episodes = self.episodic.recall_by_time(
                start_time=args.get("start_time"),
                end_time=args.get("end_time"),
                limit=limit,
            )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "count": len(episodes),
                    "used_embeddings": used_embeddings,
                    "episodes": [
                        {
                            "id": e.id,
                            "event_type": e.event_type,
                            "content": e.content,
                            "timestamp": e.timestamp,
                            "participants": e.participants,
                            "outcome": e.outcome,
                            "similarity": similarity_scores.get(e.id),
                        }
                        for e in episodes
                    ]
                }, indent=2)
            }]
        }

    async def _tool_store_belief(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Store a belief in semantic memory (both in-memory and persistent with embeddings)."""
        # Store in in-memory semantic memory for backward compatibility
        belief_id = self.semantic.store(
            subject=args["subject"],
            predicate=args["predicate"],
            obj=args["object"],
            confidence=args.get("confidence", 1.0),
            source=args.get("source"),
        )

        # Also store in persistent semantic belief store with embeddings
        persistent_stored = False
        if self._semantic_belief_store:
            try:
                await self._semantic_belief_store.store(
                    subject=args["subject"],
                    predicate=args["predicate"],
                    obj=args["object"],
                    confidence=args.get("confidence", 1.0),
                    source=args.get("source"),
                )
                persistent_stored = True
            except Exception as e:
                logger.warning(f"Failed to store belief in persistent store: {e}")

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "stored": True,
                    "belief_id": belief_id,
                    "triple": f"{args['subject']} {args['predicate']} {args['object']}",
                    "total_beliefs": len(self.semantic._beliefs),
                    "persistent": persistent_stored,
                    "embeddings_enabled": persistent_stored,
                }, indent=2)
            }]
        }

    async def _tool_query_beliefs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Query semantic memory."""
        if args.get("infer") and args.get("subject") and args.get("predicate"):
            # Use inference
            results = self.semantic.infer(args["subject"], args["predicate"])
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "query": {"subject": args.get("subject"), "predicate": args.get("predicate")},
                        "inferred": True,
                        "results": [{"object": obj, "confidence": conf} for obj, conf in results]
                    }, indent=2)
                }]
            }

        beliefs = self.semantic.query(
            subject=args.get("subject"),
            predicate=args.get("predicate"),
            obj=args.get("object"),
            min_confidence=args.get("min_confidence", 0.0),
        )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "count": len(beliefs),
                    "beliefs": [
                        {
                            "id": b.id,
                            "triple": f"{b.subject} {b.predicate} {b.object}",
                            "confidence": b.confidence,
                            "source": b.source,
                        }
                        for b in beliefs
                    ]
                }, indent=2)
            }]
        }

    async def _tool_semantic_search_beliefs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search beliefs using semantic embeddings."""
        query = args["query"]
        limit = args.get("limit", 10)
        min_similarity = args.get("min_similarity", 0.3)

        if not self._semantic_belief_store:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": "Semantic belief store not available",
                        "message": "SemanticBeliefStore is not initialized. Falling back to exact match query.",
                        "fallback_available": True,
                    }, indent=2)
                }],
                "isError": True,
            }

        try:
            results = await self._semantic_belief_store.semantic_search(
                query=query,
                limit=limit,
                min_similarity=min_similarity,
            )

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "query": query,
                        "count": len(results),
                        "embeddings_used": True,
                        "results": [
                            {
                                "belief": {
                                    "id": belief.id,
                                    "subject": belief.subject,
                                    "predicate": belief.predicate,
                                    "object": belief.object,
                                    "confidence": belief.confidence,
                                    "source": belief.source,
                                },
                                "triple": f"{belief.subject} {belief.predicate} {belief.object}",
                                "similarity": similarity,
                            }
                            for belief, similarity in results
                        ]
                    }, indent=2)
                }]
            }

        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "message": "Semantic search failed",
                    }, indent=2)
                }],
                "isError": True,
            }

    async def _tool_find_related_beliefs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Find beliefs related to an entity (exact + semantic)."""
        entity = args["entity"]
        limit = args.get("limit", 20)

        if not self._semantic_belief_store:
            # Fallback to in-memory semantic memory
            related = self.semantic.get_related(entity, limit=limit)
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "entity": entity,
                        "persistent_store": False,
                        "exact_matches": {
                            "as_subject": [
                                {
                                    "id": b.id,
                                    "triple": f"{b.subject} {b.predicate} {b.object}",
                                    "confidence": b.confidence,
                                }
                                for b in related["as_subject"]
                            ],
                            "as_object": [
                                {
                                    "id": b.id,
                                    "triple": f"{b.subject} {b.predicate} {b.object}",
                                    "confidence": b.confidence,
                                }
                                for b in related["as_object"]
                            ],
                        },
                        "semantic_matches": [],
                    }, indent=2)
                }]
            }

        try:
            results = await self._semantic_belief_store.find_related(
                entity=entity,
                limit=limit,
            )

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "entity": entity,
                        "persistent_store": True,
                        "exact_matches": results["exact"],
                        "semantic_matches": results["semantic"],
                    }, indent=2)
                }]
            }

        except Exception as e:
            logger.error(f"Find related failed: {e}")
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "message": "Find related beliefs failed",
                    }, indent=2)
                }],
                "isError": True,
            }

    async def _tool_learn_skill(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Learn a new skill."""
        skill_id = self.procedural.learn(
            name=args["name"],
            description=args["description"],
            pattern=args["pattern"],
            preconditions=args.get("preconditions"),
            postconditions=args.get("postconditions"),
            tags=args.get("tags"),
        )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "learned": True,
                    "skill_id": skill_id,
                    "name": args["name"],
                    "total_skills": len(self.procedural._skills)
                }, indent=2)
            }]
        }

    async def _tool_apply_skill(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply/retrieve a skill, optionally executing its pattern steps."""
        skill = None

        if args.get("skill_id"):
            skill = self.procedural.get(args["skill_id"])
        elif args.get("name"):
            skill = self.procedural.find_by_name(args["name"])

        if not skill:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({"error": "Skill not found"}, indent=2)
                }],
                "isError": True
            }

        # Record execution result if provided
        if args.get("record_result") and args.get("success") is not None:
            self.procedural.record_execution(
                skill.id,
                args["success"],
                args.get("execution_time", 0.0)
            )

        # Execute skill pattern steps if requested
        execution_results = None
        if args.get("execute"):
            steps = skill.pattern.get("steps", [])
            if steps:
                execution_results = []
                start_time = time.time()
                prev_output = args.get("context", {})
                success = True
                try:
                    client = await self._ensure_mcp_client()
                    for i, step in enumerate(steps):
                        step_tool = step.get("tool", "")
                        step_args = step.get("arguments", {})
                        # Inject previous step output as context
                        if prev_output and isinstance(step_args, dict):
                            step_args["_prev_output"] = prev_output
                        step_result = await client.call_tool(
                            tool_name=step_tool,
                            arguments=step_args,
                        )
                        step_entry = {
                            "step": i,
                            "tool": step_tool,
                            "success": not step_result.get("isError", False),
                        }
                        execution_results.append(step_entry)
                        if step_result.get("isError"):
                            success = False
                            break
                        # Parse output for next step
                        content = step_result.get("content", [])
                        if content and isinstance(content, list):
                            text_content = content[0].get("text", "{}")
                            try:
                                prev_output = json.loads(text_content)
                            except (json.JSONDecodeError, TypeError):
                                prev_output = {"raw": text_content}
                except Exception as e:
                    success = False
                    execution_results.append({"error": str(e)})

                exec_time = time.time() - start_time
                self.procedural.record_execution(skill.id, success, exec_time)

        result_data = {
            "skill_id": skill.id,
            "name": skill.name,
            "description": skill.description,
            "pattern": skill.pattern,
            "preconditions": skill.preconditions,
            "postconditions": skill.postconditions,
            "success_rate": skill.success_count / max(1, skill.success_count + skill.failure_count),
            "avg_execution_time": skill.avg_execution_time,
        }
        if execution_results is not None:
            result_data["executed"] = True
            result_data["execution_results"] = execution_results

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result_data, indent=2)
            }]
        }

    async def _tool_consolidate(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Consolidate memory."""
        consolidated = {"working_to_episodic": 0, "pruned_working": 0}
        importance_threshold = args.get("importance_threshold", 0.6)

        if args.get("working_to_episodic", True):
            # Save important working memory items as episodes
            for item in list(self.working._items.values()):
                if item.importance >= importance_threshold:
                    self.episodic.record(
                        event_type="consolidated_working_memory",
                        content=item.content,
                        context=item.metadata,
                        importance=item.importance,
                    )
                    consolidated["working_to_episodic"] += 1

        if args.get("prune_working", True):
            # Remove low-activation items
            initial_count = len(self.working._items)
            now = time.time()
            to_remove = [
                item_id for item_id, item in self.working._items.items()
                if item.get_activation(now) < 0.1 and item_id not in self.working._focus_items
            ]
            for item_id in to_remove:
                del self.working._items[item_id]
            consolidated["pruned_working"] = initial_count - len(self.working._items)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "consolidated": True,
                    "actions": consolidated,
                    "working_memory_size": len(self.working._items),
                    "episodic_memory_size": len(self.episodic._episodes),
                }, indent=2)
            }]
        }

    async def _tool_forget(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Explicitly forget items."""
        tier = args["tier"]
        forgotten = 0

        # Normalize: accept both "id" (singular) and "item_ids" (list)
        item_ids = args.get("item_ids", [])
        if args.get("id"):
            item_ids = [args["id"]] + item_ids

        older_than = args.get("older_than")
        cutoff_time = (time.time() - older_than) if older_than else None

        if tier == "working":
            if item_ids:
                for item_id in item_ids:
                    if item_id in self.working._items:
                        del self.working._items[item_id]
                        self.working._focus_items.discard(item_id)
                        forgotten += 1
            elif args.get("below_importance"):
                to_remove = [
                    item_id for item_id, item in self.working._items.items()
                    if item.importance < args["below_importance"]
                ]
                for item_id in to_remove:
                    del self.working._items[item_id]
                    self.working._focus_items.discard(item_id)
                    forgotten += 1
            elif cutoff_time:
                to_remove = [
                    item_id for item_id, item in self.working._items.items()
                    if item.created_at < cutoff_time
                ]
                for item_id in to_remove:
                    del self.working._items[item_id]
                    self.working._focus_items.discard(item_id)
                    forgotten += 1

        elif tier == "episodic":
            if item_ids:
                for ep_id in item_ids:
                    ep = self.episodic._get_by_id(ep_id)
                    if ep:
                        self.episodic._episodes.remove(ep)
                        forgotten += 1
            elif cutoff_time:
                to_remove = [ep for ep in self.episodic._episodes if ep.timestamp < cutoff_time]
                for ep in to_remove:
                    self.episodic._episodes.remove(ep)
                    forgotten += 1

        elif tier == "semantic":
            if item_ids:
                for belief_id in item_ids:
                    if belief_id in self.semantic._beliefs:
                        self.semantic.invalidate(belief_id, "explicit_forget")
                        forgotten += 1

        elif tier == "procedural":
            if item_ids:
                for skill_id in item_ids:
                    if skill_id in self.procedural._skills:
                        del self.procedural._skills[skill_id]
                        forgotten += 1

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "tier": tier,
                    "forgotten": forgotten,
                }, indent=2)
            }]
        }

    async def _tool_search_all(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Unified search across all tiers with normalized relevance scores."""
        query = args["query"]
        tiers = args.get("tiers", ["working", "episodic", "semantic", "procedural"])
        limit = args.get("limit_per_tier", 5)

        results = {}

        if "working" in tiers:
            items = self.working.search(query, limit=limit)
            max_activation = max((i.get_activation() for i in items), default=1.0) or 1.0
            results["working"] = [
                {
                    "id": i.id,
                    "content": str(i.content)[:200],
                    "activation": i.get_activation(),
                    "relevance": i.get_activation() / max_activation,
                }
                for i in items
            ]

        if "episodic" in tiers:
            episodes = []
            ep_scores = {}
            # Try embeddings first
            await self._ensure_mcp_client()
            if self._embeddings_available:
                query_embedding = await self._get_embedding(query)
                if query_embedding:
                    ep_results = self.episodic.recall_by_embedding(query_embedding, limit=limit)
                    episodes = [ep for ep, _ in ep_results]
                    ep_scores = {ep.id: score for ep, score in ep_results}

            if not episodes:
                episodes = self.episodic.recall_by_similarity_text(query, limit=limit)

            results["episodic"] = [
                {
                    "id": e.id,
                    "type": e.event_type,
                    "content": str(e.content)[:200],
                    "relevance": ep_scores.get(e.id, e.importance),
                }
                for e in episodes
            ]

        if "semantic" in tiers:
            # Search by subject and object
            beliefs = []
            for word in query.split()[:3]:  # Limit to first 3 words
                beliefs.extend(self.semantic.query(subject=word)[:limit])
                beliefs.extend(self.semantic.query(obj=word)[:limit])
            seen = set()
            unique_beliefs = []
            for b in beliefs:
                if b.id not in seen:
                    seen.add(b.id)
                    unique_beliefs.append(b)
            results["semantic"] = [
                {
                    "id": b.id,
                    "triple": f"{b.subject} {b.predicate} {b.object}",
                    "confidence": b.confidence,
                    "relevance": b.confidence,
                }
                for b in unique_beliefs[:limit]
            ]

        if "procedural" in tiers:
            skills = self.procedural.find_applicable({}, tags=query.split())
            results["procedural"] = [
                {
                    "id": s.id,
                    "name": s.name,
                    "description": s.description[:100],
                    "relevance": s.success_count / max(1, s.success_count + s.failure_count),
                }
                for s in skills[:limit]
            ]

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "query": query,
                    "results": results,
                    "total": sum(len(v) for v in results.values())
                }, indent=2)
            }]
        }

    async def _tool_semantic_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search memory using semantic embeddings."""
        query = args["query"]
        tiers = args.get("tiers", ["episodic", "working"])
        limit = args.get("limit", 10)
        min_similarity = args.get("min_similarity", 0.3)

        await self._ensure_mcp_client()

        if not self._embeddings_available:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": "Embeddings not available",
                        "message": "Search MCP is not running or embed capability not found. Falling back to text search.",
                        "fallback_available": True,
                    }, indent=2)
                }],
                "isError": True,
            }

        # Get query embedding
        query_embedding = await self._get_embedding(query)
        if not query_embedding:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": "Failed to get query embedding",
                        "message": "Could not compute embedding for query",
                    }, indent=2)
                }],
                "isError": True,
            }

        results = {
            "query": query,
            "embeddings_used": True,
            "results": {},
        }

        # Search episodic memory
        if "episodic" in tiers:
            await self._ensure_episode_embeddings(limit=200)
            ep_results = self.episodic.recall_by_embedding(
                query_embedding, limit=limit, min_similarity=min_similarity
            )
            results["results"]["episodic"] = [
                {
                    "id": ep.id,
                    "event_type": ep.event_type,
                    "content": ep.content,
                    "timestamp": ep.timestamp,
                    "similarity": score,
                }
                for ep, score in ep_results
            ]

        # Search working memory (compute embeddings on-the-fly)
        if "working" in tiers:
            working_results = []
            for item in self.working._items.values():
                item_text = str(item.content)
                item_embedding = await self._get_embedding(item_text)
                if item_embedding:
                    sim = cosine_similarity(query_embedding, item_embedding)
                    if sim >= min_similarity:
                        working_results.append({
                            "id": item.id,
                            "content": item.content,
                            "activation": item.get_activation(),
                            "similarity": sim,
                        })

            # Sort by similarity
            working_results.sort(key=lambda x: x["similarity"], reverse=True)
            results["results"]["working"] = working_results[:limit]

        results["total_found"] = sum(len(v) for v in results["results"].values())

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(results, indent=2)
            }]
        }

    async def _tool_get_memory_stats(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get memory statistics."""
        await self._ensure_mcp_client()

        # Count episodes with embeddings
        episodes_with_embeddings = sum(1 for ep in self.episodic._episodes if ep.embedding is not None)

        stats = {
            "working": self.working.stats(),
            "episodic": {
                **self.episodic.stats(),
                "episodes_with_embeddings": episodes_with_embeddings,
            },
            "semantic": self.semantic.stats(),
            "procedural": self.procedural.stats(),
            "embeddings": {
                "available": self._embeddings_available,
                "source": "distributed-search-mcp" if self._embeddings_available else None,
            },
        }

        # Add persistent belief store stats if available
        if self._semantic_belief_store:
            stats["semantic_belief_store"] = {
                "available": True,
                "persistent": True,
                **self._semantic_belief_store.stats(),
            }
        else:
            stats["semantic_belief_store"] = {
                "available": False,
                "persistent": False,
            }

        # Add MCP client metrics if available
        if self._mcp_client:
            stats["cross_mcp_metrics"] = self._mcp_client.get_metrics()

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(stats, indent=2)
            }]
        }

    async def _tool_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Test connectivity."""
        await self._ensure_mcp_client()

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "service": self.SERVICE_ID,
                    "status": "connected",
                    "version": self.VERSION,
                    "memory_stats": {
                        "working": self.working.stats(),
                        "episodic": self.episodic.stats(),
                        "semantic": self.semantic.stats(),
                        "procedural": self.procedural.stats(),
                    },
                    "cross_mcp": {
                        "embeddings_available": self._embeddings_available,
                        "embedding_source": "distributed-search-mcp" if self._embeddings_available else None,
                        "client_initialized": self._mcp_client is not None,
                    },
                    "semantic_belief_store": {
                        "available": self._semantic_belief_store is not None,
                        "persistent": self._semantic_belief_store is not None,
                    }
                }, indent=2)
            }]
        }

    def run(self):
        """Run the server."""
        uvicorn.run(self.app, host="0.0.0.0", port=self.port)


# =============================================================================
# MODULE-LEVEL APP (for Modal and other ASGI servers)
# =============================================================================

def create_app() -> FastAPI:
    """Create the Memory MCP FastAPI app."""
    port = int(os.getenv("PORT", os.getenv("MEMORY_MCP_PORT", "4004")))
    data_dir = os.getenv("MEMORY_DATA_DIR", "data/memory")

    server = DistributedMemoryMCP(port=port, data_dir=data_dir)
    return server.app


# Module-level app instance for ASGI servers
app = create_app()


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Entry point for running the Memory MCP server."""
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Memory MCP Server")
    parser.add_argument("--port", type=int, default=int(os.getenv("MEMORY_MCP_PORT", "4004")))
    parser.add_argument("--data-dir", type=str, default=os.getenv("MEMORY_DATA_DIR", "data/memory"))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    server = DistributedMemoryMCP(port=args.port, data_dir=args.data_dir)
    logger.info(f"Starting {server.SERVICE_NAME} on port {args.port}")
    server.run()


if __name__ == "__main__":
    main()
