"""
Distributed Memory MCP Server v2 - Using NativeMCPBase.

This is a refactored version that demonstrates the cleaner pattern
when using the base class infrastructure.

Port: 4004
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

from native.base import (
    NativeMCPBase,
    EmbeddingMixin,
    CapabilityType,
    cosine_similarity,
)

logger = logging.getLogger(__name__)


# =============================================================================
# MEMORY DATA STRUCTURES
# =============================================================================

@dataclass
class MemoryItem:
    """Item in working memory with activation dynamics."""
    id: str
    content: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    importance: float = 0.5
    decay_rate: float = 0.1
    tags: List[str] = field(default_factory=list)
    embedding: Optional[List[float]] = None

    def touch(self):
        self.accessed_at = time.time()
        self.access_count += 1

    def get_activation(self, t: Optional[float] = None) -> float:
        t = t or time.time()
        recency = math.exp(-self.decay_rate * (t - self.accessed_at))
        frequency = math.log(1 + self.access_count)
        return self.importance * recency * (1 + 0.1 * frequency)


@dataclass
class Episode:
    """Episodic memory entry."""
    id: str
    event_type: str
    content: Any
    timestamp: float
    context: Dict[str, Any] = field(default_factory=dict)
    participants: List[str] = field(default_factory=list)
    outcome: Optional[str] = None
    importance: float = 0.5
    embedding: Optional[List[float]] = None


@dataclass
class Belief:
    """Semantic memory belief (SPO triple)."""
    id: str
    subject: str
    predicate: str
    object: str
    confidence: float = 1.0
    source: Optional[str] = None
    valid_until: Optional[float] = None


@dataclass
class Skill:
    """Procedural memory skill."""
    id: str
    name: str
    description: str
    pattern: Dict[str, Any]
    success_count: int = 0
    failure_count: int = 0


# =============================================================================
# MEMORY STORES
# =============================================================================

class WorkingMemory:
    """LRU working memory with activation decay."""

    def __init__(self, capacity: int = 100):
        self.capacity = capacity
        self._items: OrderedDict[str, MemoryItem] = OrderedDict()
        self._focus: Set[str] = set()

    def add(self, content: Any, importance: float = 0.5, tags: List[str] = None) -> str:
        item_id = str(uuid.uuid4())[:8]
        now = time.time()

        while len(self._items) >= self.capacity:
            self._evict()

        self._items[item_id] = MemoryItem(
            id=item_id,
            content=content,
            created_at=now,
            accessed_at=now,
            importance=importance,
            tags=tags or [],
        )
        return item_id

    def get(self, item_id: str) -> Optional[MemoryItem]:
        if item_id in self._items:
            self._items[item_id].touch()
            self._items.move_to_end(item_id)
            return self._items[item_id]
        return None

    def focus(self, item_ids: List[str]):
        self._focus = set(item_ids)
        for item_id in item_ids:
            if item_id in self._items:
                self._items[item_id].importance = min(1.0, self._items[item_id].importance + 0.2)
                self._items[item_id].touch()

    def get_context(self, limit: int = 10) -> List[MemoryItem]:
        items = sorted(self._items.values(), key=lambda x: x.get_activation(), reverse=True)
        return items[:limit]

    def search_text(self, query: str, limit: int = 10) -> List[MemoryItem]:
        query_lower = query.lower()
        results = [i for i in self._items.values() if query_lower in str(i.content).lower()]
        return sorted(results, key=lambda x: x.get_activation(), reverse=True)[:limit]

    def search_embedding(
        self,
        query_embedding: List[float],
        limit: int = 10,
        min_sim: float = 0.3,
    ) -> List[Tuple[MemoryItem, float]]:
        results = []
        for item in self._items.values():
            if item.embedding:
                sim = cosine_similarity(query_embedding, item.embedding)
                if sim >= min_sim:
                    results.append((item, sim))
        return sorted(results, key=lambda x: x[1], reverse=True)[:limit]

    def _evict(self):
        if not self._items:
            return
        # Evict lowest activation non-focused item
        candidates = [i for i in self._items.values() if i.id not in self._focus]
        if not candidates:
            candidates = list(self._items.values())
        victim = min(candidates, key=lambda x: x.get_activation())
        del self._items[victim.id]

    def stats(self) -> Dict[str, Any]:
        with_emb = sum(1 for i in self._items.values() if i.embedding)
        return {"size": len(self._items), "capacity": self.capacity, "with_embeddings": with_emb}


class EpisodicMemory:
    """Time-indexed episodic memory."""

    def __init__(self, max_episodes: int = 10000):
        self.max_episodes = max_episodes
        self._episodes: List[Episode] = []
        self._by_type: Dict[str, List[str]] = {}

    def record(self, event_type: str, content: Any, **kwargs) -> str:
        episode_id = str(uuid.uuid4())[:12]
        episode = Episode(
            id=episode_id,
            event_type=event_type,
            content=content,
            timestamp=time.time(),
            context=kwargs.get("context", {}),
            participants=kwargs.get("participants", []),
            outcome=kwargs.get("outcome"),
            importance=kwargs.get("importance", 0.5),
        )

        self._episodes.append(episode)

        if event_type not in self._by_type:
            self._by_type[event_type] = []
        self._by_type[event_type].append(episode_id)

        while len(self._episodes) > self.max_episodes:
            self._prune()

        return episode_id

    def recall_by_time(self, limit: int = 50, **kwargs) -> List[Episode]:
        return list(reversed(self._episodes[-limit:]))

    def recall_by_type(self, event_type: str, limit: int = 50) -> List[Episode]:
        ids = self._by_type.get(event_type, [])[-limit:]
        return [self._get(eid) for eid in ids if self._get(eid)]

    def recall_by_embedding(
        self,
        query_embedding: List[float],
        limit: int = 10,
        min_sim: float = 0.3,
    ) -> List[Tuple[Episode, float]]:
        results = []
        for ep in self._episodes:
            if ep.embedding:
                sim = cosine_similarity(query_embedding, ep.embedding)
                if sim >= min_sim:
                    results.append((ep, sim * ep.importance))
        return sorted(results, key=lambda x: x[1], reverse=True)[:limit]

    def recall_by_text(self, query: str, limit: int = 10) -> List[Episode]:
        query_lower = query.lower()
        results = []
        for ep in self._episodes:
            if query_lower in str(ep.content).lower():
                results.append(ep)
        return results[-limit:]

    def get_without_embeddings(self, limit: int = 100) -> List[Episode]:
        return [ep for ep in self._episodes if ep.embedding is None][:limit]

    def set_embedding(self, episode_id: str, embedding: List[float]):
        ep = self._get(episode_id)
        if ep:
            ep.embedding = embedding

    def _get(self, episode_id: str) -> Optional[Episode]:
        for ep in self._episodes:
            if ep.id == episode_id:
                return ep
        return None

    def _prune(self):
        if self._episodes:
            oldest = self._episodes[:max(1, len(self._episodes) // 10)]
            victim = min(oldest, key=lambda x: x.importance)
            self._episodes.remove(victim)

    def stats(self) -> Dict[str, Any]:
        with_emb = sum(1 for ep in self._episodes if ep.embedding)
        return {"total": len(self._episodes), "with_embeddings": with_emb, "types": len(self._by_type)}


class SemanticMemory:
    """Knowledge graph for beliefs."""

    def __init__(self):
        self._beliefs: Dict[str, Belief] = {}
        self._by_subject: Dict[str, List[str]] = {}
        self._by_predicate: Dict[str, List[str]] = {}

    def store(self, subject: str, predicate: str, obj: str, **kwargs) -> str:
        triple_key = f"{subject}:{predicate}:{obj}"
        belief_id = hashlib.sha256(triple_key.encode()).hexdigest()[:12]

        if belief_id in self._beliefs:
            self._beliefs[belief_id].confidence = (
                self._beliefs[belief_id].confidence + kwargs.get("confidence", 1.0)
            ) / 2
            return belief_id

        belief = Belief(
            id=belief_id,
            subject=subject,
            predicate=predicate,
            object=obj,
            confidence=kwargs.get("confidence", 1.0),
            source=kwargs.get("source"),
        )

        self._beliefs[belief_id] = belief

        for idx, key in [(self._by_subject, subject), (self._by_predicate, predicate)]:
            if key not in idx:
                idx[key] = []
            idx[key].append(belief_id)

        return belief_id

    def query(self, subject: str = None, predicate: str = None, obj: str = None) -> List[Belief]:
        candidates = set(self._beliefs.keys())

        if subject and subject in self._by_subject:
            candidates &= set(self._by_subject[subject])
        if predicate and predicate in self._by_predicate:
            candidates &= set(self._by_predicate[predicate])

        now = time.time()
        return [
            self._beliefs[bid] for bid in candidates
            if not self._beliefs[bid].valid_until or self._beliefs[bid].valid_until > now
        ]

    def stats(self) -> Dict[str, Any]:
        return {"total": len(self._beliefs), "subjects": len(self._by_subject)}


class ProceduralMemory:
    """Skill/pattern storage."""

    def __init__(self):
        self._skills: Dict[str, Skill] = {}

    def learn(self, name: str, description: str, pattern: Dict[str, Any]) -> str:
        skill_id = hashlib.sha256(name.encode()).hexdigest()[:12]
        self._skills[skill_id] = Skill(
            id=skill_id,
            name=name,
            description=description,
            pattern=pattern,
        )
        return skill_id

    def get(self, skill_id: str) -> Optional[Skill]:
        return self._skills.get(skill_id)

    def find_by_name(self, name: str) -> Optional[Skill]:
        skill_id = hashlib.sha256(name.encode()).hexdigest()[:12]
        return self._skills.get(skill_id)

    def record_execution(self, skill_id: str, success: bool):
        if skill_id in self._skills:
            if success:
                self._skills[skill_id].success_count += 1
            else:
                self._skills[skill_id].failure_count += 1

    def stats(self) -> Dict[str, Any]:
        return {"total": len(self._skills)}


# =============================================================================
# MEMORY MCP SERVER
# =============================================================================

class DistributedMemoryMCPv2(NativeMCPBase, EmbeddingMixin):
    """
    Memory MCP using the base infrastructure.

    Much cleaner than v1:
    - Cross-MCP calls via self.mesh
    - Embeddings via EmbeddingMixin
    - Standard lifecycle hooks
    - Automatic capability discovery
    """

    SERVICE_ID = "distributed-memory-mcp"
    SERVICE_NAME = "Distributed Memory MCP"
    VERSION = "2.0.0"

    # What we provide
    CAPABILITIES = [
        CapabilityType.MEMORY,
        "working_memory",
        "episodic_memory",
        "semantic_memory",
        "procedural_memory",
    ]

    # Soft dependencies (nice to have)
    REQUIRES = [
        CapabilityType.EMBED,  # For semantic similarity
    ]

    TOOLS = [
        {
            "name": "focus",
            "description": "Prime working memory with content",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": {"type": "string"},
                    "importance": {"type": "number", "default": 0.7},
                    "item_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
        {
            "name": "record_episode",
            "description": "Record an event in episodic memory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "event_type": {"type": "string"},
                    "content": {"type": "object"},
                    "participants": {"type": "array", "items": {"type": "string"}},
                    "importance": {"type": "number", "default": 0.5},
                },
                "required": ["event_type", "content"],
            },
        },
        {
            "name": "recall",
            "description": "Recall from memory using semantic similarity or text search",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "tiers": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["working", "episodic"]},
                        "default": ["working", "episodic"],
                    },
                    "limit": {"type": "integer", "default": 10},
                    "use_embeddings": {"type": "boolean", "default": True},
                },
                "required": ["query"],
            },
        },
        {
            "name": "store_belief",
            "description": "Store a belief in semantic memory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "predicate": {"type": "string"},
                    "object": {"type": "string"},
                    "confidence": {"type": "number", "default": 1.0},
                },
                "required": ["subject", "predicate", "object"],
            },
        },
        {
            "name": "query_beliefs",
            "description": "Query semantic memory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "predicate": {"type": "string"},
                    "object": {"type": "string"},
                },
            },
        },
        {
            "name": "learn_skill",
            "description": "Learn a procedural skill",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "pattern": {"type": "object"},
                },
                "required": ["name", "description", "pattern"],
            },
        },
        {
            "name": "stats",
            "description": "Get memory statistics",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ]

    def __init__(self, port: int = 4004, data_dir: str = None, **kwargs):
        super().__init__(port=port, data_dir=data_dir, **kwargs)

        # Initialize memory stores
        self.working = WorkingMemory(capacity=100)
        self.episodic = EpisodicMemory(max_episodes=10000)
        self.semantic = SemanticMemory()
        self.procedural = ProceduralMemory()

        # Register lifecycle hooks
        self.on_capability_available(CapabilityType.EMBED, self._on_embeddings_available)

    async def _on_embeddings_available(self):
        """Called when embedding capability becomes available."""
        logger.info("Embeddings now available - computing missing embeddings...")
        await self._compute_missing_embeddings()

    async def _compute_missing_embeddings(self):
        """Compute embeddings for items that don't have them."""
        # Episodic memory
        episodes = self.episodic.get_without_embeddings(limit=100)
        if episodes:
            texts = [str(ep.content) for ep in episodes]
            embeddings = await self.get_embeddings(texts)
            for ep, emb in zip(episodes, embeddings):
                if emb:
                    self.episodic.set_embedding(ep.id, emb)
            logger.info(f"Computed embeddings for {sum(1 for e in embeddings if e)} episodes")

    # =========================================================================
    # TOOL HANDLERS
    # =========================================================================

    async def _tool_focus(self, args: Dict[str, Any]) -> Dict[str, Any]:
        content = args.get("content")
        importance = args.get("importance", 0.7)
        item_ids = args.get("item_ids", [])

        focused = []

        if content:
            item_id = self.working.add(content, importance=importance)
            focused.append(item_id)

            # Compute embedding if available
            if self.has_capability(CapabilityType.EMBED):
                emb = await self.get_embedding(content)
                if emb and item_id in self.working._items:
                    self.working._items[item_id].embedding = emb

        if item_ids:
            self.working.focus(item_ids)
            focused.extend(item_ids)

        return self._result({
            "focused": focused,
            "context_size": len(self.working.get_context()),
        })

    async def _tool_record_episode(self, args: Dict[str, Any]) -> Dict[str, Any]:
        episode_id = self.episodic.record(
            event_type=args["event_type"],
            content=args["content"],
            participants=args.get("participants", []),
            importance=args.get("importance", 0.5),
        )

        # Compute embedding asynchronously if available
        if self.has_capability(CapabilityType.EMBED):
            emb = await self.get_embedding(str(args["content"]))
            if emb:
                self.episodic.set_embedding(episode_id, emb)

        return self._result({"episode_id": episode_id, "embedded": emb is not None})

    async def _tool_recall(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args["query"]
        tiers = args.get("tiers", ["working", "episodic"])
        limit = args.get("limit", 10)
        use_embeddings = args.get("use_embeddings", True)

        results = {}
        used_embeddings = False

        # Get query embedding if available
        query_embedding = None
        if use_embeddings and self.has_capability(CapabilityType.EMBED):
            query_embedding = await self.get_embedding(query)

        if "working" in tiers:
            if query_embedding:
                items = self.working.search_embedding(query_embedding, limit=limit)
                results["working"] = [
                    {"id": i.id, "content": i.content, "similarity": s}
                    for i, s in items
                ]
                used_embeddings = True
            else:
                items = self.working.search_text(query, limit=limit)
                results["working"] = [{"id": i.id, "content": i.content} for i in items]

        if "episodic" in tiers:
            if query_embedding:
                # Ensure episodes have embeddings
                await self._compute_missing_embeddings()

                episodes = self.episodic.recall_by_embedding(query_embedding, limit=limit)
                results["episodic"] = [
                    {"id": ep.id, "type": ep.event_type, "content": ep.content, "similarity": s}
                    for ep, s in episodes
                ]
                used_embeddings = True
            else:
                episodes = self.episodic.recall_by_text(query, limit=limit)
                results["episodic"] = [
                    {"id": ep.id, "type": ep.event_type, "content": ep.content}
                    for ep in episodes
                ]

        return self._result({
            "query": query,
            "used_embeddings": used_embeddings,
            "results": results,
            "total": sum(len(v) for v in results.values()),
        })

    async def _tool_store_belief(self, args: Dict[str, Any]) -> Dict[str, Any]:
        belief_id = self.semantic.store(
            subject=args["subject"],
            predicate=args["predicate"],
            obj=args["object"],
            confidence=args.get("confidence", 1.0),
        )
        return self._result({"belief_id": belief_id})

    async def _tool_query_beliefs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        beliefs = self.semantic.query(
            subject=args.get("subject"),
            predicate=args.get("predicate"),
            obj=args.get("object"),
        )
        return self._result({
            "count": len(beliefs),
            "beliefs": [
                {"s": b.subject, "p": b.predicate, "o": b.object, "conf": b.confidence}
                for b in beliefs
            ],
        })

    async def _tool_learn_skill(self, args: Dict[str, Any]) -> Dict[str, Any]:
        skill_id = self.procedural.learn(
            name=args["name"],
            description=args["description"],
            pattern=args["pattern"],
        )
        return self._result({"skill_id": skill_id})

    async def _tool_stats(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return self._result({
            "working": self.working.stats(),
            "episodic": self.episodic.stats(),
            "semantic": self.semantic.stats(),
            "procedural": self.procedural.stats(),
            "embeddings_available": self.has_capability(CapabilityType.EMBED),
            "mesh": self.mesh.get_metrics(),
        })

    def _result(self, data: Any) -> Dict[str, Any]:
        """Helper to format tool results."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(data, indent=2, default=str),
            }]
        }


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Memory MCP v2")
    parser.add_argument("--port", type=int, default=int(os.getenv("MEMORY_MCP_PORT", "4004")))
    parser.add_argument("--data-dir", type=str, default=os.getenv("MEMORY_DATA_DIR", "data/memory"))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    server = DistributedMemoryMCPv2(port=args.port, data_dir=args.data_dir)
    server.run()


if __name__ == "__main__":
    main()
