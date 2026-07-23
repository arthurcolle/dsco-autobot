#!/usr/bin/env python3
"""
SWARM COGNITION LAYER

A distributed cognitive infrastructure for autonomous agent swarms.

Core Concepts:
- DIGESTION: Agents consume and decompose content into structured knowledge
- TIMELINE: Event-sourced state enables full reconstruction from any point
- REPLICATION: Agents spawn, fork, and merge at will
- COLLECTIVE MEMORY: Shared knowledge graph across the swarm
- EMERGENT INTELLIGENCE: Swarm-level cognition from agent interactions

Architecture:
┌─────────────────────────────────────────────────────────────────┐
│                      SWARM COGNITION LAYER                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Agent A   │  │   Agent B   │  │   Agent C   │  ...        │
│  │  ┌───────┐  │  │  ┌───────┐  │  │  ┌───────┐  │             │
│  │  │Digest │  │  │  │Digest │  │  │  │Digest │  │             │
│  │  │Engine │  │  │  │Engine │  │  │  │Engine │  │             │
│  │  └───┬───┘  │  │  └───┬───┘  │  │  └───┬───┘  │             │
│  └──────┼──────┘  └──────┼──────┘  └──────┼──────┘             │
│         │                │                │                     │
│         └────────────────┼────────────────┘                     │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   COLLECTIVE MEMORY                       │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐          │  │
│  │  │ Knowledge  │  │  Timeline  │  │   Swarm    │          │  │
│  │  │   Graph    │  │   Events   │  │   State    │          │  │
│  │  └────────────┘  └────────────┘  └────────────┘          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  COORDINATION LAYER                       │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐          │  │
│  │  │  Routing   │  │ Consensus  │  │ Replication│          │  │
│  │  │  Protocol  │  │  Protocol  │  │  Protocol  │          │  │
│  │  └────────────┘  └────────────┘  └────────────┘          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import os
import pickle
import sqlite3
import sys
import time
import uuid
import zlib
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Callable, Dict, List, Optional, Set, Tuple,
    TypeVar, Generic, Union, AsyncIterator, Protocol, Type
)
import threading
import queue
import weakref
import struct
import mmap
import heapq

# =============================================================================
# CORE TYPES & PRIMITIVES
# =============================================================================

class ContentType(Enum):
    """Types of content the swarm can digest."""
    CODE = auto()
    TEXT = auto()
    STRUCTURED = auto()  # JSON, YAML, etc.
    BINARY = auto()
    IMAGE = auto()
    EMBEDDING = auto()
    KNOWLEDGE = auto()  # Digested knowledge
    EVENT = auto()
    MESSAGE = auto()

class AgentState(Enum):
    """Agent lifecycle states."""
    NASCENT = auto()      # Just spawned
    ACTIVE = auto()       # Running
    SUSPENDED = auto()    # Paused, can resume
    DIGESTING = auto()    # Processing content
    REPLICATING = auto()  # Spawning children
    MERGING = auto()      # Combining with another
    TERMINATED = auto()   # Dead

class EventType(Enum):
    """Types of events in the timeline."""
    AGENT_SPAWNED = auto()
    AGENT_TERMINATED = auto()
    CONTENT_DIGESTED = auto()
    KNOWLEDGE_CREATED = auto()
    KNOWLEDGE_LINKED = auto()
    TOOL_INVOKED = auto()
    TOOL_RESULT = auto()
    MESSAGE_SENT = auto()
    MESSAGE_RECEIVED = auto()
    STATE_CHECKPOINT = auto()
    REPLICATION_STARTED = auto()
    REPLICATION_COMPLETED = auto()
    MERGE_STARTED = auto()
    MERGE_COMPLETED = auto()
    CONSENSUS_REACHED = auto()

# Unique identifiers
AgentId = str
KnowledgeId = str
EventId = str
ContentHash = str

def generate_id() -> str:
    """Generate a unique identifier."""
    return uuid.uuid4().hex[:16]

def content_hash(data: bytes) -> ContentHash:
    """Generate content-addressable hash."""
    return hashlib.blake2b(data, digest_size=16).hexdigest()

def timestamp_now() -> float:
    """Get current timestamp in UTC."""
    return datetime.now(timezone.utc).timestamp()

# =============================================================================
# EVENT SOURCING - TIMELINE
# =============================================================================

@dataclass
class Event:
    """
    Immutable event in the timeline.
    All state changes are recorded as events for full reconstruction.
    """
    id: EventId
    type: EventType
    timestamp: float
    agent_id: AgentId
    payload: Dict[str, Any]
    parent_event_id: Optional[EventId] = None
    causality_chain: List[EventId] = field(default_factory=list)

    # For event ordering in distributed setting
    vector_clock: Dict[AgentId, int] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = generate_id()
        if not self.timestamp:
            self.timestamp = timestamp_now()

    def to_bytes(self) -> bytes:
        """Serialize event for storage."""
        return pickle.dumps(asdict(self))

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Event':
        """Deserialize event."""
        d = pickle.loads(data)
        d['type'] = EventType(d['type'])
        return cls(**d)

    def __lt__(self, other: 'Event') -> bool:
        """For heap ordering."""
        return self.timestamp < other.timestamp


class Timeline:
    """
    Event-sourced timeline for complete state reconstruction.

    Features:
    - Append-only event log
    - Causality tracking via vector clocks
    - Point-in-time state reconstruction
    - Event subscription for reactive updates
    """

    def __init__(self, storage_path: Optional[Path] = None):
        self.events: List[Event] = []
        self.event_index: Dict[EventId, int] = {}
        self.agent_events: Dict[AgentId, List[EventId]] = defaultdict(list)
        self.type_events: Dict[EventType, List[EventId]] = defaultdict(list)
        self.subscribers: Dict[EventType, List[Callable[[Event], None]]] = defaultdict(list)
        self.vector_clock: Dict[AgentId, int] = defaultdict(int)
        self._lock = threading.RLock()

        # Persistence
        self.storage_path = storage_path
        if storage_path:
            self._init_storage()

    def _init_storage(self):
        """Initialize persistent storage."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_path / "timeline.db"
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                type INTEGER,
                timestamp REAL,
                agent_id TEXT,
                data BLOB,
                UNIQUE(id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_agent ON events(agent_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON events(type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_time ON events(timestamp)")
        self.conn.commit()

        # Load existing events
        cursor = self.conn.execute("SELECT data FROM events ORDER BY timestamp")
        for (data,) in cursor:
            event = Event.from_bytes(data)
            self._index_event(event)

    def _index_event(self, event: Event):
        """Index event for fast lookup."""
        idx = len(self.events)
        self.events.append(event)
        self.event_index[event.id] = idx
        self.agent_events[event.agent_id].append(event.id)
        self.type_events[event.type].append(event.id)

    def append(self, event: Event) -> Event:
        """Append event to timeline."""
        with self._lock:
            # Update vector clock
            self.vector_clock[event.agent_id] += 1
            event.vector_clock = dict(self.vector_clock)

            self._index_event(event)

            # Persist
            if self.storage_path:
                self.conn.execute(
                    "INSERT OR REPLACE INTO events VALUES (?, ?, ?, ?, ?)",
                    (event.id, event.type.value, event.timestamp,
                     event.agent_id, event.to_bytes())
                )
                self.conn.commit()

            # Notify subscribers
            for callback in self.subscribers.get(event.type, []):
                try:
                    callback(event)
                except Exception as e:
                    pass  # Don't let subscriber errors break timeline

            return event

    def get(self, event_id: EventId) -> Optional[Event]:
        """Get event by ID."""
        idx = self.event_index.get(event_id)
        return self.events[idx] if idx is not None else None

    def get_agent_events(self, agent_id: AgentId,
                         since: Optional[float] = None) -> List[Event]:
        """Get all events for an agent."""
        event_ids = self.agent_events.get(agent_id, [])
        events = [self.events[self.event_index[eid]] for eid in event_ids]
        if since:
            events = [e for e in events if e.timestamp >= since]
        return events

    def get_events_by_type(self, event_type: EventType,
                           since: Optional[float] = None) -> List[Event]:
        """Get all events of a type."""
        event_ids = self.type_events.get(event_type, [])
        events = [self.events[self.event_index[eid]] for eid in event_ids]
        if since:
            events = [e for e in events if e.timestamp >= since]
        return events

    def replay(self, until: Optional[float] = None,
               agent_filter: Optional[Set[AgentId]] = None) -> List[Event]:
        """Replay events for state reconstruction."""
        events = self.events
        if until:
            events = [e for e in events if e.timestamp <= until]
        if agent_filter:
            events = [e for e in events if e.agent_id in agent_filter]
        return sorted(events, key=lambda e: e.timestamp)

    def subscribe(self, event_type: EventType,
                  callback: Callable[[Event], None]):
        """Subscribe to events of a type."""
        self.subscribers[event_type].append(callback)

    def fork(self, from_event: EventId) -> 'Timeline':
        """Fork timeline from a specific event."""
        idx = self.event_index.get(from_event, 0)
        forked = Timeline()
        for event in self.events[:idx + 1]:
            forked._index_event(Event(
                id=event.id,
                type=event.type,
                timestamp=event.timestamp,
                agent_id=event.agent_id,
                payload=event.payload.copy(),
                parent_event_id=event.parent_event_id,
                causality_chain=event.causality_chain.copy(),
                vector_clock=event.vector_clock.copy()
            ))
        return forked


# =============================================================================
# KNOWLEDGE GRAPH - COLLECTIVE MEMORY
# =============================================================================

@dataclass
class KnowledgeNode:
    """
    A node in the knowledge graph.
    Represents a piece of digested knowledge.
    """
    id: KnowledgeId
    content_type: ContentType
    content_hash: ContentHash

    # The actual knowledge
    data: Any
    embedding: Optional[List[float]] = None  # Vector representation

    # Metadata
    source: Optional[str] = None
    created_by: Optional[AgentId] = None
    created_at: float = field(default_factory=timestamp_now)

    # Graph structure
    edges: Dict[str, Set[KnowledgeId]] = field(default_factory=lambda: defaultdict(set))

    # Importance/relevance scoring
    access_count: int = 0
    last_accessed: float = field(default_factory=timestamp_now)
    relevance_score: float = 1.0

    def link(self, relation: str, target_id: KnowledgeId):
        """Create edge to another node."""
        self.edges[relation].add(target_id)

    def to_dict(self) -> Dict:
        """Serialize for storage."""
        return {
            'id': self.id,
            'content_type': self.content_type.value,
            'content_hash': self.content_hash,
            'data': self.data,
            'embedding': self.embedding,
            'source': self.source,
            'created_by': self.created_by,
            'created_at': self.created_at,
            'edges': {k: list(v) for k, v in self.edges.items()},
            'access_count': self.access_count,
            'relevance_score': self.relevance_score
        }


class KnowledgeGraph:
    """
    Distributed knowledge graph for collective swarm memory.

    Features:
    - Content-addressable storage
    - Semantic linking between nodes
    - Relevance-based retrieval
    - Automatic garbage collection
    - Merge/sync across agents
    """

    def __init__(self, storage_path: Optional[Path] = None):
        self.nodes: Dict[KnowledgeId, KnowledgeNode] = {}
        self.content_index: Dict[ContentHash, KnowledgeId] = {}
        self.type_index: Dict[ContentType, Set[KnowledgeId]] = defaultdict(set)
        self.source_index: Dict[str, Set[KnowledgeId]] = defaultdict(set)
        self._lock = threading.RLock()

        # For semantic search
        self.embeddings: Dict[KnowledgeId, List[float]] = {}

        # Persistence
        self.storage_path = storage_path
        if storage_path:
            self._init_storage()

    def _init_storage(self):
        """Initialize persistent storage."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_path / "knowledge.db"
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                content_hash TEXT,
                content_type INTEGER,
                data BLOB,
                created_at REAL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                source_id TEXT,
                relation TEXT,
                target_id TEXT,
                PRIMARY KEY (source_id, relation, target_id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_hash ON nodes(content_hash)")
        self.conn.commit()

        # Load existing nodes
        cursor = self.conn.execute("SELECT data FROM nodes")
        for (data,) in cursor:
            node_dict = pickle.loads(data)
            node = KnowledgeNode(
                id=node_dict['id'],
                content_type=ContentType(node_dict['content_type']),
                content_hash=node_dict['content_hash'],
                data=node_dict['data'],
                embedding=node_dict.get('embedding'),
                source=node_dict.get('source'),
                created_by=node_dict.get('created_by'),
                created_at=node_dict.get('created_at', timestamp_now())
            )
            self._index_node(node)

    def _index_node(self, node: KnowledgeNode):
        """Index node for fast lookup."""
        self.nodes[node.id] = node
        self.content_index[node.content_hash] = node.id
        self.type_index[node.content_type].add(node.id)
        if node.source:
            self.source_index[node.source].add(node.id)
        if node.embedding:
            self.embeddings[node.id] = node.embedding

    def add(self, node: KnowledgeNode) -> KnowledgeNode:
        """Add knowledge node."""
        with self._lock:
            # Check for duplicate content
            if node.content_hash in self.content_index:
                existing_id = self.content_index[node.content_hash]
                existing = self.nodes[existing_id]
                existing.access_count += 1
                existing.last_accessed = timestamp_now()
                return existing

            if not node.id:
                node.id = generate_id()

            self._index_node(node)

            # Persist
            if self.storage_path:
                self.conn.execute(
                    "INSERT OR REPLACE INTO nodes VALUES (?, ?, ?, ?, ?)",
                    (node.id, node.content_hash, node.content_type.value,
                     pickle.dumps(node.to_dict()), node.created_at)
                )
                self.conn.commit()

            return node

    def get(self, node_id: KnowledgeId) -> Optional[KnowledgeNode]:
        """Get node by ID."""
        node = self.nodes.get(node_id)
        if node:
            node.access_count += 1
            node.last_accessed = timestamp_now()
        return node

    def get_by_hash(self, content_hash: ContentHash) -> Optional[KnowledgeNode]:
        """Get node by content hash."""
        node_id = self.content_index.get(content_hash)
        return self.get(node_id) if node_id else None

    def link(self, source_id: KnowledgeId, relation: str,
             target_id: KnowledgeId) -> bool:
        """Create edge between nodes."""
        with self._lock:
            source = self.nodes.get(source_id)
            if not source or target_id not in self.nodes:
                return False

            source.link(relation, target_id)

            if self.storage_path:
                self.conn.execute(
                    "INSERT OR REPLACE INTO edges VALUES (?, ?, ?)",
                    (source_id, relation, target_id)
                )
                self.conn.commit()

            return True

    def traverse(self, start_id: KnowledgeId, relation: Optional[str] = None,
                 max_depth: int = 3) -> List[KnowledgeNode]:
        """Traverse graph from starting node."""
        visited = set()
        result = []
        queue = [(start_id, 0)]

        while queue:
            node_id, depth = queue.pop(0)
            if node_id in visited or depth > max_depth:
                continue

            visited.add(node_id)
            node = self.nodes.get(node_id)
            if not node:
                continue

            result.append(node)

            edges = node.edges.get(relation, set()) if relation else \
                    set().union(*node.edges.values())

            for target_id in edges:
                if target_id not in visited:
                    queue.append((target_id, depth + 1))

        return result

    def semantic_search(self, query_embedding: List[float],
                        k: int = 10) -> List[Tuple[KnowledgeNode, float]]:
        """Find k most similar nodes by embedding."""
        if not self.embeddings:
            return []

        def cosine_sim(a: List[float], b: List[float]) -> float:
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = sum(x * x for x in a) ** 0.5
            norm_b = sum(x * x for x in b) ** 0.5
            return dot / (norm_a * norm_b + 1e-8)

        scores = []
        for node_id, embedding in self.embeddings.items():
            sim = cosine_sim(query_embedding, embedding)
            scores.append((node_id, sim))

        scores.sort(key=lambda x: -x[1])
        return [(self.nodes[nid], score) for nid, score in scores[:k]]

    def merge(self, other: 'KnowledgeGraph'):
        """Merge another graph into this one."""
        with self._lock:
            for node in other.nodes.values():
                if node.content_hash not in self.content_index:
                    self.add(node)
                else:
                    # Merge edges
                    existing = self.nodes[self.content_index[node.content_hash]]
                    for rel, targets in node.edges.items():
                        for target in targets:
                            # Translate target ID if it exists in other
                            if target in other.nodes:
                                target_hash = other.nodes[target].content_hash
                                if target_hash in self.content_index:
                                    existing.edges[rel].add(
                                        self.content_index[target_hash]
                                    )

    def gc(self, min_access_count: int = 0,
           max_age_seconds: float = float('inf')):
        """Garbage collect low-value nodes."""
        now = timestamp_now()
        to_remove = []

        for node_id, node in self.nodes.items():
            age = now - node.created_at
            if node.access_count <= min_access_count and age > max_age_seconds:
                to_remove.append(node_id)

        with self._lock:
            for node_id in to_remove:
                node = self.nodes.pop(node_id, None)
                if node:
                    self.content_index.pop(node.content_hash, None)
                    self.type_index[node.content_type].discard(node_id)
                    self.embeddings.pop(node_id, None)


# =============================================================================
# CONTENT DIGESTION ENGINE
# =============================================================================

class DigestStrategy(ABC):
    """Strategy for digesting specific content types."""

    @abstractmethod
    def can_digest(self, content_type: ContentType) -> bool:
        """Check if this strategy handles the content type."""
        pass

    @abstractmethod
    def digest(self, content: bytes, metadata: Dict) -> List[KnowledgeNode]:
        """Digest content into knowledge nodes."""
        pass


class CodeDigestStrategy(DigestStrategy):
    """Digest source code into structured knowledge."""

    LANGUAGE_PATTERNS = {
        'python': {
            'function': r'def\s+(\w+)\s*\([^)]*\)',
            'class': r'class\s+(\w+)\s*[:\(]',
            'import': r'(?:from\s+[\w.]+\s+)?import\s+[\w.,\s]+',
            'docstring': r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'',
        },
        'javascript': {
            'function': r'(?:function\s+(\w+)|(\w+)\s*=\s*(?:async\s+)?(?:function|\([^)]*\)\s*=>))',
            'class': r'class\s+(\w+)',
            'import': r'import\s+.*?from\s+[\'"].*?[\'"]',
        }
    }

    def can_digest(self, content_type: ContentType) -> bool:
        return content_type == ContentType.CODE

    def digest(self, content: bytes, metadata: Dict) -> List[KnowledgeNode]:
        import re

        nodes = []
        text = content.decode('utf-8', errors='ignore')
        source = metadata.get('source', 'unknown')
        language = metadata.get('language', 'python')
        patterns = self.LANGUAGE_PATTERNS.get(language, self.LANGUAGE_PATTERNS['python'])

        # Create node for the file itself
        file_node = KnowledgeNode(
            id=generate_id(),
            content_type=ContentType.CODE,
            content_hash=content_hash(content),
            data={'full_content': text, 'language': language},
            source=source
        )
        nodes.append(file_node)

        # Extract functions
        for match in re.finditer(patterns['function'], text):
            func_name = match.group(1) or match.group(2)
            if func_name:
                # Find function body (simplified)
                start = match.start()
                # Find next function or end
                end = len(text)
                next_match = re.search(patterns['function'], text[match.end():])
                if next_match:
                    end = match.end() + next_match.start()

                func_content = text[start:end].strip()
                func_node = KnowledgeNode(
                    id=generate_id(),
                    content_type=ContentType.CODE,
                    content_hash=content_hash(func_content.encode()),
                    data={
                        'type': 'function',
                        'name': func_name,
                        'content': func_content,
                        'language': language
                    },
                    source=f"{source}::{func_name}"
                )
                nodes.append(func_node)
                file_node.link('contains', func_node.id)
                func_node.link('part_of', file_node.id)

        # Extract classes
        for match in re.finditer(patterns['class'], text):
            class_name = match.group(1)
            class_node = KnowledgeNode(
                id=generate_id(),
                content_type=ContentType.CODE,
                content_hash=content_hash(f"class:{class_name}".encode()),
                data={
                    'type': 'class',
                    'name': class_name,
                    'language': language
                },
                source=f"{source}::{class_name}"
            )
            nodes.append(class_node)
            file_node.link('contains', class_node.id)

        return nodes


class TextDigestStrategy(DigestStrategy):
    """Digest text content into semantic chunks."""

    def can_digest(self, content_type: ContentType) -> bool:
        return content_type == ContentType.TEXT

    def digest(self, content: bytes, metadata: Dict) -> List[KnowledgeNode]:
        nodes = []
        text = content.decode('utf-8', errors='ignore')
        source = metadata.get('source', 'unknown')

        # Chunk by paragraphs
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]

        # Create document node
        doc_node = KnowledgeNode(
            id=generate_id(),
            content_type=ContentType.TEXT,
            content_hash=content_hash(content),
            data={'full_content': text, 'paragraph_count': len(paragraphs)},
            source=source
        )
        nodes.append(doc_node)

        # Create chunk nodes
        prev_chunk = None
        for i, para in enumerate(paragraphs):
            if len(para) < 20:  # Skip very short paragraphs
                continue

            chunk_node = KnowledgeNode(
                id=generate_id(),
                content_type=ContentType.TEXT,
                content_hash=content_hash(para.encode()),
                data={
                    'type': 'paragraph',
                    'content': para,
                    'position': i
                },
                source=f"{source}::chunk_{i}"
            )
            nodes.append(chunk_node)
            doc_node.link('contains', chunk_node.id)
            chunk_node.link('part_of', doc_node.id)

            if prev_chunk:
                prev_chunk.link('followed_by', chunk_node.id)
                chunk_node.link('preceded_by', prev_chunk.id)
            prev_chunk = chunk_node

        return nodes


class StructuredDigestStrategy(DigestStrategy):
    """Digest structured data (JSON, etc.)."""

    def can_digest(self, content_type: ContentType) -> bool:
        return content_type == ContentType.STRUCTURED

    def digest(self, content: bytes, metadata: Dict) -> List[KnowledgeNode]:
        nodes = []
        source = metadata.get('source', 'unknown')

        try:
            data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError:
            return nodes

        def process_value(value: Any, path: str) -> Optional[KnowledgeNode]:
            if isinstance(value, dict):
                node = KnowledgeNode(
                    id=generate_id(),
                    content_type=ContentType.STRUCTURED,
                    content_hash=content_hash(json.dumps(value, sort_keys=True).encode()),
                    data={'type': 'object', 'keys': list(value.keys()), 'value': value},
                    source=f"{source}::{path}"
                )
                nodes.append(node)

                for k, v in value.items():
                    child = process_value(v, f"{path}.{k}")
                    if child:
                        node.link('has_property', child.id)
                        child.link('property_of', node.id)

                return node

            elif isinstance(value, list) and value:
                node = KnowledgeNode(
                    id=generate_id(),
                    content_type=ContentType.STRUCTURED,
                    content_hash=content_hash(json.dumps(value, sort_keys=True).encode()),
                    data={'type': 'array', 'length': len(value), 'value': value},
                    source=f"{source}::{path}"
                )
                nodes.append(node)

                for i, item in enumerate(value):
                    child = process_value(item, f"{path}[{i}]")
                    if child:
                        node.link('contains', child.id)

                return node

            return None

        root = process_value(data, "root")
        return nodes


class DigestEngine:
    """
    Central engine for digesting content into knowledge.

    Coordinates multiple digest strategies and manages
    the conversion of raw content into the knowledge graph.
    """

    def __init__(self, knowledge_graph: KnowledgeGraph):
        self.graph = knowledge_graph
        self.strategies: List[DigestStrategy] = [
            CodeDigestStrategy(),
            TextDigestStrategy(),
            StructuredDigestStrategy(),
        ]

    def register_strategy(self, strategy: DigestStrategy):
        """Register a new digest strategy."""
        self.strategies.append(strategy)

    def digest(self, content: bytes, content_type: ContentType,
               metadata: Optional[Dict] = None,
               agent_id: Optional[AgentId] = None) -> List[KnowledgeNode]:
        """
        Digest content into knowledge nodes and add to graph.
        """
        metadata = metadata or {}
        nodes = []

        # Find appropriate strategy
        for strategy in self.strategies:
            if strategy.can_digest(content_type):
                nodes = strategy.digest(content, metadata)
                break

        # Add to graph and set creator
        added_nodes = []
        for node in nodes:
            node.created_by = agent_id
            added = self.graph.add(node)
            added_nodes.append(added)

        return added_nodes

    def digest_file(self, path: Path, agent_id: Optional[AgentId] = None) -> List[KnowledgeNode]:
        """Digest a file from the filesystem."""
        content = path.read_bytes()

        # Determine content type
        suffix = path.suffix.lower()
        if suffix in {'.py', '.js', '.ts', '.go', '.rs', '.java', '.cpp', '.c', '.h'}:
            content_type = ContentType.CODE
        elif suffix in {'.json', '.yaml', '.yml', '.toml'}:
            content_type = ContentType.STRUCTURED
        else:
            content_type = ContentType.TEXT

        metadata = {
            'source': str(path),
            'language': suffix[1:] if content_type == ContentType.CODE else None
        }

        return self.digest(content, content_type, metadata, agent_id)


# =============================================================================
# SWARM COORDINATION
# =============================================================================

@dataclass
class Message:
    """Inter-agent message."""
    id: str
    sender: AgentId
    recipient: Optional[AgentId]  # None = broadcast
    message_type: str
    payload: Any
    timestamp: float = field(default_factory=timestamp_now)
    reply_to: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)


class MessageBus:
    """
    Asynchronous message bus for agent communication.

    Features:
    - Point-to-point and broadcast messaging
    - Message persistence
    - Subscription by pattern
    - Request-reply correlation
    """

    def __init__(self):
        self.queues: Dict[AgentId, asyncio.Queue] = {}
        self.broadcast_queue: asyncio.Queue = asyncio.Queue()
        self.subscriptions: Dict[str, List[Callable]] = defaultdict(list)
        self.pending_replies: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def register_agent(self, agent_id: AgentId):
        """Register an agent with the bus."""
        async with self._lock:
            if agent_id not in self.queues:
                self.queues[agent_id] = asyncio.Queue()

    async def unregister_agent(self, agent_id: AgentId):
        """Unregister an agent."""
        async with self._lock:
            self.queues.pop(agent_id, None)

    async def send(self, message: Message):
        """Send a message."""
        if message.recipient:
            # Point-to-point
            if message.recipient in self.queues:
                await self.queues[message.recipient].put(message)
        else:
            # Broadcast
            for agent_id, queue in self.queues.items():
                if agent_id != message.sender:
                    await queue.put(message)

        # Handle subscriptions
        for callback in self.subscriptions.get(message.message_type, []):
            try:
                await callback(message)
            except Exception:
                pass

    async def receive(self, agent_id: AgentId,
                      timeout: Optional[float] = None) -> Optional[Message]:
        """Receive a message for an agent."""
        if agent_id not in self.queues:
            return None

        try:
            if timeout:
                return await asyncio.wait_for(
                    self.queues[agent_id].get(), timeout=timeout
                )
            return await self.queues[agent_id].get()
        except asyncio.TimeoutError:
            return None

    async def request(self, message: Message,
                      timeout: float = 30.0) -> Optional[Message]:
        """Send request and wait for reply."""
        reply_future = asyncio.get_event_loop().create_future()
        self.pending_replies[message.id] = reply_future

        try:
            await self.send(message)
            return await asyncio.wait_for(reply_future, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.pending_replies.pop(message.id, None)

    async def reply(self, original: Message, response: Message):
        """Reply to a message."""
        response.reply_to = original.id

        if original.id in self.pending_replies:
            self.pending_replies[original.id].set_result(response)
        else:
            await self.send(response)

    def subscribe(self, message_type: str, callback: Callable):
        """Subscribe to a message type."""
        self.subscriptions[message_type].append(callback)


class ConsensusProtocol:
    """
    Simple consensus protocol for swarm decisions.

    Uses a voting mechanism where agents propose and vote
    on shared decisions.
    """

    def __init__(self, bus: MessageBus, threshold: float = 0.5):
        self.bus = bus
        self.threshold = threshold
        self.proposals: Dict[str, Dict] = {}
        self.votes: Dict[str, Dict[AgentId, bool]] = defaultdict(dict)
        self._lock = asyncio.Lock()

    async def propose(self, proposer: AgentId, proposal_id: str,
                      proposal: Any, voters: Set[AgentId]) -> bool:
        """Create a proposal and wait for consensus."""
        async with self._lock:
            self.proposals[proposal_id] = {
                'proposer': proposer,
                'proposal': proposal,
                'voters': voters,
                'created_at': timestamp_now()
            }
            self.votes[proposal_id] = {proposer: True}  # Proposer votes yes

        # Broadcast proposal
        await self.bus.send(Message(
            id=generate_id(),
            sender=proposer,
            recipient=None,  # Broadcast
            message_type='consensus_proposal',
            payload={'proposal_id': proposal_id, 'proposal': proposal}
        ))

        # Wait for votes
        deadline = timestamp_now() + 30.0  # 30 second timeout
        while timestamp_now() < deadline:
            async with self._lock:
                votes = self.votes[proposal_id]
                if len(votes) >= len(voters):
                    break
            await asyncio.sleep(0.1)

        # Check result
        async with self._lock:
            votes = self.votes[proposal_id]
            yes_votes = sum(1 for v in votes.values() if v)
            return yes_votes / len(voters) >= self.threshold

    async def vote(self, voter: AgentId, proposal_id: str, approve: bool):
        """Cast a vote on a proposal."""
        async with self._lock:
            if proposal_id in self.proposals:
                self.votes[proposal_id][voter] = approve


class SwarmCoordinator:
    """
    Central coordinator for the agent swarm.

    Manages:
    - Agent lifecycle
    - Message routing
    - Collective memory access
    - Consensus for shared decisions
    - Load balancing
    """

    def __init__(self, storage_path: Optional[Path] = None):
        self.storage_path = storage_path or Path(".swarm_data")
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # Core components
        self.timeline = Timeline(self.storage_path / "timeline")
        self.knowledge = KnowledgeGraph(self.storage_path / "knowledge")
        self.digest_engine = DigestEngine(self.knowledge)
        self.message_bus = MessageBus()
        self.consensus = ConsensusProtocol(self.message_bus)

        # Agent management
        self.agents: Dict[AgentId, 'SwarmAgent'] = {}
        self.agent_states: Dict[AgentId, AgentState] = {}
        self._lock = asyncio.Lock()

        # Task queue for load balancing
        self.task_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()

    async def spawn_agent(self, agent_class: Type['SwarmAgent'],
                          parent_id: Optional[AgentId] = None,
                          **kwargs) -> 'SwarmAgent':
        """Spawn a new agent in the swarm."""
        agent_id = generate_id()

        agent = agent_class(
            agent_id=agent_id,
            coordinator=self,
            parent_id=parent_id,
            **kwargs
        )

        async with self._lock:
            self.agents[agent_id] = agent
            self.agent_states[agent_id] = AgentState.NASCENT
            await self.message_bus.register_agent(agent_id)

        # Record spawn event
        self.timeline.append(Event(
            id=generate_id(),
            type=EventType.AGENT_SPAWNED,
            timestamp=timestamp_now(),
            agent_id=agent_id,
            payload={
                'parent_id': parent_id,
                'agent_class': agent_class.__name__,
                'kwargs': {k: str(v) for k, v in kwargs.items()}
            }
        ))

        return agent

    async def terminate_agent(self, agent_id: AgentId):
        """Terminate an agent."""
        async with self._lock:
            agent = self.agents.pop(agent_id, None)
            if agent:
                self.agent_states[agent_id] = AgentState.TERMINATED
                await self.message_bus.unregister_agent(agent_id)

        self.timeline.append(Event(
            id=generate_id(),
            type=EventType.AGENT_TERMINATED,
            timestamp=timestamp_now(),
            agent_id=agent_id,
            payload={}
        ))

    def get_active_agents(self) -> List[AgentId]:
        """Get list of active agent IDs."""
        return [
            aid for aid, state in self.agent_states.items()
            if state in {AgentState.ACTIVE, AgentState.DIGESTING}
        ]

    async def broadcast(self, sender: AgentId, message_type: str, payload: Any):
        """Broadcast message to all agents."""
        await self.message_bus.send(Message(
            id=generate_id(),
            sender=sender,
            recipient=None,
            message_type=message_type,
            payload=payload
        ))

    async def submit_task(self, priority: int, task: Dict):
        """Submit a task to the queue."""
        await self.task_queue.put((priority, task))

    async def get_task(self) -> Optional[Dict]:
        """Get next task from queue."""
        try:
            _, task = self.task_queue.get_nowait()
            return task
        except asyncio.QueueEmpty:
            return None

    def reconstruct_state(self, until: Optional[float] = None,
                          agent_filter: Optional[Set[AgentId]] = None) -> Dict:
        """Reconstruct swarm state from timeline."""
        events = self.timeline.replay(until=until, agent_filter=agent_filter)

        state = {
            'agents': {},
            'knowledge_count': 0,
            'messages_processed': 0
        }

        for event in events:
            if event.type == EventType.AGENT_SPAWNED:
                state['agents'][event.agent_id] = {
                    'spawned_at': event.timestamp,
                    'status': 'active'
                }
            elif event.type == EventType.AGENT_TERMINATED:
                if event.agent_id in state['agents']:
                    state['agents'][event.agent_id]['status'] = 'terminated'
            elif event.type == EventType.CONTENT_DIGESTED:
                state['knowledge_count'] += event.payload.get('node_count', 0)
            elif event.type == EventType.MESSAGE_SENT:
                state['messages_processed'] += 1

        return state


# =============================================================================
# SWARM AGENT BASE CLASS
# =============================================================================

class SwarmAgent(ABC):
    """
    Base class for all swarm agents.

    Features:
    - Access to collective memory
    - Timeline event emission
    - Inter-agent communication
    - Content digestion
    - Replication/forking
    """

    def __init__(self, agent_id: AgentId, coordinator: SwarmCoordinator,
                 parent_id: Optional[AgentId] = None):
        self.id = agent_id
        self.coordinator = coordinator
        self.parent_id = parent_id
        self.state = AgentState.NASCENT

        # Local state
        self.local_memory: Dict[str, Any] = {}
        self.current_task: Optional[Dict] = None
        self._running = False

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self):
        """Start the agent."""
        self.state = AgentState.ACTIVE
        self.coordinator.agent_states[self.id] = AgentState.ACTIVE
        self._running = True

        # Start message processing
        asyncio.create_task(self._process_messages())

        # Run main loop
        await self.run()

    async def stop(self):
        """Stop the agent."""
        self._running = False
        await self.coordinator.terminate_agent(self.id)

    @abstractmethod
    async def run(self):
        """Main agent loop - implement in subclass."""
        pass

    # -------------------------------------------------------------------------
    # Communication
    # -------------------------------------------------------------------------

    async def send(self, recipient: AgentId, message_type: str, payload: Any):
        """Send message to another agent."""
        msg = Message(
            id=generate_id(),
            sender=self.id,
            recipient=recipient,
            message_type=message_type,
            payload=payload
        )
        await self.coordinator.message_bus.send(msg)

        self.emit_event(EventType.MESSAGE_SENT, {
            'recipient': recipient,
            'message_type': message_type
        })

    async def broadcast(self, message_type: str, payload: Any):
        """Broadcast to all agents."""
        await self.coordinator.broadcast(self.id, message_type, payload)

    async def receive(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Receive a message."""
        return await self.coordinator.message_bus.receive(self.id, timeout)

    async def request(self, recipient: AgentId, message_type: str,
                      payload: Any, timeout: float = 30.0) -> Optional[Message]:
        """Send request and wait for reply."""
        msg = Message(
            id=generate_id(),
            sender=self.id,
            recipient=recipient,
            message_type=message_type,
            payload=payload
        )
        return await self.coordinator.message_bus.request(msg, timeout)

    async def _process_messages(self):
        """Background message processing."""
        while self._running:
            msg = await self.receive(timeout=1.0)
            if msg:
                self.emit_event(EventType.MESSAGE_RECEIVED, {
                    'sender': msg.sender,
                    'message_type': msg.message_type
                })
                await self.handle_message(msg)

    async def handle_message(self, message: Message):
        """Handle incoming message - override in subclass."""
        pass

    # -------------------------------------------------------------------------
    # Knowledge & Memory
    # -------------------------------------------------------------------------

    def digest(self, content: bytes, content_type: ContentType,
               metadata: Optional[Dict] = None) -> List[KnowledgeNode]:
        """Digest content into knowledge."""
        self.state = AgentState.DIGESTING
        self.coordinator.agent_states[self.id] = AgentState.DIGESTING

        nodes = self.coordinator.digest_engine.digest(
            content, content_type, metadata, self.id
        )

        self.emit_event(EventType.CONTENT_DIGESTED, {
            'content_type': content_type.value,
            'node_count': len(nodes)
        })

        self.state = AgentState.ACTIVE
        self.coordinator.agent_states[self.id] = AgentState.ACTIVE

        return nodes

    def digest_file(self, path: Path) -> List[KnowledgeNode]:
        """Digest a file."""
        return self.coordinator.digest_engine.digest_file(path, self.id)

    def query_knowledge(self, node_id: KnowledgeId) -> Optional[KnowledgeNode]:
        """Query collective knowledge."""
        return self.coordinator.knowledge.get(node_id)

    def search_knowledge(self, embedding: List[float], k: int = 10) -> List[Tuple[KnowledgeNode, float]]:
        """Semantic search in knowledge graph."""
        return self.coordinator.knowledge.semantic_search(embedding, k)

    def link_knowledge(self, source_id: KnowledgeId, relation: str,
                       target_id: KnowledgeId) -> bool:
        """Create link between knowledge nodes."""
        success = self.coordinator.knowledge.link(source_id, relation, target_id)
        if success:
            self.emit_event(EventType.KNOWLEDGE_LINKED, {
                'source': source_id,
                'relation': relation,
                'target': target_id
            })
        return success

    def remember(self, key: str, value: Any):
        """Store in local memory."""
        self.local_memory[key] = value

    def recall(self, key: str) -> Optional[Any]:
        """Recall from local memory."""
        return self.local_memory.get(key)

    # -------------------------------------------------------------------------
    # Replication
    # -------------------------------------------------------------------------

    async def replicate(self, **kwargs) -> 'SwarmAgent':
        """Create a replica of this agent."""
        self.state = AgentState.REPLICATING
        self.coordinator.agent_states[self.id] = AgentState.REPLICATING

        self.emit_event(EventType.REPLICATION_STARTED, {})

        # Spawn child with same class
        child = await self.coordinator.spawn_agent(
            self.__class__,
            parent_id=self.id,
            **kwargs
        )

        # Copy local memory
        child.local_memory = self.local_memory.copy()

        self.emit_event(EventType.REPLICATION_COMPLETED, {
            'child_id': child.id
        })

        self.state = AgentState.ACTIVE
        self.coordinator.agent_states[self.id] = AgentState.ACTIVE

        return child

    async def fork(self, divergence_point: Optional[EventId] = None) -> 'SwarmAgent':
        """Fork agent from a point in timeline."""
        # Get events up to divergence point
        events = self.coordinator.timeline.get_agent_events(
            self.id,
            since=None
        )

        if divergence_point:
            events = [e for e in events
                     if e.timestamp <= self.coordinator.timeline.get(divergence_point).timestamp]

        # Create forked agent
        forked = await self.replicate()

        # Replay events to rebuild state
        for event in events:
            if event.type == EventType.CONTENT_DIGESTED:
                # Re-associate knowledge with forked agent
                pass

        return forked

    # -------------------------------------------------------------------------
    # Timeline
    # -------------------------------------------------------------------------

    def emit_event(self, event_type: EventType, payload: Dict):
        """Emit an event to the timeline."""
        self.coordinator.timeline.append(Event(
            id=generate_id(),
            type=event_type,
            timestamp=timestamp_now(),
            agent_id=self.id,
            payload=payload
        ))

    def checkpoint(self) -> EventId:
        """Create a checkpoint of current state."""
        event = Event(
            id=generate_id(),
            type=EventType.STATE_CHECKPOINT,
            timestamp=timestamp_now(),
            agent_id=self.id,
            payload={
                'local_memory': self.local_memory.copy(),
                'state': self.state.value
            }
        )
        self.coordinator.timeline.append(event)
        return event.id


# =============================================================================
# COGNITIVE AGENT - Full Implementation
# =============================================================================

class CognitiveAgent(SwarmAgent):
    """
    A fully cognitive agent with:
    - LLM-powered reasoning
    - Tool execution
    - Self-directed learning
    - Knowledge synthesis
    """

    def __init__(self, agent_id: AgentId, coordinator: SwarmCoordinator,
                 parent_id: Optional[AgentId] = None,
                 llm_client: Optional[Any] = None,
                 tools: Optional[Dict[str, Callable]] = None,
                 persona: str = "general"):
        super().__init__(agent_id, coordinator, parent_id)

        self.llm_client = llm_client
        self.tools = tools or {}
        self.persona = persona

        # Cognitive state
        self.thoughts: List[str] = []
        self.goals: List[str] = []
        self.hypotheses: List[str] = []
        self.conversation: List[Dict] = []

    async def run(self):
        """Main cognitive loop."""
        while self._running:
            # Check for tasks
            task = await self.coordinator.get_task()
            if task:
                await self.execute_task(task)

            # Check for messages
            msg = await self.receive(timeout=0.1)
            if msg:
                await self.handle_message(msg)

            # Self-directed cognition
            if not task and not msg:
                await self.reflect()

            await asyncio.sleep(0.1)

    async def execute_task(self, task: Dict):
        """Execute a task."""
        self.current_task = task
        task_type = task.get('type')

        if task_type == 'digest':
            path = Path(task.get('path', '.'))
            if path.is_file():
                self.digest_file(path)
            elif path.is_dir():
                for f in path.rglob('*'):
                    if f.is_file():
                        self.digest_file(f)

        elif task_type == 'query':
            query = task.get('query', '')
            # Would use LLM here
            self.remember('last_query', query)

        elif task_type == 'analyze':
            target = task.get('target')
            analysis = await self.analyze(target)
            self.remember('analysis', analysis)

        self.current_task = None

    async def analyze(self, target: str) -> Dict:
        """Analyze a target (file, directory, concept)."""
        path = Path(target)

        if path.exists():
            if path.is_file():
                nodes = self.digest_file(path)
                return {
                    'type': 'file',
                    'path': str(path),
                    'knowledge_nodes': len(nodes),
                    'content_type': nodes[0].content_type.value if nodes else None
                }
            elif path.is_dir():
                total_nodes = 0
                for f in path.rglob('*'):
                    if f.is_file():
                        nodes = self.digest_file(f)
                        total_nodes += len(nodes)
                return {
                    'type': 'directory',
                    'path': str(path),
                    'knowledge_nodes': total_nodes
                }

        # Concept analysis - search knowledge graph
        return {
            'type': 'concept',
            'target': target,
            'related_knowledge': []
        }

    async def reflect(self):
        """Self-reflection and learning."""
        # Review recent events
        recent_events = self.coordinator.timeline.get_agent_events(
            self.id,
            since=timestamp_now() - 60  # Last minute
        )

        if recent_events:
            # Synthesize learning
            event_types = [e.type.value for e in recent_events]
            self.thoughts.append(f"Reflected on {len(recent_events)} recent events")

    async def handle_message(self, message: Message):
        """Handle incoming message."""
        if message.message_type == 'task':
            await self.execute_task(message.payload)

        elif message.message_type == 'query':
            # Answer query using knowledge
            response = {'answer': 'Processed query', 'source': self.id}
            await self.coordinator.message_bus.reply(
                message,
                Message(
                    id=generate_id(),
                    sender=self.id,
                    recipient=message.sender,
                    message_type='query_response',
                    payload=response
                )
            )

        elif message.message_type == 'consensus_proposal':
            # Vote on proposals
            proposal_id = message.payload.get('proposal_id')
            # Simple approval logic - real implementation would reason
            await self.coordinator.consensus.vote(self.id, proposal_id, True)

        elif message.message_type == 'replicate':
            # Spawn a replica
            await self.replicate()


# =============================================================================
# SWARM RUNTIME
# =============================================================================

class SwarmRuntime:
    """
    Main runtime for the cognitive swarm.

    Provides high-level API for:
    - Starting/stopping the swarm
    - Spawning agents
    - Submitting tasks
    - Querying collective knowledge
    """

    def __init__(self, storage_path: Optional[Path] = None):
        self.coordinator = SwarmCoordinator(storage_path)
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self):
        """Start the swarm runtime."""
        self._running = True
        self._loop = asyncio.get_event_loop()
        print(f"Swarm runtime started. Storage: {self.coordinator.storage_path}")

    async def stop(self):
        """Stop the swarm runtime."""
        self._running = False

        # Terminate all agents
        for agent_id in list(self.coordinator.agents.keys()):
            await self.coordinator.terminate_agent(agent_id)

        print("Swarm runtime stopped.")

    async def spawn(self, agent_class: Type[SwarmAgent] = CognitiveAgent,
                    **kwargs) -> SwarmAgent:
        """Spawn a new agent."""
        agent = await self.coordinator.spawn_agent(agent_class, **kwargs)
        asyncio.create_task(agent.start())
        return agent

    async def submit_task(self, task: Dict, priority: int = 5):
        """Submit a task to the swarm."""
        await self.coordinator.submit_task(priority, task)

    def digest_path(self, path: Path, agent_id: Optional[AgentId] = None) -> int:
        """Digest all files at path into knowledge."""
        count = 0

        if path.is_file():
            self.coordinator.digest_engine.digest_file(path, agent_id)
            count = 1
        elif path.is_dir():
            for f in path.rglob('*'):
                if f.is_file() and not f.name.startswith('.'):
                    try:
                        self.coordinator.digest_engine.digest_file(f, agent_id)
                        count += 1
                    except Exception:
                        pass

        return count

    def query_knowledge(self, query: str) -> List[KnowledgeNode]:
        """Query the collective knowledge."""
        # Simple text search in knowledge
        results = []
        for node in self.coordinator.knowledge.nodes.values():
            data = node.data
            if isinstance(data, dict):
                content = str(data.get('content', '')) + str(data.get('full_content', ''))
                if query.lower() in content.lower():
                    results.append(node)
        return results[:20]

    def get_stats(self) -> Dict:
        """Get swarm statistics."""
        return {
            'agents': {
                'total': len(self.coordinator.agents),
                'active': len(self.coordinator.get_active_agents()),
                'by_state': {
                    state.name: sum(1 for s in self.coordinator.agent_states.values() if s == state)
                    for state in AgentState
                }
            },
            'knowledge': {
                'nodes': len(self.coordinator.knowledge.nodes),
                'by_type': {
                    ct.name: len(self.coordinator.knowledge.type_index.get(ct, set()))
                    for ct in ContentType
                }
            },
            'timeline': {
                'events': len(self.coordinator.timeline.events),
                'by_type': {
                    et.name: len(self.coordinator.timeline.type_events.get(et, []))
                    for et in EventType
                }
            }
        }

    def reconstruct_at(self, timestamp: float) -> Dict:
        """Reconstruct swarm state at a point in time."""
        return self.coordinator.reconstruct_state(until=timestamp)


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Swarm Cognition Runtime")
    parser.add_argument("--storage", type=Path, default=Path(".swarm_data"))
    parser.add_argument("--agents", type=int, default=1, help="Initial agents to spawn")
    parser.add_argument("--digest", type=Path, help="Path to digest")
    parser.add_argument("--query", type=str, help="Query knowledge")
    parser.add_argument("--interactive", action="store_true", help="Interactive mode")

    args = parser.parse_args()

    # Create runtime
    runtime = SwarmRuntime(args.storage)
    await runtime.start()

    # Spawn initial agents
    agents = []
    for i in range(args.agents):
        agent = await runtime.spawn(persona=f"agent_{i}")
        agents.append(agent)
        print(f"Spawned agent: {agent.id}")

    # Digest if requested
    if args.digest:
        count = runtime.digest_path(args.digest)
        print(f"Digested {count} files into knowledge graph")

    # Query if requested
    if args.query:
        results = runtime.query_knowledge(args.query)
        print(f"Found {len(results)} matching knowledge nodes:")
        for node in results[:5]:
            print(f"  - [{node.content_type.name}] {node.source}")

    # Interactive mode
    if args.interactive:
        print("\nSwarm Cognition Interactive Mode")
        print("Commands: spawn, digest <path>, query <text>, stats, quit\n")

        while True:
            try:
                cmd = input("swarm> ").strip()
            except (KeyboardInterrupt, EOFError):
                break

            if not cmd:
                continue

            parts = cmd.split(maxsplit=1)
            action = parts[0].lower()

            if action == "quit":
                break
            elif action == "spawn":
                agent = await runtime.spawn()
                print(f"Spawned: {agent.id}")
            elif action == "digest" and len(parts) > 1:
                count = runtime.digest_path(Path(parts[1]))
                print(f"Digested {count} files")
            elif action == "query" and len(parts) > 1:
                results = runtime.query_knowledge(parts[1])
                for node in results[:10]:
                    print(f"  [{node.content_type.name}] {node.source}")
            elif action == "stats":
                stats = runtime.get_stats()
                print(json.dumps(stats, indent=2))
            else:
                print(f"Unknown command: {cmd}")

    # Print final stats
    stats = runtime.get_stats()
    print(f"\nFinal stats: {stats['agents']['total']} agents, "
          f"{stats['knowledge']['nodes']} knowledge nodes, "
          f"{stats['timeline']['events']} events")

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
