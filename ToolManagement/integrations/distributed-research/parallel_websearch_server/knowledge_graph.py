"""Knowledge graph with spaCy NER and NetworkX.

Uses battle-tested open source libraries:
- spaCy: NER and entity extraction
- NetworkX: Graph operations and algorithms
- SQLite: Persistence
"""

import hashlib
import json
import logging
import sqlite3
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

logger = logging.getLogger(__name__)

# Database path - use project directory
DB_PATH = Path(__file__).parent.parent / "knowledge_graph.db"

# Lazy load spaCy to avoid startup cost if not used
_nlp = None


def get_nlp():
    """Lazy load spaCy model."""
    global _nlp
    if _nlp is None:
        import spacy
        try:
            _nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning("Downloading spaCy model en_core_web_sm...")
            from spacy.cli import download
            download("en_core_web_sm")
            _nlp = spacy.load("en_core_web_sm")
    return _nlp


# Entity type mappings from spaCy labels to our schema
ENTITY_TYPE_MAP = {
    "PERSON": "person",
    "ORG": "organization",
    "GPE": "location",  # Geo-political entity
    "LOC": "location",
    "FAC": "facility",
    "PRODUCT": "product",
    "EVENT": "event",
    "WORK_OF_ART": "work",
    "LAW": "law",
    "LANGUAGE": "language",
    "DATE": "date",
    "TIME": "time",
    "MONEY": "money",
    "QUANTITY": "quantity",
    "PERCENT": "percent",
    "CARDINAL": "number",
    "ORDINAL": "number",
    "NORP": "group",  # Nationalities, religious, political groups
}


def entity_id(text: str, entity_type: str) -> str:
    """Generate stable ID for an entity."""
    normalized = text.lower().strip()
    return hashlib.sha256(f"{entity_type}:{normalized}".encode()).hexdigest()[:16]


class Entity:
    """A node in the knowledge graph."""

    __slots__ = ("id", "text", "type", "aliases", "properties", "mentions", "first_seen", "last_seen")

    def __init__(
        self,
        text: str,
        entity_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ):
        self.id = entity_id(text, entity_type)
        self.text = text
        self.type = entity_type
        self.aliases: Set[str] = {text}
        self.properties: Dict[str, Any] = properties or {}
        self.mentions: int = 1
        self.first_seen = datetime.utcnow().isoformat()
        self.last_seen = self.first_seen

    def merge(self, other: "Entity"):
        """Merge another entity into this one."""
        self.aliases.update(other.aliases)
        self.mentions += other.mentions
        self.last_seen = datetime.utcnow().isoformat()
        # Merge properties, preferring newer values
        for k, v in other.properties.items():
            if k not in self.properties:
                self.properties[k] = v

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "type": self.type,
            "aliases": list(self.aliases),
            "properties": self.properties,
            "mentions": self.mentions,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


class Relationship:
    """An edge in the knowledge graph."""

    __slots__ = ("source_id", "target_id", "relation_type", "properties", "weight", "sources")

    def __init__(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: Optional[Dict[str, Any]] = None,
        source_doc: Optional[str] = None,
    ):
        self.source_id = source_id
        self.target_id = target_id
        self.relation_type = relation_type
        self.properties: Dict[str, Any] = properties or {}
        self.weight: int = 1
        self.sources: Set[str] = {source_doc} if source_doc else set()

    def strengthen(self, source_doc: Optional[str] = None):
        """Increase weight when relationship is seen again."""
        self.weight += 1
        if source_doc:
            self.sources.add(source_doc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation_type": self.relation_type,
            "properties": self.properties,
            "weight": self.weight,
            "sources": list(self.sources),
        }


class KnowledgeGraph:
    """Dynamic knowledge graph built from extracted entities."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self.graph = nx.MultiDiGraph()
        self.entities: Dict[str, Entity] = {}
        self.relationships: Dict[Tuple[str, str, str], Relationship] = {}
        self._doc_count = 0
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database with schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                type TEXT NOT NULL,
                aliases TEXT NOT NULL,
                properties TEXT NOT NULL,
                mentions INTEGER DEFAULT 1,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS relationships (
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                properties TEXT NOT NULL,
                weight INTEGER DEFAULT 1,
                sources TEXT NOT NULL,
                PRIMARY KEY (source_id, target_id, relation_type)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
            CREATE INDEX IF NOT EXISTS idx_entities_text ON entities(text);
            CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_id);
            CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_id);
        """)
        conn.commit()
        conn.close()
        logger.info(f"Knowledge graph DB initialized at {self.db_path}")

    def save(self):
        """Persist current state to SQLite."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Save entities
        for entity in self.entities.values():
            cursor.execute("""
                INSERT OR REPLACE INTO entities (id, text, type, aliases, properties, mentions, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entity.id,
                entity.text,
                entity.type,
                json.dumps(list(entity.aliases)),
                json.dumps(entity.properties),
                entity.mentions,
                entity.first_seen,
                entity.last_seen,
            ))

        # Save relationships
        for key, rel in self.relationships.items():
            cursor.execute("""
                INSERT OR REPLACE INTO relationships (source_id, target_id, relation_type, properties, weight, sources)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                rel.source_id,
                rel.target_id,
                rel.relation_type,
                json.dumps(rel.properties),
                rel.weight,
                json.dumps(list(rel.sources)),
            ))

        # Save metadata
        cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                       ("doc_count", str(self._doc_count)))

        conn.commit()
        conn.close()
        logger.info(f"Saved {len(self.entities)} entities and {len(self.relationships)} relationships")

    def load(self):
        """Load state from SQLite."""
        if not self.db_path.exists():
            logger.info("No existing database found, starting fresh")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Load entities
        cursor.execute("SELECT id, text, type, aliases, properties, mentions, first_seen, last_seen FROM entities")
        for row in cursor.fetchall():
            entity = Entity.__new__(Entity)
            entity.id = row[0]
            entity.text = row[1]
            entity.type = row[2]
            entity.aliases = set(json.loads(row[3]))
            entity.properties = json.loads(row[4])
            entity.mentions = row[5]
            entity.first_seen = row[6]
            entity.last_seen = row[7]
            self.entities[entity.id] = entity
            self.graph.add_node(entity.id, text=entity.text, type=entity.type)

        # Load relationships
        cursor.execute("SELECT source_id, target_id, relation_type, properties, weight, sources FROM relationships")
        for row in cursor.fetchall():
            rel = Relationship.__new__(Relationship)
            rel.source_id = row[0]
            rel.target_id = row[1]
            rel.relation_type = row[2]
            rel.properties = json.loads(row[3])
            rel.weight = row[4]
            rel.sources = set(json.loads(row[5]))
            key = (rel.source_id, rel.target_id, rel.relation_type)
            self.relationships[key] = rel
            self.graph.add_edge(rel.source_id, rel.target_id, relation=rel.relation_type, weight=rel.weight)

        # Load metadata
        cursor.execute("SELECT value FROM metadata WHERE key = 'doc_count'")
        row = cursor.fetchone()
        if row:
            self._doc_count = int(row[0])

        conn.close()
        logger.info(f"Loaded {len(self.entities)} entities and {len(self.relationships)} relationships from DB")

    def extract_entities(self, text: str) -> List[Entity]:
        """Extract named entities from text using spaCy."""
        nlp = get_nlp()
        doc = nlp(text)

        entities = []
        for ent in doc.ents:
            entity_type = ENTITY_TYPE_MAP.get(ent.label_, "other")
            entity = Entity(
                text=ent.text,
                entity_type=entity_type,
                properties={
                    "spacy_label": ent.label_,
                    "start_char": ent.start_char,
                    "end_char": ent.end_char,
                },
            )
            entities.append(entity)

        return entities

    def extract_noun_chunks(self, text: str) -> List[str]:
        """Extract noun chunks for potential concept nodes."""
        nlp = get_nlp()
        doc = nlp(text)
        return [chunk.text for chunk in doc.noun_chunks]

    def add_entity(self, entity: Entity) -> str:
        """Add or merge an entity into the graph."""
        if entity.id in self.entities:
            self.entities[entity.id].merge(entity)
        else:
            self.entities[entity.id] = entity
            self.graph.add_node(
                entity.id,
                text=entity.text,
                type=entity.type,
            )
        return entity.id

    def add_relationship(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: Optional[Dict[str, Any]] = None,
        source_doc: Optional[str] = None,
    ):
        """Add or strengthen a relationship."""
        key = (source_id, target_id, relation_type)

        if key in self.relationships:
            self.relationships[key].strengthen(source_doc)
        else:
            rel = Relationship(source_id, target_id, relation_type, properties, source_doc)
            self.relationships[key] = rel

        # Update NetworkX graph
        self.graph.add_edge(
            source_id,
            target_id,
            relation=relation_type,
            weight=self.relationships[key].weight,
        )

    def process_document(
        self,
        text: str,
        doc_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Process a document: extract entities and infer relationships."""
        self._doc_count += 1
        doc_id = doc_id or f"doc_{self._doc_count}"

        # Extract entities
        entities = self.extract_entities(text)
        entity_ids = []

        for entity in entities:
            eid = self.add_entity(entity)
            entity_ids.append(eid)

        # Infer co-occurrence relationships
        # Entities mentioned in same document are likely related
        for i, eid1 in enumerate(entity_ids):
            for eid2 in entity_ids[i + 1:]:
                if eid1 != eid2:
                    self.add_relationship(
                        eid1, eid2,
                        "co_occurs_with",
                        source_doc=doc_id,
                    )

        return {
            "doc_id": doc_id,
            "entities_found": len(entities),
            "entity_ids": entity_ids,
            "unique_entities": len(set(entity_ids)),
        }

    def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get entity by ID."""
        entity = self.entities.get(entity_id)
        return entity.to_dict() if entity else None

    def search_entities(
        self,
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Search entities by text match."""
        query_lower = query.lower()
        results = []

        for entity in self.entities.values():
            if entity_type and entity.type != entity_type:
                continue

            # Check text and aliases
            if query_lower in entity.text.lower() or any(
                query_lower in alias.lower() for alias in entity.aliases
            ):
                results.append(entity.to_dict())

            if len(results) >= limit:
                break

        return results

    def get_neighbors(
        self,
        entity_id: str,
        relation_type: Optional[str] = None,
        depth: int = 1,
    ) -> Dict[str, Any]:
        """Get neighboring entities."""
        if entity_id not in self.graph:
            return {"entity_id": entity_id, "neighbors": [], "edges": []}

        neighbors = set()
        edges = []

        # BFS to specified depth
        visited = {entity_id}
        frontier = [entity_id]

        for _ in range(depth):
            next_frontier = []
            for node in frontier:
                for neighbor in self.graph.neighbors(node):
                    edge_data = self.graph.get_edge_data(node, neighbor)
                    for key, data in edge_data.items():
                        if relation_type and data.get("relation") != relation_type:
                            continue
                        edges.append({
                            "source": node,
                            "target": neighbor,
                            "relation": data.get("relation"),
                            "weight": data.get("weight", 1),
                        })
                        if neighbor not in visited:
                            visited.add(neighbor)
                            next_frontier.append(neighbor)
                            neighbors.add(neighbor)
            frontier = next_frontier

        return {
            "entity_id": entity_id,
            "neighbors": [
                self.entities[n].to_dict()
                for n in neighbors
                if n in self.entities
            ],
            "edges": edges,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get graph statistics."""
        type_counts = defaultdict(int)
        for entity in self.entities.values():
            type_counts[entity.type] += 1

        relation_counts = defaultdict(int)
        for rel in self.relationships.values():
            relation_counts[rel.relation_type] += 1

        return {
            "total_entities": len(self.entities),
            "total_relationships": len(self.relationships),
            "documents_processed": self._doc_count,
            "entity_types": dict(type_counts),
            "relation_types": dict(relation_counts),
            "graph_density": nx.density(self.graph) if self.entities else 0,
            "connected_components": nx.number_weakly_connected_components(self.graph) if self.entities else 0,
        }

    def find_paths(
        self,
        source_id: str,
        target_id: str,
        max_length: int = 4,
    ) -> List[List[str]]:
        """Find paths between two entities."""
        if source_id not in self.graph or target_id not in self.graph:
            return []

        try:
            paths = list(nx.all_simple_paths(
                self.graph.to_undirected(),
                source_id,
                target_id,
                cutoff=max_length,
            ))
            return paths[:10]  # Limit results
        except nx.NetworkXError:
            return []

    def get_central_entities(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Get most central entities by PageRank."""
        if not self.entities:
            return []

        try:
            pagerank = nx.pagerank(self.graph)
            sorted_nodes = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)

            results = []
            for node_id, score in sorted_nodes[:top_n]:
                if node_id in self.entities:
                    entity_dict = self.entities[node_id].to_dict()
                    entity_dict["pagerank"] = score
                    results.append(entity_dict)

            return results
        except Exception as e:
            logger.warning(f"PageRank failed: {e}")
            return []

    def get_communities(self) -> List[List[str]]:
        """Detect communities in the graph."""
        if not self.entities:
            return []

        try:
            undirected = self.graph.to_undirected()
            communities = list(nx.community.greedy_modularity_communities(undirected))
            return [list(c) for c in communities[:20]]  # Limit
        except Exception as e:
            logger.warning(f"Community detection failed: {e}")
            return []

    def to_dict(self) -> Dict[str, Any]:
        """Export graph to dictionary."""
        return {
            "entities": [e.to_dict() for e in self.entities.values()],
            "relationships": [r.to_dict() for r in self.relationships.values()],
            "stats": self.get_stats(),
        }

    def to_cytoscape(self) -> Dict[str, Any]:
        """Export to Cytoscape.js format for visualization."""
        nodes = []
        for entity in self.entities.values():
            nodes.append({
                "data": {
                    "id": entity.id,
                    "label": entity.text,
                    "type": entity.type,
                    "mentions": entity.mentions,
                }
            })

        edges = []
        for rel in self.relationships.values():
            edges.append({
                "data": {
                    "id": f"{rel.source_id}-{rel.target_id}-{rel.relation_type}",
                    "source": rel.source_id,
                    "target": rel.target_id,
                    "label": rel.relation_type,
                    "weight": rel.weight,
                }
            })

        return {"nodes": nodes, "edges": edges}


# Global graph instance
_graph: Optional[KnowledgeGraph] = None


def get_knowledge_graph() -> KnowledgeGraph:
    """Get or create the global knowledge graph, loading from DB if exists."""
    global _graph
    if _graph is None:
        _graph = KnowledgeGraph()
        _graph.load()  # Load persisted data
        logger.info(f"Knowledge graph ready with {len(_graph.entities)} entities")
    return _graph


def save_knowledge_graph():
    """Save the global knowledge graph to disk."""
    global _graph
    if _graph is not None:
        _graph.save()
