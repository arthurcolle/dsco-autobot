"""Knowledge Graph builder with entity extraction and relationship mapping."""

import asyncio
import hashlib
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class EntityType(str):
    """Common entity types."""
    PERSON = "PERSON"
    ORGANIZATION = "ORG"
    LOCATION = "LOC"
    PRODUCT = "PRODUCT"
    EVENT = "EVENT"
    TECHNOLOGY = "TECH"
    CONCEPT = "CONCEPT"
    DATE = "DATE"
    MONEY = "MONEY"


class RelationType(str):
    """Relationship types."""
    WORKS_FOR = "works_for"
    LOCATED_IN = "located_in"
    FOUNDED = "founded"
    ACQUIRED = "acquired"
    PARTNERED_WITH = "partnered_with"
    DEVELOPS = "develops"
    COMPETES_WITH = "competes_with"
    RELATED_TO = "related_to"
    MENTIONED_WITH = "mentioned_with"


@dataclass
class Entity:
    """Extracted entity."""
    id: str
    name: str
    type: str
    mentions: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    sources: List[str] = field(default_factory=list)
    confidence: float = 1.0
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "mentions": self.mentions,
            "attributes": self.attributes,
            "sources": self.sources,
            "confidence": self.confidence,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
        }


@dataclass
class Relationship:
    """Relationship between entities."""
    id: str
    source_id: str
    target_id: str
    type: str
    properties: Dict[str, Any] = field(default_factory=dict)
    evidence: List[str] = field(default_factory=list)
    confidence: float = 1.0
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "type": self.type,
            "properties": self.properties,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "created_at": self.created_at.isoformat(),
        }


class KnowledgeGraphBuilder:
    """
    Advanced knowledge graph builder with entity extraction,
    relationship mapping, and graph construction.
    """

    def __init__(
        self,
        graph_store: Optional[Any] = None,
        use_nlp: bool = True,
        min_entity_confidence: float = 0.5,
    ):
        """Initialize KG builder.

        Args:
            graph_store: Optional graph database (Neo4j, etc.)
            use_nlp: Use NLP for entity extraction
            min_entity_confidence: Minimum confidence threshold
        """
        self.graph_store = graph_store
        self.use_nlp = use_nlp
        self.min_entity_confidence = min_entity_confidence
        self._entity_cache: Dict[str, Entity] = {}
        self._relationship_cache: Dict[str, Relationship] = {}

        # Initialize NLP if available
        self._nlp = None
        if use_nlp:
            try:
                import spacy
                # Try to load model, fallback to simple extraction if unavailable
                try:
                    self._nlp = spacy.load("en_core_web_sm")
                    logger.info("Loaded spaCy model for entity extraction")
                except OSError:
                    logger.warning("spaCy model not found, using pattern-based extraction")
            except ImportError:
                logger.warning("spaCy not installed, using pattern-based extraction")

    async def extract_entities(
        self,
        search_results: List[Dict[str, Any]],
        extracted_content: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Extract entities from search results and content.

        Args:
            search_results: Search results to process
            extracted_content: Optional extracted content

        Returns:
            List of extracted entities
        """
        entities = []
        seen_entities = set()

        # Process search results
        for result in search_results:
            title = result.get("title", "")
            excerpts = result.get("excerpts", [])
            url = result.get("url", "")

            # Extract from title
            title_entities = await self._extract_from_text(title, url)
            entities.extend(title_entities)

            # Extract from excerpts
            for excerpt in excerpts:
                if isinstance(excerpt, str):
                    excerpt_entities = await self._extract_from_text(excerpt, url)
                    entities.extend(excerpt_entities)

        # Process extracted content
        if extracted_content:
            for content in extracted_content:
                full_text = content.get("full_content", "")
                url = content.get("url", "")
                if full_text:
                    content_entities = await self._extract_from_text(full_text[:2000], url)
                    entities.extend(content_entities)

        # Deduplicate and merge entities
        merged = self._merge_entities(entities)

        # Filter by confidence
        filtered = [
            e.to_dict()
            for e in merged
            if e.confidence >= self.min_entity_confidence
        ]

        logger.info(f"Extracted {len(filtered)} unique entities")
        return filtered

    async def extract_relationships(
        self,
        entities: List[Dict[str, Any]],
        content: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Extract relationships between entities.

        Args:
            entities: List of entities
            content: Content to analyze for relationships

        Returns:
            List of relationships
        """
        relationships = []

        # Build entity lookup
        entity_lookup = {e["name"].lower(): e["id"] for e in entities}

        # Extract relationships from content
        for item in content:
            full_text = item.get("full_content", "")
            if not full_text:
                continue

            # Find co-occurrences
            for entity1 in entities:
                name1 = entity1["name"]
                for entity2 in entities:
                    if entity1["id"] == entity2["id"]:
                        continue

                    name2 = entity2["name"]

                    # Check if both entities appear in the same content
                    if name1.lower() in full_text.lower() and name2.lower() in full_text.lower():
                        rel = await self._infer_relationship(
                            entity1, entity2, full_text
                        )
                        if rel:
                            relationships.append(rel.to_dict())

        logger.info(f"Extracted {len(relationships)} relationships")
        return relationships

    async def update_graph(
        self,
        entities: List[Dict[str, Any]],
        relationships: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Update the knowledge graph.

        Args:
            entities: Entities to add/update
            relationships: Relationships to add/update
            metadata: Optional metadata

        Returns:
            List of updates performed
        """
        updates = []

        # Add/update entities
        for entity_dict in entities:
            entity_id = entity_dict["id"]
            if entity_id in self._entity_cache:
                # Merge with existing
                existing = self._entity_cache[entity_id]
                existing.last_seen = datetime.utcnow()
                existing.mentions.extend(entity_dict.get("mentions", []))
                existing.sources.extend(entity_dict.get("sources", []))
                updates.append({"type": "entity_updated", "id": entity_id})
            else:
                # Create new
                entity = Entity(
                    id=entity_dict["id"],
                    name=entity_dict["name"],
                    type=entity_dict["type"],
                    mentions=entity_dict.get("mentions", []),
                    attributes=entity_dict.get("attributes", {}),
                    sources=entity_dict.get("sources", []),
                    confidence=entity_dict.get("confidence", 1.0),
                )
                self._entity_cache[entity_id] = entity
                updates.append({"type": "entity_created", "id": entity_id})

        # Add/update relationships
        for rel_dict in relationships:
            rel_id = rel_dict["id"]
            if rel_id not in self._relationship_cache:
                rel = Relationship(
                    id=rel_dict["id"],
                    source_id=rel_dict["source_id"],
                    target_id=rel_dict["target_id"],
                    type=rel_dict["type"],
                    properties=rel_dict.get("properties", {}),
                    evidence=rel_dict.get("evidence", []),
                    confidence=rel_dict.get("confidence", 1.0),
                )
                self._relationship_cache[rel_id] = rel
                updates.append({"type": "relationship_created", "id": rel_id})

        # Persist to graph store if available
        if self.graph_store:
            await self._persist_to_store(entities, relationships)

        logger.info(f"Applied {len(updates)} graph updates")
        return updates

    async def _extract_from_text(self, text: str, source: str) -> List[Entity]:
        """Extract entities from text."""
        entities = []

        if self._nlp:
            # Use spaCy for extraction
            doc = self._nlp(text[:1000])  # Limit text length
            for ent in doc.ents:
                entity_id = self._generate_entity_id(ent.text, ent.label_)
                entities.append(Entity(
                    id=entity_id,
                    name=ent.text,
                    type=ent.label_,
                    mentions=[text],
                    sources=[source],
                    confidence=0.8,
                ))
        else:
            # Fallback to pattern-based extraction
            entities.extend(self._pattern_based_extraction(text, source))

        return entities

    def _pattern_based_extraction(self, text: str, source: str) -> List[Entity]:
        """Extract entities using patterns."""
        entities = []

        # Capitalized phrases (likely names, orgs)
        capitalized_pattern = r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b'
        matches = re.findall(capitalized_pattern, text)

        for match in matches:
            # Skip common words
            if match.lower() in {'the', 'this', 'that', 'these', 'those', 'and', 'or'}:
                continue

            # Heuristic type detection
            entity_type = EntityType.CONCEPT
            if len(match.split()) >= 2:
                entity_type = EntityType.ORGANIZATION
            if match.endswith(' Inc') or match.endswith(' Corp') or match.endswith(' LLC'):
                entity_type = EntityType.ORGANIZATION

            entity_id = self._generate_entity_id(match, entity_type)
            entities.append(Entity(
                id=entity_id,
                name=match,
                type=entity_type,
                mentions=[text[:200]],
                sources=[source],
                confidence=0.6,
            ))

        return entities

    def _merge_entities(self, entities: List[Entity]) -> List[Entity]:
        """Merge duplicate entities."""
        merged: Dict[str, Entity] = {}

        for entity in entities:
            if entity.id in merged:
                # Merge
                existing = merged[entity.id]
                existing.mentions.extend(entity.mentions)
                existing.sources.extend(entity.sources)
                existing.confidence = max(existing.confidence, entity.confidence)
            else:
                merged[entity.id] = entity

        return list(merged.values())

    async def _infer_relationship(
        self,
        entity1: Dict[str, Any],
        entity2: Dict[str, Any],
        context: str,
    ) -> Optional[Relationship]:
        """Infer relationship between entities from context."""
        # Pattern-based relationship detection
        patterns = {
            RelationType.WORKS_FOR: [
                r"{e1}.*(?:works for|employed by|engineer at|CEO of).*{e2}",
                r"{e2}.*(?:hired|employs).*{e1}",
            ],
            RelationType.FOUNDED: [
                r"{e1}.*(?:founded|created|started).*{e2}",
            ],
            RelationType.ACQUIRED: [
                r"{e1}.*(?:acquired|bought).*{e2}",
                r"{e2}.*(?:acquired by).*{e1}",
            ],
            RelationType.PARTNERED_WITH: [
                r"{e1}.*(?:partnered with|collaboration with).*{e2}",
            ],
        }

        name1 = re.escape(entity1["name"])
        name2 = re.escape(entity2["name"])

        for rel_type, pattern_list in patterns.items():
            for pattern_template in pattern_list:
                pattern = pattern_template.format(e1=name1, e2=name2)
                if re.search(pattern, context, re.IGNORECASE):
                    rel_id = self._generate_relationship_id(
                        entity1["id"], entity2["id"], rel_type
                    )
                    return Relationship(
                        id=rel_id,
                        source_id=entity1["id"],
                        target_id=entity2["id"],
                        type=rel_type,
                        evidence=[context[:200]],
                        confidence=0.7,
                    )

        # Default: mentioned together
        rel_id = self._generate_relationship_id(
            entity1["id"], entity2["id"], RelationType.MENTIONED_WITH
        )
        return Relationship(
            id=rel_id,
            source_id=entity1["id"],
            target_id=entity2["id"],
            type=RelationType.MENTIONED_WITH,
            evidence=[context[:200]],
            confidence=0.5,
        )

    def _generate_entity_id(self, name: str, entity_type: str) -> str:
        """Generate stable entity ID."""
        normalized = f"{entity_type}:{name.lower().strip()}"
        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    def _generate_relationship_id(
        self,
        source_id: str,
        target_id: str,
        rel_type: str,
    ) -> str:
        """Generate stable relationship ID."""
        normalized = f"{source_id}:{rel_type}:{target_id}"
        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    async def _persist_to_store(
        self,
        entities: List[Dict[str, Any]],
        relationships: List[Dict[str, Any]],
    ):
        """Persist to graph store (placeholder)."""
        # TODO: Implement Neo4j/other graph DB persistence
        logger.debug("Graph persistence not yet implemented")

    def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get entity by ID."""
        entity = self._entity_cache.get(entity_id)
        return entity.to_dict() if entity else None

    def get_relationships(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get all relationships for an entity."""
        relationships = []
        for rel in self._relationship_cache.values():
            if rel.source_id == entity_id or rel.target_id == entity_id:
                relationships.append(rel.to_dict())
        return relationships

    def get_graph_stats(self) -> Dict[str, Any]:
        """Get knowledge graph statistics."""
        entity_types = defaultdict(int)
        for entity in self._entity_cache.values():
            entity_types[entity.type] += 1

        rel_types = defaultdict(int)
        for rel in self._relationship_cache.values():
            rel_types[rel.type] += 1

        return {
            "total_entities": len(self._entity_cache),
            "total_relationships": len(self._relationship_cache),
            "entity_types": dict(entity_types),
            "relationship_types": dict(rel_types),
        }
