"""World Model Extractor using local MLX model.

Extracts entities and relationships for the reflexive ontology schema.
Uses Qwen3-VL-8B-abliterated via MLX for free local inference.
"""

import asyncio
import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Entity type hierarchy
ENTITY_TYPES = {
    # Physical
    "person", "organization", "group", "location", "facility", "object", "substance",
    # Abstract
    "concept", "category", "property", "metric", "pattern", "process",
    # Temporal
    "event", "state", "interval", "instant",
    # Symbolic
    "product", "work", "law", "language", "protocol", "specification", "identifier",
    # Quantitative
    "quantity", "money", "duration", "distance", "probability",
}

# Relation type hierarchy
RELATION_TYPES = {
    # Compositional
    "part_of", "contains", "member_of", "instance_of", "subtype_of",
    # Causal
    "causes", "enables", "prevents", "triggers", "inhibits", "modulates",
    # Temporal
    "precedes", "overlaps", "during", "starts", "finishes",
    # Spatial
    "located_in", "near", "adjacent_to", "contains_spatially",
    # Epistemic
    "supports", "contradicts", "elaborates", "summarizes", "cites", "inferred_from",
    # Agentive
    "owns", "controls", "authored", "responsible_for", "uses", "implements",
    # Properties
    "has_property", "ensures", "requires",
}


@dataclass
class ExtractedEntity:
    """An extracted entity from text."""
    id: str
    name: str
    entity_type: str
    confidence: float = 0.9
    source_doc: str = ""
    mentions: int = 1

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.entity_type,
            "confidence": self.confidence,
            "source_doc": self.source_doc,
            "mentions": self.mentions,
        }


@dataclass
class ExtractedRelation:
    """An extracted relation between entities."""
    source_id: str
    target_id: str
    relation_type: str
    confidence: float = 0.8
    source_doc: str = ""
    context: str = ""

    def to_dict(self) -> Dict:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "type": self.relation_type,
            "confidence": self.confidence,
            "source_doc": self.source_doc,
            "context": self.context,
        }


@dataclass
class ExtractionResult:
    """Result from world model extraction."""
    entities: List[ExtractedEntity] = field(default_factory=list)
    relations: List[ExtractedRelation] = field(default_factory=list)
    raw_response: str = ""
    error: Optional[str] = None
    latency_ms: float = 0


class MLXWorldModelExtractor:
    """Extract world model (entities + relations) using local MLX model."""

    def __init__(
        self,
        model_path: str = "/Users/arthurcolle/ablit-mlx/Huihui-Qwen3-VL-8B-Instruct-abliterated-mlx",
        max_tokens: int = 2000,
        temperature: float = 0.1,
    ):
        self.model_path = model_path
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._model = None
        self._processor = None
        self._config = None

    def _load_model(self):
        """Lazy load the MLX model."""
        if self._model is None:
            from mlx_vlm import load
            from mlx_vlm.utils import load_config

            logger.info(f"Loading MLX model from {self.model_path}")
            self._model, self._processor = load(self.model_path)
            self._config = load_config(self.model_path)
            logger.info("MLX model loaded")

    def _build_prompt(self, text: str) -> str:
        """Build extraction prompt."""
        return f'''Extract a knowledge graph from this text.

ENTITY TYPES:
- Physical: person, organization, group, location, facility
- Abstract: concept, pattern, process, property, category
- Temporal: event, state
- Symbolic: product, work, protocol, specification, language, law
- Quantitative: quantity, metric, money

RELATION TYPES:
- Compositional: part_of, contains, instance_of, subtype_of, member_of
- Causal: causes, enables, prevents, triggers, inhibits
- Agentive: owns, controls, authored, uses, implements, responsible_for
- Epistemic: supports, contradicts, elaborates, cites
- Properties: has_property, ensures, requires

TEXT:
{text[:6000]}

Return JSON with:
1. "entities": [{{"id": "e1", "name": "exact name", "type": "type"}}]
2. "relations": [{{"source": "e1", "target": "e2", "type": "relation_type"}}]

Extract ALL meaningful entities and their relationships. JSON only:'''

    def _parse_response(self, response: str, doc_id: str = "") -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
        """Parse JSON response into entities and relations."""
        entities = []
        relations = []

        # Try to extract JSON
        try:
            # Handle markdown code blocks
            if "```" in response:
                match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response)
                if match:
                    response = match.group(1)

            # Find JSON boundaries
            start = response.find('{')
            end = response.rfind('}') + 1
            if start >= 0 and end > start:
                response = response[start:end]

            data = json.loads(response)

            # Parse entities
            for e in data.get("entities", []):
                if isinstance(e, dict) and "name" in e:
                    etype = e.get("type", "concept").lower()
                    if etype not in ENTITY_TYPES:
                        etype = "concept"  # default
                    entities.append(ExtractedEntity(
                        id=e.get("id", f"e{len(entities)}"),
                        name=e["name"],
                        entity_type=etype,
                        source_doc=doc_id,
                    ))

            # Parse relations
            for r in data.get("relations", []):
                if isinstance(r, dict) and "source" in r and "target" in r:
                    rtype = r.get("type", "related_to").lower()
                    if rtype not in RELATION_TYPES:
                        rtype = "related_to"
                    relations.append(ExtractedRelation(
                        source_id=r["source"],
                        target_id=r["target"],
                        relation_type=rtype,
                        source_doc=doc_id,
                    ))

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}")

        return entities, relations

    def extract(self, text: str, doc_id: str = "") -> ExtractionResult:
        """Extract entities and relations from text."""
        from mlx_vlm import generate, apply_chat_template
        import time

        self._load_model()

        start = time.perf_counter()

        prompt = self._build_prompt(text)
        formatted = apply_chat_template(self._processor, self._config, prompt, num_images=0)

        try:
            result = generate(
                self._model,
                self._processor,
                formatted,
                max_tokens=self.max_tokens,
                temp=self.temperature,
            )
            response = result.text if hasattr(result, 'text') else str(result)

            entities, relations = self._parse_response(response, doc_id)

            latency = (time.perf_counter() - start) * 1000

            return ExtractionResult(
                entities=entities,
                relations=relations,
                raw_response=response,
                latency_ms=latency,
            )

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return ExtractionResult(error=str(e))

    async def extract_async(self, text: str, doc_id: str = "") -> ExtractionResult:
        """Async wrapper for extraction."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.extract, text, doc_id)


class WorldModelIngester:
    """Ingest corpus into world model knowledge graph."""

    def __init__(
        self,
        source_db: Path,
        target_db: Path,
        extractor: Optional[MLXWorldModelExtractor] = None,
    ):
        self.source_db = source_db
        self.target_db = target_db
        self.extractor = extractor or MLXWorldModelExtractor()

        # Aggregated data
        self.entities: Dict[str, ExtractedEntity] = {}  # normalized_name -> entity
        self.relations: List[ExtractedRelation] = []
        self.entity_id_map: Dict[str, str] = {}  # local_id -> global_id

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for deduplication."""
        return name.lower().strip()

    def _aggregate_entity(self, entity: ExtractedEntity) -> str:
        """Add entity to aggregated set, return global ID."""
        key = self._normalize_name(entity.name)

        if key in self.entities:
            # Merge
            self.entities[key].mentions += 1
            return self.entities[key].id
        else:
            # New entity with global ID
            global_id = f"ent_{len(self.entities)}"
            entity.id = global_id
            self.entities[key] = entity
            return global_id

    def _resolve_relation(self, relation: ExtractedRelation, id_map: Dict[str, str]) -> Optional[ExtractedRelation]:
        """Resolve local IDs to global IDs."""
        if relation.source_id in id_map and relation.target_id in id_map:
            return ExtractedRelation(
                source_id=id_map[relation.source_id],
                target_id=id_map[relation.target_id],
                relation_type=relation.relation_type,
                confidence=relation.confidence,
                source_doc=relation.source_doc,
            )
        return None

    def ingest_document(self, doc_id: str, text: str) -> Dict[str, Any]:
        """Ingest a single document."""
        result = self.extractor.extract(text, doc_id)

        if result.error:
            return {"error": result.error, "doc_id": doc_id}

        # Build local -> global ID map for this document
        local_id_map = {}

        for entity in result.entities:
            global_id = self._aggregate_entity(entity)
            local_id_map[entity.id] = global_id

        # Resolve and add relations
        for relation in result.relations:
            resolved = self._resolve_relation(relation, local_id_map)
            if resolved:
                self.relations.append(resolved)

        return {
            "doc_id": doc_id,
            "entities_extracted": len(result.entities),
            "relations_extracted": len(result.relations),
            "latency_ms": result.latency_ms,
        }

    def ingest_from_source(self, max_docs: int = 10, chunk_size: int = 5000) -> Dict[str, Any]:
        """Ingest full documents from source database with chunking."""
        conn = sqlite3.connect(self.source_db)
        cursor = conn.cursor()

        # Get full document content
        cursor.execute("""
            SELECT id, name, content FROM documents
            WHERE content IS NOT NULL AND content != ''
            LIMIT ?
        """, (max_docs,))

        results = []
        for doc_id, name, content in cursor.fetchall():
            if content:
                logger.info(f"Processing: {name} ({len(content):,} chars)")

                # Chunk large documents
                chunks = self._chunk_text(content, chunk_size)
                logger.info(f"  Split into {len(chunks)} chunks")

                doc_entities = 0
                doc_relations = 0
                total_latency = 0

                for i, chunk in enumerate(chunks):
                    chunk_id = f"{doc_id}_chunk_{i}"
                    result = self.ingest_document(chunk_id, chunk)

                    if not result.get("error"):
                        doc_entities += result.get("entities_extracted", 0)
                        doc_relations += result.get("relations_extracted", 0)
                        total_latency += result.get("latency_ms", 0)

                    logger.info(f"  Chunk {i+1}/{len(chunks)}: {result.get('entities_extracted', 0)} entities, {result.get('relations_extracted', 0)} relations")

                results.append({
                    "doc_id": doc_id,
                    "name": name,
                    "chunks": len(chunks),
                    "entities_extracted": doc_entities,
                    "relations_extracted": doc_relations,
                    "latency_ms": total_latency,
                })

        conn.close()

        return {
            "documents_processed": len(results),
            "total_entities": len(self.entities),
            "total_relations": len(self.relations),
            "results": results,
        }

    def _chunk_text(self, text: str, chunk_size: int = 5000) -> List[str]:
        """Split text into chunks at paragraph boundaries."""
        if len(text) <= chunk_size:
            return [text]

        chunks = []
        paragraphs = text.split('\n\n')
        current_chunk = []
        current_size = 0

        for para in paragraphs:
            para_size = len(para) + 2  # +2 for \n\n

            if current_size + para_size > chunk_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = [para]
                current_size = para_size
            else:
                current_chunk.append(para)
                current_size += para_size

        if current_chunk:
            chunks.append('\n\n'.join(current_chunk))

        return chunks

    def save_to_knowledge_graph(self):
        """Save extracted world model to knowledge graph database."""
        from parallel_websearch_server.knowledge_graph import (
            Entity, KnowledgeGraph, get_knowledge_graph, save_knowledge_graph
        )

        graph = get_knowledge_graph()

        # Build ID -> entity map for quick lookup
        id_to_entity = {ent.id: ent for ent in self.entities.values()}

        # Add entities
        for entity in self.entities.values():
            kg_entity = Entity(
                text=entity.name,
                entity_type=entity.entity_type,
                properties={
                    "confidence": entity.confidence,
                    "extraction_method": "mlx_world_model",
                    "source_doc": entity.source_doc,
                    "mention_count": entity.mentions,
                }
            )
            graph.add_entity(kg_entity)

        # Build name -> graph entity ID map (after entities are added)
        name_to_kg_id = {}
        for entity in graph.entities.values():
            name_to_kg_id[entity.text.lower()] = entity.id

        # Add relationships with proper ID resolution
        relations_added = 0
        for relation in self.relations:
            # Look up source and target entities by their global IDs
            source_ent = id_to_entity.get(relation.source_id)
            target_ent = id_to_entity.get(relation.target_id)

            if source_ent and target_ent:
                # Map to knowledge graph entity IDs
                source_kg_id = name_to_kg_id.get(source_ent.name.lower())
                target_kg_id = name_to_kg_id.get(target_ent.name.lower())

                if source_kg_id and target_kg_id:
                    graph.add_relationship(
                        source_kg_id,
                        target_kg_id,
                        relation.relation_type,
                        properties={"confidence": relation.confidence},
                        source_doc=relation.source_doc,
                    )
                    relations_added += 1

        logger.info(f"Added {relations_added}/{len(self.relations)} relationships to graph")
        save_knowledge_graph()

        return graph.get_stats()


# Convenience functions
_extractor: Optional[MLXWorldModelExtractor] = None


def get_extractor() -> MLXWorldModelExtractor:
    """Get or create global extractor instance."""
    global _extractor
    if _extractor is None:
        _extractor = MLXWorldModelExtractor()
    return _extractor


def extract_world_model(text: str, doc_id: str = "") -> ExtractionResult:
    """Extract world model from text."""
    return get_extractor().extract(text, doc_id)


async def run_world_model_ingestion(
    source_db: str = "/Users/arthurcolle/parallel_research_v2/dagi_research.db",
    max_docs: int = 10,
    chunk_size: int = 5000,
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """Run full world model ingestion pipeline with chunked document processing."""
    from parallel_websearch_server.knowledge_graph import get_knowledge_graph, save_knowledge_graph

    if clear_existing:
        # Clear existing entities and relationships
        graph = get_knowledge_graph()
        graph.entities.clear()
        graph.relationships.clear()
        graph.graph.clear()
        save_knowledge_graph()
        logger.info("Cleared existing knowledge graph")

    target_db = Path(__file__).parent.parent / "knowledge_graph.db"

    ingester = WorldModelIngester(
        source_db=Path(source_db),
        target_db=target_db,
    )

    # Ingest from source with full document content
    result = ingester.ingest_from_source(max_docs=max_docs, chunk_size=chunk_size)

    # Save to knowledge graph
    graph_stats = ingester.save_to_knowledge_graph()
    result["graph_stats"] = graph_stats

    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    # Test extraction
    test_text = """
    Dynamic registration enables runtime modification. Lisp and Clojure implement
    homoiconicity. Google and Amazon use Kubernetes for orchestration.
    """

    print("Testing MLX World Model Extractor...")
    result = extract_world_model(test_text, "test_doc")

    print(f"\nEntities ({len(result.entities)}):")
    for e in result.entities:
        print(f"  - {e.name} ({e.entity_type})")

    print(f"\nRelations ({len(result.relations)}):")
    for r in result.relations:
        print(f"  - {r.source_id} --[{r.relation_type}]--> {r.target_id}")

    print(f"\nLatency: {result.latency_ms:.0f}ms")
