"""DSPy-based World Model Extractor with constrained signatures.

Uses DSPy to enforce structured output from the local MLX model.
Enables parallel extraction across document chunks.
"""

import asyncio
import json
import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal, Annotated
from datetime import datetime

import dspy
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Entity and Relation type literals for constrained output
ENTITY_TYPES = Literal[
    # Physical
    "person", "organization", "group", "location", "facility", "object", "substance",
    # Abstract
    "concept", "category", "property", "metric", "pattern", "process",
    # Temporal
    "event", "state", "interval", "instant",
    # Symbolic
    "product", "work", "law", "language", "protocol", "specification", "identifier",
    # Quantitative
    "quantity", "money", "duration", "distance", "probability"
]

RELATION_TYPES = Literal[
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
    "has_property", "ensures", "requires"
]

# ============================================================================
# Pydantic Models for Structured Output
# ============================================================================

class ExtractedEntity(BaseModel):
    """A single extracted entity."""
    id: str = Field(description="Unique identifier like e1, e2, etc.")
    name: str = Field(description="The exact name/text of the entity")
    entity_type: str = Field(description="The semantic type: person, organization, concept, product, process, etc.")


class ExtractedRelation(BaseModel):
    """A single extracted relation between entities."""
    source: str = Field(description="ID of the source entity (e.g., e1)")
    target: str = Field(description="ID of the target entity (e.g., e2)")
    relation_type: str = Field(description="The relationship type: enables, uses, part_of, causes, etc.")


class KnowledgeGraphOutput(BaseModel):
    """Extracted knowledge graph from text."""
    entities: List[ExtractedEntity] = Field(default_factory=list, description="List of entities")
    relations: List[ExtractedRelation] = Field(default_factory=list, description="List of relations")


# ============================================================================
# DSPy Signatures using dspy.Type for structured output
# ============================================================================

# Define the signature as a simple class with typed fields
class ExtractKnowledgeGraph(dspy.Signature):
    """Extract entities and relationships from text into a knowledge graph.

    Entity types: person, organization, concept, product, process, protocol, language, event, location, metric
    Relation types: enables, uses, implements, causes, part_of, contains, subtype_of, supports, authored, owns
    """

    text: str = dspy.InputField(desc="Text to extract knowledge from")
    entities: List[ExtractedEntity] = dspy.OutputField(desc="Entities with id, name, entity_type")
    relations: List[ExtractedRelation] = dspy.OutputField(desc="Relations with source, target, relation_type")


# ============================================================================
# Custom MLX Language Model for DSPy
# ============================================================================

class MLXLM(dspy.LM):
    """DSPy Language Model wrapper for local MLX models."""

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
        self.history = []

        # Required by DSPy
        self.model = f"mlx:{Path(model_path).name}"
        self.model_type = "chat"
        self.kwargs = {
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

    def _load(self):
        """Lazy load the MLX model."""
        if self._model is None:
            from mlx_vlm import load
            from mlx_vlm.utils import load_config

            logger.info(f"Loading MLX model from {self.model_path}")
            self._model, self._processor = load(self.model_path)
            self._config = load_config(self.model_path)
            logger.info("MLX model loaded")

    def __call__(self, prompt: str = None, messages: List[Dict] = None, **kwargs) -> List[Dict[str, Any]]:
        """Generate a response from the model."""
        from mlx_vlm import generate, apply_chat_template

        self._load()

        # Build prompt from messages or use direct prompt
        if messages:
            # Convert messages to prompt
            text_parts = []
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "system":
                    text_parts.append(f"System: {content}")
                elif role == "user":
                    text_parts.append(f"User: {content}")
                elif role == "assistant":
                    text_parts.append(f"Assistant: {content}")
            prompt = "\n\n".join(text_parts)

        if not prompt:
            return [{"text": "", "logprobs": None}]

        # Format with chat template
        formatted = apply_chat_template(self._processor, self._config, prompt, num_images=0)

        # Generate
        max_tokens = kwargs.get("max_tokens", self.max_tokens)
        temperature = kwargs.get("temperature", self.temperature)

        result = generate(
            self._model,
            self._processor,
            formatted,
            max_tokens=max_tokens,
            temp=temperature,
        )

        response_text = result.text if hasattr(result, 'text') else str(result)

        # Store in history
        self.history.append({
            "prompt": prompt,
            "response": response_text,
            "kwargs": kwargs,
        })

        return [{"text": response_text, "logprobs": None}]

    def basic_request(self, prompt: str, **kwargs):
        """Basic request method for compatibility."""
        return self(prompt=prompt, **kwargs)

    def inspect_history(self, n: int = 1):
        """Return last n interactions."""
        return self.history[-n:]


# ============================================================================
# DSPy Modules for Extraction
# ============================================================================

class KnowledgeGraphExtractor(dspy.Module):
    """Extract complete knowledge graph in one pass using DSPy."""

    def __init__(self):
        super().__init__()
        self.predictor = dspy.Predict(ExtractKnowledgeGraph)

    def forward(self, text: str) -> KnowledgeGraphOutput:
        result = self.predictor(text=text[:5000])  # Truncate for context
        return KnowledgeGraphOutput(
            entities=result.entities if isinstance(result.entities, list) else [],
            relations=result.relations if isinstance(result.relations, list) else [],
        )


# ============================================================================
# Parallel World Model Ingester
# ============================================================================

@dataclass
class ChunkResult:
    """Result from processing a single chunk."""
    chunk_id: str
    entities: List[Dict]
    relations: List[Dict]
    error: Optional[str] = None
    latency_ms: float = 0


class DSPyWorldModelIngester:
    """Ingest documents using DSPy with parallel chunk processing."""

    def __init__(
        self,
        source_db: Path,
        max_workers: int = 4,
        chunk_size: int = 4000,
    ):
        self.source_db = source_db
        self.max_workers = max_workers
        self.chunk_size = chunk_size

        # Initialize DSPy with MLX
        self.lm = MLXLM()
        dspy.configure(lm=self.lm)

        # Extractor module
        self.extractor = KnowledgeGraphExtractor()

        # Aggregated results
        self.all_entities: Dict[str, Dict] = {}  # normalized_name -> entity
        self.all_relations: List[Dict] = []
        self.entity_id_map: Dict[str, str] = {}  # local_id -> global_id

    def _chunk_text(self, text: str) -> List[str]:
        """Split text into chunks at paragraph boundaries."""
        if len(text) <= self.chunk_size:
            return [text]

        chunks = []
        paragraphs = text.split('\n\n')
        current_chunk = []
        current_size = 0

        for para in paragraphs:
            para_size = len(para) + 2

            if current_size + para_size > self.chunk_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = [para]
                current_size = para_size
            else:
                current_chunk.append(para)
                current_size += para_size

        if current_chunk:
            chunks.append('\n\n'.join(current_chunk))

        return chunks

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for deduplication."""
        return name.lower().strip()

    def _process_chunk(self, chunk_id: str, text: str) -> ChunkResult:
        """Process a single chunk and extract knowledge graph."""
        import time
        start = time.perf_counter()

        try:
            kg = self.extractor(text=text)

            entities = [e.model_dump() for e in kg.entities]
            relations = [r.model_dump() for r in kg.relations]

            latency = (time.perf_counter() - start) * 1000

            return ChunkResult(
                chunk_id=chunk_id,
                entities=entities,
                relations=relations,
                latency_ms=latency,
            )
        except Exception as e:
            logger.error(f"Chunk {chunk_id} failed: {e}")
            return ChunkResult(
                chunk_id=chunk_id,
                entities=[],
                relations=[],
                error=str(e),
                latency_ms=(time.perf_counter() - start) * 1000,
            )

    def _aggregate_entity(self, entity: Dict, chunk_id: str) -> str:
        """Add entity to aggregated set, return global ID."""
        key = self._normalize_name(entity["name"])

        if key in self.all_entities:
            self.all_entities[key]["mentions"] = self.all_entities[key].get("mentions", 1) + 1
            return self.all_entities[key]["global_id"]
        else:
            global_id = f"ent_{len(self.all_entities)}"
            self.all_entities[key] = {
                **entity,
                "global_id": global_id,
                "source_chunk": chunk_id,
                "mentions": 1,
            }
            return global_id

    def _aggregate_results(self, results: List[ChunkResult]):
        """Aggregate results from multiple chunks."""
        for result in results:
            if result.error:
                continue

            # Build local -> global ID map for this chunk
            local_id_map = {}
            for entity in result.entities:
                global_id = self._aggregate_entity(entity, result.chunk_id)
                local_id_map[entity["id"]] = global_id

            # Resolve and add relations
            for relation in result.relations:
                if relation["source"] in local_id_map and relation["target"] in local_id_map:
                    self.all_relations.append({
                        "source": local_id_map[relation["source"]],
                        "target": local_id_map[relation["target"]],
                        "type": relation["relation_type"],
                        "source_chunk": result.chunk_id,
                    })

    def process_document(self, doc_id: str, content: str) -> Dict[str, Any]:
        """Process a single document with parallel chunk extraction."""
        chunks = self._chunk_text(content)
        logger.info(f"Processing doc {doc_id}: {len(content):,} chars, {len(chunks)} chunks")

        results = []

        # Process chunks (sequentially for now due to model loading)
        # TODO: True parallelism with multiple model instances
        for i, chunk in enumerate(chunks):
            chunk_id = f"{doc_id}_chunk_{i}"
            result = self._process_chunk(chunk_id, chunk)
            results.append(result)
            logger.info(f"  Chunk {i+1}/{len(chunks)}: {len(result.entities)} entities, {len(result.relations)} relations")

        # Aggregate
        self._aggregate_results(results)

        total_entities = sum(len(r.entities) for r in results)
        total_relations = sum(len(r.relations) for r in results)
        total_latency = sum(r.latency_ms for r in results)
        errors = [r.error for r in results if r.error]

        return {
            "doc_id": doc_id,
            "chunks": len(chunks),
            "entities_extracted": total_entities,
            "relations_extracted": total_relations,
            "latency_ms": total_latency,
            "errors": errors,
        }

    def ingest_from_source(self, max_docs: int = 10) -> Dict[str, Any]:
        """Ingest documents from source database."""
        conn = sqlite3.connect(self.source_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, name, content FROM documents
            WHERE content IS NOT NULL AND content != ''
            LIMIT ?
        """, (max_docs,))

        results = []
        for doc_id, name, content in cursor.fetchall():
            if content:
                logger.info(f"\nProcessing: {name}")
                result = self.process_document(doc_id, content)
                result["name"] = name
                results.append(result)

        conn.close()

        return {
            "documents_processed": len(results),
            "total_unique_entities": len(self.all_entities),
            "total_relations": len(self.all_relations),
            "results": results,
        }

    def save_to_knowledge_graph(self) -> Dict[str, Any]:
        """Save aggregated results to knowledge graph."""
        from parallel_websearch_server.knowledge_graph import (
            Entity, get_knowledge_graph, save_knowledge_graph
        )

        graph = get_knowledge_graph()

        # Build global_id -> entity map
        id_to_entity = {e["global_id"]: e for e in self.all_entities.values()}

        # Add entities
        for entity_data in self.all_entities.values():
            kg_entity = Entity(
                text=entity_data["name"],
                entity_type=entity_data["entity_type"],
                properties={
                    "confidence": 0.9,
                    "extraction_method": "dspy_mlx",
                    "source_chunk": entity_data.get("source_chunk", ""),
                    "mention_count": entity_data.get("mentions", 1),
                }
            )
            graph.add_entity(kg_entity)

        # Build name -> kg entity ID map
        name_to_kg_id = {}
        for entity in graph.entities.values():
            name_to_kg_id[entity.text.lower()] = entity.id

        # Add relations
        relations_added = 0
        for relation in self.all_relations:
            source_ent = id_to_entity.get(relation["source"])
            target_ent = id_to_entity.get(relation["target"])

            if source_ent and target_ent:
                source_kg_id = name_to_kg_id.get(source_ent["name"].lower())
                target_kg_id = name_to_kg_id.get(target_ent["name"].lower())

                if source_kg_id and target_kg_id:
                    graph.add_relationship(
                        source_kg_id,
                        target_kg_id,
                        relation["type"],
                        properties={"confidence": 0.85},
                        source_doc=relation.get("source_chunk", ""),
                    )
                    relations_added += 1

        logger.info(f"Added {relations_added}/{len(self.all_relations)} relationships to graph")
        save_knowledge_graph()

        return graph.get_stats()


# ============================================================================
# Main Entry Points
# ============================================================================

async def run_dspy_ingestion(
    source_db: str = "/Users/arthurcolle/parallel_research_v2/dagi_research.db",
    max_docs: int = 10,
    chunk_size: int = 4000,
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """Run DSPy-based world model ingestion."""
    from parallel_websearch_server.knowledge_graph import get_knowledge_graph, save_knowledge_graph

    if clear_existing:
        graph = get_knowledge_graph()
        graph.entities.clear()
        graph.relationships.clear()
        graph.graph.clear()
        save_knowledge_graph()
        logger.info("Cleared existing knowledge graph")

    ingester = DSPyWorldModelIngester(
        source_db=Path(source_db),
        chunk_size=chunk_size,
    )

    result = ingester.ingest_from_source(max_docs=max_docs)

    graph_stats = ingester.save_to_knowledge_graph()
    result["graph_stats"] = graph_stats

    return result


def test_dspy_extraction():
    """Test DSPy extraction on sample text."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    # Initialize
    lm = MLXLM()
    dspy.configure(lm=lm)

    extractor = KnowledgeGraphExtractor()

    test_text = """
    Dynamic registration enables runtime modification. Lisp and Clojure implement
    homoiconicity, which means code can be treated as data. Google and Amazon
    use Kubernetes for container orchestration. The transformer architecture
    enables attention mechanisms that power modern language models like GPT-4.
    """

    print("Testing DSPy Knowledge Graph Extraction...")
    print(f"Input: {test_text[:200]}...")

    try:
        kg = extractor(text=test_text)

        print(f"\nEntities ({len(kg.entities)}):")
        for e in kg.entities:
            if hasattr(e, 'name'):
                print(f"  - {e.name} ({e.entity_type})")
            else:
                print(f"  - {e}")

        print(f"\nRelations ({len(kg.relations)}):")
        for r in kg.relations:
            if hasattr(r, 'source'):
                print(f"  - {r.source} --[{r.relation_type}]--> {r.target}")
            else:
                print(f"  - {r}")

        return kg
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    test_dspy_extraction()
