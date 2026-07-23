"""World model batch ingestion with full reflexive ontology.

Extracts entities AND relationships from the entire corpus.
Stores in knowledge graph with proper typing and grounding categories.
"""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from parallel_websearch_server.batch_ingest import Chunk, prepare_corpus
from parallel_websearch_server.reflexive_ontology import (
    ReflexiveOntologyExtractor,
    get_grounding_category,
    get_relation_category,
    ENTITY_TYPES,
    RELATION_TYPES,
)

logger = logging.getLogger(__name__)

# HIGH PARALLELISM - devour corpus in <45 seconds
MAX_CONCURRENT_CHUNKS = 500  # All chunks at once
BATCH_SIZE = 500             # Process all in one batch

# Cost estimation per model (USD per 1M tokens)
# Based on OpenRouter pricing as of 2024
MODEL_COSTS = {
    "google/gemini-2.0-flash-lite-001": {"input": 0.075, "output": 0.30},
    "meta-llama/llama-3.1-8b-instruct": {"input": 0.055, "output": 0.055},
    "qwen/qwen-2.5-7b-instruct": {"input": 0.27, "output": 0.27},
    "deepseek/deepseek-chat": {"input": 0.14, "output": 0.28},
    # Defaults for unknown models
    "default": {"input": 0.15, "output": 0.30},
}

# Average tokens per chunk (estimated)
AVG_INPUT_TOKENS_PER_CHUNK = 1500  # ~4000 chars / 2.7 chars per token
AVG_OUTPUT_TOKENS_PER_CHUNK = 400  # JSON response ~400 tokens


def estimate_cost(
    num_chunks: int,
    num_models: int = 4,
    input_tokens_per_chunk: int = AVG_INPUT_TOKENS_PER_CHUNK,
    output_tokens_per_chunk: int = AVG_OUTPUT_TOKENS_PER_CHUNK,
    models: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Precompute estimated cost for ingestion run."""
    models = models or list(MODEL_COSTS.keys())[:4]

    total_input_tokens = num_chunks * num_models * input_tokens_per_chunk
    total_output_tokens = num_chunks * num_models * output_tokens_per_chunk

    # Calculate per-model costs
    model_costs = {}
    total_cost = 0.0

    for model in models:
        costs = MODEL_COSTS.get(model, MODEL_COSTS["default"])
        input_cost = (num_chunks * input_tokens_per_chunk / 1_000_000) * costs["input"]
        output_cost = (num_chunks * output_tokens_per_chunk / 1_000_000) * costs["output"]
        model_total = input_cost + output_cost
        model_costs[model.split("/")[-1]] = {
            "input_cost": round(input_cost, 4),
            "output_cost": round(output_cost, 4),
            "total": round(model_total, 4),
        }
        total_cost += model_total

    return {
        "num_chunks": num_chunks,
        "num_models": num_models,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_input_tokens + total_output_tokens,
        "per_model_costs": model_costs,
        "estimated_total_cost_usd": round(total_cost, 4),
        "cost_per_chunk": round(total_cost / num_chunks, 6) if num_chunks > 0 else 0,
    }


class WorldModelIngester:
    """Ingest corpus into full world model with entities and relationships."""

    def __init__(
        self,
        corpus_path: Path,
        models: Optional[List[str]] = None,
        max_concurrent_chunks: int = MAX_CONCURRENT_CHUNKS,
        batch_size: int = BATCH_SIZE,
    ):
        self.corpus_path = corpus_path
        self.models = models
        self.max_concurrent = max_concurrent_chunks
        self.batch_size = batch_size
        self._semaphore = asyncio.Semaphore(max_concurrent_chunks)

        # Aggregated entities (by normalized name)
        self.entity_data: Dict[str, Dict] = defaultdict(lambda: {
            "names": [],
            "types": defaultdict(int),
            "grounding_categories": defaultdict(int),
            "confidences": [],
            "sources": set(),
            "properties": {},
        })

        # Aggregated relationships (by source|target key)
        self.relation_data: Dict[str, Dict] = defaultdict(lambda: {
            "types": defaultdict(int),
            "categories": defaultdict(int),
            "confidences": [],
            "contexts": [],
            "sources": set(),
        })

        # Stats
        self.processed_chunks = 0
        self.total_chunks = 0
        self.errors: List[str] = []

    async def _process_chunk(
        self,
        chunk: Chunk,
        extractor: ReflexiveOntologyExtractor,
    ) -> Dict[str, Any]:
        """Process a single chunk to extract entities and relations."""
        async with self._semaphore:
            try:
                result = await extractor.extract_entities_and_relations(chunk.text)
                return {
                    "chunk": chunk,
                    "entities": result.get("entities", []),
                    "relationships": result.get("relationships", []),
                }
            except Exception as e:
                self.errors.append(f"{chunk.doc_id}:{chunk.chunk_idx} - {e}")
                return {"chunk": chunk, "entities": [], "relationships": [], "error": str(e)}

    def _aggregate_entity(self, entity: Dict, source_doc: str):
        """Aggregate entity into global counts."""
        name = entity.get("name", "").strip()
        if not name or len(name) < 2:
            return

        key = name.lower()
        self.entity_data[key]["names"].append(name)
        self.entity_data[key]["types"][entity.get("type", "concept")] += 1
        self.entity_data[key]["grounding_categories"][entity.get("grounding_category", "other")] += 1
        self.entity_data[key]["confidences"].append(entity.get("confidence", 0.5))
        self.entity_data[key]["sources"].add(source_doc)

    def _aggregate_relation(self, relation: Dict, source_doc: str):
        """Aggregate relationship into global counts."""
        source = relation.get("source", "").strip()
        target = relation.get("target", "").strip()
        if not source or not target:
            return

        key = f"{source.lower()}|{target.lower()}"
        self.relation_data[key]["source"] = source
        self.relation_data[key]["target"] = target
        self.relation_data[key]["types"][relation.get("relation_type", "related_to")] += 1
        self.relation_data[key]["categories"][relation.get("relation_category", "generic")] += 1
        self.relation_data[key]["confidences"].append(relation.get("confidence", 0.5))
        if relation.get("context"):
            self.relation_data[key]["contexts"].append(relation["context"][:200])
        self.relation_data[key]["sources"].add(source_doc)

    async def ingest_all(self, progress_callback=None) -> Dict[str, Any]:
        """Ingest entire corpus with entity and relationship extraction."""
        from parallel_websearch_server.knowledge_graph import (
            Entity, get_knowledge_graph, save_knowledge_graph
        )

        start_time = datetime.utcnow()

        # Prepare corpus
        documents = prepare_corpus(self.corpus_path)
        all_chunks = []
        for doc in documents:
            all_chunks.extend(doc.chunks)

        self.total_chunks = len(all_chunks)
        logger.info(f"WORLD MODEL: Starting ingestion of {self.total_chunks} chunks from {len(documents)} documents")

        # Get extractor
        extractor = ReflexiveOntologyExtractor(models=self.models)

        try:
            # Process chunks in batches
            for i in range(0, len(all_chunks), self.batch_size):
                batch = all_chunks[i:i+self.batch_size]

                # Process batch in parallel
                tasks = [self._process_chunk(c, extractor) for c in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Aggregate results
                for result in results:
                    if isinstance(result, Exception):
                        self.errors.append(str(result))
                        continue

                    if isinstance(result, dict):
                        chunk = result.get("chunk")
                        doc_id = chunk.doc_id if chunk else "unknown"

                        # Aggregate entities
                        for entity in result.get("entities", []):
                            self._aggregate_entity(entity, doc_id)

                        # Aggregate relationships
                        for relation in result.get("relationships", []):
                            self._aggregate_relation(relation, doc_id)

                        self.processed_chunks += 1

                # Progress callback
                if progress_callback:
                    progress_callback(
                        self.processed_chunks,
                        self.total_chunks,
                        len(self.entity_data),
                        len(self.relation_data),
                    )

                logger.info(
                    f"WORLD MODEL: {self.processed_chunks}/{self.total_chunks} chunks, "
                    f"{len(self.entity_data)} entities, {len(self.relation_data)} relations"
                )

            # Build final entities and relationships
            graph = get_knowledge_graph()

            # Add entities
            final_entities = []
            entity_id_map = {}  # name -> entity_id for relationship linking

            for key, data in self.entity_data.items():
                if not data["names"]:
                    continue

                # Pick most common values
                canonical_name = max(set(data["names"]), key=data["names"].count)
                entity_type = max(data["types"], key=data["types"].get)
                grounding_cat = max(data["grounding_categories"], key=data["grounding_categories"].get)
                avg_conf = sum(data["confidences"]) / len(data["confidences"]) if data["confidences"] else 0.5

                entity_dict = {
                    "name": canonical_name,
                    "type": entity_type,
                    "grounding_category": grounding_cat,
                    "confidence": round(avg_conf, 4),
                    "mention_count": len(data["names"]),
                    "source_docs": list(data["sources"]),
                }
                final_entities.append(entity_dict)

                # Add to graph
                entity = Entity(
                    text=canonical_name,
                    entity_type=entity_type,
                    properties={
                        "grounding_category": grounding_cat,
                        "confidence": entity_dict["confidence"],
                        "mention_count": entity_dict["mention_count"],
                        "source_docs": entity_dict["source_docs"],
                        "extraction_method": "world_model_reflexive",
                    }
                )
                graph.add_entity(entity)
                entity_id_map[key] = entity.id

            # Add relationships
            final_relations = []
            for key, data in self.relation_data.items():
                source_name = data.get("source", "")
                target_name = data.get("target", "")
                if not source_name or not target_name:
                    continue

                source_key = source_name.lower()
                target_key = target_name.lower()

                # Check both entities exist
                if source_key not in entity_id_map or target_key not in entity_id_map:
                    continue

                relation_type = max(data["types"], key=data["types"].get)
                relation_cat = max(data["categories"], key=data["categories"].get)
                avg_conf = sum(data["confidences"]) / len(data["confidences"]) if data["confidences"] else 0.5
                context = max(data["contexts"], key=len) if data["contexts"] else ""

                relation_dict = {
                    "source": source_name,
                    "target": target_name,
                    "relation_type": relation_type,
                    "relation_category": relation_cat,
                    "confidence": round(avg_conf, 4),
                    "mention_count": len(data["confidences"]),
                    "context": context[:200],
                    "source_docs": list(data["sources"]),
                }
                final_relations.append(relation_dict)

                # Add to graph
                graph.add_relationship(
                    source_id=entity_id_map[source_key],
                    target_id=entity_id_map[target_key],
                    relation_type=relation_type,
                    properties={
                        "relation_category": relation_cat,
                        "confidence": relation_dict["confidence"],
                        "mention_count": relation_dict["mention_count"],
                        "context": context,
                        "source_docs": relation_dict["source_docs"],
                        "extraction_method": "world_model_reflexive",
                    }
                )

            # Save graph
            save_knowledge_graph()

        finally:
            await extractor.close()

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return {
            "documents_processed": len(documents),
            "chunks_processed": self.processed_chunks,
            "total_chunks": self.total_chunks,
            "unique_entities": len(final_entities),
            "unique_relationships": len(final_relations),
            "total_entity_mentions": sum(e["mention_count"] for e in final_entities),
            "total_relation_mentions": sum(r["mention_count"] for r in final_relations),
            "processing_time_seconds": round(processing_time, 2),
            "chunks_per_second": round(self.processed_chunks / max(1, processing_time), 2),
            "errors": len(self.errors),
            "entity_type_breakdown": dict(sorted(
                defaultdict(int, {e["type"]: 1 for e in final_entities}).items(),
                key=lambda x: -x[1]
            )[:20]),
            "relation_type_breakdown": dict(sorted(
                defaultdict(int, {r["relation_type"]: 1 for r in final_relations}).items(),
                key=lambda x: -x[1]
            )[:20]),
            "graph_stats": graph.get_stats(),
            "top_entities": sorted(final_entities, key=lambda x: -x["mention_count"])[:20],
            "top_relations": sorted(final_relations, key=lambda x: -x["mention_count"])[:20],
        }


async def run_world_model_ingest(
    corpus_path: str = "/Users/arthurcolle/parallel_research_v2",
    models: Optional[List[str]] = None,
    max_concurrent: int = MAX_CONCURRENT_CHUNKS,
    batch_size: int = BATCH_SIZE,
) -> Dict[str, Any]:
    """Run world model batch ingestion on corpus."""
    ingester = WorldModelIngester(
        corpus_path=Path(corpus_path),
        models=models,
        max_concurrent_chunks=max_concurrent,
        batch_size=batch_size,
    )

    def progress(done, total, entities, relations):
        pct = done * 100 // total if total > 0 else 0
        print(f"\rWORLD MODEL: {done}/{total} ({pct}%), {entities} entities, {relations} relations", end="", flush=True)

    result = await ingester.ingest_all(progress_callback=progress)
    print()  # newline after progress
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    result = asyncio.run(run_world_model_ingest())
    print(json.dumps(result, indent=2, default=str))
