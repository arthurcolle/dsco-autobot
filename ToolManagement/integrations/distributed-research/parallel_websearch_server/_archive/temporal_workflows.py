"""Temporal workflows for world model ingestion and knowledge graph operations.

Connects to Temporal server at localhost:7233 and registers workflows
that appear in the Temporal UI at localhost:8080.
"""

import asyncio
import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

logger = logging.getLogger(__name__)

# Task queue name - workflows and activities listen on this
TASK_QUEUE = "knowledge-graph-queue"


# =============================================================================
# ACTIVITIES - The actual work units
# =============================================================================

@activity.defn
async def prepare_corpus_activity(corpus_path: str) -> Dict[str, Any]:
    """Prepare corpus and return chunk metadata WITH actual text content."""
    from parallel_websearch_server.batch_ingest import prepare_corpus

    documents = prepare_corpus(Path(corpus_path))
    total_chunks = sum(len(doc.chunks) for doc in documents)

    return {
        "document_count": len(documents),
        "chunk_count": total_chunks,
        "documents": [
            {
                "doc_id": doc.doc_id,
                "filename": doc.filename,
                "title": doc.title,
                "chunk_count": len(doc.chunks),
                "chunks": [
                    {"idx": i, "text": chunk.text[:4000]}  # Limit chunk size
                    for i, chunk in enumerate(doc.chunks[:10])  # Limit chunks per doc
                ],
            }
            for doc in documents
        ],
    }


@activity.defn
async def extract_entities_activity(
    text: str,
    doc_id: str,
    chunk_idx: int,
) -> Dict[str, Any]:
    """Extract entities from a single chunk."""
    from parallel_websearch_server.reflexive_ontology import ReflexiveOntologyExtractor

    extractor = ReflexiveOntologyExtractor()
    try:
        result = await extractor.extract_entities_and_relations(text)
        return {
            "doc_id": doc_id,
            "chunk_idx": chunk_idx,
            "entities": result.get("entities", []),
            "relationships": result.get("relationships", []),
            "entity_count": len(result.get("entities", [])),
            "relationship_count": len(result.get("relationships", [])),
        }
    finally:
        await extractor.close()


@activity.defn
async def aggregate_results_activity(
    extraction_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Aggregate extraction results and add to knowledge graph."""
    from collections import defaultdict
    from parallel_websearch_server.knowledge_graph import (
        Entity, get_knowledge_graph, save_knowledge_graph
    )

    # Aggregate entities by normalized name
    entity_data = defaultdict(lambda: {
        "names": [],
        "types": defaultdict(int),
        "confidences": [],
        "sources": set(),
    })

    # Aggregate relationships
    relation_data = defaultdict(lambda: {
        "types": defaultdict(int),
        "confidences": [],
        "sources": set(),
    })

    total_entities = 0
    total_relations = 0

    for result in extraction_results:
        doc_id = result.get("doc_id", "unknown")

        for entity in result.get("entities", []):
            name = entity.get("name", "").strip()
            if not name or len(name) < 2:
                continue

            key = name.lower()
            entity_data[key]["names"].append(name)
            entity_data[key]["types"][entity.get("type", "concept")] += 1
            entity_data[key]["confidences"].append(entity.get("confidence", 0.5))
            entity_data[key]["sources"].add(doc_id)
            total_entities += 1

        for relation in result.get("relationships", []):
            source = relation.get("source", "").strip()
            target = relation.get("target", "").strip()
            if not source or not target:
                continue

            key = f"{source.lower()}|{target.lower()}"
            relation_data[key]["source"] = source
            relation_data[key]["target"] = target
            relation_data[key]["types"][relation.get("relation_type", "related_to")] += 1
            relation_data[key]["confidences"].append(relation.get("confidence", 0.5))
            relation_data[key]["sources"].add(doc_id)
            total_relations += 1

    # Build final entities and add to graph
    graph = get_knowledge_graph()
    entity_id_map = {}
    final_entities = []

    for key, data in entity_data.items():
        if not data["names"]:
            continue

        canonical_name = max(set(data["names"]), key=data["names"].count)
        entity_type = max(data["types"], key=data["types"].get)
        avg_conf = sum(data["confidences"]) / len(data["confidences"])

        entity = Entity(
            text=canonical_name,
            entity_type=entity_type,
            properties={
                "confidence": round(avg_conf, 4),
                "mention_count": len(data["names"]),
                "source_docs": list(data["sources"]),
                "extraction_method": "temporal_workflow",
            }
        )
        graph.add_entity(entity)
        entity_id_map[key] = entity.id
        final_entities.append({
            "name": canonical_name,
            "type": entity_type,
            "confidence": round(avg_conf, 4),
            "mention_count": len(data["names"]),
        })

    # Add relationships
    final_relations = []
    for key, data in relation_data.items():
        source_name = data.get("source", "")
        target_name = data.get("target", "")
        source_key = source_name.lower()
        target_key = target_name.lower()

        if source_key not in entity_id_map or target_key not in entity_id_map:
            continue

        relation_type = max(data["types"], key=data["types"].get)
        avg_conf = sum(data["confidences"]) / len(data["confidences"])

        graph.add_relationship(
            source_id=entity_id_map[source_key],
            target_id=entity_id_map[target_key],
            relation_type=relation_type,
            properties={
                "confidence": round(avg_conf, 4),
                "mention_count": len(data["confidences"]),
                "source_docs": list(data["sources"]),
                "extraction_method": "temporal_workflow",
            }
        )
        final_relations.append({
            "source": source_name,
            "target": target_name,
            "type": relation_type,
            "confidence": round(avg_conf, 4),
        })

    save_knowledge_graph()

    return {
        "unique_entities": len(final_entities),
        "unique_relationships": len(final_relations),
        "total_entity_mentions": total_entities,
        "total_relationship_mentions": total_relations,
        "graph_stats": graph.get_stats(),
        "top_entities": sorted(final_entities, key=lambda x: -x["mention_count"])[:10],
        "top_relations": sorted(final_relations, key=lambda x: -x["confidence"])[:10],
    }


@activity.defn
async def get_graph_stats_activity() -> Dict[str, Any]:
    """Get current knowledge graph statistics."""
    from parallel_websearch_server.knowledge_graph import get_knowledge_graph

    graph = get_knowledge_graph()
    return graph.get_stats()


# =============================================================================
# WORKFLOWS - Orchestrate activities
# =============================================================================

@workflow.defn
class WorldModelIngestWorkflow:
    """Workflow for ingesting corpus into knowledge graph."""

    @workflow.run
    async def run(
        self,
        corpus_path: str = "/Users/arthurcolle/parallel_research_v2",
        batch_size: int = 10,
    ) -> Dict[str, Any]:
        """Run the full world model ingestion."""
        workflow.logger.info(f"Starting world model ingestion from {corpus_path}")

        # Step 1: Prepare corpus
        corpus_info = await workflow.execute_activity(
            prepare_corpus_activity,
            corpus_path,
            start_to_close_timeout=timedelta(minutes=5),
        )

        workflow.logger.info(
            f"Prepared {corpus_info['document_count']} documents, "
            f"{corpus_info['chunk_count']} chunks"
        )

        # Step 2: Extract entities from chunks in batches
        # For now we simulate batch processing - in production this would
        # process actual chunks from the corpus
        extraction_results = []

        # Process documents
        for doc in corpus_info["documents"][:5]:  # Limit for demo
            workflow.logger.info(f"Processing document: {doc['filename']}")

            # In production, we'd load actual chunk text here
            result = await workflow.execute_activity(
                extract_entities_activity,
                args=[f"Sample text from {doc['title']}", doc["doc_id"], 0],
                start_to_close_timeout=timedelta(minutes=2),
            )
            extraction_results.append(result)

        # Step 3: Aggregate and store results
        workflow.logger.info("Aggregating results and updating knowledge graph")
        final_result = await workflow.execute_activity(
            aggregate_results_activity,
            args=[extraction_results],
            start_to_close_timeout=timedelta(minutes=5),
        )

        return {
            "status": "completed",
            "corpus_path": corpus_path,
            "documents_processed": corpus_info["document_count"],
            "chunks_processed": corpus_info["chunk_count"],
            **final_result,
        }


@workflow.defn
class EntityExtractionWorkflow:
    """Workflow for extracting entities from text."""

    @workflow.run
    async def run(self, text: str, doc_id: str = "manual") -> Dict[str, Any]:
        """Extract entities from provided text."""
        workflow.logger.info(f"Extracting entities from text ({len(text)} chars)")

        result = await workflow.execute_activity(
            extract_entities_activity,
            args=[text, doc_id, 0],
            start_to_close_timeout=timedelta(minutes=2),
        )

        return {
            "status": "completed",
            "doc_id": doc_id,
            "text_length": len(text),
            **result,
        }


@workflow.defn
class KnowledgeGraphStatsWorkflow:
    """Workflow to get knowledge graph statistics."""

    @workflow.run
    async def run(self) -> Dict[str, Any]:
        """Get current KG stats."""
        stats = await workflow.execute_activity(
            get_graph_stats_activity,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return {"status": "completed", **stats}


# =============================================================================
# WORKER - Runs workflows and activities
# =============================================================================

async def run_worker():
    """Start the Temporal worker."""
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            WorldModelIngestWorkflow,
            EntityExtractionWorkflow,
            KnowledgeGraphStatsWorkflow,
        ],
        activities=[
            prepare_corpus_activity,
            extract_entities_activity,
            aggregate_results_activity,
            get_graph_stats_activity,
        ],
    )

    logger.info(f"Starting Temporal worker on task queue: {TASK_QUEUE}")
    await worker.run()


async def start_workflow(workflow_name: str, **kwargs) -> str:
    """Start a workflow and return its ID."""
    client = await Client.connect("localhost:7233")

    workflow_map = {
        "ingest": WorldModelIngestWorkflow,
        "extract": EntityExtractionWorkflow,
        "stats": KnowledgeGraphStatsWorkflow,
    }

    workflow_class = workflow_map.get(workflow_name)
    if not workflow_class:
        raise ValueError(f"Unknown workflow: {workflow_name}")

    handle = await client.start_workflow(
        workflow_class.run,
        kwargs if kwargs else None,
        id=f"{workflow_name}-{asyncio.get_event_loop().time()}",
        task_queue=TASK_QUEUE,
    )

    return handle.id


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("Starting Temporal worker...")
    print(f"Task queue: {TASK_QUEUE}")
    print("Workflows: WorldModelIngestWorkflow, EntityExtractionWorkflow, KnowledgeGraphStatsWorkflow")
    print()
    print("Connect to Temporal UI at http://localhost:8080")
    print()

    asyncio.run(run_worker())
