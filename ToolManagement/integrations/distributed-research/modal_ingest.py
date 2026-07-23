"""High-throughput ingestion using Modal vLLM OpenAI-compatible API.

Usage:
    # Deploy the Modal server first:
    modal deploy modal_kg_server.py

    # Then run ingestion:
    python modal_ingest.py --clear
"""

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

# The web_server decorator creates a URL like: https://arthurcolle--kg-extraction-server-serve.modal.run
BASE_URL = "https://arthurcolle--kg-extraction-server-serve.modal.run"
SOURCE_DB = "/Users/arthurcolle/parallel_research_v2/dagi_research.db"
CHUNK_SIZE = 4000
MAX_CONCURRENT = 8  # Conservative for 30B model

EXTRACTION_PROMPT = """You are a knowledge graph extractor. Extract entities and relationships from text.

ENTITY TYPES: person, organization, concept, product, process, protocol, language, event, location, metric, pattern, category, property

RELATION TYPES: enables, uses, implements, causes, part_of, contains, subtype_of, instance_of, supports, contradicts, authored, owns, controls, requires, ensures

Return ONLY valid JSON with this exact format:
{"entities": [{"id": "e1", "name": "exact name", "entity_type": "type"}], "relations": [{"source": "e1", "target": "e2", "relation_type": "type"}]}"""


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Entity:
    id: str
    name: str
    entity_type: str
    global_id: str = ""
    source_chunk: str = ""
    mentions: int = 1


@dataclass
class Relation:
    source: str
    target: str
    relation_type: str
    source_chunk: str = ""


# ============================================================================
# Modal Client (OpenAI-compatible)
# ============================================================================

class ModalKGClient:
    """Client for Modal vLLM OpenAI-compatible API."""

    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")

    async def health(self) -> dict:
        """Check API health (allows time for cold start)."""
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return {"status": "ok", "code": response.status_code}

    async def extract(self, text: str, chunk_id: str = None) -> dict:
        """Extract entities and relations from text using OpenAI-compatible API."""
        start = time.perf_counter()

        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            response = await client.post(
                f"{self.base_url}/v1/chat/completions",
                json={
                    "model": "kg-extractor",
                    "messages": [
                        {"role": "system", "content": EXTRACTION_PROMPT},
                        {"role": "user", "content": f"Extract knowledge graph from:\n\n{text[:5000]}"},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 2000,
                },
            )
            response.raise_for_status()
            result = response.json()

        latency_ms = (time.perf_counter() - start) * 1000

        # Parse the response
        content = result["choices"][0]["message"]["content"]
        entities, relations = self._parse_response(content)

        return {
            "entities": entities,
            "relations": relations,
            "chunk_id": chunk_id,
            "latency_ms": latency_ms,
        }

    def _parse_response(self, response: str) -> tuple:
        """Parse JSON response into entities and relations."""
        entities = []
        relations = []

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

            for e in data.get("entities", []):
                if isinstance(e, dict) and "name" in e:
                    entities.append({
                        "id": e.get("id", f"e{len(entities)}"),
                        "name": e["name"],
                        "entity_type": e.get("entity_type", e.get("type", "concept")),
                    })

            for r in data.get("relations", []):
                if isinstance(r, dict) and "source" in r and "target" in r:
                    relations.append({
                        "source": r["source"],
                        "target": r["target"],
                        "relation_type": r.get("relation_type", r.get("type", "related_to")),
                    })

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error: {e}")

        return entities, relations

    async def extract_all(
        self,
        chunks: List[Tuple[str, str]],
        max_concurrent: int = MAX_CONCURRENT,
    ) -> List[dict]:
        """Extract from all chunks with concurrency control."""
        logger.info(f"Processing {len(chunks)} chunks with max {max_concurrent} concurrent requests")

        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async def process_chunk(idx: int, text: str, chunk_id: str):
            async with semaphore:
                try:
                    result = await self.extract(text, chunk_id)
                    if idx % 10 == 0:  # Log every 10 chunks
                        logger.info(f"  Processed chunk {idx + 1}/{len(chunks)} ({len(result.get('entities', []))} entities)")
                    return result
                except Exception as e:
                    logger.warning(f"Error processing chunk {chunk_id}: {type(e).__name__}: {e}")
                    return {"entities": [], "relations": [], "chunk_id": chunk_id, "error": str(e)}

        tasks = [
            process_chunk(i, text, chunk_id)
            for i, (text, chunk_id) in enumerate(chunks)
        ]

        results = await asyncio.gather(*tasks)
        return results


# ============================================================================
# Document Chunker
# ============================================================================

@dataclass
class ChunkInfo:
    """Chunk with position metadata."""
    text: str
    position: int
    char_start: int
    char_end: int


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE) -> List[ChunkInfo]:
    """Split text into chunks at paragraph boundaries with position tracking."""
    if len(text) <= chunk_size:
        return [ChunkInfo(text=text, position=0, char_start=0, char_end=len(text))]

    chunks = []
    paragraphs = text.split('\n\n')
    current_chunk = []
    current_size = 0
    char_pos = 0
    chunk_start = 0

    for para in paragraphs:
        para_size = len(para) + 2

        if current_size + para_size > chunk_size and current_chunk:
            chunk_text_str = '\n\n'.join(current_chunk)
            chunks.append(ChunkInfo(
                text=chunk_text_str,
                position=len(chunks),
                char_start=chunk_start,
                char_end=char_pos - 2  # -2 for trailing \n\n
            ))
            current_chunk = [para]
            current_size = para_size
            chunk_start = char_pos
        else:
            current_chunk.append(para)
            current_size += para_size

        char_pos += para_size

    if current_chunk:
        chunk_text_str = '\n\n'.join(current_chunk)
        chunks.append(ChunkInfo(
            text=chunk_text_str,
            position=len(chunks),
            char_start=chunk_start,
            char_end=len(text)
        ))

    return chunks


def load_documents(
    db_path: str = SOURCE_DB,
    max_docs: int = None,
    skip_ids: List[str] = None,
) -> List[Tuple[str, str, str]]:
    """Load documents from source database. Returns (id, name, content)."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name, content FROM documents
        WHERE content IS NOT NULL AND content != ''
    """)

    documents = cursor.fetchall()
    conn.close()

    # Filter out skipped IDs
    if skip_ids:
        documents = [(id, name, content) for id, name, content in documents if id not in skip_ids]

    # Limit if specified
    if max_docs:
        documents = documents[:max_docs]

    return documents


def store_chunks_to_db(
    chunks_data: List[Tuple[str, str, ChunkInfo]],  # (chunk_id, doc_id, ChunkInfo)
    kg_db_path: str = "knowledge_graph.db"
) -> dict:
    """Store chunks to the knowledge graph database."""
    import hashlib

    conn = sqlite3.connect(kg_db_path)
    cursor = conn.cursor()

    stats = {"new": 0, "duplicates": 0}

    for chunk_id, doc_id, chunk_info in chunks_data:
        content_hash = hashlib.sha256(chunk_info.text.encode()).hexdigest()[:16]

        # Check for existing
        cursor.execute(
            "SELECT chunk_id FROM chunks WHERE content_hash = ?",
            (content_hash,)
        )
        if cursor.fetchone():
            stats["duplicates"] += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO chunks (chunk_id, content_hash, text, level, parent_id,
                                   doc_id, position, char_start, char_end, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                chunk_id,
                content_hash,
                chunk_info.text,
                "document",  # level
                None,  # parent_id
                doc_id,
                chunk_info.position,
                chunk_info.char_start,
                chunk_info.char_end,
                json.dumps({"extraction_method": "modal_vllm"})
            ))
            stats["new"] += 1
        except sqlite3.IntegrityError:
            stats["duplicates"] += 1

    conn.commit()
    conn.close()

    logger.info(f"Stored chunks: {stats['new']} new, {stats['duplicates']} duplicates")
    return stats


# ============================================================================
# Aggregator
# ============================================================================

class KGAggregator:
    """Aggregate extraction results into unified knowledge graph."""

    def __init__(self):
        self.entities: Dict[str, Entity] = {}  # normalized_name -> Entity
        self.relations: List[Relation] = []
        self.id_map: Dict[str, str] = {}  # local_id -> global_id

    def _normalize(self, name: str) -> str:
        return name.lower().strip()

    def add_entity(self, entity: dict, chunk_id: str) -> str:
        """Add entity, return global ID."""
        key = self._normalize(entity["name"])

        if key in self.entities:
            self.entities[key].mentions += 1
            return self.entities[key].global_id
        else:
            global_id = f"ent_{len(self.entities)}"
            self.entities[key] = Entity(
                id=entity["id"],
                name=entity["name"],
                entity_type=entity.get("entity_type", "concept"),
                global_id=global_id,
                source_chunk=chunk_id,
                mentions=1,
            )
            return global_id

    def add_result(self, result: dict):
        """Add extraction result."""
        chunk_id = result.get("chunk_id", "")

        # Build local -> global ID map
        local_map = {}
        for entity in result.get("entities", []):
            global_id = self.add_entity(entity, chunk_id)
            local_map[entity["id"]] = global_id

        # Add relations with resolved IDs
        for relation in result.get("relations", []):
            source = relation.get("source", "")
            target = relation.get("target", "")

            if source in local_map and target in local_map:
                self.relations.append(Relation(
                    source=local_map[source],
                    target=local_map[target],
                    relation_type=relation.get("relation_type", "related_to"),
                    source_chunk=chunk_id,
                ))

    def save_to_kg(self) -> dict:
        """Save to knowledge graph database."""
        from parallel_websearch_server.knowledge_graph import (
            Entity as KGEntity,
            get_knowledge_graph,
            save_knowledge_graph,
        )

        graph = get_knowledge_graph()

        # Build global_id -> entity map
        id_to_entity = {e.global_id: e for e in self.entities.values()}

        # Add entities
        for entity in self.entities.values():
            kg_entity = KGEntity(
                text=entity.name,
                entity_type=entity.entity_type,
                properties={
                    "confidence": 0.9,
                    "extraction_method": "modal_vllm",
                    "source_chunk": entity.source_chunk,
                    "mention_count": entity.mentions,
                }
            )
            graph.add_entity(kg_entity)

        # Build name -> kg entity ID map
        name_to_kg_id = {}
        for entity in graph.entities.values():
            name_to_kg_id[entity.text.lower()] = entity.id

        # Add relations
        relations_added = 0
        for relation in self.relations:
            source_ent = id_to_entity.get(relation.source)
            target_ent = id_to_entity.get(relation.target)

            if source_ent and target_ent:
                source_kg_id = name_to_kg_id.get(source_ent.name.lower())
                target_kg_id = name_to_kg_id.get(target_ent.name.lower())

                if source_kg_id and target_kg_id:
                    graph.add_relationship(
                        source_kg_id,
                        target_kg_id,
                        relation.relation_type,
                        properties={"confidence": 0.85},
                        source_doc=relation.source_chunk,
                    )
                    relations_added += 1

        logger.info(f"Added {relations_added}/{len(self.relations)} relationships to graph")
        save_knowledge_graph()

        return graph.get_stats()

    def get_stats(self) -> dict:
        return {
            "unique_entities": len(self.entities),
            "total_relations": len(self.relations),
            "top_entities": sorted(
                [(e.name, e.mentions) for e in self.entities.values()],
                key=lambda x: x[1],
                reverse=True,
            )[:20],
        }


# ============================================================================
# Main Ingestion
# ============================================================================

async def run_ingestion(
    source_db: str = SOURCE_DB,
    max_docs: int = None,  # None = all documents
    skip_ids: List[str] = None,
    chunk_size: int = CHUNK_SIZE,
    max_concurrent: int = MAX_CONCURRENT,
    clear_existing: bool = False,
) -> dict:
    """Run full ingestion pipeline."""

    # Clear existing if requested
    if clear_existing:
        from parallel_websearch_server.knowledge_graph import (
            get_knowledge_graph,
            save_knowledge_graph,
        )
        graph = get_knowledge_graph()
        graph.entities.clear()
        graph.relationships.clear()
        graph.graph.clear()
        save_knowledge_graph()
        logger.info("Cleared existing knowledge graph")

    # Initialize client
    client = ModalKGClient()

    # Check health
    try:
        health = await client.health()
        logger.info(f"API healthy: {health}")
    except Exception as e:
        logger.error(f"API not available: {e}")
        return {"error": str(e)}

    # Load documents
    logger.info(f"Loading documents from {source_db}")
    documents = load_documents(source_db, max_docs, skip_ids)
    logger.info(f"Loaded {len(documents)} documents" + (f" (skipped {len(skip_ids)} IDs)" if skip_ids else ""))

    # Prepare all chunks
    all_chunks = []  # (text, chunk_id) for extraction
    chunks_to_store = []  # (chunk_id, doc_id, ChunkInfo) for storage
    doc_info = []

    for doc_id, name, content in documents:
        chunk_infos = chunk_text(content, chunk_size)
        logger.info(f"  {name}: {len(content):,} chars -> {len(chunk_infos)} chunks")

        for chunk_info in chunk_infos:
            chunk_id = f"{doc_id}_chunk_{chunk_info.position}"
            all_chunks.append((chunk_info.text, chunk_id))
            chunks_to_store.append((chunk_id, doc_id, chunk_info))

        doc_info.append({
            "doc_id": doc_id,
            "name": name,
            "chunks": len(chunk_infos),
        })

    logger.info(f"Total chunks to process: {len(all_chunks)}")

    # Store chunks to database BEFORE extraction
    chunk_stats = store_chunks_to_db(chunks_to_store)

    # Run extraction
    start = time.perf_counter()
    results = await client.extract_all(all_chunks, max_concurrent=max_concurrent)
    extraction_time = time.perf_counter() - start

    logger.info(f"Extraction completed in {extraction_time:.1f}s")
    logger.info(f"Throughput: {len(all_chunks) / extraction_time:.1f} chunks/sec")

    # Aggregate results
    aggregator = KGAggregator()
    for result in results:
        aggregator.add_result(result)

    stats = aggregator.get_stats()
    logger.info(f"Aggregated: {stats['unique_entities']} entities, {stats['total_relations']} relations")

    # Save to knowledge graph
    kg_stats = aggregator.save_to_kg()

    return {
        "documents": doc_info,
        "total_chunks": len(all_chunks),
        "chunks_stored": chunk_stats,
        "extraction_time_s": round(extraction_time, 1),
        "throughput_chunks_per_sec": round(len(all_chunks) / extraction_time, 1),
        "aggregation_stats": stats,
        "kg_stats": kg_stats,
    }


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Modal-based KG ingestion")
    parser.add_argument("--source-db", default=SOURCE_DB, help="Source database path")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Chunk size in chars")
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT, help="Max concurrent requests")
    parser.add_argument("--max-docs", type=int, default=None, help="Max documents to process")
    parser.add_argument("--skip-ids", type=str, default=None, help="Comma-separated doc IDs to skip")
    parser.add_argument("--clear", action="store_true", help="Clear existing KG before ingestion")

    args = parser.parse_args()

    skip_ids = args.skip_ids.split(",") if args.skip_ids else None

    result = asyncio.run(run_ingestion(
        source_db=args.source_db,
        skip_ids=skip_ids,
        chunk_size=args.chunk_size,
        max_concurrent=args.max_concurrent,
        max_docs=args.max_docs,
        clear_existing=args.clear,
    ))

    print("\n" + "=" * 60)
    print("INGESTION RESULTS")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
