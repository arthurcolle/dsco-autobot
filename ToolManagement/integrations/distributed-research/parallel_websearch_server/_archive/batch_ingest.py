"""Batch ingestion with hierarchical chunking for full corpus processing.

Processes ALL content by chunking documents and extracting entities from each chunk.
Aggregates entities across chunks with deduplication.
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

# Chunking parameters
CHUNK_SIZE = 4000  # chars per chunk
CHUNK_OVERLAP = 400  # overlap between chunks
MIN_CHUNK_SIZE = 500  # minimum chunk size


@dataclass
class Chunk:
    """A text chunk from a document."""
    text: str
    doc_id: str
    chunk_idx: int
    start_char: int
    end_char: int
    level: int = 0  # 0 = leaf, 1+ = summary levels


@dataclass
class DocumentChunks:
    """Chunked document ready for processing."""
    doc_id: str
    filename: str
    title: str
    total_chars: int
    chunks: List[Chunk]


def chunk_by_sections(text: str, doc_id: str) -> List[Chunk]:
    """Chunk document by markdown sections first, then by size."""
    chunks = []

    # Split by major headers (##, ###)
    section_pattern = r'\n(?=#{1,3}\s)'
    sections = re.split(section_pattern, text)

    chunk_idx = 0
    char_offset = 0

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # If section is small enough, keep it as one chunk
        if len(section) <= CHUNK_SIZE:
            if len(section) >= MIN_CHUNK_SIZE:
                chunks.append(Chunk(
                    text=section,
                    doc_id=doc_id,
                    chunk_idx=chunk_idx,
                    start_char=char_offset,
                    end_char=char_offset + len(section),
                ))
                chunk_idx += 1
        else:
            # Split large sections by paragraphs/sentences
            sub_chunks = chunk_by_size(section, doc_id, chunk_idx, char_offset)
            chunks.extend(sub_chunks)
            chunk_idx += len(sub_chunks)

        char_offset += len(section) + 1  # +1 for newline

    return chunks


def chunk_by_size(text: str, doc_id: str, start_idx: int = 0, char_offset: int = 0) -> List[Chunk]:
    """Chunk text by size with overlap."""
    chunks = []

    # Split into paragraphs
    paragraphs = re.split(r'\n\n+', text)

    current_chunk = ""
    current_start = char_offset
    chunk_idx = start_idx

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # If adding this paragraph exceeds chunk size
        if len(current_chunk) + len(para) + 2 > CHUNK_SIZE:
            # Save current chunk if it's big enough
            if len(current_chunk) >= MIN_CHUNK_SIZE:
                chunks.append(Chunk(
                    text=current_chunk.strip(),
                    doc_id=doc_id,
                    chunk_idx=chunk_idx,
                    start_char=current_start,
                    end_char=current_start + len(current_chunk),
                ))
                chunk_idx += 1

            # Start new chunk with overlap
            overlap_text = current_chunk[-CHUNK_OVERLAP:] if len(current_chunk) > CHUNK_OVERLAP else ""
            current_chunk = overlap_text + "\n\n" + para if overlap_text else para
            current_start = char_offset
        else:
            current_chunk += "\n\n" + para if current_chunk else para

        char_offset += len(para) + 2

    # Don't forget the last chunk
    if len(current_chunk) >= MIN_CHUNK_SIZE:
        chunks.append(Chunk(
            text=current_chunk.strip(),
            doc_id=doc_id,
            chunk_idx=chunk_idx,
            start_char=current_start,
            end_char=current_start + len(current_chunk),
        ))

    return chunks


def prepare_corpus(corpus_path: Path) -> List[DocumentChunks]:
    """Load and chunk entire corpus."""
    metadata_path = corpus_path / "corpus_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"No corpus_metadata.json in {corpus_path}")

    with open(metadata_path) as f:
        metadata = json.load(f)

    documents = []
    total_chunks = 0
    total_chars = 0

    for doc in metadata.get("documents", []):
        # Skip combined/overview files (redundant content)
        if "ALL_RESEARCH" in doc["filename"] or doc.get("category") == "overview":
            logger.info(f"Skipping redundant file: {doc['filename']}")
            continue

        md_path = corpus_path / doc["filename"]
        if not md_path.exists():
            continue

        content = md_path.read_text()
        chunks = chunk_by_sections(content, doc["doc_id"])

        if chunks:
            documents.append(DocumentChunks(
                doc_id=doc["doc_id"],
                filename=doc["filename"],
                title=doc.get("title", doc["filename"]),
                total_chars=len(content),
                chunks=chunks,
            ))
            total_chunks += len(chunks)
            total_chars += len(content)

    logger.info(f"Prepared {len(documents)} documents, {total_chunks} chunks, {total_chars:,} chars")
    return documents


class BatchIngester:
    """Ingest corpus with parallel extraction across all chunks."""

    def __init__(
        self,
        corpus_path: Path,
        models: Optional[List[str]] = None,
        max_concurrent_chunks: int = 5,
    ):
        self.corpus_path = corpus_path
        self.models = models
        self.max_concurrent = max_concurrent_chunks
        self._semaphore = asyncio.Semaphore(max_concurrent_chunks)

        # Aggregated results
        self.entity_counts: Dict[str, Dict] = defaultdict(lambda: {
            "count": 0,
            "type": None,
            "sources": set(),
            "confidence_sum": 0,
        })
        self.relationships: List[Dict] = []
        self.processed_chunks = 0
        self.total_chunks = 0
        self.errors: List[str] = []

    async def _process_chunk(
        self,
        chunk: Chunk,
        extractor,
    ) -> Dict[str, Any]:
        """Process a single chunk."""
        async with self._semaphore:
            try:
                result = await extractor.extract_entities(chunk.text)
                return {
                    "chunk": chunk,
                    "entities": result.get("entities", []),
                    "stats": result.get("stats", {}),
                }
            except Exception as e:
                self.errors.append(f"{chunk.doc_id}:{chunk.chunk_idx} - {e}")
                return {"chunk": chunk, "entities": [], "error": str(e)}

    def _aggregate_entity(self, entity: Dict, source_doc: str):
        """Aggregate entity into global counts."""
        name = entity.get("name", "").strip()
        if not name or len(name) < 2:
            return

        # Normalize key
        key = name.lower()

        self.entity_counts[key]["count"] += 1
        self.entity_counts[key]["sources"].add(source_doc)
        self.entity_counts[key]["confidence_sum"] += entity.get("confidence", 0.5)

        # Keep the most common type
        etype = entity.get("type", "other")
        if self.entity_counts[key]["type"] is None:
            self.entity_counts[key]["type"] = etype
            self.entity_counts[key]["canonical_name"] = name

    async def ingest_all(self, progress_callback=None) -> Dict[str, Any]:
        """Ingest entire corpus with parallel chunk processing."""
        from parallel_websearch_server.dspy_entity_extractor import get_extractor
        from parallel_websearch_server.knowledge_graph import Entity, get_knowledge_graph, save_knowledge_graph

        start_time = datetime.utcnow()

        # Prepare corpus
        documents = prepare_corpus(self.corpus_path)
        all_chunks = []
        for doc in documents:
            all_chunks.extend(doc.chunks)

        self.total_chunks = len(all_chunks)
        logger.info(f"Starting ingestion of {self.total_chunks} chunks from {len(documents)} documents")

        # Get extractor
        extractor = get_extractor(self.models)

        # Process chunks in batches
        batch_size = 10
        for i in range(0, len(all_chunks), batch_size):
            batch = all_chunks[i:i+batch_size]

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
                    entities = result.get("entities", [])

                    for entity in entities:
                        self._aggregate_entity(entity, chunk.doc_id if chunk else "unknown")

                    self.processed_chunks += 1

            # Progress callback
            if progress_callback:
                progress_callback(self.processed_chunks, self.total_chunks, len(self.entity_counts))

            logger.info(f"Processed {self.processed_chunks}/{self.total_chunks} chunks, {len(self.entity_counts)} unique entities")

        # Build final entities and add to graph
        graph = get_knowledge_graph()

        final_entities = []
        for key, data in self.entity_counts.items():
            if data["count"] >= 1:  # Include all entities (was >= 2 for dedup)
                avg_conf = data["confidence_sum"] / data["count"] if data["count"] > 0 else 0.5

                entity_data = {
                    "name": data.get("canonical_name", key),
                    "type": data["type"] or "other",
                    "confidence": round(avg_conf, 4),
                    "mention_count": data["count"],
                    "source_docs": list(data["sources"]),
                }
                final_entities.append(entity_data)

                # Add to graph
                entity = Entity(
                    text=entity_data["name"],
                    entity_type=entity_data["type"],
                    properties={
                        "confidence": entity_data["confidence"],
                        "mention_count": entity_data["mention_count"],
                        "source_docs": entity_data["source_docs"],
                        "extraction_method": "parallel_llm_batch",
                    }
                )
                graph.add_entity(entity)

        # Save graph
        save_knowledge_graph()

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return {
            "documents_processed": len(documents),
            "chunks_processed": self.processed_chunks,
            "total_chunks": self.total_chunks,
            "unique_entities": len(final_entities),
            "total_mentions": sum(e["mention_count"] for e in final_entities),
            "processing_time_seconds": round(processing_time, 2),
            "errors": len(self.errors),
            "models_used": extractor.models,
            "graph_stats": graph.get_stats(),
            "top_entities": sorted(final_entities, key=lambda x: -x["mention_count"])[:20],
        }


async def run_batch_ingest(
    corpus_path: str = "/Users/arthurcolle/parallel_research_v2",
    models: Optional[List[str]] = None,
    max_concurrent: int = 5,
) -> Dict[str, Any]:
    """Run batch ingestion on corpus."""
    ingester = BatchIngester(
        corpus_path=Path(corpus_path),
        models=models,
        max_concurrent_chunks=max_concurrent,
    )

    def progress(done, total, entities):
        print(f"\rProgress: {done}/{total} chunks ({done*100//total}%), {entities} entities", end="", flush=True)

    result = await ingester.ingest_all(progress_callback=progress)
    print()  # newline after progress
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    result = asyncio.run(run_batch_ingest())
    print(json.dumps(result, indent=2, default=str))
