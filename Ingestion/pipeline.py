"""
Ingestion Pipeline

Orchestrates the full document ingestion workflow.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Callable

from .models import (
    Document,
    Chunk,
    Entity,
    Relation,
    IngestionJob,
    IngestionStatus,
    IngestionResult,
    DocumentType,
)
from .extractors import get_extractor, Extractor
from .chunkers import get_chunker, Chunker, ChunkerConfig
from .embedders import get_embedder, Embedder, EmbedderConfig

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the ingestion pipeline."""
    # Chunking
    chunk_size: int = 512
    chunk_overlap: int = 50
    min_chunk_size: int = 100
    max_chunk_size: int = 1024

    # Embedding
    embedding_provider: str = "jina"
    embedding_model: str = "jina-embeddings-v3"
    embedding_dimensions: int = 1024
    embedding_batch_size: int = 32

    # Entity extraction
    extract_entities: bool = True
    entity_extraction_model: str = "gpt-4o-mini"

    # Storage
    store_vectors: bool = True
    store_graph: bool = True
    redis_url: Optional[str] = None
    neo4j_uri: Optional[str] = None

    # Processing
    max_concurrent_jobs: int = 5
    job_timeout: int = 3600  # seconds


class IngestionPipeline:
    """
    Full document ingestion pipeline.

    Stages:
    1. Extract - Get text and metadata from document
    2. Chunk - Split into semantic chunks
    3. Embed - Generate vector embeddings
    4. Extract Entities - Identify entities and relations
    5. Store - Save to vector store and knowledge graph
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()

        # Initialize components
        self._chunker_config = ChunkerConfig(
            chunk_size=self.config.chunk_size,
            chunk_overlap=self.config.chunk_overlap,
            min_chunk_size=self.config.min_chunk_size,
            max_chunk_size=self.config.max_chunk_size,
        )

        self._embedder_config = EmbedderConfig(
            model=self.config.embedding_model,
            dimensions=self.config.embedding_dimensions,
            batch_size=self.config.embedding_batch_size,
        )

        self._embedder = get_embedder(
            self.config.embedding_provider,
            self._embedder_config
        )

        # Job tracking
        self._jobs: Dict[str, IngestionJob] = {}
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_jobs)

        # Callbacks
        self._on_progress: Optional[Callable[[IngestionJob], None]] = None

    def set_progress_callback(self, callback: Callable[[IngestionJob], None]):
        """Set callback for progress updates."""
        self._on_progress = callback

    async def ingest(
        self,
        source: str,
        content: Optional[bytes] = None,
        doc_type: Optional[DocumentType] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> IngestionResult:
        """
        Ingest a document through the full pipeline.

        Args:
            source: File path, URL, or identifier
            content: Optional raw content bytes
            doc_type: Optional document type override
            metadata: Optional additional metadata

        Returns:
            IngestionResult with document, chunks, entities, relations
        """
        # Create job
        job = IngestionJob(
            source_type="file" if not source.startswith("http") else "url",
            source_path=source,
            config={"metadata": metadata or {}},
        )
        self._jobs[job.id] = job

        try:
            async with self._semaphore:
                return await self._run_pipeline(job, source, content, doc_type, metadata)
        except Exception as e:
            job.fail(str(e))
            self._notify_progress(job)
            raise

    async def _run_pipeline(
        self,
        job: IngestionJob,
        source: str,
        content: Optional[bytes],
        doc_type: Optional[DocumentType],
        metadata: Optional[Dict[str, Any]],
    ) -> IngestionResult:
        """Run the full ingestion pipeline."""

        # Stage 1: Extract
        job.start()
        self._notify_progress(job)
        logger.info(f"[{job.id}] Extracting from {source}")

        extractor = get_extractor(source)
        document = extractor.extract(source, content)

        if doc_type:
            document.doc_type = doc_type
        if metadata:
            document.metadata.update(metadata)

        job.document_id = document.id
        job.status = IngestionStatus.CHUNKING
        job.progress = 0.2
        self._notify_progress(job)

        # Stage 2: Chunk
        logger.info(f"[{job.id}] Chunking document {document.id}")

        chunker = get_chunker(document, self._chunker_config)
        chunks = chunker.chunk(document)

        job.chunk_count = len(chunks)
        job.status = IngestionStatus.EMBEDDING
        job.progress = 0.4
        self._notify_progress(job)

        # Stage 3: Embed
        logger.info(f"[{job.id}] Embedding {len(chunks)} chunks")

        chunks = await self._embedder.embed_chunks(chunks)

        job.status = IngestionStatus.STORING
        job.progress = 0.7
        self._notify_progress(job)

        # Stage 4: Extract entities (optional)
        entities: List[Entity] = []
        relations: List[Relation] = []

        if self.config.extract_entities:
            logger.info(f"[{job.id}] Extracting entities")
            entities, relations = await self._extract_entities(document, chunks)
            job.entity_count = len(entities)
            job.relation_count = len(relations)

        job.progress = 0.9
        self._notify_progress(job)

        # Stage 5: Store
        if self.config.store_vectors:
            await self._store_vectors(document, chunks)

        if self.config.store_graph:
            await self._store_graph(document, entities, relations)

        # Complete
        job.complete()
        self._notify_progress(job)
        logger.info(f"[{job.id}] Ingestion complete: {len(chunks)} chunks, {len(entities)} entities")

        return IngestionResult(
            job_id=job.id,
            document=document,
            chunks=chunks,
            entities=entities,
            relations=relations,
        )

    async def _extract_entities(
        self,
        document: Document,
        chunks: List[Chunk]
    ) -> tuple[List[Entity], List[Relation]]:
        """Extract entities and relations from chunks using LLM."""
        import httpx
        import json
        import os

        entities: List[Entity] = []
        relations: List[Relation] = []

        # Use Anthropic/OpenAI for entity extraction
        api_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("No API key for entity extraction, skipping")
            return entities, relations

        # Sample chunks for entity extraction (to limit API calls)
        sample_chunks = chunks[:10] if len(chunks) > 10 else chunks
        combined_text = "\n\n".join(c.content[:500] for c in sample_chunks)

        prompt = f"""Extract entities and relationships from the following text.
Return JSON with this structure:
{{
    "entities": [
        {{"name": "...", "type": "person|organization|location|concept|technology", "mentions": ["..."]}}
    ],
    "relations": [
        {{"source": "entity_name", "target": "entity_name", "type": "related_to|part_of|works_for|etc"}}
    ]
}}

Text:
{combined_text[:4000]}

Return only valid JSON."""

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                if os.getenv("ANTHROPIC_API_KEY"):
                    response = await client.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": os.getenv("ANTHROPIC_API_KEY"),
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json"
                        },
                        json={
                            "model": self.config.entity_extraction_model,
                            "max_tokens": 2000,
                            "messages": [{"role": "user", "content": prompt}]
                        }
                    )
                    response.raise_for_status()
                    data = response.json()
                    text = data["content"][0]["text"]
                else:
                    response = await client.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {os.getenv('OPENAI_API_KEY')}",
                            "Content-Type": "application/json"
                        },
                        json={
                            "model": self.config.entity_extraction_model,
                            "max_tokens": 2000,
                            "messages": [{"role": "user", "content": prompt}],
                            "response_format": {"type": "json_object"}
                        }
                    )
                    response.raise_for_status()
                    data = response.json()
                    text = data["choices"][0]["message"]["content"]

                # Parse response
                result = json.loads(text)

                from .models import EntityType, RelationType

                entity_map: Dict[str, str] = {}

                for e in result.get("entities", []):
                    entity = Entity(
                        document_id=document.id,
                        name=e["name"],
                        entity_type=EntityType(e.get("type", "concept")),
                        mentions=e.get("mentions", []),
                    )
                    entities.append(entity)
                    entity_map[e["name"]] = entity.id

                for r in result.get("relations", []):
                    source_id = entity_map.get(r["source"])
                    target_id = entity_map.get(r["target"])
                    if source_id and target_id:
                        relation = Relation(
                            document_id=document.id,
                            source_entity_id=source_id,
                            target_entity_id=target_id,
                            relation_type=RelationType(r.get("type", "related_to")),
                        )
                        relations.append(relation)

        except Exception as e:
            logger.warning(f"Entity extraction failed: {e}")

        return entities, relations

    async def _store_vectors(self, document: Document, chunks: List[Chunk]):
        """Store chunk embeddings in vector store."""
        # This would integrate with a vector database like Pinecone, Qdrant, etc.
        logger.info(f"Storing {len(chunks)} vectors for document {document.id}")
        # Placeholder for vector store integration
        pass

    async def _store_graph(
        self,
        document: Document,
        entities: List[Entity],
        relations: List[Relation]
    ):
        """Store entities and relations in knowledge graph."""
        # This would integrate with Neo4j or GraphSub
        logger.info(f"Storing {len(entities)} entities, {len(relations)} relations for document {document.id}")
        # Placeholder for graph store integration
        pass

    def _notify_progress(self, job: IngestionJob):
        """Notify progress callback."""
        if self._on_progress:
            try:
                self._on_progress(job)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")

    def get_job(self, job_id: str) -> Optional[IngestionJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)

    def list_jobs(self, status: Optional[IngestionStatus] = None) -> List[IngestionJob]:
        """List all jobs, optionally filtered by status."""
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        return jobs


async def ingest_file(
    file_path: str,
    config: Optional[PipelineConfig] = None
) -> IngestionResult:
    """
    Convenience function to ingest a single file.

    Args:
        file_path: Path to file
        config: Optional pipeline configuration

    Returns:
        IngestionResult
    """
    pipeline = IngestionPipeline(config)
    return await pipeline.ingest(file_path)


async def ingest_url(
    url: str,
    config: Optional[PipelineConfig] = None
) -> IngestionResult:
    """
    Convenience function to ingest from a URL.

    Args:
        url: URL to fetch and ingest
        config: Optional pipeline configuration

    Returns:
        IngestionResult
    """
    pipeline = IngestionPipeline(config)
    return await pipeline.ingest(url)
