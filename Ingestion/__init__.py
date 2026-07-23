"""
Ingestion Service

High-throughput data ingestion pipeline for the agent platform.

Responsibilities:
- Document processing (PDF, HTML, Markdown, code)
- Embedding generation with batching
- Knowledge graph population
- Vector index management
- Stream processing for real-time data

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                      DATA SOURCES                               │
    │   Files  │  URLs  │  APIs  │  Streams  │  Webhooks             │
    └───────────────────────────┬─────────────────────────────────────┘
                                │
    ┌───────────────────────────┴─────────────────────────────────────┐
    │                    INGESTION SERVICE                            │
    │                                                                 │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
    │  │ Extractors  │  │ Chunkers    │  │ Embedders   │            │
    │  │ PDF/HTML/   │  │ Semantic    │  │ Jina/OpenAI │            │
    │  │ Code/MD     │  │ Sliding     │  │ Batched     │            │
    │  └─────────────┘  └─────────────┘  └─────────────┘            │
    │                                                                 │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
    │  │ Entity      │  │ Relation    │  │ Graph       │            │
    │  │ Extraction  │  │ Extraction  │  │ Population  │            │
    │  └─────────────┘  └─────────────┘  └─────────────┘            │
    └─────────────────────────────────────────────────────────────────┘
                                │
    ┌───────────────────────────┴─────────────────────────────────────┐
    │                      STORAGE                                    │
    │   Neo4j (Graph)  │  Vectors (Embeddings)  │  Redis (Cache)     │
    └─────────────────────────────────────────────────────────────────┘
"""

from .models import (
    Document,
    Chunk,
    Entity,
    Relation,
    IngestionJob,
    IngestionStatus,
    IngestionResult,
)

from .extractors import (
    Extractor,
    PDFExtractor,
    HTMLExtractor,
    MarkdownExtractor,
    CodeExtractor,
    TextExtractor,
    get_extractor,
)

from .chunkers import (
    Chunker,
    ChunkerConfig,
    SemanticChunker,
    SlidingWindowChunker,
    CodeChunker,
    get_chunker,
)

from .embedders import (
    Embedder,
    EmbedderConfig,
    JinaEmbedder,
    OpenAIEmbedder,
    CohereEmbedder,
    BatchEmbedder,
    LocalEmbedder,
    get_embedder,
)

from .pipeline import (
    IngestionPipeline,
    PipelineConfig,
    ingest_file,
    ingest_url,
)

from .server import (
    create_ingestion_app,
    IngestionServer,
)

__all__ = [
    # Models
    "Document",
    "Chunk",
    "Entity",
    "Relation",
    "IngestionJob",
    "IngestionStatus",
    "IngestionResult",
    # Extractors
    "Extractor",
    "PDFExtractor",
    "HTMLExtractor",
    "MarkdownExtractor",
    "CodeExtractor",
    "TextExtractor",
    "get_extractor",
    # Chunkers
    "Chunker",
    "ChunkerConfig",
    "SemanticChunker",
    "SlidingWindowChunker",
    "CodeChunker",
    "get_chunker",
    # Embedders
    "Embedder",
    "EmbedderConfig",
    "JinaEmbedder",
    "OpenAIEmbedder",
    "CohereEmbedder",
    "BatchEmbedder",
    "LocalEmbedder",
    "get_embedder",
    # Pipeline
    "IngestionPipeline",
    "PipelineConfig",
    "ingest_file",
    "ingest_url",
    # Server
    "create_ingestion_app",
    "IngestionServer",
]

__version__ = "0.1.0"
