"""
Ingestion Service Data Models

Core data structures for document processing and knowledge extraction.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Any, Optional
import uuid


class IngestionStatus(str, Enum):
    """Status of an ingestion job."""
    PENDING = "pending"
    EXTRACTING = "extracting"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    STORING = "storing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DocumentType(str, Enum):
    """Supported document types."""
    PDF = "pdf"
    HTML = "html"
    MARKDOWN = "markdown"
    CODE = "code"
    TEXT = "text"
    JSON = "json"
    CSV = "csv"


class EntityType(str, Enum):
    """Types of extracted entities."""
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    DATE = "date"
    CONCEPT = "concept"
    PRODUCT = "product"
    EVENT = "event"
    TECHNOLOGY = "technology"
    CUSTOM = "custom"


class RelationType(str, Enum):
    """Types of entity relationships."""
    MENTIONS = "mentions"
    RELATED_TO = "related_to"
    PART_OF = "part_of"
    AUTHORED_BY = "authored_by"
    OCCURRED_AT = "occurred_at"
    LOCATED_IN = "located_in"
    WORKS_FOR = "works_for"
    DEPENDS_ON = "depends_on"
    CUSTOM = "custom"


@dataclass
class Document:
    """Represents a document to be ingested."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: str = ""  # URL, file path, or identifier
    content: str = ""
    doc_type: DocumentType = DocumentType.TEXT
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Processing results
    title: Optional[str] = None
    author: Optional[str] = None
    language: str = "en"
    word_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "content": self.content,
            "doc_type": self.doc_type.value,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "title": self.title,
            "author": self.author,
            "language": self.language,
            "word_count": self.word_count,
        }


@dataclass
class Chunk:
    """A chunk of text extracted from a document."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str = ""
    content: str = ""
    index: int = 0  # Position in document

    # Metadata
    start_char: int = 0
    end_char: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Embedding
    embedding: Optional[List[float]] = None
    embedding_model: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "document_id": self.document_id,
            "content": self.content,
            "index": self.index,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "metadata": self.metadata,
            "embedding": self.embedding,
            "embedding_model": self.embedding_model,
        }


@dataclass
class Entity:
    """An extracted entity from a document."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str = ""
    chunk_id: Optional[str] = None

    name: str = ""
    entity_type: EntityType = EntityType.CONCEPT
    confidence: float = 1.0

    # Context
    mentions: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "document_id": self.document_id,
            "chunk_id": self.chunk_id,
            "name": self.name,
            "entity_type": self.entity_type.value,
            "confidence": self.confidence,
            "mentions": self.mentions,
            "attributes": self.attributes,
        }


@dataclass
class Relation:
    """A relationship between two entities."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str = ""

    source_entity_id: str = ""
    target_entity_id: str = ""
    relation_type: RelationType = RelationType.RELATED_TO

    confidence: float = 1.0
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "document_id": self.document_id,
            "source_entity_id": self.source_entity_id,
            "target_entity_id": self.target_entity_id,
            "relation_type": self.relation_type.value,
            "confidence": self.confidence,
            "attributes": self.attributes,
        }


@dataclass
class IngestionJob:
    """Tracks an ingestion job's progress."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Source
    source_type: str = "file"  # file, url, api, stream
    source_path: str = ""

    # Status
    status: IngestionStatus = IngestionStatus.PENDING
    progress: float = 0.0  # 0.0 to 1.0
    error: Optional[str] = None

    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Results
    document_id: Optional[str] = None
    chunk_count: int = 0
    entity_count: int = 0
    relation_count: int = 0

    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "source_path": self.source_path,
            "status": self.status.value,
            "progress": self.progress,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "document_id": self.document_id,
            "chunk_count": self.chunk_count,
            "entity_count": self.entity_count,
            "relation_count": self.relation_count,
            "config": self.config,
        }

    def start(self) -> None:
        """Mark job as started."""
        self.status = IngestionStatus.EXTRACTING
        self.started_at = datetime.now(timezone.utc)

    def complete(self) -> None:
        """Mark job as completed."""
        self.status = IngestionStatus.COMPLETED
        self.completed_at = datetime.now(timezone.utc)
        self.progress = 1.0

    def fail(self, error: str) -> None:
        """Mark job as failed."""
        self.status = IngestionStatus.FAILED
        self.completed_at = datetime.now(timezone.utc)
        self.error = error


@dataclass
class IngestionResult:
    """Result of an ingestion operation."""
    job_id: str
    document: Document
    chunks: List[Chunk]
    entities: List[Entity]
    relations: List[Relation]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "document": self.document.to_dict(),
            "chunks": [c.to_dict() for c in self.chunks],
            "entities": [e.to_dict() for e in self.entities],
            "relations": [r.to_dict() for r in self.relations],
        }
