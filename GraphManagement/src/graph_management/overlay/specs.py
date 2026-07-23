"""
Overlay specifications for graph objects.

Defines embedding spaces, relationships, fields, and policies for advanced
memory objects in the graph.
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence
from pydantic import BaseModel, Field
from enum import Enum


# ========= Embeddings =========


class SimilarityFn(str, Enum):
    """Vector similarity functions."""
    cosine = "cosine"
    dot = "dot"
    euclidean = "euclidean"


class EmbedWhen(str, Enum):
    """When to compute embeddings."""
    create = "create"
    update = "update"


class EmbeddingSource(BaseModel):
    """
    Defines where to extract text for embedding.

    Can specify:
    - field: single field name
    - fields: list of fields to concatenate
    - fn: custom callable(obj) -> str
    """
    field: Optional[str] = None
    fields: Optional[List[str]] = None
    fn: Optional[Callable[[Any], str]] = None
    sep: str = "\n"

    def extract(self, obj: Any) -> str:
        """Extract text from object for embedding."""
        if self.fn:
            return self.fn(obj)
        if self.field:
            return getattr(obj, self.field, "") or ""
        if self.fields:
            parts = [str(getattr(obj, f, "") or "") for f in self.fields]
            return self.sep.join([p for p in parts if p])
        return ""


class AutoLinkSimilar(BaseModel):
    """
    Configuration for automatic SIMILAR edge creation via vector search.

    After embedding, performs k-NN search and creates edges above threshold.
    """
    k: int = 20
    threshold: float = 0.78
    weight: float = 2.5
    label_scope: Optional[str] = None  # restrict to same/specific label
    rel_type: str = "SIMILAR"
    bidirectional: bool = True


class EmbeddingSpec(BaseModel):
    """
    Defines a named embedding space for an object.

    Example:
        EmbeddingSpec(
            name="content",
            provider="openai",
            model="text-embedding-3-large",
            dim=1536,
            source=EmbeddingSource(fields=["title", "text"]),
            autolink=AutoLinkSimilar(k=25, threshold=0.80)
        )
    """
    name: str
    provider: str  # "openai", "fastembed", "local", etc.
    model: str
    dim: int
    similarity: SimilarityFn = SimilarityFn.cosine
    source: EmbeddingSource
    when: List[EmbedWhen] = Field(default_factory=lambda: [EmbedWhen.create, EmbedWhen.update])
    store_property: Optional[str] = None  # property name or driver-side vector
    index_name: Optional[str] = None
    autolink: Optional[AutoLinkSimilar] = None


# ========= Relations =========


class Direction(str, Enum):
    """Relationship direction."""
    OUT = "out"
    IN = "in"
    BOTH = "both"


class Cardinality(str, Enum):
    """Relationship cardinality hints."""
    ONE_TO_ONE = "1-1"
    ONE_TO_MANY = "1-N"
    MANY_TO_ONE = "N-1"
    MANY_TO_MANY = "N-N"


class RelationSpec(BaseModel):
    """
    Declarative relationship wiring.

    Static endpoint (to_label + to_key):
        RelationSpec(
            name="BELONGS_TO",
            to_label="Conversation",
            to_key=["conversation_id"]
        )

    Dynamic endpoint (from_field with resolution):
        RelationSpec(
            name="MENTIONS",
            from_field="entities",
            resolve_to_label="Entity",
            resolve_key=["name"]
        )
    """
    name: str
    direction: Direction = Direction.OUT
    cardinality: Cardinality = Cardinality.MANY_TO_MANY

    # Static endpoint
    to_label: Optional[str] = None
    to_key: Optional[List[str]] = None

    # Relationship properties
    props: Dict[str, Any] = Field(default_factory=dict)

    # Dynamic sources
    from_field: Optional[str] = None
    resolve_to_label: Optional[str] = None
    resolve_key: Optional[List[str]] = None

    # Lifecycle hooks
    on_create: Optional[Callable[[Any, Any], None]] = None
    on_update: Optional[Callable[[Any, Any], None]] = None


# ========= Structured & Calculated Fields =========


class FieldIndexKind(str, Enum):
    """Index types for fields."""
    none = "none"
    btree = "btree"
    fulltext = "fulltext"


class PiiTreatment(str, Enum):
    """PII handling strategies."""
    none = "none"
    mask = "mask"
    hash = "hash"
    redact = "redact"


class FieldSpec(BaseModel):
    """
    Structured field metadata for schema generation and constraints.
    """
    name: str
    index: FieldIndexKind = FieldIndexKind.none
    unique: bool = False
    searchable: bool = False
    pii: PiiTreatment = PiiTreatment.none
    encrypt_at_rest: bool = False
    ttl_seconds: Optional[int] = None
    store: bool = True  # if False, virtual only


class CalcWhen(str, Enum):
    """When to compute calculated fields."""
    create = "create"
    update = "update"


class CalculatedFieldSpec(BaseModel):
    """
    Calculated property computed from object state.

    Example:
        CalculatedFieldSpec(
            name="tokens",
            fn=lambda obj: len(obj.text.split())
        )
    """
    name: str
    fn: Callable[[Any], Any]
    when: List[CalcWhen] = Field(default_factory=lambda: [CalcWhen.create, CalcWhen.update])
    store: bool = True


# ========= Policies (overlay memory) =========


class RetentionPolicy(str, Enum):
    """Memory retention policies."""
    keep = "keep"
    archive = "archive"
    ttl = "ttl"


class MemoryPolicies(BaseModel):
    """
    Memory overlay policies for retention, versioning, and salience.
    """
    retention: RetentionPolicy = RetentionPolicy.keep
    ttl_seconds: Optional[int] = None
    versioned: bool = True
    redact_on_export: bool = False

    # Salience scoring weights
    salience_alpha: float = 0.6  # recency weight
    salience_beta: float = 0.3   # frequency weight
    salience_gamma: float = 0.1  # explicit score weight
