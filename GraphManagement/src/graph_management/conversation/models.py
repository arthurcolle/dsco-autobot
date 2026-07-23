"""
Graph models for conversation system.

Hierarchical structure:
- Conversation → Messages → Blocks
- Messages → ToolCalls → ToolResults
- Messages → Entities (mentions)
- Message → Message (SIMILAR, REPLY_TO, NEXT)
"""

import time
from typing import List, Optional, Dict, Any
from pydantic import Field

from ..overlay import (
    graph_object, BasedModel, EmbeddingSpec, EmbeddingSource,
    AutoLinkSimilar, RelationSpec, FieldSpec, CalculatedFieldSpec,
    Direction, FieldIndexKind, SimilarityFn
)
from ..overlay.calcs import calc_tokens


@graph_object(
    label="Conversation",
    key=["id"],
    service_id="graph-svc",
    fields=[
        FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="title", index=FieldIndexKind.fulltext),
    ]
)
class Conversation(BasedModel):
    """Top-level conversation container."""
    id: str
    title: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict)


@graph_object(
    label="Entity",
    key=["name"],
    service_id="graph-svc",
    fields=[
        FieldSpec(name="name", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="type", index=FieldIndexKind.btree),
    ]
)
class Entity(BasedModel):
    """Named entities extracted from messages."""
    name: str
    type: Optional[str] = None  # person, place, org, concept, etc.
    metadata: Dict[str, Any] = Field(default_factory=dict)


@graph_object(
    label="Message",
    key=["id"],
    service_id="graph-svc",
    embeddings=[
        EmbeddingSpec(
            name="content",
            provider="jina",
            model="jina-embeddings-v4",
            dim=1024,
            similarity=SimilarityFn.cosine,
            source=EmbeddingSource(fields=["text"]),
            autolink=AutoLinkSimilar(
                k=20,
                threshold=0.78,
                weight=2.2,
                label_scope="Message",
                rel_type="SIMILAR",
                bidirectional=True
            )
        )
    ],
    relations=[
        RelationSpec(
            name="HAS_MESSAGE",
            direction=Direction.IN,
            to_label="Conversation",
            to_key=["conversation_id"]
        ),
        RelationSpec(
            name="MENTIONS",
            direction=Direction.OUT,
            from_field="entities",
            resolve_to_label="Entity",
            resolve_key=["name"]
        ),
    ],
    fields=[
        FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="role", index=FieldIndexKind.btree),
        FieldSpec(name="text", index=FieldIndexKind.fulltext, searchable=True),
        FieldSpec(name="conversation_id", index=FieldIndexKind.btree),
        FieldSpec(name="level", index=FieldIndexKind.btree),
        FieldSpec(name="tokens", index=FieldIndexKind.none, store=True),
    ],
    calc_fields=[
        CalculatedFieldSpec(name="tokens", fn=calc_tokens("text"))
    ]
)
class Message(BasedModel):
    """
    Message in a conversation with embedding and relationships.

    Auto-creates:
    - SIMILAR edges to semantically similar messages
    - MENTIONS edges to entities
    - HAS_MESSAGE edge to conversation
    """
    id: str
    conversation_id: str
    role: str  # "user", "assistant", "system", "tool"
    text: Optional[str] = None
    level: Optional[int] = None  # depth in conversation tree
    ts: float = Field(default_factory=time.time)
    entities: List[str] = Field(default_factory=list)
    tokens: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


@graph_object(
    label="Block",
    key=["id"],
    service_id="graph-svc",
    embeddings=[
        EmbeddingSpec(
            name="content",
            provider="jina",
            model="jina-embeddings-v4",
            dim=512,
            similarity=SimilarityFn.cosine,
            source=EmbeddingSource(field="text"),
        )
    ],
    relations=[
        RelationSpec(
            name="HAS_BLOCK",
            direction=Direction.IN,
            to_label="Message",
            to_key=["message_id"]
        ),
    ],
    fields=[
        FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="type", index=FieldIndexKind.btree),
        FieldSpec(name="message_id", index=FieldIndexKind.btree),
    ]
)
class Block(BasedModel):
    """Content block within a message (text, image, code, etc.)."""
    id: str
    message_id: str
    type: str  # "text", "image", "code", "tool_use", "tool_result"
    text: Optional[str] = None
    mime: Optional[str] = None
    uri: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


@graph_object(
    label="ToolCall",
    key=["id"],
    service_id="graph-svc",
    relations=[
        RelationSpec(
            name="CALLS_TOOL",
            direction=Direction.IN,
            to_label="Message",
            to_key=["message_id"]
        ),
    ],
    fields=[
        FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="tool_name", index=FieldIndexKind.btree),
        FieldSpec(name="message_id", index=FieldIndexKind.btree),
    ]
)
class ToolCall(BasedModel):
    """Tool invocation from a message."""
    id: str
    message_id: str
    tool_name: str
    action_name: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    issued_at: float = Field(default_factory=time.time)


@graph_object(
    label="ToolResult",
    key=["id"],
    service_id="graph-svc",
    relations=[
        RelationSpec(
            name="YIELDS",
            direction=Direction.IN,
            to_label="ToolCall",
            to_key=["call_id"]
        ),
    ],
    fields=[
        FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        FieldSpec(name="call_id", index=FieldIndexKind.btree),
        FieldSpec(name="status", index=FieldIndexKind.btree),
    ]
)
class ToolResult(BasedModel):
    """Result from a tool invocation."""
    id: str
    call_id: str
    status: str  # "ok", "error"
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    latency_ms: Optional[int] = None
    ended_at: float = Field(default_factory=time.time)
