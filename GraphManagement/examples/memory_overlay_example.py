"""
Complete example of the graph overlay system.

Demonstrates:
- @graph.object decorator with embeddings, relations, calculated fields
- BasedModel ORM usage
- Multi-space embeddings with auto-linking
- Relationship materialization (static & dynamic)
- Calculated fields (tokens, salience)
- Memory policies
"""

import time
from typing import List
from pydantic import BaseModel, Field

# Note: In production, import from graph_management.overlay
# from graph_management.overlay import (
#     graph_object, BasedModel, EmbeddingSpec, EmbeddingSource,
#     AutoLinkSimilar, RelationSpec, FieldSpec, CalculatedFieldSpec,
#     MemoryPolicies, SimilarityFn, Direction, FieldIndexKind,
#     RetentionPolicy
# )
# from graph_management.overlay.runtime import register_graph_client, register_embedder
# from graph_management.overlay.calcs import calc_tokens, calc_salience


# ========= Setup (run once) =========

def setup():
    """Initialize graph client and embedding provider."""
    # Register graph service client
    # register_graph_client(
    #     service_id="graph-svc",
    #     base_url="http://localhost:8000",
    #     api_key=None
    # )

    # Register a simple embedding provider (replace with OpenAI/FastEmbed)
    # def fake_embedder(text: str, model: str, dim: int) -> List[float]:
    #     import hashlib
    #     import random
    #     h = hashlib.sha256(f"{model}:{text}".encode()).digest()
    #     rnd = random.Random(h)
    #     return [rnd.uniform(-1, 1) for _ in range(dim)]
    #
    # register_embedder("local-demo", fake_embedder)
    pass


# ========= Domain Models =========

# @graph_object(
#     label="Conversation",
#     key=["id"],
#     service_id="graph-svc",
#     fields=[
#         FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
#         FieldSpec(name="title", index=FieldIndexKind.fulltext),
#     ]
# )
# class Conversation(BasedModel):
#     """A conversation container."""
#     id: str
#     title: str | None = None
#     created_at: float = Field(default_factory=time.time)


# @graph_object(
#     label="Entity",
#     key=["name"],
#     service_id="graph-svc",
#     fields=[
#         FieldSpec(name="name", index=FieldIndexKind.btree, unique=True),
#     ]
# )
# class Entity(BasedModel):
#     """Named entities mentioned in messages."""
#     name: str
#     type: str | None = None  # person, place, org, etc.


# @graph_object(
#     label="MemoryItem",
#     key=["id"],
#     service_id="graph-svc",
#
#     # Multi-space embeddings
#     embeddings=[
#         # Content embedding with auto-linking
#         EmbeddingSpec(
#             name="content",
#             provider="local-demo",  # Replace with "openai"
#             model="text-embedding-demo",
#             dim=256,
#             similarity=SimilarityFn.cosine,
#             source=EmbeddingSource(fields=["title", "text"], sep="\n\n"),
#             autolink=AutoLinkSimilar(
#                 k=25,
#                 threshold=0.80,
#                 weight=2.2,
#                 label_scope="MemoryItem",
#                 rel_type="SIMILAR",
#                 bidirectional=True
#             )
#         ),
#         # Title embedding (no auto-linking)
#         EmbeddingSpec(
#             name="title",
#             provider="local-demo",
#             model="text-embedding-demo",
#             dim=128,
#             similarity=SimilarityFn.cosine,
#             source=EmbeddingSource(field="title"),
#         )
#     ],
#
#     # Relationships
#     relations=[
#         # Static: belongs to conversation
#         RelationSpec(
#             name="BELONGS_TO",
#             direction=Direction.OUT,
#             to_label="Conversation",
#             to_key=["conversation_id"],
#         ),
#         # Dynamic: mentions entities (from field list)
#         RelationSpec(
#             name="MENTIONS",
#             direction=Direction.OUT,
#             from_field="entities",
#             resolve_to_label="Entity",
#             resolve_key=["name"],
#         ),
#     ],
#
#     # Structured fields
#     fields=[
#         FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
#         FieldSpec(name="title", index=FieldIndexKind.fulltext, searchable=True),
#         FieldSpec(name="text", index=FieldIndexKind.fulltext, searchable=True),
#         FieldSpec(name="entities", index=FieldIndexKind.btree, store=True),
#         FieldSpec(name="conversation_id", index=FieldIndexKind.btree, store=True),
#         FieldSpec(name="tokens", index=FieldIndexKind.none, store=True),
#         FieldSpec(name="salience", index=FieldIndexKind.btree, store=True),
#         FieldSpec(name="created_ts", index=FieldIndexKind.btree, store=True),
#         FieldSpec(name="updated_ts", index=FieldIndexKind.btree, store=True),
#         FieldSpec(name="score", index=FieldIndexKind.none, store=True),
#     ],
#
#     # Calculated fields
#     calc_fields=[
#         CalculatedFieldSpec(
#             name="tokens",
#             fn=calc_tokens("text")
#         ),
#         CalculatedFieldSpec(
#             name="salience",
#             fn=calc_salience(
#                 score_field="score",
#                 created_field="created_ts",
#                 updated_field="updated_ts",
#                 alpha=0.65,
#                 beta=0.25,
#                 gamma=0.10,
#                 half_life_seconds=5 * 24 * 3600  # 5 days
#             )
#         ),
#     ],
#
#     # Memory policies
#     policies=MemoryPolicies(
#         retention=RetentionPolicy.keep,
#         ttl_seconds=None,
#         versioned=True,
#         redact_on_export=False,
#         salience_alpha=0.65,
#         salience_beta=0.25,
#         salience_gamma=0.10
#     )
# )
# class MemoryItem(BasedModel):
#     """A memory item with embeddings, relations, and calculated fields."""
#     id: str
#     title: str | None = None
#     text: str
#     entities: List[str] = Field(default_factory=list)
#     conversation_id: str | None = None
#     score: float = 0.0
#     tokens: int | None = None
#     salience: float | None = None
#     created_ts: float = Field(default_factory=time.time)
#     updated_ts: float = Field(default_factory=time.time)


# ========= Usage Examples =========

def example_basic_usage():
    """Basic create and save."""
    # Create a memory item
    # m = MemoryItem(
    #     id="m-001",
    #     title="Roger + Amelia friendship",
    #     text="Amelia knows Roger Waters. They collaborated on a music data pipeline.",
    #     entities=["Amelia", "Roger Waters", "music", "pipeline"],
    #     conversation_id="c-77",
    #     score=0.8
    # )
    #
    # # Save to graph (triggers: calculated fields → embeddings → merge → relations)
    # m.save()
    #
    # print(f"Saved memory {m.id}")
    # print(f"  Tokens: {m.tokens}")
    # print(f"  Salience: {m.salience}")
    pass


def example_relationships():
    """Create and link objects."""
    # # Create conversation
    # conv = Conversation(id="c-77", title="Music Collaboration")
    # conv.save()
    #
    # # Create memory items
    # m1 = MemoryItem(
    #     id="m-001",
    #     text="Amelia is a data engineer.",
    #     conversation_id="c-77",
    #     entities=["Amelia"]
    # )
    # m1.save()
    #
    # m2 = MemoryItem(
    #     id="m-002",
    #     text="Roger Waters is a musician.",
    #     conversation_id="c-77",
    #     entities=["Roger Waters"]
    # )
    # m2.save()
    #
    # # Relationships automatically created:
    # # - (m1)-[:BELONGS_TO]->(conv)
    # # - (m1)-[:MENTIONS]->(:Entity {name:"Amelia"})
    # # - (m2)-[:BELONGS_TO]->(conv)
    # # - (m2)-[:MENTIONS]->(:Entity {name:"Roger Waters"})
    # # - (m1)-[:SIMILAR]->(m2) if embedding similarity > threshold
    pass


def example_orm_operations():
    """Use BasedModel ORM methods."""
    # # Load or create
    # m = MemoryItem(id="m-003", text="Example memory", conversation_id="c-77")
    # m.save()
    #
    # # Link to another object
    # other = MemoryItem(id="m-004", text="Related memory", conversation_id="c-77")
    # other.save()
    # m.link("RELATED_TO", other, props={"strength": 0.9})
    #
    # # Traverse neighbors
    # similar = m.neighbors(rel_type="SIMILAR", direction="OUT", limit=10)
    # print(f"Found {len(similar)} similar memories")
    #
    # # Update and re-save
    # m.score = 1.0
    # m.updated_ts = time.time()
    # m.save(update=True)  # Re-computes salience with new score
    pass


def example_auto_linking():
    """Demonstrate SIMILAR edge auto-creation."""
    # Create semantically similar memories
    # memories = [
    #     MemoryItem(
    #         id=f"m-{i}",
    #         title=f"Music {i}",
    #         text=f"Discussion about music and collaboration {i}",
    #         conversation_id="c-77",
    #         entities=["music"]
    #     )
    #     for i in range(1, 6)
    # ]
    #
    # # Save all (each triggers embedding + auto-link)
    # for m in memories:
    #     m.save()
    #     # After save, SIMILAR edges created to top-k matches above threshold
    #
    # # Query the graph to see SIMILAR edges:
    # # MATCH (m:MemoryItem {id:'m-1'})-[s:SIMILAR]->(m2)
    # # RETURN m2.id, s.weight
    pass


if __name__ == "__main__":
    print("Graph Overlay System Example")
    print("=" * 60)
    print()
    print("This example demonstrates the @graph.object decorator")
    print("with embeddings, relationships, and calculated fields.")
    print()
    print("To run:")
    print("1. Set up graph service at http://localhost:8000")
    print("2. Uncomment the decorator definitions above")
    print("3. Call setup() to register clients")
    print("4. Run example_*() functions")
    print()
    print("Features demonstrated:")
    print("  ✓ Multi-space embeddings (content + title)")
    print("  ✓ Auto-linking via SIMILAR edges (k-NN)")
    print("  ✓ Static relationships (BELONGS_TO)")
    print("  ✓ Dynamic relationships (MENTIONS from list)")
    print("  ✓ Calculated fields (tokens, salience)")
    print("  ✓ BasedModel ORM (.save, .link, .neighbors)")
    print("  ✓ Memory policies (retention, versioning)")
