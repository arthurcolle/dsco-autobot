#!/usr/bin/env python3
"""
Quick start script for graph management overlay system.

Sets up a minimal working example.
"""

import os
import asyncio
from graph_management import (
    graph_object, BasedModel,
    EmbeddingSpec, EmbeddingSource, AutoLinkSimilar,
    RelationSpec, FieldSpec,
    Direction, FieldIndexKind, SimilarityFn,
    register_graph_client, register_embedder,
    JinaProvider
)


async def main():
    print("🚀 Graph Management Quick Start")
    print("=" * 60)

    # Check environment
    if not os.getenv("JINA_API_KEY"):
        print("❌ JINA_API_KEY not set!")
        print("   Get your free API key: https://jina.ai/?sui=apikey")
        print("   Then: export JINA_API_KEY=your-key-here")
        return

    # 1. Register graph service
    print("\n1. Registering graph service...")
    register_graph_client(
        service_id="graph-svc",
        base_url=os.getenv("GRAPH_SERVICE_URL", "http://localhost:8000"),
        api_key=None
    )
    print("   ✓ Graph service registered")

    # 2. Register Jina embedder
    print("\n2. Registering Jina embedder...")
    jina = JinaProvider()
    register_embedder("jina", jina)
    print("   ✓ Jina embedder registered")

    # 3. Define a graph object
    print("\n3. Defining graph object...")

    @graph_object(
        label="Note",
        key=["id"],
        service_id="graph-svc",
        embeddings=[
            EmbeddingSpec(
                name="content",
                provider="jina",
                model="jina-embeddings-v4",
                dim=1024,
                similarity=SimilarityFn.cosine,
                source=EmbeddingSource(fields=["title", "text"]),
                autolink=AutoLinkSimilar(k=10, threshold=0.80)
            )
        ],
        relations=[
            RelationSpec(
                name="TAGGED_WITH",
                direction=Direction.OUT,
                from_field="tags",
                resolve_to_label="Tag",
                resolve_key=["name"]
            )
        ],
        fields=[
            FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
            FieldSpec(name="title", index=FieldIndexKind.fulltext),
            FieldSpec(name="text", index=FieldIndexKind.fulltext),
        ]
    )
    class Note(BasedModel):
        id: str
        title: str
        text: str
        tags: list[str] = []

    print("   ✓ Note model defined with embeddings and auto-linking")

    # 4. Create and save a note
    print("\n4. Creating note...")
    note = Note(
        id="note-001",
        title="Graph Databases",
        text="Graph databases excel at representing connected data using nodes and relationships.",
        tags=["database", "graph", "technology"]
    )

    print("   Note created (not yet saved):")
    print(f"     ID: {note.id}")
    print(f"     Title: {note.title}")
    print(f"     Tags: {', '.join(note.tags)}")

    # Uncomment to actually save (requires running graph service):
    # note.save()
    # print("   ✓ Note saved with embeddings and relationships")

    print("\n5. What happens on .save():")
    print("   - Computes embedding for title+text")
    print("   - Performs k-NN search for similar notes")
    print("   - Creates SIMILAR edges (if score > 0.80)")
    print("   - Creates TAGGED_WITH edges to Tag nodes")
    print("   - Stores node with properties + vector")

    print("\n" + "=" * 60)
    print("✓ Quick start complete!")
    print("\nNext steps:")
    print("  1. Start graph service: cd infra && modal serve ...")
    print("  2. Run examples: python examples/conversation_example.py")
    print("  3. Run tests: pytest tests/")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
