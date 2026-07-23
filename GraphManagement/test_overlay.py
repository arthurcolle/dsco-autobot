#!/usr/bin/env python3
"""Test script for overlay system with Jina embeddings."""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from graph_management.overlay import (
    graph_object,
    BasedModel,
    EmbeddingSpec,
    EmbeddingSource,
    AutoLinkSimilar,
    RelationSpec,
    FieldSpec,
    Direction,
    FieldIndexKind,
    SimilarityFn,
)
from graph_management.overlay.runtime import register_graph_client, register_embedder
from graph_management.embeddings.jina_provider import JinaProvider


async def main():
    print("=" * 60)
    print("Graph Overlay + Jina Embeddings Test")
    print("=" * 60)

    # Check API key
    jina_key = os.environ.get("JINA_API_KEY")
    if not jina_key:
        print("❌ JINA_API_KEY not set!")
        print("   Get your free key: https://jina.ai/?sui=apikey")
        print("   Then: export JINA_API_KEY=your-key")
        return

    print(f"✓ JINA_API_KEY: {jina_key[:10]}...")

    # Register graph client
    print("\n1. Registering graph client...")
    graph_url = os.getenv("GRAPH_SERVICE_URL", "http://localhost:8000/v1")
    register_graph_client(
        service_id="graph-svc",
        base_url=graph_url,
        api_key=None
    )
    print(f"   ✓ Graph service: {graph_url}")

    # Register Jina embedder
    print("\n2. Registering Jina embedder...")
    jina = JinaProvider()
    register_embedder("jina", jina)
    print("   ✓ Jina embedder registered")

    # Define graph object
    print("\n3. Defining graph object with auto-embeddings...")

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
                autolink=AutoLinkSimilar(k=10, threshold=0.75)
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

    print("   ✓ Note model defined with:")
    print("     - Auto embeddings (title + text)")
    print("     - Auto-linking SIMILAR edges (k=10, threshold=0.75)")
    print("     - Vector index auto-creation")

    # Test (without actually saving)
    print("\n4. Creating test note...")
    note = Note(
        id="note-001",
        title="Graph Databases",
        text="Graph databases excel at representing connected data using nodes and relationships.",
        tags=["database", "graph"]
    )
    print(f"   ✓ Note created: {note.title}")
    print(f"     ID: {note.id}")
    print(f"     Text: {note.text[:50]}...")

    print("\n5. What would happen on .save():")
    print("   1. Compute embeddings from title+text (Jina v4)")
    print("   2. Create vector index if not exists")
    print("   3. k-NN search for similar notes (k=10)")
    print("   4. Auto-create SIMILAR edges (score > 0.75)")
    print("   5. Store node with properties + content_vector")

    print("\n6. Testing Jina embeddings directly...")
    try:
        # Test text embedding
        test_text = ["Hello world", "Graph databases"]
        embeddings = await jina.embed_text(test_text)
        print(f"   ✓ Generated {len(embeddings)} embeddings")
        print(f"     Dimensions: {len(embeddings[0])}")

        # Test image embedding (if you have base64 or URL)
        # img_embeddings = await jina.embed_images(["<base64 or url>"])

    except Exception as e:
        print(f"   ✗ Embedding test failed: {e}")

    # Cleanup
    await jina.close()

    print("\n" + "=" * 60)
    print("✓ Test complete!")
    print("\nTo actually use:")
    print("  1. Start services: ./start_local.sh")
    print("  2. Bootstrap: ./bootstrap.sh")
    print("  3. Set JINA_API_KEY")
    print("  4. Call note.save() - embeddings computed automatically!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
