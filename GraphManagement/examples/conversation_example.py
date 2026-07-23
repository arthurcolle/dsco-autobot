"""
Complete conversation graph example.

Demonstrates:
- Creating conversations and messages
- Automatic embedding generation
- SIMILAR edge auto-linking
- Entity extraction and mentions
- Hybrid retrieval (graph + vector + reranking)
"""

import asyncio
import time
from typing import List

# Import conversation models
from graph_management.conversation import (
    Conversation, Message, Entity, ToolCall, ToolResult
)
from graph_management.overlay.runtime import register_graph_client, register_embedder
from graph_management.embeddings import JinaProvider
from graph_management.retrieval import hybrid_search, JinaReranker


async def setup():
    """Initialize graph client and embedding provider."""

    # Register graph service
    register_graph_client(
        service_id="graph-svc",
        base_url="http://localhost:8000",  # Your graph service
        api_key=None
    )

    # Register Jina embedder
    jina = JinaProvider()  # Uses JINA_API_KEY env var
    register_embedder("jina", jina)

    print("✓ Setup complete")


async def example_create_conversation():
    """Create a conversation with messages."""

    # Create conversation
    conv = Conversation(
        id="conv-001",
        title="Graph Database Discussion"
    )
    conv.save()
    print(f"✓ Created conversation: {conv.id}")

    # Create messages
    messages = [
        Message(
            id="msg-001",
            conversation_id="conv-001",
            role="user",
            text="What are the benefits of graph databases?",
            entities=["graph databases"],
            level=0
        ),
        Message(
            id="msg-002",
            conversation_id="conv-001",
            role="assistant",
            text="Graph databases excel at representing connected data with relationships. They use nodes and edges to model complex networks.",
            entities=["graph databases", "nodes", "edges"],
            level=1
        ),
        Message(
            id="msg-003",
            conversation_id="conv-001",
            role="user",
            text="How does Neo4j compare to other graph databases?",
            entities=["Neo4j", "graph databases"],
            level=2
        ),
    ]

    for msg in messages:
        msg.save()
        print(f"  ✓ Saved message {msg.id} (tokens: {msg.tokens})")

    # After saving, messages have:
    # - Embeddings computed
    # - SIMILAR edges created (if similarity > threshold)
    # - HAS_MESSAGE edges to conversation
    # - MENTIONS edges to entities

    print(f"✓ Created {len(messages)} messages with auto-linking")
    return conv, messages


async def example_entity_extraction():
    """Create entities from message mentions."""

    # Entities are auto-created via MENTIONS relationships
    # But we can also create them explicitly with metadata

    entities = [
        Entity(name="graph databases", type="concept"),
        Entity(name="Neo4j", type="technology"),
        Entity(name="nodes", type="concept"),
        Entity(name="edges", type="concept"),
    ]

    for ent in entities:
        ent.save()

    print(f"✓ Created {len(entities)} entities")


async def example_tool_calls():
    """Track tool invocations."""

    # Create a tool call
    call = ToolCall(
        id="call-001",
        message_id="msg-003",
        tool_name="web_search",
        action_name="search",
        args={"query": "Neo4j vs graph databases"}
    )
    call.save()

    # Create the result
    result = ToolResult(
        id="result-001",
        call_id="call-001",
        status="ok",
        output={"results": ["Neo4j is a popular graph database..."]},
        latency_ms=234
    )
    result.save()

    # Creates: (Message)-[:CALLS_TOOL]->(ToolCall)-[:YIELDS]->(ToolResult)

    print("✓ Created tool call with result")


async def example_hybrid_search():
    """Search using hybrid retrieval."""

    # Initialize reranker
    reranker = JinaReranker()

    # Perform hybrid search
    results = hybrid_search(
        service_id="graph-svc",
        query="how do graph databases work",
        conversation_id="conv-001",
        pivot_message_id="msg-002",  # Optional: boost messages near this one
        k=10,
        alpha=0.3,  # Graph closeness weight
        beta=0.4,   # Vector similarity weight
        gamma=0.2,  # Temporal decay weight
        delta=0.1,  # Reranker weight
        reranker=reranker,
        half_life_days=7.0
    )

    print(f"\n✓ Hybrid search results (top {len(results)}):")
    for i, result in enumerate(results, 1):
        print(f"  {i}. {result['message_id']}: {result['text'][:60]}...")
        print(f"     Score: {result['hybrid_score']:.4f} "
              f"(V:{result['vector_score']:.2f} "
              f"G:{result['structural_score']:.2f} "
              f"T:{result['temporal_score']:.2f} "
              f"R:{result['rerank_score']:.2f})")


async def example_graph_queries():
    """Direct graph queries for exploration."""
    from graph_management.overlay.runtime import get_graph_client

    gc = get_graph_client("graph-svc")

    # Find all messages mentioning an entity
    query = """
    MATCH (m:Message)-[:MENTIONS]->(e:Entity {name: $entity})
    RETURN m.id, m.text, m.role
    ORDER BY m.ts DESC
    LIMIT 10
    """
    result = gc.cypher(query, params={"entity": "Neo4j"})
    print(f"\n✓ Messages mentioning 'Neo4j': {len(result.get('rows', []))}")

    # Find similar messages
    query = """
    MATCH (m:Message {id: $msg_id})-[s:SIMILAR]->(m2:Message)
    RETURN m2.id, m2.text, s.weight
    ORDER BY s.weight DESC
    LIMIT 5
    """
    result = gc.cypher(query, params={"msg_id": "msg-002"})
    print(f"✓ Similar messages: {len(result.get('rows', []))}")

    # Tool call trace
    query = """
    MATCH (m:Message)-[:CALLS_TOOL]->(tc:ToolCall)-[:YIELDS]->(tr:ToolResult)
    RETURN m.id, tc.tool_name, tr.status, tr.latency_ms
    """
    result = gc.cypher(query, params={})
    print(f"✓ Tool call traces: {len(result.get('rows', []))}")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("Conversation Graph Example")
    print("=" * 60)
    print()

    await setup()
    print()

    # Create conversation structure
    conv, messages = await example_create_conversation()
    print()

    # Create entities
    await example_entity_extraction()
    print()

    # Track tool calls
    await example_tool_calls()
    print()

    # Hybrid search
    await example_hybrid_search()
    print()

    # Graph queries
    await example_graph_queries()
    print()

    print("=" * 60)
    print("✓ All examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
