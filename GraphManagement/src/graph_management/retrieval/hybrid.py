"""
Hybrid retrieval combining graph closeness and vector similarity.

Formula:
    score = α * graph_closeness + β * vector_similarity + γ * temporal_decay + δ * rerank_score
"""

import math
import time
from typing import List, Dict, Any, Optional
from ..overlay.runtime import get_graph_client


def graph_closeness_search(
    service_id: str,
    pivot_message_id: str,
    max_depth: int = 4,
    k: int = 20,
    weights: Optional[Dict[str, float]] = None
) -> List[Dict[str, Any]]:
    """
    Find messages close to pivot via graph structure.

    Traverses relationships with weighted scoring:
    - SIMILAR: 2.5
    - REPLY_TO: 1.8
    - NEXT: 1.2
    - CALLS_TOOL: 1.0
    - YIELDS: 1.0
    - HAS_BLOCK: 0.7
    - HAS_MESSAGE: 0.7
    - MENTIONS: 0.9

    Args:
        service_id: Graph service ID
        pivot_message_id: Starting message ID
        max_depth: Maximum path depth
        k: Number of results
        weights: Custom relationship weights

    Returns:
        List of messages with structural_score
    """
    gc = get_graph_client(service_id)

    default_weights = {
        "SIMILAR": 2.5,
        "REPLY_TO": 1.8,
        "NEXT": 1.2,
        "CALLS_TOOL": 1.0,
        "YIELDS": 1.0,
        "HAS_BLOCK": 0.7,
        "HAS_MESSAGE": 0.7,
        "MENTIONS": 0.9,
    }
    weights = weights or default_weights

    # Build CASE statement for weights
    weight_cases = "\n".join([
        f"        WHEN '{rel}' THEN {score}"
        for rel, score in weights.items()
    ])

    query = f"""
    MATCH (m0:Message {{id: $pivot_id}})
    MATCH p=(m0)-[r*1..{max_depth}]-(m:Message)
    WHERE m.id <> $pivot_id
    WITH m, reduce(w=0.0, x IN relationships(p) |
      w + CASE type(x)
{weight_cases}
        ELSE 0.5 END
    ) AS path_weight
    RETURN m.id AS message_id,
           m.text AS text,
           m.role AS role,
           m.ts AS ts,
           max(path_weight) AS structural_score
    ORDER BY structural_score DESC
    LIMIT $k
    """

    # Execute via client
    result = gc.cypher(query, params={
        "pivot_id": pivot_message_id,
        "k": k
    })

    return result.get("rows", [])


def hybrid_search(
    service_id: str,
    query: str,
    conversation_id: Optional[str] = None,
    pivot_message_id: Optional[str] = None,
    k: int = 20,
    alpha: float = 0.4,  # graph weight
    beta: float = 0.4,   # vector weight
    gamma: float = 0.1,  # temporal weight
    delta: float = 0.1,  # rerank weight (if used)
    reranker: Optional[Any] = None,
    half_life_days: float = 7.0
) -> List[Dict[str, Any]]:
    """
    Hybrid search combining multiple signals.

    Args:
        service_id: Graph service ID
        query: Search query text
        conversation_id: Filter to conversation
        pivot_message_id: Optional pivot for graph closeness
        k: Number of results
        alpha: Graph closeness weight
        beta: Vector similarity weight
        gamma: Temporal decay weight
        delta: Reranker weight
        reranker: Optional JinaReranker instance
        half_life_days: Recency decay half-life

    Returns:
        List of messages with hybrid_score, sorted desc
    """
    from ..embeddings import JinaProvider
    import os

    gc = get_graph_client(service_id)

    # 1. Vector search
    jina = JinaProvider(api_key=os.getenv("JINA_API_KEY"))
    query_embedding = jina.embed(query, model="jina-embeddings-v4", dim=1024)

    # Build vector search query
    vector_query = """
    CALL db.index.vector.queryNodes('msg_content_emb', $k, $vector)
    YIELD node, score
    WHERE node:Message
    """
    if conversation_id:
        vector_query += " AND node.conversation_id = $conversation_id"
    vector_query += """
    RETURN node.id AS message_id,
           node.text AS text,
           node.role AS role,
           node.ts AS ts,
           score AS vector_score
    LIMIT $k
    """

    vector_params = {"k": k * 2, "vector": query_embedding}  # Get more for fusion
    if conversation_id:
        vector_params["conversation_id"] = conversation_id

    vector_results = gc.cypher(vector_query, params=vector_params)
    vector_rows = vector_results.get("rows", [])

    # 2. Graph closeness (if pivot provided)
    graph_rows = []
    if pivot_message_id:
        graph_rows = graph_closeness_search(
            service_id=service_id,
            pivot_message_id=pivot_message_id,
            k=k * 2
        )

    # 3. Merge results
    merged = {}
    now = time.time()
    half_life_seconds = half_life_days * 24 * 3600

    for row in vector_rows:
        msg_id = row["message_id"]
        merged[msg_id] = {
            **row,
            "vector_score": row.get("vector_score", 0.0),
            "structural_score": 0.0,
            "temporal_score": 0.0,
            "rerank_score": 0.0,
        }

    for row in graph_rows:
        msg_id = row["message_id"]
        if msg_id in merged:
            merged[msg_id]["structural_score"] = row.get("structural_score", 0.0)
        else:
            merged[msg_id] = {
                **row,
                "vector_score": 0.0,
                "structural_score": row.get("structural_score", 0.0),
                "temporal_score": 0.0,
                "rerank_score": 0.0,
            }

    # 4. Temporal decay
    for msg_id, item in merged.items():
        ts = item.get("ts", now)
        age = max(0.0, now - ts)
        item["temporal_score"] = math.exp(-math.log(2.0) * age / half_life_seconds)

    # 5. Reranking (optional)
    if reranker and len(merged) > 0:
        items = list(merged.values())
        reranked = reranker.rerank_with_metadata(
            query=query,
            items=items,
            text_key="text",
            top_n=None  # Rerank all
        )
        for item in reranked:
            msg_id = item["message_id"]
            merged[msg_id]["rerank_score"] = item.get("relevance_score", 0.0)

    # 6. Normalize scores
    def normalize(values):
        if not values or max(values) == 0:
            return {k: 0.0 for k in values}
        max_val = max(values.values())
        return {k: v / max_val for k, v in values.items()}

    vector_scores = {k: v["vector_score"] for k, v in merged.items()}
    struct_scores = {k: v["structural_score"] for k, v in merged.items()}
    temporal_scores = {k: v["temporal_score"] for k, v in merged.items()}
    rerank_scores = {k: v["rerank_score"] for k, v in merged.items()}

    norm_vector = normalize(vector_scores)
    norm_struct = normalize(struct_scores)
    norm_temporal = normalize(temporal_scores)
    norm_rerank = normalize(rerank_scores)

    # 7. Compute hybrid score
    for msg_id, item in merged.items():
        item["hybrid_score"] = (
            alpha * norm_struct.get(msg_id, 0.0) +
            beta * norm_vector.get(msg_id, 0.0) +
            gamma * norm_temporal.get(msg_id, 0.0) +
            delta * norm_rerank.get(msg_id, 0.0)
        )

    # 8. Sort and return top k
    results = sorted(merged.values(), key=lambda x: x["hybrid_score"], reverse=True)
    return results[:k]
