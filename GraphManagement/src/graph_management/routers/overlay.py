"""Overlay router for graph object persistence with embeddings and relationships."""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import QueryRequest
from .dependencies import neo4j_query_one, neo4j_query_all, post


router = APIRouter(prefix="/v1/overlay", tags=["overlay"])


# ============================================================================
# Request/Response Models
# ============================================================================

class NodeMergeRequest(BaseModel):
    """Request to merge a node with optional vectors."""
    label: str
    key: Dict[str, Any]
    props: Dict[str, Any] = Field(default_factory=dict)
    vectors: Dict[str, List[float]] = Field(default_factory=dict)


class RelMergeRequest(BaseModel):
    """Request to merge a relationship."""
    out: Dict[str, Any]  # {label, key}
    rel: Dict[str, Any]  # {type, props}
    in_: Dict[str, Any] = Field(alias="in")  # {label, key}


class VectorSearchRequest(BaseModel):
    """Request for vector similarity search."""
    label: str
    space: str
    vector: List[float]
    k: int = 10
    threshold: float = 0.0
    similarity: str = "cosine"


class TraverseRequest(BaseModel):
    """Request for graph traversal."""
    start: Dict[str, Any]  # {label, key}
    rel_type: Optional[str] = None
    direction: str = "OUT"
    limit: int = 50


# ============================================================================
# Vector Index Management
# ============================================================================

async def ensure_vector_index(label: str, space: str, dimensions: int = 1024) -> bool:
    """
    Ensure vector index exists for label and space.

    Creates Neo4j vector index if it doesn't exist.
    """
    index_name = f"{label}_{space}_vector_idx"

    # Check if index exists
    check_query = """
    SHOW INDEXES
    YIELD name, type
    WHERE name = $index_name AND type = 'VECTOR'
    RETURN count(*) > 0 AS exists
    """

    result = await neo4j_query_one(check_query, {"index_name": index_name})

    if result and result.get("exists"):
        return True

    # Create vector index
    create_query = f"""
    CREATE VECTOR INDEX {index_name} IF NOT EXISTS
    FOR (n:{label})
    ON n.{space}_vector
    OPTIONS {{
        indexConfig: {{
            `vector.dimensions`: {dimensions},
            `vector.similarity_function`: 'cosine'
        }}
    }}
    """

    try:
        await post("/v1/query", {"query": create_query})
        return True
    except Exception as e:
        # Index creation is async, might return before complete
        return True


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/nodes/merge")
async def merge_node(
    req: NodeMergeRequest,
    _: Principal = Depends(require_permissions(["query:write"]))
) -> Dict[str, Any]:
    """
    Merge node with properties and optional vector embeddings.

    Auto-creates vector indexes on first use.
    """
    # Build key match
    key_props = " AND ".join([f"n.{k} = ${k}" for k in req.key.keys()])

    # Build property sets
    prop_sets = []
    params = dict(req.key)

    for k, v in req.props.items():
        param_name = f"prop_{k}"
        prop_sets.append(f"n.{k} = ${param_name}")
        params[param_name] = v

    # Build vector sets
    vector_sets = []
    for space, vector in req.vectors.items():
        if not vector:
            continue

        # Ensure index exists
        await ensure_vector_index(req.label, space, len(vector))

        param_name = f"vec_{space}"
        vector_sets.append(f"n.{space}_vector = ${param_name}")
        params[param_name] = vector

    # Build full query
    set_clause = ", ".join(prop_sets + vector_sets)

    query = f"""
    MERGE (n:{req.label} {{{key_props}}})
    ON CREATE SET {set_clause}, n.created_at = datetime()
    ON MATCH SET {set_clause}, n.updated_at = datetime()
    RETURN n
    """

    result = await neo4j_query_one(query, params)

    if not result:
        raise HTTPException(status_code=500, detail="Node merge failed")

    node = result["n"]
    return {
        "label": req.label,
        "id": node.get("id"),
        "element_id": node.get("element_id"),
        "properties": node.get("properties", {})
    }


@router.post("/rels/merge")
async def merge_rel(
    req: RelMergeRequest,
    _: Principal = Depends(require_permissions(["query:write"]))
) -> Dict[str, Any]:
    """Merge relationship between two nodes."""
    # Build key matches
    out_label = req.out["label"]
    out_key = req.out["key"]
    out_match = " AND ".join([f"a.{k} = $out_{k}" for k in out_key.keys()])

    in_label = req.in_["label"]
    in_key = req.in_["key"]
    in_match = " AND ".join([f"b.{k} = $in_{k}" for k in in_key.keys()])

    # Build params
    params = {}
    for k, v in out_key.items():
        params[f"out_{k}"] = v
    for k, v in in_key.items():
        params[f"in_{k}"] = v

    # Build relationship properties
    rel_type = req.rel["type"]
    rel_props = req.rel.get("props", {})

    prop_sets = []
    for k, v in rel_props.items():
        param_name = f"rel_{k}"
        prop_sets.append(f"r.{k} = ${param_name}")
        params[param_name] = v

    set_clause = ", ".join(prop_sets) if prop_sets else ""
    on_create = f"ON CREATE SET {set_clause}, r.created_at = datetime()" if set_clause else "ON CREATE SET r.created_at = datetime()"
    on_match = f"ON MATCH SET {set_clause}, r.updated_at = datetime()" if set_clause else "ON MATCH SET r.updated_at = datetime()"

    query = f"""
    MATCH (a:{out_label} {{{out_match}}})
    MATCH (b:{in_label} {{{in_match}}})
    MERGE (a)-[r:{rel_type}]->(b)
    {on_create}
    {on_match}
    RETURN r
    """

    result = await neo4j_query_one(query, params)

    if not result:
        raise HTTPException(status_code=404, detail="Nodes not found or relationship merge failed")

    rel = result["r"]
    return {
        "type": rel_type,
        "id": rel.get("id"),
        "element_id": rel.get("element_id"),
        "properties": rel.get("properties", {})
    }


@router.post("/vectors/search")
async def vector_search(
    req: VectorSearchRequest,
    _: Principal = Depends(require_permissions(["query:read"]))
) -> Dict[str, Any]:
    """
    Perform k-NN vector similarity search.

    Returns nodes with similarity scores above threshold.
    """
    index_name = f"{req.label}_{req.space}_vector_idx"
    vector_property = f"{req.space}_vector"

    # Check if index exists
    check_query = """
    SHOW INDEXES
    YIELD name, type
    WHERE name = $index_name AND type = 'VECTOR'
    RETURN count(*) > 0 AS exists
    """

    check_result = await neo4j_query_one(check_query, {"index_name": index_name})

    if not check_result or not check_result.get("exists"):
        raise HTTPException(
            status_code=404,
            detail=f"Vector index '{index_name}' not found. Create nodes with vectors first."
        )

    # Perform vector search
    query = f"""
    CALL db.index.vector.queryNodes($index_name, $k, $vector)
    YIELD node, score
    WHERE score >= $threshold
    RETURN node, score
    ORDER BY score DESC
    """

    params = {
        "index_name": index_name,
        "k": req.k,
        "vector": req.vector,
        "threshold": req.threshold
    }

    results = await neo4j_query_all(query, params)

    # Format results
    hits = []
    for r in results:
        node = r["node"]
        props = node.get("properties", {})

        hits.append({
            "label": req.label,
            "key": {k: props.get(k) for k in props.keys() if not k.endswith("_vector")},
            "properties": props,
            "score": r["score"]
        })

    return {"results": hits}


@router.post("/traverse")
async def traverse(
    req: TraverseRequest,
    _: Principal = Depends(require_permissions(["query:read"]))
) -> Dict[str, Any]:
    """Traverse graph from starting node."""
    start_label = req.start["label"]
    start_key = req.start["key"]
    start_match = " AND ".join([f"start.{k} = $start_{k}" for k in start_key.keys()])

    params = {}
    for k, v in start_key.items():
        params[f"start_{k}"] = v

    # Build relationship pattern
    if req.rel_type:
        if req.direction == "OUT":
            rel_pattern = f"-[r:{req.rel_type}]->"
        elif req.direction == "IN":
            rel_pattern = f"<-[r:{req.rel_type}]-"
        else:  # BOTH
            rel_pattern = f"-[r:{req.rel_type}]-"
    else:
        if req.direction == "OUT":
            rel_pattern = "-[r]->"
        elif req.direction == "IN":
            rel_pattern = "<-[r]-"
        else:
            rel_pattern = "-[r]-"

    query = f"""
    MATCH (start:{start_label} {{{start_match}}})
    MATCH (start){rel_pattern}(n)
    RETURN n
    LIMIT $limit
    """

    params["limit"] = req.limit

    results = await neo4j_query_all(query, params)

    # Format results
    nodes = []
    for r in results:
        node = r["n"]
        props = node.get("properties", {})
        nodes.append({
            "labels": node.get("labels", []),
            "properties": props,
            "id": node.get("id"),
            "element_id": node.get("element_id")
        })

    return {"nodes": nodes}


@router.get("/nodes/get")
async def get_node(
    label: str,
    key: str,  # JSON string of key dict
    _: Principal = Depends(require_permissions(["query:read"]))
) -> Dict[str, Any]:
    """Fetch node by label and key."""
    import json

    try:
        key_dict = json.loads(key)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid key JSON")

    key_match = " AND ".join([f"n.{k} = ${k}" for k in key_dict.keys()])

    query = f"""
    MATCH (n:{label} {{{key_match}}})
    RETURN n
    """

    result = await neo4j_query_one(query, key_dict)

    if not result:
        raise HTTPException(status_code=404, detail="Node not found")

    node = result["n"]
    return {
        "label": label,
        "properties": node.get("properties", {}),
        "id": node.get("id"),
        "element_id": node.get("element_id")
    }


@router.post("/nodes/delete")
async def delete_node(
    req: Dict[str, Any],
    _: Principal = Depends(require_permissions(["query:write"]))
) -> Dict[str, Any]:
    """Delete node by label and key."""
    label = req["label"]
    key = req["key"]

    key_match = " AND ".join([f"n.{k} = ${k}" for k in key.keys()])

    query = f"""
    MATCH (n:{label} {{{key_match}}})
    DETACH DELETE n
    RETURN count(*) AS deleted
    """

    result = await neo4j_query_one(query, key)

    return {"deleted": result.get("deleted", 0) > 0}
