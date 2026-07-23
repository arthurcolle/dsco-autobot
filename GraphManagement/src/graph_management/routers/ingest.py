"""Ingest router for data ingestion endpoints."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    IngestNodeRequest,
    IngestRelationshipRequest,
    NodeResponse,
    RelationshipResponse,
)
from .dependencies import (
    neo4j_query_one,
    sanitize_label,
    latest_node_spec,
    latest_rel_spec,
    required_props,
    unique_keys,
    consume_capability,
)

router = APIRouter(prefix="/v1/ingest", tags=["data_ingestion"])


@router.post("/nodes", response_model=NodeResponse)
async def ingest_node(
    req: IngestNodeRequest,
    _: Principal = Depends(require_permissions(["query:write", "schemas:read"])),
    principal: Principal = Depends(get_principal),
):
    """Ingest a node into the graph with schema validation."""
    label = sanitize_label(req.label)
    spec = await latest_node_spec(label)

    # Validate required fields
    missing = [k for k in required_props(spec) if k not in req.properties or req.properties.get(k) is None]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required properties: {missing}")

    # Capability check & budget consume
    await consume_capability(principal, kind="writer", target_label=label)

    # Build Cypher with safe label literal
    props = req.properties
    if req.if_element_id:
        # Replace semantics on existing node
        query = (
            f"MATCH (n) WHERE elementId(n) = $eid AND '{label}' IN labels(n) "
            f"SET n = $props "
            f"RETURN id(n) AS id, elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties"
        )
        rec = await neo4j_query_one(query, {"eid": req.if_element_id, "props": props})
        if not rec:
            raise HTTPException(status_code=404, detail="Target node not found or label mismatch")
    else:
        # Use first available unique key if present for MERGE, else CREATE
        uniq = unique_keys(spec)
        merge_key = next((k for k in uniq if k in props and props.get(k) is not None), None)
        if merge_key:
            # Dynamic key; build a small dict with only merge key
            key_val = props.get(merge_key)
            query = (
                f"MERGE (n:`{label}` {{`{merge_key}`: $key_val}}) "
                f"SET n += $props "
                f"RETURN id(n) AS id, elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties"
            )
            rec = await neo4j_query_one(query, {"key_val": key_val, "props": props})
        else:
            query = (
                f"CREATE (n:`{label}`) "
                f"SET n = $props "
                f"RETURN id(n) AS id, elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties"
            )
            rec = await neo4j_query_one(query, {"props": props})

    return NodeResponse(
        id=rec.get("id"),
        element_id=rec.get("element_id"),
        labels=rec.get("labels") or [label],
        properties=rec.get("properties") or {},
    )


@router.post("/relationships", response_model=RelationshipResponse)
async def ingest_relationship(
    req: IngestRelationshipRequest,
    _: Principal = Depends(require_permissions(["query:write", "schemas:read"])),
    principal: Principal = Depends(get_principal),
):
    """Ingest a relationship into the graph with schema validation."""
    rtype = sanitize_label(req.relationship_type)
    spec = await latest_rel_spec(rtype)
    missing = [k for k in required_props(spec) if k not in req.properties or req.properties.get(k) is None]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required properties: {missing}")

    # Capability check & budget consume
    await consume_capability(principal, kind="linker", relationship_type=rtype)

    # Resolve endpoints
    params: Dict[str, Any] = {"props": req.properties}
    match_a = ""
    match_b = ""
    if req.start_id is not None and req.end_id is not None:
        match_a = "MATCH (a) WHERE id(a) = $sid "
        match_b = "MATCH (b) WHERE id(b) = $eid "
        params.update({"sid": req.start_id, "eid": req.end_id})
    elif req.start_element_id and req.end_element_id:
        match_a = "MATCH (a) WHERE elementId(a) = $seid "
        match_b = "MATCH (b) WHERE elementId(b) = $eeid "
        params.update({"seid": req.start_element_id, "eeid": req.end_element_id})
    else:
        raise HTTPException(status_code=400, detail="Provide start_id/end_id or start_element_id/end_element_id")

    # Domain/range enforcement
    start_labels = spec.get("start_labels") or []
    end_labels = spec.get("end_labels") or []
    params.update({"start_labels": start_labels, "end_labels": end_labels})

    if req.if_element_id:
        # Replace properties on existing rel; also validate endpoints and type match
        query = (
            f"{match_a}{match_b}"
            "WITH a,b "
            "WHERE any(l IN labels(a) WHERE l IN $start_labels) AND any(l IN labels(b) WHERE l IN $end_labels) "
            "MATCH (a)-[r]->(b) WHERE elementId(r) = $rid AND type(r) = $rtype "
            "SET r = $props "
            "RETURN id(r) AS id, elementId(r) AS element_id, type(r) AS type, id(startNode(r)) AS sid, id(endNode(r)) AS eid, properties(r) AS properties"
        )
        params.update({"rid": req.if_element_id, "rtype": rtype})
        rec = await neo4j_query_one(query, params)
        if not rec:
            raise HTTPException(status_code=404, detail="Relationship not found or endpoint/type mismatch")
    else:
        query = (
            f"{match_a}{match_b}"
            "WITH a,b "
            "WHERE any(l IN labels(a) WHERE l IN $start_labels) AND any(l IN labels(b) WHERE l IN $end_labels) "
            f"MERGE (a)-[r:`{rtype}`]->(b) "
            "SET r += $props "
            "RETURN id(r) AS id, elementId(r) AS element_id, type(r) AS type, id(startNode(r)) AS sid, id(endNode(r)) AS eid, properties(r) AS properties"
        )
        rec = await neo4j_query_one(query, params)
        if not rec:
            raise HTTPException(status_code=400, detail="Failed to create relationship; domain/range may be invalid")

    return RelationshipResponse(
        id=rec.get("id"),
        element_id=rec.get("element_id"),
        type=rec.get("type"),
        start_node_id=rec.get("sid"),
        end_node_id=rec.get("eid"),
        properties=rec.get("properties") or {},
    )
