"""Registry router for schema registry and proposal management endpoints."""

from __future__ import annotations

import asyncio
import json as _json
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import StreamingResponse

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    UpsertNodeSchemaRequest,
    UpsertRelationshipSchemaRequest,
    NodeSchemaInfo,
    RelationshipSchemaInfo,
    SchemaVersionInfo,
    SchemaProposalRequest,
    SchemaProposalResponse,
    ProposalApprovalStatusResponse,
    ApproveProposalRequest,
)
from .dependencies import (
    neo4j_query_one,
    neo4j_query_all,
    neo4j_tx,
    approval_threshold_for_async,
)

router = APIRouter(prefix="/v1/registry", tags=["schema_registry"])


# --- Node Schema ---

@router.post("/nodes", response_model=NodeSchemaInfo)
async def upsert_node_schema(
    req: UpsertNodeSchemaRequest,
    _: Principal = Depends(require_permissions(["schemas:write"])),
    principal: Principal = Depends(get_principal),
):
    """Create or update a node schema with versioning."""
    spec = req.spec.dict()
    name = spec["label"]
    description = spec.get("description")

    # If version not provided, compute next integer version
    if req.version is None:
        rec = await neo4j_query_one(
            """
            MATCH (ns:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion)
            RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1
            """,
            {"name": name},
        )
        next_version = str(int(rec["v"]["properties"]["version"]) + 1) if rec else "1"
    else:
        next_version = req.version

    # If provided version exists, raise conflict
    if req.version is not None:
        exists = await neo4j_query_one(
            """
            MATCH (:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion {version: $version})
            RETURN v
            """,
            {"name": name, "version": req.version},
        )
        if exists:
            raise HTTPException(status_code=409, detail="Version already exists for node schema")

    stmts = [
        {
            "query": (
                "MERGE (ns:RegisteredNodeSchema {name: $name}) "
                "ON CREATE SET ns.created_at = datetime(), ns.description = $description "
                "ON MATCH SET ns.description = coalesce($description, ns.description) "
                "WITH ns "
                "CREATE (ver:SchemaVersion {version: $version, spec: $spec, created_at: datetime()}) "
                "CREATE (ns)-[:HAS_VERSION]->(ver) "
                "RETURN ns {.*, id: elementId(ns)} AS ns, ver {.*, id: elementId(ver)} AS ver"
            ),
            "parameters": {"name": name, "description": description, "version": next_version, "spec": spec},
        }
    ]
    res = await neo4j_tx(stmts)

    # Optionally attach to Org if present
    if principal.org:
        try:
            await neo4j_query_one(
                "MATCH (o:Org {id: $org_id}), (ns:RegisteredNodeSchema {name: $name}) MERGE (o)-[:OWNS]->(ns) RETURN 1 AS ok",
                {"org_id": principal.org, "name": name},
            )
        except HTTPException:
            pass

    # Normalize response
    node = res[0][0]["ns"]
    versions_records = await neo4j_query_all(
        "MATCH (ns:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v",
        {"name": name},
    )
    versions: List[SchemaVersionInfo] = []
    for r in versions_records:
        v = r["v"]
        props = v.get("properties", {})
        versions.append(
            SchemaVersionInfo(
                version=str(props.get("version")),
                spec=props.get("spec", {}),
                created_at=props.get("created_at"),
                id=v.get("element_id") or str(v.get("id")),
            )
        )
    return NodeSchemaInfo(name=node["properties"].get("name", name), description=node["properties"].get("description"), versions=versions)


@router.get("/nodes/{name}", response_model=NodeSchemaInfo)
async def get_node_schema(name: str, _: Principal = Depends(require_permissions(["schemas:read"]))):
    """Get a node schema by name with all versions."""
    ns_rec = await neo4j_query_one(
        "MATCH (ns:RegisteredNodeSchema {name: $name}) RETURN ns",
        {"name": name},
    )
    if not ns_rec:
        raise HTTPException(status_code=404, detail="Node schema not found")
    node = ns_rec["ns"]
    versions_records = await neo4j_query_all(
        "MATCH (:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v",
        {"name": name},
    )
    versions: List[SchemaVersionInfo] = []
    for r in versions_records:
        v = r["v"]
        props = v.get("properties", {})
        versions.append(
            SchemaVersionInfo(
                version=str(props.get("version")),
                spec=props.get("spec", {}),
                created_at=props.get("created_at"),
                id=v.get("element_id") or str(v.get("id")),
            )
        )
    return NodeSchemaInfo(name=node["properties"].get("name", name), description=node["properties"].get("description"), versions=versions)


# --- Relationship Schema ---

@router.post("/relationships", response_model=RelationshipSchemaInfo)
async def upsert_relationship_schema(
    req: UpsertRelationshipSchemaRequest,
    _: Principal = Depends(require_permissions(["schemas:write"])),
    principal: Principal = Depends(get_principal),
):
    """Create or update a relationship schema with versioning."""
    spec = req.spec.dict()
    rtype = spec["type"]
    description = spec.get("description")

    if req.version is None:
        rec = await neo4j_query_one(
            """
            MATCH (rs:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion)
            RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1
            """,
            {"type": rtype},
        )
        next_version = str(int(rec["v"]["properties"]["version"]) + 1) if rec else "1"
    else:
        next_version = req.version

    if req.version is not None:
        exists = await neo4j_query_one(
            "MATCH (:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion {version: $version}) RETURN v",
            {"type": rtype, "version": req.version},
        )
        if exists:
            raise HTTPException(status_code=409, detail="Version already exists for relationship schema")

    stmts = [
        {
            "query": (
                "MERGE (rs:RegisteredRelationshipSchema {type: $type}) "
                "ON CREATE SET rs.created_at = datetime(), rs.description = $description "
                "ON MATCH SET rs.description = coalesce($description, rs.description) "
                "WITH rs "
                "CREATE (ver:SchemaVersion {version: $version, spec: $spec, created_at: datetime()}) "
                "CREATE (rs)-[:HAS_VERSION]->(ver) "
                "RETURN rs {.*, id: elementId(rs)} AS rs, ver {.*, id: elementId(ver)} AS ver"
            ),
            "parameters": {"type": rtype, "description": description, "version": next_version, "spec": spec},
        }
    ]
    res = await neo4j_tx(stmts)
    if principal.org:
        try:
            await neo4j_query_one(
                "MATCH (o:Org {id: $org_id}), (rs:RegisteredRelationshipSchema {type: $type}) MERGE (o)-[:OWNS]->(rs) RETURN 1 AS ok",
                {"org_id": principal.org, "type": rtype},
            )
        except HTTPException:
            pass
    relnode = res[0][0]["rs"]
    versions_records = await neo4j_query_all(
        "MATCH (rs:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v",
        {"type": rtype},
    )
    versions: List[SchemaVersionInfo] = []
    for r in versions_records:
        v = r["v"]
        props = v.get("properties", {})
        versions.append(
            SchemaVersionInfo(
                version=str(props.get("version")),
                spec=props.get("spec", {}),
                created_at=props.get("created_at"),
                id=v.get("element_id") or str(v.get("id")),
            )
        )
    return RelationshipSchemaInfo(type=relnode["properties"].get("type", rtype), description=relnode["properties"].get("description"), versions=versions)


@router.get("/relationships/{rtype}", response_model=RelationshipSchemaInfo)
async def get_relationship_schema(rtype: str, _: Principal = Depends(require_permissions(["schemas:read"]))):
    """Get a relationship schema by type with all versions."""
    rs_rec = await neo4j_query_one(
        "MATCH (rs:RegisteredRelationshipSchema {type: $type}) RETURN rs",
        {"type": rtype},
    )
    if not rs_rec:
        raise HTTPException(status_code=404, detail="Relationship schema not found")
    node = rs_rec["rs"]
    versions_records = await neo4j_query_all(
        "MATCH (:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v",
        {"type": rtype},
    )
    versions: List[SchemaVersionInfo] = []
    for r in versions_records:
        v = r["v"]
        props = v.get("properties", {})
        versions.append(
            SchemaVersionInfo(
                version=str(props.get("version")),
                spec=props.get("spec", {}),
                created_at=props.get("created_at"),
                id=v.get("element_id") or str(v.get("id")),
            )
        )
    return RelationshipSchemaInfo(type=node["properties"].get("type", rtype), description=node["properties"].get("description"), versions=versions)


# --- Schema Proposals ---

@router.post("/proposals", response_model=SchemaProposalResponse)
async def create_proposal(
    req: SchemaProposalRequest,
    _: Principal = Depends(require_permissions(["proposals:create"])),
    principal: Principal = Depends(get_principal),
):
    """Create a new schema change proposal."""
    rec = await neo4j_query_one(
        """
        CREATE (p:SchemaProposal {
            id: toString(randomUUID()), target_kind: $target_kind, target_name: $target_name,
            change_type: $change_type, spec: $spec, patch: $patch, rationale: $rationale,
            status: 'Proposed', created_at: datetime(), updated_at: datetime()})
        RETURN p
        """,
        req.dict(),
    )
    p = rec["p"]["properties"]

    # Attach to org and creator if available
    try:
        if principal.org:
            await neo4j_query_one(
                "MATCH (p:SchemaProposal {id: $id}) MERGE (o:Org {id: $org_id}) MERGE (o)-[:HAS_PROPOSAL]->(p) RETURN 1 AS ok",
                {"id": p.get("id"), "org_id": principal.org},
            )
        if principal.sub:
            await neo4j_query_one(
                "MATCH (p:SchemaProposal {id: $id}) MERGE (u:User {sub: $sub}) ON CREATE SET u.created_at=datetime() MERGE (u)-[:CREATED]->(p) RETURN 1 AS ok",
                {"id": p.get("id"), "sub": principal.sub},
            )
    except HTTPException:
        pass

    return SchemaProposalResponse(**{k: p.get(k) for k in SchemaProposalResponse.__fields__.keys()})


@router.get("/proposals/{pid}", response_model=SchemaProposalResponse)
async def get_proposal(pid: str, _: Principal = Depends(require_permissions(["proposals:read"]))):
    """Get a schema proposal by ID."""
    rec = await neo4j_query_one(
        "MATCH (p:SchemaProposal {id: $id}) RETURN p",
        {"id": pid},
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = rec["p"]["properties"]
    return SchemaProposalResponse(**{k: p.get(k) for k in SchemaProposalResponse.__fields__.keys()})


@router.post("/proposals/{pid}/validate", response_model=SchemaProposalResponse)
async def validate_proposal(pid: str, _: Principal = Depends(require_permissions(["proposals:review"]))):
    """Validate a schema proposal."""
    rec = await neo4j_query_one(
        """
        MATCH (p:SchemaProposal {id: $id})
        SET p.status = 'Validated', p.validation = {ok: true}, p.updated_at = datetime()
        RETURN p
        """,
        {"id": pid},
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = rec["p"]["properties"]
    return SchemaProposalResponse(**{k: p.get(k) for k in SchemaProposalResponse.__fields__.keys()})


@router.post("/proposals/{pid}/apply", response_model=SchemaProposalResponse)
async def apply_proposal(pid: str, _: Principal = Depends(require_permissions(["proposals:apply"]))):
    """Apply a validated schema proposal."""
    # Fetch proposal
    rec = await neo4j_query_one("MATCH (p:SchemaProposal {id: $id}) RETURN p", {"id": pid})
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = rec["p"]["properties"]
    change = p.get("change_type")
    target_kind = p.get("target_kind")
    target_name = p.get("target_name")
    spec = p.get("spec") or {}

    # Enforce approval thresholds based on policy
    org_rec = await neo4j_query_one(
        "MATCH (o:Org)-[:HAS_PROPOSAL]->(:SchemaProposal {id: $id}) RETURN o.id AS org_id",
        {"id": pid},
    )
    org_id = (org_rec.get("org_id") if org_rec else None) or None
    required = await approval_threshold_for_async(change, org_id)
    if required > 0:
        cnt_rec = await neo4j_query_one(
            "MATCH (:SchemaProposal {id: $id})<-[:APPROVED]-(:User) RETURN count(*) AS n",
            {"id": pid},
        )
        approvals_current = int((cnt_rec.get("n") if cnt_rec else 0) or 0)
        if approvals_current < required:
            raise HTTPException(status_code=409, detail=f"Not enough approvals: {approvals_current}/{required}")

    if change in ("add_node_schema", "create_node_schema") and target_kind == "node":
        # Create or bump version for node schema
        upsert_req = UpsertNodeSchemaRequest(spec=spec, version=None)  # type: ignore[arg-type]
        _ = await upsert_node_schema(upsert_req)
        # Link proposal to version created
        await neo4j_query_one(
            """
            MATCH (p:SchemaProposal {id: $id}), (ns:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion)
            WITH p, v ORDER BY toInteger(v.version) DESC LIMIT 1
            MERGE (p)-[:INTRODUCES]->(v)
            SET p.status='Applied', p.updated_at = datetime()
            RETURN p
            """,
            {"id": pid, "name": target_name},
        )
    elif change in ("add_relationship_schema", "create_relationship_schema") and target_kind == "relationship":
        upsert_req = UpsertRelationshipSchemaRequest(spec=spec, version=None)  # type: ignore[arg-type]
        _ = await upsert_relationship_schema(upsert_req)
        await neo4j_query_one(
            """
            MATCH (p:SchemaProposal {id: $id}), (rs:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion)
            WITH p, v ORDER BY toInteger(v.version) DESC LIMIT 1
            MERGE (p)-[:INTRODUCES]->(v)
            SET p.status='Applied', p.updated_at = datetime()
            RETURN p
            """,
            {"id": pid, "type": target_name},
        )
    elif change in ("add_property",):
        # Add property to latest version of target
        prop = spec.get("property")
        if not prop or not isinstance(prop, dict) or not prop.get("name"):
            raise HTTPException(status_code=400, detail="spec.property with name is required for add_property")
        if target_kind == "node":
            latest = await neo4j_query_one(
                "MATCH (:RegisteredNodeSchema {name: $name})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1",
                {"name": target_name},
            )
        else:
            latest = await neo4j_query_one(
                "MATCH (:RegisteredRelationshipSchema {type: $type})-[:HAS_VERSION]->(v:SchemaVersion) RETURN v ORDER BY toInteger(v.version) DESC LIMIT 1",
                {"type": target_name},
            )
        if not latest:
            raise HTTPException(status_code=404, detail="Target schema not found")
        vprops = latest["v"]["properties"]
        cur_spec = vprops.get("spec", {})
        key = "properties"
        plist = list(cur_spec.get(key, []))
        # Avoid duplicates by name
        if any(p0.get("name") == prop.get("name") for p0 in plist):
            raise HTTPException(status_code=409, detail="Property already exists")
        plist.append(prop)
        cur_spec[key] = plist
        next_version = str(int(vprops.get("version", "0")) + 1)
        if target_kind == "node":
            await neo4j_query_one(
                """
                MATCH (ns:RegisteredNodeSchema {name: $name})
                CREATE (ver:SchemaVersion {version: $version, spec: $spec, created_at: datetime()})
                CREATE (ns)-[:HAS_VERSION]->(ver)
                RETURN ver
                """,
                {"name": target_name, "version": next_version, "spec": cur_spec},
            )
        else:
            await neo4j_query_one(
                """
                MATCH (rs:RegisteredRelationshipSchema {type: $type})
                CREATE (ver:SchemaVersion {version: $version, spec: $spec, created_at: datetime()})
                CREATE (rs)-[:HAS_VERSION]->(ver)
                RETURN ver
                """,
                {"type": target_name, "version": next_version, "spec": cur_spec},
            )
        await neo4j_query_one(
            "MATCH (p:SchemaProposal {id: $id}) SET p.status='Applied', p.updated_at = datetime() RETURN p",
            {"id": pid},
        )
    else:
        # Unsupported operation; mark as Rejected
        await neo4j_query_one(
            "MATCH (p:SchemaProposal {id: $id}) SET p.status='Rejected', p.updated_at = datetime(), p.validation = {ok: false, reason: 'Unsupported change_type'} RETURN p",
            {"id": pid},
        )

    # Return fresh proposal state
    return await get_proposal(pid)


@router.get("/proposals/{pid}/stream")
async def proposal_stream(pid: str, _: Principal = Depends(require_permissions(["proposals:read"]))):
    """Stream proposal status updates as Server-Sent Events."""
    async def event_stream():
        try:
            while True:
                rec = await get_proposal(pid)
                payload = {"status": rec.status, "updated_at": rec.updated_at}
                yield f"event: status\n"
                yield f"data: {_json.dumps(payload)}\n\n"
                if rec.status in ("Applied", "Rejected"):
                    break
                yield ": ping\n\n"
                await asyncio.sleep(5)
        except Exception as exc:
            err = {"error": str(exc)}
            yield "event: error\n"
            yield f"data: {_json.dumps(err)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@router.get("/proposals/{pid}/approvals", response_model=ProposalApprovalStatusResponse)
async def get_proposal_approvals(pid: str, principal: Principal = Depends(get_principal)):
    """Get approval status for a proposal."""
    rec = await neo4j_query_one("MATCH (p:SchemaProposal {id: $id}) RETURN p", {"id": pid})
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = rec["p"]["properties"]
    change = p.get("change_type")
    org_rec = await neo4j_query_one(
        "MATCH (o:Org)-[:HAS_PROPOSAL]->(:SchemaProposal {id: $id}) RETURN o.id AS org_id",
        {"id": pid},
    )
    org_id = (org_rec.get("org_id") if org_rec else None) or principal.org
    required = await approval_threshold_for_async(change, org_id)
    cnt_rec = await neo4j_query_one(
        "MATCH (:SchemaProposal {id: $id})<-[:APPROVED]-(:User) RETURN count(*) AS n",
        {"id": pid},
    )
    approvals_current = int((cnt_rec.get("n") if cnt_rec else 0) or 0)
    return ProposalApprovalStatusResponse(proposal_id=pid, approvals_current=approvals_current, approvals_required=required)


@router.post("/proposals/{pid}/approve", response_model=ProposalApprovalStatusResponse)
async def approve_proposal(
    pid: str,
    _: Principal = Depends(require_permissions(["proposals:review"])),
    principal: Principal = Depends(get_principal),
    req: Optional[ApproveProposalRequest] = None,
):
    """Approve a schema proposal."""
    # Ensure proposal exists
    rec = await neo4j_query_one("MATCH (p:SchemaProposal {id: $id}) RETURN p", {"id": pid})
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")

    # Record approval (one per user)
    await neo4j_query_one(
        """
        MATCH (p:SchemaProposal {id: $id})
        MERGE (u:User {sub: $sub})
        ON CREATE SET u.created_at = datetime()
        MERGE (u)-[a:APPROVED]->(p)
        SET a.ts = datetime(), a.comment = $comment
        RETURN 1 AS ok
        """,
        {"id": pid, "sub": principal.sub, "comment": (req.comment if req else None)},
    )
    return await get_proposal_approvals(pid)
