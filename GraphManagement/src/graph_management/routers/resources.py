"""Resources router for tools, datasets, secrets, and data contracts."""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    ToolCreateRequest,
    ToolResponse,
    ToolListResponse,
    DatasetCreateRequest,
    DatasetResponse,
    DatasetListResponse,
    DatasetTagsUpdateRequest,
    SecretCreateRequest,
    SecretResponse,
    SecretListResponse,
    DataContractCreateRequest,
    DataContractResponse,
    TagListResponse,
)
from .dependencies import (
    neo4j_query_one,
    neo4j_query_all,
    tool_response_from_props,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(prefix="/v1", tags=["resources"])


# --- Tools ---

@router.post("/tools", response_model=ToolResponse)
async def create_tool(
    req: ToolCreateRequest,
    principal: Principal = Depends(get_principal),
    _rbac: None = Depends(require_permissions_effective(["tools:write"])),
    _abac: None = Depends(enforce_abac("tools:write")),
):
    """Create or update a tool."""
    org_id = req.org_id or principal.org
    params = {"id": req.id, "name": req.name, "kind": req.kind, "provider": req.provider, "metadata": req.metadata or {}, "org_id": org_id}
    rec = await neo4j_query_one(
        """
        MERGE (t:Tool {id: $id})
        ON CREATE SET t.name=$name, t.kind=$kind, t.provider=$provider, t.metadata=$metadata, t.created_at=datetime()
        ON MATCH SET t.name=coalesce($name, t.name), t.kind=coalesce($kind, t.kind), t.provider=coalesce($provider, t.provider), t.metadata=coalesce($metadata, t.metadata), t.updated_at=datetime()
        WITH t, $org_id AS oid
        CALL {
            WITH t, oid
            WITH t, oid WHERE oid IS NOT NULL
            MATCH (o:Org {id: oid})
            MERGE (o)-[:OWNS]->(t)
        }
        RETURN t
        """,
        params,
    )
    t = rec["t"]["properties"]
    return tool_response_from_props(t)


@router.get("/orgs/{org_id}/tools", response_model=ToolListResponse)
async def list_tools(org_id: str, _rbac: None = Depends(require_permissions_effective(["tools:read"], org_param="org_id"))):
    """List all tools in an organization."""
    rows = await neo4j_query_all("MATCH (:Org {id: $org_id})-[:OWNS]->(t:Tool) RETURN t", {"org_id": org_id})
    tools: List[ToolResponse] = []
    for r in rows:
        t = r["t"]["properties"]
        tools.append(tool_response_from_props(t))
    return ToolListResponse(tools=tools)


@router.get("/tools/{tool_id}", response_model=ToolResponse)
async def get_tool(tool_id: str, _rbac: None = Depends(require_permissions_effective(["tools:read"]))):
    """Get a specific tool by ID."""
    rec = await neo4j_query_one("MATCH (t:Tool {id: $id}) RETURN t", {"id": tool_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Tool not found")
    t = rec["t"]["properties"]
    return tool_response_from_props(t)


# --- Datasets ---

@router.post("/datasets", response_model=DatasetResponse)
async def create_dataset(
    req: DatasetCreateRequest,
    principal: Principal = Depends(get_principal),
    _rbac: None = Depends(require_permissions_effective(["datasets:write"])),
    _abac: None = Depends(enforce_abac("datasets:write")),
):
    """Create or update a dataset."""
    org_id = req.org_id or principal.org
    rec = await neo4j_query_one(
        """
        MERGE (d:Dataset {id: $id})
        ON CREATE SET d.name=$name, d.system=$system, d.path=$path, d.tags=$tags, d.contract_id=$contract_id, d.created_at=datetime()
        ON MATCH SET d.name=coalesce($name, d.name), d.system=coalesce($system, d.system), d.path=coalesce($path, d.path), d.tags=coalesce($tags, d.tags), d.contract_id=coalesce($contract_id, d.contract_id), d.updated_at=datetime()
        WITH d, $org_id AS oid, $contract_id AS cid
        CALL {
            WITH d, oid
            WITH d, oid WHERE oid IS NOT NULL
            MATCH (o:Org {id: oid})
            MERGE (o)-[:OWNS]->(d)
        }
        CALL {
            WITH d, cid
            WITH d, cid WHERE cid IS NOT NULL
            MERGE (dc:DataContract {id: cid})
            MERGE (d)-[:HAS_CONTRACT]->(dc)
        }
        RETURN d
        """,
        {
            "id": req.id,
            "name": req.name,
            "system": req.system,
            "path": req.path,
            "tags": req.tags,
            "contract_id": req.contract_id,
            "org_id": org_id,
        },
    )
    d = rec["d"]["properties"]
    # Materialize Tag nodes and relationships
    if req.tags:
        await neo4j_query_one(
            """
            MATCH (d:Dataset {id: $id})
            UNWIND $tags AS tname
            MERGE (t:Tag {name: tname})
            MERGE (d)-[:TAGGED_WITH]->(t)
            RETURN 1 AS ok
            """,
            {"id": req.id, "tags": req.tags},
        )
    return DatasetResponse(id=d.get("id"), name=d.get("name"), system=d.get("system"), path=d.get("path"), tags=req.tags or d.get("tags") or [], contract_id=d.get("contract_id"))


@router.get("/orgs/{org_id}/datasets", response_model=DatasetListResponse)
async def list_datasets(org_id: str, _: Principal = Depends(require_permissions(["datasets:read"]))):
    """List all datasets in an organization."""
    rows = await neo4j_query_all("MATCH (:Org {id: $org_id})-[:OWNS]->(d:Dataset) RETURN d", {"org_id": org_id})
    datasets: List[DatasetResponse] = []
    for r in rows:
        d = r["d"]["properties"]
        datasets.append(DatasetResponse(id=d.get("id"), name=d.get("name"), system=d.get("system"), path=d.get("path"), tags=d.get("tags") or [], contract_id=d.get("contract_id")))
    return DatasetListResponse(datasets=datasets)


@router.get("/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: str, _: Principal = Depends(require_permissions(["datasets:read"]))):
    """Get a specific dataset by ID."""
    rec = await neo4j_query_one("MATCH (d:Dataset {id: $id}) RETURN d", {"id": dataset_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Dataset not found")
    d = rec["d"]["properties"]
    return DatasetResponse(id=d.get("id"), name=d.get("name"), system=d.get("system"), path=d.get("path"), tags=d.get("tags") or [], contract_id=d.get("contract_id"))


@router.post("/datasets/{dataset_id}/tags", response_model=DatasetResponse)
async def update_dataset_tags(
    dataset_id: str,
    req: DatasetTagsUpdateRequest,
    _rbac: None = Depends(require_permissions_effective(["datasets:write"])),
    _abac: None = Depends(enforce_abac("datasets:write")),
):
    """Update tags for a dataset."""
    # Set property and sync Tag nodes/edges
    rec = await neo4j_query_one(
        "MATCH (d:Dataset {id: $id}) SET d.tags = $tags, d.updated_at = datetime() RETURN d",
        {"id": dataset_id, "tags": req.tags},
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Dataset not found")
    # Ensure Tag nodes and edges
    await neo4j_query_one(
        """
        MATCH (d:Dataset {id: $id})
        OPTIONAL MATCH (d)-[old:TAGGED_WITH]->(:Tag) DELETE old
        WITH d
        UNWIND $tags AS tname
        MERGE (t:Tag {name: tname})
        MERGE (d)-[:TAGGED_WITH]->(t)
        RETURN 1 AS ok
        """,
        {"id": dataset_id, "tags": req.tags},
    )
    d = rec["d"]["properties"]
    return DatasetResponse(id=d.get("id"), name=d.get("name"), system=d.get("system"), path=d.get("path"), tags=req.tags or [], contract_id=d.get("contract_id"))


# --- Secrets ---

@router.post("/secrets", response_model=SecretResponse)
async def create_secret(
    req: SecretCreateRequest,
    principal: Principal = Depends(get_principal),
    _rbac: None = Depends(require_permissions_effective(["secrets:write"])),
    _abac: None = Depends(enforce_abac("secrets:write")),
):
    """Create or update a secret."""
    org_id = req.org_id or principal.org
    rec = await neo4j_query_one(
        """
        MERGE (s:Secret {id: $id})
        ON CREATE SET s.provider=$provider, s.key=$key, s.description=$description, s.created_at=datetime()
        ON MATCH SET s.provider=coalesce($provider, s.provider), s.key=coalesce($key, s.key), s.description=coalesce($description, s.description), s.updated_at=datetime()
        WITH s, $org_id AS oid
        CALL {
            WITH s, oid
            WITH s, oid WHERE oid IS NOT NULL
            MATCH (o:Org {id: oid})
            MERGE (o)-[:OWNS]->(s)
        }
        RETURN s
        """,
        {"id": req.id, "provider": req.provider, "key": req.key, "description": req.description, "org_id": org_id},
    )
    s = rec["s"]["properties"]
    return SecretResponse(id=s.get("id"), provider=s.get("provider"), key=s.get("key"), description=s.get("description"))


@router.get("/orgs/{org_id}/secrets", response_model=SecretListResponse)
async def list_secrets(org_id: str, _rbac: None = Depends(require_permissions_effective(["secrets:read"], org_param="org_id"))):
    """List all secrets in an organization."""
    rows = await neo4j_query_all("MATCH (:Org {id: $org_id})-[:OWNS]->(s:Secret) RETURN s", {"org_id": org_id})
    secrets: List[SecretResponse] = []
    for r in rows:
        s = r["s"]["properties"]
        secrets.append(SecretResponse(id=s.get("id"), provider=s.get("provider"), key=s.get("key"), description=s.get("description")))
    return SecretListResponse(secrets=secrets)


@router.get("/secrets/{secret_id}", response_model=SecretResponse)
async def get_secret(secret_id: str, _: Principal = Depends(require_permissions(["secrets:read"]))):
    """Get a specific secret by ID."""
    rec = await neo4j_query_one("MATCH (s:Secret {id: $id}) RETURN s", {"id": secret_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Secret not found")
    s = rec["s"]["properties"]
    return SecretResponse(id=s.get("id"), provider=s.get("provider"), key=s.get("key"), description=s.get("description"))


# --- Tags ---

@router.get("/tags", response_model=TagListResponse)
async def list_tags(_: Principal = Depends(require_permissions(["datasets:read"]))):
    """List all available tags."""
    rows = await neo4j_query_all("MATCH (t:Tag) RETURN t.name AS name", {})
    tags = [r.get("name") for r in rows if r.get("name")]
    return TagListResponse(tags=tags)


# --- Data Contracts ---

@router.post("/contracts", response_model=DataContractResponse)
async def create_data_contract(
    req: DataContractCreateRequest,
    _rbac: None = Depends(require_permissions_effective(["datasets:write"])),
    _abac: None = Depends(enforce_abac("datasets:write")),
):
    """Create or update a data contract."""
    rec = await neo4j_query_one(
        """
        MERGE (dc:DataContract {id: $id})
        ON CREATE SET dc.name = $name, dc.schema = $schema, dc.rules = $rules, dc.created_at = datetime()
        ON MATCH SET dc.name = coalesce($name, dc.name), dc.schema = coalesce($schema, dc.schema), dc.rules = coalesce($rules, dc.rules), dc.updated_at = datetime()
        RETURN dc
        """,
        {"id": req.id, "name": req.name, "schema": req.schema_, "rules": req.rules},
    )
    dc = rec["dc"]["properties"]
    return DataContractResponse(id=dc.get("id"), name=dc.get("name"), schema_=dc.get("schema") or {}, rules=dc.get("rules") or {})


@router.get("/contracts/{contract_id}", response_model=DataContractResponse)
async def get_data_contract(contract_id: str, _: Principal = Depends(require_permissions(["datasets:read"]))):
    """Get a specific data contract by ID."""
    rec = await neo4j_query_one("MATCH (dc:DataContract {id: $id}) RETURN dc", {"id": contract_id})
    if not rec:
        raise HTTPException(status_code=404, detail="Data contract not found")
    dc = rec["dc"]["properties"]
    return DataContractResponse(id=dc.get("id"), name=dc.get("name"), schema_=dc.get("schema") or {}, rules=dc.get("rules") or {})
