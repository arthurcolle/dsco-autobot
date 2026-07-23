from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict


class Statement(BaseModel):
    query: str
    parameters: Dict[str, Any] = Field(default_factory=dict)


class TransactionRequest(BaseModel):
    statements: List[Statement]


class QueryRequest(BaseModel):
    query: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    strategy: Literal["default", "one", "all"] = "default"


class QueryResponse(BaseModel):
    results: List[Any]


class SingleResultResponse(BaseModel):
    result: Any


class SchemaResponse(BaseModel):
    labels: List[str]
    relationshipTypes: List[str]
    propertyKeys: List[str]


class IndexInfo(BaseModel):
    name: str
    type: Optional[str] = None
    entityType: Optional[str] = None
    labelsOrTypes: Optional[List[str]] = None
    properties: Optional[List[str]] = None
    options: Optional[Dict[str, Any]] = None


class IndexListResponse(BaseModel):
    indexes: List[IndexInfo]


class ConstraintInfo(BaseModel):
    name: str
    type: Optional[str] = None
    entityType: Optional[str] = None
    labelsOrTypes: Optional[List[str]] = None
    properties: Optional[List[str]] = None
    options: Optional[Dict[str, Any]] = None


class ConstraintListResponse(BaseModel):
    constraints: List[ConstraintInfo]


# Optional domain-lite models for overlay operations
class NodeModel(BaseModel):
    label: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class NodeResponse(BaseModel):
    id: Optional[int] = None
    element_id: Optional[str] = None
    labels: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)


class RelationshipModel(BaseModel):
    start_id: int
    end_id: int
    relationship_type: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class RelationshipResponse(BaseModel):
    id: Optional[int] = None
    element_id: Optional[str] = None
    type: Optional[str] = None
    start_node_id: Optional[int] = None
    end_node_id: Optional[int] = None
    properties: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------
# Registry & Proposals (Overlay)
# -----------------------------


class PropertyDef(BaseModel):
    name: str
    type: str = "string"
    required: bool = False
    unique: bool = False
    indexed: bool = False
    default: Optional[Any] = None
    enum: Optional[List[Any]] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    notes: Optional[str] = None


class NodeSchemaSpec(BaseModel):
    label: str
    description: Optional[str] = None
    properties: List[PropertyDef] = Field(default_factory=list)
    indexes: List[Dict[str, Any]] = Field(default_factory=list)
    constraints: List[Dict[str, Any]] = Field(default_factory=list)
    embedding: Optional[List[float]] = None


class RelationshipSchemaSpec(BaseModel):
    type: str
    direction: Literal["outgoing", "incoming", "undirected"] = "outgoing"
    start_labels: List[str] = Field(default_factory=list)
    end_labels: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    properties: List[PropertyDef] = Field(default_factory=list)
    indexes: List[Dict[str, Any]] = Field(default_factory=list)
    constraints: List[Dict[str, Any]] = Field(default_factory=list)
    embedding: Optional[List[float]] = None


class SchemaVersionInfo(BaseModel):
    version: str
    spec: Dict[str, Any]
    created_at: Optional[str] = None
    id: Optional[str] = None


class NodeSchemaInfo(BaseModel):
    name: str
    description: Optional[str] = None
    versions: List[SchemaVersionInfo]


class RelationshipSchemaInfo(BaseModel):
    type: str
    description: Optional[str] = None
    versions: List[SchemaVersionInfo]


class UpsertNodeSchemaRequest(BaseModel):
    spec: NodeSchemaSpec
    version: Optional[str] = None


class UpsertRelationshipSchemaRequest(BaseModel):
    spec: RelationshipSchemaSpec
    version: Optional[str] = None


class SchemaProposalRequest(BaseModel):
    target_kind: Literal["node", "relationship"]
    target_name: str
    change_type: str  # e.g., add_node_schema, modify_node_schema, add_property, etc.
    spec: Optional[Dict[str, Any]] = None
    patch: Optional[Dict[str, Any]] = None
    rationale: Optional[str] = None


class SchemaProposalResponse(BaseModel):
    id: str
    status: str
    target_kind: str
    target_name: str
    change_type: str
    spec: Optional[Dict[str, Any]] = None
    patch: Optional[Dict[str, Any]] = None
    rationale: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    validation: Optional[Dict[str, Any]] = None


class PolicyResponse(BaseModel):
    mode: Literal["observe", "warn", "enforce"]


# Extended policy models (scoped policies and approvals)
class PolicyUpsertRequest(BaseModel):
    scope_type: Literal["global", "org", "env", "project"] = "global"
    scope_id: Optional[str] = None
    mode: Optional[Literal["observe", "warn", "enforce"]] = None
    approvals: Dict[str, int] = Field(default_factory=dict)  # e.g., {"add_node_schema": 1, "add_property": 1}
    rules: List[Dict[str, Any]] = Field(default_factory=list)


class PolicyDetailResponse(BaseModel):
    scope_type: Literal["global", "org", "env", "project"]
    scope_id: Optional[str] = None
    mode: Literal["observe", "warn", "enforce"]
    approvals: Dict[str, int] = Field(default_factory=dict)


# -----------------------------
# Data Model Bootstrap
# -----------------------------


class BootstrapRequest(BaseModel):
    org_id: str = "acme"
    org_name: str = "Acme"
    seed_rbac: bool = True
    seed_envs: bool = True


class BootstrapResponse(BaseModel):
    constraints_applied: int
    permissions_created: int
    roles_created: int
    environments_created: int
    org_id: str


class ModelStatusResponse(BaseModel):
    constraints: int
    indexes: int
    roles: int
    permissions: int
    environments: int


# Approvals for proposals
class ApproveProposalRequest(BaseModel):
    comment: Optional[str] = None


class ProposalApprovalStatusResponse(BaseModel):
    proposal_id: str
    approvals_current: int
    approvals_required: int


# -----------------------------
# Ingest (typed runtime instances)
# -----------------------------


class IngestNodeRequest(BaseModel):
    """Create or replace a node instance validated against a registered schema.

    - label: registered node schema name
    - properties: full property map (replace semantics on update)
    - if_element_id: optional elementId() to enforce optimistic concurrency/replace
    """

    label: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    if_element_id: Optional[str] = None


class IngestRelationshipRequest(BaseModel):
    """Create or replace a relationship instance validated against a registered schema.

    - relationship_type: registered relationship schema type name
    - start_id / end_id: numeric Neo4j ids of endpoints (preferred)
    - start_element_id / end_element_id: alternative endpoint addressing
    - properties: full property map (replace semantics on update when if_element_id provided)
    - if_element_id: optional elementId() to replace an existing relationship
    """

    relationship_type: str
    start_id: Optional[int] = None
    end_id: Optional[int] = None
    start_element_id: Optional[str] = None
    end_element_id: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    if_element_id: Optional[str] = None


# -----------------------------
# Capabilities (fine-grained write/link budgets)
# -----------------------------


class CapabilityTokenCreateRequest(BaseModel):
    """Mint a capability token bound to a subject (principal.sub).

    - kind: 'writer' (node instances) or 'linker' (relationships)
    - target_label: for writer tokens (node label)
    - relationship_type: for linker tokens (relationship type)
    - version: optional schema version hint (informational)
    - budget_per_day: max allowed uses per UTC day; 0 or negative = unlimited
    - expires_at: ISO datetime string; null means no expiry
    - subject: defaults to requester principal.sub if omitted
    - note: optional description
    """

    kind: Literal["writer", "linker"]
    target_label: Optional[str] = None
    relationship_type: Optional[str] = None
    version: Optional[str] = None
    budget_per_day: int = 0
    expires_at: Optional[str] = None
    subject: Optional[str] = None
    note: Optional[str] = None


class CapabilityTokenResponse(BaseModel):
    id: str
    kind: Literal["writer", "linker"]
    target_label: Optional[str] = None
    relationship_type: Optional[str] = None
    version: Optional[str] = None
    subject: str
    budget_per_day: int = 0
    used_today: int = 0
    day_key: Optional[str] = None
    expires_at: Optional[str] = None
    note: Optional[str] = None


# -----------------------------
# Identity, Teams, Projects
# -----------------------------


class TeamCreateRequest(BaseModel):
    id: str
    name: str
    description: Optional[str] = None


class TeamResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None


class TeamListResponse(BaseModel):
    teams: List[TeamResponse]


class ProjectCreateRequest(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    env_ids: Optional[List[str]] = None


class ProjectResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None


class ProjectListResponse(BaseModel):
    projects: List[ProjectResponse]


# -----------------------------
# Resources: Tools, Datasets, Secrets, Agents
# -----------------------------


class ToolCreateRequest(BaseModel):
    id: str
    name: str = Field(..., max_length=64)
    kind: Optional[str] = None
    provider: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    org_id: Optional[str] = None


class ToolResponse(BaseModel):
    id: str
    name: str = Field(..., max_length=64)
    kind: Optional[str] = None
    provider: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ToolListResponse(BaseModel):
    tools: List[ToolResponse]


class DatasetCreateRequest(BaseModel):
    id: str
    name: str
    system: Optional[str] = None
    path: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    contract_id: Optional[str] = None
    org_id: Optional[str] = None


class DatasetResponse(BaseModel):
    id: str
    name: str
    system: Optional[str] = None
    path: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    contract_id: Optional[str] = None


class DatasetListResponse(BaseModel):
    datasets: List[DatasetResponse]


class DatasetTagsUpdateRequest(BaseModel):
    tags: List[str] = Field(default_factory=list)


class SecretCreateRequest(BaseModel):
    id: str
    provider: str
    key: str
    description: Optional[str] = None
    org_id: Optional[str] = None


class SecretResponse(BaseModel):
    id: str
    provider: str
    key: str
    description: Optional[str] = None


class SecretListResponse(BaseModel):
    secrets: List[SecretResponse]


class AgentRegisterRequest(BaseModel):
    id: str
    name: Optional[str] = None
    project_id: Optional[str] = None
    team_id: Optional[str] = None
    capabilities: List[str] = Field(default_factory=list)
    org_id: Optional[str] = None


class AgentResponse(BaseModel):
    id: str
    name: Optional[str] = None
    capabilities: List[str] = Field(default_factory=list)


class AgentListResponse(BaseModel):
    agents: List[AgentResponse]


class RoleAssignmentRequest(BaseModel):
    principal_type: Literal["user", "agent"]
    principal_id: str  # sub for user, id for agent
    role: str
    org_id: Optional[str] = None
    project_id: Optional[str] = None


class RoleAssignmentResponse(BaseModel):
    principal_type: str
    principal_id: str
    role: str
    scope: Optional[str] = None


# -----------------------------
# Runtime: Runs and Events
# -----------------------------


class RunCreateRequest(BaseModel):
    id: str
    env_id: str
    actor_type: Literal["user", "agent"]
    actor_id: str
    org_id: Optional[str] = None
    project_id: Optional[str] = None
    inputs: Dict[str, Any] = Field(default_factory=dict)


class RunEventRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class RunFinishRequest(BaseModel):
    status: Literal["succeeded", "failed", "canceled"]
    outputs: Dict[str, Any] = Field(default_factory=dict)


class RunResponse(BaseModel):
    id: str
    env_id: str
    status: str
    actor_type: str
    actor_id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# -----------------------------
# Data Contracts and Tags
# -----------------------------


class DataContractCreateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    schema_: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    rules: Dict[str, Any] = Field(default_factory=dict)


class DataContractResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    schema_: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    rules: Dict[str, Any] = Field(default_factory=dict)


class TagListResponse(BaseModel):
    tags: List[str]


# -----------------------------
# ABAC Policy Evaluation
# -----------------------------


class ABACEvalRequest(BaseModel):
    action: str  # e.g., "query:write", "schemas:write"
    resource: Dict[str, Any] = Field(default_factory=dict)
    env_id: Optional[str] = None
    project_id: Optional[str] = None


class ABACEvalResponse(BaseModel):
    decision: Literal["allow", "deny"]
    matched: Optional[Dict[str, Any]] = None
    scope_applied: Optional[str] = None  # e.g., org:acme, env:prod, global


# -----------------------------
# Budgets and Quotas
# -----------------------------


class BudgetUpsertRequest(BaseModel):
    scope_type: Literal["org", "project", "env"]
    scope_id: str
    period: Literal["monthly", "rolling30d"] = "monthly"
    limit: float


class BudgetResponse(BaseModel):
    id: str
    scope_type: str
    scope_id: str
    period: str
    limit: float


class BudgetStatusResponse(BaseModel):
    id: str
    usage: float
    limit: float
    remaining: float
    window: str


# -----------------------------
# Auth Effective (graph-derived roles/perms)
# -----------------------------


class AuthEffectiveResponse(BaseModel):
    roles: List[str]
    permissions: List[str]
