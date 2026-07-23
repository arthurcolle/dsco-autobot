# GraphManagement Access Control and Policy Reference

Authoritative reference for roles, permissions, policies (RBAC + ABAC), proposal approvals, and capability tokens (budgeted writes) in the GraphManagement overlay.

This document is source-of-truth for product copy, API docs, and engineering onboarding. All examples assume the GraphManagement ASGI app is running and fronted by a base URL shown as `https://graph-mgmt.example.com`.

## 1) Principals and Authentication

- Principal fields
  - `sub`: subject (user/service id)
  - `roles`: list of role names
  - `org`: optional organization id (scoping)
  - `scopes`: optional OAuth scopes (informational)
- Auth modes (env-driven)
  - `disabled` (default): dev-friendly, injects `developer` role
  - `api_key`: JSON map in `AUTH_API_KEYS` env (`{"<key>": {"sub":..., "roles":[...], "org":"..."}}`). Clients must send the key via `Authorization: Bearer <key>` (preferred) or the legacy `X-API-Key` header.
  - `jwt`: HS256 via `AUTH_JWT_SECRET`; roles from `roles|role`, scopes from `scope|scopes`, optional `org`

Example API key configuration:

```bash
# Auto-generate org_admin/developer/readonly/service keys and append to .env
python scripts/generate_auth_defaults.py --write-dotenv .auth.env --append-dotenv
set -a && source .auth.env && set +a

# Scenario presets (solo | small-team | enterprise). Combine with --no-default-plans to use only the preset.
python scripts/generate_auth_defaults.py --preset solo --no-default-plans --write-dotenv .auth.env
python scripts/generate_auth_defaults.py --preset enterprise --no-default-plans --write-dotenv .auth.env

# Custom mix: label[:subject][:org][#scope+scope]=roles, repeat per tenant
python scripts/generate_auth_defaults.py \
  --plan admin=org_admin \
  --plan dashboard:dash:acme=readonly \
  --plan ingest:@svc-ingest:factory#writer:company=service \
  --no-default-plans

curl -H "Authorization: Bearer <printed-admin-key>" http://localhost:8000/health
```

## 2) Static RBAC: Roles → Permissions

Source: `src/graph_management/security.py` (`_ROLE_PERMISSIONS`)

- `org_admin`
  - `[*]` (all atomics)
- `schema_admin`
  - `schemas:*`, `proposals:*`, `policy:*`, `query:*`, `orgs:*`, `teams:*`, `projects:*`, `tools:*`, `datasets:*`, `secrets:*`, `agents:*`, `runs:*`, `budgets:*`, `events:*`
- `schema_approver`
  - `schemas:read`, `proposals:review`, `proposals:apply`
- `schema_reviewer`
  - `schemas:read`, `proposals:review`
- `developer`
  - `schemas:read`, `proposals:create`, `query:read`, `query:write`, `tools:read`, `datasets:read`, `runs:write`, `runs:read`, `events:read`
- `readonly`
  - `schemas:read`, `query:read`, `tools:read`, `datasets:read`, `runs:read`, `events:read`
- `service`
  - `schemas:read`, `query:*`, `tools:read`, `datasets:read`, `runs:*`

Helpers
- `require_permissions([...])`: dependency that validates static permissions
- `has_permission(principal, permission)`: boolean check using `_ROLE_PERMISSIONS`

## 3) Effective RBAC: Graph-Derived Roles/Permissions

Source: `src/graph_management/api.py` (`_effective_roles_perms`)

- Model
  - `(:User {sub})-[:HAS_ROLE {scope?}]->(:Role)-[:GRANTS]->(:Permission)`
  - Optional `HAS_ROLE.scope` may be `org:{org_id}` or `project:{project_id}`
- Resolution
  - `_effective_roles_perms(principal, org_id, project_id)` returns roles and perms by graph; if absent, falls back to static `_ROLE_GRANTS` expansion
  - Cache TTL: 30 seconds per `(sub|org|project)` tuple
- Enforcement
  - `require_permissions_effective(required, org_param?, project_param?)`: denies if required not in effective perms at current scope

## 4) Atomic Permissions and Role Grants

Source: `src/graph_management/api.py` (`_ATOMIC_PERMISSIONS`, `_ROLE_GRANTS`)

- Atomics (`_ATOMIC_PERMISSIONS`)
  - Full enumerated list:
    - `schemas:read`, `schemas:write`
    - `proposals:create`, `proposals:read`, `proposals:review`, `proposals:apply`
    - `policy:read`, `policy:write`
    - `query:read`, `query:write`
    - `orgs:read`, `orgs:write`
    - `teams:read`, `teams:write`
    - `projects:read`, `projects:write`
    - `tools:read`, `tools:write`
    - `datasets:read`, `datasets:write`
    - `secrets:read`, `secrets:write`
    - `agents:read`, `agents:write`
    - `runs:read`, `runs:write`
    - `budgets:read`, `budgets:write`
    - `events:read`
- Role grants (`_ROLE_GRANTS`)
  - Maps role → patterns (e.g., `schemas:*`, `query:*`, explicit atomics, or `*`)
  - Helper `_expand_grants` expands `cat:*` to the full atomic set for that category

## 5) ABAC Policy (Allow/Deny Rules + Mode)

Policy node
- `(:Policy {id, mode, approvals, rules})`
  - `id`: `global` | `org:{org_id}` | `env:{env_id}` | `project:{project_id}`
  - `mode`: `observe` | `warn` | `enforce` (default `observe`)
  - `approvals`: `{ change_type -> integer }` thresholds for proposals
  - `rules`: array of allow/deny rules (see below)

Rule schema (elements inside `Policy.rules`)
- `effect`: `"deny" | "allow"`
- `actions`: list of action strings you pass to `enforce_abac(action)`
- Optional scopes: `roles`, `env_ids`, `projects`

Rule JSON shape (informal)
```json
{
  "effect": "allow" | "deny",
  "actions": ["query:write", "runs:read"],
  "roles": ["developer", "service"],
  "env_ids": ["dev", "stage"],
  "projects": ["proj-123"]
}
```

Evaluation & precedence
- Lookup policies by precedence: `project > env > org > global`
- Deny overrides: if any `deny` rule matches → deny
- Allow: if any `allow` rule matches → allow
- Default: highest-precedence `mode` decides (`enforce` → deny, otherwise allow)

Endpoints
- Upsert policy: `POST /v1/policies/upsert` (policy:write)
  - Body: `PolicyUpsertRequest { scope_type, scope_id?, mode?, approvals{}, rules[] }`
- Get global policy: `GET /v1/policies/global` (policy:read)
- Get scoped policy: `GET /v1/policies/{scope_type}/{scope_id}` (policy:read)
- Convenience (mode only): `GET /v1/policy`, `POST /v1/policy`

Example (project-level ABAC)
```json
POST /v1/policies/upsert
{
  "scope_type": "project",
  "scope_id": "proj-123",
  "mode": "enforce",
  "approvals": {"add_node_schema": 2},
  "rules": [
    {"effect":"deny", "actions":["runs:delete"], "roles":["developer"]},
    {"effect":"allow", "actions":["query:write"], "roles":["service"]}
  ]
}
```

Error semantics
- Missing effective permission → `403 Forbidden`
- Deny rule matched → `403 Forbidden`
- No rule matched and highest-precedence mode = `enforce` → `403 Forbidden`
- Other errors (validation) → `422 Unprocessable Entity`

## 6) Proposal Approval Policy (Counts per Change Type)

- Thresholds
  - `_approval_threshold_for_async(change_type, org_id)` reads `Policy.approvals[change_type]` at `org:{org}` then `global`; default `1`
- Flow
  - Create: `POST /v1/registry/proposals`
  - Validate: `POST /v1/registry/proposals/{pid}/validate` (proposals:review)
  - Approve: `POST /v1/registry/proposals/{pid}/approve` (proposals:review)
  - Check approvals: `GET /v1/registry/proposals/{pid}/approvals`
  - Apply: `POST /v1/registry/proposals/{pid}/apply` (proposals:apply)
    - Enforces `approvals_current >= approvals_required`

## 7) Capability Tokens (Writer/Linker with Budgets)

Use capability tokens to gate runtime writes even when RBAC permits querying.

Token node
- `(:CapabilityToken {id, sub, kind, target_label?, relationship_type?, version?, budget_per_day, used_today, day_key, expires_at?, created_at, updated_at?, note?})`

Kinds
- `writer`: permits node instance ingestion for `target_label`
- `linker`: permits relationship ingestion for `relationship_type`

Budgets & expiry
- `budget_per_day <= 0` → unlimited
- `used_today` resets when `day_key` (UTC date) changes
- `expires_at` blocks use after expiration

Endpoints
- Mint: `POST /v1/capabilities` (policy:write)
  - Body: `CapabilityTokenCreateRequest { kind, target_label?|relationship_type?, version?, budget_per_day, expires_at?, subject?, note? }`
- Get: `GET /v1/capabilities/{id}` (policy:read)

Enforcement & consumption
- `_consume_capability(principal, kind, target_label?, relationship_type?)` ensures a valid token exists, resets `used_today` per UTC day, and increments if a finite budget
- Ingest requires a token:
  - Nodes: `POST /v1/ingest/nodes` → consumes `writer` for label
  - Relationships: `POST /v1/ingest/relationships` → consumes `linker` for type

Example
```json
POST /v1/capabilities
{
  "kind": "writer",
  "target_label": "Company",
  "budget_per_day": 100,
  "subject": "svc-indexer-01",
  "note": "Indexer writes up to 100 companies/day"
}
```

## 8) Ingest (Typed Runtime Instances)

Nodes: `POST /v1/ingest/nodes`
- Body: `{ label, properties, if_element_id? }`
- Validates against latest `RegisteredNodeSchema` (required fields); replace semantics with `if_element_id`
- If unique property exists in schema and present in payload, MERGE by that property; otherwise CREATE then SET properties

Sanitization
- Labels and relationship types must match `^[A-Za-z_][A-Za-z0-9_]*$` and are backtick-quoted into Cypher; invalid identifiers → `400 Bad Request`.

Errors
- Missing required properties → `400 Bad Request`
- Capability missing or budget exhausted → `403 Forbidden`
- Relationship endpoint/type mismatch or invalid domain/range → `404 Not Found` (replace mode) or `400 Bad Request` (create)

Relationships: `POST /v1/ingest/relationships`
- Body: `{ relationship_type, properties, start_id&end_id | start_element_id&end_element_id, if_element_id? }`
- Validates required properties and domain/range labels against `RegisteredRelationshipSchema.start_labels/end_labels`
- MERGE by type and endpoints; replace properties with `if_element_id`

Both require capability tokens (writer/linker), plus base permissions `query:write` and `schemas:read`.

## 9) Endpoints Summary

- Schema Registry
  - `POST /v1/registry/nodes` → upsert node schema/version
  - `GET /v1/registry/nodes/{name}` → get node schema
  - `POST /v1/registry/relationships` → upsert relationship schema/version
  - `GET /v1/registry/relationships/{type}` → get relationship schema
- Proposals
  - `POST /v1/registry/proposals` → create
  - `GET /v1/registry/proposals/{pid}` → read
  - `POST /v1/registry/proposals/{pid}/validate` → mark validated
  - `POST /v1/registry/proposals/{pid}/approve` → record approval
  - `GET /v1/registry/proposals/{pid}/approvals` → current/required
  - `POST /v1/registry/proposals/{pid}/apply` → apply
- Policies
  - `POST /v1/policies/upsert` → upsert scoped policy
  - `GET /v1/policies/global` / `GET /v1/policies/{scope_type}/{scope_id}` → read
  - `GET /v1/policy` / `POST /v1/policy` → global mode convenience
- Capabilities
  - `POST /v1/capabilities` / `GET /v1/capabilities/{id}`
- Ingest
  - `POST /v1/ingest/nodes`
  - `POST /v1/ingest/relationships`

Permissions per endpoint (static RBAC)
- `POST /v1/registry/nodes`, `/v1/registry/relationships` → `schemas:write`
- `GET /v1/registry/nodes/{name}`, `/v1/registry/relationships/{type}` → `schemas:read`
- `POST /v1/registry/proposals` → `proposals:create`
- `POST /v1/registry/proposals/{pid}/validate` → `proposals:review`
- `POST /v1/registry/proposals/{pid}/approve` → `proposals:review`
- `POST /v1/registry/proposals/{pid}/apply` → `proposals:apply`
- `GET /v1/policies/*` → `policy:read`; `POST /v1/policies/upsert` → `policy:write`
- `GET /v1/policy`, `POST /v1/policy` → `policy:read` / `policy:write`
- `POST /v1/capabilities` → `policy:write`; `GET /v1/capabilities/{id}` → `policy:read`
- `POST /v1/ingest/*` → `query:write` + `schemas:read` (+ capability token)

## 10) Common Flows

Strict Typed Ingest (no new schemas)
1. Approver configures policy mode and approvals
2. Admin mints capability tokens for services (writer/linker)
3. Services call ingest endpoints; tokens consume budgets; unknown types rejected

Queue New Schemas (governed)
1. Service extracts structure from text; calls `/v1/registry/proposals` for unknown types
2. Approvers review → validate → approve until threshold
3. Apply proposal → new schema version registered → services ingest instances

Auto Mode (fast-track; limited to safe envs)
1. Global/project policy sets approvals to `0` for additive changes
2. Service submits proposals and immediately applies (or bypasses proposals via pre-approved pipeline)
3. Provisional writes gated by capability budgets and ABAC rules

## 11) Curl Cookbook (Auth: Bearer <TOKEN>)

Set global mode to enforce
```bash
curl -X POST https://graph-mgmt.example.com/v1/policy \
  -H 'Authorization: Bearer $TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"mode":"enforce"}'
```

Upsert project policy with approvals and rules
```bash
curl -X POST https://graph-mgmt.example.com/v1/policies/upsert \
  -H 'Authorization: Bearer $TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "scope_type":"project",
    "scope_id":"proj-123",
    "mode":"enforce",
    "approvals": {"add_node_schema": 2},
    "rules": [
      {"effect":"deny","actions":["runs:delete"],"roles":["developer"]},
      {"effect":"allow","actions":["query:write"],"roles":["service"]}
    ]
  }'
```

Mint a writer capability token
```bash
curl -X POST https://graph-mgmt.example.com/v1/capabilities \
  -H 'Authorization: Bearer $TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "kind":"writer",
    "target_label":"Company",
    "budget_per_day": 100,
    "subject": "svc-indexer-01",
    "note": "Indexer writes up to 100 companies/day"
  }'
```

Ingest a Company node
```bash
curl -X POST https://graph-mgmt.example.com/v1/ingest/nodes \
  -H 'Authorization: Bearer $TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "Company",
    "properties": {"id":"acme-1","name":"Acme Robotics","country":"US"}
  }'
```

Propose a new node schema and apply (if approvals allow)
```bash
PID=$(curl -s -X POST https://graph-mgmt.example.com/v1/registry/proposals \
  -H 'Authorization: Bearer $TOKEN' -H 'Content-Type: application/json' \
  -d '{
    "target_kind":"node",
    "target_name":"Round",
    "change_type":"add_node_schema",
    "spec":{"label":"Round","properties":[{"name":"id","type":"string","required":true}]},
    "rationale":"From ingestion pipeline"
  }' | jq -r .id)

curl -X POST https://graph-mgmt.example.com/v1/registry/proposals/$PID/apply \
  -H 'Authorization: Bearer $TOKEN'
```

## 12) Schema Spec Examples

NodeSchemaSpec
```json
{
  "label": "Company",
  "description": "Organization entity",
  "properties": [
    {"name": "id", "type": "string", "required": true, "unique": true, "indexed": true},
    {"name": "name", "type": "string", "required": true},
    {"name": "country", "type": "string"}
  ],
  "indexes": [{"name": "company_id_idx", "properties": ["id"]}],
  "constraints": []
}
```

RelationshipSchemaSpec
```json
{
  "type": "LED",
  "direction": "outgoing",
  "start_labels": ["Company"],
  "end_labels": ["Round"],
  "properties": [
    {"name": "role", "type": "string"}
  ]
}
```

## 13) Endpoint‑by‑Endpoint Reference

The table lists method, path, purpose, required static permissions, effective/ABAC notes, capability tokens (if any), idempotency, and typical errors.

- GET /health
  - Purpose: liveness/probe
  - Perms: none
  - Idempotent: yes
  - Errors: 500 (internal)

- GET /v1/schema, /v1/indexes, /v1/constraints
  - Purpose: proxy to backend metadata
  - Perms: `schemas:read`
  - Idempotent: yes
  - Errors: 401/403/5xx

- POST /v1/query, /v1/tx, /v1/query/stream, /v1/query/ws
  - Purpose: query execution/transactions/streaming
  - Perms: `query:read` (read‑only), `query:write` (mutations)
  - ABAC: you may wrap these with `enforce_abac("query:write")`
  - Idempotent: depends on Cypher
  - Errors: 4xx/5xx from backend

- POST /v1/registry/nodes
  - Purpose: upsert RegisteredNodeSchema version
  - Perms: `schemas:write`
  - Idempotent: versioned; 409 if version exists
  - Errors: 400 (invalid spec), 409 (exists)

- GET /v1/registry/nodes/{name}
  - Purpose: fetch schema + versions
  - Perms: `schemas:read`

- POST /v1/registry/relationships; GET /v1/registry/relationships/{type}
  - Purpose: upsert/fetch RegisteredRelationshipSchema
  - Perms: `schemas:write` (POST), `schemas:read` (GET)

- POST /v1/registry/proposals
  - Purpose: create SchemaProposal
  - Perms: `proposals:create`
  - Idempotent: no; returns `id`

- GET /v1/registry/proposals/{pid}
  - Perms: `proposals:read`

- POST /v1/registry/proposals/{pid}/validate
  - Purpose: minimal validation
  - Perms: `proposals:review`

- POST /v1/registry/proposals/{pid}/approve
  - Purpose: record an approval by current principal
  - Perms: `proposals:review`

- GET /v1/registry/proposals/{pid}/approvals
  - Purpose: current/required approvals (per Policy.approvals)
  - Perms: `proposals:read`

- POST /v1/registry/proposals/{pid}/apply
  - Purpose: apply a proposal into registry (creates/bump versions)
  - Perms: `proposals:apply`
  - Precondition: approvals_current ≥ approvals_required
  - Errors: 409 if not enough approvals

- POST /v1/policies/upsert
  - Purpose: create/update Policy at global/org/env/project
  - Perms: `policy:write`

- GET /v1/policies/global, GET /v1/policies/{scope_type}/{scope_id}
  - Purpose: fetch Policy
  - Perms: `policy:read`

- GET /v1/policy, POST /v1/policy
  - Purpose: fetch/set only global `mode`
  - Perms: `policy:read` / `policy:write`

- POST /v1/capabilities, GET /v1/capabilities/{id}
  - Purpose: mint/get CapabilityToken
  - Perms: `policy:write` (POST), `policy:read` (GET)

- POST /v1/ingest/nodes
  - Purpose: typed node ingest with replace semantics
  - Perms: `query:write` + `schemas:read`
  - Capability: Writer[label] token (budgeted)
  - Idempotent: with `if_element_id` yes; otherwise depends on schema unique key MERGE
  - Errors: 400 (missing require/sanitization), 403 (capability), 404 (if_element_id not found)

- POST /v1/ingest/relationships
  - Purpose: typed relationship ingest with replace semantics
  - Perms: `query:write` + `schemas:read`
  - Capability: Linker[type] token (budgeted)
  - Errors: 400 (domain/range/required), 403 (capability), 404 (replace target not found)

## 14) Roles and Permissions Table

| Role            | Summary                                            | Notable Grants                     |
|-----------------|----------------------------------------------------|------------------------------------|
| org_admin       | Superuser                                          | `*`                                |
| schema_admin    | Manage schemas, proposals, policy & queries        | `schemas:*`, `proposals:*`, `policy:*`, `query:*` |
| schema_approver | Review/apply proposals                             | `proposals:review`, `proposals:apply` |
| schema_reviewer | Review proposals                                   | `proposals:review`                 |
| developer       | Propose, query read/write                          | `proposals:create`, `query:read`, `query:write` |
| readonly        | Read-only access                                   | `schemas:read`, `query:read`       |
| service         | Service workloads (broad query & runs)             | `query:*`, `runs:*`                |

## 15) ABAC Rule Recipes

- Deny destructive actions in production
```json
{"effect":"deny","actions":["runs:delete"],"env_ids":["prod"]}
```

- Allow query:write to service role in dev/stage
```json
{"effect":"allow","actions":["query:write"],"roles":["service"],"env_ids":["dev","stage"]}
```

- Require two approvals for `add_node_schema` at org scope
```json
// Policy.approvals
{"add_node_schema": 2}
```

## 16) Capability Token Design Details

- Windowing: budgets reset on UTC day (`date()`); `day_key` updated on first use each day
- Unlimited: `budget_per_day <= 0` skips increment (still checks expiry)
- Expiry: ISO string (e.g., `2025-12-31T23:59:59Z`) → disabled when reached
- Revocation: delete the CapabilityToken node; subsequent `_consume_capability` fails
- Suggested scopes (extensions): add `start_label`/`end_label` on linker; enforce both endpoint labels; add `version` checks when strict versioning is required

## 17) Auditing & Observability

- Decisions: ABAC allow/deny decisions recorded via `_record_policy_decision` (action, decision, principal, scope, reason)
- Proposals: creator, org linkage, approvals (relationship from `(:User)-[:APPROVED]->(:SchemaProposal)`)
- Capabilities: counters (`used_today`/`day_key`), creation timestamps, optional `expires_at`
- Ingest: validate errors surface with HTTP codes; you can add server logs on validation failures and capability misses

## 18) Multi‑Tenancy and Scoping

- Effective RBAC scopes by `HAS_ROLE.scope ∈ { org:{org_id}, project:{project_id} }`
- Policies link via `(:Org|:Project|:Environment)-[:HAS_POLICY]->(:Policy {id: ...})`
- Precedence: project > environment > org > global
- Role assignment (example):
```cypher
MERGE (u:User {sub:$sub})
MERGE (r:Role {name:'schema_approver'})
MERGE (u)-[:HAS_ROLE {scope:'org:acme'}]->(r)
MERGE (r)-[:GRANTS]->(:Permission {name:'proposals:review'})
MERGE (r)-[:GRANTS]->(:Permission {name:'proposals:apply'})
```

## 19) Security Best Practices

- Always set global policy `mode` to `enforce` in production
- Restrict minting of capability tokens to `schema_admin` (`policy:write`)
- Separate dev/stage/prod environments; apply ABAC rules to deny risky actions in prod
- Issue short‑lived JWTs for services; rotate API keys if `api_key` mode is used
- Sanitize input labels/types (already enforced); treat user‑provided Cypher carefully (parameters only)

## 20) Governance Workflows

- Strict ingest: only known schemas; unknown → 404/400; tokens + ABAC enforce runtime
- Queue mode: unknown → `SchemaProposal`; approvers validate → approve → apply; then ingest
- Auto mode: approvals for additive changes set to 0; proposal → immediate apply (limit to dev/stage)

## 21) Error Catalog

- 400 Bad Request
  - Missing required properties; invalid label/type; invalid relationship endpoints in create path; invalid policy payload
- 401 Unauthorized
  - Missing/invalid API key or JWT
- 403 Forbidden
  - Missing static/effective permission; ABAC deny; capability missing or budget exhausted
- 404 Not Found
  - Target entity for replace not found; schema not found
- 409 Conflict
  - Version exists; not enough approvals to apply
- 422 Unprocessable Entity
  - Request validation errors

## 22) Glossary

- Atomic permission: indivisible `cat:verb` string (e.g., `schemas:read`)
- Effective RBAC: permissions resolved from Role/Permission graph for a scope
- ABAC: attribute‑based access control with allow/deny rules and `mode`
- Capability token: budgeted writer/linker grant for runtime ingest
- Registered schema: versioned node/relationship schema specs
- Proposal: pending schema change; must be applied to take effect

## 23) End‑to‑End Sequences

- “Add new type & ingest instances”
  1. Developer creates proposal `add_node_schema` for `Round`
  2. Approvers validate; two approvers approve (org policy requires 2)
  3. Approver applies; registry now has `Round` v1
  4. Admin mints `writer:Round` token for `svc-indexer-02`
  5. Service calls `/v1/ingest/nodes` with `label=Round` and required properties

- “Guard dev vs prod with ABAC”
  1. Set global mode `observe`; project‐prod policy `mode: enforce`
  2. Rules deny `query:write` in prod, allow it in dev for `service`
  3. Same endpoint behaves differently by environment

## 24) Future Extensions & Open Questions

- Compile schema specs to JSON‑Schema and enforce types/units/enums
- Linker capabilities with `(start_label,end_label)` scoping and counts per edge type
- Versioned capabilities (Writer[T@v]) tied to specific schema versions
- Rate limiting per principal beyond capability budgets
- Audit log streaming via WebSocket for policy decisions and proposals

## 25) Bootstrap Defaults

When you call the model bootstrap endpoint, the overlay ensures base constraints, roles/permissions, and a default global policy.

- Constraints/Indexes: uniqueness for common nodes (Org, Team, Project, Environment, User, Role, Permission, Policy, DataContract, Secret, Tool, Dataset, Agent, Run, Budget, Event)
- Roles and permissions: the system ensures Role and Permission nodes exist (using the enumerated atomics and `_ROLE_GRANTS` patterns)
- Global Policy defaults
  - `id = 'global'`
  - `mode = 'observe'`
  - `approvals = { add_node_schema: 1, add_relationship_schema: 1, add_property: 1 }`

Call: `POST /v1/model/bootstrap`

## 26) Data Contracts (Optional)

Data contracts define schemas and rules for datasets. They can be referenced by pipelines or ingestion services.

- Node: `(:DataContract {id, name, schema, rules, created_at, updated_at?})`
- Endpoints
  - Create/upsert: `POST /v1/contracts` (effective `datasets:write`, ABAC `datasets:write`)
    - Body: `{ id, name, schema: {...}, rules: [...] }`
  - Read: `GET /v1/contracts/{id}` (`datasets:read`)
- Typical usage
  - Define dataset schema for “CompanyIndex” with required columns and value bounds
  - Attach as metadata for ingestion jobs; validate payloads against contract before ingest

## 27) Effective RBAC Resolution: Worked Example

Scenario: `sub=alice`, roles=`[developer]`, org=`acme`, project=`proj-1`.

1. Graph roles fetch
   - MATCH (alice:User {sub:'alice'})-[:HAS_ROLE {scope}]->(r:Role)-[:GRANTS]->(p:Permission)
   - Scopes considered: `org:acme`, `project:proj-1`
   - Suppose none found; fallback triggers
2. Fallback expansion
   - Use `_ROLE_GRANTS` for `developer` → expand `proposals:create`, `query:read`, `query:write`, ...
3. require_permissions_effective(["query:write"]) passes
4. enforce_abac("query:write") evaluated with policies by precedence
   - If project policy denies `query:write` for role `developer`, request is denied regardless of RBAC
   - If no deny; and project mode is `enforce` and no allow rule matches, request denied
   - Else allowed

## 28) Policy Evaluation Algorithm (Pseudocode)

```python
def may(principal, action, env_id=None, org_id=None, project_id=None):
    roles, perms = effective_roles(principal, org_id, project_id)
    if action not in perms and not wildcard_match(action, perms):
        return False  # RBAC fail

    policies = collect_policies(env_id, project_id, org_id)
    # Deny overrides
    for pol in policies:
        for rule in pol.rules:
            if rule.effect == 'deny' and match(rule, principal, action, env_id, project_id):
                return False
    # Allow rules
    for pol in policies:
        for rule in pol.rules:
            if rule.effect == 'allow' and match(rule, principal, action, env_id, project_id):
                return True
    # Default by highest precedence mode
    mode = first_non_null([p.mode for p in policies]) or 'observe'
    return mode != 'enforce'
```

## 29) Testing Matrix & Conformance Checklist

RBAC
- [ ] Each role resolves to expected static permissions (unit test expand)
- [ ] Effective roles/perms honored for scoped role assignments (org/project)

ABAC
- [ ] Deny rules override allows across precedence levels
- [ ] No rule match in `enforce` yields deny; in `observe` yields allow
- [ ] Project > env > org > global precedence enforced

Approvals
- [ ] Proposal apply blocked until approvals_current ≥ required
- [ ] Thresholds taken from org policy if present; else global; else default

Capabilities
- [ ] Writer/Linker tokens consumed per ingest; reset daily
- [ ] Unlimited budgets when `budget_per_day <= 0`
- [ ] Expired tokens rejected

Ingest
- [ ] Node required properties enforced; label sanitization errors on invalid
- [ ] Relationship domain/range enforced; replace mode 404 if rel not found
- [ ] Capability requirement enforced for both endpoints

Security
- [ ] JWT/API key auth; role poisoning prevented; no elevation across scopes
- [ ] Parameterized Cypher only; labels/types sanitized

## 30) Runbooks (Ops)

Emergency freeze
- Set global policy `mode` to `enforce`; add deny rule for sensitive actions (e.g., `query:write`, `runs:*`)

Token exhaustion
- `GET /v1/capabilities/{id}` to inspect `used_today` vs `budget_per_day`
- Mint temporary token with higher budget; schedule rotation

Unauthorized changes suspected
- Pull recent Policy, Proposal, Capability, and Run events; correlate with `principal.sub`
- Revoke tokens (delete nodes) and set deny rules while investigating

## 31) FAQ & Troubleshooting

Q: Why does my ingest fail with 403?
- Missing Writer/Linker capability or budget exhausted; verify token and budgets.

Q: Why does proposal apply return 409?
- Not enough approvals per Policy.approvals for your scope.

Q: Why do I get 404 on relationship replace?
- `if_element_id` refers to a non‑existent relationship between endpoints, or type mismatch.

Q: How do I allow service writes only in dev/stage?
- Upsert a project/env policy with allow rules for `service` role in those envs, deny in prod; set mode to `enforce`.

## 32) Request Model JSON Schemas (Informal)

PolicyUpsertRequest
```json
{
  "type": "object",
  "properties": {
    "scope_type": {"enum": ["global", "org", "env", "project"]},
    "scope_id": {"type": ["string", "null"]},
    "mode": {"enum": ["observe", "warn", "enforce"], "nullable": true},
    "approvals": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
    "rules": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "effect": {"enum": ["allow", "deny"]},
          "actions": {"type": "array", "items": {"type": "string"}},
          "roles": {"type": "array", "items": {"type": "string"}},
          "env_ids": {"type": "array", "items": {"type": "string"}},
          "projects": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["effect", "actions"]
      }
    }
  },
  "required": ["scope_type"]
}
```

CapabilityTokenCreateRequest
```json
{
  "type": "object",
  "properties": {
    "kind": {"enum": ["writer", "linker"]},
    "target_label": {"type": ["string", "null"]},
    "relationship_type": {"type": ["string", "null"]},
    "version": {"type": ["string", "null"]},
    "budget_per_day": {"type": "integer"},
    "expires_at": {"type": ["string", "null"]},
    "subject": {"type": ["string", "null"]},
    "note": {"type": ["string", "null"]}
  },
  "required": ["kind"]
}
```

IngestNodeRequest
```json
{
  "type": "object",
  "properties": {
    "label": {"type": "string"},
    "properties": {"type": "object"},
    "if_element_id": {"type": ["string", "null"]}
  },
  "required": ["label", "properties"]
}
```

IngestRelationshipRequest
```json
{
  "type": "object",
  "properties": {
    "relationship_type": {"type": "string"},
    "properties": {"type": "object"},
    "start_id": {"type": ["integer", "null"]},
    "end_id": {"type": ["integer", "null"]},
    "start_element_id": {"type": ["string", "null"]},
    "end_element_id": {"type": ["string", "null"]},
    "if_element_id": {"type": ["string", "null"]}
  },
  "required": ["relationship_type", "properties"]
}
```

SchemaProposalRequest
```json
{
  "type": "object",
  "properties": {
    "target_kind": {"enum": ["node", "relationship"]},
    "target_name": {"type": "string"},
    "change_type": {"type": "string"},
    "spec": {"type": ["object", "null"]},
    "patch": {"type": ["object", "null"]},
    "rationale": {"type": ["string", "null"]}
  },
  "required": ["target_kind", "target_name", "change_type"]
}
```

## 33) Endpoint JSON Examples

Create policy (project scope)
```http
POST /v1/policies/upsert HTTP/1.1
Authorization: Bearer $TOKEN
Content-Type: application/json

{
  "scope_type": "project",
  "scope_id": "proj-42",
  "mode": "enforce",
  "approvals": {"add_node_schema": 2, "add_property": 1},
  "rules": [
    {"effect":"deny","actions":["runs:delete"],"projects":["proj-42"]},
    {"effect":"allow","actions":["query:write"],"roles":["service"]}
  ]
}
```

Response
```json
{"scope_type":"project","scope_id":"proj-42","mode":"enforce","approvals":{"add_node_schema":2,"add_property":1}}
```

Mint capability token
```http
POST /v1/capabilities HTTP/1.1
Authorization: Bearer $TOKEN
Content-Type: application/json

{
  "kind":"writer",
  "target_label":"Company",
  "budget_per_day": 100,
  "subject": "svc-indexer-01"
}
```

Response
```json
{
  "id":"cap-uuid",
  "kind":"writer",
  "target_label":"Company",
  "relationship_type":null,
  "version":null,
  "subject":"svc-indexer-01",
  "budget_per_day":100,
  "used_today":0,
  "day_key":"2025-11-10",
  "expires_at":null,
  "note":null
}
```

Typed node ingest
```http
POST /v1/ingest/nodes HTTP/1.1
Authorization: Bearer $TOKEN
Content-Type: application/json

{
  "label": "Company",
  "properties": {"id":"acme-1","name":"Acme Robotics","country":"US"}
}
```

Response
```json
{"id":123,"element_id":"n42","labels":["Company"],"properties":{"id":"acme-1","name":"Acme Robotics","country":"US"}}
```

Create & apply schema proposal
```http
POST /v1/registry/proposals HTTP/1.1
Authorization: Bearer $TOKEN
Content-Type: application/json

{
  "target_kind":"node",
  "target_name":"Round",
  "change_type":"add_node_schema",
  "spec":{
    "label":"Round",
    "properties":[
      {"name":"id","type":"string","required":true,"unique":true,"indexed":true},
      {"name":"amount","type":"decimal"},
      {"name":"date","type":"date"}
    ]
  }
}
```

Response
```json
{"id":"prop-uuid","status":"Proposed","target_kind":"node","target_name":"Round","change_type":"add_node_schema"}
```

Apply
```http
POST /v1/registry/proposals/prop-uuid/apply HTTP/1.1
Authorization: Bearer $TOKEN
```

Response
```json
{"id":"prop-uuid","status":"Applied","target_kind":"node","target_name":"Round","change_type":"add_node_schema"}
```

## 34) Personas & Permissions Matrix

| Persona             | Typical Roles            | Allowed Operations (examples)                               |
|---------------------|--------------------------|-------------------------------------------------------------|
| Org Administrator   | org_admin                | All operations                                               |
| Schema Administrator| schema_admin             | Manage schemas/policies/proposals; run queries               |
| Approver            | schema_approver          | Validate/approve/apply proposals                            |
| Reviewer            | schema_reviewer          | Validate/review proposals                                   |
| Developer           | developer                | Create proposals, run queries, view schemas                 |
| Service (Agent)     | service + capability     | Ingest nodes/edges (requires Writer/Linker token), run jobs |
| Read-only Analyst   | readonly                 | Read schemas, query READ                                    |

## 35) Onboarding Playbooks

New project setup
1. Bootstrap model: `POST /v1/model/bootstrap`
2. Create Org/Environment/Project nodes (if needed) and link policies
3. Upsert project policy with `mode: enforce`, approvals, and ABAC rules
4. Register initial schemas (Company, Round, Person, etc.) via `/v1/registry/*`
5. Mint Writer/Linker capabilities for services
6. Configure ingestion pipelines to call `/v1/ingest/*`

Adding a new schema type
1. Developer submits `/v1/registry/proposals` with `add_node_schema`
2. Reviewer validates; approvers approve until threshold
3. Apply proposal; schema version appears under registry
4. Mint Writer token for new label; start ingest

Lockdown mode (incident response)
1. Set global mode `enforce`
2. Add deny rules for risky actions (e.g., `query:write`, `runs:*`) at global/org scope
3. Revoke relevant capabilities; audit recent decisions/events

## 36) Threat Model & Mitigations

Threats
- Token abuse: stolen capability used to write data
- Policy misconfiguration: unintended allows/insufficient approvals
- Injection: untrusted labels/types or ad‑hoc Cypher
- Privilege escalation: role manipulation to gain permissions

Mitigations
- Capability tokens scoped (writer/linker), daily budgets, expiry, revocation
- ABAC defaults to deny under `enforce`; deny overrides; precedence ensures most specific wins
- Sanitization of identifiers; parameterized queries
- Effective RBAC derived from graph roles; static fallback explicit and testable

Monitoring
- Audit decisions for ABAC; track approvals and token consumption; alerts on anomalies

## 37) Observability Event Taxonomy (Suggested)

Event types (source → sink)
- policy.decision (abac): {action, decision, principal, scope, reason, ts}
- capability.mint/consume/revoke: {id, kind, target, sub, delta, used_today, budget, ts}
- proposal.create/validate/approve/apply: {id, actor, counts, change_type, ts}
- ingest.node/relationship: {label/type, ok, error?, principal, capability_id, ts}

## 38) Versioning & Deprecation Policy

- Schema versions are additive by default (v1, v2, ...). Non‑backward‑compatible changes require coordinated migration.
- Policies evaluated immediately upon upsert; maintain change logs with approver IDs.
- Capability tokens are not versioned but may include an informational `version` to indicate intended schema version.

## 39) Rate Limiting & Quotas (Optional)

- Per‑principal rate limiting can be layered on top of capability budgets to prevent bursts.
- Suggested keys: `{sub}`, `{sub|action}`, `{ip}` with sliding windows.

## 40) Multi‑Environment Patterns

- Development: `observe` mode, generous approvals, auto proposals allowed
- Staging: `enforce` mode, production‑like approvals; deny high‑risk actions
- Production: strict `enforce`, higher approvals, limited capability budgets

## 41) Use Cases & Example Playbooks (7×4)

For each use case, we provide four concrete examples (“pieces”):
- RBAC: role assignment/grants (graph or static)
- Policy: ABAC + approvals (PolicyUpsertRequest JSON)
- Capabilities: budgeted Writer/Linker mint JSON
- Ingest: typed ingest request JSON/curl

Naming: replace `$TOKEN` and `$BASE` with your values.

### 41.1 Enterprise SaaS (multi‑tenant production)

- RBAC
```cypher
// Assign schema_admin to ops in org:acme
MERGE (u:User {sub:'ops-1'})
MERGE (r:Role {name:'schema_admin'})
MERGE (u)-[:HAS_ROLE {scope:'org:acme'}]->(r)
```
- Policy (enforce strict; approvals 2+)
```json
{
  "scope_type":"org","scope_id":"acme","mode":"enforce",
  "approvals":{"add_node_schema":2,"add_relationship_schema":2,"add_property":1},
  "rules":[{"effect":"deny","actions":["query:write"],"env_ids":["prod"],"roles":["developer"]}]
}
```
- Capabilities
```json
{"kind":"writer","target_label":"Customer","budget_per_day":1000,"subject":"svc-sync-01"}
```
- Ingest
```json
{"label":"Customer","properties":{"id":"c-1001","email":"a@company.com","status":"active"}}
```

### 41.2 Regulated Healthcare Ingestion (PHI)

- RBAC
```cypher
MERGE (u:User {sub:'etl-phi'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:phi-pipeline'}]->(r)
```
- Policy (deny risky ops in prod; enforce)
```json
{
  "scope_type":"project","scope_id":"phi-pipeline","mode":"enforce",
  "approvals":{"add_node_schema":3},
  "rules":[{"effect":"deny","actions":["runs:delete","query:write"],"env_ids":["prod"]}]
}
```
- Capabilities
```json
{"kind":"writer","target_label":"Patient","budget_per_day":500,"subject":"etl-phi"}
```
- Ingest
```json
{"label":"Patient","properties":{"id":"p-777","dob":"1980-04-03","mrn":"M12345"}}
```

### 41.3 News Aggregation & Enrichment

- RBAC
```cypher
MERGE (u:User {sub:'svc-news'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE]->(r)
```
- Policy (observe non‑prod; allow writes for service)
```json
{
  "scope_type":"env","scope_id":"dev","mode":"observe",
  "approvals":{},
  "rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]
}
```
- Capabilities
```json
{"kind":"writer","target_label":"Article","budget_per_day":20000,"subject":"svc-news"}
```
- Ingest
```json
{"label":"Article","properties":{"id":"a-2025-11-10-1","title":"Earnings","source":"Reuters"}}
```

### 41.4 R&D Experimentation Sandbox

- RBAC
```cypher
MERGE (u:User {sub:'researcher-1'})
MERGE (r:Role {name:'developer'})
MERGE (u)-[:HAS_ROLE {scope:'project:rd-lab'}]->(r)
```
- Policy (observe; minimal approvals)
```json
{"scope_type":"project","scope_id":"rd-lab","mode":"observe","approvals":{"add_node_schema":0},"rules":[]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Experiment","budget_per_day":1000,"subject":"researcher-1"}
```
- Ingest
```json
{"label":"Experiment","properties":{"id":"exp-42","name":"PromptAblation","status":"draft"}}
```

### 41.5 Partner Sandbox Integration

- RBAC
```cypher
MERGE (u:User {sub:'partner-xyz'})
MERGE (r:Role {name:'developer'})
MERGE (u)-[:HAS_ROLE {scope:'org:acme'}]->(r)
```
- Policy (enforce but allow limited writes)
```json
{
  "scope_type":"env","scope_id":"stage","mode":"enforce",
  "approvals":{"add_node_schema":1},
  "rules":[{"effect":"allow","actions":["query:write"],"roles":["developer"]}]
}
```
- Capabilities
```json
{"kind":"writer","target_label":"IntegrationEvent","budget_per_day":200,"subject":"partner-xyz"}
```
- Ingest
```json
{"label":"IntegrationEvent","properties":{"id":"evt-99","kind":"Webhook","partner":"xyz"}}
```

### 41.6 E‑commerce Catalog & Pricing Updates

- RBAC
```cypher
MERGE (u:User {sub:'svc-catalog'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:storefront'}]->(r)
```
- Policy (deny price edits in prod by developers)
```json
{
  "scope_type":"project","scope_id":"storefront","mode":"enforce",
  "approvals":{"add_property":2},
  "rules":[{"effect":"deny","actions":["query:write"],"env_ids":["prod"],"roles":["developer"]}]
}
```
- Capabilities
```json
{"kind":"writer","target_label":"Product","budget_per_day":5000,"subject":"svc-catalog"}
```
- Ingest
```json
{"label":"Product","properties":{"sku":"sku-1","name":"Widget","price":9.99}}
```

### 41.7 Financial KYC/AML Pipeline

- RBAC
```cypher
MERGE (u:User {sub:'aml-bot'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:kyc-aml'}]->(r)
```
- Policy (enforce; high approvals; deny destructive ops)
```json
{
  "scope_type":"project","scope_id":"kyc-aml","mode":"enforce",
  "approvals":{"add_node_schema":3,"add_relationship_schema":3},
  "rules":[{"effect":"deny","actions":["runs:delete","query:write"],"env_ids":["prod"]}]
}
```
- Capabilities
```json
{"kind":"linker","relationship_type":"FLAGGED","budget_per_day":1000,"subject":"aml-bot"}
```
- Ingest
```json
{
  "relationship_type":"FLAGGED",
  "start_element_id":"n-customer-id",
  "end_element_id":"n-alert-id",
  "properties":{"reason":"OFAC match","severity":"high"}
}
```

## 42) Viral App: Pinsurf.com (Modern Pinboard with Neural Search + Agents)

Pinsurf is a social bookmarking app for 2025: users save pages, highlights, and snippets; agents summarize and tag; neural search powers multihop RAG across the archive.

We model:
- Nodes: `User`, `Board`, `Bookmark`, `Tag`, `Highlight`, `Summary`, `Embedding`, `Domain`
- Rels: `OWNS (User→Board)`, `PINNED (Board→Bookmark)`, `HAS_TAG (Bookmark→Tag)`, `HIGHLIGHT_OF (Highlight→Bookmark)`, `SUMMARIZED_BY (Bookmark→Summary)`, `EMBEDDING_OF (Embedding→Bookmark)`, `MENTIONS_DOMAIN (Bookmark→Domain)`

Schemas (proposal snippets)
```json
// Bookmark node schema
{"label":"Bookmark","properties":[
  {"name":"id","type":"string","required":true,"unique":true,"indexed":true},
  {"name":"url","type":"string","required":true},
  {"name":"title","type":"string"},
  {"name":"saved_at","type":"datetime"},
  {"name":"visibility","type":"string","enum":["private","unlisted","public"]}
]}
```

### 42.1 User Bookmark Save Flow (Private by default)

- RBAC (assign roles)
```cypher
MERGE (u:User {sub:'user-123'})
MERGE (r:Role {name:'developer'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (deny public writes in prod for non‑service; enforce approvals=1)
```json
{
  "scope_type":"project","scope_id":"pinsurf","mode":"enforce",
  "approvals":{"add_node_schema":1,"add_relationship_schema":1},
  "rules":[{"effect":"deny","actions":["query:write"],"env_ids":["prod"],"roles":["developer"]}]
}
```

- Capabilities (writer for Bookmark)
```json
{"kind":"writer","target_label":"Bookmark","budget_per_day":1000,"subject":"user-123"}
```

- Ingest (save a bookmark)
```json
{"label":"Bookmark","properties":{
  "id":"bmk-0001","url":"https://example.com/post","title":"Post","saved_at":"2025-11-10T17:55:00Z","visibility":"private"
}}
```

### 42.2 Agent Tagging & Domain Extraction

- RBAC
```cypher
MERGE (u:User {sub:'agent-tagger'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (allow service writes)
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce",
 "rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```

- Capabilities (writer Tag, Domain; linker HAS_TAG, MENTIONS_DOMAIN)
```json
{"kind":"writer","target_label":"Tag","budget_per_day":5000,"subject":"agent-tagger"}
```
```json
{"kind":"linker","relationship_type":"HAS_TAG","budget_per_day":20000,"subject":"agent-tagger"}
```

- Ingest
```json
// Create tag
{"label":"Tag","properties":{"id":"tag-llm","name":"llm"}}
```
```json
// Link bookmark to tag
{"relationship_type":"HAS_TAG","start_element_id":"n-bmk-0001","end_element_id":"n-tag-llm","properties":{}}
```

### 42.3 Agent Summarization & Embeddings

- RBAC
```cypher
MERGE (u:User {sub:'agent-summarizer'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce",
 "rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```

- Capabilities
```json
{"kind":"writer","target_label":"Summary","budget_per_day":10000,"subject":"agent-summarizer"}
```
```json
{"kind":"linker","relationship_type":"SUMMARIZED_BY","budget_per_day":20000,"subject":"agent-summarizer"}
```

- Ingest
```json
// Create summary
{"label":"Summary","properties":{"id":"sum-0001","text":"This post covers...","model":"gpt-5","created_at":"2025-11-10T18:00:00Z"}}
```
```json
// Link summary to bookmark
{"relationship_type":"SUMMARIZED_BY","start_element_id":"n-bmk-0001","end_element_id":"n-sum-0001","properties":{}}
```

### 42.4 Highlights & Annotations

- RBAC
```cypher
MERGE (u:User {sub:'user-123'})
MERGE (r:Role {name:'developer'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (observe in dev)
```json
{"scope_type":"env","scope_id":"dev","mode":"observe","approvals":{},"rules":[]}
```

- Capabilities
```json
{"kind":"writer","target_label":"Highlight","budget_per_day":2000,"subject":"user-123"}
```

- Ingest
```json
{"label":"Highlight","properties":{
  "id":"h-0001","text":"Key quote...","start":102,"end":185
}}
```
```json
{"relationship_type":"HIGHLIGHT_OF","start_element_id":"n-h-0001","end_element_id":"n-bmk-0001","properties":{}}
```

### 42.5 Neural Search Index (Embeddings)

- RBAC
```cypher
MERGE (u:User {sub:'agent-indexer'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (allow service writes)
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce",
 "rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```

- Capabilities
```json
{"kind":"writer","target_label":"Embedding","budget_per_day":50000,"subject":"agent-indexer"}
```
```json
{"kind":"linker","relationship_type":"EMBEDDING_OF","budget_per_day":50000,"subject":"agent-indexer"}
```

- Ingest (vector truncated for brevity)
```json
{"label":"Embedding","properties":{"id":"emb-0001","vector":[0.12,0.03,0.77,...],"model":"e5-large"}}
```
```json
{"relationship_type":"EMBEDDING_OF","start_element_id":"n-emb-0001","end_element_id":"n-bmk-0001","properties":{}}
```

### 42.6 Public Boards & Sharing Rules

- RBAC
```cypher
MERGE (u:User {sub:'user-123'})
MERGE (r:Role {name:'developer'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (ABAC for public visibility changes)
```json
{
  "scope_type":"project","scope_id":"pinsurf","mode":"enforce",
  "rules":[
    {"effect":"deny","actions":["query:write"],"roles":["developer"],"env_ids":["prod"]},
    {"effect":"allow","actions":["query:write"],"roles":["service"]}
  ]
}
```

- Capabilities
```json
{"kind":"writer","target_label":"Board","budget_per_day":200,"subject":"user-123"}
```

- Ingest
```json
{"label":"Board","properties":{"id":"board-1","name":"AI Research","visibility":"public"}}
```

### 42.7 Multihop RAG Answering

Goal: Answer queries by chaining Bookmark→Summary→Highlight and Tags.

- RBAC (agent)
```cypher
MERGE (u:User {sub:'agent-rag'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```

- Policy (allow service writes/reads)
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce",
 "rules":[{"effect":"allow","actions":["query:write","query:read"],"roles":["service"]}]}
```

- Capabilities (linking citations)
```json
{"kind":"linker","relationship_type":"REFERENCES","budget_per_day":10000,"subject":"agent-rag"}
```

- Ingest (agent creates citation edges for answers)
```json
{"relationship_type":"REFERENCES","start_element_id":"n-answer-0001","end_element_id":"n-bmk-0001","properties":{"span":"lines 3-5"}}
```



## 11) Environment & Ops

- Auth env vars: `AUTH_API_KEYS`, `AUTH_JWT_SECRET`, `AUTH_MODE`
- Logging: `LOG_LEVEL`
- Overlay routing: `BACKEND_URL` (Neo4j service base)
- Debug errors in responses: `DEBUG_ERRORS=1`

## 12) Notes & Extensions

- Linker scoping: current capability enforces by `relationship_type`. If stronger scoping is desired, extend tokens with `start_label`/`end_label` and enforce before budget consumption.
- Version scoping: capability tokens include an optional `version` field (informational). Enforce specific schema versions by joining `Registered*Schema` as needed.
- JSON-Schema validation: current ingest enforces required fields and domain/range. For stricter types/units/enums, compile `NodeSchemaSpec/RelationshipSchemaSpec` into JSON-Schema and validate payloads before Cypher.

## Additional Use Cases (8×4)

For each domain: RBAC (role assignment), Policy (ABAC/approvals), Capabilities (writer/linker), Ingest (typed request).

### EdTech LMS (Courses, Lessons, Progress)

- RBAC
```cypher
MERGE (u:User {sub:'lms-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:lms'}]->(r)
```
- Policy (enforce; allow service writes)
```json
{"scope_type":"project","scope_id":"lms","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Progress","budget_per_day":100000,"subject":"lms-service"}
```
- Ingest
```json
{"label":"Progress","properties":{"id":"p-100","student_id":"s-1","lesson_id":"L-42","status":"completed","score":0.95}}
```

### IoT Fleet Telemetry & Alerts

- RBAC
```cypher
MERGE (u:User {sub:'iot-collector'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:fleet'}]->(r)
```
- Policy (deny write in prod for devs)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Telemetry","budget_per_day":500000,"subject":"iot-collector"}
```
- Ingest
```json
{"label":"Telemetry","properties":{"id":"t-1","device":"dev-9","ts":"2025-11-10T20:00:00Z","temp":71.2}}
```

### Scientific Publications & Citations

- RBAC
```cypher
MERGE (u:User {sub:'crossref-bot'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:open-research'}]->(r)
```
- Policy (enforce; allow service writes)
```json
{"scope_type":"project","scope_id":"open-research","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"linker","relationship_type":"CITES","budget_per_day":200000,"subject":"crossref-bot"}
```
- Ingest
```json
{"relationship_type":"CITES","start_element_id":"n-paper-A","end_element_id":"n-paper-B","properties":{"context":"sec 2.1"}}
```

### Legal Contracts & Clauses

- RBAC
```cypher
MERGE (u:User {sub:'legal-ai'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:contracts'}]->(r)
```
- Policy (require approvals to add clause types)
```json
{"scope_type":"project","scope_id":"contracts","mode":"enforce","approvals":{"add_node_schema":2}}
```
- Capabilities
```json
{"kind":"writer","target_label":"Clause","budget_per_day":10000,"subject":"legal-ai"}
```
- Ingest
```json
{"label":"Clause","properties":{"id":"cl-77","type":"NDA","text":"…"}}
```

### Supply Chain Shipments & Events

- RBAC
```cypher
MERGE (u:User {sub:'scm-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:supply'}]->(r)
```
- Policy (allow service writes; deny devs in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Shipment","budget_per_day":100000,"subject":"scm-service"}
```
- Ingest
```json
{"label":"Shipment","properties":{"id":"sh-9001","origin":"SFO","dest":"JFK","status":"in_transit"}}
```

### Threat Intelligence (IOCs, Actors)

- RBAC
```cypher
MERGE (u:User {sub:'ti-feed'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:threatintel'}]->(r)
```
- Policy (strict; deny destructive ops)
```json
{"scope_type":"project","scope_id":"threatintel","mode":"enforce","rules":[{"effect":"deny","actions":["runs:delete","query:write"],"env_ids":["prod"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Indicator","budget_per_day":300000,"subject":"ti-feed"}
```
- Ingest
```json
{"label":"Indicator","properties":{"id":"ioc-1","type":"domain","value":"evil.com","confidence":"high"}}
```

### HR Recruiting (Candidates, Jobs)

- RBAC
```cypher
MERGE (u:User {sub:'ats-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:ats'}]->(r)
```
- Policy (enforce; allow service writes)
```json
{"scope_type":"project","scope_id":"ats","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Candidate","budget_per_day":50000,"subject":"ats-service"}
```
- Ingest
```json
{"label":"Candidate","properties":{"id":"cand-7","name":"A. Lee","email":"a.lee@example.com","status":"applied"}}
```

### Real Estate Listings & Offers

- RBAC
```cypher
MERGE (u:User {sub:'re-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:realestate'}]->(r)
```
- Policy (enforce; deny dev write in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Listing","budget_per_day":100000,"subject":"re-service"}
```
- Ingest
```json
{"label":"Listing","properties":{"id":"list-9","address":"123 Main St","price":799000,"status":"active"}}
```

## 52) Pinsurf: Advanced Agents & Features

### 52.1 Cross‑Doc Relatedness (Graph Recommendations)

- RBAC
```cypher
MERGE (u:User {sub:'agent-curator'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:pinsurf'}]->(r)
```
- Policy (allow service)
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities (linker for RELATED_TO)
```json
{"kind":"linker","relationship_type":"RELATED_TO","budget_per_day":50000,"subject":"agent-curator"}
```
- Ingest (link bookmarks)
```json
{"relationship_type":"RELATED_TO","start_element_id":"n-bmk-0001","end_element_id":"n-bmk-0009","properties":{"score":0.82}}
```

### 52.2 Entity Extraction & Knowledge Layer

- Capabilities
```json
{"kind":"writer","target_label":"Entity","budget_per_day":50000,"subject":"agent-curator"}
```
- Ingest (entity + link)
```json
{"label":"Entity","properties":{"id":"ent-openai","type":"ORG","name":"OpenAI"}}
```
```json
{"relationship_type":"MENTIONS","start_element_id":"n-bmk-0001","end_element_id":"n-ent-openai","properties":{}}
```

### 52.3 Chunk‑Level Embeddings & Citations

- Capabilities
```json
{"kind":"writer","target_label":"Chunk","budget_per_day":100000,"subject":"agent-indexer"}
```
- Ingest (chunk + embedding)
```json
{"label":"Chunk","properties":{"id":"chk-1","bookmark_id":"bmk-0001","offset":0,"length":1024}}
```
```json
{"label":"Embedding","properties":{"id":"emb-chk-1","vector":[0.05,0.3,0.7],"model":"e5-large"}}
```
```json
{"relationship_type":"EMBEDDING_OF","start_element_id":"n-emb-chk-1","end_element_id":"n-chk-1","properties":{}}
```

### 52.4 Board‑Level Recommendations

- Capabilities
```json
{"kind":"linker","relationship_type":"RECOMMENDED","budget_per_day":10000,"subject":"agent-curator"}
```
- Ingest (recommend new bookmark to board)
```json
{"relationship_type":"RECOMMENDED","start_element_id":"n-board-1","end_element_id":"n-bmk-1002","properties":{"reason":"similar to AI Research"}}
```

## 53) Schema Migration Playbooks (Versioned)

### 53.1 Add property: Bookmark.language

Proposal
```json
{"target_kind":"node","target_name":"Bookmark","change_type":"add_property","spec":{"property":{"name":"language","type":"string"}}}
```
Migration (backfill)
```http
POST $BASE/v1/query
{"query":"MATCH (b:Bookmark) WHERE b.language IS NULL SET b.language='en' RETURN count(b) AS n","strategy":"one"}
```

### 53.2 Expand enum: visibility += friends_only

Policy approvals (2 required)
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce","approvals":{"add_property":2}}
```
Proposal
```json
{"target_kind":"node","target_name":"Bookmark","change_type":"add_property","spec":{"property":{"name":"visibility","type":"string","enum":["private","unlisted","public","friends_only"]}}}
```

### 53.3 Split title into title/subtitle (additive + deprecation)

Proposals
```json
{"target_kind":"node","target_name":"Bookmark","change_type":"add_property","spec":{"property":{"name":"subtitle","type":"string"}}}
```
Backfill
```http
POST $BASE/v1/query
{"query":"MATCH (b:Bookmark) WHERE b.subtitle IS NULL SET b.subtitle=substring(b.title,0,80) RETURN count(b) AS n","strategy":"one"}
```

## 54) Auditing & Reporting (Cypher)

- Missing embeddings
```cypher
MATCH (b:Bookmark)
WHERE NOT ( (:Embedding)-[:EMBEDDING_OF]->(b) )
RETURN b.id AS bookmark_id LIMIT 100
```

- Public boards with excessive public bookmarks
```cypher
MATCH (bd:Board {visibility:'public'})-[:PINNED]->(b:Bookmark {visibility:'public'})
WITH bd, count(b) AS c
WHERE c > 1000
RETURN bd.id AS board, c ORDER BY c DESC
```

- Tokens near exhaustion
```cypher
MATCH (c:CapabilityToken)
WHERE c.budget_per_day > 0 AND c.used_today >= c.budget_per_day * 0.9
RETURN c.id, c.sub, c.kind, c.target_label, c.relationship_type, c.used_today, c.budget_per_day
```

- Proposals needing approvals
```cypher
MATCH (p:SchemaProposal {status:'Proposed'})
OPTIONAL MATCH (:User)-[a:APPROVED]->(p)
RETURN p.id AS id, p.target_name AS target, count(a) AS approvals
```

## 55) .http Collection Outline (Dev Testing)

```http
### Set project policy
POST {{base}}/v1/policies/upsert
Authorization: Bearer {{token}}
Content-Type: application/json

{
  "scope_type":"project","scope_id":"pinsurf","mode":"enforce",
  "rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]
}

### Mint writer
POST {{base}}/v1/capabilities
Authorization: Bearer {{token}}
Content-Type: application/json

{"kind":"writer","target_label":"Bookmark","budget_per_day":1000,"subject":"user-123"}

### Ingest bookmark
POST {{base}}/v1/ingest/nodes
Authorization: Bearer {{token}}
Content-Type: application/json

{"label":"Bookmark","properties":{"id":"bmk-3","url":"https://…","visibility":"private"}}
```

## 56) SLOs & Alerts

- SLI1: Ingest success rate ≥ 99.5% over 1h (alert if < 99%)
- SLI2: Policy decision latency p95 < 50ms (alert if > 100ms)
- SLI3: Proposal apply lead time p95 < 10m (alert if > 20m)
- SLI4: Capabilities near exhaustion < 5% of tokens (alert if > 10%)

## 57) Privacy & Retention Controls

- ABAC: deny reading private bookmarks outside owner
```json
{"effect":"deny","actions":["query:read"],"roles":["developer"],"projects":["pinsurf"]}
```
- Retention: delete highlights older than 1 year
```http
POST $BASE/v1/query
{"query":"MATCH (h:Highlight) WHERE h.created_at < datetime() - duration('P365D') DETACH DELETE h RETURN count(h) AS n","strategy":"one"}
```

## 58) Enterprise: Pinsurf for Teams (Org Boards)

- RBAC
```cypher
MERGE (u:User {sub:'team-admin'})
MERGE (r:Role {name:'schema_admin'})
MERGE (u)-[:HAS_ROLE {scope:'org:acme'}]->(r)
```
- Policy (org‑wide controls)
```json
{"scope_type":"org","scope_id":"acme","mode":"enforce","approvals":{"add_node_schema":2}}
```
- Capabilities
```json
{"kind":"writer","target_label":"Board","budget_per_day":500,"subject":"team-admin"}
```
- Ingest
```json
{"label":"Board","properties":{"id":"eng-ai","name":"Engineering AI","visibility":"private"}}
```

## 59) More Use Cases II (10×4)

Each domain includes: RBAC assignment, ABAC policy, capability tokens, and a typed ingest example.

### Gaming Platform (Players, Matches, Leaderboards)

- RBAC
```cypher
MERGE (u:User {sub:'game-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:gaming'}]->(r)
```
- Policy (allow service writes, enforce)
```json
{"scope_type":"project","scope_id":"gaming","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Match","budget_per_day":200000,"subject":"game-service"}
```
- Ingest
```json
{"label":"Match","properties":{"id":"m-100","started_at":"2025-11-10T20:30:00Z","mode":"ranked","duration":900}}
```

### Media Streaming (Content, Playlists, Recs)

- RBAC
```cypher
MERGE (u:User {sub:'stream-curator'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:stream'}]->(r)
```
- Policy (allow service writes; deny devs in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"linker","relationship_type":"ADDED_TO","budget_per_day":100000,"subject":"stream-curator"}
```
- Ingest
```json
{"relationship_type":"ADDED_TO","start_element_id":"n-content-42","end_element_id":"n-playlist-1","properties":{"order":7}}
```

### Smart City Traffic (Sensors, Incidents)

- RBAC
```cypher
MERGE (u:User {sub:'traffic-collector'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:smartcity'}]->(r)
```
- Policy (enforce; allow service writes)
```json
{"scope_type":"project","scope_id":"smartcity","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Incident","budget_per_day":50000,"subject":"traffic-collector"}
```
- Ingest
```json
{"label":"Incident","properties":{"id":"inc-123","type":"collision","ts":"2025-11-10T21:00:00Z","lat":37.775,"lon":-122.419}}
```

### Banking Loan Underwriting (Apps, Scores)

- RBAC
```cypher
MERGE (u:User {sub:'underwrite-bot'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:loans'}]->(r)
```
- Policy (strict approvals for schema changes)
```json
{"scope_type":"project","scope_id":"loans","mode":"enforce","approvals":{"add_node_schema":3}}
```
- Capabilities
```json
{"kind":"writer","target_label":"Application","budget_per_day":20000,"subject":"underwrite-bot"}
```
- Ingest
```json
{"label":"Application","properties":{"id":"app-555","amount":250000,"product":"mortgage","status":"pending"}}
```

### Pharma Clinical Trials (Subjects, Observations)

- RBAC
```cypher
MERGE (u:User {sub:'trial-system'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:trials'}]->(r)
```
- Policy (deny destructive ops in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["runs:delete","query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Observation","budget_per_day":100000,"subject":"trial-system"}
```
- Ingest
```json
{"label":"Observation","properties":{"id":"obs-77","subject_id":"s-9","time":"2025-11-10T21:30:00Z","bp":120,"hr":70}}
```

### Social Threads (Posts, Replies, Reactions)

- RBAC
```cypher
MERGE (u:User {sub:'social-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:social'}]->(r)
```
- Policy (allow service; deny dev write in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"linker","relationship_type":"REPLIED_TO","budget_per_day":500000,"subject":"social-service"}
```
- Ingest
```json
{"relationship_type":"REPLIED_TO","start_element_id":"n-post-100","end_element_id":"n-post-1","properties":{"ts":"2025-11-10T21:35:00Z"}}
```

### OSS Package Registry (Packages, Versions, Deps)

- RBAC
```cypher
MERGE (u:User {sub:'registry-bot'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:registry'}]->(r)
```
- Policy (enforce; allow service writes)
```json
{"scope_type":"project","scope_id":"registry","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"linker","relationship_type":"DEPENDS_ON","budget_per_day":300000,"subject":"registry-bot"}
```
- Ingest
```json
{"relationship_type":"DEPENDS_ON","start_element_id":"n-pkg-A@1.0.0","end_element_id":"n-pkg-B@2.3.1","properties":{"scope":"runtime"}}
```

### Energy Grid Ops (Meters, Outages)

- RBAC
```cypher
MERGE (u:User {sub:'grid-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:energy'}]->(r)
```
- Policy (deny dev write in prod)
```json
{"scope_type":"env","scope_id":"prod","mode":"enforce","rules":[{"effect":"deny","actions":["query:write"],"roles":["developer"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Outage","budget_per_day":50000,"subject":"grid-service"}
```
- Ingest
```json
{"label":"Outage","properties":{"id":"out-42","started":"2025-11-10T21:40:00Z","area":"N-Sub","affected":1200}}
```

### Travel & Booking (Itineraries, Tickets)

- RBAC
```cypher
MERGE (u:User {sub:'travel-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:travel'}]->(r)
```
- Policy (allow service; enforce)
```json
{"scope_type":"project","scope_id":"travel","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capabilities
```json
{"kind":"writer","target_label":"Itinerary","budget_per_day":100000,"subject":"travel-service"}
```
- Ingest
```json
{"label":"Itinerary","properties":{"id":"it-88","user_id":"u-7","depart":"SFO","arrive":"CDG","date":"2026-02-01"}}
```

### ESG Reporting (Emissions, Projects)

- RBAC
```cypher
MERGE (u:User {sub:'esg-service'})
MERGE (r:Role {name:'service'})
MERGE (u)-[:HAS_ROLE {scope:'project:esg'}]->(r)
```
- Policy (approvals for schema changes)
```json
{"scope_type":"project","scope_id":"esg","mode":"enforce","approvals":{"add_node_schema":2,"add_property":2}}
```
- Capabilities
```json
{"kind":"writer","target_label":"Emission","budget_per_day":50000,"subject":"esg-service"}
```
- Ingest
```json
{"label":"Emission","properties":{"id":"e-101","scope":"scope2","qty_tCO2e":120.5,"period":"2025Q3"}}
```

## 60) Novel Application Concepts (12 ideas × compact examples)

Each concept includes a graph sketch and minimal RBAC/Policy/Capability/Ingest snippets to seed implementation.

### 60.1 Personal AI Memory Atlas

- Graph sketch
  - Nodes: Memory, Person, Project, Moment
  - Rels: INVOLVES(Person↔Memory), ABOUT(Project↔Memory), OCCURRED_AT(Memory→Moment)
- RBAC
```cypher
MERGE (u:User {sub:'mem-agent'})-[:HAS_ROLE]->(:Role {name:'service'})
```
- Policy
```json
{"scope_type":"project","scope_id":"mem-atlas","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Capability
```json
{"kind":"writer","target_label":"Memory","budget_per_day":20000,"subject":"mem-agent"}
```
- Ingest
```json
{"label":"Memory","properties":{"id":"mem-1","text":"Met Alex to discuss launch.","date":"2025-11-10"}}
```

### 60.2 Synthetic Biology CAD

- Graph sketch
  - Nodes: Construct, Part, Protocol, Assay
  - Rels: CONTAINS(Construct→Part), TESTED_BY(Construct→Assay), RUNS(Protocol→Assay)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'synbio-bot'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"synbio","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Construct","budget_per_day":5000,"subject":"synbio-bot"}
```
```json
{"label":"Construct","properties":{"id":"c-1","name":"pGAL1-GFP","version":"v1"}}
```

### 60.3 Spatial AR Layers

- Graph sketch
  - Nodes: Place, Anchor, Overlay, Scene
  - Rels: HAS_ANCHOR(Place→Anchor), RENDERS(Overlay→Anchor), IN_SCENE(Overlay→Scene)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'ar-renderer'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"ar-city","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Overlay","budget_per_day":100000,"subject":"ar-renderer"}
```
```json
{"label":"Overlay","properties":{"id":"ov-1","type":"label","text":"Cafe"}}
```

### 60.4 Robotics Mission Graph

- Graph sketch
  - Nodes: Mission, Waypoint, Telemetry, Fault
  - Rels: HAS_WAYPOINT(Mission→Waypoint), EMITTED(Telemetry→Mission), TRIGGERED(Fault→Mission)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'autonomy'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"robots","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Mission","budget_per_day":20000,"subject":"autonomy"}
```
```json
{"label":"Mission","properties":{"id":"ms-1","vehicle":"r2","status":"planned"}}
```

### 60.5 Decentralized Identity & VCs

- Graph sketch
  - Nodes: DID, VC, Issuer, Subject
  - Rels: ISSUED_BY(VC→Issuer), ABOUT(VC→Subject), IDENTIFIES(DID→Subject)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'vc-issuer'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"did-vc","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"VC","budget_per_day":100000,"subject":"vc-issuer"}
```
```json
{"label":"VC","properties":{"id":"vc-1","type":"UniversityDegree","issued_at":"2025-11-10"}}
```

### 60.6 DAO Governance

- Graph sketch
  - Nodes: Proposal, Vote, TreasuryTx
  - Rels: VOTED(Vote→Proposal), FUNDED_BY(TreasuryTx→Proposal)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'dao-bot'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"dao","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Proposal","budget_per_day":5000,"subject":"dao-bot"}
```
```json
{"label":"Proposal","properties":{"id":"prop-1","title":"Fund Grants","status":"active"}}
```

### 60.7 Federated Model Registry

- Graph sketch
  - Nodes: Model, Dataset, Eval, Run
  - Rels: TRAINED_ON(Model→Dataset), EVALUATED_BY(Model→Eval), PRODUCED(Run→Model)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'ml-hub'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"modelhub","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Model","budget_per_day":10000,"subject":"ml-hub"}
```
```json
{"label":"Model","properties":{"id":"m-bert","version":"1.2","task":"qa"}}
```

### 60.8 Generative Design & Asset Lineage

- Graph sketch
  - Nodes: Prompt, Asset, Style, License
  - Rels: GENERATED(Asset→Prompt), HAS_STYLE(Asset→Style), LICENSED_UNDER(Asset→License)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'gen-ai'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"gendesign","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Asset","budget_per_day":200000,"subject":"gen-ai"}
```
```json
{"label":"Asset","properties":{"id":"asset-1","type":"image","url":"ipfs://…"}}
```

### 60.9 Climate Risk & Adaptation

- Graph sketch
  - Nodes: Asset, Hazard, Measure
  - Rels: EXPOSED_TO(Asset→Hazard), MITIGATED_BY(Asset→Measure)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'climate-tool'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"climate","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Measure","budget_per_day":50000,"subject":"climate-tool"}
```
```json
{"label":"Measure","properties":{"id":"m-levy","name":"Sea wall","status":"planned"}}
```

### 60.10 Smart Contract Verification Graph

- Graph sketch
  - Nodes: Contract, Property, Proof
  - Rels: ASSERTS(Contract→Property), PROVED_BY(Property→Proof)
- RBAC/Policy/Capability/Ingest
```cypher
MERGE (u:User {sub:'verifier'})-[:HAS_ROLE]->(:Role {name:'service'})
```
```json
{"scope_type":"project","scope_id":"scv","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
```json
{"kind":"writer","target_label":"Proof","budget_per_day":10000,"subject":"verifier"}
```
```json
{"label":"Proof","properties":{"id":"pf-1","method":"Coq","result":"valid"}}
```

## 61) Capability Scoping Extensions (Design + Examples)

Goal: tighten Linker semantics by adding `start_label` and `end_label` scopes to capability tokens, and optionally version pinning for Writer.

Proposed token fields
- Linker: `{ kind:'linker', relationship_type, start_label?, end_label? }`
- Writer: `{ kind:'writer', target_label, version? }`

Enforcement pseudocode
```python
def _consume_capability_linker(principal, rtype, a_labels, b_labels):
  token = match_token(sub=principal.sub, kind='linker', rtype=rtype)
  if token.start_label and token.start_label not in a_labels: deny
  if token.end_label and token.end_label not in b_labels: deny
  decrement_budget_if_finite(token)
```

Examples
- Linker scoped to (Bookmark→Tag) only
```json
{"kind":"linker","relationship_type":"HAS_TAG","start_label":"Bookmark","end_label":"Tag","budget_per_day":50000,"subject":"agent-tagger"}
```
- Writer pinned to schema version (informational)
```json
{"kind":"writer","target_label":"Bookmark","version":"3","budget_per_day":1000,"subject":"ingest-service"}
```

## 62) Response Schemas per Endpoint (Informal)

- GET /v1/registry/nodes/{name}
```json
{"name":"Board","description":"...","versions":[{"version":"1","spec":{...},"created_at":"...","id":"..."}]}
```
- POST /v1/registry/proposals
```json
{"id":"prop-uuid","status":"Proposed","target_kind":"node","target_name":"Board","change_type":"add_node_schema"}
```
- GET /v1/registry/proposals/{pid}/approvals
```json
{"proposal_id":"prop-uuid","approvals_current":1,"approvals_required":2}
```
- POST /v1/capabilities
```json
{"id":"cap-uuid","kind":"writer","target_label":"Bookmark","subject":"user-123","budget_per_day":1000,"used_today":0,"day_key":"2025-11-10"}
```
- POST /v1/ingest/nodes
```json
{"id":123,"element_id":"n123","labels":["Bookmark"],"properties":{...}}
```

## 63) Compliance Profiles (Policy Bundles)

HIPAA (healthcare)
- Mode: `enforce`
- Deny: `query:write` to non‑service in prod; deny `runs:delete`
- Approvals: `add_node_schema:3`, `add_property:2`
- Retention: remove PHI selectors after 365 days (policy note + scheduled queries)

GDPR (EU personal data)
- Right to erasure: data contract marks PII fields; scheduled deletion APIs via `/v1/query`
- Audit: approvals and policy decisions retained 6 years

SOC 2 (controls)
- SLOs enforced; alerting on SLI breaches
- Change management: proposals + approvals mandatory for schema changes

## 64) Cost & Quota Governance

- Capabilities as cost caps: budgets reflect daily quotas per agent/service
- Add `cost_per_write` metadata per label/type; monitor `used_today * cost_per_write`
- Alerts: when projected consumption exceeds budget by 20%

Policy example
```json
{"scope_type":"project","scope_id":"pinsurf","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}],"approvals":{"add_property":1}}
```

## 65) Multi‑Tenant Patterns

- Org isolation: policies at `org:*` scopes
- Project segmentation per product area; environment gates per deployment tier
- Example
```cypher
MERGE (o:Org {id:'acme'})-[:HAS_POLICY]->(p:Policy {id:'org:acme'})
SET p.mode='enforce'
```

## 66) A/B & Canary Policies (Experimentation)

- Use project policies to allow `query:write` for canary services only
```json
{"scope_type":"project","scope_id":"pinsurf-canary","mode":"enforce","rules":[{"effect":"allow","actions":["query:write"],"roles":["service"]}]}
```
- Canary capability with small budget
```json
{"kind":"writer","target_label":"Bookmark","budget_per_day":100,"subject":"svc-canary"}
```

## 67) Real‑Time Moderation Pipeline

- Nodes: Report, Decision; Rels: REPORTS(User→Report), DECIDES(Decision→Bookmark)
- Policy: deny non‑service writes; service allowed
- Capabilities: writer for Decision, linker for DECIDES
- Ingest Decision
```json
{"label":"Decision","properties":{"id":"dec-7","action":"hide","reason":"policy"}}
```
```json
{"relationship_type":"DECIDES","start_element_id":"n-dec-7","end_element_id":"n-bmk-0001","properties":{}}
```

## 68) Knowledge Base with Citations

- Nodes: Article, Claim, Citation; Rels: SUPPORTS(Article→Claim), CITES(Citation→Article)
- Writer: Claim; Linker: SUPPORTS/CITES
- Ingest Claim
```json
{"label":"Claim","properties":{"id":"cl-1","text":"Transformers scale quadratically"}}
```
```json
{"relationship_type":"SUPPORTS","start_element_id":"n-article-9","end_element_id":"n-claim-cl-1","properties":{"evidence":"sec 3"}}
```

## 69) Growth Experiments (Pinsurf)

- Team board onboarding: auto‑create Board per invite
- Ingest InviteAccepted event; Linker ADDED_TO(User→Board) for membership
```json
{"label":"InviteAccepted","properties":{"id":"inv-999","user":"u-1","board":"eng-ai","ts":"2025-11-10T22:00:00Z"}}
```

## 70) Observability Dashboards (Queries)

- Daily ingests by label
```cypher
MATCH (c:CapabilityToken)-[:CONSUMED*0..1]->() // optional modeling
WITH date() AS d
MATCH (n) WHERE n.created_at >= datetime() - duration('P1D')
RETURN labels(n)[0] AS label, count(*) AS ingests ORDER BY ingests DESC
```
- Approvals backlog
```cypher
MATCH (p:SchemaProposal {status:'Proposed'})
OPTIONAL MATCH (:User)-[a:APPROVED]->(p)
RETURN p.id, p.target_name, count(a) AS approvals
```
