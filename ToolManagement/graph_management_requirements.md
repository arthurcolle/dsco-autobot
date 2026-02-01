# Graph Management API Requirements for Tool Management System

## Overview
The Tool Management System requires specific capabilities from the Graph Management API to enable dynamic tool registration, schema validation, and graph-based orchestration. This document outlines the requirements and proposed extensions.

## Core Entities in the Graph

### 1. Tool Node
The top-level entity representing a registered tool.

```cypher
(t:Tool {
    tool_id: "unique-tool-id",
    name: "tool_name",
    description: "Tool description",
    version: "1.0.0",
    service_name: "owning_service",
    service_url: "http://service.internal",
    created_at: timestamp(),
    updated_at: timestamp(),
    visibility: "public|private|organization",
    status: "active|deprecated|disabled",
    usage_count: 0,
    rating: 4.5,
    tags: ["tag1", "tag2"],
    category: "data-processing",
    organization_id: "org-123",
    user_id: "user-456",
    toolkit_id: "toolkit-789"
})
```

### 2. ToolAction Node
Represents the executable endpoint/action for a tool.

```cypher
(ta:ToolAction {
    action_id: "unique-action-id",
    type: "http|grpc|websocket|lambda",
    method: "POST|GET|PUT|DELETE",
    url: "http://service.internal/endpoint",
    timeout_ms: 30000,
    retry_policy: {max_attempts: 3, backoff_ms: 1000},
    auth_type: "none|bearer|api_key|oauth2",
    auth_config: {},
    headers: {},
    rate_limit: {requests_per_minute: 100},
    circuit_breaker: {threshold: 0.5, timeout_ms: 60000}
})
```

### 3. ToolSchema Nodes
Input and output schemas that integrate with your schema registry.

```cypher
(ts:ToolSchema {
    schema_id: "unique-schema-id",
    schema_type: "input|output",
    version: "1.0.0",
    json_schema: {},
    validation_rules: [],
    examples: [],
    embedding: [], // For semantic search
    created_at: timestamp()
})
```

### 4. Relationships

```cypher
// Tool ownership and action
(t:Tool)-[:HAS_ACTION]->(ta:ToolAction)
(t:Tool)-[:HAS_INPUT_SCHEMA]->(ts_in:ToolSchema {schema_type: "input"})
(t:Tool)-[:HAS_OUTPUT_SCHEMA]->(ts_out:ToolSchema {schema_type: "output"})

// Schema evolution and versioning
(ts1:ToolSchema)-[:EVOLVED_TO]->(ts2:ToolSchema)
(t:Tool)-[:USES_VERSION {active: true}]->(ts:ToolSchema)

// Tool composition and dependencies
(t1:Tool)-[:DEPENDS_ON]->(t2:Tool)
(t1:Tool)-[:CAN_CHAIN_TO {mapping: {}}]->(t2:Tool)
(t1:Tool)-[:COMPOSED_OF]->(t2:Tool)

// Organization and access control
(o:Organization)-[:OWNS]->(t:Tool)
(u:User)-[:CREATED]->(t:Tool)
(tk:Toolkit)-[:CONTAINS]->(t:Tool)

// Execution tracking
(e:Execution)-[:EXECUTED]->(t:Tool)
(e:Execution)-[:WITH_INPUT]->(input:DataInstance)
(e:Execution)-[:PRODUCED_OUTPUT]->(output:DataInstance)
```

## Required Graph Management API Extensions

### 1. Schema Registration Endpoints

#### Register Tool Schema Bundle
```python
POST /v1/schemas/tool/register
{
    "tool_name": "my_tool",
    "tool_spec": {
        "label": "Tool",
        "description": "A registered tool",
        "properties": [
            {"name": "tool_id", "type": "string", "required": true, "unique": true},
            {"name": "name", "type": "string", "required": true, "indexed": true},
            {"name": "embedding", "type": "array", "indexed": true}  # Vector index
        ]
    },
    "action_spec": {
        "label": "ToolAction",
        "properties": [...]
    },
    "input_schema_spec": {
        "label": "ToolSchema",
        "properties": [...]
    },
    "output_schema_spec": {
        "label": "ToolSchema",
        "properties": [...]
    },
    "relationships": [
        {
            "type": "HAS_ACTION",
            "start_labels": ["Tool"],
            "end_labels": ["ToolAction"],
            "cardinality": "one_to_one"
        },
        {
            "type": "HAS_INPUT_SCHEMA",
            "start_labels": ["Tool"],
            "end_labels": ["ToolSchema"]
        }
    ]
}
```

#### Validate Tool Against Schema
```python
POST /v1/schemas/tool/validate
{
    "tool_data": {...},
    "schema_version": "1.0.0"  # Optional, uses latest if not specified
}
```

### 2. Tool Discovery Endpoints

#### Semantic Tool Search
```python
POST /v1/tools/search
{
    "prompt": "I need a tool to process CSV files",
    "filters": {
        "organization_id": "org-123",
        "visibility": ["public", "organization"],
        "categories": ["data-processing"],
        "min_rating": 4.0
    },
    "limit": 10,
    "include_embeddings": false
}
```

#### Tool Dependency Graph
```python
GET /v1/tools/{tool_id}/dependencies
{
    "depth": 2,  # How many levels deep
    "include_optional": false,
    "format": "cypher|json|graphml"
}
```

### 3. Schema Evolution & Versioning

#### Propose Schema Change
```python
POST /v1/schemas/tool/propose-change
{
    "tool_id": "tool-123",
    "change_type": "add_property",
    "target": "input_schema",
    "patch": {
        "properties": {
            "new_field": {"type": "string", "required": false}
        }
    },
    "rationale": "Adding support for new feature X",
    "backward_compatible": true
}
```

#### Schema Migration Path
```python
GET /v1/schemas/tool/{tool_id}/migration-path
{
    "from_version": "1.0.0",
    "to_version": "2.0.0"
}
```

### 4. Tool Composition & Orchestration

#### Register Tool Composition
```python
POST /v1/tools/compositions
{
    "name": "data_pipeline",
    "type": "sequential|parallel|conditional",
    "tools": [
        {"tool_id": "reader-001", "config": {}},
        {"tool_id": "processor-002", "config": {}},
        {"tool_id": "writer-003", "config": {}}
    ],
    "connections": [
        {
            "from": "reader-001.output",
            "to": "processor-002.input",
            "transform": "direct|map|filter"
        }
    ],
    "validation_rules": []
}
```

#### Validate Tool Chain Compatibility
```python
POST /v1/tools/validate-chain
{
    "tools": ["tool-1", "tool-2", "tool-3"],
    "check_schema_compatibility": true,
    "check_performance_requirements": true
}
```

### 5. Runtime Integration

#### Record Tool Execution
```python
POST /v1/tools/{tool_id}/executions
{
    "execution_id": "exec-123",
    "input_data": {...},
    "output_data": {...},
    "metrics": {
        "duration_ms": 1500,
        "memory_mb": 256,
        "success": true
    },
    "context": {
        "user_id": "user-456",
        "session_id": "session-789",
        "environment": "production"
    }
}
```

#### Get Tool Execution History
```python
GET /v1/tools/{tool_id}/executions
{
    "limit": 100,
    "include_metrics": true,
    "include_io_data": false,
    "time_range": {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-31T23:59:59Z"
    }
}
```

### 6. Access Control & Capabilities

#### Create Tool Capability Token
```python
POST /v1/capabilities/tool
{
    "tool_id": "tool-123",
    "permissions": ["read", "execute"],
    "budget_per_day": 1000,
    "expires_at": "2024-12-31T23:59:59Z",
    "subject": "service-account-456",
    "constraints": {
        "max_input_size_mb": 10,
        "allowed_environments": ["staging", "production"]
    }
}
```

### 7. Graph Queries Needed

#### Find Compatible Tools
```cypher
MATCH (t1:Tool {tool_id: $tool_id})-[:HAS_OUTPUT_SCHEMA]->(os:ToolSchema)
MATCH (t2:Tool)-[:HAS_INPUT_SCHEMA]->(is:ToolSchema)
WHERE os.json_schema = is.json_schema OR
      exists((os)-[:COMPATIBLE_WITH]->(is))
RETURN t2
```

#### Get Tool Lineage
```cypher
MATCH path = (t:Tool {tool_id: $tool_id})-[:EVOLVED_TO*]->(latest:Tool)
WHERE NOT (latest)-[:EVOLVED_TO]->()
RETURN path
```

#### Find Tools by Capability
```cypher
MATCH (t:Tool)-[:HAS_INPUT_SCHEMA]->(is:ToolSchema)
WHERE is.json_schema.properties.file_type.enum CONTAINS 'csv'
AND t.visibility IN ['public', 'organization']
RETURN t
ORDER BY t.usage_count DESC, t.rating DESC
LIMIT 10
```

## Integration Points

### 1. With Integration Servers
- Integration servers register their tools dynamically
- Each registration creates Tool, ToolAction, and ToolSchema nodes
- Schemas are validated against the Graph Management Layer
- Capability tokens control execution permissions

### 2. With Schema Registry
- All tool schemas must be registered in the graph
- Schema evolution tracked through EVOLVED_TO relationships
- Backward compatibility validation before schema updates
- Automatic migration path generation

### 3. With Execution Engine
- Execution requests validated against current schemas
- Execution metrics recorded in graph for analysis
- Tool chains validated for compatibility
- Circuit breaker states maintained in graph

## Performance Considerations

### Required Indexes
```cypher
CREATE INDEX tool_name_idx FOR (t:Tool) ON (t.name);
CREATE INDEX tool_id_idx FOR (t:Tool) ON (t.tool_id);
CREATE INDEX tool_embedding_idx FOR (t:Tool) ON (t.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1536}};
CREATE INDEX schema_version_idx FOR (ts:ToolSchema) ON (ts.version);
CREATE INDEX execution_time_idx FOR (e:Execution) ON (e.timestamp);
```

### Caching Strategy
- Cache frequently accessed tool definitions
- Cache validated schema combinations
- Cache tool compatibility matrices
- TTL: 5 minutes for active tools, 30 minutes for schemas

## Security Requirements

### 1. Schema Validation
- All tool registrations must pass schema validation
- Input/output schemas validated at runtime
- Prevent schema injection attacks

### 2. Access Control
- Tool visibility enforced at query time
- Organization boundaries respected
- Capability tokens for fine-grained permissions

### 3. Audit Trail
- All tool registrations logged
- Schema changes tracked with approval workflow
- Execution history maintained for compliance

## Migration from Existing System

### Phase 1: Schema Definition
1. Define Tool, ToolAction, ToolSchema node schemas
2. Register with Graph Management API
3. Create necessary indexes and constraints

### Phase 2: Tool Migration
1. Export existing tools from current storage
2. Transform to graph schema format
3. Bulk import through Graph Management API

### Phase 3: Integration
1. Update Tool Management API to use Graph Management client
2. Implement schema validation middleware
3. Enable dynamic tool registration

### Phase 4: Advanced Features
1. Implement tool composition
2. Add semantic search
3. Enable execution tracking

## Success Metrics
- Tool registration time < 500ms
- Schema validation time < 100ms
- Tool search time < 200ms
- 99.9% availability for tool execution
- Zero schema drift incidents

## Next Steps
1. Review and approve schema definitions
2. Implement Graph Management API extensions
3. Create Tool Management client library
4. Build migration scripts
5. Deploy to staging environment