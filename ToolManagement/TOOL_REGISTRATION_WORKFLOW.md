# Tool Registration Workflow Documentation

## Overview
This document describes the workflow for registering tools dynamically in the Neo4j graph through the Graph Management API. The system allows integration servers to register their tools on-the-fly while maintaining schema integrity and version control.

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│ Integration Server  │────▶│ Tool Management API │────▶│ Graph Management│
│   (Your Service)    │     │   (Orchestrator)    │     │      API        │
└─────────────────────┘     └─────────────────────┘     └─────────────────┘
                                                                  │
                                                                  ▼
                                                          ┌─────────────┐
                                                          │    Neo4j    │
                                                          │   Database  │
                                                          └─────────────┘
```

## Core Concepts

### 1. Tool Entity Structure
Every tool in the system consists of three core components:

- **Tool**: Metadata and identification
- **ToolAction**: Execution details (HTTP endpoints, auth, etc.)
- **ToolSchema**: Input/output validation schemas

### 2. Graph Relationships
```
(Tool)-[:HAS_ACTION]->(ToolAction)
(Tool)-[:HAS_INPUT_SCHEMA]->(ToolSchema:input)
(Tool)-[:HAS_OUTPUT_SCHEMA]->(ToolSchema:output)
(Tool)-[:DEPENDS_ON]->(Tool)
(Tool)-[:CAN_CHAIN_TO]->(Tool)
```

## Registration Workflows

### Workflow 1: Basic Tool Registration

```python
# Step 1: Integration server prepares tool definition
from graph_management_client import (
    GraphManagementClient,
    ToolModel,
    ToolActionModel,
    ToolSchemaModel,
    ToolRegistration
)

# Step 2: Initialize client
client = GraphManagementClient(
    base_url="http://localhost:2014",
    api_key="your_api_key"  # Optional
)

# Step 3: Define tool metadata
tool = ToolModel(
    tool_id="data_processor_v1",  # Unique identifier
    name="DataProcessor",
    description="Processes various data formats",
    version="1.0.0",
    service_name="data_service",
    service_url="http://data-service.internal",
    visibility="organization",  # public|private|organization
    tags=["data", "etl", "processing"],
    organization_id="org_123"
)

# Step 4: Define action (how to execute)
action = ToolActionModel(
    action_id="data_processor_v1_action",
    type="http",
    method="POST",
    url="http://data-service.internal/api/process",
    timeout_ms=30000,
    auth_type="bearer",
    auth_config={"token_env": "SERVICE_TOKEN"},
    rate_limit={"requests_per_minute": 100}
)

# Step 5: Define schemas
input_schema = ToolSchemaModel(
    schema_id="data_processor_v1_input",
    schema_type="input",
    json_schema={
        "type": "object",
        "properties": {
            "input_file": {
                "type": "string",
                "description": "Path to input file"
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json", "xml"]
            },
            "options": {
                "type": "object",
                "properties": {
                    "delimiter": {"type": "string"},
                    "headers": {"type": "boolean"}
                }
            }
        },
        "required": ["input_file", "format"]
    }
)

output_schema = ToolSchemaModel(
    schema_id="data_processor_v1_output",
    schema_type="output",
    json_schema={
        "type": "object",
        "properties": {
            "processed_records": {"type": "integer"},
            "output_file": {"type": "string"},
            "errors": {
                "type": "array",
                "items": {"type": "string"}
            }
        }
    }
)

# Step 6: Register the tool
registration = ToolRegistration(
    tool=tool,
    action=action,
    input_schema=input_schema,
    output_schema=output_schema
)

result = await client.register_tool(registration)
print(f"Tool registered: {result['tool_id']}")
```

### Workflow 2: Dynamic Registration with Discovery

```python
# Integration servers can auto-register on startup
class ToolAutoRegistration:
    def __init__(self, service_name: str, service_url: str):
        self.service_name = service_name
        self.service_url = service_url
        self.client = GraphManagementClient()
        self.registered_tools = {}

    async def register_from_decorators(self):
        """Auto-discover and register decorated functions"""

        # Scan for @tool decorated functions
        for func_name, func in self.discover_tools():
            # Extract metadata from function
            tool_spec = self.extract_tool_spec(func)

            # Check if already registered
            existing = await self.client.get_tool_by_id(tool_spec.tool_id)

            if existing:
                # Check for schema changes
                if self.schema_changed(existing, tool_spec):
                    # Propose schema evolution
                    await self.propose_evolution(existing, tool_spec)
                else:
                    print(f"Tool {func_name} already registered")
            else:
                # Register new tool
                result = await self.client.register_tool(tool_spec)
                self.registered_tools[func_name] = result['tool_id']

    async def propose_evolution(self, existing, new_spec):
        """Propose schema evolution for backward compatibility"""
        proposal = await self.client.propose_schema_change(
            tool_id=existing['tool']['tool_id'],
            change_type="schema_evolution",
            target="input_schema",
            patch=new_spec.input_schema.json_schema,
            rationale="Adding new optional fields for feature X"
        )
        print(f"Schema change proposed: {proposal['proposal_id']}")
```

### Workflow 3: Tool Composition Registration

```python
# Register a composite tool that chains multiple tools
async def register_data_pipeline():
    client = GraphManagementClient()

    # Define the pipeline composition
    composition = await client.create_tool_composition(
        name="etl_pipeline",
        composition_type=CompositionType.SEQUENTIAL,
        tools=[
            {
                "tool_id": "csv_reader_v1",
                "config": {"chunk_size": 1000}
            },
            {
                "tool_id": "data_transformer_v1",
                "config": {"rules": "transform_rules.json"}
            },
            {
                "tool_id": "db_writer_v1",
                "config": {"batch_size": 100}
            }
        ],
        connections=[
            {
                "from_tool": "csv_reader_v1",
                "to_tool": "data_transformer_v1",
                "from_output": "records",
                "to_input": "data",
                "transform": "direct"
            },
            {
                "from_tool": "data_transformer_v1",
                "to_tool": "db_writer_v1",
                "from_output": "transformed_data",
                "to_input": "records",
                "transform": "batch"
            }
        ]
    )

    print(f"Pipeline registered: {composition['composition_id']}")
```

## Schema Management Workflows

### Schema Versioning Strategy

1. **Semantic Versioning**: Use MAJOR.MINOR.PATCH
   - MAJOR: Breaking changes
   - MINOR: New features (backward compatible)
   - PATCH: Bug fixes

2. **Evolution Rules**:
   - Adding optional fields: MINOR version bump
   - Removing fields: MAJOR version bump
   - Changing field types: MAJOR version bump
   - Changing required status: MAJOR version bump

### Schema Evolution Workflow

```python
# Step 1: Check current schema
current_tool = await client.get_tool_by_id("data_processor_v1")
current_schema = current_tool['input_schema']['json_schema']

# Step 2: Propose changes
new_schema = {
    **current_schema,
    "properties": {
        **current_schema["properties"],
        "new_feature": {  # Adding new optional field
            "type": "string",
            "description": "New feature support"
        }
    }
}

# Step 3: Submit proposal
proposal = await client.propose_schema_change(
    tool_id="data_processor_v1",
    change_type="add_property",
    target="input_schema",
    patch={
        "properties": {
            "new_feature": {
                "type": "string",
                "description": "New feature support"
            }
        }
    },
    rationale="Adding support for new processing feature"
)

# Step 4: Wait for approval (if policy requires)
# In enforce mode, proposals need approval
# In warn mode, changes are logged but applied
# In observe mode, changes are applied immediately

# Step 5: Apply approved changes
# This happens automatically when proposal is approved
```

## Discovery Workflows

### Tool Discovery by Capability

```python
# Find tools that can process CSV files
csv_tools = await client.search_tools(
    prompt="Process CSV files",
    filters={
        "visibility": ["public", "organization"],
        "status": "active"
    },
    limit=10
)

# Find tools that can chain after a specific tool
compatible_tools = await client.execute_query("""
    MATCH (t1:Tool {tool_id: $tool_id})-[:HAS_OUTPUT_SCHEMA]->(os:ToolSchema)
    MATCH (t2:Tool)-[:HAS_INPUT_SCHEMA]->(is:ToolSchema)
    WHERE os.json_schema = is.json_schema
    RETURN t2
""", {"tool_id": "csv_reader_v1"})
```

### Dependency Resolution

```python
# Get all dependencies for a tool
dependencies = await client.get_tool_dependencies(
    tool_id="data_processor_v1",
    depth=3,
    include_optional=True
)

# Validate all dependencies are available
for dep in dependencies['dependencies']:
    dep_tool = await client.get_tool_by_id(dep['tool_id'])
    if not dep_tool or dep_tool['tool']['status'] != 'active':
        print(f"Missing or inactive dependency: {dep['tool_id']}")
```

## Execution Tracking Workflow

```python
# Track tool execution for metrics and debugging
async def execute_and_track(tool_id: str, input_data: dict):
    import uuid
    from datetime import datetime

    execution_id = str(uuid.uuid4())
    start_time = datetime.utcnow()

    try:
        # Execute the tool (via Tool Management API)
        result = await tool_management_api.execute_tool(
            tool_id=tool_id,
            input_data=input_data
        )

        # Record successful execution
        await client.record_tool_execution(
            tool_id=tool_id,
            execution_id=execution_id,
            input_data=input_data,
            output_data=result,
            metrics={
                "success": True,
                "duration_ms": (datetime.utcnow() - start_time).total_seconds() * 1000,
                "input_size": len(str(input_data)),
                "output_size": len(str(result))
            },
            context={
                "user_id": "current_user",
                "environment": "production",
                "version": "1.0.0"
            }
        )

        return result

    except Exception as e:
        # Record failed execution
        await client.record_tool_execution(
            tool_id=tool_id,
            execution_id=execution_id,
            input_data=input_data,
            output_data=None,
            metrics={
                "success": False,
                "duration_ms": (datetime.utcnow() - start_time).total_seconds() * 1000,
                "error": str(e)
            }
        )
        raise
```

## Best Practices

### 1. Tool Naming Conventions
- Use lowercase with underscores: `data_processor`
- Include version in tool_id: `data_processor_v1`
- Use descriptive names that indicate function

### 2. Schema Design
- Always include descriptions for properties
- Use enums for known value sets
- Provide examples in schema
- Keep schemas backward compatible when possible

### 3. Error Handling
```python
try:
    result = await client.register_tool(registration)
except HTTPException as e:
    if e.status_code == 409:
        # Tool already exists, check for updates
        existing = await client.get_tool_by_id(tool.tool_id)
        # Handle update logic
    else:
        raise
```

### 4. Testing
```python
# Validate tool chain before deployment
async def test_tool_chain():
    # Validate chain compatibility
    validation = await client.validate_tool_chain(
        tool_ids=["reader_v1", "processor_v1", "writer_v1"],
        check_schema_compatibility=True
    )

    assert validation['is_valid'], "Tool chain validation failed"

    # Test with sample data
    test_input = {"test": "data"}
    for tool_id in validation['tool_chain']:
        tool = await client.get_tool_by_id(tool_id)
        # Validate input against schema
        validate_against_schema(test_input, tool['input_schema'])
```

## Security Considerations

### 1. Authentication
- Use API keys or JWT tokens for service authentication
- Implement capability tokens for fine-grained access control

### 2. Schema Validation
- Always validate inputs against registered schemas
- Prevent schema injection attacks
- Sanitize all user inputs

### 3. Rate Limiting
- Respect rate limits defined in ToolAction
- Implement circuit breakers for failing tools
- Use exponential backoff for retries

## Monitoring and Observability

### 1. Metrics to Track
- Tool registration rate
- Schema evolution frequency
- Execution success/failure rates
- Average execution time
- Tool usage patterns

### 2. Alerts
- Failed tool registrations
- Schema validation failures
- High error rates
- Performance degradation

### 3. Dashboards
```cypher
// Top used tools
MATCH (t:Tool)
RETURN t.name, t.usage_count
ORDER BY t.usage_count DESC
LIMIT 10

// Recent failures
MATCH (e:Execution {success: false})-[:EXECUTED]->(t:Tool)
WHERE e.timestamp > datetime() - duration('PT1H')
RETURN t.name, count(e) as failures
ORDER BY failures DESC

// Schema evolution timeline
MATCH (t:Tool)-[:HAS_PROPOSAL]->(p:SchemaProposal)
RETURN t.name, p.created_at, p.status
ORDER BY p.created_at DESC
```

## Migration Guide

### From File-Based to Graph-Based Registration

1. **Export existing tools**:
```python
# Export from old system
old_tools = load_tools_from_files()

# Transform to graph format
for old_tool in old_tools:
    registration = ToolRegistration(
        tool=transform_to_tool_model(old_tool),
        action=transform_to_action_model(old_tool),
        input_schema=transform_to_schema_model(old_tool['input']),
        output_schema=transform_to_schema_model(old_tool['output'])
    )

    # Register in graph
    await client.register_tool(registration)
```

2. **Update service code**:
```python
# Old way
tool_def = load_tool_from_file("tool.json")

# New way
tool_def = await client.get_tool_by_id("tool_v1")
```

3. **Maintain backward compatibility**:
```python
class CompatibilityLayer:
    async def get_tool(self, name: str):
        # Try graph first
        try:
            return await self.graph_client.get_tool_by_id(name)
        except:
            # Fall back to file system
            return self.load_from_file(name)
```

## Troubleshooting

### Common Issues

1. **Schema Validation Failures**
   - Check JSON schema syntax
   - Ensure all required fields are present
   - Validate enum values

2. **Duplicate Tool Registration**
   - Use unique tool_ids
   - Check for existing tools before registering
   - Use MERGE instead of CREATE in Cypher

3. **Connection Issues**
   - Verify Graph Management API is running
   - Check network connectivity
   - Validate API credentials

### Debug Queries

```cypher
// Find orphaned schemas
MATCH (ts:ToolSchema)
WHERE NOT (()-[:HAS_INPUT_SCHEMA|HAS_OUTPUT_SCHEMA]->(ts))
RETURN ts

// Find tools without actions
MATCH (t:Tool)
WHERE NOT (t)-[:HAS_ACTION]->()
RETURN t

// Check for circular dependencies
MATCH path = (t:Tool)-[:DEPENDS_ON*]->(t)
RETURN path
```

## Appendix

### Complete Example: Weather Service Integration

```python
import asyncio
from graph_management_client import *

async def register_weather_service():
    client = GraphManagementClient()

    # Register weather data fetcher
    weather_tool = ToolRegistration(
        tool=ToolModel(
            tool_id="weather_fetcher_v1",
            name="WeatherFetcher",
            description="Fetches weather data from various sources",
            version="1.0.0",
            service_name="weather_service",
            service_url="http://weather.internal",
            visibility=ToolVisibility.PUBLIC,
            tags=["weather", "data", "api"]
        ),
        action=ToolActionModel(
            action_id="weather_fetcher_v1_action",
            type="http",
            method="GET",
            url="http://weather.internal/api/fetch",
            timeout_ms=5000,
            rate_limit={"requests_per_minute": 60}
        ),
        input_schema=ToolSchemaModel(
            schema_id="weather_fetcher_v1_input",
            schema_type="input",
            json_schema={
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City name or coordinates"
                    },
                    "units": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "default": "celsius"
                    }
                },
                "required": ["location"]
            }
        ),
        output_schema=ToolSchemaModel(
            schema_id="weather_fetcher_v1_output",
            schema_type="output",
            json_schema={
                "type": "object",
                "properties": {
                    "temperature": {"type": "number"},
                    "humidity": {"type": "number"},
                    "conditions": {"type": "string"},
                    "forecast": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "date": {"type": "string"},
                                "high": {"type": "number"},
                                "low": {"type": "number"}
                            }
                        }
                    }
                }
            }
        )
    )

    result = await client.register_tool(weather_tool)
    print(f"Weather service registered: {result}")

    return result

# Run registration
asyncio.run(register_weather_service())
```

This completes the tool registration workflow documentation. The system provides a robust, graph-based approach to tool management with full schema validation, version control, and composition capabilities.