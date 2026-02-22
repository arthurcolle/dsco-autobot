"""
Graph Management Client for Tool Management System

This client provides a specialized interface to the Graph Management API
for tool registration, schema validation, and graph-based orchestration.
"""

from typing import Dict, List, Any, Optional, Literal
from datetime import datetime
import httpx
import logging
from pydantic import BaseModel, Field
import json
import hashlib
from enum import Enum

logger = logging.getLogger(__name__)


class ToolVisibility(str, Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    ORGANIZATION = "organization"


class ToolStatus(str, Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    DISABLED = "disabled"


class CompositionType(str, Enum):
    # Basic patterns
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    LOOP = "loop"
    MAP_REDUCE = "map_reduce"
    PIPELINE = "pipeline"
    WORKFLOW = "workflow"

    # Scale patterns (for hundreds of tools, working around token limits)
    BATCHED = "batched"
    STREAMING = "streaming"
    DELEGATED = "delegated"
    QUEUED = "queued"
    SAGA = "saga"
    SCATTER_GATHER = "scatter_gather"


class ToolModel(BaseModel):
    """Complete tool definition for graph storage"""
    tool_id: str
    name: str
    description: str
    version: str = "1.0.0"
    service_name: str
    service_url: str
    visibility: ToolVisibility = ToolVisibility.PRIVATE
    status: ToolStatus = ToolStatus.ACTIVE
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    organization_id: Optional[str] = None
    user_id: Optional[str] = None
    toolkit_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ToolActionModel(BaseModel):
    """Tool action/endpoint definition"""
    action_id: str
    type: Literal["http", "grpc", "websocket", "lambda"] = "http"
    method: Literal["POST", "GET", "PUT", "DELETE"] = "POST"
    url: str
    timeout_ms: int = 30000
    retry_policy: Dict[str, Any] = Field(default_factory=lambda: {"max_attempts": 3, "backoff_ms": 1000})
    auth_type: Literal["none", "bearer", "api_key", "oauth2"] = "none"
    auth_config: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, str] = Field(default_factory=dict)
    rate_limit: Dict[str, Any] = Field(default_factory=lambda: {"requests_per_minute": 100})
    circuit_breaker: Dict[str, Any] = Field(default_factory=lambda: {"threshold": 0.5, "timeout_ms": 60000})


class ToolSchemaModel(BaseModel):
    """Tool schema definition"""
    schema_id: str
    schema_type: Literal["input", "output"]
    version: str = "1.0.0"
    json_schema: Dict[str, Any]
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)
    examples: List[Any] = Field(default_factory=list)
    embedding: Optional[List[float]] = None


class ToolRegistration(BaseModel):
    """Complete tool registration request"""
    tool: ToolModel
    action: ToolActionModel
    input_schema: ToolSchemaModel
    output_schema: ToolSchemaModel


class GraphManagementClient:
    """
    Client for interacting with the Graph Management API.
    Provides tool-specific operations on top of the generic graph API.
    """

    def __init__(self, base_url: str = "http://localhost:2014", api_key: Optional[str] = None):
        """
        Initialize the Graph Management client.

        Args:
            base_url: Base URL of the Graph Management API
            api_key: Optional API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

    # ==================== Core Graph Operations ====================

    async def execute_query(self, query: str, parameters: Dict[str, Any] = None, strategy: str = "all") -> Dict[str, Any]:
        """Execute a Cypher query against the graph."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/v1/query",
                headers=self.headers,
                json={
                    "query": query,
                    "parameters": parameters or {},
                    "strategy": strategy,
                }
            )
            response.raise_for_status()
            return response.json()

    async def execute_transaction(self, statements: List[Dict[str, Any]], strategy: str = "all") -> Dict[str, Any]:
        """Execute multiple statements in a transaction."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/v1/tx",
                headers=self.headers,
                json={"statements": statements, "strategy": strategy}
            )
            response.raise_for_status()
            return response.json()

    # ==================== Schema Management ====================

    async def register_tool_schemas(self) -> Dict[str, Any]:
        """Register the Tool, ToolAction, and ToolSchema node schemas in the graph."""

        # Define Tool node schema
        tool_schema = {
            "label": "Tool",
            "description": "A registered tool in the system",
            "properties": [
                {"name": "tool_id", "type": "string", "required": True, "unique": True},
                {"name": "name", "type": "string", "required": True, "indexed": True},
                {"name": "description", "type": "string", "required": True},
                {"name": "version", "type": "string", "required": True},
                {"name": "service_name", "type": "string", "required": True},
                {"name": "service_url", "type": "string", "required": True},
                {"name": "visibility", "type": "string", "required": True},
                {"name": "status", "type": "string", "required": True},
                {"name": "created_at", "type": "datetime", "required": True},
                {"name": "updated_at", "type": "datetime", "required": True},
                {"name": "usage_count", "type": "integer", "default": 0},
                {"name": "rating", "type": "float", "minimum": 0, "maximum": 5},
                {"name": "tags", "type": "array"},
                {"name": "category", "type": "string", "indexed": True},
                {"name": "organization_id", "type": "string", "indexed": True},
                {"name": "user_id", "type": "string", "indexed": True},
                {"name": "toolkit_id", "type": "string", "indexed": True},
                {"name": "embedding", "type": "array", "indexed": True}  # Vector for semantic search
            ],
            "constraints": [
                {"type": "unique", "properties": ["tool_id"]},
                {"type": "exists", "properties": ["name"]}
            ]
        }

        # Define ToolAction node schema
        action_schema = {
            "label": "ToolAction",
            "description": "Executable action/endpoint for a tool",
            "properties": [
                {"name": "action_id", "type": "string", "required": True, "unique": True},
                {"name": "type", "type": "string", "required": True},
                {"name": "method", "type": "string", "required": True},
                {"name": "url", "type": "string", "required": True},
                {"name": "timeout_ms", "type": "integer", "default": 30000},
                {"name": "auth_type", "type": "string", "default": "none"},
                {"name": "auth_config", "type": "object"},
                {"name": "headers", "type": "object"},
                {"name": "rate_limit", "type": "object"},
                {"name": "circuit_breaker", "type": "object"},
                {"name": "retry_policy", "type": "object"}
            ]
        }

        # Define ToolSchema node schema
        schema_schema = {
            "label": "ToolSchema",
            "description": "Input or output schema for a tool",
            "properties": [
                {"name": "schema_id", "type": "string", "required": True, "unique": True},
                {"name": "schema_type", "type": "string", "required": True, "enum": ["input", "output"]},
                {"name": "version", "type": "string", "required": True},
                {"name": "json_schema", "type": "object", "required": True},
                {"name": "validation_rules", "type": "array"},
                {"name": "examples", "type": "array"},
                {"name": "created_at", "type": "datetime", "required": True},
                {"name": "embedding", "type": "array"}  # For schema similarity search
            ]
        }

        # Register all schemas
        async with httpx.AsyncClient(timeout=30.0) as client:
            results = {}

            for schema_name, schema_def in [
                ("Tool", tool_schema),
                ("ToolAction", action_schema),
                ("ToolSchema", schema_schema)
            ]:
                response = await client.post(
                    f"{self.base_url}/v1/schemas/nodes",
                    headers=self.headers,
                    json={"spec": schema_def}
                )
                response.raise_for_status()
                results[schema_name] = response.json()

            # Register relationships
            relationships = [
                {
                    "type": "HAS_ACTION",
                    "start_labels": ["Tool"],
                    "end_labels": ["ToolAction"],
                    "description": "Tool has an executable action"
                },
                {
                    "type": "HAS_INPUT_SCHEMA",
                    "start_labels": ["Tool"],
                    "end_labels": ["ToolSchema"],
                    "description": "Tool has input schema"
                },
                {
                    "type": "HAS_OUTPUT_SCHEMA",
                    "start_labels": ["Tool"],
                    "end_labels": ["ToolSchema"],
                    "description": "Tool has output schema"
                },
                {
                    "type": "DEPENDS_ON",
                    "start_labels": ["Tool"],
                    "end_labels": ["Tool"],
                    "description": "Tool dependency"
                },
                {
                    "type": "CAN_CHAIN_TO",
                    "start_labels": ["Tool"],
                    "end_labels": ["Tool"],
                    "properties": [
                        {"name": "mapping", "type": "object"}
                    ],
                    "description": "Tools that can be chained"
                }
            ]

            for rel_def in relationships:
                response = await client.post(
                    f"{self.base_url}/v1/schemas/relationships",
                    headers=self.headers,
                    json={"spec": rel_def}
                )
                response.raise_for_status()
                results[f"rel_{rel_def['type']}"] = response.json()

            return results

    # ==================== Tool Registration ====================

    async def register_tool(self, registration: ToolRegistration) -> Dict[str, Any]:
        """
        Register a complete tool with action and schemas in the graph.

        Args:
            registration: Complete tool registration data

        Returns:
            Registration result with created node IDs
        """
        # Generate IDs if not provided
        if not registration.tool.tool_id:
            registration.tool.tool_id = self._generate_tool_id(registration.tool.name)
        if not registration.action.action_id:
            registration.action.action_id = f"{registration.tool.tool_id}_action"
        if not registration.input_schema.schema_id:
            registration.input_schema.schema_id = f"{registration.tool.tool_id}_input"
        if not registration.output_schema.schema_id:
            registration.output_schema.schema_id = f"{registration.tool.tool_id}_output"

        # Prepare timestamps
        now = datetime.utcnow().isoformat()

        # Build transaction statements
        statements = [
            # Create Tool node
            {
                "query": """
                    MERGE (t:Tool {tool_id: $tool_id})
                    SET t += $properties
                    SET t.created_at = coalesce(t.created_at, $now)
                    SET t.updated_at = $now
                    RETURN t
                """,
                "parameters": {
                    "tool_id": registration.tool.tool_id,
                    "properties": registration.tool.model_dump(exclude={"tool_id"}),
                    "now": now
                }
            },
            # Create ToolAction node
            {
                "query": """
                    MERGE (ta:ToolAction {action_id: $action_id})
                    SET ta += $properties
                    RETURN ta
                """,
                "parameters": {
                    "action_id": registration.action.action_id,
                    "properties": registration.action.model_dump(exclude={"action_id"})
                }
            },
            # Create Input Schema node
            {
                "query": """
                    MERGE (ts:ToolSchema {schema_id: $schema_id})
                    SET ts += $properties
                    SET ts.created_at = coalesce(ts.created_at, $now)
                    RETURN ts
                """,
                "parameters": {
                    "schema_id": registration.input_schema.schema_id,
                    "properties": registration.input_schema.model_dump(exclude={"schema_id"}),
                    "now": now
                }
            },
            # Create Output Schema node
            {
                "query": """
                    MERGE (ts:ToolSchema {schema_id: $schema_id})
                    SET ts += $properties
                    SET ts.created_at = coalesce(ts.created_at, $now)
                    RETURN ts
                """,
                "parameters": {
                    "schema_id": registration.output_schema.schema_id,
                    "properties": registration.output_schema.model_dump(exclude={"schema_id"}),
                    "now": now
                }
            },
            # Create relationships
            {
                "query": """
                    MATCH (t:Tool {tool_id: $tool_id})
                    MATCH (ta:ToolAction {action_id: $action_id})
                    MATCH (tsi:ToolSchema {schema_id: $input_schema_id})
                    MATCH (tso:ToolSchema {schema_id: $output_schema_id})
                    MERGE (t)-[:HAS_ACTION]->(ta)
                    MERGE (t)-[:HAS_INPUT_SCHEMA]->(tsi)
                    MERGE (t)-[:HAS_OUTPUT_SCHEMA]->(tso)
                    RETURN t.tool_id as tool_id
                """,
                "parameters": {
                    "tool_id": registration.tool.tool_id,
                    "action_id": registration.action.action_id,
                    "input_schema_id": registration.input_schema.schema_id,
                    "output_schema_id": registration.output_schema.schema_id
                }
            }
        ]

        # Execute transaction
        result = await self.execute_transaction(statements)

        return {
            "tool_id": registration.tool.tool_id,
            "action_id": registration.action.action_id,
            "input_schema_id": registration.input_schema.schema_id,
            "output_schema_id": registration.output_schema.schema_id,
            "transaction_result": result
        }

    # ==================== Tool Discovery ====================

    async def search_tools(
        self,
        prompt: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for tools using various criteria.

        Args:
            prompt: Natural language search prompt (uses embeddings if available)
            filters: Filter criteria (visibility, organization, category, etc.)
            limit: Maximum number of results

        Returns:
            List of matching tools with their details
        """
        # Build the query based on search criteria
        where_clauses = []
        parameters = {"limit": limit}

        if filters:
            if "visibility" in filters:
                where_clauses.append("t.visibility IN $visibility")
                parameters["visibility"] = filters["visibility"]
            if "organization_id" in filters:
                where_clauses.append("t.organization_id = $org_id")
                parameters["org_id"] = filters["organization_id"]
            if "categories" in filters:
                where_clauses.append("t.category IN $categories")
                parameters["categories"] = filters["categories"]
            if "min_rating" in filters:
                where_clauses.append("t.rating >= $min_rating")
                parameters["min_rating"] = filters["min_rating"]
            if "status" in filters:
                where_clauses.append("t.status = $status")
                parameters["status"] = filters["status"]

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        query = f"""
            MATCH (t:Tool)-[:HAS_ACTION]->(ta:ToolAction)
            MATCH (t)-[:HAS_INPUT_SCHEMA]->(tsi:ToolSchema)
            MATCH (t)-[:HAS_OUTPUT_SCHEMA]->(tso:ToolSchema)
            WHERE {where_clause}
            RETURN t, ta, tsi, tso
            ORDER BY t.usage_count DESC, t.rating DESC
            LIMIT $limit
        """

        result = await self.execute_query(query, parameters)

        # Transform results
        tools = []
        for record in result.get("results", []):
            tools.append({
                "tool": record["t"],
                "action": record["ta"],
                "input_schema": record["tsi"],
                "output_schema": record["tso"]
            })

        return tools

    async def get_tool_by_id(self, tool_id: str) -> Optional[Dict[str, Any]]:
        """Get a complete tool definition by ID."""
        query = """
            MATCH (t:Tool {tool_id: $tool_id})-[:HAS_ACTION]->(ta:ToolAction)
            MATCH (t)-[:HAS_INPUT_SCHEMA]->(tsi:ToolSchema)
            MATCH (t)-[:HAS_OUTPUT_SCHEMA]->(tso:ToolSchema)
            RETURN t, ta, tsi, tso
        """

        result = await self.execute_query(query, {"tool_id": tool_id})

        if result.get("results"):
            record = result["results"][0]
            return {
                "tool": record["t"],
                "action": record["ta"],
                "input_schema": record["tsi"],
                "output_schema": record["tso"]
            }
        return None

    async def get_tool_dependencies(
        self,
        tool_id: str,
        depth: int = 2,
        include_optional: bool = False
    ) -> Dict[str, Any]:
        """Get the dependency graph for a tool."""
        query = """
            MATCH path = (t:Tool {tool_id: $tool_id})-[:DEPENDS_ON*1..$depth]->(dep:Tool)
            RETURN path
        """

        result = await self.execute_query(
            query,
            {"tool_id": tool_id, "depth": depth}
        )

        # Extract dependency information
        dependencies = []
        for record in result.get("results", []):
            # Process path to extract dependency chain
            path_data = record.get("path", {})
            dependencies.append(path_data)

        return {
            "tool_id": tool_id,
            "depth": depth,
            "dependencies": dependencies
        }

    # ==================== Tool Composition ====================

    async def create_tool_composition(
        self,
        name: str,
        composition_type: CompositionType,
        tools: List[Dict[str, Any]],
        connections: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create a tool composition/workflow in the graph."""
        composition_id = self._generate_tool_id(f"composition_{name}")
        now = datetime.utcnow().isoformat()

        statements = [
            # Create Composition node
            {
                "query": """
                    CREATE (c:ToolComposition {
                        composition_id: $composition_id,
                        name: $name,
                        type: $type,
                        created_at: $now
                    })
                    RETURN c
                """,
                "parameters": {
                    "composition_id": composition_id,
                    "name": name,
                    "type": composition_type.value,
                    "now": now
                }
            }
        ]

        # Link tools to composition
        for i, tool_config in enumerate(tools):
            statements.append({
                "query": """
                    MATCH (c:ToolComposition {composition_id: $composition_id})
                    MATCH (t:Tool {tool_id: $tool_id})
                    CREATE (c)-[:CONTAINS {order: $order, config: $config}]->(t)
                """,
                "parameters": {
                    "composition_id": composition_id,
                    "tool_id": tool_config["tool_id"],
                    "order": i,
                    "config": json.dumps(tool_config.get("config", {}))
                }
            })

        # Create connections between tools
        for conn in connections:
            statements.append({
                "query": """
                    MATCH (t1:Tool {tool_id: $from_tool})
                    MATCH (t2:Tool {tool_id: $to_tool})
                    MERGE (t1)-[:FLOWS_TO {
                        from_output: $from_output,
                        to_input: $to_input,
                        transform: $transform
                    }]->(t2)
                """,
                "parameters": {
                    "from_tool": conn["from_tool"],
                    "to_tool": conn["to_tool"],
                    "from_output": conn.get("from_output", "output"),
                    "to_input": conn.get("to_input", "input"),
                    "transform": conn.get("transform", "direct")
                }
            })

        result = await self.execute_transaction(statements)

        return {
            "composition_id": composition_id,
            "name": name,
            "type": composition_type.value,
            "tools": tools,
            "connections": connections,
            "created_at": now
        }

    async def validate_tool_chain(
        self,
        tool_ids: List[str],
        check_schema_compatibility: bool = True
    ) -> Dict[str, Any]:
        """Validate that a chain of tools is compatible."""
        validations = []

        for i in range(len(tool_ids) - 1):
            from_tool = tool_ids[i]
            to_tool = tool_ids[i + 1]

            if check_schema_compatibility:
                # Check if output schema of from_tool matches input schema of to_tool
                query = """
                    MATCH (t1:Tool {tool_id: $from_tool})-[:HAS_OUTPUT_SCHEMA]->(os:ToolSchema)
                    MATCH (t2:Tool {tool_id: $to_tool})-[:HAS_INPUT_SCHEMA]->(is:ToolSchema)
                    RETURN
                        os.json_schema as output_schema,
                        is.json_schema as input_schema,
                        os.schema_id = is.schema_id as exact_match
                """

                result = await self.execute_query(
                    query,
                    {"from_tool": from_tool, "to_tool": to_tool}
                )

                if result.get("results"):
                    record = result["results"][0]
                    compatible = record.get("exact_match", False)

                    # If not exact match, check if schemas are compatible
                    if not compatible:
                        # Implement schema compatibility check logic
                        compatible = self._check_schema_compatibility(
                            record["output_schema"],
                            record["input_schema"]
                        )

                    validations.append({
                        "from": from_tool,
                        "to": to_tool,
                        "compatible": compatible,
                        "reason": "Schema compatibility check"
                    })
                else:
                    validations.append({
                        "from": from_tool,
                        "to": to_tool,
                        "compatible": False,
                        "reason": "Tools or schemas not found"
                    })

        is_valid = all(v["compatible"] for v in validations)

        return {
            "tool_chain": tool_ids,
            "is_valid": is_valid,
            "validations": validations
        }

    # ==================== Execution Tracking ====================

    async def record_tool_execution(
        self,
        tool_id: str,
        execution_id: str,
        input_data: Any,
        output_data: Any,
        metrics: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Record a tool execution in the graph."""
        now = datetime.utcnow().isoformat()

        query = """
            MATCH (t:Tool {tool_id: $tool_id})
            CREATE (e:Execution {
                execution_id: $execution_id,
                timestamp: $now,
                metrics: $metrics,
                context: $context,
                success: $success
            })
            CREATE (e)-[:EXECUTED]->(t)

            // Update tool usage count
            SET t.usage_count = coalesce(t.usage_count, 0) + 1
            SET t.last_used_at = $now

            RETURN e, t.usage_count as usage_count
        """

        parameters = {
            "tool_id": tool_id,
            "execution_id": execution_id,
            "now": now,
            "metrics": json.dumps(metrics),
            "context": json.dumps(context or {}),
            "success": metrics.get("success", True)
        }

        result = await self.execute_query(query, parameters)

        return {
            "execution_id": execution_id,
            "tool_id": tool_id,
            "timestamp": now,
            "usage_count": result["results"][0]["usage_count"] if result.get("results") else 0
        }

    # ==================== Helper Methods ====================

    def _generate_tool_id(self, name: str) -> str:
        """Generate a unique tool ID from name."""
        timestamp = datetime.utcnow().isoformat()
        return hashlib.sha256(f"{name}_{timestamp}".encode()).hexdigest()[:16]

    def _check_schema_compatibility(
        self,
        output_schema: Dict[str, Any],
        input_schema: Dict[str, Any]
    ) -> bool:
        """
        Check if an output schema is compatible with an input schema.

        This is a simplified check. In production, you'd want more sophisticated
        schema validation using JSON Schema validators.
        """
        # Basic compatibility: check if output properties exist in input
        output_props = output_schema.get("properties", {})
        input_props = input_schema.get("properties", {})

        # Check if all required input properties can be satisfied by output
        input_required = input_schema.get("required", [])

        for prop in input_required:
            if prop not in output_props:
                return False

        # Check type compatibility for common properties
        for prop_name, input_prop_def in input_props.items():
            if prop_name in output_props:
                output_prop_def = output_props[prop_name]
                if input_prop_def.get("type") != output_prop_def.get("type"):
                    return False

        return True

    # ==================== Schema Evolution ====================

    async def propose_schema_change(
        self,
        tool_id: str,
        change_type: str,
        target: Literal["input_schema", "output_schema"],
        patch: Dict[str, Any],
        rationale: str
    ) -> Dict[str, Any]:
        """Propose a schema change for a tool."""
        proposal_id = self._generate_tool_id(f"proposal_{tool_id}")
        now = datetime.utcnow().isoformat()

        query = """
            MATCH (t:Tool {tool_id: $tool_id})
            CREATE (p:SchemaProposal {
                proposal_id: $proposal_id,
                tool_id: $tool_id,
                change_type: $change_type,
                target: $target,
                patch: $patch,
                rationale: $rationale,
                status: 'pending',
                created_at: $now
            })
            CREATE (t)-[:HAS_PROPOSAL]->(p)
            RETURN p
        """

        result = await self.execute_query(
            query,
            {
                "tool_id": tool_id,
                "proposal_id": proposal_id,
                "change_type": change_type,
                "target": target,
                "patch": json.dumps(patch),
                "rationale": rationale,
                "now": now
            }
        )

        return {
            "proposal_id": proposal_id,
            "tool_id": tool_id,
            "status": "pending",
            "created_at": now
        }

    async def get_schema_evolution_path(
        self,
        tool_id: str,
        from_version: str,
        to_version: str
    ) -> Dict[str, Any]:
        """Get the migration path between schema versions."""
        query = """
            MATCH (t:Tool {tool_id: $tool_id})-[:HAS_INPUT_SCHEMA]->(s1:ToolSchema {version: $from_version})
            MATCH (t)-[:HAS_INPUT_SCHEMA]->(s2:ToolSchema {version: $to_version})
            MATCH path = shortestPath((s1)-[:EVOLVED_TO*]->(s2))
            RETURN path
        """

        result = await self.execute_query(
            query,
            {
                "tool_id": tool_id,
                "from_version": from_version,
                "to_version": to_version
            }
        )

        if result.get("results"):
            return {
                "tool_id": tool_id,
                "from_version": from_version,
                "to_version": to_version,
                "migration_path": result["results"][0].get("path")
            }

        return {
            "tool_id": tool_id,
            "from_version": from_version,
            "to_version": to_version,
            "migration_path": None,
            "error": "No migration path found"
        }


# Example usage
if __name__ == "__main__":
    import asyncio

    async def main():
        # Initialize client
        client = GraphManagementClient(base_url="http://localhost:2014")

        # Register schemas first
        print("Registering tool schemas...")
        schema_result = await client.register_tool_schemas()
        print(f"Schemas registered: {schema_result.keys()}")

        # Register a sample tool
        tool = ToolModel(
            tool_id="csv_processor_001",
            name="csv_processor",
            description="Process CSV files",
            version="1.0.0",
            service_name="data_service",
            service_url="http://data-service.internal",
            visibility=ToolVisibility.PUBLIC,
            tags=["data", "csv", "processing"]
        )

        action = ToolActionModel(
            action_id="csv_processor_001_action",
            type="http",
            method="POST",
            url="http://data-service.internal/process_csv"
        )

        input_schema = ToolSchemaModel(
            schema_id="csv_processor_001_input",
            schema_type="input",
            json_schema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string"},
                    "delimiter": {"type": "string", "default": ","}
                },
                "required": ["file_path"]
            }
        )

        output_schema = ToolSchemaModel(
            schema_id="csv_processor_001_output",
            schema_type="output",
            json_schema={
                "type": "object",
                "properties": {
                    "rows_processed": {"type": "integer"},
                    "output_path": {"type": "string"}
                }
            }
        )

        registration = ToolRegistration(
            tool=tool,
            action=action,
            input_schema=input_schema,
            output_schema=output_schema
        )

        print("\nRegistering tool...")
        result = await client.register_tool(registration)
        print(f"Tool registered: {result['tool_id']}")

        # Search for tools
        print("\nSearching for tools...")
        tools = await client.search_tools(
            filters={"visibility": ["public"]},
            limit=5
        )
        print(f"Found {len(tools)} tools")

        # Get tool by ID
        print(f"\nRetrieving tool {result['tool_id']}...")
        tool_data = await client.get_tool_by_id(result['tool_id'])
        if tool_data:
            print(f"Tool retrieved: {tool_data['tool']['name']}")

    # Run the example
    asyncio.run(main())