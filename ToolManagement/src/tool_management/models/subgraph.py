"""
Tool Subgraph Models - Graph-aware models for Tool-ToolSchema-ToolOutput-ToolAction relationships.

This module defines the complete subgraph structure for tools:
- ToolNode: Core tool identity and metadata
- ToolSchemaNode: Input/output JSON schemas
- ToolOutputNode: Output type and structure
- ToolActionNode: Execution configuration (HTTP, MCP, Code, etc.)

Relationships:
- (Tool)-[:HAS_SCHEMA]->(ToolSchema)
- (Tool)-[:HAS_OUTPUT]->(ToolOutput)
- (Tool)-[:HAS_ACTION]->(ToolAction)
"""

from __future__ import annotations

import json
import os
import uuid
import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# =============================================================================
# ENUMS
# =============================================================================

class TransportType(str, Enum):
    """Transport protocol types."""
    HTTP = "http"
    HTTPS = "https"
    WEBSOCKET = "websocket"
    WSS = "wss"
    GRPC = "grpc"
    STDIO = "stdio"
    SSE = "sse"


class ActionType(str, Enum):
    """Type of tool action/execution method."""
    HTTP = "http"
    GRPC = "grpc"
    WEBSOCKET = "websocket"
    LAMBDA = "lambda"
    CODE = "code"
    MCP = "mcp"


class OutputType(str, Enum):
    """Type of tool output."""
    AI = "ai"
    JSON = "json"
    TEXT = "text"
    BINARY = "binary"
    STREAM = "stream"


class ParameterType(str, Enum):
    """JSON Schema parameter types."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    NULL = "null"


class RelationshipType(str, Enum):
    """Relationship types in the tool subgraph."""
    HAS_SCHEMA = "HAS_SCHEMA"
    HAS_OUTPUT = "HAS_OUTPUT"
    HAS_ACTION = "HAS_ACTION"
    HAS_CODE = "HAS_CODE"
    HAS_TRANSPORT = "HAS_TRANSPORT"
    HAS_INPUT_PARAM = "HAS_INPUT_PARAM"
    HAS_OUTPUT_FIELD = "HAS_OUTPUT_FIELD"
    DEPENDS_ON = "DEPENDS_ON"
    COMPOSED_OF = "COMPOSED_OF"
    HOSTED_AT = "HOSTED_AT"


# =============================================================================
# BASE GRAPH MODEL
# =============================================================================

def _utcnow() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


class GraphNode(BaseModel):
    """Base class for graph nodes with common properties."""

    node_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    class Config:
        extra = "allow"

    def to_neo4j_properties(self) -> Dict[str, Any]:
        """Convert to Neo4j-safe property dict."""
        props = {}
        for key, value in self.model_dump().items():
            if isinstance(value, datetime):
                props[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                props[key] = json.dumps(value)
            elif isinstance(value, Enum):
                props[key] = value.value
            elif value is not None:
                props[key] = value
        return props

    @classmethod
    def get_label(cls) -> str:
        """Get Neo4j node label."""
        return cls.__name__.replace("Node", "")


class GraphRelationship(BaseModel):
    """Represents a relationship/edge in the graph."""

    rel_type: RelationshipType
    from_id: str
    to_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)


# =============================================================================
# TOOL SCHEMA NODE
# =============================================================================

class ToolSchemaNode(GraphNode):
    """
    Schema node containing input and output JSON schemas.

    Stores the structural definitions for tool inputs and outputs,
    following JSON Schema specification.
    """

    input_schema: Dict[str, Any] = Field(
        default_factory=lambda: {"type": "object", "properties": {}, "required": []},
        description="JSON Schema for tool inputs"
    )
    output_schema: Dict[str, Any] = Field(
        default_factory=lambda: {"type": "object"},
        description="JSON Schema for tool outputs"
    )
    input_model_name: Optional[str] = Field(None, description="Pydantic model name for input")
    output_model_name: Optional[str] = Field(None, description="Pydantic model name for output")
    schema_version: str = Field("draft-07", description="JSON Schema version")
    schema_hash: Optional[str] = Field(None, description="Hash of combined schemas")

    def compute_hash(self) -> str:
        """Compute hash of the schema content."""
        content = json.dumps({
            "input": self.input_schema,
            "output": self.output_schema
        }, sort_keys=True)
        self.schema_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        return self.schema_hash

    def get_required_inputs(self) -> List[str]:
        """Get list of required input parameters."""
        return self.input_schema.get("required", [])

    def get_input_properties(self) -> Dict[str, Any]:
        """Get input property definitions."""
        return self.input_schema.get("properties", {})


# =============================================================================
# TOOL OUTPUT NODE
# =============================================================================

class ToolOutputNode(GraphNode):
    """
    Output configuration node for a tool.

    Defines how tool output should be processed and presented.
    """

    output_type: OutputType = Field(OutputType.AI, description="Type of output")
    content_template: Optional[str] = Field(None, description="Template for output content")
    format_instructions: Optional[str] = Field(None, description="Instructions for formatting")
    post_processors: List[str] = Field(default_factory=list, description="Post-processing steps")
    streaming_enabled: bool = Field(False, description="Whether streaming is supported")
    max_tokens: Optional[int] = Field(None, description="Max output tokens (for AI type)")


# =============================================================================
# TRANSPORT NODE
# =============================================================================

class TransportNode(GraphNode):
    """
    Transport/location node for service connectivity.

    Stores the connection details for reaching a tool's service:
    - Host (localhost, api.example.com)
    - Port (8787, 443, etc.)
    - Protocol (http, https, websocket, grpc)
    - Path prefix (/api/v1, /mcp, etc.)
    """

    transport_type: TransportType = Field(TransportType.HTTP, description="Transport protocol")
    host: str = Field(default="localhost", description="Service host")
    port: int = Field(default=8080, description="Service port")
    path_prefix: str = Field(default="", description="Base path prefix")
    is_secure: bool = Field(default=False, description="Whether TLS/SSL is used")

    # Connection settings
    connect_timeout_ms: int = Field(default=5000, description="Connection timeout")
    read_timeout_ms: int = Field(default=30000, description="Read timeout")
    max_retries: int = Field(default=3, description="Max connection retries")

    # Health check
    health_endpoint: Optional[str] = Field(None, description="Health check endpoint path")
    last_health_check: Optional[datetime] = Field(None, description="Last health check time")
    is_healthy: bool = Field(default=True, description="Current health status")

    # Authentication
    auth_type: Optional[str] = Field(None, description="Auth type (bearer, api_key, basic)")
    auth_header: Optional[str] = Field(None, description="Auth header name")

    @property
    def base_url(self) -> str:
        """Get the full base URL for this transport."""
        protocol = self.transport_type.value
        if self.is_secure and protocol == "http":
            protocol = "https"
        elif self.is_secure and protocol == "websocket":
            protocol = "wss"

        port_str = f":{self.port}" if self.port not in (80, 443) else ""
        path = self.path_prefix.rstrip("/") if self.path_prefix else ""
        return f"{protocol}://{self.host}{port_str}{path}"

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "TransportNode":
        """Create TransportNode from a URL string."""
        from urllib.parse import urlparse
        parsed = urlparse(url)

        # Determine transport type
        scheme = parsed.scheme.lower()
        transport_map = {
            "http": TransportType.HTTP,
            "https": TransportType.HTTPS,
            "ws": TransportType.WEBSOCKET,
            "wss": TransportType.WSS,
            "grpc": TransportType.GRPC,
        }
        transport_type = transport_map.get(scheme, TransportType.HTTP)
        is_secure = scheme in ("https", "wss")

        # Extract port
        port = parsed.port
        if port is None:
            port = 443 if is_secure else 80

        return cls(
            transport_type=transport_type,
            host=parsed.hostname or "localhost",
            port=port,
            path_prefix=parsed.path or "",
            is_secure=is_secure,
            **kwargs
        )


# =============================================================================
# INPUT PARAMETER NODE
# =============================================================================

class InputParameterNode(GraphNode):
    """
    Individual input parameter node.

    Represents a single input parameter with full schema details.
    Allows graph-based traversal of tool parameters.
    """

    name: str = Field(..., description="Parameter name")
    param_type: ParameterType = Field(ParameterType.STRING, description="Parameter type")
    description: str = Field(default="", description="Parameter description")
    required: bool = Field(default=False, description="Whether parameter is required")

    # Default and constraints
    default_value: Optional[Any] = Field(None, description="Default value")
    enum_values: Optional[List[Any]] = Field(None, description="Allowed enum values")

    # Numeric constraints
    minimum: Optional[float] = Field(None, description="Minimum value (for numbers)")
    maximum: Optional[float] = Field(None, description="Maximum value (for numbers)")

    # String constraints
    min_length: Optional[int] = Field(None, description="Min string length")
    max_length: Optional[int] = Field(None, description="Max string length")
    pattern: Optional[str] = Field(None, description="Regex pattern for validation")
    format: Optional[str] = Field(None, description="String format (email, uri, date, etc.)")

    # Array constraints
    items_type: Optional[ParameterType] = Field(None, description="Array item type")
    min_items: Optional[int] = Field(None, description="Min array items")
    max_items: Optional[int] = Field(None, description="Max array items")

    # Object constraints
    properties_schema: Optional[Dict[str, Any]] = Field(None, description="Nested object schema")

    # Order
    position: int = Field(default=0, description="Parameter position/order")

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema property definition."""
        schema: Dict[str, Any] = {"type": self.param_type.value}

        if self.description:
            schema["description"] = self.description
        if self.default_value is not None:
            schema["default"] = self.default_value
        if self.enum_values:
            schema["enum"] = self.enum_values
        if self.minimum is not None:
            schema["minimum"] = self.minimum
        if self.maximum is not None:
            schema["maximum"] = self.maximum
        if self.min_length is not None:
            schema["minLength"] = self.min_length
        if self.max_length is not None:
            schema["maxLength"] = self.max_length
        if self.pattern:
            schema["pattern"] = self.pattern
        if self.format:
            schema["format"] = self.format
        if self.items_type:
            schema["items"] = {"type": self.items_type.value}
        if self.min_items is not None:
            schema["minItems"] = self.min_items
        if self.max_items is not None:
            schema["maxItems"] = self.max_items
        if self.properties_schema:
            schema["properties"] = self.properties_schema

        return schema


# =============================================================================
# OUTPUT FIELD NODE
# =============================================================================

class OutputFieldNode(GraphNode):
    """
    Individual output field node.

    Represents a single output field with full schema details.
    """

    name: str = Field(..., description="Field name")
    field_type: ParameterType = Field(ParameterType.STRING, description="Field type")
    description: str = Field(default="", description="Field description")
    nullable: bool = Field(default=True, description="Whether field can be null")

    # Nested schema
    items_type: Optional[ParameterType] = Field(None, description="Array item type")
    properties_schema: Optional[Dict[str, Any]] = Field(None, description="Nested object schema")

    # Position
    position: int = Field(default=0, description="Field position in response")

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema property definition."""
        schema: Dict[str, Any] = {"type": self.field_type.value}

        if self.description:
            schema["description"] = self.description
        if self.items_type:
            schema["items"] = {"type": self.items_type.value}
        if self.properties_schema:
            schema["properties"] = self.properties_schema

        return schema


# =============================================================================
# TOOL ACTION NODE
# =============================================================================

class HTTPActionConfig(BaseModel):
    """Configuration for HTTP-based tool actions."""
    method: str = Field(default="POST", description="HTTP method")
    url: str = Field(default="", description="Endpoint URL")
    headers: Dict[str, str] = Field(default_factory=lambda: {"Content-Type": "application/json"})
    timeout_ms: int = Field(default=30000, description="Request timeout in milliseconds")
    retry_count: int = Field(default=0, description="Number of retries on failure")
    body_template: Optional[Dict[str, Any]] = Field(default=None, description="Request body template")


class MCPActionConfig(BaseModel):
    """Configuration for MCP-based tool actions."""
    server_url: str = Field(default="", description="MCP server URL")
    tool_name: str = Field(default="", description="Tool name on MCP server")
    timeout_ms: int = Field(default=60000, description="MCP call timeout")
    headers: Dict[str, str] = Field(default_factory=dict)


class CodeActionConfig(BaseModel):
    """Configuration for code snippet tool actions."""
    code: str = Field(default="", description="Executable code snippet")
    language: str = Field(default="python", description="Programming language")
    runtime: str = Field(default="sandbox", description="Execution runtime")
    code_hash: Optional[str] = Field(default=None, description="Hash of code for versioning")
    dependencies: List[str] = Field(default_factory=list, description="Required dependencies")


class ToolActionNode(GraphNode):
    """
    Action node defining how a tool is executed.

    Supports multiple execution types: HTTP, MCP, Code, Lambda, etc.
    """

    action_type: ActionType = Field(ActionType.HTTP, description="Type of action")
    http_config: Optional[HTTPActionConfig] = Field(None, description="HTTP action configuration")
    mcp_config: Optional[MCPActionConfig] = Field(None, description="MCP action configuration")
    code_config: Optional[CodeActionConfig] = Field(None, description="Code action configuration")
    lambda_arn: Optional[str] = Field(None, description="AWS Lambda ARN for lambda type")
    grpc_endpoint: Optional[str] = Field(None, description="gRPC endpoint for grpc type")
    fallback_action_id: Optional[str] = Field(None, description="Fallback action if primary fails")

    @classmethod
    def from_http(cls, url: str, method: str = "POST", **kwargs) -> "ToolActionNode":
        """Create HTTP action node."""
        return cls(
            action_type=ActionType.HTTP,
            http_config=HTTPActionConfig(url=url, method=method, **kwargs)
        )

    @classmethod
    def from_mcp(cls, server_url: str, tool_name: str, **kwargs) -> "ToolActionNode":
        """Create MCP action node."""
        return cls(
            action_type=ActionType.MCP,
            mcp_config=MCPActionConfig(server_url=server_url, tool_name=tool_name, **kwargs)
        )

    @classmethod
    def from_code(cls, code: str, language: str = "python", **kwargs) -> "ToolActionNode":
        """Create code snippet action node."""
        code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]
        return cls(
            action_type=ActionType.CODE,
            code_config=CodeActionConfig(code=code, language=language, code_hash=code_hash, **kwargs)
        )


# =============================================================================
# TOOL NODE
# =============================================================================

class ToolNode(GraphNode):
    """
    Core tool node containing identity and metadata.

    This is the central node in the tool subgraph, connected to:
    - ToolSchemaNode via HAS_SCHEMA
    - ToolOutputNode via HAS_OUTPUT
    - ToolActionNode via HAS_ACTION
    - TransportNode via HAS_TRANSPORT
    - InputParameterNode(s) via HAS_INPUT_PARAM
    - OutputFieldNode(s) via HAS_OUTPUT_FIELD
    """

    name: str = Field(..., description="Unique tool name")
    description: str = Field(..., description="Tool description")
    version: str = Field("1.0.0", description="Semantic version")

    # Categorization
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = Field(None)

    # Ownership
    service_name: Optional[str] = Field(None)
    service_url: Optional[str] = Field(None)
    user_id: Optional[str] = Field(None)
    organization_id: Optional[str] = Field(None)

    # Status
    is_active: bool = Field(True)
    is_deprecated: bool = Field(False)

    # Stats
    usage_count: int = Field(0)
    avg_latency_ms: Optional[float] = Field(None)
    success_rate: Optional[float] = Field(None)

    # Related node IDs (for graph traversal)
    schema_id: Optional[str] = Field(None)
    output_id: Optional[str] = Field(None)
    action_id: Optional[str] = Field(None)
    transport_id: Optional[str] = Field(None)
    input_param_ids: List[str] = Field(default_factory=list)
    output_field_ids: List[str] = Field(default_factory=list)


# =============================================================================
# TOOL SUBGRAPH
# =============================================================================

class ToolSubgraph(BaseModel):
    """
    Complete tool subgraph containing all related nodes and relationships.

    This is the primary structure for persisting tool definitions to a graph database.

    Structure:
        Tool -[HAS_SCHEMA]-> ToolSchema
        Tool -[HAS_OUTPUT]-> ToolOutput
        Tool -[HAS_ACTION]-> ToolAction
        Tool -[HAS_TRANSPORT]-> Transport
        Tool -[HAS_INPUT_PARAM]-> InputParameter (multiple)
        Tool -[HAS_OUTPUT_FIELD]-> OutputField (multiple)
    """

    tool: ToolNode
    tool_schema: ToolSchemaNode
    tool_output: ToolOutputNode
    tool_action: ToolActionNode
    transport: Optional[TransportNode] = Field(None, description="Service transport/location")
    input_params: List[InputParameterNode] = Field(default_factory=list, description="Input parameters")
    output_fields: List[OutputFieldNode] = Field(default_factory=list, description="Output fields")
    relationships: List[GraphRelationship] = Field(default_factory=list)

    def model_post_init(self, _context: Any) -> None:
        """Build relationships after model initialization."""
        del _context  # unused
        self._build_relationships()

    def _build_relationships(self):
        """Build relationships between nodes."""
        self.relationships = [
            GraphRelationship(
                rel_type=RelationshipType.HAS_SCHEMA,
                from_id=self.tool.node_id,
                to_id=self.tool_schema.node_id
            ),
            GraphRelationship(
                rel_type=RelationshipType.HAS_OUTPUT,
                from_id=self.tool.node_id,
                to_id=self.tool_output.node_id
            ),
            GraphRelationship(
                rel_type=RelationshipType.HAS_ACTION,
                from_id=self.tool.node_id,
                to_id=self.tool_action.node_id
            ),
        ]

        # Update tool with related IDs
        self.tool.schema_id = self.tool_schema.node_id
        self.tool.output_id = self.tool_output.node_id
        self.tool.action_id = self.tool_action.node_id

        # Add transport relationship if present
        if self.transport:
            self.relationships.append(GraphRelationship(
                rel_type=RelationshipType.HAS_TRANSPORT,
                from_id=self.tool.node_id,
                to_id=self.transport.node_id
            ))
            self.tool.transport_id = self.transport.node_id

        # Add input parameter relationships
        for param in self.input_params:
            self.relationships.append(GraphRelationship(
                rel_type=RelationshipType.HAS_INPUT_PARAM,
                from_id=self.tool.node_id,
                to_id=param.node_id,
                properties={"position": param.position, "required": param.required}
            ))
            self.tool.input_param_ids.append(param.node_id)

        # Add output field relationships
        for field in self.output_fields:
            self.relationships.append(GraphRelationship(
                rel_type=RelationshipType.HAS_OUTPUT_FIELD,
                from_id=self.tool.node_id,
                to_id=field.node_id,
                properties={"position": field.position}
            ))
            self.tool.output_field_ids.append(field.node_id)

    @classmethod
    def create(
        cls,
        name: str,
        description: str,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        action_type: ActionType = ActionType.HTTP,
        action_config: Optional[Dict[str, Any]] = None,
        service_url: Optional[str] = None,
        input_params: Optional[List[Dict[str, Any]]] = None,
        output_fields: Optional[List[Dict[str, Any]]] = None,
        **tool_kwargs
    ) -> "ToolSubgraph":
        """
        Factory method to create a complete tool subgraph.

        Args:
            name: Tool name
            description: Tool description
            input_schema: JSON schema for inputs (alternative to input_params)
            output_schema: JSON schema for outputs (alternative to output_fields)
            action_type: Type of action (HTTP, MCP, CODE, etc.)
            action_config: Configuration for the action
            service_url: Service URL (e.g., "http://localhost:8787")
            input_params: List of input parameter definitions
            output_fields: List of output field definitions
            **tool_kwargs: Additional tool node properties

        Returns:
            Complete ToolSubgraph instance
        """
        # Create schema node
        tool_schema = ToolSchemaNode(
            input_schema=input_schema or {"type": "object", "properties": {}, "required": []},
            output_schema=output_schema or {"type": "object"}
        )
        tool_schema.compute_hash()

        # Create output node
        tool_output = ToolOutputNode()

        # Create action node
        action_config = action_config or {}
        if action_type == ActionType.HTTP and action_config:
            tool_action = ToolActionNode.from_http(**action_config)
        elif action_type == ActionType.MCP and action_config:
            tool_action = ToolActionNode.from_mcp(**action_config)
        elif action_type == ActionType.CODE and action_config:
            tool_action = ToolActionNode.from_code(**action_config)
        else:
            tool_action = ToolActionNode(action_type=action_type)

        # Create transport node from service URL
        transport = None
        if service_url:
            transport = TransportNode.from_url(service_url)

        # Create input parameter nodes
        param_nodes: List[InputParameterNode] = []
        if input_params:
            for i, param in enumerate(input_params):
                param_type = ParameterType(param.get("type", "string"))
                param_nodes.append(InputParameterNode(
                    name=param["name"],
                    param_type=param_type,
                    description=param.get("description", ""),
                    required=param.get("required", False),
                    default_value=param.get("default"),
                    enum_values=param.get("enum"),
                    minimum=param.get("minimum"),
                    maximum=param.get("maximum"),
                    min_length=param.get("minLength"),
                    max_length=param.get("maxLength"),
                    pattern=param.get("pattern"),
                    format=param.get("format"),
                    position=i
                ))

        # Create output field nodes
        field_nodes: List[OutputFieldNode] = []
        if output_fields:
            for i, field in enumerate(output_fields):
                field_type = ParameterType(field.get("type", "string"))
                field_nodes.append(OutputFieldNode(
                    name=field["name"],
                    field_type=field_type,
                    description=field.get("description", ""),
                    nullable=field.get("nullable", True),
                    position=i
                ))

        # Create tool node
        tool = ToolNode(
            name=name,
            description=description,
            **tool_kwargs
        )

        return cls(
            tool=tool,
            tool_schema=tool_schema,
            tool_output=tool_output,
            tool_action=tool_action,
            transport=transport,
            input_params=param_nodes,
            output_fields=field_nodes
        )

    def to_cypher_create(self) -> tuple[str, Dict[str, Any]]:
        """
        Generate Cypher CREATE query for Neo4j.

        Returns:
            Tuple of (query_string, parameters)
        """
        query = """
        CREATE (t:Tool $tool_props)
        CREATE (s:ToolSchema $schema_props)
        CREATE (o:ToolOutput $output_props)
        CREATE (a:ToolAction $action_props)
        CREATE (t)-[:HAS_SCHEMA]->(s)
        CREATE (t)-[:HAS_OUTPUT]->(o)
        CREATE (t)-[:HAS_ACTION]->(a)
        RETURN t, s, o, a
        """

        params = {
            "tool_props": self.tool.to_neo4j_properties(),
            "schema_props": self.tool_schema.to_neo4j_properties(),
            "output_props": self.tool_output.to_neo4j_properties(),
            "action_props": self.tool_action.to_neo4j_properties(),
        }

        return query, params

    def to_cypher_merge(self) -> tuple[str, Dict[str, Any]]:
        """
        Generate Cypher MERGE query for Neo4j (upsert behavior).

        Returns:
            Tuple of (query_string, parameters)
        """
        query = """
        MERGE (t:Tool {name: $tool_name})
        ON CREATE SET t = $tool_props
        ON MATCH SET t += $tool_props

        MERGE (s:ToolSchema {node_id: $schema_id})
        ON CREATE SET s = $schema_props
        ON MATCH SET s += $schema_props

        MERGE (o:ToolOutput {node_id: $output_id})
        ON CREATE SET o = $output_props
        ON MATCH SET o += $output_props

        MERGE (a:ToolAction {node_id: $action_id})
        ON CREATE SET a = $action_props
        ON MATCH SET a += $action_props

        MERGE (t)-[:HAS_SCHEMA]->(s)
        MERGE (t)-[:HAS_OUTPUT]->(o)
        MERGE (t)-[:HAS_ACTION]->(a)

        RETURN t, s, o, a
        """

        params = {
            "tool_name": self.tool.name,
            "schema_id": self.tool_schema.node_id,
            "output_id": self.tool_output.node_id,
            "action_id": self.tool_action.node_id,
            "tool_props": self.tool.to_neo4j_properties(),
            "schema_props": self.tool_schema.to_neo4j_properties(),
            "output_props": self.tool_output.to_neo4j_properties(),
            "action_props": self.tool_action.to_neo4j_properties(),
        }

        return query, params

    def to_dict(self) -> Dict[str, Any]:
        """Export subgraph as a dictionary."""
        result = {
            "tool": self.tool.model_dump(),
            "tool_schema": self.tool_schema.model_dump(),
            "tool_output": self.tool_output.model_dump(),
            "tool_action": self.tool_action.model_dump(),
            "relationships": [r.model_dump() for r in self.relationships],
        }
        if self.transport:
            result["transport"] = self.transport.model_dump()
        if self.input_params:
            result["input_params"] = [p.model_dump() for p in self.input_params]
        if self.output_fields:
            result["output_fields"] = [f.model_dump() for f in self.output_fields]
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ToolSubgraph":
        """Create subgraph from dictionary."""
        # Deserialize transport if present
        transport = None
        if "transport" in data:
            transport = TransportNode(**data["transport"])

        # Deserialize input parameters if present
        input_params = []
        if "input_params" in data:
            for p in data["input_params"]:
                input_params.append(InputParameterNode(**p))

        # Deserialize output fields if present
        output_fields = []
        if "output_fields" in data:
            for f in data["output_fields"]:
                output_fields.append(OutputFieldNode(**f))

        return cls(
            tool=ToolNode(**data["tool"]),
            tool_schema=ToolSchemaNode(**data.get("tool_schema", data.get("schema", {}))),
            tool_output=ToolOutputNode(**data.get("tool_output", data.get("output", {}))),
            tool_action=ToolActionNode(**data.get("tool_action", data.get("action", {}))),
            transport=transport,
            input_params=input_params,
            output_fields=output_fields,
        )


# =============================================================================
# SUBGRAPH REPOSITORY
# =============================================================================

class ToolSubgraphRepository:
    """
    Repository for persisting tool subgraphs to various backends.

    Supports:
    - Neo4j (via neo4j driver)
    - In-memory storage
    - File-based JSON storage
    """

    def __init__(
        self,
        neo4j_driver=None,
        storage_path: Optional[str] = None
    ):
        self._driver = neo4j_driver
        self._storage_path = storage_path
        self._memory_store: Dict[str, ToolSubgraph] = {}

    async def save(self, subgraph: ToolSubgraph, upsert: bool = True) -> str:
        """
        Save a tool subgraph.

        Args:
            subgraph: The subgraph to save
            upsert: If True, use MERGE (update if exists)

        Returns:
            Tool node ID
        """
        # Save to memory
        self._memory_store[subgraph.tool.name] = subgraph

        # Save to Neo4j if driver available
        if self._driver:
            await self._save_to_neo4j(subgraph, upsert)

        # Save to file if path configured
        if self._storage_path:
            self._save_to_file(subgraph)

        return subgraph.tool.node_id

    async def _save_to_neo4j(self, subgraph: ToolSubgraph, upsert: bool = True):
        """Save subgraph to Neo4j."""
        if not self._driver:
            return

        if upsert:
            query, params = subgraph.to_cypher_merge()
        else:
            query, params = subgraph.to_cypher_create()

        async with self._driver.session() as session:
            await session.run(query, params)

    def _save_to_file(self, subgraph: ToolSubgraph):
        """Save subgraph to JSON file."""
        if not self._storage_path:
            return

        os.makedirs(self._storage_path, exist_ok=True)
        filepath = os.path.join(self._storage_path, f"{subgraph.tool.name}.json")

        with open(filepath, "w") as f:
            json.dump(subgraph.to_dict(), f, indent=2, default=str)

    def get(self, tool_name: str) -> Optional[ToolSubgraph]:
        """Get a subgraph by tool name from memory."""
        return self._memory_store.get(tool_name)

    def list_all(self) -> List[ToolSubgraph]:
        """List all subgraphs in memory."""
        return list(self._memory_store.values())

    async def load_from_neo4j(self, tool_name: str) -> Optional[ToolSubgraph]:
        """Load a subgraph from Neo4j by tool name."""
        if not self._driver:
            return None

        query = """
        MATCH (t:Tool {name: $name})
        OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
        OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
        OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
        RETURN t, s, o, a
        """

        async with self._driver.session() as session:
            result = await session.run(query, name=tool_name)
            record = await result.single()

            if not record:
                return None

            # Parse nodes from record
            tool_data = dict(record["t"])
            schema_data = dict(record["s"]) if record["s"] else {}
            output_data = dict(record["o"]) if record["o"] else {}
            action_data = dict(record["a"]) if record["a"] else {}

            # Deserialize JSON fields
            for key in ["input_schema", "output_schema", "tags"]:
                if key in schema_data and isinstance(schema_data[key], str):
                    schema_data[key] = json.loads(schema_data[key])
                if key in tool_data and isinstance(tool_data[key], str):
                    tool_data[key] = json.loads(tool_data[key])

            return ToolSubgraph(
                tool=ToolNode(**tool_data),
                tool_schema=ToolSchemaNode(**schema_data) if schema_data else ToolSchemaNode(),
                tool_output=ToolOutputNode(**output_data) if output_data else ToolOutputNode(),
                tool_action=ToolActionNode(**action_data) if action_data else ToolActionNode(),
            )

    def load_from_file(self, tool_name: str) -> Optional[ToolSubgraph]:
        """Load a subgraph from file by tool name."""
        if not self._storage_path:
            return None

        filepath = os.path.join(self._storage_path, f"{tool_name}.json")

        if not os.path.exists(filepath):
            return None

        with open(filepath, "r") as f:
            data = json.load(f)

        return ToolSubgraph.from_dict(data)
