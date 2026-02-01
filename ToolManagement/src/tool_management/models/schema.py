"""
OpenAPI Schema models for storing and managing API specifications.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid
import hashlib


class SchemaFormat(str, Enum):
    """Supported schema formats."""
    OPENAPI_3_0 = "openapi-3.0"
    OPENAPI_3_1 = "openapi-3.1"
    SWAGGER_2 = "swagger-2.0"
    JSON_SCHEMA = "json-schema"
    MCP = "mcp"


class SchemaSource(str, Enum):
    """How the schema was obtained."""
    URL = "url"
    FILE = "file"
    INLINE = "inline"
    DISCOVERED = "discovered"
    MCP_SERVER = "mcp_server"


class SchemaState(str, Enum):
    """Schema processing state."""
    PENDING = "pending"
    LOADING = "loading"
    PARSED = "parsed"
    FAILED = "failed"
    STALE = "stale"


class ParsedEndpoint(BaseModel):
    """An endpoint parsed from an OpenAPI schema."""
    path: str
    method: str
    operation_id: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    parameters: List[Dict[str, Any]] = Field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = Field(default_factory=dict)
    security: List[Dict[str, Any]] = Field(default_factory=list)
    deprecated: bool = False

    def to_tool_name(self) -> str:
        """Generate a tool name from this endpoint."""
        if self.operation_id:
            return self.operation_id
        # Generate from path and method
        path_parts = [p for p in self.path.split("/") if p and not p.startswith("{")]
        return f"{self.method.lower()}_{'_'.join(path_parts)}"


class OpenAPISchema(BaseModel):
    """
    Stored OpenAPI schema with metadata.

    Represents a cached API specification that can be used
    to generate tools, resources, and links.
    """

    # Identity
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Schema name/identifier")
    title: Optional[str] = Field(None, description="API title from spec")
    description: Optional[str] = Field(None, description="API description")
    version: Optional[str] = Field(None, description="API version from spec")

    # Source
    source: SchemaSource = Field(SchemaSource.URL)
    source_url: Optional[str] = Field(None, description="URL where schema was fetched")
    source_path: Optional[str] = Field(None, description="File path if loaded from file")

    # Format
    format: SchemaFormat = Field(SchemaFormat.OPENAPI_3_0)
    openapi_version: Optional[str] = Field(None, description="OpenAPI spec version")

    # State
    state: SchemaState = Field(SchemaState.PENDING)
    error_message: Optional[str] = Field(None, description="Error if state is FAILED")

    # Content
    raw_content: Optional[str] = Field(None, description="Raw schema content")
    content_hash: Optional[str] = Field(None, description="Hash of raw content for change detection")
    parsed_spec: Optional[Dict[str, Any]] = Field(None, description="Parsed JSON/YAML content")

    # Server info
    servers: List[Dict[str, Any]] = Field(default_factory=list, description="API servers")
    base_url: Optional[str] = Field(None, description="Primary base URL")

    # Parsed entities
    endpoints: List[ParsedEndpoint] = Field(default_factory=list)
    endpoint_count: int = Field(0)

    # Generated entity IDs
    tool_ids: List[str] = Field(default_factory=list, description="Generated tool IDs")
    resource_ids: List[str] = Field(default_factory=list, description="Generated resource IDs")

    # Associations
    service_id: Optional[str] = Field(None, description="Associated service ID")

    # Cache control
    ttl_seconds: int = Field(3600, description="Cache TTL in seconds")
    last_fetched: Optional[datetime] = Field(None)
    expires_at: Optional[datetime] = Field(None)

    # Metadata
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        extra = "allow"

    def compute_hash(self) -> str:
        """Compute hash of raw content."""
        if self.raw_content:
            return hashlib.sha256(self.raw_content.encode()).hexdigest()[:16]
        return ""

    def is_expired(self) -> bool:
        """Check if the cached schema has expired."""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at

    def is_stale(self) -> bool:
        """Check if schema should be refreshed."""
        return self.state == SchemaState.STALE or self.is_expired()

    def mark_stale(self) -> None:
        """Mark schema as needing refresh."""
        self.state = SchemaState.STALE
        self.updated_at = datetime.utcnow()

    def to_summary(self) -> Dict[str, Any]:
        """Get a summary of this schema."""
        return {
            "id": self.id,
            "name": self.name,
            "title": self.title,
            "version": self.version,
            "format": self.format.value,
            "state": self.state.value,
            "base_url": self.base_url,
            "endpoint_count": self.endpoint_count,
            "tool_count": len(self.tool_ids),
            "resource_count": len(self.resource_ids),
            "is_stale": self.is_stale(),
        }


class SchemaRegistry(BaseModel):
    """Collection of schemas with lookup indices."""

    schemas: Dict[str, OpenAPISchema] = Field(default_factory=dict)
    by_name: Dict[str, str] = Field(default_factory=dict)  # name -> id
    by_url: Dict[str, str] = Field(default_factory=dict)  # url -> id
    by_service: Dict[str, List[str]] = Field(default_factory=dict)  # service_id -> [schema_ids]

    def add(self, schema: OpenAPISchema) -> None:
        """Add a schema to the registry."""
        self.schemas[schema.id] = schema
        self.by_name[schema.name] = schema.id
        if schema.source_url:
            self.by_url[schema.source_url] = schema.id
        if schema.service_id:
            if schema.service_id not in self.by_service:
                self.by_service[schema.service_id] = []
            self.by_service[schema.service_id].append(schema.id)

    def remove(self, schema_id: str) -> Optional[OpenAPISchema]:
        """Remove a schema from the registry."""
        schema = self.schemas.pop(schema_id, None)
        if schema:
            self.by_name.pop(schema.name, None)
            if schema.source_url:
                self.by_url.pop(schema.source_url, None)
            if schema.service_id and schema.service_id in self.by_service:
                self.by_service[schema.service_id] = [
                    sid for sid in self.by_service[schema.service_id]
                    if sid != schema_id
                ]
        return schema

    def get(self, schema_id: str) -> Optional[OpenAPISchema]:
        """Get schema by ID."""
        return self.schemas.get(schema_id)

    def get_by_name(self, name: str) -> Optional[OpenAPISchema]:
        """Get schema by name."""
        schema_id = self.by_name.get(name)
        return self.schemas.get(schema_id) if schema_id else None

    def get_by_url(self, url: str) -> Optional[OpenAPISchema]:
        """Get schema by source URL."""
        schema_id = self.by_url.get(url)
        return self.schemas.get(schema_id) if schema_id else None

    def list_all(self) -> List[OpenAPISchema]:
        """List all schemas."""
        return list(self.schemas.values())

    def list_stale(self) -> List[OpenAPISchema]:
        """List schemas that need refresh."""
        return [s for s in self.schemas.values() if s.is_stale()]
