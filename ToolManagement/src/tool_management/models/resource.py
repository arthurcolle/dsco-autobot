"""
Resource and Link models for HATEOAS-style API discovery.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class LinkRelation(str, Enum):
    """Standard link relations (RFC 8288 + custom)."""
    SELF = "self"
    COLLECTION = "collection"
    ITEM = "item"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SEARCH = "search"
    EXECUTE = "execute"
    PARENT = "parent"
    CHILD = "child"
    RELATED = "related"
    ALTERNATE = "alternate"
    CANONICAL = "canonical"
    SCHEMA = "describedby"
    DOCUMENTATION = "documentation"
    HEALTH = "health"
    NEXT = "next"
    PREV = "prev"
    FIRST = "first"
    LAST = "last"


class HttpMethod(str, Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class Link(BaseModel):
    """
    Hypermedia link for resource discovery.

    Follows HAL/JSON:API conventions for discoverability.
    """
    href: str = Field(..., description="Target URL or URI template")
    rel: str = Field(..., description="Link relation type")
    method: HttpMethod = Field(HttpMethod.GET, description="HTTP method")
    title: Optional[str] = Field(None, description="Human-readable title")
    description: Optional[str] = Field(None, description="Link description")
    templated: bool = Field(False, description="Whether href is a URI template")
    type: Optional[str] = Field(None, description="Expected media type")
    deprecation: Optional[str] = Field(None, description="Deprecation URL if deprecated")

    # Request/response hints
    accepts: Optional[str] = Field(None, description="Accepted content types")
    produces: Optional[str] = Field(None, description="Produced content types")

    # Schema references
    request_schema: Optional[str] = Field(None, description="JSON Schema ref for request body")
    response_schema: Optional[str] = Field(None, description="JSON Schema ref for response")

    class Config:
        extra = "allow"


class ResourceState(str, Enum):
    """Resource availability state."""
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    DEPRECATED = "deprecated"
    EXPERIMENTAL = "experimental"


class Resource(BaseModel):
    """
    API Resource representing an entity or collection.

    Resources are discoverable via links and can be associated
    with tools for execution.
    """

    # Identity
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Resource name (e.g., 'users', 'orders')")
    description: str = Field("", description="Resource description")

    # Location
    base_path: str = Field(..., description="Base path for this resource (e.g., '/api/v1/users')")
    service_id: Optional[str] = Field(None, description="Owning service ID")
    schema_id: Optional[str] = Field(None, description="Source OpenAPI schema ID")

    # Type
    resource_type: str = Field("entity", description="Resource type (entity, collection, action)")
    is_collection: bool = Field(False, description="Whether this represents a collection")

    # State
    state: ResourceState = Field(ResourceState.AVAILABLE)

    # Links (HATEOAS)
    links: List[Link] = Field(default_factory=list, description="Available operations/relations")

    # Associated tools
    tool_ids: List[str] = Field(default_factory=list, description="Tool IDs for this resource")

    # Schema
    schema_ref: Optional[str] = Field(None, description="JSON Schema $ref for this resource")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Resource properties schema")

    # Metadata
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        extra = "allow"

    def get_link(self, rel: str) -> Optional[Link]:
        """Get a link by relation type."""
        for link in self.links:
            if link.rel == rel:
                return link
        return None

    def get_links(self, rel: str) -> List[Link]:
        """Get all links with a relation type."""
        return [link for link in self.links if link.rel == rel]

    def add_link(self, link: Link) -> None:
        """Add a link to this resource."""
        # Replace existing link with same rel+method
        self.links = [
            l for l in self.links
            if not (l.rel == link.rel and l.method == link.method)
        ]
        self.links.append(link)

    def to_hal_format(self) -> Dict[str, Any]:
        """Export in HAL (Hypertext Application Language) format."""
        hal = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "_links": {}
        }

        for link in self.links:
            link_data = {"href": link.href}
            if link.templated:
                link_data["templated"] = True
            if link.title:
                link_data["title"] = link.title
            if link.type:
                link_data["type"] = link.type

            if link.rel in hal["_links"]:
                # Multiple links with same rel
                existing = hal["_links"][link.rel]
                if isinstance(existing, list):
                    existing.append(link_data)
                else:
                    hal["_links"][link.rel] = [existing, link_data]
            else:
                hal["_links"][link.rel] = link_data

        return hal

    def to_jsonapi_format(self) -> Dict[str, Any]:
        """Export in JSON:API format."""
        return {
            "type": self.resource_type,
            "id": self.id,
            "attributes": {
                "name": self.name,
                "description": self.description,
                "base_path": self.base_path,
                "state": self.state.value,
            },
            "links": {
                link.rel: link.href for link in self.links
            },
            "meta": self.metadata,
        }


class ResourceCollection(BaseModel):
    """A collection of resources with pagination support."""

    items: List[Resource] = Field(default_factory=list)
    total: int = Field(0)
    offset: int = Field(0)
    limit: int = Field(100)

    # Collection-level links
    links: List[Link] = Field(default_factory=list)

    def add_pagination_links(self, base_url: str) -> None:
        """Add standard pagination links."""
        self.links.append(Link(
            href=f"{base_url}?offset={self.offset}&limit={self.limit}",
            rel=LinkRelation.SELF,
        ))

        if self.offset > 0:
            prev_offset = max(0, self.offset - self.limit)
            self.links.append(Link(
                href=f"{base_url}?offset={prev_offset}&limit={self.limit}",
                rel=LinkRelation.PREV,
            ))
            self.links.append(Link(
                href=f"{base_url}?offset=0&limit={self.limit}",
                rel=LinkRelation.FIRST,
            ))

        if self.offset + self.limit < self.total:
            next_offset = self.offset + self.limit
            self.links.append(Link(
                href=f"{base_url}?offset={next_offset}&limit={self.limit}",
                rel=LinkRelation.NEXT,
            ))
            last_offset = ((self.total - 1) // self.limit) * self.limit
            self.links.append(Link(
                href=f"{base_url}?offset={last_offset}&limit={self.limit}",
                rel=LinkRelation.LAST,
            ))
