"""
HATEOAS (Hypermedia As The Engine Of Application State) Support

Provides hypermedia links and self-descriptive API responses for true REST interconnectivity.
Following HAL (Hypertext Application Language) specification.
"""

from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field, HttpUrl
from enum import Enum
from urllib.parse import urljoin


class LinkRelation(str, Enum):
    """Standard link relations following IANA registry"""
    SELF = "self"
    NEXT = "next"
    PREV = "prev"
    FIRST = "first"
    LAST = "last"
    COLLECTION = "collection"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    PARENT = "parent"
    RELATED = "related"
    ALTERNATE = "alternate"
    SCHEMA = "describedby"
    DOCUMENTATION = "help"


class Link(BaseModel):
    """HAL-compliant link object"""
    href: str = Field(..., description="URI or URI template")
    templated: Optional[bool] = Field(None, description="Whether href is a URI template")
    type: Optional[str] = Field(None, description="Media type hint")
    deprecation: Optional[str] = Field(None, description="Deprecation URL if deprecated")
    name: Optional[str] = Field(None, description="Secondary key for link")
    profile: Optional[str] = Field(None, description="Profile URI")
    title: Optional[str] = Field(None, description="Human-readable title")
    hreflang: Optional[str] = Field(None, description="Language of target resource")


class HALResource(BaseModel):
    """Base class for HAL resources with _links and _embedded"""
    model_config = {"populate_by_name": True}

    links: Dict[str, Union[Link, List[Link]]] = Field(
        default_factory=dict,
        alias="_links",
        serialization_alias="_links"
    )
    embedded: Dict[str, Any] = Field(
        default_factory=dict,
        alias="_embedded",
        serialization_alias="_embedded"
    )

    def add_link(self, rel: str, href: str, **kwargs):
        """Add a link to the resource"""
        link = Link(href=href, **kwargs)
        if rel in self.links:
            # Convert to list if not already
            if not isinstance(self.links[rel], list):
                self.links[rel] = [self.links[rel]]
            self.links[rel].append(link)
        else:
            self.links[rel] = link

    def add_embedded(self, rel: str, resource: Any):
        """Add an embedded resource"""
        if rel in self.embedded:
            # Convert to list if not already
            if not isinstance(self.embedded[rel], list):
                self.embedded[rel] = [self.embedded[rel]]
            self.embedded[rel].append(resource)
        else:
            self.embedded[rel] = resource


class HATEOASBuilder:
    """Builder for adding HATEOAS links to resources"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def build_tool_response(self, tool: Dict[str, Any]) -> Dict[str, Any]:
        """Add HATEOAS links to a tool response"""
        tool_id = tool.get("id")
        tool_with_links = tool.copy()

        # Build links based on tool state and capabilities
        links = {
            "self": Link(href=f"{self.base_url}/api/v1/tools/{tool_id}"),
            "update": Link(
                href=f"{self.base_url}/api/v1/tools/{tool_id}",
                title="Update this tool",
                type="application/json"
            ),
            "delete": Link(
                href=f"{self.base_url}/api/v1/tools/{tool_id}",
                title="Delete this tool"
            ),
            "execute": Link(
                href=f"{self.base_url}/api/v1/tools/execute",
                title="Execute this tool",
                type="application/json"
            )
        }

        # Add conditional links based on tool properties
        if tool.get("user_id"):
            links["user"] = Link(
                href=f"{self.base_url}/api/v1/tools/by-user/{tool['user_id']}",
                title="Tools by this user"
            )

        if tool.get("organization_id"):
            links["organization"] = Link(
                href=f"{self.base_url}/api/v1/tools/by-organization/{tool['organization_id']}",
                title="Tools in this organization"
            )

        if tool.get("toolkit_id"):
            links["toolkit"] = Link(
                href=f"{self.base_url}/api/v1/tools/by-toolkit/{tool['toolkit_id']}",
                title="Tools in this toolkit"
            )

        if tool.get("has_ui_effects"):
            links["ui_preview"] = Link(
                href=f"{self.base_url}/api/v1/tools/{tool_id}/ui-preview",
                title="Preview UI effects",
                type="text/html"
            )

        if tool.get("dependencies"):
            links["dependencies"] = Link(
                href=f"{self.base_url}/api/v1/tools/{tool_id}/dependencies",
                title="Tool dependencies",
                templated=False
            )

        # Add schema links
        links["input_schema"] = Link(
            href=f"{self.base_url}/api/v1/tools/{tool_id}/schemas/input",
            title="Input schema",
            type="application/schema+json"
        )

        links["output_schema"] = Link(
            href=f"{self.base_url}/api/v1/tools/{tool_id}/schemas/output",
            title="Output schema",
            type="application/schema+json"
        )

        # Add composition suggestions
        if tool.get("category"):
            links["similar_tools"] = Link(
                href=f"{self.base_url}/api/v1/tools?category={tool['category']}",
                title="Similar tools in same category"
            )

        # Add documentation link
        links["documentation"] = Link(
            href=f"{self.base_url}/docs#/Tools/get_tool_api_v1_tools__tool_id__get",
            title="API documentation for this endpoint"
        )

        tool_with_links["_links"] = links

        # Add embedded resources if appropriate
        embedded = {}

        # Optionally embed related tools
        if tool.get("dependencies"):
            embedded["dependencies"] = []
            for dep_id in tool.get("dependencies", []):
                embedded["dependencies"].append({
                    "_links": {
                        "self": Link(href=f"{self.base_url}/api/v1/tools/{dep_id}")
                    }
                })

        if embedded:
            tool_with_links["_embedded"] = embedded

        return tool_with_links

    def build_collection_response(
        self,
        items: List[Dict[str, Any]],
        page: int = 1,
        limit: int = 100,
        total: int = None,
        endpoint: str = "/api/v1/tools"
    ) -> Dict[str, Any]:
        """Build a HAL collection response with pagination"""
        total = total or len(items)
        total_pages = (total + limit - 1) // limit

        response = {
            "_links": {
                "self": Link(href=f"{self.base_url}{endpoint}?page={page}&limit={limit}"),
                "first": Link(href=f"{self.base_url}{endpoint}?page=1&limit={limit}"),
                "last": Link(href=f"{self.base_url}{endpoint}?page={total_pages}&limit={limit}")
            },
            "_embedded": {
                "items": [self.build_tool_response(item) for item in items]
            },
            "page": {
                "size": len(items),
                "totalElements": total,
                "totalPages": total_pages,
                "number": page
            }
        }

        # Add next/prev links if applicable
        if page < total_pages:
            response["_links"]["next"] = Link(
                href=f"{self.base_url}{endpoint}?page={page + 1}&limit={limit}"
            )

        if page > 1:
            response["_links"]["prev"] = Link(
                href=f"{self.base_url}{endpoint}?page={page - 1}&limit={limit}"
            )

        return response

    def build_composition_response(self, composition: Dict[str, Any]) -> Dict[str, Any]:
        """Add HATEOAS links to a composition response"""
        comp_id = composition.get("id")
        comp_with_links = composition.copy()

        links = {
            "self": Link(href=f"{self.base_url}/api/v1/compositions/{comp_id}"),
            "execute": Link(
                href=f"{self.base_url}/api/v1/compositions/{comp_id}/execute",
                title="Execute this composition",
                type="application/json"
            ),
            "history": Link(
                href=f"{self.base_url}/api/v1/compositions/{comp_id}/history",
                title="Execution history for this composition"
            )
        }

        # Add links to component tools
        for tool in composition.get("tools", []):
            if tool.get("tool_id"):
                links[f"tool_{tool['tool_id']}"] = Link(
                    href=f"{self.base_url}/api/v1/tools/{tool['tool_id']}",
                    title=f"Tool: {tool.get('tool_name', tool['tool_id'])}"
                )

        comp_with_links["_links"] = links
        return comp_with_links

    def build_webhook_response(self, webhook: Dict[str, Any]) -> Dict[str, Any]:
        """Add HATEOAS links to a webhook response"""
        webhook_id = webhook.get("id")
        webhook_with_links = webhook.copy()

        links = {
            "self": Link(href=f"{self.base_url}/api/v1/webhooks/{webhook_id}"),
            "delete": Link(
                href=f"{self.base_url}/api/v1/webhooks/{webhook_id}",
                title="Unregister this webhook"
            ),
            "deliveries": Link(
                href=f"{self.base_url}/api/v1/webhooks/{webhook_id}/deliveries",
                title="Delivery history for this webhook"
            ),
            "test": Link(
                href=f"{self.base_url}/api/v1/webhooks/{webhook_id}/test",
                title="Send a test event to this webhook",
                type="application/json"
            )
        }

        webhook_with_links["_links"] = links
        return webhook_with_links

    def build_federation_node_response(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Add HATEOAS links to a federation node response"""
        node_id = node.get("id")
        node_with_links = node.copy()

        links = {
            "self": Link(href=f"{self.base_url}/api/v1/federation/nodes/{node_id}"),
            "sync": Link(
                href=f"{self.base_url}/api/v1/federation/sync/{node_id}",
                title="Manually sync this node",
                type="application/json"
            ),
            "tools": Link(
                href=f"{self.base_url}/api/v1/federation/nodes/{node_id}/tools",
                title="List tools from this node"
            ),
            "delete": Link(
                href=f"{self.base_url}/api/v1/federation/nodes/{node_id}",
                title="Remove this federation node"
            )
        }

        # Add link to the actual federated registry
        if node.get("url"):
            links["external"] = Link(
                href=node["url"],
                title="External registry URL",
                type="application/json"
            )

        node_with_links["_links"] = links
        return node_with_links

    def build_error_response(
        self,
        status_code: int,
        detail: str,
        instance: Optional[str] = None,
        title: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build an RFC 7807 Problem Details response with HATEOAS links"""
        error_titles = {
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            409: "Conflict",
            422: "Unprocessable Entity",
            500: "Internal Server Error",
            503: "Service Unavailable"
        }

        response = {
            "type": f"{self.base_url}/errors/{status_code}",
            "title": title or error_titles.get(status_code, "Error"),
            "status": status_code,
            "detail": detail,
            "instance": instance or f"{self.base_url}/api/v1/current-request"
        }

        # Add helpful links based on error type
        links = {
            "documentation": Link(
                href=f"{self.base_url}/docs",
                title="API Documentation"
            )
        }

        if status_code == 401:
            links["authenticate"] = Link(
                href=f"{self.base_url}/auth/login",
                title="Authentication endpoint"
            )
        elif status_code == 404:
            links["search"] = Link(
                href=f"{self.base_url}/api/v1/tools/search",
                title="Search for tools"
            )
            links["list"] = Link(
                href=f"{self.base_url}/api/v1/tools",
                title="List all tools"
            )

        response["_links"] = links
        return response

    def build_root_response(self) -> Dict[str, Any]:
        """Build the root API response with all available endpoints"""
        return {
            "_links": {
                "self": Link(href=f"{self.base_url}/"),
                "tools": Link(
                    href=f"{self.base_url}/api/v1/tools",
                    title="Tool management"
                ),
                "compositions": Link(
                    href=f"{self.base_url}/api/v1/compositions",
                    title="Tool compositions"
                ),
                "webhooks": Link(
                    href=f"{self.base_url}/api/v1/webhooks",
                    title="Webhook management"
                ),
                "federation": Link(
                    href=f"{self.base_url}/api/v1/federation/nodes",
                    title="Federation nodes"
                ),
                "mcp": Link(
                    href=f"{self.base_url}/mcp",
                    title="MCP WebSocket endpoint"
                ),
                "adapters": Link(
                    href=f"{self.base_url}/api/v1/adapters",
                    title="Protocol adapters"
                ),
                "events": Link(
                    href=f"{self.base_url}/api/v1/events",
                    title="Event history"
                ),
                "health": Link(
                    href=f"{self.base_url}/health",
                    title="Health check"
                ),
                "documentation": Link(
                    href=f"{self.base_url}/docs",
                    title="Interactive API documentation",
                    type="text/html"
                ),
                "openapi": Link(
                    href=f"{self.base_url}/openapi.json",
                    title="OpenAPI specification",
                    type="application/json"
                )
            },
            "version": "1.0.0",
            "title": "Tool Management API with HATEOAS"
        }


class ContentNegotiation:
    """Handle content negotiation for different hypermedia formats"""

    @staticmethod
    def get_format(accept_header: str) -> str:
        """Determine the preferred hypermedia format from Accept header"""
        if "application/hal+json" in accept_header:
            return "hal"
        elif "application/vnd.siren+json" in accept_header:
            return "siren"
        elif "application/vnd.api+json" in accept_header:
            return "json-api"
        elif "application/ld+json" in accept_header:
            return "json-ld"
        else:
            return "hal"  # Default to HAL

    @staticmethod
    def convert_to_siren(hal_resource: Dict[str, Any]) -> Dict[str, Any]:
        """Convert HAL to Siren format"""
        siren = {
            "class": ["tool"],
            "properties": {k: v for k, v in hal_resource.items()
                         if not k.startswith("_")},
            "links": [],
            "actions": []
        }

        # Convert HAL links to Siren links
        if "_links" in hal_resource:
            for rel, link in hal_resource["_links"].items():
                if isinstance(link, dict):
                    siren["links"].append({
                        "rel": [rel],
                        "href": link.get("href"),
                        "title": link.get("title")
                    })

        # Add Siren actions for state transitions
        if "execute" in hal_resource.get("_links", {}):
            siren["actions"].append({
                "name": "execute-tool",
                "title": "Execute Tool",
                "method": "POST",
                "href": hal_resource["_links"]["execute"]["href"],
                "type": "application/json",
                "fields": [
                    {"name": "input_data", "type": "object"}
                ]
            })

        return siren