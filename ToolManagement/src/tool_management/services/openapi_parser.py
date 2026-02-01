"""
OpenAPI Parser - Converts OpenAPI schemas to tools, resources, and links.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import httpx
import yaml

from ..models.schema import (
    OpenAPISchema,
    SchemaFormat,
    SchemaSource,
    SchemaState,
    ParsedEndpoint,
)
from ..models.resource import Resource, Link, HttpMethod, LinkRelation, ResourceState
from ..models.tool import Tool, ToolInput, ToolOutput, ToolVisibility, ToolStatus

logger = logging.getLogger(__name__)


class OpenAPIParser:
    """
    Parses OpenAPI specifications and generates tools, resources, and links.
    """

    # Map OpenAPI types to JSON Schema types
    TYPE_MAP = {
        "string": "string",
        "integer": "integer",
        "number": "number",
        "boolean": "boolean",
        "array": "array",
        "object": "object",
        "file": "string",  # file uploads
    }

    # Map HTTP methods to link relations
    METHOD_TO_REL = {
        "GET": LinkRelation.SELF,
        "POST": LinkRelation.CREATE,
        "PUT": LinkRelation.UPDATE,
        "PATCH": LinkRelation.UPDATE,
        "DELETE": LinkRelation.DELETE,
    }

    def __init__(self):
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def fetch_schema(self, url: str) -> Tuple[str, str]:
        """
        Fetch an OpenAPI schema from a URL.

        Returns:
            Tuple of (raw_content, content_type)
        """
        client = await self._get_client()
        response = await client.get(url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        return response.text, content_type

    def parse_content(self, content: str, content_type: str = "") -> Dict[str, Any]:
        """Parse raw content as JSON or YAML."""
        # Try JSON first
        import json
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Try YAML
        try:
            return yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse content as JSON or YAML: {e}")

    def detect_format(self, spec: Dict[str, Any]) -> SchemaFormat:
        """Detect the schema format from parsed content."""
        if "openapi" in spec:
            version = spec["openapi"]
            if version.startswith("3.1"):
                return SchemaFormat.OPENAPI_3_1
            return SchemaFormat.OPENAPI_3_0
        elif "swagger" in spec:
            return SchemaFormat.SWAGGER_2
        elif "$schema" in spec:
            return SchemaFormat.JSON_SCHEMA
        return SchemaFormat.OPENAPI_3_0  # default

    async def load_schema(
        self,
        url: Optional[str] = None,
        content: Optional[str] = None,
        name: Optional[str] = None,
        ttl_seconds: int = 3600,
    ) -> OpenAPISchema:
        """
        Load and parse an OpenAPI schema.

        Args:
            url: URL to fetch schema from
            content: Raw schema content (alternative to URL)
            name: Name for the schema
            ttl_seconds: Cache TTL

        Returns:
            Parsed OpenAPISchema
        """
        schema = OpenAPISchema(
            name=name or url or "inline",
            source=SchemaSource.URL if url else SchemaSource.INLINE,
            source_url=url,
            ttl_seconds=ttl_seconds,
            state=SchemaState.LOADING,
        )

        try:
            # Fetch if URL provided
            if url:
                raw_content, content_type = await self.fetch_schema(url)
                schema.raw_content = raw_content
                schema.last_fetched = datetime.utcnow()
            elif content:
                schema.raw_content = content
                schema.source = SchemaSource.INLINE
            else:
                raise ValueError("Either url or content must be provided")

            # Compute hash
            schema.content_hash = schema.compute_hash()

            # Parse content
            spec = self.parse_content(schema.raw_content)
            schema.parsed_spec = spec

            # Detect format
            schema.format = self.detect_format(spec)
            schema.openapi_version = spec.get("openapi") or spec.get("swagger")

            # Extract metadata
            info = spec.get("info", {})
            schema.title = info.get("title")
            schema.description = info.get("description")
            schema.version = info.get("version")

            # Extract servers
            if "servers" in spec:
                schema.servers = spec["servers"]
                if schema.servers:
                    schema.base_url = schema.servers[0].get("url", "")
            elif "host" in spec:  # Swagger 2.0
                host = spec["host"]
                basePath = spec.get("basePath", "")
                schemes = spec.get("schemes", ["https"])
                schema.base_url = f"{schemes[0]}://{host}{basePath}"

            # Parse endpoints
            schema.endpoints = self._parse_endpoints(spec)
            schema.endpoint_count = len(schema.endpoints)

            # Set expiry
            schema.expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
            schema.state = SchemaState.PARSED
            schema.updated_at = datetime.utcnow()

        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            schema.state = SchemaState.FAILED
            schema.error_message = str(e)

        return schema

    def _parse_endpoints(self, spec: Dict[str, Any]) -> List[ParsedEndpoint]:
        """Parse all endpoints from an OpenAPI spec."""
        endpoints = []
        paths = spec.get("paths", {})

        for path, path_item in paths.items():
            if not isinstance(path_item, dict):
                continue

            # Handle path-level parameters
            path_params = path_item.get("parameters", [])

            for method in ["get", "post", "put", "patch", "delete", "head", "options"]:
                if method not in path_item:
                    continue

                operation = path_item[method]
                if not isinstance(operation, dict):
                    continue

                # Merge path and operation parameters
                op_params = operation.get("parameters", [])
                all_params = path_params + op_params

                endpoint = ParsedEndpoint(
                    path=path,
                    method=method.upper(),
                    operation_id=operation.get("operationId"),
                    summary=operation.get("summary"),
                    description=operation.get("description"),
                    tags=operation.get("tags", []),
                    parameters=all_params,
                    request_body=operation.get("requestBody"),
                    responses=operation.get("responses", {}),
                    security=operation.get("security", []),
                    deprecated=operation.get("deprecated", False),
                )
                endpoints.append(endpoint)

        return endpoints

    def generate_tools(
        self,
        schema: OpenAPISchema,
        prefix: str = "",
        tags_filter: Optional[List[str]] = None,
        include_deprecated: bool = False,
    ) -> List[Tool]:
        """
        Generate Tool instances from a parsed schema.

        Args:
            schema: Parsed OpenAPI schema
            prefix: Prefix to add to tool names
            tags_filter: Only include endpoints with these tags
            include_deprecated: Include deprecated endpoints

        Returns:
            List of generated Tools
        """
        if schema.state != SchemaState.PARSED:
            raise ValueError(f"Schema not in PARSED state: {schema.state}")

        tools = []
        base_url = schema.base_url or ""

        for endpoint in schema.endpoints:
            # Filter by deprecation
            if endpoint.deprecated and not include_deprecated:
                continue

            # Filter by tags
            if tags_filter and not any(t in endpoint.tags for t in tags_filter):
                continue

            # Generate tool name
            name = prefix + endpoint.to_tool_name()

            # Build inputs from parameters
            inputs = self._params_to_inputs(endpoint.parameters)

            # Add request body if present
            if endpoint.request_body:
                body_input = self._request_body_to_input(endpoint.request_body)
                if body_input:
                    inputs.append(body_input)

            # Build output from responses
            output = self._responses_to_output(endpoint.responses)

            tool = Tool(
                name=name,
                description=endpoint.description or endpoint.summary or f"{endpoint.method} {endpoint.path}",
                version=schema.version or "1.0.0",
                inputs=inputs,
                output=output,
                tags=endpoint.tags + schema.tags,
                category=endpoint.tags[0] if endpoint.tags else None,
                visibility=ToolVisibility.PUBLIC,
                status=ToolStatus.DEPRECATED if endpoint.deprecated else ToolStatus.ACTIVE,
                service_url=base_url,
                endpoint=endpoint.path,
                metadata={
                    "method": endpoint.method,
                    "schema_id": schema.id,
                    "operation_id": endpoint.operation_id,
                    "security": endpoint.security,
                },
            )
            tools.append(tool)

        return tools

    def _params_to_inputs(self, parameters: List[Dict[str, Any]]) -> List[ToolInput]:
        """Convert OpenAPI parameters to ToolInputs."""
        inputs = []
        for param in parameters:
            if not isinstance(param, dict):
                continue

            param_schema = param.get("schema", {})
            param_type = param_schema.get("type", "string")

            inputs.append(ToolInput(
                name=param.get("name", ""),
                type=self.TYPE_MAP.get(param_type, "string"),
                description=param.get("description", ""),
                required=param.get("required", False),
                default=param_schema.get("default"),
                enum=param_schema.get("enum"),
            ))
        return inputs

    def _request_body_to_input(self, request_body: Dict[str, Any]) -> Optional[ToolInput]:
        """Convert OpenAPI request body to a ToolInput."""
        content = request_body.get("content", {})

        # Prefer JSON
        for media_type in ["application/json", "application/x-www-form-urlencoded"]:
            if media_type in content:
                schema = content[media_type].get("schema", {})
                return ToolInput(
                    name="body",
                    type="object",
                    description=request_body.get("description", "Request body"),
                    required=request_body.get("required", False),
                )

        return None

    def _responses_to_output(self, responses: Dict[str, Any]) -> Optional[ToolOutput]:
        """Convert OpenAPI responses to ToolOutput."""
        # Look for success response
        for status in ["200", "201", "202", "204"]:
            if status in responses:
                response = responses[status]
                content = response.get("content", {})

                if "application/json" in content:
                    schema = content["application/json"].get("schema", {})
                    return ToolOutput(
                        type=schema.get("type", "object"),
                        description=response.get("description", ""),
                    )

                return ToolOutput(
                    type="object",
                    description=response.get("description", ""),
                )

        return ToolOutput(type="object", description="API response")

    def generate_resources(
        self,
        schema: OpenAPISchema,
        tools: List[Tool],
    ) -> List[Resource]:
        """
        Generate Resource instances from a parsed schema.

        Groups endpoints by resource path and creates HATEOAS links.
        """
        if schema.state != SchemaState.PARSED:
            raise ValueError(f"Schema not in PARSED state: {schema.state}")

        # Group endpoints by resource path
        resource_map: Dict[str, List[Tuple[ParsedEndpoint, Tool]]] = {}

        tool_by_name = {t.name: t for t in tools}

        for endpoint in schema.endpoints:
            # Extract resource path (e.g., /users/{id} -> /users)
            resource_path = self._extract_resource_path(endpoint.path)

            if resource_path not in resource_map:
                resource_map[resource_path] = []

            # Find matching tool
            tool_name = endpoint.to_tool_name()
            for prefix in ["", schema.name + "_"]:
                if prefix + tool_name in tool_by_name:
                    resource_map[resource_path].append(
                        (endpoint, tool_by_name[prefix + tool_name])
                    )
                    break

        # Create resources
        resources = []
        base_url = schema.base_url or ""

        for path, endpoint_tools in resource_map.items():
            if not endpoint_tools:
                continue

            # Determine resource name from path
            name = self._path_to_resource_name(path)

            # Check if collection
            is_collection = not any(
                "{" in ep.path for ep, _ in endpoint_tools
            )

            resource = Resource(
                name=name,
                description=f"Resource for {path}",
                base_path=path,
                schema_id=schema.id,
                service_id=schema.service_id,
                is_collection=is_collection,
                state=ResourceState.AVAILABLE,
                tool_ids=[t.id for _, t in endpoint_tools],
                tags=schema.tags,
            )

            # Generate links
            for endpoint, tool in endpoint_tools:
                href = base_url.rstrip("/") + endpoint.path
                method = HttpMethod(endpoint.method)
                rel = self.METHOD_TO_REL.get(endpoint.method, LinkRelation.RELATED)

                # Adjust rel for collections
                if is_collection and method == HttpMethod.GET:
                    rel = LinkRelation.COLLECTION
                elif "{" in endpoint.path and method == HttpMethod.GET:
                    rel = LinkRelation.ITEM

                link = Link(
                    href=href,
                    rel=rel.value if isinstance(rel, LinkRelation) else rel,
                    method=method,
                    title=endpoint.summary or tool.name,
                    description=endpoint.description,
                    templated="{" in endpoint.path,
                )
                resource.add_link(link)

            resources.append(resource)

        return resources

    def _extract_resource_path(self, path: str) -> str:
        """Extract the base resource path from a full path."""
        # Remove path parameters to get base resource
        # /users/{id}/posts -> /users
        # /users/{id} -> /users
        parts = path.split("/")
        base_parts = []
        for part in parts:
            if part.startswith("{"):
                break
            base_parts.append(part)
        return "/".join(base_parts) or "/"

    def _path_to_resource_name(self, path: str) -> str:
        """Convert a path to a resource name."""
        # /api/v1/users -> users
        parts = [p for p in path.split("/") if p and not p.startswith("{")]
        if parts:
            return parts[-1]
        return "root"


# Global parser instance
_parser: Optional[OpenAPIParser] = None


def get_openapi_parser() -> OpenAPIParser:
    """Get the global OpenAPI parser."""
    global _parser
    if _parser is None:
        _parser = OpenAPIParser()
    return _parser
