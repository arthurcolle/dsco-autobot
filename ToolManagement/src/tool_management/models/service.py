"""
Service model for registering tool-hosting servers.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class ServiceStatus(str, Enum):
    """Service operational status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"
    OFFLINE = "offline"


class ServiceType(str, Enum):
    """Type of service."""
    MCP = "mcp"
    REST = "rest"
    GRAPHQL = "graphql"
    GRPC = "grpc"
    WEBSOCKET = "websocket"


class AuthType(str, Enum):
    """Authentication type for service access."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    OAUTH2_AUTHORIZATION_CODE = "oauth2_authorization_code"
    JWT = "jwt"
    CUSTOM = "custom"


class ServiceAuth(BaseModel):
    """Authentication configuration for a service."""
    auth_type: AuthType = Field(AuthType.NONE, description="Type of authentication")
    header_name: str = Field("Authorization", description="Header name for auth token")
    token_prefix: Optional[str] = Field(None, description="Token prefix (e.g., 'Bearer')")

    # API Key auth
    api_key: Optional[str] = Field(None, description="API key (masked in responses)")

    # OAuth2 settings
    client_id: Optional[str] = Field(None, description="OAuth client ID")
    client_secret: Optional[str] = Field(None, description="OAuth client secret (masked)")
    token_url: Optional[str] = Field(None, description="OAuth token endpoint")
    authorize_url: Optional[str] = Field(None, description="OAuth authorization endpoint")
    scopes: List[str] = Field(default_factory=list, description="OAuth scopes")

    # Basic auth
    username: Optional[str] = Field(None, description="Basic auth username")
    password: Optional[str] = Field(None, description="Basic auth password (masked)")

    # Custom auth
    custom_headers: Dict[str, str] = Field(default_factory=dict, description="Custom auth headers")

    class Config:
        extra = "allow"

    def to_safe_dict(self) -> Dict[str, Any]:
        """Return auth config with sensitive values masked."""
        data = self.model_dump()
        # Mask sensitive fields
        if data.get("api_key"):
            data["api_key"] = "***" + data["api_key"][-4:] if len(data["api_key"]) > 4 else "***"
        if data.get("client_secret"):
            data["client_secret"] = "***"
        if data.get("password"):
            data["password"] = "***"
        return data


class ServiceCapabilities(BaseModel):
    """Capabilities that a service supports."""
    tool_execution: bool = Field(True, description="Can execute tools")
    tool_discovery: bool = Field(True, description="Supports tool discovery")
    streaming: bool = Field(False, description="Supports streaming responses")
    batch_execution: bool = Field(False, description="Supports batch tool execution")
    webhooks: bool = Field(False, description="Supports webhook callbacks")
    authentication: bool = Field(False, description="Requires authentication")

    class Config:
        extra = "allow"


class ServiceMetadata(BaseModel):
    """Extended metadata for a service."""
    owner: Optional[str] = None
    contact_email: Optional[str] = None
    documentation_url: Optional[str] = None
    api_version: Optional[str] = None
    environment: Optional[str] = Field(None, description="e.g., production, staging, development")
    region: Optional[str] = Field(None, description="Geographic region")
    tags: List[str] = Field(default_factory=list)

    class Config:
        extra = "allow"


class ServiceHealthCheck(BaseModel):
    """Health check configuration."""
    endpoint: str = Field("/health", description="Health check endpoint path")
    interval_seconds: int = Field(30, description="Check interval in seconds")
    timeout_seconds: int = Field(5, description="Health check timeout")
    healthy_threshold: int = Field(2, description="Consecutive successes for healthy")
    unhealthy_threshold: int = Field(3, description="Consecutive failures for unhealthy")


class Service(BaseModel):
    """
    Service definition representing a tool-hosting server.

    A Service represents a remote server that hosts one or more tools.
    Services must register themselves and maintain heartbeats to stay active.
    """

    # Identity
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique service identifier (UUID)")
    service_id: Optional[str] = Field(None, description="Human-readable service ID (e.g., 'search-api')")
    name: str = Field(..., description="Service display name")
    description: str = Field("", description="What this service provides")
    version: str = Field("1.0.0", description="Service version")

    # Connection
    base_url: str = Field(..., description="Base URL of the service")
    service_type: ServiceType = Field(ServiceType.REST, description="Protocol type")

    # Capabilities
    capabilities: ServiceCapabilities = Field(default_factory=ServiceCapabilities)

    # Tools
    tool_names: List[str] = Field(default_factory=list, description="Names of tools provided by this service")
    tool_count: int = Field(0, description="Number of tools hosted")

    # Health
    status: ServiceStatus = Field(ServiceStatus.UNKNOWN, description="Current health status")
    health_check: ServiceHealthCheck = Field(default_factory=ServiceHealthCheck)

    # Authentication
    auth: ServiceAuth = Field(default_factory=ServiceAuth, description="Authentication configuration")
    # Legacy fields (deprecated, use 'auth' instead)
    auth_type: Optional[str] = Field(None, description="[Deprecated] Use auth.auth_type")
    auth_header: Optional[str] = Field(None, description="[Deprecated] Use auth.header_name")

    # Metadata
    metadata: ServiceMetadata = Field(default_factory=ServiceMetadata)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_heartbeat: Optional[datetime] = Field(None, description="Last successful heartbeat")
    last_health_check: Optional[datetime] = Field(None, description="Last health check time")

    # Stats
    request_count: int = Field(0, description="Total requests served")
    error_count: int = Field(0, description="Total errors")
    avg_latency_ms: Optional[float] = Field(None, description="Average response time")

    class Config:
        extra = "allow"

    def is_healthy(self) -> bool:
        """Check if service is in a healthy state."""
        return self.status in (ServiceStatus.HEALTHY, ServiceStatus.DEGRADED)

    def get_tool_url(self, tool_name: str) -> str:
        """Get the full URL for a tool endpoint."""
        base = self.base_url.rstrip("/")
        return f"{base}/tools/{tool_name}/execute"

    def to_discovery_format(self) -> Dict[str, Any]:
        """Export service info for discovery responses."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "base_url": self.base_url,
            "service_type": self.service_type.value,
            "status": self.status.value,
            "tool_count": self.tool_count,
            "capabilities": self.capabilities.model_dump(),
        }
