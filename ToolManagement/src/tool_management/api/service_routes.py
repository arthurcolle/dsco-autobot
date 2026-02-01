"""
FastAPI routes for Service Management.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..registry.service_registry import get_service_registry
from ..models.service import (
    Service,
    ServiceStatus,
    ServiceType,
    ServiceCapabilities,
    ServiceMetadata,
    ServiceHealthCheck,
)

router = APIRouter(prefix="/services", tags=["services"])


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _lookup_service(registry, identifier: str):
    """
    Look up a service by any identifier (UUID, service_id, or display name).
    Tries in order: UUID -> service_id -> name
    """
    # Try by UUID first
    service = registry.get(identifier)
    if service:
        return service
    # Try by human-readable service_id (e.g., 'search-api')
    service = registry.get_by_service_id(identifier)
    if service:
        return service
    # Try by display name
    return registry.get_by_name(identifier)


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class ServiceCreate(BaseModel):
    """Request model for registering a service."""
    name: str
    service_id: Optional[str] = None  # Human-readable ID like 'search-api'
    description: str = ""
    version: str = "1.0.0"
    base_url: str
    service_type: str = "rest"
    tool_names: List[str] = []
    capabilities: Optional[Dict[str, Any]] = None
    health_check: Optional[Dict[str, Any]] = None
    # New auth configuration
    auth: Optional[Dict[str, Any]] = None
    # Legacy fields (deprecated)
    auth_type: Optional[str] = None
    auth_header: Optional[str] = None
    metadata: Dict[str, Any] = {}


class ServiceUpdate(BaseModel):
    """Request model for updating a service."""
    description: Optional[str] = None
    version: Optional[str] = None
    base_url: Optional[str] = None
    tool_names: Optional[List[str]] = None
    capabilities: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ServiceResponse(BaseModel):
    """Response model for service data."""
    id: str
    name: str
    description: str
    version: str
    base_url: str
    service_type: str
    status: str
    tool_count: int
    tool_names: List[str]


class HeartbeatResponse(BaseModel):
    """Response for heartbeat requests."""
    service_id: str
    status: str
    acknowledged: bool


class HealthCheckResponse(BaseModel):
    """Response for health check requests."""
    service_id: str
    name: str
    status: str
    last_check: Optional[str]


# =============================================================================
# ROUTES
# =============================================================================

@router.get("", response_model=List[Dict[str, Any]])
async def list_services(
    query: Optional[str] = Query(None, description="Search query"),
    service_type: Optional[str] = Query(None, description="Service type filter"),
    status: Optional[str] = Query(None, description="Status filter"),
    has_tool: Optional[str] = Query(None, description="Filter by tool name"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List all registered services with optional filtering."""
    registry = get_service_registry()

    stype = ServiceType(service_type) if service_type else None
    sstatus = ServiceStatus(status) if status else None

    services = registry.search(
        query=query,
        service_type=stype,
        status=sstatus,
        has_tool=has_tool,
    )

    # Paginate
    services = services[offset:offset + limit]

    return [s.model_dump() for s in services]


@router.get("/names")
async def list_service_names():
    """List all service names."""
    registry = get_service_registry()
    return {"names": registry.list_names()}


@router.get("/healthy")
async def list_healthy_services():
    """List all healthy services."""
    registry = get_service_registry()
    services = registry.list_healthy()
    return {"services": [s.to_discovery_format() for s in services]}


@router.get("/stale")
async def list_stale_services():
    """List services that haven't sent heartbeats recently."""
    registry = get_service_registry()
    services = registry.list_stale()
    return {"services": [s.to_discovery_format() for s in services]}


@router.get("/by-tool/{tool_name}")
async def get_service_by_tool(tool_name: str):
    """Find the service that hosts a specific tool."""
    registry = get_service_registry()
    service = registry.get_by_tool(tool_name)
    if not service:
        raise HTTPException(status_code=404, detail=f"No service found for tool: {tool_name}")
    return service.model_dump()


@router.get("/{service_id}")
async def get_service(service_id: str):
    """Get a service by ID, service_id, or name."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")
    return service.model_dump()


@router.post("", status_code=201)
async def register_service(request: ServiceCreate):
    """Register a new service."""
    registry = get_service_registry()

    if request.name in registry:
        raise HTTPException(status_code=409, detail=f"Service already exists: {request.name}")

    # Check if service_id already exists
    if request.service_id and registry.get_by_service_id(request.service_id):
        raise HTTPException(status_code=409, detail=f"Service ID already exists: {request.service_id}")

    capabilities = ServiceCapabilities(**(request.capabilities or {}))
    health_check = ServiceHealthCheck(**(request.health_check or {}))
    metadata = ServiceMetadata(**request.metadata)

    # Build auth configuration
    from ..models.service import ServiceAuth
    auth = ServiceAuth(**(request.auth or {}))

    service = Service(
        service_id=request.service_id,
        name=request.name,
        description=request.description,
        version=request.version,
        base_url=request.base_url,
        service_type=ServiceType(request.service_type),
        tool_names=request.tool_names,
        tool_count=len(request.tool_names),
        capabilities=capabilities,
        health_check=health_check,
        auth=auth,
        auth_type=request.auth_type,  # Legacy
        auth_header=request.auth_header,  # Legacy
        metadata=metadata,
    )

    registry.register(service)
    return service.model_dump()


@router.patch("/{service_id}")
async def update_service(service_id: str, request: ServiceUpdate):
    """Update a service's information."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    update_data = request.model_dump(exclude_unset=True)

    if "status" in update_data:
        update_data["status"] = ServiceStatus(update_data["status"])

    if "tool_names" in update_data:
        registry.update_tools(service.id, update_data.pop("tool_names"))

    if "capabilities" in update_data:
        caps = update_data.pop("capabilities")
        service.capabilities = ServiceCapabilities(**caps)

    if "metadata" in update_data:
        meta = update_data.pop("metadata")
        for key, value in meta.items():
            setattr(service.metadata, key, value)

    for key, value in update_data.items():
        setattr(service, key, value)

    from datetime import datetime
    service.updated_at = datetime.utcnow()

    return service.model_dump()


@router.delete("/{service_id}", status_code=204)
async def unregister_service(service_id: str):
    """Unregister a service."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    registry.unregister(service.id)
    return None


@router.post("/{service_id}/heartbeat", response_model=HeartbeatResponse)
async def service_heartbeat(service_id: str):
    """Send a heartbeat for a service."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    registry.heartbeat(service.id)
    return HeartbeatResponse(
        service_id=service.id,
        status=service.status.value,
        acknowledged=True,
    )


@router.post("/{service_id}/health-check", response_model=HealthCheckResponse)
async def check_service_health(service_id: str):
    """Trigger a health check for a service."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    status = await registry.check_health(service)
    return HealthCheckResponse(
        service_id=service.id,
        name=service.name,
        status=status.value,
        last_check=service.last_health_check.isoformat() if service.last_health_check else None,
    )


@router.get("/{service_id}/tools")
async def get_service_tools(service_id: str):
    """Get the list of tools provided by a service."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    return {
        "service_id": service.id,
        "service_name": service.name,
        "tool_count": service.tool_count,
        "tools": service.tool_names,
    }


@router.put("/{service_id}/tools")
async def update_service_tools(service_id: str, tool_names: List[str]):
    """Update the list of tools for a service."""
    registry = get_service_registry()
    service = _lookup_service(registry, service_id)
    if not service:
        raise HTTPException(status_code=404, detail=f"Service not found: {service_id}")

    registry.update_tools(service.id, tool_names)
    return {
        "service_id": service.id,
        "service_name": service.name,
        "tool_count": len(tool_names),
        "tools": tool_names,
    }
