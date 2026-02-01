"""
Service Registry - Central registry for service registration and discovery.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

from ..models.service import Service, ServiceStatus, ServiceType

logger = logging.getLogger(__name__)


class ServiceRegistry:
    """
    Central registry for managing tool-hosting services.

    Supports:
    - Service registration/deregistration
    - Heartbeat tracking
    - Health check monitoring
    - Service discovery
    - Tool-to-service mapping
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self._services: Dict[str, Service] = {}
        self._by_name: Dict[str, str] = {}  # display name -> id mapping
        self._by_service_id: Dict[str, str] = {}  # service_id -> id mapping
        self._by_tool: Dict[str, str] = {}  # tool_name -> service_id
        self._health_check_task: Optional[asyncio.Task] = None
        self._heartbeat_timeout = timedelta(minutes=2)

    def register(self, service: Service) -> Service:
        """
        Register a service in the registry.

        Args:
            service: Service instance to register

        Returns:
            The registered service
        """
        # Check if service already exists by name or service_id
        existing_id = None
        if service.name in self._by_name:
            existing_id = self._by_name[service.name]
        elif service.service_id and service.service_id in self._by_service_id:
            existing_id = self._by_service_id[service.service_id]

        if existing_id:
            logger.warning(f"Service '{service.name}' already registered (id: {existing_id}), updating...")
            old_service = self._services[existing_id]
            self._remove_tool_mappings(old_service)
            # Remove old index entries
            if old_service.name in self._by_name:
                del self._by_name[old_service.name]
            if old_service.service_id and old_service.service_id in self._by_service_id:
                del self._by_service_id[old_service.service_id]
            del self._services[existing_id]

        self._services[service.id] = service
        self._by_name[service.name] = service.id

        # Index by service_id if provided
        if service.service_id:
            self._by_service_id[service.service_id] = service.id

        # Index tools
        for tool_name in service.tool_names:
            self._by_tool[tool_name] = service.id

        service.last_heartbeat = datetime.utcnow()
        logger.info(f"Registered service: {service.name} (service_id: {service.service_id}, id: {service.id}) with {len(service.tool_names)} tools")
        return service

    def _remove_tool_mappings(self, service: Service) -> None:
        """Remove tool mappings for a service."""
        for tool_name in service.tool_names:
            if self._by_tool.get(tool_name) == service.id:
                del self._by_tool[tool_name]

    def unregister(self, service_id: str) -> Optional[Service]:
        """Remove a service from the registry."""
        if service_id not in self._services:
            return None

        service = self._services.pop(service_id)
        if service.name in self._by_name:
            del self._by_name[service.name]
        if service.service_id and service.service_id in self._by_service_id:
            del self._by_service_id[service.service_id]
        self._remove_tool_mappings(service)
        logger.info(f"Unregistered service: {service.name}")
        return service

    def get(self, service_id: str) -> Optional[Service]:
        """Get a service by ID."""
        return self._services.get(service_id)

    def get_by_name(self, name: str) -> Optional[Service]:
        """Get a service by display name."""
        service_id = self._by_name.get(name)
        return self._services.get(service_id) if service_id else None

    def get_by_service_id(self, service_id: str) -> Optional[Service]:
        """Get a service by human-readable service_id (e.g., 'search-api')."""
        internal_id = self._by_service_id.get(service_id)
        return self._services.get(internal_id) if internal_id else None

    def get_by_tool(self, tool_name: str) -> Optional[Service]:
        """Find the service that hosts a specific tool."""
        service_id = self._by_tool.get(tool_name)
        return self._services.get(service_id) if service_id else None

    def heartbeat(self, service_id: str) -> bool:
        """
        Update the heartbeat timestamp for a service.

        Returns:
            True if service exists and was updated, False otherwise
        """
        service = self._services.get(service_id)
        if not service:
            return False

        service.last_heartbeat = datetime.utcnow()
        service.updated_at = datetime.utcnow()
        return True

    def search(
        self,
        query: Optional[str] = None,
        service_type: Optional[ServiceType] = None,
        status: Optional[ServiceStatus] = None,
        has_tool: Optional[str] = None,
    ) -> List[Service]:
        """
        Search for services matching criteria.

        Args:
            query: Text search in name/description
            service_type: Filter by service type
            status: Filter by status
            has_tool: Filter by services that have a specific tool

        Returns:
            List of matching services
        """
        results = list(self._services.values())

        if query:
            query_lower = query.lower()
            results = [
                s for s in results
                if query_lower in s.name.lower() or query_lower in s.description.lower()
            ]

        if service_type:
            results = [s for s in results if s.service_type == service_type]

        if status:
            results = [s for s in results if s.status == status]

        if has_tool:
            results = [s for s in results if has_tool in s.tool_names]

        return results

    def list_all(self) -> List[Service]:
        """List all registered services."""
        return list(self._services.values())

    def list_names(self) -> List[str]:
        """List all service names."""
        return list(self._by_name.keys())

    def list_healthy(self) -> List[Service]:
        """List all healthy services."""
        return [s for s in self._services.values() if s.is_healthy()]

    def list_stale(self, timeout: Optional[timedelta] = None) -> List[Service]:
        """List services that haven't sent a heartbeat recently."""
        timeout = timeout or self._heartbeat_timeout
        cutoff = datetime.utcnow() - timeout
        return [
            s for s in self._services.values()
            if s.last_heartbeat is None or s.last_heartbeat < cutoff
        ]

    async def check_health(self, service: Service) -> ServiceStatus:
        """
        Perform a health check on a service.

        Args:
            service: Service to check

        Returns:
            Updated status
        """
        url = f"{service.base_url.rstrip('/')}{service.health_check.endpoint}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    timeout=service.health_check.timeout_seconds,
                )
                service.last_health_check = datetime.utcnow()

                if response.status_code == 200:
                    service.status = ServiceStatus.HEALTHY
                elif response.status_code < 500:
                    service.status = ServiceStatus.DEGRADED
                else:
                    service.status = ServiceStatus.UNHEALTHY

        except httpx.TimeoutException:
            logger.warning(f"Health check timeout for service: {service.name}")
            service.status = ServiceStatus.UNHEALTHY
        except httpx.RequestError as e:
            logger.warning(f"Health check failed for service {service.name}: {e}")
            service.status = ServiceStatus.OFFLINE

        service.updated_at = datetime.utcnow()
        return service.status

    async def check_all_health(self) -> Dict[str, ServiceStatus]:
        """Run health checks on all registered services."""
        results = {}
        tasks = [self.check_health(s) for s in self._services.values()]
        statuses = await asyncio.gather(*tasks, return_exceptions=True)

        for service, status in zip(self._services.values(), statuses):
            if isinstance(status, Exception):
                results[service.name] = ServiceStatus.UNKNOWN
            else:
                results[service.name] = status

        return results

    async def start_health_checks(self, interval: int = 30) -> None:
        """Start periodic health check background task."""
        if self._health_check_task and not self._health_check_task.done():
            return

        async def health_check_loop():
            while True:
                try:
                    await self.check_all_health()
                except Exception as e:
                    logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(interval)

        self._health_check_task = asyncio.create_task(health_check_loop())
        logger.info(f"Started health check background task (interval: {interval}s)")

    async def stop_health_checks(self) -> None:
        """Stop the health check background task."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
            logger.info("Stopped health check background task")

    def update_tools(self, service_id: str, tool_names: List[str]) -> bool:
        """
        Update the list of tools for a service.

        Args:
            service_id: Service ID
            tool_names: New list of tool names

        Returns:
            True if updated, False if service not found
        """
        service = self._services.get(service_id)
        if not service:
            return False

        # Remove old mappings
        self._remove_tool_mappings(service)

        # Update and add new mappings
        service.tool_names = tool_names
        service.tool_count = len(tool_names)
        for tool_name in tool_names:
            self._by_tool[tool_name] = service_id

        service.updated_at = datetime.utcnow()
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Export registry as dictionary."""
        return {
            "name": self.name,
            "service_count": len(self._services),
            "services": [s.model_dump() for s in self._services.values()],
            "tool_count": len(self._by_tool),
        }

    def __len__(self) -> int:
        return len(self._services)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self._services.values())


# Global service registry instance
_global_service_registry: Optional[ServiceRegistry] = None


def get_service_registry() -> ServiceRegistry:
    """Get the global service registry."""
    global _global_service_registry
    if _global_service_registry is None:
        _global_service_registry = ServiceRegistry("global")
    return _global_service_registry
