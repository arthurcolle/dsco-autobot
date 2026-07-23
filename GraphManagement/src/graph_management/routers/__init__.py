"""Graph Management API routers.

This package contains the FastAPI routers organized by functional domain:

- core: Health checks, schema introspection, and query endpoints
- organizations: Teams, projects, and organizational context
- resources: Tools, datasets, secrets, and data contracts
- model: Model bootstrap and status
- policies: ABAC policies and policy management
- capabilities: Capability token management
- agents: Agent registration and management
- roles: Role assignment and management
- budgets: Budget and quota management
- runs: Run tracking and execution
- events: Event streaming
- registry: Schema registry and proposals
- ingest: Data ingestion endpoints
"""

from .core import router as core_router
from .organizations import router as organizations_router
from .resources import router as resources_router
from .model import router as model_router
from .policies import router as policies_router
from .capabilities import router as capabilities_router
from .agents import router as agents_router
from .roles import router as roles_router
from .budgets import router as budgets_router
from .runs import router as runs_router
from .events import router as events_router
from .registry import router as registry_router
from .ingest import router as ingest_router
from .overlay import router as overlay_router

__all__ = [
    "core_router",
    "organizations_router",
    "resources_router",
    "model_router",
    "policies_router",
    "capabilities_router",
    "agents_router",
    "roles_router",
    "budgets_router",
    "runs_router",
    "events_router",
    "registry_router",
    "ingest_router",
    "overlay_router",
]
