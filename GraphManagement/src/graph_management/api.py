from __future__ import annotations

"""
Graph Management API overlay as a separate Modal service.

Design:
- This service does NOT start Neo4j. It connects to the existing Neo4j
  Modal app by calling its typed /v1 endpoints over HTTP.
- Configure the base URL of the Neo4j service via BACKEND_URL env var.
  Example: BACKEND_URL=https://<your-neo4j-app>.modal.run

Endpoints:
- GET  /health
- GET  /v1/schema
- GET  /v1/indexes
- GET  /v1/constraints
- POST /v1/query
- POST /v1/tx

You can incrementally add domain operations (nodes, relationships, etc.) on top.
"""

import logging
import os
import traceback
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# --- Include routers (organized by domain) ---
from .routers import (
    core_router,
    organizations_router,
    resources_router,
    model_router,
    policies_router,
    capabilities_router,
    agents_router,
    roles_router,
    budgets_router,
    runs_router,
    events_router,
    registry_router,
    ingest_router,
    overlay_router,
)


tags_metadata = [
    {
        "name": "core",
        "description": "Core API operations including health checks, schema introspection, and query execution.",
    },
    {
        "name": "organizations",
        "description": "Organization management including teams, projects, and authentication context.",
    },
    {
        "name": "resources",
        "description": "Resource management including tools, datasets, secrets, and data contracts.",
    },
]

api = FastAPI(
    title="Graph Management API",
    description="A comprehensive API for managing graph databases with RBAC and ABAC security.",
    version="0.1.0",
    openapi_tags=tags_metadata,
)

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api.include_router(core_router)
api.include_router(organizations_router)
api.include_router(resources_router)
api.include_router(model_router)
api.include_router(policies_router)
api.include_router(capabilities_router)
api.include_router(agents_router)
api.include_router(roles_router)
api.include_router(budgets_router)
api.include_router(runs_router)
api.include_router(events_router)
api.include_router(registry_router)
api.include_router(ingest_router)
api.include_router(overlay_router)

# --- Logging and error handling ---
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, _LOG_LEVEL, logging.INFO))
_logger = logging.getLogger("graph_management_api")
_DEBUG_ERRORS = os.environ.get("DEBUG_ERRORS", "0") in {"1", "true", "True", "yes", "on"}


@api.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    _logger.warning(
        "HTTPException %s %s -> %s: %s", request.method, request.url.path, exc.status_code, exc.detail
    )
    payload: Dict[str, Any] = {"detail": exc.detail}
    payload["error"] = {
        "type": "HTTPException",
        "status_code": exc.status_code,
        "message": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        "path": request.url.path,
        "method": request.method,
    }
    return JSONResponse(status_code=exc.status_code, content=payload)


@api.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    _logger.warning("Request validation failed %s %s: %s", request.method, request.url.path, exc)
    payload: Dict[str, Any] = {"detail": exc.errors()}
    payload["error"] = {
        "type": "RequestValidationError",
        "message": str(exc),
        "path": request.url.path,
        "method": request.method,
    }
    return JSONResponse(status_code=422, content=payload)


@api.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    _logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    payload: Dict[str, Any] = {"detail": "Internal Server Error"}
    payload["error"] = {
        "type": exc.__class__.__name__,
        "message": str(exc),
        "path": request.url.path,
        "method": request.method,
    }
    if _DEBUG_ERRORS:
        payload["error"]["trace"] = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(status_code=500, content=payload)


def create_app() -> FastAPI:
    """Return the configured FastAPI application."""
    return api
