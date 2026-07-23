"""FastAPI application setup - Clean Parallel AI proxy."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.openapi.utils import get_openapi

from parallel_websearch_server.config import settings
from parallel_websearch_server.middleware import (
    ApiKeyCacheMiddleware,
    AuditMiddleware,
    LoggingMiddleware,
    RequestIdMiddleware,
)
from parallel_websearch_server.models import HealthCheckResponse, ErrorResponse, Error
from parallel_websearch_server.parallel_mcp import mount_parallel_mcp
from parallel_websearch_server.routes import (
    events,
    findall,
    mcp_endpoints,
    meta_pipeline,
    monitors,
    orchestration,
    resilience,
    search,
    storage,
    task_groups,
    tasks,
)
from parallel_websearch_server.state_store import ContextStateStore

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


SWAGGER_JS_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"
SWAGGER_CSS_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.min.css"
SWAGGER_FAVICON_URL = "https://parallel.ai/favicon-32x32.png"
REDOC_JS_URL = "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"

SWAGGER_UI_PARAMETERS = {
    "docExpansion": "list",
    "defaultModelsExpandDepth": 1,
    "defaultModelExpandDepth": 2,
    "syntaxHighlight.theme": "obsidian",
    "displayRequestDuration": True,
    "tryItOutEnabled": True,
}

# Tags matching actual Parallel AI API capabilities
OPENAPI_TAGS = [
    {"name": "Meta-Pipeline", "description": "Advanced meta-pipeline with knowledge graph integration - intelligent query planning, entity extraction, relationship mapping, and semantic enrichment."},
    {"name": "Search Orchestration", "description": "Iterative search with processor escalation - lite→base→core→pro→ultra with adaptive term discovery and synthesis."},
    {"name": "Search & Extract", "description": "Web search and content extraction (Beta)."},
    {"name": "Tasks", "description": "Task run lifecycle - create, poll, get results."},
    {"name": "Task Groups", "description": "Batch task orchestration (Beta)."},
    {"name": "FindAll", "description": "Prospecting/discovery flows (Beta)."},
    {"name": "Monitors", "description": "Scheduled web monitoring (Alpha)."},
    {"name": "Storage", "description": "Retrieve past tasks, searches, and results."},
    {"name": "Events", "description": "Real-time SSE streaming for UI updates."},
    {"name": "Resilience", "description": "Circuit breakers, health scoring, anomaly detection, dead letter queue, and distributed tracing."},
]


import asyncio


async def _process_failure_queue_periodically(interval_seconds: int = 60):
    """Background task to process storage failures with retry."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            from parallel_websearch_server.local_store import get_local_store
            store = get_local_store()
            results = store.process_failure_queue(batch_size=10)
            if results['success_count'] > 0:
                logger.info(f"Failure queue: recovered {results['success_count']} items")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error processing failure queue: {e}")


async def _cleanup_old_data_periodically(interval_hours: int = 24):
    """Background task to clean up old audit logs and events."""
    while True:
        try:
            await asyncio.sleep(interval_hours * 3600)
            from parallel_websearch_server.local_store import get_local_store
            store = get_local_store()
            events_deleted = store.cleanup_old_events(days=30)
            audits_deleted = store.cleanup_old_audit_logs(days=90)
            if events_deleted > 0 or audits_deleted > 0:
                logger.info(f"Cleanup: {events_deleted} events, {audits_deleted} audit logs")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    # Startup
    logger.info("Starting Parallel AI MCP Server")
    app.state.http_client = httpx.AsyncClient(
        base_url=settings.api_base_url,
        timeout=settings.request_timeout,
    )
    logger.info(f"Connected to Parallel API at {settings.api_base_url}")
    app.state.context_store = ContextStateStore(settings.cache_db_path)
    await app.state.context_store.initialize()
    logger.info("Context state cache initialized at %s", settings.cache_db_path)

    # Initialize local store for audit logging
    from parallel_websearch_server.local_store import get_local_store
    store = get_local_store()
    logger.info("Local store initialized for audit logging")

    # Log any pending storage failures
    failure_stats = store.get_failure_stats()
    if failure_stats['pending'] > 0:
        logger.warning(
            f"Found {failure_stats['pending']} pending storage failures from previous session"
        )

    # Start background tasks
    failure_queue_task = asyncio.create_task(
        _process_failure_queue_periodically(interval_seconds=60)
    )
    cleanup_task = asyncio.create_task(
        _cleanup_old_data_periodically(interval_hours=24)
    )

    # Register tools with Tool Management API
    try:
        from parallel_websearch_server.registry import register_tools_with_registry
        await register_tools_with_registry()
    except Exception as e:
        logger.warning(f"Failed to register with Tool Management: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Parallel AI MCP Server")

    # Cancel background tasks
    failure_queue_task.cancel()
    cleanup_task.cancel()
    try:
        await failure_queue_task
    except asyncio.CancelledError:
        pass
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    if hasattr(app.state, "http_client"):
        await app.state.http_client.aclose()
    logger.info("HTTP client closed")
    if hasattr(app.state, "context_store"):
        await app.state.context_store.close()
        logger.info("Context state cache closed")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="Parallel AI MCP Server",
        description="MCP server proxying Parallel.ai APIs with persistent task storage",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add custom middleware (order matters: last added = first executed)
    # AuditMiddleware captures complete request/response for persistent storage
    app.add_middleware(AuditMiddleware, capture_bodies=True)
    app.add_middleware(ApiKeyCacheMiddleware)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(LoggingMiddleware)

    # Exception handlers
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """Global exception handler."""
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=Error(
                    message="Internal server error",
                    detail=str(exc) if not settings.is_production else None,
                )
            ).model_dump(),
        )

    @app.exception_handler(httpx.HTTPError)
    async def httpx_exception_handler(request: Request, exc: httpx.HTTPError):
        """Handle HTTP client errors."""
        logger.error(f"HTTP client error: {exc}")
        return JSONResponse(
            status_code=502,
            content=ErrorResponse(
                error=Error(
                    message="Upstream API error",
                    detail=str(exc),
                )
            ).model_dump(),
        )

    # Include routers - ONLY real Parallel API proxies + storage
    app.include_router(mcp_endpoints.router)  # MCP-specific helpers
    app.include_router(search.router)         # /v1beta/search, /v1beta/extract
    app.include_router(tasks.router)          # /v1/tasks/runs/*
    app.include_router(task_groups.router)    # /v1beta/tasks/groups/*
    app.include_router(findall.router)        # /v1beta/findall/*
    app.include_router(monitors.router)       # /v1alpha/monitors/*
    app.include_router(storage.router)        # /v1/stored/* - retrieve past tasks/searches
    app.include_router(events.router)         # /v1/events/* - real-time SSE streaming
    app.include_router(meta_pipeline.router)  # /v1/meta/* - advanced meta-pipeline with KG
    app.include_router(resilience.router)     # /v1/resilience/* - circuit breakers, health, DLQ
    app.include_router(orchestration.router)  # /v1/orchestration/* - iterative search with escalation

    # Mount MCP server
    mcp_server = mount_parallel_mcp(app)
    app.state.mcp_server = mcp_server

    # Health check endpoint
    @app.get("/health", response_model=HealthCheckResponse)
    async def health_check():
        """Health check endpoint."""
        return HealthCheckResponse(
            status="healthy",
            version="1.0.0",
            timestamp=datetime.utcnow(),
        )

    @app.get("/")
    async def root():
        """Root endpoint."""
        return {
            "name": "Parallel AI MCP Server",
            "version": "1.0.0",
            "description": "Clean proxy to Parallel AI APIs with persistent task storage",
            "docs": "/docs",
            "health": "/health",
            "endpoints": {
                # Actual Parallel API endpoints
                "search": "/v1beta/search",
                "extract": "/v1beta/extract",
                "tasks": "/v1/tasks/runs",
                "task_groups": "/v1beta/tasks/groups",
                "findall": "/v1beta/findall/runs",
                "monitors": "/v1alpha/monitors",
                # Storage/retrieval helpers
                "stored_tasks": "/v1/stored/tasks",
                "stored_searches": "/v1/stored/searches",
                # Advanced features
                "orchestration": "/v1/orchestration/search",
                "resilience": "/v1/resilience/status",
                "meta_pipeline": "/v1/meta/query",
                # MCP
                "mcp_http": settings.mcp_http_path,
                "mcp_sse": f"{settings.mcp_sse_path}/messages/",
            },
        }

    @app.get("/docs", include_in_schema=False)
    async def swagger_ui() -> HTMLResponse:
        """Serve a customized Swagger UI."""
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} · Interactive Reference",
            swagger_js_url=SWAGGER_JS_URL,
            swagger_css_url=SWAGGER_CSS_URL,
            swagger_favicon_url=SWAGGER_FAVICON_URL,
            oauth2_redirect_url="/docs/oauth2-redirect",
            init_oauth=None,
            swagger_ui_parameters=SWAGGER_UI_PARAMETERS,
        )

    @app.get("/docs/oauth2-redirect", include_in_schema=False)
    async def swagger_ui_redirect() -> HTMLResponse:
        """OAuth2 redirect helper required by Swagger UI."""
        return get_swagger_ui_oauth2_redirect_html()

    @app.get("/redoc", include_in_schema=False)
    async def redoc_ui() -> HTMLResponse:
        """Serve a polished ReDoc experience."""
        return get_redoc_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} · ReDoc",
            redoc_js_url=REDOC_JS_URL,
            with_google_fonts=False,
            redoc_favicon_url=SWAGGER_FAVICON_URL,
        )

    def _openapi_servers() -> List[Dict[str, str]]:
        """Return the advertised server entries shown in docs."""
        local_base = f"http://{settings.host}:{settings.port}"
        servers = [{"url": local_base, "description": "Parallel AI MCP Server"}]
        servers.append(
            {
                "url": settings.api_base_url.rstrip("/"),
                "description": "Upstream Parallel API",
            }
        )
        return servers

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        openapi_schema["tags"] = OPENAPI_TAGS
        openapi_schema["servers"] = _openapi_servers()
        openapi_schema["info"]["contact"] = {
            "name": "Parallel AI",
            "url": "https://parallel.ai",
            "email": "support@parallel.ai",
        }
        openapi_schema["info"]["license"] = {"name": "Apache 2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0"}
        openapi_schema["info"]["x-logo"] = {
            "url": "https://parallel.ai/icon.png",
            "altText": "Parallel",
        }
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi

    return app


app = create_app()
