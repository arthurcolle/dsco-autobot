"""
Main FastAPI application for Tool Management.

This is the entry point that ties together the registry, tools, and API.
Includes durable task queue with admin operations.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api import router as tools_router
from .api import service_router
from .api.admin_routes import router as admin_router
from .api.retrieval_routes import router as retrieval_router
from .api.desktop_routes import router as desktop_router
from .api.gateway_routes import router as gateway_router
from .api.native_routes import router as native_router
from .api.composition_routes import router as composition_router
from .api.mcp_routes import router as mcp_router
from .api.compose_patterns import router as compose_patterns_router
from .api.semantic_mcp_routes import router as semantic_mcp_router
from .api.url_queue_routes import router as url_queue_router
from .api.auth_routes import router as auth_router
from .api.swarm_routes import router as swarm_router
from .api.tools_api import router as tools_api_router
from .api.advanced_search_routes import router as advanced_search_router
from .registry import get_registry, get_service_registry
from .persistence import get_tool_store
from .queue import get_task_queue, MCPBackend
from .native_mcp_manager import get_native_mcp_manager, init_native_mcp_manager

# Core components
from .core.router import get_router, BackendStatus
from .core.cache import get_cache
from .core.metrics import get_metrics_collector

# Import tools to register them
from . import tools  # noqa: F401

logger = logging.getLogger(__name__)

# Data directory for persistent storage
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging(debug: bool = False) -> None:
    """Configure application logging."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not debug:
        for noisy in ("uvicorn.error", "uvicorn.access", "asyncio"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle handler."""
    debug = os.getenv("DEBUG", "0") == "1"
    configure_logging(debug)

    registry = get_registry()
    service_registry = get_service_registry()
    logger.info(f"Tool Management starting with {len(registry)} tools registered")
    logger.info(f"Tools: {registry.list_names()}")
    logger.info(f"Service registry initialized with {len(service_registry)} services")

    # Initialize task queue with persistent storage
    queue = get_task_queue(db_path=str(DATA_DIR / "task_queue.db"))
    logger.info(f"Task queue initialized at {DATA_DIR / 'task_queue.db'}")

    # Register default MCP backends from environment
    await _register_default_backends(queue)

    # Register backends in persistent tool store
    await _register_persistent_backends()

    # Initialize core components
    intelligent_router = get_router()
    cache = get_cache()
    metrics = get_metrics_collector()

    # Register backends with intelligent router
    for backend in queue.list_backends():
        intelligent_router.register_backend(
            backend_id=backend.id,
            capabilities=backend.capabilities,
            max_connections=backend.max_concurrent,
        )
        intelligent_router.update_status(backend.id, BackendStatus.HEALTHY)

    logger.info(f"Initialized intelligent router with {len(queue.list_backends())} backends")
    logger.info(f"Cache initialized (max_size={cache.max_size})")

    # Start queue processor
    auto_start_queue = os.getenv("AUTO_START_QUEUE", "1") == "1"
    if auto_start_queue:
        await queue.start()
        logger.info("Task queue processor started")

    # Start native MCP servers
    auto_start_native = os.getenv("AUTO_START_NATIVE_MCPS", "1") == "1"
    native_manager = None
    if auto_start_native:
        try:
            host = os.getenv("HOST", "0.0.0.0")
            port = int(os.getenv("PORT", "2016"))
            native_manager = await init_native_mcp_manager(
                registry_url=f"http://{host}:{port}"
            )
            logger.info("Native MCP servers started")

            # Register native MCPs as backends with the queue
            for mcp_id, state in native_manager.servers.items():
                if state.status.value == "running":
                    config = state.config
                    queue.register_backend(MCPBackend(
                        id=config.id,
                        name=config.name,
                        url=f"http://localhost:{config.port}",
                        health_endpoint=config.health_endpoint,
                        mcp_endpoint=config.mcp_endpoint,
                        capabilities=config.capabilities,
                        max_concurrent=10,
                        priority=15,  # Higher priority than external MCPs
                        metadata={"native": True, "port": config.port}
                    ))
                    logger.info(f"Registered native backend: {config.name}")

        except Exception as e:
            logger.error(f"Failed to start native MCPs: {e}")

    # Initialize semantic tool index (always run, not just for native MCPs)
    try:
        from .api.semantic_mcp_routes import initialize_semantic_index
        logger.info("Initializing semantic tool index...")
        await initialize_semantic_index()
        logger.info("Semantic tool index initialized")
    except Exception as e:
        logger.error(f"Failed to initialize semantic index: {e}")

    # Initialize advanced tool index with multi-faceted embeddings
    try:
        from .api.advanced_search_routes import reindex_all
        logger.info("Initializing advanced tool index...")
        await reindex_all()
        logger.info("Advanced tool index initialized")
    except Exception as e:
        logger.error(f"Failed to initialize advanced index: {e}")

    # Auto-sync tools from all backends
    try:
        from .api.mcp_routes import refresh_mcp_tools
        logger.info("Syncing tools from backends...")
        await refresh_mcp_tools()
        logger.info("Tool sync complete")
    except Exception as e:
        logger.error(f"Failed to sync tools: {e}")

    yield

    # Stop native MCP servers
    if native_manager:
        await native_manager.stop()
        logger.info("Native MCP servers stopped")

    # Stop queue processor
    if auto_start_queue:
        await queue.stop()
        logger.info("Task queue processor stopped")

    logger.info("Tool Management shutting down")


async def _register_persistent_backends():
    """Register Modal backends in the persistent tool store and sync their tools."""
    store = get_tool_store()

    # Modal backend URLs from environment
    backends_config = [
        {
            "backend_id": "distributed-search-mcp",
            "name": "Distributed Search MCP",
            "url": os.getenv("SEARCH_MCP_URL", "https://arthurcolle--distributed-search-mcp-web.modal.run"),
            "health_endpoint": "/health",
            "mcp_endpoint": "/mcp",
            "capabilities": ["search", "read", "verify", "rerank", "embed"]
        },
        {
            "backend_id": "distributed-research-mcp",
            "name": "Distributed Research MCP",
            "url": os.getenv("RESEARCH_MCP_URL", "https://arthurcolle--distributed-research-mcp-web.modal.run"),
            "health_endpoint": "/health",
            "mcp_endpoint": "/mcp",
            "capabilities": ["deep_search", "run_task", "extract_data", "create_task_group", "find_all", "synthesize"]
        },
        {
            "backend_id": "distributed-publish-mcp",
            "name": "Distributed Publish MCP",
            "url": os.getenv("PUBLISH_MCP_URL", "https://arthurcolle--distributed-publish-mcp-web.modal.run"),
            "health_endpoint": "/health",
            "mcp_endpoint": "/mcp",
            "capabilities": ["webhooks", "events", "notifications", "syndication", "message_queue"]
        },
        {
            "backend_id": "distributed-memory-mcp",
            "name": "Distributed Memory MCP",
            "url": os.getenv("MEMORY_MCP_URL", "https://arthurcolle--distributed-memory-mcp-web.modal.run"),
            "health_endpoint": "/health",
            "mcp_endpoint": "/mcp",
            "capabilities": ["working_memory", "episodic_memory", "semantic_memory", "procedural_memory", "cognitive_architecture"]
        },
    ]

    for config in backends_config:
        try:
            store.register_backend(
                backend_id=config["backend_id"],
                name=config["name"],
                url=config["url"],
                health_endpoint=config["health_endpoint"],
                mcp_endpoint=config["mcp_endpoint"],
                capabilities=config["capabilities"]
            )
            logger.info(f"Registered persistent backend: {config['backend_id']} at {config['url']}")
        except Exception as e:
            logger.error(f"Failed to register backend {config['backend_id']}: {e}")


async def _register_default_backends(queue):
    """Register MCP backends from environment variables."""
    # Parallel AI backend
    parallel_url = os.getenv("PARALLEL_AI_URL", "http://localhost:2040")
    if parallel_url:
        queue.register_backend(MCPBackend(
            id="parallel-ai",
            name="Parallel AI MCP",
            url=parallel_url,
            health_endpoint="/health",
            mcp_endpoint="/mcp",
            capabilities=["search", "extract", "tasks", "findall", "orchestration"],
            max_concurrent=10,
            priority=10,
        ))
        logger.info(f"Registered Parallel AI backend at {parallel_url}")

    # Jina AI backend
    jina_url = os.getenv("JINA_MCP_URL", "http://localhost:8787")
    if jina_url:
        queue.register_backend(MCPBackend(
            id="jina-mcp",
            name="Jina MCP",
            url=jina_url,
            health_endpoint="/",
            mcp_endpoint="/mcp",
            capabilities=["search_web", "search_arxiv", "read_url", "embeddings", "memory"],
            max_concurrent=10,
            priority=5,
        ))
        logger.info(f"Registered Jina MCP backend at {jina_url}")

    # Native MCP backends (deployed on Modal or local)
    search_mcp_url = os.getenv("SEARCH_MCP_URL")
    if search_mcp_url:
        queue.register_backend(MCPBackend(
            id="distributed-search-mcp",
            name="Distributed Search MCP",
            url=search_mcp_url,
            health_endpoint="/health",
            mcp_endpoint="/mcp",
            capabilities=["search", "read", "verify", "rerank", "embed"],
            max_concurrent=50,
            priority=15,
            metadata={"native": True, "provider": "modal"},
        ))
        logger.info(f"Registered Search MCP backend at {search_mcp_url}")

    research_mcp_url = os.getenv("RESEARCH_MCP_URL")
    if research_mcp_url:
        queue.register_backend(MCPBackend(
            id="distributed-research-mcp",
            name="Distributed Research MCP",
            url=research_mcp_url,
            health_endpoint="/health",
            mcp_endpoint="/mcp",
            capabilities=["deep_search", "run_task", "extract_data", "create_task_group", "find_all", "synthesize"],
            max_concurrent=50,
            priority=15,
            metadata={"native": True, "provider": "modal"},
        ))
        logger.info(f"Registered Research MCP backend at {research_mcp_url}")

    publish_mcp_url = os.getenv("PUBLISH_MCP_URL")
    if publish_mcp_url:
        queue.register_backend(MCPBackend(
            id="distributed-publish-mcp",
            name="Distributed Publish MCP",
            url=publish_mcp_url,
            health_endpoint="/health",
            mcp_endpoint="/mcp",
            capabilities=["webhooks", "events", "notifications", "syndication", "message_queue"],
            max_concurrent=50,
            priority=15,
            metadata={"native": True, "provider": "modal"},
        ))
        logger.info(f"Registered Publish MCP backend at {publish_mcp_url}")

    memory_mcp_url = os.getenv("MEMORY_MCP_URL")
    if memory_mcp_url:
        queue.register_backend(MCPBackend(
            id="distributed-memory-mcp",
            name="Distributed Memory MCP",
            url=memory_mcp_url,
            health_endpoint="/health",
            mcp_endpoint="/mcp",
            capabilities=["working_memory", "episodic_memory", "semantic_memory", "procedural_memory", "cognitive_architecture"],
            max_concurrent=50,
            priority=15,
            metadata={"native": True, "provider": "modal"},
        ))
        logger.info(f"Registered Memory MCP backend at {memory_mcp_url}")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    debug = os.getenv("DEBUG", "0") == "1"
    cors_origins = os.getenv("CORS_ORIGINS", "*")

    app = FastAPI(
        title="Tool Management API",
        description="Register, discover, and execute tools",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if debug or os.getenv("ENABLE_DOCS", "1") == "1" else None,
        redoc_url="/redoc" if debug or os.getenv("ENABLE_DOCS", "1") == "1" else None,
    )

    # CORS
    origins = ["*"] if cors_origins == "*" else [o.strip() for o in cors_origins.split(",")]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request logging middleware for debugging
    @app.middleware("http")
    async def log_requests(request, call_next):
        logger.info(f"Request: {request.method} {request.url.path}")
        response = await call_next(request)
        logger.info(f"Response: {response.status_code}")
        return response

    # Root endpoint
    @app.get("/")
    async def root():
        registry = get_registry()
        service_registry = get_service_registry()
        native_manager = get_native_mcp_manager()
        native_status = native_manager.get_status() if native_manager else {}

        return {
            "name": "Tool Management API",
            "version": "1.0.0",
            "status": "running",
            "tools_registered": len(registry),
            "services_registered": len(service_registry),
            "native_mcps": {
                "running": native_status.get("running", False),
                "count": len(native_status.get("servers", {})),
                "servers": {
                    mcp_id: info.get("status")
                    for mcp_id, info in native_status.get("servers", {}).items()
                },
            },
            "endpoints": {
                "docs": "/docs",
                "redoc": "/redoc",
                "health": "/health",
                "gateway": "/api/v1/gateway",
                "tools": "/api/v1/tools",
                "services": "/api/v1/services",
                "admin": "/api/v1/admin",
                "desktop": "/api/v1/desktop",
                "native": "/api/v1/native",
                "urls": "/api/v1/urls",
            },
        }

    # Health check
    @app.get("/health")
    async def health():
        registry = get_registry()
        return {
            "status": "healthy",
            "tool_count": len(registry),
        }

    # Include retrieval routes FIRST (specific paths before wildcards)
    app.include_router(retrieval_router, prefix="/api/v1")

    # Include clean tools API (persistent, robust) - BEFORE tools_router to avoid wildcard conflicts
    app.include_router(tools_api_router)

    # Include tool routes
    app.include_router(tools_router, prefix="/api/v1")

    # Include service routes
    app.include_router(service_router, prefix="/api/v1")

    # Include admin routes for task queue management
    app.include_router(admin_router, prefix="/api/v1")

    # Include URL queue routes for search URL tracking
    app.include_router(url_queue_router, prefix="/api/v1")

    # Include desktop/filesystem MCP server routes
    app.include_router(desktop_router, prefix="/api/v1")

    # Include unified gateway routes (pipelines, batch, streaming, etc.)
    app.include_router(gateway_router, prefix="/api/v1")

    # Include native MCP management routes
    app.include_router(native_router, prefix="/api/v1")

    # Include advanced orchestration routes
    from .api.advanced_orchestration import router as orchestration_router
    app.include_router(orchestration_router, prefix="/api/v1")

    # Include composition/workflow routes with schema registry
    app.include_router(composition_router, prefix="/api/v1")

    # Include MCP protocol routes (direct /mcp access for SSE connections)
    app.include_router(mcp_router)

    # Include functional composition patterns (/compose/seq, /compose/map, etc.)
    app.include_router(compose_patterns_router)

    # Include semantic MCP routes (/semantic-mcp with intelligent context management)
    app.include_router(semantic_mcp_router)

    # Include authorization routes (/api/v1/auth for warrants and receipts)
    app.include_router(auth_router, prefix="/api/v1")

    # Include swarm routes (/api/v1/swarm for agent swarm economy)
    app.include_router(swarm_router, prefix="/api/v1")

    # Include advanced search routes (/api/v1/discover for multi-faceted tool discovery)
    app.include_router(advanced_search_router)

    return app


# Create app instance
app = create_app()


def main():
    """Run the application with uvicorn."""
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "0") == "1"

    uvicorn.run(
        "tool_management.app:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
