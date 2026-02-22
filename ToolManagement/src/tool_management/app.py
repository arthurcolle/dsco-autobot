"""
Main FastAPI application for Tool Management.

This is the entry point that ties together the registry, tools, and API.
Includes durable task queue with admin operations.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .api.routes import router as tools_router
from .api.service_routes import router as service_router
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
from .api.execution_routes import router as execution_router
from .api.advanced_search_routes import router as advanced_search_router
from .api.job_routes import router as job_router
from .oauth import router as oauth_router
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
    app.state.warm_pool_state = {
        "enabled": False,
        "interval_seconds": None,
        "last_run_at": None,
        "runs": 0,
        "backends": {},
    }
    app.state.cleanup_state = {
        "enabled": False,
        "interval_seconds": None,
        "last_run_at": None,
        "runs": 0,
        "totals": {
            "job_executions": 0,
            "workflow_executions": 0,
            "workflow_artifacts": 0,
            "workflow_side_effects": 0,
            "workflow_definitions": 0,
        },
        "last_run": {},
        "retention_seconds": {},
    }

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

    # Register backends in persistent tool store (optional; can be disabled on Modal)
    register_persistent = os.getenv("REGISTER_PERSISTENT_BACKENDS", "1") == "1"
    if register_persistent:
        await _register_persistent_backends()
    else:
        logger.info("Persistent backend registration disabled (REGISTER_PERSISTENT_BACKENDS=0)")

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

    # Initialize streaming executor for async jobs
    _cleanup_task = None
    try:
        from .core.streaming import create_tool_executor_for_jobs, get_streaming_executor
        from .persistence import get_execution_store
        tool_executor_fn = create_tool_executor_for_jobs()
        streaming_executor = get_streaming_executor(tool_executor=tool_executor_fn)
        execution_store = get_execution_store()
        cleanup_interval_seconds = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "300"))
        job_ttl = int(os.getenv("JOB_EXECUTION_TTL_SECONDS", "3600"))
        workflow_ttl = int(os.getenv("WORKFLOW_EXECUTION_TTL_SECONDS", "86400"))
        artifact_ttl = int(os.getenv("WORKFLOW_ARTIFACT_TTL_SECONDS", str(workflow_ttl)))
        side_effect_ttl = int(os.getenv("WORKFLOW_SIDE_EFFECT_TTL_SECONDS", str(workflow_ttl)))
        definition_ttl = int(os.getenv("WORKFLOW_DEFINITION_TTL_SECONDS", "0"))
        app.state.cleanup_state = {
            "enabled": True,
            "interval_seconds": cleanup_interval_seconds,
            "last_run_at": None,
            "runs": 0,
            "totals": {
                "job_executions": 0,
                "workflow_executions": 0,
                "workflow_artifacts": 0,
                "workflow_side_effects": 0,
                "workflow_definitions": 0,
            },
            "last_run": {},
            "retention_seconds": {
                "job_executions": job_ttl,
                "workflow_executions": workflow_ttl,
                "workflow_artifacts": artifact_ttl,
                "workflow_side_effects": side_effect_ttl,
                "workflow_definitions": definition_ttl,
            },
        }
        logger.info("Streaming executor initialized for async jobs")

        async def _cleanup_old_jobs():
            while True:
                await asyncio.sleep(cleanup_interval_seconds)
                cleanup_run = {
                    "job_executions": 0,
                    "workflow_executions": 0,
                    "workflow_artifacts": 0,
                    "workflow_side_effects": 0,
                    "workflow_definitions": 0,
                }
                try:
                    removed = streaming_executor.cleanup_old(max_age_seconds=job_ttl)
                    cleanup_run["job_executions"] = int(removed or 0)
                    if removed:
                        logger.info(f"Cleaned up {removed} old job executions")
                except Exception as exc:
                    logger.warning(f"Job cleanup error: {exc}")
                try:
                    removed_workflows = execution_store.cleanup_old_workflows(max_age_seconds=workflow_ttl)
                    cleanup_run["workflow_executions"] = int(removed_workflows or 0)
                    if removed_workflows:
                        logger.info(f"Cleaned up {removed_workflows} old workflow executions")
                except Exception as exc:
                    logger.warning(f"Workflow cleanup error: {exc}")
                try:
                    removed_artifacts = execution_store.cleanup_old_workflow_artifacts(max_age_seconds=artifact_ttl)
                    cleanup_run["workflow_artifacts"] = int(removed_artifacts or 0)
                    if removed_artifacts:
                        logger.info(f"Cleaned up {removed_artifacts} old workflow artifacts")
                except Exception as exc:
                    logger.warning(f"Workflow artifact cleanup error: {exc}")
                try:
                    removed_side_effects = execution_store.cleanup_old_workflow_side_effects(max_age_seconds=side_effect_ttl)
                    cleanup_run["workflow_side_effects"] = int(removed_side_effects or 0)
                    if removed_side_effects:
                        logger.info(f"Cleaned up {removed_side_effects} old workflow side-effect journal records")
                except Exception as exc:
                    logger.warning(f"Workflow side-effect cleanup error: {exc}")
                try:
                    removed_definitions = execution_store.cleanup_old_workflow_definitions(max_age_seconds=definition_ttl)
                    cleanup_run["workflow_definitions"] = int(removed_definitions or 0)
                    if removed_definitions:
                        logger.info(f"Cleaned up {removed_definitions} old workflow definitions")
                except Exception as exc:
                    logger.warning(f"Workflow definition cleanup error: {exc}")
                # Also clean up stale rate limiter entries
                try:
                    from .core.rate_limit import get_quota_manager
                    qm = get_quota_manager()
                    cleaned = qm._rate_limiter.cleanup_stale(max_idle_seconds=3600)
                    if cleaned:
                        logger.info(f"Cleaned up {cleaned} stale rate limiter entries")
                except Exception as exc:
                    logger.warning(f"Rate limiter cleanup error: {exc}")
                try:
                    previous = getattr(app.state, "cleanup_state", {})
                    totals = dict(previous.get("totals", {}))
                    for key, value in cleanup_run.items():
                        totals[key] = int(totals.get(key, 0)) + int(value or 0)
                    app.state.cleanup_state = {
                        **previous,
                        "last_run_at": datetime.utcnow().isoformat(),
                        "runs": int(previous.get("runs", 0)) + 1,
                        "totals": totals,
                        "last_run": cleanup_run,
                    }
                except Exception as exc:
                    logger.warning(f"Cleanup state update error: {exc}")

        _cleanup_task = asyncio.create_task(_cleanup_old_jobs())
    except Exception as e:
        logger.error(f"Failed to initialize streaming executor: {e}")

    # Background health poller: check remote backends periodically
    _health_poller_task = None
    _warm_pool_task = None
    enable_bg_tasks = os.getenv("ENABLE_BG_TASKS", "1") == "1"
    if enable_bg_tasks:
        try:
            async def _poll_backend_health():
                """Periodically check backend health and update DuckDB."""
                import httpx as _httpx
                await asyncio.sleep(30)  # Wait for backends to start
                while True:
                    try:
                        store = get_tool_store()
                        backends = store.list_backends()
                        for b in backends:
                            url = b.get("url") or b.get("backend_url", "")
                            health_ep = b.get("health_endpoint", "/health")
                            backend_id = b.get("backend_id", "")
                            if not url or not backend_id:
                                continue
                            try:
                                async with _httpx.AsyncClient(timeout=5.0) as client:
                                    resp = await client.get(f"{url}{health_ep}")
                                    is_healthy = resp.status_code == 200
                            except Exception:
                                is_healthy = False
                            store.update_backend_health(backend_id, is_healthy)
                    except Exception as exc:
                        logger.warning(f"Backend health poll error: {exc}")
                    await asyncio.sleep(120)

            _health_poller_task = asyncio.create_task(_poll_backend_health())
            logger.info("Backend health poller started (120s interval)")
        except Exception as e:
            logger.error(f"Failed to start health poller: {e}")

        # Keep backends warm to reduce cold-start latency on Modal-hosted MCPs.
        warm_pool_enabled = os.getenv("ENABLE_BACKEND_WARM_POOL", "1") == "1"
        warm_pool_interval = int(os.getenv("BACKEND_WARM_POOL_INTERVAL_SECONDS", "240"))
        warm_pool_timeout = float(os.getenv("BACKEND_WARM_POOL_TIMEOUT_SECONDS", "5.0"))
        if warm_pool_enabled:
            app.state.warm_pool_state = {
                "enabled": True,
                "interval_seconds": warm_pool_interval,
                "last_run_at": None,
                "runs": 0,
                "backends": {},
            }

            async def _warm_backends():
                import httpx as _httpx

                await asyncio.sleep(15)  # Give backends time to fully initialize.
                async with _httpx.AsyncClient(timeout=warm_pool_timeout) as client:
                    while True:
                        try:
                            store = get_tool_store()
                            backends = store.list_backends()
                            run_started = asyncio.get_event_loop().time()
                            backend_results = {}

                            for backend in backends:
                                backend_id = backend.get("backend_id", "")
                                url = backend.get("url") or backend.get("backend_url")
                                health_ep = backend.get("health_endpoint", "/health")
                                if not backend_id or not url:
                                    continue

                                started = asyncio.get_event_loop().time()
                                try:
                                    resp = await client.get(f"{url}{health_ep}")
                                    ok = resp.status_code == 200
                                    latency_ms = int((asyncio.get_event_loop().time() - started) * 1000)
                                except Exception as exc:
                                    ok = False
                                    latency_ms = None
                                    logger.debug(f"Warm ping failed for {backend_id}: {exc}")

                                backend_results[backend_id] = {
                                    "ok": ok,
                                    "latency_ms": latency_ms,
                                    "url": url,
                                }

                            app.state.warm_pool_state = {
                                "enabled": True,
                                "interval_seconds": warm_pool_interval,
                                "last_run_at": datetime.utcnow().isoformat(),
                                "runs": int(app.state.warm_pool_state.get("runs", 0)) + 1,
                                "duration_ms": int((asyncio.get_event_loop().time() - run_started) * 1000),
                                "backends": backend_results,
                            }
                        except Exception as exc:
                            logger.warning(f"Backend warm-pool loop error: {exc}")
                        await asyncio.sleep(warm_pool_interval)

            _warm_pool_task = asyncio.create_task(_warm_backends())
            logger.info(f"Backend warm-pool started ({warm_pool_interval}s interval)")
        else:
            logger.info("Backend warm-pool disabled (ENABLE_BACKEND_WARM_POOL=0)")

        # Initialize indexes in background (non-blocking)
        async def _background_index_init():
            await asyncio.sleep(10)  # Let the app warm up first
            try:
                from .api.semantic_mcp_routes import initialize_semantic_index
                await initialize_semantic_index()
                logger.info("Semantic tool index initialized")
            except Exception as e:
                logger.error(f"Failed to initialize semantic index: {e}")
            try:
                from .api.advanced_search_routes import reindex_all
                await reindex_all()
                logger.info("Advanced tool index initialized")
            except Exception as e:
                logger.error(f"Failed to initialize advanced index: {e}")

        asyncio.create_task(_background_index_init())
        logger.info("Index initialization started (non-blocking)")
    else:
        logger.info("Background tasks disabled (ENABLE_BG_TASKS=0)")

    # Poll-sync tools from backends (runs in background, does NOT block startup)
    sync_on_startup = os.getenv("SYNC_ON_STARTUP", "1") == "1"
    if sync_on_startup:
        async def _background_sync():
            # Sync tools from all backends into DuckDB
            try:
                from .api.tools_api import sync_all_backends as _sync_all
                logger.info("Background sync: syncing tools from all backends...")
                result = await asyncio.wait_for(_sync_all(), timeout=45)
                logger.info(f"Background sync complete: {result}")
            except asyncio.TimeoutError:
                logger.warning("Background sync: timed out after 45s (backends may still be starting)")
            except Exception as e:
                logger.error(f"Background sync failed: {e}")

            # Sync tools to Neo4j graph (optional, graceful failure)
            try:
                neo4j_url = os.getenv("NEO4J_URL")
                if neo4j_url:
                    from .services.graph_client import GraphManagementClient
                    graph = GraphManagementClient(base_url=neo4j_url)
                    store = get_tool_store()
                    tools = store.list_tools(limit=1000)
                    for tool in tools:
                        await graph.execute_query(
                            "MERGE (t:Tool {tool_id: $tool_id}) "
                            "SET t.name = $name, t.description = $description, "
                            "t.backend_id = $backend_id, t.backend_url = $backend_url, "
                            "t.category = $category, t.updated_at = datetime()",
                            {
                                "tool_id": tool.tool_id,
                                "name": tool.name,
                                "description": tool.description,
                                "backend_id": tool.backend_id,
                                "backend_url": tool.backend_url,
                                "category": tool.category or "general",
                            }
                        )
                    logger.info(f"Synced {len(tools)} tools to Neo4j graph")
            except Exception as e:
                logger.warning(f"Neo4j graph sync skipped: {e}")

        asyncio.create_task(_background_sync())
        logger.info("Background sync started (non-blocking)")
    else:
        logger.info("Startup poll-sync disabled (SYNC_ON_STARTUP=0), backends will push-announce")

    yield

    # Stop background tasks
    for _bg_task in [_cleanup_task, _health_poller_task, _warm_pool_task]:
        if _bg_task:
            _bg_task.cancel()
            try:
                await _bg_task
            except asyncio.CancelledError:
                pass

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

    # Backpressure middleware — shed load before hitting Modal's 2000 pending input queue limit
    _inflight = 0
    _max_inflight = int(os.getenv("MAX_INFLIGHT_REQUESTS", "500"))

    @app.middleware("http")
    async def backpressure(request, call_next):
        nonlocal _inflight
        # Health checks always pass through
        if request.url.path in ("/health", "/"):
            return await call_next(request)
        if _inflight >= _max_inflight:
            return JSONResponse(
                status_code=429,
                content={"error": "server_overloaded", "detail": "Too many concurrent requests, retry later"},
                headers={"Retry-After": "2"},
            )
        _inflight += 1
        try:
            response = await call_next(request)
            return response
        finally:
            _inflight -= 1

    # Root endpoint
    @app.get("/")
    async def root():
        registry = get_registry()
        service_registry = get_service_registry()

        # Remote backends from DuckDB (the real source of truth on Modal)
        store = get_tool_store()
        backends = store.list_backends()
        healthy_backends = [b for b in backends if b.get("is_healthy")]

        # Also check in-process native manager (for local dev)
        native_manager = get_native_mcp_manager()
        native_status = native_manager.get_status() if native_manager else {}

        return {
            "name": "Tool Management API",
            "version": "1.0.0",
            "status": "running",
            "tools_registered": len(registry),
            "services_registered": len(service_registry),
            "backends": {
                "total": len(backends),
                "healthy": len(healthy_backends),
                "servers": {
                    b.get("backend_id", "?"): {
                        "name": b.get("name"),
                        "healthy": b.get("is_healthy", False),
                        "url": b.get("url"),
                        "last_health_check": b.get("last_health_check"),
                        "tool_count": b.get("tool_count", 0),
                    }
                    for b in backends
                },
            },
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
                "execute": "/api/v1/execute",
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
        store = get_tool_store()
        stats = store.get_stats()
        warm_pool_state = getattr(app.state, "warm_pool_state", {"enabled": False})
        cleanup_state = getattr(app.state, "cleanup_state", {"enabled": False})
        durable_jobs = {"enabled": False}
        try:
            from .core.streaming import get_streaming_executor
            executor = get_streaming_executor()
            execution_store = getattr(executor, "_execution_store", None)
            if execution_store:
                durable_jobs = {
                    "enabled": True,
                    "db_path": str(getattr(execution_store, "db_path", "")),
                    "storage": execution_store.get_storage_stats(),
                }
        except Exception:
            pass
        return {
            "status": "healthy",
            "tool_count": stats.get("tool_count", 0),
            "backend_count": stats.get("backend_count", 0),
            "healthy_backends": stats.get("healthy_backends", 0),
            "warm_pool": warm_pool_state,
            "cleanup": cleanup_state,
            "durable_jobs": durable_jobs,
        }

    # Include retrieval routes FIRST (specific paths before wildcards)
    app.include_router(retrieval_router, prefix="/api/v1")

    # Include clean tools API (persistent, robust) - BEFORE tools_router to avoid wildcard conflicts
    app.include_router(tools_api_router)

    # Include streaming-first execution endpoints (/api/v1/execute, /api/v1/calls/*/control)
    app.include_router(execution_router)

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

    # Include async jobs API (/api/v1/jobs for submit/poll/stream/cancel)
    app.include_router(job_router, prefix="/api/v1")

    # OAuth 2.0 endpoints at root level (/.well-known/*, /oauth/*)
    app.include_router(oauth_router)

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
