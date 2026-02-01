#!/usr/bin/env python3
"""
Advanced Search API - White-label wrapper for Parallel AI.

Distributed Systems' enhanced layer on top of Parallel's search-mcp and task-mcp.
We provide what they don't: durable storage, request tracking, billing, and
beautifully contextualized tool names for agent personas.

Clients see: "Advanced Search API" (Distributed Systems brand)
Clients don't see: Parallel AI, their API keys, or any vendor details

Run with:
    python -m deploy.parallel_service
    # or
    uvicorn deploy.parallel_service:app --port 8002

Environment variables:
    PARALLEL_API_KEY: Your Parallel AI API key
    REGISTRY_URL: Central registry URL (optional)
    DB_PATH: Path to SQLite database (default: ./data/advanced_search_requests.db)
"""

import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import uvicorn
from tool_management.integrations import ParallelAdapter
from tool_management.services import ServiceWrapper, ServiceConfig


# =============================================================================
# CONFIGURATION
# =============================================================================

config = ServiceConfig(
    # Identity (internal)
    service_id="advanced-search-api",

    # Branding (what clients see)
    service_name="Advanced Search API",
    display_name="Advanced Search API",
    description=(
        "Deep web search, autonomous task execution, structured data extraction, "
        "and real-time monitoring. Enhanced with durable storage and request tracking."
    ),
    version="1.0.0",

    # Storage - THIS IS OUR DIFFERENTIATOR
    # Parallel doesn't store IDs or do anything durable - we do
    db_path=os.getenv("DB_PATH", "./data/advanced_search_requests.db"),

    # Registry - auto-register with central gateway
    registry_url=os.getenv("REGISTRY_URL", "http://localhost:2016"),
    register_on_startup=True,

    # Server
    host="0.0.0.0",
    port=int(os.getenv("PORT", "8002")),

    # Auth
    require_api_key=True,
    api_key_header="X-API-Key",

    # Rate limiting
    rate_limit_requests=500,
    rate_limit_tokens=50000,

    # Billing (higher cost for autonomous/deep operations)
    track_usage=True,
    cost_per_request_cents=5,  # $0.05 per request
    cost_per_1k_tokens_cents=0,

    # Vendor (internal - never exposed)
    _vendor_name="parallel",
)

# Tool name mapping (vendor name -> our better names)
# These names are designed to be clear and contextual for agents
TOOL_NAME_MAP = {
    # Search capabilities (from search-mcp)
    "search": "deep_search",           # More descriptive than just "search"
    "extract": "extract_data",         # Structured data extraction
    "findall": "find_all",             # Batch finding

    # Task capabilities (from task-mcp)
    "task_run": "run_task",            # Execute autonomous task
    "task_group_create": "create_task_group",  # Orchestrate multiple tasks
    "get_stored_tasks": "list_tasks",  # View task history (WE make this durable)

    # Monitoring
    "monitor_create": "create_monitor",  # Real-time monitoring
}


# =============================================================================
# SERVICE SETUP
# =============================================================================

def create_service() -> ServiceWrapper:
    """Create the Parallel service wrapper."""
    # Create Parallel adapter
    adapter = ParallelAdapter()

    # Create wrapper (hides all Parallel details)
    wrapper = ServiceWrapper(
        config=config,
        adapter=adapter,
        tool_name_map=TOOL_NAME_MAP,
    )

    return wrapper


# Create service and FastAPI app
service = create_service()
app = service.create_app()


# =============================================================================
# CUSTOM AUTH (integrate with your auth system)
# =============================================================================

# Override the default auth to integrate with your system
# See jina_service.py for example


# =============================================================================
# CUSTOM BILLING (integrate with your billing system)
# =============================================================================

# Override usage tracking to integrate with your billing
# See jina_service.py for example


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  {config.display_name:^56}  ║
║           Distributed Systems Enhanced Services              ║
╠══════════════════════════════════════════════════════════════╣
║  Service ID: {config.service_id:<45}  ║
║  Port: {config.port:<51}  ║
║  Registry: {(config.registry_url or 'None'):<47}  ║
╠══════════════════════════════════════════════════════════════╣
║  Differentiators:                                            ║
║  ✓ Durable request/response storage                          ║
║  ✓ Full request history and replay                           ║
║  ✓ Usage tracking for billing                                ║
║  ✓ Contextualized tool names for agents                      ║
╚══════════════════════════════════════════════════════════════╝

Tools available:
""")
    for vendor_name, our_name in TOOL_NAME_MAP.items():
        print(f"  • {our_name}")

    print(f"""
Endpoints:
  • GET  /          Service info
  • GET  /health    Health check
  • GET  /tools     List tools
  • POST /execute   Execute a tool
  • GET  /requests  Request history (durable!)
  • GET  /usage     Usage summary for billing

Starting server...
""")

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level="info",
    )
