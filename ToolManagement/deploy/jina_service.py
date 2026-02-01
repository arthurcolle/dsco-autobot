#!/usr/bin/env python3
"""
Search API - White-label wrapper for Jina AI.

Distributed Systems' enhanced layer on top of Jina's search capabilities.
We provide what they don't: durable storage, request tracking, billing, and
beautifully contextualized tool names for agent personas.

Clients see: "Search API" (Distributed Systems brand)
Clients don't see: Jina, their API keys, or any vendor details

Run with:
    python -m deploy.jina_service
    # or
    uvicorn deploy.jina_service:app --port 8001

Environment variables:
    JINA_API_KEY: Your Jina AI API key
    REGISTRY_URL: Central registry URL (optional)
    DB_PATH: Path to SQLite database (default: ./data/search_requests.db)
"""

import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import uvicorn
from tool_management.integrations import JinaAdapter
from tool_management.services import ServiceWrapper, ServiceConfig


# =============================================================================
# CONFIGURATION
# =============================================================================

config = ServiceConfig(
    # Identity (internal)
    service_id="search-api",

    # Branding (what clients see)
    service_name="Search API",
    display_name="Search API",
    description=(
        "Web search, content reading, fact-checking, semantic reranking, and embeddings. "
        "Enhanced with durable storage and request tracking."
    ),
    version="1.0.0",

    # Storage - THIS IS OUR DIFFERENTIATOR
    db_path=os.getenv("DB_PATH", "./data/search_requests.db"),

    # Registry - auto-register with central gateway
    registry_url=os.getenv("REGISTRY_URL", "http://localhost:2016"),
    register_on_startup=True,

    # Server
    host="0.0.0.0",
    port=int(os.getenv("PORT", "8001")),

    # Auth
    require_api_key=True,
    api_key_header="X-API-Key",

    # Rate limiting
    rate_limit_requests=1000,
    rate_limit_tokens=100000,

    # Billing
    track_usage=True,
    cost_per_request_cents=1,  # $0.01 per request
    cost_per_1k_tokens_cents=0,

    # Vendor (internal - never exposed)
    _vendor_name="jina",
)

# Tool name mapping (vendor name -> our better names)
# These names are designed to be clear and contextual for agents
TOOL_NAME_MAP = {
    "web_search": "search",      # Core web search
    "read_url": "read",          # Read/extract from URL
    "fact_check": "verify",      # Fact verification
    "rerank": "rerank",          # Semantic reranking
    "embed": "embed",            # Generate embeddings
}


# =============================================================================
# SERVICE SETUP
# =============================================================================

def create_service() -> ServiceWrapper:
    """Create the Jina service wrapper."""
    # Create Jina adapter
    adapter = JinaAdapter()

    # Create wrapper (hides all Jina details)
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
# Example: validate against your database

# async def custom_validate_api_key(api_key: str) -> dict:
#     """
#     Validate API key against your auth system.
#     Returns client info for billing/rate limiting.
#     """
#     # Example: Check against your database
#     # client = await your_db.get_client_by_api_key(api_key)
#     # if not client:
#     #     raise HTTPException(401, "Invalid API key")
#     #
#     # return {
#     #     "client_id": client.id,
#     #     "organization_id": client.organization_id,
#     #     "rate_limit": client.rate_limit,
#     # }
#     pass
#
# service._validate_api_key = custom_validate_api_key


# =============================================================================
# CUSTOM BILLING (integrate with your billing system)
# =============================================================================

# Override usage tracking to integrate with your billing
# async def custom_track_usage(request, response, client_info):
#     """
#     Track usage in your billing system.
#     """
#     # Example: Update credits
#     # await your_billing.deduct_credits(
#     #     client_id=client_info["client_id"],
#     #     amount=request.cost_cents,
#     #     request_id=request.id,
#     # )
#     pass
#
# service._track_usage = custom_track_usage


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
