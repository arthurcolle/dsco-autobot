"""
Modal deployment for Tool Management API.

Deploy with:
    modal deploy deploy/modal/modal_app.py

Custom domain: tools.distributed.systems

Version: 2.6.0 - Hardening + Batch Endpoint
"""

import modal
from pathlib import Path

# Create the Modal app
app = modal.App("tool-registry")

# Get the source directory path
SRC_DIR = Path(__file__).parent.parent.parent / "src"

# Codex Apps inventory snapshot (directory cache + curated plugin manifests).
# Refresh with: cp ~/.codex/cache/codex_app_directory/*.json data/codex_apps_snapshot/directory/
CODEX_SNAPSHOT_DIR = Path(__file__).parent.parent.parent / "data" / "codex_apps_snapshot"

# Build image with all dependencies and local source code
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("curl", "git")
    .pip_install(
        # Core web framework
        "fastapi[standard]==0.115.0",
        "uvicorn[standard]==0.32.0",
        "pydantic==2.9.2",
        "pydantic-settings==2.5.2",
        # HTTP Client
        "httpx==0.27.2",
        # Database
        "neo4j==5.25.0",
        "redis==5.2.0",
        # Monitoring
        "prometheus-client==0.21.0",
        "psutil==6.1.0",
        # WebSocket
        "websockets==13.1",
        # Data
        "yfinance==0.2.48",
        # Config
        "pyyaml==6.0.2",
        # Middleware
        "python-multipart==0.0.12",
        # Logging
        "structlog==24.4.0",
        # GraphQL
        "strawberry-graphql[fastapi]==0.243.1",
        # Vector/Embeddings
        "numpy>=1.24.0",
        # MCP
        "fastapi-mcp>=0.1.0",
        # Cryptography (Ed25519 for warrants)
        "cryptography>=41.0.0",
        # Canonical ZITADEL access-token validation (RS256/JWKS)
        "PyJWT[crypto]==2.9.0",
        # Base58 encoding (Solana addresses)
        "base58>=2.1.0",
        # Secp256k1 (wallet auth)
        "coincurve>=20.0.0",
        # Temporal
        "temporalio==1.7.1",
        # Async
        "aiofiles==24.1.0",
        "aiosqlite>=0.20.0",
        # SSE streaming
        "sse-starlette>=2.1.0",
        # HTML extraction (bs4 used by discovery/scraping paths)
        "beautifulsoup4>=4.12.0",
        # Graph utilities
        "networkx>=3.2",
        # CLI helpers imported by some service modules
        "click>=8.1.0",
        # DuckDB for persistence
        "duckdb>=0.10.0",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
)

# Bundle the codex_apps inventory so the deployed registry can serve the
# connector fabric (the cache otherwise only exists on the dev machine).
if CODEX_SNAPSHOT_DIR.exists():
    image = image.add_local_dir(CODEX_SNAPSHOT_DIR, remote_path="/root/codex_apps_snapshot")

# Volume for persistent data
data_volume = modal.Volume.from_name("tool-registry-data", create_if_missing=True)


registry_volumes = {"/data": data_volume}
registry_secrets = [
        modal.Secret.from_name("tool-registry-oauth-core"),
        modal.Secret.from_name("tool-registry-secrets"),
        modal.Secret.from_name("tool-registry-security"),
        modal.Secret.from_name("tool-registry-codex-apps-vault"),
    ]

# Non-secret settings shared by serving and explicit private reconciliation.
PRODUCTION_ENV = {
    "DATA_DIR": "/data",
    "DSCO_DEPLOYMENT_MODE": "production",
    "GRAPH_SUBSTRATE": "graphsub",
    "SYNC_ON_STARTUP": "0",
    "ENABLE_ANNOUNCE_EMBED_SYNC": "0",
    "ENABLE_ANNOUNCE_GRAPH_PROJECTION": "1",
}


def _configure_production_environment():
    import sys
    import os

    # Add source to path
    sys.path.insert(0, "/root/src")

    # Configure environment
    os.environ["DATA_DIR"] = "/data"
    os.environ["ENABLE_DOCS"] = "1"
    os.environ["DEBUG"] = "0"
    os.environ["AUTO_START_QUEUE"] = "0"  # Disable task queue to reduce startup load
    os.environ["AUTO_START_NATIVE_MCPS"] = "0"  # Native MCPs deployed separately on Modal
    os.environ["SYNC_ON_STARTUP"] = "0"  # Disable heavy backend sync — backends push-announce instead
    os.environ["ENABLE_BG_TASKS"] = "0"  # Disable health poller + index init to avoid event loop starvation
    os.environ["REGISTER_PERSISTENT_BACKENDS"] = "0"  # Avoid DuckDB write-lock contention during cold starts
    os.environ["ENABLE_ANNOUNCE_EMBED_SYNC"] = "0"  # Keep announce lightweight; skip embedding/Neo4j background work
    os.environ["ANNOUNCE_MIN_INTERVAL_SECONDS"] = "900"  # Ignore frequent heartbeat re-announces (15 min)
    os.environ["REQUIRE_BEARER_AUTH"] = "1"  # Enforce bearer auth on non-public routes
    os.environ["ALLOW_ANON_ANNOUNCE"] = "1"  # Let native MCPs keep push-announcing without auth header changes

    # Canonical customer identity authority. Legacy ToolManagement tokens stay
    # enabled during account migration, but can no longer be mistaken for
    # ZITADEL identities.
    os.environ["ENV"] = "production"
    os.environ["UNIFIED_ISSUER_URL"] = "https://auth.distributed.systems"
    os.environ["UNIFIED_JWKS_URL"] = "https://auth.distributed.systems/oauth/v2/keys"
    os.environ["UNIFIED_ALLOW_LEGACY"] = "1"
    os.environ["UNIFIED_ALLOW_HS256"] = "1"
    os.environ["UNIFIED_LEGACY_SUBJECT_IS_ACCOUNT"] = "1"
    os.environ["INTERNAL_API_BEARER_TOKEN"] = os.environ["DEFAULT_BEARER_TOKEN"]

    # API Base URL for meta-tools (points to self)
    os.environ["API_BASE_URL"] = "https://tools.distributed.systems"

    # Codex Apps integration settings
    os.environ["CODEX_BRIDGE_MODE"] = "inprocess"  # Mount bridge inside the main ASGI app
    os.environ["CODEX_OAUTH_CALLBACK_URL"] = "https://tools.distributed.systems"
    # CODEX_VAULT_MASTER_KEY must be set as a Modal secret for encrypted credential storage
    # Bundled connector inventory (see CODEX_SNAPSHOT_DIR at image build)
    os.environ["CODEX_APP_DIRECTORY_CACHE"] = "/root/codex_apps_snapshot/directory/*.json"
    os.environ["CODEX_APP_PLUGIN_MANIFESTS"] = "/root/codex_apps_snapshot/manifests/*/.app.json"

    # Native MCP URLs (deployed as separate Modal apps)
    os.environ["SEARCH_MCP_URL"] = "https://arthurcolle--distributed-search-mcp-web.modal.run"
    os.environ["RESEARCH_MCP_URL"] = "https://arthurcolle--distributed-research-mcp-web.modal.run"
    os.environ["PUBLISH_MCP_URL"] = "https://arthurcolle--distributed-publish-mcp-web.modal.run"
    os.environ["MEMORY_MCP_URL"] = "https://arthurcolle--distributed-memory-mcp-web.modal.run"

    # Neo4j Graph Management URL
    os.environ["NEO4J_URL"] = "https://arthurcolle--graph-management-neo4j-serve.modal.run"

    # API keys for external services

    # Stable master keys so encrypted credential vaults on /data survive
    # container cycles (unset would derive an unstable machine-specific key).
    os.environ.setdefault("TENANT_VAULT_MASTER_KEY", "kvSntPK3p6NxZ6O0-1DCiaYvOKjOmvBvCY4KpRhtHv8")
    os.environ.setdefault("CODEX_VAULT_MASTER_KEY", "KZn_zkzjLYuWY3sPo-Dfda8ndE4TSNpPaimk2TcT9s4")

    os.environ.update(PRODUCTION_ENV)


@app.function(
    image=image,
    volumes=registry_volumes,
    secrets=registry_secrets,
    timeout=600,
    min_containers=1,
    max_containers=1,
    scaledown_window=120,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
# Custom domain: tools.distributed.systems
# Register at: https://modal.com/settings/custom-domains
# DNS CNAME: tools.distributed.systems -> arthurcolle--tool-registry-web.modal.run
@modal.asgi_app(custom_domains=["tools.distributed.systems"])
def web():
    """Serve the Tool Management API."""
    _configure_production_environment()

    # Import and return the FastAPI app
    from tool_management.app import create_app

    return create_app()


@app.function(image=image, volumes=registry_volumes, secrets=registry_secrets, timeout=600)
async def reconcile_graphsub_projection():
    """Explicit private repair of persisted metadata; not an HTTP endpoint."""
    _configure_production_environment()
    from tool_management.persistence import get_tool_store
    from tool_management.api.tools_api import _project_announced_tools_to_graph
    from tool_management.services.graph_reconciliation import reconcile_persistent_catalog

    async def project(backend_id, tools, backend_url):
        return await _project_announced_tools_to_graph(
            backend_id, tools, backend_url, raise_errors=True,
        )

    return await reconcile_persistent_catalog(get_tool_store(), project)


@app.local_entrypoint()
def main():
    """Local entrypoint for testing."""
    print("Deploying Tool Registry to Modal...")
    print("Custom domain: tools.distributed.systems")
    print()
    print("Run 'modal deploy deploy/modal/modal_app.py' to deploy")
