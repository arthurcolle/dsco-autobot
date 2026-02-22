"""
Modal deployment for Tool Management API.

Deploy with:
    modal deploy deploy/modal/modal_app.py

Custom domain: tools.distributed.systems

Version: 2.5.0 - DuckDB Persistence for Swarm Economy
"""

import modal
from pathlib import Path

# Create the Modal app
app = modal.App("tool-registry")

# Get the source directory path
SRC_DIR = Path(__file__).parent.parent.parent / "src"

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
        # Base58 encoding (Solana addresses)
        "base58>=2.1.0",
        # Temporal
        "temporalio==1.7.1",
        # Async
        "aiofiles==24.1.0",
        # DuckDB for persistence
        "duckdb>=0.10.0",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
)

# Volume for persistent data
data_volume = modal.Volume.from_name("tool-registry-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/data": data_volume},
    timeout=600,
    min_containers=1,
    max_containers=10,
    scaledown_window=120,
)
@modal.concurrent(max_inputs=50, target_inputs=10)
# Custom domain: tools.distributed.systems
# Register at: https://modal.com/settings/custom-domains
# DNS CNAME: tools.distributed.systems -> arthurcolle--tool-registry-web.modal.run
@modal.asgi_app(custom_domains=["tools.distributed.systems"])
def web():
    """Serve the Tool Management API."""
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
    os.environ["DEFAULT_BEARER_TOKEN"] = "sk-arthur-234"  # Static token for probe/smoke testing

    # API Base URL for meta-tools (points to self)
    os.environ["API_BASE_URL"] = "https://tools.distributed.systems"

    # Native MCP URLs (deployed as separate Modal apps)
    os.environ["SEARCH_MCP_URL"] = "https://arthurcolle--distributed-search-mcp-web.modal.run"
    os.environ["RESEARCH_MCP_URL"] = "https://arthurcolle--distributed-research-mcp-web.modal.run"
    os.environ["PUBLISH_MCP_URL"] = "https://arthurcolle--distributed-publish-mcp-web.modal.run"
    os.environ["MEMORY_MCP_URL"] = "https://arthurcolle--distributed-memory-mcp-web.modal.run"

    # Neo4j Graph Management URL
    os.environ["NEO4J_URL"] = "https://arthurcolle--graph-management-neo4j-serve.modal.run"

    # API keys for external services
    os.environ["JINA_API_KEY"] = "jina_e3b2e0bc5b054de0b6fbbe50b059df94ii1L4iodLdyKwST4pSxTXiw3BbgT"
    os.environ["PARALLEL_API_KEY"] = "ge_Apqx6h_SK_S182NeG1l_vzBV_e2cpjfsPTjgB"

    # Import and return the FastAPI app
    from tool_management.app import create_app

    return create_app()


@app.local_entrypoint()
def main():
    """Local entrypoint for testing."""
    print("Deploying Tool Registry to Modal...")
    print("Custom domain: tools.distributed.systems")
    print()
    print("Run 'modal deploy deploy/modal/modal_app.py' to deploy")
