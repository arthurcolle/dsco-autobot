"""
Modal deployment for Distributed Search MCP.

Deploy with:
    modal deploy deploy/modal/search_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-search-mcp")

search_secret = modal.Secret.from_name(
    "distributed-search-provider",
    required_keys=[
        "JINA_API_KEY",
        "TOOLMANAGEMENT_BEARER_TOKEN",
    ],
)
announce_secret = modal.Secret.from_name("tool-registry-announce", required_keys=[])
backend_auth_secret = modal.Secret.from_name("tool-registry-security", required_keys=[])
postgres_secret = modal.Secret.from_name("tool-registry-postgres", required_keys=["POSTGRES_URL"])
router_secret = modal.Secret.from_name(
    "distributed-search-router", required_keys=["DSCO_ROUTER_API_KEY"]
)

SRC_DIR = Path(__file__).parent.parent.parent / "src"
NATIVE_DIR = Path(__file__).parent.parent.parent / "native"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi[standard]==0.115.0",
        "uvicorn[standard]==0.32.0",
        "pydantic==2.9.2",
        "httpx==0.27.2",
        "psycopg[binary]==3.2.3",
        "requests>=2.28.0",  # needed by native/__init__.py -> publish_mcp -> substack_client
        "coincurve>=20.0.0",  # tool_management.auth package imports wallet verification
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)


@app.function(
    image=image,
    secrets=[search_secret, announce_secret, backend_auth_secret, postgres_secret, router_secret],
    timeout=300,
    min_containers=1,
    max_containers=10,
    buffer_containers=2,

    scaledown_window=60,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    """Serve the Distributed Search MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-search-mcp-web.modal.run"
    os.environ["ENVIRONMENT"] = "production"
    os.environ["NATIVE_MCP_REQUIRE_BEARER"] = "1"
    os.environ["SEARCH_REQUIRE_DURABLE_STORE"] = "1"
    os.environ["SEARCH_REQUIRE_SIGNED_CONTEXT"] = "1"
    os.environ["ROUTER_URL"] = "https://router.distributed.systems"

    from native.distributed_search_mcp.server import app
    return app
