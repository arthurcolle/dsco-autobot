"""
Modal deployment for Distributed Search MCP.

Deploy with:
    modal deploy deploy/modal/search_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-search-mcp")

SRC_DIR = Path(__file__).parent.parent.parent / "src"
NATIVE_DIR = Path(__file__).parent.parent.parent / "native"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi[standard]==0.115.0",
        "uvicorn[standard]==0.32.0",
        "pydantic==2.9.2",
        "httpx==0.27.2",
        "requests>=2.28.0",  # needed by native/__init__.py -> publish_mcp -> substack_client
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)


@app.function(
    image=image,
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

    # Jina AI API key for search, embeddings, and reranking
    os.environ["JINA_API_KEY"] = "jina_c6997e7c38a240cabdd77d13dbfd4e40bP6-IWJ__OX8bSG51488ro0g6sQQ"

    # Bearer for federated registry-catalog source in search_all
    os.environ["TOOLMANAGEMENT_BEARER_TOKEN"] = "sk-arthur-234"

    from native.distributed_search_mcp.server import app
    return app
