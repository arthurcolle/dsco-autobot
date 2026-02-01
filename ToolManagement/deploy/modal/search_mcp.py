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
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)


@app.function(
    image=image,
    timeout=300,
    min_containers=1,  # Keep at least 1 instance running always
)
@modal.concurrent(max_inputs=50)
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
    os.environ["JINA_API_KEY"] = "jina_e3b2e0bc5b054de0b6fbbe50b059df94ii1L4iodLdyKwST4pSxTXiw3BbgT"

    from native.distributed_search_mcp.server import app
    return app
