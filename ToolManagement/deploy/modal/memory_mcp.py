"""
Modal deployment for Distributed Memory MCP.

Deploy with:
    modal deploy deploy/modal/memory_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-memory-mcp")

SRC_DIR = Path(__file__).parent.parent.parent / "src"
NATIVE_DIR = Path(__file__).parent.parent.parent / "native"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi[standard]==0.115.0",
        "uvicorn[standard]==0.32.0",
        "pydantic==2.9.2",
        "httpx==0.27.2",
        "numpy>=1.24.0",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)

data_volume = modal.Volume.from_name("memory-mcp-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/data": data_volume},
    timeout=300,
    min_containers=1,  # Keep at least 1 instance running always
)
@modal.concurrent(max_inputs=50)
@modal.asgi_app()
def web():
    """Serve the Distributed Memory MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-memory-mcp-web.modal.run"
    os.environ["MEMORY_DATA_DIR"] = "/data/memory"

    # API keys for cross-MCP calls (embeddings from search, etc.)
    os.environ["JINA_API_KEY"] = "jina_e3b2e0bc5b054de0b6fbbe50b059df94ii1L4iodLdyKwST4pSxTXiw3BbgT"

    from native.distributed_memory_mcp.server import app
    return app
