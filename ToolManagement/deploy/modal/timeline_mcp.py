"""
Modal deployment for Distributed Timeline MCP.

Temporal-aware multimodal embedding store (jina-embeddings-v4) with
scheduled ingestion. Deploy with:
    modal deploy deploy/modal/timeline_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-timeline")

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
        "requests>=2.28.0",  # needed by native/__init__.py -> publish_mcp -> substack_client
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)

data_volume = modal.Volume.from_name("timeline-mcp-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/data": data_volume},
    timeout=300,
    min_containers=1,
    max_containers=4,
    scaledown_window=300,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    """Serve the Distributed Timeline MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-timeline-web.modal.run"
    os.environ["TIMELINE_DATA_DIR"] = "/data/timeline"
    os.environ["DATA_DIR"] = "/data"

    # Jina AI API key for multimodal v4 embeddings + Reader ingestion
    os.environ["JINA_API_KEY"] = "jina_c6997e7c38a240cabdd77d13dbfd4e40bP6-IWJ__OX8bSG51488ro0g6sQQ"

    from native.timeline_mcp.server import create_server
    return create_server().app
