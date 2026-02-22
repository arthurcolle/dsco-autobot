"""
Modal deployment for Distributed Research MCP.

Deploy with:
    modal deploy deploy/modal/research_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-research-mcp")

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

anthropic_secret = modal.Secret.from_name("anthropic-secret", required_keys=[])


@app.function(
    image=image,
    secrets=[anthropic_secret],
    timeout=600,
    min_containers=1,
    max_containers=10,
    buffer_containers=2,

    scaledown_window=60,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    """Serve the Distributed Research MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-research-mcp-web.modal.run"

    # Parallel AI API key for deep research and task execution
    os.environ["PARALLEL_API_KEY"] = "ge_Apqx6h_SK_S182NeG1l_vzBV_e2cpjfsPTjgB"

    # Jina AI API key for web search and reading
    os.environ["JINA_API_KEY"] = "jina_e3b2e0bc5b054de0b6fbbe50b059df94ii1L4iodLdyKwST4pSxTXiw3BbgT"

    from native.distributed_research_mcp.server import app
    return app
