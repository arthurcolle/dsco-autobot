"""
Modal deployment for Distributed Document Q&A MCP.

Deploy with:
    modal deploy deploy/modal/document_qa_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-document-qa-mcp")

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
        "openai>=1.0.0",
        "pypdf>=4.0.0",
        "chromadb>=0.4.0",
        "Pillow>=10.0.0",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)

document_volume = modal.Volume.from_name("document-qa-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/data": document_volume},
    timeout=300,
    min_containers=1,
    max_containers=10,
    buffer_containers=2,
    scaledown_window=60,
    secrets=[modal.Secret.from_name("openai-secret")],
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    """Serve the Distributed Document Q&A MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-document-qa-mcp-web.modal.run"
    os.environ["DOCUMENT_QA_DATA_DIR"] = "/data/documents"
    os.environ["DOCUMENT_QA_PATH"] = "/root/native/distributed_document_qa/pdf_to"

    from native.distributed_document_qa.server import app
    return app
