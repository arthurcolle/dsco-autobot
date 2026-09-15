"""Modal deployment for Distributed Unit (canonical integration server: distributed-unit)."""
import modal
from pathlib import Path

app = modal.App("distributed-unit")

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


@app.function(image=image, timeout=300, min_containers=1, max_containers=5, scaledown_window=120, secrets=[modal.Secret.from_name("tool-registry-announce", required_keys=[])])
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    import sys, os
    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")
    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-unit-web.modal.run"
    from native.unit_fabric.server import app
    return app
