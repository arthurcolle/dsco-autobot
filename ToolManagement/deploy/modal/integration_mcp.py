"""Modal deployment for Distributed Integration (canonical integration server: distributed-integration)."""
import modal
from pathlib import Path

app = modal.App("distributed-integration")

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


# Pooled operator provider keys (from the repo-root .env) power the paid
# managed add-ons: the fabric only uses them when the registry marks the call
# _pooled_ok (add-on subscriber) or the caller is the operator.
ENV_FILE_DIR = Path(__file__).parent.parent.parent


@app.function(
    image=image,
    secrets=[modal.Secret.from_dotenv(ENV_FILE_DIR)],
    timeout=300,
    min_containers=1,
    max_containers=5,
    scaledown_window=120,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    import sys, os
    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")
    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-integration-web.modal.run"
    from native.integration_fabric.server import app
    return app
