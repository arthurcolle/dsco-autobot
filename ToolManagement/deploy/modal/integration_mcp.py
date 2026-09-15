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
        "cryptography>=41.0.0",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)

# Preserve the production backend/announcement credentials. Provider credentials
# are separate from the backend bearer; never use an OpenRouter key for MCP auth.
announce_secret = modal.Secret.from_name("tool-registry-announce")


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("tool-registry-secrets"),
        modal.Secret.from_name(
            "tool-registry-security",
            required_keys=["BACKEND_BEARER_TOKEN", "BYOK_ENVELOPE_KEY_INTEGRATION_FABRIC"],
        ),
        announce_secret,
        modal.Secret.from_name(
            "distributed-integration-openrouter", required_keys=["OPENROUTER_API_KEY"],
        ),
    ],
    timeout=300,
    min_containers=1,
    max_containers=5,
    scaledown_window=300,
)
@modal.concurrent(max_inputs=20, target_inputs=5)
@modal.asgi_app()
def web():
    import sys, os
    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")
    os.environ["PORT"] = "8000"
    os.environ["ENVIRONMENT"] = "production"
    os.environ["NATIVE_MCP_REQUIRE_BEARER"] = "1"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-integration-web.modal.run"
    from native.integration_fabric.server import app
    return app
