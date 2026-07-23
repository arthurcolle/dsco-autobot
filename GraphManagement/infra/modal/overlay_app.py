from __future__ import annotations

"""Modal entrypoint for the Graph Management overlay FastAPI service."""

import os
import sys
import inspect
from pathlib import Path

import modal
from modal.mount import Mount

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

_GRAPH_MANAGEMENT_DIR = SRC_PATH / "graph_management"


def _graph_management_mount() -> Mount:
    """Build a Modal mount regardless of client version quirks."""
    factory = getattr(Mount, "from_local_dir", None) or getattr(Mount, "_from_local_dir", None)
    if factory is None:
        msg = "Modal Mount helpers are unavailable; upgrade/downgrade the Modal client."
        raise AttributeError(msg)
    return factory(_GRAPH_MANAGEMENT_DIR, remote_path="/root/graph_management")

overlay_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("fastapi[standard]", "uvicorn", "httpx", "pydantic", "aiohttp", "PyJWT")
    .env(
        {
            "BACKEND_URL": os.environ.get("BACKEND_URL", ""),
            "DEBUG_ERRORS": os.environ.get("DEBUG_ERRORS", "0"),
            "LOG_LEVEL": os.environ.get("LOG_LEVEL", "INFO"),
        }
    )
    .add_local_dir(_GRAPH_MANAGEMENT_DIR, remote_path="/root/graph_management")
)

_FUNCTION_ACCEPTS_MOUNTS = "mounts" in inspect.signature(modal.App.function).parameters

if _FUNCTION_ACCEPTS_MOUNTS:
    # Mount the entire graph_management module for older Modal clients
    graph_management_mount = _graph_management_mount()
else:
    graph_management_mount = None

app = modal.App("graph-management-api", image=overlay_image)

_function_kwargs = dict(timeout=86400, cpu=1.0, memory=2048)
if graph_management_mount is not None:
    _function_kwargs["mounts"] = [graph_management_mount]

@app.function(**_function_kwargs)
@modal.asgi_app()
def serve():
    # Import here after the module is mounted
    from graph_management.api import create_app
    return create_app()


__all__ = ["app", "serve"]
