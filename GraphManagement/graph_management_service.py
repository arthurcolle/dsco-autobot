"""Compatibility shim that exposes the FastAPI overlay app for local tooling."""

from pathlib import Path
import sys

SRC_PATH = Path(__file__).resolve().parent / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from graph_management.local_env import load_local_env  # noqa: E402

load_local_env()

from graph_management.api import create_app  # noqa: E402

api = create_app()

__all__ = ["api", "create_app"]
