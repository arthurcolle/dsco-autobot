from __future__ import annotations

"""
Lightweight .env loader so local development can read environment variables
without requiring python-dotenv (keeps Modal images slim).
"""

import os
from pathlib import Path
from typing import Iterable, Tuple

_ENV_LOADED = False


def _parse_env_lines(lines: Iterable[str]) -> Iterable[Tuple[str, str]]:
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        yield key, value


def load_local_env(path: str | Path | None = None) -> None:
    """Populate os.environ from the repo-level .env if it exists."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = Path(path) if path else Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        _ENV_LOADED = True
        return

    for key, value in _parse_env_lines(env_path.read_text().splitlines()):
        os.environ.setdefault(key, value)
    _ENV_LOADED = True
