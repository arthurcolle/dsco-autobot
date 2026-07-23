"""Ensure the local ``src`` tree is importable before any user code runs.

Python automatically imports ``sitecustomize`` (if present on ``sys.path``)
when it starts up, which lets us transparently expose the project package
without requiring ``PYTHONPATH`` tweaks or ``pip install -e .`` first.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    src_path = Path(__file__).resolve().parent / "src"
    if not src_path.exists():
        return

    src_str = str(src_path)
    if src_str not in sys.path:
        # Prepend so local code wins over any globally installed package.
        sys.path.insert(0, src_str)


_ensure_src_on_path()
