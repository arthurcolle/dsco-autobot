"""Import proxy so ``graph_management`` can be used without installing the package.

The real sources live under ``src/graph_management`` (src-layout). Many tools
expect ``graph_management`` to be importable directly from the repo root
without modifying ``PYTHONPATH`` first (e.g. ``uvicorn graph_management.api``).
This shim loads the real package implementation from ``src`` and exposes it
under the canonical module name so imports Just Work.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "graph_management"
_SRC_INIT = _SRC_ROOT / "__init__.py"

if not _SRC_INIT.exists():
    raise ImportError(
        "Cannot locate src/graph_management; run from the repository root or install the package."
    )

_spec = importlib.util.spec_from_file_location(
    __name__,
    _SRC_INIT,
    submodule_search_locations=[str(_SRC_ROOT)],
)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load spec for {_SRC_INIT}")

_module = importlib.util.module_from_spec(_spec)
# Replace ourselves in sys.modules so future imports (including relative ones
# inside the src package) operate on the real module object.
sys.modules[__name__] = _module
_spec.loader.exec_module(_module)
