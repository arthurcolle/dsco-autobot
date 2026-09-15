"""Offline-testable helpers for rebuilding the tool graph projection."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Protocol


class CatalogTool(Protocol):
    """Persistent-tool fields needed by the graph projection."""

    name: str
    backend_id: str
    backend_url: str
    description: str


class CatalogStore(Protocol):
    """Pagination surface exposed by the persistent ToolStore."""

    def list_tools(self, *, limit: int, offset: int) -> List[CatalogTool]: ...


@dataclass
class BackendProjectionBatch:
    """One backend's projection input, kept private to the reconciliation run."""

    backend_url: str
    tools: List[Dict[str, str]]


Projection = Callable[[str, List[Dict[str, str]], str], Awaitable[Any]]


def group_catalog_tools_by_backend(
    tools: Iterable[CatalogTool],
) -> Dict[str, BackendProjectionBatch]:
    """Convert persisted tools into the existing announce-projection shape."""
    grouped: Dict[str, BackendProjectionBatch] = {}
    for tool in tools:
        backend_id = str(tool.backend_id or "").strip()
        name = str(tool.name or "").strip()
        if not backend_id or not name:
            raise ValueError("catalog tool is missing required projection fields")

        backend_url = str(tool.backend_url or "").strip()
        batch = grouped.get(backend_id)
        if batch is None:
            batch = BackendProjectionBatch(backend_url=backend_url, tools=[])
            grouped[backend_id] = batch
        elif not batch.backend_url and backend_url:
            batch.backend_url = backend_url

        batch.tools.append({
            "name": name,
            "description": str(tool.description or ""),
        })
    return grouped


def _safe_error_name(error: BaseException) -> str:
    """Classify an error without returning its potentially sensitive message."""
    return type(error).__name__


def _summary(
    *,
    catalog_tool_count: int = 0,
    backend_count: int = 0,
    projected_tool_count: int = 0,
    failed_tool_count: int = 0,
    errors: Counter[str] | None = None,
) -> Dict[str, Any]:
    error_counts = dict(sorted((errors or Counter()).items()))
    return {
        "catalog_tool_count": int(catalog_tool_count),
        "backend_count": int(backend_count),
        "projected_tool_count": int(projected_tool_count),
        "failed_tool_count": int(failed_tool_count),
        "error_count": int(sum(error_counts.values())),
        "errors": error_counts,
    }


def _read_catalog(store: CatalogStore, page_size: int) -> List[CatalogTool]:
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    tools: List[CatalogTool] = []
    offset = 0
    while True:
        page = list(store.list_tools(limit=page_size, offset=offset))
        if not page:
            break
        tools.extend(page)
        offset += len(page)
        if len(page) < page_size:
            break
    return tools


async def reconcile_persistent_catalog(
    store: CatalogStore,
    project: Projection,
    *,
    page_size: int = 500,
) -> Dict[str, Any]:
    """Reproject every persisted tool and return metadata-only diagnostics."""
    try:
        catalog = _read_catalog(store, page_size)
        grouped = group_catalog_tools_by_backend(catalog)
    except Exception as error:  # noqa: BLE001 - summary must remain callable on store failure
        return _summary(errors=Counter({_safe_error_name(error): 1}))

    projected_tool_count = 0
    failed_tool_count = 0
    errors: Counter[str] = Counter()

    for backend_id, batch in grouped.items():
        try:
            await project(backend_id, batch.tools, batch.backend_url)
        except Exception as error:  # noqa: BLE001 - isolate one broken backend
            errors[_safe_error_name(error)] += 1
            failed_tool_count += len(batch.tools)
        else:
            projected_tool_count += len(batch.tools)

    return _summary(
        catalog_tool_count=len(catalog),
        backend_count=len(grouped),
        projected_tool_count=projected_tool_count,
        failed_tool_count=failed_tool_count,
        errors=errors,
    )
