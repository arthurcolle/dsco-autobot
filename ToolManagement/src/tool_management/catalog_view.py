"""
Merged catalog view.

The app exposes tools from two places:

  1. The in-process :class:`ToolRegistry` (decorator-registered, directly
     executable example/builtin tools).
  2. The durable DuckDB-backed tool store (:func:`get_tool_store`) which holds
     every tool announced by remote MCP backends.

Listing/count endpoints historically read only (1), so the deployed app reported
a handful of tools even though hundreds were registered in the store. These
helpers return the *union* of both sources, deduplicated by tool name. In-process
registry tools take precedence over store entries with the same name because they
are directly executable in this process.

All store access is defensive: if the store is unavailable the helpers degrade to
the in-process registry rather than failing the request.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .registry import get_registry
from .persistence import get_tool_store

# Upper bound on store rows pulled into a merged view. Large enough to cover the
# full catalog, bounded to avoid pathological payloads.
_STORE_FETCH_LIMIT = 10000


def _store_tools(category: Optional[str] = None) -> List[Any]:
    """Return store tools (RegisteredTool), or [] if the store is unavailable."""
    try:
        store = get_tool_store()
        return store.list_tools(category=category, limit=_STORE_FETCH_LIMIT, offset=0)
    except Exception:
        return []


def _store_count() -> int:
    try:
        return get_tool_store().count_tools()
    except Exception:
        return 0


def store_tool_to_dict(t: Any) -> Dict[str, Any]:
    """Normalize a store RegisteredTool into a listing dict."""
    return {
        "id": t.tool_id,
        "name": t.name,
        "description": t.description,
        "version": t.version,
        "input_schema": t.input_schema or {"type": "object", "properties": {}},
        "output_schema": t.output_schema,
        "tags": t.tags or [],
        "category": t.category,
        "backend_id": t.backend_id,
        "service_url": t.backend_url,
        "status": "active",
        "source": "backend",
    }


def merged_names(category: Optional[str] = None) -> List[str]:
    """Union of registry + store tool names (registry first), deduped."""
    names: Dict[str, None] = {}
    for t in get_registry().list_all():
        if category and t.category != category:
            continue
        names[t.name] = None
    for t in _store_tools(category):
        names.setdefault(t.name, None)
    return list(names.keys())


def merged_count() -> int:
    """Total distinct tool count across registry + store."""
    return len(merged_names())


def counts_breakdown() -> Dict[str, int]:
    """Per-source counts plus deduped total."""
    return {
        "in_process": len(get_registry()),
        "backend": _store_count(),
        "total": merged_count(),
    }


def merged_by_category(category: Optional[str] = None) -> Dict[str, List[str]]:
    """Map category -> tool names across both sources, deduped by name."""
    out: Dict[str, List[str]] = {}
    seen: set = set()

    def add(name: str, cat: Optional[str]) -> None:
        if not cat or name in seen:
            return
        seen.add(name)
        out.setdefault(cat, []).append(name)

    for t in get_registry().list_all():
        add(t.name, t.category)
    for t in _store_tools():
        add(t.name, t.category)

    if category is not None:
        return {category: out.get(category, [])}
    return out


def merged_categories() -> List[str]:
    """Union of categories across both sources."""
    cats: Dict[str, None] = {}
    for c in get_registry().list_categories():
        cats[c] = None
    for t in _store_tools():
        if t.category:
            cats.setdefault(t.category, None)
    return list(cats.keys())


def merged_tags() -> List[str]:
    """Union of tags across both sources."""
    tags: Dict[str, None] = {}
    for tag in get_registry().list_tags():
        tags[tag] = None
    for t in _store_tools():
        for tag in (t.tags or []):
            tags.setdefault(tag, None)
    return list(tags.keys())


def merged_mcp_tools() -> List[Dict[str, Any]]:
    """Union of tools in MCP tool-list format, deduped by name."""
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for t in get_registry().to_mcp_tools():
        name = t.get("name")
        if name in seen:
            continue
        seen.add(name)
        out.append(t)
    for t in _store_tools():
        if t.name in seen:
            continue
        seen.add(t.name)
        out.append(
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema or {"type": "object", "properties": {}},
                "outputSchema": t.output_schema,
                "annotations": {
                    "tool_id": t.tool_id,
                    "backend": t.backend_id,
                    "category": t.category,
                    "tags": t.tags or [],
                    "version": t.version,
                },
            }
        )
    return out


def merged_tool_dicts(
    query: Optional[str] = None,
    tags: Optional[List[str]] = None,
    category: Optional[str] = None,
    visibility: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    Union of registry + store tools as listing dicts, deduped by name.

    Registry tools are filtered via the registry's own search; store tools are
    filtered in Python on the same criteria (visibility is registry-only).
    """
    from .models.tool import ToolVisibility  # local import to avoid cycle

    registry = get_registry()
    reg_tools = registry.search(
        query=query,
        tags=tags,
        category=category,
        visibility=visibility if isinstance(visibility, ToolVisibility) else None,
    )

    result: List[Dict[str, Any]] = []
    seen: set = set()
    for t in reg_tools:
        result.append(t.model_dump(exclude={"_callable"}))
        seen.add(t.name)

    q = query.lower() if query else None
    for t in _store_tools(category):
        if t.name in seen:
            continue
        if q and q not in t.name.lower() and q not in (t.description or "").lower():
            continue
        if tags and not (set(tags) & set(t.tags or [])):
            continue
        seen.add(t.name)
        result.append(store_tool_to_dict(t))
    return result
