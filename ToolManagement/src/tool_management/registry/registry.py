"""
Tool Registry - Central registry for tool registration and discovery.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
from functools import wraps

from ..models.tool import Tool, ToolVisibility, ToolStatus

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class ToolRegistry:
    """
    Central registry for managing tools.

    Supports:
    - Decorator-based registration
    - Manual registration
    - Discovery by name, tags, category
    - Export to various formats (MCP, JSON Schema)
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self._tools: Dict[str, Tool] = {}
        self._by_name: Dict[str, str] = {}  # name -> id mapping
        self._by_tag: Dict[str, List[str]] = {}  # tag -> [ids]
        self._by_category: Dict[str, List[str]] = {}  # category -> [ids]

    def register(self, tool: Tool) -> Tool:
        """
        Register a tool in the registry.

        Args:
            tool: Tool instance to register

        Returns:
            The registered tool

        Raises:
            ValueError: If a tool with the same name already exists
        """
        if tool.name in self._by_name:
            existing_id = self._by_name[tool.name]
            logger.warning(f"Tool '{tool.name}' already registered (id: {existing_id}), updating...")
            # Remove old indices
            old_tool = self._tools[existing_id]
            self._remove_from_indices(old_tool)

        self._tools[tool.id] = tool
        self._by_name[tool.name] = tool.id

        # Index by tags
        for tag in tool.tags:
            if tag not in self._by_tag:
                self._by_tag[tag] = []
            self._by_tag[tag].append(tool.id)

        # Index by category
        if tool.category:
            if tool.category not in self._by_category:
                self._by_category[tool.category] = []
            self._by_category[tool.category].append(tool.id)

        logger.info(f"Registered tool: {tool.name} (id: {tool.id})")
        return tool

    def _remove_from_indices(self, tool: Tool) -> None:
        """Remove a tool from secondary indices."""
        for tag in tool.tags:
            if tag in self._by_tag:
                self._by_tag[tag] = [id for id in self._by_tag[tag] if id != tool.id]
        if tool.category and tool.category in self._by_category:
            self._by_category[tool.category] = [
                id for id in self._by_category[tool.category] if id != tool.id
            ]

    def unregister(self, tool_id: str) -> Optional[Tool]:
        """Remove a tool from the registry."""
        if tool_id not in self._tools:
            return None

        tool = self._tools.pop(tool_id)
        del self._by_name[tool.name]
        self._remove_from_indices(tool)
        logger.info(f"Unregistered tool: {tool.name}")
        return tool

    def get(self, tool_id: str) -> Optional[Tool]:
        """Get a tool by ID."""
        return self._tools.get(tool_id)

    def get_by_name(self, name: str) -> Optional[Tool]:
        """Get a tool by name."""
        tool_id = self._by_name.get(name)
        return self._tools.get(tool_id) if tool_id else None

    def find_by_tag(self, tag: str) -> List[Tool]:
        """Find all tools with a specific tag."""
        tool_ids = self._by_tag.get(tag, [])
        return [self._tools[id] for id in tool_ids if id in self._tools]

    def find_by_category(self, category: str) -> List[Tool]:
        """Find all tools in a category."""
        tool_ids = self._by_category.get(category, [])
        return [self._tools[id] for id in tool_ids if id in self._tools]

    def search(
        self,
        query: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        visibility: Optional[ToolVisibility] = None,
        status: Optional[ToolStatus] = None,
    ) -> List[Tool]:
        """
        Search for tools matching criteria.

        Args:
            query: Text search in name/description
            tags: Filter by tags (OR)
            category: Filter by category
            visibility: Filter by visibility
            status: Filter by status

        Returns:
            List of matching tools
        """
        results = list(self._tools.values())

        if query:
            query_lower = query.lower()
            results = [
                t for t in results
                if query_lower in t.name.lower() or query_lower in t.description.lower()
            ]

        if tags:
            results = [
                t for t in results
                if any(tag in t.tags for tag in tags)
            ]

        if category:
            results = [t for t in results if t.category == category]

        if visibility:
            results = [t for t in results if t.visibility == visibility]

        if status:
            results = [t for t in results if t.status == status]

        return results

    def list_all(self) -> List[Tool]:
        """List all registered tools."""
        return list(self._tools.values())

    def list_names(self) -> List[str]:
        """List all tool names."""
        return list(self._by_name.keys())

    def list_tags(self) -> List[str]:
        """List all unique tags."""
        return list(self._by_tag.keys())

    def list_categories(self) -> List[str]:
        """List all categories."""
        return list(self._by_category.keys())

    def to_mcp_tools(self) -> List[Dict[str, Any]]:
        """Export all tools in MCP format."""
        return [tool.to_mcp_format() for tool in self._tools.values()]

    def to_dict(self) -> Dict[str, Any]:
        """Export registry as dictionary."""
        return {
            "name": self.name,
            "tool_count": len(self._tools),
            "tools": [t.model_dump() for t in self._tools.values()],
            "tags": self.list_tags(),
            "categories": self.list_categories(),
        }

    async def execute(self, tool_name: str, **input_data) -> Any:
        """Execute a tool by name."""
        tool = self.get_by_name(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")
        return await tool.execute(**input_data)

    def __len__(self) -> int:
        return len(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self._tools.values())


# Global registry instance
_global_registry: Optional[ToolRegistry] = None


def get_registry() -> ToolRegistry:
    """Get the global tool registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ToolRegistry("global")
    return _global_registry


def tool(
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    category: Optional[str] = None,
    visibility: ToolVisibility = ToolVisibility.PRIVATE,
    version: str = "1.0.0",
    **kwargs
) -> Callable[[F], F]:
    """
    Decorator to register a function as a tool.

    Usage:
        @tool(tags=["math", "utility"])
        def add(a: int, b: int) -> int:
            '''Add two numbers together.'''
            return a + b

        @tool(name="multiply", category="math")
        async def mult(x: float, y: float) -> float:
            '''Multiply two numbers.'''
            return x * y
    """
    def decorator(func: F) -> F:
        # Create tool from function
        t = Tool.from_function(
            func,
            name=name,
            description=description,
            tags=tags or [],
            category=category,
            visibility=visibility,
            version=version,
            **kwargs
        )

        # Register in global registry
        registry = get_registry()
        registry.register(t)

        # Preserve the original function but add tool reference
        @wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)

        wrapper._tool = t
        wrapper._tool_id = t.id
        return wrapper

    return decorator
