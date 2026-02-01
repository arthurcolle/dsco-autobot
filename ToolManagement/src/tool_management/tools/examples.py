"""
Example tools demonstrating the @tool decorator usage.

These tools are automatically registered when this module is imported.
"""

from typing import Dict, List, Optional, Any
import json

from ..registry import tool
from ..models import ToolVisibility


# =============================================================================
# MATH TOOLS
# =============================================================================

@tool(
    tags=["math", "utility"],
    category="math",
    visibility=ToolVisibility.PUBLIC,
)
def add(a: float, b: float) -> float:
    """Add two numbers together."""
    return a + b


@tool(
    tags=["math", "utility"],
    category="math",
    visibility=ToolVisibility.PUBLIC,
)
def subtract(a: float, b: float) -> float:
    """Subtract b from a."""
    return a - b


@tool(
    tags=["math", "utility"],
    category="math",
    visibility=ToolVisibility.PUBLIC,
)
def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b


@tool(
    tags=["math", "utility"],
    category="math",
    visibility=ToolVisibility.PUBLIC,
)
def divide(a: float, b: float) -> float:
    """Divide a by b."""
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b


# =============================================================================
# STRING TOOLS
# =============================================================================

@tool(
    tags=["string", "utility", "text"],
    category="text",
    visibility=ToolVisibility.PUBLIC,
)
def uppercase(text: str) -> str:
    """Convert text to uppercase."""
    return text.upper()


@tool(
    tags=["string", "utility", "text"],
    category="text",
    visibility=ToolVisibility.PUBLIC,
)
def lowercase(text: str) -> str:
    """Convert text to lowercase."""
    return text.lower()


@tool(
    tags=["string", "utility", "text"],
    category="text",
    visibility=ToolVisibility.PUBLIC,
)
def word_count(text: str) -> int:
    """Count the number of words in text."""
    return len(text.split())


@tool(
    tags=["string", "utility", "text"],
    category="text",
    visibility=ToolVisibility.PUBLIC,
)
def reverse_string(text: str) -> str:
    """Reverse a string."""
    return text[::-1]


# =============================================================================
# DATA TRANSFORMATION TOOLS
# =============================================================================

@tool(
    tags=["data", "json", "transform"],
    category="data",
    visibility=ToolVisibility.PUBLIC,
)
def json_to_dict(json_string: str) -> Dict[str, Any]:
    """Parse a JSON string into a dictionary."""
    return json.loads(json_string)


@tool(
    tags=["data", "json", "transform"],
    category="data",
    visibility=ToolVisibility.PUBLIC,
)
def dict_to_json(data: Dict[str, Any], indent: int = 2) -> str:
    """Convert a dictionary to a JSON string."""
    return json.dumps(data, indent=indent)


@tool(
    tags=["data", "list", "transform"],
    category="data",
    visibility=ToolVisibility.PUBLIC,
)
def flatten_list(nested_list: List) -> List:
    """Flatten a nested list into a single-level list."""
    result = []
    for item in nested_list:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result


@tool(
    tags=["data", "filter"],
    category="data",
    visibility=ToolVisibility.PUBLIC,
)
def filter_dict(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Filter a dictionary to only include specified keys."""
    return {k: v for k, v in data.items() if k in keys}


# =============================================================================
# ASYNC TOOL EXAMPLE
# =============================================================================

@tool(
    name="async_fetch",
    description="Simulate an async fetch operation",
    tags=["async", "network", "example"],
    category="network",
    visibility=ToolVisibility.PRIVATE,
)
async def async_fetch_example(url: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Simulate fetching data from a URL asynchronously.

    In a real implementation, this would use httpx or aiohttp.
    """
    import asyncio
    await asyncio.sleep(0.1)  # Simulate network delay
    return {
        "url": url,
        "status": 200,
        "data": {"message": f"Fetched from {url}"},
    }


# =============================================================================
# COMPLEX TOOL EXAMPLE
# =============================================================================

@tool(
    name="data_pipeline",
    description="Run a simple data transformation pipeline",
    tags=["data", "pipeline", "transform"],
    category="data",
    visibility=ToolVisibility.PUBLIC,
    version="1.1.0",
)
def data_pipeline(
    data: List[Dict[str, Any]],
    filter_key: Optional[str] = None,
    filter_value: Optional[Any] = None,
    sort_by: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Apply transformations to a list of dictionaries.

    Args:
        data: List of dictionaries to process
        filter_key: Key to filter by (optional)
        filter_value: Value to match for filtering (optional)
        sort_by: Key to sort by (optional)
        limit: Maximum number of results (optional)

    Returns:
        Transformed list of dictionaries
    """
    result = data.copy()

    # Filter
    if filter_key and filter_value is not None:
        result = [d for d in result if d.get(filter_key) == filter_value]

    # Sort
    if sort_by:
        result = sorted(result, key=lambda x: x.get(sort_by, ""))

    # Limit
    if limit:
        result = result[:limit]

    return result
