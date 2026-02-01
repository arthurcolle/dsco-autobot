#!/usr/bin/env python3
"""
Demo: Enhanced Tool Management with @tools.endpoint() and @tools.snippet()

This demonstrates:
1. Pydantic input/output schemas
2. @tools.endpoint() for API endpoints
3. @tools.snippet() for code snippets
4. @tools.function() for plain functions
5. Export to MCP, OpenAI, and Anthropic formats
"""

import asyncio
import sys
sys.path.insert(0, "src")

from pydantic import BaseModel, Field
from typing import List, Optional
from tool_management.tools_api import Tools, ToolType, ToolVisibility


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class AddRequest(BaseModel):
    """Request model for addition."""
    a: float = Field(..., description="First number")
    b: float = Field(..., description="Second number")


class AddResponse(BaseModel):
    """Response model for addition."""
    result: float = Field(..., description="Sum of a and b")
    operation: str = Field("add", description="Operation performed")


class WeatherRequest(BaseModel):
    """Request for weather data."""
    city: str = Field(..., description="City name")
    units: str = Field("celsius", description="Temperature units")


class WeatherResponse(BaseModel):
    """Weather data response."""
    city: str
    temperature: float
    conditions: str
    humidity: int


class TextAnalysis(BaseModel):
    """Text analysis result."""
    word_count: int
    char_count: int
    sentence_count: int
    keywords: List[str]


# =============================================================================
# INITIALIZE TOOLS
# =============================================================================

tools = Tools(
    service_name="demo-service",
    service_url="http://localhost:8000",
    # tool_api_url="http://tool-api.internal",  # Uncomment to enable API registration
    # graph_api_url="https://graph.brainchain.cloud",  # Uncomment to enable graph storage
)


# =============================================================================
# ENDPOINT TOOLS (with Pydantic models)
# =============================================================================

@tools.endpoint(
    tags=["math", "calculation"],
    category="math",
    visibility=ToolVisibility.PUBLIC,
)
async def add(request: AddRequest) -> AddResponse:
    """Add two numbers together."""
    return AddResponse(
        result=request.a + request.b,
        operation="add"
    )


@tools.endpoint(
    tags=["math", "calculation"],
    category="math",
)
async def multiply(request: AddRequest) -> AddResponse:
    """Multiply two numbers."""
    return AddResponse(
        result=request.a * request.b,
        operation="multiply"
    )


@tools.endpoint(
    name="get_weather",
    description="Fetch weather data for a city",
    tags=["weather", "api"],
    category="external",
)
async def weather_endpoint(request: WeatherRequest) -> WeatherResponse:
    """Get current weather for a city."""
    # Simulated weather data
    return WeatherResponse(
        city=request.city,
        temperature=22.5 if request.units == "celsius" else 72.5,
        conditions="sunny",
        humidity=45
    )


# =============================================================================
# SNIPPET TOOLS (code stored and executed dynamically)
# =============================================================================

@tools.snippet(
    name="analyze_text",
    description="Analyze text and return statistics",
    tags=["text", "analysis", "nlp"],
    category="text-processing",
)
def text_analysis_snippet(text: str) -> TextAnalysis:
    """
import re

# Count words
words = text.split()
word_count = len(words)

# Count characters
char_count = len(text)

# Count sentences
sentences = re.split(r'[.!?]+', text)
sentence_count = len([s for s in sentences if s.strip()])

# Extract keywords (simple: words > 4 chars)
keywords = list(set(w.lower() for w in words if len(w) > 4))[:10]

result = {
    "word_count": word_count,
    "char_count": char_count,
    "sentence_count": sentence_count,
    "keywords": keywords
}
    """
    pass  # Docstring is the actual code


@tools.snippet(
    name="json_transform",
    description="Transform JSON data with a jq-like syntax",
    tags=["json", "transform", "data"],
    category="data-processing",
)
def json_transform_snippet(data: dict, path: str) -> dict:
    """
import json

# Simple path-based extraction
parts = path.strip('.').split('.')
current = data

for part in parts:
    if part and isinstance(current, dict):
        current = current.get(part, {})
    elif part.isdigit() and isinstance(current, list):
        idx = int(part)
        current = current[idx] if idx < len(current) else None

result = current
    """
    pass


@tools.snippet(
    name="http_fetch",
    description="Fetch data from a URL",
    tags=["http", "api", "fetch"],
    category="network",
)
async def http_fetch_snippet(url: str, method: str = "GET") -> dict:
    """
import httpx

async with httpx.AsyncClient() as client:
    if method.upper() == "GET":
        response = await client.get(url)
    elif method.upper() == "POST":
        response = await client.post(url, json=inputs.get("body", {}))
    else:
        response = await client.request(method.upper(), url)

    result = {
        "status": response.status_code,
        "headers": dict(response.headers),
        "body": response.text
    }
    """
    pass


# =============================================================================
# FUNCTION TOOLS (regular Python functions)
# =============================================================================

@tools.function(
    tags=["string", "utility"],
    category="text",
)
def reverse_string(text: str) -> str:
    """Reverse a string."""
    return text[::-1]


@tools.function(
    tags=["list", "utility"],
    category="data",
)
def flatten_list(nested: List) -> List:
    """Flatten a nested list."""
    result = []
    for item in nested:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result


@tools.function(
    tags=["math", "statistics"],
    category="math",
)
def statistics(numbers: List[float]) -> dict:
    """Calculate basic statistics for a list of numbers."""
    if not numbers:
        return {"error": "Empty list"}

    n = len(numbers)
    total = sum(numbers)
    mean = total / n
    sorted_nums = sorted(numbers)
    median = sorted_nums[n // 2] if n % 2 else (sorted_nums[n//2 - 1] + sorted_nums[n//2]) / 2

    return {
        "count": n,
        "sum": total,
        "mean": mean,
        "median": median,
        "min": min(numbers),
        "max": max(numbers),
    }


# =============================================================================
# DEMO
# =============================================================================

async def main():
    print("=" * 70)
    print("ENHANCED TOOL MANAGEMENT DEMO")
    print("=" * 70)

    # List all registered tools
    all_tools = tools.list_tools()
    print(f"\n📦 Registered Tools ({len(all_tools)} total):")
    print("-" * 50)

    for t in all_tools:
        type_emoji = {
            ToolType.ENDPOINT: "🌐",
            ToolType.SNIPPET: "📝",
            ToolType.FUNCTION: "⚡",
        }.get(t.tool_type, "🔧")
        print(f"  {type_emoji} {t.name:20} [{t.tool_type.value:8}] - {t.description[:35]}...")

    # Group by type
    print("\n📊 By Type:")
    for tool_type in ToolType:
        count = len(tools.search(tool_type=tool_type))
        if count:
            print(f"  • {tool_type.value}: {count} tools")

    # Group by category
    print("\n📁 By Category:")
    categories = set(t.category for t in all_tools if t.category)
    for cat in sorted(categories):
        count = len(tools.search(category=cat))
        print(f"  • {cat}: {count} tools")

    # Show Pydantic schema extraction
    print("\n📋 Schema Examples:")
    print("-" * 50)

    add_tool = tools.get_by_name("add")
    if add_tool:
        print(f"\n  Tool: {add_tool.name}")
        print(f"  Input Model: {add_tool.input_model_name}")
        print(f"  Output Model: {add_tool.output_model_name}")
        print(f"  Input Schema: {add_tool.input_schema.model_dump()}")

    # Execute tools
    print("\n⚡ Tool Execution:")
    print("-" * 50)

    # Execute endpoint tool
    result = await tools.execute("add", request=AddRequest(a=10, b=5))
    print(f"  add(10, 5) = {result}")

    result = await tools.execute("multiply", request=AddRequest(a=7, b=6))
    print(f"  multiply(7, 6) = {result}")

    # Execute function tools
    result = await tools.execute("reverse_string", text="Hello World")
    print(f"  reverse_string('Hello World') = {result}")

    result = await tools.execute("flatten_list", nested=[[1, 2], [3, [4, 5]], 6])
    print(f"  flatten_list([[1,2],[3,[4,5]],6]) = {result}")

    result = await tools.execute("statistics", numbers=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print(f"  statistics([1..10]) = {result}")

    # Export formats
    print("\n📤 Export Formats:")
    print("-" * 50)

    mcp_tools = tools.to_mcp_tools()
    print(f"\n  MCP Format ({len(mcp_tools)} tools):")
    print(f"    {mcp_tools[0]}")

    openai_tools = tools.to_openai_tools()
    print(f"\n  OpenAI Format ({len(openai_tools)} tools):")
    print(f"    {openai_tools[0]}")

    anthropic_tools = tools.to_anthropic_tools()
    print(f"\n  Anthropic Format ({len(anthropic_tools)} tools):")
    print(f"    {anthropic_tools[0]}")

    # Show snippet code
    print("\n📝 Snippet Code Examples:")
    print("-" * 50)

    snippet_tool = tools.get_by_name("analyze_text")
    if snippet_tool and snippet_tool.code:
        print(f"\n  Snippet: {snippet_tool.name}")
        print(f"  Code Hash: {snippet_tool.code_hash}")
        print(f"  Code Preview:")
        for line in snippet_tool.code.split("\n")[:5]:
            print(f"    {line}")
        print("    ...")

    print("\n" + "=" * 70)
    print("Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
