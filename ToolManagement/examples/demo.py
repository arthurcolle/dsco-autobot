#!/usr/bin/env python3
"""
Demo script showing how to register and use tools.

Run: python demo.py
"""

import asyncio
import sys
sys.path.insert(0, "src")

from tool_management import Tool, ToolRegistry, tool, get_registry
from tool_management.models import ToolVisibility


# =============================================================================
# DEFINE CUSTOM TOOLS USING THE @tool DECORATOR
# =============================================================================

@tool(
    name="greet",
    description="Generate a greeting message",
    tags=["greeting", "demo"],
    category="demo",
    visibility=ToolVisibility.PUBLIC,
)
def greet(name: str, formal: bool = False) -> str:
    """Generate a greeting for the given name."""
    if formal:
        return f"Good day, {name}. How may I assist you?"
    return f"Hello, {name}!"


@tool(
    tags=["demo", "calculation"],
    category="demo",
)
def calculate_area(width: float, height: float) -> float:
    """Calculate the area of a rectangle."""
    return width * height


@tool(
    name="fetch_weather",
    description="Simulate fetching weather data",
    tags=["demo", "async", "weather"],
    category="demo",
)
async def fetch_weather(city: str) -> dict:
    """Fetch weather data for a city (simulated)."""
    await asyncio.sleep(0.1)  # Simulate API call
    return {
        "city": city,
        "temperature": 72,
        "conditions": "sunny",
        "humidity": 45,
    }


# =============================================================================
# MANUAL TOOL REGISTRATION
# =============================================================================

def register_manual_tool():
    """Demonstrate manual tool registration."""
    from tool_management.models.tool import ToolInput, ToolOutput

    # Create a tool manually
    manual_tool = Tool(
        name="manual_example",
        description="A manually registered tool",
        inputs=[
            ToolInput(name="x", type="number", description="First number"),
            ToolInput(name="y", type="number", description="Second number"),
        ],
        output=ToolOutput(type="number", description="Sum of x and y"),
        tags=["manual", "demo"],
        category="demo",
        visibility=ToolVisibility.PUBLIC,
    )

    # Define the handler
    def handler(x: float, y: float) -> float:
        return x + y

    manual_tool._callable = handler

    # Register it
    registry = get_registry()
    registry.register(manual_tool)

    return manual_tool


# =============================================================================
# MAIN DEMO
# =============================================================================

async def main():
    print("=" * 60)
    print("TOOL MANAGEMENT DEMO")
    print("=" * 60)

    # Get the global registry
    registry = get_registry()

    # Import example tools to register them
    from tool_management.tools import examples  # noqa

    # Register a manual tool
    register_manual_tool()

    # List all registered tools
    print(f"\n📦 Registered Tools ({len(registry)} total):")
    print("-" * 40)
    for t in registry:
        print(f"  • {t.name:20} [{t.category or 'uncategorized'}] - {t.description[:40]}...")

    # List by category
    print("\n📁 Categories:")
    for cat in registry.list_categories():
        tools = registry.find_by_category(cat)
        print(f"  • {cat}: {len(tools)} tools")

    # List all tags
    print(f"\n🏷️  Tags: {', '.join(registry.list_tags())}")

    # Search for tools
    print("\n🔍 Search for 'math' tools:")
    math_tools = registry.search(tags=["math"])
    for t in math_tools:
        print(f"  • {t.name}")

    # Execute some tools
    print("\n⚡ Executing tools:")
    print("-" * 40)

    # Execute greet
    result = await registry.execute("greet", name="World")
    print(f"  greet('World') = {result}")

    result = await registry.execute("greet", name="Sir Arthur", formal=True)
    print(f"  greet('Sir Arthur', formal=True) = {result}")

    # Execute calculate_area
    result = await registry.execute("calculate_area", width=5.0, height=3.0)
    print(f"  calculate_area(5, 3) = {result}")

    # Execute async tool
    result = await registry.execute("fetch_weather", city="New York")
    print(f"  fetch_weather('New York') = {result}")

    # Execute math tools
    result = await registry.execute("add", a=10, b=5)
    print(f"  add(10, 5) = {result}")

    result = await registry.execute("multiply", a=7, b=6)
    print(f"  multiply(7, 6) = {result}")

    # Show MCP format
    print("\n📋 MCP Format (first 3 tools):")
    print("-" * 40)
    mcp_tools = registry.to_mcp_tools()[:3]
    import json
    for t in mcp_tools:
        print(f"  {json.dumps(t, indent=4)[:200]}...")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
