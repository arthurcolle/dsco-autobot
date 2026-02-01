"""
Test script for the Neo4j-backed tool registry.
"""
import asyncio
from pydantic import BaseModel
from tool_registry import ToolRegistry, create_registry_app

# Define some test models
class AddInput(BaseModel):
    a: float
    b: float

class AddOutput(BaseModel):
    result: float

class GreetInput(BaseModel):
    name: str

class GreetOutput(BaseModel):
    message: str


# Create registry - update password as needed
registry = ToolRegistry(
    neo4j_uri="neo4j://127.0.0.1:7687",
    neo4j_username="neo4j",
    neo4j_password="NEO4J2026!",
    service_name="test-service",
    service_url="http://localhost:8000"
)


# Register tools using decorators
@registry.function(description="Add two numbers together", tags=["math", "calculator"])
def add_numbers(input: AddInput) -> AddOutput:
    """Adds two numbers and returns the result."""
    return AddOutput(result=input.a + input.b)


@registry.function(description="Generate a greeting message", tags=["text", "greeting"])
async def greet(input: GreetInput) -> GreetOutput:
    """Creates a personalized greeting."""
    return GreetOutput(message=f"Hello, {input.name}!")


@registry.snippet(
    language="javascript",
    description="Format a date to ISO string",
    tags=["javascript", "date", "formatting"]
)
def format_date_js():
    """
    function formatDate(date) {
        return new Date(date).toISOString();
    }
    """


@registry.snippet(
    language="sql",
    description="Get active users from database",
    tags=["sql", "users", "query"]
)
def get_active_users_sql():
    """
    SELECT id, name, email, created_at
    FROM users
    WHERE status = 'active'
    ORDER BY created_at DESC
    LIMIT 100;
    """


@registry.task(description="Process items with progress", tags=["batch", "processing"])
async def process_batch(input: AddInput):
    """Simulates batch processing with progress updates."""
    for i in range(10):
        await asyncio.sleep(0.1)
        yield {"progress": (i + 1) * 10, "step": i + 1}
    yield {"result": input.a + input.b, "steps_completed": 10}


async def main():
    print("Starting registry...")
    await registry.start()
    print("Registry started!")

    # List all tools
    tools = await registry.get_tools()
    print(f"\nRegistered {len(tools)} tools:")
    for tool in tools:
        print(f"  - {tool.name} ({tool.type}): {tool.description[:50]}...")

    # Search for tools
    print("\n\nSearching for 'add':")
    results = await registry.search("add", limit=5)
    for tool in results:
        print(f"  - {tool.name}")

    # Get a specific tool
    print("\n\nGetting 'add_numbers' tool:")
    tool = await registry.get_tool_by_name("add_numbers")
    if tool:
        print(f"  Name: {tool.name}")
        print(f"  Type: {tool.type}")
        print(f"  Input schema: {tool.input_schema}")

    await registry.stop()
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
