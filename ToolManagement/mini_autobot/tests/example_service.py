"""
Example service demonstrating mini_autobot client usage.

To run this example:
    1. Start Graph API (Terminal 1):
       NEO4J_PASSWORD=NEO4J2026! python -m mini_autobot.graph_api.server

    2. Start Tool API (Terminal 2):
       GRAPH_API_URL=http://localhost:2015 python -m mini_autobot.tool_api.server

    3. Run this example (Terminal 3):
       python -m mini_autobot.tests.example_service
"""
import asyncio
from pydantic import BaseModel, Field
from typing import List, Optional

# Import the client
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from mini_autobot.client import ToolManagementClient


# Define your Pydantic models
class AddNumbersInput(BaseModel):
    a: float = Field(description="First number")
    b: float = Field(description="Second number")


class AddNumbersOutput(BaseModel):
    result: float = Field(description="Sum of the two numbers")


class GetUserInput(BaseModel):
    user_id: str = Field(description="The user's ID")


class UserOutput(BaseModel):
    id: str
    name: str
    email: str


class SearchQuery(BaseModel):
    query: str = Field(description="Search query string")
    limit: int = Field(default=10, description="Max results")


class SearchResult(BaseModel):
    items: List[str]
    total: int


# Initialize the client
tools = ToolManagementClient(
    tool_api_url="http://localhost:2016",
    service_name="ExampleService",
    service_url="http://localhost:8000"
)


# Example 1: Simple function
@tools.function(description="Add two numbers together")
def add_numbers(input: AddNumbersInput) -> AddNumbersOutput:
    """Performs basic addition of two numeric values."""
    return AddNumbersOutput(result=input.a + input.b)


# Example 2: Async endpoint
@tools.endpoint(description="Get user by ID", path="/users/{user_id}", method="GET")
async def get_user(input: GetUserInput) -> UserOutput:
    """Retrieve a user from the database by their unique identifier."""
    # In real implementation, this would query a database
    return UserOutput(
        id=input.user_id,
        name="John Doe",
        email="john@example.com"
    )


# Example 3: Code snippet (JavaScript)
@tools.snippet(language="javascript", description="Format a date to ISO string")
def format_date():
    """
    function formatDate(dateStr) {
        const date = new Date(dateStr);
        return date.toISOString();
    }
    """


# Example 4: Code snippet (SQL)
@tools.snippet(language="sql", description="Get active users from database")
def get_active_users_sql():
    """
    SELECT id, name, email, created_at
    FROM users
    WHERE status = 'active'
    AND last_login > NOW() - INTERVAL '30 days'
    ORDER BY last_login DESC
    LIMIT 100;
    """


# Example 5: Long-running task
@tools.task(description="Process large dataset", timeout=3600)
async def process_dataset(input: SearchQuery) -> SearchResult:
    """Process a large dataset with the given search query."""
    # Simulate processing
    await asyncio.sleep(1)
    return SearchResult(
        items=["result1", "result2", "result3"],
        total=3
    )


# Example 6: Streaming task (generator)
@tools.task(description="Stream search results")
async def stream_search(input: SearchQuery):
    """Stream search results as they are found."""
    for i in range(input.limit):
        await asyncio.sleep(0.1)
        yield {"item": f"result_{i}", "index": i}


async def main():
    """Run the example."""
    print("Mini Autobot - Example Service")
    print("=" * 50)

    # Register all decorated tools with the Tool API
    print("\nRegistering tools...")
    await tools.register_tools()

    # List registered tools
    print("\nFetching registered tools...")
    all_tools = await tools.get_tools()
    print(f"Found {len(all_tools)} tools:")
    for t in all_tools:
        print(f"  - {t['name']}: {t.get('description', '')[:50]}...")

    # Test local function execution (wrapped as async)
    print("\nTesting local function 'add_numbers':")
    result = add_numbers._original_func(AddNumbersInput(a=5, b=3))
    print(f"  5 + 3 = {result.result}")

    # Test snippet retrieval
    print("\nJavaScript snippet 'format_date':")
    print(f"  {format_date()[:50]}...")

    # Search for tools
    print("\nSearching for 'user' tools:")
    search_results = await tools.search_tools("user", limit=3)
    for t in search_results or []:
        print(f"  - {t['name']}")

    # Close the client
    await tools.close()
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
