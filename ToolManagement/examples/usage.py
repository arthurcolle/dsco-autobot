"""
Example usage of the Tool Management Service

This file demonstrates how to use the Tool Management API client
to register and execute tools.
"""

import asyncio
from typing import Dict, Any
from pydantic import BaseModel, Field
from tool_mgmt_api_client import ToolManagementAPIClient


# Define input and output models for our tools
class TextInput(BaseModel):
    text: str = Field(..., description="Input text to process")
    uppercase: bool = Field(False, description="Convert to uppercase")


class TextOutput(BaseModel):
    processed_text: str = Field(..., description="Processed text result")
    character_count: int = Field(..., description="Number of characters")
    word_count: int = Field(..., description="Number of words")


class MathInput(BaseModel):
    a: float = Field(..., description="First number")
    b: float = Field(..., description="Second number")
    operation: str = Field("add", description="Operation: add, subtract, multiply, divide")


class MathOutput(BaseModel):
    result: float = Field(..., description="Calculation result")
    operation_performed: str = Field(..., description="The operation that was performed")


# Initialize the client
# For local development:
# client = ToolManagementAPIClient(
#     tool_api_url="http://localhost:8000",
#     service_name="Example Service",
#     service_url="http://localhost:8000"
# )

# For Modal deployment:
client = ToolManagementAPIClient(
    tool_api_url="https://your-app.modal.run",  # Replace with your Modal URL
    service_name="Example Service",
    service_url="https://your-app.modal.run",
    debug=True  # Enable debug logging
)


# Define tools using the decorator
@client.tool_wrapper(description="Process and analyze text")
async def process_text(input: TextInput) -> TextOutput:
    """
    Process text with various transformations and analysis.

    This tool can convert text to uppercase and provides
    character and word count statistics.
    """
    text = input.text
    if input.uppercase:
        text = text.upper()

    return TextOutput(
        processed_text=text,
        character_count=len(text),
        word_count=len(text.split())
    )


@client.tool_wrapper(description="Perform mathematical operations")
async def calculate(input: MathInput) -> MathOutput:
    """
    Perform basic mathematical operations on two numbers.

    Supports addition, subtraction, multiplication, and division.
    """
    operations = {
        "add": lambda a, b: a + b,
        "subtract": lambda a, b: a - b,
        "multiply": lambda a, b: a * b,
        "divide": lambda a, b: a / b if b != 0 else float('inf')
    }

    operation_func = operations.get(input.operation, operations["add"])
    result = operation_func(input.a, input.b)

    return MathOutput(
        result=result,
        operation_performed=input.operation
    )


async def main():
    """Main function demonstrating tool usage."""

    print("Tool Management Service Example")
    print("=" * 50)

    # 1. Health check
    print("\n1. Checking service health...")
    try:
        health = await client.health_check()
        print(f"   Service is {health['status']}")
    except Exception as e:
        print(f"   Health check failed: {e}")
        return

    # 2. Register tools
    print("\n2. Registering tools...")
    try:
        await client.register_tools()
        print("   Tools registered successfully!")
    except Exception as e:
        print(f"   Registration failed: {e}")
        return

    # 3. List available tools
    print("\n3. Listing available tools...")
    try:
        tools = await client.get_tools()
        for tool in tools:
            print(f"   - {tool['name']}: {tool['description']}")
    except Exception as e:
        print(f"   Failed to list tools: {e}")

    # 4. Execute text processing tool
    print("\n4. Executing text processing tool...")
    try:
        result = await client.execute_tool(
            tool_name="process_text",
            input_data={
                "text": "Hello World from Tool Management Service",
                "uppercase": True
            }
        )
        print(f"   Result: {result}")
    except Exception as e:
        print(f"   Execution failed: {e}")

    # 5. Execute math calculation tool
    print("\n5. Executing math calculation tool...")
    try:
        result = await client.execute_tool(
            tool_name="calculate",
            input_data={
                "a": 10,
                "b": 5,
                "operation": "multiply"
            }
        )
        print(f"   Result: {result}")
    except Exception as e:
        print(f"   Execution failed: {e}")

    # 6. Search for tools (if implemented)
    print("\n6. Searching for tools...")
    try:
        search_results = await client.search_tools(
            prompt="text processing",
            limit=5
        )
        print(f"   Found {len(search_results)} matching tools")
        for tool in search_results:
            print(f"   - {tool['name']}")
    except Exception as e:
        print(f"   Search failed: {e}")

    print("\n" + "=" * 50)
    print("Example completed!")


# Advanced example: Tool with complex input/output
class DataAnalysisInput(BaseModel):
    data: list[float] = Field(..., description="List of numerical data points")
    operation: str = Field("mean", description="Statistical operation to perform")


class DataAnalysisOutput(BaseModel):
    result: float = Field(..., description="Result of the statistical operation")
    data_points: int = Field(..., description="Number of data points analyzed")
    summary: Dict[str, float] = Field(..., description="Summary statistics")


@client.tool_wrapper(description="Perform statistical analysis on data")
async def analyze_data(input: DataAnalysisInput) -> DataAnalysisOutput:
    """
    Perform statistical analysis on numerical data.

    Supports operations: mean, median, sum, min, max
    """
    import statistics

    data = input.data
    operations = {
        "mean": statistics.mean,
        "median": statistics.median,
        "sum": sum,
        "min": min,
        "max": max
    }

    operation_func = operations.get(input.operation, statistics.mean)
    result = operation_func(data)

    summary = {
        "mean": statistics.mean(data),
        "median": statistics.median(data),
        "min": min(data),
        "max": max(data),
        "count": len(data)
    }

    return DataAnalysisOutput(
        result=result,
        data_points=len(data),
        summary=summary
    )


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())