from claudius import Claudius, EventType
import asyncio

async def main():
    # Initialize Claudius
    claudius = Claudius(objectives=["Demonstrate Claudius capabilities"], tool_dir="tools")

    # Example 1: Basic conversation
    print("Example 1: Basic conversation")
    response = claudius.prompt("Hello, Claudius. Can you explain what you are?")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
    print("\n")

    # Example 2: Using a tool
    print("Example 2: Using a tool")
    response = claudius.prompt("Can you calculate the factorial of 5?")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
        elif event.type == EventType.FUNCTION_CALL:
            print(f"\nFunction call: {event.data}")
        elif event.type == EventType.FUNCTION_RESULT:
            print(f"Function result: {event.data}")
    print("\n")

    # Example 3: Creating a new tool
    print("Example 3: Creating a new tool")
    response = claudius.prompt("Create a new tool that converts Celsius to Fahrenheit.")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
    print("\n")

    # Example 4: Using the knowledge graph
    print("Example 4: Using the knowledge graph")
    claudius.graph_operation("REGISTER_NODE", "temperature_conversion", {
        "type": "concept",
        "description": "Converting between temperature scales"
    })
    claudius.graph_operation("REGISTER_NODE", "celsius", {
        "type": "unit",
        "description": "Celsius temperature scale"
    })
    claudius.graph_operation("REGISTER_NODE", "fahrenheit", {
        "type": "unit",
        "description": "Fahrenheit temperature scale"
    })
    claudius.graph_operation("REGISTER_EDGE", "celsius", "fahrenheit", "conversion", {
        "formula": "F = C * 9/5 + 32"
    })

    response = claudius.prompt("Explain the relationship between Celsius and Fahrenheit using the knowledge graph.")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
    print("\n")

    # Example 5: Self-modification
    print("Example 5: Self-modification")
    response = claudius.prompt("Optimize your system prompt to better handle mathematical queries.")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
    print("\n")

    # Example 6: Visualization
    print("Example 6: Visualization")
    response = claudius.prompt("Create a simple bar chart showing the factorial values for numbers 1 to 5.")
    async for event in response:
        if event.type == EventType.TOKEN:
            print(event.data, end='', flush=True)
        elif event.type == EventType.FUNCTION_CALL:
            print(f"\nFunction call: {event.data}")
        elif event.type == EventType.FUNCTION_RESULT:
            print(f"Function result: {event.data}")
    print("\n")

if __name__ == "__main__":
    asyncio.run(main())
