"""
Test client for FastAPI MCP Chat Server
Demonstrates streaming chat with MCP tool integration
"""

import asyncio
import json
import httpx
from typing import AsyncGenerator
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.markdown import Markdown

console = Console()
SERVER_URL = "http://localhost:2046"


async def stream_chat(
    message: str,
    conversation_id: str = None,
    model: str = "anthropic/claude-3.5-sonnet",
    enable_tools: bool = True
) -> AsyncGenerator[dict, None]:
    """Stream chat and yield parsed events"""

    payload = {
        "message": message,
        "conversation_id": conversation_id,
        "model": model,
        "stream": True,
        "enable_tools": enable_tools,
        "temperature": 0.7,
        "max_tokens": 4000
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        async with client.stream("POST", f"{SERVER_URL}/chat", json=payload) as response:
            if response.status_code != 200:
                error = await response.aread()
                raise Exception(f"Server error {response.status_code}: {error.decode()}")

            async for line in response.aiter_lines():
                if line.startswith("event:"):
                    event_type = line.split("event:", 1)[1].strip()
                elif line.startswith("data:"):
                    data_str = line.split("data:", 1)[1].strip()
                    try:
                        event_data = json.loads(data_str)
                        event_data["_event_type"] = event_type
                        yield event_data
                    except json.JSONDecodeError:
                        continue


async def interactive_chat():
    """Interactive chat with rich display"""

    console.print(Panel.fit(
        "[bold cyan]MCP Chat Client[/bold cyan]\n"
        "FastAPI + OpenRouter + MCP Tools\n"
        f"Server: {SERVER_URL}",
        border_style="cyan"
    ))

    # Check server status
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{SERVER_URL}/")
            info = resp.json()

            console.print(f"\n[green]✓[/green] Connected to server")
            console.print(f"[dim]MCP Connected: {info.get('mcp_connected')}[/dim]")
            console.print(f"[dim]Tools Available: {info.get('tools_available', 0)}[/dim]")

            # List tools
            resp = await client.get(f"{SERVER_URL}/tools")
            tools_data = resp.json()
            tools = tools_data.get("tools", [])

            if tools:
                console.print(f"\n[bold]Available MCP Tools:[/bold]")
                for tool in tools[:5]:  # Show first 5
                    console.print(f"  • {tool['name']}: {tool.get('description', 'No description')[:60]}")
                if len(tools) > 5:
                    console.print(f"  ... and {len(tools) - 5} more")

    except Exception as e:
        console.print(f"[red]✗ Error connecting to server: {e}[/red]")
        console.print("[yellow]Make sure server is running: python fastapi_mcp_chat.py[/yellow]")
        return

    conversation_id = None

    console.print("\n[bold]Commands:[/bold]")
    console.print("  /exit - Quit")
    console.print("  /new - Start new conversation")
    console.print("  /notools - Disable tools for next message")
    console.print("")

    while True:
        console.print("[bold green]You:[/bold green]", end=" ")
        user_input = input()

        if user_input.lower() in ["/exit", "/quit", "/q"]:
            console.print("[yellow]Goodbye![/yellow]")
            break

        if user_input.lower() == "/new":
            conversation_id = None
            console.print("[yellow]Starting new conversation[/yellow]")
            continue

        enable_tools = True
        if user_input.lower().startswith("/notools"):
            enable_tools = False
            user_input = user_input.replace("/notools", "").strip()
            console.print("[dim]Tools disabled for this message[/dim]")

        if not user_input.strip():
            continue

        console.print("\n[bold blue]Assistant:[/bold blue]")

        full_response = ""
        current_tool = None
        tool_calls_count = 0

        try:
            async for event in stream_chat(
                message=user_input,
                conversation_id=conversation_id,
                enable_tools=enable_tools
            ):
                event_type = event.get("_event_type")
                data = event.get("data", {})

                if event_type == "session.start":
                    conversation_id = event["conversation_id"]
                    tools_count = data.get("tools_available", 0)
                    if tools_count > 0:
                        console.print(f"[dim]Using {tools_count} tools[/dim]")

                elif event_type == "message.start":
                    iteration = data.get("iteration", 1)
                    if iteration > 1:
                        console.print(f"\n[dim]--- Iteration {iteration} ---[/dim]")

                elif event_type == "message.delta":
                    delta = data.get("delta", "")
                    full_response += delta
                    console.print(delta, end="", flush=True)

                elif event_type == "message.end":
                    if not data.get("has_tool_calls"):
                        console.print()  # New line after message

                elif event_type == "tool.call.start":
                    tool_name = data.get("tool_name")
                    console.print(f"\n\n[cyan]🔧 Calling tool: {tool_name}[/cyan]")
                    current_tool = tool_name
                    tool_calls_count += 1

                elif event_type == "tool.call.args":
                    args = data.get("arguments", {})
                    console.print(f"[dim]Arguments: {json.dumps(args, indent=2)[:200]}[/dim]")

                elif event_type == "tool.call.execute":
                    mcp_name = data.get("mcp_tool_name")
                    console.print(f"[dim]Executing MCP tool: {mcp_name}...[/dim]")

                elif event_type == "tool.result":
                    result = data.get("result", "")
                    result_len = data.get("result_length", 0)
                    console.print(f"[green]✓[/green] Result ({result_len} chars)")
                    if result:
                        # Show truncated result
                        console.print(f"[dim]{result[:200]}{'...' if len(result) > 200 else ''}[/dim]")

                elif event_type == "tool.call.end":
                    console.print(f"[dim]Tool call complete[/dim]")
                    current_tool = None

                elif event_type == "session.end":
                    iterations = data.get("iterations", 1)
                    total_events = data.get("total_events", 0)

                    console.print(f"\n[dim]Completed in {iterations} iteration(s), {total_events} events[/dim]")
                    if tool_calls_count > 0:
                        console.print(f"[dim]Tool calls: {tool_calls_count}[/dim]")

                elif event_type == "error":
                    console.print(f"\n[red]Error: {data.get('message')}[/red]")

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
            break
        except Exception as e:
            console.print(f"\n[red]Error: {e}[/red]")

        console.print()  # Blank line before next prompt


async def simple_test():
    """Simple test without interaction"""

    console.print("[bold]Simple Test[/bold]\n")

    # Test 1: Simple query
    console.print("[green]Test 1:[/green] Simple query without tools")
    console.print("[blue]User:[/blue] Hello, how are you?\n")

    full_response = ""

    async for event in stream_chat(
        "Hello, how are you?",
        enable_tools=False
    ):
        if event.get("_event_type") == "message.delta":
            delta = event["data"]["delta"]
            full_response += delta
            console.print(delta, end="", flush=True)

    console.print("\n")

    # Test 2: Query that might use tools
    console.print("\n[green]Test 2:[/green] Query with tools enabled")
    console.print("[blue]User:[/blue] Search for information about FastAPI\n")

    full_response = ""
    tool_used = False

    async for event in stream_chat(
        "Search for information about FastAPI",
        enable_tools=True
    ):
        event_type = event.get("_event_type")

        if event_type == "message.delta":
            delta = event["data"]["delta"]
            full_response += delta
            console.print(delta, end="", flush=True)

        elif event_type == "tool.call.start":
            tool_name = event["data"]["tool_name"]
            console.print(f"\n[cyan]🔧 Tool: {tool_name}[/cyan]")
            tool_used = True

        elif event_type == "tool.result":
            console.print(f"[green]✓ Tool completed[/green]\n")

    console.print("\n")

    if tool_used:
        console.print("[green]✓ Tool calling works![/green]")
    else:
        console.print("[yellow]⚠ No tools were called[/yellow]")


async def main():
    """Main entry point"""

    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        await simple_test()
    else:
        await interactive_chat()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Exited[/yellow]")
