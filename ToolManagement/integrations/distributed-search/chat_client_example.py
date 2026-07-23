"""
Example client for the Streaming Chat Server
Demonstrates how to consume all event types from the SSE stream
"""

import asyncio
import json
import httpx
from typing import AsyncGenerator, Dict, Any
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.live import Live
from rich.table import Table
from datetime import datetime

console = Console()

SERVER_URL = "http://localhost:2046"


async def stream_chat(
    message: str,
    conversation_id: str = None,
    enable_tools: bool = False,
    enable_thinking: bool = False,
    model: str = "default",
    temperature: float = 0.7
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream chat responses and yield parsed events
    """

    payload = {
        "message": message,
        "conversation_id": conversation_id,
        "stream": True,
        "enable_tools": enable_tools,
        "enable_thinking": enable_thinking,
        "model": model,
        "temperature": temperature
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", f"{SERVER_URL}/chat", json=payload) as response:
            response.raise_for_status()

            async for line in response.aiter_lines():
                if line.startswith("event:"):
                    event_type = line.split("event:", 1)[1].strip()
                elif line.startswith("data:"):
                    data = line.split("data:", 1)[1].strip()
                    try:
                        event_data = json.loads(data)
                        event_data["_event_type"] = event_type
                        yield event_data
                    except json.JSONDecodeError:
                        continue


async def interactive_chat():
    """
    Interactive chat session with rich display of all events
    """

    console.print(Panel.fit(
        "[bold cyan]Streaming Chat Client[/bold cyan]\n"
        "Connected to comprehensive event streaming server\n"
        f"Server: {SERVER_URL}",
        border_style="cyan"
    ))

    conversation_id = None
    full_response = ""

    while True:
        console.print("\n[bold green]You:[/bold green]", end=" ")
        user_input = input()

        if user_input.lower() in ["/exit", "/quit", "/q"]:
            console.print("[yellow]Goodbye![/yellow]")
            break

        if user_input.lower() == "/new":
            conversation_id = None
            console.print("[yellow]Starting new conversation[/yellow]")
            continue

        if user_input.lower().startswith("/tools"):
            enable_tools = True
            user_input = user_input.replace("/tools", "").strip()
        else:
            enable_tools = False

        if user_input.lower().startswith("/think"):
            enable_thinking = True
            user_input = user_input.replace("/think", "").strip()
        else:
            enable_thinking = False

        if not user_input.strip():
            continue

        # Event counters
        event_counts = {}
        full_response = ""
        current_message_id = None

        console.print("[bold blue]Assistant:[/bold blue]", end=" ")

        try:
            async for event in stream_chat(
                message=user_input,
                conversation_id=conversation_id,
                enable_tools=enable_tools,
                enable_thinking=enable_thinking
            ):
                event_type = event.get("_event_type", event.get("event"))
                event_counts[event_type] = event_counts.get(event_type, 0) + 1

                # Handle different event types
                if event_type == "session.start":
                    conversation_id = event["conversation_id"]
                    current_message_id = event["data"]["message_id"]
                    console.print(f"[dim]Session {conversation_id[:8]}... started[/dim]")

                elif event_type == "thinking.start":
                    console.print("\n[italic yellow]💭 Thinking...[/italic yellow]", end=" ")

                elif event_type == "thinking.delta":
                    console.print(event["data"]["delta"], end="", flush=True)

                elif event_type == "thinking.end":
                    console.print("[dim] done[/dim]")

                elif event_type == "tool.call.start":
                    tool_name = event["data"]["tool_name"]
                    console.print(f"\n[cyan]🔧 Calling tool: {tool_name}[/cyan]", end=" ")

                elif event_type == "tool.result":
                    result = event["data"]["result"]
                    console.print(f"[green]✓[/green]")
                    console.print(f"[dim]Result: {json.dumps(result, indent=2)}[/dim]")

                elif event_type == "message.start":
                    console.print()

                elif event_type == "message.delta":
                    delta = event["data"]["delta"]
                    full_response += delta
                    console.print(delta, end="", flush=True)

                elif event_type == "message.end":
                    tokens = event["data"].get("tokens_used", 0)
                    console.print(f"\n[dim]({tokens} tokens)[/dim]")

                elif event_type == "metadata":
                    metadata = event["data"]
                    console.print(f"[dim]Total messages: {metadata.get('total_messages', 0)}[/dim]")

                elif event_type == "session.end":
                    total_events = event["data"].get("total_events", 0)
                    console.print(f"[dim]Session complete ({total_events} events)[/dim]")

        except httpx.HTTPError as e:
            console.print(f"[red]Error: {e}[/red]")
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
            break

        # Show event summary
        if event_counts:
            console.print("\n[dim]Events received:[/dim]", end=" ")
            console.print("[dim]" + ", ".join(f"{k}: {v}" for k, v in event_counts.items()) + "[/dim]")


async def simple_chat_example():
    """
    Simple example showing how to use the streaming client
    """

    console.print("[bold]Simple Chat Example[/bold]\n")

    # First message
    console.print("[green]User:[/green] Hello, how are you?")
    console.print("[blue]Assistant:[/blue]", end=" ")

    conversation_id = None
    full_response = ""

    async for event in stream_chat("Hello, how are you?"):
        if event.get("_event_type") == "session.start":
            conversation_id = event["conversation_id"]

        if event.get("_event_type") == "message.delta":
            delta = event["data"]["delta"]
            full_response += delta
            console.print(delta, end="", flush=True)

        if event.get("_event_type") == "message.end":
            console.print()

    # Second message (multi-turn)
    console.print("\n[green]User:[/green] Can you search for Python tutorials?")
    console.print("[blue]Assistant:[/blue]", end=" ")

    full_response = ""

    async for event in stream_chat(
        "Can you search for Python tutorials?",
        conversation_id=conversation_id,
        enable_tools=True
    ):
        event_type = event.get("_event_type")

        if event_type == "tool.call.start":
            console.print(f"\n[cyan]🔧 {event['data']['tool_name']}[/cyan]")

        if event_type == "message.delta":
            delta = event["data"]["delta"]
            full_response += delta
            console.print(delta, end="", flush=True)

        if event_type == "message.end":
            console.print()


async def event_inspector():
    """
    Detailed event inspector that shows all event data
    """

    console.print("[bold]Event Inspector[/bold]\n")
    console.print("Sending message with tools and thinking enabled...\n")

    async for event in stream_chat(
        "Search for the weather and calculate 2+2",
        enable_tools=True,
        enable_thinking=True
    ):
        # Pretty print each event
        event_type = event.get("_event_type", "unknown")

        table = Table(show_header=False, box=None)
        table.add_column("Key", style="cyan")
        table.add_column("Value", style="white")

        table.add_row("Event Type", f"[bold]{event_type}[/bold]")
        table.add_row("Sequence", str(event.get("sequence", "N/A")))
        table.add_row("Timestamp", event.get("timestamp", "N/A"))

        if "data" in event:
            for key, value in event["data"].items():
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, indent=2)
                table.add_row(f"  {key}", str(value))

        console.print(Panel(table, title=f"Event {event.get('sequence', 0)}", border_style="blue"))


async def get_server_info():
    """Get server information"""

    async with httpx.AsyncClient() as client:
        response = await client.get(f"{SERVER_URL}/")
        info = response.json()

        console.print(Panel.fit(
            f"[bold]{info['service']}[/bold]\n"
            f"Version: {info['version']}\n"
            f"Port: {info['port']}\n"
            f"Status: [green]{info['status']}[/green]",
            title="Server Info",
            border_style="cyan"
        ))

        console.print("\n[bold]Features:[/bold]")
        for feature in info['features']:
            console.print(f"  • {feature}")

        console.print("\n[bold]Event Types:[/bold]")
        for event_type in info['event_types']:
            console.print(f"  • {event_type}")


async def list_conversations():
    """List all conversations"""

    async with httpx.AsyncClient() as client:
        response = await client.get(f"{SERVER_URL}/conversations")
        data = response.json()

        if data['total'] == 0:
            console.print("[yellow]No active conversations[/yellow]")
            return

        table = Table(title="Active Conversations")
        table.add_column("ID", style="cyan")
        table.add_column("Created", style="green")
        table.add_column("Messages", style="yellow")
        table.add_column("Last Activity", style="blue")

        for conv in data['conversations']:
            table.add_row(
                conv['id'][:12] + "...",
                conv.get('created_at', 'N/A')[:19],
                str(conv['message_count']),
                conv.get('last_activity', 'N/A')[:19] if conv.get('last_activity') else 'N/A'
            )

        console.print(table)


async def main():
    """Main entry point with menu"""

    try:
        await get_server_info()
    except httpx.ConnectError:
        console.print("[red]Error: Cannot connect to server at {SERVER_URL}[/red]")
        console.print("[yellow]Make sure the server is running: python streaming_chat_server.py[/yellow]")
        return

    console.print("\n[bold]Select mode:[/bold]")
    console.print("1. Interactive Chat (default)")
    console.print("2. Simple Example")
    console.print("3. Event Inspector")
    console.print("4. List Conversations")

    choice = input("\nChoice (1-4, default=1): ").strip() or "1"

    if choice == "1":
        await interactive_chat()
    elif choice == "2":
        await simple_chat_example()
    elif choice == "3":
        await event_inspector()
    elif choice == "4":
        await list_conversations()
    else:
        console.print("[red]Invalid choice[/red]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Exited[/yellow]")
