#!/usr/bin/env python3
"""
Test client for the Powerful MCP FastAPI /v1/interactions endpoint

This demonstrates how to use the API with SSE streaming.

Usage:
    python test_powerful_api.py "What is the weather in San Francisco?"
"""

import asyncio
import json
import sys
from typing import AsyncGenerator
import httpx


BASE_URL = "http://localhost:2046"


async def stream_interaction(
    message: str,
    session_id: str = None,
    model: str = "anthropic/claude-haiku-4.5",
    enable_tools: bool = True
) -> None:
    """Stream an interaction and print events"""

    payload = {
        "message": message,
        "model": model,
        "enable_tools": enable_tools,
        "enable_streaming": True,
        "temperature": 0.7,
        "max_tokens": 4000
    }

    if session_id:
        payload["session_id"] = session_id

    print(f"\n{'='*80}")
    print(f"🚀 Starting interaction")
    print(f"Message: {message}")
    print(f"Model: {model}")
    print(f"Tools: {'Enabled' if enable_tools else 'Disabled'}")
    print(f"{'='*80}\n")

    async with httpx.AsyncClient(timeout=300.0) as client:
        async with client.stream(
            "POST",
            f"{BASE_URL}/v1/interactions",
            json=payload,
            headers={"Accept": "text/event-stream"}
        ) as response:
            if response.status_code != 200:
                error = await response.aread()
                print(f"❌ Error: {response.status_code}")
                print(error.decode())
                return

            # Get session info from headers
            session_id = response.headers.get("X-Session-ID")
            interaction_id = response.headers.get("X-Interaction-ID")
            is_new = response.headers.get("X-Session-New") == "true"

            print(f"📋 Session ID: {session_id}")
            print(f"📋 Interaction ID: {interaction_id}")
            print(f"📋 New session: {is_new}")
            print()

            current_message = ""
            current_iteration = 0

            async for line in response.aiter_lines():
                line = line.strip()

                if not line:
                    continue

                if line.startswith("event:"):
                    event_type = line[6:].strip()
                    continue

                if line.startswith("data:"):
                    data_str = line[5:].strip()

                    try:
                        event_data = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    event = event_data.get("event")
                    data = event_data.get("data", {})
                    sequence = event_data.get("sequence")

                    # Handle different event types
                    if event == "interaction.started":
                        print(f"🎬 Interaction started")
                        print(f"   Tools available: {data.get('tools_available', 0)}")
                        print()

                    elif event == "message.start":
                        current_iteration = data.get("iteration", 0)
                        print(f"💬 Message iteration {current_iteration} started")

                    elif event == "message.delta":
                        delta = data.get("delta", "")
                        current_message += delta
                        print(delta, end="", flush=True)

                    elif event == "message.completed":
                        print()
                        print()
                        usage = data.get("usage", {})
                        if usage:
                            print(f"📊 Tokens: {usage.get('total_tokens', 0)} total "
                                  f"({usage.get('prompt_tokens', 0)} prompt, "
                                  f"{usage.get('completion_tokens', 0)} completion)")
                        print()

                    elif event == "tool.call.initiated":
                        tool_name = data.get("tool_name")
                        print(f"🔧 Tool call: {tool_name}")

                    elif event == "tool.call.executing":
                        mcp_tool = data.get("mcp_tool_name")
                        print(f"   ⚙️  Executing: {mcp_tool}")

                    elif event == "tool.call.completed":
                        result_len = data.get("result_length", 0)
                        print(f"   ✅ Completed ({result_len} chars)")

                    elif event == "tool.call.failed":
                        error = data.get("error")
                        print(f"   ❌ Failed: {error}")

                    elif event == "usage.report":
                        cumulative = data.get("cumulative", {})
                        if cumulative:
                            print(f"📈 Cumulative usage: {cumulative.get('total', 0)} tokens")

                    elif event == "iteration.summary":
                        iter_num = data.get("iteration")
                        tool_count = data.get("tool_calls", 0)
                        print(f"🔄 Iteration {iter_num} complete: {tool_count} tool calls")
                        print()

                    elif event == "interaction.completed":
                        iterations = data.get("iterations")
                        print(f"✅ Interaction completed in {iterations} iterations")
                        print()

                    elif event == "error":
                        error_msg = data.get("message")
                        print(f"❌ Error: {error_msg}")
                        print()

    print(f"{'='*80}")
    print("Done!")
    print(f"{'='*80}\n")


async def get_api_status():
    """Get API status"""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/status")
        if response.status_code == 200:
            status = response.json()
            print("\n📊 API Status:")
            print(json.dumps(status, indent=2))
            print()
        else:
            print(f"❌ Error getting status: {response.status_code}")


async def list_sessions():
    """List active sessions"""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/v1/sessions")
        if response.status_code == 200:
            sessions = response.json()
            print(f"\n💬 Active sessions: {len(sessions)}")
            for session in sessions:
                print(f"   • {session['session_id']} - {session['message_count']} messages")
            print()
        else:
            print(f"❌ Error listing sessions: {response.status_code}")


async def list_tools():
    """List available tools"""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/v1/tools")
        if response.status_code == 200:
            tools_data = response.json()
            tools = tools_data.get("tools", [])
            print(f"\n🔧 Available tools: {len(tools)}")
            for tool in tools:
                print(f"   • {tool['name']}: {tool.get('description', 'No description')[:80]}")
            print()
        else:
            print(f"❌ Error listing tools: {response.status_code}")


async def interactive_mode():
    """Interactive chat mode"""
    print("\n" + "="*80)
    print("🤖 INTERACTIVE CHAT MODE")
    print("="*80)
    print("\nCommands:")
    print("  /status - Show API status")
    print("  /sessions - List active sessions")
    print("  /tools - List available tools")
    print("  /new - Start new session")
    print("  /exit - Exit")
    print("\nType your message and press Enter to chat.\n")

    session_id = None

    while True:
        try:
            user_input = input("\n👤 You: ").strip()

            if not user_input:
                continue

            if user_input == "/exit":
                print("👋 Goodbye!")
                break

            elif user_input == "/status":
                await get_api_status()
                continue

            elif user_input == "/sessions":
                await list_sessions()
                continue

            elif user_input == "/tools":
                await list_tools()
                continue

            elif user_input == "/new":
                session_id = None
                print("✨ Starting new session")
                continue

            # Send message
            print("\n🤖 Assistant:")
            await stream_interaction(
                message=user_input,
                session_id=session_id,
                model="anthropic/claude-haiku-4.5",
                enable_tools=True
            )

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


async def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        # No arguments - run interactive mode
        await interactive_mode()
    else:
        command = sys.argv[1]

        if command == "--status":
            await get_api_status()
        elif command == "--sessions":
            await list_sessions()
        elif command == "--tools":
            await list_tools()
        elif command == "--interactive" or command == "-i":
            await interactive_mode()
        else:
            # Treat as a message
            message = " ".join(sys.argv[1:])
            await stream_interaction(message)


if __name__ == "__main__":
    asyncio.run(main())
