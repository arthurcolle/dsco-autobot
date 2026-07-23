#!/usr/bin/env python3
"""
Test client for the Agent Server /v1/interactions endpoint

This demonstrates how to use the agent server for both simple and complex interactions.
"""

import asyncio
import json
import sys
import httpx
from typing import AsyncGenerator


async def stream_interaction(
    message: str,
    session_id: str = None,
    server_url: str = "http://localhost:2046"
) -> AsyncGenerator[dict, None]:
    """Stream an interaction from the agent server"""

    async with httpx.AsyncClient() as client:
        request_data = {
            "message": message,
            "enable_tools": True,
            "enable_context_compression": True,
            "enable_parallel_tools": True
        }

        if session_id:
            request_data["session_id"] = session_id

        async with client.stream(
            "POST",
            f"{server_url}/v1/interactions",
            json=request_data,
            headers={"Accept": "text/event-stream"},
            timeout=300.0
        ) as response:
            if response.status_code != 200:
                error_text = await response.aread()
                raise Exception(f"Error {response.status_code}: {error_text.decode()}")

            async for line in response.aiter_lines():
                if not line or line.startswith(":"):
                    continue

                if line.startswith("event: "):
                    event_type = line[7:]
                    continue

                if line.startswith("data: "):
                    data = line[6:]
                    try:
                        event_data = json.loads(data)
                        yield event_data
                    except json.JSONDecodeError:
                        continue


async def simple_interaction_example():
    """Example: Simple text interaction"""
    print("=" * 80)
    print("SIMPLE INTERACTION: 'say ok'")
    print("=" * 80)
    print()

    full_text = ""

    async for event in stream_interaction("say ok"):
        event_type = event.get("event")
        data = event.get("data", {})

        if event_type == "session.start":
            print(f"📍 Session: {event.get('session_id')}")
            print()

        elif event_type == "message.chunk":
            chunk = data.get("chunk", "")
            print(chunk, end="", flush=True)
            full_text += chunk

        elif event_type == "message.complete":
            print()
            print()
            print(f"✅ Complete (iteration {data.get('iteration')})")

        elif event_type == "interaction.complete":
            print(f"🎯 Interaction done in {data.get('iterations')} iterations")
            print()

    return full_text


async def complex_interaction_example():
    """Example: Complex multi-tool interaction"""
    print("=" * 80)
    print("COMPLEX INTERACTION: 'search for recent AI papers and summarize the top 3'")
    print("=" * 80)
    print()

    message = "search for recent AI papers on arxiv and summarize the top 3 results"
    full_text = ""
    tool_calls = []

    async for event in stream_interaction(message):
        event_type = event.get("event")
        data = event.get("data", {})

        if event_type == "session.start":
            session_id = event.get("session_id")
            print(f"📍 Session: {session_id}")
            print()

        elif event_type == "interaction.start":
            print(f"🚀 Starting interaction...")
            print(f"   Tools available: {data.get('tools_available', 0)}")
            print()

        elif event_type == "message.start":
            iteration = data.get("iteration")
            print(f"\n📝 Iteration {iteration}:")

        elif event_type == "message.chunk":
            chunk = data.get("chunk", "")
            print(chunk, end="", flush=True)
            full_text += chunk

        elif event_type == "message.complete":
            has_tools = data.get("has_tool_calls", False)
            if has_tools:
                print("\n   ⚙️  Tool calls detected...")
            else:
                print("\n   ✅ Message complete")

        elif event_type == "tool.plan":
            tools = data.get("tools", [])
            print(f"\n🔧 Tool Plan ({data.get('count')} tools):")
            for tool in tools:
                print(f"   - {tool.get('name')} (id: {tool.get('id')})")

        elif event_type == "tool.execute.start":
            tool_name = data.get("tool_name")
            print(f"\n   ▶️  Executing: {tool_name}")
            tool_calls.append(tool_name)

        elif event_type == "tool.execute.complete":
            result_preview = data.get("result_preview", "")
            result_length = data.get("result_length", 0)
            print(f"   ✅ Result ({result_length} chars): {result_preview[:100]}...")

        elif event_type == "tool.execute.error":
            error = data.get("error")
            print(f"   ❌ Error: {error}")

        elif event_type == "context.compress":
            print(f"\n📦 Compressing context...")

        elif event_type == "usage":
            usage = data.get("iteration_usage", {})
            session_total = data.get("session_total", 0)
            print(f"\n📊 Usage:")
            print(f"   Iteration: {usage.get('total_tokens', 0)} tokens")
            print(f"   Session total: {session_total} tokens")

        elif event_type == "interaction.complete":
            iterations = data.get("iterations")
            total_events = data.get("total_events")
            print(f"\n🎯 Interaction Complete!")
            print(f"   Iterations: {iterations}")
            print(f"   Total events: {total_events}")
            print(f"   Tools executed: {len(tool_calls)}")
            print()

    return full_text, tool_calls


async def session_continuity_example():
    """Example: Multi-turn conversation with session continuity"""
    print("=" * 80)
    print("SESSION CONTINUITY: Multi-turn conversation")
    print("=" * 80)
    print()

    session_id = None

    # First interaction
    print("Turn 1: 'What's the weather like?'")
    print("-" * 40)

    async for event in stream_interaction("What's the weather like in San Francisco?"):
        event_type = event.get("event")

        if event_type == "session.start":
            session_id = event.get("session_id")
            print(f"📍 Session created: {session_id}")

        elif event_type == "message.chunk":
            print(event.get("data", {}).get("chunk", ""), end="", flush=True)

        elif event_type == "interaction.complete":
            print("\n")
            break

    # Second interaction in same session
    if session_id:
        print()
        print("Turn 2: 'How about tomorrow?' (using same session)")
        print("-" * 40)

        async for event in stream_interaction(
            "How about tomorrow?",
            session_id=session_id
        ):
            event_type = event.get("event")

            if event_type == "session.start":
                is_new = event.get("data", {}).get("session_new", True)
                print(f"📍 Session: {event.get('session_id')} (new: {is_new})")

            elif event_type == "message.chunk":
                print(event.get("data", {}).get("chunk", ""), end="", flush=True)

            elif event_type == "interaction.complete":
                print("\n")
                break


async def main():
    """Run all examples"""

    examples = [
        ("Simple", simple_interaction_example),
        ("Complex", complex_interaction_example),
        ("Session Continuity", session_continuity_example)
    ]

    # Check server health
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:2046/health")
            if response.status_code == 200:
                health = response.json()
                print("Server Status:")
                print(f"  Status: {health.get('status')}")
                print(f"  MCP Connected: {health.get('mcp_connected')}")
                print(f"  Tools Available: {health.get('tools_available')}")
                print()
            else:
                print(f"WARNING: Server returned {response.status_code}")
                print()
    except Exception as e:
        print(f"ERROR: Could not connect to server: {e}")
        print("Make sure the server is running: python agent_server.py")
        sys.exit(1)

    # Run examples
    for name, example_fn in examples:
        try:
            await example_fn()
            print()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            break
        except Exception as e:
            print(f"\n❌ Error in {name} example: {e}")
            import traceback
            traceback.print_exc()
            print()

        # Wait between examples
        if name != examples[-1][0]:
            await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nExiting...")
