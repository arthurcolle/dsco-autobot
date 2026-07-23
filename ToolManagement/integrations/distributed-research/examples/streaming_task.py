"""Example: Task with streaming events.

NOTE: For most AI agents and MCP clients, use the managed endpoint instead:
  POST /v1/tasks/runs/managed

This example shows how to manually handle event streaming, which is useful when:
- Building custom UIs that display real-time progress
- You need fine-grained control over event handling
- Your application has specific requirements for streaming

For a simpler approach that handles polling automatically, see managed_task.py
"""

import asyncio
import json
import httpx

from parallel_websearch_server.config import settings


async def main():
    """Run streaming task example."""
    base_url = f"http://{settings.host}:{settings.port}"
    print(f"Connecting to {base_url}")

    async with httpx.AsyncClient(base_url=base_url, timeout=300) as client:
        # Create task run
        print("Creating task run...")
        response = await client.post(
            "/v1/tasks/runs",
            json={
                "processor": "lite",
                "input": "Analyze the current state of fusion energy research",
                "task_spec": {
                    "output_schema": {
                        "type": "json",
                        "properties": {
                            "summary": {"type": "string"},
                            "key_players": {"type": "array"},
                            "breakthroughs": {"type": "array"}
                        }
                    }
                }
            },
            headers={"parallel-beta": "search-extract-2025-10-10"}
        )

        run_data = response.json()
        run_id = run_data["run_id"]
        print(f"Task run created: {run_id}")
        print(f"Status: {run_data['status']}")

        # Stream events
        print("\nStreaming events:")
        print("=" * 80)

        async with client.stream(
            "GET",
            f"/v1/tasks/runs/{run_id}/events"
        ) as stream_response:
            async for line in stream_response.aiter_lines():
                if line.startswith("data:"):
                    try:
                        event_data = json.loads(line[5:].strip())
                        event_type = event_data.get("type", "unknown")
                        print(f"\n[{event_type}]")

                        if event_type == "progress_message":
                            print(f"  Message: {event_data.get('data', {}).get('message', '')}")
                        elif event_type == "progress_stats":
                            stats = event_data.get("data", {})
                            print(f"  Sources: {stats.get('total_sources', 0)}")
                            print(f"  Domains: {stats.get('unique_domains', 0)}")
                        elif event_type == "completed":
                            print("  Task completed!")
                            break

                    except json.JSONDecodeError:
                        pass


if __name__ == "__main__":
    asyncio.run(main())
