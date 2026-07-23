"""Example: Managed task run with automatic polling.

This is the RECOMMENDED approach for AI agents and MCP clients.
The managed endpoint handles all polling automatically and returns
results inline without requiring users to visit external URLs.
"""

import asyncio
import json
import httpx

from parallel_websearch_server.config import settings


async def main():
    """Run managed task example."""
    base_url = f"http://{settings.host}:{settings.port}"
    print(f"Connecting to {base_url}")

    async with httpx.AsyncClient(base_url=base_url, timeout=700) as client:
        print("Creating managed task run...")
        print("The server will automatically poll and return results when complete.\n")

        try:
            response = await client.post(
                "/v1/tasks/runs/managed",
                params={
                    "poll_interval_seconds": 2.0,  # Check status every 2 seconds
                    "timeout_seconds": 600,  # Wait up to 10 minutes
                    "context_id": "demo-research-session"  # Optional: persist to context
                },
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

            response.raise_for_status()
            result = response.json()

            # Display results
            print("=" * 80)
            print("TASK COMPLETED")
            print("=" * 80)
            print(f"Run ID: {result['run_id']}")
            print(f"Final Status: {result['final_status']}")
            print(f"Elapsed Time: {result['elapsed_seconds']:.2f} seconds")
            print(f"Poll Count: {result['poll_count']}")
            print()

            # Show status progression
            print("Status History:")
            for update in result['status_updates']:
                print(f"  {update['timestamp']} - {update['status']}")
            print()

            # Show results
            if result['final_status'] == 'completed':
                print("Summary:")
                print(result['summary'])
                print()

                if result['output_text']:
                    print("Text Output:")
                    print(result['output_text'])
                elif result['output_json']:
                    print("JSON Output:")
                    print(json.dumps(result['output_json'], indent=2))
            else:
                print(f"Task failed or was cancelled: {result.get('error', 'Unknown error')}")

            print("=" * 80)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 504:
                print("Task timed out. The task is still running but exceeded the timeout.")
                print("You can check status later using the run_id.")
            else:
                print(f"HTTP error: {e.response.status_code}")
                print(e.response.json())
        except httpx.TimeoutException:
            print("Request timed out. Consider increasing timeout_seconds parameter.")


async def run_multiple_tasks():
    """Example: Running multiple managed tasks concurrently."""
    base_url = f"http://{settings.host}:{settings.port}"
    print(f"Running multiple managed tasks concurrently...\n")

    async with httpx.AsyncClient(base_url=base_url, timeout=700) as client:
        tasks = [
            {
                "name": "AI Research",
                "input": "Latest developments in large language models",
                "processor": "lite"
            },
            {
                "name": "Climate Tech",
                "input": "Carbon capture technology companies",
                "processor": "lite"
            },
            {
                "name": "Quantum Computing",
                "input": "Quantum computing hardware advancements",
                "processor": "lite"
            }
        ]

        # Launch all tasks concurrently
        async def run_task(task_config):
            response = await client.post(
                "/v1/tasks/runs/managed",
                params={
                    "poll_interval_seconds": 2.0,
                    "timeout_seconds": 600
                },
                json={
                    "processor": task_config["processor"],
                    "input": task_config["input"],
                    "task_spec": {
                        "output_schema": {"type": "text"}
                    }
                },
                headers={"parallel-beta": "search-extract-2025-10-10"}
            )
            result = response.json()
            return task_config["name"], result

        # Run all tasks concurrently
        results = await asyncio.gather(
            *[run_task(task) for task in tasks],
            return_exceptions=True
        )

        # Display results
        print("=" * 80)
        print("ALL TASKS COMPLETED")
        print("=" * 80)
        for name, result in results:
            if isinstance(result, Exception):
                print(f"\n{name}: FAILED - {result}")
            else:
                print(f"\n{name}:")
                print(f"  Status: {result['final_status']}")
                print(f"  Time: {result['elapsed_seconds']:.2f}s")
                print(f"  Summary: {result['summary'][:100]}...")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "multi":
        asyncio.run(run_multiple_tasks())
    else:
        asyncio.run(main())
        print("\nTip: Run 'python examples/managed_task.py multi' to see concurrent task execution")
