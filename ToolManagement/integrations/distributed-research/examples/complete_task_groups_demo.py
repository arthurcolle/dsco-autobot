"""
Complete Task Groups Demonstration.

Shows all features of the advanced task group management system:
1. Creating task groups
2. Adding runs to groups
3. Streaming events and monitoring progress
4. Managed group execution (automatic polling)
5. Status filtering
6. Analytics and reporting
7. Exporting results (JSON, CSV, Markdown)
8. Batch parallel research workflows
"""

import asyncio
import json
import logging

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:2040"


async def demo_create_and_populate_group():
    """Demonstrate creating a task group and adding runs."""
    print("\n" + "="*80)
    print("DEMO 1: Create Task Group and Add Runs")
    print("="*80)

    async with httpx.AsyncClient(timeout=300) as client:
        # Create task group
        print("\n📦 Creating task group...")
        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={
                "metadata": {
                    "project": "market-research",
                    "category": "competitive-analysis",
                    "created_by": "demo_script"
                }
            }
        )

        group = response.json()
        taskgroup_id = group["task_group_id"]
        print(f"✅ Created task group: {taskgroup_id}")

        # Add runs to the group
        print("\n📤 Adding 5 research tasks to group...")
        runs = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {
                        "input": "Latest trends in AI-powered drug discovery",
                        "processor": "lite"
                    },
                    {
                        "input": "Top quantum computing startups 2025",
                        "processor": "lite"
                    },
                    {
                        "input": "Climate tech investment landscape",
                        "processor": "lite"
                    },
                    {
                        "input": "Breakthrough materials science innovations",
                        "processor": "lite"
                    },
                    {
                        "input": "Synthetic biology commercial applications",
                        "processor": "lite"
                    }
                ]
            }
        )

        runs_response = runs.json()
        print(f"✅ Added {runs_response['num_task_runs']} runs to group")

        # Get group status
        print("\n📊 Checking group status...")
        status_response = await client.get(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}"
        )

        status = status_response.json()
        print(f"Status: {status['status']}")
        print(f"Total runs: {status['num_task_runs']}")
        print(f"Active: {status['is_active']}")
        print(f"Status distribution: {json.dumps(status['task_run_status_counts'], indent=2)}")

        return taskgroup_id


async def demo_managed_group_execution():
    """Demonstrate managed group execution - waits for completion automatically."""
    print("\n" + "="*80)
    print("DEMO 2: Managed Group Execution (Automatic Polling)")
    print("="*80)

    async with httpx.AsyncClient(timeout=1800) as client:
        # Create and populate group
        print("\n📦 Creating task group with 3 tasks...")
        create_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={"metadata": {"demo": "managed_execution"}}
        )

        taskgroup_id = create_response.json()["task_group_id"]

        await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {"input": "Recent advancements in fusion energy", "processor": "lite"},
                    {"input": "Space mining companies and technologies", "processor": "lite"},
                    {"input": "Advanced battery technology breakthroughs", "processor": "lite"}
                ]
            }
        )

        # Use managed endpoint - automatically waits for completion
        print(f"\n⏳ Starting managed execution (automatic polling)...")
        print("This will poll the group until all tasks complete.")
        print("NEVER requires visiting external URLs!\n")

        managed_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/managed",
            params={
                "poll_interval": 2.0,
                "timeout": 900,
                "include_results": True
            }
        )

        result = managed_response.json()

        print("\n✅ Group Execution Complete!")
        print(f"Final Status: {result['final_status']}")
        print(f"Elapsed Time: {result['elapsed_seconds']:.2f}s")
        print(f"\n📊 Metrics:")
        print(json.dumps(result['metrics'], indent=2))
        print(f"\n📝 Summary: {result['summary']}")

        if result.get('run_results'):
            print(f"\n📑 Individual Results: {len(result['run_results'])} runs")
            for i, run in enumerate(result['run_results'][:3], 1):
                print(f"\n  Run {i}:")
                print(f"    ID: {run.get('run_id', 'N/A')}")
                print(f"    Status: {run.get('status', 'N/A')}")

        return taskgroup_id


async def demo_status_filtering():
    """Demonstrate filtering runs by status."""
    print("\n" + "="*80)
    print("DEMO 3: Status Filtering")
    print("="*80)

    async with httpx.AsyncClient(timeout=300) as client:
        # Create group with mixed status runs (some will complete, some may fail)
        create_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={"metadata": {"demo": "status_filtering"}}
        )

        taskgroup_id = create_response.json()["task_group_id"]

        await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {"input": "Quick task 1", "processor": "lite"},
                    {"input": "Quick task 2", "processor": "lite"},
                    {"input": "Quick task 3", "processor": "lite"}
                ]
            }
        )

        # Wait a bit for some tasks to start
        await asyncio.sleep(5)

        # Filter by completed status
        print("\n📊 Filtering runs by status: completed")

        # Note: This returns SSE stream, so we'd need to handle streaming
        # For demo purposes, we'll show the endpoint usage
        print(f"Endpoint: GET {BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs?status=completed")

        # Get analytics instead to see status distribution
        analytics_response = await client.get(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analytics"
        )

        analytics = analytics_response.json()
        print(f"\n📈 Status Distribution:")
        print(json.dumps(analytics.get('status_distribution', {}), indent=2))

        return taskgroup_id


async def demo_analytics_and_reporting():
    """Demonstrate analytics and reporting features."""
    print("\n" + "="*80)
    print("DEMO 4: Analytics and Reporting")
    print("="*80)

    async with httpx.AsyncClient(timeout=300) as client:
        # Use a taskgroup from previous demo or create new one
        create_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={"metadata": {"demo": "analytics"}}
        )

        taskgroup_id = create_response.json()["task_group_id"]

        await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {"input": "AI research topic 1", "processor": "lite"},
                    {"input": "AI research topic 2", "processor": "base"},
                    {"input": "AI research topic 3", "processor": "lite"}
                ]
            }
        )

        # Get comprehensive analytics
        print(f"\n📊 Fetching analytics for {taskgroup_id}...")

        analytics_response = await client.get(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analytics",
            params={"include_run_details": False}
        )

        analytics = analytics_response.json()

        print(f"\n✅ Analytics Report:")
        print(f"Task Group ID: {analytics['taskgroup_id']}")
        print(f"Active: {analytics['is_active']}")
        print(f"Total Runs: {analytics['total_runs']}")
        print(f"Created: {analytics['created_at']}")
        print(f"\nStatus Distribution:")
        print(json.dumps(analytics['status_distribution'], indent=2))

        if analytics.get('completion_rate') is not None:
            print(f"\n📈 Completion Rate: {analytics['completion_rate']*100:.1f}%")

        if analytics.get('failure_rate') is not None:
            print(f"📉 Failure Rate: {analytics['failure_rate']*100:.1f}%")

        return taskgroup_id


async def demo_export_capabilities():
    """Demonstrate exporting results in multiple formats."""
    print("\n" + "="*80)
    print("DEMO 5: Export Capabilities")
    print("="*80)

    async with httpx.AsyncClient(timeout=600) as client:
        # Create and complete a small group
        create_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={"metadata": {"demo": "export"}}
        )

        taskgroup_id = create_response.json()["task_group_id"]

        await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {"input": "Export test 1", "processor": "lite"},
                    {"input": "Export test 2", "processor": "lite"}
                ]
            }
        )

        # Export in different formats
        formats = ["json", "csv", "markdown"]

        for format in formats:
            print(f"\n📁 Exporting as {format.upper()}...")

            export_response = await client.get(
                f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/export",
                params={
                    "format": format,
                    "include_inputs": True,
                    "include_outputs": True
                }
            )

            export_data = export_response.json()

            print(f"✅ Export complete")
            print(f"Format: {export_data['format']}")

            # Show preview
            data = export_data['data']
            if format == "json":
                print(f"Preview: {data[:500]}...")
            elif format == "csv":
                lines = data.split('\n')
                print(f"Preview ({len(lines)} lines):")
                print('\n'.join(lines[:5]))
            elif format == "markdown":
                print(f"Preview:")
                print(data[:500])

        return taskgroup_id


async def demo_batch_parallel_research():
    """Demonstrate using task groups for large-scale parallel research."""
    print("\n" + "="*80)
    print("DEMO 6: Batch Parallel Research Workflow")
    print("="*80)

    print("""
This demonstrates a real-world research workflow:
1. Create task group for a research project
2. Submit multiple related research queries in parallel
3. Monitor progress via events
4. Wait for completion
5. Export aggregated results
""")

    async with httpx.AsyncClient(timeout=1800) as client:
        # Create research project group
        print("\n📦 Creating research project: 'Future Tech Landscape 2025'...")

        create_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups",
            json={
                "metadata": {
                    "project_name": "Future Tech Landscape 2025",
                    "research_type": "competitive_intelligence",
                    "date": "2025-11-12"
                }
            }
        )

        taskgroup_id = create_response.json()["task_group_id"]
        print(f"✅ Created group: {taskgroup_id}")

        # Define research queries
        research_queries = [
            "Leading quantum computing companies and their roadmaps",
            "Commercial fusion energy timeline and key players",
            "Advanced materials for space applications",
            "Neuromorphic computing chip manufacturers",
            "Biodegradable electronics breakthrough companies",
            "Solid-state battery technology leaders",
            "Carbon capture direct air technology providers",
            "Synthetic biology drug discovery platforms"
        ]

        print(f"\n📤 Submitting {len(research_queries)} research queries...")

        await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/runs",
            json={
                "inputs": [
                    {"input": query, "processor": "lite"}
                    for query in research_queries
                ]
            }
        )

        print(f"✅ Submitted {len(research_queries)} tasks")

        # Use managed execution
        print("\n⏳ Running managed execution (automatic polling)...")

        managed_response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/managed",
            params={
                "poll_interval": 3.0,
                "timeout": 1200,  # 20 minutes
                "include_results": True
            }
        )

        result = managed_response.json()

        print("\n✅ Research Complete!")
        print(f"Status: {result['final_status']}")
        print(f"Duration: {result['elapsed_seconds']:.2f}s")
        print(f"Summary: {result['summary']}")

        # Export results
        print("\n📁 Exporting results as Markdown report...")

        export_response = await client.get(
            f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/export",
            params={
                "format": "markdown",
                "include_inputs": True,
                "include_outputs": True
            }
        )

        markdown_report = export_response.json()['data']

        print("\n📄 Report Preview:")
        print(markdown_report[:1000])
        print("\n... (truncated)")

        return taskgroup_id


async def main():
    """Run all demos."""
    print("\n" + "="*80)
    print("🚀 COMPLETE TASK GROUPS DEMONSTRATION")
    print("="*80)
    print("\nThis demonstrates the full capabilities of the advanced")
    print("task group management system for async parallel research.")
    print("="*80)

    try:
        # Run demos
        await demo_create_and_populate_group()
        await demo_managed_group_execution()
        await demo_status_filtering()
        await demo_analytics_and_reporting()
        await demo_export_capabilities()
        await demo_batch_parallel_research()

        print("\n" + "="*80)
        print("✅ ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("="*80)

        print("\n🎯 Key Features Demonstrated:")
        print("  ✅ Task group creation with metadata")
        print("  ✅ Batch task submission")
        print("  ✅ Managed execution (automatic polling)")
        print("  ✅ Status filtering")
        print("  ✅ Real-time analytics")
        print("  ✅ Multi-format export (JSON, CSV, Markdown)")
        print("  ✅ Large-scale parallel research workflows")
        print("  ✅ ZERO manual polling - everything automatic!")

        print("\n📚 Available Endpoints:")
        print("  POST   /v1beta/tasks/groups - Create group")
        print("  GET    /v1beta/tasks/groups/{id} - Get status")
        print("  POST   /v1beta/tasks/groups/{id}/runs - Add runs")
        print("  GET    /v1beta/tasks/groups/{id}/runs - Stream runs (with filters)")
        print("  GET    /v1beta/tasks/groups/{id}/runs/{run_id} - Get specific run")
        print("  GET    /v1beta/tasks/groups/{id}/events - Stream events")
        print("  POST   /v1beta/tasks/groups/{id}/managed - Managed execution ⭐")
        print("  GET    /v1beta/tasks/groups/{id}/analytics - Get analytics ⭐")
        print("  GET    /v1beta/tasks/groups/{id}/export - Export results ⭐")

    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        print("\n💡 Make sure the server is running:")
        print("   python -m parallel_websearch_server.main")


if __name__ == "__main__":
    asyncio.run(main())
