"""
🔥 BASED Research Workflow 🔥

This is not your average research automation.
This is a NUCLEAR-POWERED, AI-OPTIMIZED, COST-MINIMIZING,
AUTO-RECOVERING, PARALLEL-EXECUTING research MONSTER.

Features demonstrated:
1. Automatic task complexity analysis
2. Intelligent processor selection
3. Cost optimization strategies
4. Task deduplication
5. Automatic failure retry with exponential backoff
6. Real-time progress tracking
7. Beautiful CLI visualization
8. Export in multiple formats

This script absolutely DOMINATES large-scale research workflows.
"""

import asyncio
import json
import sys
import time
from typing import List

import httpx

BASE_URL = "http://localhost:2040"


def print_header(text: str):
    """Print a sick header."""
    print("\n" + "=" * 80)
    print(f"🔥 {text}")
    print("=" * 80)


def print_success(text: str):
    """Print success message."""
    print(f"✅ {text}")


def print_info(text: str):
    """Print info message."""
    print(f"📊 {text}")


def print_fire(text: str):
    """Print fire message."""
    print(f"🔥 {text}")


async def demo_basic_optimization():
    """Demonstrate basic BASED optimization."""
    print_header("DEMO 1: BASED Optimization - Balanced Strategy")

    research_queries = [
        "Latest breakthroughs in quantum error correction",
        "Top fusion energy companies and their roadmaps",
        "Advanced materials for space propulsion",
        "Neuromorphic computing chip developments",
        "Synthetic biology drug discovery platforms"
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        print_info(f"Submitting {len(research_queries)} research queries with BALANCED optimization")

        # THE MAGIC: Just send queries, optimization happens automatically
        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "balanced",
                "deduplicate": True,
                "auto_retry": True
            },
            json=research_queries
        )

        result = response.json()

        print_success("Optimization Complete!")

        # Show the plan
        plan = result["optimization_plan"]
        print(f"\n📈 Optimization Plan:")
        print(f"   Strategy: {plan['strategy']}")
        print(f"   Total Tasks: {plan['total_tasks']}")
        print(f"   Estimated Cost: ${plan['estimated_cost']:.4f}")
        print(f"   Estimated Duration: ~{plan['estimated_duration']}s")

        print(f"\n🎯 Processor Distribution:")
        for processor, count in plan["processor_distribution"].items():
            print(f"   {processor}: {count} tasks")

        dedup = result["deduplication"]
        print(f"\n🔍 Deduplication:")
        print(f"   Original: {dedup['original_count']} tasks")
        print(f"   Unique: {dedup['unique_count']} tasks")
        print(f"   Duplicates Removed: {dedup['duplicates_removed']}")

        print(f"\n⚡ Task Group ID: {result['taskgroup_id']}")

        return result["taskgroup_id"]


async def demo_cost_optimization():
    """Demonstrate aggressive cost optimization."""
    print_header("DEMO 2: Cost Optimization - Minimum Spend")

    # Same queries but optimized for COST
    research_queries = [
        "Quick lookup: Tesla stock price trend",
        "Quick lookup: SpaceX launch schedule",
        "Quick lookup: Bitcoin market cap",
        "Quick lookup: NVIDIA quarterly revenue",
        "Quick lookup: Amazon AWS market share"
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        print_fire("Using COST strategy - maximum savings mode activated")

        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "cost",  # COST mode: always use cheapest
                "deduplicate": True,
                "auto_retry": True
            },
            json=research_queries
        )

        result = response.json()

        plan = result["optimization_plan"]
        print_success(f"Cost Optimization Complete!")
        print(f"   Estimated Cost: ${plan['estimated_cost']:.4f}")
        print(f"   Processors Used: {list(plan['processor_distribution'].keys())}")

        return result["taskgroup_id"]


async def demo_quality_optimization():
    """Demonstrate quality-first optimization."""
    print_header("DEMO 3: Quality Optimization - Maximum Detail")

    # Complex research questions that need deep analysis
    research_queries = [
        "Comprehensive analysis of quantum computing's impact on cryptography, "
        "including detailed timeline for post-quantum migration and specific "
        "algorithms at risk",

        "Deep investigation into fusion energy commercialization: technical "
        "challenges, regulatory landscape, major players, funding trends, and "
        "realistic timeline for grid-scale deployment",

        "Thorough examination of AGI safety alignment approaches, comparing "
        "RLHF, constitutional AI, and interpretability research with citations "
        "and expert opinions"
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        print_fire("Using QUALITY strategy - going DEEP")

        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "quality",  # QUALITY mode: use best processors
                "deduplicate": False,
                "auto_retry": True
            },
            json=research_queries
        )

        result = response.json()

        plan = result["optimization_plan"]
        print_success(f"Quality Optimization Complete!")
        print(f"   Processors: {plan['processor_distribution']}")
        print(f"   Estimated Duration: ~{plan['estimated_duration']}s")
        print(f"   This will be COMPREHENSIVE AF")

        return result["taskgroup_id"]


async def demo_adaptive_optimization():
    """Demonstrate adaptive optimization - the SMARTEST option."""
    print_header("DEMO 4: Adaptive Optimization - AI-Powered Selection")

    # Mix of simple and complex tasks
    research_queries = [
        # Simple tasks
        "Latest AI news",
        "Top tech stocks",

        # Medium complexity
        "Analyze recent developments in large language models",
        "Compare leading quantum computing companies",

        # Complex tasks
        "Comprehensive evaluation of climate tech investment landscape "
        "including market size, growth trends, top investors, breakthrough "
        "technologies, and regulatory impacts",

        "Deep analysis of neuromorphic computing: current state of hardware, "
        "software ecosystems, commercial applications, key research institutions, "
        "and comparison with traditional von Neumann architectures"
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        print_fire("Using ADAPTIVE strategy - AI analyzes each task individually")

        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "adaptive",  # ADAPTIVE: smart per-task selection
                "deduplicate": True,
                "auto_retry": True
            },
            json=research_queries
        )

        result = response.json()

        plan = result["optimization_plan"]
        print_success(f"Adaptive Optimization Complete!")

        print(f"\n🧠 Smart Processor Assignment:")
        for processor, count in sorted(plan['processor_distribution'].items()):
            print(f"   {processor}: {count} tasks")

        print(f"\n💡 The optimizer analyzed each task and assigned optimal processors")
        print(f"   Total Cost: ${plan['estimated_cost']:.4f}")
        print(f"   Perfect balance of cost, speed, and quality")

        return result["taskgroup_id"]


async def demo_deduplication():
    """Demonstrate task deduplication."""
    print_header("DEMO 5: Task Deduplication - Eliminate Redundancy")

    # Intentionally include duplicates
    research_queries = [
        "Latest AI developments",
        "Quantum computing news",
        "Latest AI developments",  # DUPLICATE
        "Climate tech startups",
        "Quantum computing news",  # DUPLICATE
        "Latest AI developments",  # DUPLICATE
        "Space technology advancements",
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        print_info(f"Submitting {len(research_queries)} tasks (with intentional duplicates)")

        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "balanced",
                "deduplicate": True,  # Deduplication ON
                "auto_retry": False
            },
            json=research_queries
        )

        result = response.json()

        dedup = result["deduplication"]

        print_success("Deduplication Complete!")
        print(f"\n🔍 Deduplication Results:")
        print(f"   Original Tasks: {dedup['original_count']}")
        print(f"   Unique Tasks: {dedup['unique_count']}")
        print(f"   Duplicates Removed: {dedup['duplicates_removed']}")
        print(f"   Savings: {(dedup['duplicates_removed']/dedup['original_count']*100):.1f}%")

        plan = result["optimization_plan"]
        print(f"\n💰 Cost Savings from Deduplication:")
        print(f"   Estimated Cost: ${plan['estimated_cost']:.4f}")
        print(f"   (Would have been ${plan['estimated_cost'] * dedup['original_count'] / dedup['unique_count']:.4f} without dedup)")

        return result["taskgroup_id"]


async def demo_massive_parallel_research():
    """Demonstrate MASSIVE parallel research - the BEAST mode."""
    print_header("DEMO 6: MASSIVE Parallel Research - BEAST MODE")

    print_fire("Generating 100 research queries across multiple domains...")

    # Generate a LOT of research tasks
    domains = [
        "artificial intelligence", "quantum computing", "fusion energy",
        "synthetic biology", "space technology", "climate tech",
        "neuromorphic computing", "advanced materials", "robotics",
        "blockchain technology"
    ]

    research_queries = []
    for i in range(10):
        for domain in domains:
            research_queries.append(
                f"Latest developments in {domain} - focus area {i+1}"
            )

    print_info(f"Generated {len(research_queries)} research tasks")

    async with httpx.AsyncClient(timeout=3600) as client:
        print_fire("Launching BASED optimization for 100-task batch...")

        start_time = time.time()

        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={
                "strategy": "balanced",
                "deduplicate": True,
                "auto_retry": True
            },
            json=research_queries
        )

        optimization_time = time.time() - start_time

        result = response.json()

        plan = result["optimization_plan"]
        dedup = result["deduplication"]

        print_success(f"Optimization Complete in {optimization_time:.2f}s!")

        print(f"\n📊 Massive Batch Statistics:")
        print(f"   Original Tasks: {dedup['original_count']}")
        print(f"   Unique Tasks: {dedup['unique_count']}")
        print(f"   Estimated Cost: ${plan['estimated_cost']:.4f}")
        print(f"   Estimated Duration: ~{plan['estimated_duration']}s")

        print(f"\n🎯 Processor Distribution:")
        for processor, count in sorted(plan['processor_distribution'].items()):
            print(f"   {processor}: {count} tasks")

        taskgroup_id = result["taskgroup_id"]

        # Now use managed execution to wait for completion
        print(f"\n⏳ Starting managed execution (this will take a while)...")
        print(f"   Task Group ID: {taskgroup_id}")

        # Note: In real usage, you'd call the managed endpoint here
        # For demo purposes, we'll just show the taskgroup_id

        print_fire(f"To monitor progress, use:")
        print(f"   GET {BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analytics")

        return taskgroup_id


async def demo_real_time_analytics():
    """Demonstrate real-time analytics for running groups."""
    print_header("DEMO 7: Real-Time Analytics - Live Monitoring")

    # First, create and start a group
    research_queries = [
        f"Research topic {i}" for i in range(10)
    ]

    async with httpx.AsyncClient(timeout=1800) as client:
        # Optimize and execute
        response = await client.post(
            f"{BASE_URL}/v1beta/tasks/groups/optimize",
            params={"strategy": "balanced"},
            json=research_queries
        )

        taskgroup_id = response.json()["taskgroup_id"]

        print_info(f"Created task group: {taskgroup_id}")
        print_info("Monitoring in real-time...")

        # Monitor for a bit
        for i in range(5):
            await asyncio.sleep(2)

            # Get analytics
            analytics_response = await client.get(
                f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analyze"
            )

            analytics = analytics_response.json()
            metrics = analytics["real_time_metrics"]

            # Display progress
            progress = metrics["progress_percent"]
            completed = metrics["completed"]
            total = metrics["total_tasks"]
            throughput = metrics["throughput"]
            eta = metrics["eta_seconds"]

            # Generate progress bar
            bar_width = 40
            filled = int(bar_width * progress / 100)
            bar = "█" * filled + "░" * (bar_width - filled)

            print(f"\r🔥 [{bar}] {progress:.1f}% | "
                  f"{completed}/{total} | "
                  f"{throughput:.2f} tasks/s | "
                  f"ETA: {eta:.0f}s", end="")

        print()  # Newline after progress bar
        print_success("Real-time monitoring demo complete!")

        return taskgroup_id


async def main():
    """Run all BASED demos."""
    print("\n" + "="*80)
    print("🔥🔥🔥 BASED RESEARCH WORKFLOW DEMONSTRATION 🔥🔥🔥")
    print("="*80)
    print()
    print("This is not your average research automation.")
    print("This is a NUCLEAR-POWERED AI optimization system.")
    print()
    print("Prepare to witness:")
    print("  • Automatic task complexity analysis")
    print("  • Intelligent processor selection")
    print("  • Cost/speed/quality optimization")
    print("  • Automatic deduplication")
    print("  • Failure recovery with retry")
    print("  • Real-time progress tracking")
    print()
    print("Let's go. 🚀")
    print("="*80)

    try:
        # Run all demos
        await demo_basic_optimization()
        await asyncio.sleep(2)

        await demo_cost_optimization()
        await asyncio.sleep(2)

        await demo_quality_optimization()
        await asyncio.sleep(2)

        await demo_adaptive_optimization()
        await asyncio.sleep(2)

        await demo_deduplication()
        await asyncio.sleep(2)

        # Skipping massive parallel for demo speed
        # await demo_massive_parallel_research()

        await demo_real_time_analytics()

        print_header("ALL DEMOS COMPLETE")

        print("\n🎯 Key Takeaways:")
        print("  ✅ Automatic optimization - no manual processor selection needed")
        print("  ✅ Cost minimization - smart processor assignment saves $$")
        print("  ✅ Quality maximization - deep research when you need it")
        print("  ✅ Deduplication - eliminate redundant API calls")
        print("  ✅ Auto-retry - failures recover automatically")
        print("  ✅ Real-time monitoring - track progress live")

        print("\n📚 Available Strategies:")
        print("  • cost     - Minimize cost (use cheapest processors)")
        print("  • speed    - Minimize time (use faster processors)")
        print("  • balanced - Balance cost and speed (recommended)")
        print("  • quality  - Maximize quality (use best processors)")
        print("  • adaptive - AI-powered per-task optimization (smartest)")

        print("\n🚀 Ready for Production:")
        print("  • Handle 1-1000+ tasks per batch")
        print("  • Automatic complexity analysis")
        print("  • Intelligent cost optimization")
        print("  • Zero manual intervention required")

        print("\n💪 THIS IS BASED.\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Make sure the server is running:")
        print("   python -m parallel_websearch_server.main")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
