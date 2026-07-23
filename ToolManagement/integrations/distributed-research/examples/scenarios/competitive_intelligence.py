"""
Real-World Scenario: E-Commerce Competitive Intelligence

Business Context:
E-commerce company needs to monitor competitor pricing, product launches,
marketing strategies, and customer sentiment across 50+ competitors daily.

Requirements:
- Process 500+ research queries per day
- Cost-sensitive (budget: $50/day)
- Results needed within 30 minutes
- Historical data retention for trend analysis
- Real-time alerts for significant changes

This demonstrates:
- Cost optimization with budget constraints
- Task prioritization (urgent vs routine)
- Caching for repeated queries
- Real-time monitoring with SSE
- Batch processing workflows
"""

import asyncio
import httpx
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict

BASE_URL = "http://localhost:2040"


class CompetitiveIntelligenceOrchestrator:
    """Orchestrates competitive intelligence research workflows"""

    def __init__(self, budget_per_day: float = 50.0):
        self.budget_per_day = budget_per_day
        self.spent_today = 0.0
        self.cache_ttl = 3600 * 4  # 4 hours for competitor data

    async def daily_competitor_analysis(self, competitors: List[str]):
        """
        Daily analysis of all competitors.

        Strategy: Use COST optimization for routine monitoring.
        """
        print("=" * 80)
        print("SCENARIO: Daily Competitor Analysis")
        print(f"Monitoring {len(competitors)} competitors")
        print(f"Budget: ${self.budget_per_day:.2f} | Spent: ${self.spent_today:.2f}")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # Generate research tasks
            tasks = []

            # Pricing analysis (highest priority)
            for competitor in competitors:
                tasks.append({
                    "task_id": f"pricing_{competitor}",
                    "input": f"Latest pricing strategy and discount patterns for {competitor}",
                    "priority": 10,
                    "complexity": 40
                })

            # Product launches (medium priority)
            for competitor in competitors[:20]:  # Top 20 only
                tasks.append({
                    "task_id": f"products_{competitor}",
                    "input": f"New product launches and updates from {competitor} in last 7 days",
                    "priority": 7,
                    "complexity": 50
                })

            # Marketing strategy (routine)
            for competitor in competitors[:10]:  # Top 10 only
                tasks.append({
                    "task_id": f"marketing_{competitor}",
                    "input": f"Current marketing campaigns and social media strategy for {competitor}",
                    "priority": 5,
                    "complexity": 60
                })

            print(f"\n[1] Generated {len(tasks)} research tasks")
            print(f"   - Pricing: {len(competitors)} tasks (priority=10)")
            print(f"   - Products: {min(20, len(competitors))} tasks (priority=7)")
            print(f"   - Marketing: {min(10, len(competitors))} tasks (priority=5)")

            # Check cache for recent results
            print("\n[2] Checking cache for recent results...")

            cache_hits = 0
            uncached_tasks = []

            for task in tasks:
                cache_key = f"competitor:{task['task_id']}:{datetime.now().strftime('%Y-%m-%d')}"

                cache_response = await client.post(
                    f"{BASE_URL}/v1beta/cache/get",
                    json={"key": cache_key}
                )

                cache_data = cache_response.json()
                if cache_data.get("found"):
                    cache_hits += 1
                else:
                    uncached_tasks.append(task)

            print(f"   Cache hits: {cache_hits}/{len(tasks)} ({cache_hits/len(tasks)*100:.1f}%)")
            print(f"   Tasks to execute: {len(uncached_tasks)}")

            if not uncached_tasks:
                print("\n✓ All results cached, no API calls needed!")
                return {"cached": True, "cost": 0.0}

            # Optimize with COST strategy to stay within budget
            print(f"\n[3] Optimizing {len(uncached_tasks)} tasks with COST strategy...")

            task_inputs = [task["input"] for task in uncached_tasks]

            optimize_response = await client.post(
                f"{BASE_URL}/v1beta/tasks/groups/optimize",
                params={
                    "strategy": "cost",  # Minimize cost
                    "deduplicate": True,
                    "auto_retry": True
                },
                json=task_inputs
            )

            result = optimize_response.json()
            plan = result["optimization_plan"]
            taskgroup_id = result["taskgroup_id"]

            estimated_cost = plan["estimated_cost"]
            remaining_budget = self.budget_per_day - self.spent_today

            print(f"   Estimated cost: ${estimated_cost:.4f}")
            print(f"   Remaining budget: ${remaining_budget:.2f}")

            if estimated_cost > remaining_budget:
                print(f"\n⚠️  WARNING: Exceeds budget by ${estimated_cost - remaining_budget:.2f}")
                print("   Consider:")
                print("   - Reducing task count")
                print("   - Using cached results only")
                print("   - Prioritizing critical tasks")
                return {"error": "budget_exceeded", "estimated_cost": estimated_cost}

            print(f"   Processor distribution: {plan['processor_distribution']}")
            print(f"   Estimated duration: {plan['estimated_duration']}s")

            # Execute with real-time monitoring
            print("\n[4] Executing tasks with real-time progress monitoring...")

            start_time = time.time()

            # Poll for completion
            completed = False
            while not completed:
                await asyncio.sleep(3)

                status_response = await client.get(
                    f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}"
                )
                status = status_response.json()

                analytics_response = await client.get(
                    f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analytics"
                )
                analytics = analytics_response.json()

                progress = analytics.get("analysis", {})

                elapsed = time.time() - start_time
                print(f"\r   Progress: {progress.get('completion_progress', 0):.1f}% | "
                      f"Success: {progress.get('success_rate', 0):.1f}% | "
                      f"Elapsed: {elapsed:.0f}s | "
                      f"ETA: {progress.get('eta_seconds', 0):.0f}s",
                      end="", flush=True)

                if not status.get("is_active"):
                    completed = True

            print()  # Newline after progress

            # Cache results for future use
            print("\n[5] Caching results...")

            export_response = await client.get(
                f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/export",
                params={"format": "json", "include_outputs": True}
            )

            results = export_response.json()["data"]

            cached_count = 0
            for task, result_data in zip(uncached_tasks, results):
                if result_data.get("status") == "completed":
                    cache_key = f"competitor:{task['task_id']}:{datetime.now().strftime('%Y-%m-%d')}"

                    await client.post(
                        f"{BASE_URL}/v1beta/cache/set",
                        json={
                            "key": cache_key,
                            "value": json.dumps(result_data.get("output", {})),
                            "ttl": self.cache_ttl
                        }
                    )
                    cached_count += 1

            print(f"   Cached {cached_count} results for 4 hours")

            # Update budget tracking
            self.spent_today += estimated_cost

            print(f"\n[6] Analysis complete!")
            print(f"   Total cost: ${estimated_cost:.4f}")
            print(f"   Budget remaining: ${self.budget_per_day - self.spent_today:.2f}")
            print(f"   Cache hit rate: {cache_hits/len(tasks)*100:.1f}%")

            return {
                "taskgroup_id": taskgroup_id,
                "tasks_executed": len(uncached_tasks),
                "tasks_cached": cache_hits,
                "cost": estimated_cost,
                "duration": time.time() - start_time,
                "success": True
            }

    async def urgent_competitor_alert(self, competitor: str, alert_type: str):
        """
        Urgent deep-dive analysis triggered by significant change.

        Strategy: Use QUALITY optimization for urgent, critical analysis.
        """
        print("\n" + "=" * 80)
        print(f"URGENT ALERT: {alert_type.upper()} detected for {competitor}")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # High-priority deep analysis tasks
            urgent_tasks = [
                f"Comprehensive analysis of {alert_type} by {competitor}: What changed, why, impact on market",
                f"Detailed comparison of {competitor}'s new {alert_type} vs our current strategy",
                f"Historical context: Previous {alert_type} moves by {competitor} and outcomes",
                f"Competitive response analysis: How other competitors reacted to similar moves",
                f"Strategic recommendations: Immediate actions we should take in response"
            ]

            print(f"\n[1] Urgent Analysis: {len(urgent_tasks)} deep-dive tasks")
            print("   Strategy: QUALITY optimization (comprehensive results)")

            # Use QUALITY strategy for best results
            optimize_response = await client.post(
                f"{BASE_URL}/v1beta/tasks/groups/optimize",
                params={
                    "strategy": "quality",  # Best quality for urgent analysis
                    "deduplicate": False,
                    "auto_retry": True
                },
                json=urgent_tasks
            )

            result = optimize_response.json()
            plan = result["optimization_plan"]

            print(f"\n[2] Optimization complete:")
            print(f"   Estimated cost: ${plan['estimated_cost']:.4f}")
            print(f"   Estimated duration: {plan['estimated_duration']}s")
            print(f"   Using premium processors: {plan['processor_distribution']}")

            # Note: In production, would use SSE streaming for real-time updates
            print("\n✓ Urgent analysis initiated - results in ~5-10 minutes")
            print("   Stakeholders will be notified via dashboard stream")

            return {
                "taskgroup_id": result["taskgroup_id"],
                "alert_type": alert_type,
                "competitor": competitor,
                "estimated_cost": plan["estimated_cost"]
            }

    async def trend_analysis_workflow(self, competitors: List[str], days: int = 30):
        """
        Historical trend analysis using DAG workflow.

        Demonstrates:
        - Multi-stage analysis with dependencies
        - Data aggregation across time periods
        - Insight generation from historical patterns
        """
        print("\n" + "=" * 80)
        print(f"TREND ANALYSIS: {days}-day competitive trends")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # Stage 1: Gather historical data (parallel)
            # Stage 2: Identify patterns (depends on stage 1)
            # Stage 3: Generate insights (depends on stage 2)

            workflow_tasks = []

            # Stage 1: Data gathering (parallel for each competitor)
            for competitor in competitors[:10]:  # Top 10 competitors
                workflow_tasks.append({
                    "task_id": f"gather_{competitor}",
                    "input_data": f"Pricing, product, and marketing changes for {competitor} over last {days} days",
                    "dependencies": [],
                    "processor": "base"
                })

            # Stage 2: Pattern identification (depends on all gathering)
            workflow_tasks.append({
                "task_id": "identify_patterns",
                "input_data": f"Analyze trends and patterns across all competitor data collected",
                "dependencies": [f"gather_{c}" for c in competitors[:10]],
                "processor": "core"
            })

            # Stage 3: Strategic insights (depends on pattern analysis)
            workflow_tasks.append({
                "task_id": "generate_insights",
                "input_data": "Generate strategic recommendations based on identified competitive trends",
                "dependencies": ["identify_patterns"],
                "processor": "pro"
            })

            print(f"\n[1] Creating DAG workflow: {len(workflow_tasks)} tasks")
            print(f"   Stage 1 (Gather): {len(competitors[:10])} tasks (parallel)")
            print(f"   Stage 2 (Analyze): 1 task (depends on all gathering)")
            print(f"   Stage 3 (Insights): 1 task (depends on analysis)")

            # Create workflow
            workflow_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/create",
                json={"tasks": workflow_tasks}
            )

            workflow_data = workflow_response.json()
            workflow_id = workflow_data["workflow_id"]

            print(f"\n[2] Workflow created: {workflow_id}")
            print(f"   Execution levels: {workflow_data['execution_levels']}")
            print(f"   Max parallelism: {workflow_data['max_parallelism']}")

            # Execute with adaptive mode
            execution_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/{workflow_id}/execute",
                json={
                    "max_parallel": 10,
                    "task_timeout": 3600.0,
                    "execution_mode": "adaptive"
                }
            )

            execution_id = execution_response.json()["execution_id"]

            print(f"\n[3] Workflow executing: {execution_id}")
            print("   Stage 1 tasks will run in parallel")
            print("   Stages 2 and 3 will run sequentially as dependencies complete")

            return {
                "workflow_id": workflow_id,
                "execution_id": execution_id,
                "success": True
            }


async def main():
    """Run competitive intelligence scenarios"""

    # Sample competitors
    competitors = [
        "Amazon", "Walmart", "Target", "Best Buy", "Costco",
        "Home Depot", "Lowe's", "Wayfair", "Overstock", "Chewy",
        "Etsy", "eBay", "Alibaba", "Wish", "Shopify Store Example"
    ]

    orchestrator = CompetitiveIntelligenceOrchestrator(budget_per_day=50.0)

    try:
        # Scenario 1: Daily routine monitoring
        print("\n🔍 SCENARIO 1: Daily Routine Monitoring")
        print("=" * 80)
        daily_result = await orchestrator.daily_competitor_analysis(competitors)

        if daily_result.get("success"):
            print("\n✅ Daily analysis complete!")
            print(f"   Execution time: {daily_result['duration']:.1f}s")
            print(f"   Cost efficiency: {daily_result['cost']/daily_result['tasks_executed']:.4f} per task")
            print(f"   Cache savings: {daily_result['tasks_cached']} tasks")

        # Scenario 2: Urgent alert response
        await asyncio.sleep(2)
        print("\n\n🚨 SCENARIO 2: Urgent Price Drop Alert")
        print("=" * 80)
        alert_result = await orchestrator.urgent_competitor_alert(
            competitor="Amazon",
            alert_type="major price reduction"
        )

        if alert_result:
            print("\n✅ Urgent analysis initiated!")
            print(f"   Task Group: {alert_result['taskgroup_id']}")
            print(f"   Investment: ${alert_result['estimated_cost']:.4f} for critical intelligence")

        # Scenario 3: Historical trend analysis
        await asyncio.sleep(2)
        print("\n\n📊 SCENARIO 3: 30-Day Trend Analysis")
        print("=" * 80)
        trend_result = await orchestrator.trend_analysis_workflow(competitors, days=30)

        if trend_result.get("success"):
            print("\n✅ Trend analysis workflow started!")
            print(f"   Workflow: {trend_result['workflow_id']}")
            print(f"   Execution: {trend_result['execution_id']}")
            print("   Multi-stage analysis in progress...")

        # Summary
        print("\n\n" + "=" * 80)
        print("COMPETITIVE INTELLIGENCE SUMMARY")
        print("=" * 80)
        print("\n✅ Key Achievements:")
        print(f"   • Processed {daily_result.get('tasks_executed', 0)} tasks within budget")
        print(f"   • Cache hit rate: {daily_result.get('tasks_cached', 0)/15*100:.0f}% (reduced costs)")
        print(f"   • Urgent alerts: Real-time response in <10 minutes")
        print(f"   • Trend analysis: Multi-stage DAG workflow with dependencies")
        print(f"   • Total cost: ${daily_result.get('cost', 0) + alert_result.get('estimated_cost', 0):.4f}")

        print("\n💡 Business Value:")
        print("   • Stay ahead of competitor moves")
        print("   • Data-driven pricing strategies")
        print("   • Automated daily intelligence gathering")
        print("   • Cost-effective at scale (50+ competitors)")
        print("   • Real-time alerts for critical changes")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
