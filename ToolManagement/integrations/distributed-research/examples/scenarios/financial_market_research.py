"""
Real-World Scenario: Financial Market Research & Analysis

Business Context:
Investment firm needs to analyze 100+ stocks, track market sentiment,
monitor regulatory changes, and generate investment theses daily.

Requirements:
- Real-time market sentiment tracking
- Regulatory filing analysis (10-K, 10-Q, 8-K)
- Competitive landscape analysis per sector
- Risk assessment and due diligence
- Speed-critical: Results needed in <15 minutes
- High accuracy required (use QUALITY optimization)

This demonstrates:
- Priority-based execution (time-sensitive tasks first)
- QUALITY optimization for critical decisions
- Parallel processing across sectors
- Real-time dashboard monitoring
- Regulatory compliance tracking
"""

import asyncio
import httpx
import json
import time
from typing import List, Dict
from datetime import datetime

BASE_URL = "http://localhost:2040"


class FinancialResearchOrchestrator:
    """Orchestrates financial market research workflows"""

    def __init__(self):
        self.sectors = [
            "Technology", "Healthcare", "Finance", "Energy",
            "Consumer", "Industrial", "Real Estate", "Materials"
        ]

    async def market_opening_analysis(self, watchlist: List[str]):
        """
        Pre-market analysis executed before market opens.

        Strategy: SPEED + QUALITY for time-critical decisions.
        Priority: Urgent execution required.
        """
        print("=" * 80)
        print("SCENARIO: Pre-Market Analysis (Time-Critical)")
        print(f"Analyzing {len(watchlist)} stocks before market open")
        print(f"Deadline: 15 minutes")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # Generate high-priority research tasks
            tasks = []

            # Overnight news and sentiment (CRITICAL - Priority 10)
            for stock in watchlist:
                tasks.append({
                    "task_id": f"sentiment_{stock}",
                    "input": f"Overnight news, sentiment analysis, and pre-market indicators for {stock}",
                    "priority": 10,
                    "processor": "core"  # Speed-optimized
                })

            # Regulatory filings check (HIGH - Priority 8)
            for stock in watchlist[:20]:  # Top 20 holdings
                tasks.append({
                    "task_id": f"filings_{stock}",
                    "input": f"Recent SEC filings (8-K, Form 4) and material changes for {stock}",
                    "priority": 8,
                    "processor": "pro"  # Quality-optimized
                })

            # Technical analysis (MEDIUM - Priority 5)
            for stock in watchlist[:10]:  # Top 10 positions
                tasks.append({
                    "task_id": f"technical_{stock}",
                    "input": f"Technical analysis: support/resistance levels, momentum indicators for {stock}",
                    "priority": 5,
                    "processor": "base"
                })

            print(f"\n[1] Generated {len(tasks)} time-critical tasks")
            print(f"   Priority 10 (Sentiment): {len(watchlist)} tasks")
            print(f"   Priority 8 (Filings): {min(20, len(watchlist))} tasks")
            print(f"   Priority 5 (Technical): {min(10, len(watchlist))} tasks")

            # Submit to intelligent distribution system
            print("\n[2] Submitting to intelligent distribution system...")

            submitted_count = 0
            for task in tasks:
                await client.post(
                    f"{BASE_URL}/v1beta/distribution/submit",
                    json={
                        "task_id": task["task_id"],
                        "input_data": task["input"],
                        "processor_tier": task.get("processor"),
                        "priority": task["priority"],
                        "complexity_score": 70  # High complexity for financial analysis
                    }
                )
                submitted_count += 1

            print(f"   ✓ Submitted {submitted_count} tasks to distribution system")

            # Check system metrics
            metrics_response = await client.get(
                f"{BASE_URL}/v1beta/distribution/metrics"
            )
            metrics = metrics_response.json()

            print(f"\n[3] Distribution system status:")
            print(f"   Queue size: {metrics['queue_size']}")
            print(f"   Active tasks: {metrics['active_tasks']}")

            for tier, stats in metrics['processors'].items():
                print(f"   {tier}: {stats['health']}, "
                      f"load={stats['current_load']}, "
                      f"utilization={stats['utilization']:.1%}")

            # Get priority-ordered batches
            print("\n[4] Processing priority-ordered batches...")

            start_time = time.time()
            processed = 0
            target = len(tasks)

            while processed < target and (time.time() - start_time) < 900:  # 15 min limit
                # Get next batch (priority-sorted by distributor)
                batch_response = await client.post(
                    f"{BASE_URL}/v1beta/distribution/batch/next",
                    json={"max_batch_size": 20}
                )

                batch = batch_response.json()

                if batch["batch_size"] == 0:
                    await asyncio.sleep(2)
                    continue

                print(f"\n   Batch {processed//20 + 1}:")
                print(f"   Size: {batch['batch_size']} tasks")

                # Show priority distribution in batch
                priorities = {}
                for task in batch["tasks"]:
                    pri = task["priority"]
                    priorities[pri] = priorities.get(pri, 0) + 1

                print(f"   Priority distribution: {priorities}")
                print(f"   Avg wait time: {sum(t['wait_time'] for t in batch['tasks'])/len(batch['tasks']):.2f}s")

                # Simulate processing
                await asyncio.sleep(1)

                # Report completions
                for task in batch["tasks"]:
                    await client.post(
                        f"{BASE_URL}/v1beta/distribution/completion",
                        json={
                            "task_id": task["task_id"],
                            "success": True,
                            "execution_time": 2.5
                        }
                    )

                processed += batch["batch_size"]

                elapsed = time.time() - start_time
                remaining = target - processed
                rate = processed / elapsed if elapsed > 0 else 0
                eta = remaining / rate if rate > 0 else 0

                print(f"   Progress: {processed}/{target} ({processed/target*100:.1f}%)")
                print(f"   Throughput: {rate:.2f} tasks/s")
                print(f"   ETA: {eta:.0f}s")

            total_time = time.time() - start_time

            print(f"\n[5] Pre-market analysis complete!")
            print(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
            print(f"   Tasks processed: {processed}")
            print(f"   Average: {total_time/processed:.2f}s per task")
            print(f"   ✓ Within deadline: {total_time < 900}")

            return {
                "total_tasks": target,
                "processed": processed,
                "duration": total_time,
                "within_deadline": total_time < 900
            }

    async def sector_deep_dive_workflow(self, sector: str, companies: List[str]):
        """
        Comprehensive sector analysis using DAG workflow.

        Multi-stage analysis:
        1. Individual company fundamentals (parallel)
        2. Sector-wide competitive analysis (depends on step 1)
        3. Macro trends and outlook (depends on step 2)
        4. Investment recommendations (depends on step 3)
        """
        print("\n" + "=" * 80)
        print(f"SCENARIO: {sector} Sector Deep Dive")
        print(f"Analyzing {len(companies)} companies in {sector}")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            workflow_tasks = []

            # Stage 1: Company fundamentals (parallel)
            for company in companies:
                workflow_tasks.append({
                    "task_id": f"fundamentals_{company}",
                    "input_data": f"Comprehensive fundamental analysis of {company}: "
                                  f"financials, growth metrics, competitive position, management quality",
                    "dependencies": [],
                    "processor": "pro"  # High quality for fundamentals
                })

            # Stage 2: Competitive landscape (depends on all fundamentals)
            workflow_tasks.append({
                "task_id": "competitive_landscape",
                "input_data": f"Analyze competitive dynamics in {sector} sector: "
                              f"market share trends, competitive advantages, barriers to entry",
                "dependencies": [f"fundamentals_{c}" for c in companies],
                "processor": "ultra"  # Premium analysis
            })

            # Stage 3: Macro trends (depends on competitive analysis)
            workflow_tasks.append({
                "task_id": "macro_trends",
                "input_data": f"Identify macro trends affecting {sector}: "
                              f"regulatory changes, technological disruption, economic factors",
                "dependencies": ["competitive_landscape"],
                "processor": "ultra"
            })

            # Stage 4: Investment thesis (depends on macro trends)
            workflow_tasks.append({
                "task_id": "investment_thesis",
                "input_data": f"Generate investment recommendations for {sector}: "
                              f"top picks, risks, price targets, allocation suggestions",
                "dependencies": ["macro_trends"],
                "processor": "ultra2x"  # Highest quality for recommendations
            })

            print(f"\n[1] Creating multi-stage workflow:")
            print(f"   Stage 1: {len(companies)} company analyses (parallel)")
            print(f"   Stage 2: Competitive landscape (1 task)")
            print(f"   Stage 3: Macro trends (1 task)")
            print(f"   Stage 4: Investment thesis (1 task)")

            # Create workflow
            workflow_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/create",
                json={"tasks": workflow_tasks}
            )

            workflow_data = workflow_response.json()
            workflow_id = workflow_data["workflow_id"]

            print(f"\n[2] Workflow created successfully")
            print(f"   Workflow ID: {workflow_id}")
            print(f"   Execution levels: {workflow_data['execution_levels']}")
            print(f"   Max parallelism: {workflow_data['max_parallelism']}")
            print(f"   Execution plan: {workflow_data['execution_plan']}")

            # Get workflow graph for visualization
            graph_response = await client.get(
                f"{BASE_URL}/v1beta/workflows/{workflow_id}/graph"
            )

            graph = graph_response.json()

            print(f"\n[3] Workflow structure:")
            print(f"   Nodes: {len(graph['nodes'])}")
            print(f"   Edges (dependencies): {len(graph['edges'])}")

            # Execute workflow
            execution_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/{workflow_id}/execute",
                json={
                    "max_parallel": 15,
                    "task_timeout": 3600.0,
                    "execution_mode": "adaptive"  # Execute as soon as dependencies satisfy
                }
            )

            execution_id = execution_response.json()["execution_id"]

            print(f"\n[4] Workflow executing: {execution_id}")
            print("   Stage 1 (Company analyses) running in parallel...")
            print("   Subsequent stages will execute as dependencies complete")

            return {
                "workflow_id": workflow_id,
                "execution_id": execution_id,
                "sector": sector,
                "companies": len(companies)
            }

    async def risk_assessment_pipeline(self, portfolio: Dict[str, float]):
        """
        Real-time risk assessment for portfolio holdings.

        Demonstrates:
        - QUALITY optimization for critical risk analysis
        - Real-time monitoring via SSE
        - Immediate alerts for risk threshold breaches
        """
        print("\n" + "=" * 80)
        print("SCENARIO: Portfolio Risk Assessment")
        print(f"Analyzing {len(portfolio)} positions")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # Generate comprehensive risk analysis tasks
            risk_tasks = []

            for ticker, allocation in portfolio.items():
                risk_tasks.extend([
                    f"Credit risk analysis for {ticker}: debt levels, credit ratings, default probability",
                    f"Market risk for {ticker}: beta, volatility, correlation with indices",
                    f"Liquidity risk for {ticker}: trading volume, bid-ask spread, market depth",
                    f"Operational risk for {ticker}: management issues, fraud indicators, governance",
                ])

            # Add portfolio-level risks
            risk_tasks.extend([
                "Portfolio concentration risk: sector exposure, single-stock risk, diversification metrics",
                "Systematic risk: market beta, interest rate sensitivity, economic cycle exposure",
                "Tail risk: Value at Risk (VaR), Conditional VaR, extreme event scenarios"
            ])

            print(f"\n[1] Generated {len(risk_tasks)} risk analysis tasks")
            print("   Using QUALITY optimization for accurate risk assessment")

            # Optimize with QUALITY strategy
            optimize_response = await client.post(
                f"{BASE_URL}/v1beta/tasks/groups/optimize",
                params={
                    "strategy": "quality",  # Highest accuracy for risk
                    "deduplicate": False,
                    "auto_retry": True
                },
                json=risk_tasks
            )

            result = optimize_response.json()
            plan = result["optimization_plan"]
            taskgroup_id = result["taskgroup_id"]

            print(f"\n[2] Optimization complete:")
            print(f"   Strategy: {plan['strategy']}")
            print(f"   Total tasks: {plan['total_tasks']}")
            print(f"   Estimated cost: ${plan['estimated_cost']:.4f}")
            print(f"   Estimated duration: {plan['estimated_duration']}s")
            print(f"   Processors: {plan['processor_distribution']}")

            print(f"\n[3] Starting real-time risk monitoring...")
            print("   Dashboard: http://localhost:2040/v1beta/dashboard/stream")
            print(f"   Scope: {taskgroup_id}")

            # In production, would use SSE streaming
            # For demo, we'll poll analytics endpoint

            start_time = time.time()
            last_progress = 0

            while True:
                await asyncio.sleep(3)

                # Get real-time analytics
                analytics_response = await client.get(
                    f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/analyze"
                )

                analytics = analytics_response.json()
                metrics = analytics.get("real_time_metrics", {})
                analysis = analytics.get("analysis", {})

                progress = metrics.get("progress_percent", 0)
                completed = metrics.get("completed", 0)
                total = metrics.get("total_tasks", 0)
                throughput = metrics.get("throughput", 0)
                eta = metrics.get("eta_seconds", 0)

                # Show progress if changed
                if progress != last_progress:
                    print(f"\r   Progress: {progress:.1f}% | "
                          f"{completed}/{total} | "
                          f"{throughput:.2f} tasks/s | "
                          f"ETA: {eta:.0f}s",
                          end="", flush=True)
                    last_progress = progress

                # Check completion
                status_response = await client.get(
                    f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}"
                )
                status = status_response.json()

                if not status.get("is_active"):
                    break

            print()  # Newline after progress

            total_duration = time.time() - start_time

            print(f"\n[4] Risk assessment complete!")
            print(f"   Duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
            print(f"   Success rate: {analysis.get('success_rate', 0):.1f}%")

            # Get final results
            export_response = await client.get(
                f"{BASE_URL}/v1beta/tasks/groups/{taskgroup_id}/export",
                params={"format": "json", "include_outputs": True}
            )

            results = export_response.json()

            print(f"\n[5] Risk analysis results ready for review")
            print("   Portfolio managers can now:")
            print("   • Review individual position risks")
            print("   • Assess portfolio-wide exposures")
            print("   • Identify concentration risks")
            print("   • Make informed rebalancing decisions")

            return {
                "taskgroup_id": taskgroup_id,
                "total_tasks": len(risk_tasks),
                "duration": total_duration,
                "cost": plan["estimated_cost"]
            }


async def main():
    """Run financial market research scenarios"""

    orchestrator = FinancialResearchOrchestrator()

    # Sample data
    watchlist = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK.B",
        "JPM", "V", "JNJ", "WMT", "PG", "MA", "HD", "DIS", "BAC", "ADBE",
        "CRM", "NFLX", "XOM", "CVX", "PFE", "KO", "PEP", "CSCO", "TMO", "ABT"
    ]

    tech_companies = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "CRM", "ADBE", "ORCL", "INTC"]

    portfolio = {
        "AAPL": 0.15, "MSFT": 0.12, "GOOGL": 0.10, "AMZN": 0.08, "NVDA": 0.07,
        "JPM": 0.06, "V": 0.05, "JNJ": 0.05, "WMT": 0.04, "PG": 0.04
    }

    try:
        # Scenario 1: Time-critical pre-market analysis
        print("\n📊 SCENARIO 1: Pre-Market Analysis (Time-Critical)")
        print("=" * 80)
        premarket_result = await orchestrator.market_opening_analysis(watchlist)

        if premarket_result["within_deadline"]:
            print("\n✅ Pre-market analysis complete within deadline!")
            print(f"   Processed {premarket_result['processed']} tasks in {premarket_result['duration']/60:.1f} minutes")
            print("   Portfolio managers have actionable intelligence before market open")
        else:
            print("\n⚠️  Analysis exceeded deadline - consider:")
            print("   • Increasing max_parallel in distribution system")
            print("   • Using faster processors")
            print("   • Reducing watchlist size")

        # Scenario 2: Deep sector analysis with DAG
        await asyncio.sleep(2)
        print("\n\n🏢 SCENARIO 2: Technology Sector Deep Dive")
        print("=" * 80)
        sector_result = await orchestrator.sector_deep_dive_workflow("Technology", tech_companies)

        if sector_result:
            print("\n✅ Sector analysis workflow executing!")
            print(f"   Workflow: {sector_result['workflow_id']}")
            print(f"   {sector_result['companies']} companies analyzed in parallel")
            print("   Multi-stage analysis: Fundamentals → Competition → Trends → Thesis")

        # Scenario 3: Portfolio risk assessment
        await asyncio.sleep(2)
        print("\n\n⚠️  SCENARIO 3: Portfolio Risk Assessment")
        print("=" * 80)
        risk_result = await orchestrator.risk_assessment_pipeline(portfolio)

        if risk_result:
            print("\n✅ Risk assessment complete!")
            print(f"   Analyzed {risk_result['total_tasks']} risk factors")
            print(f"   Duration: {risk_result['duration']/60:.1f} minutes")
            print(f"   Investment: ${risk_result['cost']:.4f} for comprehensive risk analysis")

        # Summary
        print("\n\n" + "=" * 80)
        print("FINANCIAL RESEARCH SUMMARY")
        print("=" * 80)

        print("\n✅ Capabilities Demonstrated:")
        print("   • Time-critical pre-market analysis (<15 minutes)")
        print("   • Priority-based task execution (urgent tasks first)")
        print("   • Multi-stage sector analysis with dependencies")
        print("   • Comprehensive portfolio risk assessment")
        print("   • QUALITY optimization for critical decisions")
        print("   • Real-time monitoring and progress tracking")

        print("\n💰 Business Impact:")
        print("   • Faster investment decisions (minutes vs hours)")
        print("   • Higher quality analysis (premium processors)")
        print("   • Better risk management (comprehensive coverage)")
        print("   • Scalable to 100+ stocks simultaneously")
        print("   • Automated daily workflows (reduce manual work)")

        print("\n🎯 Use Cases:")
        print("   • Hedge funds: Daily portfolio monitoring")
        print("   • Investment banks: Sector research reports")
        print("   • Asset managers: Risk assessment automation")
        print("   • Trading desks: Pre-market intelligence")
        print("   • Research analysts: Company deep dives")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
