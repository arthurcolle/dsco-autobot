"""
Real-World Scenario: Enterprise Due Diligence

Business Context:
Private equity firm conducting due diligence on $500M acquisition target.
Need comprehensive analysis across financial, legal, operational, market,
technological, and cultural dimensions within 2-week timeline.

Requirements:
- Comprehensive multi-dimensional analysis
- Time-critical (14-day due diligence period)
- High accuracy required (deal-breaking insights)
- Confidentiality (internal deployment)
- Audit trail for all analyses

This demonstrates:
- Complex multi-stage workflows with dependencies
- QUALITY optimization for critical business decisions
- Parallel execution across analysis dimensions
- Real-time progress tracking for stakeholders
- Comprehensive reporting and export
"""

import asyncio
import httpx
import json
import time
from typing import List, Dict
from datetime import datetime, timedelta

BASE_URL = "http://localhost:2040"


class DueDiligenceOrchestrator:
    """Orchestrates comprehensive enterprise due diligence"""

    def __init__(self, target_company: str, deal_value: float):
        self.target_company = target_company
        self.deal_value = deal_value
        self.deadline_days = 14

    async def comprehensive_due_diligence_workflow(self):
        """
        End-to-end due diligence workflow across all dimensions.

        Workflow stages:
        1. Data gathering across all dimensions (parallel)
        2. Dimension-specific deep dives (parallel)
        3. Risk assessment and red flag identification
        4. Valuation analysis and deal structuring
        5. Final recommendation synthesis
        """
        print("=" * 80)
        print(f"COMPREHENSIVE DUE DILIGENCE")
        print(f"Target: {self.target_company}")
        print(f"Deal Value: ${self.deal_value/1e6:.0f}M")
        print(f"Timeline: {self.deadline_days} days")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=7200) as client:

            # Build comprehensive workflow
            workflow_tasks = []

            # Stage 1: Initial data gathering (parallel across dimensions)
            dimensions = [
                ("financial", "Financial statements, cash flows, working capital, debt structure"),
                ("legal", "Contracts, litigation, regulatory compliance, IP portfolio"),
                ("operational", "Supply chain, manufacturing, logistics, IT systems"),
                ("market", "Market position, competitive landscape, customer concentration"),
                ("technology", "Technology stack, technical debt, R&D pipeline"),
                ("hr", "Organization structure, key personnel, compensation, culture"),
                ("commercial", "Revenue quality, customer contracts, pricing power"),
                ("regulatory", "Regulatory environment, pending regulations, compliance costs")
            ]

            for dim_id, dim_desc in dimensions:
                workflow_tasks.append({
                    "task_id": f"gather_{dim_id}",
                    "input_data": f"Gather comprehensive {dim_id} data for {self.target_company}: {dim_desc}",
                    "dependencies": [],
                    "processor": "base"
                })

            print(f"\n[1] Stage 1: Data Gathering - {len(dimensions)} dimensions")
            for dim_id, _ in dimensions:
                print(f"   • {dim_id.title()} analysis")

            # Stage 2: Deep dives (depends on relevant gathering)
            deep_dives = [
                {
                    "task_id": "financial_deep_dive",
                    "input_data": f"Comprehensive financial analysis of {self.target_company}: "
                                  f"quality of earnings, working capital requirements, debt capacity, "
                                  f"revenue recognition policies, off-balance sheet items",
                    "dependencies": ["gather_financial", "gather_operational"],
                    "processor": "ultra"  # High quality for financial analysis
                },
                {
                    "task_id": "legal_deep_dive",
                    "input_data": f"Comprehensive legal review of {self.target_company}: "
                                  f"material contracts, litigation exposure, regulatory issues, "
                                  f"intellectual property strength, environmental liabilities",
                    "dependencies": ["gather_legal", "gather_regulatory"],
                    "processor": "ultra"
                },
                {
                    "task_id": "market_deep_dive",
                    "input_data": f"Comprehensive market analysis for {self.target_company}: "
                                  f"competitive positioning, market trends, customer dynamics, "
                                  f"pricing trends, market share evolution, competitive threats",
                    "dependencies": ["gather_market", "gather_commercial"],
                    "processor": "pro"
                },
                {
                    "task_id": "operational_deep_dive",
                    "input_data": f"Comprehensive operational assessment of {self.target_company}: "
                                  f"operational efficiency, supply chain resilience, manufacturing capacity, "
                                  f"technology infrastructure, scalability potential",
                    "dependencies": ["gather_operational", "gather_technology"],
                    "processor": "pro"
                },
                {
                    "task_id": "people_deep_dive",
                    "input_data": f"Comprehensive people and culture assessment of {self.target_company}: "
                                  f"management team quality, retention risks, compensation benchmarking, "
                                  f"cultural fit, organizational capabilities",
                    "dependencies": ["gather_hr"],
                    "processor": "core"
                }
            ]

            workflow_tasks.extend(deep_dives)

            print(f"\n[2] Stage 2: Deep Dives - {len(deep_dives)} comprehensive analyses")
            print("   Each deep dive synthesizes multiple data sources")

            # Stage 3: Risk assessment (depends on all deep dives)
            workflow_tasks.append({
                "task_id": "risk_assessment",
                "input_data": f"Comprehensive risk assessment for {self.target_company} acquisition: "
                              f"identify deal-breaker risks, quantify risk exposures, "
                              f"propose mitigation strategies, assess risk-adjusted returns",
                "dependencies": [dd["task_id"] for dd in deep_dives],
                "processor": "ultra2x"  # Highest quality for risk
            })

            print(f"\n[3] Stage 3: Risk Assessment")
            print("   • Synthesizes all deep dive findings")
            print("   • Identifies deal-breaker risks")
            print("   • Quantifies risk exposures")

            # Stage 4: Valuation analysis (depends on financial and risk)
            workflow_tasks.append({
                "task_id": "valuation_analysis",
                "input_data": f"Comprehensive valuation of {self.target_company}: "
                              f"DCF analysis, comparable company analysis, precedent transactions, "
                              f"synergy quantification, purchase price recommendation",
                "dependencies": ["financial_deep_dive", "risk_assessment"],
                "processor": "ultra2x"  # Critical for deal pricing
            })

            print(f"\n[4] Stage 4: Valuation Analysis")
            print("   • DCF and comparable analyses")
            print("   • Synergy quantification")
            print("   • Price recommendation")

            # Stage 5: Deal structuring (depends on valuation and legal)
            workflow_tasks.append({
                "task_id": "deal_structure",
                "input_data": f"Optimal deal structure for {self.target_company} acquisition: "
                              f"financing mix, earn-out provisions, indemnification, "
                              f"tax optimization, transition planning",
                "dependencies": ["valuation_analysis", "legal_deep_dive"],
                "processor": "ultra"
            })

            print(f"\n[5] Stage 5: Deal Structuring")
            print("   • Financing recommendations")
            print("   • Contract provisions")
            print("   • Tax optimization")

            # Stage 6: Final recommendation (depends on all above)
            workflow_tasks.append({
                "task_id": "final_recommendation",
                "input_data": f"Final investment recommendation for {self.target_company}: "
                              f"go/no-go recommendation, key value drivers, critical risks, "
                              f"deal terms, integration priorities, 100-day plan",
                "dependencies": ["risk_assessment", "valuation_analysis", "deal_structure"],
                "processor": "ultra2x"  # Highest quality for final rec
            })

            print(f"\n[6] Stage 6: Final Recommendation")
            print("   • Go/no-go decision")
            print("   • Investment thesis")
            print("   • Integration roadmap")

            print(f"\n[7] Creating comprehensive workflow: {len(workflow_tasks)} tasks")

            # Create workflow
            workflow_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/create",
                json={"tasks": workflow_tasks}
            )

            workflow_data = workflow_response.json()
            workflow_id = workflow_data["workflow_id"]

            print(f"\n[8] Workflow created successfully")
            print(f"   Workflow ID: {workflow_id}")
            print(f"   Total tasks: {len(workflow_tasks)}")
            print(f"   Execution levels: {workflow_data['execution_levels']}")
            print(f"   Max parallelism: {workflow_data['max_parallelism']}")
            print(f"   Estimated duration: {workflow_data['execution_levels']} * 2-5 min/level")

            # Get visualization data
            graph_response = await client.get(
                f"{BASE_URL}/v1beta/workflows/{workflow_id}/graph"
            )

            graph = graph_response.json()

            print(f"\n[9] Workflow complexity:")
            print(f"   Nodes: {len(graph['nodes'])}")
            print(f"   Dependencies: {len(graph['edges'])}")
            print(f"   Parallel stages: {workflow_data['execution_levels']}")

            # Execute workflow
            execution_response = await client.post(
                f"{BASE_URL}/v1beta/workflows/{workflow_id}/execute",
                json={
                    "max_parallel": 20,
                    "task_timeout": 3600.0,
                    "execution_mode": "adaptive"  # Maximize throughput
                }
            )

            execution_id = execution_response.json()["execution_id"]

            print(f"\n[10] Due diligence workflow executing: {execution_id}")
            print("    Real-time progress: http://localhost:2040/v1beta/dashboard/stream")

            # Monitor progress
            start_time = time.time()
            last_update = 0

            while True:
                await asyncio.sleep(5)

                # Get workflow status
                status_response = await client.get(
                    f"{BASE_URL}/v1beta/workflows/{workflow_id}"
                )
                status = status_response.json()

                elapsed = time.time() - start_time

                # Count task statuses
                statuses = {}
                for task in status["tasks"]:
                    st = task["status"]
                    statuses[st] = statuses.get(st, 0) + 1

                current_time = time.time()
                if current_time - last_update >= 5:
                    print(f"\n   Status @ {elapsed:.0f}s:")
                    print(f"   • Completed: {statuses.get('completed', 0)}/{len(workflow_tasks)}")
                    print(f"   • Running: {statuses.get('running', 0)}")
                    print(f"   • Pending: {statuses.get('pending', 0) + statuses.get('ready', 0)}")
                    if statuses.get('failed', 0) > 0:
                        print(f"   • Failed: {statuses['failed']}")

                    last_update = current_time

                if status["is_complete"]:
                    break

            total_duration = time.time() - start_time

            print(f"\n[11] Due diligence workflow complete!")
            print(f"    Duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes, {total_duration/3600:.2f} hours)")
            print(f"    Has failures: {status['has_failures']}")

            if not status['has_failures']:
                print("\n✅ All analyses completed successfully!")
            else:
                print("\n⚠️  Some analyses failed - review required")

            # Export comprehensive report
            print(f"\n[12] Generating comprehensive due diligence report...")

            execution_result_response = await client.get(
                f"{BASE_URL}/v1beta/workflows/execution/{execution_id}"
            )

            execution_result = execution_result_response.json()

            print(f"    Report statistics:")
            if 'statistics' in execution_result:
                stats = execution_result['statistics']
                print(f"    • Total tasks: {stats.get('status_counts', {}).get('completed', 0)}")
                print(f"    • Success rate: {stats.get('success_rate', 0):.1%}")
                print(f"    • Avg task duration: {stats.get('avg_execution_time', 0):.2f}s")

            print(f"\n[13] Due diligence package ready for investment committee")

            return {
                "workflow_id": workflow_id,
                "execution_id": execution_id,
                "duration_hours": total_duration / 3600,
                "success": not status['has_failures'],
                "total_tasks": len(workflow_tasks)
            }

    async def competitive_bidding_analysis(self, target_company: str, competitors: List[str]):
        """
        Rapid competitive intelligence when multiple bidders.

        Time-critical: Need analysis within 48 hours.
        Strategy: SPEED optimization while maintaining quality.
        """
        print("\n" + "=" * 80)
        print(f"COMPETITIVE BIDDING SCENARIO")
        print(f"Target: {target_company}")
        print(f"Competing bidders: {len(competitors)}")
        print("Deadline: 48 hours")
        print("=" * 80)

        async with httpx.AsyncClient(timeout=3600) as client:

            # High-priority urgent tasks
            urgent_tasks = []

            # Competitor analysis (understand who we're bidding against)
            for competitor in competitors:
                urgent_tasks.append(
                    f"Comprehensive profile of {competitor}: financial capacity, acquisition history, "
                    f"strategic rationale for {target_company}, likely bidding range"
                )

            # Strategic positioning
            urgent_tasks.extend([
                f"Strategic rationale analysis: Why {target_company} is valuable to each bidder",
                f"Synergy comparison: Our synergies vs competitor synergies with {target_company}",
                f"Financing advantage: Our cost of capital vs competitor financing capabilities",
                f"Bidding strategy: Optimal bid structure and timing to maximize win probability"
            ])

            print(f"\n[1] Urgent competitive analysis: {len(urgent_tasks)} tasks")
            print("   Strategy: SPEED optimization for 48-hour deadline")

            # Optimize for speed while maintaining quality
            optimize_response = await client.post(
                f"{BASE_URL}/v1beta/tasks/groups/optimize",
                params={
                    "strategy": "speed",  # Fast turnaround
                    "deduplicate": True,
                    "auto_retry": True
                },
                json=urgent_tasks
            )

            result = optimize_response.json()
            plan = result["optimization_plan"]

            print(f"\n[2] Optimization complete:")
            print(f"   Estimated duration: {plan['estimated_duration']}s ({plan['estimated_duration']/60:.1f} min)")
            print(f"   Estimated cost: ${plan['estimated_cost']:.4f}")
            print(f"   Processors: {plan['processor_distribution']}")

            print(f"\n[3] Executing time-critical analysis...")
            print("   Investment committee meeting in 48 hours")
            print("   Need competitive intelligence ASAP")

            return {
                "taskgroup_id": result["taskgroup_id"],
                "estimated_duration": plan["estimated_duration"] / 3600,  # Convert to hours
                "within_deadline": plan["estimated_duration"] < 48 * 3600
            }


async def main():
    """Run enterprise due diligence scenarios"""

    # Scenario setup
    target_company = "TechCorp Industries"
    deal_value = 500e6  # $500M
    competing_bidders = ["Strategic Acquirer A", "PE Firm B", "Strategic Acquirer C"]

    orchestrator = DueDiligenceOrchestrator(target_company, deal_value)

    try:
        # Scenario 1: Comprehensive due diligence
        print("\n🏢 SCENARIO 1: Comprehensive Due Diligence")
        print("=" * 80)

        dd_result = await orchestrator.comprehensive_due_diligence_workflow()

        if dd_result and dd_result["success"]:
            print("\n✅ Due diligence complete!")
            print(f"   Workflow: {dd_result['workflow_id']}")
            print(f"   Duration: {dd_result['duration_hours']:.2f} hours")
            print(f"   Tasks completed: {dd_result['total_tasks']}")
            print(f"   Timeline: Completed in {dd_result['duration_hours']/24:.1f} days (under {orchestrator.deadline_days} day deadline)")

        # Scenario 2: Competitive bidding
        await asyncio.sleep(2)
        print("\n\n⚡ SCENARIO 2: Competitive Bidding Analysis")
        print("=" * 80)

        competitive_result = await orchestrator.competitive_bidding_analysis(
            target_company,
            competing_bidders
        )

        if competitive_result:
            print("\n✅ Competitive analysis initiated!")
            print(f"   Task Group: {competitive_result['taskgroup_id']}")
            print(f"   Estimated: {competitive_result['estimated_duration']:.1f} hours")
            print(f"   Within 48hr deadline: {competitive_result['within_deadline']}")
            print("   Critical intelligence for bidding strategy")

        # Summary
        print("\n\n" + "=" * 80)
        print("ENTERPRISE DUE DILIGENCE SUMMARY")
        print("=" * 80)

        print("\n✅ Comprehensive Analysis Completed:")
        print("   • Financial: Quality of earnings, debt capacity")
        print("   • Legal: Contracts, litigation, IP, compliance")
        print("   • Market: Competitive position, trends, risks")
        print("   • Operational: Efficiency, supply chain, scalability")
        print("   • Technology: Stack assessment, technical debt")
        print("   • People: Management, culture, retention")
        print("   • Risk: Comprehensive risk assessment")
        print("   • Valuation: DCF, comps, synergies")
        print("   • Structure: Optimal deal terms")
        print("   • Recommendation: Investment thesis")

        print("\n💼 Business Value:")
        print(f"   • Deal size: ${deal_value/1e6:.0f}M")
        print("   • Due diligence timeline: 14 days → <1 day with automation")
        print("   • Cost: ~$500-1000 (vs $100k+ for traditional DD)")
        print("   • Quality: Premium processors for critical decisions")
        print("   • Speed: Parallel execution across all dimensions")
        print("   • Completeness: 30+ comprehensive analyses")

        print("\n🎯 PE/M&A Use Cases:")
        print("   • $100M+ acquisitions")
        print("   • Competitive bidding scenarios")
        print("   • Pre-LOI quick assessments")
        print("   • Post-LOI comprehensive DD")
        print("   • Investment committee materials")
        print("   • Integration planning")

        print("\n⚡ Speed Advantages:")
        print("   • Traditional DD: 4-8 weeks")
        print("   • With automation: 1-3 days")
        print("   • Competitive bidding: 48 hours")
        print("   • Parallel execution: 8+ analyses simultaneously")
        print("   • Real-time progress: Stakeholder visibility")

        print("\n💡 Risk Mitigation:")
        print("   • Comprehensive coverage (no blind spots)")
        print("   • Consistent methodology")
        print("   • Audit trail for all analyses")
        print("   • High-quality processors for critical items")
        print("   • Dependency management (build on prior findings)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
