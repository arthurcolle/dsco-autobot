#!/usr/bin/env python3
"""
Compound Tools Demo - DSPy pipeline-based tools.

These are differentiated, high-value tools built on top of
Search API and Advanced Search API with sophisticated prompting
and persistent state management.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tool_management.compound import (
    CompoundToolRegistry,
    get_compound_registry,
    # Research & Analysis
    DeepResearchPipeline,
    CompetitiveAnalysisPipeline,
    MarketResearchPipeline,
    # Content & Writing
    ContentBriefPipeline,
    FactCheckedWriterPipeline,
    # Data Extraction
    StructuredHarvesterPipeline,
    PriceTrackerPipeline,
    # Monitoring
    BrandMonitorPipeline,
    CompetitorWatchPipeline,
    # Verification
    ClaimVerifierPipeline,
    BiasDetectorPipeline,
    # Intelligence
    EntityMapperPipeline,
    TimelineBuilderPipeline,
)
from tool_management.compound.registry import COMPOUND_TOOLS_BY_CATEGORY


async def demo_registry():
    """Demonstrate the compound tool registry."""
    print("\n" + "=" * 70)
    print("COMPOUND TOOL REGISTRY")
    print("=" * 70)

    # Get the global registry
    registry = get_compound_registry()
    summary = registry.get_summary()

    print(f"\nTotal Compound Tools: {summary['total_tools']}")
    print("\nBy Category:")
    for category, tools in summary["categories"].items():
        print(f"  {category}: {len(tools)} tools")
        for tool in tools:
            print(f"    - {tool}")


async def demo_categories():
    """Show compound tools organized by category."""
    print("\n" + "=" * 70)
    print("COMPOUND TOOLS BY CATEGORY")
    print("=" * 70)

    for cat_id, category in COMPOUND_TOOLS_BY_CATEGORY.items():
        print(f"\n{category['name']}")
        print("-" * 40)
        print(f"{category['description']}\n")

        for tool in category["tools"]:
            print(f"  {tool['name']}")
            print(f"    {tool['description']}")
            print(f"    Use cases: {', '.join(tool['use_cases'])}")
            print()


async def demo_deep_research():
    """Demonstrate the Deep Research pipeline."""
    print("\n" + "=" * 70)
    print("DEEP RESEARCH PIPELINE DEMO")
    print("=" * 70)

    pipeline = DeepResearchPipeline()

    print(f"\nPipeline: {pipeline.name}")
    print(f"Description: {pipeline.description}")
    print(f"Steps: {len(pipeline.steps)}")
    print("\nStep breakdown:")
    for i, step in enumerate(pipeline.steps, 1):
        print(f"  {i}. {step.name}")
        print(f"     Signature: {step.signature.inputs} -> {step.signature.outputs}")
        if step.tool_calls:
            print(f"     Tools: {', '.join(step.tool_calls)}")

    print("\nExecuting with topic='AI Safety'...")
    state = await pipeline.execute(
        topic="AI Safety",
        depth="standard",
        focus_areas=["alignment", "governance", "technical approaches"],
    )

    print(f"\nExecution ID: {state.id}")
    print(f"Status: {state.status}")
    print(f"Steps completed: {state.current_step}/{state.total_steps}")
    print(f"Tool calls made: {len(state.tool_calls)}")

    if state.final_result:
        print(f"\nResult preview:")
        print(f"  Executive Summary: {state.final_result.get('executive_summary', '')[:100]}...")


async def demo_competitive_analysis():
    """Demonstrate the Competitive Analysis pipeline."""
    print("\n" + "=" * 70)
    print("COMPETITIVE ANALYSIS PIPELINE DEMO")
    print("=" * 70)

    pipeline = CompetitiveAnalysisPipeline()

    print(f"\nPipeline: {pipeline.name}")
    print(f"Input schema: {pipeline._get_input_schema()}")

    print("\nExecuting for company='Acme Corp'...")
    state = await pipeline.execute(
        company="Acme Corp",
        market="enterprise software",
        known_competitors=["Competitor A", "Competitor B", "Competitor C"],
    )

    print(f"\nStatus: {state.status}")
    print(f"Intermediate data stored: {list(state.intermediate_results.keys())}")


async def demo_content_brief():
    """Demonstrate the Content Brief pipeline."""
    print("\n" + "=" * 70)
    print("CONTENT BRIEF PIPELINE DEMO")
    print("=" * 70)

    pipeline = ContentBriefPipeline()

    print("\nExecuting for topic='Kubernetes Best Practices'...")
    state = await pipeline.execute(
        topic="Kubernetes Best Practices",
        target_audience="DevOps engineers",
        content_type="guide",
        word_count=3000,
    )

    print(f"\nStatus: {state.status}")
    if state.final_result:
        print(f"Recommended title: {state.final_result.get('recommended_title')}")


async def demo_claim_verification():
    """Demonstrate the Claim Verifier pipeline."""
    print("\n" + "=" * 70)
    print("CLAIM VERIFIER PIPELINE DEMO")
    print("=" * 70)

    pipeline = ClaimVerifierPipeline()

    print("\nVerifying claim: 'GPT-4 was released in March 2023'...")
    state = await pipeline.execute(
        claim="GPT-4 was released in March 2023",
        require_primary_sources=True,
    )

    print(f"\nStatus: {state.status}")
    print(f"Steps executed: {state.current_step}/{state.total_steps}")
    if state.final_result:
        print(f"Verdict: {state.final_result.get('verdict')}")
        print(f"Confidence: {state.final_result.get('confidence')}")


async def demo_brand_monitoring():
    """Demonstrate the Brand Monitor pipeline."""
    print("\n" + "=" * 70)
    print("BRAND MONITOR PIPELINE DEMO")
    print("=" * 70)

    pipeline = BrandMonitorPipeline()

    print("\nMonitoring brand='Distributed Systems'...")
    state = await pipeline.execute(
        brand="Distributed Systems",
        keywords=["API", "tools", "agents"],
        alert_threshold=-0.5,
    )

    print(f"\nStatus: {state.status}")
    print(f"Tool calls: {len(state.tool_calls)}")


async def demo_entity_mapping():
    """Demonstrate the Entity Mapper pipeline."""
    print("\n" + "=" * 70)
    print("ENTITY MAPPER PIPELINE DEMO")
    print("=" * 70)

    pipeline = EntityMapperPipeline()

    print("\nMapping entities: ['OpenAI', 'Anthropic', 'Google DeepMind']...")
    state = await pipeline.execute(
        seed_entities=["OpenAI", "Anthropic", "Google DeepMind"],
        relationship_types=["partnership", "competition", "investment"],
        max_depth=2,
    )

    print(f"\nStatus: {state.status}")
    if state.final_result:
        print(f"Entities mapped: {state.final_result.get('entities')}")


async def demo_execution_history():
    """Show execution history and state persistence."""
    print("\n" + "=" * 70)
    print("EXECUTION HISTORY & STATE PERSISTENCE")
    print("=" * 70)

    registry = get_compound_registry()

    # Execute a few pipelines
    await registry.execute("market_research", market="AI agents", region="US")
    await registry.execute("news_digest", topics=["AI", "LLMs"], max_stories=10)
    await registry.execute("trend_analysis", domain="enterprise AI")

    # List executions
    executions = registry.list_executions(limit=10)
    print(f"\nRecent executions: {len(executions)}")
    for exec in executions:
        print(f"  - {exec.pipeline_type}: {exec.status} ({exec.current_step}/{exec.total_steps} steps)")
        print(f"    ID: {exec.id}")
        print(f"    Created: {exec.created_at}")


async def main():
    """Run all demos."""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║           COMPOUND TOOLS - DSPy Pipeline Demonstration               ║
║                  Distributed Systems Platform                        ║
╠══════════════════════════════════════════════════════════════════════╣
║  24 differentiated tools built on Search API + Advanced Search API   ║
║  with sophisticated prompting and persistent state management        ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    await demo_registry()
    await demo_categories()
    await demo_deep_research()
    await demo_competitive_analysis()
    await demo_content_brief()
    await demo_claim_verification()
    await demo_brand_monitoring()
    await demo_entity_mapping()
    await demo_execution_history()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("""
Compound Tools provide 10x more value than raw API calls:

1. RESEARCH & ANALYSIS (6 tools)
   - Deep multi-step research with synthesis
   - Verified facts and sources

2. CONTENT & WRITING (4 tools)
   - Research-backed briefs
   - Fact-checked writing

3. DATA EXTRACTION (4 tools)
   - Structured data harvesting
   - Price and contact tracking

4. MONITORING & ALERTS (4 tools)
   - Brand mentions with sentiment
   - Competitor change detection

5. VERIFICATION & TRUST (4 tools)
   - Multi-source claim verification
   - Bias and plagiarism detection

6. INTELLIGENCE & SYNTHESIS (2 tools)
   - Entity relationship mapping
   - Timeline construction

All with:
- Persistent state for long-running operations
- Full execution history
- Cost tracking
- Agent-ready integration
""")


if __name__ == "__main__":
    asyncio.run(main())
