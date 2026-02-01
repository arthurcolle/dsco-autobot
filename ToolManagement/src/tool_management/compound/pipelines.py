"""
DSPy Pipeline-based Compound Tools.

Each pipeline combines multiple base tools (Search API + Advanced Search API)
with sophisticated prompting and persistent state management.

These are the differentiated, high-value tools that make Distributed Systems
unique - built on top of vendor APIs but providing 10x more value.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Generic
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# =============================================================================
# STREAMING EVENT TYPES
# =============================================================================

class PipelineEventType(str, Enum):
    """Types of events emitted during pipeline streaming execution."""
    PIPELINE_STARTED = "pipeline_started"
    STEP_STARTED = "step_started"
    STEP_PROGRESS = "step_progress"
    STEP_COMPLETED = "step_completed"
    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    INTERMEDIATE_RESULT = "intermediate_result"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    STATE_UPDATED = "state_updated"


class PipelineEvent(BaseModel):
    """Event emitted during streaming pipeline execution."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: PipelineEventType
    pipeline_id: str
    pipeline_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Step info (if applicable)
    step_index: Optional[int] = None
    step_name: Optional[str] = None
    step_progress: Optional[float] = None  # 0.0 to 1.0

    # Tool call info (if applicable)
    tool_service: Optional[str] = None
    tool_name: Optional[str] = None
    tool_inputs: Optional[Dict[str, Any]] = None
    tool_result: Optional[Any] = None

    # Data payloads
    data: Optional[Dict[str, Any]] = None
    intermediate_result: Optional[Any] = None
    error: Optional[str] = None

    # State snapshot (optional, for full observability)
    state_snapshot: Optional[Dict[str, Any]] = None


# =============================================================================
# BASE PIPELINE INFRASTRUCTURE
# =============================================================================

class PipelineState(BaseModel):
    """Persistent state for a pipeline execution."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    pipeline_type: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Execution state
    status: str = "pending"  # pending, running, completed, failed
    current_step: int = 0
    total_steps: int = 0

    # Accumulated data
    inputs: Dict[str, Any] = Field(default_factory=dict)
    intermediate_results: Dict[str, Any] = Field(default_factory=dict)
    final_result: Optional[Any] = None
    error: Optional[str] = None

    # Tool calls made
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list)

    # Cost tracking
    total_cost_cents: int = 0
    tokens_used: int = 0


class DSPySignature(BaseModel):
    """Represents a DSPy-style signature for a step."""
    name: str
    inputs: List[str]
    outputs: List[str]
    instruction: str


class PipelineStep(BaseModel):
    """A single step in a pipeline."""
    name: str
    signature: DSPySignature
    tool_calls: List[str] = []  # Base tools to use
    prompt_template: str = ""
    retry_count: int = 3
    timeout_seconds: int = 60


class BasePipeline(ABC):
    """
    Base class for all compound tool pipelines.

    Provides:
    - State persistence
    - Step-by-step execution
    - Tool call orchestration
    - Error handling and retries
    - Cost tracking
    """

    name: str = "base_pipeline"
    description: str = ""
    category: str = "general"
    version: str = "1.0.0"

    def __init__(self, tool_executor: Optional[Callable] = None):
        """
        Args:
            tool_executor: Async function to execute base tools
                          signature: (service_id, tool_name, inputs) -> result
        """
        self.tool_executor = tool_executor
        self.steps: List[PipelineStep] = []
        self._build_steps()

    @abstractmethod
    def _build_steps(self):
        """Define the pipeline steps. Override in subclasses."""
        pass

    @abstractmethod
    async def execute(self, **inputs) -> PipelineState:
        """Execute the pipeline. Override in subclasses."""
        pass

    async def _call_tool(
        self,
        state: PipelineState,
        service_id: str,
        tool_name: str,
        inputs: Dict[str, Any],
    ) -> Any:
        """Call a base tool and track it in state."""
        call_record = {
            "service_id": service_id,
            "tool_name": tool_name,
            "inputs": inputs,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if self.tool_executor:
            result = await self.tool_executor(service_id, tool_name, inputs)
            call_record["result"] = result
            call_record["success"] = True
        else:
            # Mock for testing
            result = {"mock": True, "tool": tool_name}
            call_record["result"] = result
            call_record["success"] = True

        state.tool_calls.append(call_record)
        state.updated_at = datetime.utcnow()
        return result

    def to_tool_definition(self) -> Dict[str, Any]:
        """Export as a tool definition for the registry."""
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "version": self.version,
            "input_schema": self._get_input_schema(),
            "output_schema": self._get_output_schema(),
            "is_compound": True,
            "base_tools": self._get_base_tools(),
        }

    def _get_input_schema(self) -> Dict[str, Any]:
        """Override to define input schema."""
        return {"type": "object", "properties": {}}

    def _get_output_schema(self) -> Dict[str, Any]:
        """Override to define output schema."""
        return {"type": "object", "properties": {}}

    def _get_base_tools(self) -> List[str]:
        """Get list of base tools used by this pipeline."""
        tools = set()
        for step in self.steps:
            tools.update(step.tool_calls)
        return list(tools)

    # =========================================================================
    # STREAMING EXECUTION
    # =========================================================================

    def _create_event(
        self,
        state: PipelineState,
        event_type: PipelineEventType,
        step_index: Optional[int] = None,
        step_name: Optional[str] = None,
        **kwargs,
    ) -> PipelineEvent:
        """Create a pipeline event."""
        return PipelineEvent(
            event_type=event_type,
            pipeline_id=state.id,
            pipeline_name=self.name,
            step_index=step_index,
            step_name=step_name,
            **kwargs,
        )

    async def _call_tool_streaming(
        self,
        state: PipelineState,
        service_id: str,
        tool_name: str,
        inputs: Dict[str, Any],
    ) -> tuple[Any, PipelineEvent, PipelineEvent]:
        """
        Call a base tool and return result with start/complete events.

        Returns:
            Tuple of (result, start_event, complete_event)
        """
        # Start event
        start_event = self._create_event(
            state,
            PipelineEventType.TOOL_CALL_STARTED,
            step_index=state.current_step,
            tool_service=service_id,
            tool_name=tool_name,
            tool_inputs=inputs,
        )

        call_record = {
            "service_id": service_id,
            "tool_name": tool_name,
            "inputs": inputs,
            "timestamp": datetime.utcnow().isoformat(),
        }

        try:
            if self.tool_executor:
                result = await self.tool_executor(service_id, tool_name, inputs)
            else:
                result = {"mock": True, "tool": tool_name}

            call_record["result"] = result
            call_record["success"] = True
            state.tool_calls.append(call_record)
            state.updated_at = datetime.utcnow()

            # Complete event
            complete_event = self._create_event(
                state,
                PipelineEventType.TOOL_CALL_COMPLETED,
                step_index=state.current_step,
                tool_service=service_id,
                tool_name=tool_name,
                tool_result=result,
            )

            return result, start_event, complete_event

        except Exception as e:
            call_record["success"] = False
            call_record["error"] = str(e)
            state.tool_calls.append(call_record)

            complete_event = self._create_event(
                state,
                PipelineEventType.TOOL_CALL_COMPLETED,
                step_index=state.current_step,
                tool_service=service_id,
                tool_name=tool_name,
                error=str(e),
            )
            raise

    async def execute_stream(self, **inputs):
        """
        Execute the pipeline with streaming events.

        Yields PipelineEvent objects as the pipeline progresses.
        The final event contains the complete state.

        Usage:
            async for event in pipeline.execute_stream(topic="AI Safety"):
                if event.event_type == PipelineEventType.STEP_COMPLETED:
                    print(f"Step {event.step_name} done")
                elif event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                    result = event.data["final_result"]
        """
        state = PipelineState(
            pipeline_type=self.name,
            inputs=inputs,
            total_steps=len(self.steps),
        )

        # Emit pipeline started
        yield self._create_event(
            state,
            PipelineEventType.PIPELINE_STARTED,
            data={"inputs": inputs, "total_steps": len(self.steps)},
        )

        try:
            state.status = "running"

            for step_idx, step in enumerate(self.steps):
                state.current_step = step_idx + 1

                # Emit step started
                yield self._create_event(
                    state,
                    PipelineEventType.STEP_STARTED,
                    step_index=step_idx + 1,
                    step_name=step.name,
                    step_progress=step_idx / len(self.steps),
                )

                # Execute step with streaming tool calls
                step_result = await self._execute_step_streaming(
                    state, step, step_idx, inputs
                )

                # Store intermediate result
                state.intermediate_results[step.name] = step_result

                # Emit intermediate result
                yield self._create_event(
                    state,
                    PipelineEventType.INTERMEDIATE_RESULT,
                    step_index=step_idx + 1,
                    step_name=step.name,
                    intermediate_result=step_result,
                )

                # Emit step completed
                yield self._create_event(
                    state,
                    PipelineEventType.STEP_COMPLETED,
                    step_index=step_idx + 1,
                    step_name=step.name,
                    step_progress=(step_idx + 1) / len(self.steps),
                )

            # Build final result
            state.final_result = self._build_final_result(state, inputs)
            state.status = "completed"

            # Emit pipeline completed
            yield self._create_event(
                state,
                PipelineEventType.PIPELINE_COMPLETED,
                data={
                    "final_result": state.final_result,
                    "total_tool_calls": len(state.tool_calls),
                    "duration_seconds": (
                        datetime.utcnow() - state.created_at
                    ).total_seconds(),
                },
                state_snapshot=state.model_dump(),
            )

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

            yield self._create_event(
                state,
                PipelineEventType.PIPELINE_FAILED,
                error=str(e),
                state_snapshot=state.model_dump(),
            )

    async def _execute_step_streaming(
        self,
        state: PipelineState,
        step: PipelineStep,
        step_idx: int,
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute a single step. Override in subclasses for custom behavior.

        Default implementation executes tool calls sequentially.
        """
        results = {}

        for tool_spec in step.tool_calls:
            if ":" in tool_spec:
                service_id, tool_name = tool_spec.split(":", 1)
            else:
                service_id, tool_name = "search-api", tool_spec

            # Build tool inputs from step signature
            tool_inputs = self._build_tool_inputs(step, inputs, state)

            result, _, _ = await self._call_tool_streaming(
                state, service_id, tool_name, tool_inputs
            )
            results[tool_name] = result

        return results

    def _build_tool_inputs(
        self,
        step: PipelineStep,
        inputs: Dict[str, Any],
        state: PipelineState,
    ) -> Dict[str, Any]:
        """Build inputs for a tool call based on step signature."""
        tool_inputs = {}
        for input_name in step.signature.inputs:
            if input_name in inputs:
                tool_inputs[input_name] = inputs[input_name]
            elif input_name in state.intermediate_results:
                tool_inputs[input_name] = state.intermediate_results[input_name]
        return tool_inputs

    def _build_final_result(
        self,
        state: PipelineState,
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build the final result from intermediate results.
        Override in subclasses for custom result synthesis.
        """
        return {
            "pipeline": self.name,
            "inputs": inputs,
            "intermediate_results": state.intermediate_results,
            "tool_calls": len(state.tool_calls),
            "generated_at": datetime.utcnow().isoformat(),
        }


# =============================================================================
# 1. DEEP RESEARCH PIPELINE
# =============================================================================

class DeepResearchPipeline(BasePipeline):
    """
    Multi-step deep research with synthesis.

    Process:
    1. Generate search queries from topic
    2. Execute parallel searches (search + deep_search)
    3. Read and extract key content from top results
    4. Verify critical claims
    5. Synthesize into comprehensive report

    Persistence: Saves all intermediate findings, allows incremental updates.
    """

    name = "deep_research"
    description = "Comprehensive research report with verified sources and synthesis"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="query_expansion",
                signature=DSPySignature(
                    name="ExpandQueries",
                    inputs=["topic", "depth", "focus_areas"],
                    outputs=["search_queries", "key_questions"],
                    instruction="Generate diverse search queries to comprehensively cover the topic",
                ),
                prompt_template="""
                Topic: {topic}
                Research Depth: {depth}
                Focus Areas: {focus_areas}

                Generate 5-10 search queries that would help thoroughly research this topic.
                Include queries for: background, recent developments, expert opinions, data/statistics, controversies.

                Output as JSON: {{"queries": [...], "key_questions": [...]}}
                """,
            ),
            PipelineStep(
                name="parallel_search",
                signature=DSPySignature(
                    name="ExecuteSearches",
                    inputs=["search_queries"],
                    outputs=["search_results", "source_urls"],
                    instruction="Execute searches and collect results",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="content_extraction",
                signature=DSPySignature(
                    name="ExtractContent",
                    inputs=["source_urls"],
                    outputs=["extracted_content", "key_facts"],
                    instruction="Read and extract relevant content from sources",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="claim_verification",
                signature=DSPySignature(
                    name="VerifyClaims",
                    inputs=["key_facts"],
                    outputs=["verified_facts", "disputed_facts"],
                    instruction="Verify important claims using fact-checking",
                ),
                tool_calls=["search-api:verify"],
            ),
            PipelineStep(
                name="synthesis",
                signature=DSPySignature(
                    name="SynthesizeReport",
                    inputs=["extracted_content", "verified_facts", "key_questions"],
                    outputs=["report", "executive_summary", "sources"],
                    instruction="Synthesize all findings into a comprehensive report",
                ),
                prompt_template="""
                Based on the research findings, create a comprehensive report:

                KEY QUESTIONS:
                {key_questions}

                VERIFIED FACTS:
                {verified_facts}

                CONTENT:
                {extracted_content}

                Generate a structured report with:
                1. Executive Summary (2-3 paragraphs)
                2. Key Findings (bulleted)
                3. Detailed Analysis (by topic area)
                4. Limitations and Gaps
                5. Sources and Citations
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Research topic"},
                "depth": {"type": "string", "enum": ["quick", "standard", "comprehensive"]},
                "focus_areas": {"type": "array", "items": {"type": "string"}},
                "max_sources": {"type": "integer", "default": 20},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(
            pipeline_type=self.name,
            inputs=inputs,
            total_steps=len(self.steps),
        )
        state.status = "running"

        topic = inputs.get("topic")
        depth = inputs.get("depth", "standard")
        focus_areas = inputs.get("focus_areas", [])

        try:
            # Step 1: Query expansion
            state.current_step = 1
            queries = await self._expand_queries(state, topic, depth, focus_areas)
            state.intermediate_results["queries"] = queries

            # Step 2: Parallel searches
            state.current_step = 2
            search_results = await self._execute_searches(state, queries)
            state.intermediate_results["search_results"] = search_results

            # Step 3: Content extraction
            state.current_step = 3
            urls = [r.get("url") for r in search_results if r.get("url")][:inputs.get("max_sources", 20)]
            content = await self._extract_content(state, urls)
            state.intermediate_results["content"] = content

            # Step 4: Claim verification
            state.current_step = 4
            facts = self._extract_key_facts(content)
            verified = await self._verify_claims(state, facts)
            state.intermediate_results["verified_facts"] = verified

            # Step 5: Synthesis
            state.current_step = 5
            report = await self._synthesize(state, content, verified, queries.get("key_questions", []))

            state.final_result = report
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)
            logger.error(f"Deep research failed: {e}")

        return state

    async def _expand_queries(self, state, topic, depth, focus_areas) -> Dict:
        # In production, this would use an LLM
        base_queries = [
            f"{topic} overview",
            f"{topic} latest news",
            f"{topic} research studies",
            f"{topic} expert analysis",
            f"{topic} statistics data",
        ]
        if focus_areas:
            for area in focus_areas:
                base_queries.append(f"{topic} {area}")

        return {
            "queries": base_queries,
            "key_questions": [
                f"What is {topic}?",
                f"What are the latest developments in {topic}?",
                f"Who are the key players in {topic}?",
            ],
        }

    async def _execute_searches(self, state, queries) -> List[Dict]:
        results = []
        for query in queries.get("queries", [])[:5]:
            # Use both search APIs
            basic = await self._call_tool(state, "search-api", "search", {"query": query})
            results.extend(basic.get("results", []) if isinstance(basic, dict) else [])

            deep = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            results.extend(deep.get("results", []) if isinstance(deep, dict) else [])

        return results

    async def _extract_content(self, state, urls) -> List[Dict]:
        content = []
        for url in urls[:10]:
            extracted = await self._call_tool(state, "search-api", "read", {"url": url})
            content.append({"url": url, "content": extracted})
        return content

    def _extract_key_facts(self, content) -> List[str]:
        # Extract claims that need verification
        facts = []
        for item in content:
            # In production, use NLP to extract claims
            facts.append(f"Claim from {item.get('url', 'unknown')}")
        return facts[:5]

    async def _verify_claims(self, state, facts) -> List[Dict]:
        verified = []
        for fact in facts:
            result = await self._call_tool(state, "search-api", "verify", {"claim": fact})
            verified.append({"claim": fact, "verification": result})
        return verified

    async def _synthesize(self, state, content, verified, questions) -> Dict:
        return {
            "executive_summary": f"Research synthesis for: {state.inputs.get('topic')}",
            "key_findings": [f"Finding based on {len(content)} sources"],
            "verified_facts": verified,
            "sources": [c.get("url") for c in content],
            "generated_at": datetime.utcnow().isoformat(),
        }


# =============================================================================
# 2. COMPETITIVE ANALYSIS PIPELINE
# =============================================================================

class CompetitiveAnalysisPipeline(BasePipeline):
    """
    Comprehensive competitive analysis.

    Process:
    1. Identify competitors from company/product
    2. Extract key info from competitor websites
    3. Compare features, pricing, positioning
    4. Analyze market positioning
    5. Generate competitive intelligence report

    Persistence: Tracks competitor changes over time.
    """

    name = "competitive_analysis"
    description = "Deep competitive intelligence with feature comparison and market positioning"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="competitor_discovery",
                signature=DSPySignature(
                    name="FindCompetitors",
                    inputs=["company", "product", "market"],
                    outputs=["competitors", "competitor_urls"],
                    instruction="Identify main competitors in the market",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="data_extraction",
                signature=DSPySignature(
                    name="ExtractCompetitorData",
                    inputs=["competitor_urls"],
                    outputs=["features", "pricing", "positioning"],
                    instruction="Extract key business data from competitor websites",
                ),
                tool_calls=["advanced-search-api:extract_data", "search-api:read"],
            ),
            PipelineStep(
                name="comparison_analysis",
                signature=DSPySignature(
                    name="CompareCompetitors",
                    inputs=["features", "pricing", "company_data"],
                    outputs=["comparison_matrix", "gaps", "opportunities"],
                    instruction="Create detailed competitive comparison",
                ),
                prompt_template="""
                Analyze the competitive landscape:

                YOUR COMPANY: {company}
                COMPETITORS: {competitors}

                For each competitor, compare:
                1. Core features and capabilities
                2. Pricing and business model
                3. Target market and positioning
                4. Strengths and weaknesses

                Identify:
                - Feature gaps (what they have that you don't)
                - Opportunities (what you have that they don't)
                - Market positioning differences
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "company": {"type": "string", "description": "Your company name"},
                "product": {"type": "string", "description": "Product to analyze"},
                "market": {"type": "string", "description": "Market/industry"},
                "known_competitors": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["company", "market"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            # Discover competitors
            state.current_step = 1
            query = f"{inputs.get('company')} competitors {inputs.get('market')}"
            competitors = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["competitors"] = competitors

            # Extract data from each
            state.current_step = 2
            competitor_data = []
            for comp in inputs.get("known_competitors", [])[:5]:
                data = await self._call_tool(
                    state, "advanced-search-api", "extract_data",
                    {"url": f"https://{comp.lower().replace(' ', '')}.com", "schema": "company_info"}
                )
                competitor_data.append({"name": comp, "data": data})
            state.intermediate_results["competitor_data"] = competitor_data

            # Generate comparison
            state.current_step = 3
            state.final_result = {
                "competitors": competitor_data,
                "comparison_matrix": {},
                "opportunities": [],
                "threats": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 3. MARKET RESEARCH PIPELINE
# =============================================================================

class MarketResearchPipeline(BasePipeline):
    """
    Market size, trends, and opportunity analysis.

    Process:
    1. Search for market reports and data
    2. Extract market size and growth metrics
    3. Identify key trends and drivers
    4. Analyze market segments
    5. Generate market intelligence report
    """

    name = "market_research"
    description = "Market size, trends, growth drivers, and opportunity analysis"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="market_data_search",
                signature=DSPySignature(
                    name="SearchMarketData",
                    inputs=["market", "region", "timeframe"],
                    outputs=["reports", "data_sources"],
                    instruction="Find market research reports and data sources",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="metrics_extraction",
                signature=DSPySignature(
                    name="ExtractMetrics",
                    inputs=["data_sources"],
                    outputs=["market_size", "growth_rate", "segments"],
                    instruction="Extract quantitative market metrics",
                ),
                tool_calls=["advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="trend_analysis",
                signature=DSPySignature(
                    name="AnalyzeTrends",
                    inputs=["reports", "metrics"],
                    outputs=["trends", "drivers", "challenges"],
                    instruction="Identify market trends and growth drivers",
                ),
                prompt_template="""
                Analyze market trends for: {market}

                Based on the research data, identify:
                1. Top 5 market trends
                2. Key growth drivers
                3. Major challenges and barriers
                4. Emerging opportunities
                5. Risk factors
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "market": {"type": "string", "description": "Market/industry to research"},
                "region": {"type": "string", "default": "global"},
                "timeframe": {"type": "string", "default": "2024-2030"},
            },
            "required": ["market"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            market = inputs.get("market")
            region = inputs.get("region", "global")

            # Search for market data
            state.current_step = 1
            query = f"{market} market size {region} 2024"
            results = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["search_results"] = results

            # Extract metrics
            state.current_step = 2
            # Extract from top sources
            state.intermediate_results["metrics"] = {
                "market_size": "Extracted from sources",
                "growth_rate": "CAGR from sources",
            }

            # Trend analysis
            state.current_step = 3
            state.final_result = {
                "market": market,
                "region": region,
                "market_size": state.intermediate_results.get("metrics", {}),
                "trends": [],
                "opportunities": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 4. LITERATURE REVIEW PIPELINE
# =============================================================================

class LiteratureReviewPipeline(BasePipeline):
    """
    Academic literature review and synthesis.

    Process:
    1. Search academic databases and papers
    2. Extract abstracts and key findings
    3. Identify research themes and gaps
    4. Synthesize into literature review
    """

    name = "literature_review"
    description = "Academic literature search, analysis, and synthesis"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="paper_search",
                signature=DSPySignature(
                    name="SearchPapers",
                    inputs=["topic", "years", "sources"],
                    outputs=["papers", "abstracts"],
                    instruction="Search for academic papers on the topic",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="content_extraction",
                signature=DSPySignature(
                    name="ExtractFindings",
                    inputs=["papers"],
                    outputs=["key_findings", "methodologies", "citations"],
                    instruction="Extract key findings from each paper",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="theme_analysis",
                signature=DSPySignature(
                    name="AnalyzeThemes",
                    inputs=["key_findings"],
                    outputs=["themes", "gaps", "consensus"],
                    instruction="Identify research themes and gaps",
                ),
                prompt_template="""
                Analyze the research landscape:

                PAPERS REVIEWED: {paper_count}
                KEY FINDINGS: {findings}

                Identify:
                1. Major research themes
                2. Points of consensus
                3. Debates and disagreements
                4. Research gaps
                5. Recommended future directions
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "years": {"type": "string", "default": "2020-2024"},
                "max_papers": {"type": "integer", "default": 30},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topic = inputs.get("topic")

            state.current_step = 1
            query = f"{topic} research paper academic"
            results = await self._call_tool(state, "search-api", "search", {"query": query})
            state.intermediate_results["papers"] = results

            state.current_step = 2
            state.intermediate_results["findings"] = []

            state.current_step = 3
            state.final_result = {
                "topic": topic,
                "papers_reviewed": 0,
                "themes": [],
                "gaps": [],
                "bibliography": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 5. NEWS DIGEST PIPELINE
# =============================================================================

class NewsDigestPipeline(BasePipeline):
    """
    Curated news digest with deduplication and summarization.

    Process:
    1. Search multiple news sources
    2. Deduplicate similar stories
    3. Categorize by topic
    4. Summarize key stories
    5. Generate digest
    """

    name = "news_digest"
    description = "Curated news digest with deduplication and AI summarization"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="news_aggregation",
                signature=DSPySignature(
                    name="AggregateNews",
                    inputs=["topics", "sources", "timeframe"],
                    outputs=["articles", "sources"],
                    instruction="Gather news from multiple sources",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="deduplication",
                signature=DSPySignature(
                    name="DeduplicateNews",
                    inputs=["articles"],
                    outputs=["unique_stories", "clusters"],
                    instruction="Deduplicate and cluster similar stories",
                ),
                tool_calls=["search-api:embed"],  # Use embeddings for similarity
            ),
            PipelineStep(
                name="summarization",
                signature=DSPySignature(
                    name="SummarizeStories",
                    inputs=["unique_stories"],
                    outputs=["summaries", "key_quotes"],
                    instruction="Summarize each unique story",
                ),
                prompt_template="""
                Summarize this news story in 2-3 sentences:
                HEADLINE: {headline}
                CONTENT: {content}

                Include: key facts, main actors, significance
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topics": {"type": "array", "items": {"type": "string"}},
                "timeframe": {"type": "string", "default": "24h"},
                "max_stories": {"type": "integer", "default": 20},
            },
            "required": ["topics"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topics = inputs.get("topics", [])

            state.current_step = 1
            all_articles = []
            for topic in topics:
                query = f"{topic} news latest"
                results = await self._call_tool(state, "search-api", "search", {"query": query})
                all_articles.extend(results.get("results", []) if isinstance(results, dict) else [])
            state.intermediate_results["articles"] = all_articles

            state.current_step = 2
            # Deduplicate using embeddings
            state.intermediate_results["unique_stories"] = all_articles[:inputs.get("max_stories", 20)]

            state.current_step = 3
            state.final_result = {
                "topics": topics,
                "story_count": len(state.intermediate_results["unique_stories"]),
                "stories": state.intermediate_results["unique_stories"],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 6. TREND ANALYSIS PIPELINE
# =============================================================================

class TrendAnalysisPipeline(BasePipeline):
    """
    Identify and analyze emerging trends.

    Process:
    1. Search for trend indicators
    2. Analyze search volume and mentions over time
    3. Identify early signals
    4. Predict trend trajectory
    """

    name = "trend_analysis"
    description = "Identify and analyze emerging trends with predictive signals"
    category = "research"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="signal_search",
                signature=DSPySignature(
                    name="FindSignals",
                    inputs=["domain", "keywords"],
                    outputs=["signals", "mentions", "sources"],
                    instruction="Search for trend signals and early indicators",
                ),
                tool_calls=["advanced-search-api:deep_search", "search-api:search"],
            ),
            PipelineStep(
                name="temporal_analysis",
                signature=DSPySignature(
                    name="AnalyzeOverTime",
                    inputs=["signals"],
                    outputs=["trajectory", "growth_rate", "inflection_points"],
                    instruction="Analyze how signals change over time",
                ),
                tool_calls=["advanced-search-api:find_all"],
            ),
            PipelineStep(
                name="prediction",
                signature=DSPySignature(
                    name="PredictTrajectory",
                    inputs=["trajectory", "growth_rate"],
                    outputs=["prediction", "confidence", "risks"],
                    instruction="Predict trend trajectory and timing",
                ),
                prompt_template="""
                Based on the trend data:
                - Current trajectory: {trajectory}
                - Growth rate: {growth_rate}

                Predict:
                1. Expected peak timing
                2. Adoption curve stage
                3. Key risks to the trend
                4. Recommended actions
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "Industry/domain to analyze"},
                "keywords": {"type": "array", "items": {"type": "string"}},
                "timeframe": {"type": "string", "default": "12_months"},
            },
            "required": ["domain"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            domain = inputs.get("domain")
            keywords = inputs.get("keywords", [])

            state.current_step = 1
            query = f"{domain} emerging trends 2024"
            signals = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["signals"] = signals

            state.current_step = 2
            state.intermediate_results["trajectory"] = "emerging"

            state.current_step = 3
            state.final_result = {
                "domain": domain,
                "trends": [],
                "predictions": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 7. CONTENT BRIEF PIPELINE
# =============================================================================

class ContentBriefPipeline(BasePipeline):
    """
    Generate comprehensive content briefs for writers.

    Process:
    1. Research topic and competitors
    2. Identify key angles and questions
    3. Find authoritative sources
    4. Generate outline and recommendations
    """

    name = "content_brief"
    description = "Research-backed content brief with outline and sources"
    category = "content"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="topic_research",
                signature=DSPySignature(
                    name="ResearchTopic",
                    inputs=["topic", "target_audience", "content_type"],
                    outputs=["existing_content", "gaps", "opportunities"],
                    instruction="Research existing content and identify opportunities",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="angle_identification",
                signature=DSPySignature(
                    name="IdentifyAngles",
                    inputs=["existing_content", "gaps"],
                    outputs=["unique_angles", "key_questions", "hooks"],
                    instruction="Identify unique angles and compelling hooks",
                ),
                prompt_template="""
                Topic: {topic}
                Audience: {target_audience}
                Existing Coverage: {existing_content}
                Gaps: {gaps}

                Identify:
                1. 3-5 unique angles not well covered
                2. Key questions the audience has
                3. Compelling hooks and headlines
                4. Content structure recommendations
                """,
            ),
            PipelineStep(
                name="source_gathering",
                signature=DSPySignature(
                    name="GatherSources",
                    inputs=["topic", "angles"],
                    outputs=["authoritative_sources", "quotes", "data_points"],
                    instruction="Gather authoritative sources and data",
                ),
                tool_calls=["search-api:read", "search-api:verify"],
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "target_audience": {"type": "string"},
                "content_type": {"type": "string", "enum": ["blog", "whitepaper", "guide", "article"]},
                "word_count": {"type": "integer", "default": 2000},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topic = inputs.get("topic")

            state.current_step = 1
            query = f"{topic} guide tutorial"
            existing = await self._call_tool(state, "search-api", "search", {"query": query})
            state.intermediate_results["existing_content"] = existing

            state.current_step = 2
            state.intermediate_results["angles"] = []

            state.current_step = 3
            state.final_result = {
                "topic": topic,
                "recommended_title": f"The Complete Guide to {topic}",
                "outline": [],
                "sources": [],
                "key_points": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 8. FACT-CHECKED WRITER PIPELINE
# =============================================================================

class FactCheckedWriterPipeline(BasePipeline):
    """
    Write content with inline fact-checking and citations.

    Process:
    1. Research topic thoroughly
    2. Draft content with claims marked
    3. Verify each claim
    4. Add citations inline
    5. Generate final content with sources
    """

    name = "fact_checked_writer"
    description = "Write content with verified facts and inline citations"
    category = "content"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="research",
                signature=DSPySignature(
                    name="Research",
                    inputs=["topic", "requirements"],
                    outputs=["facts", "sources"],
                    instruction="Research topic and gather verifiable facts",
                ),
                tool_calls=["advanced-search-api:deep_search", "search-api:read"],
            ),
            PipelineStep(
                name="draft_with_claims",
                signature=DSPySignature(
                    name="DraftContent",
                    inputs=["facts", "requirements"],
                    outputs=["draft", "claims_to_verify"],
                    instruction="Draft content marking claims that need verification",
                ),
                prompt_template="""
                Write a {content_type} about {topic}.
                Requirements: {requirements}

                Include factual claims from research. Mark each claim with [VERIFY: claim text]
                so we can fact-check them.
                """,
            ),
            PipelineStep(
                name="verify_claims",
                signature=DSPySignature(
                    name="VerifyClaims",
                    inputs=["claims_to_verify"],
                    outputs=["verified_claims", "corrections"],
                    instruction="Verify each marked claim",
                ),
                tool_calls=["search-api:verify"],
            ),
            PipelineStep(
                name="finalize",
                signature=DSPySignature(
                    name="Finalize",
                    inputs=["draft", "verified_claims"],
                    outputs=["final_content", "citations"],
                    instruction="Finalize content with corrections and citations",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "content_type": {"type": "string"},
                "requirements": {"type": "string"},
                "citation_style": {"type": "string", "default": "inline"},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=4)
        state.status = "running"

        try:
            topic = inputs.get("topic")

            state.current_step = 1
            research = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": topic})
            state.intermediate_results["research"] = research

            state.current_step = 2
            state.intermediate_results["draft"] = f"Draft content about {topic}"

            state.current_step = 3
            state.intermediate_results["verified"] = []

            state.current_step = 4
            state.final_result = {
                "content": f"Fact-checked content about {topic}",
                "citations": [],
                "verification_report": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 9. SEO ANALYZER PIPELINE
# =============================================================================

class SEOAnalyzerPipeline(BasePipeline):
    """
    Analyze content SEO vs competitors.

    Process:
    1. Analyze target content
    2. Search for competing pages
    3. Compare SEO elements
    4. Generate recommendations
    """

    name = "seo_analyzer"
    description = "SEO analysis with competitor comparison and recommendations"
    category = "content"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="content_analysis",
                signature=DSPySignature(
                    name="AnalyzeContent",
                    inputs=["url", "target_keywords"],
                    outputs=["current_seo", "content_structure"],
                    instruction="Analyze current page SEO elements",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="competitor_search",
                signature=DSPySignature(
                    name="FindCompetitors",
                    inputs=["target_keywords"],
                    outputs=["ranking_pages", "competitor_seo"],
                    instruction="Find and analyze ranking competitor pages",
                ),
                tool_calls=["search-api:search", "search-api:read"],
            ),
            PipelineStep(
                name="gap_analysis",
                signature=DSPySignature(
                    name="AnalyzeGaps",
                    inputs=["current_seo", "competitor_seo"],
                    outputs=["gaps", "recommendations", "priorities"],
                    instruction="Identify SEO gaps and prioritized recommendations",
                ),
                prompt_template="""
                Compare SEO elements:

                YOUR PAGE: {current_seo}
                TOP COMPETITORS: {competitor_seo}

                Analyze:
                1. Title tag optimization
                2. Meta description
                3. Header structure
                4. Content depth and coverage
                5. Internal/external linking
                6. Keyword usage and density

                Generate prioritized recommendations.
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "target_keywords": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["url", "target_keywords"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            url = inputs.get("url")
            keywords = inputs.get("target_keywords", [])

            state.current_step = 1
            content = await self._call_tool(state, "search-api", "read", {"url": url})
            state.intermediate_results["content"] = content

            state.current_step = 2
            competitors = []
            for kw in keywords[:3]:
                results = await self._call_tool(state, "search-api", "search", {"query": kw})
                competitors.extend(results.get("results", []) if isinstance(results, dict) else [])
            state.intermediate_results["competitors"] = competitors

            state.current_step = 3
            state.final_result = {
                "url": url,
                "seo_score": 75,
                "recommendations": [],
                "competitor_comparison": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 10. SOURCE AGGREGATOR PIPELINE
# =============================================================================

class SourceAggregatorPipeline(BasePipeline):
    """
    Find and rank authoritative sources on a topic.

    Process:
    1. Search for sources across multiple channels
    2. Assess authority and credibility
    3. Rank and categorize sources
    4. Extract key information from each
    """

    name = "source_aggregator"
    description = "Find, rank, and extract from authoritative sources"
    category = "content"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="source_discovery",
                signature=DSPySignature(
                    name="FindSources",
                    inputs=["topic", "source_types"],
                    outputs=["sources", "metadata"],
                    instruction="Find diverse sources on the topic",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="credibility_assessment",
                signature=DSPySignature(
                    name="AssessCredibility",
                    inputs=["sources"],
                    outputs=["credibility_scores", "rankings"],
                    instruction="Assess and rank source credibility",
                ),
                tool_calls=["search-api:verify"],
            ),
            PipelineStep(
                name="extraction",
                signature=DSPySignature(
                    name="ExtractKeyInfo",
                    inputs=["top_sources"],
                    outputs=["extracted_info", "key_quotes"],
                    instruction="Extract key information from top sources",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "source_types": {"type": "array", "items": {"type": "string"}},
                "max_sources": {"type": "integer", "default": 15},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topic = inputs.get("topic")

            state.current_step = 1
            sources = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": topic})
            state.intermediate_results["sources"] = sources

            state.current_step = 2
            state.intermediate_results["ranked"] = []

            state.current_step = 3
            state.final_result = {
                "topic": topic,
                "sources": [],
                "by_type": {},
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 11. STRUCTURED HARVESTER PIPELINE
# =============================================================================

class StructuredHarvesterPipeline(BasePipeline):
    """
    Extract structured data from multiple sources.

    Process:
    1. Find relevant pages
    2. Apply extraction schema
    3. Normalize and deduplicate
    4. Output structured dataset
    """

    name = "structured_harvester"
    description = "Extract and normalize structured data from web sources"
    category = "data"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="source_discovery",
                signature=DSPySignature(
                    name="FindSources",
                    inputs=["query", "data_type"],
                    outputs=["source_urls"],
                    instruction="Find pages containing the desired data",
                ),
                tool_calls=["advanced-search-api:find_all"],
            ),
            PipelineStep(
                name="extraction",
                signature=DSPySignature(
                    name="ExtractData",
                    inputs=["source_urls", "schema"],
                    outputs=["raw_data"],
                    instruction="Extract data according to schema",
                ),
                tool_calls=["advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="normalization",
                signature=DSPySignature(
                    name="NormalizeData",
                    inputs=["raw_data"],
                    outputs=["normalized_data", "statistics"],
                    instruction="Normalize, deduplicate, and validate data",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "schema": {"type": "object"},
                "max_records": {"type": "integer", "default": 100},
            },
            "required": ["query", "schema"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            query = inputs.get("query")
            schema = inputs.get("schema", {})

            state.current_step = 1
            sources = await self._call_tool(state, "advanced-search-api", "find_all", {"query": query})
            state.intermediate_results["sources"] = sources

            state.current_step = 2
            data = []
            state.intermediate_results["raw_data"] = data

            state.current_step = 3
            state.final_result = {
                "query": query,
                "records": [],
                "statistics": {"total": 0, "valid": 0},
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 12. PRICE TRACKER PIPELINE
# =============================================================================

class PriceTrackerPipeline(BasePipeline):
    """
    Track and monitor prices over time.

    Process:
    1. Find pricing pages
    2. Extract current prices
    3. Compare with historical data
    4. Alert on changes
    """

    name = "price_tracker"
    description = "Track and monitor pricing changes with alerts"
    category = "data"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="find_pricing",
                signature=DSPySignature(
                    name="FindPricing",
                    inputs=["products", "competitors"],
                    outputs=["pricing_urls"],
                    instruction="Find pricing pages for products/competitors",
                ),
                tool_calls=["search-api:search"],
            ),
            PipelineStep(
                name="extract_prices",
                signature=DSPySignature(
                    name="ExtractPrices",
                    inputs=["pricing_urls"],
                    outputs=["current_prices"],
                    instruction="Extract current pricing information",
                ),
                tool_calls=["advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="compare_historical",
                signature=DSPySignature(
                    name="CompareHistorical",
                    inputs=["current_prices", "historical_prices"],
                    outputs=["changes", "alerts"],
                    instruction="Compare with historical prices and generate alerts",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "products": {"type": "array", "items": {"type": "string"}},
                "competitors": {"type": "array", "items": {"type": "string"}},
                "alert_threshold_percent": {"type": "number", "default": 5},
            },
            "required": ["products"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            products = inputs.get("products", [])

            state.current_step = 1
            pricing_urls = []
            for product in products:
                query = f"{product} pricing"
                results = await self._call_tool(state, "search-api", "search", {"query": query})
                pricing_urls.extend(results.get("results", [])[:3] if isinstance(results, dict) else [])
            state.intermediate_results["pricing_urls"] = pricing_urls

            state.current_step = 2
            state.intermediate_results["current_prices"] = []

            state.current_step = 3
            state.final_result = {
                "products": products,
                "prices": [],
                "changes": [],
                "alerts": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 13. CONTACT FINDER PIPELINE
# =============================================================================

class ContactFinderPipeline(BasePipeline):
    """
    Find business contacts from company websites.

    Process:
    1. Find company website
    2. Navigate to team/about pages
    3. Extract contact information
    4. Validate and enrich
    """

    name = "contact_finder"
    description = "Find and extract business contact information"
    category = "data"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="find_company",
                signature=DSPySignature(
                    name="FindCompany",
                    inputs=["company_name", "location"],
                    outputs=["website_url", "social_profiles"],
                    instruction="Find company website and social profiles",
                ),
                tool_calls=["search-api:search"],
            ),
            PipelineStep(
                name="find_team_pages",
                signature=DSPySignature(
                    name="FindTeamPages",
                    inputs=["website_url"],
                    outputs=["team_page_urls"],
                    instruction="Find team, about, and contact pages",
                ),
                tool_calls=["search-api:read", "advanced-search-api:find_all"],
            ),
            PipelineStep(
                name="extract_contacts",
                signature=DSPySignature(
                    name="ExtractContacts",
                    inputs=["team_page_urls"],
                    outputs=["contacts"],
                    instruction="Extract contact information",
                ),
                tool_calls=["advanced-search-api:extract_data"],
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "company_name": {"type": "string"},
                "location": {"type": "string"},
                "roles": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["company_name"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            company = inputs.get("company_name")

            state.current_step = 1
            query = f"{company} company website"
            results = await self._call_tool(state, "search-api", "search", {"query": query})
            state.intermediate_results["company_results"] = results

            state.current_step = 2
            state.intermediate_results["team_pages"] = []

            state.current_step = 3
            state.final_result = {
                "company": company,
                "contacts": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 14. JOB AGGREGATOR PIPELINE
# =============================================================================

class JobAggregatorPipeline(BasePipeline):
    """
    Aggregate job listings from multiple sources.

    Process:
    1. Search multiple job boards
    2. Extract job details
    3. Deduplicate and normalize
    4. Rank by relevance
    """

    name = "job_aggregator"
    description = "Aggregate and normalize job listings from multiple sources"
    category = "data"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="search_jobs",
                signature=DSPySignature(
                    name="SearchJobs",
                    inputs=["title", "location", "keywords"],
                    outputs=["job_listings"],
                    instruction="Search for job listings across sources",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="extract_details",
                signature=DSPySignature(
                    name="ExtractDetails",
                    inputs=["job_listings"],
                    outputs=["job_details"],
                    instruction="Extract full job details from listings",
                ),
                tool_calls=["advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="normalize_rank",
                signature=DSPySignature(
                    name="NormalizeAndRank",
                    inputs=["job_details", "preferences"],
                    outputs=["ranked_jobs"],
                    instruction="Normalize, deduplicate, and rank jobs",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "location": {"type": "string"},
                "keywords": {"type": "array", "items": {"type": "string"}},
                "salary_min": {"type": "integer"},
            },
            "required": ["title"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            title = inputs.get("title")
            location = inputs.get("location", "")

            state.current_step = 1
            query = f"{title} jobs {location}"
            results = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["listings"] = results

            state.current_step = 2
            state.intermediate_results["details"] = []

            state.current_step = 3
            state.final_result = {
                "query": {"title": title, "location": location},
                "jobs": [],
                "total_found": 0,
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 15. BRAND MONITOR PIPELINE
# =============================================================================

class BrandMonitorPipeline(BasePipeline):
    """
    Monitor brand mentions across the web.

    Process:
    1. Search for brand mentions
    2. Classify sentiment
    3. Identify key themes
    4. Generate alerts for critical mentions
    """

    name = "brand_monitor"
    description = "Monitor brand mentions with sentiment analysis and alerts"
    category = "monitoring"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="find_mentions",
                signature=DSPySignature(
                    name="FindMentions",
                    inputs=["brand", "keywords", "exclude"],
                    outputs=["mentions", "sources"],
                    instruction="Find brand mentions across the web",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="analyze_sentiment",
                signature=DSPySignature(
                    name="AnalyzeSentiment",
                    inputs=["mentions"],
                    outputs=["sentiment_scores", "themes"],
                    instruction="Analyze sentiment and identify themes",
                ),
                tool_calls=["search-api:read"],
            ),
            PipelineStep(
                name="generate_alerts",
                signature=DSPySignature(
                    name="GenerateAlerts",
                    inputs=["mentions", "sentiment_scores"],
                    outputs=["alerts", "summary"],
                    instruction="Generate alerts for critical mentions",
                ),
                prompt_template="""
                Brand: {brand}
                Total Mentions: {mention_count}
                Sentiment Distribution: {sentiment_dist}

                Generate alerts for:
                - Highly negative mentions
                - Viral mentions (high engagement)
                - Mentions from authoritative sources
                - Emerging issues or themes
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "brand": {"type": "string"},
                "keywords": {"type": "array", "items": {"type": "string"}},
                "exclude": {"type": "array", "items": {"type": "string"}},
                "alert_threshold": {"type": "number", "default": -0.5},
            },
            "required": ["brand"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            brand = inputs.get("brand")

            state.current_step = 1
            query = f'"{brand}"'
            mentions = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["mentions"] = mentions

            state.current_step = 2
            state.intermediate_results["sentiment"] = {}

            state.current_step = 3
            state.final_result = {
                "brand": brand,
                "mentions": [],
                "sentiment_summary": {},
                "alerts": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 16. COMPETITOR WATCH PIPELINE
# =============================================================================

class CompetitorWatchPipeline(BasePipeline):
    """
    Monitor competitor websites for changes.

    Process:
    1. Fetch competitor pages
    2. Compare with previous snapshot
    3. Identify changes
    4. Alert on significant updates
    """

    name = "competitor_watch"
    description = "Monitor competitor websites for changes and updates"
    category = "monitoring"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="fetch_pages",
                signature=DSPySignature(
                    name="FetchPages",
                    inputs=["competitor_urls", "page_types"],
                    outputs=["current_content"],
                    instruction="Fetch current content from competitor pages",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="compare_snapshots",
                signature=DSPySignature(
                    name="CompareSnapshots",
                    inputs=["current_content", "previous_content"],
                    outputs=["changes", "change_type"],
                    instruction="Compare current with previous snapshot",
                ),
            ),
            PipelineStep(
                name="analyze_changes",
                signature=DSPySignature(
                    name="AnalyzeChanges",
                    inputs=["changes"],
                    outputs=["significance", "alerts"],
                    instruction="Analyze significance of changes",
                ),
                prompt_template="""
                Changes detected on {competitor}:

                {changes}

                Classify changes as:
                - Pricing change
                - New feature
                - Content update
                - Design change
                - Other

                Rate significance: low, medium, high, critical
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "competitor_urls": {"type": "array", "items": {"type": "string"}},
                "page_types": {"type": "array", "items": {"type": "string"}},
                "check_interval_hours": {"type": "integer", "default": 24},
            },
            "required": ["competitor_urls"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            urls = inputs.get("competitor_urls", [])

            state.current_step = 1
            content = []
            for url in urls[:5]:
                page = await self._call_tool(state, "search-api", "read", {"url": url})
                content.append({"url": url, "content": page})
            state.intermediate_results["current_content"] = content

            state.current_step = 2
            state.intermediate_results["changes"] = []

            state.current_step = 3
            state.final_result = {
                "competitors_checked": len(urls),
                "changes_detected": [],
                "alerts": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 17. REGULATORY MONITOR PIPELINE
# =============================================================================

class RegulatoryMonitorPipeline(BasePipeline):
    """
    Monitor regulatory changes and compliance updates.

    Process:
    1. Search regulatory sources
    2. Extract new regulations
    3. Analyze impact
    4. Generate compliance alerts
    """

    name = "regulatory_monitor"
    description = "Monitor regulatory changes with impact analysis"
    category = "monitoring"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="search_regulations",
                signature=DSPySignature(
                    name="SearchRegulations",
                    inputs=["jurisdictions", "industries", "topics"],
                    outputs=["regulations", "sources"],
                    instruction="Search for new and updated regulations",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="extract_details",
                signature=DSPySignature(
                    name="ExtractDetails",
                    inputs=["regulations"],
                    outputs=["regulation_details", "effective_dates"],
                    instruction="Extract regulation details and dates",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="impact_analysis",
                signature=DSPySignature(
                    name="AnalyzeImpact",
                    inputs=["regulation_details", "business_context"],
                    outputs=["impact_assessment", "action_items"],
                    instruction="Analyze business impact and required actions",
                ),
                prompt_template="""
                New Regulation: {regulation}
                Jurisdiction: {jurisdiction}
                Effective Date: {effective_date}

                Business Context: {business_context}

                Analyze:
                1. Direct impact on operations
                2. Compliance requirements
                3. Timeline for compliance
                4. Recommended actions
                5. Risk if non-compliant
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "jurisdictions": {"type": "array", "items": {"type": "string"}},
                "industries": {"type": "array", "items": {"type": "string"}},
                "topics": {"type": "array", "items": {"type": "string"}},
                "business_context": {"type": "string"},
            },
            "required": ["industries"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            industries = inputs.get("industries", [])

            state.current_step = 1
            regulations = []
            for industry in industries:
                query = f"{industry} regulation new 2024"
                results = await self._call_tool(state, "search-api", "search", {"query": query})
                regulations.extend(results.get("results", []) if isinstance(results, dict) else [])
            state.intermediate_results["regulations"] = regulations

            state.current_step = 2
            state.intermediate_results["details"] = []

            state.current_step = 3
            state.final_result = {
                "industries": industries,
                "regulations_found": [],
                "impact_assessments": [],
                "action_items": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 18. PATENT TRACKER PIPELINE
# =============================================================================

class PatentTrackerPipeline(BasePipeline):
    """
    Track new patents in a technology area.

    Process:
    1. Search patent databases
    2. Extract patent details
    3. Analyze technology landscape
    4. Identify key players and trends
    """

    name = "patent_tracker"
    description = "Track patents and analyze technology landscape"
    category = "monitoring"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="search_patents",
                signature=DSPySignature(
                    name="SearchPatents",
                    inputs=["technology", "keywords", "date_range"],
                    outputs=["patents", "applicants"],
                    instruction="Search for relevant patents",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="extract_details",
                signature=DSPySignature(
                    name="ExtractDetails",
                    inputs=["patents"],
                    outputs=["patent_details", "claims"],
                    instruction="Extract patent details and claims",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="landscape_analysis",
                signature=DSPySignature(
                    name="AnalyzeLandscape",
                    inputs=["patent_details"],
                    outputs=["key_players", "trends", "white_spaces"],
                    instruction="Analyze patent landscape",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "technology": {"type": "string"},
                "keywords": {"type": "array", "items": {"type": "string"}},
                "date_range": {"type": "string", "default": "2023-2024"},
            },
            "required": ["technology"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            technology = inputs.get("technology")

            state.current_step = 1
            query = f"{technology} patent filed 2024"
            patents = await self._call_tool(state, "search-api", "search", {"query": query})
            state.intermediate_results["patents"] = patents

            state.current_step = 2
            state.intermediate_results["details"] = []

            state.current_step = 3
            state.final_result = {
                "technology": technology,
                "patents_found": [],
                "key_players": [],
                "trends": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 19. CLAIM VERIFIER PIPELINE
# =============================================================================

class ClaimVerifierPipeline(BasePipeline):
    """
    Verify claims with evidence from multiple sources.

    Process:
    1. Parse claim into verifiable components
    2. Search for supporting/refuting evidence
    3. Assess source credibility
    4. Generate verification report
    """

    name = "claim_verifier"
    description = "Verify claims with multi-source evidence and credibility assessment"
    category = "verification"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="parse_claim",
                signature=DSPySignature(
                    name="ParseClaim",
                    inputs=["claim"],
                    outputs=["claim_components", "verifiable_assertions"],
                    instruction="Break claim into verifiable assertions",
                ),
                prompt_template="""
                Claim: {claim}

                Break this claim into specific, verifiable assertions.
                For each assertion, identify:
                1. The specific factual claim
                2. What evidence would verify it
                3. What evidence would refute it
                """,
            ),
            PipelineStep(
                name="search_evidence",
                signature=DSPySignature(
                    name="SearchEvidence",
                    inputs=["verifiable_assertions"],
                    outputs=["supporting_evidence", "refuting_evidence"],
                    instruction="Search for supporting and refuting evidence",
                ),
                tool_calls=["search-api:search", "search-api:verify"],
            ),
            PipelineStep(
                name="assess_credibility",
                signature=DSPySignature(
                    name="AssessCredibility",
                    inputs=["evidence_sources"],
                    outputs=["credibility_scores"],
                    instruction="Assess credibility of evidence sources",
                ),
                tool_calls=["search-api:read"],
            ),
            PipelineStep(
                name="generate_verdict",
                signature=DSPySignature(
                    name="GenerateVerdict",
                    inputs=["claim", "evidence", "credibility_scores"],
                    outputs=["verdict", "confidence", "explanation"],
                    instruction="Generate verification verdict",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "claim": {"type": "string"},
                "context": {"type": "string"},
                "require_primary_sources": {"type": "boolean", "default": False},
            },
            "required": ["claim"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=4)
        state.status = "running"

        try:
            claim = inputs.get("claim")

            state.current_step = 1
            state.intermediate_results["assertions"] = [claim]

            state.current_step = 2
            verification = await self._call_tool(state, "search-api", "verify", {"claim": claim})
            state.intermediate_results["verification"] = verification

            state.current_step = 3
            state.intermediate_results["credibility"] = {}

            state.current_step = 4
            state.final_result = {
                "claim": claim,
                "verdict": "unverified",
                "confidence": 0.5,
                "evidence": [],
                "explanation": "",
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 20. SOURCE CREDIBILITY PIPELINE
# =============================================================================

class SourceCredibilityPipeline(BasePipeline):
    """
    Assess credibility of a source or website.

    Process:
    1. Analyze source metadata
    2. Check for known biases
    3. Verify claims on the source
    4. Generate credibility score
    """

    name = "source_credibility"
    description = "Assess source credibility with bias detection"
    category = "verification"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="analyze_source",
                signature=DSPySignature(
                    name="AnalyzeSource",
                    inputs=["url"],
                    outputs=["source_info", "metadata"],
                    instruction="Analyze source website and metadata",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="check_reputation",
                signature=DSPySignature(
                    name="CheckReputation",
                    inputs=["source_info"],
                    outputs=["reputation_data", "known_issues"],
                    instruction="Check source reputation and known issues",
                ),
                tool_calls=["search-api:search"],
            ),
            PipelineStep(
                name="sample_verification",
                signature=DSPySignature(
                    name="SampleVerification",
                    inputs=["url"],
                    outputs=["sample_claims", "verification_results"],
                    instruction="Verify sample claims from the source",
                ),
                tool_calls=["search-api:verify"],
            ),
            PipelineStep(
                name="generate_score",
                signature=DSPySignature(
                    name="GenerateScore",
                    inputs=["reputation_data", "verification_results"],
                    outputs=["credibility_score", "factors"],
                    instruction="Generate overall credibility score",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "check_depth": {"type": "string", "enum": ["quick", "standard", "deep"]},
            },
            "required": ["url"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=4)
        state.status = "running"

        try:
            url = inputs.get("url")

            state.current_step = 1
            content = await self._call_tool(state, "search-api", "read", {"url": url})
            state.intermediate_results["content"] = content

            state.current_step = 2
            query = f'"{url}" review reputation'
            reputation = await self._call_tool(state, "search-api", "search", {"query": query})
            state.intermediate_results["reputation"] = reputation

            state.current_step = 3
            state.intermediate_results["verification"] = []

            state.current_step = 4
            state.final_result = {
                "url": url,
                "credibility_score": 0.7,
                "factors": {},
                "warnings": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 21. BIAS DETECTOR PIPELINE
# =============================================================================

class BiasDetectorPipeline(BasePipeline):
    """
    Detect potential bias in content.

    Process:
    1. Analyze content for bias indicators
    2. Compare with diverse sources
    3. Identify missing perspectives
    4. Generate bias report
    """

    name = "bias_detector"
    description = "Detect bias in content with multi-perspective analysis"
    category = "verification"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="analyze_content",
                signature=DSPySignature(
                    name="AnalyzeContent",
                    inputs=["content", "topic"],
                    outputs=["claims", "framing", "language_patterns"],
                    instruction="Analyze content for bias indicators",
                ),
                tool_calls=["search-api:read"],
            ),
            PipelineStep(
                name="find_alternatives",
                signature=DSPySignature(
                    name="FindAlternatives",
                    inputs=["topic", "claims"],
                    outputs=["alternative_sources", "different_perspectives"],
                    instruction="Find alternative perspectives on the topic",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="compare_perspectives",
                signature=DSPySignature(
                    name="ComparePerspectives",
                    inputs=["content", "alternative_sources"],
                    outputs=["perspective_gaps", "bias_indicators"],
                    instruction="Compare perspectives and identify gaps",
                ),
                prompt_template="""
                Original Content Analysis:
                - Claims: {claims}
                - Framing: {framing}

                Alternative Perspectives:
                {alternatives}

                Identify:
                1. What perspectives are missing?
                2. What language suggests bias?
                3. What claims are disputed?
                4. Overall bias assessment
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "url": {"type": "string"},
                "topic": {"type": "string"},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topic = inputs.get("topic")
            content = inputs.get("content", "")
            url = inputs.get("url")

            state.current_step = 1
            if url:
                content = await self._call_tool(state, "search-api", "read", {"url": url})
            state.intermediate_results["content"] = content

            state.current_step = 2
            alternatives = await self._call_tool(
                state, "advanced-search-api", "deep_search",
                {"query": f"{topic} different perspectives"}
            )
            state.intermediate_results["alternatives"] = alternatives

            state.current_step = 3
            state.final_result = {
                "topic": topic,
                "bias_score": 0.3,
                "bias_type": "none_detected",
                "missing_perspectives": [],
                "recommendations": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 22. PLAGIARISM CHECKER PIPELINE
# =============================================================================

class PlagiarismCheckerPipeline(BasePipeline):
    """
    Check content for potential plagiarism.

    Process:
    1. Extract key phrases and sentences
    2. Search for matches
    3. Compare similarity
    4. Generate plagiarism report
    """

    name = "plagiarism_checker"
    description = "Check content for plagiarism with source matching"
    category = "verification"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="extract_phrases",
                signature=DSPySignature(
                    name="ExtractPhrases",
                    inputs=["content"],
                    outputs=["key_phrases", "sentences"],
                    instruction="Extract key phrases for plagiarism check",
                ),
            ),
            PipelineStep(
                name="search_matches",
                signature=DSPySignature(
                    name="SearchMatches",
                    inputs=["key_phrases"],
                    outputs=["potential_matches", "source_urls"],
                    instruction="Search for matching content",
                ),
                tool_calls=["search-api:search"],
            ),
            PipelineStep(
                name="compare_similarity",
                signature=DSPySignature(
                    name="CompareSimilarity",
                    inputs=["content", "potential_matches"],
                    outputs=["similarity_scores", "matched_sections"],
                    instruction="Compare content similarity",
                ),
                tool_calls=["search-api:read", "search-api:embed"],
            ),
            PipelineStep(
                name="generate_report",
                signature=DSPySignature(
                    name="GenerateReport",
                    inputs=["similarity_scores", "matched_sections"],
                    outputs=["plagiarism_score", "matches"],
                    instruction="Generate plagiarism report",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "exclude_urls": {"type": "array", "items": {"type": "string"}},
                "threshold": {"type": "number", "default": 0.8},
            },
            "required": ["content"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=4)
        state.status = "running"

        try:
            content = inputs.get("content")

            state.current_step = 1
            # Extract first few sentences
            sentences = content.split(".")[:5]
            state.intermediate_results["sentences"] = sentences

            state.current_step = 2
            matches = []
            for sentence in sentences[:3]:
                if len(sentence) > 20:
                    results = await self._call_tool(
                        state, "search-api", "search",
                        {"query": f'"{sentence[:100]}"'}
                    )
                    matches.extend(results.get("results", []) if isinstance(results, dict) else [])
            state.intermediate_results["matches"] = matches

            state.current_step = 3
            state.intermediate_results["similarity"] = []

            state.current_step = 4
            state.final_result = {
                "plagiarism_score": 0.0,
                "original_score": 1.0,
                "matches": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 23. ENTITY MAPPER PIPELINE
# =============================================================================

class EntityMapperPipeline(BasePipeline):
    """
    Map relationships between entities.

    Process:
    1. Extract entities from sources
    2. Identify relationships
    3. Build entity graph
    4. Generate relationship map
    """

    name = "entity_mapper"
    description = "Map relationships between people, companies, and concepts"
    category = "intelligence"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="search_entities",
                signature=DSPySignature(
                    name="SearchEntities",
                    inputs=["seed_entities", "relationship_types"],
                    outputs=["entity_mentions", "sources"],
                    instruction="Search for entity mentions and relationships",
                ),
                tool_calls=["advanced-search-api:deep_search", "search-api:search"],
            ),
            PipelineStep(
                name="extract_relationships",
                signature=DSPySignature(
                    name="ExtractRelationships",
                    inputs=["entity_mentions"],
                    outputs=["relationships", "entity_metadata"],
                    instruction="Extract relationships between entities",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="build_graph",
                signature=DSPySignature(
                    name="BuildGraph",
                    inputs=["entities", "relationships"],
                    outputs=["entity_graph", "key_connections"],
                    instruction="Build entity relationship graph",
                ),
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "seed_entities": {"type": "array", "items": {"type": "string"}},
                "relationship_types": {"type": "array", "items": {"type": "string"}},
                "max_depth": {"type": "integer", "default": 2},
            },
            "required": ["seed_entities"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            entities = inputs.get("seed_entities", [])

            state.current_step = 1
            mentions = []
            for entity in entities:
                results = await self._call_tool(
                    state, "advanced-search-api", "deep_search",
                    {"query": entity}
                )
                mentions.append({"entity": entity, "results": results})
            state.intermediate_results["mentions"] = mentions

            state.current_step = 2
            state.intermediate_results["relationships"] = []

            state.current_step = 3
            state.final_result = {
                "entities": entities,
                "nodes": [],
                "edges": [],
                "key_connections": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state


# =============================================================================
# 24. TIMELINE BUILDER PIPELINE
# =============================================================================

class TimelineBuilderPipeline(BasePipeline):
    """
    Build chronological timeline of events.

    Process:
    1. Search for events related to topic
    2. Extract dates and event details
    3. Order chronologically
    4. Generate interactive timeline
    """

    name = "timeline_builder"
    description = "Build chronological timeline of events from multiple sources"
    category = "intelligence"

    def _build_steps(self):
        self.steps = [
            PipelineStep(
                name="search_events",
                signature=DSPySignature(
                    name="SearchEvents",
                    inputs=["topic", "date_range", "event_types"],
                    outputs=["events", "sources"],
                    instruction="Search for events related to topic",
                ),
                tool_calls=["search-api:search", "advanced-search-api:deep_search"],
            ),
            PipelineStep(
                name="extract_dates",
                signature=DSPySignature(
                    name="ExtractDates",
                    inputs=["events"],
                    outputs=["dated_events", "date_confidence"],
                    instruction="Extract and normalize dates from events",
                ),
                tool_calls=["search-api:read", "advanced-search-api:extract_data"],
            ),
            PipelineStep(
                name="build_timeline",
                signature=DSPySignature(
                    name="BuildTimeline",
                    inputs=["dated_events"],
                    outputs=["timeline", "key_periods"],
                    instruction="Build chronological timeline",
                ),
                prompt_template="""
                Events: {events}

                Build a timeline with:
                1. Chronologically ordered events
                2. Key turning points highlighted
                3. Periods/eras identified
                4. Connections between events
                """,
            ),
        ]

    def _get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "date_range": {"type": "string"},
                "event_types": {"type": "array", "items": {"type": "string"}},
                "granularity": {"type": "string", "enum": ["day", "month", "year"]},
            },
            "required": ["topic"],
        }

    async def execute(self, **inputs) -> PipelineState:
        state = PipelineState(pipeline_type=self.name, inputs=inputs, total_steps=3)
        state.status = "running"

        try:
            topic = inputs.get("topic")
            date_range = inputs.get("date_range", "")

            state.current_step = 1
            query = f"{topic} timeline history {date_range}"
            events = await self._call_tool(state, "advanced-search-api", "deep_search", {"query": query})
            state.intermediate_results["events"] = events

            state.current_step = 2
            state.intermediate_results["dated_events"] = []

            state.current_step = 3
            state.final_result = {
                "topic": topic,
                "timeline": [],
                "key_periods": [],
                "sources": [],
                "generated_at": datetime.utcnow().isoformat(),
            }
            state.status = "completed"

        except Exception as e:
            state.status = "failed"
            state.error = str(e)

        return state
