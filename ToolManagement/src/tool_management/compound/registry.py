"""
Compound Tool Registry - Manages DSPy pipeline-based compound tools.

Provides:
- Registration and discovery of compound tools
- Execution with state persistence
- Tool categorization and search
- Integration with base tool services
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type
from uuid import uuid4

from pydantic import BaseModel, Field

from .pipelines import (
    BasePipeline,
    PipelineState,
    PipelineEvent,
    PipelineEventType,
    # Research & Analysis
    DeepResearchPipeline,
    CompetitiveAnalysisPipeline,
    MarketResearchPipeline,
    LiteratureReviewPipeline,
    NewsDigestPipeline,
    TrendAnalysisPipeline,
    # Content & Writing
    ContentBriefPipeline,
    FactCheckedWriterPipeline,
    SEOAnalyzerPipeline,
    SourceAggregatorPipeline,
    # Data Extraction
    StructuredHarvesterPipeline,
    PriceTrackerPipeline,
    ContactFinderPipeline,
    JobAggregatorPipeline,
    # Monitoring & Alerts
    BrandMonitorPipeline,
    CompetitorWatchPipeline,
    RegulatoryMonitorPipeline,
    PatentTrackerPipeline,
    # Verification & Trust
    ClaimVerifierPipeline,
    SourceCredibilityPipeline,
    BiasDetectorPipeline,
    PlagiarismCheckerPipeline,
    # Intelligence & Synthesis
    EntityMapperPipeline,
    TimelineBuilderPipeline,
)

from .advanced_pipelines import (
    # Advanced Reasoning
    MultiDimensionalReasoningPipeline,
    SelfInterrogatingResearchPipeline,
    DialecticalReasoningPipeline,
    SteelmanArgumentPipeline,
    RootCauseAnalysisPipeline,
    ScenarioPlanningPipeline,
    DecisionMatrixPipeline,
    SWOTAnalysisPipeline,
    HypothesisTestingPipeline,
    # Meta & Code
    CodeSynthesisPipeline,
    CodeReviewPipeline,
    # Document Processing
    DocumentUnderstandingPipeline,
    PDFAnalysisPipeline,
    ArxivPaperAnalyzerPipeline,
    # Multi-Agent
    MultiAgentDebatePipeline,
    ExpertPanelPipeline,
)

from .cognitive_pipelines import (
    # Cognitive Substrate - Ontological Learning
    SchemaInferencePipeline,
    CrossDomainMorphismPipeline,
    OntologyInductionPipeline,
    AffordanceMiningPipeline,
    KnowledgeGraphExpansionPipeline,
    EntityResolutionPipeline,
    RelationshipInferencePipeline,
    TemporalPatternPipeline,
    ContextCorrelationPipeline,
    StructuralPatternPipeline,
)

logger = logging.getLogger(__name__)


# =============================================================================
# COMPOUND TOOL METADATA
# =============================================================================

class CompoundToolMetadata(BaseModel):
    """Metadata for a compound tool."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    category: str
    version: str = "1.0.0"

    # Input/Output schemas
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)

    # Pipeline info
    step_count: int = 0
    base_tools: List[str] = []

    # Billing
    estimated_cost_cents: int = 0
    estimated_time_seconds: int = 30

    # Categorization
    tags: List[str] = []
    use_cases: List[str] = []


# =============================================================================
# COMPOUND TOOL REGISTRY
# =============================================================================

class CompoundToolRegistry:
    """
    Registry for compound tools (DSPy pipelines).

    Usage:
        registry = CompoundToolRegistry()
        registry.register_defaults()

        # List tools by category
        research_tools = registry.list_by_category("research")

        # Execute a compound tool
        result = await registry.execute("deep_research", topic="AI Safety")
    """

    # Default pipelines to register (50 total)
    DEFAULT_PIPELINES: List[Type[BasePipeline]] = [
        # Research & Analysis (6)
        DeepResearchPipeline,
        CompetitiveAnalysisPipeline,
        MarketResearchPipeline,
        LiteratureReviewPipeline,
        NewsDigestPipeline,
        TrendAnalysisPipeline,
        # Content & Writing (4)
        ContentBriefPipeline,
        FactCheckedWriterPipeline,
        SEOAnalyzerPipeline,
        SourceAggregatorPipeline,
        # Data Extraction (4)
        StructuredHarvesterPipeline,
        PriceTrackerPipeline,
        ContactFinderPipeline,
        JobAggregatorPipeline,
        # Monitoring & Alerts (4)
        BrandMonitorPipeline,
        CompetitorWatchPipeline,
        RegulatoryMonitorPipeline,
        PatentTrackerPipeline,
        # Verification & Trust (4)
        ClaimVerifierPipeline,
        SourceCredibilityPipeline,
        BiasDetectorPipeline,
        PlagiarismCheckerPipeline,
        # Intelligence & Synthesis (2)
        EntityMapperPipeline,
        TimelineBuilderPipeline,
        # Advanced Reasoning (9)
        MultiDimensionalReasoningPipeline,
        SelfInterrogatingResearchPipeline,
        DialecticalReasoningPipeline,
        SteelmanArgumentPipeline,
        RootCauseAnalysisPipeline,
        ScenarioPlanningPipeline,
        DecisionMatrixPipeline,
        SWOTAnalysisPipeline,
        HypothesisTestingPipeline,
        # Meta & Code (2)
        CodeSynthesisPipeline,
        CodeReviewPipeline,
        # Document Processing (3)
        DocumentUnderstandingPipeline,
        PDFAnalysisPipeline,
        ArxivPaperAnalyzerPipeline,
        # Multi-Agent (2)
        MultiAgentDebatePipeline,
        ExpertPanelPipeline,
        # Cognitive Substrate (10)
        SchemaInferencePipeline,
        CrossDomainMorphismPipeline,
        OntologyInductionPipeline,
        AffordanceMiningPipeline,
        KnowledgeGraphExpansionPipeline,
        EntityResolutionPipeline,
        RelationshipInferencePipeline,
        TemporalPatternPipeline,
        ContextCorrelationPipeline,
        StructuralPatternPipeline,
    ]

    def __init__(self, tool_executor: Optional[Callable] = None):
        """
        Args:
            tool_executor: Async function to execute base tools
                          signature: (service_id, tool_name, inputs) -> result
        """
        self.tool_executor = tool_executor
        self._pipelines: Dict[str, BasePipeline] = {}
        self._metadata: Dict[str, CompoundToolMetadata] = {}
        self._execution_history: Dict[str, PipelineState] = {}

    def register_defaults(self):
        """Register all default compound tools."""
        for pipeline_class in self.DEFAULT_PIPELINES:
            self.register(pipeline_class)
        logger.info(f"Registered {len(self._pipelines)} compound tools")

    def register(self, pipeline_class: Type[BasePipeline]):
        """Register a pipeline as a compound tool."""
        pipeline = pipeline_class(tool_executor=self.tool_executor)
        self._pipelines[pipeline.name] = pipeline

        # Generate metadata
        self._metadata[pipeline.name] = CompoundToolMetadata(
            name=pipeline.name,
            description=pipeline.description,
            category=pipeline.category,
            version=pipeline.version,
            input_schema=pipeline._get_input_schema(),
            output_schema=pipeline._get_output_schema(),
            step_count=len(pipeline.steps),
            base_tools=pipeline._get_base_tools(),
            tags=self._get_tags_for_category(pipeline.category),
        )

        logger.debug(f"Registered compound tool: {pipeline.name}")

    def _get_tags_for_category(self, category: str) -> List[str]:
        """Get default tags for a category."""
        category_tags = {
            "research": ["research", "analysis", "information"],
            "content": ["content", "writing", "seo"],
            "data": ["data", "extraction", "harvesting"],
            "monitoring": ["monitoring", "alerts", "tracking"],
            "verification": ["verification", "fact-check", "trust"],
            "intelligence": ["intelligence", "synthesis", "mapping"],
        }
        return category_tags.get(category, [])

    def get(self, name: str) -> Optional[BasePipeline]:
        """Get a pipeline by name."""
        return self._pipelines.get(name)

    def get_metadata(self, name: str) -> Optional[CompoundToolMetadata]:
        """Get metadata for a compound tool."""
        return self._metadata.get(name)

    def list_all(self) -> List[CompoundToolMetadata]:
        """List all registered compound tools."""
        return list(self._metadata.values())

    def list_by_category(self, category: str) -> List[CompoundToolMetadata]:
        """List compound tools by category."""
        return [m for m in self._metadata.values() if m.category == category]

    def list_categories(self) -> List[str]:
        """List all categories."""
        return list(set(m.category for m in self._metadata.values()))

    def search(self, query: str) -> List[CompoundToolMetadata]:
        """Search compound tools by name or description."""
        query_lower = query.lower()
        return [
            m for m in self._metadata.values()
            if query_lower in m.name.lower() or query_lower in m.description.lower()
        ]

    async def execute(
        self,
        name: str,
        **inputs,
    ) -> PipelineState:
        """Execute a compound tool and return the state."""
        pipeline = self._pipelines.get(name)
        if not pipeline:
            raise ValueError(f"Compound tool not found: {name}")

        state = await pipeline.execute(**inputs)

        # Store in history
        self._execution_history[state.id] = state

        return state

    async def execute_stream(
        self,
        name: str,
        **inputs,
    ):
        """
        Execute a compound tool with streaming events.

        Yields PipelineEvent objects as the pipeline progresses.

        Usage:
            async for event in registry.execute_stream("deep_research", topic="AI"):
                if event.event_type == PipelineEventType.STEP_COMPLETED:
                    print(f"Step done: {event.step_name}")
                elif event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                    result = event.data["final_result"]
        """
        pipeline = self._pipelines.get(name)
        if not pipeline:
            raise ValueError(f"Compound tool not found: {name}")

        final_state = None
        async for event in pipeline.execute_stream(**inputs):
            # Capture final state for history
            if event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                if event.state_snapshot:
                    final_state = PipelineState(**event.state_snapshot)
            elif event.event_type == PipelineEventType.PIPELINE_FAILED:
                if event.state_snapshot:
                    final_state = PipelineState(**event.state_snapshot)

            yield event

        # Store in history
        if final_state:
            self._execution_history[final_state.id] = final_state

    def get_execution(self, execution_id: str) -> Optional[PipelineState]:
        """Get a previous execution by ID."""
        return self._execution_history.get(execution_id)

    def list_executions(
        self,
        pipeline_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[PipelineState]:
        """List execution history."""
        executions = list(self._execution_history.values())

        if pipeline_type:
            executions = [e for e in executions if e.pipeline_type == pipeline_type]
        if status:
            executions = [e for e in executions if e.status == status]

        # Sort by created_at descending
        executions.sort(key=lambda e: e.created_at, reverse=True)

        return executions[:limit]

    def to_tool_definitions(self) -> List[Dict[str, Any]]:
        """Export all compound tools as tool definitions."""
        return [p.to_tool_definition() for p in self._pipelines.values()]

    def get_summary(self) -> Dict[str, Any]:
        """Get registry summary."""
        categories = {}
        for m in self._metadata.values():
            if m.category not in categories:
                categories[m.category] = []
            categories[m.category].append(m.name)

        return {
            "total_tools": len(self._pipelines),
            "categories": categories,
            "executions": len(self._execution_history),
        }


# =============================================================================
# GLOBAL REGISTRY
# =============================================================================

_global_registry: Optional[CompoundToolRegistry] = None


def get_compound_registry(
    tool_executor: Optional[Callable] = None,
    auto_register: bool = True,
) -> CompoundToolRegistry:
    """Get or create the global compound tool registry."""
    global _global_registry

    if _global_registry is None:
        _global_registry = CompoundToolRegistry(tool_executor=tool_executor)
        if auto_register:
            _global_registry.register_defaults()

    return _global_registry


# =============================================================================
# COMPOUND TOOL CATEGORIES
# =============================================================================

COMPOUND_TOOLS_BY_CATEGORY = {
    "research": {
        "name": "Research & Analysis",
        "description": "Deep research, competitive analysis, and trend identification",
        "tools": [
            {
                "name": "deep_research",
                "description": "Multi-step research with verified sources and synthesis",
                "use_cases": ["Market research", "Due diligence", "Competitive intel"],
            },
            {
                "name": "competitive_analysis",
                "description": "Comprehensive competitor comparison",
                "use_cases": ["Competitive positioning", "Feature gaps", "Market entry"],
            },
            {
                "name": "market_research",
                "description": "Market size, trends, and opportunity analysis",
                "use_cases": ["Market sizing", "Investment analysis", "Business planning"],
            },
            {
                "name": "literature_review",
                "description": "Academic literature search and synthesis",
                "use_cases": ["Academic research", "Technology assessment", "Due diligence"],
            },
            {
                "name": "news_digest",
                "description": "Curated news with deduplication and summarization",
                "use_cases": ["Daily briefings", "Industry tracking", "Crisis monitoring"],
            },
            {
                "name": "trend_analysis",
                "description": "Identify and predict emerging trends",
                "use_cases": ["Strategic planning", "Innovation scouting", "Investment timing"],
            },
        ],
    },
    "content": {
        "name": "Content & Writing",
        "description": "Research-backed content creation and optimization",
        "tools": [
            {
                "name": "content_brief",
                "description": "Generate research-backed content briefs",
                "use_cases": ["Content strategy", "Outsourced writing", "SEO content"],
            },
            {
                "name": "fact_checked_writer",
                "description": "Write content with inline fact-checking",
                "use_cases": ["Journalism", "Technical writing", "Compliance content"],
            },
            {
                "name": "seo_analyzer",
                "description": "SEO analysis with competitor comparison",
                "use_cases": ["SEO optimization", "Content audits", "Ranking improvement"],
            },
            {
                "name": "source_aggregator",
                "description": "Find and rank authoritative sources",
                "use_cases": ["Research", "Content creation", "Citation gathering"],
            },
        ],
    },
    "data": {
        "name": "Data Extraction",
        "description": "Structured data harvesting and tracking",
        "tools": [
            {
                "name": "structured_harvester",
                "description": "Extract structured data from multiple sources",
                "use_cases": ["Data collection", "Lead generation", "Market data"],
            },
            {
                "name": "price_tracker",
                "description": "Monitor and track pricing changes",
                "use_cases": ["Competitive pricing", "Deal hunting", "Market tracking"],
            },
            {
                "name": "contact_finder",
                "description": "Find business contact information",
                "use_cases": ["Sales prospecting", "Recruitment", "Partnerships"],
            },
            {
                "name": "job_aggregator",
                "description": "Aggregate job listings from multiple sources",
                "use_cases": ["Job search", "Market analysis", "Recruitment intel"],
            },
        ],
    },
    "monitoring": {
        "name": "Monitoring & Alerts",
        "description": "Real-time monitoring with intelligent alerts",
        "tools": [
            {
                "name": "brand_monitor",
                "description": "Monitor brand mentions with sentiment",
                "use_cases": ["Brand management", "PR monitoring", "Crisis detection"],
            },
            {
                "name": "competitor_watch",
                "description": "Monitor competitor website changes",
                "use_cases": ["Competitive intel", "Feature tracking", "Pricing alerts"],
            },
            {
                "name": "regulatory_monitor",
                "description": "Track regulatory changes and compliance",
                "use_cases": ["Compliance", "Risk management", "Policy tracking"],
            },
            {
                "name": "patent_tracker",
                "description": "Track new patents in technology areas",
                "use_cases": ["IP strategy", "Innovation tracking", "Competitive intel"],
            },
        ],
    },
    "verification": {
        "name": "Verification & Trust",
        "description": "Fact-checking, credibility assessment, and bias detection",
        "tools": [
            {
                "name": "claim_verifier",
                "description": "Verify claims with multi-source evidence",
                "use_cases": ["Fact-checking", "Due diligence", "Content verification"],
            },
            {
                "name": "source_credibility",
                "description": "Assess source reliability and bias",
                "use_cases": ["Research validation", "Content curation", "Trust assessment"],
            },
            {
                "name": "bias_detector",
                "description": "Detect bias in content",
                "use_cases": ["Media analysis", "Content review", "Research validation"],
            },
            {
                "name": "plagiarism_checker",
                "description": "Check content for plagiarism",
                "use_cases": ["Content verification", "Academic integrity", "SEO uniqueness"],
            },
        ],
    },
    "intelligence": {
        "name": "Intelligence & Synthesis",
        "description": "Entity mapping, timeline building, and knowledge synthesis",
        "tools": [
            {
                "name": "entity_mapper",
                "description": "Map relationships between entities",
                "use_cases": ["Network analysis", "Due diligence", "Investigations"],
            },
            {
                "name": "timeline_builder",
                "description": "Build chronological event timelines",
                "use_cases": ["Historical analysis", "Project tracking", "Case building"],
            },
        ],
    },
    "reasoning": {
        "name": "Advanced Reasoning",
        "description": "Cognitive bias reduction, dialectical analysis, and structured decision making",
        "tools": [
            {
                "name": "multi_dimensional_reasoning",
                "description": "Analyze from multiple cognitive dimensions with bias correction",
                "use_cases": ["Complex decisions", "Policy analysis", "Strategic thinking"],
            },
            {
                "name": "self_interrogating_research",
                "description": "Research with self-questioning and belief updating",
                "use_cases": ["Deep research", "Hypothesis validation", "Critical analysis"],
            },
            {
                "name": "dialectical_reasoning",
                "description": "Thesis-antithesis-synthesis dialectical analysis",
                "use_cases": ["Conflict resolution", "Policy debate", "Philosophical inquiry"],
            },
            {
                "name": "steelman_argument",
                "description": "Build the strongest version of any argument",
                "use_cases": ["Debate prep", "Argument validation", "Fair assessment"],
            },
            {
                "name": "root_cause_analysis",
                "description": "5 Whys and fishbone diagram root cause analysis",
                "use_cases": ["Problem solving", "Incident analysis", "Process improvement"],
            },
            {
                "name": "scenario_planning",
                "description": "Generate and analyze future scenarios with strategic implications",
                "use_cases": ["Strategic planning", "Risk assessment", "Contingency planning"],
            },
            {
                "name": "decision_matrix",
                "description": "Structured decision making with weighted criteria analysis",
                "use_cases": ["Vendor selection", "Investment decisions", "Prioritization"],
            },
            {
                "name": "swot_analysis",
                "description": "SWOT analysis with evidence-based insights",
                "use_cases": ["Strategic planning", "Competitive positioning", "Business assessment"],
            },
            {
                "name": "hypothesis_testing",
                "description": "Scientific hypothesis formulation and testing",
                "use_cases": ["Research validation", "A/B testing", "Experimental design"],
            },
        ],
    },
    "meta": {
        "name": "Meta & Code",
        "description": "Code synthesis, review, and meta-programming",
        "tools": [
            {
                "name": "code_synthesis",
                "description": "Generate code from natural language specifications",
                "use_cases": ["Rapid prototyping", "Automation", "Code generation"],
            },
            {
                "name": "code_review",
                "description": "Multi-perspective code review with security, performance, and maintainability",
                "use_cases": ["Code quality", "Security audit", "Performance optimization"],
            },
        ],
    },
    "document": {
        "name": "Document Processing",
        "description": "Deep document understanding, PDF analysis, and paper review",
        "tools": [
            {
                "name": "document_understanding",
                "description": "Deep document understanding with structure extraction and verification",
                "use_cases": ["Contract analysis", "Report processing", "Document QA"],
            },
            {
                "name": "pdf_analysis",
                "description": "Comprehensive PDF analysis with vision and text extraction",
                "use_cases": ["Document digitization", "Data extraction", "Form processing"],
            },
            {
                "name": "arxiv_paper_analyzer",
                "description": "Deep analysis of arXiv research papers",
                "use_cases": ["Literature review", "Research tracking", "Paper summarization"],
            },
        ],
    },
    "multi_agent": {
        "name": "Multi-Agent Coordination",
        "description": "Multi-agent debate, expert panels, and distributed reasoning",
        "tools": [
            {
                "name": "multi_agent_debate",
                "description": "Multi-agent debate with different perspectives",
                "use_cases": ["Perspective gathering", "Conflict exploration", "Idea stress-testing"],
            },
            {
                "name": "expert_panel",
                "description": "Consult with simulated panel of domain experts",
                "use_cases": ["Expert consultation", "Multi-stakeholder analysis", "Comprehensive review"],
            },
        ],
    },
    "cognitive": {
        "name": "Cognitive Substrate",
        "description": "Ontological learning, schema inference, and knowledge graph operations for advanced cognitive agents",
        "tools": [
            {
                "name": "schema_inference",
                "description": "Infer schema structure from data instances",
                "use_cases": ["Data modeling", "API discovery", "Format detection"],
            },
            {
                "name": "cross_domain_morphism",
                "description": "Discover structural isomorphisms between domains for knowledge transfer",
                "use_cases": ["Analogy finding", "Transfer learning", "Domain mapping"],
            },
            {
                "name": "ontology_induction",
                "description": "Induce formal ontology from domain data",
                "use_cases": ["Taxonomy building", "Knowledge base creation", "Domain modeling"],
            },
            {
                "name": "affordance_mining",
                "description": "Extract possible actions and state transitions from data",
                "use_cases": ["Action discovery", "UI generation", "Workflow mining"],
            },
            {
                "name": "knowledge_graph_expansion",
                "description": "Expand knowledge graph through intelligent traversal and discovery",
                "use_cases": ["Graph enrichment", "Gap filling", "Entity discovery"],
            },
            {
                "name": "entity_resolution",
                "description": "Resolve and merge duplicate or related entities",
                "use_cases": ["Data deduplication", "Master data management", "Record linkage"],
            },
            {
                "name": "relationship_inference",
                "description": "Infer hidden relationships between entities",
                "use_cases": ["Link prediction", "Network analysis", "Connection discovery"],
            },
            {
                "name": "temporal_pattern",
                "description": "Extract temporal patterns and predict event sequences",
                "use_cases": ["Event prediction", "Trend detection", "Anomaly detection"],
            },
            {
                "name": "context_correlation",
                "description": "Find correlations and bridges across different contexts",
                "use_cases": ["Context switching", "Information retrieval", "Relevance ranking"],
            },
            {
                "name": "structural_pattern",
                "description": "Extract deep structural patterns that transfer across domains",
                "use_cases": ["Pattern recognition", "Architecture discovery", "Design mining"],
            },
        ],
    },
}
