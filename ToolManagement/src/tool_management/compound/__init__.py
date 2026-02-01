"""
Compound Tools - DSPy pipeline-based tools built on Search API and Advanced Search API.

These are differentiated, high-value tools that combine multiple base tools
with sophisticated prompting and persistent state management.

50 total compound tools across 11 categories:
- Research & Analysis (6)
- Content & Writing (4)
- Data Extraction (4)
- Monitoring & Alerts (4)
- Verification & Trust (4)
- Intelligence & Synthesis (2)
- Advanced Reasoning (9)
- Meta & Code (2)
- Document Processing (3)
- Multi-Agent (2)
- Cognitive Substrate (10) - Ontological learning for advanced cognitive agents
"""

from .pipelines import (
    # Base infrastructure
    BasePipeline,
    PipelineState,
    PipelineStep,
    DSPySignature,
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

from .registry import CompoundToolRegistry, get_compound_registry

from .streaming import (
    # Streaming infrastructure
    StreamConfig,
    ExecutionStatus,
    ExecutionContext,
    PipelineChain,
    PipelineChainStep,
    StreamingExecutor,
    ExecutionMetrics,
    StreamSubscription,
    AdvancedConnectionManager,
)

__all__ = [
    # Base Infrastructure
    "BasePipeline",
    "PipelineState",
    "PipelineStep",
    "DSPySignature",
    "PipelineEvent",
    "PipelineEventType",
    # Research & Analysis
    "DeepResearchPipeline",
    "CompetitiveAnalysisPipeline",
    "MarketResearchPipeline",
    "LiteratureReviewPipeline",
    "NewsDigestPipeline",
    "TrendAnalysisPipeline",

    # Content & Writing
    "ContentBriefPipeline",
    "FactCheckedWriterPipeline",
    "SEOAnalyzerPipeline",
    "SourceAggregatorPipeline",

    # Data Extraction
    "StructuredHarvesterPipeline",
    "PriceTrackerPipeline",
    "ContactFinderPipeline",
    "JobAggregatorPipeline",

    # Monitoring & Alerts
    "BrandMonitorPipeline",
    "CompetitorWatchPipeline",
    "RegulatoryMonitorPipeline",
    "PatentTrackerPipeline",

    # Verification & Trust
    "ClaimVerifierPipeline",
    "SourceCredibilityPipeline",
    "BiasDetectorPipeline",
    "PlagiarismCheckerPipeline",

    # Intelligence & Synthesis
    "EntityMapperPipeline",
    "TimelineBuilderPipeline",

    # Advanced Reasoning (from advanced_pipelines)
    "MultiDimensionalReasoningPipeline",
    "SelfInterrogatingResearchPipeline",
    "DialecticalReasoningPipeline",
    "SteelmanArgumentPipeline",
    "RootCauseAnalysisPipeline",
    "ScenarioPlanningPipeline",
    "DecisionMatrixPipeline",
    "SWOTAnalysisPipeline",
    "HypothesisTestingPipeline",

    # Meta & Code
    "CodeSynthesisPipeline",
    "CodeReviewPipeline",

    # Document Processing
    "DocumentUnderstandingPipeline",
    "PDFAnalysisPipeline",
    "ArxivPaperAnalyzerPipeline",

    # Multi-Agent
    "MultiAgentDebatePipeline",
    "ExpertPanelPipeline",

    # Cognitive Substrate (from cognitive_pipelines)
    "SchemaInferencePipeline",
    "CrossDomainMorphismPipeline",
    "OntologyInductionPipeline",
    "AffordanceMiningPipeline",
    "KnowledgeGraphExpansionPipeline",
    "EntityResolutionPipeline",
    "RelationshipInferencePipeline",
    "TemporalPatternPipeline",
    "ContextCorrelationPipeline",
    "StructuralPatternPipeline",

    # Registry
    "CompoundToolRegistry",
    "get_compound_registry",

    # Streaming Infrastructure
    "StreamConfig",
    "ExecutionStatus",
    "ExecutionContext",
    "PipelineChain",
    "PipelineChainStep",
    "StreamingExecutor",
    "ExecutionMetrics",
    "StreamSubscription",
    "AdvancedConnectionManager",
]
