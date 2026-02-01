"""
Pre-built Workflow Library - Common AI/NLP/Data Processing Patterns.

Named workflows that can be invoked by name with token limits and cost controls.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from enum import Enum


# =============================================================================
# RESOURCE LIMITS PER STAGE
# =============================================================================

class ResourceLimits(BaseModel):
    """Resource limits for a workflow stage."""
    max_tokens: Optional[int] = Field(None, description="Max tokens to process/generate")
    max_cost_cents: Optional[int] = Field(None, description="Max cost in cents")
    max_duration_seconds: Optional[float] = Field(None, description="Max execution time")
    max_requests: Optional[int] = Field(None, description="Max API requests")

    # Rate limiting
    rate_limit_per_second: Optional[int] = Field(None, description="Max requests per second")
    rate_limit_per_minute: Optional[int] = Field(None, description="Max requests per minute")

    # Memory limits
    max_memory_mb: Optional[int] = Field(None, description="Max memory usage")
    max_output_tokens: Optional[int] = Field(None, description="Max output tokens")


class StageWithLimits(BaseModel):
    """A workflow stage with resource limits."""
    name: str
    tool: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    limits: ResourceLimits = Field(default_factory=ResourceLimits)

    # Stage dependencies
    depends_on: List[str] = Field(default_factory=list)

    # Retry policy
    retry_on_failure: bool = False
    max_retries: int = 0


# =============================================================================
# PRE-BUILT WORKFLOW TEMPLATES
# =============================================================================

class WorkflowCategory(str, Enum):
    """Categories of pre-built workflows."""
    NLP = "nlp"
    EXTRACTION = "extraction"
    ANALYSIS = "analysis"
    TRANSFORMATION = "transformation"
    SEARCH = "search"
    GENERATION = "generation"


class NamedWorkflow(BaseModel):
    """A pre-built named workflow template."""
    id: str
    name: str
    description: str
    category: WorkflowCategory

    # Stages
    stages: List[StageWithLimits]

    # Global limits
    total_max_tokens: Optional[int] = None
    total_max_cost_cents: Optional[int] = None
    total_max_duration_seconds: Optional[float] = None

    # Required inputs
    required_inputs: List[str] = Field(default_factory=list)

    # Tags
    tags: List[str] = Field(default_factory=list)

    # Examples
    example_inputs: Optional[Dict[str, Any]] = None


# =============================================================================
# WORKFLOW LIBRARY - PRE-BUILT PATTERNS
# =============================================================================

WORKFLOW_LIBRARY: Dict[str, NamedWorkflow] = {}


def register_workflow(workflow: NamedWorkflow):
    """Register a named workflow in the library."""
    WORKFLOW_LIBRARY[workflow.id] = workflow


# -----------------------------------------------------------------------------
# NLP Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="named_entity_extraction",
    name="Named Entity Extraction",
    description="Extract named entities (people, places, organizations) from text",
    category=WorkflowCategory.NLP,
    stages=[
        StageWithLimits(
            name="segment",
            tool="text_segmenter",
            arguments={"max_chunk_size": 2000, "overlap": 200},
            limits=ResourceLimits(
                max_tokens=100000,
                max_duration_seconds=30.0
            )
        ),
        StageWithLimits(
            name="extract_entities",
            tool="ner_extractor",
            arguments={"entity_types": ["PERSON", "ORG", "GPE", "DATE"]},
            limits=ResourceLimits(
                max_tokens=50000,
                max_cost_cents=100,
                rate_limit_per_second=10
            ),
            depends_on=["segment"]
        ),
        StageWithLimits(
            name="deduplicate",
            tool="entity_deduplicator",
            arguments={"similarity_threshold": 0.9},
            limits=ResourceLimits(
                max_tokens=10000,
                max_duration_seconds=10.0
            ),
            depends_on=["extract_entities"]
        )
    ],
    total_max_tokens=200000,
    total_max_cost_cents=200,
    total_max_duration_seconds=120.0,
    required_inputs=["text"],
    tags=["nlp", "extraction", "entities"],
    example_inputs={
        "text": "Apple Inc. was founded by Steve Jobs in Cupertino, California."
    }
))

register_workflow(NamedWorkflow(
    id="text_segmentation",
    name="Intelligent Text Segmentation",
    description="Split text into semantic segments with overlap and metadata",
    category=WorkflowCategory.NLP,
    stages=[
        StageWithLimits(
            name="preprocess",
            tool="text_cleaner",
            arguments={"remove_extra_whitespace": True},
            limits=ResourceLimits(max_tokens=50000)
        ),
        StageWithLimits(
            name="segment",
            tool="semantic_segmenter",
            arguments={
                "method": "sentence_transformer",
                "max_segment_tokens": 500,
                "overlap_tokens": 50
            },
            limits=ResourceLimits(
                max_tokens=100000,
                max_cost_cents=50,
                max_duration_seconds=60.0
            ),
            depends_on=["preprocess"]
        ),
        StageWithLimits(
            name="add_metadata",
            tool="segment_annotator",
            arguments={"include_position": True, "include_context": True},
            limits=ResourceLimits(max_tokens=20000),
            depends_on=["segment"]
        )
    ],
    total_max_tokens=200000,
    required_inputs=["text"],
    tags=["nlp", "segmentation", "chunking"]
))

register_workflow(NamedWorkflow(
    id="sentiment_analysis",
    name="Multi-Level Sentiment Analysis",
    description="Analyze sentiment at document, paragraph, and sentence levels",
    category=WorkflowCategory.ANALYSIS,
    stages=[
        StageWithLimits(
            name="segment_by_paragraph",
            tool="paragraph_segmenter",
            limits=ResourceLimits(max_tokens=50000)
        ),
        StageWithLimits(
            name="sentence_split",
            tool="sentence_splitter",
            limits=ResourceLimits(max_tokens=100000),
            depends_on=["segment_by_paragraph"]
        ),
        StageWithLimits(
            name="analyze_sentences",
            tool="sentiment_analyzer",
            arguments={"granularity": "sentence"},
            limits=ResourceLimits(
                max_tokens=150000,
                max_cost_cents=100,
                rate_limit_per_second=20
            ),
            depends_on=["sentence_split"]
        ),
        StageWithLimits(
            name="aggregate_paragraph",
            tool="sentiment_aggregator",
            arguments={"level": "paragraph"},
            limits=ResourceLimits(max_tokens=10000),
            depends_on=["analyze_sentences"]
        ),
        StageWithLimits(
            name="aggregate_document",
            tool="sentiment_aggregator",
            arguments={"level": "document"},
            limits=ResourceLimits(max_tokens=5000),
            depends_on=["aggregate_paragraph"]
        )
    ],
    total_max_tokens=300000,
    total_max_cost_cents=150,
    required_inputs=["text"],
    tags=["nlp", "sentiment", "analysis"]
))

# -----------------------------------------------------------------------------
# Extraction Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="structured_data_extraction",
    name="Structured Data Extraction from Text",
    description="Extract structured data using LLM with schema validation",
    category=WorkflowCategory.EXTRACTION,
    stages=[
        StageWithLimits(
            name="chunk_text",
            tool="text_chunker",
            arguments={"chunk_size": 4000, "overlap": 400},
            limits=ResourceLimits(max_tokens=100000)
        ),
        StageWithLimits(
            name="llm_extract",
            tool="llm_extractor",
            arguments={
                "model": "claude-3-sonnet-20240229",
                "temperature": 0.0,
                "schema_ref": "extraction_schema"
            },
            limits=ResourceLimits(
                max_tokens=200000,
                max_output_tokens=50000,
                max_cost_cents=500,
                rate_limit_per_second=5
            ),
            depends_on=["chunk_text"]
        ),
        StageWithLimits(
            name="validate",
            tool="schema_validator",
            limits=ResourceLimits(max_tokens=10000),
            depends_on=["llm_extract"]
        ),
        StageWithLimits(
            name="merge_results",
            tool="result_merger",
            arguments={"deduplicate": True},
            limits=ResourceLimits(max_tokens=20000),
            depends_on=["validate"]
        )
    ],
    total_max_tokens=400000,
    total_max_cost_cents=600,
    required_inputs=["text", "schema"],
    tags=["extraction", "llm", "structured"]
))

register_workflow(NamedWorkflow(
    id="table_extraction",
    name="Table and Structured Content Extraction",
    description="Extract tables, lists, and structured content from documents",
    category=WorkflowCategory.EXTRACTION,
    stages=[
        StageWithLimits(
            name="detect_structure",
            tool="structure_detector",
            arguments={"types": ["table", "list", "code"]},
            limits=ResourceLimits(
                max_tokens=50000,
                max_duration_seconds=30.0
            )
        ),
        StageWithLimits(
            name="extract_tables",
            tool="table_extractor",
            limits=ResourceLimits(
                max_tokens=100000,
                max_cost_cents=50
            ),
            depends_on=["detect_structure"]
        ),
        StageWithLimits(
            name="parse_to_dataframe",
            tool="table_parser",
            arguments={"format": "pandas"},
            limits=ResourceLimits(max_tokens=50000),
            depends_on=["extract_tables"]
        )
    ],
    total_max_tokens=200000,
    required_inputs=["document"],
    tags=["extraction", "tables", "structured"]
))

# -----------------------------------------------------------------------------
# Analysis Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="document_similarity",
    name="Document Similarity Comparison",
    description="Segment → Embed → Compare all pairs for similarity",
    category=WorkflowCategory.ANALYSIS,
    stages=[
        StageWithLimits(
            name="segment_all",
            tool="document_segmenter",
            arguments={"segment_size": 1000},
            limits=ResourceLimits(
                max_tokens=500000,
                max_duration_seconds=60.0
            )
        ),
        StageWithLimits(
            name="embed_segments",
            tool="embedding_generator",
            arguments={"model": "text-embedding-3-small"},
            limits=ResourceLimits(
                max_tokens=1000000,
                max_cost_cents=200,
                rate_limit_per_second=50
            ),
            depends_on=["segment_all"]
        ),
        StageWithLimits(
            name="compute_similarity",
            tool="pairwise_similarity",
            arguments={"metric": "cosine"},
            limits=ResourceLimits(
                max_tokens=100000,
                max_duration_seconds=120.0,
                max_memory_mb=1024
            ),
            depends_on=["embed_segments"]
        ),
        StageWithLimits(
            name="cluster_similar",
            tool="clustering",
            arguments={"method": "hierarchical", "threshold": 0.7},
            limits=ResourceLimits(max_duration_seconds=60.0),
            depends_on=["compute_similarity"]
        )
    ],
    total_max_tokens=2000000,
    total_max_cost_cents=300,
    required_inputs=["documents"],
    tags=["analysis", "similarity", "embeddings", "clustering"]
))

register_workflow(NamedWorkflow(
    id="topic_modeling",
    name="Topic Modeling and Extraction",
    description="Extract topics from a collection of documents",
    category=WorkflowCategory.ANALYSIS,
    stages=[
        StageWithLimits(
            name="preprocess",
            tool="text_preprocessor",
            arguments={"lowercase": True, "remove_stopwords": True},
            limits=ResourceLimits(max_tokens=200000)
        ),
        StageWithLimits(
            name="vectorize",
            tool="tfidf_vectorizer",
            limits=ResourceLimits(
                max_tokens=500000,
                max_memory_mb=2048
            ),
            depends_on=["preprocess"]
        ),
        StageWithLimits(
            name="extract_topics",
            tool="topic_extractor",
            arguments={"method": "lda", "num_topics": 10},
            limits=ResourceLimits(
                max_tokens=1000000,
                max_duration_seconds=300.0,
                max_memory_mb=4096
            ),
            depends_on=["vectorize"]
        ),
        StageWithLimits(
            name="label_topics",
            tool="topic_labeler",
            arguments={"top_n_words": 10},
            limits=ResourceLimits(
                max_tokens=50000,
                max_cost_cents=100
            ),
            depends_on=["extract_topics"]
        )
    ],
    total_max_tokens=2000000,
    total_max_cost_cents=150,
    required_inputs=["documents"],
    tags=["analysis", "topics", "lda", "clustering"]
))

# -----------------------------------------------------------------------------
# Search Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="semantic_search",
    name="Semantic Search with Reranking",
    description="Embed query → Search → Rerank → Extract relevant content",
    category=WorkflowCategory.SEARCH,
    stages=[
        StageWithLimits(
            name="embed_query",
            tool="query_embedder",
            limits=ResourceLimits(
                max_tokens=1000,
                max_cost_cents=5
            )
        ),
        StageWithLimits(
            name="vector_search",
            tool="vector_searcher",
            arguments={"top_k": 100},
            limits=ResourceLimits(
                max_duration_seconds=10.0,
                rate_limit_per_second=10
            ),
            depends_on=["embed_query"]
        ),
        StageWithLimits(
            name="rerank",
            tool="semantic_reranker",
            arguments={"top_n": 10},
            limits=ResourceLimits(
                max_tokens=100000,
                max_cost_cents=50,
                max_duration_seconds=30.0
            ),
            depends_on=["vector_search"]
        ),
        StageWithLimits(
            name="extract_snippets",
            tool="snippet_extractor",
            arguments={"max_snippet_length": 200},
            limits=ResourceLimits(max_tokens=20000),
            depends_on=["rerank"]
        )
    ],
    total_max_tokens=200000,
    total_max_cost_cents=100,
    required_inputs=["query", "index"],
    tags=["search", "semantic", "reranking", "embeddings"]
))

# -----------------------------------------------------------------------------
# Generation Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="summarization_cascade",
    name="Multi-Level Summarization Cascade",
    description="Segment → Summarize each → Merge → Final summary",
    category=WorkflowCategory.GENERATION,
    stages=[
        StageWithLimits(
            name="segment",
            tool="document_segmenter",
            arguments={"max_segment_tokens": 2000},
            limits=ResourceLimits(max_tokens=100000)
        ),
        StageWithLimits(
            name="summarize_segments",
            tool="llm_summarizer",
            arguments={
                "model": "claude-3-haiku-20240307",
                "max_summary_tokens": 200,
                "temperature": 0.3
            },
            limits=ResourceLimits(
                max_tokens=500000,
                max_output_tokens=100000,
                max_cost_cents=200,
                rate_limit_per_second=10
            ),
            depends_on=["segment"]
        ),
        StageWithLimits(
            name="merge_summaries",
            tool="summary_merger",
            limits=ResourceLimits(max_tokens=50000),
            depends_on=["summarize_segments"]
        ),
        StageWithLimits(
            name="final_summary",
            tool="llm_summarizer",
            arguments={
                "model": "claude-3-sonnet-20240229",
                "max_summary_tokens": 500,
                "temperature": 0.5
            },
            limits=ResourceLimits(
                max_tokens=50000,
                max_output_tokens=10000,
                max_cost_cents=100
            ),
            depends_on=["merge_summaries"]
        )
    ],
    total_max_tokens=800000,
    total_max_cost_cents=400,
    required_inputs=["document"],
    tags=["generation", "summarization", "llm"]
))

register_workflow(NamedWorkflow(
    id="qa_generation",
    name="Question-Answer Pair Generation",
    description="Generate Q&A pairs from documents for training/testing",
    category=WorkflowCategory.GENERATION,
    stages=[
        StageWithLimits(
            name="segment",
            tool="content_segmenter",
            arguments={"segment_by": "topic"},
            limits=ResourceLimits(max_tokens=100000)
        ),
        StageWithLimits(
            name="generate_questions",
            tool="question_generator",
            arguments={
                "num_questions_per_segment": 3,
                "difficulty": "medium"
            },
            limits=ResourceLimits(
                max_tokens=500000,
                max_output_tokens=100000,
                max_cost_cents=300
            ),
            depends_on=["segment"]
        ),
        StageWithLimits(
            name="generate_answers",
            tool="answer_generator",
            limits=ResourceLimits(
                max_tokens=500000,
                max_output_tokens=100000,
                max_cost_cents=300
            ),
            depends_on=["generate_questions"]
        ),
        StageWithLimits(
            name="validate_qa",
            tool="qa_validator",
            arguments={"check_answerability": True},
            limits=ResourceLimits(max_tokens=100000),
            depends_on=["generate_answers"]
        )
    ],
    total_max_tokens=1500000,
    total_max_cost_cents=700,
    required_inputs=["document"],
    tags=["generation", "qa", "training"]
))

# -----------------------------------------------------------------------------
# Transformation Workflows
# -----------------------------------------------------------------------------

register_workflow(NamedWorkflow(
    id="text_to_knowledge_graph",
    name="Text to Knowledge Graph",
    description="Extract entities, relations, and build knowledge graph",
    category=WorkflowCategory.TRANSFORMATION,
    stages=[
        StageWithLimits(
            name="extract_entities",
            tool="ner_extractor",
            limits=ResourceLimits(
                max_tokens=200000,
                max_cost_cents=100
            )
        ),
        StageWithLimits(
            name="extract_relations",
            tool="relation_extractor",
            limits=ResourceLimits(
                max_tokens=300000,
                max_cost_cents=200,
                rate_limit_per_second=5
            ),
            depends_on=["extract_entities"]
        ),
        StageWithLimits(
            name="resolve_entities",
            tool="entity_resolver",
            arguments={"use_knowledge_base": True},
            limits=ResourceLimits(
                max_tokens=100000,
                max_cost_cents=50
            ),
            depends_on=["extract_entities"]
        ),
        StageWithLimits(
            name="build_graph",
            tool="graph_builder",
            arguments={"format": "networkx"},
            limits=ResourceLimits(
                max_duration_seconds=60.0,
                max_memory_mb=2048
            ),
            depends_on=["extract_relations", "resolve_entities"]
        )
    ],
    total_max_tokens=800000,
    total_max_cost_cents=400,
    required_inputs=["text"],
    tags=["transformation", "knowledge_graph", "entities", "relations"]
))


def get_workflow_library() -> Dict[str, NamedWorkflow]:
    """Get all registered workflows."""
    return WORKFLOW_LIBRARY


def get_workflow_by_id(workflow_id: str) -> Optional[NamedWorkflow]:
    """Get a workflow by ID."""
    return WORKFLOW_LIBRARY.get(workflow_id)


def get_workflows_by_category(category: WorkflowCategory) -> List[NamedWorkflow]:
    """Get all workflows in a category."""
    return [w for w in WORKFLOW_LIBRARY.values() if w.category == category]


def get_workflows_by_tag(tag: str) -> List[NamedWorkflow]:
    """Get all workflows with a specific tag."""
    return [w for w in WORKFLOW_LIBRARY.values() if tag in w.tags]
