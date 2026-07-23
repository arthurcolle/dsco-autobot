"""API routes for search orchestration.

Provides endpoints for:
- Iterative search with processor escalation
- Parallel multi-processor search
- Search plan management
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from parallel_websearch_server.search_orchestrator import (
    EscalationStrategy,
    OrchestratedSearchResult,
    ProcessorTier,
    SearchIteration,
    SearchOrchestrator,
    SearchPlan,
    get_search_orchestrator,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/orchestration", tags=["Search Orchestration"])


# =============================================================================
# Request/Response Models
# =============================================================================

class IterativeSearchRequest(BaseModel):
    """Request for iterative search."""
    objective: str = Field(..., description="What to search for")
    max_iterations: int = Field(default=3, ge=1, le=10, description="Maximum iterations")
    initial_terms: Optional[List[str]] = Field(default=None, description="Starting search terms")
    escalation_strategy: str = Field(
        default="adaptive",
        description="Escalation strategy: fixed, linear, adaptive, parallel, hybrid"
    )
    starting_processor: str = Field(default="lite", description="Initial processor tier")
    final_processor: str = Field(default="ultra", description="Final synthesis processor")
    enable_synthesis: bool = Field(default=True, description="Generate final synthesis")


class ParallelSearchRequest(BaseModel):
    """Request for parallel search."""
    objective: str = Field(..., description="What to search for")
    terms: List[str] = Field(..., description="Search terms")
    processors: List[str] = Field(
        default=["lite", "base", "core"],
        description="Processors to run in parallel"
    )


class SearchIterationResponse(BaseModel):
    """Response model for a search iteration."""
    iteration_id: str
    iteration_number: int
    processor: str
    input_terms: List[str]
    discovered_terms: List[str]
    result_count: int
    quality_score: float
    duration_ms: float
    started_at: str
    completed_at: str


class OrchestratedSearchResponse(BaseModel):
    """Response model for orchestrated search."""
    search_id: str
    objective: str
    final_terms: List[str]
    iterations: List[SearchIterationResponse]
    final_results: List[Dict[str, Any]]
    synthesis: Optional[str]
    total_duration_ms: float
    total_api_calls: int
    escalation_path: List[str]
    quality_metrics: Dict[str, Any]


class ParallelSearchResponse(BaseModel):
    """Response for parallel search."""
    objective: str
    terms: List[str]
    results: List[Dict[str, Any]]


# =============================================================================
# Helper Functions
# =============================================================================

def get_orchestrator(request: Request) -> SearchOrchestrator:
    """Get search orchestrator with HTTP client from request state."""
    http_client = request.app.state.http_client

    # Get auth headers from request
    auth_headers = {}
    if "x-api-key" in request.headers:
        auth_headers["x-api-key"] = request.headers["x-api-key"]
    if "authorization" in request.headers:
        auth_headers["authorization"] = request.headers["authorization"]

    return get_search_orchestrator(http_client, auth_headers)


def _convert_iteration(it: SearchIteration) -> SearchIterationResponse:
    """Convert SearchIteration to response model."""
    return SearchIterationResponse(
        iteration_id=it.iteration_id,
        iteration_number=it.iteration_number,
        processor=it.processor.value,
        input_terms=it.input_terms,
        discovered_terms=it.discovered_terms,
        result_count=it.result_count,
        quality_score=it.quality_score,
        duration_ms=it.duration_ms,
        started_at=it.started_at,
        completed_at=it.completed_at,
    )


def _parse_processor(value: str) -> ProcessorTier:
    """Parse processor tier from string."""
    try:
        return ProcessorTier(value.lower())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid processor: {value}. Must be one of: lite, base, core, pro, ultra"
        )


def _parse_strategy(value: str) -> EscalationStrategy:
    """Parse escalation strategy from string."""
    try:
        return EscalationStrategy(value.lower())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid strategy: {value}. Must be one of: fixed, linear, adaptive, parallel, hybrid"
        )


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/search", response_model=OrchestratedSearchResponse)
async def iterative_search(
    request: Request,
    body: IterativeSearchRequest,
) -> OrchestratedSearchResponse:
    """Execute iterative search with processor escalation.

    This endpoint implements a sophisticated multi-stage search strategy:

    1. **Term Discovery**: Uses a lightweight processor to discover initial search terms
    2. **Iterative Search**: Executes searches and refines terms based on results
    3. **Processor Escalation**: Automatically upgrades to more powerful processors
    4. **Synthesis**: Uses the most capable processor to synthesize final results

    ## Escalation Strategies

    - `fixed`: Use starting processor only
    - `linear`: Progress through lite → base → core → pro → ultra
    - `adaptive`: Escalate based on result quality (recommended)
    - `parallel`: Run multiple processors simultaneously
    - `hybrid`: Combine adaptive with parallel for key stages

    ## Example Usage

    ```json
    {
        "objective": "Find Python machine learning frameworks",
        "max_iterations": 3,
        "escalation_strategy": "adaptive",
        "starting_processor": "lite",
        "final_processor": "ultra"
    }
    ```
    """
    orchestrator = get_orchestrator(request)

    try:
        result = await orchestrator.iterative_search(
            objective=body.objective,
            max_iterations=body.max_iterations,
            initial_terms=body.initial_terms,
            escalation_strategy=_parse_strategy(body.escalation_strategy),
            starting_processor=_parse_processor(body.starting_processor),
            final_processor=_parse_processor(body.final_processor),
            enable_synthesis=body.enable_synthesis,
        )

        return OrchestratedSearchResponse(
            search_id=result.search_id,
            objective=result.objective,
            final_terms=result.final_terms,
            iterations=[_convert_iteration(it) for it in result.iterations],
            final_results=result.final_results,
            synthesis=result.synthesis,
            total_duration_ms=result.total_duration_ms,
            total_api_calls=result.total_api_calls,
            escalation_path=[p.value for p in result.escalation_path],
            quality_metrics=result.quality_metrics,
        )

    except Exception as e:
        logger.error(f"Iterative search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/parallel", response_model=ParallelSearchResponse)
async def parallel_search(
    request: Request,
    body: ParallelSearchRequest,
) -> ParallelSearchResponse:
    """Execute searches in parallel across multiple processors.

    Runs the same search terms simultaneously across multiple processor tiers,
    allowing you to compare results and aggregate findings.

    ## Example Usage

    ```json
    {
        "objective": "AI startups in healthcare",
        "terms": ["AI healthcare", "medical AI", "healthtech startups"],
        "processors": ["lite", "base", "core"]
    }
    ```
    """
    orchestrator = get_orchestrator(request)

    processors = [_parse_processor(p) for p in body.processors]

    try:
        results = await orchestrator.parallel_search(
            objective=body.objective,
            processors=processors,
            terms=body.terms,
        )

        # Aggregate results from all processors
        all_results = []
        for processor, processor_results in results:
            for result in processor_results:
                result["_processor"] = processor.value
                all_results.append(result)

        return ParallelSearchResponse(
            objective=body.objective,
            terms=body.terms,
            results=all_results,
        )

    except Exception as e:
        logger.error(f"Parallel search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/processors")
async def list_processors() -> Dict[str, Any]:
    """List available processor tiers and their characteristics."""
    return {
        "processors": [
            {
                "name": "lite",
                "description": "Lightweight processor for quick term discovery",
                "use_cases": ["Initial term generation", "Quick searches", "High-volume operations"],
                "relative_cost": 1,
            },
            {
                "name": "base",
                "description": "Standard processor for general searches",
                "use_cases": ["General web search", "Term refinement", "Moderate complexity"],
                "relative_cost": 2,
            },
            {
                "name": "core",
                "description": "Core processor for thorough analysis",
                "use_cases": ["Deep research", "Complex queries", "Quality results"],
                "relative_cost": 4,
            },
            {
                "name": "pro",
                "description": "Professional processor for advanced tasks",
                "use_cases": ["Expert analysis", "Multi-faceted research", "High accuracy"],
                "relative_cost": 8,
            },
            {
                "name": "ultra",
                "description": "Most capable processor for synthesis",
                "use_cases": ["Result synthesis", "Comprehensive analysis", "Final summaries"],
                "relative_cost": 16,
            },
        ],
        "escalation_order": ["lite", "base", "core", "pro", "ultra"],
    }


@router.get("/strategies")
async def list_strategies() -> Dict[str, Any]:
    """List available escalation strategies."""
    return {
        "strategies": [
            {
                "name": "fixed",
                "description": "Use a single specified processor for all iterations",
                "best_for": "Predictable cost, consistent results",
            },
            {
                "name": "linear",
                "description": "Progress through processors in order: lite → base → core → pro → ultra",
                "best_for": "Guaranteed progression, comprehensive coverage",
            },
            {
                "name": "adaptive",
                "description": "Escalate based on result quality - upgrade when results are poor",
                "best_for": "Cost efficiency, most use cases (recommended)",
            },
            {
                "name": "parallel",
                "description": "Run multiple processors simultaneously",
                "best_for": "Speed, result comparison, redundancy",
            },
            {
                "name": "hybrid",
                "description": "Combine adaptive escalation with parallel execution at key stages",
                "best_for": "Maximum coverage with efficiency",
            },
        ],
        "default": "adaptive",
    }


@router.post("/discover-terms")
async def discover_terms(
    request: Request,
    objective: str = Query(..., description="Search objective"),
    processor: str = Query(default="lite", description="Processor to use"),
) -> Dict[str, Any]:
    """Discover search terms for an objective using AI.

    Uses the specified processor to generate a comprehensive list of search
    terms based on the given objective.
    """
    orchestrator = get_orchestrator(request)
    processor_tier = _parse_processor(processor)

    try:
        terms = await orchestrator.term_engine.discover_initial_terms(
            objective=objective,
            processor=processor_tier,
        )

        return {
            "objective": objective,
            "processor": processor,
            "terms": terms,
            "term_count": len(terms),
        }

    except Exception as e:
        logger.error(f"Term discovery failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
