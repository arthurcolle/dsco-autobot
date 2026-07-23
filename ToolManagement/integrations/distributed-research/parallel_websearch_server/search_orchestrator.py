"""Iterative Search Orchestration with Processor Escalation.

Implements a multi-stage search strategy that:
1. Uses lightweight processors to discover search terms
2. Escalates to more powerful processors for deeper analysis
3. Iteratively refines results through multiple passes
4. Synthesizes final results with the most capable processor

Search Strategy Pipeline:
    lite -> discover terms -> delegated search -> refine terms -> ultra synthesis

Example:
    orchestrator = SearchOrchestrator(http_client)

    result = await orchestrator.iterative_search(
        objective="Find all machine learning frameworks for Python",
        max_iterations=3,
        escalation_strategy="adaptive",
    )
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from parallel_websearch_server.resilience import (
    get_circuit_breaker,
    get_distributed_tracer,
    get_anomaly_detector,
    get_health_scorer,
    CircuitBreakerError,
)

logger = logging.getLogger(__name__)


class ProcessorTier(str, Enum):
    """Available processor tiers in order of capability/cost."""
    LITE = "lite"
    BASE = "base"
    CORE = "core"
    PRO = "pro"
    ULTRA = "ultra"

    @classmethod
    def escalation_order(cls) -> List['ProcessorTier']:
        """Get processors in escalation order."""
        return [cls.LITE, cls.BASE, cls.CORE, cls.PRO, cls.ULTRA]

    def next_tier(self) -> Optional['ProcessorTier']:
        """Get the next tier up."""
        order = self.escalation_order()
        idx = order.index(self)
        if idx < len(order) - 1:
            return order[idx + 1]
        return None


class EscalationStrategy(str, Enum):
    """Strategies for processor escalation."""
    FIXED = "fixed"           # Use specified processor only
    LINEAR = "linear"         # lite -> base -> core -> pro -> ultra
    ADAPTIVE = "adaptive"     # Escalate based on result quality
    PARALLEL = "parallel"     # Run multiple processors in parallel
    HYBRID = "hybrid"         # Combine adaptive with parallel for key stages


@dataclass
class SearchIteration:
    """Record of a single search iteration."""
    iteration_id: str
    iteration_number: int
    processor: ProcessorTier
    input_terms: List[str]
    discovered_terms: List[str]
    results: List[Dict[str, Any]]
    result_count: int
    quality_score: float
    duration_ms: float
    started_at: str
    completed_at: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchPlan:
    """Execution plan for iterative search."""
    plan_id: str
    objective: str
    stages: List[Dict[str, Any]]
    estimated_iterations: int
    escalation_strategy: EscalationStrategy
    created_at: str


@dataclass
class OrchestratedSearchResult:
    """Final result of orchestrated search."""
    search_id: str
    objective: str
    final_terms: List[str]
    all_discovered_terms: Set[str]
    iterations: List[SearchIteration]
    final_results: List[Dict[str, Any]]
    synthesis: Optional[str]
    total_duration_ms: float
    total_api_calls: int
    escalation_path: List[ProcessorTier]
    quality_metrics: Dict[str, Any]
    plan: SearchPlan


class TermDiscoveryEngine:
    """Engine for discovering and refining search terms."""

    # Prompt templates for different stages
    INITIAL_TERMS_PROMPT = """Given the search objective: "{objective}"

Generate a comprehensive list of search terms that would help find relevant information.
Include:
- Direct keywords and phrases
- Related concepts and synonyms
- Technical terms if applicable
- Common variations and abbreviations

Output as a JSON array of strings."""

    REFINE_TERMS_PROMPT = """Based on the search results for objective: "{objective}"

Current search terms used: {current_terms}

Results summary:
{results_summary}

Suggest additional search terms that would:
1. Fill gaps in the current results
2. Explore related areas not yet covered
3. Find more specific/detailed information
4. Discover alternative approaches or solutions

Output as a JSON array of new search terms (exclude terms already used)."""

    SYNTHESIS_PROMPT = """Synthesize the search results for objective: "{objective}"

Search iterations performed: {iteration_count}
Total unique terms explored: {term_count}
Total results found: {result_count}

Results by iteration:
{iteration_summaries}

Provide a comprehensive synthesis that:
1. Summarizes key findings
2. Identifies patterns and themes
3. Highlights the most relevant results
4. Notes any gaps or areas needing further research
5. Provides actionable recommendations"""

    def __init__(self, http_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        self.client = http_client
        self.auth_headers = auth_headers

    async def discover_initial_terms(
        self,
        objective: str,
        processor: ProcessorTier = ProcessorTier.LITE,
    ) -> List[str]:
        """Use AI to discover initial search terms."""
        prompt = self.INITIAL_TERMS_PROMPT.format(objective=objective)

        try:
            response = await self._call_processor(
                processor=processor,
                input_text=prompt,
                task_spec={"output_format": "json"},
            )

            terms = self._parse_terms_response(response)
            logger.info(f"Discovered {len(terms)} initial terms for objective")
            return terms

        except Exception as e:
            logger.error(f"Failed to discover initial terms: {e}")
            # Fallback: extract keywords from objective
            return self._extract_keywords(objective)

    async def refine_terms(
        self,
        objective: str,
        current_terms: List[str],
        results: List[Dict[str, Any]],
        processor: ProcessorTier = ProcessorTier.BASE,
    ) -> List[str]:
        """Refine search terms based on current results."""
        results_summary = self._summarize_results(results)

        prompt = self.REFINE_TERMS_PROMPT.format(
            objective=objective,
            current_terms=json.dumps(current_terms),
            results_summary=results_summary,
        )

        try:
            response = await self._call_processor(
                processor=processor,
                input_text=prompt,
                task_spec={"output_format": "json"},
            )

            new_terms = self._parse_terms_response(response)
            # Filter out terms already used
            new_terms = [t for t in new_terms if t.lower() not in
                        {ct.lower() for ct in current_terms}]

            logger.info(f"Refined search: {len(new_terms)} new terms discovered")
            return new_terms

        except Exception as e:
            logger.error(f"Failed to refine terms: {e}")
            return []

    async def synthesize_results(
        self,
        objective: str,
        iterations: List[SearchIteration],
        processor: ProcessorTier = ProcessorTier.ULTRA,
    ) -> str:
        """Synthesize all search results into a comprehensive summary."""
        iteration_summaries = "\n\n".join(
            f"Iteration {it.iteration_number} ({it.processor.value}):\n"
            f"  Terms: {', '.join(it.input_terms[:5])}{'...' if len(it.input_terms) > 5 else ''}\n"
            f"  Results: {it.result_count} found\n"
            f"  Quality: {it.quality_score:.2f}"
            for it in iterations
        )

        all_terms = set()
        for it in iterations:
            all_terms.update(it.input_terms)
            all_terms.update(it.discovered_terms)

        prompt = self.SYNTHESIS_PROMPT.format(
            objective=objective,
            iteration_count=len(iterations),
            term_count=len(all_terms),
            result_count=sum(it.result_count for it in iterations),
            iteration_summaries=iteration_summaries,
        )

        try:
            response = await self._call_processor(
                processor=processor,
                input_text=prompt,
            )
            return response.get("output", {}).get("content", "")

        except Exception as e:
            logger.error(f"Failed to synthesize results: {e}")
            return f"Synthesis failed: {e}"

    async def _call_processor(
        self,
        processor: ProcessorTier,
        input_text: str,
        task_spec: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Call the Parallel API with specified processor."""
        payload = {
            "processor": processor.value,
            "input": input_text,
        }
        if task_spec:
            payload["task_spec"] = task_spec

        response = await self.client.post(
            "/v1/tasks/runs/managed",
            json=payload,
            headers=self.auth_headers,
            params={"poll_interval_seconds": 2, "timeout_seconds": 120},
        )
        response.raise_for_status()
        return response.json()

    def _parse_terms_response(self, response: Dict[str, Any]) -> List[str]:
        """Parse search terms from API response."""
        output = response.get("output_text") or response.get("output_json")

        if isinstance(output, list):
            return [str(t) for t in output]

        if isinstance(output, str):
            # Try to parse as JSON
            try:
                parsed = json.loads(output)
                if isinstance(parsed, list):
                    return [str(t) for t in parsed]
            except json.JSONDecodeError:
                pass

            # Fall back to line splitting
            lines = [l.strip().strip('"-,') for l in output.split('\n')]
            return [l for l in lines if l and len(l) > 2]

        return []

    def _summarize_results(self, results: List[Dict[str, Any]], max_results: int = 10) -> str:
        """Create a summary of search results."""
        if not results:
            return "No results found."

        summaries = []
        for i, result in enumerate(results[:max_results]):
            title = result.get("title", "Untitled")
            snippet = result.get("snippet", result.get("description", ""))[:200]
            summaries.append(f"{i+1}. {title}\n   {snippet}")

        return "\n\n".join(summaries)

    def _extract_keywords(self, text: str) -> List[str]:
        """Extract basic keywords from text as fallback."""
        import re
        # Remove punctuation and split
        words = re.findall(r'\b\w+\b', text.lower())
        # Filter common words
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
                    'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
                    'would', 'could', 'should', 'may', 'might', 'must', 'shall',
                    'can', 'need', 'dare', 'ought', 'used', 'to', 'of', 'in',
                    'for', 'on', 'with', 'at', 'by', 'from', 'as', 'into', 'through',
                    'during', 'before', 'after', 'above', 'below', 'between', 'under',
                    'again', 'further', 'then', 'once', 'and', 'but', 'if', 'or',
                    'because', 'until', 'while', 'although', 'all', 'find', 'get'}
        keywords = [w for w in words if w not in stopwords and len(w) > 2]
        return list(dict.fromkeys(keywords))[:10]  # Dedupe and limit


class SearchOrchestrator:
    """Orchestrates multi-stage iterative search with processor escalation.

    Features:
    - Automatic term discovery and refinement
    - Intelligent processor escalation based on result quality
    - Parallel search execution for efficiency
    - Result deduplication and ranking
    - Quality scoring and adaptive iteration
    - Comprehensive synthesis with ultra processor
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        auth_headers: Optional[Dict[str, str]] = None,
    ):
        self.client = http_client
        self.auth_headers = auth_headers or {}

        # Components
        self.term_engine = TermDiscoveryEngine(http_client, self.auth_headers)
        self.circuit_breaker = get_circuit_breaker("search-orchestrator")
        self.tracer = get_distributed_tracer()
        self.anomaly_detector = get_anomaly_detector()
        self.health_scorer = get_health_scorer()

        # Configuration
        self.min_quality_threshold = 0.3
        self.escalation_quality_threshold = 0.6
        self.max_results_per_search = 20

    async def iterative_search(
        self,
        objective: str,
        max_iterations: int = 3,
        initial_terms: Optional[List[str]] = None,
        escalation_strategy: EscalationStrategy = EscalationStrategy.ADAPTIVE,
        starting_processor: ProcessorTier = ProcessorTier.LITE,
        final_processor: ProcessorTier = ProcessorTier.ULTRA,
        enable_synthesis: bool = True,
    ) -> OrchestratedSearchResult:
        """Execute iterative search with processor escalation.

        Args:
            objective: What we're searching for
            max_iterations: Maximum search iterations
            initial_terms: Optional starting search terms
            escalation_strategy: How to escalate processors
            starting_processor: Initial processor tier
            final_processor: Processor for final synthesis
            enable_synthesis: Whether to synthesize results

        Returns:
            OrchestratedSearchResult with all iterations and final synthesis
        """
        search_id = f"orch_{uuid.uuid4().hex[:12]}"
        start_time = time.time()
        started_at = datetime.utcnow().isoformat()

        logger.info(f"Starting orchestrated search {search_id}: {objective}")

        # Create execution plan
        plan = self._create_plan(
            search_id=search_id,
            objective=objective,
            max_iterations=max_iterations,
            escalation_strategy=escalation_strategy,
            starting_processor=starting_processor,
        )

        # Initialize state
        iterations: List[SearchIteration] = []
        all_terms: Set[str] = set()
        all_results: List[Dict[str, Any]] = []
        escalation_path: List[ProcessorTier] = []
        current_processor = starting_processor
        total_api_calls = 0

        # Get initial terms
        if initial_terms:
            current_terms = initial_terms
        else:
            current_terms = await self.term_engine.discover_initial_terms(
                objective, current_processor
            )
            total_api_calls += 1

        all_terms.update(current_terms)

        # Iterative search loop
        for iteration_num in range(max_iterations):
            iteration_start = time.time()

            # Determine processor for this iteration
            if escalation_strategy == EscalationStrategy.LINEAR:
                current_processor = self._linear_escalation(
                    iteration_num, max_iterations, starting_processor, final_processor
                )
            elif escalation_strategy == EscalationStrategy.ADAPTIVE:
                if iterations and iterations[-1].quality_score < self.escalation_quality_threshold:
                    next_tier = current_processor.next_tier()
                    if next_tier:
                        current_processor = next_tier
                        logger.info(f"Escalating to {current_processor.value} due to quality")

            escalation_path.append(current_processor)

            # Execute search with current terms
            try:
                results = await self._execute_search(
                    terms=current_terms,
                    processor=current_processor,
                )
                total_api_calls += 1

            except CircuitBreakerError as e:
                logger.warning(f"Circuit breaker open: {e}")
                break
            except Exception as e:
                logger.error(f"Search iteration {iteration_num} failed: {e}")
                continue

            # Score result quality
            quality_score = self._score_results(results, objective)

            # Discover new terms from results
            discovered_terms = []
            if iteration_num < max_iterations - 1:
                discovered_terms = await self.term_engine.refine_terms(
                    objective=objective,
                    current_terms=list(all_terms),
                    results=results,
                    processor=current_processor,
                )
                total_api_calls += 1
                all_terms.update(discovered_terms)

            # Record iteration
            iteration = SearchIteration(
                iteration_id=f"{search_id}_iter_{iteration_num}",
                iteration_number=iteration_num + 1,
                processor=current_processor,
                input_terms=current_terms.copy(),
                discovered_terms=discovered_terms,
                results=results,
                result_count=len(results),
                quality_score=quality_score,
                duration_ms=(time.time() - iteration_start) * 1000,
                started_at=datetime.fromtimestamp(iteration_start).isoformat(),
                completed_at=datetime.utcnow().isoformat(),
                metadata={
                    "escalation_strategy": escalation_strategy.value,
                    "terms_count": len(current_terms),
                }
            )
            iterations.append(iteration)
            all_results.extend(results)

            logger.info(
                f"Iteration {iteration_num + 1}: {len(results)} results, "
                f"quality={quality_score:.2f}, processor={current_processor.value}"
            )

            # Check if we should stop early
            if quality_score >= 0.9:
                logger.info("High quality results achieved, stopping early")
                break

            # Update terms for next iteration
            if discovered_terms:
                current_terms = discovered_terms[:10]  # Use top discovered terms
            else:
                break  # No new terms to explore

        # Deduplicate results
        final_results = self._deduplicate_results(all_results)

        # Synthesize results with final processor
        synthesis = None
        if enable_synthesis and iterations:
            synthesis = await self.term_engine.synthesize_results(
                objective=objective,
                iterations=iterations,
                processor=final_processor,
            )
            total_api_calls += 1

        # Calculate quality metrics
        quality_metrics = self._calculate_quality_metrics(iterations, final_results)

        total_duration = (time.time() - start_time) * 1000

        result = OrchestratedSearchResult(
            search_id=search_id,
            objective=objective,
            final_terms=list(all_terms),
            all_discovered_terms=all_terms,
            iterations=iterations,
            final_results=final_results,
            synthesis=synthesis,
            total_duration_ms=total_duration,
            total_api_calls=total_api_calls,
            escalation_path=escalation_path,
            quality_metrics=quality_metrics,
            plan=plan,
        )

        logger.info(
            f"Orchestrated search {search_id} completed: "
            f"{len(iterations)} iterations, {len(final_results)} results, "
            f"{total_duration:.0f}ms, {total_api_calls} API calls"
        )

        return result

    async def parallel_search(
        self,
        objective: str,
        processors: List[ProcessorTier],
        terms: List[str],
    ) -> List[Tuple[ProcessorTier, List[Dict[str, Any]]]]:
        """Execute searches in parallel across multiple processors.

        Args:
            objective: Search objective
            processors: List of processors to use
            terms: Search terms

        Returns:
            List of (processor, results) tuples
        """
        tasks = [
            self._execute_search(terms, processor)
            for processor in processors
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        output = []
        for processor, result in zip(processors, results):
            if isinstance(result, Exception):
                logger.error(f"Parallel search failed for {processor.value}: {result}")
                output.append((processor, []))
            else:
                output.append((processor, result))

        return output

    def _create_plan(
        self,
        search_id: str,
        objective: str,
        max_iterations: int,
        escalation_strategy: EscalationStrategy,
        starting_processor: ProcessorTier,
    ) -> SearchPlan:
        """Create an execution plan for the search."""
        stages = []

        if escalation_strategy == EscalationStrategy.LINEAR:
            order = ProcessorTier.escalation_order()
            start_idx = order.index(starting_processor)
            step = max(1, (len(order) - start_idx) // max_iterations)

            for i in range(max_iterations):
                idx = min(start_idx + i * step, len(order) - 1)
                stages.append({
                    "iteration": i + 1,
                    "processor": order[idx].value,
                    "type": "search_and_refine",
                })

        elif escalation_strategy == EscalationStrategy.ADAPTIVE:
            stages = [
                {
                    "iteration": i + 1,
                    "processor": "adaptive",
                    "type": "search_and_refine",
                    "escalation_trigger": f"quality < {self.escalation_quality_threshold}",
                }
                for i in range(max_iterations)
            ]

        stages.append({
            "iteration": max_iterations + 1,
            "processor": ProcessorTier.ULTRA.value,
            "type": "synthesis",
        })

        return SearchPlan(
            plan_id=f"{search_id}_plan",
            objective=objective,
            stages=stages,
            estimated_iterations=max_iterations,
            escalation_strategy=escalation_strategy,
            created_at=datetime.utcnow().isoformat(),
        )

    def _linear_escalation(
        self,
        iteration: int,
        max_iterations: int,
        start: ProcessorTier,
        end: ProcessorTier,
    ) -> ProcessorTier:
        """Calculate processor for linear escalation."""
        order = ProcessorTier.escalation_order()
        start_idx = order.index(start)
        end_idx = order.index(end)

        if max_iterations <= 1:
            return end

        progress = iteration / (max_iterations - 1)
        idx = int(start_idx + progress * (end_idx - start_idx))
        return order[min(idx, end_idx)]

    async def _execute_search(
        self,
        terms: List[str],
        processor: ProcessorTier,
    ) -> List[Dict[str, Any]]:
        """Execute a search with the given terms and processor."""
        # Build search query
        query = " OR ".join(f'"{term}"' for term in terms[:5])

        payload = {
            "queries": terms[:10],  # Limit queries
            "max_results": self.max_results_per_search,
        }

        async with self.circuit_breaker:
            response = await self.client.post(
                "/v1beta/search",
                json=payload,
                headers=self.auth_headers,
            )
            response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        # Track health
        self.health_scorer.record_success("search-api", response.elapsed.total_seconds() * 1000)

        return results

    def _score_results(
        self,
        results: List[Dict[str, Any]],
        objective: str,
    ) -> float:
        """Score the quality of search results."""
        if not results:
            return 0.0

        # Simple heuristic scoring
        score = 0.0

        # Result count factor (more results = better, with diminishing returns)
        count_score = min(len(results) / 10, 1.0) * 0.3

        # Title relevance (check if objective keywords appear in titles)
        objective_words = set(objective.lower().split())
        relevant_count = 0
        for result in results:
            title = result.get("title", "").lower()
            if any(word in title for word in objective_words):
                relevant_count += 1
        relevance_score = (relevant_count / max(len(results), 1)) * 0.5

        # Source diversity
        sources = set(result.get("source", result.get("url", "")) for result in results)
        diversity_score = min(len(sources) / 5, 1.0) * 0.2

        score = count_score + relevance_score + diversity_score
        return min(score, 1.0)

    def _deduplicate_results(
        self,
        results: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Remove duplicate results based on URL/content."""
        seen_urls = set()
        seen_titles = set()
        unique_results = []

        for result in results:
            url = result.get("url", "")
            title = result.get("title", "").lower()

            # Skip if we've seen this URL or very similar title
            if url in seen_urls:
                continue
            if title in seen_titles:
                continue

            seen_urls.add(url)
            seen_titles.add(title)
            unique_results.append(result)

        return unique_results

    def _calculate_quality_metrics(
        self,
        iterations: List[SearchIteration],
        final_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Calculate overall quality metrics for the search."""
        if not iterations:
            return {"error": "No iterations completed"}

        quality_scores = [it.quality_score for it in iterations]
        durations = [it.duration_ms for it in iterations]
        result_counts = [it.result_count for it in iterations]

        return {
            "iterations_completed": len(iterations),
            "avg_quality_score": sum(quality_scores) / len(quality_scores),
            "max_quality_score": max(quality_scores),
            "min_quality_score": min(quality_scores),
            "total_unique_results": len(final_results),
            "total_results_found": sum(result_counts),
            "avg_iteration_duration_ms": sum(durations) / len(durations),
            "terms_discovered": sum(len(it.discovered_terms) for it in iterations),
            "processors_used": list(set(it.processor.value for it in iterations)),
        }


# =============================================================================
# Factory Function
# =============================================================================

_orchestrator: Optional[SearchOrchestrator] = None


def get_search_orchestrator(
    http_client: Optional[httpx.AsyncClient] = None,
    auth_headers: Optional[Dict[str, str]] = None,
) -> SearchOrchestrator:
    """Get or create the global search orchestrator."""
    global _orchestrator

    if _orchestrator is None:
        if http_client is None:
            raise ValueError("http_client required for first initialization")
        _orchestrator = SearchOrchestrator(http_client, auth_headers)

    return _orchestrator
