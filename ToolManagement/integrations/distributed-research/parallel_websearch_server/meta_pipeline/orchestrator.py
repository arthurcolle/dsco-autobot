"""Meta-pipeline orchestrator for intelligent query planning and execution."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """Pipeline execution stages."""
    DECOMPOSE = "decompose"
    SEARCH = "search"
    EXTRACT = "extract"
    ENTITY_EXTRACT = "entity_extract"
    RELATIONSHIP_MAP = "relationship_map"
    SEMANTIC_ENRICH = "semantic_enrich"
    KG_UPDATE = "kg_update"
    FUSION = "fusion"
    RANK = "rank"


class QueryIntent(str, Enum):
    """Detected query intent types."""
    FACTUAL = "factual"
    EXPLORATORY = "exploratory"
    COMPARATIVE = "comparative"
    TEMPORAL = "temporal"
    ENTITY_FOCUSED = "entity_focused"
    RELATIONSHIP_FOCUSED = "relationship_focused"


@dataclass
class PipelineStageResult:
    """Result from a pipeline stage."""
    stage: PipelineStage
    status: str  # success, failed, skipped
    data: Dict[str, Any]
    duration_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class QueryPlan:
    """Execution plan for a query."""
    query_id: str
    original_query: str
    intent: QueryIntent
    stages: List[PipelineStage]
    parallel_groups: List[List[PipelineStage]]
    priority: int = 5
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class PipelineContext(BaseModel):
    """Context passed through pipeline stages."""
    query_id: str
    original_query: str
    decomposed_queries: List[str] = []
    search_results: List[Dict[str, Any]] = []
    extracted_content: List[Dict[str, Any]] = []
    entities: List[Dict[str, Any]] = []
    relationships: List[Dict[str, Any]] = []
    enriched_data: Dict[str, Any] = {}
    kg_updates: List[Dict[str, Any]] = []
    final_results: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}


class MetaPipelineOrchestrator:
    """
    Advanced meta-pipeline orchestrator that intelligently plans and executes
    multi-stage queries with automatic KG integration.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        kg_builder: Optional[Any] = None,
        enable_parallel: bool = True,
        max_parallel: int = 5,
    ):
        """Initialize orchestrator.

        Args:
            http_client: HTTP client for API calls
            kg_builder: Knowledge graph builder instance
            enable_parallel: Enable parallel stage execution
            max_parallel: Max parallel operations
        """
        self.http_client = http_client
        self.kg_builder = kg_builder
        self.enable_parallel = enable_parallel
        self.max_parallel = max_parallel
        self._active_pipelines: Dict[str, PipelineContext] = {}

    async def plan_query(self, query: str, context: Optional[Dict[str, Any]] = None) -> QueryPlan:
        """Analyze query and create optimal execution plan.

        Args:
            query: User query
            context: Optional context from previous interactions

        Returns:
            Execution plan with stages and parallelization strategy
        """
        import uuid
        query_id = str(uuid.uuid4())

        # Detect intent
        intent = self._detect_intent(query, context)
        logger.info(f"Query intent detected: {intent} for query: {query[:100]}")

        # Build stage plan based on intent
        stages = self._build_stage_plan(intent, query, context)

        # Identify parallelizable groups
        parallel_groups = self._identify_parallel_groups(stages)

        plan = QueryPlan(
            query_id=query_id,
            original_query=query,
            intent=intent,
            stages=stages,
            parallel_groups=parallel_groups,
            metadata={
                "has_context": context is not None,
                "stage_count": len(stages),
                "parallel_group_count": len(parallel_groups),
            }
        )

        logger.info(
            f"Created query plan {query_id}: {len(stages)} stages, "
            f"{len(parallel_groups)} parallel groups"
        )
        return plan

    async def execute_pipeline(
        self,
        plan: QueryPlan,
        auth_headers: Dict[str, str],
    ) -> PipelineContext:
        """Execute a query plan through the meta-pipeline.

        Args:
            plan: Query execution plan
            auth_headers: Authentication headers for API calls

        Returns:
            Pipeline context with all results
        """
        context = PipelineContext(
            query_id=plan.query_id,
            original_query=plan.original_query,
            metadata={
                "intent": plan.intent,
                "started_at": datetime.utcnow().isoformat(),
            }
        )

        self._active_pipelines[plan.query_id] = context

        try:
            # Execute stages (with parallelization where possible)
            for parallel_group in plan.parallel_groups:
                if self.enable_parallel and len(parallel_group) > 1:
                    # Execute stages in parallel
                    tasks = [
                        self._execute_stage(stage, context, auth_headers)
                        for stage in parallel_group
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Handle results and errors
                    for stage, result in zip(parallel_group, results):
                        if isinstance(result, Exception):
                            logger.error(f"Stage {stage} failed: {result}")
                        else:
                            logger.debug(f"Stage {stage} completed successfully")
                else:
                    # Execute sequentially
                    for stage in parallel_group:
                        await self._execute_stage(stage, context, auth_headers)

            context.metadata["completed_at"] = datetime.utcnow().isoformat()
            context.metadata["status"] = "success"

            logger.info(
                f"Pipeline {plan.query_id} completed: "
                f"{len(context.entities)} entities, "
                f"{len(context.relationships)} relationships, "
                f"{len(context.final_results)} final results"
            )

        except Exception as e:
            logger.exception(f"Pipeline {plan.query_id} failed: {e}")
            context.metadata["status"] = "failed"
            context.metadata["error"] = str(e)
        finally:
            if plan.query_id in self._active_pipelines:
                del self._active_pipelines[plan.query_id]

        return context

    def _detect_intent(self, query: str, context: Optional[Dict[str, Any]]) -> QueryIntent:
        """Detect query intent using heuristics and patterns."""
        query_lower = query.lower()

        # Relationship patterns
        if any(word in query_lower for word in ["relationship", "connection", "link", "related to", "associated with"]):
            return QueryIntent.RELATIONSHIP_FOCUSED

        # Entity patterns
        if any(word in query_lower for word in ["who is", "what is", "about", "profile of"]):
            return QueryIntent.ENTITY_FOCUSED

        # Temporal patterns
        if any(word in query_lower for word in ["when", "timeline", "history", "since", "until", "recent", "latest"]):
            return QueryIntent.TEMPORAL

        # Comparative patterns
        if any(word in query_lower for word in ["compare", "versus", "vs", "difference between", "better than"]):
            return QueryIntent.COMPARATIVE

        # Exploratory patterns
        if any(word in query_lower for word in ["explore", "discover", "find", "search for", "look for"]):
            return QueryIntent.EXPLORATORY

        # Default to factual
        return QueryIntent.FACTUAL

    def _build_stage_plan(
        self,
        intent: QueryIntent,
        query: str,
        context: Optional[Dict[str, Any]]
    ) -> List[PipelineStage]:
        """Build execution stages based on intent."""
        stages = []

        # Always decompose complex queries
        if len(query.split()) > 5 or "and" in query.lower() or "," in query:
            stages.append(PipelineStage.DECOMPOSE)

        # Core stages based on intent
        if intent in [QueryIntent.FACTUAL, QueryIntent.EXPLORATORY, QueryIntent.TEMPORAL]:
            stages.extend([
                PipelineStage.SEARCH,
                PipelineStage.ENTITY_EXTRACT,
                PipelineStage.SEMANTIC_ENRICH,
            ])
        elif intent == QueryIntent.ENTITY_FOCUSED:
            stages.extend([
                PipelineStage.SEARCH,
                PipelineStage.EXTRACT,
                PipelineStage.ENTITY_EXTRACT,
                PipelineStage.RELATIONSHIP_MAP,
                PipelineStage.SEMANTIC_ENRICH,
            ])
        elif intent == QueryIntent.RELATIONSHIP_FOCUSED:
            stages.extend([
                PipelineStage.SEARCH,
                PipelineStage.EXTRACT,
                PipelineStage.ENTITY_EXTRACT,
                PipelineStage.RELATIONSHIP_MAP,
                PipelineStage.SEMANTIC_ENRICH,
            ])
        elif intent == QueryIntent.COMPARATIVE:
            stages.extend([
                PipelineStage.SEARCH,
                PipelineStage.EXTRACT,
                PipelineStage.ENTITY_EXTRACT,
                PipelineStage.FUSION,
                PipelineStage.SEMANTIC_ENRICH,
            ])

        # Always add KG update and ranking
        stages.extend([
            PipelineStage.KG_UPDATE,
            PipelineStage.RANK,
        ])

        return stages

    def _identify_parallel_groups(self, stages: List[PipelineStage]) -> List[List[PipelineStage]]:
        """Identify which stages can run in parallel."""
        # Define dependencies
        dependencies = {
            PipelineStage.DECOMPOSE: [],
            PipelineStage.SEARCH: [PipelineStage.DECOMPOSE],
            PipelineStage.EXTRACT: [PipelineStage.SEARCH],
            PipelineStage.ENTITY_EXTRACT: [PipelineStage.SEARCH],
            PipelineStage.RELATIONSHIP_MAP: [PipelineStage.ENTITY_EXTRACT],
            PipelineStage.SEMANTIC_ENRICH: [PipelineStage.ENTITY_EXTRACT],
            PipelineStage.KG_UPDATE: [PipelineStage.ENTITY_EXTRACT, PipelineStage.RELATIONSHIP_MAP],
            PipelineStage.FUSION: [PipelineStage.EXTRACT],
            PipelineStage.RANK: [PipelineStage.KG_UPDATE, PipelineStage.SEMANTIC_ENRICH],
        }

        groups = []
        remaining = set(stages)
        completed = set()

        while remaining:
            # Find stages whose dependencies are satisfied
            ready = []
            for stage in remaining:
                deps = dependencies.get(stage, [])
                if all(dep in completed or dep not in stages for dep in deps):
                    ready.append(stage)

            if not ready:
                # Deadlock - add remaining sequentially
                groups.extend([[stage] for stage in remaining])
                break

            groups.append(ready)
            for stage in ready:
                remaining.remove(stage)
                completed.add(stage)

        return groups

    async def _execute_stage(
        self,
        stage: PipelineStage,
        context: PipelineContext,
        auth_headers: Dict[str, str],
    ) -> PipelineStageResult:
        """Execute a single pipeline stage."""
        start_time = datetime.utcnow()

        try:
            if stage == PipelineStage.DECOMPOSE:
                await self._stage_decompose(context)
            elif stage == PipelineStage.SEARCH:
                await self._stage_search(context, auth_headers)
            elif stage == PipelineStage.EXTRACT:
                await self._stage_extract(context, auth_headers)
            elif stage == PipelineStage.ENTITY_EXTRACT:
                await self._stage_entity_extract(context)
            elif stage == PipelineStage.RELATIONSHIP_MAP:
                await self._stage_relationship_map(context)
            elif stage == PipelineStage.SEMANTIC_ENRICH:
                await self._stage_semantic_enrich(context)
            elif stage == PipelineStage.KG_UPDATE:
                await self._stage_kg_update(context)
            elif stage == PipelineStage.FUSION:
                await self._stage_fusion(context)
            elif stage == PipelineStage.RANK:
                await self._stage_rank(context)

            duration = (datetime.utcnow() - start_time).total_seconds() * 1000

            return PipelineStageResult(
                stage=stage,
                status="success",
                data={},
                duration_ms=duration,
            )
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.exception(f"Stage {stage} failed: {e}")

            return PipelineStageResult(
                stage=stage,
                status="failed",
                data={},
                duration_ms=duration,
                error=str(e),
            )

    async def _stage_decompose(self, context: PipelineContext):
        """Decompose complex query into sub-queries."""
        # Simple decomposition for now - can be enhanced with LLM
        query = context.original_query

        # Split on conjunctions and commas
        parts = query.replace(" and ", ",").replace(";", ",").split(",")
        context.decomposed_queries = [p.strip() for p in parts if p.strip()]

        if not context.decomposed_queries:
            context.decomposed_queries = [query]

        logger.info(f"Decomposed query into {len(context.decomposed_queries)} parts")

    async def _stage_search(self, context: PipelineContext, auth_headers: Dict[str, str]):
        """Execute search stage."""
        from parallel_websearch_server.auth import get_auth_headers

        queries = context.decomposed_queries or [context.original_query]

        # Execute searches in parallel
        tasks = []
        for query in queries[:3]:  # Limit to 3 sub-queries
            payload = {
                "objective": query,
                "max_results": 10,
            }
            tasks.append(
                self.http_client.post(
                    "/v1beta/search",
                    json=payload,
                    headers=get_auth_headers(),
                )
            )

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for response in responses:
            if isinstance(response, Exception):
                logger.error(f"Search failed: {response}")
                continue

            if response.status_code == 200:
                data = response.json()
                context.search_results.extend(data.get("results", []))

        logger.info(f"Search stage collected {len(context.search_results)} results")

    async def _stage_extract(self, context: PipelineContext, auth_headers: Dict[str, str]):
        """Execute extract stage on search results."""
        from parallel_websearch_server.auth import get_auth_headers

        # Get top URLs from search results
        urls = []
        for result in context.search_results[:5]:
            if "url" in result:
                urls.append(result["url"])

        if not urls:
            return

        payload = {
            "objective": context.original_query,
            "urls": urls,
            "full_content": {"max_chars_per_result": 5000},
            "excerpts": {"max_chars_per_result": 500},
        }

        response = await self.http_client.post(
            "/v1beta/extract",
            json=payload,
            headers=get_auth_headers(),
        )

        if response.status_code == 200:
            data = response.json()
            context.extracted_content = data.get("results", [])
            logger.info(f"Extracted content from {len(context.extracted_content)} URLs")

    async def _stage_entity_extract(self, context: PipelineContext):
        """Extract entities from search results and content."""
        if self.kg_builder:
            entities = await self.kg_builder.extract_entities(
                search_results=context.search_results,
                extracted_content=context.extracted_content,
            )
            context.entities = entities
            logger.info(f"Extracted {len(entities)} entities")

    async def _stage_relationship_map(self, context: PipelineContext):
        """Map relationships between entities."""
        if self.kg_builder and context.entities:
            relationships = await self.kg_builder.extract_relationships(
                entities=context.entities,
                content=context.extracted_content,
            )
            context.relationships = relationships
            logger.info(f"Mapped {len(relationships)} relationships")

    async def _stage_semantic_enrich(self, context: PipelineContext):
        """Enrich results with semantic analysis."""
        # Placeholder for semantic enrichment
        context.enriched_data = {
            "entity_count": len(context.entities),
            "relationship_count": len(context.relationships),
            "result_count": len(context.search_results),
        }

    async def _stage_kg_update(self, context: PipelineContext):
        """Update knowledge graph with new entities and relationships."""
        if self.kg_builder:
            updates = await self.kg_builder.update_graph(
                entities=context.entities,
                relationships=context.relationships,
                metadata={
                    "query_id": context.query_id,
                    "query": context.original_query,
                }
            )
            context.kg_updates = updates
            logger.info(f"Updated KG with {len(updates)} changes")

    async def _stage_fusion(self, context: PipelineContext):
        """Fuse results from multiple sources."""
        # Deduplicate and merge results
        seen_urls = set()
        fused_results = []

        for result in context.search_results:
            url = result.get("url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                fused_results.append(result)

        context.final_results = fused_results
        logger.info(f"Fused to {len(fused_results)} unique results")

    async def _stage_rank(self, context: PipelineContext):
        """Rank results using KG and semantic information."""
        # Simple ranking based on entity mentions
        results = context.final_results or context.search_results

        entity_names = {e.get("name", "").lower() for e in context.entities}

        def score_result(result):
            score = 0
            title = result.get("title", "").lower()
            excerpts = result.get("excerpts", [])

            for entity in entity_names:
                if entity in title:
                    score += 3
                for excerpt in excerpts:
                    if isinstance(excerpt, str) and entity in excerpt.lower():
                        score += 1

            return score

        ranked = sorted(results, key=score_result, reverse=True)
        context.final_results = ranked
        logger.info(f"Ranked {len(ranked)} results")
