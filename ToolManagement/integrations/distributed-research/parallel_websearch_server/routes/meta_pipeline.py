"""Meta-pipeline API routes for advanced query processing."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.dependencies import get_http_client, get_kg_builder, get_meta_orchestrator
from parallel_websearch_server.meta_pipeline.orchestrator import (
    MetaPipelineOrchestrator,
    QueryIntent,
)
from parallel_websearch_server.meta_pipeline.kg_builder import KnowledgeGraphBuilder

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/meta", tags=["Meta-Pipeline"])


class MetaQueryRequest(BaseModel):
    """Advanced meta-pipeline query request."""
    query: str = Field(..., description="Query to process through meta-pipeline")
    context: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional context from previous interactions"
    )
    enable_kg: bool = Field(
        True,
        description="Enable knowledge graph integration"
    )
    enable_parallel: bool = Field(
        True,
        description="Enable parallel stage execution"
    )
    intent_override: Optional[str] = Field(
        None,
        description="Override auto-detected intent"
    )


class MetaQueryResponse(BaseModel):
    """Meta-pipeline query response."""
    query_id: str
    query: str
    intent: str
    results: List[Dict[str, Any]]
    entities: List[Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    pipeline_metadata: Dict[str, Any]
    kg_stats: Optional[Dict[str, Any]] = None


class EntityResponse(BaseModel):
    """Entity retrieval response."""
    entity_id: str
    name: str
    type: str
    mentions: List[str]
    attributes: Dict[str, Any]
    sources: List[str]
    relationships: List[Dict[str, Any]]


class KnowledgeGraphStatsResponse(BaseModel):
    """Knowledge graph statistics response."""
    total_entities: int
    total_relationships: int
    entity_types: Dict[str, int]
    relationship_types: Dict[str, int]


@router.post("/query", response_model=MetaQueryResponse, operation_id="meta_query")
async def meta_query(
    request: MetaQueryRequest,
    orchestrator: MetaPipelineOrchestrator = Depends(get_meta_orchestrator),
) -> MetaQueryResponse:
    """
    Execute advanced meta-pipeline query with automatic KG integration.

    This endpoint provides:
    - Intelligent query decomposition
    - Parallel multi-stage execution
    - Automatic entity extraction
    - Relationship mapping
    - Knowledge graph integration
    - Semantic enrichment
    - Graph-aware ranking

    Args:
        request: Meta-query request

    Returns:
        Enhanced results with entities, relationships, and KG integration
    """
    logger.info(
        f"Meta-pipeline query: {request.query[:100]}",
        extra={
            "enable_kg": request.enable_kg,
            "enable_parallel": request.enable_parallel,
        }
    )

    try:
        # Plan query execution
        plan = await orchestrator.plan_query(
            query=request.query,
            context=request.context,
        )

        # Override intent if requested
        if request.intent_override:
            try:
                plan.intent = QueryIntent(request.intent_override)
                logger.info(f"Intent overridden to: {plan.intent}")
            except ValueError:
                logger.warning(f"Invalid intent override: {request.intent_override}")

        # Execute pipeline
        context = await orchestrator.execute_pipeline(
            plan=plan,
            auth_headers=get_auth_headers(),
        )

        # Get KG stats if available
        kg_stats = None
        if request.enable_kg and orchestrator.kg_builder:
            kg_stats = orchestrator.kg_builder.get_graph_stats()

        response = MetaQueryResponse(
            query_id=context.query_id,
            query=context.original_query,
            intent=plan.intent,
            results=context.final_results or context.search_results,
            entities=context.entities,
            relationships=context.relationships,
            pipeline_metadata={
                "stages_executed": len(plan.stages),
                "parallel_groups": len(plan.parallel_groups),
                "decomposed_queries": len(context.decomposed_queries),
                "search_result_count": len(context.search_results),
                "entity_count": len(context.entities),
                "relationship_count": len(context.relationships),
                **context.metadata,
            },
            kg_stats=kg_stats,
        )

        logger.info(
            f"Meta-pipeline completed: {len(response.results)} results, "
            f"{len(response.entities)} entities, {len(response.relationships)} relationships"
        )

        return response

    except Exception as e:
        logger.exception(f"Meta-pipeline query failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Meta-pipeline execution failed",
                "message": str(e),
            }
        )


@router.get("/entities/{entity_id}", response_model=EntityResponse, operation_id="get_entity")
async def get_entity(
    entity_id: str,
    kg_builder: KnowledgeGraphBuilder = Depends(get_kg_builder),
) -> EntityResponse:
    """
    Retrieve entity from knowledge graph.

    Args:
        entity_id: Entity ID

    Returns:
        Entity with relationships
    """
    entity = kg_builder.get_entity(entity_id)

    if not entity:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Entity {entity_id} not found"}
        )

    relationships = kg_builder.get_relationships(entity_id)

    return EntityResponse(
        entity_id=entity["id"],
        name=entity["name"],
        type=entity["type"],
        mentions=entity.get("mentions", []),
        attributes=entity.get("attributes", {}),
        sources=entity.get("sources", []),
        relationships=relationships,
    )


@router.get("/kg/stats", response_model=KnowledgeGraphStatsResponse, operation_id="kg_stats")
async def get_kg_stats(
    kg_builder: KnowledgeGraphBuilder = Depends(get_kg_builder),
) -> KnowledgeGraphStatsResponse:
    """
    Get knowledge graph statistics.

    Returns:
        Statistics about entities and relationships in the KG
    """
    stats = kg_builder.get_graph_stats()

    return KnowledgeGraphStatsResponse(
        total_entities=stats["total_entities"],
        total_relationships=stats["total_relationships"],
        entity_types=stats["entity_types"],
        relationship_types=stats["relationship_types"],
    )


@router.post("/kg/search", operation_id="kg_search")
async def search_knowledge_graph(
    query: str = Query(..., description="Entity name or pattern to search"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(10, ge=1, le=100, description="Max results to return"),
    kg_builder: KnowledgeGraphBuilder = Depends(get_kg_builder),
) -> Dict[str, Any]:
    """
    Search knowledge graph for entities.

    Args:
        query: Search query
        entity_type: Optional entity type filter
        limit: Max results

    Returns:
        Matching entities
    """
    # Simple search through cached entities
    results = []
    query_lower = query.lower()

    for entity in kg_builder._entity_cache.values():
        if query_lower in entity.name.lower():
            if entity_type and entity.type != entity_type:
                continue

            results.append(entity.to_dict())

            if len(results) >= limit:
                break

    return {
        "query": query,
        "entity_type": entity_type,
        "count": len(results),
        "results": results,
    }


@router.get("/kg/graph", operation_id="get_graph")
async def get_graph_data(
    center_entity: Optional[str] = Query(None, description="Center entity ID"),
    depth: int = Query(1, ge=1, le=3, description="Relationship depth"),
    kg_builder: KnowledgeGraphBuilder = Depends(get_kg_builder),
) -> Dict[str, Any]:
    """
    Get graph data for visualization.

    Args:
        center_entity: Optional center entity to start from
        depth: Depth of relationships to include

    Returns:
        Graph data with nodes and edges
    """
    nodes = []
    edges = []

    if center_entity:
        # Get subgraph centered on entity
        visited = set()
        to_visit = [(center_entity, 0)]

        while to_visit:
            entity_id, current_depth = to_visit.pop(0)

            if entity_id in visited or current_depth > depth:
                continue

            visited.add(entity_id)

            # Add entity as node
            entity = kg_builder.get_entity(entity_id)
            if entity:
                nodes.append({
                    "id": entity["id"],
                    "label": entity["name"],
                    "type": entity["type"],
                    "depth": current_depth,
                })

                # Add relationships as edges
                rels = kg_builder.get_relationships(entity_id)
                for rel in rels:
                    edges.append({
                        "id": rel["id"],
                        "source": rel["source_id"],
                        "target": rel["target_id"],
                        "type": rel["type"],
                        "confidence": rel.get("confidence", 1.0),
                    })

                    # Add connected entities to visit
                    if current_depth < depth:
                        if rel["source_id"] != entity_id:
                            to_visit.append((rel["source_id"], current_depth + 1))
                        if rel["target_id"] != entity_id:
                            to_visit.append((rel["target_id"], current_depth + 1))
    else:
        # Return full graph (limited)
        for entity in list(kg_builder._entity_cache.values())[:100]:
            nodes.append({
                "id": entity.id,
                "label": entity.name,
                "type": entity.type,
            })

        for rel in list(kg_builder._relationship_cache.values())[:200]:
            edges.append({
                "id": rel.id,
                "source": rel.source_id,
                "target": rel.target_id,
                "type": rel.type,
                "confidence": rel.confidence,
            })

    return {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(edges),
        }
    }
