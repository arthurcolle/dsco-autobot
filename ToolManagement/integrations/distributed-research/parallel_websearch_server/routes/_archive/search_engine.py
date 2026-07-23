"""Clean Search Engine UI with real-time ingestion stats and advanced features."""

import asyncio
import json
import logging
import os
import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from parallel_websearch_server.knowledge_graph import KnowledgeGraph, get_knowledge_graph, save_knowledge_graph
from parallel_websearch_server.turso_store import get_turso_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/search", tags=["Search Engine"])

# Path to the research dataset
RESEARCH_DATA_PATH = Path("/Users/arthurcolle/parallel_research_v2")

# Activity log storage (in-memory for simplicity)
activity_log: List[Dict[str, Any]] = []

# SSE client queues for real-time streaming
_activity_subscribers: Set[asyncio.Queue] = set()


class SearchQuery(BaseModel):
    q: str
    entity_types: Optional[List[str]] = None
    min_mentions: int = Field(0, ge=0)
    limit: int = 20


class IngestRequest(BaseModel):
    source_path: str = str(RESEARCH_DATA_PATH)
    max_docs: int = 50


class CompareEntitiesRequest(BaseModel):
    entity_ids: List[str] = Field(..., min_length=2, max_length=5)


class ExtractRequest(BaseModel):
    """Request for parallel entity extraction."""
    text: str = Field(..., min_length=10, max_length=50000, description="Text to extract entities from")
    models: Optional[List[str]] = Field(None, description="Models to use (default: gemini-flash, llama, qwen, deepseek)")
    extract_relationships: bool = Field(True, description="Also extract relationships between entities")
    add_to_graph: bool = Field(False, description="Add extracted entities to the knowledge graph")
    entity_types: Optional[List[str]] = Field(None, description="Specific entity types to extract")


class ExtractedEntityResponse(BaseModel):
    """Single extracted entity."""
    name: str
    type: str
    confidence: float
    grounded: bool
    models: List[str]
    consensus_votes: int
    context: str = ""


class RelationshipResponse(BaseModel):
    """Relationship between entities."""
    source: str
    target: str
    relation_type: str
    confidence: float
    distance_chars: int
    context: str


class ExtractionStats(BaseModel):
    """Statistics about the extraction."""
    total_raw_extractions: int
    consensus_entities: int
    grounding_rate: float
    models_responded: int
    avg_latency_ms: float


class ExtractResponse(BaseModel):
    """Response from parallel entity extraction."""
    doc_id: str
    entities: List[ExtractedEntityResponse]
    entity_count: int
    relationships: List[RelationshipResponse] = []
    stats: ExtractionStats
    models_used: List[str]
    model_latencies: Optional[Dict[str, float]] = None
    processing_time_ms: float
    from_cache: bool
    added_to_graph: Optional[int] = None


class BenchmarkRequest(BaseModel):
    """Request for model benchmarking."""
    n_models: int = Field(15, ge=6, le=30, description="Number of models to evaluate per tier")


class ModelScore(BaseModel):
    """Score for a single model."""
    model_id: str
    latency_ms: float
    price_per_1k: float
    precision: float
    recall: float
    f1: float
    intelligence_score: float
    value_score: float
    errors: int
    eval_time: str


class BenchmarkResponse(BaseModel):
    """Response from model benchmarking."""
    benchmark_time_ms: float
    models_evaluated: int
    top_models: List[ModelScore]
    recommendation: Optional[ModelScore] = None


# =============================================================================
# Helper Functions
# =============================================================================

def log_activity(action: str, details: Dict[str, Any]):
    """Log an activity to the activity log and broadcast to SSE subscribers."""
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "details": details,
    }
    activity_log.append(event)
    # Keep only last 100 entries
    if len(activity_log) > 100:
        activity_log.pop(0)

    # Broadcast to all SSE subscribers
    for queue in _activity_subscribers.copy():
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # Skip if queue is full


# =============================================================================
# API Endpoints
# =============================================================================

@router.get("/stats", operation_id="search_engine_stats")
async def get_stats(
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get current search engine statistics."""
    kg_stats = graph.get_stats()

    # Get Turso stats
    try:
        store = get_turso_store()
        turso_stats = store.get_stats()
    except:
        turso_stats = {}

    return {
        "knowledge_graph": kg_stats,
        "storage": turso_stats,
        "research_corpus": {
            "available": RESEARCH_DATA_PATH.exists(),
            "path": str(RESEARCH_DATA_PATH),
        }
    }


@router.get("/activity", operation_id="get_activity_log")
async def get_activity_log_endpoint(
    limit: int = Query(50, ge=1, le=100),
) -> Dict[str, Any]:
    """Get recent activity log."""
    return {
        "total": len(activity_log),
        "activities": activity_log[-limit:][::-1],  # Most recent first
    }


@router.get("/activity/stream", operation_id="stream_activity_log")
async def stream_activity_log():
    """Stream activity log in real-time via SSE."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _activity_subscribers.add(queue)

    async def event_generator():
        try:
            # Send initial connection event
            yield {
                "event": "connected",
                "data": json.dumps({"subscribers": len(_activity_subscribers)})
            }

            # Stream recent activities first
            for activity in activity_log[-10:]:
                yield {
                    "event": "activity",
                    "data": json.dumps(activity)
                }

            # Stream new activities as they come
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "event": "activity",
                        "data": json.dumps(event)
                    }
                except asyncio.TimeoutError:
                    # Send keepalive ping
                    yield {"event": "ping", "data": ""}
        except asyncio.CancelledError:
            pass
        finally:
            _activity_subscribers.discard(queue)

    return EventSourceResponse(event_generator())


@router.get("/entity/{entity_id}", operation_id="get_entity_details")
async def get_entity_details(
    entity_id: str,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get detailed information about an entity including relationships."""
    entity = graph.entities.get(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    # Get relationships
    relationships = []
    for rel in graph.relationships.values():
        if rel.source_id == entity_id:
            target = graph.entities.get(rel.target_id)
            relationships.append({
                "direction": "outgoing",
                "relation": rel.relation_type,
                "entity_id": rel.target_id,
                "entity_text": target.text if target else "Unknown",
                "entity_type": target.type if target else "unknown",
                "weight": rel.weight,
            })
        elif rel.target_id == entity_id:
            source = graph.entities.get(rel.source_id)
            relationships.append({
                "direction": "incoming",
                "relation": rel.relation_type,
                "entity_id": rel.source_id,
                "entity_text": source.text if source else "Unknown",
                "entity_type": source.type if source else "unknown",
                "weight": rel.weight,
            })

    log_activity("view_entity", {"entity_id": entity_id, "entity_text": entity.text})

    # Collect sources from relationships
    sources = set()
    for rel in graph.relationships.values():
        if rel.source_id == entity_id or rel.target_id == entity_id:
            sources.update(rel.sources)

    return {
        "id": entity.id,
        "text": entity.text,
        "type": entity.type,
        "mentions": entity.mentions,
        "aliases": list(entity.aliases),
        "properties": entity.properties,
        "sources": list(sources),
        "relationships": relationships,
        "relationship_count": len(relationships),
    }


@router.post("/compare", operation_id="compare_entities")
async def compare_entities(
    request: CompareEntitiesRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Compare multiple entities side-by-side."""
    entities = []
    common_relationships = []

    for eid in request.entity_ids:
        entity = graph.entities.get(eid)
        if entity:
            # Get relationship count
            rel_count = sum(
                1 for r in graph.relationships.values()
                if r.source_id == eid or r.target_id == eid
            )
            entities.append({
                "id": eid,
                "text": entity.text,
                "type": entity.type,
                "mentions": entity.mentions,
                "aliases": list(entity.aliases)[:5],
                "relationship_count": rel_count,
            })

    # Find common connections
    if len(entities) >= 2:
        entity_ids_set = set(request.entity_ids)
        for rel in graph.relationships.values():
            if rel.source_id in entity_ids_set and rel.target_id in entity_ids_set:
                source = graph.entities.get(rel.source_id)
                target = graph.entities.get(rel.target_id)
                common_relationships.append({
                    "source": source.text if source else rel.source_id,
                    "target": target.text if target else rel.target_id,
                    "relation": rel.relation_type,
                })

    log_activity("compare_entities", {"entity_ids": request.entity_ids})

    return {
        "entities": entities,
        "common_relationships": common_relationships,
    }


@router.get("/export/csv", operation_id="export_entities_csv")
async def export_entities_csv(
    entity_type: Optional[str] = Query(None),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
):
    """Export entities to CSV format."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Text", "Type", "Mentions", "Aliases", "First Seen", "Last Seen"])

    for entity in graph.entities.values():
        if entity_type and entity.type != entity_type:
            continue
        writer.writerow([
            entity.id,
            entity.text,
            entity.type,
            entity.mentions,
            "|".join(entity.aliases),
            entity.first_seen,
            entity.last_seen,
        ])

    output.seek(0)
    log_activity("export_csv", {"entity_type": entity_type, "count": len(graph.entities)})

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=entities.csv"}
    )


@router.get("/export/json", operation_id="export_entities_json")
async def export_entities_json(
    entity_type: Optional[str] = Query(None),
    include_relationships: bool = Query(True),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
):
    """Export entities and relationships to JSON format."""
    entities = []
    for entity in graph.entities.values():
        if entity_type and entity.type != entity_type:
            continue
        entities.append(entity.to_dict())

    relationships = []
    if include_relationships:
        for rel in graph.relationships.values():
            relationships.append({
                "source_id": rel.source_id,
                "target_id": rel.target_id,
                "relation_type": rel.relation_type,
                "weight": rel.weight,
            })

    data = {
        "exported_at": datetime.utcnow().isoformat(),
        "entity_count": len(entities),
        "relationship_count": len(relationships),
        "entities": entities,
        "relationships": relationships,
    }

    log_activity("export_json", {"entity_type": entity_type, "count": len(entities)})

    return JSONResponse(
        content=data,
        headers={"Content-Disposition": "attachment; filename=knowledge_graph.json"}
    )


@router.post("/query", operation_id="search_query")
async def search_query(
    request: SearchQuery,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Search the knowledge graph."""
    results = []
    query_lower = request.q.lower()

    # Search entities
    for entity in graph.entities.values():
        if request.entity_types and entity.type not in request.entity_types:
            continue

        # Filter by minimum mentions
        if entity.mentions < request.min_mentions:
            continue

        # Check text match
        score = 0
        if query_lower in entity.text.lower():
            score = 100 if entity.text.lower() == query_lower else 80
        elif any(query_lower in alias.lower() for alias in entity.aliases):
            score = 60

        if score > 0:
            # Boost by mentions
            score += min(entity.mentions * 2, 20)
            results.append({
                "id": entity.id,
                "text": entity.text,
                "type": entity.type,
                "mentions": entity.mentions,
                "score": score,
                "aliases": list(entity.aliases)[:5],
            })

    # Sort by score
    results.sort(key=lambda x: x["score"], reverse=True)

    log_activity("search", {"query": request.q, "results_count": len(results)})

    return {
        "query": request.q,
        "total": len(results),
        "results": results[:request.limit],
    }


@router.post("/analyze", operation_id="analyze_entities_with_ai")
async def analyze_entities_with_ai(
    entity_ids: Optional[List[str]] = None,
    limit: int = Query(20, ge=1, le=50),
    model: str = Query("haiku", regex="^(haiku|flash|deepseek)$"),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Use AI (Claude Haiku via OpenRouter) to analyze and improve entity extraction."""
    from parallel_websearch_server.ai_analyzer import analyze_and_improve_entities

    # Get entities to analyze
    if entity_ids:
        entities = [graph.get_entity(eid) for eid in entity_ids if graph.get_entity(eid)]
    else:
        # Get top entities by mentions
        all_entities = sorted(
            [e.to_dict() for e in graph.entities.values()],
            key=lambda x: x.get("mentions", 0),
            reverse=True
        )[:limit]
        entities = all_entities

    if not entities:
        return {"error": "No entities to analyze", "analyzed": 0}

    # Run AI analysis
    results = await analyze_and_improve_entities(entities, model=model)

    log_activity("ai_analyze", {"entities": len(entities), "model": model, "improvements": len(results.get("improvements", []))})

    return {
        "model": model,
        "entities_analyzed": len(entities),
        **results
    }


@router.post("/extract/parallel", operation_id="extract_entities_parallel_llm", response_model=ExtractResponse)
async def extract_entities_parallel_llm(
    request: ExtractRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> ExtractResponse:
    """Extract entities using multiple LLMs in parallel with consensus voting.

    Runs extraction across multiple OpenRouter models simultaneously.
    Entities found by 2+ models are included in consensus results.
    All entities are validated against source text (grounding).
    """
    from parallel_websearch_server.dspy_entity_extractor import get_extractor

    extractor = get_extractor(request.models)
    result = await extractor.process_document(
        text=request.text,
        doc_id=f"parallel_extract_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        extract_relationships=request.extract_relationships
    )

    # Optionally add to knowledge graph
    if request.add_to_graph and result.get("entities"):
        added_count = 0
        for entity_data in result["entities"]:
            from parallel_websearch_server.knowledge_graph import Entity
            entity = Entity(
                text=entity_data["name"],
                entity_type=entity_data.get("type", "other"),
                properties={
                    "confidence": entity_data.get("confidence", 0.5),
                    "extraction_method": "parallel_llm",
                    "consensus_votes": entity_data.get("consensus_votes", 0),
                    "models": entity_data.get("models", []),
                }
            )
            graph.add_entity(entity)
            added_count += 1

        result["added_to_graph"] = added_count
        save_knowledge_graph()

    log_activity("parallel_extract", {
        "text_length": len(request.text),
        "entities_found": result.get("entity_count", 0),
        "models_used": result.get("models_used", []),
        "relationships_found": len(result.get("relationships", [])),
    })

    return result


@router.post("/models/benchmark", operation_id="benchmark_extraction_models", response_model=BenchmarkResponse)
async def benchmark_extraction_models(
    request: BenchmarkRequest,
) -> BenchmarkResponse:
    """Run benchmark to find best models for entity extraction.

    Evaluates models from OpenRouter's middle tier (excluding top/bottom 7.5% by price)
    for latency, price, and extraction quality (intelligence).

    Returns top 5 models ranked by value_score = intelligence / (price * latency).
    """
    from parallel_websearch_server.model_evaluator import quick_benchmark

    start = datetime.utcnow()
    top_models = await quick_benchmark(request.n_models)

    return {
        "benchmark_time_ms": round((datetime.utcnow() - start).total_seconds() * 1000, 2),
        "models_evaluated": request.n_models,
        "top_models": top_models,
        "recommendation": top_models[0] if top_models else None,
    }


@router.get("/models/list", operation_id="list_available_models")
async def list_available_models(
    tier: str = Query("mid", description="Price tier: low, mid, high, all"),
    limit: int = Query(50, ge=10, le=200),
) -> Dict[str, Any]:
    """List available OpenRouter models in the filtered middle tier."""
    from parallel_websearch_server.model_evaluator import get_evaluator

    evaluator = get_evaluator()
    all_models = await evaluator.fetch_models()

    n = len(all_models)
    if tier == "low":
        models = all_models[:n//3]
    elif tier == "high":
        models = all_models[2*n//3:]
    elif tier == "mid":
        models = all_models[n//3:2*n//3]
    else:
        models = all_models

    return {
        "total_filtered": len(all_models),
        "tier": tier,
        "count": min(len(models), limit),
        "models": models[:limit],
    }


@router.post("/ingest/parallel", operation_id="ingest_with_parallel_extraction")
async def ingest_with_parallel_extraction(
    background_tasks: BackgroundTasks,
    max_docs: int = Query(10, ge=1, le=50),
    models: Optional[List[str]] = Query(None, description="Models to use for extraction"),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest research corpus using parallel LLM extraction instead of spaCy.

    Uses multiple LLMs (haiku, flash, deepseek, qwen) via OpenRouter for consensus-based
    entity extraction, which can be more accurate for domain-specific content.
    """
    if not RESEARCH_DATA_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Research data not found at {RESEARCH_DATA_PATH}")

    metadata_path = RESEARCH_DATA_PATH / "corpus_metadata.json"
    if not metadata_path.exists():
        raise HTTPException(status_code=404, detail="corpus_metadata.json not found")

    with open(metadata_path) as f:
        metadata = json.load(f)

    from parallel_websearch_server.dspy_entity_extractor import get_extractor
    from parallel_websearch_server.knowledge_graph import Entity

    extractor = get_extractor(models)
    documents = metadata.get("documents", [])[:max_docs]
    total_entities = 0
    processed = 0

    for doc in documents:
        md_path = RESEARCH_DATA_PATH / doc["filename"]
        if md_path.exists():
            try:
                content = md_path.read_text()[:15000]  # Larger context for better extraction
                result = await extractor.process_document(
                    text=content,
                    doc_id=doc["doc_id"],
                    extract_relationships=True
                )

                # Add entities to graph
                for entity_data in result.get("entities", []):
                    entity = Entity(
                        text=entity_data["name"],
                        entity_type=entity_data.get("type", "other"),
                        properties={
                            "confidence": entity_data.get("confidence", 0.5),
                            "extraction_method": "parallel_llm",
                            "source_doc": doc["doc_id"],
                        }
                    )
                    graph.add_entity(entity)
                    total_entities += 1

                processed += 1
                log_activity("parallel_ingest_doc", {"doc": doc["filename"], "entities": len(result.get("entities", []))})

            except Exception as e:
                logger.warning(f"Failed to process {doc['filename']}: {e}")

    save_knowledge_graph()
    log_activity("parallel_ingest", {"documents": processed, "entities": total_entities})

    return {
        "documents_processed": processed,
        "total_entities_extracted": total_entities,
        "extraction_method": "parallel_llm",
        "models_used": extractor.models,
        "graph_stats": graph.get_stats(),
    }


@router.post("/ingest/research", operation_id="ingest_research_corpus")
async def ingest_research_corpus(
    background_tasks: BackgroundTasks,
    max_docs: int = Query(50, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest the DAGI research corpus into the knowledge graph."""
    if not RESEARCH_DATA_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Research data not found at {RESEARCH_DATA_PATH}")

    # Load metadata
    metadata_path = RESEARCH_DATA_PATH / "corpus_metadata.json"
    if not metadata_path.exists():
        raise HTTPException(status_code=404, detail="corpus_metadata.json not found")

    with open(metadata_path) as f:
        metadata = json.load(f)

    documents = metadata.get("documents", [])[:max_docs]
    total_entities = 0
    processed = 0

    for doc in documents:
        # Try to load the markdown file
        md_path = RESEARCH_DATA_PATH / doc["filename"]
        if md_path.exists():
            try:
                content = md_path.read_text()[:50000]  # Limit size
                result = graph.process_document(
                    text=content,
                    doc_id=doc["doc_id"],
                    metadata={
                        "title": doc["title"],
                        "category": doc.get("category"),
                        "topics": doc.get("topics", []),
                        "filename": doc["filename"],
                    },
                )
                total_entities += result["entities_found"]
                processed += 1
            except Exception as e:
                logger.warning(f"Failed to process {doc['filename']}: {e}")

    # Persist to SQLite
    save_knowledge_graph()

    log_activity("ingest", {"documents": processed, "entities": total_entities})

    return {
        "documents_processed": processed,
        "total_entities_extracted": total_entities,
        "graph_stats": graph.get_stats(),
    }


# =============================================================================
# Clean Search UI
# =============================================================================

@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def search_engine_ui(
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
):
    """Clean, white search engine UI."""
    stats = graph.get_stats()

    # Check if research data is available
    research_available = RESEARCH_DATA_PATH.exists()

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Knowledge Search Engine</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #fafafa;
            color: #1a1a1a;
            min-height: 100vh;
        }}
        .header {{
            background: white;
            border-bottom: 1px solid #e5e5e5;
            padding: 16px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{ font-size: 20px; font-weight: 600; }}
        .header-links a {{
            color: #666;
            text-decoration: none;
            margin-left: 20px;
            font-size: 14px;
        }}
        .header-links a:hover {{ color: #000; }}
        .main-layout {{
            display: grid;
            grid-template-columns: 1fr 320px;
            gap: 24px;
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }}
        .main-content {{ min-width: 0; }}
        .sidebar {{ display: flex; flex-direction: column; gap: 16px; }}

        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }}
        .stat-card {{
            background: white;
            border: 1px solid #e5e5e5;
            border-radius: 8px;
            padding: 16px;
            text-align: center;
        }}
        .stat-value {{ font-size: 28px; font-weight: 700; color: #000; }}
        .stat-label {{
            font-size: 11px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 4px;
        }}

        /* Sections */
        .section {{
            background: white;
            border: 1px solid #e5e5e5;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 16px;
        }}
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        .section-title {{ font-size: 16px; font-weight: 600; }}

        /* Buttons */
        .btn {{
            padding: 8px 16px;
            border: none;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .btn-primary {{ background: #000; color: white; }}
        .btn-primary:hover {{ background: #333; }}
        .btn-success {{ background: #16a34a; color: white; }}
        .btn-success:hover {{ background: #15803d; }}
        .btn-secondary {{ background: #f5f5f5; color: #333; border: 1px solid #e5e5e5; }}
        .btn-secondary:hover {{ background: #eee; }}
        .btn-sm {{ padding: 6px 12px; font-size: 12px; }}
        .btn:disabled {{ background: #9ca3af; cursor: not-allowed; }}
        .btn-group {{ display: flex; gap: 8px; }}

        /* Search Box */
        .search-box {{ display: flex; gap: 12px; }}
        .search-input {{
            flex: 1;
            padding: 12px 16px;
            font-size: 15px;
            border: 2px solid #e5e5e5;
            border-radius: 8px;
            outline: none;
            transition: border-color 0.2s;
        }}
        .search-input:focus {{ border-color: #000; }}

        /* Filters */
        .filters {{ display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; align-items: center; }}
        .filter-chip {{
            padding: 5px 12px;
            background: #f5f5f5;
            border: 1px solid #e5e5e5;
            border-radius: 20px;
            font-size: 12px;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .filter-chip:hover {{ background: #eee; }}
        .filter-chip.active {{ background: #000; color: white; border-color: #000; }}
        .filter-label {{ font-size: 12px; color: #666; margin-right: 4px; }}
        .filter-select {{
            padding: 5px 10px;
            border: 1px solid #e5e5e5;
            border-radius: 6px;
            font-size: 12px;
            background: white;
        }}

        /* Results */
        .results-list {{ max-height: 450px; overflow-y: auto; }}
        .result-item {{
            padding: 14px 16px;
            border-bottom: 1px solid #f0f0f0;
            display: flex;
            align-items: center;
            gap: 14px;
            cursor: pointer;
            transition: background 0.1s;
        }}
        .result-item:hover {{ background: #f8f8f8; }}
        .result-item:last-child {{ border-bottom: none; }}
        .result-type {{
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            min-width: 85px;
            text-align: center;
        }}
        .type-person {{ background: #fce7f3; color: #9d174d; }}
        .type-organization {{ background: #ede9fe; color: #5b21b6; }}
        .type-location {{ background: #d1fae5; color: #065f46; }}
        .type-product {{ background: #fef3c7; color: #92400e; }}
        .type-event {{ background: #fee2e2; color: #991b1b; }}
        .type-other {{ background: #f3f4f6; color: #374151; }}
        .result-content {{ flex: 1; min-width: 0; }}
        .result-text {{ font-weight: 500; margin-bottom: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .result-meta {{ font-size: 12px; color: #666; }}
        .result-actions {{
            display: flex;
            gap: 6px;
            opacity: 0;
            transition: opacity 0.2s;
        }}
        .result-item:hover .result-actions {{ opacity: 1; }}
        .result-checkbox {{ width: 16px; height: 16px; cursor: pointer; }}

        /* Ingest Status */
        .ingest-status {{
            font-size: 13px;
            color: #666;
            padding: 12px;
            background: #f9fafb;
            border-radius: 6px;
            margin-top: 12px;
            display: none;
        }}
        .ingest-status.active {{ display: block; }}
        .ingest-status.error {{ background: #fef2f2; color: #991b1b; }}
        .ingest-status.success {{ background: #f0fdf4; color: #166534; }}

        /* Types Grid */
        .types-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
            gap: 8px;
            margin-top: 12px;
        }}
        .type-stat {{
            padding: 10px;
            background: #f9fafb;
            border-radius: 6px;
            text-align: center;
        }}
        .type-stat-count {{ font-size: 18px; font-weight: 600; }}
        .type-stat-label {{ font-size: 10px; color: #666; text-transform: uppercase; }}

        /* Empty State */
        .empty-state {{
            padding: 50px 20px;
            text-align: center;
            color: #666;
        }}
        .empty-state h3 {{ margin-bottom: 8px; color: #333; }}

        /* Loading */
        .loading {{ text-align: center; padding: 40px; color: #666; }}
        @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
        .spinner {{
            display: inline-block;
            width: 18px;
            height: 18px;
            border: 2px solid #e5e5e5;
            border-top-color: #000;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-right: 8px;
            vertical-align: middle;
        }}

        /* Modal */
        .modal-overlay {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.5);
            z-index: 1000;
            align-items: center;
            justify-content: center;
        }}
        .modal-overlay.active {{ display: flex; }}
        .modal {{
            background: white;
            border-radius: 12px;
            width: 90%;
            max-width: 600px;
            max-height: 80vh;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }}
        .modal-header {{
            padding: 16px 20px;
            border-bottom: 1px solid #e5e5e5;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .modal-title {{ font-size: 18px; font-weight: 600; }}
        .modal-close {{
            background: none;
            border: none;
            font-size: 24px;
            cursor: pointer;
            color: #666;
        }}
        .modal-body {{
            padding: 20px;
            overflow-y: auto;
            flex: 1;
        }}
        .detail-row {{
            display: flex;
            border-bottom: 1px solid #f0f0f0;
            padding: 10px 0;
        }}
        .detail-label {{
            width: 120px;
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
        }}
        .detail-value {{ flex: 1; }}
        .tag {{
            display: inline-block;
            padding: 2px 8px;
            background: #f3f4f6;
            border-radius: 4px;
            font-size: 12px;
            margin: 2px;
        }}
        .relationship-item {{
            padding: 8px 12px;
            background: #f9fafb;
            border-radius: 6px;
            margin-bottom: 6px;
            font-size: 13px;
        }}
        .relationship-arrow {{ color: #666; margin: 0 8px; }}

        /* Activity Log */
        .activity-list {{ max-height: 300px; overflow-y: auto; }}
        @keyframes slideIn {{
            from {{ opacity: 0; transform: translateX(-10px); }}
            to {{ opacity: 1; transform: translateX(0); }}
        }}
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
        .activity-item {{
            padding: 10px 0;
            border-bottom: 1px solid #f0f0f0;
            font-size: 13px;
        }}
        .activity-item:last-child {{ border-bottom: none; }}
        .activity-time {{ font-size: 11px; color: #999; }}
        .activity-action {{
            display: inline-block;
            padding: 2px 6px;
            background: #e5e5e5;
            border-radius: 4px;
            font-size: 11px;
            margin-right: 6px;
        }}

        /* Compare Panel */
        .compare-panel {{
            background: #fffbeb;
            border: 1px solid #fcd34d;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 16px;
            display: none;
        }}
        .compare-panel.active {{ display: block; }}
        .compare-items {{ display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0; }}
        .compare-tag {{
            display: flex;
            align-items: center;
            padding: 4px 8px;
            background: white;
            border: 1px solid #e5e5e5;
            border-radius: 4px;
            font-size: 12px;
        }}
        .compare-tag-remove {{
            margin-left: 6px;
            cursor: pointer;
            color: #999;
        }}

        @media (max-width: 900px) {{
            .main-layout {{ grid-template-columns: 1fr; }}
            .sidebar {{ order: -1; }}
            .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Knowledge Search Engine</h1>
        <div class="header-links">
            <a href="/browser">Data Browser</a>
            <a href="/browser/kg">Graph Explorer</a>
            <a href="/docs">API Docs</a>
        </div>
    </div>

    <div class="main-layout">
        <div class="main-content">
            <!-- Stats -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="stat-entities">{stats["total_entities"]}</div>
                    <div class="stat-label">Entities</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="stat-relationships">{stats["total_relationships"]}</div>
                    <div class="stat-label">Relationships</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="stat-documents">{stats["documents_processed"]}</div>
                    <div class="stat-label">Documents</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="stat-components">{stats["connected_components"]}</div>
                    <div class="stat-label">Components</div>
                </div>
            </div>

            <!-- Compare Panel -->
            <div class="compare-panel" id="compare-panel">
                <div style="font-weight: 600; font-size: 13px;">Compare Entities</div>
                <div class="compare-items" id="compare-items"></div>
                <div class="btn-group">
                    <button class="btn btn-primary btn-sm" onclick="runComparison()">Compare Selected</button>
                    <button class="btn btn-secondary btn-sm" onclick="clearComparison()">Clear</button>
                </div>
            </div>

            <!-- Ingest Section -->
            <div class="section">
                <div class="section-header">
                    <div>
                        <div class="section-title">DAGI Research Corpus</div>
                        <div style="font-size: 12px; color: #666; margin-top: 4px;">
                            {'28 documents on Distributed AGI' if research_available else 'Research data not found'}
                        </div>
                    </div>
                    <button class="btn btn-success" id="ingest-btn" onclick="ingestResearch()" {'disabled' if not research_available else ''}>
                        Ingest Corpus
                    </button>
                </div>
                <div class="ingest-status" id="ingest-status"></div>
                <div class="types-grid" id="types-grid">
                    {_render_type_stats(stats.get("entity_types", {}))}
                </div>
            </div>

            <!-- Search -->
            <div class="section">
                <div class="search-box">
                    <input type="text" class="search-input" id="search-input" placeholder="Search entities..." onkeyup="handleSearch(event)">
                    <button class="btn btn-primary" onclick="doSearch()">Search</button>
                </div>
                <div class="filters">
                    <span class="filter-chip active" data-type="">All Types</span>
                    <span class="filter-chip" data-type="person">Person</span>
                    <span class="filter-chip" data-type="organization">Organization</span>
                    <span class="filter-chip" data-type="location">Location</span>
                    <span class="filter-chip" data-type="product">Product</span>
                    <span class="filter-chip" data-type="event">Event</span>
                    <span class="filter-chip" data-type="other">Other</span>
                    <span style="margin-left: auto;"></span>
                    <span class="filter-label">Min mentions:</span>
                    <select class="filter-select" id="min-mentions">
                        <option value="0">Any</option>
                        <option value="2">2+</option>
                        <option value="5">5+</option>
                        <option value="10">10+</option>
                    </select>
                </div>
            </div>

            <!-- Results -->
            <div class="section">
                <div class="section-header">
                    <span id="results-count" style="font-size: 13px; color: #666;">Enter a search query</span>
                    <div class="btn-group">
                        <button class="btn btn-secondary btn-sm" onclick="exportCSV()">Export CSV</button>
                        <button class="btn btn-secondary btn-sm" onclick="exportJSON()">Export JSON</button>
                    </div>
                </div>
                <div class="results-list" id="results-list">
                    <div class="empty-state">
                        <h3>Search the Knowledge Graph</h3>
                        <p>Enter a query above to find entities</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- Sidebar -->
        <div class="sidebar">
            <!-- Activity Log -->
            <div class="section">
                <div class="section-header">
                    <span class="section-title">Activity Log</span>
                    <span id="stream-status" style="font-size: 12px; margin-left: 8px;"><span style="color: #f59e0b;">● Connecting...</span></span>
                </div>
                <div class="activity-list" id="activity-list">
                    <div class="empty-state" style="padding: 20px;">
                        <p style="font-size: 13px;">Connecting to stream...</p>
                    </div>
                </div>
            </div>

            <!-- Quick Stats -->
            <div class="section">
                <div class="section-title" style="margin-bottom: 12px;">Entity Types</div>
                <div id="sidebar-types">
                    {_render_sidebar_types(stats.get("entity_types", {}))}
                </div>
            </div>
        </div>
    </div>

    <!-- Entity Detail Modal -->
    <div class="modal-overlay" id="entity-modal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title" id="modal-title">Entity Details</span>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body" id="modal-body">
                <div class="loading"><span class="spinner"></span>Loading...</div>
            </div>
        </div>
    </div>

    <!-- Compare Modal -->
    <div class="modal-overlay" id="compare-modal">
        <div class="modal" style="max-width: 800px;">
            <div class="modal-header">
                <span class="modal-title">Entity Comparison</span>
                <button class="modal-close" onclick="closeCompareModal()">&times;</button>
            </div>
            <div class="modal-body" id="compare-body">
                <div class="loading"><span class="spinner"></span>Loading...</div>
            </div>
        </div>
    </div>

    <script>
        let activeType = '';
        let compareList = [];

        // Filter chips
        document.querySelectorAll('.filter-chip').forEach(chip => {{
            chip.addEventListener('click', () => {{
                document.querySelectorAll('.filter-chip').forEach(c => c.classList.remove('active'));
                chip.classList.add('active');
                activeType = chip.dataset.type;
                doSearch();
            }});
        }});

        function handleSearch(e) {{ if (e.key === 'Enter') doSearch(); }}

        async function doSearch() {{
            const query = document.getElementById('search-input').value.trim();
            if (!query) return;

            const resultsList = document.getElementById('results-list');
            resultsList.innerHTML = '<div class="loading"><span class="spinner"></span>Searching...</div>';

            try {{
                const body = {{
                    q: query,
                    limit: 50,
                    min_mentions: parseInt(document.getElementById('min-mentions').value) || 0
                }};
                if (activeType) body.entity_types = [activeType];

                const response = await fetch('/v1/search/query', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(body)
                }});
                const data = await response.json();

                document.getElementById('results-count').textContent =
                    data.total + ' result' + (data.total !== 1 ? 's' : '') + ' for "' + query + '"';

                if (data.results.length === 0) {{
                    resultsList.innerHTML = '<div class="empty-state"><h3>No results found</h3><p>Try a different search term</p></div>';
                    return;
                }}

                resultsList.innerHTML = data.results.map(r => `
                    <div class="result-item" onclick="showEntityDetail('${{r.id}}')">
                        <input type="checkbox" class="result-checkbox" onclick="event.stopPropagation(); toggleCompare('${{r.id}}', '${{r.text}}')" ${{compareList.some(c => c.id === r.id) ? 'checked' : ''}}>
                        <span class="result-type type-${{r.type}}">${{r.type}}</span>
                        <div class="result-content">
                            <div class="result-text">${{r.text}}</div>
                            <div class="result-meta">${{r.mentions}} mention${{r.mentions !== 1 ? 's' : ''}} &middot; Score: ${{r.score}}</div>
                        </div>
                        <div class="result-actions">
                            <button class="btn btn-secondary btn-sm" onclick="event.stopPropagation(); showEntityDetail('${{r.id}}')">Details</button>
                        </div>
                    </div>
                `).join('');
            }} catch (e) {{
                resultsList.innerHTML = '<div class="empty-state"><h3>Error</h3><p>' + e.message + '</p></div>';
            }}
        }}

        async function showEntityDetail(entityId) {{
            document.getElementById('entity-modal').classList.add('active');
            document.getElementById('modal-body').innerHTML = '<div class="loading"><span class="spinner"></span>Loading...</div>';

            try {{
                const response = await fetch('/v1/search/entity/' + entityId);
                const data = await response.json();

                document.getElementById('modal-title').textContent = data.text;
                let html = `
                    <div class="detail-row">
                        <div class="detail-label">Type</div>
                        <div class="detail-value"><span class="result-type type-${{data.type}}">${{data.type}}</span></div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Mentions</div>
                        <div class="detail-value">${{data.mentions}}</div>
                    </div>`;

                if (data.aliases && data.aliases.length > 0) {{
                    html += `<div class="detail-row">
                        <div class="detail-label">Aliases</div>
                        <div class="detail-value">${{data.aliases.map(a => '<span class="tag">' + a + '</span>').join('')}}</div>
                    </div>`;
                }}

                if (data.sources && data.sources.length > 0) {{
                    html += `<div class="detail-row">
                        <div class="detail-label">Sources</div>
                        <div class="detail-value">${{data.sources.slice(0, 5).map(s => '<span class="tag">' + s + '</span>').join('')}}</div>
                    </div>`;
                }}

                if (data.relationships && data.relationships.length > 0) {{
                    html += `<div style="margin-top: 16px; font-weight: 600; margin-bottom: 8px;">Relationships (${{data.relationships.length}})</div>`;
                    data.relationships.slice(0, 10).forEach(r => {{
                        const arrow = r.direction === 'outgoing' ? '&rarr;' : '&larr;';
                        html += `<div class="relationship-item">
                            <span class="result-type type-${{r.entity_type}}" style="font-size: 9px;">${{r.entity_type}}</span>
                            <span class="relationship-arrow">${{arrow}}</span>
                            <strong>${{r.entity_text}}</strong>
                            <span style="color: #666; font-size: 12px; margin-left: 8px;">(${{r.relation}})</span>
                        </div>`;
                    }});
                }}

                document.getElementById('modal-body').innerHTML = html;
            }} catch (e) {{
                document.getElementById('modal-body').innerHTML = '<div class="empty-state"><p>Error loading entity</p></div>';
            }}
        }}

        function closeModal() {{ document.getElementById('entity-modal').classList.remove('active'); }}
        function closeCompareModal() {{ document.getElementById('compare-modal').classList.remove('active'); }}

        function toggleCompare(id, text) {{
            const idx = compareList.findIndex(c => c.id === id);
            if (idx >= 0) {{
                compareList.splice(idx, 1);
            }} else if (compareList.length < 5) {{
                compareList.push({{ id, text }});
            }}
            updateComparePanel();
        }}

        function updateComparePanel() {{
            const panel = document.getElementById('compare-panel');
            const items = document.getElementById('compare-items');

            if (compareList.length > 0) {{
                panel.classList.add('active');
                items.innerHTML = compareList.map(c => `
                    <div class="compare-tag">
                        ${{c.text}}
                        <span class="compare-tag-remove" onclick="toggleCompare('${{c.id}}', '${{c.text}}')">&times;</span>
                    </div>
                `).join('');
            }} else {{
                panel.classList.remove('active');
            }}
        }}

        function clearComparison() {{
            compareList = [];
            updateComparePanel();
            document.querySelectorAll('.result-checkbox').forEach(cb => cb.checked = false);
        }}

        async function runComparison() {{
            if (compareList.length < 2) {{ alert('Select at least 2 entities to compare'); return; }}

            document.getElementById('compare-modal').classList.add('active');
            document.getElementById('compare-body').innerHTML = '<div class="loading"><span class="spinner"></span>Loading...</div>';

            try {{
                const response = await fetch('/v1/search/compare', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ entity_ids: compareList.map(c => c.id) }})
                }});
                const data = await response.json();

                let html = '<table style="width: 100%; border-collapse: collapse;">';
                html += '<tr style="background: #f9fafb;"><th style="padding: 10px; text-align: left; border-bottom: 2px solid #e5e5e5;">Property</th>';
                data.entities.forEach(e => {{
                    html += `<th style="padding: 10px; text-align: left; border-bottom: 2px solid #e5e5e5;">${{e.text}}</th>`;
                }});
                html += '</tr>';

                html += '<tr><td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">Type</td>';
                data.entities.forEach(e => {{
                    html += `<td style="padding: 10px; border-bottom: 1px solid #f0f0f0;"><span class="result-type type-${{e.type}}">${{e.type}}</span></td>`;
                }});
                html += '</tr>';

                html += '<tr><td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">Mentions</td>';
                data.entities.forEach(e => {{
                    html += `<td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">${{e.mentions}}</td>`;
                }});
                html += '</tr>';

                html += '<tr><td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">Relationships</td>';
                data.entities.forEach(e => {{
                    html += `<td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">${{e.relationship_count}}</td>`;
                }});
                html += '</tr></table>';

                if (data.common_relationships && data.common_relationships.length > 0) {{
                    html += '<div style="margin-top: 16px; font-weight: 600;">Common Connections</div>';
                    data.common_relationships.forEach(r => {{
                        html += `<div class="relationship-item">${{r.source}} <span class="relationship-arrow">&harr;</span> ${{r.target}} (${{r.relation}})</div>`;
                    }});
                }}

                document.getElementById('compare-body').innerHTML = html;
            }} catch (e) {{
                document.getElementById('compare-body').innerHTML = '<div class="empty-state"><p>Error comparing entities</p></div>';
            }}
        }}

        async function ingestResearch() {{
            const btn = document.getElementById('ingest-btn');
            const status = document.getElementById('ingest-status');

            btn.disabled = true;
            btn.textContent = 'Ingesting...';
            status.className = 'ingest-status active';
            status.innerHTML = '<span class="spinner"></span> Processing research documents...';

            try {{
                const response = await fetch('/v1/search/ingest/research?max_docs=30', {{ method: 'POST' }});
                if (!response.ok) throw new Error('Server error: ' + response.status);
                const data = await response.json();

                status.className = 'ingest-status active success';
                status.innerHTML = `<strong>Ingestion complete!</strong><br>Documents: ${{data.documents_processed}} | Entities: ${{data.total_entities_extracted}} | Total: ${{data.graph_stats.total_entities}}`;

                document.getElementById('stat-entities').textContent = data.graph_stats.total_entities;
                document.getElementById('stat-relationships').textContent = data.graph_stats.total_relationships;
                document.getElementById('stat-documents').textContent = data.graph_stats.documents_processed;
                document.getElementById('stat-components').textContent = data.graph_stats.connected_components;
                updateTypeStats(data.graph_stats.entity_types);
                btn.textContent = 'Ingested!';
                // Activity updates automatically via SSE stream
            }} catch (e) {{
                status.className = 'ingest-status active error';
                status.innerHTML = '<strong>Error:</strong> ' + e.message;
                btn.textContent = 'Retry';
                btn.disabled = false;
            }}
        }}

        function updateTypeStats(types) {{
            document.getElementById('types-grid').innerHTML = Object.entries(types || {{}}).map(([type, count]) => `
                <div class="type-stat">
                    <div class="type-stat-count">${{count}}</div>
                    <div class="type-stat-label">${{type}}</div>
                </div>
            `).join('');
        }}

        async function refreshStats() {{
            try {{
                const response = await fetch('/v1/search/stats');
                const data = await response.json();
                const stats = data.knowledge_graph;
                document.getElementById('stat-entities').textContent = stats.total_entities;
                document.getElementById('stat-relationships').textContent = stats.total_relationships;
                document.getElementById('stat-documents').textContent = stats.documents_processed;
                document.getElementById('stat-components').textContent = stats.connected_components;
                updateTypeStats(stats.entity_types);
            }} catch (e) {{}}
        }}

        // Real-time activity streaming via SSE
        let activityEvents = [];
        let eventSource = null;
        let connectionStatus = 'disconnected';

        function formatActivity(a) {{
            const time = new Date(a.timestamp).toLocaleTimeString();
            let detail = '';
            let icon = '📌';
            if (a.action === 'search') {{ detail = '"' + a.details.query + '" (' + a.details.results_count + ')'; icon = '🔍'; }}
            else if (a.action === 'view_entity') {{ detail = a.details.entity_text; icon = '👁️'; }}
            else if (a.action === 'compare_entities') {{ detail = a.details.entity_ids.length + ' entities'; icon = '⚖️'; }}
            else if (a.action === 'export_csv') {{ detail = a.details.count + ' entities'; icon = '📊'; }}
            else if (a.action === 'export_json') {{ detail = a.details.count + ' entities'; icon = '📦'; }}
            else if (a.action === 'ingest') {{ detail = a.details.documents + ' docs'; icon = '📥'; }}
            return `<div class="activity-item" style="animation: slideIn 0.3s ease-out;">
                <span class="activity-action">${{icon}} ${{a.action}}</span>
                ${{detail}}
                <div class="activity-time">${{time}}</div>
            </div>`;
        }}

        function renderActivities() {{
            const list = document.getElementById('activity-list');
            if (activityEvents.length === 0) {{
                list.innerHTML = '<div class="empty-state" style="padding: 20px;"><p style="font-size: 13px;">Waiting for activity...</p></div>';
                return;
            }}
            list.innerHTML = activityEvents.slice(-20).reverse().map(formatActivity).join('');
        }}

        function updateConnectionStatus(status) {{
            connectionStatus = status;
            const indicator = document.getElementById('stream-status');
            if (indicator) {{
                if (status === 'connected') {{
                    indicator.innerHTML = '<span style="color: #22c55e;">● Live</span>';
                }} else if (status === 'connecting') {{
                    indicator.innerHTML = '<span style="color: #f59e0b;">● Connecting...</span>';
                }} else {{
                    indicator.innerHTML = '<span style="color: #ef4444;">● Disconnected</span>';
                }}
            }}
        }}

        function connectActivityStream() {{
            if (eventSource) eventSource.close();
            updateConnectionStatus('connecting');

            eventSource = new EventSource('/v1/search/activity/stream');

            eventSource.addEventListener('connected', (e) => {{
                updateConnectionStatus('connected');
                console.log('Activity stream connected:', JSON.parse(e.data));
            }});

            eventSource.addEventListener('activity', (e) => {{
                const activity = JSON.parse(e.data);
                activityEvents.push(activity);
                if (activityEvents.length > 100) activityEvents.shift();
                renderActivities();
            }});

            eventSource.addEventListener('ping', () => {{
                // Keepalive - connection is healthy
            }});

            eventSource.onerror = () => {{
                updateConnectionStatus('disconnected');
                eventSource.close();
                // Reconnect after 3 seconds
                setTimeout(connectActivityStream, 3000);
            }};
        }}

        function exportCSV() {{ window.open('/v1/search/export/csv', '_blank'); }}
        function exportJSON() {{ window.open('/v1/search/export/json', '_blank'); }}

        // Start SSE connection and periodic stats refresh
        setInterval(refreshStats, 10000);
        connectActivityStream();
    </script>
</body>
</html>'''

    return HTMLResponse(html)


def _render_type_stats(entity_types: Dict[str, int]) -> str:
    """Render entity type statistics."""
    if not entity_types:
        return '<div style="color: #999; font-size: 13px;">No entities yet. Ingest data to populate.</div>'

    html = ""
    for type_name, count in sorted(entity_types.items(), key=lambda x: x[1], reverse=True):
        html += f'''
        <div class="type-stat">
            <div class="type-stat-count">{count}</div>
            <div class="type-stat-label">{type_name}</div>
        </div>'''
    return html


def _render_sidebar_types(entity_types: Dict[str, int]) -> str:
    """Render sidebar entity type list."""
    if not entity_types:
        return '<div style="color: #999; font-size: 12px;">No entities yet</div>'

    type_colors = {
        'person': '#fce7f3',
        'organization': '#ede9fe',
        'location': '#d1fae5',
        'product': '#fef3c7',
        'event': '#fee2e2',
    }

    html = ""
    for type_name, count in sorted(entity_types.items(), key=lambda x: x[1], reverse=True):
        color = type_colors.get(type_name, '#f3f4f6')
        html += f'''
        <div style="display: flex; justify-content: space-between; padding: 8px 10px; margin-bottom: 4px; background: {color}; border-radius: 6px; font-size: 13px;">
            <span style="text-transform: capitalize;">{type_name}</span>
            <span style="font-weight: 600;">{count}</span>
        </div>'''
    return html
