"""Knowledge Graph API routes."""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from parallel_websearch_server.knowledge_graph import (
    KnowledgeGraph,
    get_knowledge_graph,
)
from parallel_websearch_server.turso_store import get_turso_store, TursoStore
from parallel_websearch_server.jina_client import get_jina_client, JinaClient

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/kg", tags=["Knowledge Graph"])


# =============================================================================
# Request/Response Models
# =============================================================================

class ProcessTextRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Text to process")
    doc_id: Optional[str] = Field(None, description="Document identifier")
    source_type: Optional[str] = Field(None, description="Source type (search, extract, chat, etc)")
    source_id: Optional[str] = Field(None, description="Source record ID")
    compute_embeddings: bool = Field(False, description="Compute Jina embeddings for entities")


class ProcessBatchRequest(BaseModel):
    texts: List[str] = Field(..., min_items=1)
    compute_embeddings: bool = False


class SearchEntitiesRequest(BaseModel):
    query: str
    entity_type: Optional[str] = None
    limit: int = 20


class PathsRequest(BaseModel):
    source_id: str
    target_id: str
    max_length: int = 4


class UpdateEntityRequest(BaseModel):
    entity_type: Optional[str] = Field(None, description="New entity type")
    text: Optional[str] = Field(None, description="New entity text/name")
    aliases: Optional[List[str]] = Field(None, description="Updated aliases")
    merge_into: Optional[str] = Field(None, description="Merge this entity into another entity ID")
    delete: bool = Field(False, description="Mark entity as deleted/invalid")
    notes: Optional[str] = Field(None, description="Human notes about this edit")


# Valid entity types for the schema
VALID_ENTITY_TYPES = [
    "person", "organization", "technology", "concept", "location", "event",
    "document", "system", "protocol", "algorithm", "data_structure", "api",
    "framework", "language", "standard", "product", "method", "metric",
    "dataset", "model", "tool", "library", "service", "platform"
]


def _init_edit_tracking_table():
    """Initialize the human edits tracking table."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_edits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id TEXT NOT NULL,
            edit_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            notes TEXT,
            edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (entity_id) REFERENCES entities(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_edits_entity ON entity_edits(entity_id)")
    conn.commit()
    conn.close()


# Initialize on module load
_init_edit_tracking_table()


# =============================================================================
# API Endpoints
# =============================================================================

@router.post("/process", operation_id="process_text_for_kg")
async def process_text(
    request: ProcessTextRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Extract entities from text and add to knowledge graph."""
    result = graph.process_document(
        text=request.text,
        doc_id=request.doc_id,
        metadata={
            "source_type": request.source_type,
            "source_id": request.source_id,
        } if request.source_type else None,
    )

    # Optionally compute embeddings with Jina
    if request.compute_embeddings and result["entity_ids"]:
        try:
            jina = get_jina_client()
            entity_texts = [
                graph.entities[eid].text
                for eid in result["entity_ids"]
                if eid in graph.entities
            ]
            if entity_texts:
                embeddings = await jina.embed(entity_texts)
                result["embeddings_computed"] = len(embeddings)
        except Exception as e:
            logger.warning(f"Failed to compute embeddings: {e}")
            result["embeddings_error"] = str(e)

    # Persist to Turso
    try:
        store = get_turso_store()
        conn = store._get_conn()
        conn.execute(
            """INSERT INTO kg_documents (doc_id, source_type, source_id, entities_found, processed_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(doc_id) DO UPDATE SET entities_found = excluded.entities_found""",
            (request.doc_id, request.source_type, request.source_id, result["entities_found"])
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to persist document: {e}")

    return result


@router.post("/process/batch", operation_id="process_batch_for_kg")
async def process_batch(
    request: ProcessBatchRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Process multiple texts at once."""
    results = []
    total_entities = 0

    for i, text in enumerate(request.texts):
        result = graph.process_document(text, doc_id=f"batch_{i}")
        results.append(result)
        total_entities += result["entities_found"]

    return {
        "documents_processed": len(request.texts),
        "total_entities": total_entities,
        "results": results,
    }


@router.get("/entities", operation_id="list_kg_entities")
async def list_entities(
    entity_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """List entities in the knowledge graph."""
    entities = []
    for entity in graph.entities.values():
        if entity_type and entity.type != entity_type:
            continue
        entities.append(entity.to_dict())
        if len(entities) >= limit:
            break

    return {
        "total": len(graph.entities),
        "returned": len(entities),
        "entities": entities,
    }


@router.get("/entities/{entity_id}", operation_id="get_kg_entity")
async def get_entity(
    entity_id: str,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get entity details by ID."""
    entity = graph.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")
    return entity


@router.patch("/entities/{entity_id}", operation_id="update_kg_entity")
async def update_entity(
    entity_id: str,
    request: UpdateEntityRequest,
) -> Dict[str, Any]:
    """Update an entity's type, text, or other properties. Tracks edit history."""
    import sqlite3
    from pathlib import Path
    from datetime import datetime

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        raise HTTPException(status_code=500, detail="Knowledge graph database not found")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get current entity
    cursor.execute("SELECT * FROM entities WHERE id = ?", (entity_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    old_entity = dict(row)
    changes = []

    # Handle delete
    if request.delete:
        cursor.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
        cursor.execute(
            "INSERT INTO entity_edits (entity_id, edit_type, old_value, new_value, notes) VALUES (?, ?, ?, ?, ?)",
            (entity_id, "delete", json.dumps(old_entity), None, request.notes)
        )
        conn.commit()
        conn.close()
        return {"status": "deleted", "entity_id": entity_id}

    # Handle merge
    if request.merge_into:
        cursor.execute("SELECT id FROM entities WHERE id = ?", (request.merge_into,))
        target = cursor.fetchone()
        if not target:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Merge target {request.merge_into} not found")

        # Update all relationships to point to the new entity
        cursor.execute(
            "UPDATE relationships SET source_id = ? WHERE source_id = ?",
            (request.merge_into, entity_id)
        )
        cursor.execute(
            "UPDATE relationships SET target_id = ? WHERE target_id = ?",
            (request.merge_into, entity_id)
        )
        # Update entity_sources
        cursor.execute(
            "UPDATE OR IGNORE entity_sources SET entity_id = ? WHERE entity_id = ?",
            (request.merge_into, entity_id)
        )
        # Delete the old entity
        cursor.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
        cursor.execute(
            "INSERT INTO entity_edits (entity_id, edit_type, old_value, new_value, notes) VALUES (?, ?, ?, ?, ?)",
            (entity_id, "merge", json.dumps(old_entity), request.merge_into, request.notes)
        )
        conn.commit()
        conn.close()
        return {"status": "merged", "entity_id": entity_id, "merged_into": request.merge_into}

    # Handle type update
    if request.entity_type:
        if request.entity_type not in VALID_ENTITY_TYPES:
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"Invalid entity type. Must be one of: {', '.join(VALID_ENTITY_TYPES)}"
            )
        if request.entity_type != old_entity["type"]:
            cursor.execute(
                "UPDATE entities SET type = ? WHERE id = ?",
                (request.entity_type, entity_id)
            )
            cursor.execute(
                "INSERT INTO entity_edits (entity_id, edit_type, old_value, new_value, notes) VALUES (?, ?, ?, ?, ?)",
                (entity_id, "type_change", old_entity["type"], request.entity_type, request.notes)
            )
            changes.append(f"type: {old_entity['type']} -> {request.entity_type}")

    # Handle text update
    if request.text and request.text != old_entity["text"]:
        cursor.execute(
            "UPDATE entities SET text = ? WHERE id = ?",
            (request.text, entity_id)
        )
        cursor.execute(
            "INSERT INTO entity_edits (entity_id, edit_type, old_value, new_value, notes) VALUES (?, ?, ?, ?, ?)",
            (entity_id, "text_change", old_entity["text"], request.text, request.notes)
        )
        changes.append(f"text: {old_entity['text']} -> {request.text}")

    # Handle aliases update
    if request.aliases is not None:
        new_aliases = json.dumps(request.aliases)
        cursor.execute(
            "UPDATE entities SET aliases = ? WHERE id = ?",
            (new_aliases, entity_id)
        )
        cursor.execute(
            "INSERT INTO entity_edits (entity_id, edit_type, old_value, new_value, notes) VALUES (?, ?, ?, ?, ?)",
            (entity_id, "aliases_change", old_entity["aliases"], new_aliases, request.notes)
        )
        changes.append("aliases updated")

    # Mark as human-edited in properties
    props = json.loads(old_entity["properties"]) if old_entity["properties"] else {}
    props["human_edited"] = True
    props["last_edit"] = datetime.now().isoformat()
    cursor.execute(
        "UPDATE entities SET properties = ? WHERE id = ?",
        (json.dumps(props), entity_id)
    )

    conn.commit()

    # Get updated entity
    cursor.execute("SELECT * FROM entities WHERE id = ?", (entity_id,))
    updated = dict(cursor.fetchone())
    conn.close()

    return {
        "status": "updated",
        "entity_id": entity_id,
        "changes": changes,
        "entity": updated,
    }


@router.get("/entities/{entity_id}/edits", operation_id="get_entity_edits")
async def get_entity_edits(entity_id: str) -> Dict[str, Any]:
    """Get edit history for an entity."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return {"edits": [], "entity_id": entity_id}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM entity_edits
        WHERE entity_id = ?
        ORDER BY edited_at DESC
    """, (entity_id,))

    edits = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {"edits": edits, "entity_id": entity_id, "count": len(edits)}


@router.get("/edits/recent", operation_id="get_recent_edits")
async def get_recent_edits(limit: int = Query(50, ge=1, le=200)) -> Dict[str, Any]:
    """Get recent entity edits across all entities."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return {"edits": [], "total": 0}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT ee.*, e.text as entity_text
        FROM entity_edits ee
        LEFT JOIN entities e ON ee.entity_id = e.id
        ORDER BY ee.edited_at DESC
        LIMIT ?
    """, (limit,))

    edits = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {"edits": edits, "count": len(edits)}


@router.get("/entities/{entity_id}/neighbors", operation_id="get_entity_neighbors")
async def get_entity_neighbors(
    entity_id: str,
    relation_type: Optional[str] = Query(None),
    depth: int = Query(1, ge=1, le=3),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get neighboring entities."""
    return graph.get_neighbors(entity_id, relation_type, depth)


@router.post("/search", operation_id="search_kg_entities")
async def search_entities(
    request: SearchEntitiesRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Search entities by text."""
    results = graph.search_entities(request.query, request.entity_type, request.limit)
    return {"query": request.query, "results": results, "count": len(results)}


@router.post("/paths", operation_id="find_kg_paths")
async def find_paths(
    request: PathsRequest,
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Find paths between two entities."""
    paths = graph.find_paths(request.source_id, request.target_id, request.max_length)

    # Resolve entity names for readability
    resolved_paths = []
    for path in paths:
        resolved = []
        for node_id in path:
            entity = graph.entities.get(node_id)
            resolved.append({
                "id": node_id,
                "text": entity.text if entity else "Unknown",
                "type": entity.type if entity else "unknown",
            })
        resolved_paths.append(resolved)

    return {
        "source_id": request.source_id,
        "target_id": request.target_id,
        "paths_found": len(paths),
        "paths": resolved_paths,
    }


@router.get("/stats", operation_id="get_kg_stats")
async def get_stats(
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get knowledge graph statistics."""
    return graph.get_stats()


@router.get("/types", operation_id="get_entity_types")
async def get_entity_types() -> Dict[str, Any]:
    """Get all entity types with counts from the local SQLite database."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return {"types": [], "total_entities": 0}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT type, COUNT(*) as count
        FROM entities
        GROUP BY type
        ORDER BY count DESC
    """)
    types = [{"type": row[0], "count": row[1]} for row in cursor.fetchall()]

    cursor.execute("SELECT COUNT(*) FROM entities")
    total = cursor.fetchone()[0]

    conn.close()
    return {"types": types, "total_entities": total}


@router.get("/entities/by-type/{entity_type}", operation_id="get_entities_by_type")
async def get_entities_by_type(
    entity_type: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("mentions", regex="^(mentions|text|first_seen)$"),
) -> Dict[str, Any]:
    """Get entities of a specific type with pagination."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return {"entities": [], "total": 0, "type": entity_type}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM entities WHERE type = ?", (entity_type,))
    total = cursor.fetchone()[0]

    # Get entities with sorting
    order = "mentions DESC" if sort_by == "mentions" else f"{sort_by} ASC"
    cursor.execute(f"""
        SELECT id, text, type, aliases, properties, mentions, first_seen, last_seen
        FROM entities
        WHERE type = ?
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, (entity_type, limit, offset))

    entities = []
    for row in cursor.fetchall():
        entities.append({
            "id": row["id"],
            "text": row["text"],
            "type": row["type"],
            "aliases": json.loads(row["aliases"]) if row["aliases"] else [],
            "properties": json.loads(row["properties"]) if row["properties"] else {},
            "mentions": row["mentions"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
        })

    conn.close()
    return {
        "entities": entities,
        "total": total,
        "type": entity_type,
        "limit": limit,
        "offset": offset,
    }


@router.get("/entities/{entity_id}/sources", operation_id="get_entity_sources")
async def get_entity_sources(entity_id: str) -> Dict[str, Any]:
    """Get source documents/chunks for an entity."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    if not db_path.exists():
        return {"sources": [], "entity_id": entity_id}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.chunk_id, c.doc_id, c.level, c.text, c.metadata
        FROM entity_sources es
        JOIN chunks c ON es.chunk_id = c.chunk_id
        WHERE es.entity_id = ?
        LIMIT 20
    """, (entity_id,))

    sources = []
    for row in cursor.fetchall():
        sources.append({
            "chunk_id": row["chunk_id"],
            "doc_id": row["doc_id"],
            "level": row["level"],
            "text_preview": row["text"][:500] if row["text"] else "",
            "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
        })

    conn.close()
    return {"sources": sources, "entity_id": entity_id, "count": len(sources)}


@router.get("/central", operation_id="get_central_entities")
async def get_central_entities(
    top_n: int = Query(10, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Get most central entities by PageRank."""
    entities = graph.get_central_entities(top_n)
    return {"entities": entities}


@router.get("/communities", operation_id="get_kg_communities")
async def get_communities(
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Detect communities in the graph."""
    communities = graph.get_communities()

    # Resolve entity info
    resolved = []
    for community in communities:
        members = []
        for node_id in community:
            entity = graph.entities.get(node_id)
            if entity:
                members.append({"id": node_id, "text": entity.text, "type": entity.type})
        if members:
            resolved.append(members)

    return {"communities": resolved, "count": len(resolved)}


@router.get("/export", operation_id="export_kg")
async def export_graph(
    format: str = Query("json", regex="^(json|cytoscape)$"),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Export the knowledge graph."""
    if format == "cytoscape":
        return graph.to_cytoscape()
    return graph.to_dict()


@router.get("/relationships", operation_id="list_kg_relationships")
async def list_relationships(
    relation_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """List relationships in the graph."""
    relationships = []
    for rel in graph.relationships.values():
        if relation_type and rel.relation_type != relation_type:
            continue

        source = graph.entities.get(rel.source_id)
        target = graph.entities.get(rel.target_id)

        relationships.append({
            "source": {"id": rel.source_id, "text": source.text if source else "Unknown"},
            "target": {"id": rel.target_id, "text": target.text if target else "Unknown"},
            "relation": rel.relation_type,
            "weight": rel.weight,
        })

        if len(relationships) >= limit:
            break

    return {"total": len(graph.relationships), "returned": len(relationships), "relationships": relationships}


# =============================================================================
# Process stored data into knowledge graph
# =============================================================================

@router.post("/ingest/search", operation_id="ingest_search_results")
async def ingest_search_results(
    limit: int = Query(10, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest recent search results into knowledge graph."""
    store = get_turso_store()
    conn = store._get_conn()

    results = conn.execute(
        "SELECT * FROM search_requests ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    desc = conn.execute("SELECT * FROM search_requests LIMIT 0").description
    processed = 0
    total_entities = 0

    for row in results:
        search = store._row_to_dict(row, desc)
        search_results = search.get("results") or []

        for result in search_results:
            text = f"{result.get('title', '')} {result.get('snippet', '')} {result.get('content', '')}"
            if text.strip():
                r = graph.process_document(
                    text=text,
                    doc_id=f"search_{search['id']}_{result.get('url', '')}",
                    metadata={"source_type": "search", "source_id": str(search["id"])},
                )
                total_entities += r["entities_found"]
                processed += 1

    return {"documents_processed": processed, "total_entities": total_entities}


@router.post("/ingest/extract", operation_id="ingest_extract_results")
async def ingest_extract_results(
    limit: int = Query(10, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest recent extract results into knowledge graph."""
    store = get_turso_store()
    conn = store._get_conn()

    results = conn.execute(
        "SELECT * FROM extract_requests ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    desc = conn.execute("SELECT * FROM extract_requests LIMIT 0").description
    processed = 0
    total_entities = 0

    for row in results:
        extract = store._row_to_dict(row, desc)
        extract_results = extract.get("results") or []

        for result in extract_results:
            content = result.get("content", "") or result.get("text", "")
            if content:
                r = graph.process_document(
                    text=content[:10000],  # Limit size
                    doc_id=f"extract_{extract['id']}_{result.get('url', '')}",
                    metadata={"source_type": "extract", "source_id": str(extract["id"])},
                )
                total_entities += r["entities_found"]
                processed += 1

    return {"documents_processed": processed, "total_entities": total_entities}


@router.post("/ingest/chat", operation_id="ingest_chat_results")
async def ingest_chat_results(
    limit: int = Query(10, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest recent chat completions into knowledge graph."""
    store = get_turso_store()
    conn = store._get_conn()

    results = conn.execute(
        "SELECT * FROM chat_completions ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    desc = conn.execute("SELECT * FROM chat_completions LIMIT 0").description
    processed = 0
    total_entities = 0

    for row in results:
        chat = store._row_to_dict(row, desc)
        response = chat.get("response", "")
        messages = chat.get("messages") or []

        # Process response
        if response:
            r = graph.process_document(
                text=response,
                doc_id=f"chat_{chat['id']}_response",
                metadata={"source_type": "chat", "source_id": str(chat["id"])},
            )
            total_entities += r["entities_found"]
            processed += 1

        # Process user messages
        for msg in messages:
            if msg.get("role") == "user" and msg.get("content"):
                r = graph.process_document(
                    text=msg["content"],
                    doc_id=f"chat_{chat['id']}_user",
                    metadata={"source_type": "chat", "source_id": str(chat["id"])},
                )
                total_entities += r["entities_found"]
                processed += 1

    return {"documents_processed": processed, "total_entities": total_entities}


@router.post("/ingest/all", operation_id="ingest_all_data")
async def ingest_all_data(
    limit_per_type: int = Query(20, ge=1, le=100),
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
) -> Dict[str, Any]:
    """Ingest all stored data into knowledge graph."""
    results = {
        "search": await ingest_search_results(limit=limit_per_type, graph=graph),
        "extract": await ingest_extract_results(limit=limit_per_type, graph=graph),
        "chat": await ingest_chat_results(limit=limit_per_type, graph=graph),
    }

    total_docs = sum(r["documents_processed"] for r in results.values())
    total_entities = sum(r["total_entities"] for r in results.values())

    return {
        "total_documents_processed": total_docs,
        "total_entities_extracted": total_entities,
        "by_source": results,
        "graph_stats": graph.get_stats(),
    }


# =============================================================================
# HTML Browser UI
# =============================================================================

browser_router = APIRouter(prefix="/browser/kg", tags=["Browser UI"])


# Entity type colors for consistent styling
TYPE_COLORS = {
    "person": "#f472b6",
    "organization": "#a78bfa",
    "location": "#34d399",
    "product": "#fbbf24",
    "event": "#f87171",
    "technology": "#60a5fa",
    "concept": "#818cf8",
    "system": "#f59e0b",
    "document": "#10b981",
    "algorithm": "#ec4899",
    "data_structure": "#8b5cf6",
    "protocol": "#14b8a6",
    "framework": "#f97316",
    "api": "#06b6d4",
    "standard": "#84cc16",
    "language": "#eab308",
}


def _kg_base_html(title: str, content: str) -> str:
    """Base HTML template for KG browser."""
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Knowledge Graph</title>
    <script src="https://unpkg.com/cytoscape@3.28.1/dist/cytoscape.min.js"></script>
    <style>
        :root {{
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-tertiary: #334155;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --accent: #3b82f6;
            --person: #f472b6;
            --org: #a78bfa;
            --location: #34d399;
            --product: #fbbf24;
            --event: #f87171;
            --other: #94a3b8;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
        }}
        .header {{
            background: var(--bg-secondary);
            padding: 15px 20px;
            border-bottom: 1px solid var(--bg-tertiary);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{ font-size: 1.25rem; }}
        .header a {{ color: var(--accent); text-decoration: none; }}
        .container {{ display: flex; height: calc(100vh - 60px); }}
        .sidebar {{
            width: 350px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--bg-tertiary);
            overflow-y: auto;
            padding: 15px;
        }}
        .graph-container {{
            flex: 1;
            position: relative;
        }}
        #cy {{
            width: 100%;
            height: 100%;
        }}
        .stats {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 20px;
        }}
        .stat {{
            background: var(--bg-tertiary);
            padding: 12px;
            border-radius: 8px;
        }}
        .stat-label {{ font-size: 0.75rem; color: var(--text-secondary); }}
        .stat-value {{ font-size: 1.25rem; font-weight: 600; }}
        .entity-list {{ margin-top: 15px; }}
        .entity-item {{
            background: var(--bg-tertiary);
            padding: 10px;
            border-radius: 6px;
            margin-bottom: 8px;
            cursor: pointer;
            transition: background 0.2s;
        }}
        .entity-item:hover {{ background: var(--accent); }}
        .entity-type {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.7rem;
            margin-left: 8px;
        }}
        .type-person {{ background: var(--person); color: #000; }}
        .type-organization {{ background: var(--org); color: #000; }}
        .type-location {{ background: var(--location); color: #000; }}
        .type-product {{ background: var(--product); color: #000; }}
        .type-event {{ background: var(--event); color: #fff; }}
        .type-other {{ background: var(--other); color: #000; }}
        .btn {{
            padding: 8px 16px;
            border-radius: 6px;
            border: none;
            cursor: pointer;
            font-size: 0.875rem;
            background: var(--accent);
            color: white;
            margin-right: 8px;
        }}
        .btn:hover {{ opacity: 0.9; }}
        .btn-secondary {{ background: var(--bg-tertiary); }}
        .controls {{ margin-bottom: 15px; }}
        .search-box {{
            width: 100%;
            padding: 10px;
            border-radius: 6px;
            border: 1px solid var(--bg-tertiary);
            background: var(--bg-primary);
            color: var(--text-primary);
            margin-bottom: 10px;
        }}
        .detail-panel {{
            position: absolute;
            top: 20px;
            right: 20px;
            width: 300px;
            background: var(--bg-secondary);
            border-radius: 10px;
            padding: 15px;
            display: none;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        }}
        .detail-panel.active {{ display: block; }}
        .detail-panel h3 {{ margin-bottom: 10px; }}
        .detail-row {{ margin-bottom: 8px; }}
        .detail-label {{ color: var(--text-secondary); font-size: 0.75rem; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Knowledge Graph Explorer</h1>
        <nav>
            <a href="/browser/kg">Graph View</a> |
            <a href="/browser/kg/types">Browse by Type</a> |
            <a href="/browser">Data Browser</a> |
            <a href="/docs">API Docs</a>
        </nav>
    </div>
    {content}
</body>
</html>'''


@browser_router.get("", response_class=HTMLResponse, include_in_schema=False)
async def kg_browser_home(
    graph: KnowledgeGraph = Depends(get_knowledge_graph),
):
    """Knowledge graph browser home."""
    import sqlite3
    from pathlib import Path

    stats = graph.get_stats()
    central = graph.get_central_entities(10)
    cytoscape_data = json.dumps(graph.to_cytoscape())

    # Get entity types from SQLite for filter buttons
    type_filters = []
    db_path = Path("knowledge_graph.db")
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT type, COUNT(*) as count
            FROM entities
            GROUP BY type
            ORDER BY count DESC
            LIMIT 12
        """)
        type_filters = [(row[0], row[1]) for row in cursor.fetchall()]
        conn.close()

    # Build type filter buttons
    type_buttons_html = '<button class="type-filter-btn active" onclick="filterByType(null)">All</button>'
    for entity_type, count in type_filters:
        color = TYPE_COLORS.get(entity_type, "#94a3b8")
        type_buttons_html += f'''<button class="type-filter-btn" style="--btn-color: {color};" onclick="filterByType('{entity_type}')">{entity_type.replace("_", " ").title()} ({count})</button>'''

    entity_html = ""
    for entity in central:
        type_class = f"type-{entity['type']}" if entity['type'] in ['person', 'organization', 'location', 'product', 'event'] else 'type-other'
        entity_html += f'''
        <div class="entity-item" onclick="focusNode('{entity["id"]}')">
            {entity["text"]}
            <span class="entity-type {type_class}">{entity["type"]}</span>
            <span style="float:right; color: var(--text-secondary); font-size: 0.75rem;">{entity["mentions"]} mentions</span>
        </div>'''

    content = f'''
    <div class="container">
        <div class="sidebar">
            <div class="stats">
                <div class="stat">
                    <div class="stat-label">Entities</div>
                    <div class="stat-value">{stats["total_entities"]}</div>
                </div>
                <div class="stat">
                    <div class="stat-label">Relationships</div>
                    <div class="stat-value">{stats["total_relationships"]}</div>
                </div>
                <div class="stat">
                    <div class="stat-label">Documents</div>
                    <div class="stat-value">{stats["documents_processed"]}</div>
                </div>
                <div class="stat">
                    <div class="stat-label">Components</div>
                    <div class="stat-value">{stats["connected_components"]}</div>
                </div>
            </div>

            <div class="controls">
                <input type="text" class="search-box" placeholder="Search entities..." onkeyup="searchEntities(this.value)">
                <button class="btn" onclick="ingestAll()">Ingest All Data</button>
                <button class="btn btn-secondary" onclick="resetView()">Reset View</button>
            </div>

            <div class="type-filters">
                <h4>Filter by Type</h4>
                <div class="type-filter-grid">
                    {type_buttons_html}
                </div>
            </div>

            <h3>Top Entities (PageRank)</h3>
            <div class="entity-list" id="entity-list">
                {entity_html if entity_html else '<p style="color: var(--text-secondary);">No entities yet. Click "Ingest All Data" to process stored results.</p>'}
            </div>

            <div class="data-source-note">
                <small>Data from: knowledge_graph.db</small>
            </div>
        </div>

        <div class="graph-container">
            <div id="cy"></div>
            <div class="detail-panel" id="detail-panel">
                <h3 id="detail-title">Entity Details</h3>
                <div id="detail-content"></div>
            </div>
        </div>
    </div>

    <script>
        const graphData = {cytoscape_data};

        const cy = cytoscape({{
            container: document.getElementById('cy'),
            elements: graphData,
            style: [
                {{
                    selector: 'node',
                    style: {{
                        'label': 'data(label)',
                        'background-color': function(ele) {{
                            const type = ele.data('type');
                            const colors = {{
                                'person': '#f472b6',
                                'organization': '#a78bfa',
                                'location': '#34d399',
                                'product': '#fbbf24',
                                'event': '#f87171',
                            }};
                            return colors[type] || '#94a3b8';
                        }},
                        'width': function(ele) {{ return Math.max(20, Math.min(60, ele.data('mentions') * 5)); }},
                        'height': function(ele) {{ return Math.max(20, Math.min(60, ele.data('mentions') * 5)); }},
                        'color': '#f1f5f9',
                        'font-size': '10px',
                        'text-valign': 'bottom',
                        'text-margin-y': '5px',
                    }}
                }},
                {{
                    selector: 'edge',
                    style: {{
                        'width': function(ele) {{ return Math.max(1, Math.min(5, ele.data('weight'))); }},
                        'line-color': '#475569',
                        'curve-style': 'bezier',
                        'opacity': 0.6,
                    }}
                }},
                {{
                    selector: ':selected',
                    style: {{
                        'border-width': 3,
                        'border-color': '#3b82f6',
                    }}
                }}
            ],
            layout: {{
                name: 'cose',
                animate: false,
                nodeRepulsion: 8000,
                idealEdgeLength: 100,
            }}
        }});

        cy.on('tap', 'node', function(evt) {{
            const node = evt.target;
            showDetail(node.data());
        }});

        function showDetail(data) {{
            const panel = document.getElementById('detail-panel');
            document.getElementById('detail-title').textContent = data.label;
            document.getElementById('detail-content').innerHTML = `
                <div class="detail-row">
                    <div class="detail-label">Type</div>
                    <div>${{data.type}}</div>
                </div>
                <div class="detail-row">
                    <div class="detail-label">Mentions</div>
                    <div>${{data.mentions}}</div>
                </div>
                <div class="detail-row">
                    <div class="detail-label">ID</div>
                    <div style="font-size: 0.75rem; word-break: break-all;">${{data.id}}</div>
                </div>
                <div id="sources-container">
                    <div class="detail-label" style="margin-top: 12px;">Loading sources...</div>
                </div>
            `;
            panel.classList.add('active');
            // Load entity sources asynchronously
            loadEntitySources(data.id);
        }}

        function focusNode(nodeId) {{
            const node = cy.getElementById(nodeId);
            if (node.length) {{
                cy.animate({{
                    fit: {{ eles: node.neighborhood().add(node), padding: 50 }},
                    duration: 500
                }});
                node.select();
                showDetail(node.data());
            }}
        }}

        function resetView() {{
            cy.fit();
            document.getElementById('detail-panel').classList.remove('active');
        }}

        async function ingestAll() {{
            const btn = event.target;
            btn.textContent = 'Processing...';
            btn.disabled = true;
            try {{
                const response = await fetch('/v1/kg/ingest/all?limit_per_type=50', {{ method: 'POST' }});
                const data = await response.json();
                alert(`Processed ${{data.total_documents_processed}} documents, extracted ${{data.total_entities_extracted}} entities`);
                location.reload();
            }} catch (e) {{
                alert('Error: ' + e.message);
            }}
            btn.textContent = 'Ingest All Data';
            btn.disabled = false;
        }}

        async function searchEntities(query) {{
            if (query.length < 2) return;
            try {{
                const response = await fetch('/v1/kg/search', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ query: query, limit: 10 }})
                }});
                const data = await response.json();
                const list = document.getElementById('entity-list');
                if (data.results.length === 0) {{
                    list.innerHTML = '<p style="color: var(--text-secondary);">No matches found</p>';
                    return;
                }}
                list.innerHTML = data.results.map(e => `
                    <div class="entity-item" onclick="focusNode('${{e.id}}')">
                        ${{e.text}}
                        <span class="entity-type type-${{e.type}}">${{e.type}}</span>
                    </div>
                `).join('');
            }} catch (e) {{
                console.error(e);
            }}
        }}

        // Filter graph by entity type
        function filterByType(entityType) {{
            // Update button states
            document.querySelectorAll('.type-filter-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');

            if (entityType === null) {{
                // Show all nodes
                cy.nodes().show();
                cy.edges().show();
            }} else {{
                // Hide nodes that don't match the type
                cy.nodes().forEach(node => {{
                    if (node.data('type') === entityType) {{
                        node.show();
                    }} else {{
                        node.hide();
                    }}
                }});
                // Show edges only between visible nodes
                cy.edges().forEach(edge => {{
                    const source = cy.getElementById(edge.data('source'));
                    const target = cy.getElementById(edge.data('target'));
                    if (source.visible() && target.visible()) {{
                        edge.show();
                    }} else {{
                        edge.hide();
                    }}
                }});
            }}
            cy.fit(cy.elements(':visible'), 50);
        }}

        // Async fetch entity sources for detail panel
        async function loadEntitySources(entityId) {{
            const container = document.getElementById('sources-container');
            try {{
                const response = await fetch(`/v1/kg/entities/${{entityId}}/sources`);
                const data = await response.json();
                if (data.sources && data.sources.length > 0) {{
                    const sourcesHtml = data.sources.slice(0, 3).map(s => `
                        <div style="background: var(--bg-tertiary); padding: 8px; border-radius: 6px; margin-top: 8px; font-size: 0.75rem;">
                            <strong>${{s.doc_id}}</strong> (${{s.level}})<br/>
                            <span style="color: var(--text-secondary);">${{s.text_preview.substring(0, 150)}}...</span>
                        </div>
                    `).join('');
                    container.innerHTML = `
                        <div class="detail-label" style="margin-top: 12px;">Sources (${{data.count}})</div>
                        ${{sourcesHtml}}
                    `;
                }} else {{
                    container.innerHTML = `<div class="detail-label" style="margin-top: 12px; color: var(--text-secondary);">No source documents linked</div>`;
                }}
            }} catch (e) {{
                container.innerHTML = `<div class="detail-label" style="margin-top: 12px; color: var(--text-secondary);">Could not load sources</div>`;
                console.error('Failed to load sources:', e);
            }}
        }}
    </script>

    <style>
        .type-filters {{
            margin: 15px 0;
            padding: 12px;
            background: var(--bg-tertiary);
            border-radius: 8px;
        }}
        .type-filters h4 {{
            margin: 0 0 10px 0;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }}
        .type-filter-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }}
        .type-filter-btn {{
            padding: 5px 10px;
            border-radius: 14px;
            border: 1px solid var(--bg-tertiary);
            background: var(--bg-secondary);
            color: var(--text-primary);
            font-size: 0.7rem;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .type-filter-btn:hover {{
            border-color: var(--btn-color, var(--accent));
            background: var(--btn-color, var(--accent));
            color: #000;
        }}
        .type-filter-btn.active {{
            background: var(--btn-color, var(--accent));
            border-color: var(--btn-color, var(--accent));
            color: #000;
            font-weight: 600;
        }}
        .data-source-note {{
            margin-top: 20px;
            padding: 10px;
            background: var(--bg-tertiary);
            border-radius: 6px;
            text-align: center;
        }}
        .data-source-note small {{
            color: var(--text-secondary);
            font-size: 0.7rem;
        }}
    </style>
    '''

    return HTMLResponse(_kg_base_html("Explorer", content))


@browser_router.get("/types", response_class=HTMLResponse, include_in_schema=False)
async def kg_types_browser():
    """Entity types browser - view all entity types with counts."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    types_data = []
    total_entities = 0

    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT type, COUNT(*) as count
            FROM entities
            GROUP BY type
            ORDER BY count DESC
        """)
        types_data = [(row[0], row[1]) for row in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) FROM entities")
        total_entities = cursor.fetchone()[0]
        conn.close()

    # Build type cards HTML
    type_cards = ""
    for entity_type, count in types_data:
        color = TYPE_COLORS.get(entity_type, "#94a3b8")
        percentage = (count / total_entities * 100) if total_entities > 0 else 0
        type_cards += f'''
        <a href="/browser/kg/types/{entity_type}" class="type-card" style="--type-color: {color};">
            <div class="type-icon" style="background: {color};">{entity_type[0].upper()}</div>
            <div class="type-info">
                <div class="type-name">{entity_type.replace("_", " ").title()}</div>
                <div class="type-count">{count:,} entities</div>
                <div class="type-bar" style="width: {percentage}%; background: {color};"></div>
            </div>
        </a>'''

    content = f'''
    <div class="types-container">
        <div class="types-header">
            <h2>Entity Types</h2>
            <div class="types-summary">
                <span class="total-badge">{total_entities:,} Total Entities</span>
                <span class="type-count-badge">{len(types_data)} Types</span>
            </div>
        </div>
        <p class="subtitle">Browse entities by class/type. Click a type to view all entities of that class.</p>

        <div class="type-grid">
            {type_cards if type_cards else '<p class="no-data">No entities in the knowledge graph yet.</p>'}
        </div>

        <div class="data-source-info">
            <h3>Data Sources</h3>
            <p>Entities are extracted from:</p>
            <ul>
                <li><strong>Local Corpus:</strong> Documents ingested via local_ingest.py from /Users/arthurcolle/parallel_research_v2</li>
                <li><strong>Entity Extraction:</strong> Uses OpenRouter (gpt-4.1-mini/nano) for structured entity extraction</li>
                <li><strong>Validation:</strong> Each entity is validated against source text for accuracy</li>
            </ul>
            <p class="source-path">Database: knowledge_graph.db</p>
        </div>
    </div>

    <style>
        .types-container {{ max-width: 1400px; margin: 0 auto; padding: 30px; }}
        .types-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}
        .types-header h2 {{ color: var(--text-primary); margin: 0; }}
        .subtitle {{ color: var(--text-secondary); margin-bottom: 25px; }}
        .types-summary {{ display: flex; gap: 12px; }}
        .total-badge, .type-count-badge {{
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 500;
        }}
        .total-badge {{ background: var(--accent); color: white; }}
        .type-count-badge {{ background: var(--bg-tertiary); color: var(--text-primary); }}
        .type-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 16px;
            margin-bottom: 40px;
        }}
        .type-card {{
            display: flex;
            align-items: center;
            padding: 16px;
            background: var(--bg-secondary);
            border-radius: 12px;
            text-decoration: none;
            color: var(--text-primary);
            border: 1px solid var(--bg-tertiary);
            transition: all 0.2s;
        }}
        .type-card:hover {{
            transform: translateY(-2px);
            border-color: var(--type-color);
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }}
        .type-icon {{
            width: 48px;
            height: 48px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 1.25rem;
            color: #000;
            margin-right: 14px;
            flex-shrink: 0;
        }}
        .type-info {{ flex: 1; min-width: 0; }}
        .type-name {{
            font-weight: 600;
            font-size: 1rem;
            margin-bottom: 4px;
        }}
        .type-count {{
            color: var(--text-secondary);
            font-size: 0.875rem;
            margin-bottom: 8px;
        }}
        .type-bar {{
            height: 4px;
            border-radius: 2px;
            max-width: 100%;
        }}
        .data-source-info {{
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 24px;
            border: 1px solid var(--bg-tertiary);
        }}
        .data-source-info h3 {{
            margin: 0 0 12px 0;
            color: var(--text-primary);
        }}
        .data-source-info p {{ color: var(--text-secondary); margin: 8px 0; }}
        .data-source-info ul {{
            margin: 12px 0;
            padding-left: 20px;
            color: var(--text-secondary);
        }}
        .data-source-info li {{ margin: 8px 0; }}
        .source-path {{
            font-family: monospace;
            font-size: 0.8rem;
            margin-top: 16px;
            padding: 8px 12px;
            background: var(--bg-tertiary);
            border-radius: 6px;
            display: inline-block;
        }}
        .no-data {{ color: var(--text-secondary); text-align: center; padding: 40px; }}
    </style>
    '''

    return HTMLResponse(_kg_base_html("Entity Types", content))


@browser_router.get("/types/{entity_type}", response_class=HTMLResponse, include_in_schema=False)
async def kg_type_detail(entity_type: str, page: int = Query(1, ge=1)):
    """View all entities of a specific type."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    entities = []
    total = 0
    per_page = 50
    offset = (page - 1) * per_page

    if db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM entities WHERE type = ?", (entity_type,))
        total = cursor.fetchone()[0]

        cursor.execute("""
            SELECT id, text, type, aliases, properties, mentions, first_seen
            FROM entities
            WHERE type = ?
            ORDER BY mentions DESC
            LIMIT ? OFFSET ?
        """, (entity_type, per_page, offset))

        for row in cursor.fetchall():
            props = json.loads(row["properties"]) if row["properties"] else {}
            entities.append({
                "id": row["id"],
                "text": row["text"],
                "mentions": row["mentions"],
                "first_seen": row["first_seen"],
                "validated": props.get("validated"),
                "validation_confidence": props.get("validation_confidence"),
            })
        conn.close()

    total_pages = (total + per_page - 1) // per_page
    color = TYPE_COLORS.get(entity_type, "#94a3b8")

    # Build entity rows
    entity_rows = ""
    for e in entities:
        validation_badge = ""
        if e["validated"] is True:
            validation_badge = '<span class="badge badge-valid">Validated</span>'
        elif e["validated"] is False:
            validation_badge = '<span class="badge badge-invalid">Invalid</span>'
        else:
            validation_badge = '<span class="badge badge-pending">Pending</span>'

        entity_rows += f'''
        <tr class="entity-row" onclick="showEntityDetail('{e["id"]}')">
            <td class="entity-name">{e["text"]}</td>
            <td class="entity-mentions">{e["mentions"]}</td>
            <td class="entity-status">{validation_badge}</td>
            <td class="entity-date">{e["first_seen"][:10] if e["first_seen"] else "N/A"}</td>
        </tr>'''

    # Build pagination
    pagination = ""
    if total_pages > 1:
        pages_html = ""
        for p in range(max(1, page - 2), min(total_pages + 1, page + 3)):
            active = "active" if p == page else ""
            pages_html += f'<a href="?page={p}" class="page-btn {active}">{p}</a>'

        pagination = f'''
        <div class="pagination">
            {"" if page == 1 else f'<a href="?page={page-1}" class="page-btn">Prev</a>'}
            {pages_html}
            {"" if page >= total_pages else f'<a href="?page={page+1}" class="page-btn">Next</a>'}
        </div>'''

    content = f'''
    <div class="type-detail-container">
        <div class="type-header" style="--type-color: {color};">
            <a href="/browser/kg/types" class="back-link">All Types</a>
            <div class="type-title">
                <div class="type-icon-large" style="background: {color};">{entity_type[0].upper()}</div>
                <div>
                    <h2>{entity_type.replace("_", " ").title()}</h2>
                    <p>{total:,} entities</p>
                </div>
            </div>
        </div>

        <div class="entity-table-container">
            <table class="entity-table">
                <thead>
                    <tr>
                        <th>Entity Name</th>
                        <th>Mentions</th>
                        <th>Status</th>
                        <th>First Seen</th>
                    </tr>
                </thead>
                <tbody>
                    {entity_rows if entity_rows else '<tr><td colspan="4" class="no-data">No entities found</td></tr>'}
                </tbody>
            </table>
        </div>

        {pagination}

        <div id="entity-detail-modal" class="modal">
            <div class="modal-content">
                <span class="modal-close" onclick="closeModal()">&times;</span>
                <div id="modal-body">Loading...</div>
            </div>
        </div>
    </div>

    <style>
        .type-detail-container {{ max-width: 1200px; margin: 0 auto; padding: 30px; }}
        .back-link {{
            color: var(--accent);
            text-decoration: none;
            font-size: 0.875rem;
            margin-bottom: 10px;
            display: inline-block;
        }}
        .back-link:hover {{ text-decoration: underline; }}
        .type-header {{
            margin-bottom: 24px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--bg-tertiary);
        }}
        .type-title {{
            display: flex;
            align-items: center;
            gap: 16px;
            margin-top: 10px;
        }}
        .type-icon-large {{
            width: 64px;
            height: 64px;
            border-radius: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 1.75rem;
            color: #000;
        }}
        .type-title h2 {{ margin: 0; color: var(--text-primary); }}
        .type-title p {{ margin: 4px 0 0 0; color: var(--text-secondary); }}
        .entity-table-container {{
            background: var(--bg-secondary);
            border-radius: 12px;
            border: 1px solid var(--bg-tertiary);
            overflow: hidden;
        }}
        .entity-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .entity-table th {{
            background: var(--bg-tertiary);
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .entity-table td {{
            padding: 14px 16px;
            border-bottom: 1px solid var(--bg-tertiary);
        }}
        .entity-row {{ cursor: pointer; transition: background 0.2s; }}
        .entity-row:hover {{ background: var(--bg-tertiary); }}
        .entity-name {{ font-weight: 500; }}
        .entity-mentions {{ color: var(--text-secondary); }}
        .entity-date {{ color: var(--text-secondary); font-size: 0.875rem; }}
        .badge {{
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .badge-valid {{ background: #10b981; color: #fff; }}
        .badge-invalid {{ background: #ef4444; color: #fff; }}
        .badge-pending {{ background: var(--bg-tertiary); color: var(--text-secondary); }}
        .pagination {{
            display: flex;
            justify-content: center;
            gap: 8px;
            margin-top: 20px;
        }}
        .page-btn {{
            padding: 8px 14px;
            border-radius: 6px;
            background: var(--bg-secondary);
            color: var(--text-primary);
            text-decoration: none;
            border: 1px solid var(--bg-tertiary);
            transition: all 0.2s;
        }}
        .page-btn:hover {{ background: var(--bg-tertiary); }}
        .page-btn.active {{ background: var(--accent); border-color: var(--accent); }}
        .modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .modal.active {{ display: flex; }}
        .modal-content {{
            background: var(--bg-secondary);
            border-radius: 16px;
            padding: 24px;
            max-width: 700px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
            position: relative;
        }}
        .modal-close {{
            position: absolute;
            top: 16px;
            right: 20px;
            font-size: 1.5rem;
            cursor: pointer;
            color: var(--text-secondary);
        }}
        .modal-close:hover {{ color: var(--text-primary); }}
        .no-data {{ text-align: center; color: var(--text-secondary); padding: 40px; }}
        .source-item {{
            background: var(--bg-tertiary);
            padding: 12px;
            border-radius: 8px;
            margin: 8px 0;
            font-size: 0.875rem;
        }}
        .source-item .doc-id {{
            font-weight: 600;
            color: var(--accent);
            margin-bottom: 4px;
        }}
        .source-item .text-preview {{
            color: var(--text-secondary);
            font-size: 0.8rem;
            margin-top: 6px;
            line-height: 1.4;
        }}
    </style>

    <script>
        async function showEntityDetail(entityId) {{
            const modal = document.getElementById('entity-detail-modal');
            const body = document.getElementById('modal-body');
            modal.classList.add('active');
            body.innerHTML = 'Loading...';

            try {{
                // Fetch entity details and sources
                const [entityRes, sourcesRes] = await Promise.all([
                    fetch(`/v1/kg/entities/${{entityId}}`),
                    fetch(`/v1/kg/entities/${{entityId}}/sources`)
                ]);

                const entity = await entityRes.json();
                const sources = await sourcesRes.json();

                let sourcesHtml = '<p style="color: var(--text-secondary);">No source information available</p>';
                if (sources.sources && sources.sources.length > 0) {{
                    sourcesHtml = sources.sources.map(s => `
                        <div class="source-item">
                            <div class="doc-id">${{s.doc_id}} (${{s.level}})</div>
                            <div class="text-preview">${{s.text_preview.substring(0, 300)}}...</div>
                        </div>
                    `).join('');
                }}

                body.innerHTML = `
                    <h3 style="margin: 0 0 20px 0;">${{entity.text || 'Unknown'}}</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 20px;">
                        <div class="detail-box">
                            <div class="label">Type</div>
                            <div class="value">${{entity.type || 'N/A'}}</div>
                        </div>
                        <div class="detail-box">
                            <div class="label">Mentions</div>
                            <div class="value">${{entity.mentions || 1}}</div>
                        </div>
                        <div class="detail-box">
                            <div class="label">First Seen</div>
                            <div class="value">${{entity.first_seen ? entity.first_seen.substring(0,10) : 'N/A'}}</div>
                        </div>
                        <div class="detail-box">
                            <div class="label">Aliases</div>
                            <div class="value">${{(entity.aliases && entity.aliases.length) ? entity.aliases.join(', ') : 'None'}}</div>
                        </div>
                    </div>
                    <h4 style="margin: 20px 0 12px 0; color: var(--text-secondary);">Source Documents (${{sources.count || 0}})</h4>
                    ${{sourcesHtml}}
                    <style>
                        .detail-box {{ background: var(--bg-tertiary); padding: 12px; border-radius: 8px; }}
                        .detail-box .label {{ font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 4px; }}
                        .detail-box .value {{ font-weight: 500; }}
                    </style>
                `;
            }} catch (e) {{
                body.innerHTML = `<p style="color: #ef4444;">Error loading entity: ${{e.message}}</p>`;
            }}
        }}

        function closeModal() {{
            document.getElementById('entity-detail-modal').classList.remove('active');
        }}

        document.addEventListener('keydown', function(e) {{
            if (e.key === 'Escape') closeModal();
        }});
    </script>
    '''

    return HTMLResponse(_kg_base_html(f"{entity_type.title()} Entities", content))


@browser_router.get("/edit", response_class=HTMLResponse, include_in_schema=False)
async def kg_entity_editor(search: str = Query("", description="Search query")):
    """Entity editor page for human review and corrections."""
    import sqlite3
    from pathlib import Path

    db_path = Path("knowledge_graph.db")
    entities = []
    recent_edits = []
    stats = {"total": 0, "human_edited": 0, "by_type": {}}

    if db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get stats
        cursor.execute("SELECT COUNT(*) FROM entities")
        stats["total"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM entities WHERE properties LIKE '%human_edited%'")
        stats["human_edited"] = cursor.fetchone()[0]

        cursor.execute("SELECT type, COUNT(*) as cnt FROM entities GROUP BY type ORDER BY cnt DESC")
        stats["by_type"] = {row["type"]: row["cnt"] for row in cursor.fetchall()}

        # Search or get recent entities needing review
        if search:
            cursor.execute("""
                SELECT id, text, type, mentions, properties
                FROM entities
                WHERE text LIKE ? OR type LIKE ?
                ORDER BY mentions DESC
                LIMIT 100
            """, (f"%{search}%", f"%{search}%"))
        else:
            # Get entities that might need review (high mentions but not human-edited)
            cursor.execute("""
                SELECT id, text, type, mentions, properties
                FROM entities
                WHERE properties NOT LIKE '%human_edited%'
                ORDER BY mentions DESC
                LIMIT 100
            """)

        for row in cursor.fetchall():
            props = json.loads(row["properties"]) if row["properties"] else {}
            entities.append({
                "id": row["id"],
                "text": row["text"],
                "type": row["type"],
                "mentions": row["mentions"],
                "human_edited": props.get("human_edited", False),
            })

        # Get recent edits
        cursor.execute("""
            SELECT ee.*, e.text as entity_text
            FROM entity_edits ee
            LEFT JOIN entities e ON ee.entity_id = e.id
            ORDER BY ee.edited_at DESC
            LIMIT 20
        """)
        recent_edits = [dict(row) for row in cursor.fetchall()]

        conn.close()

    # Build type options for dropdown
    type_options = "".join([f'<option value="{t}">{t.replace("_", " ").title()}</option>' for t in VALID_ENTITY_TYPES])

    # Build entity rows
    entity_rows = ""
    for e in entities:
        edited_badge = '<span class="badge badge-edited">Edited</span>' if e["human_edited"] else ''
        entity_rows += f'''
        <tr class="entity-row" data-id="{e["id"]}">
            <td class="entity-text">{e["text"]}</td>
            <td><span class="type-badge type-{e["type"]}">{e["type"]}</span></td>
            <td>{e["mentions"]}</td>
            <td>{edited_badge}</td>
            <td>
                <button class="btn-sm" onclick="openEditModal('{e["id"]}', '{e["text"]}', '{e["type"]}')">Edit</button>
                <button class="btn-sm btn-danger" onclick="deleteEntity('{e["id"]}')">Delete</button>
            </td>
        </tr>'''

    # Build recent edits list
    edits_html = ""
    for edit in recent_edits:
        edits_html += f'''
        <div class="edit-item">
            <span class="edit-type">{edit["edit_type"]}</span>
            <span class="edit-entity">{edit.get("entity_text", edit["entity_id"][:8])}</span>
            <span class="edit-change">{edit.get("old_value", "")[:30]} → {edit.get("new_value", "")[:30]}</span>
            <span class="edit-time">{edit["edited_at"]}</span>
        </div>'''

    content = f'''
    <div class="editor-container">
        <div class="editor-header">
            <h2>Entity Editor</h2>
            <p class="subtitle">Review and correct entity classifications. {stats["total"]:,} entities, {stats["human_edited"]:,} human-edited.</p>
        </div>

        <div class="editor-layout">
            <div class="editor-main">
                <div class="search-panel">
                    <form method="get" action="/browser/kg/edit">
                        <input type="text" name="search" class="search-input" placeholder="Search entities..." value="{search}">
                        <button type="submit" class="btn">Search</button>
                        <a href="/browser/kg/edit" class="btn btn-secondary">Reset</a>
                    </form>
                </div>

                <div class="entity-table-container">
                    <table class="entity-table">
                        <thead>
                            <tr>
                                <th>Entity Name</th>
                                <th>Type</th>
                                <th>Mentions</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {entity_rows if entity_rows else '<tr><td colspan="5" class="no-data">No entities found</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="editor-sidebar">
                <div class="sidebar-section">
                    <h3>Entity Types</h3>
                    <div class="type-stats">
                        {"".join([f'<div class="type-stat"><span class="type-badge type-{t}">{t}</span><span>{c:,}</span></div>' for t, c in list(stats["by_type"].items())[:12]])}
                    </div>
                </div>

                <div class="sidebar-section">
                    <h3>Recent Edits</h3>
                    <div class="edits-list">
                        {edits_html if edits_html else '<p class="no-data">No edits yet</p>'}
                    </div>
                </div>
            </div>
        </div>

        <!-- Edit Modal -->
        <div id="edit-modal" class="modal">
            <div class="modal-content">
                <span class="modal-close" onclick="closeEditModal()">&times;</span>
                <h3>Edit Entity</h3>
                <form id="edit-form" onsubmit="submitEdit(event)">
                    <input type="hidden" id="edit-id" name="entity_id">

                    <div class="form-group">
                        <label>Entity Name</label>
                        <input type="text" id="edit-text" name="text" class="form-input">
                    </div>

                    <div class="form-group">
                        <label>Entity Type</label>
                        <select id="edit-type" name="entity_type" class="form-select">
                            {type_options}
                        </select>
                    </div>

                    <div class="form-group">
                        <label>Notes (optional)</label>
                        <textarea id="edit-notes" name="notes" class="form-textarea" placeholder="Why are you making this change?"></textarea>
                    </div>

                    <div class="form-actions">
                        <button type="submit" class="btn">Save Changes</button>
                        <button type="button" class="btn btn-secondary" onclick="closeEditModal()">Cancel</button>
                    </div>
                </form>
            </div>
        </div>
    </div>

    <style>
        .editor-container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
        .editor-header {{ margin-bottom: 20px; }}
        .editor-header h2 {{ margin: 0; color: var(--text-primary); }}
        .subtitle {{ color: var(--text-secondary); margin: 8px 0 0 0; }}
        .editor-layout {{ display: grid; grid-template-columns: 1fr 320px; gap: 20px; }}
        .editor-main {{ min-width: 0; }}
        .editor-sidebar {{ display: flex; flex-direction: column; gap: 16px; }}
        .sidebar-section {{
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 16px;
            border: 1px solid var(--bg-tertiary);
        }}
        .sidebar-section h3 {{ margin: 0 0 12px 0; font-size: 0.9rem; color: var(--text-secondary); }}
        .search-panel {{
            display: flex;
            gap: 10px;
            margin-bottom: 16px;
        }}
        .search-input {{
            flex: 1;
            padding: 10px 14px;
            border-radius: 8px;
            border: 1px solid var(--bg-tertiary);
            background: var(--bg-secondary);
            color: var(--text-primary);
            font-size: 0.9rem;
        }}
        .entity-table-container {{
            background: var(--bg-secondary);
            border-radius: 12px;
            border: 1px solid var(--bg-tertiary);
            overflow: hidden;
        }}
        .entity-table {{ width: 100%; border-collapse: collapse; }}
        .entity-table th {{
            background: var(--bg-tertiary);
            padding: 12px 14px;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            font-size: 0.75rem;
            text-transform: uppercase;
        }}
        .entity-table td {{ padding: 12px 14px; border-bottom: 1px solid var(--bg-tertiary); }}
        .entity-row:hover {{ background: rgba(59, 130, 246, 0.1); }}
        .entity-text {{ font-weight: 500; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        .type-badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 10px;
            font-size: 0.7rem;
            font-weight: 500;
            background: var(--bg-tertiary);
        }}
        .type-concept {{ background: #818cf8; color: #000; }}
        .type-organization {{ background: #a78bfa; color: #000; }}
        .type-person {{ background: #f472b6; color: #000; }}
        .type-technology {{ background: #60a5fa; color: #000; }}
        .type-system {{ background: #f59e0b; color: #000; }}
        .type-algorithm {{ background: #ec4899; color: #000; }}
        .type-document {{ background: #10b981; color: #000; }}
        .type-location {{ background: #34d399; color: #000; }}
        .badge {{ padding: 3px 8px; border-radius: 10px; font-size: 0.7rem; }}
        .badge-edited {{ background: #3b82f6; color: #fff; }}
        .btn-sm {{
            padding: 4px 10px;
            border-radius: 6px;
            border: none;
            cursor: pointer;
            font-size: 0.75rem;
            background: var(--bg-tertiary);
            color: var(--text-primary);
            margin-right: 4px;
        }}
        .btn-sm:hover {{ background: var(--accent); }}
        .btn-danger {{ background: #dc2626; color: #fff; }}
        .btn-danger:hover {{ background: #b91c1c; }}
        .type-stats {{ display: flex; flex-direction: column; gap: 6px; }}
        .type-stat {{ display: flex; justify-content: space-between; align-items: center; }}
        .edits-list {{ display: flex; flex-direction: column; gap: 8px; max-height: 400px; overflow-y: auto; }}
        .edit-item {{
            background: var(--bg-tertiary);
            padding: 8px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            display: flex;
            flex-direction: column;
            gap: 2px;
        }}
        .edit-type {{ font-weight: 600; color: var(--accent); }}
        .edit-entity {{ color: var(--text-primary); }}
        .edit-change {{ color: var(--text-secondary); font-size: 0.7rem; }}
        .edit-time {{ color: var(--text-secondary); font-size: 0.65rem; }}
        .modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .modal.active {{ display: flex; }}
        .modal-content {{
            background: var(--bg-secondary);
            border-radius: 16px;
            padding: 24px;
            width: 500px;
            max-width: 90%;
            position: relative;
        }}
        .modal-close {{
            position: absolute;
            top: 16px;
            right: 20px;
            font-size: 1.5rem;
            cursor: pointer;
            color: var(--text-secondary);
        }}
        .modal-close:hover {{ color: var(--text-primary); }}
        .form-group {{ margin-bottom: 16px; }}
        .form-group label {{ display: block; margin-bottom: 6px; color: var(--text-secondary); font-size: 0.85rem; }}
        .form-input, .form-select, .form-textarea {{
            width: 100%;
            padding: 10px 12px;
            border-radius: 8px;
            border: 1px solid var(--bg-tertiary);
            background: var(--bg-primary);
            color: var(--text-primary);
            font-size: 0.9rem;
        }}
        .form-textarea {{ min-height: 80px; resize: vertical; }}
        .form-actions {{ display: flex; gap: 10px; margin-top: 20px; }}
        .no-data {{ text-align: center; color: var(--text-secondary); padding: 20px; }}
    </style>

    <script>
        function openEditModal(id, text, type) {{
            document.getElementById('edit-id').value = id;
            document.getElementById('edit-text').value = text;
            document.getElementById('edit-type').value = type;
            document.getElementById('edit-notes').value = '';
            document.getElementById('edit-modal').classList.add('active');
        }}

        function closeEditModal() {{
            document.getElementById('edit-modal').classList.remove('active');
        }}

        async function submitEdit(e) {{
            e.preventDefault();
            const id = document.getElementById('edit-id').value;
            const data = {{
                text: document.getElementById('edit-text').value,
                entity_type: document.getElementById('edit-type').value,
                notes: document.getElementById('edit-notes').value || null,
            }};

            try {{
                const response = await fetch(`/v1/kg/entities/${{id}}`, {{
                    method: 'PATCH',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(data)
                }});
                const result = await response.json();
                if (response.ok) {{
                    alert('Entity updated successfully!');
                    location.reload();
                }} else {{
                    alert('Error: ' + (result.detail || 'Unknown error'));
                }}
            }} catch (e) {{
                alert('Error: ' + e.message);
            }}
        }}

        async function deleteEntity(id) {{
            if (!confirm('Are you sure you want to delete this entity? This cannot be undone.')) return;

            try {{
                const response = await fetch(`/v1/kg/entities/${{id}}`, {{
                    method: 'PATCH',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ delete: true, notes: 'Manual deletion from editor' }})
                }});
                const result = await response.json();
                if (response.ok) {{
                    alert('Entity deleted');
                    location.reload();
                }} else {{
                    alert('Error: ' + (result.detail || 'Unknown error'));
                }}
            }} catch (e) {{
                alert('Error: ' + e.message);
            }}
        }}

        document.addEventListener('keydown', function(e) {{
            if (e.key === 'Escape') closeEditModal();
        }});
    </script>
    '''

    return HTMLResponse(_kg_base_html("Entity Editor", content))
