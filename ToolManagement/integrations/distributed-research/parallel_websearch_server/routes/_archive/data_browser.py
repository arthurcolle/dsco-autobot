"""Data browser routes for viewing stored API data."""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from parallel_websearch_server.turso_store import get_turso_store, TursoStore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/data", tags=["Data Browser"])


async def run_sync(func, *args, **kwargs):
    """Run a synchronous function in a thread pool to avoid blocking."""
    if kwargs:
        func = partial(func, **kwargs)
    return await asyncio.to_thread(func, *args)


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""
    items: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


def get_store() -> TursoStore:
    """Dependency to get Turso store."""
    return get_turso_store()


# =============================================================================
# API Endpoints
# =============================================================================

def _list_tasks_sync(store: TursoStore, page: int, page_size: int, status: Optional[str], processor: Optional[str]) -> dict:
    """Synchronous task listing - runs in thread pool."""
    try:
        conn = store._get_conn()
        offset = (page - 1) * page_size
        logger.info(f"_list_tasks_sync: page={page}, page_size={page_size}, offset={offset}, status={status}, processor={processor}")

        # Simple case - no filters
        if not status and not processor:
            logger.info("Executing simple query without filters")
            total = conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0]
            logger.info(f"Total count: {total}")
            # Convert to explicit int to ensure correct type
            params = (int(page_size), int(offset))
            logger.info(f"Query params: {params}, types: {[type(p) for p in params]}")
            results = conn.execute(
                "SELECT * FROM task_runs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                params
            ).fetchall()
            logger.info(f"Got {len(results)} results")
        else:
            # Build filtered query
            where_clauses = []
            params = []
            if status:
                where_clauses.append("status = ?")
                params.append(str(status))
            if processor:
                where_clauses.append("processor = ?")
                params.append(str(processor))
            where_sql = " AND ".join(where_clauses)

            total = conn.execute(
                f"SELECT COUNT(*) FROM task_runs WHERE {where_sql}", tuple(params)
            ).fetchone()[0]
            params.extend([int(page_size), int(offset)])
            results = conn.execute(
                f"SELECT * FROM task_runs WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                tuple(params)
            ).fetchall()

        desc = conn.execute("SELECT * FROM task_runs LIMIT 0").description
        items = [store._row_to_dict(row, desc) for row in results]
        total_pages = (total + page_size - 1) // page_size

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
        }
    except Exception as e:
        logger.error(f"_list_tasks_sync error: {e}", exc_info=True)
        raise


@router.get("/tasks", response_model=PaginatedResponse, operation_id="list_stored_tasks")
async def list_stored_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    processor: Optional[str] = Query(None),
    store: TursoStore = Depends(get_store),
) -> PaginatedResponse:
    """List stored task runs with pagination."""
    data = await asyncio.to_thread(_list_tasks_sync, store, page, page_size, status, processor)
    return PaginatedResponse(**data)


@router.get("/tasks/{run_id}", operation_id="get_stored_task")
async def get_stored_task(
    run_id: str,
    store: TursoStore = Depends(get_store),
) -> Dict[str, Any]:
    """Get a stored task run by ID."""
    task = await asyncio.to_thread(store.get_task_run, run_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task run {run_id} not found")
    return task


@router.get("/tasks/{run_id}/download", operation_id="download_task")
async def download_task(
    run_id: str,
    store: TursoStore = Depends(get_store),
) -> Response:
    """Download a task run as JSON."""
    task = await asyncio.to_thread(store.get_task_run, run_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task run {run_id} not found")

    content = json.dumps(task, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="task_{run_id}.json"'}
    )


def _list_findall_sync(store: TursoStore, page: int, page_size: int, status: Optional[str]) -> dict:
    """Synchronous findall listing - runs in thread pool."""
    conn = store._get_conn()
    offset = (page - 1) * page_size

    if not status:
        total = conn.execute("SELECT COUNT(*) FROM findall_runs").fetchone()[0]
        results = conn.execute(
            "SELECT * FROM findall_runs ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (page_size, offset)
        ).fetchall()
    else:
        total = conn.execute(
            "SELECT COUNT(*) FROM findall_runs WHERE status = ?", (status,)
        ).fetchone()[0]
        results = conn.execute(
            "SELECT * FROM findall_runs WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (status, page_size, offset)
        ).fetchall()

    desc = conn.execute("SELECT * FROM findall_runs LIMIT 0").description
    items = [store._row_to_dict(row, desc) for row in results]
    total_pages = (total + page_size - 1) // page_size

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }


@router.get("/findall", response_model=PaginatedResponse, operation_id="list_stored_findall")
async def list_stored_findall(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    store: TursoStore = Depends(get_store),
) -> PaginatedResponse:
    """List stored FindAll runs with pagination."""
    data = await asyncio.to_thread(_list_findall_sync, store, page, page_size, status)
    return PaginatedResponse(**data)


@router.get("/findall/{findall_id}", operation_id="get_stored_findall")
async def get_stored_findall(
    findall_id: str,
    store: TursoStore = Depends(get_store),
) -> Dict[str, Any]:
    """Get a stored FindAll run by ID."""
    run = await asyncio.to_thread(store.get_findall_run, findall_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"FindAll run {findall_id} not found")
    return run


@router.get("/findall/{findall_id}/download", operation_id="download_findall")
async def download_findall(
    findall_id: str,
    store: TursoStore = Depends(get_store),
) -> Response:
    """Download a FindAll run as JSON."""
    run = await asyncio.to_thread(store.get_findall_run, findall_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"FindAll run {findall_id} not found")

    content = json.dumps(run, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="findall_{findall_id}.json"'}
    )


def _list_chat_sync(store: TursoStore, page: int, page_size: int) -> dict:
    """Synchronous chat listing - runs in thread pool."""
    conn = store._get_conn()
    offset = (page - 1) * page_size

    total = conn.execute("SELECT COUNT(*) FROM chat_completions").fetchone()[0]
    results = conn.execute(
        "SELECT * FROM chat_completions ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (page_size, offset)
    ).fetchall()

    desc = conn.execute("SELECT * FROM chat_completions LIMIT 0").description
    items = [store._row_to_dict(row, desc) for row in results]
    total_pages = (total + page_size - 1) // page_size

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }


@router.get("/chat", response_model=PaginatedResponse, operation_id="list_stored_chat")
async def list_stored_chat(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    store: TursoStore = Depends(get_store),
) -> PaginatedResponse:
    """List stored chat completions with pagination."""
    data = await asyncio.to_thread(_list_chat_sync, store, page, page_size)
    return PaginatedResponse(**data)


@router.get("/chat/{completion_id}", operation_id="get_stored_chat")
async def get_stored_chat(
    completion_id: str,
    store: TursoStore = Depends(get_store),
) -> Dict[str, Any]:
    """Get a stored chat completion by ID."""
    chat = await asyncio.to_thread(store.get_chat_completion, completion_id)
    if not chat:
        raise HTTPException(status_code=404, detail=f"Chat completion {completion_id} not found")
    return chat


def _download_chat_sync(store: TursoStore, row_id: int) -> Optional[dict]:
    """Synchronous chat download - runs in thread pool."""
    conn = store._get_conn()
    result = conn.execute(
        "SELECT * FROM chat_completions WHERE id = ?", (row_id,)
    ).fetchone()
    if not result:
        return None
    desc = conn.execute("SELECT * FROM chat_completions LIMIT 0").description
    return store._row_to_dict(result, desc)


@router.get("/chat/{row_id}/download", operation_id="download_chat")
async def download_chat(
    row_id: int,
    store: TursoStore = Depends(get_store),
) -> Response:
    """Download a chat completion as JSON."""
    chat = await asyncio.to_thread(_download_chat_sync, store, row_id)
    if not chat:
        raise HTTPException(status_code=404, detail=f"Chat completion {row_id} not found")

    content = json.dumps(chat, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="chat_{row_id}.json"'}
    )


def _list_search_sync(store: TursoStore, page: int, page_size: int) -> dict:
    """Synchronous search listing - runs in thread pool."""
    conn = store._get_conn()
    offset = (page - 1) * page_size

    total = conn.execute("SELECT COUNT(*) FROM search_requests").fetchone()[0]
    results = conn.execute(
        "SELECT * FROM search_requests ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (page_size, offset)
    ).fetchall()

    desc = conn.execute("SELECT * FROM search_requests LIMIT 0").description
    items = [store._row_to_dict(row, desc) for row in results]
    total_pages = (total + page_size - 1) // page_size

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }


@router.get("/search", response_model=PaginatedResponse, operation_id="list_stored_search")
async def list_stored_search(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    store: TursoStore = Depends(get_store),
) -> PaginatedResponse:
    """List stored search requests with pagination."""
    data = await asyncio.to_thread(_list_search_sync, store, page, page_size)
    return PaginatedResponse(**data)


def _download_search_sync(store: TursoStore, row_id: int) -> Optional[dict]:
    """Synchronous search download - runs in thread pool."""
    conn = store._get_conn()
    result = conn.execute(
        "SELECT * FROM search_requests WHERE id = ?", (row_id,)
    ).fetchone()
    if not result:
        return None
    desc = conn.execute("SELECT * FROM search_requests LIMIT 0").description
    return store._row_to_dict(result, desc)


@router.get("/search/{row_id}/download", operation_id="download_search")
async def download_search(
    row_id: int,
    store: TursoStore = Depends(get_store),
) -> Response:
    """Download a search request as JSON."""
    search = await asyncio.to_thread(_download_search_sync, store, row_id)
    if not search:
        raise HTTPException(status_code=404, detail=f"Search request {row_id} not found")

    content = json.dumps(search, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="search_{row_id}.json"'}
    )


def _list_extract_sync(store: TursoStore, page: int, page_size: int) -> dict:
    """Synchronous extract listing - runs in thread pool."""
    conn = store._get_conn()
    offset = (page - 1) * page_size

    total = conn.execute("SELECT COUNT(*) FROM extract_requests").fetchone()[0]
    results = conn.execute(
        "SELECT * FROM extract_requests ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (page_size, offset)
    ).fetchall()

    desc = conn.execute("SELECT * FROM extract_requests LIMIT 0").description
    items = [store._row_to_dict(row, desc) for row in results]
    total_pages = (total + page_size - 1) // page_size

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }


@router.get("/extract", response_model=PaginatedResponse, operation_id="list_stored_extract")
async def list_stored_extract(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    store: TursoStore = Depends(get_store),
) -> PaginatedResponse:
    """List stored extract requests with pagination."""
    data = await asyncio.to_thread(_list_extract_sync, store, page, page_size)
    return PaginatedResponse(**data)


def _download_extract_sync(store: TursoStore, row_id: int) -> Optional[dict]:
    """Synchronous extract download - runs in thread pool."""
    conn = store._get_conn()
    result = conn.execute(
        "SELECT * FROM extract_requests WHERE id = ?", (row_id,)
    ).fetchone()
    if not result:
        return None
    desc = conn.execute("SELECT * FROM extract_requests LIMIT 0").description
    return store._row_to_dict(result, desc)


@router.get("/extract/{row_id}/download", operation_id="download_extract")
async def download_extract(
    row_id: int,
    store: TursoStore = Depends(get_store),
) -> Response:
    """Download an extract request as JSON."""
    extract = await asyncio.to_thread(_download_extract_sync, store, row_id)
    if not extract:
        raise HTTPException(status_code=404, detail=f"Extract request {row_id} not found")

    content = json.dumps(extract, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="extract_{row_id}.json"'}
    )


@router.get("/stats", operation_id="get_data_stats")
async def get_data_stats(
    store: TursoStore = Depends(get_store),
) -> Dict[str, Any]:
    """Get statistics about stored data."""
    return await asyncio.to_thread(store.get_stats)


@router.get("/search-all", operation_id="search_all_data")
async def search_all_data(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    store: TursoStore = Depends(get_store),
) -> Dict[str, List[Dict[str, Any]]]:
    """Search across all stored data."""
    return await asyncio.to_thread(store.search_all, q, limit)


# =============================================================================
# HTML Browser Pages
# =============================================================================

def _base_html(title: str, content: str, nav_active: str = "") -> str:
    """Generate base HTML template."""
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Parallel AI Data Browser</title>
    <style>
        :root {{
            --bg-primary: #ffffff;
            --bg-secondary: #f8fafc;
            --bg-tertiary: #f1f5f9;
            --text-primary: #1e293b;
            --text-secondary: #64748b;
            --accent: #3b82f6;
            --accent-hover: #2563eb;
            --success: #22c55e;
            --warning: #f59e0b;
            --error: #ef4444;
            --border: #e2e8f0;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        nav {{
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 15px 0;
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        nav .container {{
            display: flex;
            align-items: center;
            gap: 30px;
        }}
        nav .logo {{
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--accent);
            text-decoration: none;
        }}
        nav .nav-links {{ display: flex; gap: 5px; }}
        nav a {{
            color: var(--text-secondary);
            text-decoration: none;
            padding: 8px 16px;
            border-radius: 6px;
            transition: all 0.2s;
        }}
        nav a:hover, nav a.active {{
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }}
        h1 {{ margin-bottom: 20px; font-size: 1.75rem; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 20px;
        }}
        .stat-card h3 {{ color: var(--text-secondary); font-size: 0.875rem; margin-bottom: 5px; }}
        .stat-card .value {{ font-size: 2rem; font-weight: 700; }}
        .card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 10px;
            margin-bottom: 15px;
            overflow: hidden;
        }}
        .card-header {{
            background: var(--bg-tertiary);
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
        }}
        .card-header h3 {{ font-size: 1rem; }}
        .card-body {{ padding: 20px; }}
        .card-meta {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            margin-bottom: 15px;
        }}
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary);
            font-size: 0.875rem;
        }}
        .meta-item strong {{ color: var(--text-primary); }}
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .badge-completed {{ background: var(--success); color: #000; }}
        .badge-running {{ background: var(--warning); color: #000; }}
        .badge-queued {{ background: var(--accent); color: #fff; }}
        .badge-failed {{ background: var(--error); color: #fff; }}
        .content-preview {{
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 15px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.875rem;
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 300px;
            overflow-y: auto;
            color: var(--text-primary);
        }}
        .btn {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 0.875rem;
            font-weight: 500;
            text-decoration: none;
            border: none;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .btn-primary {{ background: var(--accent); color: #fff; }}
        .btn-primary:hover {{ background: var(--accent-hover); }}
        .btn-secondary {{ background: var(--bg-tertiary); color: var(--text-primary); }}
        .btn-secondary:hover {{ background: var(--border); }}
        .pagination {{
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            margin-top: 30px;
        }}
        .pagination a, .pagination span {{
            padding: 8px 16px;
            border-radius: 6px;
            text-decoration: none;
        }}
        .pagination a {{ background: var(--bg-secondary); color: var(--text-primary); }}
        .pagination a:hover {{ background: var(--bg-tertiary); }}
        .pagination .current {{ background: var(--accent); color: #fff; }}
        .empty-state {{
            text-align: center;
            padding: 60px 20px;
            color: var(--text-secondary);
        }}
        .empty-state h3 {{ margin-bottom: 10px; }}
        .search-box {{
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }}
        .search-box input {{
            flex: 1;
            padding: 10px 15px;
            border-radius: 6px;
            border: 1px solid var(--border);
            background: var(--bg-secondary);
            color: var(--text-primary);
            font-size: 1rem;
        }}
        .search-box input:focus {{ outline: none; border-color: var(--accent); }}
        .detail-section {{ margin-bottom: 25px; }}
        .detail-section h4 {{
            color: var(--text-secondary);
            font-size: 0.875rem;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .json-viewer {{
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.875rem;
            white-space: pre-wrap;
            word-break: break-word;
            overflow-x: auto;
        }}
        .json-key {{ color: #be185d; }}
        .json-string {{ color: #0369a1; }}
        .json-number {{ color: #b45309; }}
        .json-boolean {{ color: #7c3aed; }}
        .json-null {{ color: #64748b; }}
    </style>
</head>
<body>
    <nav>
        <div class="container">
            <a href="/browser" class="logo">⚡ Parallel AI Data</a>
            <div class="nav-links">
                <a href="/browser" class="{'active' if nav_active == 'home' else ''}">Overview</a>
                <a href="/browser/tasks" class="{'active' if nav_active == 'tasks' else ''}">Tasks</a>
                <a href="/browser/findall" class="{'active' if nav_active == 'findall' else ''}">FindAll</a>
                <a href="/browser/chat" class="{'active' if nav_active == 'chat' else ''}">Chat</a>
                <a href="/browser/search" class="{'active' if nav_active == 'search' else ''}">Search</a>
                <a href="/browser/extract" class="{'active' if nav_active == 'extract' else ''}">Extract</a>
            </div>
        </div>
    </nav>
    <main class="container">
        {content}
    </main>
    <script>
        function formatJson(obj) {{
            return JSON.stringify(obj, null, 2)
                .replace(/"([^"]+)":/g, '<span class="json-key">"$1"</span>:')
                .replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>')
                .replace(/: (\\d+)/g, ': <span class="json-number">$1</span>')
                .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
                .replace(/: null/g, ': <span class="json-null">null</span>');
        }}
    </script>
</body>
</html>'''


def _pagination_html(page: int, total_pages: int, base_url: str) -> str:
    """Generate pagination HTML."""
    if total_pages <= 1:
        return ""

    parts = ['<div class="pagination">']

    if page > 1:
        parts.append(f'<a href="{base_url}?page={page-1}">← Previous</a>')

    # Show page numbers
    start = max(1, page - 2)
    end = min(total_pages, page + 2)

    if start > 1:
        parts.append(f'<a href="{base_url}?page=1">1</a>')
        if start > 2:
            parts.append('<span>...</span>')

    for p in range(start, end + 1):
        if p == page:
            parts.append(f'<span class="current">{p}</span>')
        else:
            parts.append(f'<a href="{base_url}?page={p}">{p}</a>')

    if end < total_pages:
        if end < total_pages - 1:
            parts.append('<span>...</span>')
        parts.append(f'<a href="{base_url}?page={total_pages}">{total_pages}</a>')

    if page < total_pages:
        parts.append(f'<a href="{base_url}?page={page+1}">Next →</a>')

    parts.append('</div>')
    return ''.join(parts)


def _status_badge(status: str) -> str:
    """Generate status badge HTML."""
    badge_class = {
        'completed': 'badge-completed',
        'running': 'badge-running',
        'queued': 'badge-queued',
        'failed': 'badge-failed',
        'cancelled': 'badge-failed',
    }.get(status, 'badge-queued')
    return f'<span class="badge {badge_class}">{status}</span>'


def _truncate(text: str, length: int = 200) -> str:
    """Truncate text to specified length."""
    if not text:
        return ""
    if len(text) <= length:
        return text
    return text[:length] + "..."


# Browser routes
browser_router = APIRouter(prefix="/browser", tags=["Browser UI"])


@browser_router.get("", response_class=HTMLResponse, include_in_schema=False)
async def browser_home(store: TursoStore = Depends(get_store)):
    """Data browser home page with overview."""
    stats = await asyncio.to_thread(store.get_stats)

    stats_html = ""
    for table, data in stats.items():
        label = table.replace("_", " ").title()
        stats_html += f'''
        <div class="stat-card">
            <h3>{label}</h3>
            <div class="value">{data["count"]}</div>
        </div>'''

    content = f'''
    <h1>Data Browser</h1>
    <p style="color: var(--text-secondary); margin-bottom: 30px;">
        Browse and download all API requests and responses stored in your Turso database.
    </p>
    <div class="stats-grid">
        {stats_html}
    </div>
    <div class="card">
        <div class="card-header">
            <h3>Quick Links</h3>
        </div>
        <div class="card-body" style="display: flex; gap: 15px; flex-wrap: wrap;">
            <a href="/browser/tasks" class="btn btn-primary">View Tasks</a>
            <a href="/browser/findall" class="btn btn-primary">View FindAll Runs</a>
            <a href="/browser/chat" class="btn btn-primary">View Chat Completions</a>
            <a href="/browser/search" class="btn btn-primary">View Search Requests</a>
            <a href="/browser/extract" class="btn btn-primary">View Extract Requests</a>
        </div>
    </div>
    '''

    return HTMLResponse(_base_html("Overview", content, "home"))


@browser_router.get("/tasks", response_class=HTMLResponse, include_in_schema=False)
async def browser_tasks(
    page: int = Query(1, ge=1),
    store: TursoStore = Depends(get_store),
):
    """Browse task runs."""
    page_size = 10
    # Call sync helper directly to avoid FastAPI Query object issues
    data_dict = await asyncio.to_thread(_list_tasks_sync, store, page, page_size, None, None)
    data = PaginatedResponse(**data_dict)

    if not data.items:
        cards_html = '''
        <div class="empty-state">
            <h3>No task runs found</h3>
            <p>Task runs will appear here once you make API requests.</p>
        </div>'''
    else:
        cards_html = ""
        for task in data.items:
            preview = _truncate(task.get("input_text") or json.dumps(task.get("input_json") or {}), 300)
            output_preview = _truncate(task.get("output_text") or json.dumps(task.get("output_json") or {}), 200)

            cards_html += f'''
            <div class="card">
                <div class="card-header">
                    <h3><code>{task["run_id"]}</code></h3>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        {_status_badge(task["status"])}
                        <a href="/browser/tasks/{task["run_id"]}" class="btn btn-secondary">View</a>
                        <a href="/v1/data/tasks/{task["run_id"]}/download" class="btn btn-secondary">⬇ Download</a>
                    </div>
                </div>
                <div class="card-body">
                    <div class="card-meta">
                        <div class="meta-item"><strong>Processor:</strong> {task.get("processor", "N/A")}</div>
                        <div class="meta-item"><strong>Created:</strong> {task.get("created_at", "N/A")}</div>
                    </div>
                    <div class="detail-section">
                        <h4>Input</h4>
                        <div class="content-preview">{preview}</div>
                    </div>
                    {f'<div class="detail-section"><h4>Output</h4><div class="content-preview">{output_preview}</div></div>' if output_preview else ''}
                </div>
            </div>'''

    pagination = _pagination_html(page, data.total_pages, "/browser/tasks")

    content = f'''
    <h1>Task Runs</h1>
    <p style="color: var(--text-secondary); margin-bottom: 20px;">
        Showing {len(data.items)} of {data.total} task runs
    </p>
    {cards_html}
    {pagination}
    '''

    return HTMLResponse(_base_html("Task Runs", content, "tasks"))


@browser_router.get("/tasks/{run_id}", response_class=HTMLResponse, include_in_schema=False)
async def browser_task_detail(
    run_id: str,
    store: TursoStore = Depends(get_store),
):
    """View task run detail."""
    task = await asyncio.to_thread(store.get_task_run, run_id)
    if not task:
        return HTMLResponse(_base_html("Not Found", '<div class="empty-state"><h3>Task not found</h3></div>', "tasks"))

    input_display = task.get("input_text") or json.dumps(task.get("input_json"), indent=2) or "N/A"
    output_display = task.get("output_text") or json.dumps(task.get("output_json"), indent=2) or "N/A"
    task_spec_display = json.dumps(task.get("task_spec"), indent=2) if task.get("task_spec") else "N/A"
    metadata_display = json.dumps(task.get("metadata"), indent=2) if task.get("metadata") else "N/A"

    content = f'''
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h1>Task Run</h1>
        <div style="display: flex; gap: 10px;">
            <a href="/browser/tasks" class="btn btn-secondary">← Back to List</a>
            <a href="/v1/data/tasks/{run_id}/download" class="btn btn-primary">⬇ Download JSON</a>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <h3><code>{run_id}</code></h3>
            {_status_badge(task["status"])}
        </div>
        <div class="card-body">
            <div class="card-meta">
                <div class="meta-item"><strong>Processor:</strong> {task.get("processor", "N/A")}</div>
                <div class="meta-item"><strong>Created:</strong> {task.get("created_at", "N/A")}</div>
                <div class="meta-item"><strong>Modified:</strong> {task.get("modified_at", "N/A")}</div>
                <div class="meta-item"><strong>Completed:</strong> {task.get("completed_at") or "In Progress"}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Input</h3></div>
        <div class="card-body">
            <div class="json-viewer">{input_display}</div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Output</h3></div>
        <div class="card-body">
            <div class="json-viewer">{output_display}</div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Task Spec</h3></div>
        <div class="card-body">
            <div class="json-viewer">{task_spec_display}</div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Metadata</h3></div>
        <div class="card-body">
            <div class="json-viewer">{metadata_display}</div>
        </div>
    </div>

    {f'<div class="card"><div class="card-header"><h3>Error</h3></div><div class="card-body"><div class="content-preview" style="color: var(--error);">{task.get("error")}</div></div></div>' if task.get("error") else ''}
    '''

    return HTMLResponse(_base_html(f"Task {run_id}", content, "tasks"))


@browser_router.get("/findall", response_class=HTMLResponse, include_in_schema=False)
async def browser_findall(
    page: int = Query(1, ge=1),
    store: TursoStore = Depends(get_store),
):
    """Browse FindAll runs."""
    page_size = 10
    # Call sync helper directly to avoid FastAPI Query object issues
    data_dict = await asyncio.to_thread(_list_findall_sync, store, page, page_size, None)
    data = PaginatedResponse(**data_dict)

    if not data.items:
        cards_html = '''
        <div class="empty-state">
            <h3>No FindAll runs found</h3>
            <p>FindAll runs will appear here once you make API requests.</p>
        </div>'''
    else:
        cards_html = ""
        for run in data.items:
            cards_html += f'''
            <div class="card">
                <div class="card-header">
                    <h3><code>{run["findall_id"]}</code></h3>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        {_status_badge(run["status"])}
                        <a href="/v1/data/findall/{run["findall_id"]}/download" class="btn btn-secondary">⬇ Download</a>
                    </div>
                </div>
                <div class="card-body">
                    <div class="card-meta">
                        <div class="meta-item"><strong>Entity Type:</strong> {run.get("entity_type", "N/A")}</div>
                        <div class="meta-item"><strong>Generator:</strong> {run.get("generator", "N/A")}</div>
                        <div class="meta-item"><strong>Match Limit:</strong> {run.get("match_limit", "N/A")}</div>
                        <div class="meta-item"><strong>Found:</strong> {run.get("candidates_found", 0)}</div>
                        <div class="meta-item"><strong>Matched:</strong> {run.get("candidates_matched", 0)}</div>
                    </div>
                    <div class="detail-section">
                        <h4>Objective</h4>
                        <div class="content-preview">{run.get("objective", "N/A")}</div>
                    </div>
                </div>
            </div>'''

    pagination = _pagination_html(page, data.total_pages, "/browser/findall")

    content = f'''
    <h1>FindAll Runs</h1>
    <p style="color: var(--text-secondary); margin-bottom: 20px;">
        Showing {len(data.items)} of {data.total} FindAll runs
    </p>
    {cards_html}
    {pagination}
    '''

    return HTMLResponse(_base_html("FindAll Runs", content, "findall"))


@browser_router.get("/chat", response_class=HTMLResponse, include_in_schema=False)
async def browser_chat(
    page: int = Query(1, ge=1),
    store: TursoStore = Depends(get_store),
):
    """Browse chat completions."""
    page_size = 10
    # Call sync helper directly to avoid FastAPI Query object issues
    data_dict = await asyncio.to_thread(_list_chat_sync, store, page, page_size)
    data = PaginatedResponse(**data_dict)

    if not data.items:
        cards_html = '''
        <div class="empty-state">
            <h3>No chat completions found</h3>
            <p>Chat completions will appear here once you make API requests.</p>
        </div>'''
    else:
        cards_html = ""
        for chat in data.items:
            messages = chat.get("messages") or []
            last_user_msg = ""
            for m in reversed(messages):
                if m.get("role") == "user":
                    last_user_msg = _truncate(m.get("content", ""), 200)
                    break

            response_preview = _truncate(chat.get("response") or "", 200)

            cards_html += f'''
            <div class="card">
                <div class="card-header">
                    <h3>Chat #{chat["id"]} {f'<code style="font-size: 0.75rem; color: var(--text-secondary);">{chat.get("completion_id", "")[:20]}...</code>' if chat.get("completion_id") else ''}</h3>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        <span class="badge badge-completed">{chat.get("model", "N/A")}</span>
                        <a href="/v1/data/chat/{chat["id"]}/download" class="btn btn-secondary">⬇ Download</a>
                    </div>
                </div>
                <div class="card-body">
                    <div class="card-meta">
                        <div class="meta-item"><strong>Messages:</strong> {len(messages)}</div>
                        <div class="meta-item"><strong>Tokens:</strong> {chat.get("usage_total_tokens") or "N/A"}</div>
                        <div class="meta-item"><strong>Duration:</strong> {chat.get("duration_ms") or "N/A"}ms</div>
                        <div class="meta-item"><strong>Created:</strong> {chat.get("created_at", "N/A")}</div>
                    </div>
                    <div class="detail-section">
                        <h4>User Message</h4>
                        <div class="content-preview">{last_user_msg or "N/A"}</div>
                    </div>
                    <div class="detail-section">
                        <h4>Response</h4>
                        <div class="content-preview">{response_preview or "N/A"}</div>
                    </div>
                </div>
            </div>'''

    pagination = _pagination_html(page, data.total_pages, "/browser/chat")

    content = f'''
    <h1>Chat Completions</h1>
    <p style="color: var(--text-secondary); margin-bottom: 20px;">
        Showing {len(data.items)} of {data.total} chat completions
    </p>
    {cards_html}
    {pagination}
    '''

    return HTMLResponse(_base_html("Chat Completions", content, "chat"))


@browser_router.get("/search", response_class=HTMLResponse, include_in_schema=False)
async def browser_search(
    page: int = Query(1, ge=1),
    store: TursoStore = Depends(get_store),
):
    """Browse search requests."""
    page_size = 10
    # Call sync helper directly to avoid FastAPI Query object issues
    data_dict = await asyncio.to_thread(_list_search_sync, store, page, page_size)
    data = PaginatedResponse(**data_dict)

    if not data.items:
        cards_html = '''
        <div class="empty-state">
            <h3>No search requests found</h3>
            <p>Search requests will appear here once you make API requests.</p>
        </div>'''
    else:
        cards_html = ""
        for search in data.items:
            queries = search.get("search_queries") or []
            results = search.get("results") or []
            results_preview = ""
            for r in results[:3]:
                results_preview += f"• {r.get('title', 'Untitled')}\\n"

            cards_html += f'''
            <div class="card">
                <div class="card-header">
                    <h3>Search #{search["id"]}</h3>
                    <a href="/v1/data/search/{search["id"]}/download" class="btn btn-secondary">⬇ Download</a>
                </div>
                <div class="card-body">
                    <div class="card-meta">
                        <div class="meta-item"><strong>Results:</strong> {search.get("results_count", 0)}</div>
                        <div class="meta-item"><strong>Max Results:</strong> {search.get("max_results") or "N/A"}</div>
                        <div class="meta-item"><strong>Duration:</strong> {search.get("duration_ms") or "N/A"}ms</div>
                        <div class="meta-item"><strong>Created:</strong> {search.get("created_at", "N/A")}</div>
                    </div>
                    <div class="detail-section">
                        <h4>Objective</h4>
                        <div class="content-preview">{search.get("objective", "N/A")}</div>
                    </div>
                    {f'<div class="detail-section"><h4>Search Queries</h4><div class="content-preview">{chr(10).join(queries)}</div></div>' if queries else ''}
                    {f'<div class="detail-section"><h4>Top Results</h4><div class="content-preview">{results_preview}</div></div>' if results_preview else ''}
                </div>
            </div>'''

    pagination = _pagination_html(page, data.total_pages, "/browser/search")

    content = f'''
    <h1>Search Requests</h1>
    <p style="color: var(--text-secondary); margin-bottom: 20px;">
        Showing {len(data.items)} of {data.total} search requests
    </p>
    {cards_html}
    {pagination}
    '''

    return HTMLResponse(_base_html("Search Requests", content, "search"))


@browser_router.get("/extract", response_class=HTMLResponse, include_in_schema=False)
async def browser_extract(
    page: int = Query(1, ge=1),
    store: TursoStore = Depends(get_store),
):
    """Browse extract requests."""
    page_size = 10
    # Call sync helper directly to avoid FastAPI Query object issues
    data_dict = await asyncio.to_thread(_list_extract_sync, store, page, page_size)
    data = PaginatedResponse(**data_dict)

    if not data.items:
        cards_html = '''
        <div class="empty-state">
            <h3>No extract requests found</h3>
            <p>Extract requests will appear here once you make API requests.</p>
        </div>'''
    else:
        cards_html = ""
        for extract in data.items:
            urls = extract.get("urls") or []
            urls_preview = "\\n".join(urls[:3])
            if len(urls) > 3:
                urls_preview += f"\\n... and {len(urls) - 3} more"

            cards_html += f'''
            <div class="card">
                <div class="card-header">
                    <h3>Extract #{extract["id"]}</h3>
                    <a href="/v1/data/extract/{extract["id"]}/download" class="btn btn-secondary">⬇ Download</a>
                </div>
                <div class="card-body">
                    <div class="card-meta">
                        <div class="meta-item"><strong>URLs:</strong> {len(urls)}</div>
                        <div class="meta-item"><strong>Results:</strong> {extract.get("results_count", 0)}</div>
                        <div class="meta-item"><strong>Errors:</strong> {extract.get("errors_count", 0)}</div>
                        <div class="meta-item"><strong>Duration:</strong> {extract.get("duration_ms") or "N/A"}ms</div>
                        <div class="meta-item"><strong>Created:</strong> {extract.get("created_at", "N/A")}</div>
                    </div>
                    {f'<div class="detail-section"><h4>Objective</h4><div class="content-preview">{extract.get("objective")}</div></div>' if extract.get("objective") else ''}
                    <div class="detail-section">
                        <h4>URLs</h4>
                        <div class="content-preview">{urls_preview}</div>
                    </div>
                </div>
            </div>'''

    pagination = _pagination_html(page, data.total_pages, "/browser/extract")

    content = f'''
    <h1>Extract Requests</h1>
    <p style="color: var(--text-secondary); margin-bottom: 20px;">
        Showing {len(data.items)} of {data.total} extract requests
    </p>
    {cards_html}
    {pagination}
    '''

    return HTMLResponse(_base_html("Extract Requests", content, "extract"))
