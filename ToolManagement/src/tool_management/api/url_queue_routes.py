"""
URL Queue API Routes.

Provides endpoints for:
- Viewing queued URLs
- Manual URL submission
- Queue statistics
- URL status management
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..queue.url_queue import (
    get_url_queue,
    QueuedURL,
    URLStatus,
    URLSource,
)

router = APIRouter(prefix="/urls", tags=["url-queue"])


# =============================================================================
# Request/Response Models
# =============================================================================

class EnqueueURLRequest(BaseModel):
    url: str = Field(..., description="URL to enqueue")
    source: str = Field(default="api", description="Source of the URL")
    source_query: Optional[str] = Field(None, description="Query that found this URL")
    user_id: Optional[str] = Field(None, description="User ID to associate with")
    session_id: Optional[str] = Field(None, description="Session ID to associate with")
    title: Optional[str] = Field(None, description="Page title")
    description: Optional[str] = Field(None, description="Page description/snippet")
    priority: int = Field(default=0, description="Processing priority")
    metadata: Optional[dict] = Field(None, description="Additional metadata")


class EnqueueBatchRequest(BaseModel):
    urls: List[dict] = Field(..., description="List of URLs with metadata")
    source: str = Field(default="api", description="Source for all URLs")
    source_query: Optional[str] = Field(None, description="Query that found these URLs")
    user_id: Optional[str] = Field(None, description="User ID to associate with")
    session_id: Optional[str] = Field(None, description="Session ID to associate with")


class URLResponse(BaseModel):
    id: str
    url: str
    source: str
    source_query: Optional[str]
    user_id: Optional[str]
    session_id: Optional[str]
    status: str
    title: Optional[str]
    description: Optional[str]
    priority: int
    created_at: Optional[str]
    processed_at: Optional[str]
    error: Optional[str]

    @classmethod
    def from_queued_url(cls, url: QueuedURL) -> "URLResponse":
        return cls(
            id=url.id,
            url=url.url,
            source=url.source.value,
            source_query=url.source_query,
            user_id=url.user_id,
            session_id=url.session_id,
            status=url.status.value,
            title=url.title,
            description=url.description,
            priority=url.priority,
            created_at=url.created_at.isoformat() if url.created_at else None,
            processed_at=url.processed_at.isoformat() if url.processed_at else None,
            error=url.error,
        )


class UpdateStatusRequest(BaseModel):
    status: str = Field(..., description="New status")
    error: Optional[str] = Field(None, description="Error message if failed")
    processed_data: Optional[dict] = Field(None, description="Processing results")


class QueueStatsResponse(BaseModel):
    total: int
    pending: int
    completed: int
    failed: int
    by_status: dict
    by_source: dict


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/enqueue", response_model=URLResponse)
async def enqueue_url(request: EnqueueURLRequest):
    """
    Add a single URL to the processing queue.

    URLs are deduplicated by URL + user_id + session_id.
    """
    queue = get_url_queue()

    try:
        source = URLSource(request.source)
    except ValueError:
        source = URLSource.API

    result = queue.enqueue(
        url=request.url,
        source=source,
        source_query=request.source_query,
        user_id=request.user_id,
        session_id=request.session_id,
        title=request.title,
        description=request.description,
        priority=request.priority,
        metadata=request.metadata,
    )

    if result is None:
        raise HTTPException(
            status_code=409,
            detail="URL already queued for this user/session"
        )

    return URLResponse.from_queued_url(result)


@router.post("/enqueue-batch")
async def enqueue_batch(request: EnqueueBatchRequest):
    """
    Add multiple URLs to the processing queue.

    Returns count of successfully queued URLs.
    """
    queue = get_url_queue()

    try:
        source = URLSource(request.source)
    except ValueError:
        source = URLSource.API

    results = queue.enqueue_batch(
        urls=request.urls,
        source=source,
        source_query=request.source_query,
        user_id=request.user_id,
        session_id=request.session_id,
    )

    return {
        "queued": len(results),
        "submitted": len(request.urls),
        "urls": [URLResponse.from_queued_url(u) for u in results]
    }


@router.get("/", response_model=List[URLResponse])
async def list_urls(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    session_id: Optional[str] = Query(None, description="Filter by session ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    source: Optional[str] = Query(None, description="Filter by source"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List queued URLs with optional filters."""
    queue = get_url_queue()

    status_enum = URLStatus(status) if status else None
    source_enum = URLSource(source) if source else None

    urls = queue.list_urls(
        user_id=user_id,
        session_id=session_id,
        status=status_enum,
        source=source_enum,
        limit=limit,
        offset=offset,
    )

    return [URLResponse.from_queued_url(u) for u in urls]


@router.get("/stats", response_model=QueueStatsResponse)
async def get_stats(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    session_id: Optional[str] = Query(None, description="Filter by session ID"),
):
    """Get URL queue statistics."""
    queue = get_url_queue()
    stats = queue.stats(user_id=user_id, session_id=session_id)
    return QueueStatsResponse(**stats)


@router.get("/processors")
async def list_processors():
    """List all available content processors."""
    from ..queue.content_processors import ProcessorRegistry

    return {
        "processors": ProcessorRegistry.list_all(),
        "count": len(ProcessorRegistry._processors),
    }


@router.get("/next/pending")
async def get_next_pending(
    user_id: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
):
    """Get the next URL pending processing."""
    queue = get_url_queue()
    url = queue.get_next_pending(user_id=user_id, session_id=session_id)

    if url is None:
        return {"url": None, "message": "No pending URLs"}

    return URLResponse.from_queued_url(url)


@router.get("/search/by-query")
async def search_by_query(
    query: str = Query(..., description="Search query to match"),
    user_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100),
):
    """Find URLs that were discovered by a specific search query."""
    queue = get_url_queue()

    # Get all URLs and filter by query
    all_urls = queue.list_urls(user_id=user_id, limit=1000)
    matching = [
        u for u in all_urls
        if u.source_query and query.lower() in u.source_query.lower()
    ][:limit]

    return {
        "query": query,
        "count": len(matching),
        "urls": [
            {
                "id": u.id,
                "url": u.url,
                "title": u.title,
                "source_query": u.source_query,
                "has_content": u.content is not None,
                "content_length": len(u.content) if u.content else 0,
            }
            for u in matching
        ]
    }


@router.get("/{url_id}", response_model=URLResponse)
async def get_url(url_id: str):
    """Get a specific queued URL by ID."""
    queue = get_url_queue()
    url = queue.get(url_id)

    if url is None:
        raise HTTPException(status_code=404, detail="URL not found")

    return URLResponse.from_queued_url(url)


@router.patch("/{url_id}/status")
async def update_url_status(url_id: str, request: UpdateStatusRequest):
    """Update the status of a queued URL."""
    queue = get_url_queue()

    try:
        status = URLStatus(request.status)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {[s.value for s in URLStatus]}"
        )

    success = queue.update_status(
        url_id=url_id,
        status=status,
        error=request.error,
        processed_data=request.processed_data,
    )

    if not success:
        raise HTTPException(status_code=404, detail="URL not found")

    updated_url = queue.get(url_id)
    if updated_url is None:
        raise HTTPException(status_code=404, detail="URL not found after update")
    return URLResponse.from_queued_url(updated_url)


@router.delete("/{url_id}")
async def delete_url(url_id: str):
    """Delete a queued URL."""
    queue = get_url_queue()
    success = queue.delete(url_id)

    if not success:
        raise HTTPException(status_code=404, detail="URL not found")

    return {"deleted": True, "id": url_id}


@router.post("/cleanup")
async def cleanup_old_urls(
    days: int = Query(30, ge=1, le=365, description="Delete URLs older than N days"),
    status: Optional[str] = Query(None, description="Only delete URLs with this status"),
):
    """Clean up old URLs from the queue."""
    queue = get_url_queue()

    status_enum = URLStatus(status) if status else None
    count = queue.cleanup_old(days=days, status=status_enum)

    return {"deleted": count, "older_than_days": days}


@router.get("/{url_id}/content")
async def get_url_content(url_id: str):
    """Get the cached content for a URL."""
    queue = get_url_queue()
    url = queue.get(url_id)

    if url is None:
        raise HTTPException(status_code=404, detail="URL not found")

    return {
        "id": url.id,
        "url": url.url,
        "title": url.title,
        "description": url.description,
        "content": url.content,
        "content_length": len(url.content) if url.content else 0,
        "has_content": url.content is not None,
        "source_query": url.source_query,
    }


@router.post("/process")
async def process_pending(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    session_id: Optional[str] = Query(None, description="Filter by session ID"),
    limit: int = Query(10, ge=1, le=50, description="Max URLs to process"),
    concurrency: int = Query(3, ge=1, le=10, description="Concurrent fetches"),
):
    """
    Process pending URLs using trafilatura (local, no API costs).

    Fetches content from pending URLs and caches it in the queue.
    Uses trafilatura for high-quality extraction, with bs4 fallback.
    """
    from ..queue.content_fetcher import process_pending_urls

    result = await process_pending_urls(
        user_id=user_id,
        session_id=session_id,
        limit=limit,
        concurrency=concurrency,
    )

    return result


@router.post("/fetch-single")
async def fetch_single_url(
    url: str = Query(..., description="URL to fetch"),
    user_id: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    enqueue: bool = Query(True, description="Add to queue after fetching"),
):
    """
    Fetch content from a single URL using trafilatura (no API costs).

    Optionally enqueues the URL with cached content.
    """
    from ..queue.content_fetcher import fetch_url_content

    result = await fetch_url_content(url)

    response = {
        "url": url,
        "success": result.success,
        "title": result.title,
        "description": result.description,
        "content_length": len(result.content) if result.content else 0,
        "word_count": result.word_count,
        "method": result.method,
        "error": result.error,
        "fetch_time_ms": result.fetch_time_ms,
    }

    # Optionally enqueue with content
    if enqueue and result.success:
        queue = get_url_queue()
        from ..queue.url_queue import URLSource

        queued = queue.enqueue(
            url=url,
            source=URLSource.API,
            user_id=user_id,
            session_id=session_id,
            title=result.title,
            description=result.description,
            content=result.content,
        )

        if queued:
            response["queued_id"] = queued.id
            response["queued"] = True
        else:
            response["queued"] = False
            response["queued_message"] = "URL already exists"

    # Include content preview
    if result.content:
        response["content_preview"] = result.content[:500]

    return response


@router.post("/process-content")
async def process_content(
    url: str = Query(..., description="URL to fetch and process"),
    processors: Optional[str] = Query(
        None,
        description="Comma-separated processor names (default: all)"
    ),
    fetch_fresh: bool = Query(True, description="Fetch fresh content vs use cached"),
):
    """
    Fetch URL and run it through content processors.

    Available processors:
    - text_extractor: Extract clean text
    - entity_extractor: Extract named entities (NER)
    - summarizer: Generate summary
    - embedding_generator: Create vector embeddings
    - keyword_extractor: Extract keywords/topics
    - link_extractor: Extract and categorize links
    - structured_data_extractor: Extract JSON-LD, OpenGraph
    - table_extractor: Extract tables as data
    - sentiment_analyzer: Analyze sentiment
    - qa_generator: Generate Q&A pairs
    """
    from ..queue.content_processors import ProcessingPipeline
    import httpx

    # Parse processor list
    proc_list = None
    if processors:
        proc_list = [p.strip() for p in processors.split(",")]

    # Check if we have cached content
    queue = get_url_queue()
    cached = queue.get_by_url(url)

    html = None
    fetch_info = {}

    if cached and cached.content and not fetch_fresh:
        # Use cached - but we need HTML, not extracted text
        # So we'll need to fetch anyway for full processing
        pass

    # Fetch fresh HTML
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; ContentProcessor/1.0)"
            })
            response.raise_for_status()
            html = response.text
            fetch_info = {
                "status_code": response.status_code,
                "content_length": len(html),
            }
    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": f"Failed to fetch: {str(e)}",
        }

    # Run through pipeline
    pipeline = ProcessingPipeline()
    ctx = await pipeline.process(url, html, proc_list)

    # Build response
    results = {}
    for name, result in ctx.results.items():
        if result.success:
            results[name] = {
                "success": True,
                "data": result.data,
                "processing_time_ms": result.processing_time_ms,
            }
        else:
            results[name] = {
                "success": False,
                "error": result.error,
            }

    return {
        "url": url,
        "title": ctx.title,
        "fetch_info": fetch_info,
        "processors_run": len(ctx.results),
        "successful": sum(1 for r in ctx.results.values() if r.success),
        "results": results,
    }


@router.post("/{url_id}/enrich")
async def enrich_url(
    url_id: str,
    processors: Optional[str] = Query(
        "keyword_extractor,entity_extractor,summarizer",
        description="Comma-separated processor names"
    ),
):
    """
    Enrich a queued URL with additional processing.

    Runs specified processors and stores results in processed_data.
    """
    from ..queue.content_processors import ProcessingPipeline
    import httpx

    queue = get_url_queue()
    queued_url = queue.get(url_id)

    if not queued_url:
        raise HTTPException(status_code=404, detail="URL not found")

    # Parse processor list
    proc_list = [p.strip() for p in processors.split(",")] if processors else None

    # Fetch HTML
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(queued_url.url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; ContentProcessor/1.0)"
            })
            response.raise_for_status()
            html = response.text
    except Exception as e:
        return {
            "url_id": url_id,
            "success": False,
            "error": f"Failed to fetch: {str(e)}",
        }

    # Run pipeline
    pipeline = ProcessingPipeline()
    ctx = await pipeline.process(queued_url.url, html, proc_list)

    # Collect results
    enrichment_data = {}
    for name, result in ctx.results.items():
        if result.success:
            enrichment_data[name] = result.data

    # Update queue entry
    existing_data = queued_url.processed_data or {}
    existing_data.update(enrichment_data)

    queue.update_status(
        url_id,
        queued_url.status,
        processed_data=existing_data,
    )

    return {
        "url_id": url_id,
        "url": queued_url.url,
        "processors_run": len(ctx.results),
        "successful": sum(1 for r in ctx.results.values() if r.success),
        "enrichment_data": enrichment_data,
    }
