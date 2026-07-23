"""Routes for exposing the llms.txt documentation catalog."""

from typing import List

from fastapi import APIRouter, HTTPException, Query

from parallel_websearch_server.models import (
    DocCatalog,
    DocSearchResponse,
    DocSection,
    DocSectionSummary,
)
from parallel_websearch_server.services.docs_catalog import (
    get_doc_catalog,
    get_section_by_slug,
    list_section_summaries,
    search_catalog,
)

router = APIRouter(prefix="/v1/docs", tags=["Documentation"])


@router.get("/catalog", response_model=DocCatalog)
async def fetch_catalog(refresh: bool = Query(False, description="Reload llms.txt from disk")) -> DocCatalog:
    """Return the full documentation catalog."""
    return get_doc_catalog(refresh=refresh)


@router.get("/sections", response_model=List[DocSectionSummary])
async def fetch_sections(
    refresh: bool = Query(False, description="Reload llms.txt from disk"),
) -> List[DocSectionSummary]:
    """Return lightweight metadata for each documentation section."""
    catalog = get_doc_catalog(refresh=refresh)
    return list_section_summaries(catalog)


@router.get("/sections/{section_slug}", response_model=DocSection)
async def fetch_section(
    section_slug: str,
    refresh: bool = Query(False, description="Reload llms.txt from disk"),
) -> DocSection:
    """Return the requested section with all of its resources."""
    catalog = get_doc_catalog(refresh=refresh)
    section = get_section_by_slug(catalog, section_slug)
    if not section:
        raise HTTPException(status_code=404, detail=f"Section '{section_slug}' not found")
    return section


@router.get("/search", response_model=DocSearchResponse)
async def search_docs(
    query: str = Query(..., min_length=2, description="Keyword to search within the catalog"),
    refresh: bool = Query(False, description="Reload llms.txt from disk"),
) -> DocSearchResponse:
    """Run a simple substring search through documentation titles and descriptions."""
    catalog = get_doc_catalog(refresh=refresh)
    return search_catalog(catalog, query)
