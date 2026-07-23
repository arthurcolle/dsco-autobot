"""Utilities for parsing and serving the llms.txt documentation catalog."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple

from parallel_websearch_server.models import (
    DocCatalog,
    DocResource,
    DocResourceWithSection,
    DocSearchResponse,
    DocSection,
    DocSectionSummary,
)

TITLE_PATTERN = re.compile(r"^#\s+(?P<title>.+)$")
SECTION_PATTERN = re.compile(r"^##\s+(?P<section>.+)$")
ENTRY_PATTERN = re.compile(
    r"^- \[(?P<title>.+?)\]\((?P<url>https?://[^\s)]+)\)(?::\s*(?P<desc>.*))?$"
)
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")

LLMS_PATH = Path(__file__).resolve().parents[2] / "llms.txt"


def slugify(value: str) -> str:
    """Generate a URL-friendly slug from the provided value."""
    return NON_ALNUM_PATTERN.sub("-", value.lower()).strip("-")


def _finalize_entry(
    resources: List[DocResource],
    current_entry: Optional[Tuple[str, str]],
    description_lines: List[str],
) -> None:
    """Persist the in-progress entry into the provided resources list."""
    if not current_entry:
        return

    description = "\n".join(description_lines).strip()
    resources.append(
        DocResource(
            title=current_entry[0],
            url=current_entry[1],
            description=description or None,
        )
    )
    description_lines.clear()


def _parse_llms_text(text: str) -> Tuple[str, List[DocSection]]:
    """Parse llms.txt markdown content into structured sections."""
    sections: List[DocSection] = []
    current_section_name: Optional[str] = None
    current_resources: List[DocResource] = []
    current_entry: Optional[Tuple[str, str]] = None
    description_lines: List[str] = []
    catalog_title: str = "Parallel"

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        if not line.strip():
            if current_entry is not None:
                description_lines.append("")
            continue

        title_match = TITLE_PATTERN.match(line)
        if title_match:
            catalog_title = title_match.group("title").strip()
            continue

        section_match = SECTION_PATTERN.match(line)
        if section_match:
            _finalize_entry(current_resources, current_entry, description_lines)
            if current_section_name:
                sections.append(
                    DocSection(
                        name=current_section_name,
                        slug=slugify(current_section_name),
                        resources=list(current_resources),
                    )
                )
                current_resources.clear()
            current_section_name = section_match.group("section").strip()
            current_entry = None
            description_lines.clear()
            continue

        entry_match = ENTRY_PATTERN.match(line)
        if entry_match:
            _finalize_entry(current_resources, current_entry, description_lines)
            current_entry = (entry_match.group("title").strip(), entry_match.group("url").strip())
            inline_desc = entry_match.group("desc")
            description_lines.clear()
            if inline_desc:
                description_lines.append(inline_desc.strip())
            continue

        if current_entry is not None:
            description_lines.append(line.strip())

    _finalize_entry(current_resources, current_entry, description_lines)
    if current_section_name:
        sections.append(
            DocSection(
                name=current_section_name,
                slug=slugify(current_section_name),
                resources=list(current_resources),
            )
        )

    if not sections:
        raise ValueError("No sections could be parsed from llms.txt")

    return catalog_title, sections


def _load_catalog_from_disk(path: Path = LLMS_PATH) -> DocCatalog:
    """Read llms.txt from disk and parse it into a DocCatalog."""
    if not path.exists():
        raise FileNotFoundError(f"llms.txt not found at {path}")

    text = path.read_text(encoding="utf-8")
    catalog_title, sections = _parse_llms_text(text)
    stat_result = path.stat()
    last_modified = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc)

    return DocCatalog(
        title=catalog_title,
        sections=sections,
        source_path=str(path.resolve()),
        last_modified=last_modified,
    )


@lru_cache(maxsize=1)
def _get_cached_catalog() -> DocCatalog:
    """Return the cached DocCatalog."""
    return _load_catalog_from_disk()


def get_doc_catalog(refresh: bool = False) -> DocCatalog:
    """Get the cached documentation catalog, optionally refreshing it."""
    if refresh:
        _get_cached_catalog.cache_clear()
    return _get_cached_catalog()


def list_section_summaries(catalog: DocCatalog) -> List[DocSectionSummary]:
    """Return lightweight metadata for each documentation section."""
    return [
        DocSectionSummary(
            name=section.name,
            slug=section.slug,
            total_resources=len(section.resources),
        )
        for section in catalog.sections
    ]


def get_section_by_slug(catalog: DocCatalog, slug_or_name: str) -> Optional[DocSection]:
    """Retrieve a documentation section by slug or case-insensitive name."""
    normalized = slug_or_name.strip().lower()
    for section in catalog.sections:
        if section.slug == normalized or section.name.lower() == normalized:
            return section
    return None


def search_catalog(catalog: DocCatalog, query: str) -> DocSearchResponse:
    """Perform a simple substring search across documentation resources."""
    normalized_query = query.strip()
    if not normalized_query:
        return DocSearchResponse(query=query, total_results=0, results=[])

    lowered_query = normalized_query.lower()
    matches: List[DocResourceWithSection] = []

    for section in catalog.sections:
        for resource in section.resources:
            haystack = " ".join(
                part.lower()
                for part in (resource.title, resource.description or "", section.name)
                if part
            )
            if lowered_query in haystack:
                matches.append(
                    DocResourceWithSection(
                        title=resource.title,
                        url=resource.url,
                        description=resource.description,
                        section=section.name,
                    )
                )

    return DocSearchResponse(
        query=query,
        total_results=len(matches),
        results=matches,
    )
