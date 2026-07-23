"""Tool Management registry integration for distributed-research server."""

import hashlib
import logging
from typing import Any, Dict, List

import httpx

from parallel_websearch_server.config import settings

logger = logging.getLogger(__name__)


# Tool definitions for parallel_websearch_server
TOOL_DEFINITIONS = [
    {
        "name": "parallel_search",
        "description": "Web search using Parallel AI. Returns relevant URLs with excerpts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "objective": {"type": "string", "description": "What you want to find. Natural language description of search goal."},
                "max_results": {"type": "integer", "default": 10, "description": "Maximum number of results to return"},
                "search_queries": {"type": "array", "items": {"type": "string"}, "description": "Optional specific search queries to control exact searches performed"},
            },
            "required": ["objective"],
        },
        "endpoint": "/v1beta/search",
        "category": "search",
        "tags": ["search", "parallel", "web"],
    },
    {
        "name": "parallel_extract",
        "description": "Extract structured content from URLs using Parallel AI.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL to extract content from"},
                "schema": {"type": "object", "description": "Extraction schema"},
            },
            "required": ["url"],
        },
        "endpoint": "/v1beta/extract",
        "category": "content",
        "tags": ["extract", "scrape", "content"],
    },
    {
        "name": "parallel_findall",
        "description": "Prospecting/discovery flow - find all entities matching criteria.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Discovery query"},
                "entity_type": {"type": "string", "description": "Type of entities to find"},
                "max_results": {"type": "integer", "default": 100},
            },
            "required": ["query"],
        },
        "endpoint": "/v1beta/findall/ingest",
        "category": "discovery",
        "tags": ["findall", "prospecting", "discovery"],
    },
    {
        "name": "parallel_task_create",
        "description": "Create and run a Parallel AI task.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Task prompt/instruction"},
                "processor": {"type": "string", "default": "lite"},
            },
            "required": ["prompt"],
        },
        "endpoint": "/v1/tasks/runs",
        "category": "tasks",
        "tags": ["tasks", "parallel"],
    },
    {
        "name": "parallel_orchestrated_search",
        "description": "Intelligent orchestrated search with automatic processor escalation and term discovery.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "max_iterations": {"type": "integer", "default": 3},
                "synthesis": {"type": "boolean", "default": True},
            },
            "required": ["query"],
        },
        "endpoint": "/v1/orchestration/search",
        "category": "search",
        "tags": ["search", "orchestration", "intelligent"],
    },
    {
        "name": "parallel_meta_query",
        "description": "Knowledge graph meta-pipeline query with entity extraction and relationship mapping.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language query"},
                "include_entities": {"type": "boolean", "default": True},
                "include_relationships": {"type": "boolean", "default": True},
            },
            "required": ["query"],
        },
        "endpoint": "/v1/meta/query",
        "category": "knowledge",
        "tags": ["knowledge-graph", "meta", "entities"],
    },
]


def _fingerprint(tool: Dict[str, Any]) -> str:
    """Generate fingerprint for tool definition."""
    import json
    data = f"{tool['name']}:{tool['description']}:{json.dumps(tool['input_schema'], sort_keys=True)}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


async def register_tools_with_registry():
    """Register all tools with Tool Management API."""
    if not settings.registry_url:
        logger.info("No registry URL configured, skipping tool registration")
        return

    service_url = f"http://{settings.host}:{settings.port}"

    async with httpx.AsyncClient(timeout=30) as client:
        # Get existing tools to check fingerprints
        try:
            resp = await client.get(f"{settings.registry_url}/api/v1/tools")
            existing = {t["name"]: t for t in resp.json()} if resp.status_code == 200 else {}
        except Exception as e:
            logger.warning(f"Could not fetch existing tools: {e}")
            existing = {}

        registered = []
        skipped = []
        failed = []

        for tool_def in TOOL_DEFINITIONS:
            fingerprint = _fingerprint(tool_def)

            # Check if unchanged
            if tool_def["name"] in existing:
                existing_fp = existing[tool_def["name"]].get("metadata", {}).get("fingerprint")
                if existing_fp == fingerprint:
                    skipped.append(tool_def["name"])
                    continue

            payload = {
                "name": tool_def["name"],
                "description": tool_def["description"],
                "input_schema": tool_def["input_schema"],
                "output_schema": {"type": "object"},
                "action": {
                    "type": "http",
                    "http": {
                        "method": "POST",
                        "url": f"{service_url}{tool_def['endpoint']}",
                        "timeout_ms": 60000,
                    },
                },
                "service_url": service_url,
                "service_name": settings.server_name,
                "category": tool_def.get("category", "research"),
                "tags": tool_def.get("tags", []),
                "metadata": {"fingerprint": fingerprint},
            }

            try:
                if tool_def["name"] in existing:
                    # Update
                    resp = await client.patch(
                        f"{settings.registry_url}/api/v1/tools/{tool_def['name']}",
                        json=payload,
                    )
                else:
                    # Create
                    resp = await client.post(
                        f"{settings.registry_url}/api/v1/tools",
                        json=payload,
                    )

                if resp.status_code in (200, 201):
                    registered.append(tool_def["name"])
                else:
                    failed.append(tool_def["name"])
                    logger.warning(f"Failed to register {tool_def['name']}: {resp.status_code}")
            except Exception as e:
                failed.append(tool_def["name"])
                logger.error(f"Error registering {tool_def['name']}: {e}")

        logger.info(f"[ToolMgmt] registered={len(registered)} skipped={len(skipped)} failed={len(failed)}")
        if registered:
            logger.info(f"[ToolMgmt] Synced {len(registered)} tools with registry at {settings.registry_url}")


async def deregister_tools():
    """Cleanup - could deregister tools if needed."""
    pass
