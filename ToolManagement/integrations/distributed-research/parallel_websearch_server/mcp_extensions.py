"""Custom MCP prompt and resource handlers for the Parallel Web Search server."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Mapping

from mcp import types
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.shared.exceptions import McpError
from pydantic import AnyUrl
from typing_extensions import Protocol

from parallel_websearch_server.services.docs_catalog import (
    get_doc_catalog,
    get_section_by_slug,
)

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from fastapi_mcp import FastApiMCP

logger = logging.getLogger(__name__)

DOC_RESOURCE_SCHEME = "parallel-docs"
DOC_CATALOG_URI = f"{DOC_RESOURCE_SCHEME}://catalog"
DOC_SECTION_URI_PREFIX = f"{DOC_RESOURCE_SCHEME}://section/"


class PromptBuilder(Protocol):
    """Callable contract for building a prompt result."""

    def __call__(self, arguments: Dict[str, str]) -> types.GetPromptResult: ...


@dataclass(frozen=True)
class PromptDefinition:
    """Holds metadata plus the builder used to render a prompt."""

    prompt: types.Prompt
    builder: PromptBuilder


def register_mcp_extensions(mcp: "FastApiMCP") -> None:
    """Attach prompt and resource handlers to the FastApiMCP server."""

    server = mcp.server
    logger.info("Registering MCP prompt/resource extensions for %s", mcp.name)

    @server.list_prompts()
    async def _list_prompts() -> types.ListPromptsResult:
        prompt_list = [definition.prompt for definition in PROMPT_DEFINITIONS.values()]
        logger.debug("Serving prompts/list with %d prompts: %s", len(prompt_list), [p.name for p in prompt_list])
        return types.ListPromptsResult(prompts=prompt_list)

    @server.get_prompt()
    async def _get_prompt(
        name: str, arguments: Dict[str, str] | None
    ) -> types.GetPromptResult:
        try:
            definition = PROMPT_DEFINITIONS[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise _unknown_prompt(name) from exc
        args = arguments or {}
        logger.debug("Serving prompts/get for %s with args=%s", name, args)
        result = definition.builder(args)
        logger.debug("Prompt %s produced %d message(s)", name, len(result.messages))
        return result

    @server.list_resources()
    async def _list_resources() -> types.ListResourcesResult:
        resources = _build_doc_resources()
        logger.debug("Serving resources/list with %d entries", len(resources))
        return types.ListResourcesResult(resources=resources)

    @server.read_resource()
    async def _read_resource(uri: AnyUrl) -> Iterable[ReadResourceContents]:
        logger.debug("resources/read requested for %s", uri)
        contents = _read_doc_resource(str(uri))
        logger.debug("resources/read for %s returning %d content block(s)", uri, len(contents))
        return contents


def _unknown_prompt(name: str) -> McpError:
    return McpError(
        types.ErrorData(
            code=types.INVALID_PARAMS,
            message=f"Unknown prompt '{name}'",
        )
    )


def _unknown_resource(uri: str) -> McpError:
    return McpError(
        types.ErrorData(
            code=types.INVALID_PARAMS,
            message=f"Unknown resource '{uri}'",
        )
    )


def _build_doc_resources() -> List[types.Resource]:
    """Convert the llms.txt catalog into MCP Resource entries."""
    catalog = get_doc_catalog()
    logger.debug(
        "Building doc resource list from catalog '%s' updated %s",
        catalog.title,
        catalog.last_modified.isoformat(),
    )
    resources: List[types.Resource] = [
        types.Resource(
            name="parallel.docs.catalog",
            title=f"{catalog.title} catalog",
            uri=DOC_CATALOG_URI,
            description="Structured view of the bundled Parallel documentation catalog.",
            mimeType="application/json",
            annotations=types.Annotations(priority=0.4),
            meta={
                "source_path": catalog.source_path,
                "last_modified": catalog.last_modified.isoformat(),
                "total_sections": len(catalog.sections),
            },
        )
    ]

    for section in catalog.sections:
        resources.append(
            types.Resource(
                name=f"parallel.docs.section.{section.slug}",
                title=f"{section.name} resources",
                uri=f"{DOC_SECTION_URI_PREFIX}{section.slug}",
                description=f"{len(section.resources)} curated docs covering {section.name}.",
                mimeType="application/json",
                annotations=types.Annotations(priority=0.35),
                meta={
                    "slug": section.slug,
                "section": section.name,
                "total_resources": len(section.resources),
                "sample_titles": [resource.title for resource in section.resources[:3]],
            },
        )
        )

    return resources


def _as_json_content(payload: Mapping[str, Any]) -> ReadResourceContents:
    """Serialize a payload into JSON resource content."""
    serialized = json.dumps(payload, indent=2, ensure_ascii=True)
    return ReadResourceContents(content=serialized, mime_type="application/json")


def _read_doc_resource(uri: str) -> Iterable[ReadResourceContents]:
    """Return JSON-encoded documentation data for the requested URI."""
    catalog = get_doc_catalog()
    if uri == DOC_CATALOG_URI:
        payload = {
            "title": catalog.title,
            "source_path": catalog.source_path,
            "last_modified": catalog.last_modified.isoformat(),
            "sections": [
                {
                    "name": section.name,
                    "slug": section.slug,
                    "total_resources": len(section.resources),
                }
                for section in catalog.sections
            ],
        }
        return [_as_json_content(payload)]

    if uri.startswith(DOC_SECTION_URI_PREFIX):
        slug = uri[len(DOC_SECTION_URI_PREFIX) :].strip("/")
        if not slug:
            raise _unknown_resource(uri)
        section = get_section_by_slug(catalog, slug)
        if not section:
            raise _unknown_resource(uri)
        payload = {
            "name": section.name,
            "slug": section.slug,
            "total_resources": len(section.resources),
            "source_path": catalog.source_path,
            "last_modified": catalog.last_modified.isoformat(),
            "resources": [
                {
                    "title": resource.title,
                    "url": str(resource.url),
                    "description": resource.description,
                }
                for resource in section.resources
            ],
        }
        logger.debug(
            "Section resource %s contains %d entries", section.slug, len(section.resources)
        )
        return [_as_json_content(payload)]

    raise _unknown_resource(uri)


def _build_research_prompt(arguments: Dict[str, str]) -> types.GetPromptResult:
    """Template for orchestrating Search + Extract + FindAll research flows."""
    objective = arguments.get("objective") or "Complete the stated research goal."
    context = arguments.get("context")
    deliverable = arguments.get("deliverable") or "Provide findings with citations and open questions."
    freshness = arguments.get("freshness") or "Prefer sources updated within the last 6 months."

    lines = [
        "You are the lead researcher operating Parallel's Search, Extract, FindAll, and Monitor tools.",
        f"Primary objective: {objective}",
        f"Preferred deliverable: {deliverable}",
        f"Freshness guidance: {freshness}",
        "If confidence is low, expand coverage or run a clarifying search.",
    ]
    if context:
        lines.insert(2, f"Context: {context}")

    workflow = (
        "Workflow:\n"
        "1. Decompose the question into concrete sub-goals and choose the best tool for each.\n"
        "2. Use /v1beta/search for broad coverage, rerun with refined queries if the signal is weak.\n"
        "3. Use /v1beta/extract to pull structured snippets from high-value URLs discovered via Search or external hints.\n"
        "4. Run /v1beta/findall when you need exhaustive multi-page coverage or enrichment.\n"
        "5. Summarize the evidence, cite sources, and call out any risks or follow-ups."
    )

    docs_hint = (
        "Reference the documentation resources exposed via resources/list (parallel-docs scheme) "
        "whenever you need endpoint specifics or headers."
    )

    text = "\n\n".join(["\n".join(lines), workflow, docs_hint])
    return types.GetPromptResult(
        description="Kick-starts a structured research plan that leans on Search, Extract, and FindAll.",
        messages=[
            types.PromptMessage(
                role="system",
                content=types.TextContent(type="text", text=text),
            )
        ],
    )


def _build_task_prompt(arguments: Dict[str, str]) -> types.GetPromptResult:
    """Template for planning Task API runs and Task Groups."""
    task_goal = arguments.get("task_goal") or "Deliver a high-quality task run."
    input_schema_hint = arguments.get("input_schema") or "Infer inputs from the provided request."
    output_expectations = arguments.get("output_expectations") or "Return structured findings with citations."
    cadence = arguments.get("cadence") or "Run once unless a follow-up cadence is specified."

    overview = [
        "You are configuring Parallel Task + Task Group executions through the MCP Task tools.",
        f"Task goal: {task_goal}",
        f"Input schema guidance: {input_schema_hint}",
        f"Output expectations: {output_expectations}",
        f"Cadence / follow-up: {cadence}",
    ]

    checklist = (
        "Execution checklist:\n"
        "- Validate whether a single /v1/tasks run is sufficient or if a Task Group is required.\n"
        "- When building the payload, keep schemas minimal and only include fields documented in /v1/tasks or /v1beta/tasks/groups.\n"
        "- IMPORTANT: Use /v1/tasks/runs/managed for agent workflows - it automatically polls and returns results without requiring users to visit external URLs or manually check status.\n"
        "- Only use basic /v1/tasks/runs if you need to handle polling/streaming yourself or have specific reasons to manage the lifecycle manually.\n"
        "- Track task_run_id or group_id for reference, but the managed endpoint handles all status tracking automatically.\n"
        "- Suggest monitors when the user needs recurring coverage of the same objective."
    )

    text = "\n\n".join(["\n".join(overview), checklist])

    return types.GetPromptResult(
        description="Shapes the system message for setting up Parallel Task or Task Group runs.",
        messages=[
            types.PromptMessage(
                role="system",
                content=types.TextContent(type="text", text=text),
            )
        ],
    )


PROMPT_DEFINITIONS: Dict[str, PromptDefinition] = {
    "parallel.research.brief": PromptDefinition(
        prompt=types.Prompt(
            name="parallel.research.brief",
            title="Parallel research playbook",
            description="System instructions optimized for Search, Extract, and FindAll orchestration.",
            arguments=[
                types.PromptArgument(
                    name="objective",
                    description="Primary research target or question.",
                    required=True,
                ),
                types.PromptArgument(
                    name="context",
                    description="Optional background, constraints, or user preferences.",
                    required=False,
                ),
                types.PromptArgument(
                    name="deliverable",
                    description="Preferred output shape (report, outline, tl;dr, etc.).",
                    required=False,
                ),
                types.PromptArgument(
                    name="freshness",
                    description="How recent the information must be.",
                    required=False,
                ),
            ],
        ),
        builder=_build_research_prompt,
    ),
    "parallel.tasks.brief": PromptDefinition(
        prompt=types.Prompt(
            name="parallel.tasks.brief",
            title="Parallel task run blueprint",
            description="Template for configuring Task runs, Task Groups, and follow-on monitors.",
            arguments=[
                types.PromptArgument(
                    name="task_goal",
                    description="What the downstream task should achieve.",
                    required=True,
                ),
                types.PromptArgument(
                    name="input_schema",
                    description="Input schema hints or custom fields that must be passed.",
                    required=False,
                ),
                types.PromptArgument(
                    name="output_expectations",
                    description="Describe the structure/quality bar for outputs.",
                    required=False,
                ),
                types.PromptArgument(
                    name="cadence",
                    description="Whether this should repeat (e.g., daily monitor, ad-hoc).",
                    required=False,
                ),
            ],
        ),
        builder=_build_task_prompt,
    ),
}


__all__ = [
    "register_mcp_extensions",
]
