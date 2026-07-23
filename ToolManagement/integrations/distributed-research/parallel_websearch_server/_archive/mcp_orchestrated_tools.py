"""
Sophisticated MCP tools using the advanced TaskOrchestrator.

These tools NEVER return platform URLs. They always:
1. Poll automatically until completion
2. Return results inline
3. Handle all errors gracefully
4. Stream progress when supported
5. Cache results when appropriate
"""

import logging
from typing import Any, Dict, List, Optional

from mcp import types
from mcp.server.lowlevel.helper_types import Tool

from parallel_websearch_server.orchestration import (
    OrchestrationConfig,
    TaskExecutionRequest,
    TaskOrchestrator,
    TaskPriority,
)

logger = logging.getLogger(__name__)


def create_orchestrated_mcp_tools(
    orchestrator: TaskOrchestrator,
) -> List[Tool]:
    """
    Create MCP tools that use the advanced orchestrator.

    These tools provide a superior user experience by:
    - Automatically handling all polling
    - Returning results inline without external URLs
    - Streaming progress updates
    - Caching results for identical requests
    - Handling errors with automatic retries

    Args:
        orchestrator: Task orchestrator instance

    Returns:
        List of MCP tool definitions
    """

    async def parallel_research_task(arguments: Dict[str, Any]) -> types.CallToolResult:
        """
        Execute a research task on Parallel.ai with automatic result retrieval.

        This tool handles the complete lifecycle:
        1. Submits task to Parallel.ai
        2. Polls status automatically
        3. Retrieves and returns final results
        4. Never sends users to external URLs

        Args from MCP client:
            input (dict | str): Task input object (preferred) such as {"query": "..."}.
                Plain text prompts are automatically wrapped into {"query": "<text>"}.
            processor (str, optional): Tier - lite, base, core, pro, ultra, ultra2x
            task_spec (dict, optional): JSON object describing the desired output schema. If provided,
                it takes precedence over output_type + output_schema.
            output_type (str, optional): "text" or "json" (used when task_spec is omitted)
            output_schema (dict, optional): JSON schema for structured output (used when task_spec is omitted)
            priority (str, optional): Task priority - critical, high, normal, low, background
            timeout (int, optional): Override default timeout in seconds
            cache_key (str, optional): Key for result caching/deduplication
            context_id (str, optional): Context for state persistence

        Returns:
            Comprehensive result with output, metadata, and timing information
        """
        try:
            # Extract and validate arguments
            raw_input = arguments.get("input")
            if not raw_input:
                return types.CallToolResult(
                    content=[types.TextContent(
                        type="text",
                        text="Error: 'input' parameter is required"
                    )],
                    isError=True
                )

            processor = arguments.get("processor", "lite")
            task_spec_override = arguments.get("task_spec")
            output_type = arguments.get("output_type", "text")
            output_schema = arguments.get("output_schema")
            priority = arguments.get("priority", "normal")
            timeout = arguments.get("timeout")
            cache_key = arguments.get("cache_key")
            context_id = arguments.get("context_id")

            # Normalize input payload
            if isinstance(raw_input, dict):
                structured_input = raw_input
            else:
                structured_input = {"query": str(raw_input)}

            # Build task spec
            if task_spec_override is not None:
                if not isinstance(task_spec_override, dict):
                    return types.CallToolResult(
                        content=[types.TextContent(
                            type="text",
                            text="Error: 'task_spec' must be a JSON object"
                        )],
                        isError=True
                    )
                task_spec = task_spec_override
            elif output_schema or output_type:
                task_spec = {
                    "output_schema": {
                        "type": output_type,
                        **(output_schema or {})
                    }
                }
            else:
                task_spec = None

            # Create execution request
            request = TaskExecutionRequest(
                processor=processor,
                input=structured_input,
                task_spec=task_spec,
                priority=TaskPriority(priority),
                timeout=timeout,
                idempotency_key=cache_key,
                context_id=context_id,
                tags={
                    "source": "mcp",
                    "tool": "parallel_research_task"
                }
            )

            # Execute with orchestrator
            logger.info(
                f"Executing research task: processor={processor}, "
                f"priority={priority}, timeout={timeout or 'default'}"
            )

            result = await orchestrator.execute_task(request)

            # Format response
            response_parts = []

            # Main output
            if result.output_text:
                response_parts.append(f"# Research Results\n\n{result.output_text}")
            elif result.output_json:
                import json
                response_parts.append(
                    f"# Research Results\n\n```json\n{json.dumps(result.output_json, indent=2)}\n```"
                )
            else:
                response_parts.append("# Research Results\n\nNo output generated")

            # Metadata
            metadata = f"""
---
**Execution Metadata:**
- Run ID: `{result.run_id}`
- Status: {result.final_status}
- Processor: {processor}
- Duration: {result.elapsed_seconds:.2f}s
- Polls: {result.poll_count}
- Retries: {result.retry_count}
"""

            if result.estimated_cost:
                metadata += f"- Estimated Cost: ${result.estimated_cost:.6f}\n"

            if result.cached:
                metadata += "- ✓ Result from cache\n"

            if result.circuit_breaker_status:
                metadata += f"- Circuit Breaker: {result.circuit_breaker_status}\n"

            response_parts.append(metadata)

            # Error information if failed
            if result.error:
                response_parts.append(f"\n**Error:** {result.error}")

            full_response = "\n".join(response_parts)

            return types.CallToolResult(
                content=[types.TextContent(
                    type="text",
                    text=full_response
                )],
                isError=(result.status.value in ["failed", "timeout", "circuit_open"])
            )

        except Exception as e:
            logger.error(f"Task execution failed: {e}", exc_info=True)
            return types.CallToolResult(
                content=[types.TextContent(
                    type="text",
                    text=f"Task execution error: {str(e)}"
                )],
                isError=True
            )

    async def parallel_batch_research(arguments: Dict[str, Any]) -> types.CallToolResult:
        """
        Execute multiple research tasks in parallel with automatic orchestration.

        Perfect for scenarios like:
        - Researching multiple companies/topics simultaneously
        - Comparing different approaches to the same problem
        - Gathering data from multiple sources concurrently

        Args from MCP client:
            tasks (list): List of task configurations, each with:
                - input (dict | str): Task input object preferred by the Parallel.ai API (e.g., {"query": "..."})
                    or a string that is automatically wrapped into {"query": "<text>"}.
                - processor (str, optional): Processor tier
                - task_spec (dict, optional): JSON object defining the output schema for this run
                - output_type (str, optional): "text" or "json" (used when task_spec is omitted)
                - priority (str, optional): Task priority
            global_timeout (int, optional): Timeout for entire batch
            fail_fast (bool, optional): Stop on first failure (default: false)

        Returns:
            Results from all tasks with aggregated metadata
        """
        try:
            import asyncio
            import json

            tasks_config = arguments.get("tasks", [])
            if not tasks_config:
                return types.CallToolResult(
                    content=[types.TextContent(
                        type="text",
                        text="Error: 'tasks' parameter is required and must be non-empty"
                    )],
                    isError=True
                )

            global_timeout = arguments.get("global_timeout")
            fail_fast = arguments.get("fail_fast", False)

            logger.info(f"Executing batch of {len(tasks_config)} research tasks")

            # Create execution requests
            execution_tasks = []
            for idx, task_config in enumerate(tasks_config):
                raw_input = task_config["input"]
                if isinstance(raw_input, dict):
                    structured_input = raw_input
                else:
                    structured_input = {"query": str(raw_input)}

                task_spec_override = task_config.get("task_spec")
                if task_spec_override is not None and not isinstance(task_spec_override, dict):
                    return types.CallToolResult(
                        content=[types.TextContent(
                            type="text",
                            text=f"Error: task_spec for task {idx + 1} must be a JSON object"
                        )],
                        isError=True
                    )

                request = TaskExecutionRequest(
                    processor=task_config.get("processor", "lite"),
                    input=structured_input,
                    task_spec=task_spec_override,
                    priority=TaskPriority(task_config.get("priority", "normal")),
                    timeout=task_config.get("timeout"),
                    context_id=task_config.get("context_id"),
                    tags={
                        "source": "mcp",
                        "tool": "parallel_batch_research",
                        "batch_index": str(idx)
                    }
                )
                execution_tasks.append(orchestrator.execute_task(request))

            # Execute all tasks
            if fail_fast:
                results = await asyncio.gather(*execution_tasks)
            else:
                results = await asyncio.gather(*execution_tasks, return_exceptions=True)

            # Format response
            response_parts = [f"# Batch Research Results ({len(results)} tasks)\n"]

            total_duration = 0.0
            total_cost = 0.0
            success_count = 0
            failed_count = 0

            for idx, result in enumerate(results):
                task_input = tasks_config[idx]["input"]
                if isinstance(task_input, dict):
                    input_preview = json.dumps(task_input, ensure_ascii=False)
                else:
                    input_preview = str(task_input)
                response_parts.append(f"\n## Task {idx + 1}: {input_preview[:60]}...")

                if isinstance(result, Exception):
                    response_parts.append(f"\n**Status:** Failed\n**Error:** {str(result)}")
                    failed_count += 1
                else:
                    if result.output_text:
                        preview = result.output_text[:200]
                        response_parts.append(f"\n**Output:** {preview}...")
                    elif result.output_json:
                        preview = json.dumps(result.output_json, indent=2)[:200]
                        response_parts.append(f"\n**Output:**\n```json\n{preview}...\n```")

                    response_parts.append(
                        f"\n**Status:** {result.final_status} | "
                        f"Duration: {result.elapsed_seconds:.1f}s | "
                        f"Run ID: `{result.run_id}`"
                    )

                    total_duration += result.elapsed_seconds
                    if result.estimated_cost:
                        total_cost += result.estimated_cost

                    if result.final_status == "completed":
                        success_count += 1
                    else:
                        failed_count += 1

            # Summary
            response_parts.append(f"""
---
**Batch Summary:**
- Total Tasks: {len(results)}
- Successful: {success_count}
- Failed: {failed_count}
- Total Duration: {total_duration:.2f}s
- Estimated Total Cost: ${total_cost:.6f}
""")

            return types.CallToolResult(
                content=[types.TextContent(
                    type="text",
                    text="\n".join(response_parts)
                )],
                isError=(failed_count > 0 if fail_fast else False)
            )

        except Exception as e:
            logger.error(f"Batch execution failed: {e}", exc_info=True)
            return types.CallToolResult(
                content=[types.TextContent(
                    type="text",
                    text=f"Batch execution error: {str(e)}"
                )],
                isError=True
            )

    async def parallel_get_stats(arguments: Dict[str, Any]) -> types.CallToolResult:
        """
        Get orchestrator statistics and health metrics.

        Returns:
            Current orchestrator state including:
            - Active tasks
            - Queue depth
            - Success/failure rates
            - Circuit breaker status
            - Performance metrics
        """
        try:
            stats = await orchestrator.get_stats()

            response = f"""# Orchestrator Statistics

**Queue Status:**
- Queued: {stats.total_queued}
- Running: {stats.total_running}
- Completed: {stats.total_completed}
- Failed: {stats.total_failed}
- Active Connections: {stats.active_connections}

**Performance:**
- Success Rate: {stats.success_rate * 100 if stats.success_rate else 0:.1f}%
- Avg Wait Time: {stats.avg_wait_time if stats.avg_wait_time else 'N/A'}
- Avg Execution Time: {stats.avg_execution_time if stats.avg_execution_time else 'N/A'}

**Circuit Breaker:**
- Status: {'OPEN (blocking requests)' if stats.circuit_breaker_open else 'CLOSED (healthy)'}

**By Priority:**
{chr(10).join(f'- {k}: {v}' for k, v in stats.by_priority.items()) if stats.by_priority else '- None'}

**By Processor:**
{chr(10).join(f'- {k}: {v}' for k, v in stats.by_processor.items()) if stats.by_processor else '- None'}
"""

            return types.CallToolResult(
                content=[types.TextContent(type="text", text=response)]
            )

        except Exception as e:
            logger.error(f"Failed to get stats: {e}", exc_info=True)
            return types.CallToolResult(
                content=[types.TextContent(
                    type="text",
                    text=f"Error retrieving stats: {str(e)}"
                )],
                isError=True
            )

    # Return MCP tool definitions
    return [
        Tool(
            name="parallel_research_task",
            description=(
                "Execute a research task on Parallel.ai with automatic polling and result retrieval. "
                "Accepts a structured input object (e.g., {'query': '...'}). "
                "Use task_spec to declare structured outputs or rely on output_type/output_schema for quick text/json responses. "
                "Supports multiple processor tiers, priority levels, and caching, and never requires users to visit external URLs."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "input": {
                        "type": "object",
                        "description": "Structured task input (e.g., {'query': 'Describe...'}).",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Human-readable prompt"
                            }
                        },
                        "required": ["query"],
                        "additionalProperties": True
                    },
                    "processor": {
                        "type": "string",
                        "enum": ["lite", "base", "core", "pro", "ultra", "ultra2x"],
                        "default": "lite",
                        "description": "Processor tier (affects speed, quality, and cost)"
                    },
                    "output_type": {
                        "type": "string",
                        "enum": ["text", "json"],
                        "default": "text",
                        "description": "Output format"
                    },
                    "output_schema": {
                        "type": "object",
                        "description": "JSON schema for structured output (when output_type=json)"
                    },
                    "task_spec": {
                        "type": "object",
                        "description": "Full task specification object (input/output schema) sent directly to Parallel.ai."
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["critical", "high", "normal", "low", "background"],
                        "default": "normal",
                        "description": "Task execution priority"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Maximum execution time in seconds (default: 900)"
                    },
                    "cache_key": {
                        "type": "string",
                        "description": "Optional key for caching/deduplication"
                    },
                    "context_id": {
                        "type": "string",
                        "description": "Optional context ID for state persistence"
                    }
                },
                "required": ["input"]
            },
            handler=parallel_research_task
        ),
        Tool(
            name="parallel_batch_research",
            description=(
                "Execute multiple research tasks in parallel with automatic orchestration. "
                "Each task accepts a structured input object (e.g., {'query': '...'}) and optional task_spec. "
                "Perfect for comparative research, multi-topic analysis, or high-throughput workloads. "
                "All results are aggregated and returned inline."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "input": {
                                    "type": "object",
                                    "description": "Structured input for the task",
                                    "properties": {
                                        "query": {"type": "string"}
                                    },
                                    "required": ["query"],
                                    "additionalProperties": True
                                },
                                "processor": {"type": "string", "enum": ["lite", "base", "core", "pro", "ultra", "ultra2x"]},
                                "task_spec": {
                                    "type": "object",
                                    "description": "Optional per-task task_spec JSON object describing outputs"
                                },
                                "output_type": {"type": "string", "enum": ["text", "json"]},
                                "priority": {"type": "string", "enum": ["critical", "high", "normal", "low", "background"]}
                            },
                            "required": ["input"]
                        },
                        "description": "List of task configurations to execute"
                    },
                    "global_timeout": {
                        "type": "integer",
                        "description": "Timeout for entire batch in seconds"
                    },
                    "fail_fast": {
                        "type": "boolean",
                        "default": False,
                        "description": "Stop execution on first failure"
                    }
                },
                "required": ["tasks"]
            },
            handler=parallel_batch_research
        ),
        Tool(
            name="parallel_get_stats",
            description="Get orchestrator statistics including queue depth, success rates, and circuit breaker status",
            inputSchema={
                "type": "object",
                "properties": {}
            },
            handler=parallel_get_stats
        )
    ]


__all__ = ["create_orchestrated_mcp_tools"]
