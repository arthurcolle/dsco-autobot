"""
Functional Composition Patterns - Clean, Intentional API Design

Based on well-established functional programming patterns that AI agents understand.
Each endpoint has a clear, single purpose.
"""
from __future__ import annotations

import asyncio
import json
import logging
import inspect
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import httpx

from ..registry import get_registry
from ..queue import get_task_queue
from ..compound.pipelines import PipelineEvent, PipelineEventType
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/compose", tags=["compose"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class SeqRequest(BaseModel):
    """Sequential composition: a → b → c"""
    tools: List[str] = Field(..., description="Tools to execute in sequence")
    arguments: List[Dict[str, Any]] = Field(default_factory=list, description="Arguments for each tool")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Initial inputs")


class MapRequest(BaseModel):
    """Map: Apply tool to each item"""
    tool: str = Field(..., description="Tool to apply to each item")
    items: List[Any] = Field(..., description="Items to process")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Additional arguments for the tool")


class ReduceRequest(BaseModel):
    """Reduce: Aggregate items"""
    tool: str = Field(..., description="Aggregation tool")
    items: List[Any] = Field(..., description="Items to aggregate")
    initial: Optional[Any] = Field(None, description="Initial accumulator value")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Additional arguments")


class ForkRequest(BaseModel):
    """Fork: Run tools in parallel, join results"""
    tools: List[str] = Field(..., description="Tools to run in parallel")
    arguments: List[Dict[str, Any]] = Field(default_factory=list, description="Arguments for each tool")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Shared input data")
    join_strategy: str = Field("merge", description="How to join results: merge|array|first")


class FilterRequest(BaseModel):
    """Filter: Select items matching condition"""
    predicate: str = Field(..., description="Tool that returns boolean")
    items: List[Any] = Field(..., description="Items to filter")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Additional arguments for predicate")


# =============================================================================
# TOOL EXECUTION HELPER
# =============================================================================

def _get_tool_parameter_names(tool_name: str) -> List[str]:
    """Extract parameter names from a tool using inspection"""
    registry = get_registry()
    tool = registry.get_by_name(tool_name)

    if not tool:
        return []

    # If tool has a callable, inspect its signature
    if tool._callable:
        sig = inspect.signature(tool._callable)
        return [p.name for p in sig.parameters.values()]

    # Fallback: use input schema if available
    if tool.inputs:
        return [inp.name for inp in tool.inputs]

    return []


def _smart_map_to_parameters(value: Any, target_tool_name: str) -> Dict[str, Any]:
    """
    Intelligently map a value to the parameters of a target tool.

    Uses metaprogramming to inspect the actual function signature
    and map outputs to the correct parameter names.
    """
    param_names = _get_tool_parameter_names(target_tool_name)

    # If no parameters found, return as-is
    if not param_names:
        if isinstance(value, dict):
            return value
        return {"value": value}

    # If value is a dict with "error", pass through
    if isinstance(value, dict) and "error" in value:
        return value

    # If value is a dict and has matching keys, use it directly
    if isinstance(value, dict):
        matching_keys = [k for k in value.keys() if k in param_names]
        if matching_keys:
            return value
        # Otherwise map the whole dict to first param
        return {param_names[0]: value}

    # For simple values, map to the first parameter
    return {param_names[0]: value}


async def _execute_tool_direct(tool_name: str, inputs: Dict[str, Any]) -> Any:
    """Execute a tool through the registry or via backends"""
    registry = get_registry()
    queue = get_task_queue()

    # First try to execute via registry (for built-in tools)
    if tool_name in registry.list_names():
        try:
            result = await registry.execute(tool_name, **inputs)
            return result
        except Exception as e:
            logger.error(f"Registry tool execution failed for {tool_name}: {e}")
            return {"error": str(e), "tool": tool_name}

    # If not in registry, route to appropriate backend via task queue
    try:
        # Find which backend has this tool
        backends = queue.list_backends()
        for backend in backends:
            # Check if this backend has the tool (by trying to execute it)
            if backend.is_healthy:
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            f"{backend.url}/mcp",
                            json={
                                "jsonrpc": "2.0",
                                "id": str(uuid4()),
                                "method": "tools/call",
                                "params": {
                                    "name": tool_name,
                                    "arguments": inputs
                                }
                            },
                            timeout=30.0
                        )

                        if response.status_code == 200:
                            result = response.json()
                            if "result" in result:
                                content = result["result"].get("content", [])
                                if content and len(content) > 0:
                                    text = content[0].get("text", "{}")
                                    try:
                                        return json.loads(text)
                                    except (json.JSONDecodeError, ValueError):
                                        return {"output": text}
                except Exception as backend_error:
                    logger.debug(f"Backend {backend.id} doesn't have tool {tool_name}: {backend_error}")
                    continue

        return {"error": f"Tool not found: {tool_name}", "tool": tool_name}

    except Exception as e:
        logger.error(f"Tool execution failed for {tool_name}: {e}")
        return {"error": str(e), "tool": tool_name}


# =============================================================================
# COMPOSITION PATTERNS
# =============================================================================

@router.post("/seq")
async def seq(request: SeqRequest) -> Dict[str, Any]:
    """
    Sequential composition: tool1(x) → tool2(out1) → tool3(out2)

    Output of each tool becomes input to the next tool.

    Example:
    ```json
    {
      "tools": ["add", "multiply", "uppercase"],
      "arguments": [
        {"a": 5, "b": 3},
        {"b": 2},
        {}
      ],
      "inputs": {}
    }
    ```

    This will:
    1. add(5, 3) → 8
    2. multiply(8, 2) → 16
    3. uppercase("16") → "16" (if uppercase took a number, otherwise would need string conversion)
    """
    execution_id = str(uuid4())
    current_output = request.inputs
    results = []

    try:
        for i, tool_name in enumerate(request.tools):
            # Get user-provided arguments for this step
            user_args = request.arguments[i] if i < len(request.arguments) else {}

            # Smart map previous output to this tool's parameters
            if i == 0:
                # First tool: use inputs + user args
                mapped_args = {**current_output, **user_args}
            else:
                # Subsequent tools: intelligently map previous output
                mapped_args = _smart_map_to_parameters(current_output, tool_name)
                # Merge with user-provided args (user args take precedence)
                mapped_args = {**mapped_args, **user_args}

            logger.info(f"Executing {tool_name} with args: {mapped_args}")

            result = await _execute_tool_direct(tool_name, mapped_args)
            results.append(result)

            # Set current output for next iteration
            current_output = result

        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": current_output,
            "steps": results
        }

    except Exception as e:
        logger.error(f"Sequential composition failed: {e}")
        return {
            "execution_id": execution_id,
            "status": "failed",
            "error": str(e),
            "steps": results
        }


@router.post("/map")
async def map_pattern(request: MapRequest) -> Dict[str, Any]:
    """
    Map: Apply tool to each item in collection

    [a, b, c] → [f(a), f(b), f(c)]

    Example:
    ```json
    {
      "tool": "classify",
      "items": ["doc1", "doc2", "doc3"],
      "arguments": {"categories": ["tech", "science", "business"]}
    }
    ```
    """
    execution_id = str(uuid4())
    results = []

    try:
        for item in request.items:
            # Smart map each item to the tool's parameters
            mapped_args = _smart_map_to_parameters(item, request.tool)
            # Merge with user-provided arguments
            args = {**mapped_args, **request.arguments}

            logger.info(f"Map: executing {request.tool} with {args}")
            result = await _execute_tool_direct(request.tool, args)
            results.append(result)

        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": results,
            "count": len(results)
        }

    except Exception as e:
        logger.error(f"Map failed: {e}")
        return {
            "execution_id": execution_id,
            "status": "failed",
            "error": str(e),
            "partial_results": results
        }


@router.post("/reduce")
async def reduce_pattern(request: ReduceRequest) -> Dict[str, Any]:
    """
    Reduce: Aggregate items into single result

    [a, b, c] → aggregate(a, b, c)

    Example:
    ```json
    {
      "tool": "merge_results",
      "items": [{"score": 0.8}, {"score": 0.9}, {"score": 0.7}],
      "initial": {"total": 0},
      "arguments": {"operation": "sum"}
    }
    ```
    """
    execution_id = str(uuid4())

    try:
        accumulator = request.initial if request.initial is not None else {}

        for item in request.items:
            args = {**request.arguments, "accumulator": accumulator, "item": item}
            result = await _execute_tool_direct(request.tool, args)
            accumulator = result

        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": accumulator
        }

    except Exception as e:
        logger.error(f"Reduce failed: {e}")
        return {
            "execution_id": execution_id,
            "status": "failed",
            "error": str(e)
        }


@router.post("/fork")
async def fork_pattern(request: ForkRequest) -> Dict[str, Any]:
    """
    Fork: Run tools in parallel, join results

    Fork execution to multiple parallel paths, then join results.

    Example:
    ```json
    {
      "tools": ["search_google", "search_bing", "search_duckduckgo"],
      "arguments": [
        {"query": "AI news"},
        {"query": "AI news"},
        {"query": "AI news"}
      ],
      "inputs": {},
      "join_strategy": "merge"
    }
    ```
    """
    execution_id = str(uuid4())

    try:
        tasks = []
        for i, tool_name in enumerate(request.tools):
            args = {**request.arguments[i], **request.inputs} if i < len(request.arguments) else request.inputs
            tasks.append(_execute_tool_direct(tool_name, args))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Join results based on strategy
        if request.join_strategy == "merge":
            joined = {}
            for i, result in enumerate(results):
                if isinstance(result, dict):
                    joined.update(result)
                else:
                    joined[request.tools[i]] = result
        elif request.join_strategy == "array":
            joined = results
        elif request.join_strategy == "first":
            joined = results[0] if results else None
        else:
            joined = {"results": results}

        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": joined,
            "parallel_count": len(results)
        }

    except Exception as e:
        logger.error(f"Fork failed: {e}")
        return {
            "execution_id": execution_id,
            "status": "failed",
            "error": str(e)
        }


@router.post("/filter")
async def filter_pattern(request: FilterRequest) -> Dict[str, Any]:
    """
    Filter: Select items matching predicate

    [a, b, c] → [items where predicate(item) == true]

    Example:
    ```json
    {
      "predicate": "is_relevant",
      "items": ["doc1", "doc2", "doc3"],
      "arguments": {"threshold": 0.7}
    }
    ```
    """
    execution_id = str(uuid4())
    filtered = []

    try:
        for item in request.items:
            args = {**request.arguments, "input": item}
            result = await _execute_tool_direct(request.predicate, args)

            # Check if predicate returned true
            is_match = False
            if isinstance(result, bool):
                is_match = result
            elif isinstance(result, dict) and "match" in result:
                is_match = result["match"]
            elif isinstance(result, dict) and "result" in result:
                is_match = bool(result["result"])

            if is_match:
                filtered.append(item)

        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": filtered,
            "original_count": len(request.items),
            "filtered_count": len(filtered)
        }

    except Exception as e:
        logger.error(f"Filter failed: {e}")
        return {
            "execution_id": execution_id,
            "status": "failed",
            "error": str(e),
            "partial_results": filtered
        }


@router.get("/patterns")
async def list_patterns():
    """List available composition patterns"""
    return {
        "patterns": [
            {
                "name": "seq",
                "description": "Sequential composition: a → b → c",
                "use_case": "Chain tools where output feeds into next"
            },
            {
                "name": "map",
                "description": "Apply tool to each item: [a,b,c] → [f(a),f(b),f(c)]",
                "use_case": "Process collection of items uniformly"
            },
            {
                "name": "reduce",
                "description": "Aggregate items: [a,b,c] → aggregate",
                "use_case": "Combine multiple results into one"
            },
            {
                "name": "fork",
                "description": "Parallel execution with join",
                "use_case": "Run multiple tools concurrently, merge results"
            },
            {
                "name": "filter",
                "description": "Select items matching predicate",
                "use_case": "Filter collection based on condition"
            }
        ]
    }


# =============================================================================
# STREAMING ENDPOINTS - SSE (Server-Sent Events)
# =============================================================================

@router.post("/seq/stream")
async def seq_stream(request: SeqRequest):
    """
    Sequential composition with Server-Sent Events streaming.

    Returns real-time updates as each tool executes.

    Example usage with curl:
    ```bash
    curl -N http://localhost:2016/compose/seq/stream \\
      -H "Content-Type: application/json" \\
      -d '{"tools": ["search", "analyze"], "arguments": [...], "inputs": {}}'
    ```

    Events:
    - step_started: Tool execution begins
    - step_progress: Progress updates (if available)
    - step_completed: Tool execution finished
    - pipeline_completed: All steps done
    """
    execution_id = str(uuid4())

    async def event_stream():
        """Generate SSE events for each step"""
        current_output = request.inputs
        results = []

        # Send pipeline started event
        start_event = PipelineEvent(
            event_type=PipelineEventType.PIPELINE_STARTED,
            pipeline_id=execution_id,
            pipeline_name="sequential_composition",
            data={"total_steps": len(request.tools), "tools": request.tools}
        )
        yield f"data: {start_event.model_dump_json()}\n\n"

        try:
            for i, tool_name in enumerate(request.tools):
                # Get user-provided arguments for this step
                user_args = request.arguments[i] if i < len(request.arguments) else {}

                # Smart map previous output to this tool's parameters
                if i == 0:
                    mapped_args = {**current_output, **user_args}
                else:
                    mapped_args = _smart_map_to_parameters(current_output, tool_name)
                    mapped_args = {**mapped_args, **user_args}

                # Step started event
                step_start_event = PipelineEvent(
                    event_type=PipelineEventType.STEP_STARTED,
                    pipeline_id=execution_id,
                    pipeline_name="sequential_composition",
                    step_index=i,
                    step_name=tool_name,
                    tool_name=tool_name,
                    tool_inputs=mapped_args,
                    data={"step": i + 1, "of": len(request.tools)}
                )
                yield f"data: {step_start_event.model_dump_json()}\n\n"

                # Execute tool
                import time
                start_time = time.time()
                result = await _execute_tool_direct(tool_name, mapped_args)
                duration_ms = (time.time() - start_time) * 1000

                results.append(result)
                current_output = result

                # Step completed event
                step_complete_event = PipelineEvent(
                    event_type=PipelineEventType.STEP_COMPLETED,
                    pipeline_id=execution_id,
                    pipeline_name="sequential_composition",
                    step_index=i,
                    step_name=tool_name,
                    tool_name=tool_name,
                    tool_result=result,
                    data={
                        "duration_ms": duration_ms,
                        "step": i + 1,
                        "of": len(request.tools)
                    }
                )
                yield f"data: {step_complete_event.model_dump_json()}\n\n"

            # Pipeline completed event
            complete_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_COMPLETED,
                pipeline_id=execution_id,
                pipeline_name="sequential_composition",
                data={
                    "final_result": current_output,
                    "all_steps": results,
                    "total_steps": len(results)
                }
            )
            yield f"data: {complete_event.model_dump_json()}\n\n"

        except Exception as e:
            # Pipeline failed event
            error_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_FAILED,
                pipeline_id=execution_id,
                pipeline_name="sequential_composition",
                error=str(e),
                data={"partial_results": results}
            )
            yield f"data: {error_event.model_dump_json()}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@router.post("/fork/stream")
async def fork_stream(request: ForkRequest):
    """
    Parallel composition with Server-Sent Events streaming.

    Returns real-time updates as each tool executes in parallel.
    """
    execution_id = str(uuid4())

    async def event_stream():
        """Generate SSE events for parallel execution"""
        # Send pipeline started event
        start_event = PipelineEvent(
            event_type=PipelineEventType.PIPELINE_STARTED,
            pipeline_id=execution_id,
            pipeline_name="parallel_composition",
            data={
                "total_tools": len(request.tools),
                "tools": request.tools,
                "join_strategy": request.join_strategy
            }
        )
        yield f"data: {start_event.model_dump_json()}\n\n"

        try:
            # Execute all tools in parallel with callbacks for events
            async def execute_single_tool(i: int, tool_name: str):
                """Execute a single tool and return events and result"""
                args = {**request.arguments[i], **request.inputs} if i < len(request.arguments) else request.inputs

                events = []

                # Tool started event
                tool_start_event = PipelineEvent(
                    event_type=PipelineEventType.TOOL_CALL_STARTED,
                    pipeline_id=execution_id,
                    pipeline_name="parallel_composition",
                    step_index=i,
                    tool_name=tool_name,
                    tool_inputs=args
                )
                events.append(tool_start_event)

                # Execute
                import time
                start_time = time.time()
                result = await _execute_tool_direct(tool_name, args)
                duration_ms = (time.time() - start_time) * 1000

                # Tool completed event
                tool_complete_event = PipelineEvent(
                    event_type=PipelineEventType.TOOL_CALL_COMPLETED,
                    pipeline_id=execution_id,
                    pipeline_name="parallel_composition",
                    step_index=i,
                    tool_name=tool_name,
                    tool_result=result,
                    data={"duration_ms": duration_ms}
                )
                events.append(tool_complete_event)

                return i, result, events

            # Execute all tools in parallel
            tasks = [execute_single_tool(i, tool_name) for i, tool_name in enumerate(request.tools)]
            all_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Emit events and collect results
            results = [None] * len(request.tools)
            for item in all_results:
                if isinstance(item, (Exception, BaseException)):
                    continue
                if isinstance(item, tuple) and len(item) == 3:
                    idx, result, events = item
                    results[idx] = result
                    for event in events:
                        yield f"data: {event.model_dump_json()}\n\n"

            # Join results based on strategy
            if request.join_strategy == "merge":
                joined = {}
                for i, result in enumerate(results):
                    if isinstance(result, dict):
                        joined.update(result)
                    else:
                        joined[request.tools[i]] = result
            elif request.join_strategy == "array":
                joined = results
            elif request.join_strategy == "first":
                joined = results[0] if results else None
            else:
                joined = {"results": results}

            # Pipeline completed event
            complete_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_COMPLETED,
                pipeline_id=execution_id,
                pipeline_name="parallel_composition",
                data={
                    "final_result": joined,
                    "parallel_count": len(results)
                }
            )
            yield f"data: {complete_event.model_dump_json()}\n\n"

        except Exception as e:
            # Pipeline failed event
            error_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_FAILED,
                pipeline_id=execution_id,
                pipeline_name="parallel_composition",
                error=str(e)
            )
            yield f"data: {error_event.model_dump_json()}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
