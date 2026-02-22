"""
Compositional Workflow API - Advanced Tool Composition with Schema Registry.

This API enables sophisticated multi-step workflows with:
- Registered Pydantic schemas as reusable data contracts
- LLM-mediated transformations (text → structured outputs)
- Multiple execution modes (passthrough, transform, map, reduce)
- Complex compositions (segment → classify → embed → compare)
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field, create_model
import httpx

from ..registry import get_registry
from ..queue import get_task_queue
from .workflow_library import (
    get_workflow_library,
    get_workflow_by_id,
    get_workflows_by_category,
    get_workflows_by_tag,
    NamedWorkflow,
    WorkflowCategory,
    ResourceLimits
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/compose", tags=["composition"])


# =============================================================================
# SCHEMA REGISTRY - Pydantic Models as First-Class Entities
# =============================================================================

class SchemaType(str, Enum):
    """Types of registered schemas."""
    PYDANTIC = "pydantic"
    JSON_SCHEMA = "json_schema"
    OPENAPI = "openapi"


class RegisteredSchema(BaseModel):
    """A registered Pydantic schema that can be referenced by workflows."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str = Field(..., description="Schema name for referencing")
    description: str = ""
    schema_type: SchemaType = SchemaType.PYDANTIC

    # Pydantic schema definition
    pydantic_model: Optional[Dict[str, Any]] = Field(None, description="Pydantic model definition")
    json_schema: Optional[Dict[str, Any]] = Field(None, description="JSON Schema")

    # Metadata
    version: str = "1.0.0"
    tags: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Usage tracking
    used_by_workflows: List[str] = Field(default_factory=list)
    usage_count: int = 0


# In-memory schema registry
_schema_registry: Dict[str, RegisteredSchema] = {}


# =============================================================================
# COMPOSITION EXECUTION MODES
# =============================================================================

class ExecutionMode(str, Enum):
    """How data flows between steps in a composition."""
    PASSTHROUGH = "passthrough"      # Direct: output → input
    TRANSFORM = "transform"          # LLM: text → structured output
    MAP = "map"                      # Apply to each element
    REDUCE = "reduce"                # Aggregate results
    FILTER = "filter"                # Filter elements
    SEGMENT = "segment"              # Split input into parts
    CLASSIFY = "classify"            # Classify each element
    EMBED = "embed"                  # Generate embeddings
    COMPARE = "compare"              # Compare pairs
    MERGE = "merge"                  # Merge multiple inputs


class TransformConfig(BaseModel):
    """Configuration for LLM-mediated transformation."""
    llm_provider: str = "anthropic"  # anthropic, openai, etc.
    model: str = "claude-3-sonnet-20240229"
    prompt_template: Optional[str] = None
    output_schema: Optional[str] = None  # Reference to registered schema
    temperature: float = 0.0
    max_tokens: int = 4000


# =============================================================================
# WORKFLOW COMPOSITION MODELS
# =============================================================================

class CompositionStep(BaseModel):
    """A single step in a compositional workflow."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""

    # Execution
    mode: ExecutionMode
    tool: Optional[str] = None  # Tool to execute

    # Data transformation
    input_schema: Optional[str] = None   # Reference to registered schema
    output_schema: Optional[str] = None  # Reference to registered schema
    transform_config: Optional[TransformConfig] = None

    # Input mapping
    input_from: Optional[str] = None  # Step ID to get input from
    input_mapping: Dict[str, str] = Field(default_factory=dict)  # Map outputs to inputs

    # Conditional execution
    condition: Optional[str] = None  # Python expression

    # Metadata
    timeout_seconds: float = 300.0
    retry_on_failure: bool = False
    max_retries: int = 0


class WorkflowComposition(BaseModel):
    """A complete compositional workflow definition."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""
    version: str = "1.0.0"

    # Steps
    steps: List[CompositionStep] = Field(default_factory=list)

    # Entry point
    entry_step: Optional[str] = None  # Step ID to start with

    # Global settings
    timeout_seconds: float = 600.0
    max_parallel_steps: int = 5

    # Schema references
    input_schema: Optional[str] = None
    output_schema: Optional[str] = None

    # Metadata
    tags: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Execution stats
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0


# In-memory workflow registry
_workflow_registry: Dict[str, WorkflowComposition] = {}


class CompositionExecutionRequest(BaseModel):
    """Request to execute a composition."""
    workflow_id: Optional[str] = None
    workflow: Optional[WorkflowComposition] = None  # Inline workflow
    inputs: Dict[str, Any] = Field(default_factory=dict)
    stream: bool = False


class CompositionExecutionResponse(BaseModel):
    """Response from composition execution."""
    execution_id: str
    workflow_id: str
    workflow_name: str
    status: str  # pending, running, completed, failed
    output: Any = None
    step_results: Dict[str, Any] = Field(default_factory=dict)
    duration_ms: float = 0
    error: Optional[str] = None


# =============================================================================
# SCHEMA REGISTRY ROUTES
# =============================================================================

@router.post("/schemas", response_model=RegisteredSchema)
async def register_schema(
    name: str,
    schema: Dict[str, Any],
    description: str = "",
    schema_type: SchemaType = SchemaType.JSON_SCHEMA,
    tags: List[str] = None,
):
    """
    Register a Pydantic schema for use in workflows.

    Example JSON Schema:
    ```json
    {
      "type": "object",
      "properties": {
        "title": {"type": "string"},
        "segments": {
          "type": "array",
          "items": {"type": "string"}
        }
      },
      "required": ["title"]
    }
    ```
    """
    registered = RegisteredSchema(
        name=name,
        description=description,
        schema_type=schema_type,
        json_schema=schema,
        tags=tags or []
    )

    _schema_registry[registered.id] = registered

    logger.info(f"Registered schema: {name} ({registered.id})")

    return registered


@router.get("/schemas", response_model=List[RegisteredSchema])
async def list_schemas(
    tag: Optional[str] = None,
    limit: int = 100
):
    """List all registered schemas."""
    schemas = list(_schema_registry.values())

    if tag:
        schemas = [s for s in schemas if tag in s.tags]

    return schemas[:limit]


@router.get("/schemas/{schema_id}", response_model=RegisteredSchema)
async def get_schema(schema_id: str):
    """Get a registered schema by ID."""
    if schema_id not in _schema_registry:
        raise HTTPException(status_code=404, detail=f"Schema not found: {schema_id}")

    return _schema_registry[schema_id]


@router.get("/schemas/by-name/{name}", response_model=RegisteredSchema)
async def get_schema_by_name(name: str):
    """Get a registered schema by name."""
    for schema in _schema_registry.values():
        if schema.name == name:
            return schema

    raise HTTPException(status_code=404, detail=f"Schema not found: {name}")


# =============================================================================
# WORKFLOW COMPOSITION ROUTES
# =============================================================================

@router.post("/workflows", response_model=WorkflowComposition)
async def create_workflow(workflow: WorkflowComposition):
    """
    Create a compositional workflow.

    Example: Segment → Classify → Embed → Compare
    ```python
    {
      "name": "document_analysis",
      "steps": [
        {
          "name": "segment",
          "mode": "segment",
          "tool": "text_splitter",
          "output_schema": "text_segment"
        },
        {
          "name": "classify",
          "mode": "map",
          "tool": "classifier",
          "input_from": "segment",
          "output_schema": "classification"
        },
        {
          "name": "embed",
          "mode": "map",
          "tool": "embedder",
          "input_from": "segment"
        },
        {
          "name": "compare",
          "mode": "compare",
          "tool": "similarity",
          "input_from": "embed"
        }
      ]
    }
    ```
    """
    # Validate schema references
    for step in workflow.steps:
        if step.input_schema and step.input_schema not in [s.name for s in _schema_registry.values()]:
            raise HTTPException(400, f"Unknown input schema: {step.input_schema}")
        if step.output_schema and step.output_schema not in [s.name for s in _schema_registry.values()]:
            raise HTTPException(400, f"Unknown output schema: {step.output_schema}")

    _workflow_registry[workflow.id] = workflow

    logger.info(f"Created workflow: {workflow.name} ({workflow.id})")

    return workflow


@router.get("/workflows", response_model=List[WorkflowComposition])
async def list_workflows(
    tag: Optional[str] = None,
    limit: int = 100
):
    """List all registered workflows."""
    workflows = list(_workflow_registry.values())

    if tag:
        workflows = [w for w in workflows if tag in w.tags]

    return workflows[:limit]


@router.get("/workflows/{workflow_id}", response_model=WorkflowComposition)
async def get_workflow(workflow_id: str):
    """Get a workflow by ID."""
    if workflow_id not in _workflow_registry:
        raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")

    return _workflow_registry[workflow_id]


# =============================================================================
# WORKFLOW EXECUTION
# =============================================================================

@router.post("/execute", response_model=CompositionExecutionResponse)
async def execute_composition(
    request: CompositionExecutionRequest,
    background_tasks: BackgroundTasks
):
    """
    Execute a compositional workflow.

    Supports complex multi-step patterns like:
    - text → segment → classify → embed → compare
    - input → transform (LLM) → validate → store
    - data → map → filter → reduce → output
    """
    # Get workflow
    if request.workflow_id:
        if request.workflow_id not in _workflow_registry:
            raise HTTPException(404, f"Workflow not found: {request.workflow_id}")
        workflow = _workflow_registry[request.workflow_id]
    elif request.workflow:
        workflow = request.workflow
    else:
        raise HTTPException(400, "Either workflow_id or workflow must be provided")

    execution_id = str(uuid4())

    # Execute workflow
    try:
        result = await _execute_workflow(workflow, request.inputs, execution_id)

        # Apply output guard
        try:
            from ..guards.output_guard import apply_output_guard
            result = apply_output_guard(
                result,
                tool_name=f"compose:{workflow.name}",
                source_context={"workflow_id": workflow.id},
            )
        except ImportError:
            pass

        # Update stats
        workflow.execution_count += 1
        workflow.success_count += 1

        return CompositionExecutionResponse(
            execution_id=execution_id,
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status="completed",
            output=result.get("output"),
            step_results=result.get("step_results", {}),
            duration_ms=result.get("duration_ms", 0)
        )

    except Exception as e:
        workflow.execution_count += 1
        workflow.failure_count += 1

        logger.error(f"Workflow execution failed: {e}")

        return CompositionExecutionResponse(
            execution_id=execution_id,
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status="failed",
            error=str(e),
            duration_ms=0
        )


async def _execute_workflow(
    workflow: WorkflowComposition,
    inputs: Dict[str, Any],
    execution_id: str
) -> Dict[str, Any]:
    """Execute a workflow composition."""
    import time
    start_time = time.time()

    step_results = {}
    current_data = inputs

    # Execute steps in order
    for step in workflow.steps:
        logger.info(f"Executing step: {step.name} (mode={step.mode})")

        # Get input data
        if step.input_from and step.input_from in step_results:
            current_data = step_results[step.input_from]

        # Apply input mapping
        if step.input_mapping:
            mapped_data = {}
            for target_key, source_key in step.input_mapping.items():
                if source_key in current_data:
                    mapped_data[target_key] = current_data[source_key]
            current_data = mapped_data

        # Execute based on mode
        result = await _execute_step(step, current_data, step_results)

        step_results[step.id] = result
        current_data = result

    duration_ms = (time.time() - start_time) * 1000

    return {
        "output": current_data,
        "step_results": step_results,
        "duration_ms": duration_ms
    }


async def _execute_step(
    step: CompositionStep,
    input_data: Any,
    previous_results: Dict[str, Any]
) -> Any:
    """Execute a single step in the workflow."""

    if step.mode == ExecutionMode.PASSTHROUGH:
        # Direct passthrough
        if step.tool:
            return await _execute_tool(step.tool, input_data)
        return input_data

    elif step.mode == ExecutionMode.TRANSFORM:
        # LLM-mediated transformation
        if not step.transform_config:
            raise ValueError(f"Transform step {step.name} requires transform_config")

        return await _llm_transform(input_data, step.transform_config)

    elif step.mode == ExecutionMode.MAP:
        # Apply operation to each element
        if not isinstance(input_data, list):
            input_data = [input_data]

        results = []
        for item in input_data:
            if step.tool:
                result = await _execute_tool(step.tool, item)
            else:
                result = item
            results.append(result)

        return results

    elif step.mode == ExecutionMode.SEGMENT:
        # Split input into segments
        if step.tool:
            return await _execute_tool(step.tool, input_data)

        # Default: split text by paragraphs
        if isinstance(input_data, str):
            return [seg.strip() for seg in input_data.split("\n\n") if seg.strip()]

        return input_data

    elif step.mode == ExecutionMode.CLASSIFY:
        # Classify input
        if step.tool:
            return await _execute_tool(step.tool, input_data)

        # Default: use LLM to classify
        if step.transform_config:
            return await _llm_transform(input_data, step.transform_config)

        return {"classification": "unknown"}

    elif step.mode == ExecutionMode.EMBED:
        # Generate embeddings
        if step.tool:
            return await _execute_tool(step.tool, input_data)

        # Fallback: return placeholder
        return {"embeddings": [], "note": "No embedding tool configured"}

    elif step.mode == ExecutionMode.COMPARE:
        # Compare pairs
        if not isinstance(input_data, list):
            raise ValueError(f"Compare mode requires list input")

        if step.tool:
            return await _execute_tool(step.tool, input_data)

        # Default: pairwise comparison placeholder
        comparisons = []
        for i in range(len(input_data)):
            for j in range(i + 1, len(input_data)):
                comparisons.append({
                    "pair": [i, j],
                    "similarity": 0.5  # Placeholder
                })

        return comparisons

    elif step.mode == ExecutionMode.REDUCE:
        # Aggregate results
        if step.tool:
            return await _execute_tool(step.tool, input_data)

        # Default: collect all
        return input_data

    elif step.mode == ExecutionMode.MERGE:
        # Merge multiple inputs
        merged = {}
        if isinstance(input_data, dict):
            merged.update(input_data)

        for step_id, result in previous_results.items():
            if isinstance(result, dict):
                merged.update(result)

        return merged

    else:
        return input_data


async def _execute_tool(tool_name: str, inputs: Any) -> Any:
    """Execute a registered tool via the persistent tool store registry."""
    from ..persistence import get_tool_store

    # Convert inputs to dict if needed
    if not isinstance(inputs, dict):
        inputs = {"input": inputs}

    store = get_tool_store()

    try:
        # Resolve tool to its backend via DuckDB registry
        backend_id, backend_url, resolved_name = store.resolve_tool(tool_name)

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{backend_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": str(uuid4()),
                    "method": "tools/call",
                    "params": {
                        "name": resolved_name,
                        "arguments": inputs
                    }
                },
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

            return {"error": f"Tool execution failed with status {response.status_code}"}

    except ValueError as e:
        # Tool or backend not found in registry
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Tool execution error: {str(e)}"}


async def _llm_transform(
    input_text: Any,
    config: TransformConfig
) -> Dict[str, Any]:
    """Transform text input to structured output using LLM.

    Supports multiple providers:
    - anthropic: Claude models via Anthropic API
    - openai: GPT models via OpenAI API
    - openrouter: Any model via OpenRouter API

    Args:
        input_text: Text or dict to transform
        config: TransformConfig with provider, model, prompt settings

    Returns:
        Dict with extracted structured data
    """
    import os

    # Convert input to text
    if isinstance(input_text, dict):
        text = json.dumps(input_text, indent=2)
    else:
        text = str(input_text)

    # Build prompt
    base_prompt = config.prompt_template or "Extract structured information from the following text."

    # Get output schema if specified
    output_schema_str = ""
    if config.output_schema:
        schema = _schema_registry.get(config.output_schema)
        if schema:
            output_schema_str = f"\n\nOutput should match this JSON schema:\n{json.dumps(schema.json_schema, indent=2)}"

    full_prompt = f"""{base_prompt}
{output_schema_str}

Text:
{text}

Respond with valid JSON only, no additional text or markdown."""

    try:
        if config.llm_provider == "anthropic":
            result = await _call_anthropic(full_prompt, config)
        elif config.llm_provider == "openai":
            result = await _call_openai(full_prompt, config)
        elif config.llm_provider == "openrouter":
            result = await _call_openrouter(full_prompt, config)
        else:
            # Default to OpenRouter which supports many models
            result = await _call_openrouter(full_prompt, config)

        return {
            "extracted": result,
            "raw_text": text,
            "model": config.model,
            "provider": config.llm_provider
        }

    except Exception as e:
        logger.error(f"LLM transform failed: {e}")
        return {
            "error": str(e),
            "raw_text": text,
            "model": config.model,
            "provider": config.llm_provider
        }


async def _call_anthropic(prompt: str, config: TransformConfig) -> Dict[str, Any]:
    """Call Anthropic API for LLM transform."""
    import os

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": config.model,
                "max_tokens": config.max_tokens,
                "temperature": config.temperature,
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        response.raise_for_status()
        data = response.json()

        # Extract text content
        content = data.get("content", [])
        if content and len(content) > 0:
            text = content[0].get("text", "{}")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                # Try to extract JSON from text
                return _extract_json_from_text(text)

        return {}


async def _call_openai(prompt: str, config: TransformConfig) -> Dict[str, Any]:
    """Call OpenAI API for LLM transform."""
    import os

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": config.model,
                "max_tokens": config.max_tokens,
                "temperature": config.temperature,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"}
            }
        )
        response.raise_for_status()
        data = response.json()

        # Extract content
        choices = data.get("choices", [])
        if choices:
            text = choices[0].get("message", {}).get("content", "{}")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return _extract_json_from_text(text)

        return {}


async def _call_openrouter(prompt: str, config: TransformConfig) -> Dict[str, Any]:
    """Call OpenRouter API for LLM transform."""
    import os

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY environment variable not set")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "http://localhost:2016",
                "X-Title": "ToolManagement LLM Transform",
                "Content-Type": "application/json"
            },
            json={
                "model": config.model,
                "max_tokens": config.max_tokens,
                "temperature": config.temperature,
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        response.raise_for_status()
        data = response.json()

        # Extract content
        choices = data.get("choices", [])
        if choices:
            text = choices[0].get("message", {}).get("content", "{}")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return _extract_json_from_text(text)

        return {}


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    """Extract JSON from text that may contain other content."""
    import re

    # Try to find JSON block in markdown code fence
    json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find JSON object directly
    json_match = re.search(r'\{[\s\S]*\}', text)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # Try to find JSON array
    json_match = re.search(r'\[[\s\S]*\]', text)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # Return as wrapped text if no JSON found
    return {"text": text}


# =============================================================================
# SIMPLE BATCH COMPOSITION - Just Provide Tools + Workflow Type
# =============================================================================

class WorkflowType(str, Enum):
    """Common workflow control flow patterns."""
    SEQUENTIAL = "sequential"      # tool1 → tool2 → tool3
    MAP = "map"                    # Apply same tool to each input element
    REDUCE = "reduce"              # Aggregate results from multiple calls
    MAP_REDUCE = "map_reduce"      # Map then reduce
    PARALLEL = "parallel"          # Run all tools in parallel, merge results
    SEGMENT_PROCESS = "segment_process"  # Segment → Process each → Aggregate
    CLASSIFY_ROUTE = "classify_route"    # Classify → Route to different tools
    PIPE = "pipe"                  # Functional composition: f(g(h(x)))


class SimpleComposition(BaseModel):
    """Simple composition - just tools, args, and workflow type."""
    workflow_type: WorkflowType
    tools: List[str] = Field(..., description="Tool names to execute")
    arguments: List[Dict[str, Any]] = Field(default_factory=list, description="Arguments for each tool")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Initial inputs")

    # Optional configuration
    timeout_seconds: float = 300.0
    parallel_limit: int = 5


@router.post("/compose-simple", response_model=Dict[str, Any])
async def compose_simple(composition: SimpleComposition):
    """
    Simple composition endpoint - just provide tools and workflow type!

    Examples:

    **Sequential**: tool1(x) → tool2 → tool3
    ```json
    {
      "workflow_type": "sequential",
      "tools": ["search", "extract", "summarize"],
      "arguments": [
        {"query": "AI trends"},
        {"schema": "article"},
        {"max_length": 100}
      ],
      "inputs": {}
    }
    ```

    **Map**: Apply same tool to each element
    ```json
    {
      "workflow_type": "map",
      "tools": ["classify"],
      "arguments": [{"categories": ["tech", "science", "business"]}],
      "inputs": {"items": ["text1", "text2", "text3"]}
    }
    ```

    **Map-Reduce**: Process elements, then aggregate
    ```json
    {
      "workflow_type": "map_reduce",
      "tools": ["embed", "aggregate"],
      "arguments": [{}, {"method": "average"}],
      "inputs": {"texts": ["doc1", "doc2", "doc3"]}
    }
    ```

    **Segment-Process**: Split → Process → Aggregate
    ```json
    {
      "workflow_type": "segment_process",
      "tools": ["segment", "classify", "summarize"],
      "arguments": [
        {"chunk_size": 500},
        {},
        {"combine": true}
      ],
      "inputs": {"text": "long document..."}
    }
    ```
    """
    execution_id = str(uuid4())

    try:
        result = await _execute_simple_composition(composition, execution_id)

        # Apply output guard to composition results
        try:
            from ..guards.output_guard import apply_output_guard
            result = apply_output_guard(
                result,
                tool_name=f"compose:{composition.workflow_type.value}",
                source_context={"workflow": composition.workflow_type.value, "tools": composition.tools},
            )
        except ImportError:
            pass

        return {
            "execution_id": execution_id,
            "workflow_type": composition.workflow_type.value,
            "status": "completed",
            "result": result,
            "tools_executed": composition.tools
        }

    except Exception as e:
        logger.error(f"Simple composition failed: {e}")
        return {
            "execution_id": execution_id,
            "workflow_type": composition.workflow_type.value,
            "status": "failed",
            "error": str(e),
            "tools_executed": composition.tools
        }


async def _execute_simple_composition(
    composition: SimpleComposition,
    execution_id: str
) -> Any:
    """Execute a simple composition based on workflow type."""

    workflow_type = composition.workflow_type
    tools = composition.tools
    arguments = composition.arguments
    inputs = composition.inputs

    # Ensure arguments list matches tools list
    while len(arguments) < len(tools):
        arguments.append({})

    if workflow_type == WorkflowType.SEQUENTIAL:
        # Sequential: tool1(input) → tool2(output1) → tool3(output2)
        current_data = inputs
        results = []

        for i, tool_name in enumerate(tools):
            args = {**arguments[i], **current_data} if isinstance(current_data, dict) else arguments[i]
            result = await _execute_tool(tool_name, args)
            results.append(result)
            current_data = result

        return {"final_output": current_data, "intermediate": results}

    elif workflow_type == WorkflowType.MAP:
        # Map: Apply same tool to each input element
        if not tools:
            raise ValueError("MAP requires at least one tool")

        tool_name = tools[0]
        tool_args = arguments[0] if arguments else {}

        # Get list of items to map over
        items = inputs.get("items", [])
        if not isinstance(items, list):
            items = [items]

        # Apply tool to each item in parallel (with timeout guard)
        map_tasks = [
            _execute_tool(tool_name, {**tool_args, "input": item})
            for item in items
        ]
        try:
            raw_results = await asyncio.wait_for(
                asyncio.gather(*map_tasks, return_exceptions=True),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            raw_results = [TimeoutError(f"Map phase exceeded 300s timeout")]
        results = [
            {"error": str(r)} if isinstance(r, Exception) else r
            for r in raw_results
        ]

        return {"mapped_results": results, "count": len(results)}

    elif workflow_type == WorkflowType.REDUCE:
        # Reduce: Aggregate multiple results in parallel
        if not tools:
            raise ValueError("REDUCE requires at least one tool")

        reduce_tasks = [
            _execute_tool(tool_name, arguments[i] if i < len(arguments) else {})
            for i, tool_name in enumerate(tools)
        ]
        try:
            raw_results = await asyncio.wait_for(
                asyncio.gather(*reduce_tasks, return_exceptions=True),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            raw_results = [TimeoutError(f"Reduce phase exceeded 300s timeout")]
        results = [
            {"error": str(r)} if isinstance(r, Exception) else r
            for r in raw_results
        ]

        return {"aggregated": results, "count": len(results)}

    elif workflow_type == WorkflowType.MAP_REDUCE:
        # Map-Reduce: Apply tool1 to each element, then reduce with tool2
        if len(tools) < 2:
            raise ValueError("MAP_REDUCE requires at least 2 tools (map, reduce)")

        map_tool = tools[0]
        reduce_tool = tools[1]

        map_args = arguments[0] if arguments else {}
        reduce_args = arguments[1] if len(arguments) > 1 else {}

        # Map phase
        items = inputs.get("items", [])
        if not isinstance(items, list):
            items = [items]

        mr_tasks = [
            _execute_tool(map_tool, {**map_args, "input": item})
            for item in items
        ]
        try:
            raw_map_results = await asyncio.wait_for(
                asyncio.gather(*mr_tasks, return_exceptions=True),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            raw_map_results = [TimeoutError(f"Map-reduce map phase exceeded 300s timeout")]
        map_results = [
            {"error": str(r)} if isinstance(r, Exception) else r
            for r in raw_map_results
        ]

        # Reduce phase
        reduce_input = {**reduce_args, "items": map_results}
        final_result = await _execute_tool(reduce_tool, reduce_input)

        return {
            "map_results": map_results,
            "reduced_result": final_result,
            "count": len(map_results)
        }

    elif workflow_type == WorkflowType.PARALLEL:
        # Parallel: Run all tools concurrently, merge results
        tasks = []
        for i, tool_name in enumerate(tools):
            args = {**arguments[i], **inputs} if isinstance(inputs, dict) else arguments[i]
            tasks.append(_execute_tool(tool_name, args))

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            results = [TimeoutError(f"Parallel phase exceeded 300s timeout")]

        # Merge results
        merged = {}
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                merged[tools[i]] = {"error": str(result)}
            else:
                merged[tools[i]] = result

        return {"parallel_results": merged}

    elif workflow_type == WorkflowType.SEGMENT_PROCESS:
        # Segment-Process: segment → process each → aggregate
        if len(tools) < 2:
            raise ValueError("SEGMENT_PROCESS requires at least 2 tools")

        segment_tool = tools[0]
        process_tool = tools[1]
        aggregate_tool = tools[2] if len(tools) > 2 else None

        segment_args = arguments[0] if arguments else {}
        process_args = arguments[1] if len(arguments) > 1 else {}
        aggregate_args = arguments[2] if len(arguments) > 2 else {}

        # Segment
        segment_result = await _execute_tool(segment_tool, {**segment_args, **inputs})

        # Get segments
        segments = segment_result if isinstance(segment_result, list) else [segment_result]

        # Process each segment in parallel
        seg_tasks = [
            _execute_tool(process_tool, {**process_args, "input": segment})
            for segment in segments
        ]
        try:
            processed_raw = await asyncio.wait_for(
                asyncio.gather(*seg_tasks, return_exceptions=True),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            processed_raw = [TimeoutError(f"Segment-process phase exceeded 300s timeout")]
        processed = [
            {"error": str(r)} if isinstance(r, Exception) else r
            for r in processed_raw
        ]

        # Aggregate (optional)
        if aggregate_tool:
            agg_input = {**aggregate_args, "items": processed}
            final_result = await _execute_tool(aggregate_tool, agg_input)
            return {
                "segments": segments,
                "processed": processed,
                "aggregated": final_result
            }

        return {"segments": segments, "processed": processed}

    elif workflow_type == WorkflowType.PIPE:
        # Pipe: Functional composition f(g(h(x)))
        current = inputs
        results = []

        # Execute in reverse order (last tool first for traditional pipe)
        for i in range(len(tools) - 1, -1, -1):
            tool_name = tools[i]
            args = {**arguments[i], **current} if isinstance(current, dict) else arguments[i]
            result = await _execute_tool(tool_name, args)
            results.insert(0, result)
            current = result

        return {"piped_output": current, "steps": results}

    else:
        raise ValueError(f"Unknown workflow type: {workflow_type}")


# =============================================================================
# NAMED WORKFLOW LIBRARY
# =============================================================================

@router.get("/library")
async def get_library():
    """
    Get all pre-built named workflows from the library.

    Returns workflows with token limits and cost controls already configured.
    """
    library = get_workflow_library()

    return {
        "workflows": [
            {
                "id": w.id,
                "name": w.name,
                "description": w.description,
                "category": w.category.value,
                "tags": w.tags,
                "stages_count": len(w.stages),
                "total_max_tokens": w.total_max_tokens,
                "total_max_cost_cents": w.total_max_cost_cents,
                "required_inputs": w.required_inputs,
                "example_inputs": w.example_inputs
            }
            for w in library.values()
        ],
        "count": len(library),
        "categories": list(set(w.category.value for w in library.values()))
    }


@router.get("/library/{workflow_id}")
async def get_library_workflow(workflow_id: str):
    """Get a specific workflow from the library."""
    workflow = get_workflow_by_id(workflow_id)

    if not workflow:
        raise HTTPException(404, f"Workflow not found: {workflow_id}")

    return workflow


@router.get("/library/category/{category}")
async def get_library_by_category(category: str):
    """Get all workflows in a category."""
    try:
        cat = WorkflowCategory(category)
    except ValueError:
        raise HTTPException(400, f"Invalid category: {category}")

    workflows = get_workflows_by_category(cat)

    return {
        "category": category,
        "workflows": workflows,
        "count": len(workflows)
    }


@router.get("/library/tag/{tag}")
async def get_library_by_tag(tag: str):
    """Get all workflows with a specific tag."""
    workflows = get_workflows_by_tag(tag)

    return {
        "tag": tag,
        "workflows": workflows,
        "count": len(workflows)
    }


@router.post("/execute-named")
async def execute_named_workflow(
    workflow_id: str,
    inputs: Dict[str, Any],
    override_limits: Optional[Dict[str, ResourceLimits]] = None
):
    """
    Execute a named workflow from the library.

    Example:
    ```json
    {
      "workflow_id": "named_entity_extraction",
      "inputs": {
        "text": "Apple Inc. was founded by Steve Jobs in California."
      }
    }
    ```

    The workflow has pre-configured token limits and cost controls.
    You can optionally override limits per stage.
    """
    workflow = get_workflow_by_id(workflow_id)

    if not workflow:
        raise HTTPException(404, f"Workflow not found: {workflow_id}")

    # Validate required inputs
    for required in workflow.required_inputs:
        if required not in inputs:
            raise HTTPException(400, f"Missing required input: {required}")

    execution_id = str(uuid4())

    try:
        # Execute the workflow with token limits enforced
        result = await _execute_named_workflow(workflow, inputs, execution_id, override_limits)

        return {
            "execution_id": execution_id,
            "workflow_id": workflow.id,
            "workflow_name": workflow.name,
            "status": "completed",
            "result": result.get("output"),
            "resource_usage": result.get("resource_usage", {}),
            "stages_executed": result.get("stages_executed", [])
        }

    except Exception as e:
        logger.error(f"Named workflow execution failed: {e}")
        return {
            "execution_id": execution_id,
            "workflow_id": workflow.id,
            "workflow_name": workflow.name,
            "status": "failed",
            "error": str(e)
        }


async def _execute_named_workflow(
    workflow: NamedWorkflow,
    inputs: Dict[str, Any],
    execution_id: str,
    override_limits: Optional[Dict[str, ResourceLimits]] = None
) -> Dict[str, Any]:
    """Execute a named workflow with token limits."""
    import time
    start_time = time.time()

    stage_results = {}
    current_data = inputs
    resource_usage = {
        "total_tokens": 0,
        "total_cost_cents": 0,
        "stages": {}
    }

    for stage in workflow.stages:
        # Check if dependencies are met
        for dep in stage.depends_on:
            if dep not in stage_results:
                raise ValueError(f"Stage {stage.name} depends on {dep} which hasn't executed")

        # Get limits (use override if provided)
        limits = override_limits.get(stage.name, stage.limits) if override_limits else stage.limits

        # Execute stage with limits
        stage_start = time.time()

        try:
            # Prepare inputs
            stage_inputs = {**stage.arguments, **current_data}

            # Execute tool (with token limit enforcement)
            result = await _execute_tool_with_limits(stage.tool, stage_inputs, limits)

            # Track usage
            stage_duration = time.time() - stage_start
            stage_usage = {
                "duration_seconds": stage_duration,
                "tokens_used": result.get("_tokens_used", 0),
                "cost_cents": result.get("_cost_cents", 0)
            }

            resource_usage["total_tokens"] += stage_usage["tokens_used"]
            resource_usage["total_cost_cents"] += stage_usage["cost_cents"]
            resource_usage["stages"][stage.name] = stage_usage

            # Check limits
            if limits.max_tokens and stage_usage["tokens_used"] > limits.max_tokens:
                raise ValueError(f"Stage {stage.name} exceeded token limit: {stage_usage['tokens_used']} > {limits.max_tokens}")

            if limits.max_cost_cents and stage_usage["cost_cents"] > limits.max_cost_cents:
                raise ValueError(f"Stage {stage.name} exceeded cost limit: {stage_usage['cost_cents']} > {limits.max_cost_cents}")

            stage_results[stage.name] = result
            current_data = result

        except Exception as e:
            if stage.retry_on_failure and stage.max_retries > 0:
                # Retry logic
                for attempt in range(stage.max_retries):
                    try:
                        result = await _execute_tool_with_limits(stage.tool, stage_inputs, limits)
                        stage_results[stage.name] = result
                        current_data = result
                        break
                    except Exception as retry_error:
                        if attempt == stage.max_retries - 1:
                            raise retry_error
            else:
                raise e

    # Check global limits
    if workflow.total_max_tokens and resource_usage["total_tokens"] > workflow.total_max_tokens:
        logger.warning(f"Workflow exceeded total token limit: {resource_usage['total_tokens']} > {workflow.total_max_tokens}")

    if workflow.total_max_cost_cents and resource_usage["total_cost_cents"] > workflow.total_max_cost_cents:
        logger.warning(f"Workflow exceeded total cost limit: {resource_usage['total_cost_cents']} > {workflow.total_max_cost_cents}")

    duration_ms = (time.time() - start_time) * 1000

    return {
        "output": current_data,
        "resource_usage": resource_usage,
        "stages_executed": list(stage_results.keys()),
        "duration_ms": duration_ms
    }


async def _execute_tool_with_limits(
    tool_name: str,
    inputs: Dict[str, Any],
    limits: ResourceLimits
) -> Any:
    """Execute a tool with resource limits enforced."""
    import time
    start_time = time.time()

    # TODO: Implement actual token counting and cost tracking
    # For now, return placeholder with mock usage

    try:
        result = await _execute_tool(tool_name, inputs)

        # Mock token/cost tracking
        duration = time.time() - start_time

        # Check duration limit
        if limits.max_duration_seconds and duration > limits.max_duration_seconds:
            raise ValueError(f"Tool {tool_name} exceeded duration limit: {duration:.2f}s > {limits.max_duration_seconds}s")

        # Add usage metadata
        if isinstance(result, dict):
            result["_tokens_used"] = len(str(inputs)) + len(str(result))  # Mock
            result["_cost_cents"] = result["_tokens_used"] // 1000  # Mock
            result["_duration_seconds"] = duration

        return result

    except Exception as e:
        raise e


@router.get("/workflow-types")
async def list_workflow_types():
    """List available workflow types and their descriptions."""
    return {
        "workflow_types": [
            {
                "type": "sequential",
                "description": "Execute tools in sequence: tool1(x) → tool2(out1) → tool3(out2)",
                "example": "search → extract → summarize"
            },
            {
                "type": "map",
                "description": "Apply same tool to each input element",
                "example": "classify each document"
            },
            {
                "type": "reduce",
                "description": "Aggregate results from multiple tool calls",
                "example": "combine multiple search results"
            },
            {
                "type": "map_reduce",
                "description": "Map operation followed by reduction",
                "example": "embed documents → compute average embedding"
            },
            {
                "type": "parallel",
                "description": "Run all tools in parallel, merge results",
                "example": "search Google + Bing + DuckDuckGo simultaneously"
            },
            {
                "type": "segment_process",
                "description": "Split input → process each segment → aggregate",
                "example": "split document → classify each chunk → summarize categories"
            },
            {
                "type": "pipe",
                "description": "Functional composition: f(g(h(x)))",
                "example": "validate → transform → store"
            }
        ]
    }


# =============================================================================
# EXAMPLE WORKFLOWS
# =============================================================================

@router.get("/examples")
async def get_example_workflows():
    """Get example compositional workflows."""

    return {
        "examples": [
            {
                "name": "document_analysis",
                "description": "Segment → Classify → Embed → Compare",
                "workflow": {
                    "name": "document_analysis",
                    "steps": [
                        {
                            "name": "segment",
                            "mode": "segment",
                            "description": "Split document into segments"
                        },
                        {
                            "name": "classify",
                            "mode": "map",
                            "input_from": "segment",
                            "description": "Classify each segment"
                        },
                        {
                            "name": "embed",
                            "mode": "map",
                            "input_from": "segment",
                            "description": "Generate embeddings"
                        },
                        {
                            "name": "compare",
                            "mode": "compare",
                            "input_from": "embed",
                            "description": "Compare all pairs"
                        }
                    ]
                }
            },
            {
                "name": "text_to_structured",
                "description": "LLM: Text → Structured Pydantic Output",
                "workflow": {
                    "name": "text_to_structured",
                    "steps": [
                        {
                            "name": "extract",
                            "mode": "transform",
                            "transform_config": {
                                "llm_provider": "anthropic",
                                "model": "claude-3-sonnet-20240229",
                                "output_schema": "structured_data"
                            }
                        }
                    ]
                }
            }
        ]
    }
