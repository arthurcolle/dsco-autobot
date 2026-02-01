"""
Tool Composition and Pipeline Engine.

Allows chaining tools together with:
- Sequential execution
- Parallel execution (fan-out/fan-in)
- Conditional branching
- Error handling and retries
- Data transformation between steps
"""

from __future__ import annotations

import asyncio
import uuid
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)


class StepType(str, Enum):
    TOOL = "tool"           # Execute a tool
    TRANSFORM = "transform"  # Transform data
    CONDITION = "condition"  # Conditional branching
    PARALLEL = "parallel"    # Fan-out parallel execution
    AGGREGATE = "aggregate"  # Fan-in aggregation
    LOOP = "loop"           # Loop over items
    WAIT = "wait"           # Wait for condition/time


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StepResult:
    """Result of a pipeline step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0
    retries: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStep:
    """A single step in a pipeline."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    type: StepType = StepType.TOOL

    # For TOOL type
    tool_name: Optional[str] = None
    tool_inputs: Dict[str, Any] = field(default_factory=dict)
    input_mapping: Dict[str, str] = field(default_factory=dict)  # Map from previous outputs

    # For TRANSFORM type
    transform_fn: Optional[str] = None  # jmespath or simple expression

    # For CONDITION type
    condition: Optional[str] = None  # Expression to evaluate
    if_true: Optional[str] = None   # Step ID if true
    if_false: Optional[str] = None  # Step ID if false

    # For PARALLEL type
    parallel_steps: List[str] = field(default_factory=list)  # Step IDs to run in parallel

    # For LOOP type
    loop_over: Optional[str] = None  # Expression for items to loop
    loop_step: Optional[str] = None  # Step ID to execute for each item

    # Error handling
    on_error: str = "fail"  # fail, skip, retry, fallback
    max_retries: int = 3
    retry_delay_ms: int = 1000
    fallback_value: Any = None

    # Timeout
    timeout_ms: int = 30000

    # Dependencies
    depends_on: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value,
            "tool_name": self.tool_name,
            "tool_inputs": self.tool_inputs,
            "input_mapping": self.input_mapping,
            "depends_on": self.depends_on,
        }


@dataclass
class Pipeline:
    """A composable pipeline of tool executions."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    version: str = "1.0.0"

    steps: List[PipelineStep] = field(default_factory=list)
    entry_point: Optional[str] = None  # First step ID

    # Global settings
    timeout_ms: int = 300000  # 5 minutes
    max_parallel: int = 10

    # Input/output schema
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)

    created_at: datetime = field(default_factory=datetime.utcnow)

    def add_step(self, step: PipelineStep) -> "Pipeline":
        """Add a step to the pipeline."""
        self.steps.append(step)
        if self.entry_point is None:
            self.entry_point = step.id
        return self

    def add_tool(
        self,
        tool_name: str,
        inputs: Dict[str, Any] = None,
        name: str = None,
        depends_on: List[str] = None,
        **kwargs
    ) -> PipelineStep:
        """Add a tool execution step."""
        step = PipelineStep(
            name=name or tool_name,
            type=StepType.TOOL,
            tool_name=tool_name,
            tool_inputs=inputs or {},
            depends_on=depends_on or [],
            **kwargs
        )
        self.add_step(step)
        return step

    def add_transform(
        self,
        transform: str,
        name: str = None,
        depends_on: List[str] = None,
    ) -> PipelineStep:
        """Add a data transformation step."""
        step = PipelineStep(
            name=name or "transform",
            type=StepType.TRANSFORM,
            transform_fn=transform,
            depends_on=depends_on or [],
        )
        self.add_step(step)
        return step

    def add_parallel(
        self,
        steps: List[PipelineStep],
        name: str = None,
    ) -> PipelineStep:
        """Add parallel execution of multiple steps."""
        for s in steps:
            if s not in self.steps:
                self.steps.append(s)

        parallel = PipelineStep(
            name=name or "parallel",
            type=StepType.PARALLEL,
            parallel_steps=[s.id for s in steps],
        )
        self.add_step(parallel)
        return parallel

    def get_step(self, step_id: str) -> Optional[PipelineStep]:
        """Get a step by ID."""
        for step in self.steps:
            if step.id == step_id:
                return step
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "steps": [s.to_dict() for s in self.steps],
            "entry_point": self.entry_point,
            "timeout_ms": self.timeout_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Pipeline":
        """Create pipeline from dictionary."""
        steps = [
            PipelineStep(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", ""),
                type=StepType(s.get("type", "tool")),
                tool_name=s.get("tool_name"),
                tool_inputs=s.get("tool_inputs", {}),
                input_mapping=s.get("input_mapping", {}),
                depends_on=s.get("depends_on", []),
            )
            for s in data.get("steps", [])
        ]

        return cls(
            id=data.get("id", str(uuid.uuid4())),
            name=data.get("name", ""),
            description=data.get("description", ""),
            steps=steps,
            entry_point=data.get("entry_point"),
            timeout_ms=data.get("timeout_ms", 300000),
        )


class PipelineExecutor:
    """Executes pipelines with full orchestration capabilities."""

    def __init__(self, tool_executor: Callable):
        """
        Initialize executor.

        Args:
            tool_executor: Async function(tool_name, inputs) -> result
        """
        self.tool_executor = tool_executor
        self._running: Dict[str, asyncio.Task] = {}

    async def execute(
        self,
        pipeline: Pipeline,
        inputs: Dict[str, Any] = None,
        context: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Execute a pipeline.

        Args:
            pipeline: The pipeline to execute
            inputs: Initial input data
            context: Additional context (client_id, trace_id, etc.)

        Returns:
            Dictionary with results, timing, and metadata
        """
        start_time = time.time()
        execution_id = str(uuid.uuid4())

        # Execution state
        state = {
            "inputs": inputs or {},
            "outputs": {},  # step_id -> output
            "results": {},  # step_id -> StepResult
            "current_step": None,
        }

        logger.info(f"Starting pipeline {pipeline.name} ({execution_id})")

        try:
            # Build execution order using topological sort
            execution_order = self._topological_sort(pipeline)

            for step_id in execution_order:
                step = pipeline.get_step(step_id)
                if not step:
                    continue

                state["current_step"] = step_id

                # Check dependencies are satisfied
                if not self._dependencies_satisfied(step, state):
                    logger.warning(f"Skipping step {step_id} - dependencies not met")
                    continue

                # Execute step
                result = await self._execute_step(step, state, pipeline)
                state["results"][step_id] = result

                if result.status == StepStatus.COMPLETED:
                    state["outputs"][step_id] = result.output
                elif result.status == StepStatus.FAILED and step.on_error == "fail":
                    raise Exception(f"Step {step_id} failed: {result.error}")

            # Collect final output
            final_output = self._collect_output(pipeline, state)

            return {
                "execution_id": execution_id,
                "pipeline_id": pipeline.id,
                "status": "completed",
                "output": final_output,
                "step_results": {
                    k: {
                        "status": v.status.value,
                        "output": v.output,
                        "duration_ms": v.duration_ms,
                        "error": v.error,
                    }
                    for k, v in state["results"].items()
                },
                "duration_ms": (time.time() - start_time) * 1000,
            }

        except Exception as e:
            logger.error(f"Pipeline {pipeline.name} failed: {e}")
            return {
                "execution_id": execution_id,
                "pipeline_id": pipeline.id,
                "status": "failed",
                "error": str(e),
                "step_results": {
                    k: {
                        "status": v.status.value,
                        "output": v.output,
                        "duration_ms": v.duration_ms,
                        "error": v.error,
                    }
                    for k, v in state["results"].items()
                },
                "duration_ms": (time.time() - start_time) * 1000,
            }

    async def _execute_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
        pipeline: Pipeline,
    ) -> StepResult:
        """Execute a single pipeline step."""
        start_time = time.time()
        retries = 0

        while True:
            try:
                if step.type == StepType.TOOL:
                    output = await self._execute_tool_step(step, state)
                elif step.type == StepType.TRANSFORM:
                    output = self._execute_transform_step(step, state)
                elif step.type == StepType.PARALLEL:
                    output = await self._execute_parallel_step(step, state, pipeline)
                elif step.type == StepType.CONDITION:
                    output = self._execute_condition_step(step, state)
                elif step.type == StepType.LOOP:
                    output = await self._execute_loop_step(step, state, pipeline)
                else:
                    output = None

                return StepResult(
                    step_id=step.id,
                    status=StepStatus.COMPLETED,
                    output=output,
                    duration_ms=(time.time() - start_time) * 1000,
                    retries=retries,
                )

            except asyncio.TimeoutError:
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.FAILED,
                    error="Timeout",
                    duration_ms=(time.time() - start_time) * 1000,
                    retries=retries,
                )

            except Exception as e:
                if step.on_error == "retry" and retries < step.max_retries:
                    retries += 1
                    logger.warning(f"Step {step.id} failed, retry {retries}/{step.max_retries}")
                    await asyncio.sleep(step.retry_delay_ms / 1000)
                    continue

                if step.on_error == "skip":
                    return StepResult(
                        step_id=step.id,
                        status=StepStatus.SKIPPED,
                        error=str(e),
                        duration_ms=(time.time() - start_time) * 1000,
                        retries=retries,
                    )

                if step.on_error == "fallback":
                    return StepResult(
                        step_id=step.id,
                        status=StepStatus.COMPLETED,
                        output=step.fallback_value,
                        duration_ms=(time.time() - start_time) * 1000,
                        retries=retries,
                        metadata={"used_fallback": True},
                    )

                return StepResult(
                    step_id=step.id,
                    status=StepStatus.FAILED,
                    error=str(e),
                    duration_ms=(time.time() - start_time) * 1000,
                    retries=retries,
                )

    async def _execute_tool_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
    ) -> Any:
        """Execute a tool step."""
        # Build inputs from static values and mappings
        inputs = dict(step.tool_inputs)

        # Resolve any expressions in static inputs (e.g., ${step1} or ${step1.field})
        for key, value in inputs.items():
            if isinstance(value, str):
                resolved = self._resolve_expression(value, state)
                if resolved is not None:
                    inputs[key] = resolved

        # Apply input mappings from previous step outputs
        for target_key, source_expr in step.input_mapping.items():
            value = self._resolve_expression(source_expr, state)
            inputs[target_key] = value

        # Execute with timeout
        result = await asyncio.wait_for(
            self.tool_executor(step.tool_name, inputs),
            timeout=step.timeout_ms / 1000,
        )

        return result

    def _execute_transform_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
    ) -> Any:
        """Execute a data transformation step."""
        # Simple expression evaluation
        # Supports: $.outputs.step_id.field, $.inputs.field
        return self._resolve_expression(step.transform_fn, state)

    async def _execute_parallel_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
        pipeline: Pipeline,
    ) -> Dict[str, Any]:
        """Execute multiple steps in parallel."""
        tasks = {}
        for step_id in step.parallel_steps:
            parallel_step = pipeline.get_step(step_id)
            if parallel_step:
                tasks[step_id] = self._execute_step(parallel_step, state, pipeline)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        output = {}
        for step_id, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                output[step_id] = {"error": str(result)}
            else:
                state["results"][step_id] = result
                state["outputs"][step_id] = result.output
                output[step_id] = result.output

        return output

    def _execute_condition_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
    ) -> str:
        """Evaluate condition and return next step ID."""
        result = self._resolve_expression(step.condition, state)
        return step.if_true if result else step.if_false

    async def _execute_loop_step(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
        pipeline: Pipeline,
    ) -> List[Any]:
        """Execute a step for each item in a collection."""
        items = self._resolve_expression(step.loop_over, state)
        if not isinstance(items, (list, tuple)):
            items = [items]

        loop_step = pipeline.get_step(step.loop_step)
        if not loop_step:
            return []

        results = []
        for i, item in enumerate(items):
            # Create scoped state with loop item
            loop_state = dict(state)
            loop_state["loop_item"] = item
            loop_state["loop_index"] = i

            result = await self._execute_step(loop_step, loop_state, pipeline)
            results.append(result.output)

        return results

    def _resolve_expression(self, expr: str, state: Dict[str, Any]) -> Any:
        """
        Resolve a simple expression against state.

        Supports:
        - $.inputs.field
        - $.outputs.step_id
        - $.outputs.step_id.field
        - $.loop_item
        - ${step_id} - Direct reference to step output
        - ${step_id.field} - Reference to field in step output
        - Literal values
        """
        if not expr or not isinstance(expr, str):
            return expr

        # Handle ${step_id} syntax for direct step output reference
        if expr.startswith("${") and expr.endswith("}"):
            ref = expr[2:-1]  # Remove ${ and }
            parts = ref.split(".")
            step_id = parts[0]

            # Get the step output
            if step_id in state.get("outputs", {}):
                result = state["outputs"][step_id]

                # Navigate nested fields if specified (e.g., ${step1.data.value})
                for field in parts[1:]:
                    if isinstance(result, dict) and field in result:
                        result = result[field]
                    else:
                        return None
                return result

            # Also check if it's an input reference
            if step_id == "inputs":
                result = state.get("inputs", {})
                for field in parts[1:]:
                    if isinstance(result, dict) and field in result:
                        result = result[field]
                    else:
                        return None
                return result

            return None

        # Handle $.path.to.value syntax
        if not expr.startswith("$."):
            return expr

        parts = expr[2:].split(".")
        current = state

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def _dependencies_satisfied(
        self,
        step: PipelineStep,
        state: Dict[str, Any],
    ) -> bool:
        """Check if all step dependencies are satisfied."""
        for dep_id in step.depends_on:
            if dep_id not in state["results"]:
                return False
            if state["results"][dep_id].status == StepStatus.FAILED:
                return False
        return True

    def _topological_sort(self, pipeline: Pipeline) -> List[str]:
        """Sort steps by dependencies."""
        # Build dependency graph
        in_degree = {s.id: 0 for s in pipeline.steps}
        graph = {s.id: [] for s in pipeline.steps}

        for step in pipeline.steps:
            for dep_id in step.depends_on:
                if dep_id in graph:
                    graph[dep_id].append(step.id)
                    in_degree[step.id] += 1

        # Kahn's algorithm
        queue = [s_id for s_id, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result

    def _collect_output(
        self,
        pipeline: Pipeline,
        state: Dict[str, Any],
    ) -> Any:
        """Collect final pipeline output."""
        # Return output from last step
        if pipeline.steps:
            last_step = pipeline.steps[-1]
            return state["outputs"].get(last_step.id)
        return state["outputs"]


# Convenience functions for building pipelines
def chain(*tools: str) -> Pipeline:
    """Create a simple sequential pipeline."""
    pipeline = Pipeline(name="chain")
    prev_step = None

    for tool_name in tools:
        step = pipeline.add_tool(
            tool_name,
            depends_on=[prev_step.id] if prev_step else [],
        )
        prev_step = step

    return pipeline


def parallel(*tools: str) -> Pipeline:
    """Create a pipeline that runs tools in parallel."""
    pipeline = Pipeline(name="parallel")
    steps = [PipelineStep(name=t, type=StepType.TOOL, tool_name=t) for t in tools]
    pipeline.add_parallel(steps)
    return pipeline
