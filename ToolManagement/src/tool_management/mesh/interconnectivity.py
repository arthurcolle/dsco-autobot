"""
Interconnectivity Module for Tool Management

Provides advanced features for tool composition, chaining, webhooks,
event-driven architecture, and federation with other tool registries.
"""

import asyncio
import json
import httpx
import logging
from typing import Dict, List, Any, Optional, Union, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pydantic import BaseModel, Field as PydanticField, HttpUrl
import hashlib
import hmac
import uuid
from collections import defaultdict

logger = logging.getLogger(__name__)


# ============================================================================
# Error Handling and Recovery
# ============================================================================

class CircuitState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreaker:
    """
    Circuit breaker pattern for fault tolerance.

    Prevents cascading failures by failing fast when a service is unhealthy.
    """
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3

    state: CircuitState = field(default=CircuitState.CLOSED)
    failure_count: int = field(default=0)
    success_count: int = field(default=0)
    last_failure_time: Optional[datetime] = field(default=None)
    half_open_calls: int = field(default=0)

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if self.last_failure_time and \
               (datetime.utcnow() - self.last_failure_time).total_seconds() > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                return True
            return False

        if self.state == CircuitState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls

        return False

    def record_success(self):
        """Record a successful execution."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            self.half_open_calls += 1
            if self.success_count >= self.half_open_max_calls:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                logger.info(f"Circuit {self.name} closed after successful recovery")
        else:
            self.failure_count = 0

    def record_failure(self):
        """Record a failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit {self.name} reopened after failure in half-open state")
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit {self.name} opened after {self.failure_count} failures")

    def reset(self):
        """Reset the circuit breaker."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0


class RetryPolicy:
    """Configurable retry policy with exponential backoff."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[tuple] = None
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or (Exception,)

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number."""
        import random

        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            delay = delay * (0.5 + random.random())

        return delay

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if we should retry based on exception and attempt count."""
        if attempt >= self.max_retries:
            return False
        return isinstance(exception, self.retryable_exceptions)


async def execute_with_retry(
    func: Callable,
    *args,
    retry_policy: Optional[RetryPolicy] = None,
    circuit_breaker: Optional[CircuitBreaker] = None,
    timeout: Optional[float] = None,
    **kwargs
) -> Any:
    """
    Execute a function with retry, circuit breaker, and timeout support.

    Args:
        func: Async function to execute
        *args: Positional arguments for func
        retry_policy: Retry configuration
        circuit_breaker: Circuit breaker for fault tolerance
        timeout: Execution timeout in seconds
        **kwargs: Keyword arguments for func

    Returns:
        Result of the function execution
    """
    policy = retry_policy or RetryPolicy()
    attempt = 0
    last_exception = None

    while attempt <= policy.max_retries:
        # Check circuit breaker
        if circuit_breaker and not circuit_breaker.can_execute():
            raise RuntimeError(f"Circuit breaker {circuit_breaker.name} is OPEN")

        try:
            # Execute with timeout if specified
            if timeout:
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=timeout
                )
            else:
                result = await func(*args, **kwargs)

            # Record success
            if circuit_breaker:
                circuit_breaker.record_success()

            return result

        except asyncio.TimeoutError as e:
            last_exception = e
            logger.warning(f"Execution timed out after {timeout}s (attempt {attempt + 1})")

            if circuit_breaker:
                circuit_breaker.record_failure()

            if not policy.should_retry(e, attempt):
                raise

        except Exception as e:
            last_exception = e
            logger.warning(f"Execution failed (attempt {attempt + 1}): {e}")

            if circuit_breaker:
                circuit_breaker.record_failure()

            if not policy.should_retry(e, attempt):
                raise

        # Wait before retry
        delay = policy.get_delay(attempt)
        logger.info(f"Retrying in {delay:.2f}s...")
        await asyncio.sleep(delay)
        attempt += 1

    raise last_exception or RuntimeError("Max retries exceeded")


class ExecutionContext:
    """
    Context manager for tool execution with comprehensive error handling.

    Provides circuit breaker, retry, timeout, and recovery features.
    """

    def __init__(
        self,
        name: str,
        retry_policy: Optional[RetryPolicy] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        timeout: Optional[float] = None,
        fallback: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
        on_success: Optional[Callable] = None
    ):
        self.name = name
        self.retry_policy = retry_policy or RetryPolicy()
        self.circuit_breaker = circuit_breaker
        self.timeout = timeout
        self.fallback = fallback
        self.on_error = on_error
        self.on_success = on_success
        self.execution_start: Optional[datetime] = None
        self.execution_end: Optional[datetime] = None

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with full error handling."""
        self.execution_start = datetime.utcnow()

        try:
            result = await execute_with_retry(
                func,
                *args,
                retry_policy=self.retry_policy,
                circuit_breaker=self.circuit_breaker,
                timeout=self.timeout,
                **kwargs
            )

            self.execution_end = datetime.utcnow()

            if self.on_success:
                await self._call_hook(self.on_success, result)

            return result

        except Exception as e:
            self.execution_end = datetime.utcnow()

            if self.on_error:
                await self._call_hook(self.on_error, e)

            if self.fallback:
                logger.info(f"Executing fallback for {self.name}")
                try:
                    return await self._call_hook(self.fallback, e, *args, **kwargs)
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}")
                    raise e

            raise

    async def _call_hook(self, hook: Callable, *args, **kwargs) -> Any:
        """Call a hook function, handling both sync and async."""
        if asyncio.iscoroutinefunction(hook):
            return await hook(*args, **kwargs)
        return hook(*args, **kwargs)

    def get_execution_time(self) -> Optional[float]:
        """Get execution time in seconds."""
        if self.execution_start and self.execution_end:
            return (self.execution_end - self.execution_start).total_seconds()
        return None


# ============================================================================
# Tool Composition and Chaining
# ============================================================================

class CompositionType(str, Enum):
    """Types of tool composition"""
    # Basic patterns
    SEQUENTIAL = "sequential"      # Tools run one after another
    PARALLEL = "parallel"          # Tools run simultaneously
    CONDITIONAL = "conditional"    # Tools run based on conditions
    LOOP = "loop"                  # Tools run in a loop
    MAP_REDUCE = "map_reduce"      # Map-reduce pattern
    PIPELINE = "pipeline"          # Data pipeline pattern
    WORKFLOW = "workflow"          # Complex workflow with branches

    # Scale patterns (for hundreds of tools, working around token limits)
    BATCHED = "batched"            # Execute tools in chunks with aggregation between batches
    STREAMING = "streaming"        # Wave-based execution with intermediate summaries
    DELEGATED = "delegated"        # Hierarchical multi-agent delegation (split work across sub-agents)
    QUEUED = "queued"              # Async job queue execution (decouple planning from execution)
    SAGA = "saga"                  # Distributed transaction pattern with compensation
    SCATTER_GATHER = "scatter_gather"  # Fan-out to workers, gather and aggregate results


class ToolComposition(BaseModel):
    """Represents a composition of multiple tools"""
    id: str = PydanticField(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str
    type: CompositionType
    tools: List[Dict[str, Any]]  # Tool configurations
    connections: List[Dict[str, Any]]  # How tools are connected
    metadata: Dict[str, Any] = PydanticField(default_factory=dict)
    created_at: datetime = PydanticField(default_factory=datetime.utcnow)
    updated_at: datetime = PydanticField(default_factory=datetime.utcnow)


class ToolChain:
    """Manages tool chaining and composition"""

    def __init__(self, tool_executor: Callable):
        self.tool_executor = tool_executor
        self.compositions: Dict[str, ToolComposition] = {}
        self.execution_history: List[Dict[str, Any]] = []

    async def create_composition(
        self,
        name: str,
        description: str,
        type: CompositionType,
        tools: List[Dict[str, Any]],
        connections: Optional[List[Dict[str, Any]]] = None,
        validate: bool = True
    ) -> ToolComposition:
        """Create a new tool composition with optional validation."""
        composition = ToolComposition(
            name=name,
            description=description,
            type=type,
            tools=tools,
            connections=connections or []
        )

        if validate:
            validation_result = self.validate_composition(composition)
            if not validation_result["valid"]:
                raise ValueError(f"Invalid composition: {validation_result['errors']}")

        self.compositions[composition.id] = composition
        return composition

    def validate_composition(self, composition: ToolComposition) -> Dict[str, Any]:
        """
        Validate a tool composition for correctness.

        Checks:
        - All tools have required fields (tool_id or tool_name)
        - Connections reference valid tools
        - No circular dependencies in workflows
        - Type-specific requirements are met
        """
        errors = []
        warnings = []
        tool_ids = set()

        # Check tool definitions
        for i, tool in enumerate(composition.tools):
            tool_id = tool.get("id") or tool.get("tool_id") or tool.get("tool_name")

            if not tool_id:
                errors.append(f"Tool at index {i} has no identifier (id, tool_id, or tool_name)")
            else:
                if tool_id in tool_ids:
                    errors.append(f"Duplicate tool identifier: {tool_id}")
                tool_ids.add(tool_id)

        # Validate connections
        for conn in composition.connections:
            source = conn.get("from") or conn.get("source")
            target = conn.get("to") or conn.get("target")

            if source and source not in tool_ids:
                errors.append(f"Connection source '{source}' not found in tools")
            if target and target not in tool_ids:
                errors.append(f"Connection target '{target}' not found in tools")

        # Type-specific validation
        if composition.type == CompositionType.MAP_REDUCE:
            if len(composition.tools) < 2:
                errors.append("MAP_REDUCE requires at least 2 tools (mapper and reducer)")

        if composition.type == CompositionType.WORKFLOW:
            cycle = self._detect_cycle(composition.tools, composition.connections)
            if cycle:
                errors.append(f"Circular dependency detected in workflow: {' -> '.join(cycle)}")

        if composition.type == CompositionType.SAGA:
            for i, tool in enumerate(composition.tools):
                if not tool.get("compensation"):
                    warnings.append(f"Tool at index {i} has no compensation defined for SAGA pattern")

        if composition.type == CompositionType.SCATTER_GATHER:
            if not composition.metadata.get("reducer"):
                warnings.append("SCATTER_GATHER has no reducer defined, using default merge")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "tool_count": len(composition.tools),
            "connection_count": len(composition.connections)
        }

    def _detect_cycle(
        self,
        tools: List[Dict[str, Any]],
        connections: List[Dict[str, Any]]
    ) -> Optional[List[str]]:
        """Detect circular dependencies in the tool graph."""
        # Build adjacency list
        graph: Dict[str, List[str]] = defaultdict(list)
        tool_ids = set()

        for tool in tools:
            tool_id = tool.get("id") or tool.get("tool_id") or tool.get("tool_name")
            if tool_id:
                tool_ids.add(tool_id)

        for conn in connections:
            source = conn.get("from") or conn.get("source")
            target = conn.get("to") or conn.get("target")
            if source and target:
                graph[source].append(target)

        # DFS to detect cycle
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    result = dfs(neighbor)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]

            path.pop()
            rec_stack.remove(node)
            return None

        for tool_id in tool_ids:
            if tool_id not in visited:
                cycle = dfs(tool_id)
                if cycle:
                    return cycle

        return None

    async def execute_composition(
        self,
        composition_id: str,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a tool composition"""
        composition = self.compositions.get(composition_id)
        if not composition:
            raise ValueError(f"Composition '{composition_id}' not found")

        execution_id = str(uuid.uuid4())
        execution_start = datetime.utcnow()

        try:
            if composition.type == CompositionType.SEQUENTIAL:
                result = await self._execute_sequential(composition, input_data, context)
            elif composition.type == CompositionType.PARALLEL:
                result = await self._execute_parallel(composition, input_data, context)
            elif composition.type == CompositionType.CONDITIONAL:
                result = await self._execute_conditional(composition, input_data, context)
            elif composition.type == CompositionType.LOOP:
                result = await self._execute_loop(composition, input_data, context)
            elif composition.type == CompositionType.MAP_REDUCE:
                result = await self._execute_map_reduce(composition, input_data, context)
            elif composition.type == CompositionType.PIPELINE:
                result = await self._execute_pipeline(composition, input_data, context)
            elif composition.type == CompositionType.WORKFLOW:
                result = await self._execute_workflow(composition, input_data, context)
            # Scale patterns
            elif composition.type == CompositionType.BATCHED:
                result = await self._execute_batched(composition, input_data, context)
            elif composition.type == CompositionType.STREAMING:
                result = await self._execute_streaming(composition, input_data, context)
            elif composition.type == CompositionType.DELEGATED:
                result = await self._execute_delegated(composition, input_data, context)
            elif composition.type == CompositionType.QUEUED:
                result = await self._execute_queued(composition, input_data, context)
            elif composition.type == CompositionType.SAGA:
                result = await self._execute_saga(composition, input_data, context)
            elif composition.type == CompositionType.SCATTER_GATHER:
                result = await self._execute_scatter_gather(composition, input_data, context)
            else:
                raise ValueError(f"Unsupported composition type: {composition.type}")

            # Record execution
            self.execution_history.append({
                "execution_id": execution_id,
                "composition_id": composition_id,
                "input": input_data,
                "output": result,
                "status": "success",
                "started_at": execution_start,
                "completed_at": datetime.utcnow()
            })

            return result

        except Exception as e:
            # Record failed execution
            self.execution_history.append({
                "execution_id": execution_id,
                "composition_id": composition_id,
                "input": input_data,
                "error": str(e),
                "status": "failed",
                "started_at": execution_start,
                "completed_at": datetime.utcnow()
            })
            raise

    async def _execute_sequential(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute tools sequentially"""
        current_data = input_data

        for tool_config in composition.tools:
            tool_result = await self.tool_executor(
                tool_id=tool_config.get("tool_id"),
                tool_name=tool_config.get("tool_name"),
                input_data=current_data
            )
            current_data = tool_result

        return current_data

    async def _execute_parallel(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute tools in parallel"""
        tasks = []

        for tool_config in composition.tools:
            task = self.tool_executor(
                tool_id=tool_config.get("tool_id"),
                tool_name=tool_config.get("tool_name"),
                input_data=input_data
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        return {
            "results": results,
            "execution_type": "parallel",
            "tool_count": len(results)
        }

    async def _execute_conditional(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute tools based on conditions"""
        results = []

        for tool_config in composition.tools:
            condition = tool_config.get("condition")

            if condition:
                # Evaluate condition (simplified - would use proper expression evaluator)
                if self._evaluate_condition(condition, input_data, context):
                    result = await self.tool_executor(
                        tool_id=tool_config.get("tool_id"),
                        tool_name=tool_config.get("tool_name"),
                        input_data=input_data
                    )
                    results.append(result)

        return {"results": results, "execution_type": "conditional"}

    async def _execute_loop(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute tools in a loop"""
        max_iterations = context.get("max_iterations", 10) if context else 10
        results = []
        current_data = input_data

        for i in range(max_iterations):
            for tool_config in composition.tools:
                result = await self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=current_data
                )
                results.append(result)
                current_data = result

                # Check loop termination condition
                if self._check_loop_termination(result, tool_config):
                    return {"results": results, "iterations": i + 1}

        return {"results": results, "iterations": max_iterations}

    async def _execute_map_reduce(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute map-reduce pattern"""
        # Map phase
        map_tool = composition.tools[0] if composition.tools else None
        reduce_tool = composition.tools[1] if len(composition.tools) > 1 else None

        if not map_tool or not reduce_tool:
            raise ValueError("Map-reduce requires at least 2 tools (mapper and reducer)")

        # Assume input_data contains a list to map over
        items = input_data.get("items", [])
        map_tasks = []

        for item in items:
            task = self.tool_executor(
                tool_id=map_tool.get("tool_id"),
                tool_name=map_tool.get("tool_name"),
                input_data={"item": item}
            )
            map_tasks.append(task)

        map_results = await asyncio.gather(*map_tasks)

        # Reduce phase
        reduce_result = await self.tool_executor(
            tool_id=reduce_tool.get("tool_id"),
            tool_name=reduce_tool.get("tool_name"),
            input_data={"items": map_results}
        )

        return {
            "map_results": map_results,
            "reduce_result": reduce_result,
            "execution_type": "map_reduce"
        }

    async def _execute_pipeline(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute tools as a data pipeline with transformations"""
        pipeline_data = input_data
        transformations = []

        for tool_config in composition.tools:
            # Apply any pre-transformation
            if "pre_transform" in tool_config:
                pipeline_data = self._apply_transformation(
                    pipeline_data,
                    tool_config["pre_transform"]
                )

            # Execute tool
            result = await self.tool_executor(
                tool_id=tool_config.get("tool_id"),
                tool_name=tool_config.get("tool_name"),
                input_data=pipeline_data
            )

            # Apply any post-transformation
            if "post_transform" in tool_config:
                result = self._apply_transformation(
                    result,
                    tool_config["post_transform"]
                )

            transformations.append({
                "tool": tool_config.get("tool_name"),
                "output": result
            })

            pipeline_data = result

        return {
            "final_output": pipeline_data,
            "transformations": transformations,
            "execution_type": "pipeline"
        }

    async def _execute_workflow(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute a complex workflow with branches, joins, and state management.

        Workflow structure:
        - Each tool config can have 'branches' for conditional paths
        - Supports 'join' points to merge parallel branches
        - Maintains workflow state across steps
        - Handles error recovery and rollback
        """
        workflow_state = {
            "input": input_data,
            "current_step": 0,
            "branch_results": {},
            "completed_steps": [],
            "errors": [],
            "context": context or {}
        }

        # Build dependency graph from connections
        dependencies = self._build_dependency_graph(composition.connections)
        execution_order = self._topological_sort(composition.tools, dependencies)

        results = {}

        for step_idx, tool_config in enumerate(execution_order):
            tool_id = tool_config.get("id") or tool_config.get("tool_id") or f"step_{step_idx}"
            workflow_state["current_step"] = step_idx
            step_input = {}
            result = None

            try:
                # Check if this step has dependencies that need to be resolved
                step_input = self._resolve_workflow_input(
                    tool_config,
                    input_data,
                    results,
                    workflow_state
                )

                # Check for branching conditions
                if "branches" in tool_config:
                    branch_results = await self._execute_branches(
                        tool_config["branches"],
                        step_input,
                        workflow_state
                    )
                    results[tool_id] = branch_results
                    workflow_state["branch_results"][tool_id] = branch_results

                # Check for join point (merge parallel branches)
                elif "join" in tool_config:
                    join_sources = tool_config["join"]
                    joined_data = {
                        src: results.get(src) for src in join_sources if src in results
                    }

                    # Execute join tool with merged data
                    result = await self.tool_executor(
                        tool_id=tool_config.get("tool_id"),
                        tool_name=tool_config.get("tool_name"),
                        input_data={"merged": joined_data, **step_input}
                    )
                    results[tool_id] = result

                # Standard tool execution
                else:
                    result = await self.tool_executor(
                        tool_id=tool_config.get("tool_id"),
                        tool_name=tool_config.get("tool_name"),
                        input_data=step_input
                    )
                    results[tool_id] = result

                workflow_state["completed_steps"].append(tool_id)

                # Check for early termination
                if tool_config.get("terminal") and self._check_terminal_condition(
                    result, tool_config.get("terminal_condition")
                ):
                    break

            except Exception as e:
                error_info = {
                    "step": tool_id,
                    "error": str(e),
                    "input": step_input
                }
                workflow_state["errors"].append(error_info)

                # Check error handling strategy
                error_strategy = tool_config.get("on_error", "fail")
                if error_strategy == "continue":
                    results[tool_id] = {"error": str(e), "status": "failed"}
                    continue
                elif error_strategy == "fallback":
                    fallback_value = tool_config.get("fallback_value", {})
                    results[tool_id] = fallback_value
                    continue
                elif error_strategy == "retry":
                    max_retries = tool_config.get("max_retries", 3)
                    retry_result = await self._retry_step(
                        tool_config, step_input, max_retries
                    )
                    if retry_result is not None:
                        results[tool_id] = retry_result
                        continue
                # Default: fail the workflow
                raise

        # Get final output from the last step or specified output step
        output_step = composition.metadata.get("output_step")
        if output_step and output_step in results:
            final_output = results[output_step]
        else:
            final_output = results.get(list(results.keys())[-1]) if results else {}

        return {
            "final_output": final_output,
            "all_results": results,
            "workflow_state": workflow_state,
            "execution_type": "workflow",
            "steps_completed": len(workflow_state["completed_steps"]),
            "errors": workflow_state["errors"]
        }

    def _build_dependency_graph(
        self,
        connections: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """Build a dependency graph from connections."""
        deps = defaultdict(list)
        for conn in connections:
            source = conn.get("from") or conn.get("source")
            target = conn.get("to") or conn.get("target")
            if source and target:
                deps[target].append(source)
        return dict(deps)

    def _topological_sort(
        self,
        tools: List[Dict[str, Any]],
        dependencies: Dict[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """Sort tools in topological order based on dependencies."""
        # Create ID mapping
        tool_by_id = {}
        for tool in tools:
            tool_id = tool.get("id") or tool.get("tool_id") or tool.get("tool_name")
            tool_by_id[tool_id] = tool

        # If no dependencies, return original order
        if not dependencies:
            return tools

        # Kahn's algorithm for topological sort
        in_degree = defaultdict(int)
        for tool_id in tool_by_id:
            in_degree[tool_id] = len(dependencies.get(tool_id, []))

        queue = [tid for tid in tool_by_id if in_degree[tid] == 0]
        result = []

        while queue:
            current = queue.pop(0)
            if current in tool_by_id:
                result.append(tool_by_id[current])

            # Update in-degrees
            for tool_id, deps in dependencies.items():
                if current in deps:
                    in_degree[tool_id] -= 1
                    if in_degree[tool_id] == 0 and tool_id not in [t.get("id") or t.get("tool_id") for t in result]:
                        queue.append(tool_id)

        # Add any remaining tools (no dependencies defined)
        for tool in tools:
            if tool not in result:
                result.append(tool)

        return result

    def _resolve_workflow_input(
        self,
        tool_config: Dict[str, Any],
        initial_input: Dict[str, Any],
        step_results: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve input for a workflow step, including references to previous results."""
        input_spec = tool_config.get("input", {})
        resolved = {}

        for key, value in input_spec.items():
            if isinstance(value, str) and value.startswith("$"):
                # Reference resolution
                if value == "$input":
                    resolved[key] = initial_input
                elif value.startswith("$results."):
                    ref_path = value[9:]  # Remove "$results."
                    parts = ref_path.split(".")
                    ref_value = step_results
                    for part in parts:
                        if isinstance(ref_value, dict):
                            ref_value = ref_value.get(part, {})
                    resolved[key] = ref_value
                elif value.startswith("$context."):
                    ref_path = value[9:]
                    resolved[key] = workflow_state["context"].get(ref_path)
                else:
                    resolved[key] = value
            else:
                resolved[key] = value

        # If no explicit input mapping, use initial input
        if not resolved:
            resolved = initial_input.copy()

        return resolved

    async def _execute_branches(
        self,
        branches: List[Dict[str, Any]],
        input_data: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute conditional branches in parallel."""
        tasks = []
        branch_names = []

        for branch in branches:
            condition = branch.get("condition")

            # Check if branch should execute
            if condition:
                if not self._evaluate_condition(condition, input_data, workflow_state["context"]):
                    continue

            branch_name = branch.get("name", f"branch_{len(tasks)}")
            branch_names.append(branch_name)

            # Execute branch tool
            task = self.tool_executor(
                tool_id=branch.get("tool_id"),
                tool_name=branch.get("tool_name"),
                input_data=input_data
            )
            tasks.append(task)

        if not tasks:
            return {"skipped": True, "reason": "No branch conditions met"}

        results = await asyncio.gather(*tasks, return_exceptions=True)

        return {
            name: result if not isinstance(result, Exception) else {"error": str(result)}
            for name, result in zip(branch_names, results)
        }

    def _check_terminal_condition(
        self,
        result: Any,
        condition: Optional[Dict[str, Any]]
    ) -> bool:
        """Check if a terminal condition is met."""
        if condition is None:
            return False
        return self._evaluate_condition(condition, result if isinstance(result, dict) else {"value": result}, None)

    async def _retry_step(
        self,
        tool_config: Dict[str, Any],
        input_data: Dict[str, Any],
        max_retries: int
    ) -> Optional[Any]:
        """Retry a failed step with exponential backoff."""
        retry_delay = tool_config.get("retry_delay", 1.0)

        for attempt in range(max_retries):
            try:
                await asyncio.sleep(retry_delay * (2 ** attempt))
                result = await self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=input_data
                )
                return result
            except Exception:
                if attempt == max_retries - 1:
                    return None
                continue
        return None

    def _evaluate_condition(
        self,
        condition: Dict[str, Any],
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> bool:
        """Evaluate a condition for conditional execution"""
        # Simplified condition evaluation
        # In production, would use a proper expression evaluator
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")

        if field in input_data:
            field_value = input_data[field]
            if operator == "equals":
                return field_value == value
            elif operator == "greater_than":
                return field_value > value
            elif operator == "less_than":
                return field_value < value
            elif operator == "contains":
                return value in field_value

        return False

    def _check_loop_termination(self, result: Dict[str, Any], tool_config: Dict[str, Any]) -> bool:
        """Check if loop should terminate"""
        termination_condition = tool_config.get("termination_condition")
        if termination_condition:
            return self._evaluate_condition(termination_condition, result, None)
        return False

    def _apply_transformation(self, data: Any, transform: Dict[str, Any]) -> Any:
        """Apply a data transformation"""
        transform_type = transform.get("type")

        if transform_type == "extract":
            # Extract specific fields
            fields = transform.get("fields", [])
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if k in fields}

        elif transform_type == "rename":
            # Rename fields
            mappings = transform.get("mappings", {})
            if isinstance(data, dict):
                return {mappings.get(k, k): v for k, v in data.items()}

        elif transform_type == "filter":
            # Filter data
            condition = transform.get("condition")
            if condition and isinstance(data, list):
                return [item for item in data if self._evaluate_condition(condition, item, None)]

        return data

    # =========================================================================
    # Scale Patterns for Large Tool Sets
    # =========================================================================

    async def _execute_batched(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute tools in batches with aggregation between batches.
        Useful for processing hundreds of tools while managing token limits.
        """
        batch_size = context.get("batch_size", 10) if context else 10
        aggregator = context.get("aggregator", "concat") if context else "concat"

        all_results = []
        tools = composition.tools

        for i in range(0, len(tools), batch_size):
            batch = tools[i:i + batch_size]
            batch_tasks = []

            for tool_config in batch:
                task = self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=input_data
                )
                batch_tasks.append(task)

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Process results
            processed = []
            for result in batch_results:
                if isinstance(result, Exception):
                    processed.append({"error": str(result)})
                else:
                    processed.append(result)

            all_results.extend(processed)

            # Aggregate between batches if needed
            if aggregator == "summarize" and len(all_results) > batch_size:
                all_results = self._aggregate_results(all_results, aggregator)

        return {
            "results": all_results,
            "total_tools": len(tools),
            "batch_size": batch_size,
            "execution_type": "batched"
        }

    async def _execute_streaming(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Wave-based execution with intermediate summaries.
        Tools execute in waves, with results summarized between waves.
        """
        wave_size = context.get("wave_size", 5) if context else 5
        tools = composition.tools
        waves = []
        current_context = input_data.copy()

        for i in range(0, len(tools), wave_size):
            wave_tools = tools[i:i + wave_size]
            wave_tasks = []

            for tool_config in wave_tools:
                # Each tool in the wave gets the accumulated context
                task = self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=current_context
                )
                wave_tasks.append(task)

            wave_results = await asyncio.gather(*wave_tasks, return_exceptions=True)

            wave_summary = {
                "wave": len(waves) + 1,
                "results": [
                    r if not isinstance(r, Exception) else {"error": str(r)}
                    for r in wave_results
                ]
            }
            waves.append(wave_summary)

            # Update context with wave results for next iteration
            current_context["previous_wave"] = wave_summary

        return {
            "waves": waves,
            "total_waves": len(waves),
            "total_tools": len(tools),
            "execution_type": "streaming"
        }

    async def _execute_delegated(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Hierarchical multi-agent delegation pattern.
        Splits work across sub-agents, each handling a subset of tools.
        """
        num_agents = context.get("num_agents", 3) if context else 3
        tools = composition.tools

        # Split tools among agents
        tools_per_agent = len(tools) // num_agents
        agent_assignments = []

        for i in range(num_agents):
            start = i * tools_per_agent
            end = start + tools_per_agent if i < num_agents - 1 else len(tools)
            agent_assignments.append({
                "agent_id": f"agent_{i}",
                "tools": tools[start:end]
            })

        # Execute each agent's work
        agent_results = []
        for assignment in agent_assignments:
            agent_tasks = []
            for tool_config in assignment["tools"]:
                task = self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=input_data
                )
                agent_tasks.append(task)

            results = await asyncio.gather(*agent_tasks, return_exceptions=True)
            agent_results.append({
                "agent_id": assignment["agent_id"],
                "results": [
                    r if not isinstance(r, Exception) else {"error": str(r)}
                    for r in results
                ]
            })

        return {
            "agent_results": agent_results,
            "num_agents": num_agents,
            "total_tools": len(tools),
            "execution_type": "delegated"
        }

    async def _execute_queued(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Async job queue execution pattern.
        Decouples planning from execution with priority queue.
        """
        max_concurrent = context.get("max_concurrent", 5) if context else 5
        tools = composition.tools

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)

        async def execute_with_semaphore(tool_config: Dict[str, Any], priority: int):
            async with semaphore:
                try:
                    result = await self.tool_executor(
                        tool_id=tool_config.get("tool_id"),
                        tool_name=tool_config.get("tool_name"),
                        input_data=input_data
                    )
                    return {
                        "tool_id": tool_config.get("tool_id"),
                        "priority": priority,
                        "status": "completed",
                        "result": result
                    }
                except Exception as e:
                    return {
                        "tool_id": tool_config.get("tool_id"),
                        "priority": priority,
                        "status": "failed",
                        "error": str(e)
                    }

        # Sort by priority if specified
        prioritized_tools = [
            (tool, tool.get("priority", 0))
            for tool in tools
        ]
        prioritized_tools.sort(key=lambda x: -x[1])  # Higher priority first

        tasks = [
            execute_with_semaphore(tool, priority)
            for tool, priority in prioritized_tools
        ]

        results = await asyncio.gather(*tasks)

        return {
            "results": results,
            "total_tools": len(tools),
            "max_concurrent": max_concurrent,
            "execution_type": "queued"
        }

    async def _execute_saga(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Distributed transaction pattern with compensation.
        Executes tools sequentially, with rollback on failure.
        """
        completed_steps = []
        current_data = input_data.copy()

        for i, tool_config in enumerate(composition.tools):
            try:
                result = await self.tool_executor(
                    tool_id=tool_config.get("tool_id"),
                    tool_name=tool_config.get("tool_name"),
                    input_data=current_data
                )

                completed_steps.append({
                    "step": i,
                    "tool_id": tool_config.get("tool_id"),
                    "result": result,
                    "compensation": tool_config.get("compensation")
                })

                current_data = result if isinstance(result, dict) else {"result": result}

            except Exception as e:
                # Execute compensations in reverse order
                compensation_results = []
                for step in reversed(completed_steps):
                    if step.get("compensation"):
                        try:
                            comp_result = await self.tool_executor(
                                tool_id=step["compensation"].get("tool_id"),
                                tool_name=step["compensation"].get("tool_name"),
                                input_data=step["result"]
                            )
                            compensation_results.append({
                                "step": step["step"],
                                "status": "compensated",
                                "result": comp_result
                            })
                        except Exception as comp_error:
                            compensation_results.append({
                                "step": step["step"],
                                "status": "compensation_failed",
                                "error": str(comp_error)
                            })

                return {
                    "status": "rolled_back",
                    "failed_at": i,
                    "error": str(e),
                    "completed_steps": len(completed_steps),
                    "compensations": compensation_results,
                    "execution_type": "saga"
                }

        return {
            "status": "completed",
            "completed_steps": len(completed_steps),
            "final_result": current_data,
            "execution_type": "saga"
        }

    async def _execute_scatter_gather(
        self,
        composition: ToolComposition,
        input_data: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Fan-out to workers, gather and aggregate results.
        Distributes work, collects results, then reduces.
        """
        tools = composition.tools
        reducer_config = composition.metadata.get("reducer")

        # Scatter phase: execute all tools in parallel
        scatter_tasks = []
        for tool_config in tools:
            task = self.tool_executor(
                tool_id=tool_config.get("tool_id"),
                tool_name=tool_config.get("tool_name"),
                input_data=input_data
            )
            scatter_tasks.append(task)

        scatter_results = await asyncio.gather(*scatter_tasks, return_exceptions=True)

        # Process scatter results
        gathered = []
        errors = []
        for i, result in enumerate(scatter_results):
            if isinstance(result, Exception):
                errors.append({
                    "index": i,
                    "tool_id": tools[i].get("tool_id"),
                    "error": str(result)
                })
            else:
                gathered.append({
                    "index": i,
                    "tool_id": tools[i].get("tool_id"),
                    "result": result
                })

        # Gather phase: aggregate results
        if reducer_config:
            try:
                reduced = await self.tool_executor(
                    tool_id=reducer_config.get("tool_id"),
                    tool_name=reducer_config.get("tool_name"),
                    input_data={"gathered": gathered, "errors": errors}
                )
            except Exception as e:
                reduced = {"error": str(e), "type": "reducer_failed"}
        else:
            # Default aggregation
            reduced = self._aggregate_results([g["result"] for g in gathered], "merge")

        return {
            "gathered": gathered,
            "errors": errors,
            "reduced": reduced,
            "total_tools": len(tools),
            "successful": len(gathered),
            "failed": len(errors),
            "execution_type": "scatter_gather"
        }

    def _aggregate_results(self, results: List[Any], strategy: str) -> Any:
        """Aggregate results based on strategy."""
        if strategy == "concat":
            return results
        elif strategy == "merge":
            merged = {}
            for r in results:
                if isinstance(r, dict):
                    merged.update(r)
            return merged
        elif strategy == "summarize":
            # Keep only the last N results as a summary
            return results[-10:] if len(results) > 10 else results
        return results


# ============================================================================
# Webhook and Event System
# ============================================================================

class EventType(str, Enum):
    """Types of events in the system"""
    TOOL_CREATED = "tool.created"
    TOOL_UPDATED = "tool.updated"
    TOOL_DELETED = "tool.deleted"
    TOOL_EXECUTED = "tool.executed"
    TOOL_FAILED = "tool.failed"
    COMPOSITION_CREATED = "composition.created"
    COMPOSITION_EXECUTED = "composition.executed"
    WEBHOOK_REGISTERED = "webhook.registered"
    WEBHOOK_TRIGGERED = "webhook.triggered"


@dataclass
class Event:
    """Represents a system event"""
    type: EventType
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Webhook(BaseModel):
    """Webhook configuration"""
    id: str = PydanticField(default_factory=lambda: str(uuid.uuid4()))
    url: HttpUrl
    events: List[EventType]
    secret: Optional[str] = None
    active: bool = True
    headers: Dict[str, str] = PydanticField(default_factory=dict)
    retry_config: Dict[str, Any] = PydanticField(default_factory=lambda: {
        "max_retries": 3,
        "retry_delay": 5,
        "backoff_multiplier": 2
    })
    created_at: datetime = PydanticField(default_factory=datetime.utcnow)


class EventBus:
    """Event bus for publishing and subscribing to events"""

    def __init__(self):
        self.subscribers: Dict[EventType, List[Callable]] = defaultdict(list)
        self.webhooks: Dict[str, Webhook] = {}
        self.event_history: List[Event] = []
        self.webhook_deliveries: List[Dict[str, Any]] = []

    async def publish(self, event_type: EventType, data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None):
        """Publish an event"""
        event = Event(
            type=event_type,
            data=data,
            metadata=metadata or {}
        )

        # Store event
        self.event_history.append(event)

        # Notify internal subscribers
        for subscriber in self.subscribers[event_type]:
            asyncio.create_task(self._call_subscriber(subscriber, event))

        # Trigger webhooks
        for webhook in self.webhooks.values():
            if webhook.active and event_type in webhook.events:
                asyncio.create_task(self._trigger_webhook(webhook, event))

        logger.info(f"Published event: {event_type}")

    def subscribe(self, event_type: EventType, handler: Callable):
        """Subscribe to an event type"""
        self.subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable):
        """Unsubscribe from an event type"""
        if handler in self.subscribers[event_type]:
            self.subscribers[event_type].remove(handler)

    async def register_webhook(self, webhook: Webhook) -> str:
        """Register a webhook"""
        self.webhooks[webhook.id] = webhook

        # Publish webhook registration event
        await self.publish(
            EventType.WEBHOOK_REGISTERED,
            {"webhook_id": webhook.id, "url": str(webhook.url)}
        )

        return webhook.id

    async def unregister_webhook(self, webhook_id: str):
        """Unregister a webhook"""
        if webhook_id in self.webhooks:
            del self.webhooks[webhook_id]

    async def _call_subscriber(self, subscriber: Callable, event: Event):
        """Call a subscriber with error handling"""
        try:
            if asyncio.iscoroutinefunction(subscriber):
                await subscriber(event)
            else:
                subscriber(event)
        except Exception as e:
            logger.error(f"Error calling subscriber: {e}")

    async def _trigger_webhook(self, webhook: Webhook, event: Event):
        """Trigger a webhook with retries"""
        delivery_id = str(uuid.uuid4())
        payload = {
            "id": event.id,
            "type": event.type.value,
            "timestamp": event.timestamp.isoformat(),
            "data": event.data,
            "metadata": event.metadata
        }

        # Create signature if secret is configured
        headers = webhook.headers.copy()
        if webhook.secret:
            signature = self._create_webhook_signature(webhook.secret, payload)
            headers["X-Webhook-Signature"] = signature

        headers["X-Webhook-Delivery"] = delivery_id
        headers["X-Event-Type"] = event.type.value

        # Attempt delivery with retries
        retry_count = 0
        max_retries = webhook.retry_config.get("max_retries", 3)
        retry_delay = webhook.retry_config.get("retry_delay", 5)
        backoff_multiplier = webhook.retry_config.get("backoff_multiplier", 2)

        while retry_count <= max_retries:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        str(webhook.url),
                        json=payload,
                        headers=headers,
                        timeout=30.0
                    )

                    # Record successful delivery
                    self.webhook_deliveries.append({
                        "delivery_id": delivery_id,
                        "webhook_id": webhook.id,
                        "event_id": event.id,
                        "status": "success",
                        "response_code": response.status_code,
                        "retry_count": retry_count,
                        "timestamp": datetime.utcnow()
                    })

                    # Publish webhook triggered event
                    await self.publish(
                        EventType.WEBHOOK_TRIGGERED,
                        {
                            "webhook_id": webhook.id,
                            "event_id": event.id,
                            "delivery_id": delivery_id,
                            "status": "success"
                        }
                    )

                    return

            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    # Record failed delivery
                    self.webhook_deliveries.append({
                        "delivery_id": delivery_id,
                        "webhook_id": webhook.id,
                        "event_id": event.id,
                        "status": "failed",
                        "error": str(e),
                        "retry_count": retry_count,
                        "timestamp": datetime.utcnow()
                    })

                    logger.error(f"Webhook delivery failed after {max_retries} retries: {e}")
                    break

                # Wait before retrying
                await asyncio.sleep(retry_delay)
                retry_delay *= backoff_multiplier

    def _create_webhook_signature(self, secret: str, payload: Dict[str, Any]) -> str:
        """Create HMAC signature for webhook payload"""
        message = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"


# ============================================================================
# Protocol Adapters
# ============================================================================

class ProtocolAdapter:
    """Base class for protocol adapters"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert external format to internal format"""
        raise NotImplementedError

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal format to external format"""
        raise NotImplementedError


class OpenAIAdapter(ProtocolAdapter):
    """Adapter for OpenAI function calling format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert OpenAI function to internal tool format"""
        function = external_tool.get("function", external_tool)

        return {
            "name": function.get("name"),
            "description": function.get("description"),
            "input_schema": function.get("parameters", {}),
            "output_schema": {},  # OpenAI doesn't specify output schema
            "metadata": {
                "source": "openai",
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to OpenAI function format"""
        return {
            "type": "function",
            "function": {
                "name": internal_tool.get("name"),
                "description": internal_tool.get("description"),
                "parameters": internal_tool.get("input_schema", {})
            }
        }


class AnthropicAdapter(ProtocolAdapter):
    """Adapter for Anthropic tool use format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Anthropic tool to internal format"""
        return {
            "name": external_tool.get("name"),
            "description": external_tool.get("description"),
            "input_schema": external_tool.get("input_schema", {}),
            "output_schema": {},  # Anthropic doesn't specify output schema
            "metadata": {
                "source": "anthropic",
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to Anthropic format"""
        return {
            "name": internal_tool.get("name"),
            "description": internal_tool.get("description"),
            "input_schema": internal_tool.get("input_schema", {})
        }


class LangChainAdapter(ProtocolAdapter):
    """Adapter for LangChain tool format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert LangChain tool to internal format"""
        return {
            "name": external_tool.get("name"),
            "description": external_tool.get("description"),
            "input_schema": external_tool.get("args_schema", {}),
            "output_schema": external_tool.get("output_schema", {}),
            "metadata": {
                "source": "langchain",
                "return_direct": external_tool.get("return_direct", False),
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to LangChain format"""
        return {
            "name": internal_tool.get("name"),
            "description": internal_tool.get("description"),
            "args_schema": internal_tool.get("input_schema", {}),
            "output_schema": internal_tool.get("output_schema", {}),
            "return_direct": internal_tool.get("metadata", {}).get("return_direct", False)
        }


class MCPAdapter(ProtocolAdapter):
    """Adapter for Model Context Protocol (MCP) tool format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert MCP tool to internal format"""
        input_schema = external_tool.get("inputSchema", {})

        return {
            "name": external_tool.get("name"),
            "description": external_tool.get("description"),
            "input_schema": input_schema,
            "output_schema": {},
            "metadata": {
                "source": "mcp",
                "annotations": external_tool.get("annotations", {}),
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to MCP format"""
        result = {
            "name": internal_tool.get("name"),
            "description": internal_tool.get("description"),
            "inputSchema": internal_tool.get("input_schema", {
                "type": "object",
                "properties": {},
                "required": []
            })
        }

        metadata = internal_tool.get("metadata", {})
        if "annotations" in metadata:
            result["annotations"] = metadata["annotations"]

        return result


class GeminiAdapter(ProtocolAdapter):
    """Adapter for Google Gemini function calling format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Gemini function declaration to internal format"""
        func_decl = external_tool.get("functionDeclarations", [external_tool])[0] \
            if "functionDeclarations" in external_tool else external_tool

        parameters = func_decl.get("parameters", {})

        return {
            "name": func_decl.get("name"),
            "description": func_decl.get("description"),
            "input_schema": {
                "type": parameters.get("type", "object"),
                "properties": parameters.get("properties", {}),
                "required": parameters.get("required", [])
            },
            "output_schema": {},
            "metadata": {
                "source": "gemini",
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to Gemini function declaration format"""
        input_schema = internal_tool.get("input_schema", {})

        return {
            "functionDeclarations": [{
                "name": internal_tool.get("name"),
                "description": internal_tool.get("description"),
                "parameters": {
                    "type": input_schema.get("type", "object"),
                    "properties": input_schema.get("properties", {}),
                    "required": input_schema.get("required", [])
                }
            }]
        }


class CohereAdapter(ProtocolAdapter):
    """Adapter for Cohere tool format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Cohere tool to internal format"""
        parameter_definitions = external_tool.get("parameter_definitions", {})

        properties = {}
        required = []

        for param_name, param_def in parameter_definitions.items():
            properties[param_name] = {
                "type": param_def.get("type", "string"),
                "description": param_def.get("description", "")
            }
            if param_def.get("required", False):
                required.append(param_name)

        return {
            "name": external_tool.get("name"),
            "description": external_tool.get("description"),
            "input_schema": {
                "type": "object",
                "properties": properties,
                "required": required
            },
            "output_schema": {},
            "metadata": {
                "source": "cohere",
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to Cohere format"""
        input_schema = internal_tool.get("input_schema", {})
        properties = input_schema.get("properties", {})
        required = input_schema.get("required", [])

        parameter_definitions = {}
        for prop_name, prop_def in properties.items():
            parameter_definitions[prop_name] = {
                "type": prop_def.get("type", "string"),
                "description": prop_def.get("description", ""),
                "required": prop_name in required
            }

        return {
            "name": internal_tool.get("name"),
            "description": internal_tool.get("description"),
            "parameter_definitions": parameter_definitions
        }


class MistralAdapter(ProtocolAdapter):
    """Adapter for Mistral AI tool format"""

    def to_internal(self, external_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Mistral tool to internal format"""
        function = external_tool.get("function", external_tool)

        return {
            "name": function.get("name"),
            "description": function.get("description"),
            "input_schema": function.get("parameters", {}),
            "output_schema": {},
            "metadata": {
                "source": "mistral",
                "original": external_tool
            }
        }

    def from_internal(self, internal_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool to Mistral format"""
        return {
            "type": "function",
            "function": {
                "name": internal_tool.get("name"),
                "description": internal_tool.get("description"),
                "parameters": internal_tool.get("input_schema", {})
            }
        }


# ============================================================================
# Tool Registry Federation
# ============================================================================

class RegistryNode(BaseModel):
    """Represents a federated registry node"""
    id: str = PydanticField(default_factory=lambda: str(uuid.uuid4()))
    name: str
    url: HttpUrl
    api_key: Optional[str] = None
    active: bool = True
    capabilities: List[str] = PydanticField(default_factory=list)
    last_sync: Optional[datetime] = None
    metadata: Dict[str, Any] = PydanticField(default_factory=dict)


class ToolFederation:
    """Manages federation with other tool registries"""

    def __init__(self):
        self.nodes: Dict[str, RegistryNode] = {}
        self.cached_tools: Dict[str, List[Dict[str, Any]]] = {}
        self.sync_interval = timedelta(minutes=15)

    async def add_node(self, node: RegistryNode) -> str:
        """Add a federated registry node"""
        self.nodes[node.id] = node
        await self.sync_node(node.id)
        return node.id

    async def remove_node(self, node_id: str):
        """Remove a federated registry node"""
        if node_id in self.nodes:
            del self.nodes[node_id]
            if node_id in self.cached_tools:
                del self.cached_tools[node_id]

    async def sync_node(self, node_id: str):
        """Sync tools from a specific node"""
        node = self.nodes.get(node_id)
        if not node or not node.active:
            return

        try:
            headers = {}
            if node.api_key:
                headers["Authorization"] = f"Bearer {node.api_key}"

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{node.url}/api/v1/tools",
                    headers=headers,
                    timeout=30.0
                )
                response.raise_for_status()

                tools = response.json()
                self.cached_tools[node_id] = tools

                node.last_sync = datetime.utcnow()
                logger.info(f"Synced {len(tools)} tools from node {node.name}")

        except Exception as e:
            logger.error(f"Failed to sync node {node.name}: {e}")

    async def sync_all_nodes(self):
        """Sync all active nodes"""
        tasks = []
        for node_id, node in self.nodes.items():
            if node.active:
                # Check if sync is needed
                if not node.last_sync or \
                   datetime.utcnow() - node.last_sync > self.sync_interval:
                    tasks.append(self.sync_node(node_id))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def search_federated(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Search across all federated registries"""
        results = []

        # Search local cached tools
        for node_id, tools in self.cached_tools.items():
            node = self.nodes[node_id]
            for tool in tools:
                # Apply filters
                if self._matches_filters(tool, filters):
                    # Add node information
                    tool["_federation"] = {
                        "node_id": node_id,
                        "node_name": node.name,
                        "node_url": str(node.url)
                    }
                    results.append(tool)

        return results

    async def execute_federated_tool(
        self,
        node_id: str,
        tool_id: str,
        input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a tool on a federated node"""
        node = self.nodes.get(node_id)
        if not node:
            raise ValueError(f"Node '{node_id}' not found")

        headers = {}
        if node.api_key:
            headers["Authorization"] = f"Bearer {node.api_key}"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{node.url}/api/v1/tools/execute",
                json={
                    "tool_id": tool_id,
                    "input_data": input_data
                },
                headers=headers,
                timeout=120.0
            )
            response.raise_for_status()
            return response.json()

    def _matches_filters(self, tool: Dict[str, Any], filters: Optional[Dict[str, Any]]) -> bool:
        """Check if a tool matches the provided filters"""
        if not filters:
            return True

        for key, value in filters.items():
            tool_value = tool.get(key)
            if tool_value != value:
                return False

        return True


# ============================================================================
# Interconnectivity Manager
# ============================================================================

class InterconnectivityManager:
    """Main manager for all interconnectivity features"""

    def __init__(self, tool_executor: Callable):
        self.tool_chain = ToolChain(tool_executor)
        self.event_bus = EventBus()
        self.federation = ToolFederation()
        self.adapters = {
            "openai": OpenAIAdapter(),
            "anthropic": AnthropicAdapter(),
            "langchain": LangChainAdapter(),
            "mcp": MCPAdapter(),
            "gemini": GeminiAdapter(),
            "cohere": CohereAdapter(),
            "mistral": MistralAdapter(),
        }

    async def initialize(self):
        """Initialize the interconnectivity manager"""
        # Start background tasks
        asyncio.create_task(self._sync_federation_periodically())

    async def _sync_federation_periodically(self):
        """Background task to sync federated nodes"""
        while True:
            try:
                await self.federation.sync_all_nodes()
            except Exception as e:
                logger.error(f"Federation sync error: {e}")

            await asyncio.sleep(900)  # Sync every 15 minutes

    def get_adapter(self, protocol: str) -> Optional[ProtocolAdapter]:
        """Get a protocol adapter"""
        return self.adapters.get(protocol)

    def register_adapter(self, name: str, adapter: ProtocolAdapter):
        """Register a new protocol adapter"""
        self.adapters[name] = adapter
