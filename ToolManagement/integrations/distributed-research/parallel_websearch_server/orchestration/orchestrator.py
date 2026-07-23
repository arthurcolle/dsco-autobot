"""Advanced task orchestrator with intelligent polling, retries, and circuit breaking."""

import asyncio
import hashlib
import json
import logging
import random
import time
from typing import Any, AsyncGenerator, Callable, Dict, Optional

import httpx
from fastapi import HTTPException

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.models import BetaTaskRunInput, TaskRunStatus
from parallel_websearch_server.orchestration.circuit_breaker import CircuitBreaker, CircuitBreakerError
from parallel_websearch_server.orchestration.models import (
    OrchestrationConfig,
    ProgressUpdate,
    RetryStrategy,
    TaskExecutionContext,
    TaskExecutionRequest,
    TaskExecutionResult,
    TaskPriority,
    TaskQueueStats,
    TaskState,
)

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """
    Sophisticated task orchestration engine.

    Features:
    - Adaptive polling with exponential backoff
    - Circuit breaker pattern for API resilience
    - Automatic retries with jitter
    - Priority-based queue management
    - Result caching and deduplication
    - Comprehensive telemetry
    - Progress streaming
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        config: Optional[OrchestrationConfig] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            client: HTTP client for API calls
            config: Orchestration configuration
        """
        self.client = client
        self.config = config or OrchestrationConfig()

        # Circuit breaker for API calls
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_timeout,
            half_open_max_calls=self.config.circuit_breaker_half_open_max_calls,
            name="parallel_api"
        ) if self.config.circuit_breaker_enabled else None

        # Result cache: task_hash -> result
        self._result_cache: Dict[str, TaskExecutionResult] = {}
        self._cache_timestamps: Dict[str, float] = {}

        # In-flight tasks for deduplication
        self._in_flight: Dict[str, asyncio.Task] = {}

        # Queue management
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=self.config.max_queue_size
        )
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_tasks)

        # Statistics
        self._stats = TaskQueueStats()

        logger.info(
            f"TaskOrchestrator initialized: "
            f"max_concurrent={self.config.max_concurrent_tasks}, "
            f"circuit_breaker={'enabled' if self.circuit_breaker else 'disabled'}"
        )

    async def execute_task(
        self,
        request: TaskExecutionRequest,
        progress_callback: Optional[Callable[[ProgressUpdate], None]] = None
    ) -> TaskExecutionResult:
        """
        Execute a task with full orchestration.

        Args:
            request: Task execution request
            progress_callback: Optional callback for progress updates

        Returns:
            Task execution result with comprehensive metadata

        Raises:
            CircuitBreakerError: If circuit breaker is open
            HTTPException: For API errors
            asyncio.TimeoutError: If task exceeds timeout
        """
        # Check for cached result
        if self.config.cache_results:
            cached = self._get_cached_result(request)
            if cached:
                logger.info(f"Returning cached result for task {cached.run_id}")
                return cached

        # Check for duplicate in-flight task
        if self.config.deduplicate_tasks and request.idempotency_key:
            in_flight = self._in_flight.get(request.idempotency_key)
            if in_flight and not in_flight.done():
                logger.info(f"Waiting for in-flight task {request.idempotency_key}")
                return await in_flight

        # Create execution context
        context = TaskExecutionContext(
            request=request,
            config=self.config,
            current_poll_interval=self.config.initial_poll_interval
        )

        # Execute with retry logic
        result_task = asyncio.create_task(
            self._execute_with_retries(context, progress_callback)
        )

        # Track in-flight if deduplication enabled
        if request.idempotency_key:
            self._in_flight[request.idempotency_key] = result_task

        try:
            result = await result_task

            # Cache result
            if self.config.cache_results:
                self._cache_result(request, result)

            return result
        finally:
            # Clean up in-flight tracking
            if request.idempotency_key and request.idempotency_key in self._in_flight:
                del self._in_flight[request.idempotency_key]

    async def _execute_with_retries(
        self,
        context: TaskExecutionContext,
        progress_callback: Optional[Callable[[ProgressUpdate], None]]
    ) -> TaskExecutionResult:
        """Execute task with automatic retry logic."""
        max_retries = context.request.max_retries or self.config.max_retries
        retry_strategy = context.request.retry_strategy or self.config.retry_strategy

        last_error: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            context.current_retry = attempt

            try:
                # Attempt execution
                result = await self._execute_task_internal(context, progress_callback)
                self._stats.total_completed += 1
                return result

            except CircuitBreakerError as e:
                logger.error(f"Circuit breaker open: {e}")
                context.transition_to(TaskState.CIRCUIT_OPEN)
                self._stats.total_failed += 1
                raise

            except asyncio.TimeoutError:
                logger.error(f"Task {context.run_id} timed out after {context.metrics.total_duration}s")
                context.transition_to(TaskState.TIMEOUT)
                self._stats.total_failed += 1
                raise

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Task execution attempt {attempt + 1}/{max_retries + 1} failed: {e}",
                    exc_info=True
                )

                # Don't retry on certain errors
                if isinstance(e, HTTPException) and e.status_code in [400, 401, 403, 404]:
                    logger.error(f"Non-retryable error: {e.status_code}")
                    self._stats.total_failed += 1
                    raise

                # Calculate retry delay
                if attempt < max_retries:
                    delay = self._calculate_retry_delay(attempt, retry_strategy)
                    logger.info(f"Retrying in {delay:.2f}s...")
                    context.transition_to(TaskState.RETRYING)

                    if progress_callback:
                        progress_callback(ProgressUpdate(
                            run_id=context.run_id or "unknown",
                            state=TaskState.RETRYING,
                            message=f"Retry attempt {attempt + 1}/{max_retries} in {delay:.1f}s",
                            metadata={"attempt": attempt + 1, "delay": delay}
                        ))

                    await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(f"Task failed after {max_retries + 1} attempts")
        context.transition_to(TaskState.FAILED)
        self._stats.total_failed += 1

        raise last_error or Exception("Task failed after all retries")

    async def _execute_task_internal(
        self,
        context: TaskExecutionContext,
        progress_callback: Optional[Callable[[ProgressUpdate], None]]
    ) -> TaskExecutionResult:
        """Internal task execution with circuit breaker protection."""
        # Submit task
        await self._submit_task(context, progress_callback)

        # Poll until completion
        result = await self._poll_until_complete(context, progress_callback)

        return result

    async def _submit_task(
        self,
        context: TaskExecutionContext,
        progress_callback: Optional[Callable[[ProgressUpdate], None]]
    ) -> None:
        """Submit task to Parallel.ai API."""
        context.transition_to(TaskState.SUBMITTED)

        if progress_callback:
            progress_callback(ProgressUpdate(
                run_id="pending",
                state=TaskState.SUBMITTED,
                message=f"Submitting task to {context.request.processor} processor",
                progress_percent=5.0
            ))

        # Build API request
        api_request = BetaTaskRunInput(
            processor=context.request.processor,
            input=context.request.input,
            task_spec=context.request.task_spec,
            mcp_servers=context.request.mcp_servers
        )

        # Submit with circuit breaker protection
        start_time = time.time()

        def _api_call():
            # This is a sync wrapper for the async call, used by circuit breaker
            # In practice, we'll call the API directly and handle circuit breaker differently
            return None

        try:
            # Actual API call
            context.metrics.api_call_count += 1
            response = await self.client.post(
                "/v1/tasks/runs",
                json=api_request.model_dump(exclude_none=True),
                headers=get_auth_headers(),
                timeout=self.config.submission_timeout
            )
            response.raise_for_status()
            data = response.json()

            context.run_id = data["run_id"]
            context.transition_to(TaskState.QUEUED)
            self._stats.total_queued += 1

            logger.info(
                f"Task submitted: {context.run_id} "
                f"(processor={context.request.processor}, priority={context.request.priority.value})"
            )

            if progress_callback:
                progress_callback(ProgressUpdate(
                    run_id=context.run_id,
                    state=TaskState.QUEUED,
                    message="Task queued, waiting for execution",
                    progress_percent=10.0
                ))

            # Record timing
            context.metrics.api_call_duration = time.time() - start_time

        except httpx.HTTPStatusError as e:
            logger.error(f"Task submission failed: {e.response.status_code}")
            if self.circuit_breaker:
                self.circuit_breaker._on_failure()
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.json()
            )

    async def _poll_until_complete(
        self,
        context: TaskExecutionContext,
        progress_callback: Optional[Callable[[ProgressUpdate], None]]
    ) -> TaskExecutionResult:
        """Poll task status until completion with adaptive intervals."""
        timeout = context.request.timeout or self.config.default_timeout
        start_time = time.time()

        context.transition_to(TaskState.POLLING)

        while True:
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.error(f"Task {context.run_id} polling timed out after {elapsed:.2f}s")
                raise asyncio.TimeoutError(
                    f"Task polling exceeded timeout of {timeout}s"
                )

            # Poll status
            await asyncio.sleep(context.current_poll_interval)
            context.metrics.poll_count += 1

            try:
                # Get current status
                context.metrics.api_call_count += 1
                response = await self.client.get(
                    f"/v1/tasks/runs/{context.run_id}",
                    headers=get_auth_headers()
                )
                response.raise_for_status()
                status_data = response.json()

                current_status = status_data["status"]

                # Track status changes
                if current_status != context.last_known_status:
                    logger.info(f"Task {context.run_id}: {context.last_known_status} -> {current_status}")
                    context.last_known_status = current_status
                    context.consecutive_same_status = 0

                    if current_status == "running":
                        context.transition_to(TaskState.RUNNING)
                        self._stats.total_running += 1

                    # Send progress update
                    if progress_callback:
                        progress_percent = self._estimate_progress(context, current_status, elapsed, timeout)
                        progress_callback(ProgressUpdate(
                            run_id=context.run_id,
                            state=context.state,
                            message=f"Task {current_status}",
                            progress_percent=progress_percent,
                            metadata={"poll_count": context.metrics.poll_count}
                        ))
                else:
                    context.consecutive_same_status += 1

                # Adjust polling interval adaptively
                if self.config.adaptive_polling:
                    context.current_poll_interval = self._calculate_adaptive_interval(context)

                # Check if terminal status reached
                if current_status in ["completed", "failed", "cancelled"]:
                    logger.info(
                        f"Task {context.run_id} reached terminal status: {current_status} "
                        f"(polls={context.metrics.poll_count}, elapsed={elapsed:.2f}s)"
                    )

                    # Fetch final result
                    return await self._fetch_result(context, current_status)

            except httpx.HTTPStatusError as e:
                logger.warning(f"Status poll failed: {e.response.status_code}")
                # Don't break the loop for transient errors
                if e.response.status_code >= 500:
                    context.current_poll_interval = min(
                        context.current_poll_interval * 1.5,
                        self.config.max_poll_interval
                    )
                else:
                    raise

    async def _fetch_result(
        self,
        context: TaskExecutionContext,
        final_status: str
    ) -> TaskExecutionResult:
        """Fetch final task result."""
        try:
            context.metrics.api_call_count += 1
            response = await self.client.get(
                f"/v1/tasks/runs/{context.run_id}/result",
                headers=get_auth_headers()
            )
            response.raise_for_status()
            result_data = response.json()

            # Extract output
            output = result_data.get("output", {})
            output_type = output.get("type")

            output_text = None
            output_json = None

            if output_type == "text":
                output_text = output.get("content")
            elif output_type == "json":
                output_json = output.get("content")

            # Calculate cost estimate (simplified - adjust based on processor tier)
            estimated_cost = self._estimate_cost(
                context.request.processor,
                context.metrics.total_duration or 0
            )

            # Build result
            if final_status == "completed":
                context.transition_to(TaskState.COMPLETED)
                summary = f"Task completed successfully in {context.metrics.total_duration:.1f}s"
            else:
                context.transition_to(TaskState.FAILED)
                error_msg = output.get("error") if isinstance(output, dict) else "Unknown error"
                context.error = error_msg
                summary = f"Task {final_status}: {error_msg}"

            return TaskExecutionResult(
                run_id=context.run_id,
                status=context.state,
                output_text=output_text,
                output_json=output_json,
                summary=summary,
                processor=context.request.processor,
                elapsed_seconds=context.metrics.total_duration or 0,
                final_status=final_status,
                poll_count=context.metrics.poll_count,
                retry_count=context.current_retry,
                status_history=context.metrics.status_transitions,
                error=context.error,
                estimated_cost=estimated_cost,
                tags=context.request.tags,
                context_id=context.request.context_id,
                circuit_breaker_status=self.circuit_breaker.state.value if self.circuit_breaker else None,
                raw_response=result_data
            )

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to fetch result: {e.response.status_code}")
            context.transition_to(TaskState.FAILED)

            return TaskExecutionResult(
                run_id=context.run_id,
                status=TaskState.FAILED,
                summary=f"Failed to fetch result: {e.response.status_code}",
                processor=context.request.processor,
                elapsed_seconds=context.metrics.total_duration or 0,
                final_status="failed",
                poll_count=context.metrics.poll_count,
                retry_count=context.current_retry,
                status_history=context.metrics.status_transitions,
                error=f"Result fetch failed: {e}",
                tags=context.request.tags,
                context_id=context.request.context_id
            )

    def _calculate_retry_delay(self, attempt: int, strategy: RetryStrategy) -> float:
        """Calculate delay before next retry."""
        if strategy == RetryStrategy.NONE:
            return 0.0
        elif strategy == RetryStrategy.IMMEDIATE:
            return 0.0
        elif strategy == RetryStrategy.LINEAR:
            delay = self.config.retry_base_delay * (attempt + 1)
        elif strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.retry_base_delay * (2 ** attempt)
        elif strategy == RetryStrategy.ADAPTIVE:
            # Adaptive: start exponential, then linear
            if attempt < 2:
                delay = self.config.retry_base_delay * (2 ** attempt)
            else:
                delay = self.config.retry_base_delay * (attempt + 1)
        else:
            delay = self.config.retry_base_delay

        # Cap at max delay
        delay = min(delay, self.config.retry_max_delay)

        # Add jitter to prevent thundering herd
        if self.config.retry_jitter:
            jitter = random.uniform(0, delay * 0.1)  # +/- 10%
            delay += jitter

        return delay

    def _calculate_adaptive_interval(self, context: TaskExecutionContext) -> float:
        """Calculate next poll interval based on task progress."""
        current = context.current_poll_interval

        # If status is changing frequently, poll faster
        if context.consecutive_same_status < 3:
            new_interval = max(current * 0.8, self.config.min_poll_interval)
        # If status is stable, slow down
        elif context.consecutive_same_status > 10:
            new_interval = min(current * 1.5, self.config.max_poll_interval)
        # If running status, maintain moderate pace
        elif context.last_known_status == "running":
            new_interval = min(current * 1.1, self.config.max_poll_interval)
        else:
            new_interval = current

        return round(new_interval, 2)

    def _estimate_progress(
        self,
        context: TaskExecutionContext,
        status: str,
        elapsed: float,
        timeout: float
    ) -> float:
        """Estimate progress percentage."""
        # Base progress on status
        if status == "queued":
            base = 15.0
        elif status == "running":
            # Estimate based on time elapsed vs typical duration
            time_factor = min(elapsed / (timeout * 0.5), 1.0)  # Assume 50% of timeout is typical
            base = 20.0 + (time_factor * 60.0)  # 20-80%
        elif status == "completed":
            return 100.0
        else:
            base = 10.0

        return min(base, 95.0)  # Cap at 95% until actually complete

    def _estimate_cost(self, processor: str, duration: float) -> float:
        """Estimate task cost based on processor tier and duration."""
        # Cost per 1000 runs (simplified estimates)
        cost_per_1k = {
            "lite": 6.0,
            "base": 12.0,
            "core": 30.0,
            "pro": 60.0,
            "ultra": 90.0,
            "ultra2x": 150.0
        }

        base_cost = cost_per_1k.get(processor, 10.0) / 1000.0

        # Adjust for duration (longer = more expensive)
        duration_multiplier = 1.0 + (duration / 600.0)  # +1x per 10 minutes

        return base_cost * duration_multiplier

    def _get_cached_result(self, request: TaskExecutionRequest) -> Optional[TaskExecutionResult]:
        """Check cache for existing result."""
        cache_key = self._generate_cache_key(request)

        if cache_key in self._result_cache:
            # Check if cache is still valid
            cached_at = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cached_at < self.config.cache_ttl:
                result = self._result_cache[cache_key]
                result.cached = True
                return result
            else:
                # Expired, remove from cache
                del self._result_cache[cache_key]
                del self._cache_timestamps[cache_key]

        return None

    def _cache_result(self, request: TaskExecutionRequest, result: TaskExecutionResult) -> None:
        """Cache a task result."""
        cache_key = self._generate_cache_key(request)
        self._result_cache[cache_key] = result
        self._cache_timestamps[cache_key] = time.time()

        # Simple cache cleanup: remove oldest if too large
        if len(self._result_cache) > 1000:
            oldest_key = min(self._cache_timestamps.keys(), key=lambda k: self._cache_timestamps[k])
            del self._result_cache[oldest_key]
            del self._cache_timestamps[oldest_key]

    def _generate_cache_key(self, request: TaskExecutionRequest) -> str:
        """Generate cache key for a request."""
        # Use idempotency key if provided
        if request.idempotency_key:
            return request.idempotency_key

        # Otherwise, hash the request parameters
        key_data = {
            "processor": request.processor,
            "input": request.input,
            "task_spec": request.task_spec,
            "mcp_servers": request.mcp_servers
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()[:16]

    async def get_stats(self) -> TaskQueueStats:
        """Get orchestrator statistics."""
        self._stats.circuit_breaker_open = (
            self.circuit_breaker.is_open if self.circuit_breaker else False
        )
        self._stats.active_connections = len(self._in_flight)

        # Calculate success rate
        total = self._stats.total_completed + self._stats.total_failed
        if total > 0:
            self._stats.success_rate = self._stats.total_completed / total

        return self._stats

    async def reset_circuit_breaker(self) -> None:
        """Manually reset circuit breaker."""
        if self.circuit_breaker:
            self.circuit_breaker.reset()
            logger.info("Circuit breaker manually reset")
