"""
Temporal Workflow Activities

Concrete task execution activities that run within Temporal workflows.
These are the actual operations performed by agents.
"""

import logging
from typing import Any, Dict, Optional, List
from dataclasses import dataclass
from temporalio import activity

logger = logging.getLogger(__name__)


@dataclass
class ActivityResult:
    """Result of an activity execution."""
    success: bool
    output: Any
    error: Optional[str] = None
    duration_seconds: float = 0.0
    tokens_used: int = 0
    cost_usd: float = 0.0


@activity.defn
async def execute_code_activity(
    code: str,
    language: str = "python",
    timeout_seconds: float = 30.0,
) -> ActivityResult:
    """
    Execute code in specified language.

    Args:
        code: Code to execute
        language: Programming language (python, javascript, etc)
        timeout_seconds: Execution timeout

    Returns:
        ActivityResult with success/output/error
    """
    try:
        logger.info(f"Executing {language} code ({len(code)} chars)")

        if language != "python":
            raise ValueError(f"Unsupported language: {language}")

        # In real implementation:
        # 1. Create isolated sandbox
        # 2. Execute code with timeout
        # 3. Capture output and errors
        # 4. Return result

        # For now, simulate execution
        import asyncio
        await asyncio.sleep(0.1)  # Simulate execution time

        result = ActivityResult(
            success=True,
            output={"stdout": f"Executed {len(code)} chars of {language}"},
            duration_seconds=0.1,
            tokens_used=len(code) // 4,  # Rough estimate
            cost_usd=0.01,
        )

        logger.info(f"Code execution succeeded")
        return result

    except Exception as e:
        logger.error(f"Code execution failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def search_web_activity(
    query: str,
    max_results: int = 10,
) -> ActivityResult:
    """
    Perform web search.

    Args:
        query: Search query
        max_results: Max search results to return

    Returns:
        ActivityResult with search results
    """
    try:
        logger.info(f"Searching for: {query}")

        # In real implementation:
        # 1. Call search API (Google, DuckDuckGo, etc)
        # 2. Parse results
        # 3. Extract relevant info
        # 4. Return results

        # For now, simulate search
        import asyncio
        await asyncio.sleep(0.2)  # Simulate network latency

        result = ActivityResult(
            success=True,
            output={
                "query": query,
                "results": [
                    {"title": f"Result {i}", "url": f"http://example.com/{i}"}
                    for i in range(max_results)
                ],
            },
            duration_seconds=0.2,
            tokens_used=len(query) * 2,
            cost_usd=0.02,
        )

        logger.info(f"Search returned {max_results} results")
        return result

    except Exception as e:
        logger.error(f"Search failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def call_llm_activity(
    prompt: str,
    model: str = "claude-3.5-sonnet",
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> ActivityResult:
    """
    Call LLM API for text generation.

    Args:
        prompt: Input prompt
        model: Model name
        max_tokens: Max output tokens
        temperature: Sampling temperature (0-1)

    Returns:
        ActivityResult with generated text
    """
    try:
        logger.info(f"Calling {model} with {len(prompt)} char prompt")

        # In real implementation:
        # 1. Call LLM API (OpenAI, Anthropic, etc)
        # 2. Stream or wait for completion
        # 3. Track tokens and cost
        # 4. Return result

        # For now, simulate LLM call
        import asyncio
        await asyncio.sleep(0.3)  # Simulate API latency

        output_tokens = max_tokens // 2  # Rough estimate
        prompt_tokens = len(prompt) // 4

        result = ActivityResult(
            success=True,
            output={
                "text": f"Generated response to: {prompt[:50]}...",
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": output_tokens,
            },
            duration_seconds=0.3,
            tokens_used=prompt_tokens + output_tokens,
            cost_usd=0.05,  # Rough estimate
        )

        logger.info(f"LLM call completed, {output_tokens} output tokens")
        return result

    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def aggregate_results_activity(
    results: List[Dict[str, Any]],
) -> ActivityResult:
    """
    Aggregate multiple results into final output.

    Args:
        results: List of results to aggregate

    Returns:
        ActivityResult with aggregated output
    """
    try:
        logger.info(f"Aggregating {len(results)} results")

        aggregated = {
            "total_results": len(results),
            "successful": sum(1 for r in results if r.get("success")),
            "failed": sum(1 for r in results if not r.get("success")),
            "combined_output": [r.get("output") for r in results if r.get("success")],
        }

        result = ActivityResult(
            success=True,
            output=aggregated,
            duration_seconds=0.1,
            cost_usd=0.0,
        )

        logger.info(f"Aggregation complete: {aggregated['successful']} successful")
        return result

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def update_agent_state_activity(
    agent_id: str,
    capability_name: str,
    success: bool,
    confidence_adjustment: float = 0.0,
) -> ActivityResult:
    """
    Update agent's capability state after task execution.

    Args:
        agent_id: Agent being updated
        capability_name: Capability that was used
        success: Whether execution succeeded
        confidence_adjustment: Adjustment to confidence score

    Returns:
        ActivityResult confirming state update
    """
    try:
        logger.info(f"Updating {agent_id} capability: {capability_name} (success={success})")

        # In real implementation:
        # 1. Query GraphSub for current state
        # 2. Update capability score
        # 3. Log transaction
        # 4. Persist to GraphSub

        result = ActivityResult(
            success=True,
            output={
                "agent_id": agent_id,
                "capability": capability_name,
                "updated": True,
                "adjustment": confidence_adjustment,
            },
            duration_seconds=0.05,
            cost_usd=0.0,
        )

        logger.info(f"State update complete for {agent_id}")
        return result

    except Exception as e:
        logger.error(f"State update failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def deposit_pheromone_activity(
    agent_id: str,
    task_id: str,
    signal_type: str,
    intensity: float,
    ttl_seconds: int = 300,
) -> ActivityResult:
    """
    Deposit stigmergic signal (pheromone) in GraphSub.

    Args:
        agent_id: Agent depositing signal
        task_id: Task being signaled
        signal_type: Type of signal (progress, success, warning, etc)
        intensity: Signal intensity (0-1)
        ttl_seconds: Time-to-live for signal

    Returns:
        ActivityResult confirming deposition
    """
    try:
        logger.info(f"Depositing {signal_type} pheromone from {agent_id}")

        # In real implementation:
        # 1. Calculate decay rate
        # 2. Create pheromone edge in GraphSub
        # 3. Set TTL
        # 4. Persist

        result = ActivityResult(
            success=True,
            output={
                "agent_id": agent_id,
                "task_id": task_id,
                "signal": signal_type,
                "intensity": intensity,
                "ttl": ttl_seconds,
            },
            duration_seconds=0.05,
            cost_usd=0.0,
        )

        logger.info(f"Pheromone deposited: {signal_type}")
        return result

    except Exception as e:
        logger.error(f"Pheromone deposition failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )


@activity.defn
async def handle_task_failure_activity(
    agent_id: str,
    task_id: str,
    error: str,
    refund_percentage: float = 1.0,
) -> ActivityResult:
    """
    Handle task failure: refund budget, update state, deposit warning.

    Args:
        agent_id: Agent that failed
        task_id: Failed task
        error: Error message
        refund_percentage: Budget refund percentage (0-1)

    Returns:
        ActivityResult confirming failure handling
    """
    try:
        logger.warning(f"Handling failure for task {task_id} on {agent_id}: {error}")

        # In real implementation:
        # 1. Refund budget to agent
        # 2. Update task status to failed
        # 3. Deposit warning pheromone
        # 4. Log failure for analysis

        result = ActivityResult(
            success=True,
            output={
                "agent_id": agent_id,
                "task_id": task_id,
                "error": error,
                "refund_percentage": refund_percentage,
                "handled": True,
            },
            duration_seconds=0.1,
            cost_usd=0.0,
        )

        logger.info(f"Failure handling complete for {task_id}")
        return result

    except Exception as e:
        logger.error(f"Failure handling failed: {e}")
        return ActivityResult(
            success=False,
            output=None,
            error=str(e),
            duration_seconds=0.0,
        )
