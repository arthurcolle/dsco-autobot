"""
Advanced Orchestration Engine - Context-Aware Multi-Agent Workflows

Features:
- Context-aware tool recommendations based on user intent
- Intelligent tool chaining with automatic parameter mapping
- Conditional execution and retry logic
- Multi-agent workflow orchestration
- Tool usage analytics and optimization
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import httpx

from ..embeddings.semantic_tool_store import get_tool_store
from ..queue import get_task_queue

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orchestration", tags=["orchestration"])


# =============================================================================
# DATA MODELS
# =============================================================================

class WorkflowIntent(str, Enum):
    """High-level user intents"""
    RESEARCH = "research"
    REMEMBER = "remember"
    NOTIFY = "notify"
    TRANSFORM = "transform"
    ANALYZE = "analyze"
    PUBLISH = "publish"


class ExecutionStrategy(str, Enum):
    """Execution strategies for workflows"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    RETRY = "retry"
    FALLBACK = "fallback"


@dataclass
class ToolRecommendation:
    """A recommended tool with context"""
    tool_name: str
    backend: str
    confidence: float
    reasoning: str
    inputs_template: Dict[str, Any]
    depends_on: List[str] = field(default_factory=list)


@dataclass
class WorkflowContext:
    """Context for workflow execution"""
    intent: WorkflowIntent
    user_query: str
    available_data: Dict[str, Any]
    execution_history: List[Dict[str, Any]] = field(default_factory=list)
    learned_patterns: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolUsageMetrics:
    """Metrics for tool usage"""
    tool_name: str
    backend: str
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    avg_duration_ms: float = 0.0
    last_used: Optional[datetime] = None
    success_rate: float = 0.0


# =============================================================================
# INTELLIGENT RECOMMENDATION ENGINE
# =============================================================================

class ContextAwareRecommender:
    """Recommends tools based on user intent and context"""

    def __init__(self):
        self.tool_store = get_tool_store()
        self.intent_patterns = self._build_intent_patterns()
        self.usage_metrics: Dict[str, ToolUsageMetrics] = {}

    def _build_intent_patterns(self) -> Dict[WorkflowIntent, List[str]]:
        """Define patterns for different intents"""
        return {
            WorkflowIntent.RESEARCH: [
                "search → extract_data → synthesize",
                "deep_search → consolidate → store_belief",
                "find_all → verify → record_episode"
            ],
            WorkflowIntent.REMEMBER: [
                "focus → record_episode → consolidate",
                "store_belief → query_beliefs",
                "record_episode → recall_episodes"
            ],
            WorkflowIntent.NOTIFY: [
                "publish_event → create_webhook",
                "send_notification → trigger_webhook"
            ],
            WorkflowIntent.TRANSFORM: [
                "read → extract_data → embed",
                "search → rerank → consolidate"
            ],
            WorkflowIntent.ANALYZE: [
                "deep_search → synthesize → store_belief",
                "recall_episodes → query_beliefs → consolidate"
            ],
            WorkflowIntent.PUBLISH: [
                "consolidate → publish_event → send_notification",
                "record_episode → publish_event"
            ]
        }

    async def recommend_workflow(
        self,
        context: WorkflowContext,
        max_steps: int = 5
    ) -> List[ToolRecommendation]:
        """Recommend a complete workflow based on context"""

        # Get relevant tools through semantic search
        search_results = await self.tool_store.search_tools(
            query=context.user_query,
            limit=15,
            min_similarity=0.2
        )

        # Get intent-specific patterns
        patterns = self.intent_patterns.get(context.intent, [])

        # Build recommendation chain
        recommendations = []
        used_tools = set()

        for result in search_results:
            tool = result['tool']
            tool_name = tool['name']
            backend = result['backend']
            similarity = result['similarity']

            if tool_name in used_tools:
                continue

            # Check if tool fits the intent pattern
            fits_pattern = any(tool_name in pattern for pattern in patterns)
            confidence = similarity * (1.2 if fits_pattern else 1.0)

            # Determine dependencies
            depends_on = []
            if len(recommendations) > 0:
                # This tool might depend on previous outputs
                depends_on = [recommendations[-1].tool_name]

            # Generate reasoning
            reasoning = self._generate_reasoning(
                tool_name, context.intent, similarity, fits_pattern
            )

            # Create inputs template
            inputs_template = self._generate_inputs_template(
                tool, depends_on, context.available_data
            )

            recommendation = ToolRecommendation(
                tool_name=tool_name,
                backend=backend,
                confidence=min(confidence, 1.0),
                reasoning=reasoning,
                inputs_template=inputs_template,
                depends_on=depends_on
            )

            recommendations.append(recommendation)
            used_tools.add(tool_name)

            if len(recommendations) >= max_steps:
                break

        # Sort by confidence
        recommendations.sort(key=lambda x: x.confidence, reverse=True)

        return recommendations[:max_steps]

    def _generate_reasoning(
        self,
        tool_name: str,
        intent: WorkflowIntent,
        similarity: float,
        fits_pattern: bool
    ) -> str:
        """Generate human-readable reasoning for recommendation"""

        base = f"Similarity score: {similarity:.3f}"

        if fits_pattern:
            return f"{base}. Fits common {intent.value} workflow pattern."
        else:
            return f"{base}. Relevant to query but outside typical patterns."

    def _generate_inputs_template(
        self,
        tool: Dict[str, Any],
        depends_on: List[str],
        available_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate input template with parameter mapping"""

        schema = tool.get('inputSchema', {})
        properties = schema.get('properties', {})
        template = {}

        for param_name, param_info in properties.items():
            # Check if we have this data available
            if param_name in available_data:
                template[param_name] = f"{{{{data.{param_name}}}}}"
            elif depends_on:
                # Map from previous step output
                template[param_name] = f"{{{{steps.{depends_on[0]}.result.{param_name}}}}}"
            else:
                # Provide default or placeholder
                param_type = param_info.get('type', 'string')
                if param_type == 'string':
                    template[param_name] = f"<{param_name}>"
                elif param_type == 'integer':
                    template[param_name] = 0
                elif param_type == 'boolean':
                    template[param_name] = False
                elif param_type == 'object':
                    template[param_name] = {}
                elif param_type == 'array':
                    template[param_name] = []

        return template

    def record_usage(
        self,
        tool_name: str,
        backend: str,
        success: bool,
        duration_ms: float
    ):
        """Record tool usage for analytics"""

        key = f"{backend}:{tool_name}"

        if key not in self.usage_metrics:
            self.usage_metrics[key] = ToolUsageMetrics(
                tool_name=tool_name,
                backend=backend
            )

        metrics = self.usage_metrics[key]
        metrics.total_calls += 1

        if success:
            metrics.successful_calls += 1
        else:
            metrics.failed_calls += 1

        # Update average duration
        metrics.avg_duration_ms = (
            (metrics.avg_duration_ms * (metrics.total_calls - 1) + duration_ms)
            / metrics.total_calls
        )

        metrics.last_used = datetime.now(timezone.utc)
        metrics.success_rate = metrics.successful_calls / metrics.total_calls

    def get_analytics(self) -> Dict[str, Any]:
        """Get usage analytics"""

        sorted_by_calls = sorted(
            self.usage_metrics.values(),
            key=lambda x: x.total_calls,
            reverse=True
        )

        sorted_by_success = sorted(
            self.usage_metrics.values(),
            key=lambda x: x.success_rate,
            reverse=True
        )

        return {
            "total_unique_tools_used": len(self.usage_metrics),
            "total_calls": sum(m.total_calls for m in self.usage_metrics.values()),
            "most_used_tools": [
                {
                    "tool": m.tool_name,
                    "backend": m.backend,
                    "calls": m.total_calls,
                    "success_rate": m.success_rate
                }
                for m in sorted_by_calls[:10]
            ],
            "most_reliable_tools": [
                {
                    "tool": m.tool_name,
                    "backend": m.backend,
                    "success_rate": m.success_rate,
                    "calls": m.total_calls
                }
                for m in sorted_by_success[:10]
                if m.total_calls >= 5  # Minimum calls for reliability
            ],
            "backend_stats": self._get_backend_stats()
        }

    def _get_backend_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics per backend"""

        backend_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_calls": 0,
            "successful_calls": 0,
            "avg_duration_ms": 0.0,
            "tools_used": set()
        })

        for metrics in self.usage_metrics.values():
            stats = backend_stats[metrics.backend]
            stats["total_calls"] = int(stats["total_calls"]) + metrics.total_calls
            stats["successful_calls"] = int(stats["successful_calls"]) + metrics.successful_calls
            stats["tools_used"].add(metrics.tool_name)  # type: ignore

        # Convert sets to counts and calculate success rates
        result = {}
        for backend, stats in backend_stats.items():
            total = int(stats["total_calls"])
            successful = int(stats["successful_calls"])
            result[backend] = {
                "total_calls": total,
                "successful_calls": successful,
                "success_rate": successful / max(total, 1),
                "unique_tools_used": len(stats["tools_used"])  # type: ignore
            }

        return result


# =============================================================================
# INTELLIGENT WORKFLOW ORCHESTRATOR
# =============================================================================

class IntelligentOrchestrator:
    """Orchestrates complex multi-step workflows with intelligence"""

    def __init__(self):
        self.recommender = ContextAwareRecommender()
        self.gateway_url = "http://localhost:2016/api/v1/gateway/execute"

    async def execute_workflow(
        self,
        context: WorkflowContext,
        strategy: ExecutionStrategy = ExecutionStrategy.SEQUENTIAL,
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """Execute a complete workflow with the given strategy"""

        # Get recommendations
        recommendations = await self.recommender.recommend_workflow(context)

        if not recommendations:
            raise HTTPException(status_code=400, detail="No suitable workflow found")

        # Execute based on strategy
        if strategy == ExecutionStrategy.SEQUENTIAL:
            return await self._execute_sequential(recommendations, context, max_retries)
        elif strategy == ExecutionStrategy.PARALLEL:
            return await self._execute_parallel(recommendations, context)
        elif strategy == ExecutionStrategy.RETRY:
            return await self._execute_with_retry(recommendations, context, max_retries)
        else:
            raise HTTPException(status_code=400, detail=f"Strategy {strategy} not implemented")

    async def _execute_sequential(
        self,
        recommendations: List[ToolRecommendation],
        context: WorkflowContext,
        max_retries: int
    ) -> Dict[str, Any]:
        """Execute tools sequentially with data flow"""

        results = []
        workflow_data = context.available_data.copy()

        for i, rec in enumerate(recommendations):
            step_name = f"step_{i}"

            # Resolve input parameters from context and previous results
            inputs = self._resolve_inputs(rec.inputs_template, workflow_data, results)

            # Execute tool
            for attempt in range(max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            self.gateway_url,
                            json={"tool": rec.tool_name, "inputs": inputs}
                        )

                        if response.status_code == 200:
                            result = response.json()

                            # Record metrics
                            self.recommender.record_usage(
                                tool_name=rec.tool_name,
                                backend=result.get('backend', rec.backend),
                                success=(result.get('status') == 'success'),
                                duration_ms=result.get('duration_ms', 0)
                            )

                            results.append({
                                "step": step_name,
                                "tool": rec.tool_name,
                                "backend": result.get('backend'),
                                "status": result.get('status'),
                                "result": result.get('result'),
                                "duration_ms": result.get('duration_ms')
                            })

                            # Update workflow data for next step
                            workflow_data[step_name] = result.get('result')

                            break  # Success, exit retry loop

                except Exception as e:
                    if attempt == max_retries:
                        results.append({
                            "step": step_name,
                            "tool": rec.tool_name,
                            "status": "failed",
                            "error": str(e)
                        })
                    else:
                        logger.warning(f"Retry {attempt + 1} for {rec.tool_name}: {e}")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff

        return {
            "workflow_id": datetime.now(timezone.utc).isoformat(),
            "intent": context.intent.value,
            "strategy": ExecutionStrategy.SEQUENTIAL.value,
            "total_steps": len(recommendations),
            "completed_steps": len([r for r in results if r.get('status') == 'success']),
            "results": results,
            "total_duration_ms": sum(r.get('duration_ms', 0) for r in results)
        }

    async def _execute_parallel(
        self,
        recommendations: List[ToolRecommendation],
        context: WorkflowContext
    ) -> Dict[str, Any]:
        """Execute tools in parallel"""

        async def execute_tool(rec: ToolRecommendation, idx: int):
            inputs = self._resolve_inputs(rec.inputs_template, context.available_data, [])

            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        self.gateway_url,
                        json={"tool": rec.tool_name, "inputs": inputs}
                    )

                    if response.status_code == 200:
                        result = response.json()
                        return {
                            "step": f"parallel_{idx}",
                            "tool": rec.tool_name,
                            "backend": result.get('backend'),
                            "status": result.get('status'),
                            "result": result.get('result'),
                            "duration_ms": result.get('duration_ms')
                        }
            except Exception as e:
                return {
                    "step": f"parallel_{idx}",
                    "tool": rec.tool_name,
                    "status": "failed",
                    "error": str(e)
                }

        # Execute all in parallel
        tasks = [execute_tool(rec, i) for i, rec in enumerate(recommendations)]
        results = await asyncio.gather(*tasks)

        return {
            "workflow_id": datetime.now(timezone.utc).isoformat(),
            "intent": context.intent.value,
            "strategy": ExecutionStrategy.PARALLEL.value,
            "total_steps": len(recommendations),
            "completed_steps": len([r for r in results if r.get('status') == 'success']),
            "results": results,
            "total_duration_ms": max((r.get('duration_ms', 0) for r in results), default=0)
        }

    async def _execute_with_retry(
        self,
        recommendations: List[ToolRecommendation],
        context: WorkflowContext,
        max_retries: int
    ) -> Dict[str, Any]:
        """Execute with retry and fallback logic"""
        return await self._execute_sequential(recommendations, context, max_retries)

    def _resolve_inputs(
        self,
        template: Dict[str, Any],
        available_data: Dict[str, Any],
        previous_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Resolve input template with actual data"""

        resolved = {}

        for key, value in template.items():
            if isinstance(value, str) and value.startswith('{{') and value.endswith('}}'):
                # This is a template variable
                var_path = value[2:-2].strip()

                if var_path.startswith('data.'):
                    # Get from available data
                    data_key = var_path[5:]
                    resolved[key] = available_data.get(data_key, value)

                elif var_path.startswith('steps.'):
                    # Get from previous step result
                    parts = var_path.split('.')
                    if len(parts) >= 3:
                        step_name = parts[1]
                        # Find the result
                        for result in previous_results:
                            if result.get('step') == step_name:
                                # Navigate the path
                                current = result.get('result', {})
                                for part in parts[3:]:
                                    if isinstance(current, dict):
                                        current = current.get(part)
                                    else:
                                        break
                                resolved[key] = current
                                break
                    if key not in resolved:
                        resolved[key] = value
                else:
                    resolved[key] = value
            else:
                resolved[key] = value

        return resolved


# =============================================================================
# API ENDPOINTS
# =============================================================================

# Global orchestrator instance
_orchestrator: Optional[IntelligentOrchestrator] = None

def get_orchestrator() -> IntelligentOrchestrator:
    """Get or create global orchestrator instance"""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = IntelligentOrchestrator()
    return _orchestrator


class RecommendRequest(BaseModel):
    """Request for tool recommendations"""
    intent: WorkflowIntent
    query: str
    available_data: Dict[str, Any] = Field(default_factory=dict)
    max_steps: int = Field(default=5, ge=1, le=10)


class ExecuteWorkflowRequest(BaseModel):
    """Request to execute an intelligent workflow"""
    intent: WorkflowIntent
    query: str
    available_data: Dict[str, Any] = Field(default_factory=dict)
    strategy: ExecutionStrategy = ExecutionStrategy.SEQUENTIAL
    max_retries: int = Field(default=2, ge=0, le=5)


@router.post("/recommend")
async def recommend_workflow(request: RecommendRequest):
    """
    Get intelligent tool recommendations based on intent and query.

    Example:
    ```json
    {
      "intent": "research",
      "query": "find information about AI startups",
      "available_data": {},
      "max_steps": 5
    }
    ```
    """
    orchestrator = get_orchestrator()

    context = WorkflowContext(
        intent=request.intent,
        user_query=request.query,
        available_data=request.available_data
    )

    recommendations = await orchestrator.recommender.recommend_workflow(
        context,
        max_steps=request.max_steps
    )

    return {
        "intent": request.intent.value,
        "query": request.query,
        "recommendations": [
            {
                "tool": rec.tool_name,
                "backend": rec.backend,
                "confidence": rec.confidence,
                "reasoning": rec.reasoning,
                "inputs_template": rec.inputs_template,
                "depends_on": rec.depends_on
            }
            for rec in recommendations
        ]
    }


@router.post("/execute")
async def execute_intelligent_workflow(request: ExecuteWorkflowRequest):
    """
    Execute an intelligent workflow with automatic tool selection and chaining.

    Example:
    ```json
    {
      "intent": "research",
      "query": "research semantic search technologies",
      "available_data": {"topic": "semantic search"},
      "strategy": "sequential",
      "max_retries": 2
    }
    ```
    """
    orchestrator = get_orchestrator()

    context = WorkflowContext(
        intent=request.intent,
        user_query=request.query,
        available_data=request.available_data
    )

    result = await orchestrator.execute_workflow(
        context,
        strategy=request.strategy,
        max_retries=request.max_retries
    )

    return result


@router.get("/analytics")
async def get_analytics():
    """Get usage analytics and optimization insights"""
    orchestrator = get_orchestrator()
    return orchestrator.recommender.get_analytics()


@router.get("/health")
async def health_check():
    """Health check for orchestration engine"""
    return {
        "status": "healthy",
        "service": "advanced-orchestration",
        "features": [
            "context-aware-recommendations",
            "intelligent-tool-chaining",
            "usage-analytics",
            "retry-logic",
            "parallel-execution"
        ]
    }
