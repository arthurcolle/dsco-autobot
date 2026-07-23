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
import os
import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..embeddings.semantic_tool_store import get_tool_store

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
        self.intent_defaults = self._build_intent_defaults()
        self.usage_metrics: Dict[str, ToolUsageMetrics] = {}
        self._indexed = False

    def _build_intent_defaults(self) -> Dict[WorkflowIntent, List[str]]:
        """Default tool names per intent — used when no embedding match is strong."""
        return {
            WorkflowIntent.RESEARCH: ["search", "read", "verify"],
            WorkflowIntent.REMEMBER: ["record_episode", "store_belief"],
            WorkflowIntent.NOTIFY: ["publish_event"],
            WorkflowIntent.TRANSFORM: [],
            WorkflowIntent.ANALYZE: ["search", "deep_search"],
            WorkflowIntent.PUBLISH: ["publish_event"],
        }

    async def _ensure_indexed(self):
        """Lazily index registry tools + MCP backend tools into the semantic store.

        Reads tools from DuckDB (persistent) instead of making HTTP calls to
        backends. Wraps in a 15s timeout so it never blocks requests.
        """
        if self._indexed:
            return
        self._indexed = True
        try:
            await asyncio.wait_for(self._do_index(), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("Lazy index timed out after 15s — will serve with whatever is cached")
        except Exception as e:
            logger.error(f"Lazy index failed: {e}")
            self._indexed = False

    async def _do_index(self):
        """Internal: index registry + DuckDB tools into semantic store."""
        # Index local registry tools
        from ..registry import get_registry
        registry = get_registry()
        tools = registry.to_mcp_tools()
        if tools:
            await self.tool_store.index_tools_batch(tools, backend="registry")
            logger.info(f"Lazy-indexed {len(tools)} registry tools")

        # Index tools from DuckDB persistent store (no HTTP needed)
        from ..persistence.tool_store import get_tool_store as get_persistent_store
        store = get_persistent_store()
        for backend_info in store.list_backends():
            bid = backend_info.get("backend_id", "")
            if not bid:
                continue
            try:
                db_tools = store.list_tools(backend_id=bid, limit=500)
                mcp_tools = []
                for t in db_tools:
                    mcp_tools.append({
                        "name": t.name,
                        "description": t.description or "",
                        "inputSchema": t.input_schema or {},
                        "outputSchema": t.output_schema or {},
                        "tags": t.tags or [],
                        "category": t.category or "general",
                    })
                if mcp_tools:
                    await self.tool_store.index_tools_batch(mcp_tools, backend=bid)
                    logger.info(f"Lazy-indexed {len(mcp_tools)} tools from DuckDB backend {bid}")
            except Exception as e:
                logger.debug(f"Skip indexing {bid}: {e}")

    async def recommend_workflow(
        self,
        context: WorkflowContext,
        max_steps: int = 5
    ) -> List[ToolRecommendation]:
        """Recommend tools using embedding similarity against name+description+schema.

        No hard similarity threshold — always returns the best match.
        Uses intent-aware defaults with priority-weighted confidence boost.

        Performance: single embedding call + single matrix search resolves both
        semantic results AND intent-default lookups in one pass.
        """
        await self._ensure_indexed()

        # Intent-aware defaults (e.g., research → search tool)
        defaults = self.intent_defaults.get(context.intent, [])

        # ONE search call: semantic top-10 + direct name lookup for intent defaults
        search_results, name_map = await self.tool_store.search_tools_with_names(
            query=context.user_query,
            extra_names=defaults,
            limit=10,
            min_similarity=0.0,
        )

        # Score and rank — process defaults first so they always get their boost
        recommendations = []
        used_tools: set = set()

        # Build priority map: first default = highest priority
        default_priority = {name: i for i, name in enumerate(defaults)} if defaults else {}

        # Process intent defaults first (boosted confidence, priority-ordered)
        for dname in defaults:
            result = name_map.get(dname)
            if not result:
                continue

            tool = result['tool']
            tool_name = tool['name']
            backend = result['backend']
            similarity = result['similarity']

            if tool_name in used_tools:
                continue

            # First default gets 0.85 floor, second 0.80, third 0.75, rest 0.70
            priority_idx = default_priority.get(tool_name, len(defaults))
            floor = max(0.85 - 0.05 * priority_idx, 0.70)
            confidence = max(similarity, floor)  # priority-weighted floor
            inputs_template = self._generate_inputs_template(
                tool, [], context.available_data
            )
            recommendations.append(ToolRecommendation(
                tool_name=tool_name,
                backend=backend,
                confidence=min(confidence, 1.0),
                reasoning=f"similarity={similarity:.3f} [intent-default]",
                inputs_template=inputs_template,
                depends_on=[],
            ))
            used_tools.add(tool_name)

        # Then add search results (no boost, pure embedding similarity)
        for result in search_results:
            tool = result['tool']
            tool_name = tool['name']
            backend = result['backend']
            similarity = result['similarity']

            if tool_name in used_tools:
                continue

            inputs_template = self._generate_inputs_template(
                tool, [], context.available_data
            )
            recommendations.append(ToolRecommendation(
                tool_name=tool_name,
                backend=backend,
                confidence=min(similarity, 1.0),
                reasoning=f"similarity={similarity:.3f}",
                inputs_template=inputs_template,
                depends_on=[],
            ))
            used_tools.add(tool_name)

        # Sort by confidence descending, return top N
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
        """Generate input template with parameter mapping.

        Uses available_data keys directly, falls back to query text for strings.
        """
        schema = tool.get('inputSchema', {})
        properties = schema.get('properties', {})
        query = available_data.get('_query', '')
        template = {}

        for param_name, param_info in properties.items():
            if param_name in available_data:
                template[param_name] = available_data[param_name]
            elif depends_on:
                template[param_name] = f"{{{{steps.{depends_on[0]}.result.{param_name}}}}}"
            else:
                param_type = param_info.get('type', 'string')
                default = param_info.get('default')
                if default is not None:
                    template[param_name] = default
                elif param_type == 'string' and query:
                    template[param_name] = query
                elif param_type in ('integer', 'number'):
                    template[param_name] = 0
                elif param_type == 'boolean':
                    template[param_name] = False
                elif param_type == 'object':
                    template[param_name] = {}
                elif param_type == 'array':
                    template[param_name] = []
                else:
                    template[param_name] = query if query else f"<{param_name}>"

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
        self._http: Optional[Any] = None  # lazy httpx.AsyncClient
        self._backend_url_cache: Dict[str, Optional[str]] = {}  # backend_id -> url
        self._registry = None

    def _get_registry(self):
        """Get the in-memory tool registry (cached)."""
        if self._registry is None:
            from ..registry import get_registry
            self._registry = get_registry()
        return self._registry

    async def _get_http(self):
        """Get or create persistent httpx client."""
        if self._http is None:
            import httpx
            self._http = httpx.AsyncClient(timeout=55.0)
        return self._http

    def _resolve_backend_url(self, backend: Optional[str]) -> Optional[str]:
        """Resolve backend ID to URL with caching."""
        if not backend:
            return None
        if backend in self._backend_url_cache:
            return self._backend_url_cache[backend]

        url = None
        try:
            from ..queue import get_task_queue
            queue = get_task_queue()
            backend_obj = next((b for b in queue.list_backends() if b.id == backend), None)
            if backend_obj:
                url = backend_obj.url
        except Exception:
            pass

        if not url:
            try:
                from ..persistence.tool_store import get_tool_store as _get_ps
                ps = _get_ps()
                bi = ps.get_backend(backend)
                url = bi["url"] if bi else None
            except Exception:
                pass

        self._backend_url_cache[backend] = url
        return url

    async def _execute_tool_direct(
        self, tool_name: str, inputs: Dict[str, Any], backend: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute a tool: local registry first, then MCP backend directly."""
        import time
        registry = self._get_registry()
        t0 = time.time()

        # Try local in-memory registry first (sub-ms)
        tool = registry.get_by_name(tool_name)
        if tool:
            try:
                result = await asyncio.wait_for(tool.execute(**inputs), timeout=30.0)
                return {
                    "status": "success",
                    "result": result,
                    "backend": "local-registry",
                    "duration_ms": (time.time() - t0) * 1000,
                }
            except asyncio.TimeoutError:
                return {"status": "error", "error": f"Tool '{tool_name}' timed out", "duration_ms": (time.time() - t0) * 1000}
            except Exception as exc:
                return {"status": "error", "error": str(exc), "duration_ms": (time.time() - t0) * 1000}

        # Call MCP backend directly — bypass tools_api/output_guard to keep structured data
        try:
            backend_url = self._resolve_backend_url(backend)
            if not backend_url:
                return {"status": "error", "error": f"Backend '{backend}' not found", "duration_ms": (time.time() - t0) * 1000}

            client = await self._get_http()
            resp = await client.post(
                f"{backend_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": "orch",
                    "method": "tools/call",
                    "params": {"name": tool_name, "arguments": inputs},
                },
            )
            if resp.status_code != 200:
                return {"status": "error", "error": f"MCP {resp.status_code}: {resp.text[:200]}", "duration_ms": (time.time() - t0) * 1000}

            data = resp.json()
            if "error" in data:
                return {"status": "error", "error": str(data["error"]), "duration_ms": (time.time() - t0) * 1000}

            # Raw MCP result: {"result": {"content": [{"type":"text","text":"{...}"}]}}
            return {
                "status": "success",
                "result": data.get("result"),
                "backend": backend or "mcp",
                "duration_ms": (time.time() - t0) * 1000,
            }
        except asyncio.TimeoutError:
            return {"status": "error", "error": f"Tool '{tool_name}' backend timeout", "duration_ms": (time.time() - t0) * 1000}
        except Exception as exc:
            return {"status": "error", "error": str(exc), "duration_ms": (time.time() - t0) * 1000}

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
            return {
                "workflow_id": datetime.now(timezone.utc).isoformat(),
                "intent": context.intent.value,
                "strategy": strategy.value,
                "total_steps": 0,
                "completed_steps": 0,
                "results": [],
                "total_duration_ms": 0,
                "message": "No suitable tools found for this query",
            }

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
        """Execute tools sequentially with multi-round follow-up.

        After each successful step, assesses whether the result is substantive
        (contains actual answer data) or just search links.  If not substantive,
        continues to the next recommendation and auto-injects follow-up steps
        (deep_search, read) when the original recommendation list is exhausted.

        After all rounds, if no single step produced a substantive answer,
        calls an LLM to synthesize a final answer from all collected data.
        """
        import time as _time

        results: List[Dict[str, Any]] = []
        workflow_data = context.available_data.copy()
        t_start = _time.time()
        had_substantive = False
        used_tools: set = set()
        followups_injected = False

        # Work on a mutable copy so we can append follow-ups
        recs = list(recommendations)
        max_rounds = len(recs) + 4  # safety cap

        i = 0
        while i < len(recs) and i < max_rounds:
            rec = recs[i]
            step_name = f"step_{i}"
            inputs = self._resolve_inputs(rec.inputs_template, workflow_data, results)

            # After a substantive success, only continue if the next tool
            # references previous step outputs via {{steps.*}} templates
            has_step_refs = any(
                isinstance(v, str) and '{{steps.' in v
                for v in inputs.values()
            )
            if had_substantive and not has_step_refs:
                break

            result = await self._execute_tool_direct(rec.tool_name, inputs, backend=rec.backend)
            used_tools.add(rec.tool_name)

            self.recommender.record_usage(
                tool_name=rec.tool_name,
                backend=result.get('backend', rec.backend),
                success=(result.get('status') == 'success'),
                duration_ms=result.get('duration_ms', 0)
            )

            if result.get('status') == 'success':
                extracted = self._extract_result(result.get('result'))
                results.append({
                    "step": step_name,
                    "tool": rec.tool_name,
                    "backend": result.get('backend'),
                    "status": "success",
                    "result": extracted,
                    "duration_ms": result.get('duration_ms')
                })
                workflow_data[step_name] = extracted

                # --- Multi-round quality gate ---
                if self._result_is_substantive(extracted, context.user_query):
                    had_substantive = True
                else:
                    logger.info(
                        f"Step {step_name} ({rec.tool_name}) returned links-only; "
                        f"continuing to next round"
                    )
                    # Enrich workflow_data with URLs from search results
                    if isinstance(extracted, dict):
                        urls = [
                            r.get("url")
                            for r in extracted.get("results", [])[:5]
                            if isinstance(r, dict) and r.get("url")
                        ]
                        if urls:
                            workflow_data["_search_urls"] = urls
                            workflow_data["_search_url"] = urls[0]

                    # Auto-inject follow-up recommendations if we've exhausted originals
                    if not followups_injected and i + 1 >= len(recs):
                        followups = self._generate_followup_recommendations(
                            extracted, context, used_tools
                        )
                        if followups:
                            recs.extend(followups)
                            followups_injected = True
                            logger.info(
                                f"Injected {len(followups)} follow-up step(s): "
                                f"{[f.tool_name for f in followups]}"
                            )
            else:
                results.append({
                    "step": step_name,
                    "tool": rec.tool_name,
                    "status": "failed",
                    "error": result.get('error', 'unknown error')
                })

            i += 1

        # --- Final LLM synthesis if no single round was substantive ---
        answer_obj: Optional[Dict[str, Any]] = None
        if not had_substantive and results:
            logger.info("No substantive result found; attempting LLM synthesis")
            answer_obj = await self._llm_summarize_answer(results, context.user_query)
            if answer_obj:
                results.append({
                    "step": f"step_{len(results)}",
                    "tool": "_llm_synthesis",
                    "backend": "anthropic",
                    "status": "success",
                    "result": answer_obj,
                    "duration_ms": 0,
                })

        resp: Dict[str, Any] = {
            "workflow_id": datetime.now(timezone.utc).isoformat(),
            "intent": context.intent.value,
            "strategy": ExecutionStrategy.SEQUENTIAL.value,
            "total_steps": len(results),
            "completed_steps": len([r for r in results if r.get('status') == 'success']),
            "results": results,
            "total_duration_ms": (_time.time() - t_start) * 1000,
        }
        if answer_obj:
            resp["answer"] = answer_obj.get("answer", "")
            resp["confidence"] = answer_obj.get("confidence", "unknown")
            resp["sources"] = answer_obj.get("sources", [])
            resp["key_facts"] = answer_obj.get("key_facts", [])
        return resp

    async def _execute_parallel(
        self,
        recommendations: List[ToolRecommendation],
        context: WorkflowContext
    ) -> Dict[str, Any]:
        """Execute tools in parallel"""
        import time as _time
        t_start = _time.time()

        async def execute_one(rec: ToolRecommendation, idx: int):
            inputs = self._resolve_inputs(rec.inputs_template, context.available_data, [])
            result = await self._execute_tool_direct(rec.tool_name, inputs, backend=rec.backend)
            return {
                "step": f"parallel_{idx}",
                "tool": rec.tool_name,
                "backend": result.get('backend'),
                "status": result.get('status'),
                "result": self._extract_result(result.get('result')) if result.get('status') == 'success' else None,
                "error": result.get('error'),
                "duration_ms": result.get('duration_ms')
            }

        tasks = [execute_one(rec, i) for i, rec in enumerate(recommendations)]
        results = list(await asyncio.gather(*tasks))

        return {
            "workflow_id": datetime.now(timezone.utc).isoformat(),
            "intent": context.intent.value,
            "strategy": ExecutionStrategy.PARALLEL.value,
            "total_steps": len(recommendations),
            "completed_steps": len([r for r in results if r.get('status') == 'success']),
            "results": results,
            "total_duration_ms": (_time.time() - t_start) * 1000
        }

    async def _execute_with_retry(
        self,
        recommendations: List[ToolRecommendation],
        context: WorkflowContext,
        max_retries: int
    ) -> Dict[str, Any]:
        """Execute with retry and fallback logic"""
        return await self._execute_sequential(recommendations, context, max_retries)

    @staticmethod
    def _extract_result(raw: Any) -> Any:
        """Extract clean, structured data from tool results.

        Handles three shapes:
        1. MCP envelope: {"content":[{"type":"text","text":"{...}"}], "_guard":{...}}
        2. Raw JSON string (possibly truncated by output guard)
        3. Already-parsed dict

        For search results, returns only title/url/description per hit.
        Handles truncated JSON by extracting individual result objects via regex.
        """
        # Step 1: unwrap MCP content envelope → get inner text
        if isinstance(raw, dict) and "content" in raw:
            content_list = raw.get("content", [])
            if isinstance(content_list, list):
                for item in content_list:
                    if isinstance(item, dict) and item.get("type") == "text":
                        raw = item.get("text", "")
                        break  # use first text block

        # Step 2: parse JSON string → dict
        if isinstance(raw, str):
            try:
                raw = json.loads(raw, strict=False)
            except (json.JSONDecodeError, ValueError):
                # Truncated JSON — extract what we can via regex
                return IntelligentOrchestrator._extract_from_truncated(raw)

        if not isinstance(raw, dict):
            return raw

        # Step 3: extract search results into clean summaries
        return IntelligentOrchestrator._clean_search_results(raw)

    @staticmethod
    def _clean_search_results(raw: Dict[str, Any]) -> Any:
        """Extract clean title/url/description from search result dicts."""
        search_results = raw.get("results")
        if isinstance(search_results, list) and search_results:
            extracted = []
            for sr in search_results:
                if not isinstance(sr, dict):
                    continue
                entry: Dict[str, Any] = {}
                for key in ("title", "url", "description", "date"):
                    val = sr.get(key)
                    if val:
                        entry[key] = val
                if entry:
                    extracted.append(entry)
            return {"query": raw.get("query", ""), "results": extracted}
        return raw

    @staticmethod
    def _extract_from_truncated(text: str) -> Any:
        """Extract search results from truncated JSON.

        The output guard truncates large JSON responses mid-stream.
        Instead of failing, we extract individual result objects that
        completed before truncation.
        """

        # Try to extract the query
        query_match = re.search(r'"query"\s*:\s*"([^"]*)"', text)
        query = query_match.group(1) if query_match else ""

        # Extract individual result objects by finding title/url/description patterns
        results = []
        # Find all "title": "..." occurrences
        title_pattern = re.compile(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"')
        url_pattern = re.compile(r'"url"\s*:\s*"((?:[^"\\]|\\.)*)"')
        desc_pattern = re.compile(r'"description"\s*:\s*"((?:[^"\\]|\\.)*)"')

        # Split by result boundaries — each result starts with {"title" or similar
        # Find positions of all title matches
        titles = list(title_pattern.finditer(text))
        urls = list(url_pattern.finditer(text))
        descs = list(desc_pattern.finditer(text))

        # Group by proximity: for each title, find the nearest url and description
        for t_match in titles:
            t_pos = t_match.start()
            entry: Dict[str, str] = {"title": t_match.group(1)}

            # Find the url closest after this title (within 2000 chars)
            for u_match in urls:
                if u_match.start() > t_pos and u_match.start() - t_pos < 2000:
                    entry["url"] = u_match.group(1)
                    break

            # Find the description closest after this title (within 2000 chars)
            for d_match in descs:
                if d_match.start() > t_pos and d_match.start() - t_pos < 2000:
                    entry["description"] = d_match.group(1)
                    break

            if len(entry) >= 2:  # At least title + one other field
                results.append(entry)

        if results:
            return {"query": query, "results": results}

        # Last resort: return trimmed text
        if len(text) > 2000:
            return text[:2000] + "... [truncated]"
        return text

    @staticmethod
    def _result_is_substantive(extracted: Any, query: str) -> bool:
        """Check if result contains actual answer data, not just search links.

        Returns True if the result has substantive content that answers the
        query. Returns False if it's only search result metadata (title/url/
        description) without extracted content.
        """
        if extracted is None:
            return False
        if isinstance(extracted, str):
            # Plain text — substantive if long enough and not truncated boilerplate
            return len(extracted) > 100 and "[OUTPUT TRUNCATED" not in extracted

        if not isinstance(extracted, dict):
            return bool(extracted)

        # Check for search-result-only structure
        results = extracted.get("results")
        if not isinstance(results, list):
            # Not a search result structure — treat as substantive
            return True

        if not results:
            return False

        # Check if any result entry has data beyond link metadata
        LINK_KEYS = {"title", "url", "description", "date"}
        for r in results:
            if not isinstance(r, dict):
                continue
            data_keys = set(r.keys()) - LINK_KEYS
            if data_keys:
                # Has extra fields — check they contain real content
                for dk in data_keys:
                    val = r.get(dk)
                    if isinstance(val, str) and len(val) > 200:
                        if "[OUTPUT TRUNCATED" not in val and "Oops" not in val:
                            return True
                    elif isinstance(val, (dict, list)) and val:
                        return True

        # All entries are just link metadata — not substantive
        return False

    def _generate_followup_recommendations(
        self,
        extracted: Any,
        context: WorkflowContext,
        already_used: set,
    ) -> List[ToolRecommendation]:
        """Generate follow-up tool recommendations when search returned only links.

        Produces up to 2 follow-ups:
        1. deep_search on research MCP (LLM-synthesized answer)
        2. read on search MCP (fetch top URL content)
        """
        followups: List[ToolRecommendation] = []

        # Extract top URLs from search results for context
        urls: List[str] = []
        if isinstance(extracted, dict):
            for r in extracted.get("results", [])[:3]:
                if isinstance(r, dict) and r.get("url"):
                    urls.append(r["url"])

        # Follow-up 1: deep_search on research MCP — uses LLM synthesis
        if "deep_search" not in already_used:
            followups.append(ToolRecommendation(
                tool_name="deep_search",
                backend="distributed-research-mcp",
                confidence=0.82,
                reasoning="auto-followup: search returned links without answer, using deep_search for LLM-synthesized content",
                inputs_template={
                    "query": context.user_query,
                    "objective": context.user_query,
                    **({"sources": urls} if urls else {}),
                },
                depends_on=[],
            ))

        # Follow-up 2: read top URL via search MCP
        if "read" not in already_used and urls:
            followups.append(ToolRecommendation(
                tool_name="read",
                backend="distributed-search-mcp",
                confidence=0.75,
                reasoning="auto-followup: reading top URL from search results",
                inputs_template={"url": urls[0], "query": context.user_query},
                depends_on=[],
            ))

        return followups

    async def _llm_summarize_answer(
        self,
        results: List[Dict[str, Any]],
        query: str,
    ) -> Optional[Dict[str, Any]]:
        """Call Anthropic LLM with structured output to extract an answer.

        Uses tool_use to force a structured JSON response with answer, confidence,
        and sources.  Falls back to plain text if tool_use extraction fails.
        """
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return None

        # Build context from all successful results
        context_parts: List[str] = []
        for r in results:
            if r.get("status") != "success":
                continue
            result_data = r.get("result")
            if isinstance(result_data, dict):
                context_parts.append(json.dumps(result_data, default=str)[:4000])
            elif isinstance(result_data, str):
                context_parts.append(result_data[:4000])

        if not context_parts:
            return None

        combined = "\n---\n".join(context_parts)
        prompt = (
            f"User query: {query}\n\n"
            f"Below are search/research results collected across multiple rounds.\n"
            f"Use the provide_answer tool to give a structured response.\n\n"
            f"Results:\n{combined}"
        )

        answer_tool = {
            "name": "provide_answer",
            "description": "Provide a structured answer to the user's query based on the research results.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "answer": {
                        "type": "string",
                        "description": "A concise, direct answer to the user's query. Include specific numbers, dates, or facts when available."
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "How confident you are in this answer based on the available data."
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "URLs or source names that support this answer."
                    },
                    "key_facts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Bullet-point key facts extracted from the results."
                    },
                },
                "required": ["answer", "confidence"],
            },
        }

        try:
            import httpx as _httpx
            async with _httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 1024,
                        "system": "You are a concise research assistant. Always use the provide_answer tool.",
                        "messages": [{"role": "user", "content": prompt}],
                        "tools": [answer_tool],
                        "tool_choice": {"type": "tool", "name": "provide_answer"},
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for block in data.get("content", []):
                        if block.get("type") == "tool_use" and block.get("name") == "provide_answer":
                            return block.get("input", {})
                        if block.get("type") == "text" and block.get("text"):
                            return {"answer": block["text"], "confidence": "medium"}
                    return None
                else:
                    logger.warning(f"LLM summary call failed ({resp.status_code})")
                    return None
        except Exception as e:
            logger.error(f"LLM summary error: {e}")
            return None

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

    data = dict(request.available_data)
    data.setdefault("_query", request.query)

    context = WorkflowContext(
        intent=request.intent,
        user_query=request.query,
        available_data=data
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

    # Inject query into available_data so input templates can use it
    data = dict(request.available_data)
    data.setdefault("_query", request.query)

    context = WorkflowContext(
        intent=request.intent,
        user_query=request.query,
        available_data=data
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
