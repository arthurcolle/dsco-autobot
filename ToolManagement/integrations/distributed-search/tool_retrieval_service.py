"""
Tool Retrieval Service
Main service integrating embedder, retriever, and context management
"""

import asyncio
import time
import logging
from typing import List, Dict, Optional
from tool_embedder import ToolEmbedder
from tool_retriever import ToolRetriever, ToolDependencyGraph

logger = logging.getLogger(__name__)


class ContextBudgetManager:
    """Manage tool context within token budget"""

    def __init__(self, embedder, max_tool_tokens: int = 8000):
        self.embedder = embedder
        self.max_tokens = max_tool_tokens
        self.hot_tier = set()
        self.token_cache = {}

    async def allocate_context(
        self,
        retrieved_tools: List[Dict],
        force_include: Optional[List[str]] = None
    ) -> Dict:
        """
        Allocate tools within budget.

        Returns:
        {
            "selected_tools": [tool_dicts],
            "tokens_used": int,
            "tools_dropped": int,
            "efficiency": float
        }
        """

        budget = self.max_tokens
        selected = []
        dropped = 0

        # Force-include tools (e.g., hot tier)
        if force_include:
            for tool_id in force_include:
                tokens = self._estimate_tokens(tool_id)
                if budget >= tokens:
                    budget -= tokens
                    # Find tool in retrieved list or get from DB
                    tool = next((t for t in retrieved_tools if t['tool_id'] == tool_id), None)
                    if not tool:
                        schema = self.embedder.get_tool_schema(tool_id)
                        if schema:
                            tool = {
                                'tool_id': tool_id,
                                'schema': schema,
                                'score': 1.0
                            }
                    if tool:
                        selected.append(tool)

        # Add retrieved tools in order of relevance
        for tool in retrieved_tools:
            if any(t['tool_id'] == tool['tool_id'] for t in selected):
                continue  # Already included

            tokens = self._estimate_tokens(tool['tool_id'])

            if budget >= tokens:
                budget -= tokens
                selected.append(tool)
            else:
                dropped += 1

        tokens_used = self.max_tokens - budget

        return {
            "selected_tools": selected,
            "tokens_used": tokens_used,
            "tools_dropped": dropped,
            "efficiency": len(selected) / max(len(retrieved_tools), 1)
        }

    def _estimate_tokens(self, tool_id: str) -> int:
        """Estimate token count for tool schema"""

        if tool_id in self.token_cache:
            return self.token_cache[tool_id]

        import json
        schema = self.embedder.get_tool_schema(tool_id)
        if not schema:
            return 100  # Default estimate

        # Rough heuristic
        base = 60
        desc = schema.get('description', '')
        desc_tokens = len(desc.split()) * 1.3

        param_str = json.dumps(schema.get('parameters', {}))
        param_tokens = len(param_str) / 4

        total = int(base + desc_tokens + param_tokens)
        self.token_cache[tool_id] = total

        return total

    async def update_hot_tier(self, top_n: int = 10):
        """Update hot tier from usage stats"""
        hot_tools = self.embedder.get_hot_tier(top_n)
        self.hot_tier = set(hot_tools)
        logger.info(f"Updated hot tier: {len(self.hot_tier)} tools")


class QueryPlanner:
    """Plan tool sequences for complex queries"""

    def __init__(self, retriever, graph):
        self.retriever = retriever
        self.graph = graph
        self.llm_func = None  # Set by caller for query decomposition

    def set_llm_function(self, func):
        """Set LLM function for query decomposition"""
        self.llm_func = func

    async def plan(self, query: str, enable_multi_hop: bool = True) -> Dict:
        """
        Plan tool retrieval for a query.

        Returns:
        {
            "is_multi_hop": bool,
            "hops": [...],
            "all_tools": [tool_dicts],
            "estimated_tokens": int
        }
        """

        # Analyze complexity
        complexity = self._analyze_complexity(query)

        if not enable_multi_hop or not complexity['is_multi_hop']:
            # Single hop - simple retrieval
            tools = await self.retriever.retrieve_for_query(
                query,
                top_k=12,
                use_reranking=True
            )

            return {
                "is_multi_hop": False,
                "hops": [{
                    "description": query,
                    "tools": tools,
                    "category": None
                }],
                "all_tools": tools
            }

        # Multi-hop query
        logger.info(f"Multi-hop query detected: {complexity['estimated_hops']} hops")

        hops = await self._decompose_query(query, complexity['estimated_hops'])

        hop_plans = []
        all_tools_dict = {}  # Use dict to deduplicate while preserving metadata

        for i, hop_desc in enumerate(hops):
            logger.info(f"Planning hop {i+1}: {hop_desc}")

            category = self._infer_category(hop_desc)
            tools = await self.retriever.retrieve_for_query(
                hop_desc,
                top_k=8,
                category_filter=category,
                use_reranking=True
            )

            # Graph enhancement
            if i > 0 and hop_plans:
                prev_tool_ids = [t['tool_id'] for t in hop_plans[-1]['tools']]
                predicted = self.graph.predict_next_tools(prev_tool_ids, top_k=3)

                # Add predicted tools if not already present
                for pred_id in predicted:
                    if pred_id not in [t['tool_id'] for t in tools]:
                        schema = self.retriever.embedder.get_tool_schema(pred_id)
                        if schema:
                            tools.append({
                                'tool_id': pred_id,
                                'tool_name': pred_id.split(':', 1)[1],
                                'schema': schema,
                                'score': 0.5,  # Lower score for predicted
                                'source': 'graph'
                            })

            hop_plans.append({
                "description": hop_desc,
                "tools": tools,
                "category": category
            })

            # Add to all_tools (deduplicate)
            for tool in tools:
                all_tools_dict[tool['tool_id']] = tool

        # Convert back to list (preserves insertion order)
        all_tools = list(all_tools_dict.values())

        return {
            "is_multi_hop": True,
            "hops": hop_plans,
            "all_tools": all_tools
        }

    def _analyze_complexity(self, query: str) -> Dict:
        """Determine if query is multi-hop"""

        signals = {
            'has_and': ' and ' in query.lower(),
            'has_then': ' then ' in query.lower(),
            'multiple_verbs': len([
                w for w in query.lower().split()
                if w in ['find', 'search', 'get', 'extract', 'analyze', 'summarize', 'create', 'compare']
            ]) > 1,
            'length': len(query.split()) > 15
        }

        complexity_score = sum(signals.values())

        return {
            'is_multi_hop': complexity_score >= 2,
            'estimated_hops': min(complexity_score + 1, 3),
            'signals': signals
        }

    async def _decompose_query(self, query: str, num_hops: int) -> List[str]:
        """Decompose query into sequential steps"""

        # Simple heuristic-based decomposition for now
        # TODO: Use LLM for better decomposition

        query_lower = query.lower()

        # Pattern detection
        if 'find' in query_lower or 'search' in query_lower:
            if 'analyze' in query_lower or 'summarize' in query_lower:
                return [
                    f"Search for information about: {query.split('and')[0].strip()}",
                    "Extract relevant content from search results",
                    "Analyze and summarize the findings"
                ]

        if 'compare' in query_lower:
            if ' and ' in query_lower:
                parts = query.split(' and ')
                return [
                    f"Get information about: {parts[0].strip()}",
                    f"Get information about: {parts[1].strip()}",
                    "Compare the information"
                ]

        # Default: split by keywords
        return [query]

    def _infer_category(self, hop_desc: str) -> Optional[str]:
        """Infer tool category from hop description"""

        desc_lower = hop_desc.lower()

        if any(kw in desc_lower for kw in ['search', 'find', 'lookup']):
            return 'search'
        elif any(kw in desc_lower for kw in ['extract', 'read', 'get']):
            return 'extraction'
        elif any(kw in desc_lower for kw in ['analyze', 'summarize', 'compare']):
            return 'analysis'

        return None


class ToolRetrievalService:
    """Main service for tool retrieval - integrates all components"""

    def __init__(self, db_path: str = "tool_index.db"):
        self.embedder = ToolEmbedder(db_path)
        self.retriever = ToolRetriever(self.embedder)
        self.graph = ToolDependencyGraph(self.embedder)
        self.planner = QueryPlanner(self.retriever, self.graph)
        self.budget_mgr = ContextBudgetManager(self.embedder)
        self.initialized = False

    def set_jina_functions(self, embed_func, rerank_func):
        """Set Jina MCP tool functions"""
        self.embedder.set_embed_function(embed_func)
        self.retriever.set_jina_functions(embed_func, rerank_func)

    async def initialize(self, mcp_registry):
        """Initialize on server startup"""

        logger.info("Initializing Tool Retrieval Service...")

        # Embed all tools
        num_embedded = await self.embedder.embed_all_tools(mcp_registry)
        logger.info(f"Embedded {num_embedded} tools")

        # Load usage graph
        await self.graph.load_from_db()

        # Update hot tier
        await self.budget_mgr.update_hot_tier()

        self.initialized = True
        logger.info("✅ Tool Retrieval Service initialized")

    async def get_tools_for_query(
        self,
        query: str,
        enable_multi_hop: bool = True,
        max_tools: int = 15
    ) -> Dict:
        """
        Main entry point: get tools for a query.

        Returns:
        {
            "plan": {...},
            "selected_tools": [tool_dicts],
            "metrics": {...}
        }
        """

        if not self.initialized:
            logger.warning("Service not initialized - using all tools")
            return None

        start = time.time()

        # Plan the query
        plan = await self.planner.plan(query, enable_multi_hop)

        # Allocate context budget
        allocation = await self.budget_mgr.allocate_context(
            plan['all_tools'],
            force_include=list(self.budget_mgr.hot_tier)
        )

        retrieval_time_ms = (time.time() - start) * 1000

        result = {
            "plan": plan,
            "selected_tools": allocation['selected_tools'][:max_tools],
            "metrics": {
                "retrieval_time_ms": retrieval_time_ms,
                "total_tools_available": len(self.embedder.get_all_tool_ids()),
                "tools_retrieved": len(plan['all_tools']),
                "tools_selected": len(allocation['selected_tools']),
                "tools_dropped": allocation['tools_dropped'],
                "tokens_used": allocation['tokens_used'],
                "efficiency": allocation['efficiency'],
                "is_multi_hop": plan['is_multi_hop']
            }
        }

        logger.info(f"Retrieved {len(result['selected_tools'])} tools in {retrieval_time_ms:.1f}ms")

        return result

    async def log_tool_usage(
        self,
        session_id: str,
        query: str,
        tool_sequence: List[str],
        latencies: Optional[Dict[str, float]] = None
    ):
        """Log actual tool usage for learning"""

        # Update usage counts
        for tool_id in tool_sequence:
            latency = latencies.get(tool_id, 0.0) if latencies else 0.0
            self.embedder.increment_usage(tool_id, latency)

        # Update graph
        await self.graph.add_sequence(tool_sequence)

        # Log to database
        self.embedder.log_tool_sequence(session_id, query, tool_sequence)

        # Periodically update hot tier
        # TODO: Do this on a schedule rather than every request
        await self.budget_mgr.update_hot_tier()

    def get_stats(self) -> Dict:
        """Get service statistics"""

        return {
            **self.embedder.get_stats(),
            "hot_tier_size": len(self.budget_mgr.hot_tier),
            "graph_nodes": len(self.graph.graph),
            "cache_size": len(self.retriever.cache.cache)
        }
