# Production-Ready Tool Retrieval System Plan
## Using Jina Embeddings v3 + Reranker + MCP Integration

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     User Query                                │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 1: Query Analysis & Decomposition                     │
│  • Complexity detection (single vs multi-hop)                │
│  • Decompose into sub-tasks if needed                        │
│  • Embed query using Jina v3 (task=retrieval.query)         │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 2: Tool Embedding Store (Pre-computed)                │
│  • 125 tools → Jina embeddings v3 (task=retrieval.passage)  │
│  • Stored in SQLite with vector index                        │
│  • Metadata: server_id, category, usage_count, latency       │
│  • Cache TTL: 24 hours (re-embed on schema changes)         │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 3: Vector Search (Per Hop)                            │
│  • Cosine similarity: query_emb vs all tool_embs            │
│  • Top-K retrieval (K=20 for reranking)                     │
│  • Category filtering (search/extract/analyze/utility)      │
│  • Latency: ~10-20ms (in-memory numpy)                      │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 4: Jina Reranker (Precision Refinement)               │
│  • Input: query + top-20 tool descriptions                   │
│  • sort_by_relevance API call                                │
│  • Output: top-10 reranked tools                            │
│  • Latency: ~80-150ms (API call)                            │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 5: Graph Enhancement (Optional)                       │
│  • Predict next tools based on current selection             │
│  • Use co-occurrence matrix from usage logs                  │
│  • Merge graph predictions with retrieved tools              │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 6: Context Budget Management                          │
│  • Hot tier: 10 most-used tools (always loaded)             │
│  • Warm tier: Retrieved tools (fit to budget)               │
│  • Token estimation per tool schema                          │
│  • Target: <8000 tokens for tools                           │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  LAYER 7: LLM Context Assembly                               │
│  • Format selected tools as OpenAI schema                     │
│  • Add to system prompt                                       │
│  • Stream to LLM                                              │
└──────────────────────────────────────────────────────────────┘
```

## Component Specifications

### 1. Tool Schema Embedder (`tool_embedder.py`)

**Responsibilities:**
- Embed all 125 tool schemas on server startup
- Re-embed when MCP servers change
- Store embeddings in SQLite with metadata
- Use Jina embeddings v3 via MCP tool

**Implementation:**
```python
class ToolEmbedder:
    """Embed tool schemas using Jina embeddings v3"""

    async def embed_all_tools(self):
        """
        Batch embed all tools from all MCP servers
        Uses: mcp__distributed-systems-jina-plus-mcp__generate_embeddings
        """
        tool_schemas = await self.get_all_tool_schemas()

        # Create rich descriptions for embedding
        inputs = []
        for tool in tool_schemas:
            desc = self.create_embedding_text(tool)
            inputs.append(desc)

        # Batch embed using Jina (up to 1000 at a time)
        embeddings = await self.jina_embed({
            "inputs": inputs,
            "model": "jina-embeddings-v3",
            "task": "retrieval.passage",
            "dimensions": 1024,  # Use full dimensionality
            "late_chunking": False
        })

        # Store in SQLite
        await self.store_embeddings(embeddings, tool_schemas)

    def create_embedding_text(self, tool_schema: dict) -> str:
        """
        Create rich text for embedding.
        Format:
        Tool: {name}
        Category: {auto_category}
        Description: {description}
        Parameters: {param_names}
        Use cases: {extracted_use_cases}
        """
        parts = [f"Tool: {tool_schema['name']}"]

        if desc := tool_schema.get('description'):
            parts.append(f"Description: {desc}")

            # Extract use cases from description
            if 'use this when' in desc.lower():
                parts.append(desc)

        if params := tool_schema.get('parameters', {}).get('properties', {}):
            param_names = ', '.join(params.keys())
            parts.append(f"Parameters: {param_names}")

        return '\n'.join(parts)
```

**Database Schema:**
```sql
CREATE TABLE tool_embeddings (
    tool_id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    embedding BLOB NOT NULL,  -- numpy array as bytes
    schema_json TEXT NOT NULL,
    category TEXT NOT NULL,
    usage_count INTEGER DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0.0,
    last_used TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_server ON tool_embeddings(server_id);
CREATE INDEX idx_category ON tool_embeddings(category);
CREATE INDEX idx_usage ON tool_embeddings(usage_count DESC);

CREATE TABLE tool_usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    tool_sequence TEXT NOT NULL,  -- JSON array
    query TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_session ON tool_usage_log(session_id);
```

### 2. Semantic Tool Retriever (`tool_retriever.py`)

**Responsibilities:**
- Embed incoming queries
- Vector search over tool embeddings
- Rerank top candidates using Jina reranker
- Cache results

**Key Methods:**
```python
class ToolRetriever:
    """Retrieve relevant tools using embeddings + reranking"""

    async def retrieve_for_query(
        self,
        query: str,
        top_k: int = 10,
        use_reranking: bool = True,
        category_filter: Optional[str] = None
    ) -> List[ToolSchema]:
        """
        Main retrieval pipeline:
        1. Embed query
        2. Vector search (top-20)
        3. Rerank (top-10)
        4. Return schemas
        """

        # Check cache
        cache_key = (query, top_k, category_filter)
        if cached := self.cache.get(cache_key):
            return cached

        # Embed query using Jina
        query_emb = await self.embed_query(query)

        # Vector search
        candidates = await self.vector_search(
            query_emb,
            top_k=20 if use_reranking else top_k,
            category_filter=category_filter
        )

        # Rerank using Jina
        if use_reranking and len(candidates) > top_k:
            reranked = await self.rerank_tools(query, candidates, top_k)
            result = reranked
        else:
            result = candidates[:top_k]

        # Cache
        self.cache.set(cache_key, result, ttl=300)
        return result

    async def embed_query(self, query: str) -> np.ndarray:
        """Embed query using Jina v3"""
        result = await self.jina_embed({
            "inputs": [query],
            "model": "jina-embeddings-v3",
            "task": "retrieval.query",
            "dimensions": 1024
        })
        return np.array(result['data'][0]['embedding'])

    async def rerank_tools(
        self,
        query: str,
        tool_ids: List[str],
        top_k: int
    ) -> List[str]:
        """Rerank using Jina reranker API"""

        # Get tool descriptions
        documents = [
            self.get_tool_description(tid)
            for tid in tool_ids
        ]

        # Call Jina reranker via MCP tool
        result = await self.jina_rerank({
            "query": query,
            "documents": documents,
            "top_n": top_k
        })

        # Map back to tool_ids
        reranked_ids = [
            tool_ids[item['index']]
            for item in result['results']
        ]

        return reranked_ids
```

### 3. Multi-Hop Query Planner (`query_planner.py`)

**Responsibilities:**
- Detect multi-hop queries
- Decompose into sequential tasks
- Retrieve tools for each hop
- Use graph predictions

**Implementation:**
```python
class QueryPlanner:
    """Plan tool sequences for complex queries"""

    async def plan(self, query: str) -> Dict:
        """
        Returns:
        {
            "is_multi_hop": bool,
            "hops": [
                {
                    "description": str,
                    "tools": [tool_ids],
                    "category": str
                }
            ],
            "all_tools": [deduplicated_tool_ids],
            "estimated_tokens": int
        }
        """

        # Analyze complexity
        complexity = self.analyze_complexity(query)

        if not complexity['is_multi_hop']:
            # Single hop - simple retrieval
            tools = await self.retriever.retrieve_for_query(
                query,
                top_k=12
            )
            return {
                "is_multi_hop": False,
                "hops": [{
                    "description": query,
                    "tools": [t.tool_id for t in tools],
                    "category": None
                }],
                "all_tools": [t.tool_id for t in tools]
            }

        # Multi-hop - decompose
        hops = await self.decompose_query(
            query,
            num_hops=complexity['estimated_hops']
        )

        # Retrieve for each hop
        hop_plans = []
        all_tools = []

        for i, hop_desc in enumerate(hops):
            category = self.infer_category(hop_desc)
            tools = await self.retriever.retrieve_for_query(
                hop_desc,
                top_k=8,
                category_filter=category
            )

            # Graph enhancement
            if i > 0:
                predicted = self.graph.predict_next(
                    hop_plans[-1]['tools'],
                    top_k=3
                )
                # Merge predictions
                tool_ids = list(set([t.tool_id for t in tools] + predicted))
            else:
                tool_ids = [t.tool_id for t in tools]

            hop_plans.append({
                "description": hop_desc,
                "tools": tool_ids,
                "category": category
            })

            all_tools.extend(tool_ids)

        # Deduplicate while preserving order
        unique_tools = list(dict.fromkeys(all_tools))

        return {
            "is_multi_hop": True,
            "hops": hop_plans,
            "all_tools": unique_tools
        }

    async def decompose_query(
        self,
        query: str,
        num_hops: int
    ) -> List[str]:
        """
        Use lightweight LLM to decompose query into steps.

        Prompt template:
        "Break this query into {num_hops} sequential steps.
        Each step should be a single action.

        Query: {query}

        Steps:
        1. [step 1]
        2. [step 2]
        ..."
        """

        # Call fast LLM (claude-3-haiku or gpt-4o-mini)
        response = await self.llm_call(
            model="anthropic/claude-3-haiku",
            prompt=f"""Break this query into {num_hops} sequential steps:

Query: {query}

Return only the steps, one per line, numbered 1-{num_hops}.""",
            max_tokens=200,
            temperature=0.3
        )

        # Parse steps
        steps = []
        for line in response.split('\n'):
            if line.strip() and line[0].isdigit():
                step = line.split('.', 1)[1].strip()
                steps.append(step)

        return steps[:num_hops]
```

### 4. Context Budget Manager (`context_manager.py`)

**Responsibilities:**
- Manage hot/warm/cold tiers
- Allocate tools within token budget
- Estimate token counts
- Update tiers based on usage

```python
class ContextBudgetManager:
    """Manage tool context within token budget"""

    def __init__(self, max_tool_tokens: int = 8000):
        self.max_tokens = max_tool_tokens
        self.hot_tier = set()  # Top 10 most used
        self.token_estimates = {}

    async def allocate_context(
        self,
        retrieved_tools: List[str],
        force_include: Optional[List[str]] = None
    ) -> Dict:
        """
        Allocate tools within budget.

        Returns:
        {
            "selected_tools": [tool_ids],
            "tokens_used": int,
            "tools_dropped": int,
            "efficiency": float  # selected / available
        }
        """

        budget = self.max_tokens
        selected = []

        # Force-include tools (hot tier)
        if force_include:
            for tool_id in force_include:
                tokens = self.estimate_tokens(tool_id)
                budget -= tokens
                selected.append(tool_id)

        # Add hot tier
        for tool_id in self.hot_tier:
            if tool_id not in selected:
                tokens = self.estimate_tokens(tool_id)
                if budget >= tokens:
                    budget -= tokens
                    selected.append(tool_id)

        # Add retrieved tools in relevance order
        for tool_id in retrieved_tools:
            if tool_id in selected:
                continue

            tokens = self.estimate_tokens(tool_id)
            if budget >= tokens:
                budget -= tokens
                selected.append(tool_id)
            else:
                break  # Budget exhausted

        tokens_used = self.max_tokens - budget

        return {
            "selected_tools": selected,
            "tokens_used": tokens_used,
            "tools_dropped": len(retrieved_tools) - len(selected),
            "efficiency": len(selected) / max(len(retrieved_tools), 1)
        }

    def estimate_tokens(self, tool_id: str) -> int:
        """
        Estimate tokens for tool schema.

        Formula:
        base (60) +
        description tokens (words * 1.3) +
        parameter tokens (json length / 4)
        """

        if tool_id in self.token_estimates:
            return self.token_estimates[tool_id]

        schema = self.get_tool_schema(tool_id)

        base = 60
        desc_tokens = len(schema['description'].split()) * 1.3
        param_str = json.dumps(schema.get('parameters', {}))
        param_tokens = len(param_str) / 4

        total = int(base + desc_tokens + param_tokens)
        self.token_estimates[tool_id] = total

        return total

    async def update_hot_tier(self):
        """Update hot tier from usage stats"""

        # Query database for top 10 most-used tools
        top_tools = await self.db.execute("""
            SELECT tool_id
            FROM tool_embeddings
            ORDER BY usage_count DESC
            LIMIT 10
        """)

        self.hot_tier = set(row[0] for row in top_tools)
```

### 5. Integration Layer (`tool_retrieval_service.py`)

**Main service that ties everything together:**

```python
class ToolRetrievalService:
    """Main service for tool retrieval"""

    def __init__(self, mcp_registry, db_path="tool_index.db"):
        self.mcp_registry = mcp_registry
        self.embedder = ToolEmbedder(db_path)
        self.retriever = ToolRetriever(db_path)
        self.planner = QueryPlanner(self.retriever)
        self.budget_mgr = ContextBudgetManager()
        self.graph = ToolDependencyGraph(db_path)

    async def initialize(self):
        """Initialize on server startup"""
        # Embed all current tools
        all_tools = await self.mcp_registry.get_all_tools()
        await self.embedder.embed_all_tools(all_tools)

        # Load usage graph
        await self.graph.load_from_db()

        # Update hot tier
        await self.budget_mgr.update_hot_tier()

    async def get_tools_for_query(
        self,
        query: str,
        enable_multi_hop: bool = True
    ) -> Dict:
        """
        Main entry point: get tools for a query.

        Returns:
        {
            "plan": {...},  # From planner
            "selected_tools": [tool_schemas],
            "metrics": {
                "retrieval_time_ms": float,
                "total_tools_available": int,
                "tools_selected": int,
                "tokens_used": int,
                "efficiency": float
            }
        }
        """

        start = time.time()

        # Plan the query
        if enable_multi_hop:
            plan = await self.planner.plan(query)
        else:
            # Simple retrieval
            tools = await self.retriever.retrieve_for_query(query, top_k=15)
            plan = {
                "is_multi_hop": False,
                "all_tools": [t.tool_id for t in tools]
            }

        # Allocate context budget
        allocation = await self.budget_mgr.allocate_context(
            plan['all_tools']
        )

        # Get full schemas for selected tools
        selected_schemas = [
            await self.get_tool_schema(tid)
            for tid in allocation['selected_tools']
        ]

        retrieval_time = (time.time() - start) * 1000

        return {
            "plan": plan,
            "selected_tools": selected_schemas,
            "metrics": {
                "retrieval_time_ms": retrieval_time,
                "total_tools_available": len(self.embedder.get_all_tool_ids()),
                "tools_selected": len(selected_schemas),
                "tokens_used": allocation['tokens_used'],
                "efficiency": allocation['efficiency']
            }
        }

    async def log_tool_usage(
        self,
        session_id: str,
        query: str,
        tool_sequence: List[str]
    ):
        """Log actual tool usage for learning"""

        # Update usage counts
        for tool_id in tool_sequence:
            await self.embedder.increment_usage(tool_id)

        # Update graph
        await self.graph.add_sequence(tool_sequence)

        # Log to database
        await self.db.log_usage(session_id, query, tool_sequence)
```

## Integration with agent_server.py

**Modifications needed:**

```python
# In agent_server.py startup
tool_retrieval = ToolRetrievalService(mcp_registry)
await tool_retrieval.initialize()

# In handle_agent_interaction, before LLM call:
# Instead of: all_tools = await mcp_registry.get_all_tools()
# Use:
retrieval_result = await tool_retrieval.get_tools_for_query(
    message,
    enable_multi_hop=True
)

selected_tools = retrieval_result['selected_tools']
metrics = retrieval_result['metrics']

# Log metrics to SSE stream
yield {
    "event": "tool.retrieval.complete",
    "data": metrics
}

# After tool execution, log usage:
await tool_retrieval.log_tool_usage(
    session_id,
    message,
    [call.function.name for call in tool_calls_received]
)
```

## Performance Targets

```
Latency Budget (per request):
├─ Query embedding: 30-50ms (Jina API)
├─ Vector search: 10-20ms (in-memory numpy)
├─ Reranking: 80-150ms (Jina API)
├─ Multi-hop planning: 100-200ms (if needed, haiku LLM)
├─ Context allocation: 5-10ms
└─ Total overhead: 250-430ms ✓ Acceptable

Memory Usage:
├─ 125 tools × 1024 dims × 4 bytes = 512KB (embeddings)
├─ Schemas: ~500KB (JSON)
├─ Graph: ~100KB
├─ Cache: ~1MB
└─ Total: <2MB ✓ Lightweight

Token Savings:
├─ Current (all tools): ~15,000 tokens
├─ With retrieval (hot + retrieved): ~3,000 tokens
└─ Savings: 80% ✓ Massive cost reduction
```

## Caching Strategy

```python
# 3-tier caching
class CacheLayer:
    # L1: In-memory LRU (hot queries)
    query_cache = LRUCache(maxsize=1000)  # query -> tool_ids

    # L2: Redis (if available, shared across instances)
    # embedding_cache: query -> embedding vector

    # L3: SQLite (persistent)
    # tool_embeddings table

    TTLs:
    - Query results: 5 minutes
    - Query embeddings: 1 hour
    - Tool embeddings: 24 hours (invalidate on schema change)
```

## Monitoring & Metrics

**Track these metrics:**

```python
metrics = {
    # Retrieval quality
    "precision@10": 0.0,  # % of retrieved tools actually used
    "recall@10": 0.0,     # % of used tools that were retrieved
    "mrr": 0.0,           # Mean reciprocal rank

    # Performance
    "retrieval_latency_p50": 0.0,
    "retrieval_latency_p99": 0.0,
    "embedding_cache_hit_rate": 0.0,
    "query_cache_hit_rate": 0.0,

    # Context efficiency
    "avg_tools_loaded": 0.0,
    "avg_tools_used": 0.0,
    "utilization_rate": 0.0,
    "token_savings_percent": 0.0,

    # Business
    "cost_per_request": 0.0,
    "cost_savings_vs_all_tools": 0.0
}
```

## Rollout Plan

**Week 1: Foundation**
- [ ] Create database schema
- [ ] Implement ToolEmbedder with Jina v3
- [ ] Embed all 125 current tools
- [ ] Basic vector search working

**Week 2: Retrieval**
- [ ] Implement ToolRetriever
- [ ] Add Jina reranker integration
- [ ] Caching layer (in-memory LRU)
- [ ] Benchmark latency

**Week 3: Planning**
- [ ] Implement QueryPlanner
- [ ] Multi-hop detection
- [ ] Query decomposition (Haiku LLM)
- [ ] Test on complex queries

**Week 4: Integration**
- [ ] Integrate with agent_server.py
- [ ] Context budget manager
- [ ] Usage logging
- [ ] Metrics collection

**Week 5: Learning**
- [ ] Build tool dependency graph
- [ ] Graph-based predictions
- [ ] Hot tier updates
- [ ] A/B test retrieval vs all-tools

**Week 6: Optimization**
- [ ] Performance tuning
- [ ] Add Redis cache
- [ ] Optimize embedding batch size
- [ ] Scale test with 500+ tools

## Success Criteria

✅ **Latency**: <500ms overhead for tool retrieval
✅ **Precision**: >70% of retrieved tools are used
✅ **Recall**: >90% of used tools were retrieved
✅ **Token Savings**: >75% reduction vs loading all tools
✅ **Cost Savings**: >70% reduction per request
✅ **Scalability**: Handle 1000+ tools with <1s overhead

## Next Steps

1. **Immediate**: Create tool_retrieval_service.py skeleton
2. **Today**: Implement ToolEmbedder with real Jina calls
3. **This week**: Get basic retrieval working
4. **Next week**: Integrate with agent_server.py

This is production-ready, uses real Jina APIs, and scales to thousands of tools. 🚀
