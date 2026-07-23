# Tool Retrieval Architecture: Multi-Hop RAG for Dynamic MCP Context Management

## The Problem

**Context Explosion with Scale:**
- Current: 3 MCP servers × 125 tools = manageable
- Future: 50 MCP servers × 2000 tools = context catastrophe
- Tool schemas consume 100-300 tokens each
- LLM context budget: finite and expensive
- Loading all tools: slow TTFT, high cost, diluted attention

**Multi-Hop Complexity:**
Many queries require sequential tool execution:
```
Query: "Find recent AI safety papers and create a risk assessment"

Hop 1: Search (search_arxiv) → paper URLs
Hop 2: Extract (read_url) → paper content
Hop 3: Analyze (answer_with_evidence) → risk assessment

Each hop needs different tools, but we need to plan ahead!
```

## Solution: Tool Retrieval as RAG

Treat tool schemas as documents. Use Jina embeddings + reranking to build an intelligent retrieval system.

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Tool Index (Offline)                              │
│ • Pre-embed all tool schemas (name+desc+params)            │
│ • Store in vector index with metadata                      │
│ • Update on MCP server connect/disconnect                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 2: Query Understanding (Online)                      │
│ • Embed user query                                          │
│ • Analyze complexity (single vs multi-hop)                 │
│ • Decompose into sub-tasks if needed                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Multi-Hop Planning                                │
│ • Predict tool sequence needed                             │
│ • For each hop: retrieve top-k relevant tools              │
│ • Use tool co-occurrence graph for refinement              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 4: Reranking & Selection                             │
│ • Use Jina reranker for precision                          │
│ • Deduplicate across hops                                  │
│ • Apply context budget constraints                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 5: Context Assembly                                  │
│ • Hot tier: Always-loaded core tools (~5-10)               │
│ • Warm tier: Retrieved tools (top-K by relevance)          │
│ • Cold tier: Available on-demand                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 6: Execution & Learning                              │
│ • Track which tools actually get called                    │
│ • Build co-occurrence matrix                               │
│ • Refine retrieval using feedback                          │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Tool Schema Embeddings

```python
class ToolSchemaIndex:
    """
    Vector index for all tool schemas across all MCP servers.
    Uses Jina embeddings v3 with task="retrieval.query"
    """

    def __init__(self, jina_embed_tool):
        self.embeddings = {}  # tool_id -> embedding vector
        self.schemas = {}     # tool_id -> full schema
        self.metadata = {}    # tool_id -> {server_id, category, usage_count}

    async def index_tool(self, server_id: str, tool_schema: dict):
        """Index a single tool schema"""
        tool_id = f"{server_id}:{tool_schema['name']}"

        # Create rich description for embedding
        description = self._create_search_text(tool_schema)

        # Embed using Jina with retrieval.passage task
        embedding = await self.jina_embed({
            "input": description,
            "task": "retrieval.passage",
            "dimensions": 768
        })

        self.embeddings[tool_id] = embedding
        self.schemas[tool_id] = tool_schema
        self.metadata[tool_id] = {
            "server_id": server_id,
            "category": self._categorize_tool(tool_schema),
            "usage_count": 0,
            "avg_latency": None
        }

    def _create_search_text(self, schema: dict) -> str:
        """
        Create rich text representation for semantic search.
        Combines name, description, parameters, examples.
        """
        parts = [
            f"Tool: {schema['name']}",
            f"Purpose: {schema.get('description', '')}",
        ]

        # Add parameter context
        if params := schema.get('parameters', {}).get('properties', {}):
            param_list = ', '.join(params.keys())
            parts.append(f"Parameters: {param_list}")

        # Add usage hints from description
        if 'Use this when' in schema.get('description', ''):
            parts.append(schema['description'])

        return '\n'.join(parts)

    def _categorize_tool(self, schema: dict) -> str:
        """Auto-categorize tools for filtering"""
        desc = schema.get('description', '').lower()

        if any(kw in desc for kw in ['search', 'find', 'query']):
            return 'search'
        elif any(kw in desc for kw in ['read', 'extract', 'fetch', 'download']):
            return 'extraction'
        elif any(kw in desc for kw in ['analyze', 'summarize', 'generate']):
            return 'analysis'
        elif any(kw in desc for kw in ['write', 'create', 'update', 'modify']):
            return 'mutation'
        else:
            return 'utility'
```

### 2. Query-Based Retrieval

```python
class ToolRetriever:
    """
    Retrieve relevant tools for a given query using semantic search + reranking.
    """

    def __init__(self, index: ToolSchemaIndex, jina_rerank_tool):
        self.index = index
        self.reranker = jina_rerank_tool
        self.query_cache = LRUCache(maxsize=1000)

    async def retrieve_tools(
        self,
        query: str,
        top_k: int = 15,
        category_filter: Optional[str] = None,
        use_reranking: bool = True
    ) -> List[str]:
        """
        Retrieve top-k most relevant tools for query.

        Returns list of tool_ids ranked by relevance.
        """
        # Check cache first
        cache_key = (query, top_k, category_filter)
        if cache_key in self.query_cache:
            return self.query_cache[cache_key]

        # Embed query
        query_embedding = await self._embed_query(query)

        # Get candidate tools via cosine similarity
        candidates = self._vector_search(
            query_embedding,
            top_k=top_k * 3,  # Over-retrieve for reranking
            category_filter=category_filter
        )

        if use_reranking and len(candidates) > top_k:
            # Use Jina reranker for precision
            reranked = await self._rerank_tools(query, candidates, top_k)
            result = reranked
        else:
            result = candidates[:top_k]

        self.query_cache[cache_key] = result
        return result

    async def _embed_query(self, query: str):
        """Embed query with retrieval.query task"""
        return await jina_embed({
            "input": query,
            "task": "retrieval.query",
            "dimensions": 768
        })

    def _vector_search(self, query_vec, top_k, category_filter=None):
        """Cosine similarity search over tool embeddings"""
        scores = []

        for tool_id, tool_vec in self.index.embeddings.items():
            # Apply category filter
            if category_filter:
                if self.index.metadata[tool_id]['category'] != category_filter:
                    continue

            # Cosine similarity
            score = cosine_similarity(query_vec, tool_vec)
            scores.append((tool_id, score))

        # Sort by score descending
        scores.sort(key=lambda x: x[1], reverse=True)
        return [tool_id for tool_id, _ in scores[:top_k]]

    async def _rerank_tools(self, query: str, tool_ids: List[str], top_k: int):
        """Use Jina reranker for final precision"""
        documents = [
            self.index.schemas[tid]['description']
            for tid in tool_ids
        ]

        reranked = await self.reranker({
            "query": query,
            "documents": documents,
            "top_n": top_k
        })

        # Map back to tool_ids
        return [tool_ids[r['index']] for r in reranked['results']]
```

### 3. Multi-Hop Planning

This is the key innovation: predict the sequence of tools needed.

```python
class MultiHopPlanner:
    """
    Decompose complex queries into multi-hop tool sequences.
    """

    def __init__(self, retriever: ToolRetriever):
        self.retriever = retriever
        self.tool_graph = ToolDependencyGraph()  # Learned from usage

    async def plan_tool_sequence(
        self,
        query: str,
        max_hops: int = 4
    ) -> List[List[str]]:
        """
        Returns: List of tool sets, one per hop.
        Example: [
            ['search_arxiv', 'search_web'],      # Hop 1: Search
            ['read_url', 'extract'],             # Hop 2: Extract
            ['answer_with_evidence']             # Hop 3: Analyze
        ]
        """
        # Analyze query complexity
        complexity = await self._analyze_complexity(query)

        if complexity['is_simple']:
            # Single hop - direct retrieval
            tools = await self.retriever.retrieve_tools(query, top_k=10)
            return [tools]

        # Multi-hop query - decompose
        hops = await self._decompose_query(query, max_hops)

        # Retrieve tools for each hop
        hop_tools = []
        for hop_desc in hops:
            tools = await self.retriever.retrieve_tools(
                hop_desc,
                top_k=8,
                category_filter=self._infer_category(hop_desc)
            )

            # Enhance with graph-based predictions
            if hop_tools:  # After first hop
                predicted_next = self.tool_graph.predict_next_tools(
                    hop_tools[-1],
                    top_k=3
                )
                tools = self._merge_and_dedupe(tools, predicted_next)

            hop_tools.append(tools)

        return hop_tools

    async def _analyze_complexity(self, query: str) -> dict:
        """
        Determine if query is simple (single tool) or complex (multi-hop).
        Uses heuristics + lightweight LLM call.
        """
        # Heuristic signals
        signals = {
            'has_and': ' and ' in query.lower(),
            'has_then': ' then ' in query.lower(),
            'multiple_verbs': self._count_action_verbs(query) > 1,
            'length': len(query.split()) > 15
        }

        complexity_score = sum(signals.values())

        return {
            'is_simple': complexity_score <= 1,
            'estimated_hops': min(complexity_score + 1, 4),
            'signals': signals
        }

    async def _decompose_query(self, query: str, max_hops: int) -> List[str]:
        """
        Break complex query into sequential sub-tasks.
        Uses small LLM call for decomposition.
        """
        # Use lightweight model for fast decomposition
        prompt = f"""Break this query into {max_hops} sequential steps:

Query: {query}

Return steps as:
1. [First action needed]
2. [Second action needed]
...
"""

        response = await llm_call(prompt, model="fast", max_tokens=200)

        # Parse steps
        steps = [
            line.split('. ', 1)[1].strip()
            for line in response.split('\n')
            if line and line[0].isdigit()
        ]

        return steps[:max_hops]
```

### 4. Tool Dependency Graph

Learn from execution patterns:

```python
class ToolDependencyGraph:
    """
    Directed graph tracking tool usage patterns.
    Edge weights = co-occurrence frequency.
    """

    def __init__(self):
        self.graph = nx.DiGraph()
        self.sequences = []  # Historical sequences for analysis

    def add_sequence(self, tool_sequence: List[str]):
        """Learn from an actual tool execution sequence"""
        self.sequences.append(tool_sequence)

        for i in range(len(tool_sequence) - 1):
            current = tool_sequence[i]
            next_tool = tool_sequence[i + 1]

            if self.graph.has_edge(current, next_tool):
                self.graph[current][next_tool]['weight'] += 1
            else:
                self.graph.add_edge(current, next_tool, weight=1)

    def predict_next_tools(self, current_tools: List[str], top_k: int = 5) -> List[str]:
        """
        Given current tool(s), predict what tools are likely needed next.
        Uses graph traversal + edge weights.
        """
        candidates = defaultdict(float)

        for tool in current_tools:
            if tool in self.graph:
                for neighbor in self.graph.neighbors(tool):
                    weight = self.graph[tool][neighbor]['weight']
                    # Decay weight by graph distance
                    candidates[neighbor] += weight

                    # Also look at 2-hop neighbors (with decay)
                    if neighbor in self.graph:
                        for second_hop in self.graph.neighbors(neighbor):
                            candidates[second_hop] += weight * 0.3

        # Sort by score
        ranked = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
        return [tool_id for tool_id, _ in ranked[:top_k]]

    def get_tool_clusters(self, n_clusters: int = 10):
        """
        Cluster tools by co-occurrence patterns.
        Useful for preloading tool groups.
        """
        # Build co-occurrence matrix
        tools = list(self.graph.nodes())
        n = len(tools)
        cooccurrence = np.zeros((n, n))

        for seq in self.sequences:
            for i, t1 in enumerate(seq):
                for t2 in seq[i:]:
                    idx1, idx2 = tools.index(t1), tools.index(t2)
                    cooccurrence[idx1, idx2] += 1
                    cooccurrence[idx2, idx1] += 1

        # Cluster using spectral clustering
        from sklearn.cluster import SpectralClustering
        clustering = SpectralClustering(n_clusters=n_clusters, affinity='precomputed')
        labels = clustering.fit_predict(cooccurrence)

        # Group tools by cluster
        clusters = defaultdict(list)
        for tool, label in zip(tools, labels):
            clusters[label].append(tool)

        return dict(clusters)
```

### 5. Context Budget Manager

```python
class ContextBudgetManager:
    """
    Manage tool loading within token budget constraints.
    3-tier system: Hot, Warm, Cold
    """

    def __init__(self, max_tool_tokens: int = 10000):
        self.max_tokens = max_tool_tokens
        self.hot_tier = set()  # Always loaded
        self.warm_tier = set()  # Recently used
        self.cold_tier = set()  # Available but not loaded

    def allocate_context(
        self,
        retrieved_tools: List[str],
        hot_tools: Optional[List[str]] = None
    ) -> List[str]:
        """
        Given retrieved tools, decide what to actually load.
        Returns: List of tool_ids to include in LLM context.
        """
        if hot_tools is None:
            hot_tools = list(self.hot_tier)

        budget = self.max_tokens
        selected = []

        # Always include hot tier (core utilities)
        for tool_id in hot_tools:
            tool_size = self._estimate_tokens(tool_id)
            budget -= tool_size
            selected.append(tool_id)

        # Add retrieved tools in order of relevance
        for tool_id in retrieved_tools:
            if tool_id in selected:
                continue

            tool_size = self._estimate_tokens(tool_id)

            if budget >= tool_size:
                selected.append(tool_id)
                budget -= tool_size
            else:
                # Budget exhausted
                break

        return selected

    def _estimate_tokens(self, tool_id: str) -> int:
        """Estimate token count for tool schema"""
        schema = tool_index.schemas[tool_id]

        # Rough heuristic:
        # name (10) + description (50-200) + parameters (50-500)
        base = 60
        desc_tokens = len(schema.get('description', '').split()) * 1.3
        param_tokens = len(str(schema.get('parameters', {}))).split() * 1.3

        return int(base + desc_tokens + param_tokens)

    def update_tiers(self, usage_stats: dict):
        """Update hot/warm/cold tiers based on usage patterns"""
        # Hot tier: top 10 most frequently used tools
        sorted_tools = sorted(
            usage_stats.items(),
            key=lambda x: x[1]['usage_count'],
            reverse=True
        )
        self.hot_tier = set(tid for tid, _ in sorted_tools[:10])

        # Warm tier: recently used (last 100 requests)
        # Cold tier: everything else
```

## Implementation Flow

### Request Processing Pipeline

```
User Query: "Find recent papers on quantum computing and analyze their methodology"

Step 1: Query Analysis (50ms)
├─ Complexity: Multi-hop detected
├─ Estimated hops: 3
└─ Categories: search, extraction, analysis

Step 2: Decomposition (100ms via fast LLM)
├─ Hop 1: "Search for quantum computing papers"
├─ Hop 2: "Extract methodology sections from papers"
└─ Hop 3: "Analyze and compare methodologies"

Step 3: Tool Retrieval (Per Hop)
│
├─ Hop 1 Tools (150ms):
│   ├─ Embed query: "search quantum computing papers"
│   ├─ Vector search: Top 15 candidates
│   ├─ Rerank: Top 5
│   └─ Selected: [search_arxiv, search_web, search_images]
│
├─ Hop 2 Tools (150ms):
│   ├─ Embed query: "extract methodology sections"
│   ├─ Vector search + graph prediction
│   └─ Selected: [read_url, extract, parallel_read_url]
│
└─ Hop 3 Tools (150ms):
    ├─ Embed query: "analyze methodologies"
    └─ Selected: [answer_with_evidence, compare_texts, summarize_text]

Step 4: Deduplication & Budget (50ms)
├─ All tools: 11 unique tools
├─ Estimated tokens: 2,800
├─ Hot tier tools: +500 tokens
├─ Total: 3,300 tokens ✓ (under 10K budget)
└─ Context ready for LLM

Step 5: Execution with Adaptive Retrieval
├─ Execute Hop 1 → paper URLs retrieved
├─ Refine Hop 2: Based on actual URLs from Hop 1
│   └─ Add parallel_read_url since multiple URLs
├─ Execute Hop 2 → methodology text extracted
└─ Execute Hop 3 → comparative analysis generated

Step 6: Learning Update (async, 20ms)
├─ Record sequence: search_arxiv → read_url → answer_with_evidence
├─ Update graph: strengthen edge weights
└─ Update usage stats: increment tool counters
```

## Performance Characteristics

**Latency Budget:**
```
Query embedding:          30-50ms
Vector search:            10-20ms (in-memory)
Reranking (Jina API):     80-120ms
Multi-hop decomposition:  100-150ms (fast LLM)
Graph predictions:        5-10ms
Total overhead:           ~250-350ms

Target: Add <400ms to TTFT
Acceptable for 10x context efficiency
```

**Memory Budget:**
```
Per Tool:
├─ Embedding (768 dims × float32): 3KB
├─ Schema JSON: 2-5KB
└─ Metadata: 0.5KB

For 2000 tools:
├─ Embeddings: 6MB
├─ Schemas: 8MB
├─ Graph: 4MB
└─ Total: ~20MB (easily cacheable)
```

## Advanced Optimizations

### 1. Hierarchical Clustering for Fast Retrieval

```python
class HierarchicalToolIndex:
    """
    2-level index: clusters → tools
    Reduces search space for faster retrieval.
    """
    def __init__(self):
        self.clusters = {}  # cluster_id → {tools, centroid}
        self.n_clusters = 20

    def build_clusters(self, tool_embeddings):
        """
        Cluster tools by semantic similarity.
        Search clusters first, then tools within cluster.
        """
        from sklearn.cluster import KMeans

        vectors = np.array(list(tool_embeddings.values()))
        tool_ids = list(tool_embeddings.keys())

        kmeans = KMeans(n_clusters=self.n_clusters)
        labels = kmeans.fit_predict(vectors)

        for i in range(self.n_clusters):
            cluster_tools = [tool_ids[j] for j, l in enumerate(labels) if l == i]
            self.clusters[i] = {
                'tools': cluster_tools,
                'centroid': kmeans.cluster_centers_[i]
            }

    def retrieve(self, query_embedding, top_k=10):
        """
        Two-stage retrieval:
        1. Find top-3 relevant clusters
        2. Search within those clusters
        """
        # Stage 1: Cluster search
        cluster_scores = []
        for cid, cluster in self.clusters.items():
            score = cosine_similarity(query_embedding, cluster['centroid'])
            cluster_scores.append((cid, score))

        top_clusters = sorted(cluster_scores, key=lambda x: x[1], reverse=True)[:3]

        # Stage 2: Tool search within top clusters
        candidates = []
        for cid, _ in top_clusters:
            candidates.extend(self.clusters[cid]['tools'])

        # Final ranking of candidates
        return self._rank_tools(query_embedding, candidates, top_k)
```

### 2. Streaming Tool Loading

```python
async def stream_tool_context(query: str):
    """
    Stream tool schemas to LLM as they're retrieved.
    Enables instant TTFT while tools load in background.
    """
    # Immediately send hot tier tools
    hot_tools = get_hot_tier_tools()
    yield format_tools_for_llm(hot_tools)

    # Retrieve warm tier in background
    retrieved = await retrieve_tools_async(query, top_k=15)

    # Stream as they arrive
    for tool_id in retrieved:
        if tool_id not in hot_tools:
            tool_schema = get_tool_schema(tool_id)
            yield format_single_tool(tool_schema)
```

### 3. Query Expansion for Better Recall

```python
async def expand_query_for_retrieval(query: str):
    """
    Expand query with synonyms and related concepts.
    Improves retrieval recall for edge cases.
    """
    # Use Jina's expand_query tool if available
    expanded = await jina_expand_query(query)

    # Or use lightweight expansion
    expansions = [
        query,
        extract_keywords(query),
        extract_intent(query),
        paraphrase_query(query)
    ]

    # Retrieve with each expansion
    all_tools = []
    for exp in expansions:
        tools = await retrieve_tools(exp, top_k=5)
        all_tools.extend(tools)

    # Deduplicate and rerank
    unique_tools = list(set(all_tools))
    return await rerank_tools(query, unique_tools, top_k=15)
```

## Evaluation Metrics

**Retrieval Quality:**
```python
def evaluate_retrieval():
    """
    Metrics to track retrieval quality.
    """
    metrics = {
        'precision@k': 0.0,  # % of retrieved tools actually used
        'recall@k': 0.0,     # % of used tools that were retrieved
        'mrr': 0.0,          # Mean reciprocal rank of first relevant tool
        'ndcg': 0.0,         # Normalized DCG for ranking quality
        'latency_p50': 0.0,  # Median retrieval latency
        'latency_p99': 0.0,  # 99th percentile latency
    }
    return metrics
```

**Context Efficiency:**
```python
def measure_efficiency():
    """
    Track how efficiently we use context budget.
    """
    return {
        'tools_loaded': 0,        # Number of tools in context
        'tools_used': 0,          # Number actually called
        'utilization': 0.0,       # used / loaded ratio
        'tokens_saved': 0,        # vs loading all tools
        'cost_reduction': 0.0,    # $ saved per request
    }
```

## Migration Path

**Phase 1: Index Building (Week 1)**
- Embed all current 125 tools
- Build initial tool graph from logs
- Benchmark retrieval latency

**Phase 2: Hybrid Mode (Week 2-3)**
- Keep loading all tools (fallback)
- Add retrieval in parallel
- Compare retrieved vs actual usage
- Tune top-k, reranking threshold

**Phase 3: Retrieval-First (Week 4)**
- Switch to retrieval-first mode
- Load all tools only if retrieval fails
- Monitor precision/recall

**Phase 4: Multi-Hop (Week 5-6)**
- Add query decomposition
- Build tool dependency graph
- Enable adaptive retrieval

**Phase 5: Scale Testing (Week 7+)**
- Add 10+ more MCP servers
- Stress test with 500+ tools
- Optimize clustering, caching

## Expected Improvements

**With 2000 Tools:**
```
Current (Load All):
├─ TTFT: ~3-5 seconds (large context)
├─ Cost: $0.15 per request (200K tokens)
├─ Tool utilization: 2-5 tools (0.1-0.25%)
└─ Context dilution: High

With Tool Retrieval:
├─ TTFT: <1 second (<400ms overhead + fast context)
├─ Cost: $0.02 per request (15K tokens)
├─ Tool utilization: 8-12 tools (80-120% precision)
├─ Context dilution: Minimal
└─ Cost reduction: 87% 🎯
```

## Conclusion

This architecture enables **scaling to thousands of tools** while maintaining:
- **Sub-second TTFT** through efficient retrieval
- **High precision** through multi-stage reranking
- **Multi-hop planning** for complex queries
- **Continuous learning** from usage patterns
- **Massive cost reduction** (80%+) on context

The key insight: **Tools are documents**. Apply the same RAG techniques we use for knowledge retrieval, but to the tool selection problem itself.

