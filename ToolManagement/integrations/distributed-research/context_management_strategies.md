# Context Management Strategies for Token Limit Prevention

## Overview
This document outlines comprehensive strategies to manage context and prevent token limit errors (like the 200k token limit error) in LLM applications.

## Core Strategies

### 1. Sliding Window Approach
**Description:** Maintain a fixed-size window of recent context, automatically evicting oldest content.

```python
class SlidingWindowContext:
    def __init__(self, max_tokens=180000):  # Leave 20k buffer
        self.max_tokens = max_tokens
        self.messages = []
        self.token_counts = []

    def add_message(self, message, token_count):
        self.messages.append(message)
        self.token_counts.append(token_count)

        # Evict oldest messages if over limit
        while sum(self.token_counts) > self.max_tokens:
            self.messages.pop(0)
            self.token_counts.pop(0)

    def get_context(self):
        return self.messages
```

### 2. Semantic Chunking and Compression
**Description:** Break content into semantic units and compress less relevant portions.

```python
class SemanticChunker:
    def __init__(self):
        self.importance_threshold = 0.7

    def chunk_and_compress(self, text):
        chunks = self.create_semantic_chunks(text)
        compressed_chunks = []

        for chunk in chunks:
            importance = self.calculate_importance(chunk)
            if importance > self.importance_threshold:
                compressed_chunks.append(chunk)
            else:
                # Summarize less important chunks
                compressed_chunks.append(self.summarize(chunk))

        return compressed_chunks

    def calculate_importance(self, chunk):
        # Factors: recency, keyword density, user focus
        return importance_score
```

### 3. Priority-Based Eviction System
**Description:** Assign priorities to different context types and evict based on importance.

```python
class PriorityContextManager:
    def __init__(self):
        self.priority_levels = {
            'system_prompt': 10,      # Never evict
            'current_task': 9,        # Keep as long as possible
            'recent_interaction': 7,  # Important but can compress
            'search_results': 5,      # Compress aggressively
            'historical_context': 3   # First to evict
        }
        self.context_items = []

    def evict_low_priority(self, needed_tokens):
        # Sort by priority
        sorted_items = sorted(self.context_items,
                            key=lambda x: x['priority'])

        freed_tokens = 0
        for item in sorted_items:
            if freed_tokens >= needed_tokens:
                break
            if item['priority'] < 7:  # Threshold for eviction
                self.context_items.remove(item)
                freed_tokens += item['token_count']
```

### 4. Hierarchical Summarization
**Description:** Create multi-level summaries of content for different detail levels.

```python
class HierarchicalSummarizer:
    def __init__(self):
        self.summary_levels = {
            'full': 1.0,      # 100% of content
            'detailed': 0.5,  # 50% compression
            'standard': 0.3,  # 30% of original
            'brief': 0.1      # 10% of original
        }

    def create_summaries(self, content):
        summaries = {}
        for level, ratio in self.summary_levels.items():
            target_length = int(len(content) * ratio)
            summaries[level] = self.summarize_to_length(
                content, target_length
            )
        return summaries

    def get_appropriate_summary(self, available_tokens):
        # Return the most detailed summary that fits
        pass
```

### 5. Dynamic Context Loading
**Description:** Load relevant context on-demand based on current task.

```python
class DynamicContextLoader:
    def __init__(self):
        self.context_store = {}  # External storage
        self.loaded_context = []
        self.relevance_threshold = 0.6

    def load_relevant_context(self, query):
        relevant_items = []

        for key, context in self.context_store.items():
            relevance = self.calculate_relevance(query, context)
            if relevance > self.relevance_threshold:
                relevant_items.append({
                    'context': context,
                    'relevance': relevance
                })

        # Sort by relevance and load top items
        relevant_items.sort(key=lambda x: x['relevance'],
                           reverse=True)
        return relevant_items[:self.max_items]
```

### 6. Token Budget Management
**Description:** Allocate token budgets to different context categories.

```python
class TokenBudgetManager:
    def __init__(self, total_budget=180000):
        self.total_budget = total_budget
        self.allocations = {
            'system': 5000,
            'conversation': 50000,
            'search_results': 80000,
            'code_context': 30000,
            'buffer': 15000
        }

    def check_budget(self, category, tokens_needed):
        return tokens_needed <= self.allocations[category]

    def reallocate(self, from_category, to_category, amount):
        if self.allocations[from_category] >= amount:
            self.allocations[from_category] -= amount
            self.allocations[to_category] += amount
```

## Implementation Guidelines

### Phase 1: Immediate Solutions
1. **Token Counting**: Implement accurate token counting before adding to context
2. **Hard Limits**: Set maximum context at 180k tokens (90% of limit)
3. **Truncation**: Implement aggressive truncation for search results

### Phase 2: Smart Compression
1. **Summarization Pipeline**: Use smaller models to summarize large content
2. **Deduplication**: Remove duplicate information across messages
3. **Format Optimization**: Remove unnecessary formatting and whitespace

### Phase 3: Advanced Management
1. **Context Database**: Store full context externally with retrieval
2. **Semantic Search**: Load only relevant portions based on current task
3. **Incremental Updates**: Update context incrementally rather than full replacement

## Specific Solutions for Search Results

### Search Result Compression
```python
def compress_search_results(results, max_tokens=50000):
    """Compress search results to fit within token budget"""
    compressed = []
    current_tokens = 0

    for result in results:
        # Extract key information only
        summary = {
            'url': result['url'],
            'title': result['title'],
            'key_points': extract_key_sentences(result['excerpts'], 3),
            'relevance': result.get('relevance_score', 0)
        }

        tokens = count_tokens(summary)
        if current_tokens + tokens > max_tokens:
            break

        compressed.append(summary)
        current_tokens += tokens

    return compressed
```

### Parallel Processing with Context Limits
```python
async def parallel_search_with_limits(queries, token_limit=40000):
    """Process searches in parallel with token limits"""
    tasks = []
    token_budget_per_query = token_limit // len(queries)

    for query in queries:
        task = search_with_limit(query, token_budget_per_query)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    return combine_and_compress(results, token_limit)
```

## Monitoring and Alerts

### Token Usage Monitor
```python
class TokenMonitor:
    def __init__(self, warning_threshold=0.8, critical_threshold=0.95):
        self.warning = warning_threshold
        self.critical = critical_threshold
        self.max_tokens = 200000

    def check_usage(self, current_tokens):
        ratio = current_tokens / self.max_tokens

        if ratio > self.critical:
            raise TokenLimitError("Critical token limit reached")
        elif ratio > self.warning:
            return "WARNING: Approaching token limit"
        return "OK"
```

## Best Practices

1. **Always Count First**: Count tokens before adding to context
2. **Prioritize Recent**: Keep recent interactions, compress older ones
3. **Summarize Early**: Summarize content as soon as it's not immediately needed
4. **Cache Wisely**: Cache summaries, not full content
5. **Monitor Continuously**: Track token usage throughout the conversation
6. **Fail Gracefully**: Have fallback strategies when limits are approached

## Emergency Recovery

When token limit is reached:
1. Immediately dump non-essential context
2. Summarize current conversation to key points
3. Preserve only critical system prompts and current task
4. Notify user of context reset if necessary
5. Resume with minimal context

## Configuration Example

```yaml
context_management:
  max_tokens: 180000
  warning_threshold: 144000  # 80%
  critical_threshold: 171000  # 95%

  budgets:
    system: 5000
    conversation: 50000
    search: 80000
    code: 30000
    buffer: 15000

  compression:
    search_results: aggressive
    code_snippets: moderate
    conversation: light

  eviction_order:
    - historical_search_results
    - old_code_context
    - summarized_conversation
    - recent_search_results
    - current_task
    - system_prompt  # Never evict
```

This comprehensive strategy ensures that token limit errors are prevented through proactive management, intelligent compression, and dynamic loading of relevant context.