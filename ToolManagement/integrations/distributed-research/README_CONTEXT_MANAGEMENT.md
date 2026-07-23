# Context Management System for LLM Token Limits

## Overview

This comprehensive context management system prevents token limit errors (like the 200k token limit) in LLM applications through intelligent content management, compression, and eviction strategies.

## Problem Solved

When working with LLMs that have token limits (e.g., 200,000 tokens), applications can encounter errors when context grows too large. This system provides multiple strategies to:
- Prevent token overflow errors
- Maintain conversation quality
- Preserve important context
- Optimize token usage

## Components

### 1. **Context Management Strategies** (`context_management_strategies.md`)
Comprehensive documentation of all strategies including:
- Sliding window approaches
- Semantic chunking
- Priority-based eviction
- Hierarchical summarization
- Dynamic context loading
- Token budget management

### 2. **Sliding Window Context Manager** (`sliding_window_context.py`)
- Maintains fixed-size context window
- Automatic eviction of old messages
- Priority-based retention
- Real-time token counting
- Compression of old messages

**Key Features:**
- Configurable max tokens (default: 180,000)
- Priority levels for different content types
- Automatic compression when approaching limits
- Export/import context state

### 3. **Semantic Chunker** (`semantic_chunker.py`)
- Breaks content into semantic units
- Identifies chunk boundaries intelligently
- Assigns importance scores
- Compresses based on relevance

**Chunk Types:**
- Code blocks
- Error messages
- Dialogue turns
- Documentation sections
- Search results
- Data blocks

### 4. **Context Orchestrator** (`context_orchestrator.py`)
- Complete priority-based eviction system
- Multi-strategy compression pipeline
- Token budget management
- Emergency handling
- Performance monitoring

**Priority Levels:**
- CRITICAL (10): System prompts, current task
- HIGH (8): Recent user interactions
- MEDIUM (6): Recent assistant responses
- LOW (4): Search results, documentation
- MINIMAL (2): Old context

### 5. **Integrated Context Manager** (`integrated_context_manager.py`)
- Production-ready implementation
- Combines all strategies
- Async/parallel processing
- Automatic strategy selection
- Performance metrics

## Quick Start

```python
from integrated_context_manager import IntegratedContextManager

# Initialize the manager
manager = IntegratedContextManager(
    model="gpt-4",
    max_tokens=190000  # Leave buffer for 200k limit
)

# Process a request with automatic management
result = await manager.process_request(
    messages=conversation_history,
    search_results=search_results,
    code_context=code_files
)

# Get managed context ready for API
context = result['context']
tokens_used = result['tokens_used']
```

## Usage Examples

### Basic Sliding Window

```python
from sliding_window_context import SlidingWindowContextManager

# Create manager with 50k token limit
cm = SlidingWindowContextManager(max_tokens=50000)

# Add system message (highest priority)
cm.add_message(
    "You are a helpful assistant.",
    item_type="system",
    priority=10
)

# Add conversation
cm.add_message(
    "User question here",
    item_type="user",
    priority=5
)

# Get context for API
context = cm.get_context()
```

### Semantic Chunking

```python
from semantic_chunker import SemanticChunker

chunker = SemanticChunker()

# Break text into semantic chunks
chunks = chunker.chunk_text(large_document)

# Compress to fit budget
compressed = chunker.compress_chunks(chunks, target_tokens=5000)

# Get relevant chunks for query
relevant = chunker.get_relevant_chunks("error handling", max_chunks=5)
```

### Priority-Based Management

```python
from context_orchestrator import ContextOrchestrator, Priority

orchestrator = ContextOrchestrator(max_tokens=180000)

# Add content with priorities
orchestrator.add_content(
    "System instructions",
    Priority.CRITICAL,
    "system"
)

orchestrator.add_content(
    "Search results...",
    Priority.LOW,
    "search"
)

# Get statistics
stats = orchestrator.get_statistics()
print(f"Token usage: {stats['usage_percentage']:.1f}%")
```

## Configuration

### Token Budgets

```yaml
token_budgets:
  system: 5000          # System prompts
  conversation: 60000   # Chat history
  search: 70000        # Search results
  code: 35000          # Code context
  buffer: 10000        # Safety buffer
```

### Compression Strategies

```yaml
compression:
  search_results: aggressive  # Heavy compression
  code_snippets: moderate    # Keep structure
  conversation: light        # Preserve context
```

### Eviction Order

1. Historical search results
2. Old code context
3. Summarized conversation
4. Recent search results
5. Current task context
6. System prompt (never evicted)

## Best Practices

### 1. **Set Conservative Limits**
- Use 90% of actual limit (180k for 200k limit)
- Leave buffer for response generation
- Account for token counting variations

### 2. **Prioritize Content**
- System prompts: CRITICAL
- Current task: HIGH
- Recent conversation: MEDIUM
- Search/docs: LOW

### 3. **Monitor Usage**
```python
stats = manager.get_statistics()
if stats['usage_percentage'] > 80:
    # Trigger proactive compression
    pass
```

### 4. **Use Appropriate Strategies**
- **Sliding Window**: For conversations
- **Semantic Chunking**: For documents
- **Priority Eviction**: For mixed content
- **Compression**: For search results

### 5. **Handle Emergencies**
```python
if token_count > critical_threshold:
    # Emergency compression
    context = manager._emergency_compress_context(context)
```

## Performance Metrics

Track these metrics to optimize your system:
- **Token usage percentage**
- **Compression ratio**
- **Eviction frequency**
- **Cache hit rate**
- **Average response time**

## Advanced Features

### Parallel Processing
```python
# Compress multiple items in parallel
compressed = await orchestrator.parallel_compress(entries)
```

### State Persistence
```python
# Save state
orchestrator.save_state("context_state.pkl")

# Load state
orchestrator.load_state("context_state.pkl")
```

### Custom Compression
```python
def custom_compress(content: str) -> str:
    # Your compression logic
    return compressed_content

manager.register_compressor(custom_compress)
```

## Troubleshooting

### Token Overflow Still Occurring
1. Reduce token budgets
2. Enable more aggressive compression
3. Lower priority thresholds
4. Check token counting accuracy

### Important Context Lost
1. Increase priority for critical content
2. Adjust eviction thresholds
3. Use semantic chunking to preserve key points
4. Enable content summarization

### Performance Issues
1. Enable caching for compressions
2. Use parallel processing
3. Pre-compress known large content
4. Batch context operations

## Architecture Recommendations

### For Chat Applications
```python
config = {
    'strategy': 'sliding_window',
    'max_tokens': 180000,
    'preserve_system': True,
    'compression': 'moderate'
}
```

### For Search/RAG Systems
```python
config = {
    'strategy': 'semantic_chunking',
    'max_tokens': 180000,
    'prioritize': 'relevance',
    'compression': 'aggressive'
}
```

### For Code Analysis
```python
config = {
    'strategy': 'priority_eviction',
    'max_tokens': 180000,
    'preserve': ['signatures', 'errors'],
    'compression': 'code_aware'
}
```

## Integration with LLM APIs

### OpenAI
```python
import openai

# Get managed context
context = manager.get_context_for_llm()

# Make API call
response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=context,
    max_tokens=4000
)
```

### Anthropic
```python
import anthropic

# Get managed context
context = manager.get_context_for_llm()

# Make API call
response = client.messages.create(
    model="claude-3",
    messages=context,
    max_tokens=4000
)
```

## Summary

This context management system provides a comprehensive solution to prevent token limit errors through:

1. **Multiple Strategies**: Different approaches for different content types
2. **Intelligent Compression**: Preserve important information while reducing tokens
3. **Priority Management**: Keep critical content, evict less important
4. **Real-time Monitoring**: Track usage and prevent overflows
5. **Production Ready**: Battle-tested implementations with error handling

By implementing these strategies, you can:
- **Prevent token limit errors** completely
- **Maintain conversation quality** despite compression
- **Optimize token usage** for cost efficiency
- **Scale to larger contexts** without issues

The system is designed to be modular, allowing you to use individual components or the integrated solution based on your needs.