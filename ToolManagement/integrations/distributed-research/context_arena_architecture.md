# Context Arena Architecture

## Overview

A sophisticated context management system that separates tool outputs into isolated "arenas" while keeping the main conversation window clean. Agents acquire context through multi-hop RAG retrieval rather than direct injection.

## Core Concept

```
┌─────────────────────────────────────────────────────────────┐
│                     Main Conversation                        │
│  [Clean, focused context - only essential messages]          │
└─────────────────────────────────────────────────────────────┘
                             ↑
                    [RAG Extraction]
                             ↑
┌─────────────────────────────────────────────────────────────┐
│                      Context Arenas                          │
├──────────────┬──────────────┬──────────────┬────────────────┤
│ Search Arena │  Code Arena  │  Docs Arena  │ Custom Arenas  │
│              │              │              │                │
│ • Web results│ • File reads │ • API docs   │ • Database     │
│ • RAG chunks │ • Git diffs  │ • Tutorials  │ • Logs         │
│ • Summaries  │ • AST data   │ • Examples   │ • Metrics      │
└──────────────┴──────────────┴──────────────┴────────────────┘
```

## Key Principles

### 1. **Isolation**
- Tool outputs NEVER directly enter main conversation
- Each arena is independently managed
- Arenas can be cleared without affecting conversation

### 2. **Lazy Loading**
- Content enters arenas but isn't in active context
- Only retrieved when specifically needed
- Multi-hop reasoning to find relevant pieces

### 3. **Clean Conversation**
- Main context only contains:
  - System prompt
  - User messages
  - Agent responses
  - Extracted insights (not raw data)

### 4. **Multi-hop RAG**
- First hop: Identify which arena(s) to search
- Second hop: Find relevant documents/chunks
- Third hop: Extract specific information
- Fourth hop: Synthesize and return

## Arena Types

### Search Arena
```yaml
type: search_arena
capacity: unlimited
indexing: vector_embeddings
retention: 30_minutes
access_pattern: semantic_search
compression: aggressive
```

### Code Arena
```yaml
type: code_arena
capacity: 100_files
indexing: ast_and_embeddings
retention: session
access_pattern: structural_search
compression: moderate
```

### Documentation Arena
```yaml
type: documentation_arena
capacity: unlimited
indexing: hierarchical
retention: persistent
access_pattern: multi_hop_qa
compression: none
```

### Conversation Memory Arena
```yaml
type: memory_arena
capacity: last_100_turns
indexing: temporal_and_semantic
retention: session
access_pattern: similarity_and_recency
compression: light
```

## Multi-hop RAG Process

### Step 1: Query Understanding
```python
query = "What was the error in the search results about authentication?"
intent = extract_intent(query)  # -> {arena: "search", topic: "authentication error"}
```

### Step 2: Arena Selection
```python
relevant_arenas = select_arenas(intent)  # -> ["search_arena", "code_arena"]
```

### Step 3: Document Retrieval
```python
candidates = []
for arena in relevant_arenas:
    docs = arena.retrieve_relevant(intent, k=10)
    candidates.extend(docs)
```

### Step 4: Refinement
```python
refined = rerank_and_filter(candidates, query)
relevant_chunks = extract_passages(refined, max_tokens=2000)
```

### Step 5: Synthesis
```python
answer = synthesize_response(relevant_chunks, query)
return answer  # Only this goes to main context
```

## Implementation Architecture

### Arena Manager
- Creates and manages arenas
- Routes tool outputs to correct arena
- Handles arena lifecycle (creation, clearing, deletion)
- Monitors arena statistics

### RAG Extractor
- Vector embedding generation
- Similarity search
- Reranking algorithms
- Chunk extraction
- Multi-hop reasoning

### Agent Interface
- Query formulation
- Arena selection strategy
- Result synthesis
- Context injection (only extracted results)

### Conversation Protector
- Enforces isolation
- Validates context additions
- Monitors token usage
- Prevents pollution

## Benefits

1. **Unlimited Tool Usage**: Tools can generate massive outputs without affecting main context
2. **Selective Retrieval**: Only relevant information enters conversation
3. **Clean History**: Conversation remains readable and focused
4. **Parallel Processing**: Multiple agents can work in different arenas simultaneously
5. **Efficient Token Usage**: Main context uses minimal tokens
6. **Better Reasoning**: Agents must think about what they need before retrieving

## Example Flow

```python
# User asks a question
user: "Fix the authentication bug in the login system"

# Agent workflow:
1. Search Arena gets populated with web results about auth bugs
2. Code Arena gets populated with login system files
3. Agent queries: "login authentication implementation"
4. RAG extracts: specific login function + relevant bug fix patterns
5. Only the synthesis enters main context:
   "I found the issue in login.py line 45 where token validation is missing"

# Main context stays clean - no raw search results or full file dumps
```

## Advanced Features

### Arena Chaining
```python
# Results from one arena inform queries to another
search_insights = search_arena.extract("OAuth2 best practices")
code_issues = code_arena.extract(f"problems related to: {search_insights}")
```

### Cross-Arena Reasoning
```python
# Compare information across arenas
docs_spec = docs_arena.extract("API specification")
code_impl = code_arena.extract("API implementation")
discrepancies = compare(docs_spec, code_impl)
```

### Temporal Queries
```python
# Access historical information
past_errors = memory_arena.extract("errors from last hour")
current_code = code_arena.extract("recently modified files")
correlation = analyze(past_errors, current_code)
```

### Arena Persistence
```python
# Save important arenas for future sessions
search_arena.persist("auth_research_session")
# Later...
search_arena.load("auth_research_session")
```

## Configuration

```yaml
context_arena_config:
  max_conversation_tokens: 50000
  max_extraction_tokens: 2000

  arenas:
    - name: search
      type: search_arena
      max_documents: 1000
      embedding_model: text-embedding-ada-002

    - name: code
      type: code_arena
      max_files: 100
      index_strategy: hybrid_ast_semantic

    - name: docs
      type: documentation_arena
      persistent: true
      index_strategy: hierarchical

  extraction:
    strategy: multi_hop
    max_hops: 4
    reranking: true
    synthesis: true

  protection:
    enforce_isolation: true
    max_direct_injection: 0
    audit_all_additions: true
```

## Comparison with Traditional Approach

### Traditional (Context Pollution)
```
Conversation Context:
- System prompt (500 tokens)
- User message (100 tokens)
- Search results (50,000 tokens) ← Pollution!
- File contents (30,000 tokens) ← Pollution!
- Assistant response (500 tokens)
Total: 81,100 tokens ❌
```

### Arena Architecture
```
Conversation Context:
- System prompt (500 tokens)
- User message (100 tokens)
- Extracted insights (500 tokens) ← Clean!
- Assistant response (500 tokens)
Total: 1,600 tokens ✅

Arenas (isolated):
- Search Arena: 50,000 tokens
- Code Arena: 30,000 tokens
(Not counted in conversation context)
```

## Implementation Priority

1. **Phase 1**: Basic arena creation and isolation
2. **Phase 2**: Simple RAG extraction (single-hop)
3. **Phase 3**: Multi-hop reasoning
4. **Phase 4**: Cross-arena intelligence
5. **Phase 5**: Advanced features (persistence, chaining)

This architecture provides true separation of concerns - keeping conversation context clean while maintaining access to vast amounts of information through intelligent retrieval.