# Workflow Library - Pre-Built AI/NLP Patterns with Token Limits

The Workflow Library provides **11 pre-built, production-ready workflows** with token limits and cost controls already configured.

## 🎯 Quick Start

```bash
# List all workflows
curl http://localhost:2016/api/v1/compose/library | jq '.workflows[] | {id, name, category}'

# Get workflow details
curl http://localhost:2016/api/v1/compose/library/named_entity_extraction | jq '.'

# Execute a named workflow
curl -X POST http://localhost:2016/api/v1/compose/execute-named \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "named_entity_extraction",
    "inputs": {"text": "Apple Inc. was founded by Steve Jobs in California."}
  }'
```

## 📚 Complete Workflow Catalog

### 🔤 NLP Workflows

#### 1. Named Entity Extraction
**ID**: `named_entity_extraction`
**Category**: NLP
**Token Limit**: 200,000
**Cost Limit**: $2.00

Extracts named entities (people, places, organizations) from text.

**Stages**:
1. **Segment** (100K tokens) - Split text into processable chunks
2. **Extract Entities** (50K tokens, $1.00) - NER extraction
3. **Deduplicate** (10K tokens) - Remove duplicate entities

**Example**:
```json
{
  "workflow_id": "named_entity_extraction",
  "inputs": {
    "text": "Apple Inc. was founded by Steve Jobs in Cupertino, California on April 1, 1976."
  }
}
```

**Expected Output**:
```json
{
  "entities": [
    {"text": "Apple Inc.", "type": "ORG"},
    {"text": "Steve Jobs", "type": "PERSON"},
    {"text": "Cupertino", "type": "GPE"},
    {"text": "California", "type": "GPE"},
    {"text": "April 1, 1976", "type": "DATE"}
  ]
}
```

---

#### 2. Intelligent Text Segmentation
**ID**: `text_segmentation`
**Token Limit**: 200,000

Splits text into semantic segments with overlap and metadata.

**Stages**:
1. **Preprocess** (50K tokens) - Clean text
2. **Segment** (100K tokens, $0.50) - Semantic segmentation
3. **Add Metadata** (20K tokens) - Annotate segments

**Use Cases**:
- Chunking for RAG systems
- Document preprocessing for embeddings
- Context-aware splitting

---

### 📊 Analysis Workflows

#### 3. Multi-Level Sentiment Analysis
**ID**: `sentiment_analysis`
**Token Limit**: 300,000
**Cost Limit**: $1.50

Analyzes sentiment at document, paragraph, and sentence levels.

**Stages**:
1. **Segment by Paragraph** (50K tokens)
2. **Sentence Split** (100K tokens)
3. **Analyze Sentences** (150K tokens, $1.00, 20 req/sec)
4. **Aggregate Paragraph** (10K tokens)
5. **Aggregate Document** (5K tokens)

---

#### 4. Document Similarity Comparison
**ID**: `document_similarity`
**Token Limit**: 2,000,000
**Cost Limit**: $3.00

Segments → Embeds → Compares all pairs for similarity.

**Stages**:
1. **Segment All** (500K tokens)
2. **Embed Segments** (1M tokens, $2.00, 50 req/sec)
3. **Compute Similarity** (100K tokens, 1GB RAM)
4. **Cluster Similar** - Hierarchical clustering

**Use Cases**:
- Finding duplicate documents
- Content clustering
- Plagiarism detection

---

#### 5. Topic Modeling
**ID**: `topic_modeling`
**Token Limit**: 2,000,000
**Cost Limit**: $1.50

Extracts topics from document collections using LDA.

**Stages**:
1. **Preprocess** (200K tokens)
2. **Vectorize** (500K tokens, 2GB RAM)
3. **Extract Topics** (1M tokens, 5 min timeout, 4GB RAM)
4. **Label Topics** (50K tokens, $1.00)

---

### 🔍 Extraction Workflows

#### 6. Structured Data Extraction
**ID**: `structured_data_extraction`
**Token Limit**: 400,000
**Cost Limit**: $6.00

Uses LLM to extract structured data with schema validation.

**Stages**:
1. **Chunk Text** (100K tokens)
2. **LLM Extract** (200K output tokens, $5.00, 5 req/sec)
3. **Validate** (10K tokens) - Schema validation
4. **Merge Results** (20K tokens) - Deduplication

**Example**:
```json
{
  "workflow_id": "structured_data_extraction",
  "inputs": {
    "text": "Contact: John Doe (john@example.com), Phone: 555-1234",
    "schema": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "email": {"type": "string"},
        "phone": {"type": "string"}
      }
    }
  }
}
```

---

#### 7. Table Extraction
**ID**: `table_extraction`
**Token Limit**: 200,000

Extracts tables and structured content from documents.

**Stages**:
1. **Detect Structure** (50K tokens)
2. **Extract Tables** (100K tokens, $0.50)
3. **Parse to DataFrame** (50K tokens) - Convert to pandas

---

### 🔎 Search Workflows

#### 8. Semantic Search with Reranking
**ID**: `semantic_search`
**Token Limit**: 200,000
**Cost Limit**: $1.00

Embeds query → Searches → Reranks → Extracts snippets.

**Stages**:
1. **Embed Query** (1K tokens, $0.05)
2. **Vector Search** (top 100, 10 req/sec)
3. **Rerank** (100K tokens, $0.50, 30s timeout) - Top 10
4. **Extract Snippets** (20K tokens) - 200 chars each

**Use Cases**:
- RAG systems
- Document search
- Q&A systems

---

### ✨ Generation Workflows

#### 9. Multi-Level Summarization Cascade
**ID**: `summarization_cascade`
**Token Limit**: 800,000
**Cost Limit**: $4.00

Segments → Summarizes each → Merges → Final summary.

**Stages**:
1. **Segment** (100K tokens, 2000 tokens/segment)
2. **Summarize Segments** (500K tokens, 100K output, $2.00, 10 req/sec)
3. **Merge Summaries** (50K tokens)
4. **Final Summary** (50K tokens, 10K output, $1.00)

**Models Used**:
- Segment summaries: `claude-3-haiku-20240307` (fast, cheap)
- Final summary: `claude-3-sonnet-20240229` (high quality)

---

#### 10. Q&A Pair Generation
**ID**: `qa_generation`
**Token Limit**: 1,500,000
**Cost Limit**: $7.00

Generates question-answer pairs for training/testing.

**Stages**:
1. **Segment** (100K tokens) - By topic
2. **Generate Questions** (500K tokens, 100K output, $3.00) - 3 per segment
3. **Generate Answers** (500K tokens, 100K output, $3.00)
4. **Validate Q&A** (100K tokens) - Check answerability

---

### 🔄 Transformation Workflows

#### 11. Text to Knowledge Graph
**ID**: `text_to_knowledge_graph`
**Token Limit**: 800,000
**Cost Limit**: $4.00

Extracts entities and relations, builds knowledge graph.

**Stages**:
1. **Extract Entities** (200K tokens, $1.00)
2. **Extract Relations** (300K tokens, $2.00, 5 req/sec)
3. **Resolve Entities** (100K tokens, $0.50) - Link to knowledge base
4. **Build Graph** (60s timeout, 2GB RAM) - NetworkX format

**Output**: NetworkX graph with nodes (entities) and edges (relations)

---

## 💰 Cost Management

### Per-Stage Token Limits

Each stage can have:
- **max_tokens**: Maximum tokens to process
- **max_output_tokens**: Maximum output tokens
- **max_cost_cents**: Maximum cost in cents
- **max_duration_seconds**: Timeout
- **rate_limit_per_second**: Max requests/sec
- **max_memory_mb**: Memory limit

### Global Workflow Limits

- **total_max_tokens**: Total tokens across all stages
- **total_max_cost_cents**: Total cost budget
- **total_max_duration_seconds**: Overall timeout

### Example with Limits

```json
{
  "workflow_id": "named_entity_extraction",
  "inputs": {"text": "..."},
  "override_limits": {
    "extract_entities": {
      "max_tokens": 25000,
      "max_cost_cents": 50
    }
  }
}
```

---

## 📊 Resource Usage Tracking

Execution response includes resource usage:

```json
{
  "execution_id": "uuid",
  "workflow_id": "named_entity_extraction",
  "status": "completed",
  "result": {...},
  "resource_usage": {
    "total_tokens": 45230,
    "total_cost_cents": 45,
    "stages": {
      "segment": {
        "duration_seconds": 2.3,
        "tokens_used": 15000,
        "cost_cents": 0
      },
      "extract_entities": {
        "duration_seconds": 8.7,
        "tokens_used": 28000,
        "cost_cents": 42
      },
      "deduplicate": {
        "duration_seconds": 0.5,
        "tokens_used": 2230,
        "cost_cents": 3
      }
    }
  }
}
```

---

## 🎯 API Endpoints

```bash
# List all workflows
GET /api/v1/compose/library

# Get workflow by ID
GET /api/v1/compose/library/{workflow_id}

# Get workflows by category
GET /api/v1/compose/library/category/nlp

# Get workflows by tag
GET /api/v1/compose/library/tag/extraction

# Execute named workflow
POST /api/v1/compose/execute-named
{
  "workflow_id": "...",
  "inputs": {...},
  "override_limits": {...}  # Optional
}
```

---

## 🏷️ Categories & Tags

### Categories
- `nlp` - Natural language processing
- `extraction` - Data extraction
- `analysis` - Analysis and insights
- `transformation` - Data transformation
- `search` - Search and retrieval
- `generation` - Content generation

### Common Tags
- `segmentation`, `chunking`
- `entities`, `ner`
- `sentiment`, `classification`
- `embeddings`, `similarity`
- `summarization`, `qa`
- `knowledge_graph`, `relations`
- `llm`, `structured`

---

## 🚀 Best Practices

### 1. Choose the Right Workflow
- **NER**: Use `named_entity_extraction` for people, places, orgs
- **Chunking**: Use `text_segmentation` for RAG preprocessing
- **Search**: Use `semantic_search` for semantic retrieval
- **Extraction**: Use `structured_data_extraction` for custom schemas

### 2. Set Appropriate Limits
- Start with default limits
- Monitor `resource_usage` in responses
- Adjust limits based on actual usage
- Use `override_limits` for fine-tuning

### 3. Handle Errors
- Workflows fail if stage limits are exceeded
- Check `resource_usage` to identify bottlenecks
- Retry failed workflows with higher limits

### 4. Cost Optimization
- Use cheaper models (Haiku) for intermediate steps
- Use better models (Sonnet/Opus) for final output
- Set conservative token limits to prevent runaway costs
- Monitor `total_cost_cents` in responses

---

## 📈 Performance Tips

### Rate Limiting
```json
{
  "limits": {
    "rate_limit_per_second": 10,
    "rate_limit_per_minute": 100
  }
}
```

### Parallel Execution
Stages without dependencies run in parallel automatically.

### Retries
```json
{
  "stages": [{
    "retry_on_failure": true,
    "max_retries": 3
  }]
}
```

---

## 🎊 Summary

**11 Production-Ready Workflows** with:
✅ Token limits per stage
✅ Cost controls ($0.05 - $7.00)
✅ Rate limiting
✅ Automatic resource tracking
✅ Retry policies
✅ Memory limits

**Just provide**: `workflow_id` + `inputs`
**Get back**: Results + resource usage + cost breakdown

No need to configure stages, limits, or dependencies - it's all pre-built! 🚀
