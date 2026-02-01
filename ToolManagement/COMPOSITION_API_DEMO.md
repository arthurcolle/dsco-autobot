# Compositional Workflow API - Demonstration

The Composition API enables sophisticated multi-step workflows with registered Pydantic schemas and different execution modes.

## 🚀 Quick Start

Base URL: `http://localhost:2016/api/v1/compose`

## 📋 API Endpoints

### 1. Schema Registry

Register Pydantic schemas as reusable data contracts:

```bash
# Register a schema
POST /api/v1/compose/schemas
{
  "name": "TextSegment",
  "description": "A segment of text with metadata",
  "schema": {
    "type": "object",
    "properties": {
      "text": {"type": "string"},
      "index": {"type": "integer"},
      "category": {"type": "string"},
      "confidence": {"type": "number"}
    },
    "required": ["text", "index"]
  },
  "tags": ["text", "nlp"]
}

# List all schemas
GET /api/v1/compose/schemas

# Get schema by name
GET /api/v1/compose/schemas/by-name/TextSegment
```

### 2. Workflow Composition

Create multi-step workflows with typed data flow:

```bash
# Create a workflow
POST /api/v1/compose/workflows
{
  "name": "document_analysis",
  "description": "Segment → Classify → Embed → Compare",
  "steps": [
    {
      "name": "segment",
      "mode": "segment",
      "output_schema": "TextSegment"
    },
    {
      "name": "classify",
      "mode": "map",
      "input_from": "segment",
      "tool": "classifier"
    },
    {
      "name": "embed",
      "mode": "map",
      "input_from": "segment",
      "tool": "embedder"
    },
    {
      "name": "compare",
      "mode": "compare",
      "input_from": "embed"
    }
  ]
}

# List workflows
GET /api/v1/compose/workflows

# Execute a workflow
POST /api/v1/compose/execute
{
  "workflow_id": "<workflow-id>",
  "inputs": {
    "text": "Your document text here..."
  }
}
```

## 🔧 Execution Modes

The API supports multiple execution modes for different data flow patterns:

### PASSTHROUGH
Direct pass-through: `output[i] → input[i+1]`
```json
{
  "mode": "passthrough",
  "tool": "validate"
}
```

### TRANSFORM
LLM-mediated transformation: `text → structured output`
```json
{
  "mode": "transform",
  "transform_config": {
    "llm_provider": "anthropic",
    "model": "claude-3-sonnet-20240229",
    "output_schema": "StructuredData",
    "temperature": 0.0
  }
}
```

### MAP
Apply operation to each element: `[a,b,c] → [f(a), f(b), f(c)]`
```json
{
  "mode": "map",
  "tool": "classifier"
}
```

### SEGMENT
Split input into parts: `text → [segment1, segment2, ...]`
```json
{
  "mode": "segment",
  "tool": "text_splitter"
}
```

### CLASSIFY
Classify each element: `element → classification`
```json
{
  "mode": "classify",
  "tool": "classifier",
  "output_schema": "Classification"
}
```

### EMBED
Generate embeddings: `text → vector`
```json
{
  "mode": "embed",
  "tool": "embedder"
}
```

### COMPARE
Compare pairs: `[v1, v2, ...] → [(v1,v2,sim), ...]`
```json
{
  "mode": "compare",
  "tool": "similarity"
}
```

### REDUCE
Aggregate results: `[r1, r2, ...] → aggregated`
```json
{
  "mode": "reduce",
  "tool": "aggregator"
}
```

## 💡 Example Workflows

### 1. Document Analysis Pipeline

```json
{
  "name": "document_analysis",
  "description": "Complete document analysis: segment → classify → embed → compare",
  "steps": [
    {
      "name": "segment",
      "mode": "segment",
      "description": "Split document into semantic segments",
      "output_schema": "TextSegment"
    },
    {
      "name": "classify",
      "mode": "map",
      "input_from": "segment",
      "tool": "text_classifier",
      "description": "Classify each segment by topic"
    },
    {
      "name": "embed",
      "mode": "map",
      "input_from": "segment",
      "tool": "text_embedder",
      "description": "Generate embeddings for each segment"
    },
    {
      "name": "compare",
      "mode": "compare",
      "input_from": "embed",
      "description": "Find similar segments"
    }
  ]
}
```

### 2. Text to Structured Data (LLM Transform)

```json
{
  "name": "extract_structured_data",
  "description": "Use LLM to convert text to structured Pydantic output",
  "steps": [
    {
      "name": "extract",
      "mode": "transform",
      "transform_config": {
        "llm_provider": "anthropic",
        "model": "claude-3-sonnet-20240229",
        "prompt_template": "Extract contact information from the following text:\n\n{text}\n\nReturn as JSON.",
        "output_schema": "ContactInfo",
        "temperature": 0.0,
        "max_tokens": 2000
      }
    },
    {
      "name": "validate",
      "mode": "passthrough",
      "tool": "validator",
      "input_schema": "ContactInfo"
    }
  ]
}
```

### 3. Multi-Document Comparison

```json
{
  "name": "document_comparison",
  "description": "Compare multiple documents semantically",
  "steps": [
    {
      "name": "embed_all",
      "mode": "map",
      "tool": "document_embedder",
      "description": "Embed each document"
    },
    {
      "name": "compare_pairs",
      "mode": "compare",
      "input_from": "embed_all",
      "description": "Compare all pairs"
    },
    {
      "name": "rank_similarities",
      "mode": "reduce",
      "input_from": "compare_pairs",
      "tool": "similarity_ranker"
    }
  ]
}
```

## 🎯 Key Features

### 1. Registered Schemas as Data Contracts
- Define Pydantic schemas once, reuse everywhere
- Type-safe data flow between workflow steps
- JSON Schema validation

### 2. LLM-Mediated Transformations
- Convert unstructured text → structured Pydantic outputs
- Configurable LLM providers (Anthropic, OpenAI, etc.)
- Templated prompts with schema enforcement

### 3. Compositional Patterns
- Chain tools with different execution modes
- Map/Reduce patterns for parallel processing
- Conditional branching and data merging

### 4. Meta-API Capabilities
- Tools calling tools in complex workflows
- Automatic input/output mapping
- Schema-driven validation at each step

## 🧪 Testing

```bash
# Get example workflows
curl http://localhost:2016/api/v1/compose/examples | jq '.'

# Test schema registration
curl -X POST http://localhost:2016/api/v1/compose/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "name": "TestSchema",
    "schema": {"type": "object", "properties": {"value": {"type": "string"}}}
  }'

# Test workflow creation
curl -X POST http://localhost:2016/api/v1/compose/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_workflow",
    "steps": [{"name": "step1", "mode": "passthrough"}]
  }'
```

## 📊 Execution Flow

```
Input Data
    ↓
Step 1 (SEGMENT) → [seg1, seg2, seg3]
    ↓
Step 2 (MAP + CLASSIFY) → [class1, class2, class3]
    ↓
Step 3 (MAP + EMBED) → [vec1, vec2, vec3]
    ↓
Step 4 (COMPARE) → [(0,1,0.8), (0,2,0.6), (1,2,0.7)]
    ↓
Output: Similarity Matrix
```

## 🔗 Integration with Tool Registry

The Composition API integrates seamlessly with the Tool Registry:

- **Tools**: Reference any registered tool by name
- **Schemas**: Use registered Pydantic schemas for type safety
- **Services**: Leverage native MCP services (Search, Research, Publish)
- **Queue**: Async execution through the task queue

## 🚧 Future Enhancements

- [ ] Real LLM integration (Anthropic, OpenAI)
- [ ] Conditional branching based on data
- [ ] Parallel execution optimization
- [ ] Streaming results
- [ ] Workflow versioning
- [ ] Checkpoint/resume for long-running workflows
- [ ] Visual workflow editor
- [ ] Workflow marketplace

---

**Compositional Workflows = Meta-API Power** 🚀

Use schemas, tools, and execution modes to build complex data processing pipelines!
