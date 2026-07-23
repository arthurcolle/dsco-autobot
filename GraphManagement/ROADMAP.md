# GraphManagement Implementation Roadmap

## Architecture Boundaries

### ToolManagement Repo (Separate)
- `@tools.endpoint` decorator for FastAPI endpoints
- Tool action types (HTTP, Adhoc, Snippet)
- Service discovery & registry
- Multi-hop retrieval planner
- AssistSpec/AssistEngine (sidecar operations)
- Tool execution orchestration

### GraphManagement Repo (This Repo)
- `@graph.object` decorator for Pydantic models
- `BasedModel` ORM layer
- Graph client with embeddings & vector search
- Conversation graph service
- Memory overlay with salience
- Schema management & constraints
- Relationship modeling & auto-linking

---

## Phase 1: Core Graph Object System ⭐ PRIORITY

### 1.1 Enhanced GraphClient
**Location:** `src/graph_management/graph_client/enhanced_client.py`

**Features:**
- `merge_node()` with vector support
- `merge_rel()` with relationship properties
- `vector_search()` for k-NN similarity
- `traverse()` for graph navigation
- Connection pooling & retry logic

**API Extensions:**
```python
class GraphClient:
    def merge_node(
        self,
        label: str,
        key: Dict[str, Any],
        props: Dict[str, Any] = None,
        vectors: Dict[str, List[float]] = None
    ) -> Dict[str, Any]: ...

    def vector_search(
        self,
        label: str,
        space: str,
        vector: List[float],
        k: int,
        threshold: float,
        similarity: str = "cosine"
    ) -> List[Dict[str, Any]]: ...
```

### 1.2 Overlay Specifications
**Location:** `src/graph_management/overlay/specs.py`

**Models:**
- `EmbeddingSpec` - Multi-space embeddings with auto-linking
- `RelationSpec` - Declarative relationship wiring
- `FieldSpec` - Structured field metadata
- `CalculatedFieldSpec` - Computed properties
- `MemoryPolicies` - Retention, versioning, salience

**Example:**
```python
@dataclass
class EmbeddingSpec:
    name: str
    provider: str  # "openai", "fastembed", "local"
    model: str
    dim: int
    source: EmbeddingSource
    autolink: Optional[AutoLinkSimilar] = None
```

### 1.3 @graph.object Decorator
**Location:** `src/graph_management/overlay/decorator.py`

**Capabilities:**
- Auto-generate schemas from Pydantic models
- Multi-space embedding computation
- Relationship materialization (static & dynamic)
- Calculated field evaluation
- Lifecycle hooks (create/update)
- Policy enforcement

**Usage:**
```python
from graph_management.overlay import object as graph_object

@graph_object(
    label="MemoryItem",
    key=["id"],
    service_id="graph-svc",
    embeddings=[
        EmbeddingSpec(
            name="content",
            provider="openai",
            model="text-embedding-3-large",
            dim=1536,
            source=EmbeddingSource(fields=["title", "text"]),
            autolink=AutoLinkSimilar(k=25, threshold=0.80)
        )
    ],
    relations=[
        RelationSpec(
            name="BELONGS_TO",
            to_label="Conversation",
            to_key=["conversation_id"]
        ),
        RelationSpec(
            name="MENTIONS",
            from_field="entities",
            resolve_to_label="Entity",
            resolve_key=["name"]
        )
    ],
    calc_fields=[
        CalculatedFieldSpec(name="tokens", fn=calc_tokens("text")),
        CalculatedFieldSpec(name="salience", fn=calc_salience(...))
    ],
    policies=MemoryPolicies(
        retention=RetentionPolicy.keep,
        versioned=True,
        salience_alpha=0.65
    )
)
class MemoryItem(BaseModel):
    id: str
    title: str | None = None
    text: str
    entities: List[str] = []
    conversation_id: str | None = None
```

### 1.4 BasedModel ORM Layer
**Location:** `src/graph_management/overlay/based_model.py`

**Methods:**
- `.save(update=False)` - Persist to graph
- `.link(rel_type, other, props)` - Create relationship
- `.unlink(rel_type, other)` - Remove relationship
- `.neighbors(rel_type, direction, limit)` - Traversal

**Example:**
```python
class BasedModel(GraphObject):
    def save(self, *, update: bool = False):
        return self.merge(is_update=update)

    def link(self, rel_type: str, other: "BasedModel", props=None):
        return self.relate_to(other, rel_type, props or {})
```

---

## Phase 2: Conversation Graph Service

### 2.1 Graph Schema
**Location:** `src/graph_management/conversation/schema.cypher`

```cypher
-- Constraints
CREATE CONSTRAINT conv_id IF NOT EXISTS FOR (c:Conversation) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT msg_id  IF NOT EXISTS FOR (m:Message) REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT blk_id  IF NOT EXISTS FOR (b:Block) REQUIRE b.id IS UNIQUE;

-- Vector Indexes
CREATE VECTOR INDEX msg_emb IF NOT EXISTS
  FOR (m:Message) ON (m.embedding)
  OPTIONS {indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }};
```

### 2.2 Conversation Models
**Location:** `src/graph_management/conversation/models.py`

```python
@graph_object(
    label="Conversation",
    key=["id"],
    service_id="graph-svc"
)
class Conversation(BasedModel):
    id: str
    title: str | None = None
    created_at: float

@graph_object(
    label="Message",
    key=["id"],
    service_id="graph-svc",
    embeddings=[
        EmbeddingSpec(
            name="content",
            provider="openai",
            model="text-embedding-3-large",
            dim=1536,
            source=EmbeddingSource(field="text"),
            autolink=AutoLinkSimilar(k=20, threshold=0.78)
        )
    ],
    relations=[
        RelationSpec(name="HAS_MESSAGE", direction="IN", to_label="Conversation", to_key=["conversation_id"]),
        RelationSpec(name="MENTIONS", from_field="entities", resolve_to_label="Entity", resolve_key=["name"])
    ]
)
class Message(BasedModel):
    id: str
    role: str
    text: str | None = None
    conversation_id: str
    entities: List[str] = []
```

### 2.3 Conversation Service API
**Location:** `src/graph_management/conversation/api.py`

**Endpoints:**
- `POST /conversations` - Create conversation
- `POST /conversations/{id}/messages` - Add message
- `GET /conversations/{id}/retrieval` - Hybrid retrieval (graph + vector)
- `POST /conversations/{id}/similarity` - Find similar messages

---

## Phase 3: Advanced Features

### 3.1 Embedding Providers
**Location:** `src/graph_management/embeddings/`

**Providers:**
- `OpenAIProvider` - OpenAI API
- `FastEmbedProvider` - Local FastEmbed
- `CustomProvider` - User-defined

**Registry:**
```python
from graph_management.embeddings import register_provider

register_provider("openai", OpenAIProvider(api_key=...))
register_provider("fastembed", FastEmbedProvider(model="all-MiniLM-L6-v2"))
```

### 3.2 Hybrid Retrieval
**Location:** `src/graph_management/retrieval/hybrid.py`

**Scoring:**
```python
def hybrid_score(
    structural_score: float,
    vector_score: float,
    temporal_decay: float,
    alpha: float = 0.6,
    beta: float = 0.3,
    gamma: float = 0.1
) -> float:
    return alpha * structural_score + beta * vector_score + gamma * temporal_decay
```

**Graph Closeness Query:**
```cypher
MATCH (m0:Message {id: $pivot})
MATCH p=(m0)-[r:SIMILAR|REPLY_TO|NEXT|CALLS_TOOL*1..4]-(m:Message)
WITH m, reduce(w=0.0, x IN relationships(p) |
  w + CASE type(x)
    WHEN 'SIMILAR' THEN 2.5
    WHEN 'REPLY_TO' THEN 1.8
    WHEN 'NEXT' THEN 1.2
    ELSE 0.5 END
) AS structural_score
RETURN m.id, max(structural_score) AS score
ORDER BY score DESC LIMIT $k
```

### 3.3 Calculated Fields Library
**Location:** `src/graph_management/overlay/calcs.py`

**Utilities:**
```python
def calc_tokens(field: str) -> Callable:
    """Token count for field"""
    def _fn(obj): return len(getattr(obj, field, "").split())
    return _fn

def calc_salience(
    score_field: str = "score",
    created_field: str = "created_ts",
    alpha: float = 0.6,
    beta: float = 0.3,
    gamma: float = 0.1,
    half_life_seconds: float = 7*24*3600
) -> Callable:
    """Salience with recency/frequency/explicit score"""
    ...
```

---

## Phase 4: Integration & Production

### 4.1 API Enhancements
**Extend:** `src/graph_management/api.py`

**New Endpoints:**
- `POST /v1/overlay/objects` - Create graph objects
- `POST /v1/overlay/link` - Create relationships
- `POST /v1/retrieval/hybrid` - Hybrid search
- `GET /v1/conversations/{id}` - Get conversation subgraph

### 4.2 Migration Tools
**Location:** `src/graph_management/migrations/`

**Auto-generate Cypher DDL:**
```python
from graph_management.migrations import generate_ddl

# Read all @graph.object decorated classes
# Generate constraints, indexes, vector indexes
ddl = generate_ddl(registry)
```

### 4.3 Testing Strategy
**Location:** `tests/overlay/`

**Test Coverage:**
- Unit: Individual overlay components
- Integration: Full @graph.object lifecycle
- E2E: Conversation creation → retrieval
- Performance: Vector search benchmarks

---

## Implementation Order

### Week 1-2: Foundation
1. ✅ Enhanced GraphClient with vectors
2. ✅ Overlay specifications (specs.py)
3. ✅ @graph.object decorator (basic)
4. ✅ BasedModel ORM layer

### Week 3-4: Conversation Graph
5. ✅ Conversation models & schema
6. ✅ Message persistence with embeddings
7. ✅ Relationship auto-wiring
8. ✅ Basic retrieval API

### Week 5-6: Advanced Features
9. ✅ Embedding providers (OpenAI, FastEmbed)
10. ✅ AutoLinkSimilar implementation
11. ✅ Calculated fields & policies
12. ✅ Hybrid retrieval engine

### Week 7-8: Production
13. ✅ API integration
14. ✅ Migration tools
15. ✅ Comprehensive tests
16. ✅ Documentation & examples

---

## Dependencies

**Required:**
- `pydantic>=2.0` ✅ (already have)
- `httpx` ✅ (already have)
- `neo4j-driver` or equivalent
- `numpy` (vector operations)

**Optional:**
- `openai` (OpenAI embeddings)
- `fastembed` (local embeddings)
- `sentence-transformers` (alternative embeddings)

---

## Success Criteria

### Core Features
- [ ] @graph.object supports all spec types
- [ ] Multi-space embeddings working
- [ ] Relationship auto-wiring (static + dynamic)
- [ ] Calculated fields execute on create/update
- [ ] AutoLinkSimilar creates edges

### Conversation System
- [ ] Message persistence with embeddings
- [ ] SIMILAR edges auto-created
- [ ] Hybrid retrieval combines graph + vector
- [ ] Tool call/result graph representation

### Performance
- [ ] Vector search <100ms for k=20
- [ ] Graph closeness query <200ms depth=4
- [ ] Embedding computation <500ms per message
- [ ] Batch operations support 100+ objects

### Integration
- [ ] Works with existing GraphManagementClient
- [ ] Compatible with current API endpoints
- [ ] Backward compatible with schemas
- [ ] Clean separation from ToolManagement

---

## Notes

**Design Philosophy:**
- Declarative over imperative
- Graph-first (everything persisted as relationships)
- Type-aware (rich semantics for async/streaming)
- Composable (embeddings, relations, fields pluggable)
- Observable (execution traces enable retrieval)

**Not in Scope (ToolManagement handles):**
- `@tools.endpoint` decorator
- Tool discovery & registry
- Multi-hop planning
- Sidecar assistance (AssistSpec)
- Tool execution orchestration

**Integration Points:**
- ToolManagement calls GraphManagement for object persistence
- Conversation graph stores ToolCall/ToolResult nodes
- Memory overlay enables semantic tool retrieval
- Shared schemas for cross-service consistency
