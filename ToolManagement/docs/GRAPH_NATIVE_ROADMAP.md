# Graph-Native Tool Management — Target Architecture & Roadmap

**Status:** design target / migration plan
**Source of truth for the graph model:** `../GRAPH_MANAGEMENT_SPECIFICATION.md` (v2.0)
**Gap owner:** Tool Management service (`tools.distributed.systems`)

---

## 1. The gap, stated precisely

Today the Tool Management service stores tools in a **registry** (relational store +
embedded vector index for semantic discovery). This works, and powers the deployed
service. But it does **not** faithfully reproduce the system's graph-based design, in
which a tool is not a row — it is a **subgraph**:

```
                    ┌──────────── Tool ────────────┐
                    │  id · name · code · version  │
                    └──┬─────────┬─────────┬───────┘
             HAS_ACTION│  HAS_INPUT_SCHEMA│ HAS_OUTPUT_SCHEMA
                       ▼        │         ▼
                 ToolAction     │    Schema (typed)
                 (invocable     │    JSON-Schema node,
                  behavior)     │    versioned
                                │
        EXTENDS / EVOLVED_TO / DEPENDS_ON  ←→  other Tools
                                │
                     SubgraphSchema (the tool's
                     declared subgraph boundary)
```

The registry collapses this structure into opaque fields. That loses:

- **Typed I/O as first-class graph nodes** — input/output schemas should be `Schema`
  nodes reachable by edges, queryable, composable, and independently versioned
- **Tool lineage** — `EXTENDS` / `EVOLVED_TO` edges make tool versioning and
  deprecation a graph walk, not a convention
- **Dependency resolution** — `DEPENDS_ON` edges let the composition engine validate a
  pipeline against actual schema compatibility
- **Uniformity with the rest of the system** — agents, conversations, messages,
  executions, organizations, and capability tokens are already graph-native
  (`GRAPH_MANAGEMENT_SPECIFICATION.md` §2–3)

## 2. Current state (what exists today)

| Capability | State |
|---|---|
| Tool registration / execution / composition | ✅ registry-backed, deployed |
| Semantic discovery (embeddings + vector search) | ✅ ~50 ms, LRU context management |
| MCP server (DRAFT-2025-v3) + protocol adapters | ✅ |
| `@tool` decorator → graph synchronization | ✅ partial (syncs tool + schema nodes at runtime; tools served from latest version) |
| GraphManagement service (Neo4j local + Modal) | ✅ separate service, port 8005 |
| **Tools-as-subgraphs served *from* the graph** | ❌ target state |

## 3. Target architecture

1. **Every tool is a `SubgraphSchema` instance**: one `Tool` node + `ToolAction` nodes
   + typed input/output `Schema` nodes, connected by the §3.2 instance relationships
   from the graph spec (`HAS_ACTION`, `HAS_INPUT_SCHEMA`, `HAS_OUTPUT_SCHEMA`, …).
2. **The registry becomes a projection**, not the source of truth: the graph is
   canonical; the relational store remains a fast read path, rebuilt from the graph.
3. **Semantic discovery stays** — embeddings attach to graph nodes, so discovery
   results return *subgraphs* (tool + actions + schemas) rather than flat rows.
4. **Composition planning over edges**: `DEPENDS_ON` + schema-node compatibility
   checks validate sequential/parallel/map-reduce plans statically before execution.
5. **Federation = graph sync**: external registries sync as subgraph diffs, not blob
   imports.
6. **Authorization stays graph-native**: `CapabilityToken` nodes (§2.3) bind
   tenants to tool subgraphs.

## 4. Migration plan

| Phase | Deliverable | Exit criteria |
|---|---|---|
| 0 | Graph write-path behind the `@tool` decorator for new + updated tools | All new tools appear as subgraphs in GraphManagement |
| 1 | Dual-write adapter (registry ⇄ graph), backfill job for the existing catalog | 100% of deployed tools readable as subgraphs |
| 2 | Graph read-path for tool detail + lineage + dependencies | `GET /tools/{id}` served from graph projection |
| 3 | Semantic discovery over graph nodes (embeddings per node, not per row) | Discovery returns subgraphs; parity ≤ current 50 ms target |
| 4 | Composition planner validates against `DEPENDS_ON` + typed schemas | Static validation rejects incompatible pipelines pre-execution |
| 5 | Registry demoted to rebuild-able cache; federation as subgraph sync | Graph deletion + rebuild passes integrity tests |

## 5. Why this matters (the one-paragraph version)

Tools-as-subgraphs is what turns a tool registry into a **tool economy**: versioned,
typed, composable, dependency-aware, and federatable — and it makes the tool surface
and the agent substrate literally the same data structure. Combined with GraphSub (our
Rust graph engine for million-agent coordination), this is the layer competitors would
have to rebuild from scratch.
