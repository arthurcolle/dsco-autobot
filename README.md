# Autobot — Autonomous Agent Platform (architecture 2023, reimplemented and deployed by Distributed Systems, Inc.)

**The heavyweight, multi-service agent platform whose architecture started Distributed Systems, Inc.**
Designed and first built in 2023: 13 cooperating microservices, an event-driven Engine API
core, and a graph-native data model — a full production-style autonomous agent system,
operated solo. This repository is a deployed reimplementation of that architecture under
Distributed Systems, Inc., written fresh; it is not code carried over from any prior employer.

> **Status & lineage.** Autobot is the *first-designed* layer of the platform: deliberately
> heavy, service-per-concern, and proven in real use. Its Tool Management API powers
> `tools.distributed.systems` and is a key part of the company's strategy, not a leftover:
> it is the hosted execution boundary that Distributed Search, the Python SDK, and
> organization runtime builds all pass through. Its lessons — that a local-first runtime beats a
> service fleet for individual operators, that routing authority is a first-class
> problem, and that agents need a graph substrate, not tables — were **distilled into
> the current stack**: `dsco-cli` (sovereign C runtime), `dsco-router` (inference
> control plane + trained dispatch controller), and GraphSub (Rust graph substrate).
> See the workspace root `README.md` for the platform map.

---

## Architecture

A producer-consumer, event-driven design with backpressure. The **Engine API** is the
single connection point: it runs the main event loop, dispatches every interaction
between microservices, and maintains a WebSocket to the public-facing
**Autonomous Agent API** (previously `autobot.brainchain.cloud`) for a real-time view
of all system operations, including 1 Hz liveness heartbeats from every service.

```
                      Planning                    Scheduling            Completions
                          API                          API                    API
   Dynamic               ┌────────────────────────────┬──────────────────────────┐
   Schema                │                            │                          │
      │                  │         E N G I N E   A P I          (central dispatch)│
   Vision                │   websocket mesh + 1Hz heartbeats + routing              │
   (real-time            └────────────────────────────┬──────────────────────────┘
    streaming)                 │            │             │              │
      │                  AgentState      Embeddings    Graph Mgmt     Tool Mgmt
   Document             Mgmt                                 │               │
   Ingestion                  │            Conversations ────┘               │
      │                       └──────────── Evaluations                        │
   Conversations                                                    Autonomous Agent API
   & Messages                                                       (public frontend)
```

## Services

| # | Service | Port | Directory | Status |
|---|---------|------|-----------|--------|
| 1 | **Engine** — central coordinator, event bus, dispatch | 8000 | `Engine/` | ✅ Production-ready (Phase 2 complete; Temporal-based ticks/signals/CRDT state sync — see `Engine/ENGINE_API_SPEC.md`) |
| 2 | **Completions** — multi-provider LLM completion (OpenRouter, Anthropic, OpenAI) | 8001 | `Completions/` | ✅ Functional |
| 3 | **Conversations** — conversation & message state | 8002 | `Conversations/` | ✅ Functional |
| 4 | **ToolManagement** — tool registry, execution, MCP, composition | 8003 | `ToolManagement/` | ✅ Functional — deployed at **tools.distributed.systems** |
| 5 | **TaskScheduler** — task routing & execution | 8004 | `TaskScheduler/` | ✅ Functional |
| 6 | **GraphManagement** — graph infrastructure for the whole system | 8005 | `GraphManagement/` | ✅ Functional (Neo4j local + Modal wrappers) |
| 7 | **DynamicSchema** — dynamic schema API | 8006 | `DynamicSchema/` | ✅ Basic |
| 8+ | Ingestion, Realtime (streaming vision), Engine API rebuild, Agent Management, Evaluations | — | `Ingestion/`, `Realtime/`, … | Partial / in flight |

Per the original event-driven architecture design: **10 services running locally, 3 in
flight** (Agent Management API, Evaluations API, and the ground-up Engine API
message-bus rebuild).

## Why event-driven

The single "get-and-dispatch" runtime outgrew itself the moment we needed stream
enrichment across heterogeneous event streams (completions, document ingestion into the
graph, realtime vision). The result: producer-consumer with backpressure, one central
dispatch, per-service heartbeats, and a real health-check path from Engine to the
public API.

## Graph-native substrate

The system's data model is a graph, not a set of tables. `GraphManagement/` implements
the full graph infrastructure spec (`ToolManagement/GRAPH_MANAGEMENT_SPECIFICATION.md`,
v2.0): tools, their actions, their typed input/output schemas, agents, conversations,
executions, organizations and capability tokens all live as typed nodes with explicit
relationships (`HAS_ACTION`, `HAS_INPUT_SCHEMA`, `EXTENDS`, `EVOLVED_TO`,
`DEPENDS_ON`). The `@tool` decorator synchronizes tool code and schema nodes into the
graph at runtime; tools are always served from their latest version.

> Roadmap: migrating ToolManagement's registry to fully serve from this graph model —
> see `ToolManagement/docs/GRAPH_NATIVE_ROADMAP.md`.

## Tool Management highlights (`ToolManagement/`)

- 51 tools across 4 distributed MCP backends behind one unified gateway
- Semantic tool discovery: embedded tool catalog, natural-language search, ~50 ms
  lookup, LRU context-window management — built to scale to millions of tools
- Composition engine: sequential / parallel / conditional / loop / map-reduce /
  pipeline patterns, with skill acquisition (learned workflows in procedural memory)
- MCP server (DRAFT-2025-v3), HATEOAS REST, protocol adapters (OpenAI, Anthropic,
  LangChain, MCP), webhooks with signed retries, registry federation
- Production deploy on Modal; per-tenant credentials in an encrypted vault with
  short-lived AES-GCM envelopes (see `TENANT_CREDENTIALS_WIRING.md`)

## Running locally

```bash
docker compose up          # per-service Dockerfiles also provided
# or per service:
cd ToolManagement && pip install -r requirements.txt && python fastapi_app.py
./do health && ./do list   # fast CLI against the deployed API
```

## Repository map

```
Engine/            central dispatch, event bus, Temporal workflows (see ENGINE_API_SPEC.md)
ToolManagement/    tool registry + execution + MCP      → tools.distributed.systems
GraphManagement/   graph infrastructure (Neo4j, Modal)
TaskScheduler/     task routing + execution
DynamicSchema/     dynamic schema service
Conversations/     conversation/message state
Completions/       multi-provider LLM completions
Ingestion/         document ingestion pipeline (chunkers, embedders, extractors)
Realtime/          real-time streaming vision console
libs/ infra/ scripts/   shared libraries, infra, ops scripts
```

---

*Distributed Systems, Inc. — the "distributed systems" in the name is literal.*
