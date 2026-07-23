# dsco-autobot Services Overview

> **Updated:** January 14, 2026
> **Status:** All services have Dockerfiles and basic structure

---

## Service Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL CLIENTS                                   │
│                     (Web UI, CLI, SDK, API Consumers)                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ENGINE (Port 8000)                                  │
│              Agent Orchestration & Task Execution                           │
│         ┌─────────────┬─────────────┬─────────────┐                         │
│         │   Runtime   │  Substrate  │     API     │                         │
│         │  OODA Loop  │  GraphSub   │   FastAPI   │                         │
│         │   Agents    │   Redis     │   WebSocket │                         │
│         └─────────────┴─────────────┴─────────────┘                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
           ┌────────────────────┼────────────────────┐
           │                    │                    │
           ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│   COMPLETIONS    │  │  CONVERSATIONS   │  │  TOOL MANAGEMENT │
│   (Port 8001)    │  │   (Port 8002)    │  │   (Port 8003)    │
│                  │  │                  │  │                  │
│  LLM Completion  │  │  Conversation    │  │  Tool Registry   │
│  Multi-Provider  │  │  Management      │  │  MCP Integration │
│  OpenRouter/API  │  │  History/State   │  │  400+ Tools      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  TASK SCHEDULER  │  │ GRAPH MANAGEMENT │  │  DYNAMIC SCHEMA  │
│   (Port 8004)    │  │   (Port 8005)    │  │   (Port 8006)    │
│                  │  │                  │  │                  │
│  Priority Queue  │  │  Neo4j/GraphSub  │  │  Schema Gen      │
│  Flexible Sched  │  │  RBAC/ABAC       │  │  LLM-powered     │
└──────────────────┘  └──────────────────┘  └──────────────────┘
           │                    │                    │
           └────────────────────┼────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INFRASTRUCTURE                                     │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌─────────┐           │
│  │Temporal │  │  Redis  │  │PostgreSQL│  │GraphSub │  │  Neo4j  │           │
│  │ :7233   │  │  :6379  │  │  :5432   │  │  :7879  │  │  :7474  │           │
│  └─────────┘  └─────────┘  └──────────┘  └─────────┘  └─────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Service Details

### 1. Engine (Port 8000)

**Location:** `/Engine`
**Status:** ✅ Production-ready (Phase 2 complete)
**Dockerfile:** `Engine/Dockerfile`

**Responsibilities:**
- Agent lifecycle management (spawn, execute, terminate)
- OODA loop execution (Observe-Orient-Decide-Act)
- Stigmergic coordination via pheromones
- Task distribution via Redis mailbox
- Capability token management (Ed25519)

**Key Components:**
- `runtime/agent_executor.py` - OODA loop implementation
- `runtime/agent_pool.py` - Agent pool management
- `runtime/capability_matcher.py` - Skill matching
- `runtime/resource_manager.py` - Budget/quota management
- `substrate/` - GraphSub, Redis, Stigmergy clients
- `api/server.py` - FastAPI REST API

**Endpoints:**
- `GET /health` - Health check
- `POST /api/tasks` - Submit task
- `GET /api/tasks/{id}` - Task status
- `POST /api/agents` - Spawn agent
- `GET /api/agents` - List agents

---

### 2. Completions (Port 8001)

**Location:** `/Completions`
**Status:** ✅ Functional
**Dockerfile:** `Completions/Dockerfile`

**Responsibilities:**
- LLM completion requests
- Multi-provider support (OpenRouter, Anthropic, OpenAI)
- Response formatting and streaming
- Tool call handling

**Key Files:**
- `app.py` - FastAPI application
- `openrouter.py` - OpenRouter client
- `openai_client.py` - OpenAI client
- `provider_configs.py` - Provider configuration

**Endpoints:**
- `GET /health` - Health check
- `POST /v1/completions` - Generate completion
- `POST /v1/chat/completions` - Chat completion

---

### 3. Conversations (Port 8002)

**Location:** `/Conversations`
**Status:** ✅ Functional
**Dockerfile:** `Conversations/Dockerfile`

**Responsibilities:**
- Conversation state management
- Message history storage
- Conversation branching/forking
- User context management

**Key Files:**
- `fastapi_app.py` - FastAPI application
- `conversation_api_dataclasses.py` - Data models
- `modal_wrapper.py` - Modal deployment

**Endpoints:**
- `GET /health` - Health check
- `POST /conversations` - Create conversation
- `GET /conversations/{id}` - Get conversation
- `POST /conversations/{id}/messages` - Add message

---

### 4. ToolManagement (Port 8003)

**Location:** `/ToolManagement`
**Status:** ✅ Functional
**Dockerfile:** `ToolManagement/Dockerfile`

**Responsibilities:**
- Tool registry and discovery
- MCP (Model Context Protocol) server
- Tool execution and result handling
- Compound tool composition

**Key Files:**
- `src/tool_management/tools_api.py` - FastAPI application
- `src/tool_management/intelligence/` - Tool routing/embedding
- `examples/` - Tool usage examples

**Endpoints:**
- `GET /health` - Health check
- `GET /tools` - List tools
- `POST /tools/execute` - Execute tool
- `GET /tools/{id}` - Get tool details

---

### 5. TaskScheduler (Port 8004)

**Location:** `/TaskScheduler`
**Status:** ✅ Functional
**Dockerfile:** `TaskScheduler/Dockerfile`

**Responsibilities:**
- Priority-based task scheduling
- Task dependency management
- Flexible execution strategies
- Task graph visualization

**Key Files:**
- `app.py` - FastAPI application
- `scheduling/flexible_scheduler.py` - Scheduler implementation
- `scheduling/priority_queue_executor.py` - Priority queue
- `models/task_graph.py` - Task graph model

**Endpoints:**
- `GET /health` - Health check
- `POST /tasks` - Schedule task
- `GET /tasks/{id}` - Get task status
- `GET /graph` - Get task graph

---

### 6. GraphManagement (Port 8005)

**Location:** `/GraphManagement`
**Status:** ✅ Functional
**Dockerfile:** `GraphManagement/Dockerfile`

**Responsibilities:**
- Graph database operations (Neo4j/GraphSub)
- RBAC/ABAC security policies
- Schema management
- Query execution

**Key Files:**
- `app.py` - Entry point
- `src/graph_management/api.py` - FastAPI application
- `src/graph_management/routers/` - API routers

**Endpoints:**
- `GET /health` - Health check
- `GET /v1/schema` - Get schema
- `POST /v1/query` - Execute query
- `GET /v1/indexes` - List indexes

---

### 7. DynamicSchema (Port 8006)

**Location:** `/DynamicSchema`
**Status:** ✅ Basic
**Dockerfile:** `DynamicSchema/Dockerfile`

**Responsibilities:**
- Dynamic schema generation
- LLM-powered schema inference
- Code generation

**Key Files:**
- `llm_app.py` - FastAPI application
- `app.py` - Main application
- `openrouter.py` - LLM client

**Endpoints:**
- `GET /health` - Health check
- `POST /generate` - Generate schema

---

## Infrastructure Services

### Temporal (Port 7233)

**Image:** `temporalio/auto-setup:latest`
**Purpose:** Durable workflow execution

**Endpoints:**
- gRPC: `localhost:7233`
- UI: `http://localhost:8080`

---

### Redis (Port 6379)

**Image:** `redis:7-alpine`
**Purpose:** Task mailbox, caching, pub/sub

**Configuration:**
- `appendfsync always` (Jepsen-safe)
- `maxmemory 2gb`

---

### PostgreSQL (Port 5432)

**Image:** `postgres:15-alpine`
**Purpose:** Temporal state persistence

**Credentials:**
- User: `temporal`
- Password: `temporal`
- Database: `temporal`

---

### GraphSub (Port 7879)

**Note:** Runs separately (not in Docker Compose)
**Purpose:** Stigmergic graph database

**Endpoints:**
- HTTP API: `http://localhost:7879`
- Cypher: `http://localhost:7879/cypher`
- GraphQL: `http://localhost:7879/graphql`

---

## Quick Start

### Start Infrastructure Only

```bash
cd infra/docker
docker-compose up -d postgres redis temporal temporal-ui
```

### Start Full Stack

```bash
cd infra/docker
docker-compose -f docker-compose.full.yml up -d
```

### Start Individual Service

```bash
# Engine
cd Engine
uvicorn api.server:app --host 0.0.0.0 --port 8000

# Completions
cd Completions
uvicorn app:app --host 0.0.0.0 --port 8001
```

---

## Service Dependencies

```
Engine
├── Temporal (required)
├── Redis (required)
└── GraphSub (required)

Completions
├── Redis (optional, for caching)
└── External LLM APIs (required)

Conversations
├── Redis (required)
└── GraphSub (optional)

ToolManagement
├── Redis (required)
└── GraphSub (optional)

TaskScheduler
├── Redis (required)
└── Temporal (optional)

GraphManagement
├── Neo4j or GraphSub (required)
└── Redis (optional)

DynamicSchema
├── Redis (optional)
└── External LLM APIs (required)
```

---

## Port Reference

| Service | Port | Protocol |
|---------|------|----------|
| DynamicSchema | 2010 | HTTP |
| TaskScheduler | 2011 | HTTP |
| Completions | 2012 | HTTP |
| Conversations | 2013 | HTTP |
| GraphManagement | 2014 | HTTP |
| Ingestion | 2015 | HTTP |
| ToolManagement | 2016 | HTTP |
| Engine | 2020 | HTTP |
| Temporal gRPC | 7233 | gRPC |
| Temporal UI | 8080 | HTTP |
| Redis | 6379 | Redis |
| Redis Commander | 8081 | HTTP |
| PostgreSQL | 5432 | PostgreSQL |
| GraphSub | 7879 | HTTP |
| Prometheus | 9090 | HTTP |
| Grafana | 3000 | HTTP |

---

## Files Created/Updated

### New Dockerfiles

- `Engine/Dockerfile`
- `Engine/Dockerfile.worker`
- `Completions/Dockerfile`
- `Conversations/Dockerfile`
- `ToolManagement/Dockerfile`
- `TaskScheduler/Dockerfile`
- `GraphManagement/Dockerfile`

### New Docker Compose

- `infra/docker/docker-compose.full.yml` - Full stack deployment
- `infra/docker/prometheus.yml` - Prometheus configuration

### New Entry Points

- `GraphManagement/app.py` - Service entry point
