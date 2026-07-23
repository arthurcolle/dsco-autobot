# Phase 1: Foundation - Quick Start Guide

**Status:** ✅ COMPLETE - All infrastructure running

This guide walks you through verifying the Phase 1 foundation is working.

---

## 🚀 Quick Verification (5 minutes)

### Step 1: Verify Docker Services

```bash
cd /Users/arthurcolle/Dsco/dsco-autobot/infra/docker

# Start Temporal, Redis, PostgreSQL
docker-compose up -d

# Verify all services are healthy
docker-compose ps

# Expected output:
# CONTAINER   STATUS
# dsco-postgres          Up, healthy
# dsco-temporal          Up, healthy
# dsco-temporal-ui       Up, healthy
# dsco-redis             Up, healthy
# dsco-redis-commander   Up
```

### Step 2: Verify GraphSub

```bash
cd /Users/arthurcolle/parallel_research_v2/graphsub

# Build (first time only)
cargo build --release

# Start GraphSub
./target/debug/graphsub start --shard-count 32 --memory-max 1024MB

# Expected output:
# INFO graphsub: Starting GraphSub node
# INFO graphsub::server: GraphSub server listening on 0.0.0.0:7878
# INFO graphsub::api: HTTP API server listening on http://0.0.0.0:7879
```

### Step 3: Test Connectivity

```bash
# Test Redis
redis-cli -h localhost -p 6379 ping
# Expected: PONG

# Test GraphSub HTTP API
curl http://localhost:7879/health
# Expected: 200 OK

# Test GraphSub Cypher
curl -X POST http://localhost:7879/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "RETURN 42 AS answer"}'
# Expected: JSON response with answer=42
```

### Step 4: Access UIs

```bash
# Temporal UI (workflow visualization)
open http://localhost:8080

# Redis Commander (data inspection)
open http://localhost:8081
```

---

## 📋 Service Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| **Temporal gRPC** | localhost:7233 | Workflow execution |
| **Temporal HTTP** | localhost:6933 | HTTP API (metrics, admin) |
| **Temporal UI** | http://localhost:8080 | Workflow visualization |
| **Redis** | localhost:6379 | Mailbox & session store |
| **Redis Commander** | http://localhost:8081 | Data browser |
| **PostgreSQL** | localhost:5432 | Temporal state database |
| **GraphSub HTTP** | http://localhost:7879 | Graph API & Cypher |
| **GraphSub Cypher** | http://localhost:7879/cypher | Cypher queries |
| **GraphSub GraphQL** | http://localhost:7879/graphql | GraphQL playground |

---

## 🔐 Credentials

```
PostgreSQL:
  Host: localhost:5432
  User: temporal
  Password: temporal
  Database: temporal

Redis:
  No authentication by default
  Configured with: appendfsync always (critical for safety)
```

---

## ⚠️ Critical Jepsen Safety Settings

The Phase 1 infrastructure has been configured with Jepsen-recommended safety settings:

### Redis Configuration

```bash
# In docker-compose.yml:
redis-server \
  --appendonly yes \           # Enable persistence
  --appendfsync always \       # CRITICAL: Sync before acknowledge
  --maxmemory 2gb \
  --maxmemory-policy allkeys-lru
```

**Why:** Without `appendfsync always`, Redis can lose data on power failure. See [Jepsen NATS analysis](https://jepsen.io/analyses/nats-2.12.1).

### Verify Redis Fsync Setting

```bash
# Should print "always"
redis-cli CONFIG GET appendfsync
```

---

## 🧪 Running Tests

### Run Integration Tests

```bash
cd /Users/arthurcolle/Dsco/dsco-autobot/Engine

# Install test dependencies (if needed)
pip install pytest pytest-asyncio httpx

# Run all integration tests
pytest tests/integration/test_phase1_infrastructure.py -v

# Run specific test
pytest tests/integration/test_phase1_infrastructure.py::TestInfrastructureConnectivity::test_redis_connectivity -v
```

### Test Output Example

```
test_redis_connectivity PASSED
test_redis_persistence_settings PASSED
test_temporal_connectivity PASSED
test_graphsub_http_api PASSED
test_graphsub_rest_api PASSED
test_graphsub_cypher_endpoint PASSED
test_substrate_module_imports PASSED
```

---

## 📚 Using the Substrate APIs

### GraphSub Client Example

```python
from engine.substrate.graphsub_client import GraphSubClient, PheromoneTrail

async with GraphSubClient("http://localhost:7879") as gs:
    # Create agent node
    agent = await gs.create_agent_node(
        agent_id="test-agent-1",
        model="claude-3.5-sonnet",
        budget_remaining=50.00,
        capabilities={"python": 0.95, "search": 0.88}
    )
    print(f"Created agent: {agent.id}")

    # Deposit pheromone
    trail = PheromoneTrail(
        signal_type="success",
        intensity=0.9,
        decay_rate=0.01,
        ttl_seconds=300
    )
    await gs.deposit_pheromone("test-agent-1", "test-task-1", trail)
    print("Deposited pheromone")
```

### Redis Mailbox Example

```python
from engine.substrate.redis_mailbox import RedisMailbox

async with RedisMailbox("redis://localhost:6379", agent_id="agent-1") as mailbox:
    # Create consumer group
    await mailbox.create_consumer_group("tasks:pending", "default")

    # Publish task
    msg_id = await mailbox.publish_task(
        task_id="task-123",
        task_data={"type": "echo", "message": "hello"},
        priority=5
    )
    print(f"Published task: {msg_id}")

    # Consume tasks
    async for message in mailbox.consume_tasks("agent-1", pool_name="default"):
        print(f"Received: {message.payload}")
        await mailbox.acknowledge_message(message.id, "tasks:pending", "default")
```

### Stigmergy Coordinator Example

```python
from engine.substrate.stigmergy import StigmergicCoordinator
from engine.substrate.graphsub_client import GraphSubClient

gs = GraphSubClient("http://localhost:7879")
coordinator = StigmergicCoordinator(gs)

# Deposit progress signal
await coordinator.deposit_progress_pheromone(
    agent_id="agent-1",
    task_id="task-123",
    progress_fraction=0.5,  # 50% complete
    estimated_remaining_seconds=30
)

# Scan for attractive tasks
tasks = await coordinator.scan_attractive_tasks(
    agent_id="agent-1",
    required_capabilities=["python"],
    limit=10
)
print(f"Found {len(tasks)} attractive tasks")
```

### Capability Tokens Example

```python
from engine.substrate.capability_tokens import (
    CapabilityTokenManager,
    Permissions,
    CapabilityScope
)

mgr = CapabilityTokenManager("admin")

# Create token for agent
token = mgr.create_token(
    holder_id="agent-1",
    permissions=Permissions.AGENT_BASIC,
    scope=CapabilityScope.global_scope(),
    expires_in_seconds=3600
)

# Verify token is valid
if mgr.verify_token(token):
    print("Token is valid and not expired")

# Check authorization
if mgr.check_authorization(token, Permissions.READ_NODE, "some-node"):
    print("Agent is authorized to read the node")
```

---

## 🔧 Troubleshooting

### "Connection refused" to Redis

```bash
# Check if Redis is running
docker-compose ps dsco-redis

# If not running, start it
docker-compose up -d dsco-redis

# Check Redis port
lsof -i :6379
```

### "Connection refused" to Temporal

```bash
# Check if Temporal is running
docker-compose ps dsco-temporal

# If not running, start it
docker-compose up -d dsco-temporal

# Check Temporal port
lsof -i :7233
```

### GraphSub not responding

```bash
# Check if GraphSub process is running
ps aux | grep graphsub

# Verify it's listening on port 7879
lsof -i :7879

# Restart GraphSub
# Ctrl+C to stop, then:
./target/debug/graphsub start --shard-count 32 --memory-max 1024MB
```

### Redis fsync warning

```bash
# Check current setting
redis-cli CONFIG GET appendfsync
# If not "always", update docker-compose.yml

# Force update
redis-cli CONFIG SET appendfsync always
redis-cli CONFIG REWRITE  # Persist to config file
```

---

## 📖 File Structure

```
Engine/
├── substrate/
│   ├── __init__.py
│   ├── graphsub_client.py      (523 lines) - GraphSub MCP wrapper
│   ├── redis_mailbox.py        (414 lines) - Redis Streams mailbox
│   ├── stigmergy.py            (384 lines) - Pheromone coordination
│   └── capability_tokens.py    (420 lines) - Cryptographic auth
├── runtime/                     (TODO: Phase 2)
├── tests/
│   ├── conftest.py             (Test fixtures and mocks)
│   ├── unit/                    (TODO: Phase 2)
│   └── integration/
│       └── test_phase1_infrastructure.py (Infrastructure tests)
├── PHASE1_FOUNDATION.md         (Detailed architecture doc)
└── PHASE1_README.md             (This file)

infra/
└── docker/
    └── docker-compose.yml       (Temporal, Redis, PostgreSQL)
```

---

## ✅ Phase 1 Completion Checklist

- [x] Docker Compose with Temporal, Redis, PostgreSQL
- [x] GraphSub server running independently
- [x] GraphSubClient (523 lines) - Node/edge CRUD + pheromones
- [x] RedisMailbox (414 lines) - Durable task delivery
- [x] StigmergicCoordinator (384 lines) - Pheromone coordination
- [x] CapabilityTokenManager (420 lines) - Cryptographic auth
- [x] Test fixtures and mocks
- [x] Integration tests
- [x] Jepsen safety settings applied (Redis appendfsync always)
- [x] Documentation and README
- [x] Critical port corrections (GraphSub on 7879, not 54545)

---

## 🎯 Next Steps: Phase 2

Phase 2 focuses on building the **Agent Runtime** (OODA loop):

1. **AgentExecutor** (500 lines) - Observe-Orient-Decide-Act loop
2. **CapabilityMatcher** (200 lines) - Task-to-agent matching
3. **ResourceManager** (300 lines) - Budget and quota tracking
4. **Integration tests** - Multi-agent coordination tests

**Start Phase 2:** Follow instructions in `PHASE2_PLANNING.md` (coming soon)

---

## 📞 Support

- **Temporal docs:** https://temporal.io/docs
- **Redis docs:** https://redis.io/documentation
- **GraphSub docs:** See `/Users/arthurcolle/parallel_research_v2/graphsub/README.md`
- **Jepsen safety:** https://jepsen.io/analyses/nats-2.12.1

---

**Last Updated:** 2025-12-08
**Phase 1 Status:** ✅ COMPLETE
**Ready for:** Phase 2 - Agent Runtime Implementation
