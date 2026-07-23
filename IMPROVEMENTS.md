# dsco-autobot - Self-Improvement Notes

> Auto-generated analysis with actionable improvements

## Executive Summary

**Current State**: Production-grade foundation with Phase 1 & 2 complete
**Maturity**: 65% - Core infrastructure solid, needs integration polish
**Priority**: MEDIUM - Focus on hardening and documentation

---

## Architecture Assessment

### Strengths
- **Stigmergic coordination** - Unique differentiator, no central scheduler bottleneck
- **Capability tokens** - Ed25519 cryptographic security, delegatable, offline-verifiable
- **OODA loop** - Well-designed agent decision cycle (Observe-Orient-Decide-Act)
- **Jepsen-tested configurations** - Redis `appendfsync always` prevents data loss
- **Temporal integration** - Durable workflow execution with proper retry policies
- **Multi-service architecture** - Clean separation (Engine, ToolManagement, Completions, etc.)
- **Comprehensive documentation** - 80KB+ in specs and guides

### Weaknesses
- **Service coupling** - Some services have implicit dependencies
- **Test coverage gaps** - Integration tests exist but unit tests sparse
- **Deployment complexity** - Many moving parts (Temporal, Redis, PostgreSQL, GraphSub)
- **Frontend incomplete** - chatbot-ui exists but not fully integrated
- **No CI/CD pipeline** - Missing automated testing and deployment

---

## Immediate Improvements (Week 1)

### 1. Consolidate Configuration
```python
# engine/config.py - Single source of truth
from pydantic_settings import BaseSettings
from functools import lru_cache

class EngineSettings(BaseSettings):
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "autonomous-coworker"

    # Redis
    redis_url: str = "redis://localhost:6379"
    redis_mailbox_stream: str = "agent:mailbox"

    # GraphSub
    graphsub_url: str = "http://localhost:7879"

    # Agent Runtime
    agent_pool_size: int = 10
    ooda_tick_interval_ms: int = 100
    pheromone_decay_rate: float = 0.1

    # Security
    capability_token_ttl_seconds: int = 3600
    ed25519_seed: Optional[str] = None

    class Config:
        env_prefix = "DSCO_"
        env_file = ".env"

@lru_cache
def get_settings() -> EngineSettings:
    return EngineSettings()
```

### 2. Add Health Check Endpoints
```python
# engine/api/health.py
from fastapi import APIRouter

router = APIRouter(prefix="/health")

@router.get("/live")
async def liveness():
    return {"status": "alive"}

@router.get("/ready")
async def readiness():
    checks = {
        "temporal": await check_temporal(),
        "redis": await check_redis(),
        "graphsub": await check_graphsub(),
    }
    healthy = all(checks.values())
    return {"status": "ready" if healthy else "degraded", "checks": checks}

@router.get("/startup")
async def startup():
    return {"status": "started", "version": __version__}
```

### 3. Standardize Error Handling
```python
# engine/errors.py
class DSCOError(Exception):
    """Base exception for all DSCO errors."""
    code: str = "DSCO_ERROR"

class AgentExecutionError(DSCOError):
    code = "AGENT_EXECUTION_ERROR"

class CapabilityDeniedError(DSCOError):
    code = "CAPABILITY_DENIED"

class ResourceExhaustedError(DSCOError):
    code = "RESOURCE_EXHAUSTED"

# Middleware
@app.exception_handler(DSCOError)
async def dsco_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"error": exc.code, "message": str(exc)}
    )
```

### 4. Add Metrics Collection
```python
# engine/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Agent metrics
agents_active = Gauge('dsco_agents_active', 'Active agents')
agent_tasks_total = Counter('dsco_agent_tasks_total', 'Tasks by agent', ['agent_id', 'status'])
task_duration = Histogram('dsco_task_duration_seconds', 'Task execution time')

# Stigmergy metrics
pheromones_deposited = Counter('dsco_pheromones_total', 'Pheromones deposited', ['type'])
pheromone_strength = Gauge('dsco_pheromone_strength', 'Current pheromone strength', ['node_id'])

# Resource metrics
api_credits_used = Counter('dsco_api_credits_total', 'API credits consumed', ['provider'])
api_credits_remaining = Gauge('dsco_api_credits_remaining', 'Remaining credits', ['provider'])
```

---

## Medium-Term Improvements (Month 1)

### 5. Create Integration Test Suite
```python
# tests/integration/test_full_pipeline.py
import pytest
from testcontainers.redis import RedisContainer
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="module")
def services():
    with RedisContainer() as redis, PostgresContainer() as postgres:
        yield {"redis": redis, "postgres": postgres}

@pytest.mark.asyncio
async def test_agent_lifecycle(services):
    """Test complete agent lifecycle: spawn → execute → complete."""
    agent = await spawn_agent(capabilities=["file_read", "file_write"])
    task = await submit_task(agent.id, "Read config.yaml")
    result = await wait_for_completion(task.id, timeout=30)
    assert result.status == "completed"

@pytest.mark.asyncio
async def test_stigmergic_coordination(services):
    """Test pheromone-based task discovery."""
    # Agent A deposits pheromone
    await deposit_pheromone(node_id="task_123", strength=1.0, type="attractive")

    # Agent B should discover it
    discovered = await scan_pheromones(agent_id="agent_b", radius=10)
    assert "task_123" in [p.node_id for p in discovered]

@pytest.mark.asyncio
async def test_capability_delegation(services):
    """Test capability token delegation with attenuation."""
    root_token = create_capability_token(permissions=["*"])
    delegated = delegate_token(root_token, permissions=["file_read"])

    # Should work
    assert await verify_capability(delegated, "file_read")
    # Should fail (attenuated)
    assert not await verify_capability(delegated, "file_write")
```

### 6. Docker Compose Improvements
```yaml
# infra/docker/docker-compose.yml
version: "3.9"

services:
  engine:
    build: ../../Engine
    depends_on:
      temporal:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      - DSCO_TEMPORAL_HOST=temporal:7233
      - DSCO_REDIS_URL=redis://redis:6379
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health/ready"]
      interval: 10s
      timeout: 5s
      retries: 3

  tool-management:
    build: ../../ToolManagement
    depends_on:
      - engine
    ports:
      - "8001:8001"

  completions:
    build: ../../Completions
    depends_on:
      - engine
    ports:
      - "8002:8002"

  # Add missing service definitions...

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/dashboards:/var/lib/grafana/dashboards
```

### 7. Add OpenTelemetry Tracing
```python
# engine/tracing.py
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

def setup_tracing(service_name: str):
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)

tracer = setup_tracing("dsco-engine")

# Usage in agent_executor.py
@tracer.start_as_current_span("ooda_loop")
async def ooda_loop(self):
    with tracer.start_as_current_span("observe"):
        observations = await self.observe()
    with tracer.start_as_current_span("orient"):
        context = await self.orient(observations)
    # ...
```

### 8. Implement Circuit Breaker for External Calls
```python
# engine/resilience.py
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=30)
async def call_llm(messages: List[Dict]) -> str:
    """LLM call with circuit breaker."""
    return await provider.generate(messages)

@circuit(failure_threshold=3, recovery_timeout=60)
async def call_graphsub(query: str) -> Dict:
    """GraphSub call with circuit breaker."""
    return await graphsub_client.query(query)
```

---

## Long-Term Improvements (Quarter 1)

### 9. Multi-Tenant Support
```python
# engine/multitenancy.py
class TenantContext:
    tenant_id: str
    resource_quotas: ResourceQuotas
    capability_root: CapabilityToken

    @classmethod
    def from_request(cls, request: Request) -> "TenantContext":
        token = request.headers.get("Authorization")
        return cls.from_token(token)

# Middleware
@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    tenant = TenantContext.from_request(request)
    request.state.tenant = tenant
    response = await call_next(request)
    return response
```

### 10. Event Sourcing for Audit
```python
# engine/events.py
class DomainEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    aggregate_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any]
    metadata: Dict[str, Any] = {}

class EventStore:
    async def append(self, event: DomainEvent): ...
    async def get_events(self, aggregate_id: str) -> List[DomainEvent]: ...
    async def replay(self, aggregate_id: str) -> Any: ...

# Events
class AgentSpawned(DomainEvent):
    event_type = "agent.spawned"

class TaskAssigned(DomainEvent):
    event_type = "task.assigned"

class CapabilityGranted(DomainEvent):
    event_type = "capability.granted"
```

### 11. GraphQL API
```python
# engine/api/graphql.py
import strawberry
from strawberry.fastapi import GraphQLRouter

@strawberry.type
class Agent:
    id: str
    capabilities: List[str]
    status: str
    current_task: Optional["Task"]

@strawberry.type
class Query:
    @strawberry.field
    async def agents(self) -> List[Agent]:
        return await agent_pool.list_agents()

    @strawberry.field
    async def agent(self, id: str) -> Optional[Agent]:
        return await agent_pool.get_agent(id)

@strawberry.type
class Mutation:
    @strawberry.mutation
    async def spawn_agent(self, capabilities: List[str]) -> Agent:
        return await agent_pool.spawn(capabilities)

@strawberry.type
class Subscription:
    @strawberry.subscription
    async def task_updates(self, task_id: str) -> AsyncIterator[TaskUpdate]:
        async for update in task_events(task_id):
            yield update

schema = strawberry.Schema(Query, Mutation, Subscription)
graphql_router = GraphQLRouter(schema)
```

### 12. Agent Marketplace
```python
# engine/marketplace.py
class AgentTemplate(BaseModel):
    """Reusable agent configuration."""
    id: str
    name: str
    description: str
    capabilities: List[str]
    default_config: Dict[str, Any]
    author: str
    version: str
    downloads: int = 0
    rating: float = 0.0

class Marketplace:
    async def publish(self, template: AgentTemplate): ...
    async def search(self, query: str) -> List[AgentTemplate]: ...
    async def install(self, template_id: str) -> Agent: ...
```

---

## Code Quality Improvements

### Current Issues
1. **Inconsistent logging** - Mix of print() and logging
2. **Large files** - Some modules exceed 500 lines
3. **Missing type hints** - Especially in workflow definitions
4. **Duplicate code** - Similar patterns in multiple services

### Refactoring Targets
```
Engine/runtime/
├── agent_executor.py     718 LOC → Split into:
│   ├── executor.py       # Core execution
│   ├── ooda.py           # OODA loop implementation
│   └── decision.py       # Decision tree logic

Engine/substrate/
├── graphsub_client.py    493 LOC → OK (well-factored)
├── redis_mailbox.py      447 LOC → OK
├── capability_tokens.py  492 LOC → Split if adding features
└── stigmergy.py          379 LOC → OK
```

### Testing Requirements
```
tests/
├── unit/
│   ├── test_capability_tokens.py    # Token creation, verification, delegation
│   ├── test_stigmergy.py            # Pheromone deposit, decay, scanning
│   ├── test_ooda.py                 # Decision tree logic
│   └── test_resource_manager.py     # Budget allocation, refunds
├── integration/
│   ├── test_agent_lifecycle.py      # Spawn → Execute → Complete
│   ├── test_multi_agent.py          # Coordination scenarios
│   └── test_workflow_durability.py  # Failure recovery
└── e2e/
    └── test_full_system.py          # Docker compose based
```

---

## Documentation Gaps

### Missing Docs
- [ ] API reference (auto-generated from FastAPI)
- [ ] Agent development tutorial
- [ ] Deployment runbook
- [ ] Troubleshooting guide
- [ ] Performance tuning guide
- [ ] Security audit checklist

### Existing Docs to Update
- [ ] PHASE1_COMPLETE.md → Add lessons learned
- [ ] PHASE2_COMPLETE.md → Add production deployment notes
- [ ] PLATFORM_SPEC.md → Keep in sync with code

---

## Performance Optimizations

### Identified Bottlenecks
1. **Redis Streams** - Consider partitioning for high throughput
2. **GraphSub queries** - Add query result caching
3. **Capability verification** - Cache verified tokens
4. **OODA tick interval** - Make adaptive based on load

### Optimization Targets
| Metric | Current | Target |
|--------|---------|--------|
| Agent spawn time | ~500ms | < 100ms |
| Task assignment | ~200ms | < 50ms |
| Pheromone scan | ~100ms | < 20ms |
| Capability verify | ~50ms | < 5ms (cached) |

### Caching Strategy
```python
# engine/cache.py
from cachetools import TTLCache
from functools import lru_cache

# In-memory caches
capability_cache = TTLCache(maxsize=10000, ttl=300)
pheromone_cache = TTLCache(maxsize=50000, ttl=60)

# Redis-backed caches (distributed)
async def get_or_compute(key: str, compute_fn, ttl: int = 300):
    cached = await redis.get(key)
    if cached:
        return json.loads(cached)
    result = await compute_fn()
    await redis.setex(key, ttl, json.dumps(result))
    return result
```

---

## Security Hardening

### Current Security Model
- Ed25519 capability tokens ✓
- Token delegation with attenuation ✓
- Offline verification ✓

### Missing Security Features
- [ ] Rate limiting per agent/tenant
- [ ] Request signing for inter-service calls
- [ ] Secrets management (Vault integration)
- [ ] Audit logging for security events
- [ ] Input validation for all endpoints
- [ ] Sandboxed execution for untrusted code

### Security Checklist
```python
# engine/security/audit.py
class SecurityAuditLogger:
    async def log_capability_granted(self, token: CapabilityToken, granter: str): ...
    async def log_capability_denied(self, token: CapabilityToken, action: str): ...
    async def log_suspicious_activity(self, agent_id: str, details: Dict): ...
```

---

## Integration Opportunities

### Cross-Repo Synergies
1. **AutonomousCoworker** → Use dsco-autobot Engine as backend
2. **dsco-mcp-integrations** → Integrate 400+ tools via MCP
3. **dsco-graphsub** → Already integrated, enhance with caching
4. **dsco-temporal** → Share workflow patterns

### External Integrations
- [ ] OpenTelemetry collectors
- [ ] Datadog/New Relic APM
- [ ] PagerDuty alerting
- [ ] Slack notifications
- [ ] GitHub Actions for CI/CD

---

## Priority Matrix

| Improvement | Impact | Effort | Priority |
|-------------|--------|--------|----------|
| Health checks | HIGH | LOW | P0 |
| Metrics/tracing | HIGH | MEDIUM | P0 |
| Integration tests | HIGH | MEDIUM | P0 |
| Error handling | MEDIUM | LOW | P1 |
| Docker compose fixes | MEDIUM | LOW | P1 |
| Circuit breakers | MEDIUM | LOW | P1 |
| Multi-tenant | HIGH | HIGH | P2 |
| Event sourcing | MEDIUM | HIGH | P2 |
| GraphQL API | LOW | MEDIUM | P3 |
| Marketplace | LOW | HIGH | P3 |

---

## Next Actions

1. **Today**: Add health check endpoints to Engine
2. **This Week**: Set up Prometheus metrics
3. **Next Week**: Create integration test suite
4. **This Month**: Deploy with full observability stack
