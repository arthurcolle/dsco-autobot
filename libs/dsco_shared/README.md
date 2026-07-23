# dsco-shared

Shared distributed-systems primitives for the dsco-autobot services. One package,
editable-installed into every service (`pip install -e ../libs/dsco_shared`).

**Phase 1 (this revision):** addressing only — `dsco_shared.config.service_registry`
is the single source of truth for cross-service URLs.

Later phases add: resilient HTTP transport + typed service clients, OpenTelemetry
+ Prometheus + structlog observability, Ed25519 capability-token auth, Redis
Streams event bus, and Postgres outbox / ownership helpers.
