# Local MCP repair verification — 2026-09-04

User authority: fix problems identified in MCP_PERFORMANCE_REVIEW_20260904.md. No deployment or credential changes performed. Existing dirty changes preserved.

## Repairs
- mcp_server.py: known catalog names dispatched through existing execute_tool envelope; customer restrictions retained; bounded page cache with 1-second default freshness (MCP_TOOLS_LIST_CACHE_TTL_MS, capped at 60 seconds), explicit invalidation, cache-disable support, store isolation and stale fallback on refresh failures. warm_tools_list_snapshot remains an invalidator, not a full snapshot prewarmer.
- advanced_tool_index.py: normalized matrix cache, batched cosine scoring, TTL/fingerprint refresh and mutation invalidation, LRU byte/entry bounds, exact identifier ordering. Default refresh window 5 seconds; default caps 8 entries and 256 MiB accounted cache storage.
- execution_routes.py: auto collected requests submit one tracked execution and wait boundedly for its terminal result; timeout returns the same job rather than cancelling and resubmitting. Explicit slow/job/stream and callback behavior covered by tests.
- MCP_PERFORMANCE_PLAN.md: historical claims explicitly marked as non-current deployment evidence.

## Coordinator verification
Combined pytest command covering the three new repair suites plus full catalog advertising, meta-tools, customer OAuth, CLI compatibility, queue pooling, policy/recommendations, admission/batch planning, and execution groups: **49 passed in 3.87 seconds**, 89 existing deprecation warnings. git diff --check on all three implementation files: clean.

Search worker additionally measured an offline synthetic 1500-tool, 128-dimension workload: cold rebuild p50 39.43 ms / p95 50.45 ms; warm cached p50 3.44 ms / p95 3.75 ms; accounted cache bytes 6,704,640. This is worker-reported local benchmark evidence, not a production or old-implementation speedup claim.

## Residual boundaries
Production rollout and latency remain unverified. Cache freshness is bounded during healthy refresh, but stale catalog pages may be served when refresh fails. Search caching does not remove remote embedding latency. API-level exact identifier ordering does not certify the separate C runtime's discovery ranking. Backend credential failure from the historical weather job was not independently diagnosed here and is not claimed fixed. Existing complete repository test suite was not run; only the 49 relevant tests above.

## Recovery / economics / receipt
Initial three native workers all hit provider HTTP-0 timeouts without successful implementation. Retried on three available Codex executor workers with isolated file scopes; all completed. Runtime reported zero estimated metered tool cost; subscription usage is not a measured zero total cost. No paid external backend invocation initiated for tests. This local receipt records authority, outcome, verification, reuse and residual risk; no durable-bus delivery claimed.

Rollback: reverse only this task's reviewed hunks and remove its three new test files; do not reset the dirty checkout. Runtime knobs permit disabling catalog caching and setting matrix cache entry/byte caps to zero. Production promotion requires destination-aware approval and live verification.
