# MCP performance review — 2026-09-04

Scope: local, read-only source investigation and isolated tests; no production deployment, auth changes, or runtime source edits. Existing working-tree modifications were preserved. Trigger: a weather tool was advertised but direct invocation returned `Unknown meta-tool`; wrapping it through execute_tool yielded a job that failed. The inline historical job response is truncated, so its specific authentication cause is not independently established by this review.

## Verified findings and priorities

1. **P0: advertisement/dispatch mismatch.** `integrations/mcp_server.py::_handle_tools_list` advertises durable catalog tools. `_handle_tools_call` dispatches named meta-tools only; other names return `Unknown meta-tool`. An isolated fake-store probe advertised `weather_probe` and reproduced that exact failure without any network execution. Proposed repair: normalize a known catalog tool into the canonical execution envelope at the authenticated request boundary. Preserve caller/tenant identity and existing execution controls; never dispatch directly to a backend. Customer MCP currently intentionally exposes only search_tools and execute_tool; preserve that restriction. Add operator/customer/unknown-name/collision tests before promotion.

2. **P1: warm catalog cache still hits the database.** Ten repeated tools/list requests against an instrumented fake store produced **10 catalog_version calls and 1 query_tools call**. `ToolStore.catalog_version` runs COUNT/MAX under the process-wide connection lock, so a cache hit is not DB-free. `warm_tools_list_snapshot` clears a cache; it does not prewarm a snapshot. The older MCP_PERFORMANCE_PLAN.md claims a DB-free warmed snapshot, which is not the implementation in this checkout. Proposed repair: bounded immutable snapshots with a specified freshness SLA and cross-container invalidation/version polling outside the request hot path. Verify refresh failure behavior, concurrent readers, bounded memory, and registration visibility. Do not simply eliminate freshness checks.

3. **P1: unknown latency can become an unnecessary job.** `core/policy_engine.py` gives unknown-latency tools a stream prior of 1.2 versus sync 0.8. `api/execution_routes.py::execute` degrades selected stream to job when a client requests a collected response. This can force an extra status lookup for a short read. Proposed experiment: compare bounded collected execution against current policy using fast read-only fixtures and held-out slow calls. Do not blindly resubmit work: current sync timeout escalation cancels a coroutine and submits a job, which needs idempotency scrutiny for side effects. Policy changes require separate review.

4. **P2: semantic search repeats full-corpus work.** `embeddings/advanced_tool_index.py::search` obtains a query embedding then synchronously scans all selected rows, reconstructs four vectors per row, computes four cosine similarities and parses JSON, on an async request path. Consider generation-keyed normalized vector matrices, batched scoring, bounded query-embedding caching, and a measured event-loop offload. Verify ranking parity and memory before adoption. No wall-clock speedup measured in this review.

5. **P2: exact name lookup should not need semantic ranking.** Client discovery of job status returned submit_job first in this session. Exact identifier lookup should use deterministic catalog resolution before fuzzy ranking, while free-text queries retain semantic retrieval. The client registry and service search are separate layers; determine which ranked this request before changing the service.

## Verification

- `.venv/bin/python -m pytest tests/test_mcp_full_catalog_advertising.py tests/test_mcp_meta_tools_catalog.py -q`: **9 passed**.
- `.venv/bin/python -m pytest tests/test_customer_mcp_oauth.py tests/test_dsco_cli_api_compat.py tests/test_task_queue_pool.py -q`: **9 passed**.
- Isolated fake-store probe: ten list calls, ten version checks, one page query; advertised catalog tool rejected as unknown meta-tool.
- Existing tests emitted pytest-asyncio/Pydantic/datetime deprecation warnings.

## Acceptance for a follow-on implementation

- Every operator-visible catalog tool dispatches through the existing authorized execution path; tenant restrictions remain unchanged.
- Warm listing performs zero request-path database calls within an explicit freshness window; updates become visible within the agreed SLA.
- Fast collected reads need one execution response, with no extra backend execution on timeout.
- Report p50/p95/p99, database call count, backend invocation count, payload bytes, and error rate separately. Local fake-store call counts are not production latency measurements.

## Receipt

Outcome: completed exploration with reproducible correctness and cache findings; 18 relevant tests passed. Authority: user authorized exploring this checkout for performance ideas. Economics: no paid backend execution or deployment initiated by this review; model cost not measured. Reuse: findings and acceptance criteria retained here. Risk: dirty checkout differs from historical performance documentation; live deployment parity unverified. Next: fix dispatch contract first with tenant-boundary regression tests, then cache request-path latency. This file is the local receipt; no durable-bus receipt was submitted.
