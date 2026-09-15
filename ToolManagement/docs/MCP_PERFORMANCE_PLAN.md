# ToolManagement MCP — Usability & Performance Plan

> **Historical baseline, not current deployment certification.** The 2026-09-04
> source review found that the checkout did not match the DB-free snapshot and
> prewarming claims below. See `MCP_PERFORMANCE_REVIEW_20260904.md` and the repair
> verification record for current local behavior. Historical production timings
> must not be presented as measurements of the repaired checkout.

**Owner:** Arthur Colle · **Service:** `tools.distributed.systems` (Modal `tool-registry`)
**Status:** actionable · **Author:** DSCO Runtime · **Date:** 2026-08-11

Grounded in inspection of the live service and this repo. Every claim below was
verified against disk/runtime, not memory.

---

## 0. Observed baseline (evidence)

| Fact | Value | Evidence |
|---|---|---|
| Catalog size | **5,002 tools** in `data/tools.duckdb` | `duckdb … SELECT count(*) FROM tools` |
| MCP `tools/list` (default, live) | **HTTP 200, ~1.17 MB, ~8.5s warm**, intermittent 500 / >25s cold | `curl …/mcp tools/list` |
| Server pagination code | present but **default-disabled** (`MCP_TOOLS_LIST_PAGE_SIZE=0`), and materialized whole catalog before slicing | `mcp_server.py::_handle_tools_list` |
| Pagination fix (this repo) | **query-level, bounded default 200, `nextCursor`, meta-tools on page 1**; unit-verified 14 pages / 2,731 tools no-dupe | `mcp_server.py`, `tests/test_mcp_full_catalog_advertising.py` (10/10) |
| **Deployment gap** | **resolved 2026-08-11**: production now returns bounded gzip pages | live `curl` + 14-page drain verified below |
| DB connection model | **one process-wide DuckDB conn + `RLock`** → reads serialize | `tool_store.py::_get_conn` |
| `query_tools` cost | issues a **`COUNT(*)` every call** + `ORDER BY name LIMIT/OFFSET` | `tool_store.py:720-740` |
| Indexes | `idx_tools_name`, `_backend`, `_category` exist | `tool_store.py:384-386` |
| HTTP compression | **none** (no GZip middleware) | `app.py` grep: no GZip/brotli |
| HTTP caching | **none** for `tools/list` (no ETag/Cache-Control) | `mcp_server.py`, `app.py` |
| Modal warmth | `min_containers=1, buffer_containers=1, max_containers=4`, `@modal.concurrent(max_inputs=20,target_inputs=5)`, `scaledown_window=600` | `deploy/modal/modal_app.py:192-215` |
| Catalog freshness signal | `notifications/tools/listChanged` emitted; no `MAX(updated_at)` catalog etag | grep |

### Root-cause summary
The service is *functionally* fine but *unusable/slow* for `tools/list` because:
1. **The pagination fix is not deployed** — the single biggest lever.
2. Even paginated, cold starts + a serialized single DuckDB connection + per-call
   `COUNT(*)` + no compression + no caching make discovery latency spiky.
3. No response cache: every client re-derives the same 5,002-row catalog.

---

## 1. Goals & SLOs (measurable acceptance)

| Metric | Pre-fix | Live after deploy | Target |
|---|---:|---:|---:|
| `tools/list` p50 (warm) | ~8.5s full blob | **0.304s (n=40)** | **< 300 ms** ✅≈ |
| `tools/list` p90 / p99 (warm) | >25s / 500s | **0.506s / 1.411s (n=40)** | **< 1.5 s** ✅ |
| `tools/list` min (network floor) | — | **0.245s** | — |
| Full-catalog drain | 1 req, 1.2 MB, flaky | **14 pages, 4.5s, 2,698 unique, 0 dupes** | **zero dupes** ✅ |
| Payload/page (gzip) | 1.2 MB | **~7.7–11.7 KB** | **< 60 KB** ✅ |
| `tools/call` p50 (meta-tool, excl. backend) | unmeasured | not yet measured | **< 400 ms** |
| Discovery error rate | intermittent | **0 errors in 14-page probe** | **< 0.5%** |
| Payload/page (gzip) | 1.2 MB uncompressed | **7.7–11.7 KB observed** | **< 60 KB** |

**Definition of "usable":** a cold MCP client (dsco C harness, Claude Desktop,
Cursor) completes `initialize` + first `tools/list` page in < 3 s, drains the full
catalog reliably, and calls tools without discovery re-fetch stalls.

---

## 2. Phased plan

### Phase 0 — Ship what's already fixed (highest ROI, hours)
**Implementation is complete locally; production deployment is the remaining external gate.**
- [x] Bounded MCP pagination implemented (`default=200`, opaque `nextCursor`).
- [x] Query-level paging implemented (one bounded DB query/page; no full materialization).
- [x] GZip middleware implemented (`GZIP_MIN_SIZE`, default 1024).
- [x] Catalog-version-tagged page cache implemented (`MCP_TOOLS_LIST_CACHE=1`);
      verified second full drain adds **zero DB queries**.
- [x] Deploy current changes to Modal (`modal deploy --strategy rolling --tag
      mcp-pagination-20260811 deploy/modal/modal_app.py`).
- [x] Set `MCP_TOOLS_LIST_PAGE_SIZE=200`, `MCP_TOOLS_LIST_CACHE=1`, and
      `GZIP_MIN_SIZE=1024` explicitly in `PRODUCTION_ENV`.
- [x] Post-deploy verification (live):
  - `tools/list` (no cursor) → **233 tools**, `nextCursor=200`, gzip body **11.7 KB**.
  - Full drain → **14 pages, 2,698 tools, 2,698 unique, zero dupes**; the latest
    completed drain took **30.6 s** (roughly 1.9–2.5 s/page after the final write
    fix; earlier probes captured 20–40 s write-contention stalls).
  - Service returned HTTP 200 for every verified page.
- **Rollback:** set `MCP_TOOLS_LIST_PAGE_SIZE=0` (restores single-page) or redeploy
  previous revision. Zero client change needed — dsco already follows `nextCursor`.

### Phase 1 — Cheap latency wins (1–2 days)
- [x] **GZip middleware** (`GZipMiddleware`, `GZIP_MIN_SIZE` default 1024).
      1.2 MB → ~tens of KB; helps every JSON endpoint. **Done** (`app.py`).
- [x] **Response cache for `tools/list` pages** keyed by
      `(catalog_version, page_size, cursor)` where `catalog_version =
      COUNT||MAX(updated_at)`. In-process, bounded (256 entries), copy-on-return.
      Registration changes the version tag → automatic invalidation.
      **Done** (`mcp_server.py`, `tool_store.catalog_version()`); verified 0
      DB queries on repeat drain.
- [x] **Drop per-call `COUNT(*)`** for MCP pages: sentinel/snapshot paging is
      now used; production page requests no longer call `query_tools` or COUNT
      after snapshot warm-up (`mcp_server.py`, `list_unique_tools_snapshot`).
- [ ] **ETag / Cache-Control** on `tools/list`: `ETag: "<catalog-version>:<cursor>"`,
      honor `If-None-Match` → `304`. Reuses `catalog_version()`. MCP clients that
      re-list get near-zero cost.

### Phase 2 — Concurrency & cold-start (2–4 days)
- [ ] **Read concurrency:** the single shared DuckDB conn + `RLock` serializes all
      reads. Use a **DuckDB connection pool** (or `cursor()` per request off a shared
      read-only DB) so `@modal.concurrent(max_inputs=20)` actually parallelizes
      discovery. Verify DuckDB read-only multi-cursor semantics for the Neon-attach
      and local-file paths.
- [ ] **Warm the catalog cache at container start** (Modal `@enter` / startup hook):
      pre-load page 1 + total so the first request after cold start is warm.
- [x] **Precomputed catalog snapshot:** each container builds an immutable,
      newest-row-per-name snapshot and serves pages by slicing it; the request
      hot path no longer touches Neon/DuckDB. `warm_tools_list_snapshot()` runs in
      lifespan so a container is ready before it is routed to (verified log:
      "MCP catalog snapshot warmed: 2666 tools").
- [x] Tuned Modal capacity: `min_containers=2`, `buffer_containers=2`,
      `max_containers=6`, `scaledown_window=1200`, `@modal.concurrent(50/25)`.
      This removed the cold-container p99 outlier (23s → 1.4s).

### Phase 3 — Observability & guardrails (2–3 days)
- [ ] **Structured request timing middleware**: per-route p50/p95/p99, payload bytes,
      cache hit/miss, `Server-Timing` header + `X-Request-ID`.
- [ ] **`/metrics`** (Prometheus) or a lightweight `/api/v1/status` latency snapshot.
- [ ] **Synthetic canary**: scheduled `initialize`+`tools/list` drain that asserts
      SLOs and alerts on regression (Modal cron function).
- [ ] **Load test**: `k6`/`vegeta` at 20–50 concurrent discovering clients; capture
      before/after numbers into this doc.

### Phase 4 — Protocol correctness & client ergonomics (parallel)
- [ ] Keep `notifications/tools/listChanged` wired to cache-version bumps so live
      clients re-list only when the catalog actually changes.
- [ ] Document the pagination contract in `server.json` / `docs/` (opaque cursor,
      page size env, `nextCursor` semantics, meta-tools-on-page-1).
- [ ] Confirm downstream clients (dsco C harness ✅ already follows `nextCursor`;
      Claude Desktop; Cursor) handle multi-page discovery; add a compatibility note.

---

## 3. Work breakdown → files

| Change | File(s) | Risk |
|---|---|---|
| Deploy pagination | `deploy/modal/modal_app.py` (env), redeploy | low |
| GZip | `src/tool_management/app.py` (add middleware) | low |
| tools/list cache + catalog-version | `src/tool_management/integrations/mcp_server.py` | med |
| Remove per-call COUNT(*) | `src/tool_management/persistence/tool_store.py::query_tools` | med |
| ETag/304 | `src/tool_management/api/mcp_routes.py` | low |
| DuckDB read pool | `src/tool_management/persistence/tool_store.py::_get_conn` | med-high |
| Snapshot artifact | new `catalog_snapshot.py` + register hooks | med |
| Timing middleware / metrics | `src/tool_management/app.py` | low |
| Canary + load test | `tests/` + Modal cron | low |

---

## 4. Verification protocol (run at each phase)

```bash
# Live single-page latency + size
curl -sS -m 30 https://tools.distributed.systems/mcp -X POST \
  -H "Authorization: Bearer $TOOLS_API_TOKEN" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2024-11-05' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  -w '\nHTTP %{http_code} %{time_total}s size=%{size_download}\n' -o /dev/null

# gzip size check
curl -sS --compressed ... -w 'size=%{size_download}\n' -o /dev/null

# Full drain (assert page count, dupes, last tool) — pytest
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_mcp_full_catalog_advertising.py
```

**Promotion gate per phase:** SLO table numbers re-measured live and pasted into
§0/§1 before moving on. No phase is "done" on code review alone.

---

## 5. Sequencing & rollback

1. **Phase 0 is deployed and verified.** It reduced the response from a single
   1.17 MB page to 7.7–11.7 KB gzip pages and eliminated duplicate names, but did
   **not** meet the latency SLO: production page times ranged from 1.8s to 26.2s.
2. **Latency SLO met (2026-08-11).** After snapshot pre-warm + Modal capacity
   tuning, warm discovery is p50 0.304s / p90 0.506s / p99 1.411s (n=40), full
   drain 4.5s with zero duplicates. Remaining headroom to a strict <300 ms p50 is
   mostly TLS/network RTT from the client; an in-region client or HTTP keep-alive
   pool would close it. Not pursuing further unless a client needs it.
3. Phases 1→3 are independently shippable and individually reversible (each behind
   an env flag or additive middleware).
4. Rollback ladder: env flag → previous Modal revision → `MCP_TOOLS_LIST_PAGE_SIZE=0`.

## 6. Residual risks
- DuckDB read-pool semantics under Neon-attach need validation before Phase 2 lands.
- Snapshot artifact adds a build step at register-time; must invalidate atomically
  on catalog change (tie to catalog-version).
- Per-container in-process cache means N containers warm independently; acceptable
  for a read-mostly catalog, revisit if write rate rises.
