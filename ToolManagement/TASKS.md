# Next 25 Tasks — ToolManagement (auth unification + architectural debt)

**Generated:** 2026-08-31  
**Status 2026-08-31 (autonomous run, pass 2):** T01–T06 ✓ (pass 1). **T19: DISCOVERED ALREADY IMPLEMENTED** — `api/job_routes.py` (561 ln) provides POST /submit (with completion-webhook URL + streaming control state), GET /{job_id}, /{job_id}/stream, /{job_id}/detach, /{job_id}/cancel, list + analytics; ARCHITECTURE_REVIEW.md (2026-01-20) is stale on this point. **T20 ✓ EXECUTED**: worker pool (`QUEUE_WORKERS`, default 4, lock-protected claim caps double-claiming), event-driven pickup (`_wakeup` Event + jittered backoff replaces 1s polling), per-backend circuit breaker (threshold 3 consecutive failures → 30s open, half-open probe, auto-close on success). **T21 PARTIAL ✓**: single pooled httpx.AsyncClient per queue process replaces per-task client creation; per-request timeout preserved. **T22 still open.** Evidence: tests/test_task_queue_pool.py 3/3 (parallel no-double-claim, breaker open/recover/half-open, pooled-client reuse) + durability_test.py 12/13 (the 1 failure is environment-only: it probes a live research service port, connection-refused, unrelated to changes).

**Status 2026-08-31 (autonomous run, pass 1):** T01–T06 executed. T01 ✓ (12-case suite green, pytest path fixed) · T02 ✓ (prod fail-hard on TOKEN_SECRET + default password; dev unchanged) · T03 ✓ (oauth.py mints v2 iss/aud/exp/iat tokens, dual-length decode, identity exposes aud/iss) · T04 ✓ (POST /oauth/introspect live, revocation drill green) · T05 ✓ (auth/scopes.py, 13 audiences + alias resolver) · T06 ✓ code+enforce verified e2e on unit_fabric (401 MCP challenge, valid aud token 200, wrong-aud 403); operator flips UNIFIED_SHADOW=1/UNIFIED_ENFORCE=1 per service. Loopback trust now opt-in (UNIFIED_LOOPBACK_TRUST=1).
**Scope:** post-review work queue grounded in `AUTH_UNIFIED.md` (P1 landed, P2–P4 specified) and `ARCHITECTURE_REVIEW.md` (blocking patterns: hard timeouts, single worker thread, no connection pooling, no background-job pattern).
**Format:** each task has an owner-ready spec, acceptance criteria, and dependencies. Waves are dependency-ordered; tasks inside a wave are parallelizable unless marked ⛓ (chain).

---

## Wave A — Auth core hardening (P1→P2 transition)

**T01 — Promote unified-auth tests into the suite + CI hook**
Move/extend `tests/test_unified_auth.py` (currently green, 11/11): add round-trip tests for `mint_service_token` across every audience name, expired-token case at skew boundary (`exp + skew` exactly), revocation-introspection mock (fail-open + fail-closed paths), and shadow-mode (`enforce=False`) decision logging.
✅ *Accept:* suite green in CI; no `sys.path` hacks (fix conftest).
⛓ dep: none.

**T02 — Kill the `sha256(MCP_PASSWORD)` token-secret fallback (config only)**
In `oauth.py` and `unified.py::_effective_secret`, fail hard (raise at startup) when neither `TOKEN_SECRET` nor a proper key material is configured in `ENV=production`; keep the legacy derivation only under `ENV=development` with a loud warning log.
✅ *Accept:* prod boot refuses to start without explicit secret; dev path unchanged; documented in AUTH_UNIFIED.md.
⛓ dep: T01.

**T03 — Audit-token/compat keys migration plan + `v:2` adoption policy**
Decide the cutover: oauth.py mints v2 tokens (`iss/aud/exp/iat` + compat keys) for *new* logins while legacy tokens age out (30d TTL ⇒ ~30d migration). Add a `legacy-token-count` metric endpoint.
✅ *Accept:* written plan with cutover date; oauth.py issues v2 on new auth-code exchanges; legacy accepted, counted, and expiring naturally.
⛓ dep: T01. ⚠️ touches live issuance path — needs test env verification per repo's own bar.

**T04 — Introspection endpoint (RFC 7662) on the IdP**
Add `POST /oauth/introspect` to oauth.py backed by the existing DuckDB revocation list (token → `{active, revoked, exp, scope, aud, sub}`), with rate limiting and no token-logging.
✅ *Accept:* unified.py's `UNIFIED_INTROSPECTION_URL` works end-to-end in a local integration test; revoked token rejected within ≤60s.
⛓ dep: T03.

**T05 — Per-subdomain audience + scope vocabulary registered in code (single source of truth)**
New `auth/scopes.py`: `SUBDOMAINS = {tools, search, research, publish, memory, swarm, timeline, bolt.n4j, integration_fabric, finance_fabric, unit_fabric, video_qa, doc_qa}`, each with default required scopes and public paths. All services import from it — no hand-rolled configs.
✅ *Accept:* one importable registry; every future subdomain listed by default with conservative scopes.
⛓ dep: none.

## Wave B — Rollout: enforce unified auth per subdomain (shadow → enforce)

Order by traffic risk (lowest first). Each: wire `make_auth_middleware` into the service, run ≥1 day shadow, review deny-lines, flip enforce. ⛓ dep: T01–T05. Each task ⛓ on the previous *enforce flip* (staged, not big-bang).

**T06 — `unit_fabric` (pilot, lowest blast radius)** — shadow→enforce; smoke: 401 shape matches MCP OAuth spec; legit client passes.
**T07 — `timeline_mcp`** — same protocol.
**T08 — `distributed_video_qa` + `distributed_document_qa`** — same protocol.
**T09 — `finance_fabric`** ⚠️ — shadow review must cover tenant BYOK paths (TENANT_CREDENTIALS note: unresolved env-identity bug history); no tenant-facing regression.
**T10 — `integration_fabric`** ⚠️ — same caution; verify weather/econ live branches unaffected by middleware.
**T11 — `distributed_search_mcp` + `distributed_research_mcp`** — bulk of live read traffic; shadow review with real LLM-client replay.
**T12 — `distributed_publish_mcp`** ⚠️ — external-publication semantics; scope `publish`; require operator-identity (not agent-identity) tokens for publish actions.
**T13 — `distributed_memory_mcp` + `distributed_swarm`** — internal state planes; loopback exemption documented and tested.
**T14 — `tools` registry itself (the IdP host)** — protect `/api/v1/*` with `tools` audience; `/oauth/*`, health, docs stay public; this is the last flip (it hosts the issuer).

✅ *Accept (each):* shadow log shows zero false-positive denies on replayed legit traffic; enforce flip recorded with date; revocation drill (revoke token, verify ≤60s propagation or immediate local reject).

## Wave C — P2: asymmetric signing (JWKS), kills shared secret

**T15 — Key management service (KMS-lite)**
`auth/keys.py`: generate Ed25519 keypair (reuse warrants.py primitives), persist private key in DuckDB (or age-encrypted file), publish JWKS (`/.well-known/jwks.json`, `kid`, `use=sig`, rotation-ready with overlapping keys).
✅ *Accept:* JWKS served; `kid`-keyed; rotation runbook in doc; private key never in env.
⛓ dep: T05.

**T16 — oauth.py issues RS/Ed25519 JWTs (same claim set incl. `aud`)**
✅ *Accept:* new tokens verify via JWKS in unified.py; HS256 accepted only during migration window (flagged, counted); `TOKEN_SECRET` no longer needed by verifier.
⛓ dep: T15, T14.

**T17 — unified.py JWKS verification path**
`kid`-keyed fetch+cache (10-min TTL), trust-on-first-use pinned to issuer URL, alg allowlist (no `none`, no key-confusion).
✅ *Accept:* unit tests for alg-confusion and `none`-alg rejection; cache invalidation on unknown `kid`.
⛓ dep: T15. (Do in parallel with T16; integration ⛓ T16.)

**T18 — Remove HMAC secret distribution from all services (fleet sweep)**
Every subdomain config drops `TOKEN_SECRET`/`MCP_PASSWORD` sharing; only the issuer holds signing keys.
✅ *Accept:* grep sweep shows no `TOKEN_SECRET` outside IdP; all enforce-flips stable for 7 days post-cutover.
⛓ dep: T17 + all Wave B flips.

## Wave D — Architectural debt (from ARCHITECTURE_REVIEW.md, critical)

**T19 — Background job spine: `POST /gateway/jobs/submit`, `GET /gateway/jobs/{id}`, `DELETE /gateway/jobs/{id}`, completion webhooks**
Persisted via existing `DurableTaskQueue` (SQLite); job status machine `queued→running→done|failed|cancelled`; webhook on completion with retry.
✅ *Accept:* a >30s tool runs as a job; client polls/webhooks; survives process restart (durable queue).
⛓ dep: none.

**T20 — Worker pool in task_queue.py (N workers + event-driven dispatch)**
Replace single `_process_queue` thread with N workers + `asyncio.Queue` handoff; per-backend concurrency caps; circuit breaker for failing backends.
✅ *Accept:* concurrent-load test (20 jobs, 2 slow backends) shows no head-of-line blocking; breaker trips/reopens correctly.
⛓ dep: T19.

**T21 — Kill hard-coded 30s/60s timeouts; per-tool timeout taxonomy**
MCP server + gateway: default timeout from tool metadata (`timeout_s` field on tool schema), with a global max; httpx clients pooled (single `AsyncClient` per process, reused).
✅ *Accept:* no fixed 30s in hot paths; long-running tools opt into job mode (T19) instead; connection count flat under load.
⛓ dep: T19, T20 (partial parallel ok).

**T22 — Compose/workflow durability: checkpoint intermediate state**
compose_patterns: persist step outputs to the durable store; on mid-pipeline failure, resume from last checkpoint (map-reduce + sequential first, then loop patterns).
✅ *Accept:* kill -9 mid-sequential-pipeline → resume completes with same final output; no re-execution of completed steps.
⛓ dep: T19.

## Wave E — P4: warrants × OAuth fusion (DSCO-native end-state)

**T23 — `POST /oauth/warrants`: mint warrant bound to identity token**
Operator/user-authenticated minting: holder pubkey, capability set, constraints (tool classes: `fs/net/exec/secrets/control`), TTL. Reuses warrants.py `Warrant.mint`.
✅ *Accept:* minted warrant verifies via JWKS-issuer pubkey set; attenuation-only enforced in code path.
⛓ dep: T16.

**T24 — Warrant verification hook in tool execution (gateway)**
`execute_tool` accepts `Authorization: Bearer <identity>` + `X-Warrant: <warrant>`; capability check against warrant constraints; receipt persisted per execution.
✅ *Accept:* out-of-scope tool call denied with receipt; in-scope allowed with receipt; aligns with DSCO runtime capability classes.
⛓ dep: T23, T21.

## Wave F — Ops baseline (README "Next Steps", now cheap on the auth/queue spine)

**T25 — Rate limiting + metrics + admin view (one task, three small parts)**
(a) token-bucket rate limits per token/audience on execute endpoints; (b) Prometheus-style `/metrics` (exec counts, latencies, auth denials by reason, queue depth, breaker state); (c) minimal admin page: jobs queue, auth denial feed, federation nodes.
✅ *Accept:* limits enforced with 429 + `Retry-After`; `/metrics` scraped by local agent; admin page shows live deny-line and queue depth.
⛓ dep: T04 (denial telemetry), T19 (queue depth).

---

## Execution order (critical path)

```
T01 → T02 → T03 → T04 → T05 ─┬→ T06..T14 (staged flips) ─┐
                              ├→ T15 → T17 ─┐            │
T19 → T20 → T21 → T22 (parallel track, no auth dep)       ├→ T18 ←────┘
                                             T23 → T24 → T25
```

**Total: 25 tasks. 2 waves fully parallelizable (D vs A/B/C); the staged enforce flips (T06–T14) are the longest pole — start shadow mode immediately after T05.**
