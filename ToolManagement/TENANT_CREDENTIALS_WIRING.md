# Tenant Credentials — Wiring Plan (BYOK pivot, phase 1)

**Status as of this update:** partial wiring landed, UNVERIFIED by test
execution (bash/python both blocked all session by the capability gate's
lethal-trifecta rule — prior `.env` read + untrusted external API content
this session; override was not requested/granted). Everything below was
verified only by careful manual code inspection, not by running pytest.
Do not treat any of this as done until `pytest tests/test_tenant_credentials.py`
plus a real integration_fabric/finance_fabric smoke test have actually run.

## What is actually landed right now

| File | Change | Confidence |
|---|---|---|
| `services/tenant_credentials.py` | New module: `resolve_credential`, `CredentialResolver`, `FileTenantCredentialStore`, `InMemoryTenantCredentialStore` | Not run; logic reviewed twice, looks correct |
| `tests/test_tenant_credentials.py` | 8 unit tests for the above | **Not executed.** Written to be run the moment exec access returns |
| `native/integration_fabric/server.py` | `_execute_tool` now pops an optional `tenant_id` out of `arguments` and threads it as an explicit parameter through `_live()`. The `weather`/`openweather` branch fully converted to `resolve_credential()`, including `_live_openweather` accepting a resolved key instead of reading the module constant directly. `POOLABLE_CATEGORIES = {"weather", "econ"}` added but **only weather is actually wired** — `econ`/FRED still uses the old `FRED_KEY` module constant unconverted. | Reviewed line-by-line; behavior-preserving when `tenant_id` absent (100% of current traffic) |
| `native/finance_fabric/server.py` | Import + policy comment added (finance categories are never poolable). **Brokerage/Alpaca conversion was attempted, found buggy mid-edit, and reverted.** See "A bug caught and reverted" below. | Reverted change is inert; file behavior is byte-identical to before this session for every domain handler |

## A bug caught and reverted (worth recording, not hiding)

First attempt wired `_h_brokerage`'s `live` flag to check
`resolve_credential(tenant_id, "alpaca", ...)`, but `_alpaca_headers()` — the
method that actually builds the outbound HTTP auth headers — still read
`os.getenv("ALPACA_API_KEY", "")` / `os.getenv("ALPACA_API_SECRET", "")`
directly, unaware of any tenant-resolved key. Net effect: a tenant's own
BYOK key could flip `live=True`, but the real outbound Alpaca API call
would still authenticate as the **operator's** account, not the tenant's —
a live financial API call executed under the wrong identity. This is a
correctness/security bug, not a stylistic issue, and it could not have been
caught without deliberately tracing the call all the way to
`_alpaca_headers()`. It has been reverted; `_h_brokerage` is back to its
original, unmodified env-var gating. The full fix requires resolving the
key once and threading it explicitly into every Alpaca `httpx` call site in
that handler (`account`, `order`, `positions`, etc.) — a larger, must-be-
tested change, tracked as remaining work below, not attempted again without
execution access to verify it.

## Original wiring plan (still the target end-state)

## Why this is split into two commits

`tenant_credentials.py` is net-new, additive, zero blast radius — nothing
imports it yet, so it cannot break anything currently running. It was safe
to write and land without execution access because correctness is
verifiable by inspection (pure functions/dataclasses, no I/O side effects
except the file-backend test, which is exercised by its own test).

Wiring it into `finance_fabric/server.py` and `integration_fabric/server.py`
touches the actual dispatch path for 2,490 live backend tools. That
change must be test-verified before merge per the repo's own bar (and
DSCO's RSI law: "test relevant behavior... independent verifier for
high-blast changes"). It needs `pytest` execution, which is blocked this
session. Treat everything below as a precise, ready-to-apply spec — not a
claim that it's done.

## The problem in the current code

Both fabrics compute live/sandbox eligibility **once, at module import
time**, from the operator's own process env:

```python
# native/finance_fabric/server.py, module level
ALPACA_LIVE = bool(os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_API_SECRET"))
...
# inside a handler:
live = provider == "alpaca" and ALPACA_LIVE
```

```python
# native/integration_fabric/server.py, module level
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
...
# inside _live():
if base and base[2]:   # base[2] is OPENAI_KEY looked up from OPENAI_COMPAT
    return await self._live_openai_chat(args, provider)
```

There is no per-request tenant identity in scope at all — these are
separate FastAPI/MCP services that receive only `arguments` from the
gateway's dispatch call, not the caller's `client_id`/tenant.

## Required changes, in order

### 1. Thread `tenant_id` through the call path (gateway -> fabric)

The gateway (`tools_api.py` / execution routes) already resolves a
`client_id` per request (see `ToolRequest.client_id` used throughout
`billing_hooks.py`). That `client_id` is not currently forwarded in the
HTTP call the gateway makes to each backend fabric's `/execute` (or
equivalent) endpoint.

**Change:** add `tenant_id` (defaulting to `client_id`) to the request
body/headers the gateway sends when proxying to a backend MCP, and accept
it in `NativeMCPBase._execute_tool` (or the specific request model each
fabric uses) so it reaches `IntegrationFabricMCP._execute_tool` /
`FinanceFabricMCP`'s handler methods as an explicit parameter, not a
global.

Verification: a unit test on the gateway's proxy call asserting the
outbound request body includes `tenant_id` when `client_id` is set on the
inbound `ToolRequest`.

### 2. Replace module-level live constants with per-call resolution

Convert e.g.

```python
ALPACA_LIVE = bool(os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_API_SECRET"))
...
live = provider == "alpaca" and ALPACA_LIVE
```

to

```python
from tool_management.services.tenant_credentials import resolve_credential

...
def _alpaca_live(self, tenant_id: Optional[str]) -> bool:
    # Tenant BYOK first; pooled operator key only if explicitly allow-listed
    # for this provider (brokerage/card-issuing should NOT be pooled —
    # see "Which categories may pool" below).
    return resolve_credential(
        tenant_id, "alpaca",
        env_fallback_var="ALPACA_API_KEY",
        allow_pooled_fallback=False,
    ) is not None
...
live = provider == "alpaca" and self._alpaca_live(tenant_id)
```

Same pattern for every `_live_*` branch in `integration_fabric/server.py`'s
`_live()` method — each becomes a `resolve_credential(tenant_id, provider,
env_fallback_var=..., allow_pooled_fallback=<category policy>)` call
instead of a bare module constant.

Verification: extend the existing sandbox-mode assertions (implicit in
current manual smoke tests — there's no automated test file for
`finance_fabric`/`integration_fabric` sandbox responses found yet, which
is itself a gap worth closing) to cover: (a) no tenant credential -> same
sandbox response as today (regression guard), (b) tenant credential
present -> live path attempted.

### 3. Which categories may pool the operator's own key vs. must be strict BYOK

Do not blanket-enable `allow_pooled_fallback=True`. Recommended allow-list
for phase 1, matching the tiering from the earlier BYOK design discussion:

| Category | Pool operator key? | Why |
|---|---|---|
| weather, econ (FRED), market-data reads (AlphaVantage quote/search) | Yes, with a hard per-tenant rate cap | Cheap, read-only, low abuse surface |
| `ai__*` (chat/embed/complete) | Only with strict per-tenant token budget via `BillingManager` | Real $ cost per call, must be metered before pooling |
| brokerage (Alpaca), card issuing, payments (Stripe), banking (Plaid) | **No — strict BYOK only** | Real money movement / PII; operator must never be the credential of record for a tenant's financial actions |
| vcs (GitHub), docs, crm, support, project, esign | Strict BYOK only | Writes to a tenant's real external accounts; pooling makes no sense (whose GitHub repo would it write to?) |

Encode this as a small `POOLABLE_CATEGORIES: set[str]` constant next to
`CATEGORY_OPS` in each fabric's `catalog.py`, and have the `_live()`/handler
call sites pass `allow_pooled_fallback = category in POOLABLE_CATEGORIES`.

### 4. Connect-flow surface (user-facing, later phase)

Once (1)-(3) are wired and tested, expose:

- `POST /api/v1/credentials/{provider}` — tenant sets a BYOK key (goes to
  `tenant_credentials.resolve_credential`'s store; value never echoed back)
- `GET /api/v1/credentials` — list connected providers for the caller's
  tenant (metadata only, via `store.list_providers` / `get_metadata`)
- `DELETE /api/v1/credentials/{provider}` — disconnect
- OAuth providers (GitHub, Slack, Notion, HubSpot, Stripe, Google,
  Microsoft) can reuse `codex_apps_oauth.py`'s existing `KNOWN_PROVIDERS`
  registry almost as-is — swap `CodexAppsVault.set_secret(scope, key, value)`
  for `TenantCredentialStore.set_secret(tenant_id, provider, value)` at the
  token-exchange completion point in that file.

## Acceptance criteria for calling phase 1 done

1. `pytest tests/test_tenant_credentials.py` passes (8 tests, written, **not
   yet run**).
2. New fabric-level tests confirm: same tenant's own key -> live path
   attempted; different tenant, same provider, no key -> still sandbox;
   no regression on today's zero-credential sandbox responses. **Not yet
   written for integration_fabric's weather path — do this before treating
   even that one converted branch as verified.**
3. `tenant_id` visible end-to-end in a manual smoke test: gateway request
   with a distinct client_id -> fabric log/response shows that id reached
   the handler. **Not yet run.**
4. No provider in the "strict BYOK only" row of the table above can ever
   resolve through the operator's env var, even if `allow_pooled_fallback`
   is accidentally passed `True` at a call site — enforce this with a
   fabric-level assertion/test, not just code review discipline.
5. **New, added after this session's brokerage bug:** for every domain where
   a resolved credential is used to gate a `live` flag, grep for every
   downstream call site that builds outbound auth headers/payloads in that
   same handler and confirm each one uses the *same resolved credential*,
   not a bare `os.getenv(...)`. The Alpaca bug above is the exact failure
   mode this check exists to catch — do it explicitly, don't rely on
   remembering to look.

## Immediate next steps (in order, next session with exec access)

1. Run `pytest tests/test_tenant_credentials.py` — fix anything that fails.
2. Add a fabric-level test for integration_fabric's `openweather` path:
   assert tenant credential -> live attempted; no credential -> sandbox
   response unchanged from today's baseline.
3. Only then attempt the Alpaca/brokerage conversion again — this time
   also updating `_alpaca_headers()` (or passing the resolved key/secret
   explicitly into it) so gate and auth headers can never diverge. Write
   the test for that divergence case FIRST (assert wrong-account auth is
   impossible) before writing the implementation.
4. Convert the remaining integration_fabric branches (`ai__*`, `github`,
   `fred`, `media`) to `resolve_credential()` one at a time, each with its
   own before/after test, same discipline as steps 2-3.

## What is explicitly NOT in phase 1

- Hosted (Modal) database-backed `TenantCredentialStore` implementation —
  `FileTenantCredentialStore` is local-dev/single-instance only; a
  Postgres/KMS-backed implementation of the same `TenantCredentialStore`
  protocol is a separate, later patch once phase 1 wiring is verified.
- The `/api/v1/credentials/*` HTTP surface (item 4 above) — store + resolver
  only; the user-facing connect flow is phase 2.
- Billing integration for pooled `ai__*` calls — `BillingManager` already
  exists and is tested; wiring per-call cost metering into the pooled-key
  path is phase 2, gated on phase 1 being verified first.
