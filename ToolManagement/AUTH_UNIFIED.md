# Unified Authentication Across Subdomains — Design & Roadmap

> Superseded direction (2026-09-01): ZITADEL at
> `auth.distributed.systems` is now the canonical issuer. ToolManagement's
> local OAuth/password system is migration-only. The principal/account/role
> contract and cutover sequence are in
> `docs/UNIFIED_CUSTOMER_IDENTITY.md`.

**Date:** 2026-08-31
**Status:** Phase-1 groundwork landed and tested; T01–T05 executed (suite green), T06 pilot wired with enforce-mode verified end-to-end on unit_fabric (401 MCP-spec challenge / valid aud-bound token passes / wrong-audience 403). Loopback trust is opt-in (`UNIFIED_LOOPBACK_TRUST=1`); internal fabric calls should use minted service tokens. Phases 2–4 specified below.
**Owner:** Distributed Systems, Inc. / DSCO

## Goal

One identity substrate for every `*.distributed.systems` subdomain service —
`tools`, `search`, `research`, `publish`, `memory`, `swarm`, `timeline`,
`bolt.n4j`, fabrics, and every future subdomain — with per-service audience
binding, scopes, revocation, and a clean path to MCP-spec OAuth.

## What already exists (do not rebuild)

- **`src/tool_management/oauth.py`** — a working OAuth 2.0/OIDC authorization
  server ("Sign in with Distributed"): auth code + PKCE for MCP clients
  (RFC 9728 + 8414 + 7591), user accounts, consent, userinfo, refresh tokens,
  durable revocation (DuckDB via `persistence/oauth_store.py`).
- **`src/tool_management/auth/warrants.py`** — Ed25519 warrant system:
  issuer→holder capability grants, constraints, attenuation-only delegation,
  receipts. This is the *capability layer*; unified auth supplies the
  *identity layer* on top of which warrants are minted.
- MCP resource-server challenge is already half-implemented in oauth.py
  (401 + `WWW-Authenticate: resource_metadata=…`).

**Consequence: no Keycloak/Logto/Authentik needed.** We finish our own IdP.

## Known weaknesses this roadmap fixes

| # | Weakness | Fix phase |
|---|---|---|
| 1 | Tokens carry no `aud` — any token works on any subdomain | P1 (done) |
| 2 | Access-token MAC truncated to 16 hex (64-bit) | P1 (v2 format, done) |
| 3 | `TOKEN_SECRET` falls back to `sha256(MCP_PASSWORD)` | P2 (asymmetric keys) |
| 4 | Verification lives inside oauth.py only; other services have ad-hoc auth | P1 (done: shared verifier + middleware) |
| 5 | No JWKS — HMAC shared secret caps rotation | P2 |
| 6 | No chokepoint: TLS/auth policy split between Modal (prod) and launchd (local) | P3 (edge) |
| 7 | Warrants not wired to HTTP entrypoints | P4 |

## Phase 1 — Landed: shared verification + middleware (`auth/unified.py`)

Accepts, in order:
1. **Legacy oauth.py tokens** (`payload.16-hex`) — byte-compatible verification; audience skipped with a debug log so existing MCP clients keep working.
2. **Unified v2 tokens** (`payload.32-hex`) — full HMAC digest, standard claims `iss/aud/exp/iat` + compat keys; minted by `mint_service_token()`.
3. **Standard JWT (HS256)** — so a later issuer upgrade doesn't require re-plumbing every service.
4. *(hook reserved)* RS/Ed25519 JWT via JWKS + warrant attachment.

Enforcement (`verify_request`): signature → revocation (optional remote introspection against oauth.py's durable revocation list, RFC 7662-style, 60s cache) → expiry (60s clock skew) → audience → scopes.

Deployment surface:
- FastAPI: `app.add_middleware(make_auth_middleware(AuthMiddlewareConfig(audience="search", ...)))`
- Per-route: `Depends(require_auth)`
- Plain-HTTP native MCP servers: `await verify_request(authorization_header, audience=..., required_scopes=...)`

Rollout protocol (per subdomain): deploy with `enforce=False` (**shadow mode**, log-only decisions for a day of real traffic) → review deny-lines for false positives → flip `enforce=True`. Health/readiness/docs and `/oauth/*` are always public; loopback (`127.0.0.1`) fabric traffic is exempted *explicitly and identically in every service*.

Tested: 11/11 cases (legacy byte-exact verify, v2 round-trip, audience mismatch, insufficient scope, expiry, tamper, missing bearer, JWT HS256). Test file kept at `/tmp/test_unified.py`; promote to `tests/test_unified_auth.py` on next test-suite pass.

## Phase 2 — Asymmetric signing (kill the shared secret)

1. Generate an Ed25519 or RSA signing keypair; store the private key with oauth.py's durable store; publish JWKS at `/.well-known/jwks.json` with `kid` + `use=sig` (warrants.py already has Ed25519 primitives — reuse).
2. oauth.py begins issuing RS/Ed25519 JWTs (same claim set incl. `aud`); HS256 accepted during the migration window only.
3. `unified.py` verifier gains a JWKS fetch+cache path (kid-keyed, 10-min cache, trust-on-first-use pinned to issuer URL).
4. Rotation: publish overlapping keys, retire old `kid` after all services refresh. No fleet-wide secret distribution, no restart-based rotation.
5. Remove the `sha256(MCP_PASSWORD)` secret-derivation fallback once every service validates via JWKS.

## Phase 3 — Edge unification

- Stand up `auth.distributed.systems` (or keep IdP on `tools` behind `/oauth/*` — decision point) as the only token-issuing surface.
- All subdomain services validate via JWKS only; no service shares `TOKEN_SECRET` anymore.
- Edge (Modal proxy in prod; Caddy/Traefik in front of supervisor locally) terminates TLS and forwards `Authorization`; auth decisions stay in-service (no proxy-level gating) so capability semantics remain in code.

## Phase 4 — Warrants + OAuth fusion (the DSCO-native end-state)

- OAuth identity token answers *who you are*; the warrant answers *what the agent may do*, with attenuation and receipts (warrants.py).
- `POST /oauth/warrants` — a user (or the operator) mints a warrant bound to an agent holder pubkey, constrained (e.g., `tool.execute`, subpath-scoped, TTL-bound).
- MCP clients present `Authorization: Bearer <identity JWT>`; agents present the warrant alongside tool calls; services verify both (identity via JWKS, warrant via issuer pubkey set) and emit receipts.
- This unifies with DSCO runtime's capability-gate model (fs/net/exec/secrets/control classes): subdomain scopes (`tools`, `search`, …) at the transport layer, warrant constraints at the tool layer.

## Subdomain scope vocabulary

| Scope | Grants |
|---|---|
| `tools` | tool registry read/search/execute |
| `tools:admin` | register/update/delete tools |
| `mcp` | MCP protocol sessions |
| `search` / `research` / `publish` / `memory` | respective native MCP fabrics |
| `swarm` | swarm orchestration endpoints |
| `openid profile email` | identity (OIDC) |

## Acceptance criteria for "unified auth across all subdomains"

1. Every HTTP entrypoint of every subdomain service routes through `unified.py` verification (middleware or per-route) with its own `aud`.
2. A token minted for `search` is rejected by `tools` and vice versa.
3. Revocation at oauth.py takes effect fleet-wide within the introspection TTL (≤60s), or immediately via JWKS-free shared verification for internal tokens.
4. Shadow-mode logs show zero false-positive denials on legitimate traffic before each `enforce=True` flip.
5. No service other than the issuer holds anything beyond its JWKS-trusted issuer URL (post-P2).
6. `pytest tests/test_unified_auth.py` green; one smoke test per subdomain proves 401 shape matches MCP OAuth spec.
