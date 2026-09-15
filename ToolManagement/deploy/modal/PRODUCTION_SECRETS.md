# Production secret contract

The Modal deployment intentionally has no credential defaults. Create or rotate
these secrets before deploying the public commercial surface.

## `tool-registry-security` (mounted identically on registry and workers)

- `BACKEND_BEARER_TOKEN`: shared bearer for registry-to-worker MCP calls.
- `BYOK_ENVELOPE_KEY_INTEGRATION_FABRIC`: 128/192/256-bit AES key, encoded as
  base64 or hex. The registry and Distributed Integration must receive the
  exact same value; it encrypts short-lived, backend-bound tenant BYOK envelopes.
- `BYOK_ENVELOPE_KEY_DISTRIBUTED_PUBLISH_MCP`: a separate 128/192/256-bit AES
  key, encoded as base64 or hex. The registry and Distributed Publish worker
  must receive the exact same value. Never reuse the Integration Fabric key.
- `PUBLISH_AUTH_WEBHOOK_SIGNING_KEY`: high-entropy root key used by the registry
  to derive a unique signing secret for each Substack authorization webhook.
- `REGISTRY_ANNOUNCE_TOKEN`

Never reuse `BACKEND_BEARER_TOKEN` as a customer, operator, or announcement token.

## Dedicated Codex Apps vault secret: `tool-registry-codex-apps-vault`

- `CODEX_VAULT_MASTER_KEY`: a high-entropy secret used only to encrypt the
  persistent Codex Apps connector/OAuth credential vault. It is mandatory in
  production; the registry refuses to start without it.

Provision it separately from provider and platform credentials, for example:

```sh
modal secret create tool-registry-codex-apps-vault CODEX_VAULT_MASTER_KEY="$(openssl rand -base64 48)"
```

Do not rotate this value without a migration plan for existing encrypted vault
contents. A new value intentionally cannot decrypt records written by the old
one.

## Registry application secrets

- `DEFAULT_BEARER_TOKEN`
- `INTERNAL_API_BEARER_TOKEN`
- `MCP_PASSWORD`
- `TOKEN_SECRET`
- `TENANT_VAULT_MASTER_KEY`
- `STRIPE_SECRET_KEY`
- `STRIPE_PRICE_ID`
- `STRIPE_WEBHOOK_SECRET`
- `STRIPE_API_VERSION` (currently `2026-07-29.dahlia`)
- `TOOLMANAGEMENT_EMAIL_BEARER_TOKEN`
- `EMAIL_ALLOWED_TENANT_IDS` (owner user id only until mailbox isolation exists)

Keep `BILLING_DEV_MODE` absent in production.

## `tool-registry-postgres` — Postgres credit ledger

This dedicated secret must contain `POSTGRES_URL`. Prepaid balances,
idempotency keys, and ledger entries use Postgres atomic updates; the rest of
the billing catalog continues to use the persistent registry volume during the
staged migration. Use a dedicated database/role rather than sharing an
application owner URL.

## Graph substrate

The registry defaults to `GRAPH_SUBSTRATE=graphsub`. Set `GRAPHSUB_URL` to a
dedicated, authenticated DSCO GraphSub instance (not the public marketing
domain) and set `GRAPHSUB_BEARER_TOKEN` when that instance requires it. Leave
`NEO4J_URL` unused unless a temporary compatibility projection is explicitly
required with `GRAPH_SUBSTRATE=neo4j_compat`; GraphSub does not silently fall
back to Neo4j.

## Worker secrets

- Existing `tool-registry-secrets`: `JINA_API_KEY`, `PARALLEL_API_KEY`
- Existing `dsc-platform-tool-management`: `TOOL_MANAGEMENT_AUTHORIZATION`
- `distributed-integration-production`: only the provider credentials intentionally
  available to paid managed add-ons

Existing provider-specific secrets such as `anthropic-secret` and
`openai-secret` remain separate. Do not mount `substack-credentials` in the
shared Distributed Publish worker; production customer sessions must arrive
only through backend-bound tenant envelopes.

Distributed Publish additionally requires the dedicated
`distributed-publish-openrouter` secret containing only `OPENROUTER_API_KEY`.
Do not mount the broader registry provider secret into the publish worker.

## Distributed Email

Add these keys to the existing `email-mcp-secrets` secret used by
`/Users/arthurcolle/protonmail-bridge/modal_app.py`:

- `TOOLMANAGEMENT_ANNOUNCE_TOKEN` (same value as `REGISTRY_ANNOUNCE_TOKEN`)
- `TOOLMANAGEMENT_EMAIL_BEARER_TOKEN` (same value as the registry copy)
- `TOOLMANAGEMENT_URL=https://tools.distributed.systems`
- `EMAIL_MCP_PUBLIC_URL=https://arthurcolle--email-mcp-emailmcp-serve.modal.run`
- `EMAIL_ALLOWED_TENANT_IDS` (owner user id only)

Leave `EMAIL_DYNAMIC_OAUTH_ENABLED` and `EMAIL_OAUTH_AUTO_APPROVE` unset/false.

## Stripe event destination

Configure Stripe to send these events to
`https://tools.distributed.systems/api/v1/billing/webhook`:

- `checkout.session.completed`
- `invoice.paid`
- `invoice.payment_failed`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
