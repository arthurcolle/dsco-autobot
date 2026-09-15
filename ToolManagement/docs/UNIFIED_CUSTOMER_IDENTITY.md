# Unified customer identity contract

Status: first ToolManagement consumer slice implemented. The canonical issuer
is live at `https://auth.distributed.systems`; the broader customer control
plane belongs at `https://api.distributed.systems`.

## Non-negotiable identity boundaries

- ZITADEL authenticates humans and services. Products do not store another
  password or issue another human identity.
- A principal is `(issuer, subject)`. Email is a verified, mutable profile
  attribute and is never a database key.
- A customer account is an economic and tenant boundary. It is never a human
  subject ID.
- One principal may belong to many accounts. Requests select an account using
  `X-DSCO-Account-ID`; the value must be present in the authenticated
  principal's authorized account set.
- Stripe, Mercury, CRM, and provider account IDs are external links to a
  customer account. They are not authentication identities.
- Router provider OAuth remains a customer-owned upstream connection. It is
  distinct from signing into Distributed Systems.

## Canonical control-plane records

The broader API should own these durable records in PostgreSQL/Neon:

| Record | Required fields |
|---|---|
| `principal` | `id`, `issuer`, `subject`, `type`, `status`, email/display snapshots |
| `customer_account` | `id`, `slug`, `display_name`, `legal_name`, `type`, `status`, `zitadel_org_id` |
| `account_membership` | `account_id`, `principal_id`, `role`, `status`, timestamps |
| `entitlement` | `account_id`, `product`, `capability`, `plan`, `status`, limits, start/end |
| `external_account` | `account_id`, `provider`, `kind`, `external_id`, metadata |
| `oauth_client` | `product`, `zitadel_client_id`, redirect URIs, audiences, status |
| `audit_event` | actor principal, account, action, resource, decision, request/trace IDs |

Unique constraints:

- `(issuer, subject)` on principals
- `(account_id, principal_id)` on memberships
- `(account_id, product, capability)` on entitlements
- `(provider, kind, external_id)` on external accounts

## Roles and capabilities

Platform roles are deliberately rare:

- `platform.creator`
- `platform.admin`
- `platform.support`

Account roles:

- `owner`
- `admin`
- `billing`
- `developer`
- `viewer`

Entitlements are product capabilities, not roles:

- `router.invoke`
- `tools.discover`
- `tools.execute`
- `graphsub.read`
- `graphsub.write`
- `cli.login`

## Arthur bootstrap

The initialized ZITADEL instance already has the verified Arthur human
(`arthur@distributed.systems`, initial login name `admin`) in the Distributed
Systems organization. For the first production cut:

1. Complete the forced password change and enroll MFA/passkey on that human.
2. Create a separate, offline break-glass administrator before reducing the
   initial human's instance-wide privileges.
3. Create the `Distributed Systems, Inc.` customer account using the ZITADEL
   organization ID as its immutable authentication-side account ID.
4. Bind Arthur's `(https://auth.distributed.systems, sub)` principal to that
   account as `owner`.
5. Assign `platform.creator` and `platform.admin` in the DSCO platform project.
6. Give the internal account explicit product entitlements. Do not implement
   an email-domain or hardcoded-ID bypass.

## Product consumption

All products validate `RS256`, exact issuer, expiry, and an allowed audience
using `https://auth.distributed.systems/oauth/v2/keys`. Mutable membership and
entitlement decisions come from the account control plane; short safe caches
may be used, but authorization failures fail closed.

- `tools.distributed.systems`: this repository. Canonical JWT validation and
  account selection are implemented in `auth/unified.py`. Local passwords and
  HMAC tokens remain migration-only; new local signup is disabled in
  production.
- `router.distributed.systems`: validate the same principal/account contract;
  keep upstream model-provider OAuth connections attached to the account.
- GraphSub: validate the same contract and scope every graph query/mutation by
  account.
- `dsco-cli`: use Authorization Code + PKCE (loopback) or Device Authorization;
  never collect the user's password. Store refresh material in the OS keychain.

## Migration sequence

1. Register one DSCO platform project and product clients in ZITADEL. Configure
   JWT access tokens and exact product audiences.
2. Provision the canonical principal/account records and Arthur's roles.
3. Run ToolManagement in dual-validation mode. Existing `sk-user-*` and HMAC
   tokens map through an explicit legacy account binding only.
4. Link existing Tools users to canonical principals through an authenticated
   account-link flow. Never auto-link on email alone.
5. Cut product UIs and CLI to ZITADEL, stop local signup, then stop local token
   issuance after the migration window.
6. Remove `UNIFIED_ALLOW_LEGACY`, `UNIFIED_ALLOW_HS256`, and
   `UNIFIED_LEGACY_SUBJECT_IS_ACCOUNT`; rotate the legacy secrets after clients
   have migrated.
