# RuntimeSpec build control plane

The browser handoff is deliberately cross-plane and credential-safe:

1. `distributed.systems` serializes a credential-free `distributed.systems/v1alpha1` `RuntimeSpec` as base64url JSON in `https://tools.distributed.systems/account#runtime-spec=...`.
2. `/account` reads the fragment locally, bounds and parses it, then removes it with `history.replaceState`. The fragment is never sent in an HTTP request.
3. The customer signs into Tool Management. Its `sk-user-*` key is a distinct identity plane; do not send a public-site/API token to `tools.distributed.systems`.
4. The account page calls the authenticated API below and asks the customer to approve the exact digest.

## API

All endpoints require `Authorization: Bearer <ToolManagement sk-user key>` and only expose rows owned by that tenant.

`POST /api/v1/runtime-builds`

Headers: `Idempotency-Key: <8-128 character opaque key>`

```json
{"runtime_spec": {"apiVersion":"distributed.systems/v1alpha1", "kind":"RuntimeSpec"}}
```

The complete RuntimeSpec must satisfy `scripts/cloud_build_factory.py`'s closed schema. Credentials, secret-like fields, activation leases, signing keys, and build contexts are rejected. The response is `202` with a tenant-owned `request_id`, immutable `runtime_spec_sha256`, `status: "pending_approval"`, `approval: "required"`, and status/approval URLs. Reusing an idempotency key with different content returns `409`.

`POST /api/v1/runtime-builds/{request_id}/approve`

Records explicit customer approval. It returns `blocked` with a machine-readable `blocker_code` unless a separately attested private build worker, offline signer, and clean pinned checkout exist. It never fabricates an artifact URL.

`GET /api/v1/runtime-builds/{request_id}` and `GET /api/v1/runtime-builds?limit=50`

Return status metadata and, only after a real worker attests it, an artifact reference. While blocked, the response includes the honest customer-owned build prerequisites: clean checkout, issuer-provided activation lease, and customer-controlled release signing material.

## CORS

Production permits `https://distributed.systems` and `https://www.distributed.systems`, but the preferred flow navigates to the Tool Management account page and makes same-origin requests. No public-site bearer token is compatible with the Tool Management account plane.
