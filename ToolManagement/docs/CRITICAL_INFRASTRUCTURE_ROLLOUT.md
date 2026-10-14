# Critical infrastructure release verification

`scripts/verify_mcp_rollout.py` is an operation-level, read-only release gate.
It does not deploy a release and does not infer provider readiness from a
heartbeat or tool catalog. It rejects synthetic results, protocol errors,
missing result contracts, missing declared features, and incomplete server
inventory. It never retries a tool invocation. Remote endpoints must use HTTPS;
tokens are read from named environment variables and are not written to reports.

Run it on the fleet operator machine after applying the tested changes through
the existing fleet deployment process:

```sh
python scripts/verify_mcp_rollout.py /path/to/production-coverage.json
```

The manifest must have `expected_server_ids` containing the complete deployment
inventory and a `servers` entry for every id. Each server entry contains:

- `id`, `url` (the actual MCP endpoint), and optional `token_env`.
- Optional `expected_version`, checked against MCP initialization.
- `required_features`: a list drawn from `tools`, `prompts`, and `resources`.
- `probes`: explicit operator-approved read operations with `tool`, `effect`
  set to `read`, `arguments`, and nonempty `equals` result contracts. Equality
  keys use dot-separated JSON object paths. Provider probes should assert
  `mode: live` and concrete output/provenance fields.
- `prompts`: prompt-get parameters including `name` and `arguments`.
- `skill_uris`: actual skill resource URIs to read and verify as nonempty text.

Include discovery/lookup, classification, and reranking probes with valid known
fixtures; include provider operations for every production integration rather
than only metadata tools. The verifier checks every advertised list method and
each specified prompt and skill resource. A zero exit code proves only the
explicit manifest contracts at the time of the run. It is not proof of all
unlisted operations or future availability. Write operations require a separate
controlled test account and are deliberately excluded from this verifier.

Before deployment, reconcile the production checkout against the patch base.
Do not replace a newer deployed file with an older repository copy. Keep the
previous release available, apply changes through the established fleet rollout,
and run the gate against all servers. A failed gate must block release completion.
Missing credentials, provider bindings, or live adapters are deployment blockers,
not a reason to accept sandbox results.

## Current source verification boundary

The repair tests exercise the actual streaming executor with an HTTP test
transport, search/classification modules with deterministic dependency fakes,
and the MCP handlers with isolated imports. They verify error identity,
replay prevention, retrieval fallback, ranking validation, and protocol dispatch.
They do not establish production provider access. Full checkout tests and the
production coverage gate are required before declaring the Hetzner rollout done.
