# OpenRouter image generation repair — 2026-09-07

## Verified outcome

`openrouter_ai_image_generate` now generates images through OpenRouter's dedicated `/api/v1/images` endpoint. End-to-end MCP job `8b47c1e0-fb0e-41aa-a48f-119f8ca17a9a` completed with `mode=live`, model `openai/gpt-image-2`, one 1536 × 656 PNG, and provider-reported cost **$0.08998**. The original returned image and its usage metadata are in `dsco-heat-trading/assets/brand/heat_trading_gpt_image_2.{png,json}`. The final `heat_trading_logo.png` removes only an unwanted trailing period; the provider original is preserved.

## Root causes and changes

1. The durable job executor's native-worker HTTP transport omitted the server-side bearer header, while the synchronous transport already used it. `src/tool_management/core/streaming.py` now reuses `api.tools_api._backend_headers` for MCP, SSE, stream cancellation, and backend health requests, with execution tenant context. Caller-provided credentials are not used as transport authentication.
2. The registered image-generation operation had no live adapter. `native/integration_fabric/server.py` now calls the dedicated OpenRouter Images API, preserves model/provider pins, returns standard MCP image blocks plus usage metadata, and reports missing credentials/invalid inputs/empty images as errors rather than sandbox success. There are no automatic retries of the billable POST.
3. The worker had no OpenRouter credential. The principal-supplied key was validated with OpenRouter and installed in the new **`distributed-integration-openrouter`** Modal secret containing only `OPENROUTER_API_KEY`. The secret is mounted on the integration worker only. The existing backend bearer and announcement secrets were retained; authentication was not disabled.

`deploy/modal/integration_mcp.py` is also reconciled to the actual production deployment settings (named security/announcement secrets, cryptography dependency, production bearer enforcement), plus the dedicated OpenRouter secret. No credential values belong in this document, tests, or git.

## Deployment and verification

- Distributed Integration: **v13 → v14**.
- Tool Management registry: **v6 → v7**.
- Both deployed using Modal's rolling strategy from isolated copies of deployed source. Only the integration adapter, registry transport, and integration secret mount were changed in production; unrelated dirty checkout files were not promoted.
- **32 targeted tests passed** against the isolated production candidate: image adapter, bearer propagation, BYOK boundaries, streaming/cancellation, and stream fallback/taxonomy.
- Three local collected-response tests also failed against the unchanged production baseline. Those tests cover separate unpublished collected-response behavior and were not folded into this repair. One other collected-response test passed. This is not a claim that the entire repository suite passes.
- Registry and integration health endpoints returned HTTP 200; unauthenticated integration MCP still returned HTTP 401.
- Post-deploy source hashes matched the candidates:
  - Integration adapter: `01577c48397d0d691ca4938217506fc1a7d49f68a142d0da98b6eccbda1fe155`.
  - Registry transport: `4b826d7434562a51a1808b39d6826797671456cf694fe5358a56c9236654c52b`.
- A no-charge empty-prompt smoke call reached the adapter's validation through the MCP job route. A real image call then completed in approximately 52.3 seconds.

## Recovery and boundaries

Restricted source snapshots, source-hash manifests, and the image collector are under `~/.dsco/workspace/artifacts/openrouter-repair-20260908/`. Snapshots were transferred with SHA-256 checks. They are recovery material, not public artifacts.

Previous deployments remain available:

```sh
modal app rollback distributed-integration v13
modal app rollback tool-registry v6
```

Rollback archive integrity was checked; an actual production rollback was not performed. Historical months-old job records were preserved, not cancelled or rewritten. No logo was published and no trading runtime was changed.

## Reuse

For dedicated GPT image generation, discover current models at `https://openrouter.ai/api/v1/images/models`, not only the chat-model list. Call the existing MCP tool with `params.model`, `params.prompt`, `params.n`, `params.quality`, and `params.aspect_ratio`. Collect the durable job and inspect both its outer status and nested `result.isError`, then persist and verify the returned image bytes.
