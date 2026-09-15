# Tool Management on demand in Codex

Configure Codex with `scripts/tool_management_mcp.py` as its `distributed-tools`
stdio server. The script uses only Python's standard library and must be run
directly, without importing the ToolManagement server package. Initialization,
tool listing and ping do not read credentials, perform DNS/HTTP requests, start
integration processes, or load the server runtime.

The two advertised tools are:

- `search_tools`: keyword discovery through `POST /api/v1/tools/query`, returning
  backend-qualified IDs and full input schemas. Use a short keyword, exact tool
  name, or the optional `backend_id` filter. Results are bounded and paginated.
- `execute_tool`: execution through `POST /api/v1/tools/execute`, preserving
  arguments, timeout, idempotency key, trace ID and session ID. The hosted API
  retains authentication, tenant binding, routing and policy enforcement.

Tool calls run concurrently with up to eight workers. Local ping and discovery
of these two entry points remain responsive during an upstream request. Network
errors are returned for the individual call. Executions are not automatically
retried, and credential-bearing HTTP requests do not follow redirects.

## Configuration

Place the existing Tool Management API credentials in
`~/.config/dsco/tool-management.json`, with mode `0600`. For environment-based
credentials, the file can contain:

```json
{
  "api_url": "https://tools.distributed.systems",
  "bearer_token_env_var": "TOOL_MANAGEMENT_API_KEY"
}
```

The client also accepts `http_headers` and `env_http_headers`. Credentials are
read only when a tool is called. The optional first script argument selects a
different configuration file. Credentials must belong to the configured API.

Use absolute paths in `~/.codex/config.toml`:

```toml
[mcp_servers.distributed-tools]
command = "/absolute/path/to/python3"
args = ["/absolute/path/to/ToolManagement/scripts/tool_management_mcp.py"]
enabled = true
startup_timeout_sec = 5
tool_timeout_sec = 660
```

If using the environment credential example, add
`env_vars = ["TOOL_MANAGEMENT_API_KEY"]` to that section. Disable direct
`document-qa`, `video-qa`, and `openrouter` MCP entries with `enabled = false`
after verifying their Tool Management integration tools. Retain the entries for
rollback. Back up the Codex configuration before editing it. These changes take
effect on the next Codex launch.

## Verification on 2026-09-08

- The Python MCP SDK initialized the real stdio subprocess and listed exactly
  two tools in 26 ms. A separate process test initialized successfully with a
  nonexistent credential file.
- `search_tools` returned schemas for `list_documents`, `list_videos`, and
  `openrouter_ai_chat` from the deployed API.
- Document and video listing executed through the canonical API successfully.
- `distributed-integration:openrouter_ai_chat` returned a live OpenRouter
  completion from `openai/gpt-4o-mini`, using 13 tokens. Reported provider cost
  was $0.0000028215.
- Installed Codex 0.153.4 completed an actual `distributed-tools.search_tools`
  call and returned `distributed-document-qa-mcp:list_documents`.
- Focused tests passed: `tests/test_lazy_tool_management_mcp.py` and
  `tests/test_customer_mcp_oauth.py` (16 tests).

## Direct HTTP endpoint repair

The deployed `/mcp` returned HTTP 200 with `{"status":"ok"}` for an initialized
notification. [MCP Streamable HTTP requires an empty HTTP 202 response for
accepted notifications](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#sending-messages-to-the-server).
`src/tool_management/api/mcp_routes.py` now returns the required response and has
a regression test. That server change is local and has not been deployed. The
configured stdio client talks directly to the REST API and requires no server
deployment to work.
