# Codex Apps MCP Server

Exposes the ToolManagement Codex Apps catalog to Claude via the Model Context Protocol.

## Tools

- `codex_list_connectors` — browse the catalog
- `codex_search_connectors` — search by keyword
- `codex_get_connector` — full connector metadata
- `codex_activate_connector` — activate a connector
- `codex_deactivate_connector` — deactivate a connector
- `codex_list_activated` — list active connectors
- `codex_authorize_connector` — store a token/API key
- `codex_proxy_call` — make a proxied API call through an activated connector

## Claude Desktop config

```json
{
  "mcpServers": {
    "codex-apps": {
      "command": "bash",
      "args": ["/Users/arthurcolle/Dsco/dsco-autobot/ToolManagement/start_codex_apps_mcp.sh"],
      "env": {
        "TOOLMANAGEMENT_API_URL": "https://tools.distributed.systems",
        "TOOLMANAGEMENT_BEARER_TOKEN": "<from secret manager>"
      }
    }
  }
}
```
