"""Public landing page for tools.distributed.systems.

One page, no theater: this is a tool router. We maintain a registry and a
meta-router; you integrate your servers with it via the Tool Management
client. Self-contained HTML so the deployment does not need an asset build.
"""

from __future__ import annotations

import html
import json


def render_landing(*, tool_count: int, backend_count: int, healthy_backends: int) -> str:
    """Render the public front door with live registry counts."""
    stats = json.dumps(
        {
            "tool_count": int(tool_count),
            "backend_count": int(backend_count),
            "healthy_backends": int(healthy_backends),
        }
    )
    return f"""<!doctype html>
<!-- mcp-name: systems.distributed/tools -->
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Distributed Systems — Tool Router</title>
<style>
  :root {{ --ink:#16181d; --mut:#6b7280; --line:#e5e7eb; --accent:#2563eb; }}
  * {{ box-sizing:border-box; margin:0; }}
  body {{ font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         color:var(--ink); background:#fff; max-width:46rem; margin:0 auto; padding:4rem 1.5rem 5rem; }}
  h1 {{ font-size:1.9rem; letter-spacing:-0.02em; margin-bottom:.35rem; }}
  .sub {{ color:var(--mut); margin-bottom:2.2rem; }}
  h2 {{ font-size:1.05rem; text-transform:uppercase; letter-spacing:.06em; color:var(--mut); margin:2.4rem 0 .8rem; }}
  p, li {{ margin-bottom:.8rem; }}
  code, pre {{ font:0.86em/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }}
  code {{ background:#f3f4f6; padding:.12em .38em; border-radius:4px; }}
  pre {{ background:#f8f9fa; border:1px solid var(--line); border-radius:8px; padding:1rem 1.15rem;
        overflow-x:auto; margin:1rem 0 1.4rem; }}
  pre code {{ background:none; padding:0; }}
  ul {{ padding-left:1.25rem; }}
  table {{ border-collapse:collapse; width:100%; margin:.6rem 0 1rem; }}
  td, th {{ border-top:1px solid var(--line); padding:.45rem .75rem .45rem 0; text-align:left; vertical-align:top; }}
  th {{ color:var(--mut); font-weight:600; }}
  a {{ color:var(--accent); text-decoration:none; }}
  a:hover {{ text-decoration:underline; }}
  footer {{ margin-top:3rem; padding-top:1.2rem; border-top:1px solid var(--line);
            color:var(--mut); font-size:.88rem; }}
</style>
</head>
<body>

<h1>Tool Router</h1>
<p class="sub">Distributed Systems &middot; <code>tools.distributed.systems</code></p>

<p>We maintain a tool router here. It is a registry plus a meta-router: backend
servers register their tools once, and every registered tool becomes globally
searchable and executable through a single endpoint.</p>

<p>This service predates MCP. The protocol layer was added later as a binding;
the registry and routing core were built first and remain independent of any
single protocol.</p>

<h2>Integrate your server</h2>

<p>Use the Tool Management client (<code>tool_management.client</code>) to
integrate your servers with the meta-router. Point it at your service, declare
your tools, register:</p>

<pre><code>from tool_management.client import ToolManagementClient, RegisteredTool

client = ToolManagementClient(base_url="https://tools.distributed.systems")

await client.register_service()          # announce your backend
await client.register_tool(RegisteredTool(
    name="search",
    description="Search your domain",
    input_schema={{"query": {{"type": "string"}}}},
    provider_id="your-backend",
    provider_url="https://your-backend.example.com",
))</code></pre>

<p>Registered tools are immediately available through semantic search
(<code>/api/v1/tools/search</code>) and execution
(<code>/api/v1/tools/execute</code>). Native MCP servers under
<code>native/</code> use this same path.</p>

<h2>Live status</h2>
<table>
  <tr><th scope="row">Registered tools</th><td id="tool_count">&hellip;</td></tr>
  <tr><th scope="row">Backends</th><td id="backend_count">&hellip;</td></tr>
  <tr><th scope="row">Healthy backends</th><td id="healthy_backends">&hellip;</td></tr>
</table>
<script>
(function () {{
  var stats = {stats};
  document.getElementById('tool_count').textContent =
    stats.tool_count.toLocaleString('en-US');
  document.getElementById('backend_count').textContent = stats.backend_count;
  document.getElementById('healthy_backends').textContent = stats.healthy_backends;
}})();
</script>

<h2>Endpoints</h2>
<ul>
  <li><a href="/health"><code>/health</code></a> &mdash; liveness and registry counts</li>
  <li><a href="/docs"><code>/docs</code></a> &mdash; OpenAPI documentation</li>
  <li><a href="/api/v1/status"><code>/api/v1/status</code></a> &mdash; detailed backend status</li>
</ul>

<footer>Distributed Systems, Inc. &middot; local-first infrastructure &middot;
<a href="https://github.com/arthurcolle/dsco-autobot">source</a></footer>

</body>
</html>"""
