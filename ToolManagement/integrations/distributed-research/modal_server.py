"""
Modal deployment for distributed-research (Parallel AI MCP Server)
================================================================

Deploy:
    modal deploy modal_server.py

The server exposes:
- /mcp          - Streamable HTTP MCP
- /mcp/sse      - SSE MCP endpoint
- /v1beta/*     - Parallel AI API proxies
"""

import modal

# Modal app
app = modal.App("distributed-research-mcp")

# Image with all dependencies
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi>=0.109.0",
        "fastapi-mcp>=0.4.0",
        "uvicorn[standard]>=0.31.0",
        "pydantic>=2.11.0,<3.0.0",
        "pydantic-settings>=2.5.2",
        "httpx>=0.28.1",
        "aiohttp>=3.10",
        "python-dotenv>=1.0.0",
        "structlog>=23.2.0",
        "sse-starlette>=1.6.1",
        "parallel-web>=0.1.0",
        "psutil>=5.9",
        "libsql-experimental>=0.0.55",
        "tiktoken>=0.5.0",
        "anthropic>=0.40.0",
        "mcp",
    )
)

# Secrets (updated with fresh keys)
secrets = [modal.Secret.from_name("distributed-systems")]


@app.function(
    image=image,
    secrets=secrets,
    keep_warm=1,
    timeout=600,
)
@modal.asgi_app()
def serve():
    from parallel_websearch_server.app import app as fastapi_app
    return fastapi_app


if __name__ == "__main__":
    import uvicorn
    from parallel_websearch_server.app import app as fastapi_app

    uvicorn.run(fastapi_app, host="0.0.0.0", port=4002)
