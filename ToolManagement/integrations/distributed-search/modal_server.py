"""
Minimal Jina MCP Server for Modal
=================================
Proxies Jina AI tools (search, read, embed, rerank) with OAuth support.

Deploy: modal deploy modal_server.py
Local:  modal serve modal_server.py
"""

import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import asyncio
import modal
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

# Modal app
app = modal.App("jina-mcp-server")

# Modal image with dependencies
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "fastapi",
    "uvicorn",
    "httpx",
    "httpx-sse",
    "mcp[cli]",
    "pydantic",
    "python-jose[cryptography]",
    "passlib[bcrypt]",
)


# =============================================================================
# Request Models
# =============================================================================

class SearchRequest(BaseModel):
    query: str

class ReadRequest(BaseModel):
    url: str
    selector: Optional[str] = None

class EmbedRequest(BaseModel):
    input: List[str]

class RerankRequest(BaseModel):
    query: str
    documents: List[str]
    top_k: Optional[int] = None

class ToolCallRequest(BaseModel):
    args: Dict = {}


# =============================================================================
# OAuth / Auth
# =============================================================================

security = HTTPBearer(auto_error=False)

def get_valid_tokens() -> set:
    """Get valid API tokens from Modal secrets"""
    tokens_str = os.environ.get("API_TOKENS", "")
    if not tokens_str:
        return set()
    return set(t.strip() for t in tokens_str.split(",") if t.strip())


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    """Verify bearer token"""
    # Check if auth is disabled (for development)
    if os.environ.get("AUTH_DISABLED", "").lower() == "true":
        return "anonymous"

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    valid_tokens = get_valid_tokens()

    # Also check JINA_API_KEY as a valid token for simplicity
    jina_key = os.environ.get("JINA_API_KEY", "")
    if jina_key:
        valid_tokens.add(jina_key)

    if token not in valid_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return token


# =============================================================================
# MCP Connection
# =============================================================================

@dataclass
class MCPConnection:
    session: Any
    tools: List[Dict]
    client_ctx: Any = None

mcp_conn: Optional[MCPConnection] = None


async def call_tool_impl(session, name: str, args: Dict, timeout: float = 60.0) -> str:
    """Internal: Call MCP tool and return result"""
    try:
        result = await asyncio.wait_for(session.call_tool(name, args), timeout=timeout)
    except asyncio.TimeoutError:
        return json.dumps({"error": f"Tool '{name}' timed out after {timeout}s"})
    except Exception as e:
        raise  # Re-raise for retry logic

    if isinstance(result, str):
        return result

    content = result.get("content", []) if isinstance(result, dict) else getattr(result, "content", [])
    if not content:
        return json.dumps(result) if isinstance(result, dict) else str(result)

    parts = []
    for item in content:
        if isinstance(item, dict):
            parts.append(item.get("text", "") if item.get("type") == "text" else json.dumps(item))
        else:
            parts.append(getattr(item, "text", str(item)))

    return "\n".join(parts)


async def call_tool(name: str, args: Dict, timeout: float = 60.0) -> str:
    """Call MCP tool with automatic reconnection on failure"""
    conn = await get_mcp()
    try:
        return await call_tool_impl(conn.session, name, args, timeout)
    except Exception as e:
        # Connection might be stale, try reconnecting once
        print(f"Tool call failed, reconnecting: {e}")
        conn = await get_mcp(force_reconnect=True)
        try:
            return await call_tool_impl(conn.session, name, args, timeout)
        except Exception as e2:
            return json.dumps({"error": str(e2)})


async def _init_mcp() -> MCPConnection:
    """Internal: initialize MCP connection"""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    jina_key = os.environ.get("JINA_API_KEY")
    if not jina_key:
        raise HTTPException(503, "JINA_API_KEY not configured")

    mcp_url = os.environ.get("JINA_MCP_URL", "https://mcp.jina.ai/v1")
    headers = {"Authorization": f"Bearer {jina_key}"}

    client_ctx = streamablehttp_client(mcp_url, headers=headers)
    read, write, _ = await client_ctx.__aenter__()

    session = ClientSession(read, write)
    await session.__aenter__()
    await session.initialize()

    resp = await session.list_tools()
    items = resp.get("tools") if isinstance(resp, dict) else getattr(resp, "tools", resp)

    tools = []
    for t in (items or []):
        if isinstance(t, dict):
            name = t.get("name")
            desc = t.get("description", "")
            schema = t.get("input_schema", {})
        else:
            name = getattr(t, "name", None)
            desc = getattr(t, "description", "")
            schema = getattr(t, "input_schema", {})
        tools.append({"name": name, "description": desc, "input_schema": schema})

    return MCPConnection(session=session, tools=tools, client_ctx=client_ctx)


async def get_mcp(force_reconnect: bool = False) -> MCPConnection:
    """Get or create MCP connection with 30s timeout"""
    global mcp_conn

    if mcp_conn is not None and not force_reconnect:
        return mcp_conn

    # Close existing connection if reconnecting
    if mcp_conn and mcp_conn.client_ctx:
        try:
            await mcp_conn.client_ctx.__aexit__(None, None, None)
        except:
            pass
        mcp_conn = None

    try:
        mcp_conn = await asyncio.wait_for(_init_mcp(), timeout=30.0)
        return mcp_conn
    except asyncio.TimeoutError:
        raise HTTPException(504, "MCP connection timeout - try again")
    except Exception as e:
        raise HTTPException(503, f"MCP connection failed: {e}")


def find_tool(tools: List[Dict], *keywords: str) -> Optional[str]:
    """Find tool by keywords in name"""
    for t in tools:
        name = t["name"].lower()
        if any(k in name for k in keywords):
            return t["name"]
    return None


# =============================================================================
# FastAPI App
# =============================================================================

@asynccontextmanager
async def lifespan(_app: FastAPI):
    print("Jina MCP Server starting...")
    # Pre-warm MCP connection
    try:
        await get_mcp()
        print("MCP connection established")
    except Exception as e:
        print(f"MCP pre-warm failed: {e}")
    yield
    global mcp_conn
    if mcp_conn and mcp_conn.client_ctx:
        try:
            await mcp_conn.client_ctx.__aexit__(None, None, None)
        except:
            pass
    print("Shutdown complete")


web_app = FastAPI(
    title="Jina MCP Server",
    version="1.0.0",
    lifespan=lifespan,
)

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Public endpoints ---

@web_app.get("/")
async def root():
    return {"service": "jina-mcp-server", "status": "ok"}


@web_app.get("/health")
async def health():
    return {"status": "ok", "connected": mcp_conn is not None}


# --- Protected endpoints ---

@web_app.get("/tools")
async def list_tools(_: str = Depends(verify_token)):
    """List available Jina tools"""
    conn = await get_mcp()
    return {"tools": conn.tools}


@web_app.post("/search")
async def search(req: SearchRequest, _: str = Depends(verify_token)):
    """Web search via Jina"""
    conn = await get_mcp()
    tool = find_tool(conn.tools, "search")
    if not tool:
        raise HTTPException(400, "No search tool available")

    result = await call_tool(tool, {"query": req.query})
    try:
        return json.loads(result)
    except:
        return {"raw": result}


@web_app.post("/read")
async def read_url(req: ReadRequest, _: str = Depends(verify_token)):
    """Read URL content via Jina"""
    conn = await get_mcp()
    tool = find_tool(conn.tools, "read", "fetch")
    if not tool:
        raise HTTPException(400, "No read tool available")

    args = {"url": req.url}
    if req.selector:
        args["target_selector"] = req.selector

    result = await call_tool(tool, args)
    try:
        return json.loads(result)
    except:
        return {"content": result}


@web_app.post("/embed")
async def embed(req: EmbedRequest, _: str = Depends(verify_token)):
    """Generate embeddings via Jina"""
    conn = await get_mcp()
    tool = find_tool(conn.tools, "embed")
    if not tool:
        raise HTTPException(400, "No embedding tool available")

    result = await call_tool(tool, {"input": req.input})
    try:
        return json.loads(result)
    except:
        return {"raw": result}


@web_app.post("/rerank")
async def rerank(req: RerankRequest, _: str = Depends(verify_token)):
    """Rerank documents via Jina"""
    conn = await get_mcp()
    tool = find_tool(conn.tools, "rerank", "relevance")
    if not tool:
        raise HTTPException(400, "No rerank tool available")

    args = {"query": req.query, "documents": req.documents}
    if req.top_k:
        args["top_n"] = req.top_k

    result = await call_tool(tool, args)
    try:
        return json.loads(result)
    except:
        return {"raw": result}


@web_app.post("/tool/{tool_name}")
async def call_any_tool(tool_name: str, req: ToolCallRequest, _: str = Depends(verify_token)):
    """Call any Jina tool by name"""
    conn = await get_mcp()

    if not any(t["name"] == tool_name for t in conn.tools):
        raise HTTPException(404, f"Tool '{tool_name}' not found")

    result = await call_tool(tool_name, req.args)
    try:
        return json.loads(result)
    except:
        return {"raw": result}


# =============================================================================
# Modal Deployment
# =============================================================================

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("distributed-systems")],
    keep_warm=1,
    allow_concurrent_inputs=100,
    timeout=300,
)
@modal.asgi_app()
def serve():
    return web_app


# =============================================================================
# Local development
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv
    load_dotenv()

    # Disable auth for local dev
    os.environ["AUTH_DISABLED"] = "true"

    uvicorn.run(web_app, host="0.0.0.0", port=8080)
