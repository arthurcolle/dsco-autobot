# Tool Management Integration Fix Plan

## Problems Identified

### P1: Service URLs pointing to localhost
- **Location:** `src/tool_management/api/service_routes.py`, native MCP servers
- **Root Cause:** Native MCPs self-register with `localhost:PORT` instead of their deployed URLs
- **Impact:** Remote registry can't reach backends

### P2: Modal services scaling to zero
- **Location:** `deploy/modal/*.py`
- **Root Cause:** `scaledown_window=120` causes cold starts
- **Impact:** First request after inactivity times out
- **Status:** ✅ FIXED - Changed to `min_containers=1`

### P3: Tools not synced to registry automatically
- **Location:** `src/tool_management/api/mcp_routes.py:70-102`
- **Root Cause:** Tool sync is lazy, only triggered by SSE connection or manual call
- **Impact:** Registry doesn't know what tools backends provide

### P4: Batch endpoint requires explicit backend
- **Location:** `src/tool_management/api/mcp_routes.py:314-396`
- **Root Cause:** No intelligent routing - client must specify backend
- **Impact:** Clients need to know internal architecture

### P5: Meta-tools failing (search_tools, execute_tool)
- **Location:** `src/tool_management/integrations/mcp_server.py:297-880`
- **Root Cause:** Hardcoded `localhost:2016`, no connection pooling, 30s timeout
- **Impact:** Meta-tools fail in deployed environment

### P6: dsco-parallel-mcp failing locally
- **Location:** Claude Code settings pointing to `localhost:2040`
- **Root Cause:** Service not running locally
- **Impact:** MCP connection fails

### P7: No WebSocket synchronization for real-time updates
- **Location:** `src/tool_management/native/sync.py`
- **Root Cause:** Fire-and-forget registration, no acknowledgment
- **Impact:** Registry can drift from actual backend state

### P8: Memory MCP not pre-registered
- **Location:** Missing from service initialization
- **Root Cause:** Only 3 of 4 native MCPs were in default registration
- **Status:** ✅ FIXED - Added manual registration

---

## Fix Implementation Plan

### Phase 1: Critical Fixes (Immediate)

#### 1.1 Fix Native MCP Self-Registration with Correct URLs

**File:** `native/base_server.py` (new) + each native MCP server

```python
# native/base_server.py
import os
import httpx
from typing import List, Dict

class NativeMCPBase:
    """Base class for native MCPs with auto-registration."""

    def __init__(self, service_id: str, name: str, tools: List[Dict]):
        self.service_id = service_id
        self.name = name
        self.tools = tools
        self.registry_url = os.getenv("REGISTRY_URL", "http://localhost:2016")
        self.service_url = os.getenv("SERVICE_URL")  # Must be set in Modal

    async def register_with_registry(self):
        """Register this MCP with the central registry."""
        if not self.service_url:
            print(f"Warning: SERVICE_URL not set, skipping registration")
            return

        async with httpx.AsyncClient() as client:
            # Register/update service
            await client.post(
                f"{self.registry_url}/api/v1/services",
                json={
                    "service_id": self.service_id,
                    "name": self.name,
                    "base_url": self.service_url,  # Use actual deployed URL
                    "service_type": "mcp",
                    "capabilities": {"mcp_protocol": True, "tool_execution": True}
                }
            )

            # Sync tools
            await client.post(
                f"{self.registry_url}/api/v1/admin/sync-mcp-tools"
            )
```

**Update each native MCP to inherit from base:**

```python
# native/distributed_search_mcp/server.py
from native.base_server import NativeMCPBase

class SearchMCP(NativeMCPBase):
    def __init__(self):
        super().__init__(
            service_id="distributed-search-mcp",
            name="Distributed Search MCP",
            tools=[...]
        )

# On startup
@app.on_event("startup")
async def startup():
    mcp = SearchMCP()
    await mcp.register_with_registry()
```

#### 1.2 Fix Hardcoded URLs in MCP Server

**File:** `src/tool_management/integrations/mcp_server.py`

```python
# Before (line 486)
response = await client.get(
    f"http://localhost:2016/api/v1/admin/search-tools-semantic?query={query}",
    timeout=10.0
)

# After
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:2016")

response = await client.get(
    f"{REGISTRY_URL}/api/v1/admin/search-tools-semantic?query={query}",
    timeout=10.0
)
```

Apply same fix to all hardcoded URLs (lines 486, 517, 583, etc.)

#### 1.3 Add Intelligent Tool Routing to Batch Endpoint

**File:** `src/tool_management/api/mcp_routes.py`

```python
@router.post("/batch")
async def mcp_batch_execute(request: Request):
    body = await request.json()
    calls = body.get("calls", [])

    queue = get_task_queue()
    backends_map = {b.id: b for b in queue.list_backends()}

    # Build tool -> backend mapping from synced tools
    tool_backend_map = await _build_tool_backend_map()

    async def execute_single(call: dict) -> dict:
        tool_name = call.get("tool")

        # Auto-resolve backend if not specified
        backend_id = call.get("backend")
        if not backend_id:
            # Parse namespaced tool: "distributed-search-mcp:search" -> backend, tool
            if ":" in tool_name:
                backend_id, tool_name = tool_name.split(":", 1)
            else:
                # Look up from tool_backend_map
                backend_id = tool_backend_map.get(tool_name)

        if not backend_id or backend_id not in backends_map:
            return {"tool": tool_name, "error": f"Backend not found for tool: {tool_name}"}

        # ... rest of execution logic

async def _build_tool_backend_map() -> Dict[str, str]:
    """Build mapping of tool names to backend IDs."""
    queue = get_task_queue()
    tool_map = {}

    for backend in queue.list_backends():
        # Fetch tools from backend
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{backend.url}/mcp",
                    json={"jsonrpc": "2.0", "method": "tools/list", "id": 1},
                    timeout=5.0
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for tool in data.get("result", {}).get("tools", []):
                        tool_map[tool["name"]] = backend.id
        except:
            pass

    return tool_map
```

#### 1.4 Add Connection Pooling

**File:** `src/tool_management/core/http_client.py` (new)

```python
import httpx
from contextlib import asynccontextmanager

# Global connection pool
_client: httpx.AsyncClient = None

def get_http_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )
    return _client

async def close_http_client():
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
```

Update all files to use `get_http_client()` instead of `httpx.AsyncClient()`.

---

### Phase 2: Automatic Tool Sync

#### 2.1 Add Tool Sync on Startup

**File:** `src/tool_management/app.py`

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ... existing startup code ...

    # Auto-sync tools from all backends after registration
    await _sync_all_backend_tools()

    yield

    # ... existing shutdown code ...

async def _sync_all_backend_tools():
    """Sync tools from all registered backends."""
    from .api.mcp_routes import refresh_mcp_tools

    logger.info("Syncing tools from all backends...")
    try:
        await refresh_mcp_tools()
        logger.info("Tool sync complete")
    except Exception as e:
        logger.error(f"Tool sync failed: {e}")
```

#### 2.2 Add Periodic Background Sync

**File:** `src/tool_management/background_tasks.py` (new)

```python
import asyncio
from datetime import datetime

class BackgroundSyncManager:
    def __init__(self, interval_seconds: int = 60):
        self.interval = interval_seconds
        self._task: asyncio.Task = None

    async def start(self):
        self._task = asyncio.create_task(self._sync_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _sync_loop(self):
        while True:
            try:
                await self._sync_backends()
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background sync error: {e}")
                await asyncio.sleep(5)  # Brief delay before retry

    async def _sync_backends(self):
        from .api.mcp_routes import refresh_mcp_tools
        from .queue import get_task_queue

        queue = get_task_queue()

        # Health check all backends
        for backend in queue.list_backends():
            try:
                client = get_http_client()
                resp = await client.get(
                    f"{backend.url}{backend.health_endpoint}",
                    timeout=5.0
                )
                backend.healthy = resp.status_code == 200
            except:
                backend.healthy = False

        # Refresh tools from healthy backends
        await refresh_mcp_tools()
```

---

### Phase 3: WebSocket Synchronization

#### 3.1 Add WebSocket Sync Client for Native MCPs

**File:** `native/registry_client.py` (new)

```python
import asyncio
import websockets
import json
from datetime import datetime

class RegistryClient:
    """WebSocket client for real-time registry sync."""

    def __init__(self, registry_url: str, service_id: str):
        self.registry_url = registry_url.replace("http", "ws")
        self.service_id = service_id
        self._ws = None
        self._connected = False
        self._tools = []

    async def connect(self):
        """Establish WebSocket connection."""
        ws_url = f"{self.registry_url}/api/v1/native/sync"

        while True:
            try:
                self._ws = await websockets.connect(ws_url)
                self._connected = True

                # Send registration
                await self._send({
                    "type": "register",
                    "service_id": self.service_id,
                    "tools": self._tools,
                    "timestamp": datetime.utcnow().isoformat()
                })

                # Start receive loop
                await self._receive_loop()

            except Exception as e:
                print(f"WebSocket error: {e}, reconnecting in 5s...")
                self._connected = False
                await asyncio.sleep(5)

    async def _receive_loop(self):
        async for message in self._ws:
            data = json.loads(message)

            if data["type"] == "ack":
                print(f"Registration acknowledged: {data}")
            elif data["type"] == "sync_request":
                # Registry is asking for current tools
                await self._send({
                    "type": "tools_update",
                    "service_id": self.service_id,
                    "tools": self._tools
                })
            elif data["type"] == "ping":
                await self._send({"type": "pong"})

    async def _send(self, data: dict):
        if self._ws and self._connected:
            await self._ws.send(json.dumps(data))

    async def update_tools(self, tools: list):
        """Update tools and notify registry."""
        self._tools = tools
        if self._connected:
            await self._send({
                "type": "tools_update",
                "service_id": self.service_id,
                "tools": tools
            })
```

#### 3.2 Add WebSocket Endpoint to Registry

**File:** `src/tool_management/api/native_routes.py`

```python
from fastapi import WebSocket, WebSocketDisconnect

# Connected native MCPs
_native_connections: Dict[str, WebSocket] = {}

@router.websocket("/sync")
async def native_sync_websocket(websocket: WebSocket):
    await websocket.accept()
    service_id = None

    try:
        while True:
            data = await websocket.receive_json()

            if data["type"] == "register":
                service_id = data["service_id"]
                _native_connections[service_id] = websocket

                # Update service registry
                service_registry = get_service_registry()
                await service_registry.update_service(
                    service_id,
                    tools=data.get("tools", []),
                    last_heartbeat=datetime.utcnow()
                )

                # Acknowledge
                await websocket.send_json({
                    "type": "ack",
                    "service_id": service_id,
                    "status": "registered"
                })

            elif data["type"] == "tools_update":
                # Sync tools to registry
                await _sync_tools_from_native(data["service_id"], data["tools"])

            elif data["type"] == "pong":
                # Heartbeat response
                pass

    except WebSocketDisconnect:
        if service_id and service_id in _native_connections:
            del _native_connections[service_id]
            logger.info(f"Native MCP disconnected: {service_id}")

async def broadcast_sync_request():
    """Ask all connected natives to resync their tools."""
    for service_id, ws in _native_connections.items():
        try:
            await ws.send_json({"type": "sync_request"})
        except:
            pass
```

---

### Phase 4: Fix Local MCP Configuration

#### 4.1 Update Claude Code Settings

**File:** `~/.claude/settings.json`

Remove or fix failing local MCPs:

```json
{
  "mcpServers": {
    "distributed-systems-tools": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://tools.distributed.systems/mcp/sse"]
    }
  }
}
```

Remove:
- `dsco-parallel-mcp` (localhost:2040 - not running)
- `dsco-jina-mcp` (if not needed locally)
- `tool-registry` (localhost:2016 - use remote instead)

---

## Implementation Order

1. **Day 1: Critical Path**
   - [ ] Fix hardcoded URLs in mcp_server.py
   - [ ] Add connection pooling
   - [ ] Update Modal deployments (already done)
   - [ ] Fix Claude Code settings

2. **Day 2: Auto-Sync**
   - [ ] Add startup tool sync
   - [ ] Add background sync manager
   - [ ] Add intelligent routing to batch endpoint

3. **Day 3: Native MCP Updates**
   - [ ] Create base_server.py with registration logic
   - [ ] Update each native MCP to use base
   - [ ] Add WebSocket sync client

4. **Day 4: Registry Updates**
   - [ ] Add WebSocket sync endpoint
   - [ ] Add tool persistence (optional, use existing SQLite)
   - [ ] Add health-based routing

5. **Day 5: Testing & Deploy**
   - [ ] Test all 4 native MCPs via batch endpoint
   - [ ] Test meta-tools (search_tools, execute_tool)
   - [ ] Deploy all changes to Modal
   - [ ] Verify Claude Code integration

---

## Verification Checklist

After implementation, verify:

```bash
# 1. All backends healthy
curl https://tools.distributed.systems/mcp/health

# 2. Tools synced
curl https://tools.distributed.systems/api/v1/admin/sync-mcp-tools

# 3. Batch execution works without explicit backend
curl -X POST https://tools.distributed.systems/mcp/batch \
  -H "Content-Type: application/json" \
  -d '{"calls": [{"tool": "search", "arguments": {"query": "test"}}]}'

# 4. Meta-tools work
curl -X POST https://tools.distributed.systems/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "search_tools", "arguments": {"query": "search"}}, "id": 1}'

# 5. SSE connection works
curl -N https://tools.distributed.systems/mcp/sse
```
