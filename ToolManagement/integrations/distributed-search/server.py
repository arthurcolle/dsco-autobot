#!/usr/bin/env python3
"""
Jina Agent Server
=================

Minimal API surface:
- Jina tools proxy (search, read, embed, rerank)
- Agent memory with semantic search
- Work queue

Usage:
    python server.py
"""

import asyncio
import hashlib
import json
import os
import sqlite3
import struct
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

# MCP imports
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
except ImportError:
    print("ERROR: mcp package not found. Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)


# =============================================================================
# Config
# =============================================================================

JINA_API_KEY = os.getenv("JINA_API_KEY")
JINA_MCP_URL = os.getenv("JINA_MCP_URL", "https://mcp.jina.ai/sse")


# =============================================================================
# Memory Store
# =============================================================================

class Memory:
    """Agent memory with semantic search capability"""

    def __init__(self, db_path: str = "memory.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()
        self._embed_fn = None

    def _init_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT,
                embedding BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS urls (
                id TEXT PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                domain TEXT,
                title TEXT,
                summary TEXT,
                status TEXT DEFAULT 'pending',
                visited_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS queue (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                claimed_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_items_type ON items(type);
            CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);
            CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
            CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, priority DESC);
        """)
        self.conn.commit()

    def set_embed_fn(self, fn):
        """Set the embedding function (async callable)"""
        self._embed_fn = fn

    def _hash_id(self, s: str) -> str:
        return hashlib.sha256(s.encode()).hexdigest()[:16]

    # --- Items (general memory) ---

    async def store(
        self,
        content: str,
        type: str = "text",
        metadata: Dict = None,
        id: str = None
    ) -> Dict:
        """Store content in memory, optionally with embedding"""
        item_id = id or self._hash_id(content)

        embedding = None
        if self._embed_fn:
            try:
                result = await self._embed_fn({"input": [content]})
                if isinstance(result, str):
                    result = json.loads(result)
                if "data" in result and result["data"]:
                    embedding = result["data"][0].get("embedding")
            except Exception as e:
                print(f"Embedding failed: {e}")

        embedding_blob = None
        if embedding:
            embedding_blob = struct.pack(f'{len(embedding)}f', *embedding)

        self.conn.execute("""
            INSERT INTO items (id, type, content, metadata, embedding)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                content = excluded.content,
                metadata = excluded.metadata,
                embedding = excluded.embedding,
                updated_at = CURRENT_TIMESTAMP
        """, (item_id, type, content, json.dumps(metadata or {}), embedding_blob))
        self.conn.commit()

        return {"id": item_id, "type": type, "stored": True}

    async def search(
        self,
        query: str,
        type: str = None,
        limit: int = 10
    ) -> List[Dict]:
        """Search memory - semantic if embeddings available, else text"""

        # Try semantic search first
        if self._embed_fn:
            try:
                result = await self._embed_fn({"input": [query]})
                if isinstance(result, str):
                    result = json.loads(result)
                if "data" in result and result["data"]:
                    query_emb = result["data"][0].get("embedding")
                    if query_emb:
                        return self._vector_search(query_emb, type, limit)
            except Exception as e:
                print(f"Semantic search failed, falling back to text: {e}")

        # Fall back to text search
        return self._text_search(query, type, limit)

    def _vector_search(self, query_emb: List[float], type: str, limit: int) -> List[Dict]:
        """Cosine similarity search"""
        sql = "SELECT id, type, content, metadata, embedding FROM items WHERE embedding IS NOT NULL"
        params = []
        if type:
            sql += " AND type = ?"
            params.append(type)

        cursor = self.conn.execute(sql, params)

        results = []
        for row in cursor.fetchall():
            emb_blob = row["embedding"]
            if not emb_blob:
                continue

            n_floats = len(emb_blob) // 4
            stored_emb = struct.unpack(f'{n_floats}f', emb_blob)

            # Cosine similarity
            dot = sum(a * b for a, b in zip(query_emb, stored_emb))
            norm_q = sum(a * a for a in query_emb) ** 0.5
            norm_s = sum(a * a for a in stored_emb) ** 0.5
            score = dot / (norm_q * norm_s) if norm_q and norm_s else 0

            results.append({
                "id": row["id"],
                "type": row["type"],
                "content": row["content"],
                "metadata": json.loads(row["metadata"]),
                "score": score
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:limit]

    def _text_search(self, query: str, type: str, limit: int) -> List[Dict]:
        """Simple text search fallback"""
        sql = "SELECT id, type, content, metadata FROM items WHERE content LIKE ?"
        params = [f"%{query}%"]
        if type:
            sql += " AND type = ?"
            params.append(type)
        sql += " LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(sql, params)
        return [
            {
                "id": row["id"],
                "type": row["type"],
                "content": row["content"],
                "metadata": json.loads(row["metadata"]),
                "score": 1.0
            }
            for row in cursor.fetchall()
        ]

    def get(self, id: str) -> Optional[Dict]:
        """Get item by ID"""
        cursor = self.conn.execute(
            "SELECT id, type, content, metadata, created_at FROM items WHERE id = ?",
            (id,)
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "type": row["type"],
            "content": row["content"],
            "metadata": json.loads(row["metadata"]),
            "created_at": row["created_at"]
        }

    def delete(self, id: str) -> bool:
        """Delete item"""
        cursor = self.conn.execute("DELETE FROM items WHERE id = ?", (id,))
        self.conn.commit()
        return cursor.rowcount > 0

    def list_items(self, type: str = None, limit: int = 100, offset: int = 0) -> List[Dict]:
        """List items"""
        sql = "SELECT id, type, content, metadata, created_at FROM items"
        params = []
        if type:
            sql += " WHERE type = ?"
            params.append(type)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = self.conn.execute(sql, params)
        return [
            {
                "id": row["id"],
                "type": row["type"],
                "content": row["content"],
                "metadata": json.loads(row["metadata"]),
                "created_at": row["created_at"]
            }
            for row in cursor.fetchall()
        ]

    # --- URLs ---

    def add_url(self, url: str, title: str = None, metadata: Dict = None) -> Dict:
        """Track a URL"""
        url_id = self._hash_id(url)
        domain = urlparse(url).netloc

        self.conn.execute("""
            INSERT INTO urls (id, url, domain, title)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title = COALESCE(excluded.title, title)
        """, (url_id, url, domain, title))
        self.conn.commit()

        return {"id": url_id, "url": url, "status": "pending"}

    def mark_visited(self, url: str, summary: str = None) -> Dict:
        """Mark URL as visited"""
        self.conn.execute("""
            UPDATE urls SET
                status = 'visited',
                summary = COALESCE(?, summary),
                visited_at = CURRENT_TIMESTAMP
            WHERE url = ?
        """, (summary, url))
        self.conn.commit()
        return {"url": url, "status": "visited"}

    def get_urls(
        self,
        status: str = None,
        domain: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Query URLs"""
        sql = "SELECT * FROM urls WHERE 1=1"
        params = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if domain:
            sql += " AND domain = ?"
            params.append(domain)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    # --- Queue ---

    def enqueue(self, type: str, payload: Dict, priority: int = 0) -> Dict:
        """Add item to queue"""
        item_id = uuid.uuid4().hex[:16]
        self.conn.execute("""
            INSERT INTO queue (id, type, payload, priority)
            VALUES (?, ?, ?, ?)
        """, (item_id, type, json.dumps(payload), priority))
        self.conn.commit()
        return {"id": item_id, "type": type, "status": "pending"}

    def dequeue(self, type: str = None, batch_size: int = 1) -> List[Dict]:
        """Claim items from queue"""
        sql = """
            SELECT id, type, payload, priority FROM queue
            WHERE status = 'pending'
        """
        params = []
        if type:
            sql += " AND type = ?"
            params.append(type)
        sql += " ORDER BY priority DESC, created_at ASC LIMIT ?"
        params.append(batch_size)

        cursor = self.conn.execute(sql, params)
        items = []
        for row in cursor.fetchall():
            self.conn.execute("""
                UPDATE queue SET status = 'claimed', claimed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (row["id"],))
            items.append({
                "id": row["id"],
                "type": row["type"],
                "payload": json.loads(row["payload"]),
                "priority": row["priority"]
            })
        self.conn.commit()
        return items

    def complete(self, id: str, success: bool = True) -> bool:
        """Mark queue item as complete"""
        status = "completed" if success else "failed"
        cursor = self.conn.execute("""
            UPDATE queue SET status = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (status, id))
        self.conn.commit()
        return cursor.rowcount > 0

    def queue_stats(self) -> Dict:
        """Get queue statistics"""
        cursor = self.conn.execute("""
            SELECT status, COUNT(*) as count FROM queue GROUP BY status
        """)
        stats = {row["status"]: row["count"] for row in cursor.fetchall()}
        return {
            "pending": stats.get("pending", 0),
            "claimed": stats.get("claimed", 0),
            "completed": stats.get("completed", 0),
            "failed": stats.get("failed", 0)
        }

    def close(self):
        if self.conn:
            self.conn.close()


# =============================================================================
# MCP Connection
# =============================================================================

@dataclass
class MCP:
    session: Any
    tools: List[Dict]
    tool_names: Dict[str, str]  # display_name -> actual_name


async def call_tool(session, name: str, args: Dict) -> str:
    """Call MCP tool and return result as string"""
    try:
        result = await session.call_tool(name, args)
    except Exception as e:
        return json.dumps({"error": str(e)})

    if isinstance(result, str):
        return result

    content = result.get("content", []) if isinstance(result, dict) else getattr(result, "content", [])
    if not content:
        return json.dumps(result) if isinstance(result, dict) else str(result)

    parts = []
    for item in content:
        if isinstance(item, dict):
            if item.get("type") == "text":
                parts.append(item.get("text", ""))
            else:
                parts.append(json.dumps(item))
        else:
            parts.append(getattr(item, "text", str(item)))

    return "\n".join(parts)


# =============================================================================
# Request/Response Models
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

class MemoryStoreRequest(BaseModel):
    content: str
    type: str = "text"
    metadata: Optional[Dict] = None
    id: Optional[str] = None

class MemorySearchRequest(BaseModel):
    query: str
    type: Optional[str] = None
    limit: int = 10

class URLAddRequest(BaseModel):
    urls: List[str]

class URLVisitRequest(BaseModel):
    url: str
    summary: Optional[str] = None

class QueuePushRequest(BaseModel):
    type: str
    payload: Dict
    priority: int = 0

class QueuePopRequest(BaseModel):
    type: Optional[str] = None
    batch_size: int = 1

class QueueCompleteRequest(BaseModel):
    id: str
    success: bool = True


# =============================================================================
# Global State
# =============================================================================

mcp: Optional[MCP] = None
memory: Optional[Memory] = None


# =============================================================================
# App
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp, memory

    print("Starting Jina Agent Server...")

    # Init memory
    memory = Memory("memory.db")
    print("Memory initialized")

    # Connect to Jina MCP
    if not JINA_API_KEY:
        print("WARNING: JINA_API_KEY not set - Jina tools disabled")
    else:
        try:
            print(f"Connecting to Jina MCP: {JINA_MCP_URL}")
            headers = {"Authorization": f"Bearer {JINA_API_KEY}"}

            client_ctx = sse_client(JINA_MCP_URL, headers)
            streams = await client_ctx.__aenter__()
            read, write = streams

            session = ClientSession(read, write)
            await session.__aenter__()
            await session.initialize()

            # Fetch tools
            resp = await session.list_tools()
            items = resp.get("tools") if isinstance(resp, dict) else getattr(resp, "tools", resp)

            tools = []
            tool_names = {}
            for t in (items or []):
                name = getattr(t, "name", None) or t.get("name")
                desc = getattr(t, "description", None) or t.get("description", "")
                schema = getattr(t, "input_schema", None) or t.get("input_schema", {})
                tools.append({"name": name, "description": desc, "input_schema": schema})
                tool_names[name] = name

            mcp = MCP(session=session, tools=tools, tool_names=tool_names)
            print(f"Connected. {len(tools)} tools:")
            for t in tools:
                print(f"  - {t['name']}")

            # Set up embedding function for memory
            embed_tool = None
            for t in tools:
                if "embed" in t["name"].lower():
                    embed_tool = t["name"]
                    break

            if embed_tool:
                async def embed_fn(params):
                    return await call_tool(session, embed_tool, params)
                memory.set_embed_fn(embed_fn)
                print(f"Memory semantic search enabled via {embed_tool}")

        except Exception as e:
            print(f"MCP connection failed: {e}")
            import traceback
            traceback.print_exc()

    print("Server ready!")

    try:
        yield
    finally:
        print("Shutting down...")
        if memory:
            memory.close()


app = FastAPI(
    title="Jina Agent Server",
    version="2.0.0",
    description="Minimal API: Jina tools + Agent memory",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# =============================================================================
# Endpoints
# =============================================================================

# --- Health ---

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "mcp_connected": mcp is not None,
        "tools_available": len(mcp.tools) if mcp else 0
    }


@app.get("/tools")
async def list_tools():
    """List available Jina tools"""
    if not mcp:
        return {"tools": [], "message": "MCP not connected"}
    return {"tools": mcp.tools}


# --- Jina Tools ---

@app.post("/search")
async def search(req: SearchRequest):
    """Web search via Jina"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Find search tool
    tool = None
    for t in mcp.tools:
        if "search" in t["name"].lower():
            tool = t["name"]
            break

    if not tool:
        raise HTTPException(400, "No search tool available")

    result = await call_tool(mcp.session, tool, {"query": req.query})

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/read")
async def read(req: ReadRequest):
    """Read URL content via Jina"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Find read tool
    tool = None
    for t in mcp.tools:
        name = t["name"].lower()
        if "read" in name or "fetch" in name:
            tool = t["name"]
            break

    if not tool:
        raise HTTPException(400, "No read tool available")

    args = {"url": req.url}
    if req.selector:
        args["target_selector"] = req.selector

    result = await call_tool(mcp.session, tool, args)

    # Auto-track in memory
    if memory:
        memory.add_url(req.url)
        if result and not result.startswith('{"error'):
            memory.mark_visited(req.url, result[:500] if len(result) > 500 else result)

    try:
        return json.loads(result)
    except:
        return {"content": result}


@app.post("/embed")
async def embed(req: EmbedRequest):
    """Generate embeddings via Jina"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    tool = None
    for t in mcp.tools:
        if "embed" in t["name"].lower():
            tool = t["name"]
            break

    if not tool:
        raise HTTPException(400, "No embedding tool available")

    result = await call_tool(mcp.session, tool, {"input": req.input})

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/rerank")
async def rerank(req: RerankRequest):
    """Rerank documents via Jina"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    tool = None
    for t in mcp.tools:
        name = t["name"].lower()
        if "rerank" in name or "relevance" in name or "sort" in name:
            tool = t["name"]
            break

    if not tool:
        raise HTTPException(400, "No rerank tool available")

    args = {"query": req.query, "documents": req.documents}
    if req.top_k:
        args["top_n"] = req.top_k

    result = await call_tool(mcp.session, tool, args)

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/tool/{tool_name}")
async def call_any_tool(tool_name: str, args: Dict = {}):
    """Call any Jina tool by name"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Check tool exists
    if not any(t["name"] == tool_name for t in mcp.tools):
        raise HTTPException(404, f"Tool '{tool_name}' not found")

    result = await call_tool(mcp.session, tool_name, args)

    try:
        return json.loads(result)
    except:
        return {"raw": result}


# --- Memory ---

@app.post("/memory")
async def memory_store(req: MemoryStoreRequest):
    """Store content in memory"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    return await memory.store(
        content=req.content,
        type=req.type,
        metadata=req.metadata,
        id=req.id
    )


@app.get("/memory")
async def memory_search_get(
    q: str = Query(..., description="Search query"),
    type: str = Query(None, description="Filter by type"),
    limit: int = Query(10, ge=1, le=100)
):
    """Search memory (GET)"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    results = await memory.search(q, type, limit)
    return {"results": results, "count": len(results)}


@app.post("/memory/search")
async def memory_search_post(req: MemorySearchRequest):
    """Search memory (POST)"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    results = await memory.search(req.query, req.type, req.limit)
    return {"results": results, "count": len(results)}


@app.get("/memory/{item_id}")
async def memory_get(item_id: str):
    """Get memory item by ID"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    item = memory.get(item_id)
    if not item:
        raise HTTPException(404, "Item not found")
    return item


@app.delete("/memory/{item_id}")
async def memory_delete(item_id: str):
    """Delete memory item"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    deleted = memory.delete(item_id)
    if not deleted:
        raise HTTPException(404, "Item not found")
    return {"deleted": True}


@app.get("/memory/list/all")
async def memory_list(
    type: str = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List memory items"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    items = memory.list_items(type, limit, offset)
    return {"items": items, "count": len(items)}


# --- URLs ---

@app.post("/urls")
async def add_urls(req: URLAddRequest):
    """Track URLs"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    results = [memory.add_url(url) for url in req.urls]
    return {"added": len(results), "urls": results}


@app.put("/urls/visited")
async def mark_visited(req: URLVisitRequest):
    """Mark URL as visited"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    return memory.mark_visited(req.url, req.summary)


@app.get("/urls")
async def get_urls(
    status: str = Query(None),
    domain: str = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    """Query URLs"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    urls = memory.get_urls(status, domain, limit)
    return {"urls": urls, "count": len(urls)}


# --- Queue ---

@app.post("/queue")
async def queue_push(req: QueuePushRequest):
    """Add item to queue"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    return memory.enqueue(req.type, req.payload, req.priority)


@app.post("/queue/pop")
async def queue_pop(req: QueuePopRequest):
    """Claim items from queue"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    items = memory.dequeue(req.type, req.batch_size)
    return {"items": items, "count": len(items)}


@app.post("/queue/complete")
async def queue_complete(req: QueueCompleteRequest):
    """Mark queue item as complete"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    completed = memory.complete(req.id, req.success)
    if not completed:
        raise HTTPException(404, "Queue item not found")
    return {"completed": True, "id": req.id}


@app.get("/queue/stats")
async def queue_stats():
    """Get queue statistics"""
    if not memory:
        raise HTTPException(503, "Memory not initialized")

    return memory.queue_stats()


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )
