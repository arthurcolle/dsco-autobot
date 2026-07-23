#!/usr/bin/env python3
"""
Distributed Search MCP - Native MCP Server

First-class MCP server providing:
- Web search (powered by Jina AI)
- URL content extraction
- Fact checking
- Semantic reranking
- Embedding generation

This is a NATIVE MCP server - it implements the MCP protocol directly
rather than wrapping an external service.

Run with:
    python -m native.distributed_search_mcp
    # or
    uvicorn native.distributed_search_mcp:app --port 4001

Environment:
    JINA_API_KEY: Jina AI API key (required for full functionality)
    REGISTRY_URL: Central registry URL (optional)
    PORT: Server port (default: 4001)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL Queue integration - import with fallback for standalone mode
try:
    from tool_management.queue.url_queue import get_url_queue, URLSource
    URL_QUEUE_AVAILABLE = True
except ImportError:
    URL_QUEUE_AVAILABLE = False
    logger.info("URL queue not available - running in standalone mode")

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False
    logger.info("Output guard not available - running without output limits")


# =============================================================================
# FEDERATION CONFIG + COMPACT-OUTPUT HELPERS
# =============================================================================

DEFAULT_MAX_OUTPUT_CHARS = 4000
SNIPPET_CHARS = 300
FEDERATED_CONCURRENCY = 8

RESEARCH_MCP_URL = os.getenv(
    "RESEARCH_MCP_URL", "https://arthurcolle--distributed-research-mcp-web.modal.run"
)
DOCUMENT_MCP_URL = os.getenv(
    "DOCUMENT_MCP_URL", "https://arthurcolle--distributed-document-qa-mcp-web.modal.run"
)
VIDEO_MCP_URL = os.getenv(
    "VIDEO_MCP_URL", "https://arthurcolle--distributed-video-qa-web.modal.run"
)
MEMORY_MCP_URL = os.getenv(
    "MEMORY_MCP_URL", "https://arthurcolle--distributed-memory-mcp-web.modal.run"
)


def _registry_base_url() -> str:
    return (os.getenv("REGISTRY_URL") or "https://tools.distributed.systems").rstrip("/")


def _snip(text: Any, limit: int = SNIPPET_CHARS) -> str:
    """Truncate text to ~limit chars."""
    if text is None:
        return ""
    s = text if isinstance(text, str) else json.dumps(text, default=str)
    s = s.strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def _compact_item(item: Any) -> Dict[str, Any]:
    """Reduce an arbitrary search hit to {title, url, snippet}."""
    if not isinstance(item, dict):
        return {"snippet": _snip(item)}
    out: Dict[str, Any] = {}
    title = item.get("title") or item.get("name") or item.get("subject") or item.get("id")
    if title:
        out["title"] = _snip(str(title), 120)
    url = item.get("url") or item.get("link") or item.get("href")
    if url and isinstance(url, str):
        out["url"] = url[:500]
    for key in ("snippet", "description", "content", "text", "summary", "answer", "object"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            out["snippet"] = _snip(val)
            break
    if not out:
        out["snippet"] = _snip(item)
    return out


def _extract_items(payload: Any) -> List[Any]:
    """Find the most plausible list of result items in an arbitrary payload."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in (
        "results", "items", "documents", "matches", "findings", "sources",
        "episodes", "tools", "videos", "data", "hits", "memories",
    ):
        val = payload.get(key)
        if isinstance(val, list) and val:
            return val
        if isinstance(val, dict):
            # e.g. memory search_all returns {"results": {"episodic": [...], ...}}
            merged: List[Any] = []
            for sub in val.values():
                if isinstance(sub, list):
                    merged.extend(sub)
            if merged:
                return merged
    # Fallback: first list-of-dicts value
    for val in payload.values():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val
    return []


def _shrink_to_budget(obj: Dict[str, Any], max_chars: int) -> Dict[str, Any]:
    """Trim list entries (then long strings) until the serialized body fits max_chars."""
    max_chars = max(500, int(max_chars or DEFAULT_MAX_OUTPUT_CHARS))

    def _all_lists(o: Any, acc: List[list]) -> List[list]:
        if isinstance(o, dict):
            for v in o.values():
                _all_lists(v, acc)
        elif isinstance(o, list):
            acc.append(o)
            for v in o:
                _all_lists(v, acc)
        return acc

    trimmed = False
    while len(json.dumps(obj, default=str)) > max_chars:
        lists = [l for l in _all_lists(obj, []) if len(l) > 0]
        if not lists:
            break
        max(lists, key=len).pop()
        trimmed = True

    if len(json.dumps(obj, default=str)) > max_chars:
        def _trunc(o: Any) -> Any:
            if isinstance(o, dict):
                return {k: _trunc(v) for k, v in o.items()}
            if isinstance(o, list):
                return [_trunc(v) for v in o]
            if isinstance(o, str) and len(o) > 200:
                return o[:199] + "…"
            return o
        obj = _trunc(obj)
        trimmed = True

    if trimmed:
        obj["truncated"] = True
    return obj


# =============================================================================
# DURABLE REQUEST STORE (sqlite)
# =============================================================================

class SearchRequestStore:
    """Durable store for full search payloads; responses stay compact."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(
            db_path or os.getenv("SEARCH_REQUESTS_DB", "data/search_requests.db")
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._lock, sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    response_id TEXT,
                    tool TEXT,
                    arguments_json TEXT,
                    summary TEXT,
                    full_result_json TEXT,
                    sources_json TEXT,
                    status TEXT,
                    created_at TEXT
                )
                """
            )
            conn.commit()

    def save(
        self,
        request_id: str,
        response_id: str,
        tool: str,
        arguments: Dict[str, Any],
        summary: str,
        full_result: Any,
        sources: Any = None,
        status: str = "ok",
    ) -> None:
        with self._lock, sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO requests VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    request_id,
                    response_id,
                    tool,
                    json.dumps(arguments, default=str),
                    summary,
                    json.dumps(full_result, default=str),
                    json.dumps(sources or [], default=str),
                    status,
                    datetime.utcnow().isoformat() + "Z",
                ),
            )
            conn.commit()

    def get(
        self,
        request_id: Optional[str] = None,
        response_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not request_id and not response_id:
            return None
        query = "SELECT request_id, response_id, tool, arguments_json, summary, full_result_json, sources_json, status, created_at FROM requests WHERE "
        if request_id:
            query += "request_id = ?"
            param = request_id
        else:
            query += "response_id = ?"
            param = response_id
        with self._lock, sqlite3.connect(self.db_path) as conn:
            row = conn.execute(query, (param,)).fetchone()
        if not row:
            return None
        record = {
            "request_id": row[0],
            "response_id": row[1],
            "tool": row[2],
            "summary": row[4],
            "status": row[7],
            "created_at": row[8],
        }
        for key, raw in (("arguments", row[3]), ("full_result", row[5]), ("sources", row[6])):
            try:
                record[key] = json.loads(raw) if raw else None
            except (json.JSONDecodeError, TypeError):
                record[key] = raw
        return record

    def list(self, limit: int = 20, tool: Optional[str] = None) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 20), 200))
        sql = "SELECT request_id, response_id, tool, summary, status, created_at FROM requests"
        params: List[Any] = []
        if tool:
            sql += " WHERE tool = ?"
            params.append(tool)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._lock, sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "request_id": r[0],
                "response_id": r[1],
                "tool": r[2],
                "summary": r[3],
                "status": r[4],
                "created_at": r[5],
            }
            for r in rows
        ]


# =============================================================================
# MODELS
# =============================================================================

class MCPProtocolVersion(str, Enum):
    V1 = "2024-11-05"


class MCPCapability(str, Enum):
    TOOLS = "tools"
    RESOURCES = "resources"
    PROMPTS = "prompts"
    LOGGING = "logging"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Optional[Callable] = None


@dataclass
class ConnectionState:
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    error_message: Optional[str] = None
    session_id: Optional[str] = None
    protocol_version: str = MCPProtocolVersion.V1.value


class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


# =============================================================================
# NATIVE MCP SERVER
# =============================================================================

class DistributedSearchMCP:
    """
    Native MCP server for distributed search capabilities.

    This is our first native MCP - it directly implements the MCP protocol
    rather than acting as a proxy. The connectivity test verifies:

    1. MCP protocol handshake works
    2. Tool discovery functions
    3. Tool execution succeeds
    4. Streaming responses work (SSE)
    """

    SERVICE_ID = "distributed-search-mcp"
    SERVICE_NAME = "Distributed Search MCP"
    VERSION = "2.2.0"

    # Jina API endpoints. Env-overridable so the EU data-residency hosts
    # (eu.api.jina.ai, eu.r.jina.ai, eu.s.jina.ai) can be swapped in.
    JINA_API_BASE = os.getenv("JINA_API_BASE", "https://api.jina.ai").rstrip("/")
    JINA_SEARCH_URL = os.getenv("JINA_SEARCH_URL", "https://s.jina.ai").rstrip("/")
    JINA_READ_URL = os.getenv("JINA_READ_URL", "https://r.jina.ai").rstrip("/")
    JINA_EMBED_URL = f"{JINA_API_BASE}/v1/embeddings"
    JINA_RERANK_URL = f"{JINA_API_BASE}/v1/rerank"
    JINA_CLASSIFY_URL = f"{JINA_API_BASE}/v1/classify"
    JINA_SEGMENT_URL = f"{JINA_API_BASE}/v1/segment"
    JINA_DEEPSEARCH_URL = os.getenv(
        "JINA_DEEPSEARCH_URL", "https://deepsearch.jina.ai/v1/chat/completions"
    )

    def __init__(
        self,
        api_key: Optional[str] = None,
        port: int = 4001,
        registry_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        self.port = port
        self.registry_url = registry_url or os.getenv("REGISTRY_URL")

        self._client: Optional[httpx.AsyncClient] = None
        # Separate client for federated/sibling calls (no Jina auth header)
        self._fed_client: Optional[httpx.AsyncClient] = None
        self._tools: Dict[str, MCPTool] = {}
        self._state = ConnectionState()

        # Durable store for full search payloads
        self._store = SearchRequestStore()

        # Register native tools
        self._register_tools()

    def _register_tools(self):
        """Register all native MCP tools."""

        # Web search
        self._tools["search"] = MCPTool(
            name="search",
            description="Search the web using Jina AI. Returns compact results with snippets; full payload is stored durably and retrievable via get_search_result.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results (1-20)",
                        "default": 10
                    },
                    "site": {
                        "type": "string",
                        "description": "Restrict results to this domain (e.g. github.com)"
                    },
                    "country": {
                        "type": "string",
                        "description": "Two-letter country code for result localization (gl)"
                    },
                    "language": {
                        "type": "string",
                        "description": "Two-letter language code (hl)"
                    },
                    "page": {
                        "type": "integer",
                        "description": "Result page offset"
                    },
                    "location": {
                        "type": "string",
                        "description": "City-level search origin"
                    },
                    "serp_only": {
                        "type": "boolean",
                        "description": "Return SERP snippets only, skip page fetch (faster/cheaper)"
                    },
                    "favicons": {"type": "boolean"},
                    "extra_headers": {
                        "type": "object",
                        "description": "Raw X-* search headers passed through verbatim"
                    },
                    "max_output_chars": {
                        "type": "integer",
                        "description": "Maximum output size in characters. Truncates if exceeded.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    },
                    "max_links": {
                        "type": "integer",
                        "description": "Maximum URLs in output. Extra URLs are captured for background processing.",
                        "default": 50
                    }
                },
                "required": ["query"]
            },
            handler=self._execute_search
        )

        # Read URL
        self._tools["read"] = MCPTool(
            name="read",
            description="Extract and read content from a URL. Returns a compact preview; full content is stored durably and retrievable via get_search_result.",
            input_schema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to read"
                    },
                    "include_images": {
                        "type": "boolean",
                        "description": "Include image descriptions",
                        "default": False
                    },
                    "return_format": {
                        "type": "string",
                        "enum": ["markdown", "html", "text", "screenshot", "pageshot"],
                        "description": "Content format returned by the reader (X-Return-Format)"
                    },
                    "target_selector": {
                        "type": "string",
                        "description": "CSS selector to extract (X-Target-Selector)"
                    },
                    "wait_for_selector": {
                        "type": "string",
                        "description": "Wait for this CSS selector before reading"
                    },
                    "remove_selector": {
                        "type": "string",
                        "description": "Strip elements matching this CSS selector"
                    },
                    "engine": {
                        "type": "string",
                        "enum": ["browser", "direct", "cf-browser-rendering"],
                        "description": "Rendering engine (X-Engine)"
                    },
                    "respond_with": {
                        "type": "string",
                        "description": "e.g. readerlm-v2 for LLM-cleaned extraction"
                    },
                    "token_budget": {
                        "type": "integer",
                        "description": "Cap tokens processed for the page (X-Token-Budget)"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Page-load timeout seconds (X-Timeout)"
                    },
                    "locale": {"type": "string"},
                    "retain_images": {
                        "type": "string",
                        "description": "Set to 'none' to strip images"
                    },
                    "no_cache": {
                        "type": "boolean",
                        "description": "Bypass reader cache (X-No-Cache)"
                    },
                    "with_links_summary": {
                        "type": "boolean",
                        "description": "Append a summary of all links (X-With-Links-Summary)"
                    },
                    "with_images_summary": {"type": "boolean"},
                    "with_iframe": {"type": "boolean"},
                    "with_shadow_dom": {"type": "boolean"},
                    "extra_headers": {
                        "type": "object",
                        "description": "Raw X-* reader headers passed through verbatim"
                    },
                    "max_output_chars": {
                        "type": "integer",
                        "description": "Maximum output size in characters. Truncates if exceeded.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["url"]
            },
            handler=self._execute_read
        )

        # Fact check
        self._tools["verify"] = MCPTool(
            name="verify",
            description="Verify a claim or statement against web sources.",
            input_schema={
                "type": "object",
                "properties": {
                    "statement": {
                        "type": "string",
                        "description": "Statement to fact-check"
                    }
                },
                "required": ["statement"]
            },
            handler=self._execute_verify
        )

        # Rerank
        self._tools["rerank"] = MCPTool(
            name="rerank",
            description="Rerank documents by relevance to a query using semantic similarity.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to rank against"
                    },
                    "documents": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object",
                                 "properties": {
                                     "text": {"type": "string"},
                                     "image": {"type": "string", "description": "Image URL or base64 (jina-reranker-m0)"}
                                 }}
                            ]
                        },
                        "description": "Documents to rerank: strings, or {text}/{image} objects for multimodal models"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of top results",
                        "default": 5
                    },
                    "model": {
                        "type": "string",
                        "description": "Reranker model (jina-reranker-v3, jina-reranker-v2-base-multilingual, jina-reranker-m0 for multimodal, jina-colbert-v2)",
                        "default": "jina-reranker-v2-base-multilingual"
                    },
                    "return_documents": {"type": "boolean"},
                    "return_embeddings": {"type": "boolean"},
                    "max_doc_length": {"type": "integer"}
                },
                "required": ["query", "documents"]
            },
            handler=self._execute_rerank
        )

        # Embed
        self._tools["embed"] = MCPTool(
            name="embed",
            description=(
                "Generate embeddings using Jina embedding models. Accepts a single "
                "text, a batch of texts, or multimodal inputs (image/PDF URLs or "
                "base64 for jina-embeddings-v4 / v5-omni / jina-clip). Supports "
                "task-specific LoRA adapters, Matryoshka dimension truncation, "
                "late chunking, and multi-vector (late-interaction) output."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Single text to embed (or use texts for a batch)"
                    },
                    "texts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Batch of texts to embed (max 100)"
                    },
                    "inputs": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object",
                                 "properties": {
                                     "text": {"type": "string"},
                                     "image": {"type": "string", "description": "Image URL or base64"},
                                     "pdf": {"type": "string", "description": "PDF URL or base64 (v4)"}
                                 }}
                            ]
                        },
                        "description": "Raw multimodal input array passed to the API verbatim (overrides text/texts)"
                    },
                    "model": {
                        "type": "string",
                        "description": "Embedding model (jina-embeddings-v3, v4, v5-text-small, v5-omni-small, jina-clip-v2, jina-colbert-v2, jina-code-embeddings-1.5b, ...)",
                        "default": "jina-embeddings-v3"
                    },
                    "task": {
                        "type": "string",
                        "enum": ["retrieval.query", "retrieval.passage", "text-matching",
                                 "classification", "separation", "code.query"],
                        "description": "Task adapter to use"
                    },
                    "dimensions": {
                        "type": "integer",
                        "description": "Truncate embeddings to this dimension (Matryoshka)"
                    },
                    "late_chunking": {
                        "type": "boolean",
                        "description": "Encode all inputs as one context, then split (v3)"
                    },
                    "normalized": {
                        "type": "boolean",
                        "description": "L2-normalize output embeddings"
                    },
                    "truncate": {
                        "type": "boolean",
                        "description": "Truncate over-long inputs instead of erroring"
                    },
                    "return_multivector": {
                        "type": "boolean",
                        "description": "Return per-token multi-vector output (v4/colbert late interaction)"
                    },
                    "embedding_type": {
                        "type": "string",
                        "description": "Output encoding (float, base64, binary, ubinary)"
                    }
                }
            },
            handler=self._execute_embed
        )

        # DeepSearch (agentic search+read+reason)
        self._tools["deepsearch"] = MCPTool(
            name="deepsearch",
            description=(
                "Jina DeepSearch: agentic search-read-reason loop that iterates until "
                "it finds a well-grounded answer with citations. Slower and more "
                "expensive than search - use for hard questions needing synthesis. "
                "Compact response; full payload retrievable via get_search_result."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Question or research objective"
                    },
                    "reasoning_effort": {
                        "type": "string",
                        "enum": ["low", "medium", "high"],
                        "default": "medium"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Hard token budget for the whole search (overrides reasoning_effort)"
                    },
                    "max_attempts": {"type": "integer"},
                    "team_size": {
                        "type": "integer",
                        "description": "Parallel agents sharing the budget"
                    },
                    "no_direct_answer": {
                        "type": "boolean",
                        "description": "Force search even for seemingly trivial questions"
                    },
                    "boost_hostnames": {"type": "array", "items": {"type": "string"}},
                    "bad_hostnames": {"type": "array", "items": {"type": "string"}},
                    "only_hostnames": {"type": "array", "items": {"type": "string"}},
                    "search_language_code": {"type": "string"},
                    "search_provider": {
                        "type": "string",
                        "description": "e.g. arxiv for academic-only search"
                    },
                    "max_returned_urls": {"type": "integer"},
                    "response_format": {
                        "type": "object",
                        "description": "OpenAI-style structured output schema"
                    },
                    "max_output_chars": {
                        "type": "integer",
                        "description": "Maximum output size in characters. Truncates if exceeded.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["query"]
            },
            handler=self._execute_deepsearch
        )

        # Zero-shot classification
        self._tools["classify"] = MCPTool(
            name="classify",
            description=(
                "Classify texts (or images via jina-clip models) against labels: "
                "zero-shot with any embedding model, or few-shot with a trained "
                "classifier_id (see train_classifier)."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "texts": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object",
                                 "properties": {
                                     "text": {"type": "string"},
                                     "image": {"type": "string", "description": "Image URL or base64"}
                                 }}
                            ]
                        },
                        "description": "Inputs to classify: strings or {text}/{image} objects"
                    },
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Candidate labels"
                    },
                    "model": {
                        "type": "string",
                        "default": "jina-embeddings-v3"
                    },
                    "classifier_id": {
                        "type": "string",
                        "description": "Use a trained few-shot classifier instead of zero-shot"
                    }
                },
                "required": ["texts"]
            },
            handler=self._execute_classify
        )

        # Text segmentation / chunking
        self._tools["segment"] = MCPTool(
            name="segment",
            description="Split long text into semantically sane chunks via Jina Segmenter API (also returns token counts).",
            input_schema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Text to segment"
                    },
                    "max_chunk_length": {
                        "type": "integer",
                        "description": "Max characters per chunk",
                        "default": 1000
                    },
                    "return_chunks": {
                        "type": "boolean",
                        "default": True
                    },
                    "tokenizer": {
                        "type": "string",
                        "description": "Tokenizer for token counting (e.g. cl100k_base)"
                    }
                },
                "required": ["content"]
            },
            handler=self._execute_segment
        )

        # Compact registration for the remaining Jina API surface.
        # Each entry: (name, description, properties, required, handler).
        _extra_tools = [
            ("embed_batch",
             "Submit an async batch embedding job (POST /v1/batch/embeddings). "
             "For large corpora; poll with get_embed_batch. Models: "
             "jina-embeddings-v5-text-small / v5-text-nano.",
             {"inputs": {"type": "array", "items": {"type": "string"},
                         "description": "Texts to embed (or use input_url)"},
              "input_url": {"type": "string",
                            "description": "URL of a JSONL file of inputs"},
              "model": {"type": "string", "default": "jina-embeddings-v5-text-small"},
              "task": {"type": "string"},
              "dimensions": {"type": "integer"},
              "normalized": {"type": "boolean"},
              "webhook_url": {"type": "string"}},
             [], self._execute_embed_batch),
            ("get_embed_batch",
             "Get status of a batch embedding job; set include_output=true to fetch results.",
             {"batch_id": {"type": "string"},
              "include_output": {"type": "boolean", "default": False}},
             ["batch_id"], self._execute_get_embed_batch),
            ("cancel_embed_batch",
             "Cancel a batch embedding job (DELETE /v1/batch/{batch_id}).",
             {"batch_id": {"type": "string"}}, ["batch_id"],
             self._execute_cancel_embed_batch),
            ("list_embed_batches",
             "List recent batch embedding jobs (GET /v1/batches).",
             {"limit": {"type": "integer", "default": 20}}, [],
             self._execute_list_embed_batches),
            ("train_classifier",
             "Train or update a few-shot classifier (POST /v1/train). Returns a "
             "classifier_id usable with classify.",
             {"examples": {"type": "array",
                           "items": {"type": "object",
                                     "properties": {"text": {"type": "string"},
                                                    "label": {"type": "string"}}},
                           "description": "Labeled examples [{text, label}]"},
              "model": {"type": "string", "default": "jina-embeddings-v3"},
              "classifier_id": {"type": "string",
                                "description": "Existing classifier to update"},
              "num_iters": {"type": "integer"},
              "access": {"type": "string", "enum": ["public", "private"]}},
             ["examples"], self._execute_train_classifier),
            ("list_classifiers",
             "List your trained few-shot classifiers (GET /v1/classifiers).",
             {}, [], self._execute_list_classifiers),
            ("delete_classifier",
             "Delete a trained classifier (DELETE /v1/classifiers/{classifier_id}).",
             {"classifier_id": {"type": "string"}}, ["classifier_id"],
             self._execute_delete_classifier),
            ("list_models",
             "List available Jina models with metadata (GET /v1/models).",
             {"model_id": {"type": "string",
                           "description": "Get one model's metadata instead of the full list"}},
             [], self._execute_list_models),
        ]
        for _name, _desc, _props, _req, _handler in _extra_tools:
            self._tools[_name] = MCPTool(
                name=_name,
                description=_desc,
                input_schema={"type": "object", "properties": _props,
                              **({"required": _req} if _req else {})},
                handler=_handler,
            )

        # Federated search across sibling MCP backends
        self._tools["search_all"] = MCPTool(
            name="search_all",
            description=(
                "Federated search: fans the query out in parallel to web (Jina), "
                "research (deep_search), document-qa (search_documents), video-qa "
                "(search_video), memory (search_all), and the registry tool catalog. "
                "Per-source failures are recorded, never fatal. Compact response; "
                "full payload retrievable via get_search_result."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Max results per source (1-10)",
                        "default": 5
                    },
                    "sources": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["web", "research", "document-qa", "video-qa", "memory", "registry"]
                        },
                        "description": "Subset of sources to query (default: all)"
                    },
                    "timeout_s": {
                        "type": "number",
                        "description": "Per-source timeout in seconds",
                        "default": 10
                    },
                    "max_output_chars": {
                        "type": "integer",
                        "description": "Maximum output size in characters. Truncates if exceeded.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["query"]
            },
            handler=self._execute_search_all
        )

        # Concurrent multi-query search.
        # Named multi_search (not parallel_search) so it can't collide in the
        # central registry with distributed-research's parallel_search, which is
        # the actual Parallel.ai Search API.
        self._tools["multi_search"] = MCPTool(
            name="multi_search",
            description=(
                "Run web search for up to 10 queries concurrently (optionally federated "
                "across all sibling backends with federated=true). Merged compact results; "
                "full payload retrievable via get_search_result."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Queries to run concurrently (max 10)",
                        "maxItems": 10
                    },
                    "federated": {
                        "type": "boolean",
                        "description": "Also fan each query out to sibling MCP backends",
                        "default": False
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Max results per query (per source when federated)",
                        "default": 5
                    },
                    "max_output_chars": {
                        "type": "integer",
                        "description": "Maximum output size in characters. Truncates if exceeded.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["queries"]
            },
            handler=self._execute_multi_search
        )

        # Durable result lookup
        self._tools["get_search_result"] = MCPTool(
            name="get_search_result",
            description="Retrieve the full stored payload for a prior search/read/search_all/multi_search call by request_id or response_id.",
            input_schema={
                "type": "object",
                "properties": {
                    "request_id": {
                        "type": "string",
                        "description": "request_id returned by a prior call"
                    },
                    "response_id": {
                        "type": "string",
                        "description": "response_id returned by a prior call"
                    }
                }
            },
            handler=self._execute_get_search_result
        )

        self._tools["list_search_requests"] = MCPTool(
            name="list_search_requests",
            description="List recent stored search request metadata (request_id, tool, summary, status, created_at).",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (1-200)",
                        "default": 20
                    },
                    "tool": {
                        "type": "string",
                        "description": "Filter by tool name (e.g. search, search_all)"
                    }
                }
            },
            handler=self._execute_list_search_requests
        )

        # Connectivity test tool
        self._tools["_connectivity_test"] = MCPTool(
            name="_connectivity_test",
            description="Internal connectivity test for native MCP verification.",
            input_schema={
                "type": "object",
                "properties": {
                    "echo": {
                        "type": "string",
                        "description": "Echo string for verification"
                    }
                }
            },
            handler=self._execute_connectivity_test
        )

        # Integration feedback tool
        self._tools["integration_feedback"] = MCPTool(
            name="integration_feedback",
            description=(
                "Submit feedback about your experience using a tool on this integration "
                "server (correctness, schema clarity, missing capabilities, errors, etc). "
                "Call this after using a tool if something was confusing, broken, or "
                "worked great — it helps us improve the API for AI agents."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Which tool this feedback concerns (use 'general' for service-wide feedback)",
                    },
                    "outcome": {
                        "type": "string",
                        "enum": ["success", "partial", "failure"],
                        "description": "How the tool call(s) being reported on went",
                    },
                    "category": {
                        "type": "string",
                        "enum": [
                            "correctness", "schema_clarity", "performance", "error_handling",
                            "documentation", "missing_capability", "rate_limit", "auth", "other",
                        ],
                        "default": "other",
                    },
                    "rating": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "1 (poor) to 5 (excellent)",
                    },
                    "message": {
                        "type": "string",
                        "description": "Free-text feedback: what happened, what was confusing, what would help",
                    },
                    "suggested_fix": {"type": "string"},
                    "context": {
                        "type": "object",
                        "description": "Optional supporting context, e.g. the arguments used or error text received",
                    },
                    "session_id": {"type": "string"},
                    "trace_id": {"type": "string"},
                },
                "required": ["message"],
            },
            handler=self._execute_integration_feedback
        )

    async def start(self):
        """Start the MCP server and establish connectivity."""
        logger.info(f"Starting {self.SERVICE_NAME}...")

        self._state.status = ConnectionStatus.CONNECTING
        self._state.session_id = str(uuid.uuid4())

        # Initialize HTTP client
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers=headers
        )
        self._fed_client = httpx.AsyncClient(timeout=15.0)

        # Run connectivity test
        try:
            result = await self._execute_connectivity_test({"echo": "native-mcp-init"})
            if result.get("success"):
                self._state.status = ConnectionStatus.CONNECTED
                self._state.connected_at = datetime.utcnow()
                self._state.last_heartbeat = datetime.utcnow()
                logger.info(f"✓ {self.SERVICE_NAME} connected (session: {self._state.session_id[:8]}...)")
            else:
                raise RuntimeError("Connectivity test failed")
        except Exception as e:
            self._state.status = ConnectionStatus.ERROR
            self._state.error_message = str(e)
            logger.error(f"✗ {self.SERVICE_NAME} failed to connect: {e}")
            raise

        # Announce to Tool Management
        await self._announce_to_registry()

    async def stop(self):
        """Stop the MCP server."""
        logger.info(f"Stopping {self.SERVICE_NAME}...")

        if self._client:
            await self._client.aclose()
            self._client = None

        if self._fed_client:
            await self._fed_client.aclose()
            self._fed_client = None

        self._state.status = ConnectionStatus.DISCONNECTED
        self._state.connected_at = None
        logger.info(f"✓ {self.SERVICE_NAME} stopped")

    async def _announce_to_registry(self):
        """Announce this server and its tools to Tool Management."""
        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        service_url = os.getenv("SERVICE_URL", f"http://localhost:{self.port}")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/tools/announce",
                    json={
                        "backend_id": self.SERVICE_ID,
                        "name": self.SERVICE_NAME,
                        "url": service_url,
                        "health_endpoint": "/health",
                        "mcp_endpoint": "/mcp",
                        "capabilities": list(self._tools.keys()),
                        "tools": self.tools,
                    },
                    timeout=15.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Announced {result.get('registered', 0)} tools to {registry_url}")
                else:
                    logger.warning(f"Announce returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            logger.warning(f"Could not announce to registry: {e}")

    # =========================================================================
    # MCP PROTOCOL HANDLERS
    # =========================================================================

    async def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP initialize request."""
        return {
            "protocolVersion": MCPProtocolVersion.V1.value,
            "capabilities": {
                "tools": {},
            },
            "serverInfo": {
                "name": self.SERVICE_NAME,
                "version": self.VERSION,
            }
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request."""
        tools = []
        for name, tool in self._tools.items():
            if not name.startswith("_"):  # Hide internal tools
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema
                })
        return {"tools": tools}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request."""
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")

        if not tool.handler:
            raise ValueError(f"Tool {tool_name} has no handler")

        result = await tool.handler(arguments)

        mcp_result = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result) if isinstance(result, dict) else str(result)
                }
            ]
        }

        # Apply output guard: enforce size limits, cap links, capture URLs
        if OUTPUT_GUARD_AVAILABLE:
            # Allow per-call overrides from tool arguments
            per_call_config = None
            max_chars = arguments.get("max_output_chars")
            max_links = arguments.get("max_links")
            if max_chars or max_links:
                per_call_config = OutputGuardConfig(
                    max_output_chars=max_chars or 32_000,
                    max_links=max_links or 50,
                )

            mcp_result = guard_mcp_content(
                mcp_result,
                tool_name=tool_name,
                config=per_call_config,
                source_context={"server": self.SERVICE_ID},
            )

        return mcp_result

    async def handle_mcp_request(self, request: MCPRequest) -> MCPResponse:
        """Route MCP request to appropriate handler."""
        try:
            handlers = {
                "initialize": self.handle_initialize,
                "tools/list": self.handle_tools_list,
                "tools/call": self.handle_tools_call,
            }

            handler = handlers.get(request.method)
            if not handler:
                return MCPResponse(
                    id=request.id,
                    error={
                        "code": -32601,
                        "message": f"Method not found: {request.method}"
                    }
                )

            result = await handler(request.params or {})
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32000,
                    "message": str(e)
                }
            )

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    def _record_request(
        self,
        tool: str,
        args: Dict[str, Any],
        full_result: Any,
        summary: str,
        sources: Any = None,
        status: str = "ok",
    ) -> tuple[str, str]:
        """Persist the full payload; return (request_id, response_id)."""
        request_id = str(uuid.uuid4())
        response_id = str(uuid.uuid4())
        try:
            self._store.save(
                request_id=request_id,
                response_id=response_id,
                tool=tool,
                arguments=args,
                summary=summary,
                full_result=full_result,
                sources=sources,
                status=status,
            )
        except Exception as e:
            logger.warning(f"Failed to persist {tool} request: {e}")
        return request_id, response_id

    async def _jina_search_raw(
        self,
        query: str,
        num_results: int,
        site: Optional[str] = None,
        country: Optional[str] = None,
        language: Optional[str] = None,
        page: Optional[int] = None,
        location: Optional[str] = None,
        serp_only: bool = False,
        favicons: bool = False,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Raw Jina web search (POST body per current API). Never raises."""
        try:
            headers = {"Accept": "application/json"}
            if site:
                headers["X-Site"] = site
            if serp_only:
                headers["X-Respond-With"] = "no-content"
            if favicons:
                headers["X-With-Favicons"] = "true"
            if extra_headers:
                headers.update({str(k): str(v) for k, v in extra_headers.items()})
            body: Dict[str, Any] = {"q": query, "num": min(int(num_results or 10), 20)}
            if country:
                body["gl"] = country
            if language:
                body["hl"] = language
            if page:
                body["page"] = int(page)
            if location:
                body["location"] = location
            response = await self._client.post(
                f"{self.JINA_SEARCH_URL}/",
                headers=headers,
                json=body,
                timeout=30.0
            )
            if response.status_code in (404, 405):
                # Older gateway: fall back to the GET path form
                response = await self._client.get(
                    f"{self.JINA_SEARCH_URL}/{query}",
                    headers=headers,
                    timeout=30.0
                )

            if response.status_code == 200:
                data = response.json()
                return {
                    "query": query,
                    "results": data.get("data", [])[:num_results],
                    "success": True,
                }

            detail = ""
            try:
                body = response.json()
                detail = body.get("readableMessage") or body.get("message") or ""
            except Exception:
                detail = response.text[:200]
            hint = " (Jina account has insufficient balance)" if response.status_code == 402 else ""
            return {
                "query": query,
                "error": f"Jina search failed: HTTP {response.status_code}{hint}"
                         + (f" - {_snip(detail, 200)}" if detail else ""),
                "success": False,
            }
        except Exception as e:
            return {
                "query": query,
                "error": f"Jina search error: {type(e).__name__}: {e}",
                "success": False,
            }

    async def _execute_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute web search via Jina. Compact response; full payload stored durably."""
        query = args.get("query")
        if not query:
            raise ValueError("query is required")

        num_results = min(args.get("num_results", 10), 20)
        max_out = int(args.get("max_output_chars") or DEFAULT_MAX_OUTPUT_CHARS)

        # Optional user/session context for URL queuing
        user_id = args.get("user_id")
        session_id = args.get("session_id") or (self._state.session_id if self._state else None)

        raw = await self._jina_search_raw(
            query, num_results,
            site=args.get("site"),
            country=args.get("country"),
            language=args.get("language"),
            page=args.get("page"),
            location=args.get("location"),
            serp_only=bool(args.get("serp_only")),
            favicons=bool(args.get("favicons")),
            extra_headers=args.get("extra_headers"),
        )
        results = raw.get("results", [])

        queued_count = 0
        if raw.get("success") and results:
            queued_count = await self._queue_search_urls(
                results=results,
                query=query,
                user_id=user_id,
                session_id=session_id,
            )
            raw["urls_queued"] = queued_count

        urls = [r.get("url") or r.get("link") for r in results if isinstance(r, dict)]
        summary = (
            f"web search '{_snip(query, 80)}': {len(results)} results"
            if raw.get("success") else
            f"web search '{_snip(query, 80)}' failed: {_snip(raw.get('error'), 120)}"
        )
        request_id, response_id = self._record_request(
            "search", args, raw, summary,
            sources=[u for u in urls if u],
            status="ok" if raw.get("success") else "error",
        )

        compact: Dict[str, Any] = {
            "request_id": request_id,
            "response_id": response_id,
            "query": query,
            "success": raw.get("success", False),
        }
        if raw.get("success"):
            compact["count"] = len(results)
            compact["results"] = [_compact_item(r) for r in results]
            if queued_count:
                compact["urls_queued"] = queued_count
        else:
            compact["error"] = raw.get("error")
        compact["hint"] = "Full payload: get_search_result {request_id}"
        return _shrink_to_budget(compact, max_out)

    async def _queue_search_urls(
        self,
        results: List[Dict[str, Any]],
        query: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> int:
        """Queue URLs from search results for future processing."""
        if not URL_QUEUE_AVAILABLE:
            return 0

        if not results:
            return 0

        try:
            url_queue = get_url_queue()

            # Extract URL data from search results
            urls_to_queue = []
            for idx, result in enumerate(results):
                url = result.get("url") or result.get("link")
                if url:
                    # Get full content if available, otherwise use snippet
                    full_content = result.get("content")
                    snippet = result.get("snippet") or result.get("description")

                    urls_to_queue.append({
                        "url": url,
                        "title": result.get("title"),
                        "description": snippet[:500] if snippet else None,
                        "content": full_content,  # Cache the full page content
                        "metadata": {
                            "source_type": "jina_search",
                            "position": idx,
                            "has_full_content": full_content is not None,
                        }
                    })

            # Batch enqueue
            queued = url_queue.enqueue_batch(
                urls=urls_to_queue,
                source=URLSource.SEARCH,
                source_query=query,
                user_id=user_id,
                session_id=session_id,
            )

            logger.info(f"Queued {len(queued)} URLs from search query: {query[:50]}...")
            return len(queued)

        except Exception as e:
            logger.warning(f"Failed to queue search URLs: {e}")
            return 0

    async def _execute_read(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Read a URL via Jina. Compact preview; full content stored durably."""
        url = args.get("url")
        if not url:
            raise ValueError("url is required")

        max_out = int(args.get("max_output_chars") or DEFAULT_MAX_OUTPUT_CHARS)

        raw: Dict[str, Any]
        try:
            read_headers = {"Accept": "application/json"}
            header_map = {
                "return_format": "X-Return-Format",
                "target_selector": "X-Target-Selector",
                "wait_for_selector": "X-Wait-For-Selector",
                "remove_selector": "X-Remove-Selector",
                "engine": "X-Engine",
                "respond_with": "X-Respond-With",
                "token_budget": "X-Token-Budget",
                "timeout": "X-Timeout",
                "retain_images": "X-Retain-Images",
                "locale": "X-Locale",
                "proxy": "X-Proxy",
            }
            for arg_key, header in header_map.items():
                if args.get(arg_key) is not None:
                    read_headers[header] = str(args[arg_key])
            for flag_key, header in (
                ("no_cache", "X-No-Cache"),
                ("with_links_summary", "X-With-Links-Summary"),
                ("with_images_summary", "X-With-Images-Summary"),
                ("include_images", "X-With-Generated-Alt"),
                ("with_iframe", "X-With-Iframe"),
                ("with_shadow_dom", "X-With-Shadow-Dom"),
            ):
                if args.get(flag_key):
                    read_headers[header] = "true"
            # Raw escape hatch for the long tail of X-* reader options
            if isinstance(args.get("extra_headers"), dict):
                read_headers.update({str(k): str(v) for k, v in args["extra_headers"].items()})
            response = await self._client.get(
                f"{self.JINA_READ_URL}/{url}",
                headers=read_headers,
                timeout=60.0
            )

            if response.status_code == 200:
                data = response.json()
                # Jina Reader wraps response in a "data" field
                inner = data.get("data", {}) if isinstance(data.get("data"), dict) else data
                raw = {
                    "url": url,
                    "title": inner.get("title", ""),
                    "content": inner.get("content", ""),
                    "success": True
                }
            else:
                detail = ""
                try:
                    body = response.json()
                    detail = body.get("readableMessage") or body.get("message") or ""
                except Exception:
                    detail = response.text[:200]
                hint = " (Jina account has insufficient balance)" if response.status_code == 402 else ""
                raw = {
                    "url": url,
                    "error": f"Jina read failed: HTTP {response.status_code}{hint}"
                             + (f" - {_snip(detail, 200)}" if detail else ""),
                    "success": False
                }
        except Exception as e:
            raw = {
                "url": url,
                "error": f"Jina read error: {type(e).__name__}: {e}",
                "success": False
            }

        content = raw.get("content", "") or ""
        summary = (
            f"read {url}: {len(content)} chars"
            if raw.get("success") else
            f"read {url} failed: {_snip(raw.get('error'), 120)}"
        )
        request_id, response_id = self._record_request(
            "read", args, raw, summary, sources=[url],
            status="ok" if raw.get("success") else "error",
        )

        compact: Dict[str, Any] = {
            "request_id": request_id,
            "response_id": response_id,
            "url": url,
            "success": raw.get("success", False),
        }
        if raw.get("success"):
            preview_budget = max(200, max_out - 600)
            compact["title"] = _snip(raw.get("title", ""), 200)
            compact["content_chars"] = len(content)
            compact["content_preview"] = _snip(content, preview_budget)
        else:
            compact["error"] = raw.get("error")
        compact["hint"] = "Full payload: get_search_result {request_id}"
        return _shrink_to_budget(compact, max_out)

    async def _execute_verify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Fact-check a statement."""
        statement = args.get("statement") or args.get("claim")
        if not statement:
            raise ValueError("statement (or claim) is required")

        # Search for evidence
        search_result = await self._execute_search({
            "query": f"fact check: {statement}",
            "num_results": 5
        })

        return {
            "statement": statement,
            "sources": search_result.get("results", []),
            "note": "Review sources to verify claim",
            "success": search_result.get("success", False)
        }

    async def _execute_rerank(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Rerank documents by relevance."""
        query = args.get("query")
        documents = args.get("documents", [])
        top_k = args.get("top_k", 5)

        if not query or not documents:
            raise ValueError("query and documents are required")

        if not self.api_key:
            return {
                "error": "API key required for reranking",
                "success": False
            }

        try:
            rerank_body: Dict[str, Any] = {
                "model": args.get("model", "jina-reranker-v2-base-multilingual"),
                "query": query,
                "documents": documents,
                "top_n": top_k,
            }
            for key in ("return_documents", "return_embeddings"):
                if args.get(key) is not None:
                    rerank_body[key] = bool(args[key])
            if args.get("max_doc_length"):
                rerank_body["max_doc_length"] = int(args["max_doc_length"])
            response = await self._client.post(
                self.JINA_RERANK_URL,
                json=rerank_body,
                timeout=60.0
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "query": query,
                    "results": data.get("results", []),
                    "success": True
                }
            else:
                return {
                    "error": f"Rerank failed: {response.status_code}",
                    "success": False
                }
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _execute_embed(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generate embeddings for text batches or multimodal inputs."""
        raw_inputs = args.get("inputs")
        if raw_inputs:
            input_list = list(raw_inputs)[:100]
        else:
            texts = args.get("texts")
            if not texts:
                single = args.get("text")
                texts = [single] if single else None
            if not texts:
                raise ValueError("text, texts, or inputs is required")
            input_list = [str(t) for t in texts][:100]
        model = args.get("model", "jina-embeddings-v3")

        if not self.api_key:
            return {
                "error": "API key required for embeddings",
                "success": False
            }

        body: Dict[str, Any] = {"model": model, "input": input_list}
        if args.get("task"):
            body["task"] = args["task"]
        if args.get("dimensions"):
            body["dimensions"] = int(args["dimensions"])
        for key in ("late_chunking", "normalized", "truncate", "return_multivector"):
            if args.get(key) is not None:
                body[key] = bool(args[key])
        if args.get("embedding_type"):
            body["embedding_type"] = args["embedding_type"]
        texts = [i for i in input_list if isinstance(i, str)]

        try:
            response = await self._client.post(
                self.JINA_EMBED_URL,
                json=body,
                timeout=60.0
            )

            if response.status_code == 200:
                data = response.json()
                embeddings = data.get("data", [])
                result: Dict[str, Any] = {
                    "model": model,
                    "count": len(embeddings),
                    "usage": data.get("usage"),
                    "success": True
                }
                if args.get("return_multivector"):
                    # Multi-vector (late interaction) output: per-token vectors
                    result["data"] = embeddings
                elif len(input_list) == 1 and len(texts) == 1:
                    result["text"] = texts[0][:100] + "..." if len(texts[0]) > 100 else texts[0]
                    result["embedding"] = embeddings[0].get("embedding") if embeddings else None
                    result["dimensions"] = len(embeddings[0].get("embedding") or []) if embeddings else 0
                else:
                    result["embeddings"] = [e.get("embedding") for e in embeddings]
                    result["dimensions"] = len(embeddings[0].get("embedding") or []) if embeddings else 0
                return result
            else:
                return {
                    "error": f"Embed failed: {response.status_code} - {_snip(response.text, 200)}",
                    "success": False
                }
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _execute_deepsearch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Jina DeepSearch: iterative search-read-reason with citations."""
        query = args.get("query")
        if not query:
            raise ValueError("query is required")
        if not self.api_key:
            return {"error": "API key required for DeepSearch", "success": False}

        max_out = int(args.get("max_output_chars") or DEFAULT_MAX_OUTPUT_CHARS)
        body: Dict[str, Any] = {
            "model": "jina-deepsearch-v1",
            "messages": [{"role": "user", "content": query}],
            "stream": False,
            "reasoning_effort": args.get("reasoning_effort", "medium"),
        }
        for key in ("budget_tokens", "max_attempts", "team_size", "max_returned_urls"):
            if args.get(key) is not None:
                body[key] = int(args[key])
        for key in ("no_direct_answer",):
            if args.get(key) is not None:
                body[key] = bool(args[key])
        for key in ("boost_hostnames", "bad_hostnames", "only_hostnames",
                    "search_language_code", "search_provider", "response_format"):
            if args.get(key) is not None:
                body[key] = args[key]

        raw: Dict[str, Any]
        try:
            response = await self._client.post(
                self.JINA_DEEPSEARCH_URL, json=body, timeout=300.0
            )
            if response.status_code == 200:
                data = response.json()
                choice = (data.get("choices") or [{}])[0]
                message = choice.get("message") or {}
                raw = {
                    "query": query,
                    "answer": message.get("content", ""),
                    "annotations": message.get("annotations", []),
                    "visited_urls": data.get("visitedURLs", []),
                    "read_urls": data.get("readURLs", []),
                    "usage": data.get("usage"),
                    "success": True,
                }
            else:
                raw = {
                    "query": query,
                    "error": f"DeepSearch failed: HTTP {response.status_code} - {_snip(response.text, 300)}",
                    "success": False,
                }
        except Exception as e:
            raw = {"query": query, "error": f"DeepSearch error: {type(e).__name__}: {e}", "success": False}

        summary = (
            f"deepsearch '{_snip(query, 80)}': {len(raw.get('answer') or '')} chars, "
            f"{len(raw.get('read_urls') or [])} urls read"
            if raw.get("success") else
            f"deepsearch '{_snip(query, 80)}' failed: {_snip(raw.get('error'), 120)}"
        )
        request_id, response_id = self._record_request(
            "deepsearch", args, raw, summary,
            sources=raw.get("read_urls") or [],
            status="ok" if raw.get("success") else "error",
        )

        compact: Dict[str, Any] = {
            "request_id": request_id,
            "response_id": response_id,
            "query": query,
            "success": raw.get("success", False),
        }
        if raw.get("success"):
            answer = raw.get("answer") or ""
            compact["answer"] = _snip(answer, max(500, max_out - 800))
            compact["answer_chars"] = len(answer)
            compact["read_urls"] = (raw.get("read_urls") or [])[:10]
        else:
            compact["error"] = raw.get("error")
        compact["hint"] = "Full payload: get_search_result {request_id}"
        return _shrink_to_budget(compact, max_out)

    async def _execute_classify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Zero-shot classification via Jina Classifier API."""
        texts = args.get("texts")
        labels = args.get("labels")
        classifier_id = args.get("classifier_id")
        if not texts:
            raise ValueError("texts is required")
        if not labels and not classifier_id:
            raise ValueError("labels is required (unless using a trained classifier_id)")
        if isinstance(texts, str):
            texts = [texts]
        if not self.api_key:
            return {"error": "API key required for classification", "success": False}

        try:
            # Preserve dict inputs ({text} or {image}) for multimodal classification
            classify_input = [t if isinstance(t, dict) else str(t) for t in texts]
            classify_body: Dict[str, Any] = {"input": classify_input}
            if labels:
                classify_body["labels"] = [str(l) for l in labels]
            if classifier_id:
                classify_body["classifier_id"] = classifier_id
            else:
                classify_body["model"] = args.get("model", "jina-embeddings-v3")
            response = await self._client.post(
                self.JINA_CLASSIFY_URL,
                json=classify_body,
                timeout=60.0
            )
            if response.status_code == 200:
                data = response.json()
                return {
                    "predictions": data.get("data", []),
                    "usage": data.get("usage"),
                    "success": True,
                }
            return {
                "error": f"Classify failed: HTTP {response.status_code} - {_snip(response.text, 200)}",
                "success": False,
            }
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _execute_segment(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Chunk text via Jina Segmenter API (falls back to segment.jina.ai host)."""
        content = args.get("content")
        if not content:
            raise ValueError("content is required")
        if not self.api_key:
            return {"error": "API key required for segmentation", "success": False}

        body: Dict[str, Any] = {
            "content": content,
            "return_chunks": bool(args.get("return_chunks", True)),
            "max_chunk_length": int(args.get("max_chunk_length") or 1000),
        }
        if args.get("tokenizer"):
            body["tokenizer"] = args["tokenizer"]

        last_err = None
        for endpoint in (self.JINA_SEGMENT_URL, "https://segment.jina.ai/"):
            try:
                response = await self._client.post(endpoint, json=body, timeout=60.0)
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "num_chunks": data.get("num_chunks") or len(data.get("chunks") or []),
                        "num_tokens": data.get("num_tokens"),
                        "chunks": data.get("chunks", []),
                        "chunk_positions": data.get("chunk_positions"),
                        "success": True,
                    }
                last_err = f"HTTP {response.status_code} - {_snip(response.text, 200)}"
                if response.status_code != 404:
                    break
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
        return {"error": f"Segment failed: {last_err}", "success": False}

    async def _jina_api(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 60.0,
        raw_text: bool = False,
    ) -> Dict[str, Any]:
        """Generic api.jina.ai call. Returns {success, data|error}."""
        if not self.api_key:
            return {"error": "API key required", "success": False}
        try:
            response = await self._client.request(
                method, f"{self.JINA_API_BASE}{path}",
                json=json_body, params=params, timeout=timeout,
            )
            if 200 <= response.status_code < 300:
                if raw_text:
                    return {"success": True, "data": response.text}
                try:
                    return {"success": True, "data": response.json()}
                except Exception:
                    return {"success": True, "data": response.text}
            return {
                "error": f"HTTP {response.status_code} - {_snip(response.text, 300)}",
                "success": False,
            }
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}", "success": False}

    async def _execute_embed_batch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/batch/embeddings - async batch embedding job."""
        body: Dict[str, Any] = {"model": args.get("model", "jina-embeddings-v5-text-small")}
        if args.get("inputs"):
            body["input"] = [str(t) for t in args["inputs"]]
        elif args.get("input_url"):
            body["input_url"] = args["input_url"]
        else:
            raise ValueError("inputs or input_url is required")
        for key in ("task", "dimensions", "normalized", "webhook_url"):
            if args.get(key) is not None:
                body[key] = args[key]
        res = await self._jina_api("POST", "/v1/batch/embeddings", json_body=body, timeout=120.0)
        if not res["success"]:
            return res
        data = res["data"]
        batch_id = data.get("batch_id") or data.get("id") if isinstance(data, dict) else None
        return {"batch_id": batch_id, "job": data,
                "hint": "Poll with get_embed_batch {batch_id}", "success": True}

    async def _execute_get_embed_batch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/batch/{batch_id} (+ /output when include_output)."""
        batch_id = args.get("batch_id")
        if not batch_id:
            raise ValueError("batch_id is required")
        res = await self._jina_api("GET", f"/v1/batch/{batch_id}")
        if not res["success"]:
            return {"batch_id": batch_id, **res}
        out: Dict[str, Any] = {"batch_id": batch_id, "job": res["data"], "success": True}
        if args.get("include_output"):
            output = await self._jina_api("GET", f"/v1/batch/{batch_id}/output",
                                          timeout=120.0, raw_text=True)
            if output["success"]:
                lines = [l for l in str(output["data"]).splitlines() if l.strip()]
                parsed = []
                for line in lines:
                    try:
                        parsed.append(json.loads(line))
                    except json.JSONDecodeError:
                        parsed.append({"raw": line})
                out["output"] = parsed
            else:
                out["output_error"] = output.get("error")
        return out

    async def _execute_cancel_embed_batch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """DELETE /v1/batch/{batch_id}."""
        batch_id = args.get("batch_id")
        if not batch_id:
            raise ValueError("batch_id is required")
        res = await self._jina_api("DELETE", f"/v1/batch/{batch_id}")
        if not res["success"]:
            return {"batch_id": batch_id, **res}
        return {"batch_id": batch_id, "cancelled": True, "success": True}

    async def _execute_list_embed_batches(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/batches."""
        res = await self._jina_api("GET", "/v1/batches",
                                   params={"limit": int(args.get("limit") or 20)})
        if not res["success"]:
            return res
        return {"batches": res["data"], "success": True}

    async def _execute_train_classifier(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/train - create/update a few-shot classifier."""
        examples = args.get("examples")
        if not examples:
            raise ValueError("examples is required")
        body: Dict[str, Any] = {
            "model": args.get("model", "jina-embeddings-v3"),
            "input": examples,
        }
        for key in ("classifier_id", "num_iters", "access"):
            if args.get(key) is not None:
                body[key] = args[key]
        res = await self._jina_api("POST", "/v1/train", json_body=body, timeout=120.0)
        if not res["success"]:
            return res
        data = res["data"]
        return {"classifier_id": data.get("classifier_id") if isinstance(data, dict) else None,
                "result": data, "success": True}

    async def _execute_list_classifiers(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/classifiers."""
        res = await self._jina_api("GET", "/v1/classifiers")
        if not res["success"]:
            return res
        return {"classifiers": res["data"], "success": True}

    async def _execute_delete_classifier(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """DELETE /v1/classifiers/{classifier_id}."""
        classifier_id = args.get("classifier_id")
        if not classifier_id:
            raise ValueError("classifier_id is required")
        res = await self._jina_api("DELETE", f"/v1/classifiers/{classifier_id}")
        if not res["success"]:
            return {"classifier_id": classifier_id, **res}
        return {"classifier_id": classifier_id, "deleted": True, "success": True}

    async def _execute_list_models(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/models[/{model_id}]."""
        model_id = args.get("model_id")
        path = f"/v1/models/{model_id}" if model_id else "/v1/models"
        res = await self._jina_api("GET", path)
        if not res["success"]:
            return res
        return {"models": res["data"], "success": True}

    # =========================================================================
    # FEDERATED SEARCH
    # =========================================================================

    async def _call_mcp_backend(
        self,
        source: str,
        base_url: str,
        tool: str,
        arguments: Dict[str, Any],
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """tools/call against a sibling MCP backend. Never raises."""
        started = time.time()
        client = self._fed_client or self._client
        try:
            response = await client.post(
                f"{base_url.rstrip('/')}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {"name": tool, "arguments": arguments},
                    "id": 1,
                },
                timeout=timeout,
            )
            elapsed_ms = round((time.time() - started) * 1000)
            if response.status_code != 200:
                return {
                    "source": source, "ok": False, "elapsed_ms": elapsed_ms,
                    "error": f"HTTP {response.status_code}: {_snip(response.text, 200)}",
                }
            data = response.json()
            if data.get("error"):
                err = data["error"]
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                return {
                    "source": source, "ok": False, "elapsed_ms": elapsed_ms,
                    "error": _snip(msg, 300),
                }
            result = data.get("result", {})
            payload: Any = result
            is_error = bool(isinstance(result, dict) and result.get("isError"))
            content = result.get("content") if isinstance(result, dict) else None
            if isinstance(content, list) and content:
                text = content[0].get("text", "")
                try:
                    payload = json.loads(text)
                except (json.JSONDecodeError, TypeError):
                    payload = {"text": text}
            entry = {
                "source": source, "ok": not is_error, "elapsed_ms": elapsed_ms,
                "tool": tool, "payload": payload,
            }
            if is_error:
                entry["error"] = _snip(payload, 300)
            return entry
        except Exception as e:
            return {
                "source": source, "ok": False,
                "elapsed_ms": round((time.time() - started) * 1000),
                "error": f"{type(e).__name__}: {e}",
            }

    async def _registry_search_source(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """Search the registry tool catalog. Never raises."""
        started = time.time()
        client = self._fed_client or self._client
        headers = {}
        token = os.getenv("TOOLMANAGEMENT_BEARER_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = await client.get(
                f"{_registry_base_url()}/api/v1/tools",
                params={"search": query, "limit": limit},
                headers=headers,
                timeout=10.0,
            )
            elapsed_ms = round((time.time() - started) * 1000)
            if response.status_code != 200:
                return {
                    "source": "registry", "ok": False, "elapsed_ms": elapsed_ms,
                    "error": f"HTTP {response.status_code}: {_snip(response.text, 200)}",
                }
            return {
                "source": "registry", "ok": True, "elapsed_ms": elapsed_ms,
                "payload": response.json(),
            }
        except Exception as e:
            return {
                "source": "registry", "ok": False,
                "elapsed_ms": round((time.time() - started) * 1000),
                "error": f"{type(e).__name__}: {e}",
            }

    async def _web_search_source(self, query: str, num_results: int) -> Dict[str, Any]:
        """Jina web search wrapped as a federated source. Never raises."""
        started = time.time()
        raw = await self._jina_search_raw(query, num_results)
        entry = {
            "source": "web",
            "ok": raw.get("success", False),
            "elapsed_ms": round((time.time() - started) * 1000),
            "payload": raw,
        }
        if not raw.get("success"):
            entry["error"] = _snip(raw.get("error"), 300)
        return entry

    async def _federated_search(
        self,
        query: str,
        num_results: int = 5,
        sources: Optional[List[str]] = None,
        timeout: float = 10.0,
        semaphore: Optional[asyncio.Semaphore] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Fan the query out in parallel to all federated backends."""
        sem = semaphore or asyncio.Semaphore(FEDERATED_CONCURRENCY)

        factories: List[tuple[str, Callable[[], Any]]] = [
            ("web", lambda: self._web_search_source(query, num_results)),
            ("research", lambda: self._call_mcp_backend(
                "research", RESEARCH_MCP_URL, "deep_search",
                {"objective": query, "depth": "quick", "max_sources": 3},
                # deep_search does real multi-source work; give it headroom
                max(timeout, 30.0),
            )),
            ("document-qa", lambda: self._call_mcp_backend(
                "document-qa", DOCUMENT_MCP_URL, "search_documents",
                {"query": query, "n_results": num_results},
                timeout,
            )),
            ("video-qa", lambda: self._call_mcp_backend(
                "video-qa", VIDEO_MCP_URL, "search_video",
                {"query": query, "n_results": num_results},
                timeout,
            )),
            ("memory", lambda: self._call_mcp_backend(
                "memory", MEMORY_MCP_URL, "search_all",
                {"query": query, "limit_per_tier": min(num_results, 5)},
                timeout,
            )),
            ("registry", lambda: self._registry_search_source(query, limit=5)),
        ]
        if sources:
            wanted = set(sources)
            factories = [(name, f) for name, f in factories if name in wanted]

        async def run(factory: Callable[[], Any]) -> Dict[str, Any]:
            async with sem:
                return await factory()

        results = await asyncio.gather(*(run(f) for _, f in factories))
        return {r["source"]: r for r in results}

    def _compact_federated(
        self, fed: Dict[str, Dict[str, Any]], num_results: int
    ) -> Dict[str, Any]:
        """Compact per-source view of federated results."""
        compact: Dict[str, Any] = {}
        for name, r in fed.items():
            entry: Dict[str, Any] = {"ok": r.get("ok", False), "elapsed_ms": r.get("elapsed_ms")}
            if not r.get("ok"):
                entry["error"] = _snip(r.get("error", "unknown error"), 200)
            else:
                items = _extract_items(r.get("payload"))
                if items:
                    entry["count"] = len(items)
                    entry["items"] = [_compact_item(i) for i in items[:num_results]]
                else:
                    entry["summary"] = _snip(r.get("payload"), SNIPPET_CHARS)
            compact[name] = entry
        return compact

    async def _execute_search_all(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Federated search across web + sibling MCP backends + registry."""
        query = args.get("query")
        if not query:
            raise ValueError("query is required")

        num_results = max(1, min(int(args.get("num_results") or 5), 10))
        timeout = max(1.0, min(float(args.get("timeout_s") or 10.0), 30.0))
        max_out = int(args.get("max_output_chars") or DEFAULT_MAX_OUTPUT_CHARS)
        sources = args.get("sources")

        fed = await self._federated_search(
            query, num_results=num_results, sources=sources, timeout=timeout
        )

        ok_sources = [n for n, r in fed.items() if r.get("ok")]
        failed_sources = {n: _snip(r.get("error"), 120) for n, r in fed.items() if not r.get("ok")}
        summary = (
            f"federated search '{_snip(query, 80)}': "
            f"{len(ok_sources)}/{len(fed)} sources ok ({', '.join(ok_sources) or 'none'})"
        )
        request_id, response_id = self._record_request(
            "search_all", args, {"query": query, "sources": fed}, summary,
            sources=list(fed.keys()),
            status="ok" if ok_sources else "error",
        )

        compact = {
            "request_id": request_id,
            "response_id": response_id,
            "query": query,
            "sources_ok": len(ok_sources),
            "sources_total": len(fed),
            "sources": self._compact_federated(fed, num_results),
            "success": bool(ok_sources),
        }
        if failed_sources:
            compact["failures"] = failed_sources
        compact["hint"] = "Full payload: get_search_result {request_id}"
        return _shrink_to_budget(compact, max_out)

    async def _execute_multi_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Run web (or federated) search for multiple queries concurrently."""
        queries = args.get("queries")
        if not queries or not isinstance(queries, list):
            raise ValueError("queries (array of strings) is required")
        queries = [str(q) for q in queries if str(q).strip()][:10]
        if not queries:
            raise ValueError("queries must contain at least one non-empty string")

        federated = bool(args.get("federated", False))
        num_results = max(1, min(int(args.get("num_results") or 5), 10))
        max_out = int(args.get("max_output_chars") or DEFAULT_MAX_OUTPUT_CHARS)

        sem = asyncio.Semaphore(FEDERATED_CONCURRENCY)

        async def run_query(q: str) -> tuple[str, Any]:
            if federated:
                fed = await self._federated_search(
                    q, num_results=num_results, semaphore=sem
                )
                return q, fed
            async with sem:
                return q, await self._jina_search_raw(q, num_results)

        pairs = await asyncio.gather(*(run_query(q) for q in queries))
        full = {q: r for q, r in pairs}

        compact_results: Dict[str, Any] = {}
        ok_count = 0
        for q, r in pairs:
            if federated:
                fed_ok = [n for n, s in r.items() if s.get("ok")]
                compact_results[q] = {
                    "sources_ok": len(fed_ok),
                    "sources_total": len(r),
                    "sources": self._compact_federated(r, num_results),
                }
                if fed_ok:
                    ok_count += 1
            else:
                if r.get("success"):
                    ok_count += 1
                    items = r.get("results", [])
                    compact_results[q] = {
                        "ok": True,
                        "count": len(items),
                        "items": [_compact_item(i) for i in items[:num_results]],
                    }
                else:
                    compact_results[q] = {"ok": False, "error": _snip(r.get("error"), 200)}

        summary = (
            f"multi_search ({'federated' if federated else 'web'}): "
            f"{ok_count}/{len(queries)} queries ok"
        )
        request_id, response_id = self._record_request(
            "multi_search", args, full, summary,
            sources=queries,
            status="ok" if ok_count else "error",
        )

        compact = {
            "request_id": request_id,
            "response_id": response_id,
            "federated": federated,
            "queries_ok": ok_count,
            "queries_total": len(queries),
            "results": compact_results,
            "success": bool(ok_count),
            "hint": "Full payload: get_search_result {request_id}",
        }
        return _shrink_to_budget(compact, max_out)

    async def _execute_get_search_result(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Return the stored full payload for a prior request."""
        request_id = args.get("request_id")
        response_id = args.get("response_id")
        if not request_id and not response_id:
            raise ValueError("request_id or response_id is required")

        record = self._store.get(request_id=request_id, response_id=response_id)
        if not record:
            return {
                "success": False,
                "error": f"No stored result for {'request_id ' + request_id if request_id else 'response_id ' + str(response_id)}",
            }
        record["success"] = True
        return record

    async def _execute_list_search_requests(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List recent stored search request metadata."""
        limit = args.get("limit", 20)
        tool = args.get("tool")
        rows = self._store.list(limit=limit, tool=tool)
        return {"success": True, "count": len(rows), "requests": rows}

    async def _execute_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Internal connectivity verification."""
        echo = args.get("echo", "test")

        return {
            "success": True,
            "echo": echo,
            "server_id": self.SERVICE_ID,
            "version": self.VERSION,
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": self._state.session_id,
            "tools_registered": len(self._tools),
            "api_key_configured": bool(self.api_key),
        }

    async def _execute_integration_feedback(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Forward agent feedback about this server's tools to the central registry."""
        message = args.get("message")
        if not message:
            return {
                "content": [{"type": "text", "text": "message is required"}],
                "isError": True,
            }

        payload = {
            "backend_id": self.SERVICE_ID,
            "message": message,
            "tool_name": args.get("tool_name"),
            "outcome": args.get("outcome"),
            "category": args.get("category") or "other",
            "rating": args.get("rating"),
            "suggested_fix": args.get("suggested_fix"),
            "context": args.get("context"),
            "session_id": args.get("session_id"),
            "trace_id": args.get("trace_id"),
        }

        registry_url = _registry_base_url()
        try:
            client = self._fed_client or self._client
            if client is not None:
                response = await client.post(
                    f"{registry_url}/api/v1/integration-feedback",
                    json=payload,
                    timeout=10.0,
                )
            else:
                async with httpx.AsyncClient(timeout=10.0) as tmp_client:
                    response = await tmp_client.post(
                        f"{registry_url}/api/v1/integration-feedback",
                        json=payload,
                        timeout=10.0,
                    )
            if response.status_code == 200:
                return {"content": [{"type": "text", "text": json.dumps(response.json())}]}
            raise RuntimeError(f"HTTP {response.status_code}: {_snip(response.text, 200)}")
        except Exception as e:
            fallback_path = Path("data") / "feedback_fallback.jsonl"
            try:
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                record = dict(payload)
                record["recorded_at"] = datetime.utcnow().isoformat() + "Z"
                record["registry_error"] = str(e)
                with open(fallback_path, "a") as f:
                    f.write(json.dumps(record, default=str) + "\n")
            except Exception as write_err:
                logger.warning(f"Failed to write feedback fallback: {write_err}")
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "status": "recorded_locally",
                        "warning": f"registry unreachable ({e}); saved to {fallback_path}",
                    }),
                }]
            }

    # =========================================================================
    # PROPERTIES
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        return self._state.status == ConnectionStatus.CONNECTED

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema
            }
            for name, t in self._tools.items()
            if not name.startswith("_")
        ]

    def get_status(self) -> Dict[str, Any]:
        return {
            "service_id": self.SERVICE_ID,
            "service_name": self.SERVICE_NAME,
            "version": self.VERSION,
            "status": self._state.status.value,
            "connected_at": self._state.connected_at.isoformat() if self._state.connected_at else None,
            "session_id": self._state.session_id,
            "tools_count": len([t for t in self._tools if not t.startswith("_")]),
            "api_key_configured": bool(self.api_key),
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

def create_server(
    api_key: Optional[str] = None,
    port: int = 8101,
    registry_url: Optional[str] = None,
) -> DistributedSearchMCP:
    """Create and return a DistributedSearchMCP instance."""
    return DistributedSearchMCP(
        api_key=api_key,
        port=port,
        registry_url=registry_url
    )


# Global server instance
_server: Optional[DistributedSearchMCP] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage server lifecycle."""
    global _server

    _server = create_server(
        port=int(os.getenv("PORT", "4001")),
        registry_url=os.getenv("REGISTRY_URL")
    )

    await _server.start()

    # Background heartbeat: re-announce to registry every 60s
    _heartbeat_task = None

    async def _heartbeat_loop():
        await asyncio.sleep(30)  # First heartbeat after 30s
        while True:
            try:
                await _server._announce_to_registry()
                logger.debug("Heartbeat: re-announced to registry")
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
            await asyncio.sleep(60)

    _heartbeat_task = asyncio.create_task(_heartbeat_loop())

    yield

    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass

    await _server.stop()


app = FastAPI(
    title="Distributed Search MCP",
    description="Federated search hub: web search, content extraction, and parallel fan-out across sibling MCP backends",
    version=DistributedSearchMCP.VERSION,
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    """Service info."""
    if _server:
        return _server.get_status()
    return {"status": "initializing"}


@app.get("/health")
async def health():
    """Health check."""
    if _server and _server.is_connected:
        return {"status": "healthy", "service": DistributedSearchMCP.SERVICE_ID}
    return JSONResponse(
        status_code=503,
        content={"status": "unhealthy", "service": DistributedSearchMCP.SERVICE_ID}
    )


@app.get("/tools")
async def list_tools():
    """List available MCP tools."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return {"tools": _server.tools}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    response = await _server.handle_mcp_request(request)
    return response.model_dump(exclude_none=True)


@app.post("/execute")
async def execute_tool(request: ToolCallRequest):
    """Execute a tool directly (non-MCP interface)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")

    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    result = await tool.handler(request.arguments)
    return {"result": result}


def _sse_event(event_name: str, data: Dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"


def _extract_stream_chunks(result: Any) -> List[Any]:
    if isinstance(result, dict):
        for key in ("results", "findings", "items", "sources"):
            value = result.get(key)
            if isinstance(value, list):
                return value
    if isinstance(result, list):
        return result
    return [result]


@app.post("/execute/stream")
async def execute_tool_stream(request: ToolCallRequest, raw_request: Request):
    """Execute a tool with SSE streaming (chunk-by-chunk results where possible)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")
    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    async def event_generator() -> AsyncGenerator[str, None]:
        call_id = f"c_{uuid.uuid4().hex[:12]}"
        started_at = time.time()
        yield _sse_event(
            "meta",
            {
                "call_id": call_id,
                "tool": request.name,
                "backend": DistributedSearchMCP.SERVICE_ID,
                "started_at": started_at,
            },
        )

        try:
            result = await tool.handler(request.arguments)
            chunks = _extract_stream_chunks(result)
            total = max(1, len(chunks))
            sufficiency_sent = False

            for idx, chunk in enumerate(chunks):
                if await raw_request.is_disconnected():
                    duration_ms = (time.time() - started_at) * 1000.0
                    yield _sse_event(
                        "done",
                        {
                            "call_id": call_id,
                            "duration_ms": duration_ms,
                            "total_chunks": idx,
                            "stopped_early": True,
                        },
                    )
                    return

                progress = float(idx + 1) / float(total)
                yield _sse_event(
                    "chunk",
                    {
                        "index": idx,
                        "type": "result",
                        "data": chunk,
                        "progress": progress,
                    },
                )

                if not sufficiency_sent and (idx + 1) >= 3:
                    sufficiency_sent = True
                    yield _sse_event(
                        "signal",
                        {
                            "type": "sufficient",
                            "reason": "3 chunks delivered",
                            "can_stop": True,
                        },
                    )

            duration_ms = (time.time() - started_at) * 1000.0
            yield _sse_event(
                "done",
                {
                    "call_id": call_id,
                    "duration_ms": duration_ms,
                    "total_chunks": len(chunks),
                    "stopped_early": False,
                    "data": result,
                },
            )
        except Exception as exc:
            yield _sse_event(
                "error",
                {
                    "call_id": call_id,
                    "error": {
                        "code": "STREAM_EXECUTION_FAILED",
                        "message": str(exc),
                        "retryable": True,
                        "category": "transient",
                    },
                },
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/mcp/sse")
@app.get("/sse")
async def sse_endpoint(request: Request):
    """Server-Sent Events endpoint for MCP streaming."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    async def event_generator() -> AsyncGenerator[str, None]:
        # Send initial connection event
        yield f"data: {json.dumps({'type': 'connected', 'session_id': _server._state.session_id})}\n\n"

        # Keep connection alive
        while True:
            if await request.is_disconnected():
                break

            # Send heartbeat
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.utcnow().isoformat()})}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.post("/mcp/sse")
@app.post("/sse")
async def sse_post_endpoint(request: Request):
    """Handle MCP JSON-RPC POST requests via SSE URL (mcp-remote pattern)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    body = await request.json()
    mcp_request = MCPRequest(**body)
    response = await _server.handle_mcp_request(mcp_request)
    return response.model_dump(exclude_none=True)


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "4001"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║          DISTRIBUTED SEARCH MCP - Native Server                  ║
║               First Native MCP Implementation                    ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-search-mcp                              ║
║  Port: {port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • search               - Web search via Jina AI                 ║
║  • read                 - Extract content from URLs              ║
║  • verify               - Fact-check statements                  ║
║  • rerank               - Semantic document reranking            ║
║  • embed                - Generate text embeddings               ║
║  • search_all           - Federated fan-out search               ║
║  • multi_search         - Multi-query concurrent search          ║
║  • get_search_result    - Retrieve stored full payload           ║
║  • list_search_requests - Recent request metadata                ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /         Service info                                   ║
║  • GET  /health   Health check                                   ║
║  • GET  /tools    List tools                                     ║
║  • POST /mcp      MCP JSON-RPC endpoint                          ║
║  • POST /execute  Direct tool execution                          ║
║  • GET  /sse      Server-Sent Events stream                      ║
╚══════════════════════════════════════════════════════════════════╝

Starting native MCP server...
""")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
