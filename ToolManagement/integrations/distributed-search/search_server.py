#!/usr/bin/env python3
"""
Jina Search Server - Focused on search capabilities
====================================================

A clean, focused API server built around Jina's search capabilities:
- Semantic search over MCP tools
- Text embeddings (Jina v3)
- Reranking, segmentation, classification
- Web search and content extraction
- URL and image link tracking for agent memory

Usage:
    python search_server.py

Endpoints:
    POST /v1/search       - Semantic search over tools
    POST /v1/embed        - Generate embeddings for text
    POST /v1/rerank       - Rerank documents by relevance
    POST /v1/segment      - Segment text into chunks
    POST /v1/classify     - Classify text into categories
    POST /v1/web/search   - Search the web
    POST /v1/web/read     - Read and extract content from URL
    POST /v1/urls         - Store/query visited URLs
    POST /v1/images       - Store/query image links
    POST /v1/interact     - Chat with search tools (minimal wrapper)
"""

import asyncio
import base64
import json
import os
import sqlite3
import sys
import time
import uuid
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from enum import Enum

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

load_dotenv()

# MCP imports
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
except ImportError:
    print("ERROR: mcp package not found. Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Tool retrieval
try:
    from tool_retrieval_service import ToolRetrievalService
    RETRIEVAL_AVAILABLE = True
except ImportError:
    ToolRetrievalService = None
    RETRIEVAL_AVAILABLE = False


# ============================================================================
# Configuration
# ============================================================================

JINA_API_KEY = os.getenv("JINA_API_KEY")
JINA_MCP_URL = os.getenv("JINA_MCP_URL", "https://mcp.jina.ai/sse")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "anthropic/claude-sonnet-4")


# ============================================================================
# URL and Image Store - Agent Memory
# ============================================================================

class VisitStatus(str, Enum):
    NOT_VISITED = "not_visited"
    VISITED = "visited"
    FAILED = "failed"
    QUEUED = "queued"


class ContentStore:
    """SQLite store for URLs and images discovered during search"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

        # URLs table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                id TEXT PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                title TEXT,
                description TEXT,
                content_summary TEXT,
                content_type TEXT,
                status TEXT DEFAULT 'not_visited',
                visit_count INTEGER DEFAULT 0,
                last_visited TIMESTAMP,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_query TEXT,
                tags TEXT,
                embedding BLOB
            )
        """)

        # Images table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                source_url TEXT,
                alt_text TEXT,
                caption TEXT,
                width INTEGER,
                height INTEGER,
                content_type TEXT,
                status TEXT DEFAULT 'not_visited',
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_query TEXT,
                tags TEXT,
                embedding BLOB,
                FOREIGN KEY (source_url) REFERENCES urls(url)
            )
        """)

        # Indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_url_status ON urls(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_url_domain ON urls(domain)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_url_discovered ON urls(discovered_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_img_status ON images(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_img_source ON images(source_url)")

        self.conn.commit()

    def _url_id(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def _extract_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        try:
            return urlparse(url).netloc
        except:
            return ""

    # URL methods
    def add_url(
        self,
        url: str,
        title: str = None,
        description: str = None,
        source_query: str = None,
        tags: List[str] = None
    ) -> dict:
        """Add or update a URL"""
        url_id = self._url_id(url)
        domain = self._extract_domain(url)

        self.conn.execute("""
            INSERT INTO urls (id, url, domain, title, description, source_query, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title = COALESCE(excluded.title, title),
                description = COALESCE(excluded.description, description),
                source_query = COALESCE(excluded.source_query, source_query)
        """, (url_id, url, domain, title, description, source_query, json.dumps(tags or [])))
        self.conn.commit()

        return {"id": url_id, "url": url, "status": "added"}

    def mark_visited(
        self,
        url: str,
        content_summary: str = None,
        content_type: str = None,
        status: str = "visited"
    ) -> dict:
        """Mark URL as visited with optional content summary"""
        self.conn.execute("""
            UPDATE urls SET
                status = ?,
                content_summary = COALESCE(?, content_summary),
                content_type = COALESCE(?, content_type),
                visit_count = visit_count + 1,
                last_visited = CURRENT_TIMESTAMP
            WHERE url = ?
        """, (status, content_summary, content_type, url))
        self.conn.commit()

        return {"url": url, "status": status}

    def get_urls(
        self,
        status: str = None,
        domain: str = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "discovered_at DESC"
    ) -> List[dict]:
        """Query URLs with filters"""
        query = "SELECT * FROM urls WHERE 1=1"
        params = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if domain:
            query += " AND domain = ?"
            params.append(domain)

        query += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_url(self, url: str) -> Optional[dict]:
        """Get single URL by URL string"""
        cursor = self.conn.execute("SELECT * FROM urls WHERE url = ?", (url,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_unvisited_urls(self, limit: int = 50) -> List[dict]:
        """Get URLs that haven't been visited yet"""
        return self.get_urls(status="not_visited", limit=limit)

    def search_urls(self, query: str, limit: int = 50) -> List[dict]:
        """Simple text search in URLs"""
        cursor = self.conn.execute("""
            SELECT * FROM urls
            WHERE url LIKE ? OR title LIKE ? OR description LIKE ? OR content_summary LIKE ?
            ORDER BY visit_count DESC, discovered_at DESC
            LIMIT ?
        """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit))
        return [dict(row) for row in cursor.fetchall()]

    # Image methods
    def add_image(
        self,
        url: str,
        source_url: str = None,
        alt_text: str = None,
        caption: str = None,
        width: int = None,
        height: int = None,
        source_query: str = None,
        tags: List[str] = None
    ) -> dict:
        """Add or update an image"""
        img_id = self._url_id(url)

        self.conn.execute("""
            INSERT INTO images (id, url, source_url, alt_text, caption, width, height, source_query, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                alt_text = COALESCE(excluded.alt_text, alt_text),
                caption = COALESCE(excluded.caption, caption)
        """, (img_id, url, source_url, alt_text, caption, width, height, source_query, json.dumps(tags or [])))
        self.conn.commit()

        return {"id": img_id, "url": url, "status": "added"}

    def get_images(
        self,
        source_url: str = None,
        status: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[dict]:
        """Query images with filters"""
        query = "SELECT * FROM images WHERE 1=1"
        params = []

        if source_url:
            query += " AND source_url = ?"
            params.append(source_url)
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY discovered_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> dict:
        """Get storage statistics"""
        url_stats = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'visited' THEN 1 ELSE 0 END) as visited,
                SUM(CASE WHEN status = 'not_visited' THEN 1 ELSE 0 END) as not_visited,
                COUNT(DISTINCT domain) as domains
            FROM urls
        """).fetchone()

        img_stats = self.conn.execute("""
            SELECT COUNT(*) as total FROM images
        """).fetchone()

        return {
            "urls": {
                "total": url_stats[0],
                "visited": url_stats[1],
                "not_visited": url_stats[2],
                "domains": url_stats[3]
            },
            "images": {
                "total": img_stats[0]
            }
        }

    def close(self):
        if self.conn:
            self.conn.close()


class ContentGraph:
    """Track relationships between URLs - links, references, similarity"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

        # Links between URLs
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS url_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                target_url TEXT NOT NULL,
                link_type TEXT DEFAULT 'href',
                anchor_text TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_url, target_url, link_type)
            )
        """)

        # Content fingerprints for change detection
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS content_fingerprints (
                url TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                content_hash TEXT,
                word_count INTEGER,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                change_count INTEGER DEFAULT 0,
                previous_fingerprints TEXT
            )
        """)

        # Semantic clusters
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS url_clusters (
                url TEXT PRIMARY KEY,
                cluster_id TEXT,
                cluster_label TEXT,
                similarity_score REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Extracted entities
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS extracted_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_value TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                context TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_links_source ON url_links(source_url)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_links_target ON url_links(target_url)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_url ON extracted_entities(url)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON extracted_entities(entity_type)")
        self.conn.commit()

    def add_link(self, source_url: str, target_url: str, link_type: str = "href", anchor_text: str = None):
        """Record a link between URLs"""
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO url_links (source_url, target_url, link_type, anchor_text)
                VALUES (?, ?, ?, ?)
            """, (source_url, target_url, link_type, anchor_text))
            self.conn.commit()
        except:
            pass

    def add_links_batch(self, source_url: str, links: List[dict]):
        """Add multiple links from a page"""
        for link in links:
            self.add_link(
                source_url=source_url,
                target_url=link.get("url"),
                link_type=link.get("type", "href"),
                anchor_text=link.get("text")
            )

    def get_outlinks(self, url: str) -> List[dict]:
        """Get all links from a URL"""
        cursor = self.conn.execute(
            "SELECT * FROM url_links WHERE source_url = ?", (url,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_inlinks(self, url: str) -> List[dict]:
        """Get all links pointing to a URL (backlinks)"""
        cursor = self.conn.execute(
            "SELECT * FROM url_links WHERE target_url = ?", (url,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def update_fingerprint(self, url: str, content: str) -> dict:
        """Update content fingerprint and detect changes"""
        import hashlib

        # Create fingerprint (simhash-like)
        words = content.lower().split()
        word_count = len(words)
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:32]

        # Simple fingerprint: hash of sorted unique words
        unique_words = sorted(set(words))[:100]
        fingerprint = hashlib.md5(" ".join(unique_words).encode()).hexdigest()

        # Check for existing fingerprint
        cursor = self.conn.execute(
            "SELECT fingerprint, content_hash, change_count, previous_fingerprints FROM content_fingerprints WHERE url = ?",
            (url,)
        )
        row = cursor.fetchone()

        changed = False
        change_type = None

        if row:
            old_fingerprint = row[0]
            old_hash = row[1]
            change_count = row[2] or 0
            prev_fps = json.loads(row[3]) if row[3] else []

            if content_hash != old_hash:
                changed = True
                change_count += 1

                # Determine change type
                if fingerprint != old_fingerprint:
                    change_type = "major"  # Structure changed
                else:
                    change_type = "minor"  # Only content changed

                # Store previous fingerprint
                prev_fps.append({"fp": old_fingerprint, "hash": old_hash, "ts": datetime.now().isoformat()})
                prev_fps = prev_fps[-10:]  # Keep last 10

                self.conn.execute("""
                    UPDATE content_fingerprints
                    SET fingerprint = ?, content_hash = ?, word_count = ?,
                        last_checked = CURRENT_TIMESTAMP, change_count = ?,
                        previous_fingerprints = ?
                    WHERE url = ?
                """, (fingerprint, content_hash, word_count, change_count, json.dumps(prev_fps), url))
        else:
            self.conn.execute("""
                INSERT INTO content_fingerprints (url, fingerprint, content_hash, word_count)
                VALUES (?, ?, ?, ?)
            """, (url, fingerprint, content_hash, word_count))
            change_count = 0

        self.conn.commit()

        return {
            "url": url,
            "changed": changed,
            "change_type": change_type,
            "change_count": change_count,
            "word_count": word_count,
            "fingerprint": fingerprint[:8]
        }

    def add_entity(self, url: str, entity_type: str, entity_value: str, confidence: float = 1.0, context: str = None):
        """Store extracted entity"""
        self.conn.execute("""
            INSERT INTO extracted_entities (url, entity_type, entity_value, confidence, context)
            VALUES (?, ?, ?, ?, ?)
        """, (url, entity_type, entity_value, confidence, context))
        self.conn.commit()

    def add_entities_batch(self, url: str, entities: List[dict]):
        """Add multiple entities"""
        for e in entities:
            self.add_entity(
                url=url,
                entity_type=e.get("type"),
                entity_value=e.get("value"),
                confidence=e.get("confidence", 1.0),
                context=e.get("context")
            )

    def get_entities(self, url: str = None, entity_type: str = None, limit: int = 100) -> List[dict]:
        """Query extracted entities"""
        query = "SELECT * FROM extracted_entities WHERE 1=1"
        params = []

        if url:
            query += " AND url = ?"
            params.append(url)
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)

        query += " ORDER BY discovered_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def set_cluster(self, url: str, cluster_id: str, label: str = None, score: float = None):
        """Assign URL to a semantic cluster"""
        self.conn.execute("""
            INSERT OR REPLACE INTO url_clusters (url, cluster_id, cluster_label, similarity_score, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (url, cluster_id, label, score))
        self.conn.commit()

    def get_cluster_urls(self, cluster_id: str) -> List[dict]:
        """Get all URLs in a cluster"""
        cursor = self.conn.execute(
            "SELECT * FROM url_clusters WHERE cluster_id = ? ORDER BY similarity_score DESC",
            (cluster_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        if self.conn:
            self.conn.close()


class WebhookManager:
    """Manage webhooks for event notifications"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = None
        self.http_client = None
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                events TEXT NOT NULL,
                secret TEXT,
                active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_triggered TIMESTAMP,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                webhook_id TEXT NOT NULL,
                event TEXT NOT NULL,
                payload TEXT,
                response_status INTEGER,
                response_body TEXT,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()

    def set_http_client(self, client):
        self.http_client = client

    def register(self, url: str, events: List[str], secret: str = None) -> dict:
        """Register a webhook"""
        webhook_id = hashlib.sha256(f"{url}:{time.time()}".encode()).hexdigest()[:16]

        self.conn.execute("""
            INSERT INTO webhooks (id, url, events, secret)
            VALUES (?, ?, ?, ?)
        """, (webhook_id, url, json.dumps(events), secret))
        self.conn.commit()

        return {"id": webhook_id, "url": url, "events": events}

    def unregister(self, webhook_id: str):
        """Remove a webhook"""
        self.conn.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
        self.conn.commit()

    def list_webhooks(self) -> List[dict]:
        """List all webhooks"""
        cursor = self.conn.execute("SELECT * FROM webhooks WHERE active = 1")
        return [dict(row) for row in cursor.fetchall()]

    async def trigger(self, event: str, payload: dict):
        """Trigger webhooks for an event"""
        if not self.http_client:
            return

        cursor = self.conn.execute(
            "SELECT * FROM webhooks WHERE active = 1 AND events LIKE ?",
            (f'%"{event}"%',)
        )

        for row in cursor.fetchall():
            webhook = dict(row)
            try:
                # Add signature if secret exists
                headers = {"Content-Type": "application/json"}
                if webhook.get("secret"):
                    import hmac
                    signature = hmac.new(
                        webhook["secret"].encode(),
                        json.dumps(payload).encode(),
                        hashlib.sha256
                    ).hexdigest()
                    headers["X-Webhook-Signature"] = signature

                response = await self.http_client.post(
                    webhook["url"],
                    json={"event": event, "payload": payload, "timestamp": datetime.now().isoformat()},
                    headers=headers,
                    timeout=10.0
                )

                # Log
                self.conn.execute("""
                    INSERT INTO webhook_logs (webhook_id, event, payload, response_status, response_body)
                    VALUES (?, ?, ?, ?, ?)
                """, (webhook["id"], event, json.dumps(payload), response.status_code, response.text[:500]))

                self.conn.execute("""
                    UPDATE webhooks SET last_triggered = CURRENT_TIMESTAMP, success_count = success_count + 1
                    WHERE id = ?
                """, (webhook["id"],))

            except Exception as e:
                self.conn.execute("""
                    UPDATE webhooks SET failure_count = failure_count + 1 WHERE id = ?
                """, (webhook["id"],))
                self.conn.execute("""
                    INSERT INTO webhook_logs (webhook_id, event, payload, response_status, response_body)
                    VALUES (?, ?, ?, ?, ?)
                """, (webhook["id"], event, json.dumps(payload), 0, str(e)))

            self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()


class SmartPrioritizer:
    """ML-based priority scoring for queue items"""

    def __init__(self):
        self.domain_scores = {}  # domain -> avg relevance
        self.query_embeddings = {}  # query -> embedding
        self.embed_func = None

    def set_embed_func(self, func):
        self.embed_func = func

    async def compute_priority(
        self,
        url: str,
        title: str = None,
        description: str = None,
        source_query: str = None,
        domain_history: dict = None
    ) -> dict:
        """Compute smart priority score (0-100)"""
        from urllib.parse import urlparse

        score = 50  # Base score
        factors = {}

        # Domain reputation
        domain = urlparse(url).netloc
        if domain_history and domain in domain_history:
            domain_score = domain_history[domain].get("avg_relevance", 0.5) * 20
            score += domain_score
            factors["domain_reputation"] = domain_score

        # URL quality signals
        url_lower = url.lower()
        if any(x in url_lower for x in ["/api/", "/docs/", "/guide", "/tutorial"]):
            score += 10
            factors["url_quality"] = 10
        if any(x in url_lower for x in ["/login", "/signup", "/cart", "/checkout"]):
            score -= 20
            factors["url_quality"] = -20

        # Content signals from title/description
        if title or description:
            text = f"{title or ''} {description or ''}".lower()
            if any(x in text for x in ["official", "documentation", "guide", "reference"]):
                score += 15
                factors["content_signals"] = 15

        # Query relevance (if we have embeddings)
        if self.embed_func and source_query and (title or description):
            try:
                # Get query embedding
                if source_query not in self.query_embeddings:
                    result = await self.embed_func({
                        "inputs": [source_query],
                        "model": "jina-embeddings-v3",
                        "task": "retrieval.query",
                        "dimensions": 256,
                        "normalize": True
                    })
                    if isinstance(result, str):
                        result = json.loads(result)
                    self.query_embeddings[source_query] = result["data"][0]["embedding"]

                # Get content embedding
                content = f"{title or ''} {description or ''}"
                result = await self.embed_func({
                    "inputs": [content],
                    "model": "jina-embeddings-v3",
                    "task": "retrieval.passage",
                    "dimensions": 256,
                    "normalize": True
                })
                if isinstance(result, str):
                    result = json.loads(result)
                content_emb = result["data"][0]["embedding"]

                # Cosine similarity
                import numpy as np
                query_emb = np.array(self.query_embeddings[source_query])
                content_emb = np.array(content_emb)
                similarity = float(np.dot(query_emb, content_emb))

                relevance_score = similarity * 30
                score += relevance_score
                factors["query_relevance"] = round(relevance_score, 2)

            except Exception as e:
                factors["embedding_error"] = str(e)

        # Clamp score
        score = max(0, min(100, score))

        return {
            "priority": int(score),
            "factors": factors
        }

    def update_domain_score(self, domain: str, relevance: float):
        """Update domain reputation based on feedback"""
        if domain not in self.domain_scores:
            self.domain_scores[domain] = {"total": 0, "count": 0}

        self.domain_scores[domain]["total"] += relevance
        self.domain_scores[domain]["count"] += 1
        self.domain_scores[domain]["avg_relevance"] = (
            self.domain_scores[domain]["total"] / self.domain_scores[domain]["count"]
        )


class ProcessingQueue:
    """Queue for event-driven URL/content processing"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

        # Queue table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_queue (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                queue_type TEXT DEFAULT 'url',
                priority INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                payload TEXT,
                source_query TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                completed_at TIMESTAMP,
                worker_id TEXT,
                result TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON processing_queue(status, priority DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_type ON processing_queue(queue_type, status)")
        self.conn.commit()

    def enqueue(
        self,
        url: str,
        queue_type: str = "url",
        priority: int = 0,
        payload: dict = None,
        source_query: str = None
    ) -> dict:
        """Add item to processing queue"""
        item_id = hashlib.sha256(f"{url}:{time.time()}".encode()).hexdigest()[:16]

        try:
            self.conn.execute("""
                INSERT INTO processing_queue (id, url, queue_type, priority, payload, source_query)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (item_id, url, queue_type, priority, json.dumps(payload) if payload else None, source_query))
            self.conn.commit()
            return {"id": item_id, "url": url, "status": "queued"}
        except sqlite3.IntegrityError:
            return {"id": item_id, "url": url, "status": "already_queued"}

    def enqueue_batch(
        self,
        items: List[dict],
        queue_type: str = "url",
        source_query: str = None
    ) -> dict:
        """Enqueue multiple items"""
        results = []
        for item in items:
            url = item.get("url") or item
            priority = item.get("priority", 0) if isinstance(item, dict) else 0
            payload = item.get("payload") if isinstance(item, dict) else None

            result = self.enqueue(
                url=url,
                queue_type=queue_type,
                priority=priority,
                payload=payload,
                source_query=source_query
            )
            results.append(result)

        return {"queued": len([r for r in results if r["status"] == "queued"]), "items": results}

    def claim_batch(
        self,
        batch_size: int = 10,
        queue_type: str = None,
        worker_id: str = None
    ) -> List[dict]:
        """Claim a batch of items for processing"""
        worker = worker_id or f"worker_{uuid.uuid4().hex[:8]}"

        query = """
            SELECT id, url, queue_type, priority, payload, source_query, retry_count
            FROM processing_queue
            WHERE status = 'pending'
        """
        params = []

        if queue_type:
            query += " AND queue_type = ?"
            params.append(queue_type)

        query += " ORDER BY priority DESC, created_at ASC LIMIT ?"
        params.append(batch_size)

        cursor = self.conn.execute(query, params)
        items = [dict(row) for row in cursor.fetchall()]

        if items:
            ids = [item["id"] for item in items]
            placeholders = ",".join("?" * len(ids))
            self.conn.execute(f"""
                UPDATE processing_queue
                SET status = 'processing', claimed_at = CURRENT_TIMESTAMP, worker_id = ?
                WHERE id IN ({placeholders})
            """, [worker] + ids)
            self.conn.commit()

        return items

    def complete(self, item_id: str, result: dict = None, error: str = None) -> dict:
        """Mark item as completed or failed"""
        status = "failed" if error else "completed"

        self.conn.execute("""
            UPDATE processing_queue
            SET status = ?, completed_at = CURRENT_TIMESTAMP, result = ?, error = ?
            WHERE id = ?
        """, (status, json.dumps(result) if result else None, error, item_id))
        self.conn.commit()

        return {"id": item_id, "status": status}

    def complete_batch(self, results: List[dict]) -> dict:
        """Complete multiple items"""
        completed = 0
        failed = 0

        for item in results:
            result = self.complete(
                item_id=item["id"],
                result=item.get("result"),
                error=item.get("error")
            )
            if result["status"] == "completed":
                completed += 1
            else:
                failed += 1

        return {"completed": completed, "failed": failed}

    def retry_failed(self, max_retries: int = 3) -> int:
        """Requeue failed items that haven't exceeded retry limit"""
        cursor = self.conn.execute("""
            UPDATE processing_queue
            SET status = 'pending', retry_count = retry_count + 1, error = NULL
            WHERE status = 'failed' AND retry_count < ?
        """, (max_retries,))
        self.conn.commit()
        return cursor.rowcount

    def get_stats(self) -> dict:
        """Get queue statistics"""
        cursor = self.conn.execute("""
            SELECT
                queue_type,
                status,
                COUNT(*) as count
            FROM processing_queue
            GROUP BY queue_type, status
        """)

        stats = {}
        for row in cursor.fetchall():
            qt = row[0]
            if qt not in stats:
                stats[qt] = {}
            stats[qt][row[1]] = row[2]

        return stats

    def peek(self, queue_type: str = None, limit: int = 10) -> List[dict]:
        """Peek at pending items without claiming"""
        query = "SELECT * FROM processing_queue WHERE status = 'pending'"
        params = []

        if queue_type:
            query += " AND queue_type = ?"
            params.append(queue_type)

        query += " ORDER BY priority DESC, created_at ASC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        if self.conn:
            self.conn.close()


# ============================================================================
# Advanced Features - Direct API, Caching, RAG, Analytics
# ============================================================================

class JinaDirectAPI:
    """Direct Jina API calls - fallback when MCP is unavailable"""

    BASE_URL = "https://api.jina.ai/v1"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or JINA_API_KEY
        self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=60.0,
                headers={"Authorization": f"Bearer {self.api_key}"}
            )
        return self._client

    async def embed(
        self,
        inputs: List[str],
        model: str = "jina-embeddings-v3",
        task: str = "retrieval.passage",
        dimensions: int = 1024
    ) -> Dict[str, Any]:
        """Generate embeddings using direct API"""
        resp = await self.client.post(
            f"{self.BASE_URL}/embeddings",
            json={
                "model": model,
                "input": inputs,
                "task": task,
                "dimensions": dimensions,
                "normalized": True
            }
        )
        resp.raise_for_status()
        return resp.json()

    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: str = "jina-reranker-v2-base-multilingual",
        top_n: int = 10
    ) -> Dict[str, Any]:
        """Rerank documents using direct API"""
        resp = await self.client.post(
            f"{self.BASE_URL}/rerank",
            json={
                "model": model,
                "query": query,
                "documents": documents,
                "top_n": top_n
            }
        )
        resp.raise_for_status()
        return resp.json()

    async def segment(
        self,
        content: str,
        tokenizer: str = "cl100k_base",
        max_chunk_length: int = 1000
    ) -> Dict[str, Any]:
        """Segment text into chunks"""
        resp = await self.client.post(
            f"{self.BASE_URL}/segmenter",
            json={
                "content": content,
                "tokenizer": tokenizer,
                "max_chunk_length": max_chunk_length,
                "return_chunks": True
            }
        )
        resp.raise_for_status()
        return resp.json()

    async def classify(
        self,
        inputs: List[str],
        labels: List[str]
    ) -> Dict[str, Any]:
        """Zero-shot classification"""
        resp = await self.client.post(
            f"{self.BASE_URL}/classify",
            json={
                "model": "jina-embeddings-v3",
                "input": inputs,
                "labels": labels
            }
        )
        resp.raise_for_status()
        return resp.json()

    async def search_web(self, query: str) -> Dict[str, Any]:
        """Web search via Jina Reader"""
        resp = await self.client.get(
            f"https://s.jina.ai/{query}",
            headers={"Accept": "application/json"}
        )
        resp.raise_for_status()
        return resp.json()

    async def read_url(self, url: str) -> Dict[str, Any]:
        """Read URL content via Jina Reader"""
        resp = await self.client.get(
            f"https://r.jina.ai/{url}",
            headers={"Accept": "application/json"}
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        if self._client:
            await self._client.aclose()


class SemanticCache:
    """LRU cache with semantic similarity matching"""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600, similarity_threshold: float = 0.92):
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.threshold = similarity_threshold
        self.cache: Dict[str, Dict[str, Any]] = {}  # key -> {embedding, result, timestamp}
        self.access_order: List[str] = []  # LRU tracking
        self._jina: Optional[JinaDirectAPI] = None

    def set_jina(self, jina: JinaDirectAPI):
        self._jina = jina

    def _hash_key(self, query: str, params: Dict) -> str:
        """Create cache key from query + params"""
        param_str = json.dumps(params, sort_keys=True)
        return hashlib.sha256(f"{query}:{param_str}".encode()).hexdigest()[:16]

    def _evict_expired(self):
        """Remove expired entries"""
        now = time.time()
        expired = [k for k, v in self.cache.items() if now - v['timestamp'] > self.ttl]
        for k in expired:
            del self.cache[k]
            if k in self.access_order:
                self.access_order.remove(k)

    def _evict_lru(self):
        """Evict least recently used if over capacity"""
        while len(self.cache) >= self.max_size and self.access_order:
            oldest = self.access_order.pop(0)
            if oldest in self.cache:
                del self.cache[oldest]

    async def get(self, query: str, params: Dict = None) -> Optional[Any]:
        """Get cached result - exact match only (fast path)"""
        self._evict_expired()
        key = self._hash_key(query, params or {})

        if key in self.cache:
            # Update access order
            if key in self.access_order:
                self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]['result']

        return None

    async def get_semantic(self, query: str, query_embedding: List[float], params: Dict = None) -> Optional[Any]:
        """Get cached result with semantic similarity matching"""
        self._evict_expired()

        # First try exact match
        exact = await self.get(query, params)
        if exact:
            return exact

        # Try semantic matching
        query_vec = np.array(query_embedding, dtype=np.float32)
        best_score = 0.0
        best_key = None

        for key, entry in self.cache.items():
            if 'embedding' not in entry:
                continue
            cached_vec = np.array(entry['embedding'], dtype=np.float32)
            # Cosine similarity (vectors should be normalized)
            score = float(np.dot(query_vec, cached_vec))
            if score > best_score:
                best_score = score
                best_key = key

        if best_score >= self.threshold and best_key:
            # Update access order
            if best_key in self.access_order:
                self.access_order.remove(best_key)
            self.access_order.append(best_key)
            return self.cache[best_key]['result']

        return None

    def set(self, query: str, result: Any, embedding: List[float] = None, params: Dict = None):
        """Store result in cache"""
        self._evict_lru()
        key = self._hash_key(query, params or {})

        self.cache[key] = {
            'result': result,
            'embedding': embedding,
            'timestamp': time.time(),
            'query': query
        }
        self.access_order.append(key)

    def invalidate(self, pattern: str = None):
        """Invalidate cache entries matching pattern"""
        if pattern is None:
            self.cache.clear()
            self.access_order.clear()
        else:
            to_remove = [k for k, v in self.cache.items() if pattern in v.get('query', '')]
            for k in to_remove:
                del self.cache[k]
                if k in self.access_order:
                    self.access_order.remove(k)

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'ttl_seconds': self.ttl,
            'oldest_entry_age': time.time() - min((v['timestamp'] for v in self.cache.values()), default=time.time())
        }


class QueryExpander:
    """Expand queries with synonyms, related terms, and reformulations"""

    # Common expansions for search optimization
    SYNONYMS = {
        'api': ['endpoint', 'interface', 'service'],
        'search': ['find', 'query', 'lookup', 'retrieve'],
        'embed': ['embedding', 'vector', 'encode'],
        'read': ['fetch', 'get', 'extract', 'scrape'],
        'write': ['save', 'store', 'create', 'update'],
        'delete': ['remove', 'clear', 'drop'],
        'analyze': ['examine', 'inspect', 'evaluate'],
        'summarize': ['condense', 'abstract', 'brief'],
        'compare': ['diff', 'contrast', 'match'],
        'convert': ['transform', 'translate', 'parse'],
        'list': ['enumerate', 'show', 'display'],
        'auth': ['authentication', 'login', 'credential'],
        'config': ['configuration', 'settings', 'options'],
        'error': ['exception', 'failure', 'bug', 'issue'],
        'test': ['validate', 'check', 'verify'],
        'ml': ['machine learning', 'ai', 'model'],
        'nlp': ['natural language', 'text processing', 'language'],
        'db': ['database', 'storage', 'persistence'],
        'url': ['link', 'uri', 'address', 'web'],
        'file': ['document', 'content', 'data'],
    }

    def __init__(self, jina: JinaDirectAPI = None):
        self._jina = jina

    def expand_synonyms(self, query: str, max_expansions: int = 3) -> List[str]:
        """Expand query with synonym-based variations"""
        queries = [query]
        words = query.lower().split()

        for word in words:
            if word in self.SYNONYMS:
                for syn in self.SYNONYMS[word][:max_expansions]:
                    expanded = query.lower().replace(word, syn)
                    if expanded not in queries:
                        queries.append(expanded)

        return queries[:max_expansions + 1]

    async def expand_semantic(self, query: str, top_k: int = 3) -> List[str]:
        """Generate semantic query variations using embeddings"""
        if not self._jina:
            return [query]

        # Generate variations by prompting patterns
        variations = [
            query,
            f"how to {query}",
            f"{query} examples",
            f"best way to {query}",
            f"{query} tool",
        ]

        return variations[:top_k + 1]

    def extract_entities(self, query: str) -> Dict[str, List[str]]:
        """Extract entities from query for filtering"""
        import re

        entities = {
            'urls': re.findall(r'https?://[^\s]+', query),
            'emails': re.findall(r'[\w\.-]+@[\w\.-]+', query),
            'filetypes': re.findall(r'\.(py|js|ts|go|rs|java|json|yaml|md|txt)\b', query.lower()),
            'domains': re.findall(r'\b(?:github|arxiv|docs|api)\.(?:com|org|io|ai)\b', query.lower()),
            'quoted': re.findall(r'"([^"]+)"', query),
        }

        return {k: v for k, v in entities.items() if v}


class MultiQueryFusion:
    """Execute multiple queries and fuse results using RRF"""

    def __init__(self, jina: JinaDirectAPI = None):
        self._jina = jina
        self.k = 60  # RRF constant

    def reciprocal_rank_fusion(
        self,
        result_lists: List[List[Dict]],
        id_key: str = 'id',
        score_key: str = 'score'
    ) -> List[Dict]:
        """
        Fuse multiple result lists using Reciprocal Rank Fusion.
        RRF score = sum(1 / (k + rank)) for each list where item appears
        """
        fused_scores: Dict[str, float] = {}
        item_map: Dict[str, Dict] = {}

        for results in result_lists:
            for rank, item in enumerate(results, 1):
                item_id = item.get(id_key) or str(item)

                if item_id not in fused_scores:
                    fused_scores[item_id] = 0.0
                    item_map[item_id] = item

                fused_scores[item_id] += 1.0 / (self.k + rank)

        # Sort by fused score
        sorted_ids = sorted(fused_scores.keys(), key=lambda x: fused_scores[x], reverse=True)

        results = []
        for item_id in sorted_ids:
            item = item_map[item_id].copy()
            item['rrf_score'] = fused_scores[item_id]
            results.append(item)

        return results

    async def multi_query_search(
        self,
        queries: List[str],
        search_func,
        top_k: int = 10
    ) -> List[Dict]:
        """Execute multiple queries and fuse results"""
        # Run queries in parallel
        tasks = [search_func(q, top_k=top_k * 2) for q in queries]
        result_lists = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out errors
        valid_results = [r for r in result_lists if not isinstance(r, Exception) and r]

        if not valid_results:
            return []

        # Fuse results
        fused = self.reciprocal_rank_fusion(valid_results)
        return fused[:top_k]


class SearchSession:
    """Track multi-query search sessions for context and personalization"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()
        self.active_sessions: Dict[str, Dict] = {}

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS search_sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                query_count INTEGER DEFAULT 0,
                metadata TEXT DEFAULT '{}'
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS session_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                query TEXT NOT NULL,
                results_count INTEGER,
                clicked_results TEXT,
                dwell_time_ms INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES search_sessions(session_id)
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_session_queries_session
            ON session_queries(session_id)
        """)

        self.conn.commit()

    def create_session(self, user_id: str = None) -> str:
        """Create a new search session"""
        session_id = str(uuid.uuid4())[:8]

        self.conn.execute("""
            INSERT INTO search_sessions (session_id, user_id)
            VALUES (?, ?)
        """, (session_id, user_id))
        self.conn.commit()

        self.active_sessions[session_id] = {
            'queries': [],
            'context': [],
            'preferences': {}
        }

        return session_id

    def add_query(
        self,
        session_id: str,
        query: str,
        results_count: int = 0,
        clicked: List[str] = None
    ):
        """Add a query to the session"""
        self.conn.execute("""
            INSERT INTO session_queries (session_id, query, results_count, clicked_results)
            VALUES (?, ?, ?, ?)
        """, (session_id, query, results_count, json.dumps(clicked or [])))

        self.conn.execute("""
            UPDATE search_sessions
            SET query_count = query_count + 1, last_active = CURRENT_TIMESTAMP
            WHERE session_id = ?
        """, (session_id,))

        self.conn.commit()

        # Update in-memory session
        if session_id in self.active_sessions:
            self.active_sessions[session_id]['queries'].append(query)

    def get_session_context(self, session_id: str, last_n: int = 5) -> List[str]:
        """Get recent queries from session for context"""
        cursor = self.conn.execute("""
            SELECT query FROM session_queries
            WHERE session_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (session_id, last_n))

        return [row[0] for row in cursor.fetchall()][::-1]  # Oldest first

    def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """Get session statistics"""
        cursor = self.conn.execute("""
            SELECT
                s.created_at,
                s.last_active,
                s.query_count,
                COUNT(q.id) as total_queries,
                AVG(q.results_count) as avg_results
            FROM search_sessions s
            LEFT JOIN session_queries q ON s.session_id = q.session_id
            WHERE s.session_id = ?
            GROUP BY s.session_id
        """, (session_id,))

        row = cursor.fetchone()
        if row:
            return {
                'session_id': session_id,
                'created_at': row[0],
                'last_active': row[1],
                'query_count': row[2],
                'total_queries': row[3],
                'avg_results': row[4]
            }
        return {}

    def close(self):
        if self.conn:
            self.conn.close()


class UserProfile:
    """User profiles for personalized search"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                preferred_domains TEXT DEFAULT '[]',
                topic_affinities TEXT DEFAULT '{}',
                search_history_summary TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS user_interactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                url TEXT NOT NULL,
                interaction_type TEXT NOT NULL,
                score REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_interactions_user
            ON user_interactions(user_id)
        """)

        self.conn.commit()

    def get_or_create(self, user_id: str) -> Dict[str, Any]:
        """Get or create user profile"""
        cursor = self.conn.execute("""
            SELECT user_id, preferred_domains, topic_affinities, search_history_summary
            FROM user_profiles WHERE user_id = ?
        """, (user_id,))

        row = cursor.fetchone()
        if row:
            return {
                'user_id': row[0],
                'preferred_domains': json.loads(row[1]),
                'topic_affinities': json.loads(row[2]),
                'search_history_summary': row[3]
            }

        # Create new profile
        self.conn.execute("""
            INSERT INTO user_profiles (user_id) VALUES (?)
        """, (user_id,))
        self.conn.commit()

        return {
            'user_id': user_id,
            'preferred_domains': [],
            'topic_affinities': {},
            'search_history_summary': ''
        }

    def record_interaction(
        self,
        user_id: str,
        url: str,
        interaction_type: str,  # 'click', 'bookmark', 'share', 'dwell', 'skip'
        score: float = 1.0
    ):
        """Record a user interaction"""
        self.conn.execute("""
            INSERT INTO user_interactions (user_id, url, interaction_type, score)
            VALUES (?, ?, ?, ?)
        """, (user_id, url, interaction_type, score))
        self.conn.commit()

        # Update topic affinities based on URL domain
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        if domain:
            self._update_domain_affinity(user_id, domain, score if interaction_type in ('click', 'bookmark', 'share') else -score * 0.5)

    def _update_domain_affinity(self, user_id: str, domain: str, delta: float):
        """Update domain affinity score"""
        cursor = self.conn.execute("""
            SELECT topic_affinities FROM user_profiles WHERE user_id = ?
        """, (user_id,))

        row = cursor.fetchone()
        if row:
            affinities = json.loads(row[0])
            affinities[domain] = affinities.get(domain, 0.0) + delta

            self.conn.execute("""
                UPDATE user_profiles
                SET topic_affinities = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (json.dumps(affinities), user_id))
            self.conn.commit()

    def get_personalization_boost(self, user_id: str, url: str) -> float:
        """Get personalization score boost for a URL"""
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        profile = self.get_or_create(user_id)
        affinities = profile.get('topic_affinities', {})

        # Normalize to 0-1 range
        if domain in affinities:
            max_affinity = max(abs(v) for v in affinities.values()) if affinities else 1
            return 0.5 + (affinities[domain] / max_affinity) * 0.5

        return 0.5  # Neutral

    def close(self):
        if self.conn:
            self.conn.close()


class PageRankScorer:
    """Compute PageRank scores from link graph"""

    def __init__(self, graph: ContentGraph, damping: float = 0.85, iterations: int = 20):
        self.graph = graph
        self.damping = damping
        self.iterations = iterations
        self._scores: Dict[str, float] = {}
        self._last_computed: float = 0

    def compute(self, force: bool = False) -> Dict[str, float]:
        """Compute PageRank scores for all URLs in the graph"""
        # Check if we need to recompute (every 5 minutes)
        if not force and time.time() - self._last_computed < 300 and self._scores:
            return self._scores

        # Get all links
        cursor = self.graph.conn.execute("""
            SELECT source_url, target_url FROM url_links
        """)

        links = cursor.fetchall()
        if not links:
            return {}

        # Build adjacency list
        outlinks: Dict[str, List[str]] = {}
        all_urls: set = set()

        for source, target in links:
            if source not in outlinks:
                outlinks[source] = []
            outlinks[source].append(target)
            all_urls.add(source)
            all_urls.add(target)

        n = len(all_urls)
        if n == 0:
            return {}

        url_list = list(all_urls)
        url_to_idx = {url: i for i, url in enumerate(url_list)}

        # Initialize scores
        scores = np.ones(n) / n

        # Iterate
        for _ in range(self.iterations):
            new_scores = np.ones(n) * (1 - self.damping) / n

            for source, targets in outlinks.items():
                source_idx = url_to_idx[source]
                share = scores[source_idx] * self.damping / len(targets)

                for target in targets:
                    if target in url_to_idx:
                        new_scores[url_to_idx[target]] += share

            scores = new_scores

        # Store results
        self._scores = {url: float(scores[url_to_idx[url]]) for url in url_list}
        self._last_computed = time.time()

        return self._scores

    def get_score(self, url: str) -> float:
        """Get PageRank score for a URL"""
        if not self._scores:
            self.compute()
        return self._scores.get(url, 0.0)

    def get_top_urls(self, n: int = 10) -> List[Tuple[str, float]]:
        """Get top N URLs by PageRank"""
        if not self._scores:
            self.compute()
        sorted_urls = sorted(self._scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_urls[:n]


class SimHashDeduplicator:
    """Near-duplicate detection using SimHash"""

    def __init__(self, hash_bits: int = 64, threshold: int = 3):
        self.hash_bits = hash_bits
        self.threshold = threshold  # Max Hamming distance for duplicates
        self.hashes: Dict[str, int] = {}  # url -> simhash

    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenization with shingles"""
        words = text.lower().split()
        # Create 3-grams
        shingles = []
        for i in range(len(words) - 2):
            shingles.append(' '.join(words[i:i+3]))
        return shingles if shingles else words

    def _hash_token(self, token: str) -> int:
        """Hash a token to 64-bit integer"""
        return int(hashlib.md5(token.encode()).hexdigest()[:16], 16)

    def compute_simhash(self, text: str) -> int:
        """Compute SimHash for text"""
        tokens = self._tokenize(text)
        if not tokens:
            return 0

        v = [0] * self.hash_bits

        for token in tokens:
            token_hash = self._hash_token(token)
            for i in range(self.hash_bits):
                if (token_hash >> i) & 1:
                    v[i] += 1
                else:
                    v[i] -= 1

        simhash = 0
        for i in range(self.hash_bits):
            if v[i] > 0:
                simhash |= (1 << i)

        return simhash

    def hamming_distance(self, hash1: int, hash2: int) -> int:
        """Compute Hamming distance between two hashes"""
        xor = hash1 ^ hash2
        return bin(xor).count('1')

    def add_document(self, url: str, text: str) -> Dict[str, Any]:
        """Add document and check for duplicates"""
        simhash = self.compute_simhash(text)

        # Check for near-duplicates
        duplicates = []
        for existing_url, existing_hash in self.hashes.items():
            distance = self.hamming_distance(simhash, existing_hash)
            if distance <= self.threshold:
                duplicates.append({
                    'url': existing_url,
                    'distance': distance,
                    'similarity': 1 - (distance / self.hash_bits)
                })

        self.hashes[url] = simhash

        return {
            'url': url,
            'simhash': hex(simhash),
            'is_duplicate': len(duplicates) > 0,
            'duplicates': duplicates
        }

    def find_duplicates(self, text: str) -> List[Dict[str, Any]]:
        """Find duplicates of given text without adding it"""
        simhash = self.compute_simhash(text)

        duplicates = []
        for url, existing_hash in self.hashes.items():
            distance = self.hamming_distance(simhash, existing_hash)
            if distance <= self.threshold:
                duplicates.append({
                    'url': url,
                    'distance': distance,
                    'similarity': 1 - (distance / self.hash_bits)
                })

        return sorted(duplicates, key=lambda x: x['distance'])


class RAGPipeline:
    """Retrieval-Augmented Generation pipeline"""

    def __init__(
        self,
        jina: JinaDirectAPI,
        content_store: ContentStore,
        cache: SemanticCache = None
    ):
        self.jina = jina
        self.store = content_store
        self.cache = cache
        self.chunk_size = 500
        self.chunk_overlap = 50

    async def retrieve(
        self,
        query: str,
        top_k: int = 5,
        filters: Dict = None
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant documents for query"""
        # Check cache first
        if self.cache:
            cached = await self.cache.get(f"rag:{query}", filters)
            if cached:
                return cached

        # Get visited URLs with content
        urls = self.store.get_urls(status='visited', limit=100)

        if not urls:
            return []

        # Create documents from summaries
        documents = []
        for url_data in urls:
            if url_data.get('content_summary'):
                documents.append({
                    'id': url_data['url'],
                    'text': f"{url_data.get('title', '')} - {url_data['content_summary']}",
                    'url': url_data['url'],
                    'title': url_data.get('title', '')
                })

        if not documents:
            return []

        # Rerank by query relevance
        try:
            rerank_result = await self.jina.rerank(
                query=query,
                documents=[d['text'] for d in documents],
                top_n=top_k
            )

            results = []
            for item in rerank_result.get('results', []):
                idx = item.get('index', 0)
                if idx < len(documents):
                    doc = documents[idx].copy()
                    doc['relevance_score'] = item.get('relevance_score', 0)
                    results.append(doc)

            # Cache results
            if self.cache:
                self.cache.set(f"rag:{query}", results, params=filters)

            return results

        except Exception as e:
            # Fallback to simple keyword matching
            query_words = set(query.lower().split())
            scored = []
            for doc in documents:
                doc_words = set(doc['text'].lower().split())
                overlap = len(query_words & doc_words)
                if overlap > 0:
                    scored.append((overlap / len(query_words), doc))

            scored.sort(reverse=True)
            return [doc for _, doc in scored[:top_k]]

    async def generate_context(
        self,
        query: str,
        retrieved_docs: List[Dict],
        max_context_length: int = 4000
    ) -> str:
        """Generate context string from retrieved documents"""
        context_parts = []
        total_length = 0

        for doc in retrieved_docs:
            doc_text = f"[{doc.get('title', 'Source')}]({doc['url']})\n{doc['text']}\n"

            if total_length + len(doc_text) > max_context_length:
                break

            context_parts.append(doc_text)
            total_length += len(doc_text)

        return "\n---\n".join(context_parts)

    async def rag_query(
        self,
        query: str,
        top_k: int = 5,
        include_sources: bool = True
    ) -> Dict[str, Any]:
        """Execute RAG query - retrieve and prepare for generation"""
        # Retrieve
        docs = await self.retrieve(query, top_k=top_k)

        # Generate context
        context = await self.generate_context(query, docs)

        # Build prompt for LLM
        rag_prompt = f"""Answer the following question based on the provided context. If the context doesn't contain relevant information, say so.

Context:
{context}

Question: {query}

Answer:"""

        result = {
            'query': query,
            'context': context,
            'rag_prompt': rag_prompt,
            'retrieved_documents': docs if include_sources else None,
            'document_count': len(docs)
        }

        return result


class SearchAnalytics:
    """Track and analyze search patterns"""

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS search_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                query TEXT,
                results_count INTEGER,
                latency_ms REAL,
                user_id TEXT,
                session_id TEXT,
                metadata TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_search_events_type
            ON search_events(event_type)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_search_events_created
            ON search_events(created_at)
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ab_tests (
                test_id TEXT PRIMARY KEY,
                test_name TEXT NOT NULL,
                variants TEXT NOT NULL,
                traffic_split TEXT NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ended_at TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ab_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                variant TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(test_id, user_id)
            )
        """)

        self.conn.commit()

    def log_event(
        self,
        event_type: str,
        query: str = None,
        results_count: int = None,
        latency_ms: float = None,
        user_id: str = None,
        session_id: str = None,
        metadata: Dict = None
    ):
        """Log a search event"""
        self.conn.execute("""
            INSERT INTO search_events
            (event_type, query, results_count, latency_ms, user_id, session_id, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event_type,
            query,
            results_count,
            latency_ms,
            user_id,
            session_id,
            json.dumps(metadata or {})
        ))
        self.conn.commit()

    def get_query_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get query statistics for the last N hours"""
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_queries,
                COUNT(DISTINCT query) as unique_queries,
                AVG(results_count) as avg_results,
                AVG(latency_ms) as avg_latency_ms,
                MIN(latency_ms) as min_latency_ms,
                MAX(latency_ms) as max_latency_ms
            FROM search_events
            WHERE event_type = 'search'
            AND created_at > datetime('now', ?)
        """, (f'-{hours} hours',))

        row = cursor.fetchone()
        return {
            'period_hours': hours,
            'total_queries': row[0],
            'unique_queries': row[1],
            'avg_results': row[2],
            'avg_latency_ms': row[3],
            'min_latency_ms': row[4],
            'max_latency_ms': row[5]
        }

    def get_top_queries(self, limit: int = 10, hours: int = 24) -> List[Dict[str, Any]]:
        """Get most frequent queries"""
        cursor = self.conn.execute("""
            SELECT query, COUNT(*) as count, AVG(results_count) as avg_results
            FROM search_events
            WHERE event_type = 'search'
            AND created_at > datetime('now', ?)
            AND query IS NOT NULL
            GROUP BY query
            ORDER BY count DESC
            LIMIT ?
        """, (f'-{hours} hours', limit))

        return [
            {'query': row[0], 'count': row[1], 'avg_results': row[2]}
            for row in cursor.fetchall()
        ]

    def get_zero_result_queries(self, limit: int = 10, hours: int = 24) -> List[str]:
        """Get queries that returned no results"""
        cursor = self.conn.execute("""
            SELECT DISTINCT query
            FROM search_events
            WHERE event_type = 'search'
            AND results_count = 0
            AND created_at > datetime('now', ?)
            LIMIT ?
        """, (f'-{hours} hours', limit))

        return [row[0] for row in cursor.fetchall()]

    # A/B Testing
    def create_ab_test(
        self,
        test_name: str,
        variants: List[str],
        traffic_split: List[float]
    ) -> str:
        """Create a new A/B test"""
        test_id = str(uuid.uuid4())[:8]

        self.conn.execute("""
            INSERT INTO ab_tests (test_id, test_name, variants, traffic_split)
            VALUES (?, ?, ?, ?)
        """, (test_id, test_name, json.dumps(variants), json.dumps(traffic_split)))
        self.conn.commit()

        return test_id

    def get_ab_variant(self, test_id: str, user_id: str) -> str:
        """Get or assign A/B test variant for user"""
        # Check existing assignment
        cursor = self.conn.execute("""
            SELECT variant FROM ab_assignments
            WHERE test_id = ? AND user_id = ?
        """, (test_id, user_id))

        row = cursor.fetchone()
        if row:
            return row[0]

        # Get test config
        cursor = self.conn.execute("""
            SELECT variants, traffic_split FROM ab_tests
            WHERE test_id = ? AND is_active = 1
        """, (test_id,))

        row = cursor.fetchone()
        if not row:
            return None

        variants = json.loads(row[0])
        split = json.loads(row[1])

        # Assign variant based on user hash
        user_hash = int(hashlib.md5(f"{test_id}:{user_id}".encode()).hexdigest()[:8], 16)
        bucket = (user_hash % 100) / 100.0

        cumulative = 0.0
        variant = variants[-1]
        for i, s in enumerate(split):
            cumulative += s
            if bucket < cumulative:
                variant = variants[i]
                break

        # Store assignment
        self.conn.execute("""
            INSERT OR IGNORE INTO ab_assignments (test_id, user_id, variant)
            VALUES (?, ?, ?)
        """, (test_id, user_id, variant))
        self.conn.commit()

        return variant

    def get_ab_results(self, test_id: str) -> Dict[str, Any]:
        """Get A/B test results"""
        cursor = self.conn.execute("""
            SELECT
                a.variant,
                COUNT(*) as users,
                AVG(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) as click_rate
            FROM ab_assignments a
            LEFT JOIN search_events e ON a.user_id = e.user_id
            WHERE a.test_id = ?
            GROUP BY a.variant
        """, (test_id,))

        return {
            'test_id': test_id,
            'variants': [
                {'variant': row[0], 'users': row[1], 'click_rate': row[2]}
                for row in cursor.fetchall()
            ]
        }

    def close(self):
        if self.conn:
            self.conn.close()


class TemporalScorer:
    """Time-based relevance scoring with decay"""

    def __init__(self, half_life_days: float = 7.0):
        self.half_life = half_life_days * 24 * 3600  # Convert to seconds

    def decay_score(self, timestamp: datetime, base_score: float = 1.0) -> float:
        """Apply exponential decay to score based on age"""
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

        age_seconds = (datetime.now() - timestamp).total_seconds()
        decay = 0.5 ** (age_seconds / self.half_life)

        return base_score * decay

    def boost_recent(self, timestamp: datetime, max_boost: float = 2.0, boost_hours: int = 24) -> float:
        """Boost score for very recent content"""
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

        age_hours = (datetime.now() - timestamp).total_seconds() / 3600

        if age_hours < boost_hours:
            # Linear boost that decreases with age
            return 1.0 + (max_boost - 1.0) * (1 - age_hours / boost_hours)

        return 1.0

    def combined_temporal_score(
        self,
        timestamp: datetime,
        base_score: float = 1.0,
        use_decay: bool = True,
        use_boost: bool = True
    ) -> float:
        """Combine decay and boost for final temporal score"""
        score = base_score

        if use_decay:
            score = self.decay_score(timestamp, score)

        if use_boost:
            score *= self.boost_recent(timestamp)

        return score


# Import numpy for vector operations (used by multiple classes above)
try:
    import numpy as np
except ImportError:
    # Fallback for basic operations
    class NumpyFallback:
        @staticmethod
        def array(data, dtype=None):
            return list(data)
        @staticmethod
        def dot(a, b):
            return sum(x * y for x, y in zip(a, b))
        @staticmethod
        def ones(n):
            return [1.0] * n
        @staticmethod
        def zeros(n, dtype=None):
            return [0.0] * n
    np = NumpyFallback()


# ============================================================================
# Ultra-Advanced Features - Vector Store, LTR, Intent, Diversification
# ============================================================================

class VectorStore:
    """
    In-memory vector store with HNSW-like approximate nearest neighbor search.
    Uses a simple but effective approach: LSH (Locality Sensitive Hashing) + brute force.
    """

    def __init__(self, dimension: int = 1024, num_tables: int = 10, num_bits: int = 8):
        self.dimension = dimension
        self.num_tables = num_tables
        self.num_bits = num_bits

        # Storage
        self.vectors: Dict[str, np.ndarray] = {}
        self.metadata: Dict[str, Dict] = {}

        # LSH tables for approximate search
        self.hash_tables: List[Dict[int, List[str]]] = [{} for _ in range(num_tables)]
        self.random_planes: List[np.ndarray] = []

        # Initialize random hyperplanes for LSH
        self._init_hash_functions()

    def _init_hash_functions(self):
        """Initialize random hyperplanes for LSH hashing"""
        for _ in range(self.num_tables):
            planes = np.random.randn(self.num_bits, self.dimension).astype(np.float32)
            # Normalize planes
            norms = np.linalg.norm(planes, axis=1, keepdims=True)
            planes = planes / (norms + 1e-10)
            self.random_planes.append(planes)

    def _compute_hash(self, vector: np.ndarray, table_idx: int) -> int:
        """Compute LSH hash for a vector"""
        planes = self.random_planes[table_idx]
        projections = np.dot(planes, vector)
        bits = (projections > 0).astype(int)
        hash_val = sum(b << i for i, b in enumerate(bits))
        return hash_val

    def add(self, doc_id: str, vector: List[float], metadata: Dict = None):
        """Add a vector to the store"""
        vec = np.array(vector, dtype=np.float32)

        # Normalize
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm

        self.vectors[doc_id] = vec
        self.metadata[doc_id] = metadata or {}

        # Add to hash tables
        for i, table in enumerate(self.hash_tables):
            h = self._compute_hash(vec, i)
            if h not in table:
                table[h] = []
            table[h].append(doc_id)

    def remove(self, doc_id: str):
        """Remove a vector from the store"""
        if doc_id not in self.vectors:
            return

        vec = self.vectors[doc_id]

        # Remove from hash tables
        for i, table in enumerate(self.hash_tables):
            h = self._compute_hash(vec, i)
            if h in table and doc_id in table[h]:
                table[h].remove(doc_id)

        del self.vectors[doc_id]
        if doc_id in self.metadata:
            del self.metadata[doc_id]

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_fn: callable = None
    ) -> List[Dict]:
        """
        Search for similar vectors using LSH + reranking.
        """
        if not self.vectors:
            return []

        query = np.array(query_vector, dtype=np.float32)
        norm = np.linalg.norm(query)
        if norm > 0:
            query = query / norm

        # Collect candidates from all hash tables
        candidates = set()
        for i, table in enumerate(self.hash_tables):
            h = self._compute_hash(query, i)
            if h in table:
                candidates.update(table[h])

        # If few candidates, fall back to brute force on subset
        if len(candidates) < top_k * 2:
            # Sample additional candidates
            all_ids = list(self.vectors.keys())
            sample_size = min(len(all_ids), top_k * 10)
            candidates.update(np.random.choice(all_ids, sample_size, replace=False))

        # Score candidates
        scored = []
        for doc_id in candidates:
            if filter_fn and not filter_fn(self.metadata.get(doc_id, {})):
                continue

            vec = self.vectors[doc_id]
            score = float(np.dot(query, vec))
            scored.append({
                'id': doc_id,
                'score': score,
                'metadata': self.metadata.get(doc_id, {})
            })

        # Sort and return top_k
        scored.sort(key=lambda x: x['score'], reverse=True)
        return scored[:top_k]

    def batch_add(self, items: List[Dict]):
        """Add multiple vectors at once"""
        for item in items:
            self.add(item['id'], item['vector'], item.get('metadata'))

    def get_stats(self) -> Dict:
        """Get store statistics"""
        return {
            'total_vectors': len(self.vectors),
            'dimension': self.dimension,
            'num_tables': self.num_tables,
            'avg_bucket_size': np.mean([
                len(bucket) for table in self.hash_tables
                for bucket in table.values()
            ]) if any(self.hash_tables) else 0
        }


class QueryIntentClassifier:
    """
    Classify query intent: navigational, informational, transactional, or conversational.
    Uses keyword patterns and query structure analysis.
    """

    NAVIGATIONAL_PATTERNS = [
        r'\b(go to|open|visit|navigate|show me)\b',
        r'\b(homepage|website|site|page|url)\b',
        r'\b(login|signin|sign in|log in)\b',
        r'^[a-zA-Z0-9-]+\.(com|org|net|io|ai|dev)$',
    ]

    INFORMATIONAL_PATTERNS = [
        r'^how (to|do|does|can|should)\b',  # "how to", "how do I", etc.
        r'\b(tutorial|guide|learn|understand|introduction|basics)\b',
        r'\b(what is|what are|what does)\b',
        r'\b(example|examples|sample|samples)\b',
        r'\b(documentation|docs|reference|manual)\b',
        r'\b(best practices|tips|tricks|advice)\b',
    ]

    TRANSACTIONAL_PATTERNS = [
        r'\b(buy|purchase|order|subscribe)\b',  # Removed 'download' and 'install'
        r'\b(price|cost|pricing|discount|coupon|deal)\b',
        r'\b(free trial|signup|sign up|register)\b',
        r'\b(book|reserve|schedule|appointment)\b',
        r'\b(add to cart|checkout|pay|payment)\b',
    ]

    CONVERSATIONAL_PATTERNS = [
        r'\?$',  # Ends with question mark
        r'\b(explain|tell me|describe|define)\b',
        r'\b(difference between|compare|versus|vs)\b',
        r'\b(like|similar to|prefer)\b',
    ]

    CODE_PATTERNS = [
        r'\b(error|exception|bug|fix|debug)\b',
        r'\b(function|class|method|variable|import)\b',
        r'\b(python|javascript|typescript|java|rust|golang|c\+\+)\b',
        r'[{}\[\]();=<>]',
        r'\b(npm|pip|cargo|gradle|maven|install|setup|configure)\b',
        r'\bdef\s+\w+|class\s+\w+|function\s+\w+',
    ]

    def __init__(self):
        import re
        self.nav_patterns = [re.compile(p, re.IGNORECASE) for p in self.NAVIGATIONAL_PATTERNS]
        self.info_patterns = [re.compile(p, re.IGNORECASE) for p in self.INFORMATIONAL_PATTERNS]
        self.trans_patterns = [re.compile(p, re.IGNORECASE) for p in self.TRANSACTIONAL_PATTERNS]
        self.conv_patterns = [re.compile(p, re.IGNORECASE) for p in self.CONVERSATIONAL_PATTERNS]
        self.code_patterns = [re.compile(p, re.IGNORECASE) for p in self.CODE_PATTERNS]

    def classify(self, query: str) -> Dict[str, Any]:
        """Classify query intent with confidence scores"""
        scores = {
            'navigational': 0.0,
            'informational': 0.0,
            'transactional': 0.0,
            'conversational': 0.0,
            'code': 0.0
        }

        query_lower = query.lower()

        # Check navigational patterns
        for pattern in self.nav_patterns:
            if pattern.search(query):
                scores['navigational'] += 0.35

        # Check informational patterns (high priority - "how to" queries)
        for pattern in self.info_patterns:
            if pattern.search(query):
                scores['informational'] += 0.4

        # Check transactional patterns
        for pattern in self.trans_patterns:
            if pattern.search(query):
                scores['transactional'] += 0.35

        # Check conversational patterns
        for pattern in self.conv_patterns:
            if pattern.search(query):
                scores['conversational'] += 0.3

        # Check code patterns
        for pattern in self.code_patterns:
            if pattern.search(query):
                scores['code'] += 0.3

        # Special handling: "how to" followed by technical term -> code-related informational
        if query_lower.startswith('how to ') and scores['code'] > 0:
            scores['informational'] += 0.2
            scores['code'] += 0.1

        # Query length heuristics
        word_count = len(query.split())
        if word_count <= 2:
            scores['navigational'] += 0.15
        elif word_count >= 4:
            scores['informational'] += 0.15
            scores['conversational'] += 0.05

        # Default to informational if no strong signal
        if max(scores.values()) < 0.25:
            scores['informational'] = 0.5

        # Normalize
        total = sum(scores.values())
        if total > 0:
            scores = {k: v / total for k, v in scores.items()}

        # Determine primary intent
        primary_intent = max(scores, key=scores.get)

        return {
            'primary_intent': primary_intent,
            'confidence': scores[primary_intent],
            'all_scores': scores,
            'query_length': word_count
        }

    def get_search_strategy(self, intent: str) -> Dict[str, Any]:
        """Get recommended search strategy based on intent"""
        strategies = {
            'navigational': {
                'top_k': 3,
                'use_reranking': False,
                'boost_exact_match': True,
                'include_urls': True
            },
            'informational': {
                'top_k': 10,
                'use_reranking': True,
                'expand_query': True,
                'include_snippets': True
            },
            'transactional': {
                'top_k': 5,
                'use_reranking': True,
                'boost_commercial': True,
                'include_pricing': True
            },
            'conversational': {
                'top_k': 5,
                'use_reranking': True,
                'use_rag': True,
                'generate_answer': True
            },
            'code': {
                'top_k': 10,
                'use_reranking': True,
                'boost_docs': True,
                'include_code_snippets': True
            }
        }
        return strategies.get(intent, strategies['informational'])


class MMRDiversifier:
    """
    Maximal Marginal Relevance (MMR) for result diversification.
    Balances relevance with diversity to avoid redundant results.
    """

    def __init__(self, lambda_param: float = 0.7):
        """
        lambda_param: Trade-off between relevance (1.0) and diversity (0.0)
        """
        self.lambda_param = lambda_param

    def diversify(
        self,
        query_embedding: List[float],
        candidates: List[Dict],
        top_k: int = 10,
        embedding_key: str = 'embedding',
        lambda_param: float = None
    ) -> List[Dict]:
        """
        Apply MMR to diversify results.

        Args:
            query_embedding: Query vector
            candidates: List of dicts with 'embedding' and 'score' keys
            top_k: Number of results to return
            embedding_key: Key for embeddings in candidate dicts
            lambda_param: Override for relevance/diversity trade-off (uses instance default if None)

        Returns:
            Diversified list of candidates
        """
        # Use instance lambda if not overridden
        effective_lambda = lambda_param if lambda_param is not None else self.lambda_param
        if not candidates:
            return []

        query = np.array(query_embedding, dtype=np.float32)
        query_norm = np.linalg.norm(query)
        if query_norm > 0:
            query = query / query_norm

        # Extract embeddings
        embeddings = []
        for c in candidates:
            if embedding_key in c and c[embedding_key]:
                emb = np.array(c[embedding_key], dtype=np.float32)
                norm = np.linalg.norm(emb)
                if norm > 0:
                    emb = emb / norm
                embeddings.append(emb)
            else:
                embeddings.append(None)

        selected = []
        selected_indices = []
        remaining = list(range(len(candidates)))

        while len(selected) < top_k and remaining:
            best_score = float('-inf')
            best_idx = None

            for idx in remaining:
                if embeddings[idx] is None:
                    # No embedding, use original score
                    mmr_score = candidates[idx].get('score', 0)
                else:
                    # Relevance to query
                    relevance = float(np.dot(query, embeddings[idx]))

                    # Max similarity to already selected
                    max_sim = 0.0
                    for sel_idx in selected_indices:
                        if embeddings[sel_idx] is not None:
                            sim = float(np.dot(embeddings[idx], embeddings[sel_idx]))
                            max_sim = max(max_sim, sim)

                    # MMR score
                    mmr_score = effective_lambda * relevance - (1 - effective_lambda) * max_sim

                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = idx

            if best_idx is not None:
                candidate = candidates[best_idx].copy()
                candidate['mmr_score'] = best_score
                candidate['diversity_rank'] = len(selected) + 1
                selected.append(candidate)
                selected_indices.append(best_idx)
                remaining.remove(best_idx)

        return selected


class LearningToRank:
    """
    Simple Learning to Rank (LTR) model using gradient boosting on features.
    Learns from user feedback to improve ranking.
    """

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

        # Feature weights (learned from feedback)
        self.weights = {
            'bm25_score': 1.0,
            'semantic_score': 1.0,
            'pagerank': 0.5,
            'recency': 0.3,
            'click_rate': 0.8,
            'domain_authority': 0.4,
            'title_match': 0.6,
            'exact_match': 0.7
        }

        # Click-through data
        self.click_data: Dict[str, Dict] = {}

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ltr_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                clicked INTEGER DEFAULT 0,
                dwell_time_ms INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ltr_weights (
                feature TEXT PRIMARY KEY,
                weight REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ltr_query
            ON ltr_feedback(query)
        """)

        self.conn.commit()

        # Load saved weights
        self._load_weights()

    def _load_weights(self):
        """Load weights from database"""
        cursor = self.conn.execute("SELECT feature, weight FROM ltr_weights")
        for row in cursor:
            self.weights[row[0]] = row[1]

    def _save_weights(self):
        """Save weights to database"""
        for feature, weight in self.weights.items():
            self.conn.execute("""
                INSERT OR REPLACE INTO ltr_weights (feature, weight, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (feature, weight))
        self.conn.commit()

    def extract_features(self, query: str, doc: Dict) -> Dict[str, float]:
        """Extract ranking features from a document"""
        features = {}

        # Text matching features
        query_lower = query.lower()
        title = doc.get('title', '').lower()
        content = doc.get('content', '').lower()

        # Exact match
        features['exact_match'] = 1.0 if query_lower in title else 0.0

        # Title match (word overlap)
        query_words = set(query_lower.split())
        title_words = set(title.split())
        if query_words:
            features['title_match'] = len(query_words & title_words) / len(query_words)
        else:
            features['title_match'] = 0.0

        # Existing scores
        features['bm25_score'] = doc.get('bm25_score', 0.0)
        features['semantic_score'] = doc.get('score', doc.get('semantic_score', 0.0))
        features['pagerank'] = doc.get('pagerank', 0.0)
        features['recency'] = doc.get('temporal_score', 0.5)

        # Domain authority (simplified)
        url = doc.get('url', '')
        if 'github.com' in url or 'arxiv.org' in url or 'docs.' in url:
            features['domain_authority'] = 0.8
        elif '.edu' in url or '.gov' in url:
            features['domain_authority'] = 0.9
        else:
            features['domain_authority'] = 0.5

        # Click-through rate from history
        doc_id = doc.get('id', doc.get('url', ''))
        if doc_id in self.click_data:
            cd = self.click_data[doc_id]
            if cd['impressions'] > 0:
                features['click_rate'] = cd['clicks'] / cd['impressions']
            else:
                features['click_rate'] = 0.0
        else:
            features['click_rate'] = 0.0

        return features

    def score(self, query: str, doc: Dict) -> float:
        """Compute LTR score for a document"""
        features = self.extract_features(query, doc)

        score = sum(
            self.weights.get(feat, 0.0) * value
            for feat, value in features.items()
        )

        return score

    def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        """Rerank documents using LTR model"""
        scored_docs = []
        for doc in docs:
            ltr_score = self.score(query, doc)
            doc_copy = doc.copy()
            doc_copy['ltr_score'] = ltr_score
            doc_copy['ltr_features'] = self.extract_features(query, doc)
            scored_docs.append(doc_copy)

        scored_docs.sort(key=lambda x: x['ltr_score'], reverse=True)
        return scored_docs

    def record_impression(self, query: str, doc_id: str, position: int):
        """Record that a document was shown"""
        self.conn.execute("""
            INSERT INTO ltr_feedback (query, doc_id, position)
            VALUES (?, ?, ?)
        """, (query, doc_id, position))
        self.conn.commit()

        # Update click data
        if doc_id not in self.click_data:
            self.click_data[doc_id] = {'impressions': 0, 'clicks': 0}
        self.click_data[doc_id]['impressions'] += 1

    def record_click(self, query: str, doc_id: str, dwell_time_ms: int = None):
        """Record that a document was clicked"""
        self.conn.execute("""
            UPDATE ltr_feedback
            SET clicked = 1, dwell_time_ms = ?
            WHERE query = ? AND doc_id = ?
            AND id = (
                SELECT id FROM ltr_feedback
                WHERE query = ? AND doc_id = ?
                ORDER BY created_at DESC LIMIT 1
            )
        """, (dwell_time_ms, query, doc_id, query, doc_id))
        self.conn.commit()

        # Update click data
        if doc_id not in self.click_data:
            self.click_data[doc_id] = {'impressions': 1, 'clicks': 0}
        self.click_data[doc_id]['clicks'] += 1

    def learn_from_feedback(self, learning_rate: float = 0.01):
        """
        Update weights based on click feedback.
        Uses a simple online learning approach.
        """
        # Get recent feedback
        cursor = self.conn.execute("""
            SELECT query, doc_id, position, clicked, dwell_time_ms
            FROM ltr_feedback
            WHERE created_at > datetime('now', '-1 day')
        """)

        feedback = cursor.fetchall()
        if not feedback:
            return

        # Group by query
        query_feedback: Dict[str, List] = {}
        for row in feedback:
            query = row[0]
            if query not in query_feedback:
                query_feedback[query] = []
            query_feedback[query].append({
                'doc_id': row[1],
                'position': row[2],
                'clicked': row[3],
                'dwell_time': row[4]
            })

        # Update weights using pairwise learning
        for query, docs in query_feedback.items():
            clicked_docs = [d for d in docs if d['clicked']]
            unclicked_docs = [d for d in docs if not d['clicked']]

            for clicked in clicked_docs:
                for unclicked in unclicked_docs:
                    # Clicked doc should rank higher
                    if clicked['position'] > unclicked['position']:
                        # Wrong order, update weights
                        # This is a simplified gradient update
                        for feature in self.weights:
                            self.weights[feature] *= (1 + learning_rate * 0.1)

        self._save_weights()

    def get_stats(self) -> Dict:
        """Get LTR statistics"""
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_impressions,
                SUM(clicked) as total_clicks,
                AVG(CASE WHEN clicked = 1 THEN position END) as avg_click_position
            FROM ltr_feedback
        """)
        row = cursor.fetchone()

        return {
            'total_impressions': row[0],
            'total_clicks': row[1],
            'avg_click_position': row[2],
            'click_through_rate': row[1] / row[0] if row[0] else 0,
            'weights': self.weights
        }

    def close(self):
        if self.conn:
            self.conn.close()


class SpellCorrector:
    """
    Spell correction using edit distance and frequency-based ranking.
    """

    def __init__(self):
        # Common words and their frequencies
        self.vocabulary: Dict[str, int] = {}
        self.max_edit_distance = 2

        # Load common words
        self._load_default_vocabulary()

    def _load_default_vocabulary(self):
        """Load common technical and search terms"""
        # Common English stop words (high frequency - won't be corrected)
        stop_words = [
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'up', 'about', 'into', 'over', 'after',
            'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
            'this', 'that', 'these', 'those', 'it', 'its', 'my', 'your', 'his', 'her',
            'what', 'which', 'who', 'when', 'where', 'why', 'how', 'all', 'each',
            'can', 'get', 'got', 'use', 'using', 'used', 'make', 'made', 'like',
            'just', 'also', 'more', 'most', 'other', 'some', 'any', 'no', 'not',
            'only', 'own', 'same', 'than', 'too', 'very', 'now', 'new', 'way'
        ]

        # Technical/programming terms
        tech_words = [
            'search', 'find', 'query', 'document', 'text', 'content',
            'machine', 'learning', 'deep', 'neural', 'network', 'model',
            'python', 'javascript', 'typescript', 'java', 'rust', 'golang', 'code',
            'api', 'endpoint', 'server', 'client', 'database', 'cache', 'data',
            'embedding', 'vector', 'semantic', 'similarity', 'ranking', 'index',
            'algorithm', 'function', 'class', 'method', 'variable', 'type',
            'error', 'exception', 'debug', 'test', 'deploy', 'build', 'run',
            'github', 'gitlab', 'docker', 'kubernetes', 'aws', 'cloud', 'web',
            'authentication', 'authorization', 'security', 'encryption', 'token',
            'performance', 'optimization', 'memory', 'cpu', 'gpu', 'fast', 'slow',
            'transformer', 'attention', 'bert', 'gpt', 'llm', 'nlp', 'ai',
            'retrieval', 'generation', 'classification', 'clustering', 'training',
            'fastapi', 'flask', 'django', 'react', 'vue', 'angular', 'node',
            'tutorial', 'documentation', 'example', 'guide', 'reference', 'help',
            'file', 'files', 'folder', 'directory', 'path', 'string', 'list',
            'array', 'object', 'json', 'xml', 'html', 'css', 'sql', 'http',
            'request', 'response', 'post', 'get', 'put', 'delete', 'patch',
            'install', 'setup', 'config', 'configuration', 'settings', 'options',
            'import', 'export', 'module', 'package', 'library', 'framework',
            'create', 'read', 'update', 'delete', 'add', 'remove', 'edit', 'save',
            'open', 'close', 'start', 'stop', 'load', 'download', 'upload',
            'image', 'video', 'audio', 'media', 'format', 'size', 'width', 'height'
        ]

        # Add stop words with very high frequency
        for word in stop_words:
            self.vocabulary[word] = 2000

        # Add tech words with high frequency
        for i, word in enumerate(tech_words):
            self.vocabulary[word] = 1000 - i

    def add_word(self, word: str, frequency: int = 1):
        """Add a word to vocabulary"""
        word = word.lower()
        self.vocabulary[word] = self.vocabulary.get(word, 0) + frequency

    def add_words_from_text(self, text: str):
        """Extract and add words from text"""
        words = text.lower().split()
        for word in words:
            # Clean word
            word = ''.join(c for c in word if c.isalnum())
            if len(word) >= 3:
                self.add_word(word)

    def _edit_distance(self, s1: str, s2: str) -> int:
        """Compute Levenshtein edit distance"""
        if len(s1) < len(s2):
            s1, s2 = s2, s1

        if len(s2) == 0:
            return len(s1)

        prev_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        return prev_row[-1]

    def _generate_candidates(self, word: str) -> List[str]:
        """Generate candidate corrections"""
        letters = 'abcdefghijklmnopqrstuvwxyz'
        word = word.lower()
        candidates = set()

        # Edit distance 1
        # Deletions
        for i in range(len(word)):
            candidates.add(word[:i] + word[i+1:])

        # Insertions
        for i in range(len(word) + 1):
            for c in letters:
                candidates.add(word[:i] + c + word[i:])

        # Substitutions
        for i in range(len(word)):
            for c in letters:
                if c != word[i]:
                    candidates.add(word[:i] + c + word[i+1:])

        # Transpositions
        for i in range(len(word) - 1):
            candidates.add(word[:i] + word[i+1] + word[i] + word[i+2:])

        return list(candidates)

    def correct(self, word: str) -> Dict[str, Any]:
        """Correct a potentially misspelled word"""
        word_lower = word.lower()

        # Already correct?
        if word_lower in self.vocabulary:
            return {
                'original': word,
                'corrected': word,
                'is_correct': True,
                'confidence': 1.0,
                'alternatives': []
            }

        # Generate candidates and filter by vocabulary
        candidates = self._generate_candidates(word_lower)
        valid_candidates = [
            (c, self.vocabulary[c], self._edit_distance(word_lower, c))
            for c in candidates
            if c in self.vocabulary
        ]

        if not valid_candidates:
            # Try edit distance 2
            candidates2 = set()
            for c1 in candidates:
                candidates2.update(self._generate_candidates(c1))

            valid_candidates = [
                (c, self.vocabulary[c], self._edit_distance(word_lower, c))
                for c in candidates2
                if c in self.vocabulary
            ]

        if not valid_candidates:
            return {
                'original': word,
                'corrected': word,
                'is_correct': False,
                'confidence': 0.0,
                'alternatives': []
            }

        # Sort by (edit_distance, -frequency)
        valid_candidates.sort(key=lambda x: (x[2], -x[1]))

        best = valid_candidates[0]
        alternatives = [
            {'word': c[0], 'frequency': c[1], 'distance': c[2]}
            for c in valid_candidates[1:5]
        ]

        return {
            'original': word,
            'corrected': best[0],
            'is_correct': False,
            'confidence': 1.0 / (1 + best[2]),
            'alternatives': alternatives
        }

    def correct_query(self, query: str) -> Dict[str, Any]:
        """Correct a full query"""
        words = query.split()
        corrected_words = []
        corrections = []

        for word in words:
            result = self.correct(word)
            corrected_words.append(result['corrected'])
            if not result['is_correct']:
                corrections.append(result)

        corrected_query = ' '.join(corrected_words)

        return {
            'original_query': query,
            'corrected_query': corrected_query,
            'was_corrected': len(corrections) > 0,
            'corrections': corrections
        }


class AutoComplete:
    """
    Query auto-completion using prefix trie and popularity ranking.
    """

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

        # Trie for prefix matching
        self.trie: Dict = {}

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                query TEXT PRIMARY KEY,
                frequency INTEGER DEFAULT 1,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

        # Load existing queries into trie
        self._load_queries()

    def _load_queries(self):
        """Load queries from database into trie"""
        cursor = self.conn.execute("""
            SELECT query, frequency FROM query_history
            ORDER BY frequency DESC
        """)

        for row in cursor:
            self._add_to_trie(row[0], row[1])

    def _add_to_trie(self, query: str, frequency: int):
        """Add a query to the trie"""
        node = self.trie
        for char in query.lower():
            if char not in node:
                node[char] = {}
            node = node[char]

        if '_queries' not in node:
            node['_queries'] = []
        node['_queries'].append((query, frequency))

    def add_query(self, query: str):
        """Record a query for future autocompletion"""
        query = query.strip().lower()
        if len(query) < 2:
            return

        # Update database
        self.conn.execute("""
            INSERT INTO query_history (query, frequency, last_used)
            VALUES (?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(query) DO UPDATE SET
                frequency = frequency + 1,
                last_used = CURRENT_TIMESTAMP
        """, (query,))
        self.conn.commit()

        # Update trie
        cursor = self.conn.execute(
            "SELECT frequency FROM query_history WHERE query = ?",
            (query,)
        )
        row = cursor.fetchone()
        if row:
            self._add_to_trie(query, row[0])

    def suggest(self, prefix: str, limit: int = 10) -> List[Dict]:
        """Get autocomplete suggestions for a prefix"""
        prefix = prefix.strip().lower()
        if len(prefix) < 1:
            return []

        # Navigate to prefix node
        node = self.trie
        for char in prefix:
            if char not in node:
                return []
            node = node[char]

        # Collect all queries under this prefix
        suggestions = []
        self._collect_queries(node, suggestions)

        # Sort by frequency and return top results
        suggestions.sort(key=lambda x: x[1], reverse=True)

        return [
            {'query': q, 'frequency': f}
            for q, f in suggestions[:limit]
        ]

    def _collect_queries(self, node: Dict, results: List):
        """Recursively collect all queries from a trie node"""
        if '_queries' in node:
            results.extend(node['_queries'])

        for key, child in node.items():
            if key != '_queries' and isinstance(child, dict):
                self._collect_queries(child, results)

    def get_trending(self, limit: int = 10, hours: int = 24) -> List[Dict]:
        """Get trending queries from recent history"""
        cursor = self.conn.execute("""
            SELECT query, frequency
            FROM query_history
            WHERE last_used > datetime('now', ?)
            ORDER BY frequency DESC
            LIMIT ?
        """, (f'-{hours} hours', limit))

        return [
            {'query': row[0], 'frequency': row[1]}
            for row in cursor.fetchall()
        ]

    def close(self):
        if self.conn:
            self.conn.close()


class SemanticRouter:
    """
    Route queries to specialized handlers based on semantic similarity.
    Useful for multi-domain search systems.
    """

    def __init__(self, jina: 'JinaDirectAPI' = None):
        self._jina = jina
        self.routes: Dict[str, Dict] = {}
        self.route_embeddings: Dict[str, List[float]] = {}

    def add_route(
        self,
        name: str,
        description: str,
        examples: List[str],
        handler: Any,  # Can be string or callable
        metadata: Dict = None
    ):
        """Add a semantic route"""
        self.routes[name] = {
            'description': description,
            'examples': examples,
            'handler': handler,
            'metadata': metadata or {}
        }

    async def compute_route_embeddings(self):
        """Compute embeddings for all routes"""
        if not self._jina:
            return

        for name, route in self.routes.items():
            # Combine description and examples
            texts = [route['description']] + route['examples']
            combined = ' '.join(texts)

            try:
                result = await self._jina.embed([combined])
                if 'data' in result and result['data']:
                    self.route_embeddings[name] = result['data'][0]['embedding']
            except Exception:
                pass

    async def route(self, query: str) -> Dict[str, Any]:
        """Route a query to the best handler"""
        if not self._jina or not self.route_embeddings:
            # Fallback to keyword matching
            return self._keyword_route(query)

        # Embed query
        try:
            result = await self._jina.embed([query], task='retrieval.query')
            if 'data' not in result or not result['data']:
                return self._keyword_route(query)

            query_emb = np.array(result['data'][0]['embedding'], dtype=np.float32)
            query_norm = np.linalg.norm(query_emb)
            if query_norm > 0:
                query_emb = query_emb / query_norm
        except Exception:
            return self._keyword_route(query)

        # Find best matching route
        best_route = None
        best_score = -1

        for name, emb in self.route_embeddings.items():
            route_emb = np.array(emb, dtype=np.float32)
            norm = np.linalg.norm(route_emb)
            if norm > 0:
                route_emb = route_emb / norm

            score = float(np.dot(query_emb, route_emb))
            if score > best_score:
                best_score = score
                best_route = name

        if best_route and best_score > 0.3:
            return {
                'route': best_route,
                'handler': self.routes[best_route]['handler'],
                'confidence': best_score,
                'metadata': self.routes[best_route]['metadata']
            }

        return {
            'route': 'default',
            'handler': 'general_search',
            'confidence': 0.0,
            'metadata': {}
        }

    def _keyword_route(self, query: str) -> Dict[str, Any]:
        """Fallback keyword-based routing"""
        query_lower = query.lower()

        for name, route in self.routes.items():
            # Check if query matches examples
            for example in route['examples']:
                if example.lower() in query_lower or query_lower in example.lower():
                    return {
                        'route': name,
                        'handler': route['handler'],
                        'confidence': 0.5,
                        'metadata': route['metadata']
                    }

        return {
            'route': 'default',
            'handler': 'general_search',
            'confidence': 0.0,
            'metadata': {}
        }


class ExplainableSearch:
    """
    Generate explanations for why results were returned.
    Helps users understand ranking decisions.
    """

    def __init__(self):
        self.feature_descriptions = {
            'semantic_score': 'Semantic similarity to your query',
            'bm25_score': 'Keyword match score',
            'title_match': 'Query terms found in title',
            'exact_match': 'Exact phrase match',
            'pagerank': 'Page authority score',
            'recency': 'Content freshness',
            'click_rate': 'Popularity with other users',
            'domain_authority': 'Source credibility',
            'personalization_boost': 'Matches your interests',
            'rrf_score': 'Combined ranking from multiple methods'
        }

    def explain_result(self, result: Dict, query: str) -> Dict[str, Any]:
        """Generate explanation for a single result"""
        explanations = []

        # Check for various scoring factors
        if result.get('semantic_score', 0) > 0.7:
            explanations.append({
                'factor': 'High semantic relevance',
                'description': 'Content meaning closely matches your query',
                'score': result['semantic_score']
            })

        if result.get('title_match', 0) > 0.5:
            explanations.append({
                'factor': 'Title match',
                'description': 'Your search terms appear in the title',
                'score': result['title_match']
            })

        if result.get('exact_match'):
            explanations.append({
                'factor': 'Exact match',
                'description': f'Contains exact phrase: "{query}"',
                'score': 1.0
            })

        if result.get('pagerank', 0) > 0.01:
            explanations.append({
                'factor': 'Authoritative source',
                'description': 'Many other pages link to this content',
                'score': result['pagerank']
            })

        if result.get('recency', 0) > 0.8:
            explanations.append({
                'factor': 'Recent content',
                'description': 'Recently published or updated',
                'score': result['recency']
            })

        if result.get('personalization_boost', 0.5) > 0.6:
            explanations.append({
                'factor': 'Personalized for you',
                'description': 'Based on your previous interactions',
                'score': result['personalization_boost']
            })

        # Highlight matching terms
        highlights = self._find_highlights(query, result)

        return {
            'result_id': result.get('id', result.get('url')),
            'explanations': explanations,
            'highlights': highlights,
            'summary': self._generate_summary(explanations)
        }

    def _find_highlights(self, query: str, result: Dict) -> List[Dict]:
        """Find query terms in result content"""
        highlights = []
        query_terms = query.lower().split()

        # Check title
        title = result.get('title', '')
        for term in query_terms:
            if term in title.lower():
                highlights.append({
                    'field': 'title',
                    'term': term,
                    'context': title
                })

        # Check snippet/content
        content = result.get('snippet', result.get('content', ''))[:500]
        for term in query_terms:
            if term in content.lower():
                # Find context around term
                idx = content.lower().find(term)
                start = max(0, idx - 30)
                end = min(len(content), idx + len(term) + 30)
                context = content[start:end]

                highlights.append({
                    'field': 'content',
                    'term': term,
                    'context': f'...{context}...'
                })

        return highlights

    def _generate_summary(self, explanations: List[Dict]) -> str:
        """Generate a human-readable summary"""
        if not explanations:
            return "This result was included based on general relevance."

        top_factors = sorted(explanations, key=lambda x: x.get('score', 0), reverse=True)[:2]
        factor_names = [f['factor'].lower() for f in top_factors]

        if len(factor_names) == 1:
            return f"Ranked highly due to {factor_names[0]}."
        else:
            return f"Ranked highly due to {factor_names[0]} and {factor_names[1]}."


class ConversationalSearch:
    """
    Multi-turn conversational search with context memory.
    Maintains conversation state and refines queries.
    """

    def __init__(self, db_path: str = "content_store.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

        # Active conversations
        self.conversations: Dict[str, Dict] = {}

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id TEXT PRIMARY KEY,
                user_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                context TEXT DEFAULT '[]'
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS conversation_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()

    def start_conversation(self, user_id: str = None) -> str:
        """Start a new conversation"""
        conv_id = str(uuid.uuid4())[:12]

        self.conn.execute("""
            INSERT INTO conversations (conversation_id, user_id)
            VALUES (?, ?)
        """, (conv_id, user_id))
        self.conn.commit()

        self.conversations[conv_id] = {
            'turns': [],
            'context': [],
            'entities': set(),
            'topic': None
        }

        return conv_id

    def add_turn(
        self,
        conversation_id: str,
        role: str,
        content: str,
        metadata: Dict = None
    ):
        """Add a turn to the conversation"""
        self.conn.execute("""
            INSERT INTO conversation_turns (conversation_id, role, content, metadata)
            VALUES (?, ?, ?, ?)
        """, (conversation_id, role, content, json.dumps(metadata or {})))

        self.conn.execute("""
            UPDATE conversations
            SET last_active = CURRENT_TIMESTAMP
            WHERE conversation_id = ?
        """, (conversation_id,))

        self.conn.commit()

        # Update in-memory state
        if conversation_id in self.conversations:
            self.conversations[conversation_id]['turns'].append({
                'role': role,
                'content': content,
                'metadata': metadata
            })

    def get_context(self, conversation_id: str, max_turns: int = 5) -> List[Dict]:
        """Get recent conversation context"""
        cursor = self.conn.execute("""
            SELECT role, content, metadata
            FROM conversation_turns
            WHERE conversation_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (conversation_id, max_turns))

        turns = [
            {'role': row[0], 'content': row[1], 'metadata': json.loads(row[2])}
            for row in cursor.fetchall()
        ]

        return turns[::-1]  # Oldest first

    def expand_query_with_context(
        self,
        conversation_id: str,
        query: str
    ) -> str:
        """Expand query using conversation context"""
        context = self.get_context(conversation_id)

        if not context:
            return query

        # Extract key entities and topics from context
        context_terms = set()
        for turn in context:
            words = turn['content'].lower().split()
            # Keep nouns and technical terms (simple heuristic: longer words)
            context_terms.update(w for w in words if len(w) > 5)

        # Check if query references context (pronouns, "it", "that", etc.)
        query_lower = query.lower()
        needs_expansion = any(
            pronoun in query_lower.split()
            for pronoun in ['it', 'that', 'this', 'they', 'them', 'those']
        )

        if needs_expansion and context:
            # Get the last user query for context
            last_query = None
            for turn in reversed(context):
                if turn['role'] == 'user':
                    last_query = turn['content']
                    break

            if last_query:
                # Simple expansion: append key terms from last query
                last_terms = [w for w in last_query.split() if len(w) > 3][:3]
                expanded = f"{query} {' '.join(last_terms)}"
                return expanded

        return query

    def detect_follow_up(self, conversation_id: str, query: str) -> Dict[str, Any]:
        """Detect if query is a follow-up question"""
        follow_up_indicators = [
            'what about', 'how about', 'and', 'also', 'more',
            'another', 'else', 'instead', 'rather', 'but',
            'why', 'how', 'when', 'where'  # At start of short queries
        ]

        query_lower = query.lower()
        is_follow_up = False
        indicator = None

        for ind in follow_up_indicators:
            if query_lower.startswith(ind):
                is_follow_up = True
                indicator = ind
                break

        # Very short queries are often follow-ups
        if len(query.split()) <= 3:
            context = self.get_context(conversation_id, 1)
            if context:
                is_follow_up = True

        # Get context references for the result
        context_refs = []
        if is_follow_up:
            context = self.get_context(conversation_id, 3)
            for turn in context:
                if turn['role'] == 'user':
                    context_refs.append(turn['content'][:50])

        return {
            'is_follow_up': is_follow_up,
            'indicator': indicator,
            'should_expand': is_follow_up,
            'context_references': context_refs
        }

    def get_conversation_history(self, conversation_id: str) -> List[Dict]:
        """Get full conversation history"""
        cursor = self.conn.execute("""
            SELECT role, content, metadata, created_at
            FROM conversation_turns
            WHERE conversation_id = ?
            ORDER BY created_at ASC
        """, (conversation_id,))

        turns = [
            {
                'role': row[0],
                'content': row[1],
                'metadata': json.loads(row[2]),
                'timestamp': row[3]
            }
            for row in cursor.fetchall()
        ]

        return turns

    def end_conversation(self, conversation_id: str):
        """End a conversation and clean up"""
        # Remove from active conversations
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]

        # Mark as ended in database (optional - could add status field)
        # For now, just leave the history in the database

    def close(self):
        if self.conn:
            self.conn.close()


# ============================================================================
# Request/Response Models
# ============================================================================

class SearchRequest(BaseModel):
    """Semantic search over available tools"""
    query: str = Field(..., min_length=1, description="Search query")
    top_k: int = Field(default=10, ge=1, le=50, description="Number of results")
    use_reranking: bool = Field(default=True, description="Apply reranking")
    category: Optional[str] = Field(None, description="Filter by category")
    min_score: float = Field(default=0.0, ge=0.0, le=1.0)


class EmbedRequest(BaseModel):
    """Generate embeddings for text"""
    inputs: List[str] = Field(..., min_length=1, max_length=100)
    model: str = Field(default="jina-embeddings-v3")
    task: str = Field(default="retrieval.passage", description="Task type: retrieval.query, retrieval.passage, etc.")
    dimensions: int = Field(default=1024, ge=256, le=4096)
    normalize: bool = Field(default=True)


class RerankRequest(BaseModel):
    """Rerank documents by relevance to query"""
    query: str = Field(..., min_length=1)
    documents: List[str] = Field(..., min_length=1, max_length=100)
    top_n: int = Field(default=10, ge=1, le=100)
    model: str = Field(default="jina-reranker-v2-base-multilingual")


class SegmentRequest(BaseModel):
    """Segment text into semantic chunks"""
    content: str = Field(..., min_length=1, description="Text to segment")
    max_chunk_length: int = Field(default=1000, ge=100, le=10000)
    return_tokens: bool = Field(default=False)
    return_chunks: bool = Field(default=True)


class ClassifyRequest(BaseModel):
    """Classify text into categories"""
    input: str = Field(..., min_length=1, description="Text to classify")
    labels: List[str] = Field(..., min_length=2, max_length=20, description="Classification labels")
    model: str = Field(default="jina-embeddings-v3")


class URLAddRequest(BaseModel):
    """Add URL(s) to tracking"""
    urls: List[str] = Field(..., min_length=1, max_length=100)
    source_query: Optional[str] = None
    tags: Optional[List[str]] = None


class URLMarkVisitedRequest(BaseModel):
    """Mark URL as visited"""
    url: str
    content_summary: Optional[str] = None
    content_type: Optional[str] = None
    status: str = Field(default="visited")


class URLQueryRequest(BaseModel):
    """Query stored URLs"""
    status: Optional[str] = None
    domain: Optional[str] = None
    search: Optional[str] = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class ImageAddRequest(BaseModel):
    """Add image(s) to tracking"""
    images: List[dict] = Field(..., min_length=1, max_length=100)
    source_url: Optional[str] = None
    source_query: Optional[str] = None


class ImageQueryRequest(BaseModel):
    """Query stored images"""
    source_url: Optional[str] = None
    status: Optional[str] = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class QueueEnqueueRequest(BaseModel):
    """Enqueue items for processing"""
    items: List[dict] = Field(..., min_length=1, max_length=500, description="Items with url, optional priority/payload")
    queue_type: str = Field(default="url", description="Queue type: url, image, content")
    source_query: Optional[str] = None


class QueueClaimRequest(BaseModel):
    """Claim items from queue for processing"""
    batch_size: int = Field(default=10, ge=1, le=100)
    queue_type: Optional[str] = None
    worker_id: Optional[str] = None


class QueueCompleteRequest(BaseModel):
    """Mark items as completed"""
    results: List[dict] = Field(..., min_length=1, description="List of {id, result?, error?}")


class PaginatedRequest(BaseModel):
    """Base for paginated requests"""
    cursor: Optional[str] = Field(None, description="Cursor for next page")
    limit: int = Field(default=20, ge=1, le=100, description="Items per page")


class HybridSearchRequest(BaseModel):
    """Combined keyword + semantic search"""
    query: str = Field(..., min_length=1)
    keyword_weight: float = Field(default=0.3, ge=0.0, le=1.0, description="Weight for keyword matching")
    semantic_weight: float = Field(default=0.7, ge=0.0, le=1.0, description="Weight for semantic matching")
    filters: Optional[dict] = Field(None, description="Additional filters")
    limit: int = Field(default=20, ge=1, le=100)


class ExtractRequest(BaseModel):
    """Extract structured data from content"""
    url: str = Field(..., min_length=1)
    content: Optional[str] = Field(None, description="Content to extract from (if not fetching)")
    extract_links: bool = Field(default=True)
    extract_images: bool = Field(default=True)
    extract_entities: bool = Field(default=True)
    entity_types: List[str] = Field(default=["person", "org", "date", "money", "email", "phone", "url"])
    auto_enqueue_links: bool = Field(default=False, description="Auto-enqueue discovered links")


class WebhookRegisterRequest(BaseModel):
    """Register a webhook"""
    url: str = Field(..., min_length=1)
    events: List[str] = Field(..., min_length=1, description="Events: url.visited, queue.completed, content.changed, etc.")
    secret: Optional[str] = Field(None, description="Secret for HMAC signature")


class SmartEnqueueRequest(BaseModel):
    """Enqueue with smart prioritization"""
    items: List[dict] = Field(..., min_length=1, max_length=100)
    queue_type: str = Field(default="url")
    source_query: Optional[str] = Field(None, description="Query for relevance scoring")
    compute_priority: bool = Field(default=True, description="Use ML-based priority")


class ContentDiffRequest(BaseModel):
    """Check if content has changed"""
    url: str
    content: str = Field(..., min_length=1)


class ClusterRequest(BaseModel):
    """Assign URLs to semantic clusters"""
    urls: List[str] = Field(..., min_length=1, max_length=100)
    num_clusters: int = Field(default=5, ge=2, le=20)
    labels: Optional[List[str]] = Field(None, description="Optional cluster labels")


class FeedbackRequest(BaseModel):
    """Provide relevance feedback for learning"""
    url: str
    query: str
    relevance: float = Field(..., ge=0.0, le=1.0, description="0=irrelevant, 1=highly relevant")


class WebSearchRequest(BaseModel):
    """Search the web"""
    query: str = Field(..., min_length=1)
    num_results: int = Field(default=5, ge=1, le=20)


class WebReadRequest(BaseModel):
    """Read and extract content from URL"""
    url: str = Field(..., min_length=1)
    target_selector: Optional[str] = Field(None, description="CSS selector to target")
    wait_for: Optional[str] = Field(None, description="Wait for selector before extracting")


class InteractRequest(BaseModel):
    """Minimal interaction with search tools"""
    message: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    model: str = Field(default=DEFAULT_MODEL)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=4000, gt=0)
    tools_enabled: bool = Field(default=True)


# Advanced Feature Request Models

class MultiQuerySearchRequest(BaseModel):
    """Multi-query search with result fusion"""
    queries: List[str] = Field(..., min_length=1, description="Multiple queries to execute and fuse")
    top_k: int = Field(default=10, ge=1, le=50)
    expand_queries: bool = Field(default=True, description="Auto-expand with synonyms")


class RAGQueryRequest(BaseModel):
    """RAG (Retrieval-Augmented Generation) query"""
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=5, ge=1, le=20)
    include_sources: bool = Field(default=True)
    max_context_length: int = Field(default=4000, ge=500, le=16000)


class SessionCreateRequest(BaseModel):
    """Create a search session"""
    user_id: Optional[str] = None


class SessionQueryRequest(BaseModel):
    """Add query to session"""
    session_id: str
    query: str
    results_count: int = 0
    clicked: List[str] = Field(default_factory=list)


class UserInteractionRequest(BaseModel):
    """Record user interaction for personalization"""
    user_id: str
    url: str
    interaction_type: str = Field(..., description="click, bookmark, share, dwell, skip")
    score: float = Field(default=1.0, ge=0.0, le=2.0)


class DeduplicateRequest(BaseModel):
    """Check for near-duplicate content"""
    url: str
    text: str


class ABTestCreateRequest(BaseModel):
    """Create an A/B test"""
    test_name: str
    variants: List[str] = Field(..., min_length=2)
    traffic_split: List[float] = Field(..., min_length=2, description="Must sum to 1.0")


class ABTestVariantRequest(BaseModel):
    """Get A/B test variant for user"""
    test_id: str
    user_id: str


class CacheInvalidateRequest(BaseModel):
    """Invalidate cache entries"""
    pattern: Optional[str] = Field(None, description="Pattern to match, or None to clear all")


class AdvancedSearchRequest(BaseModel):
    """Advanced search with all features enabled"""
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=10, ge=1, le=50)
    expand_queries: bool = Field(default=True)
    use_cache: bool = Field(default=True)
    use_pagerank: bool = Field(default=True)
    use_temporal_decay: bool = Field(default=True)
    user_id: Optional[str] = Field(None, description="For personalization")
    session_id: Optional[str] = Field(None, description="For session context")


# Ultra-Advanced Request Models

class UltraSearchRequest(BaseModel):
    """Ultra-advanced search with all ML features"""
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=10, ge=1, le=50)
    use_intent_detection: bool = Field(default=True)
    use_spell_correction: bool = Field(default=True)
    use_query_expansion: bool = Field(default=True)
    use_ltr: bool = Field(default=True, description="Learning to Rank")
    use_mmr: bool = Field(default=True, description="Maximal Marginal Relevance diversification")
    use_personalization: bool = Field(default=True)
    use_explanations: bool = Field(default=False)
    conversation_id: Optional[str] = Field(None, description="For conversational search")
    user_id: Optional[str] = None
    lambda_diversity: float = Field(default=0.7, ge=0.0, le=1.0, description="Relevance vs diversity tradeoff")


class VectorAddRequest(BaseModel):
    """Add vector to the vector store"""
    doc_id: str
    vector: List[float]
    metadata: Optional[Dict[str, Any]] = None


class VectorSearchRequest(BaseModel):
    """Search the vector store"""
    query_vector: List[float]
    top_k: int = Field(default=10, ge=1, le=100)


class SpellCheckRequest(BaseModel):
    """Check spelling of a query"""
    query: str


class AutoCompleteRequest(BaseModel):
    """Get autocomplete suggestions"""
    prefix: str
    limit: int = Field(default=10, ge=1, le=50)


class IntentClassifyRequest(BaseModel):
    """Classify query intent"""
    query: str


class DiversifyRequest(BaseModel):
    """Diversify search results using MMR"""
    query_embedding: List[float]
    candidates: List[Dict[str, Any]]
    top_k: int = Field(default=10, ge=1, le=50)
    lambda_param: float = Field(default=0.7, ge=0.0, le=1.0)


class LTRFeedbackRequest(BaseModel):
    """Record LTR feedback"""
    query: str
    doc_id: str
    clicked: bool = False
    dwell_time_ms: Optional[int] = None


class ConversationStartRequest(BaseModel):
    """Start a conversational search"""
    user_id: Optional[str] = None


class ConversationTurnRequest(BaseModel):
    """Add a turn to conversation"""
    conversation_id: str
    role: str = Field(..., description="user or assistant")
    content: str
    metadata: Optional[Dict[str, Any]] = None


class ExplainRequest(BaseModel):
    """Explain a search result"""
    query: str
    result: Dict[str, Any]


class SemanticRouteRequest(BaseModel):
    """Route a query to specialized handler"""
    query: str


# ============================================================================
# MCP State
# ============================================================================

@dataclass
class MCPConnection:
    """Single MCP connection state"""
    session: Any
    tools: List[Dict[str, Any]]
    openai_tools: List[Dict[str, Any]]
    tool_map: Dict[str, str]  # sanitized -> original


def sanitize_name(name: str) -> str:
    """Sanitize tool name for OpenAI compatibility"""
    import re
    import unicodedata
    cleaned = unicodedata.normalize("NFKD", name or "")
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_-").lower()
    return cleaned[:64] if cleaned else "tool"


async def fetch_tools(session) -> List[Dict[str, Any]]:
    """Fetch tools from MCP server"""
    resp = await session.list_tools()
    items = resp.get("tools") if isinstance(resp, dict) else getattr(resp, "tools", resp)

    tools = []
    for t in (items or []):
        name = getattr(t, "name", None) or t.get("name")
        desc = getattr(t, "description", None) or t.get("description", "")
        schema = (
            getattr(t, "input_schema", None) or
            t.get("input_schema") or
            {"type": "object", "properties": {}}
        )
        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}
        schema.setdefault("type", "object")
        schema.setdefault("properties", {})

        tools.append({"name": name, "description": desc, "input_schema": schema})

    return tools


def to_openai_format(tools: List[Dict]) -> Tuple[List[Dict], Dict[str, str]]:
    """Convert MCP tools to OpenAI format"""
    openai_tools = []
    name_map = {}
    used = set()

    for t in tools:
        name = t.get("name")
        if not name:
            continue

        sanitized = sanitize_name(name)
        counter = 1
        while sanitized in used:
            sanitized = f"{sanitize_name(name)}_{counter}"
            counter += 1
        used.add(sanitized)
        name_map[sanitized] = name

        openai_tools.append({
            "type": "function",
            "function": {
                "name": sanitized,
                "description": t.get("description", ""),
                "parameters": t.get("input_schema", {"type": "object", "properties": {}})
            }
        })

    return openai_tools, name_map


async def call_tool(session, name: str, args: Dict) -> str:
    """Call MCP tool and return flattened result"""
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


# ============================================================================
# Global State
# ============================================================================

mcp: Optional[MCPConnection] = None
retrieval: Optional[ToolRetrievalService] = None
http_client: Optional[httpx.AsyncClient] = None
content_store: Optional[ContentStore] = None
processing_queue: Optional[ProcessingQueue] = None
content_graph: Optional[ContentGraph] = None
webhook_manager: Optional[WebhookManager] = None
smart_prioritizer: Optional[SmartPrioritizer] = None

# Advanced Features
jina_direct: Optional[JinaDirectAPI] = None
semantic_cache: Optional[SemanticCache] = None
query_expander: Optional[QueryExpander] = None
multi_query_fusion: Optional[MultiQueryFusion] = None
search_session: Optional[SearchSession] = None
user_profile: Optional[UserProfile] = None
pagerank_scorer: Optional[PageRankScorer] = None
simhash_dedup: Optional[SimHashDeduplicator] = None
rag_pipeline: Optional[RAGPipeline] = None
search_analytics: Optional[SearchAnalytics] = None

# Ultra-Advanced Features
vector_store: Optional[VectorStore] = None
intent_classifier: Optional[QueryIntentClassifier] = None
mmr_diversifier: Optional[MMRDiversifier] = None
learning_to_rank: Optional[LearningToRank] = None
spell_corrector: Optional[SpellCorrector] = None
auto_complete: Optional[AutoComplete] = None
semantic_router: Optional[SemanticRouter] = None
explainable_search: Optional[ExplainableSearch] = None
conversational_search: Optional[ConversationalSearch] = None
temporal_scorer: Optional[TemporalScorer] = None


# ============================================================================
# Application
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp, retrieval, http_client, content_store, processing_queue
    global content_graph, webhook_manager, smart_prioritizer
    global jina_direct, semantic_cache, query_expander, multi_query_fusion
    global search_session, user_profile, pagerank_scorer, simhash_dedup
    global rag_pipeline, search_analytics, temporal_scorer

    print("Starting Jina Search Server...")

    # Initialize all stores (same DB for simplicity)
    db_path = "content_store.db"
    content_store = ContentStore(db_path)
    processing_queue = ProcessingQueue(db_path)
    content_graph = ContentGraph(db_path)
    webhook_manager = WebhookManager(db_path)
    smart_prioritizer = SmartPrioritizer()
    print("Content store, queue, graph, webhooks initialized")

    # Initialize advanced features
    jina_direct = JinaDirectAPI()
    semantic_cache = SemanticCache(max_size=1000, ttl_seconds=3600)
    semantic_cache.set_jina(jina_direct)
    query_expander = QueryExpander(jina_direct)
    multi_query_fusion = MultiQueryFusion(jina_direct)
    search_session = SearchSession(db_path)
    user_profile = UserProfile(db_path)
    pagerank_scorer = PageRankScorer(content_graph)
    simhash_dedup = SimHashDeduplicator()
    rag_pipeline = RAGPipeline(jina_direct, content_store, semantic_cache)
    search_analytics = SearchAnalytics(db_path)
    temporal_scorer = TemporalScorer(half_life_days=7.0)
    print("Advanced features initialized: cache, RAG, analytics, personalization")

    # Initialize ultra-advanced features
    global vector_store, intent_classifier, mmr_diversifier, learning_to_rank
    global spell_corrector, auto_complete, semantic_router, explainable_search
    global conversational_search

    vector_store = VectorStore(dimension=1024, num_tables=10, num_bits=8)
    intent_classifier = QueryIntentClassifier()
    mmr_diversifier = MMRDiversifier()
    learning_to_rank = LearningToRank(db_path)
    spell_corrector = SpellCorrector()  # No db_path needed - in-memory vocabulary
    auto_complete = AutoComplete(db_path)
    semantic_router = SemanticRouter()
    explainable_search = ExplainableSearch()
    conversational_search = ConversationalSearch(db_path)

    # Add default routes to semantic router
    semantic_router.add_route(
        name="code_search",
        description="Search for code, programming, APIs, functions",
        examples=["how to parse JSON in Python", "React hooks example", "SQL query syntax"],
        handler=lambda q: {"route": "code", "strategy": "code_focused"},
        metadata={"priority": 1}
    )
    semantic_router.add_route(
        name="academic_search",
        description="Search for papers, research, academic content",
        examples=["machine learning papers", "neural network architecture research", "transformer attention mechanism"],
        handler=lambda q: {"route": "academic", "strategy": "semantic_heavy"},
        metadata={"priority": 1}
    )
    semantic_router.add_route(
        name="product_search",
        description="Search for products, shopping, commercial items",
        examples=["buy laptop under $1000", "best wireless headphones", "iPhone vs Samsung"],
        handler=lambda q: {"route": "product", "strategy": "commercial"},
        metadata={"priority": 1}
    )

    print("Ultra-advanced features initialized: vector store, LTR, spell check, autocomplete, intent, MMR, conversational")

    # HTTP client
    http_client = httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": "http://localhost:8080",
        },
        timeout=120.0
    )

    # Connect to Jina MCP
    if JINA_API_KEY:
        try:
            print(f"Connecting to Jina MCP: {JINA_MCP_URL}")
            headers = {"Authorization": f"Bearer {JINA_API_KEY}"}

            client_ctx = sse_client(JINA_MCP_URL, headers)
            streams = await client_ctx.__aenter__()
            read, write = streams

            session = ClientSession(read, write)
            await session.__aenter__()
            await session.initialize()

            tools = await fetch_tools(session)
            openai_tools, tool_map = to_openai_format(tools)

            mcp = MCPConnection(
                session=session,
                tools=tools,
                openai_tools=openai_tools,
                tool_map=tool_map
            )

            print(f"Connected. {len(tools)} tools available:")
            for t in tools:
                print(f"  - {t['name']}")

            # Initialize retrieval service
            if RETRIEVAL_AVAILABLE:
                print("Initializing tool retrieval...")
                retrieval = ToolRetrievalService("tool_index.db")

                # Check for embedding/rerank tools
                tool_names = [t["name"] for t in tools]
                if "generate_embeddings" in tool_names and "sort_by_relevance" in tool_names:
                    async def embed_fn(params):
                        return await call_tool(session, "generate_embeddings", params)
                    async def rerank_fn(params):
                        return await call_tool(session, "sort_by_relevance", params)

                    retrieval.set_jina_functions(embed_fn, rerank_fn)

                    # Create mock registry for initialization
                    class MockRegistry:
                        def __init__(self, tools):
                            self._tools = tools
                        def get_all_tools(self):
                            return self._tools, [], {}
                        @property
                        def connections(self):
                            class MockState:
                                def __init__(self, tools):
                                    self.tools = tools
                                    self.server_id = "jina"
                            return {"jina": MockState(self._tools)}

                    mock_reg = MockRegistry(tools)
                    await retrieval.initialize(mock_reg)

                    stats = retrieval.get_stats()
                    print(f"Retrieval ready: {stats['total_tools']} tools indexed")
        except Exception as e:
            print(f"MCP connection failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("WARNING: JINA_API_KEY not set")

    print("Server ready!")

    # Set up webhook manager with HTTP client
    if webhook_manager:
        webhook_manager.set_http_client(http_client)

    try:
        yield
    finally:
        print("Shutting down...")
        if content_graph:
            content_graph.close()
        if webhook_manager:
            webhook_manager.close()
        if processing_queue:
            processing_queue.close()
        if content_store:
            content_store.close()
        if http_client:
            await http_client.aclose()
        # Ultra-advanced feature cleanup
        if learning_to_rank:
            learning_to_rank.close()
        if auto_complete:
            auto_complete.close()
        if conversational_search:
            conversational_search.close()


app = FastAPI(
    title="Jina Search Server",
    version="1.0.0",
    description="Focused search API powered by Jina",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# ============================================================================
# Endpoints
# ============================================================================

@app.get("/")
async def root():
    """API info"""
    return {
        "service": "Jina Search Server",
        "version": "2.0.0",
        "endpoints": {
            "search": {
                "semantic_search": "POST /v1/search",
                "hybrid_search": "POST /v1/search/hybrid",
                "web_search": "POST /v1/web/search",
                "web_read": "POST /v1/web/read"
            },
            "text_processing": {
                "embed": "POST /v1/embed",
                "rerank": "POST /v1/rerank",
                "segment": "POST /v1/segment",
                "classify": "POST /v1/classify"
            },
            "extraction": {
                "extract": "POST /v1/extract",
                "content_diff": "POST /v1/content/diff",
                "entities": "GET /v1/entities"
            },
            "graph": {
                "links": "GET /v1/graph/links",
                "cluster": "POST /v1/cluster"
            },
            "url_tracking": {
                "add_urls": "POST /v1/urls",
                "get_urls_paged": "GET /v1/urls/page",
                "mark_visited": "PUT /v1/urls/visited",
                "get_unvisited": "GET /v1/urls/unvisited",
                "stats": "GET /v1/urls/stats"
            },
            "image_tracking": {
                "add_images": "POST /v1/images",
                "get_images_paged": "GET /v1/images/page"
            },
            "queue": {
                "enqueue": "POST /v1/queue/enqueue",
                "smart_enqueue": "POST /v1/queue/smart-enqueue",
                "claim": "POST /v1/queue/claim",
                "complete": "POST /v1/queue/complete",
                "peek": "GET /v1/queue/peek",
                "stats": "GET /v1/queue/stats",
                "retry": "POST /v1/queue/retry"
            },
            "learning": {
                "feedback": "POST /v1/feedback"
            },
            "webhooks": {
                "register": "POST /v1/webhooks",
                "list": "GET /v1/webhooks",
                "delete": "DELETE /v1/webhooks/{id}"
            },
            "agent": {
                "interact": "POST /v1/interact",
                "tools": "GET /v1/tools"
            },
            "system": {
                "health": "GET /health"
            }
        }
    }


@app.get("/health")
async def health():
    """Health check"""
    return {
        "status": "healthy",
        "mcp_connected": mcp is not None,
        "tools_available": len(mcp.tools) if mcp else 0,
        "retrieval_ready": retrieval is not None and retrieval.initialized,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/v1/tools")
async def list_tools():
    """List available tools"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Group by category based on name/description
    search_tools = []
    embed_tools = []
    other_tools = []

    for t in mcp.tools:
        name = t["name"].lower()
        desc = (t.get("description") or "").lower()

        if "search" in name or "search" in desc or "web" in name:
            search_tools.append(t)
        elif "embed" in name or "vector" in desc:
            embed_tools.append(t)
        else:
            other_tools.append(t)

    return {
        "search": search_tools,
        "embedding": embed_tools,
        "other": other_tools,
        "total": len(mcp.tools)
    }


@app.post("/v1/search")
async def semantic_search(req: SearchRequest):
    """
    Semantic search over available tools.
    Uses embeddings + reranking for best results.
    """
    if not retrieval or not retrieval.initialized:
        raise HTTPException(503, "Retrieval service not available")

    start = time.time()

    result = await retrieval.get_tools_for_query(
        query=req.query,
        enable_multi_hop=True,
        max_tools=req.top_k
    )

    if not result:
        return {"results": [], "query": req.query, "time_ms": 0}

    # Filter by category and score
    tools = result["selected_tools"]
    if req.category:
        tools = [t for t in tools if t.get("category") == req.category]
    tools = [t for t in tools if t.get("score", 0) >= req.min_score]

    elapsed_ms = (time.time() - start) * 1000

    return {
        "results": tools[:req.top_k],
        "query": req.query,
        "is_multi_hop": result["metrics"]["is_multi_hop"],
        "time_ms": round(elapsed_ms, 1),
        "metrics": result["metrics"]
    }


@app.post("/v1/embed")
async def generate_embeddings(req: EmbedRequest):
    """Generate embeddings using Jina v3"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    result = await call_tool(mcp.session, "generate_embeddings", {
        "inputs": req.inputs,
        "model": req.model,
        "task": req.task,
        "dimensions": req.dimensions,
        "normalize": req.normalize
    })

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/v1/rerank")
async def rerank_documents(req: RerankRequest):
    """Rerank documents by relevance"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    result = await call_tool(mcp.session, "sort_by_relevance", {
        "query": req.query,
        "documents": req.documents,
        "top_n": req.top_n
    })

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/v1/segment")
async def segment_text(req: SegmentRequest):
    """Segment text into semantic chunks using Jina Segmenter"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Find segmentation tool
    segment_tool = None
    for t in mcp.tools:
        name = t["name"].lower()
        if "segment" in name or "chunk" in name:
            segment_tool = t["name"]
            break

    if not segment_tool:
        raise HTTPException(400, "No segmentation tool available")

    result = await call_tool(mcp.session, segment_tool, {
        "content": req.content,
        "max_chunk_length": req.max_chunk_length,
        "return_tokens": req.return_tokens,
        "return_chunks": req.return_chunks
    })

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/v1/classify")
async def classify_text(req: ClassifyRequest):
    """
    Classify text into categories using embeddings similarity.
    Computes embeddings for input and labels, returns ranked labels.
    """
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # First, check if there's a dedicated classification tool
    classify_tool = None
    for t in mcp.tools:
        name = t["name"].lower()
        if "classif" in name or "categoriz" in name:
            classify_tool = t["name"]
            break

    if classify_tool:
        result = await call_tool(mcp.session, classify_tool, {
            "input": req.input,
            "labels": req.labels
        })
        try:
            return json.loads(result)
        except:
            return {"raw": result}

    # Fallback: use embeddings + cosine similarity
    # Embed input and all labels
    all_texts = [req.input] + req.labels
    embed_result = await call_tool(mcp.session, "generate_embeddings", {
        "inputs": all_texts,
        "model": req.model,
        "task": "retrieval.query",
        "dimensions": 1024,
        "normalize": True
    })

    try:
        data = json.loads(embed_result) if isinstance(embed_result, str) else embed_result
        embeddings = [item["embedding"] for item in data["data"]]

        # Compute cosine similarities
        import numpy as np
        input_emb = np.array(embeddings[0])
        label_embs = [np.array(e) for e in embeddings[1:]]

        scores = []
        for i, label in enumerate(req.labels):
            similarity = float(np.dot(input_emb, label_embs[i]))
            scores.append({"label": label, "score": similarity})

        scores.sort(key=lambda x: x["score"], reverse=True)

        return {
            "input": req.input,
            "predictions": scores,
            "top_label": scores[0]["label"] if scores else None
        }
    except Exception as e:
        return {"error": str(e), "raw": embed_result}


# ============================================================================
# URL Tracking Endpoints
# ============================================================================

@app.post("/v1/urls")
async def add_urls(req: URLAddRequest):
    """Add URLs to tracking for future reference"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    results = []
    for url in req.urls:
        result = content_store.add_url(
            url=url,
            source_query=req.source_query,
            tags=req.tags
        )
        results.append(result)

    return {"added": len(results), "urls": results}


@app.put("/v1/urls/visited")
async def mark_url_visited(req: URLMarkVisitedRequest):
    """Mark a URL as visited with optional content summary"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    result = content_store.mark_visited(
        url=req.url,
        content_summary=req.content_summary,
        content_type=req.content_type,
        status=req.status
    )
    return result


@app.get("/v1/urls")
async def get_urls(
    status: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Query stored URLs with filters"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    if search:
        urls = content_store.search_urls(search, limit)
    else:
        urls = content_store.get_urls(
            status=status,
            domain=domain,
            limit=limit,
            offset=offset
        )

    return {"urls": urls, "count": len(urls)}


@app.get("/v1/urls/unvisited")
async def get_unvisited_urls(limit: int = Query(50, ge=1, le=500)):
    """Get URLs that haven't been visited yet - for agents to explore"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    urls = content_store.get_unvisited_urls(limit)
    return {"urls": urls, "count": len(urls)}


@app.get("/v1/urls/stats")
async def get_url_stats():
    """Get URL and image tracking statistics"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    return content_store.get_stats()


# ============================================================================
# Image Tracking Endpoints
# ============================================================================

@app.post("/v1/images")
async def add_images(req: ImageAddRequest):
    """Add images to tracking"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    results = []
    for img in req.images:
        result = content_store.add_image(
            url=img.get("url"),
            source_url=req.source_url or img.get("source_url"),
            alt_text=img.get("alt_text"),
            caption=img.get("caption"),
            width=img.get("width"),
            height=img.get("height"),
            source_query=req.source_query,
            tags=img.get("tags")
        )
        results.append(result)

    return {"added": len(results), "images": results}


@app.get("/v1/images")
async def get_images(
    source_url: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Query stored images with filters"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    images = content_store.get_images(
        source_url=source_url,
        status=status,
        limit=limit,
        offset=offset
    )

    return {"images": images, "count": len(images)}


# ============================================================================
# Processing Queue Endpoints (Event-Driven)
# ============================================================================

@app.post("/v1/queue/enqueue")
async def enqueue_items(req: QueueEnqueueRequest):
    """
    Enqueue URLs/items for processing.
    Use this to explicitly queue search results for later processing.
    """
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    result = processing_queue.enqueue_batch(
        items=req.items,
        queue_type=req.queue_type,
        source_query=req.source_query
    )

    return result


@app.post("/v1/queue/claim")
async def claim_batch(req: QueueClaimRequest):
    """
    Claim a batch of items for processing.
    Items are marked as 'processing' and won't be claimed by other workers.
    """
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    items = processing_queue.claim_batch(
        batch_size=req.batch_size,
        queue_type=req.queue_type,
        worker_id=req.worker_id
    )

    return {
        "claimed": len(items),
        "items": items,
        "worker_id": req.worker_id
    }


@app.post("/v1/queue/complete")
async def complete_batch(req: QueueCompleteRequest):
    """
    Mark items as completed or failed.
    Each result should have: {id, result?: {...}, error?: "..."}
    """
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    result = processing_queue.complete_batch(req.results)
    return result


@app.get("/v1/queue/peek")
async def peek_queue(
    queue_type: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100)
):
    """Peek at pending items without claiming them"""
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    items = processing_queue.peek(queue_type=queue_type, limit=limit)
    return {"items": items, "count": len(items)}


@app.get("/v1/queue/stats")
async def queue_stats():
    """Get queue statistics by type and status"""
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    return processing_queue.get_stats()


@app.post("/v1/queue/retry")
async def retry_failed(max_retries: int = Query(3, ge=1, le=10)):
    """Requeue failed items that haven't exceeded retry limit"""
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")

    count = processing_queue.retry_failed(max_retries)
    return {"requeued": count}


# ============================================================================
# Paginated List Endpoints (with cursors)
# ============================================================================

@app.get("/v1/urls/page")
async def get_urls_paginated(
    status: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None, description="Cursor from previous response"),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Get URLs with cursor-based pagination.
    Returns next_cursor for fetching more results.
    """
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    # Decode cursor (it's the last seen ID or timestamp)
    offset = 0
    if cursor:
        try:
            import base64
            offset = int(base64.b64decode(cursor).decode())
        except:
            offset = 0

    urls = content_store.get_urls(
        status=status,
        domain=domain,
        limit=limit + 1,  # Fetch one extra to check if more exist
        offset=offset
    )

    has_more = len(urls) > limit
    if has_more:
        urls = urls[:limit]

    # Create next cursor
    next_cursor = None
    if has_more:
        import base64
        next_cursor = base64.b64encode(str(offset + limit).encode()).decode()

    return {
        "urls": urls,
        "count": len(urls),
        "has_more": has_more,
        "next_cursor": next_cursor
    }


@app.get("/v1/images/page")
async def get_images_paginated(
    source_url: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Get images with cursor-based pagination"""
    if not content_store:
        raise HTTPException(503, "Content store not initialized")

    offset = 0
    if cursor:
        try:
            import base64
            offset = int(base64.b64decode(cursor).decode())
        except:
            offset = 0

    images = content_store.get_images(
        source_url=source_url,
        status=status,
        limit=limit + 1,
        offset=offset
    )

    has_more = len(images) > limit
    if has_more:
        images = images[:limit]

    next_cursor = None
    if has_more:
        import base64
        next_cursor = base64.b64encode(str(offset + limit).encode()).decode()

    return {
        "images": images,
        "count": len(images),
        "has_more": has_more,
        "next_cursor": next_cursor
    }


# ============================================================================
# Advanced Search & Extraction Endpoints
# ============================================================================

@app.post("/v1/search/hybrid")
async def hybrid_search(req: HybridSearchRequest):
    """
    Combined keyword + semantic search over stored URLs.
    Fuses BM25-style keyword matching with embedding similarity.
    """
    if not content_store:
        raise HTTPException(503, "Content store not initialized")
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Keyword search (simple text matching)
    keyword_results = content_store.search_urls(req.query, limit=req.limit * 2)

    # Semantic search via embeddings
    semantic_results = []
    if retrieval and retrieval.initialized:
        result = await retrieval.get_tools_for_query(req.query, max_tools=req.limit * 2)
        if result:
            semantic_results = result.get("selected_tools", [])

    # Fuse results
    url_scores = {}

    # Add keyword scores
    for i, url_data in enumerate(keyword_results):
        url = url_data.get("url")
        if url:
            rank_score = 1.0 / (i + 1)  # Reciprocal rank
            url_scores[url] = url_scores.get(url, 0) + rank_score * req.keyword_weight

    # Add semantic scores
    for i, tool_data in enumerate(semantic_results):
        url = tool_data.get("tool_id", "")
        score = tool_data.get("score", 0)
        url_scores[url] = url_scores.get(url, 0) + score * req.semantic_weight

    # Sort and return
    sorted_urls = sorted(url_scores.items(), key=lambda x: x[1], reverse=True)[:req.limit]

    results = []
    for url, score in sorted_urls:
        url_data = content_store.get_url(url)
        if url_data:
            url_data["fusion_score"] = round(score, 4)
            results.append(url_data)

    return {
        "results": results,
        "query": req.query,
        "method": "hybrid",
        "weights": {"keyword": req.keyword_weight, "semantic": req.semantic_weight}
    }


@app.post("/v1/extract")
async def extract_content(req: ExtractRequest):
    """
    Extract structured data from a URL/content:
    - Links (with anchor text)
    - Images (with alt text)
    - Named entities (people, orgs, dates, etc.)
    - Auto-enqueue discovered links
    """
    if not content_graph:
        raise HTTPException(503, "Content graph not initialized")
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    content = req.content
    extracted = {
        "url": req.url,
        "links": [],
        "images": [],
        "entities": [],
        "enqueued": 0
    }

    # Fetch content if not provided
    if not content:
        read_tool = None
        for t in mcp.tools:
            if "read" in t["name"].lower() or "fetch" in t["name"].lower():
                read_tool = t["name"]
                break

        if read_tool:
            result = await call_tool(mcp.session, read_tool, {"url": req.url})
            content = result
            extracted["fetched"] = True

    if not content:
        return extracted

    # Extract links using regex
    if req.extract_links:
        import re
        # Find URLs
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        found_urls = re.findall(url_pattern, content)
        unique_urls = list(set(found_urls))[:100]

        for url in unique_urls:
            link_data = {"url": url, "type": "href"}
            extracted["links"].append(link_data)
            content_graph.add_link(req.url, url, "href")

        # Auto-enqueue if requested
        if req.auto_enqueue_links and processing_queue and unique_urls:
            for url in unique_urls[:50]:
                processing_queue.enqueue(url, queue_type="url", source_query=f"extracted from {req.url}")
            extracted["enqueued"] = min(len(unique_urls), 50)

    # Extract images
    if req.extract_images:
        import re
        # Find image URLs
        img_pattern = r'https?://[^\s<>"]+\.(?:jpg|jpeg|png|gif|webp|svg)'
        found_images = re.findall(img_pattern, content, re.IGNORECASE)
        unique_images = list(set(found_images))[:50]

        for img_url in unique_images:
            img_data = {"url": img_url, "source_url": req.url}
            extracted["images"].append(img_data)

            if content_store:
                content_store.add_image(url=img_url, source_url=req.url)

    # Extract entities
    if req.extract_entities:
        import re

        entity_patterns = {
            "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            "phone": r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            "date": r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b',
            "money": r'\$\d+(?:,\d{3})*(?:\.\d{2})?|\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:USD|EUR|GBP)',
            "url": r'https?://[^\s<>"{}|\\^`\[\]]+',
        }

        for entity_type in req.entity_types:
            if entity_type in entity_patterns:
                matches = re.findall(entity_patterns[entity_type], content)
                for match in set(matches)[:20]:
                    entity_data = {"type": entity_type, "value": match}
                    extracted["entities"].append(entity_data)
                    content_graph.add_entity(req.url, entity_type, match)

    # Update fingerprint
    change_info = content_graph.update_fingerprint(req.url, content)
    extracted["content_changed"] = change_info

    # Trigger webhook if content changed
    if webhook_manager and change_info.get("changed"):
        await webhook_manager.trigger("content.changed", {
            "url": req.url,
            "change_type": change_info.get("change_type"),
            "change_count": change_info.get("change_count")
        })

    return extracted


@app.post("/v1/content/diff")
async def check_content_diff(req: ContentDiffRequest):
    """Check if content has changed since last visit"""
    if not content_graph:
        raise HTTPException(503, "Content graph not initialized")

    result = content_graph.update_fingerprint(req.url, req.content)
    return result


@app.get("/v1/graph/links")
async def get_url_links(
    url: str = Query(...),
    direction: str = Query("both", description="outlinks, inlinks, or both")
):
    """Get links to/from a URL"""
    if not content_graph:
        raise HTTPException(503, "Content graph not initialized")

    result = {"url": url}

    if direction in ["outlinks", "both"]:
        result["outlinks"] = content_graph.get_outlinks(url)

    if direction in ["inlinks", "both"]:
        result["inlinks"] = content_graph.get_inlinks(url)

    return result


@app.get("/v1/entities")
async def get_entities(
    url: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500)
):
    """Query extracted entities"""
    if not content_graph:
        raise HTTPException(503, "Content graph not initialized")

    entities = content_graph.get_entities(url=url, entity_type=entity_type, limit=limit)
    return {"entities": entities, "count": len(entities)}


# ============================================================================
# Webhooks
# ============================================================================

@app.post("/v1/webhooks")
async def register_webhook(req: WebhookRegisterRequest):
    """Register a webhook for event notifications"""
    if not webhook_manager:
        raise HTTPException(503, "Webhook manager not initialized")

    result = webhook_manager.register(
        url=req.url,
        events=req.events,
        secret=req.secret
    )
    return result


@app.get("/v1/webhooks")
async def list_webhooks():
    """List registered webhooks"""
    if not webhook_manager:
        raise HTTPException(503, "Webhook manager not initialized")

    return {"webhooks": webhook_manager.list_webhooks()}


@app.delete("/v1/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: str):
    """Remove a webhook"""
    if not webhook_manager:
        raise HTTPException(503, "Webhook manager not initialized")

    webhook_manager.unregister(webhook_id)
    return {"deleted": webhook_id}


# ============================================================================
# Smart Prioritization
# ============================================================================

@app.post("/v1/queue/smart-enqueue")
async def smart_enqueue(req: SmartEnqueueRequest):
    """
    Enqueue items with ML-based priority scoring.
    Considers: domain reputation, URL quality, query relevance.
    """
    if not processing_queue:
        raise HTTPException(503, "Processing queue not initialized")
    if not smart_prioritizer:
        raise HTTPException(503, "Smart prioritizer not initialized")

    results = []

    for item in req.items:
        url = item.get("url")
        title = item.get("title")
        description = item.get("description")

        # Compute smart priority
        if req.compute_priority:
            priority_result = await smart_prioritizer.compute_priority(
                url=url,
                title=title,
                description=description,
                source_query=req.source_query,
                domain_history=smart_prioritizer.domain_scores
            )
            priority = priority_result["priority"]
            factors = priority_result["factors"]
        else:
            priority = item.get("priority", 50)
            factors = {}

        # Enqueue with computed priority
        result = processing_queue.enqueue(
            url=url,
            queue_type=req.queue_type,
            priority=priority,
            payload={"title": title, "description": description, "factors": factors},
            source_query=req.source_query
        )
        result["priority"] = priority
        result["factors"] = factors
        results.append(result)

    return {
        "queued": len([r for r in results if r.get("status") == "queued"]),
        "items": results
    }


@app.post("/v1/feedback")
async def provide_feedback(req: FeedbackRequest):
    """
    Provide relevance feedback to improve prioritization.
    Updates domain reputation scores.
    """
    if not smart_prioritizer:
        raise HTTPException(503, "Smart prioritizer not initialized")

    from urllib.parse import urlparse
    domain = urlparse(req.url).netloc

    smart_prioritizer.update_domain_score(domain, req.relevance)

    return {
        "url": req.url,
        "domain": domain,
        "relevance": req.relevance,
        "domain_stats": smart_prioritizer.domain_scores.get(domain, {})
    }


@app.post("/v1/cluster")
async def cluster_urls(req: ClusterRequest):
    """
    Cluster URLs by semantic similarity.
    Uses embeddings to group similar content.
    """
    if not mcp:
        raise HTTPException(503, "MCP not connected")
    if not content_graph:
        raise HTTPException(503, "Content graph not initialized")

    # Get embeddings for all URLs (using their titles/descriptions)
    texts = []
    for url in req.urls:
        url_data = content_store.get_url(url) if content_store else None
        if url_data:
            text = f"{url_data.get('title', '')} {url_data.get('description', '')} {url}"
        else:
            text = url
        texts.append(text)

    # Embed all texts
    embed_result = await call_tool(mcp.session, "generate_embeddings", {
        "inputs": texts,
        "model": "jina-embeddings-v3",
        "task": "retrieval.passage",
        "dimensions": 256,
        "normalize": True
    })

    try:
        data = json.loads(embed_result) if isinstance(embed_result, str) else embed_result
        embeddings = [item["embedding"] for item in data["data"]]
    except:
        raise HTTPException(500, "Failed to generate embeddings")

    # Simple k-means clustering
    import numpy as np
    embeddings_np = np.array(embeddings)

    # Initialize centroids randomly
    n_clusters = min(req.num_clusters, len(req.urls))
    indices = np.random.choice(len(embeddings_np), n_clusters, replace=False)
    centroids = embeddings_np[indices]

    # Run k-means iterations
    for _ in range(10):
        # Assign to nearest centroid
        distances = np.linalg.norm(embeddings_np[:, np.newaxis] - centroids, axis=2)
        assignments = np.argmin(distances, axis=1)

        # Update centroids
        new_centroids = []
        for i in range(n_clusters):
            mask = assignments == i
            if np.any(mask):
                new_centroids.append(embeddings_np[mask].mean(axis=0))
            else:
                new_centroids.append(centroids[i])
        centroids = np.array(new_centroids)

    # Create results
    clusters = {}
    for i, (url, assignment) in enumerate(zip(req.urls, assignments)):
        cluster_id = f"cluster_{assignment}"
        label = req.labels[assignment] if req.labels and assignment < len(req.labels) else cluster_id

        # Compute similarity to centroid
        similarity = float(np.dot(embeddings_np[i], centroids[assignment]))

        if cluster_id not in clusters:
            clusters[cluster_id] = {"label": label, "urls": []}

        clusters[cluster_id]["urls"].append({
            "url": url,
            "similarity": round(similarity, 4)
        })

        # Store in graph
        content_graph.set_cluster(url, cluster_id, label, similarity)

    return {
        "num_clusters": n_clusters,
        "clusters": clusters
    }


@app.post("/v1/web/search")
async def web_search(req: WebSearchRequest):
    """Search the web using Jina"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Find web search tool
    search_tool = None
    for t in mcp.tools:
        if "search" in t["name"].lower() and "web" in t.get("description", "").lower():
            search_tool = t["name"]
            break

    if not search_tool:
        # Try common names
        for name in ["search_the_web", "web_search", "search"]:
            if any(t["name"] == name for t in mcp.tools):
                search_tool = name
                break

    if not search_tool:
        raise HTTPException(400, "No web search tool available")

    result = await call_tool(mcp.session, search_tool, {
        "query": req.query
    })

    try:
        return json.loads(result)
    except:
        return {"raw": result}


@app.post("/v1/web/read")
async def web_read(req: WebReadRequest):
    """Read and extract content from URL"""
    if not mcp:
        raise HTTPException(503, "MCP not connected")

    # Find read tool
    read_tool = None
    for t in mcp.tools:
        name = t["name"].lower()
        if "read" in name or "fetch" in name or "extract" in name:
            read_tool = t["name"]
            break

    if not read_tool:
        raise HTTPException(400, "No web read tool available")

    args = {"url": req.url}
    if req.target_selector:
        args["target_selector"] = req.target_selector
    if req.wait_for:
        args["wait_for"] = req.wait_for

    result = await call_tool(mcp.session, read_tool, args)

    try:
        return json.loads(result)
    except:
        return {"content": result}


@app.post("/v1/interact")
async def interact(req: InteractRequest):
    """
    Minimal interaction endpoint focused on search tasks.
    Streams SSE events for tool calls and responses.
    """
    if not mcp:
        raise HTTPException(503, "MCP not connected")
    if not http_client:
        raise HTTPException(503, "HTTP client not ready")

    interaction_id = f"int_{uuid.uuid4().hex[:12]}"

    return StreamingResponse(
        _stream_interaction(interaction_id, req),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Interaction-ID": interaction_id
        }
    )


async def _stream_interaction(
    interaction_id: str,
    req: InteractRequest
) -> AsyncGenerator[str, None]:
    """Stream interaction with search tools"""

    seq = 0

    def sse(event: str, data: Dict) -> str:
        nonlocal seq
        payload = {
            "event": event,
            "data": data,
            "id": interaction_id,
            "seq": seq,
            "ts": datetime.utcnow().isoformat() + "Z"
        }
        seq += 1
        return f"event: {event}\ndata: {json.dumps(payload)}\n\n"

    # Select tools - use retrieval if available
    if retrieval and retrieval.initialized and req.tools_enabled:
        result = await retrieval.get_tools_for_query(req.message, max_tools=10)
        if result:
            tools = []
            tool_map = {}
            for t in result["selected_tools"]:
                schema = t.get("schema", {})
                name = t.get("tool_name", t.get("tool_id", "").split(":")[-1])
                tools.append({
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": schema.get("description", ""),
                        "parameters": schema.get("parameters", schema.get("input_schema", {}))
                    }
                })
                tool_map[name] = name

            yield sse("tools.selected", {
                "count": len(tools),
                "names": [t["function"]["name"] for t in tools]
            })
        else:
            tools = mcp.openai_tools
            tool_map = mcp.tool_map
    else:
        tools = mcp.openai_tools if req.tools_enabled else None
        tool_map = mcp.tool_map if req.tools_enabled else {}

    messages = [{"role": "user", "content": req.message}]

    # Stream from LLM
    yield sse("message.start", {"model": req.model})

    max_iterations = 10
    iteration = 0

    while iteration < max_iterations:
        iteration += 1

        payload = {
            "model": req.model,
            "messages": messages,
            "stream": True,
            "temperature": req.temperature,
            "max_tokens": req.max_tokens
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        full_response = ""
        tool_calls = {}

        async with http_client.stream(
            "POST",
            "https://openrouter.ai/api/v1/chat/completions",
            json=payload
        ) as resp:
            if resp.status_code >= 400:
                body = await resp.aread()
                yield sse("error", {"status": resp.status_code, "body": body.decode()})
                return

            async for line in resp.aiter_lines():
                if not line or not line.startswith("data: "):
                    continue

                data = line[6:].strip()
                if data == "[DONE]":
                    break

                try:
                    obj = json.loads(data)
                except:
                    continue

                choices = obj.get("choices", [])
                if not choices:
                    continue

                delta = choices[0].get("delta", {})

                if "content" in delta and delta["content"]:
                    full_response += delta["content"]
                    yield sse("message.delta", {"delta": delta["content"]})

                for tc in delta.get("tool_calls", []):
                    idx = tc.get("index", 0)
                    if idx not in tool_calls:
                        tool_calls[idx] = {
                            "id": tc.get("id"),
                            "function": {"name": "", "arguments": ""}
                        }
                    fn = tc.get("function", {})
                    if fn.get("name"):
                        tool_calls[idx]["function"]["name"] = fn["name"]
                    if "arguments" in fn:
                        tool_calls[idx]["function"]["arguments"] += fn["arguments"]

        yield sse("message.complete", {"text": full_response, "has_tools": bool(tool_calls)})

        # Add assistant message
        assistant_msg = {"role": "assistant", "content": full_response}
        if tool_calls:
            assistant_msg["tool_calls"] = [
                {"id": tc["id"], "type": "function", "function": tc["function"]}
                for tc in [tool_calls[i] for i in sorted(tool_calls.keys())]
            ]
        messages.append(assistant_msg)

        # Execute tool calls
        if tool_calls:
            for idx in sorted(tool_calls.keys()):
                tc = tool_calls[idx]
                fn_name = tc["function"]["name"]
                fn_args_str = tc["function"]["arguments"]

                try:
                    fn_args = json.loads(fn_args_str)
                except:
                    fn_args = {}

                original_name = tool_map.get(fn_name, fn_name)

                yield sse("tool.start", {"name": fn_name, "args": fn_args})

                result = await call_tool(mcp.session, original_name, fn_args)

                yield sse("tool.complete", {
                    "name": fn_name,
                    "result_preview": result[:500] if len(result) > 500 else result
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result
                })

            continue  # Get next response with tool results

        break  # No tool calls, we're done

    yield sse("done", {"iterations": iteration})


# ============================================================================
# Advanced Feature Endpoints
# ============================================================================

@app.post("/v1/search/advanced")
async def advanced_search(req: AdvancedSearchRequest):
    """
    Advanced search with all features: query expansion, caching,
    PageRank, temporal decay, and personalization.
    """
    start_time = time.time()

    # Check cache first
    if req.use_cache and semantic_cache:
        cached = await semantic_cache.get(req.query, {'top_k': req.top_k})
        if cached:
            return {"results": cached, "cached": True, "latency_ms": 0}

    # Expand queries if enabled
    queries = [req.query]
    if req.expand_queries and query_expander:
        queries = query_expander.expand_synonyms(req.query, max_expansions=3)

    # Execute search (using multi-query fusion if multiple queries)
    results = []
    if len(queries) > 1 and multi_query_fusion:
        # Use direct API for search
        async def search_one(q: str, top_k: int = 10):
            if jina_direct:
                try:
                    resp = await jina_direct.search_web(q)
                    return resp.get('results', [])[:top_k]
                except Exception:
                    return []
            return []

        results = await multi_query_fusion.multi_query_search(queries, search_one, req.top_k)
    else:
        # Single query search
        if jina_direct:
            try:
                resp = await jina_direct.search_web(req.query)
                results = resp.get('results', [])[:req.top_k]
            except Exception as e:
                results = []

    # Apply PageRank boost
    if req.use_pagerank and pagerank_scorer and results:
        for r in results:
            url = r.get('url', '')
            if url:
                pr_score = pagerank_scorer.get_score(url)
                r['pagerank'] = pr_score
                r['score'] = r.get('score', 1.0) * (1 + pr_score * 0.5)

    # Apply temporal decay
    if req.use_temporal_decay and temporal_scorer and results:
        for r in results:
            if 'published' in r or 'date' in r:
                timestamp = r.get('published') or r.get('date')
                if timestamp:
                    temporal_mult = temporal_scorer.combined_temporal_score(timestamp)
                    r['temporal_score'] = temporal_mult
                    r['score'] = r.get('score', 1.0) * temporal_mult

    # Apply personalization boost
    if req.user_id and user_profile and results:
        for r in results:
            url = r.get('url', '')
            if url:
                pers_boost = user_profile.get_personalization_boost(req.user_id, url)
                r['personalization_boost'] = pers_boost
                r['score'] = r.get('score', 1.0) * pers_boost

    # Sort by final score
    results.sort(key=lambda x: x.get('score', 0), reverse=True)
    results = results[:req.top_k]

    # Cache result
    if req.use_cache and semantic_cache:
        semantic_cache.set(req.query, results, params={'top_k': req.top_k})

    # Log to analytics
    latency = (time.time() - start_time) * 1000
    if search_analytics:
        search_analytics.log_event(
            'search',
            query=req.query,
            results_count=len(results),
            latency_ms=latency,
            user_id=req.user_id,
            session_id=req.session_id
        )

    # Add to session if provided
    if req.session_id and search_session:
        search_session.add_query(req.session_id, req.query, len(results))

    return {
        "query": req.query,
        "expanded_queries": queries if len(queries) > 1 else None,
        "results": results,
        "cached": False,
        "latency_ms": latency
    }


@app.post("/v1/rag")
async def rag_query(req: RAGQueryRequest):
    """Retrieval-Augmented Generation - retrieve context for your query"""
    if not rag_pipeline:
        raise HTTPException(500, "RAG pipeline not initialized")

    result = await rag_pipeline.rag_query(
        query=req.query,
        top_k=req.top_k,
        include_sources=req.include_sources
    )

    return result


@app.post("/v1/search/multi")
async def multi_query_search(req: MultiQuerySearchRequest):
    """Execute multiple queries and fuse results using RRF"""
    if not multi_query_fusion:
        raise HTTPException(500, "Multi-query fusion not initialized")

    queries = req.queries

    # Expand queries if requested
    if req.expand_queries and query_expander:
        expanded = []
        for q in queries:
            expanded.extend(query_expander.expand_synonyms(q, max_expansions=2))
        queries = list(set(expanded))  # Dedupe

    async def search_func(q: str, top_k: int = 10):
        if jina_direct:
            try:
                resp = await jina_direct.search_web(q)
                return resp.get('results', [])[:top_k]
            except Exception:
                return []
        return []

    results = await multi_query_fusion.multi_query_search(queries, search_func, req.top_k)

    return {
        "queries_executed": queries,
        "results": results
    }


@app.post("/v1/session")
async def create_session(req: SessionCreateRequest):
    """Create a new search session"""
    if not search_session:
        raise HTTPException(500, "Search sessions not initialized")

    session_id = search_session.create_session(req.user_id)
    return {"session_id": session_id}


@app.post("/v1/session/query")
async def add_session_query(req: SessionQueryRequest):
    """Add a query to a session"""
    if not search_session:
        raise HTTPException(500, "Search sessions not initialized")

    search_session.add_query(
        req.session_id,
        req.query,
        req.results_count,
        req.clicked
    )
    return {"status": "added"}


@app.get("/v1/session/{session_id}")
async def get_session(session_id: str):
    """Get session information and context"""
    if not search_session:
        raise HTTPException(500, "Search sessions not initialized")

    stats = search_session.get_session_stats(session_id)
    context = search_session.get_session_context(session_id)

    return {
        "stats": stats,
        "recent_queries": context
    }


@app.post("/v1/user/interaction")
async def record_interaction(req: UserInteractionRequest):
    """Record user interaction for personalization"""
    if not user_profile:
        raise HTTPException(500, "User profiles not initialized")

    user_profile.record_interaction(
        req.user_id,
        req.url,
        req.interaction_type,
        req.score
    )
    return {"status": "recorded"}


@app.get("/v1/user/{user_id}")
async def get_user_profile(user_id: str):
    """Get user profile with affinities"""
    if not user_profile:
        raise HTTPException(500, "User profiles not initialized")

    profile = user_profile.get_or_create(user_id)
    return profile


@app.post("/v1/deduplicate")
async def check_duplicate(req: DeduplicateRequest):
    """Check if content is a near-duplicate of existing content"""
    if not simhash_dedup:
        raise HTTPException(500, "Deduplication not initialized")

    result = simhash_dedup.add_document(req.url, req.text)
    return result


@app.get("/v1/pagerank")
async def get_pagerank(top_n: int = Query(default=20, ge=1, le=100)):
    """Get top URLs by PageRank score"""
    if not pagerank_scorer:
        raise HTTPException(500, "PageRank not initialized")

    # Force recompute if needed
    pagerank_scorer.compute()
    top_urls = pagerank_scorer.get_top_urls(top_n)

    return {
        "top_urls": [{"url": url, "score": score} for url, score in top_urls]
    }


@app.get("/v1/analytics/stats")
async def get_analytics_stats(hours: int = Query(default=24, ge=1, le=168)):
    """Get search analytics for the last N hours"""
    if not search_analytics:
        raise HTTPException(500, "Analytics not initialized")

    stats = search_analytics.get_query_stats(hours)
    top_queries = search_analytics.get_top_queries(limit=10, hours=hours)
    zero_results = search_analytics.get_zero_result_queries(limit=10, hours=hours)

    return {
        "stats": stats,
        "top_queries": top_queries,
        "zero_result_queries": zero_results
    }


@app.post("/v1/ab-test")
async def create_ab_test(req: ABTestCreateRequest):
    """Create a new A/B test"""
    if not search_analytics:
        raise HTTPException(500, "Analytics not initialized")

    # Validate traffic split sums to 1.0
    if abs(sum(req.traffic_split) - 1.0) > 0.01:
        raise HTTPException(400, "Traffic split must sum to 1.0")

    if len(req.variants) != len(req.traffic_split):
        raise HTTPException(400, "Number of variants must match traffic split")

    test_id = search_analytics.create_ab_test(
        req.test_name,
        req.variants,
        req.traffic_split
    )

    return {"test_id": test_id}


@app.post("/v1/ab-test/variant")
async def get_ab_variant(req: ABTestVariantRequest):
    """Get A/B test variant for a user"""
    if not search_analytics:
        raise HTTPException(500, "Analytics not initialized")

    variant = search_analytics.get_ab_variant(req.test_id, req.user_id)
    if not variant:
        raise HTTPException(404, "Test not found or inactive")

    return {"variant": variant}


@app.get("/v1/ab-test/{test_id}/results")
async def get_ab_results(test_id: str):
    """Get A/B test results"""
    if not search_analytics:
        raise HTTPException(500, "Analytics not initialized")

    results = search_analytics.get_ab_results(test_id)
    return results


@app.get("/v1/cache/stats")
async def get_cache_stats():
    """Get semantic cache statistics"""
    if not semantic_cache:
        raise HTTPException(500, "Cache not initialized")

    return semantic_cache.stats()


@app.post("/v1/cache/invalidate")
async def invalidate_cache(req: CacheInvalidateRequest):
    """Invalidate cache entries"""
    if not semantic_cache:
        raise HTTPException(500, "Cache not initialized")

    semantic_cache.invalidate(req.pattern)
    return {"status": "invalidated", "pattern": req.pattern}


@app.post("/v1/embed/direct")
async def embed_direct(req: EmbedRequest):
    """Generate embeddings using direct Jina API (bypasses MCP)"""
    if not jina_direct:
        raise HTTPException(500, "Direct API not initialized")

    try:
        result = await jina_direct.embed(
            inputs=req.inputs,
            model=req.model,
            task=req.task,
            dimensions=req.dimensions
        )
        return result
    except Exception as e:
        raise HTTPException(500, f"Embedding failed: {str(e)}")


@app.post("/v1/rerank/direct")
async def rerank_direct(req: RerankRequest):
    """Rerank documents using direct Jina API (bypasses MCP)"""
    if not jina_direct:
        raise HTTPException(500, "Direct API not initialized")

    try:
        result = await jina_direct.rerank(
            query=req.query,
            documents=req.documents,
            top_n=req.top_n
        )
        return result
    except Exception as e:
        raise HTTPException(500, f"Reranking failed: {str(e)}")


@app.get("/v1/query/expand")
async def expand_query(
    query: str = Query(..., min_length=1),
    max_expansions: int = Query(default=5, ge=1, le=10)
):
    """Expand query with synonyms and variations"""
    if not query_expander:
        raise HTTPException(500, "Query expander not initialized")

    synonyms = query_expander.expand_synonyms(query, max_expansions)
    entities = query_expander.extract_entities(query)
    semantic = await query_expander.expand_semantic(query, max_expansions)

    return {
        "original": query,
        "synonym_expansions": synonyms,
        "semantic_variations": semantic,
        "extracted_entities": entities
    }


# ============================================================================
# Ultra-Advanced Search Endpoints
# ============================================================================

@app.post("/v1/search/ultra")
async def ultra_search(req: UltraSearchRequest):
    """
    Ultimate search endpoint combining all ultra-advanced features:
    - Intent classification
    - Spell correction
    - Query expansion
    - Learning to Rank
    - MMR diversification
    - Personalization
    - Explainable results
    - Conversational context
    """
    pipeline_steps = []
    corrected_query = req.query
    intent_info = None
    explanations = []

    # Step 1: Spell correction
    if req.use_spell_correction and spell_corrector:
        correction = spell_corrector.correct_query(req.query)
        if correction["was_corrected"]:
            corrected_query = correction["corrected"]
            pipeline_steps.append({
                "step": "spell_correction",
                "original": req.query,
                "corrected": corrected_query,
                "corrections": correction["corrections"]
            })

    # Step 2: Intent classification
    if req.use_intent_detection and intent_classifier:
        intent_info = intent_classifier.classify(corrected_query)
        strategy = intent_classifier.get_search_strategy(intent_info["primary_intent"])
        pipeline_steps.append({
            "step": "intent_classification",
            "intent": intent_info["primary_intent"],
            "confidence": intent_info["confidence"],
            "strategy": strategy
        })

    # Step 3: Conversational context expansion
    expanded_query = corrected_query
    if req.conversation_id and conversational_search:
        follow_up = conversational_search.detect_follow_up(req.conversation_id, corrected_query)
        if follow_up["is_follow_up"]:
            expanded_query = conversational_search.expand_query_with_context(
                req.conversation_id, corrected_query
            )
            pipeline_steps.append({
                "step": "conversational_expansion",
                "original": corrected_query,
                "expanded": expanded_query,
                "context_used": follow_up["context_references"]
            })

    # Step 4: Query expansion with synonyms/semantic
    if req.use_query_expansion and query_expander:
        synonyms = query_expander.expand_synonyms(expanded_query, max_expansions=3)
        if synonyms:
            pipeline_steps.append({
                "step": "query_expansion",
                "expansions": synonyms[:3]
            })

    # Step 5: Perform semantic search (using MCP or direct API)
    search_results = []
    if mcp:
        try:
            result = await call_tool(mcp.session, "web_search", {"query": expanded_query})
            if isinstance(result, dict) and "results" in result:
                search_results = result["results"][:20]  # Get top 20 for reranking
            elif isinstance(result, list):
                search_results = result[:20]
        except Exception as e:
            pipeline_steps.append({"step": "search", "error": str(e)})

    # Step 6: Learning to Rank reranking
    if req.use_ltr and learning_to_rank and search_results:
        # Convert to format expected by LTR
        docs = []
        for r in search_results:
            docs.append({
                "id": r.get("url", r.get("id", "")),
                "title": r.get("title", ""),
                "content": r.get("content", r.get("snippet", "")),
                "url": r.get("url", ""),
                "score": r.get("score", 0.5)
            })

        reranked = learning_to_rank.rerank(expanded_query, docs)
        search_results = reranked
        pipeline_steps.append({
            "step": "learning_to_rank",
            "reranked_count": len(reranked)
        })

    # Step 7: MMR diversification
    if req.use_mmr and mmr_diversifier and search_results and jina_direct:
        try:
            # Get query embedding
            embed_result = await jina_direct.embed(
                inputs=[expanded_query],
                task="retrieval.query",
                dimensions=1024
            )
            query_embedding = embed_result["data"][0]["embedding"]

            # Get document embeddings
            doc_texts = [r.get("content", r.get("snippet", r.get("title", ""))) for r in search_results]
            doc_embed_result = await jina_direct.embed(
                inputs=doc_texts[:10],  # Limit for API efficiency
                task="retrieval.passage",
                dimensions=1024
            )

            candidates = []
            for i, emb in enumerate(doc_embed_result["data"]):
                candidates.append({
                    "id": search_results[i].get("url", str(i)),
                    "embedding": emb["embedding"],
                    "doc": search_results[i]
                })

            diversified = mmr_diversifier.diversify(
                query_embedding=query_embedding,
                candidates=candidates,
                top_k=min(10, len(candidates)),
                lambda_param=req.lambda_diversity
            )

            search_results = [d["doc"] for d in diversified]
            pipeline_steps.append({
                "step": "mmr_diversification",
                "lambda": req.lambda_diversity,
                "output_count": len(diversified)
            })
        except Exception as e:
            pipeline_steps.append({"step": "mmr_diversification", "error": str(e)})

    # Step 8: Personalization boost
    if req.use_personalization and user_profile and req.user_id:
        profile = user_profile.get_profile(req.user_id)
        if profile["affinities"]:
            # Boost results matching user affinities
            for r in search_results:
                content = r.get("content", r.get("snippet", "")).lower()
                for affinity, weight in profile["affinities"].items():
                    if affinity.lower() in content:
                        r["score"] = r.get("score", 0.5) * (1 + weight * 0.2)

            search_results = sorted(search_results, key=lambda x: x.get("score", 0), reverse=True)
            pipeline_steps.append({
                "step": "personalization",
                "affinities_applied": list(profile["affinities"].keys())[:5]
            })

    # Step 9: Generate explanations
    if req.use_explanations and explainable_search and search_results:
        for r in search_results[:5]:  # Explain top 5
            explanation = explainable_search.explain_result(r, expanded_query)
            explanations.append(explanation)
        pipeline_steps.append({
            "step": "explanations_generated",
            "count": len(explanations)
        })

    # Step 10: Update conversational context
    if req.conversation_id and conversational_search:
        conversational_search.add_turn(
            req.conversation_id,
            role="user",
            content=corrected_query,
            metadata={"results_count": len(search_results), "intent": intent_info.get("intent") if intent_info else None}
        )

    # Track for autocomplete
    if auto_complete:
        auto_complete.add_query(corrected_query)

    return {
        "query": req.query,
        "processed_query": expanded_query,
        "intent": intent_info,
        "results": search_results[:10],
        "explanations": explanations if req.use_explanations else None,
        "pipeline": pipeline_steps,
        "conversation_id": req.conversation_id
    }


@app.get("/v1/intent")
async def classify_intent(query: str = Query(..., min_length=1)):
    """Classify query intent (navigational, informational, transactional, conversational, code)"""
    if not intent_classifier:
        raise HTTPException(500, "Intent classifier not initialized")

    intent_info = intent_classifier.classify(query)
    strategy = intent_classifier.get_search_strategy(intent_info["primary_intent"])

    return {
        "query": query,
        "intent": intent_info["primary_intent"],
        "confidence": intent_info["confidence"],
        "all_scores": intent_info["all_scores"],
        "recommended_strategy": strategy
    }


@app.get("/v1/spell-check")
async def spell_check(query: str = Query(..., min_length=1)):
    """Check and correct spelling in query"""
    if not spell_corrector:
        raise HTTPException(500, "Spell corrector not initialized")

    result = spell_corrector.correct_query(query)
    return result


@app.get("/v1/autocomplete")
async def autocomplete(
    prefix: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=50)
):
    """Get autocomplete suggestions for a prefix"""
    if not auto_complete:
        raise HTTPException(500, "Autocomplete not initialized")

    suggestions = auto_complete.suggest(prefix, limit)
    return {
        "prefix": prefix,
        "suggestions": suggestions
    }


@app.get("/v1/autocomplete/trending")
async def get_trending_queries(
    limit: int = Query(default=10, ge=1, le=100),
    hours: int = Query(default=24, ge=1, le=168)
):
    """Get trending queries from the past N hours"""
    if not auto_complete:
        raise HTTPException(500, "Autocomplete not initialized")

    trending = auto_complete.get_trending(limit, hours)
    return {
        "period_hours": hours,
        "trending": trending
    }


class VectorAddRequest(BaseModel):
    doc_id: str
    vector: List[float]
    metadata: Optional[Dict[str, Any]] = None


class VectorSearchRequest(BaseModel):
    query_vector: List[float]
    top_k: int = 10


@app.post("/v1/vector/add")
async def add_vector(req: VectorAddRequest):
    """Add a vector to the in-memory vector store"""
    if not vector_store:
        raise HTTPException(500, "Vector store not initialized")

    vector_store.add(req.doc_id, req.vector, req.metadata)
    return {"status": "added", "doc_id": req.doc_id}


@app.post("/v1/vector/search")
async def search_vectors(req: VectorSearchRequest):
    """Search for similar vectors using approximate nearest neighbor"""
    if not vector_store:
        raise HTTPException(500, "Vector store not initialized")

    results = vector_store.search(req.query_vector, req.top_k)
    return {
        "results": results,
        "count": len(results)
    }


@app.get("/v1/vector/stats")
async def vector_stats():
    """Get vector store statistics"""
    if not vector_store:
        raise HTTPException(500, "Vector store not initialized")

    return {
        "total_vectors": len(vector_store.vectors),
        "dimension": vector_store.dimension,
        "num_tables": vector_store.num_tables
    }


class MMRDiversifyRequest(BaseModel):
    query_embedding: List[float]
    candidates: List[Dict[str, Any]]  # Each with 'id', 'embedding', optional 'doc'
    top_k: int = 10
    lambda_param: float = 0.7


@app.post("/v1/diversify")
async def diversify_results(req: MMRDiversifyRequest):
    """Apply Maximal Marginal Relevance diversification"""
    if not mmr_diversifier:
        raise HTTPException(500, "MMR diversifier not initialized")

    diversified = mmr_diversifier.diversify(
        query_embedding=req.query_embedding,
        candidates=req.candidates,
        top_k=req.top_k,
        lambda_param=req.lambda_param
    )

    return {
        "diversified": diversified,
        "count": len(diversified),
        "lambda": req.lambda_param
    }


class LTRFeedbackRequest(BaseModel):
    query: str
    doc_id: str
    dwell_time_ms: int


@app.post("/v1/ltr/feedback")
async def record_ltr_feedback(req: LTRFeedbackRequest):
    """Record click/dwell feedback for Learning to Rank"""
    if not learning_to_rank:
        raise HTTPException(500, "Learning to Rank not initialized")

    learning_to_rank.record_click(req.query, req.doc_id, req.dwell_time_ms)
    return {"status": "recorded", "query": req.query, "doc_id": req.doc_id}


@app.post("/v1/ltr/learn")
async def trigger_ltr_learning(learning_rate: float = Query(default=0.01, ge=0.001, le=0.1)):
    """Trigger a learning round from accumulated feedback"""
    if not learning_to_rank:
        raise HTTPException(500, "Learning to Rank not initialized")

    learning_to_rank.learn_from_feedback(learning_rate)
    return {"status": "learning_complete", "learning_rate": learning_rate}


@app.get("/v1/ltr/weights")
async def get_ltr_weights():
    """Get current Learning to Rank feature weights"""
    if not learning_to_rank:
        raise HTTPException(500, "Learning to Rank not initialized")

    return {
        "weights": learning_to_rank.weights,
        "feedback_count": len(learning_to_rank.click_data)
    }


@app.post("/v1/conversation/start")
async def start_conversation(user_id: str = Query(default="anonymous")):
    """Start a new conversational search session"""
    if not conversational_search:
        raise HTTPException(500, "Conversational search not initialized")

    conversation_id = conversational_search.start_conversation(user_id)
    return {
        "conversation_id": conversation_id,
        "user_id": user_id,
        "message": "Conversation started. Use this ID in /v1/search/ultra for context-aware search."
    }


@app.get("/v1/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get conversation history and context"""
    if not conversational_search:
        raise HTTPException(500, "Conversational search not initialized")

    history = conversational_search.get_conversation_history(conversation_id)
    if not history:
        raise HTTPException(404, "Conversation not found")

    return {
        "conversation_id": conversation_id,
        "turns": history,
        "turn_count": len(history)
    }


@app.post("/v1/conversation/{conversation_id}/end")
async def end_conversation(conversation_id: str):
    """End a conversational search session"""
    if not conversational_search:
        raise HTTPException(500, "Conversational search not initialized")

    conversational_search.end_conversation(conversation_id)
    return {"status": "ended", "conversation_id": conversation_id}


class ExplainRequest(BaseModel):
    query: str
    result: Dict[str, Any]


@app.post("/v1/explain")
async def explain_result(req: ExplainRequest):
    """Generate explanation for why a result matched"""
    if not explainable_search:
        raise HTTPException(500, "Explainable search not initialized")

    explanation = explainable_search.explain_result(req.result, req.query)
    return explanation


@app.post("/v1/route")
async def route_query(query: str = Query(..., min_length=1)):
    """Route query to specialized handler based on semantics"""
    if not semantic_router:
        raise HTTPException(500, "Semantic router not initialized")

    route_result = await semantic_router.route(query)
    return route_result


@app.get("/v1/routes")
async def list_routes():
    """List all registered semantic routes"""
    if not semantic_router:
        raise HTTPException(500, "Semantic router not initialized")

    routes = []
    for name, route in semantic_router.routes.items():
        routes.append({
            "name": name,
            "description": route["description"],
            "examples": route["examples"][:3],
            "metadata": route["metadata"]
        })

    return {"routes": routes, "count": len(routes)}


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 78)
    print("JINA SEARCH SERVER v4.0 - Ultra-Advanced ML Search Engine")
    print("=" * 78)
    print()
    print("SEARCH")
    print("  POST /v1/search          - Semantic search over tools")
    print("  POST /v1/search/hybrid   - Combined keyword + semantic search")
    print("  POST /v1/search/advanced - Full-featured search (cache, PageRank, personalization)")
    print("  POST /v1/search/multi    - Multi-query search with RRF fusion")
    print("  POST /v1/search/ultra    - ULTIMATE search (intent, spell, LTR, MMR, explanations)")
    print("  POST /v1/web/search      - Web search")
    print("  POST /v1/web/read        - Extract web content")
    print()
    print("ULTRA-ADVANCED ML")
    print("  GET  /v1/intent          - Classify query intent (nav/info/trans/conv/code)")
    print("  GET  /v1/spell-check     - Spell correction with edit distance")
    print("  GET  /v1/autocomplete    - Query autocomplete (prefix trie)")
    print("  GET  /v1/autocomplete/trending - Trending queries")
    print("  POST /v1/diversify       - MMR diversification (relevance vs diversity)")
    print("  POST /v1/explain         - Explainable search (why did this match?)")
    print("  POST /v1/route           - Semantic query routing")
    print("  GET  /v1/routes          - List semantic routes")
    print()
    print("LEARNING TO RANK")
    print("  POST /v1/ltr/feedback    - Record click/dwell feedback")
    print("  POST /v1/ltr/learn       - Trigger learning round")
    print("  GET  /v1/ltr/weights     - Get current feature weights")
    print()
    print("CONVERSATIONAL SEARCH")
    print("  POST /v1/conversation/start      - Start conversation session")
    print("  GET  /v1/conversation/{id}       - Get conversation history")
    print("  POST /v1/conversation/{id}/end   - End conversation")
    print()
    print("VECTOR STORE (ANN)")
    print("  POST /v1/vector/add      - Add vector to store")
    print("  POST /v1/vector/search   - Approximate nearest neighbor search")
    print("  GET  /v1/vector/stats    - Vector store statistics")
    print()
    print("TEXT PROCESSING")
    print("  POST /v1/embed           - Generate embeddings (via MCP)")
    print("  POST /v1/embed/direct    - Generate embeddings (direct API)")
    print("  POST /v1/rerank          - Rerank documents (via MCP)")
    print("  POST /v1/rerank/direct   - Rerank documents (direct API)")
    print("  POST /v1/segment         - Segment text into chunks")
    print("  POST /v1/classify        - Classify text into categories")
    print()
    print("RAG & RETRIEVAL")
    print("  POST /v1/rag             - RAG query (retrieve context for LLM)")
    print("  GET  /v1/query/expand    - Expand query with synonyms/variations")
    print("  POST /v1/deduplicate     - Check content for near-duplicates (SimHash)")
    print()
    print("EXTRACTION & GRAPH")
    print("  POST /v1/extract         - Extract links, images, entities from content")
    print("  POST /v1/content/diff    - Detect content changes (fingerprinting)")
    print("  GET  /v1/graph/links     - Get URL link graph (outlinks/inlinks)")
    print("  GET  /v1/entities        - Query extracted entities")
    print("  POST /v1/cluster         - Cluster URLs by semantic similarity")
    print("  GET  /v1/pagerank        - Get top URLs by PageRank score")
    print()
    print("AGENT MEMORY")
    print("  POST /v1/urls            - Add URLs to tracking")
    print("  GET  /v1/urls/page       - Query URLs (cursor pagination)")
    print("  PUT  /v1/urls/visited    - Mark URL visited + store summary")
    print("  GET  /v1/urls/unvisited  - Get URLs to explore")
    print("  POST /v1/images          - Add images to tracking")
    print("  GET  /v1/images/page     - Query images (cursor pagination)")
    print()
    print("PROCESSING QUEUE")
    print("  POST /v1/queue/enqueue       - Add items to queue")
    print("  POST /v1/queue/smart-enqueue - ML-prioritized enqueue")
    print("  POST /v1/queue/claim         - Claim batch for processing")
    print("  POST /v1/queue/complete      - Mark items done/failed")
    print("  GET  /v1/queue/peek          - Preview pending items")
    print("  POST /v1/queue/retry         - Retry failed items")
    print()
    print("SESSIONS & PERSONALIZATION")
    print("  POST /v1/session         - Create search session")
    print("  POST /v1/session/query   - Add query to session")
    print("  GET  /v1/session/{id}    - Get session context")
    print("  POST /v1/user/interaction - Record interaction (click, bookmark, etc)")
    print("  GET  /v1/user/{id}       - Get user profile with affinities")
    print()
    print("ANALYTICS & A/B TESTING")
    print("  GET  /v1/analytics/stats     - Search analytics (queries, latency)")
    print("  POST /v1/ab-test             - Create A/B test")
    print("  POST /v1/ab-test/variant     - Get variant for user")
    print("  GET  /v1/ab-test/{id}/results - Get A/B test results")
    print()
    print("CACHING")
    print("  GET  /v1/cache/stats      - Semantic cache statistics")
    print("  POST /v1/cache/invalidate - Invalidate cache entries")
    print()
    print("LEARNING & WEBHOOKS")
    print("  POST /v1/feedback        - Relevance feedback (improves priority)")
    print("  POST /v1/webhooks        - Register webhook for events")
    print("  GET  /v1/webhooks        - List webhooks")
    print()
    print("AGENT")
    print("  POST /v1/interact        - Chat with search tools (SSE)")
    print("  GET  /v1/tools           - List available MCP tools")
    print()
    print("Environment:")
    print(f"  JINA_API_KEY: {'set' if JINA_API_KEY else 'MISSING'}")
    print(f"  OPENROUTER_API_KEY: {'set' if OPENROUTER_API_KEY else 'MISSING'}")
    print()
    print("=" * 78)
    print()

    uvicorn.run(
        "search_server:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )
