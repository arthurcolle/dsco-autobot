#!/usr/bin/env python3
"""
Distributed Document Q&A MCP

Provides text extraction, table/figure detection, semantic search, and Q&A over PDFs.
Wraps the pdf_to processing pipeline.

Run with:
    python -m native.distributed_document_qa.server --port 4006

Environment:
    DOCUMENT_QA_PATH  Path to the pdf_to source directory
    DOCUMENT_DATA_DIR Where processed document data is stored (default: document_data)
    OPENAI_API_KEY    Required for embeddings and vision
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase, CapabilityType

logger = logging.getLogger(__name__)

_DOC_QA_DEFAULT = str(
    Path.home()
    / "Library/Mobile Documents/com~apple~CloudDocs"
    / "Untitled Folder/NewMachine/what/pdf_to"
)
DOCUMENT_QA_PATH = os.environ.get("DOCUMENT_QA_PATH", _DOC_QA_DEFAULT)

if DOCUMENT_QA_PATH not in sys.path:
    sys.path.insert(0, DOCUMENT_QA_PATH)

DATA_DIR = os.environ.get("DOCUMENT_DATA_DIR", "document_data")
QA_MODEL = os.environ.get("QA_MODEL", "gpt-4o")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-large")

_executor = ThreadPoolExecutor(max_workers=4)


class DistributedDocumentQAMCP(NativeMCPBase):
    SERVICE_ID = "distributed-document-qa-mcp"
    SERVICE_NAME = "Distributed Document Q&A MCP"
    VERSION = "1.0.0"

    CAPABILITIES = [
        CapabilityType.READ,
        CapabilityType.SEARCH,
        CapabilityType.EXECUTE,
    ]

    REQUIRES = []
    DEPENDS = []

    TOOLS = [
        {
            "name": "process_document",
            "description": (
                "Process a PDF (local path or URL) into searchable structured content. "
                "Extracts text, tables, figures, equations, and metadata. "
                "Stores embeddings for semantic Q&A."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Local file path or URL to a PDF"},
                    "enable_ocr": {"type": "boolean", "default": False, "description": "Run OCR on scanned pages"},
                    "extract_figures": {"type": "boolean", "default": True},
                    "extract_tables": {"type": "boolean", "default": True},
                },
                "required": ["source"],
            },
        },
        {
            "name": "list_documents",
            "description": "List all processed documents with metadata.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "ask_document",
            "description": "Ask a question about a processed document using RAG over its content.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "doc_id": {"type": "string", "description": "Specific document ID (optional, searches all if omitted)"},
                    "n_context": {"type": "integer", "default": 8},
                },
                "required": ["question"],
            },
        },
        {
            "name": "search_documents",
            "description": "Semantic search across all processed document content.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "doc_id": {"type": "string"},
                    "n_results": {"type": "integer", "default": 10},
                    "search_type": {"type": "string", "enum": ["all", "text", "tables", "figures"], "default": "all"},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_document_text",
            "description": "Get the full extracted text from a document.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id": {"type": "string"},
                    "page": {"type": "integer", "description": "Specific page number (optional, all pages if omitted)"},
                },
                "required": ["doc_id"],
            },
        },
        {
            "name": "extract_tables",
            "description": "Get all detected tables from a document as structured JSON.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id": {"type": "string"},
                    "page": {"type": "integer"},
                },
                "required": ["doc_id"],
            },
        },
        {
            "name": "extract_figures",
            "description": "Get all detected figures/plots from a document with descriptions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id": {"type": "string"},
                    "page": {"type": "integer"},
                },
                "required": ["doc_id"],
            },
        },
        {
            "name": "summarize_document",
            "description": "Generate a summary of a document at short/medium/detailed level.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id": {"type": "string"},
                    "level": {"type": "string", "enum": ["short", "medium", "detailed"], "default": "medium"},
                    "focus": {"type": "string", "description": "Optional focus area, e.g. 'methodology', 'results'"},
                },
                "required": ["doc_id"],
            },
        },
        {
            "name": "get_document_metadata",
            "description": "Get metadata for a processed document (title, authors, abstract, page count, etc.).",
            "inputSchema": {
                "type": "object",
                "properties": {"doc_id": {"type": "string"}},
                "required": ["doc_id"],
            },
        },
        {
            "name": "compare_documents",
            "description": "Compare two documents on methodology, findings, and key concepts.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id_1": {"type": "string"},
                    "doc_id_2": {"type": "string"},
                },
                "required": ["doc_id_1", "doc_id_2"],
            },
        },
        {
            "name": "delete_document",
            "description": "Remove a document from the store.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "doc_id": {"type": "string"},
                    "delete_files": {"type": "boolean", "default": False},
                },
                "required": ["doc_id"],
            },
        },
    ]

    def __init__(self, port: int, **kwargs):
        super().__init__(port=port, **kwargs)
        self._store: Optional[Any] = None
        self._processor: Optional[Any] = None
        self._oai: Optional[Any] = None

    def _get_oai(self):
        if self._oai is None:
            import openai
            self._oai = openai.OpenAI()
        return self._oai

    def _get_store(self):
        if self._store is None:
            self._store = DocumentStore(data_dir=DATA_DIR, embedding_model=EMBEDDING_MODEL)
        return self._store

    def _get_processor(self):
        if self._processor is None:
            try:
                from pdf_to_structure_unified import UnifiedPDFProcessor
                self._processor = UnifiedPDFProcessor()
            except ImportError:
                self._processor = _FallbackPDFProcessor()
        return self._processor

    def _ok(self, data: Any) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps(data, indent=2, default=str)}]}

    def _err(self, msg: str) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps({"error": msg})}], "isError": True}

    async def _run_sync(self, fn, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))

    # -------------------------------------------------------------------------

    async def _tool_process_document(self, args: Dict[str, Any]) -> Dict[str, Any]:
        source = args.get("source")
        if not source:
            return self._err("source is required")

        def _run():
            store = self._get_store()
            proc = self._get_processor()

            # Download if URL
            if source.startswith("http://") or source.startswith("https://"):
                resp = httpx.get(source, follow_redirects=True, timeout=60.0)
                resp.raise_for_status()
                suffix = ".pdf"
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                tmp.write(resp.content)
                tmp.flush()
                local_path = tmp.name
                original_name = source.split("/")[-1].split("?")[0] or "document.pdf"
            else:
                local_path = source
                original_name = Path(source).name

            doc_id = hashlib.sha256(source.encode()).hexdigest()[:16]

            result = proc.process(
                local_path,
                doc_id=doc_id,
                extract_figures=args.get("extract_figures", True),
                extract_tables=args.get("extract_tables", True),
                enable_ocr=args.get("enable_ocr", False),
            )
            store.ingest(result)

            return {
                "status": "ok",
                "doc_id": doc_id,
                "source": source,
                "title": result.get("title", original_name),
                "page_count": result.get("page_count", 0),
                "text_chunks": result.get("text_chunk_count", 0),
                "tables_found": len(result.get("tables", [])),
                "figures_found": len(result.get("figures", [])),
            }

        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_list_documents(self, args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return self._get_store().list_documents()
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_ask_document(self, args: Dict[str, Any]) -> Dict[str, Any]:
        question = args.get("question")
        if not question:
            return self._err("question is required")

        def _run():
            store = self._get_store()
            client = self._get_oai()
            doc_id = args.get("doc_id")
            n = args.get("n_context", 8)

            results = store.search_text(question, doc_id=doc_id, n_results=n)
            context = "\n\n".join(
                f"[Page {r.get('page', '?')}] {r['text']}" for r in results
            )

            messages = [
                {"role": "system", "content": "You are a document Q&A assistant. Answer based on the provided document excerpts. Cite page numbers when relevant. Be precise."},
                {"role": "user", "content": f"Document context:\n{context}\n\nQuestion: {question}"},
            ]
            resp = client.chat.completions.create(model=QA_MODEL, messages=messages, max_completion_tokens=1500)
            return {
                "answer": resp.choices[0].message.content.strip(),
                "doc_id": doc_id,
                "sources_used": len(results),
            }
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_search_documents(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args.get("query")
        if not query:
            return self._err("query is required")
        def _run():
            store = self._get_store()
            t = args.get("search_type", "all")
            doc_id = args.get("doc_id")
            n = args.get("n_results", 10)
            if t == "text":
                return {"text": store.search_text(query, doc_id=doc_id, n_results=n)}
            elif t == "tables":
                return {"tables": store.search_tables(query, doc_id=doc_id, n_results=n)}
            elif t == "figures":
                return {"figures": store.search_figures(query, doc_id=doc_id, n_results=n)}
            return store.search_all(query, doc_id=doc_id, n_results=n)
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_get_document_text(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            store = self._get_store()
            page = args.get("page")
            text = store.get_text(doc_id, page=page)
            if not text:
                raise ValueError(f"No text found for {doc_id}")
            return {"doc_id": doc_id, "page": page, "text": text}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_extract_tables(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            store = self._get_store()
            tables = store.get_tables(doc_id, page=args.get("page"))
            return {"doc_id": doc_id, "count": len(tables), "tables": tables}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_extract_figures(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            store = self._get_store()
            figures = store.get_figures(doc_id, page=args.get("page"))
            return {"doc_id": doc_id, "count": len(figures), "figures": figures}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_summarize_document(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            store = self._get_store()
            client = self._get_oai()
            info = store.get_doc_info(doc_id)
            if not info:
                raise ValueError(f"Document {doc_id} not found")
            text = store.get_text(doc_id)[:12000]
            level = args.get("level", "medium")
            focus = args.get("focus", "")
            length_map = {"short": "2-3 sentences", "medium": "a paragraph", "detailed": "3-5 paragraphs"}
            length_desc = length_map.get(level, "a paragraph")
            focus_prompt = f" Focus particularly on: {focus}." if focus else ""
            prompt = f"Summarize this document in {length_desc}.{focus_prompt}\n\nDocument: {info.get('title', doc_id)}\n\n{text}"
            resp = client.chat.completions.create(
                model=QA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=1000,
            )
            return {
                "doc_id": doc_id,
                "title": info.get("title"),
                "level": level,
                "summary": resp.choices[0].message.content.strip(),
            }
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_get_document_metadata(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            info = self._get_store().get_doc_info(doc_id)
            if not info:
                raise ValueError(f"Document {doc_id} not found")
            return info
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_compare_documents(self, args: Dict[str, Any]) -> Dict[str, Any]:
        d1, d2 = args.get("doc_id_1"), args.get("doc_id_2")
        if not d1 or not d2:
            return self._err("doc_id_1 and doc_id_2 are required")
        def _run():
            store = self._get_store()
            client = self._get_oai()
            info1 = store.get_doc_info(d1)
            info2 = store.get_doc_info(d2)
            if not info1:
                raise ValueError(f"Document {d1} not found")
            if not info2:
                raise ValueError(f"Document {d2} not found")
            t1 = store.get_text(d1)[:6000]
            t2 = store.get_text(d2)[:6000]
            prompt = (
                f"Compare these two documents:\n\n"
                f"Document 1: {info1.get('title', d1)}\n{t1}\n\n"
                f"Document 2: {info2.get('title', d2)}\n{t2}\n\n"
                "Compare on: methodology, key findings, data sources, conclusions, and unique contributions."
            )
            resp = client.chat.completions.create(
                model=QA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=1500,
            )
            return {
                "doc_1": {"id": d1, "title": info1.get("title")},
                "doc_2": {"id": d2, "title": info2.get("title")},
                "comparison": resp.choices[0].message.content.strip(),
            }
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_delete_document(self, args: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = args.get("doc_id")
        if not doc_id:
            return self._err("doc_id is required")
        def _run():
            import shutil
            store = self._get_store()
            counts = store.delete_document(doc_id)
            if args.get("delete_files"):
                out_dir = os.path.join(DATA_DIR, doc_id)
                if os.path.isdir(out_dir):
                    shutil.rmtree(out_dir, ignore_errors=True)
                    counts["files_deleted"] = True
            return {"status": "deleted", "doc_id": doc_id, **counts}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))


# =============================================================================
# Lightweight DocumentStore (SQLite + ChromaDB)
# =============================================================================

class DocumentStore:
    """
    Persistent store for processed document content.
    Uses ChromaDB for semantic search and SQLite for metadata/raw content.
    """

    def __init__(self, data_dir: str = "document_data", embedding_model: str = "text-embedding-3-large"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.embedding_model = embedding_model
        self._chroma: Optional[Any] = None
        self._db_path = self.data_dir / "documents.db"
        self._init_db()

    def _init_db(self):
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    title TEXT,
                    source TEXT,
                    page_count INTEGER,
                    processed_at TEXT,
                    metadata JSON
                );
                CREATE TABLE IF NOT EXISTS text_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT,
                    page INTEGER,
                    chunk_index INTEGER,
                    text TEXT,
                    FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
                );
                CREATE TABLE IF NOT EXISTS tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT,
                    page INTEGER,
                    table_index INTEGER,
                    content JSON,
                    description TEXT
                );
                CREATE TABLE IF NOT EXISTS figures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT,
                    page INTEGER,
                    figure_index INTEGER,
                    image_path TEXT,
                    description TEXT,
                    caption TEXT
                );
            """)

    def _get_chroma(self):
        if self._chroma is None:
            import chromadb
            import openai
            client = chromadb.PersistentClient(path=str(self.data_dir / "chroma"))
            oai = openai.OpenAI()
            model = self.embedding_model

            def _embed(texts):
                resp = oai.embeddings.create(model=model, input=texts)
                return [d.embedding for d in resp.data]

            self._chroma = (client, _embed)
        return self._chroma

    def ingest(self, result: dict):
        import sqlite3
        from datetime import datetime, timezone
        doc_id = result["doc_id"]
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO documents VALUES (?,?,?,?,?,?)",
                (doc_id, result.get("title", ""), result.get("source", ""),
                 result.get("page_count", 0),
                 datetime.now(timezone.utc).isoformat(),
                 json.dumps(result.get("metadata", {})))
            )
            for chunk in result.get("text_chunks", []):
                conn.execute(
                    "INSERT INTO text_chunks (doc_id, page, chunk_index, text) VALUES (?,?,?,?)",
                    (doc_id, chunk.get("page", 0), chunk.get("index", 0), chunk.get("text", ""))
                )
            for i, table in enumerate(result.get("tables", [])):
                conn.execute(
                    "INSERT INTO tables (doc_id, page, table_index, content, description) VALUES (?,?,?,?,?)",
                    (doc_id, table.get("page", 0), i, json.dumps(table.get("data", [])), table.get("description", ""))
                )
            for i, fig in enumerate(result.get("figures", [])):
                conn.execute(
                    "INSERT INTO figures (doc_id, page, figure_index, image_path, description, caption) VALUES (?,?,?,?,?,?)",
                    (doc_id, fig.get("page", 0), i, fig.get("image_path", ""),
                     fig.get("description", ""), fig.get("caption", ""))
                )

        # Embed text chunks into chroma
        chunks = result.get("text_chunks", [])
        if chunks:
            try:
                client, embed_fn = self._get_chroma()
                col = client.get_or_create_collection(f"doc_{doc_id}")
                texts = [c["text"] for c in chunks]
                embeddings = embed_fn(texts)
                col.upsert(
                    ids=[f"{doc_id}_{i}" for i in range(len(chunks))],
                    embeddings=embeddings,
                    documents=texts,
                    metadatas=[{"doc_id": doc_id, "page": c.get("page", 0), "chunk_index": i}
                                for i, c in enumerate(chunks)],
                )
            except Exception as e:
                logger.warning(f"Chroma ingest failed: {e}")

    def list_documents(self) -> List[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute("SELECT doc_id, title, source, page_count, processed_at FROM documents").fetchall()
        return [{"doc_id": r[0], "title": r[1], "source": r[2], "page_count": r[3], "processed_at": r[4]} for r in rows]

    def get_doc_info(self, doc_id: str) -> Optional[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                "SELECT doc_id, title, source, page_count, processed_at, metadata FROM documents WHERE doc_id=?",
                (doc_id,)
            ).fetchone()
        if not row:
            return None
        return {"doc_id": row[0], "title": row[1], "source": row[2],
                "page_count": row[3], "processed_at": row[4],
                "metadata": json.loads(row[5] or "{}")}

    def get_text(self, doc_id: str, page: Optional[int] = None) -> str:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            if page is not None:
                rows = conn.execute(
                    "SELECT text FROM text_chunks WHERE doc_id=? AND page=? ORDER BY chunk_index",
                    (doc_id, page)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT text FROM text_chunks WHERE doc_id=? ORDER BY page, chunk_index",
                    (doc_id,)
                ).fetchall()
        return "\n".join(r[0] for r in rows)

    def get_tables(self, doc_id: str, page: Optional[int] = None) -> List[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            if page is not None:
                rows = conn.execute(
                    "SELECT page, table_index, content, description FROM tables WHERE doc_id=? AND page=?",
                    (doc_id, page)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT page, table_index, content, description FROM tables WHERE doc_id=?",
                    (doc_id,)
                ).fetchall()
        return [{"page": r[0], "index": r[1], "data": json.loads(r[2] or "[]"), "description": r[3]} for r in rows]

    def get_figures(self, doc_id: str, page: Optional[int] = None) -> List[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            if page is not None:
                rows = conn.execute(
                    "SELECT page, figure_index, image_path, description, caption FROM figures WHERE doc_id=? AND page=?",
                    (doc_id, page)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT page, figure_index, image_path, description, caption FROM figures WHERE doc_id=?",
                    (doc_id,)
                ).fetchall()
        return [{"page": r[0], "index": r[1], "image_path": r[2], "description": r[3], "caption": r[4]} for r in rows]

    def search_text(self, query: str, doc_id: Optional[str] = None, n_results: int = 10) -> List[Dict]:
        try:
            client, embed_fn = self._get_chroma()
            q_emb = embed_fn([query])[0]
            if doc_id:
                col = client.get_or_create_collection(f"doc_{doc_id}")
                results = col.query(query_embeddings=[q_emb], n_results=min(n_results, 100))
            else:
                cols = client.list_collections()
                all_results = []
                for col_meta in cols:
                    col = client.get_collection(col_meta.name)
                    r = col.query(query_embeddings=[q_emb], n_results=min(n_results, 10))
                    for i, doc in enumerate(r.get("documents", [[]])[0]):
                        meta = (r.get("metadatas", [[]])[0] or [{}])[i]
                        all_results.append({"text": doc, **meta,
                                            "distance": (r.get("distances", [[]])[0] or [1.0])[i]})
                all_results.sort(key=lambda x: x.get("distance", 1.0))
                return all_results[:n_results]

            out = []
            for i, doc in enumerate(results.get("documents", [[]])[0]):
                meta = (results.get("metadatas", [[]])[0] or [{}])[i]
                out.append({"text": doc, **meta})
            return out
        except Exception as e:
            logger.warning(f"Search failed: {e}")
            return []

    def search_tables(self, query: str, doc_id: Optional[str] = None, n_results: int = 10) -> List[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            if doc_id:
                rows = conn.execute(
                    "SELECT doc_id, page, table_index, content, description FROM tables WHERE doc_id=? AND description LIKE ?",
                    (doc_id, f"%{query}%")
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT doc_id, page, table_index, content, description FROM tables WHERE description LIKE ?",
                    (f"%{query}%",)
                ).fetchall()
        return [{"doc_id": r[0], "page": r[1], "index": r[2],
                 "data": json.loads(r[3] or "[]"), "description": r[4]}
                for r in rows[:n_results]]

    def search_figures(self, query: str, doc_id: Optional[str] = None, n_results: int = 10) -> List[Dict]:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            if doc_id:
                rows = conn.execute(
                    "SELECT doc_id, page, figure_index, image_path, description, caption FROM figures WHERE doc_id=? AND (description LIKE ? OR caption LIKE ?)",
                    (doc_id, f"%{query}%", f"%{query}%")
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT doc_id, page, figure_index, image_path, description, caption FROM figures WHERE description LIKE ? OR caption LIKE ?",
                    (f"%{query}%", f"%{query}%")
                ).fetchall()
        return [{"doc_id": r[0], "page": r[1], "index": r[2],
                 "image_path": r[3], "description": r[4], "caption": r[5]}
                for r in rows[:n_results]]

    def search_all(self, query: str, doc_id: Optional[str] = None, n_results: int = 10) -> Dict:
        return {
            "text": self.search_text(query, doc_id, n_results),
            "tables": self.search_tables(query, doc_id, n_results // 2),
            "figures": self.search_figures(query, doc_id, n_results // 2),
        }

    def delete_document(self, doc_id: str) -> Dict:
        import sqlite3
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("DELETE FROM text_chunks WHERE doc_id=?", (doc_id,))
            conn.execute("DELETE FROM tables WHERE doc_id=?", (doc_id,))
            conn.execute("DELETE FROM figures WHERE doc_id=?", (doc_id,))
            conn.execute("DELETE FROM documents WHERE doc_id=?", (doc_id,))
        try:
            client, _ = self._get_chroma()
            client.delete_collection(f"doc_{doc_id}")
        except Exception:
            pass
        return {"chunks_deleted": True, "tables_deleted": True, "figures_deleted": True}


# =============================================================================
# Fallback processor (when pdf_to is not available)
# =============================================================================

class _FallbackPDFProcessor:
    """Basic PDF processor using pypdf + OpenAI vision for figures."""

    def process(self, path: str, doc_id: str, extract_figures: bool = True,
                extract_tables: bool = True, enable_ocr: bool = False) -> dict:
        try:
            from pypdf import PdfReader
        except ImportError:
            raise RuntimeError("pypdf not installed. Run: pip install pypdf")

        reader = PdfReader(path)
        chunks = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            if text.strip():
                for j in range(0, len(text), 2000):
                    chunks.append({"page": i + 1, "index": j // 2000, "text": text[j:j + 2000]})

        title = reader.metadata.get("/Title", Path(path).stem) if reader.metadata else Path(path).stem

        return {
            "doc_id": doc_id,
            "source": path,
            "title": str(title),
            "page_count": len(reader.pages),
            "text_chunks": chunks,
            "text_chunk_count": len(chunks),
            "tables": [],
            "figures": [],
            "metadata": {},
        }


def create_server(port: Optional[int] = None) -> DistributedDocumentQAMCP:
    port = port or int(os.environ.get("PORT", "4006"))
    return DistributedDocumentQAMCP(port=port)


# Module-level ASGI app for Modal @asgi_app import (factory target)
app = create_server().app


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Document Q&A MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4006")))
    args = parser.parse_args()
    server = create_server(args.port)
    server.run()
