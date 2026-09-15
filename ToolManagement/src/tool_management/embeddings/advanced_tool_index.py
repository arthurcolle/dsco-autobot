"""
Advanced Tool Index - Sophisticated semantic tool discovery.

Multi-faceted embeddings, tool composition graphs, hybrid retrieval,
and learned affinities for intelligent tool selection at scale.
"""
from __future__ import annotations

import os
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from pathlib import Path
from collections import OrderedDict, defaultdict
from threading import RLock
import numpy as np

from .openrouter_embeddings import get_embedding_service, cosine_similarity

logger = logging.getLogger(__name__)
_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Use DATA_DIR from environment (set to /data on Modal volume)
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = Path("/tmp/tool_index_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ToolFacets:
    """Multi-faceted representation of a tool."""
    name: str
    backend: str

    # Core embeddings (1024-dim each for Jina v3)
    purpose_embedding: np.ndarray      # What the tool does
    input_embedding: np.ndarray        # What it needs
    output_embedding: np.ndarray       # What it produces
    example_embedding: np.ndarray      # Example use cases

    # Structured metadata
    input_types: List[str] = field(default_factory=list)   # e.g., ["string:query", "integer:limit"]
    output_types: List[str] = field(default_factory=list)  # e.g., ["array:results", "object:metadata"]
    capabilities: List[str] = field(default_factory=list)  # e.g., ["search", "async", "paginated"]

    # Usage statistics
    call_count: int = 0
    success_rate: float = 1.0
    avg_latency_ms: float = 0.0

    # Composition data
    chains_with: List[str] = field(default_factory=list)   # Tools often used after this one
    chained_from: List[str] = field(default_factory=list)  # Tools often used before this one


@dataclass
class QueryIntent:
    """Parsed intent from a user query."""
    action: str              # e.g., "search", "store", "transform", "analyze"
    entities: List[str]      # e.g., ["web", "documents", "memory"]
    constraints: List[str]   # e.g., ["fast", "authenticated", "batch"]
    output_expectation: str  # e.g., "list", "single", "stream"
    confidence: float


@dataclass(frozen=True)
class _CachedToolRow:
    """Search metadata cached alongside normalized embedding matrices."""

    tool_id: str
    name: str
    backend: str
    tool: Dict[str, Any]
    capabilities: Tuple[str, ...]
    call_count: int
    success_count: int
    lexical_terms: frozenset[str]
    normalized_name: str
    identifier_forms: frozenset[str]
    name_identifier_forms: frozenset[str]


@dataclass
class _SearchMatrixCacheEntry:
    """A freshness-bounded snapshot of rows and normalized facet vectors."""

    backend_filter: Optional[str]
    fingerprint: Tuple[Any, ...]
    checked_at: float
    built_at: float
    rows: List[_CachedToolRow]
    purpose: np.ndarray
    inputs: np.ndarray
    outputs: np.ndarray
    examples: np.ndarray
    byte_size: int


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _tokens(text: Any) -> List[str]:
    return _TOKEN_RE.findall(str(text or "").lower())


def _normalized_text(text: Any) -> str:
    return " ".join(_tokens(text))


def _identifier_forms(text: Any) -> Set[str]:
    raw = str(text or "").strip().lower()
    tokens = _tokens(raw)
    forms = {raw} if raw else set()
    if tokens:
        forms.add(" ".join(tokens))
        forms.add("".join(tokens))
    return forms


class AdvancedToolIndex:
    """
    Sophisticated tool index with multi-faceted embeddings and intelligent retrieval.

    Features:
    - Multi-vector embeddings per tool (purpose, inputs, outputs, examples)
    - Tool composition graph based on type compatibility
    - Hybrid search combining semantic, keyword, and usage signals
    - Query intent classification for better matching
    - Learned tool affinities from execution patterns
    """

    # Action keywords for intent classification
    ACTION_KEYWORDS = {
        "search": ["search", "find", "look", "query", "discover", "locate", "get"],
        "store": ["store", "save", "remember", "persist", "cache", "keep", "write"],
        "transform": ["transform", "convert", "parse", "format", "extract", "process"],
        "analyze": ["analyze", "understand", "evaluate", "assess", "check", "verify"],
        "create": ["create", "generate", "make", "build", "compose", "produce"],
        "delete": ["delete", "remove", "forget", "clear", "purge", "drop"],
        "send": ["send", "publish", "notify", "broadcast", "emit", "push"],
        "retrieve": ["retrieve", "fetch", "read", "load", "pull", "download"],
    }

    def __init__(self, db_path: Optional[str] = None):
        self.embedding_service = get_embedding_service()
        if db_path is None:
            # Namespace the index per embedder: local-hash vectors and Jina
            # vectors live in different spaces and must never be compared.
            embedder_name = getattr(self.embedding_service, "name", "")
            suffix = "_local" if "local" in str(embedder_name) else ""
            db_path = str(DATA_DIR / f"advanced_tool_index{suffix}.db")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"AdvancedToolIndex using database at {self.db_path}")

        self._init_db()

        # In-memory caches for fast retrieval
        self._facets_cache: Dict[str, ToolFacets] = {}
        self._composition_graph: Dict[str, Set[str]] = defaultdict(set)
        self._reverse_graph: Dict[str, Set[str]] = defaultdict(set)
        self._matrix_cache: OrderedDict[Optional[str], _SearchMatrixCacheEntry] = OrderedDict()
        self._matrix_cache_lock = RLock()
        self._matrix_cache_total_bytes = 0
        self._matrix_cache_ttl_seconds = max(
            0.0,
            _env_float("ADVANCED_TOOL_INDEX_MATRIX_CACHE_TTL_SECONDS", 5.0),
        )
        self._matrix_cache_max_entries = max(
            0,
            _env_int("ADVANCED_TOOL_INDEX_MATRIX_CACHE_MAX_ENTRIES", 8),
        )
        self._matrix_cache_max_bytes = max(
            0,
            _env_int("ADVANCED_TOOL_INDEX_MATRIX_CACHE_MAX_BYTES", 256 * 1024 * 1024),
        )

    def _init_db(self):
        """Initialize the advanced index database."""
        conn = sqlite3.connect(str(self.db_path))

        # Multi-faceted tool embeddings
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tool_facets (
                tool_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                backend TEXT NOT NULL,
                tool_data TEXT NOT NULL,

                purpose_embedding BLOB,
                input_embedding BLOB,
                output_embedding BLOB,
                example_embedding BLOB,

                input_types TEXT DEFAULT '[]',
                output_types TEXT DEFAULT '[]',
                capabilities TEXT DEFAULT '[]',

                call_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                total_latency_ms REAL DEFAULT 0,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tool composition edges (which tools chain well together)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tool_compositions (
                source_tool TEXT NOT NULL,
                target_tool TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                success_rate REAL DEFAULT 1.0,
                occurrence_count INTEGER DEFAULT 1,
                PRIMARY KEY (source_tool, target_tool)
            )
        """)

        # Execution history for learning
        conn.execute("""
            CREATE TABLE IF NOT EXISTS execution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                tool_sequence TEXT NOT NULL,
                success INTEGER,
                total_latency_ms REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Query patterns for intent learning
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_patterns (
                query_hash TEXT PRIMARY KEY,
                query_text TEXT NOT NULL,
                matched_tools TEXT NOT NULL,
                selected_tool TEXT,
                was_successful INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_facets_backend ON tool_facets(backend)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_compositions_source ON tool_compositions(source_tool)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_history_session ON execution_history(session_id)")

        conn.commit()
        conn.close()

    async def index_tool(self, tool: Dict[str, Any], backend: str) -> str:
        """
        Index a tool with multi-faceted embeddings.

        Generates separate embeddings for:
        - Purpose: What the tool does (from name + description)
        - Inputs: What it needs (from input schema)
        - Outputs: What it produces (from output schema)
        - Examples: How it's used (synthetic examples)
        """
        name = tool.get("name", "")
        if not name:
            return ""

        tool_id = f"{backend}:{name}"

        # Generate faceted text representations
        purpose_text = self._generate_purpose_text(tool, backend)
        input_text = self._generate_input_text(tool)
        output_text = self._generate_output_text(tool)
        example_text = self._generate_example_text(tool)

        # Batch embed all facets
        texts = [purpose_text, input_text, output_text, example_text]
        embeddings = await self.embedding_service.embed(texts)

        # Extract structured type information
        input_types = self._extract_types(tool.get("inputSchema", {}))
        output_types = self._extract_types(tool.get("outputSchema", {}))
        capabilities = self._infer_capabilities(tool)

        # Store in database
        conn = sqlite3.connect(str(self.db_path))
        now = datetime.now(timezone.utc).isoformat()

        conn.execute("""
            INSERT OR REPLACE INTO tool_facets
            (tool_id, name, backend, tool_data, purpose_embedding, input_embedding,
             output_embedding, example_embedding, input_types, output_types,
             capabilities, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tool_id, name, backend, json.dumps(tool),
            embeddings[0].tobytes(), embeddings[1].tobytes(),
            embeddings[2].tobytes(), embeddings[3].tobytes(),
            json.dumps(input_types), json.dumps(output_types),
            json.dumps(capabilities), now
        ))

        conn.commit()
        conn.close()
        self._invalidate_matrix_cache()

        logger.debug(f"Indexed tool with facets: {tool_id}")
        return tool_id

    async def index_tools_batch(self, tools: List[Dict[str, Any]], backend: str):
        """Batch index multiple tools efficiently."""
        if not tools:
            return

        # Prepare all texts for batch embedding
        all_texts = []
        tool_info = []

        for tool in tools:
            name = tool.get("name", "")
            if not name:
                continue

            purpose_text = self._generate_purpose_text(tool, backend)
            input_text = self._generate_input_text(tool)
            output_text = self._generate_output_text(tool)
            example_text = self._generate_example_text(tool)

            all_texts.extend([purpose_text, input_text, output_text, example_text])
            tool_info.append({
                "tool": tool,
                "name": name,
                "tool_id": f"{backend}:{name}",
                "input_types": self._extract_types(tool.get("inputSchema", {})),
                "output_types": self._extract_types(tool.get("outputSchema", {})),
                "capabilities": self._infer_capabilities(tool)
            })

        if not all_texts:
            return

        # Batch embed all facets
        logger.info(f"Generating {len(all_texts)} faceted embeddings for {len(tool_info)} tools...")
        embeddings = await self.embedding_service.embed(all_texts)

        # Store all in database
        conn = sqlite3.connect(str(self.db_path))
        now = datetime.now(timezone.utc).isoformat()

        for i, info in enumerate(tool_info):
            base_idx = i * 4
            conn.execute("""
                INSERT OR REPLACE INTO tool_facets
                (tool_id, name, backend, tool_data, purpose_embedding, input_embedding,
                 output_embedding, example_embedding, input_types, output_types,
                 capabilities, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                info["tool_id"], info["name"], backend, json.dumps(info["tool"]),
                embeddings[base_idx].tobytes(), embeddings[base_idx + 1].tobytes(),
                embeddings[base_idx + 2].tobytes(), embeddings[base_idx + 3].tobytes(),
                json.dumps(info["input_types"]), json.dumps(info["output_types"]),
                json.dumps(info["capabilities"]), now
            ))

        conn.commit()
        conn.close()
        self._invalidate_matrix_cache()

        # Build composition graph based on type compatibility
        await self._build_composition_graph(tool_info)

        logger.info(f"Batch indexed {len(tool_info)} tools with facets from {backend}")

    async def search(
        self,
        query: str,
        limit: int = 10,
        backend_filter: Optional[str] = None,
        intent_weight: float = 0.3,
        usage_weight: float = 0.2
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search combining multiple signals.

        Scoring formula:
        score = (1 - intent_weight - usage_weight) * semantic_score
              + intent_weight * intent_match_score
              + usage_weight * usage_score
        """
        # Parse query intent and retain an explicit lexical signal. Semantic
        # similarity alone can rank an integration action containing one query
        # verb above an exact built-in match (for example, a comment action
        # above ``add`` for "add two numbers").
        intent = self._classify_intent(query)
        query_terms = set(_tokens(query))
        normalized_query = _normalized_text(query)
        query_identifier_forms = _identifier_forms(query)

        # Generate query embedding
        query_embeddings = await self.embedding_service.embed([query])
        query_embedding = np.asarray(query_embeddings[0], dtype=np.float64)

        results = []
        semantic_weight = 1.0 - intent_weight - usage_weight

        corpus = self._get_search_matrix_cache(backend_filter)
        if not corpus.rows:
            return []

        query_norm = self._normalize_vector(query_embedding)
        if corpus.purpose.shape[1] != query_norm.shape[0]:
            logger.warning(
                "Skipping advanced tool search: query embedding dim %s does not match corpus dim %s",
                query_norm.shape[0],
                corpus.purpose.shape[1],
            )
            self._invalidate_matrix_cache()
            return []

        # Multi-facet semantic similarity (weighted combination), batched over
        # the cached normalized corpus so repeated queries avoid vector
        # reconstruction and per-row cosine calls.
        purpose_sims = corpus.purpose @ query_norm
        input_sims = corpus.inputs @ query_norm
        output_sims = corpus.outputs @ query_norm
        example_sims = corpus.examples @ query_norm

        for idx, cached in enumerate(corpus.rows):
            purpose_sim = float(purpose_sims[idx])
            input_sim = float(input_sims[idx])
            output_sim = float(output_sims[idx])
            example_sim = float(example_sims[idx])

            # Weight purpose highest, then examples, then I/O
            semantic_score = (
                0.4 * purpose_sim +
                0.25 * example_sim +
                0.2 * input_sim +
                0.15 * output_sim
            )

            # Intent match score
            capabilities = list(cached.capabilities)
            intent_score = self._compute_intent_match(intent, capabilities, cached.name)

            # Usage score (success-weighted call count)
            success_rate = cached.success_count / max(cached.call_count, 1)
            usage_score = min(1.0, (cached.call_count / 100) * success_rate)

            base_score = (
                semantic_weight * semantic_score +
                intent_weight * intent_score +
                usage_weight * usage_score
            )

            lexical_score = (
                len(query_terms & cached.lexical_terms) / len(query_terms)
                if query_terms else 0.0
            )
            exact_name_match = (
                bool(cached.normalized_name)
                and cached.normalized_name == normalized_query
            )
            name_phrase_match = (
                bool(cached.normalized_name)
                and f" {cached.normalized_name} " in f" {normalized_query} "
            )
            identifier_rank = self._identifier_match_rank(
                query_identifier_forms,
                cached,
            )
            if identifier_rank:
                lexical_score = 1.0
            elif name_phrase_match:
                lexical_score = max(lexical_score, 0.95)

            final_score = 0.75 * base_score + 0.25 * lexical_score
            if identifier_rank:
                final_score = max(final_score, 1.0)
            elif exact_name_match:
                final_score = max(final_score, 0.99)

            results.append({
                "tool_id": cached.tool_id,
                "tool": dict(cached.tool),
                "backend": cached.backend,
                "score": float(final_score),
                "semantic_score": float(semantic_score),
                "intent_score": float(intent_score),
                "usage_score": float(usage_score),
                "lexical_score": float(lexical_score),
                "identifier_score": float(1.0 if identifier_rank else 0.0),
                "_identifier_rank": identifier_rank,
                "facet_scores": {
                    "purpose": float(purpose_sim),
                    "input": float(input_sim),
                    "output": float(output_sim),
                    "example": float(example_sim)
                },
                "intent": {
                    "action": intent.action,
                    "entities": intent.entities,
                    "confidence": intent.confidence
                }
            })

        # Exact identifiers sort before fuzzy ranking. Ties retain SQLite row
        # order through Python's stable sort.
        results.sort(
            key=lambda x: (x["_identifier_rank"], x["score"]),
            reverse=True,
        )
        for result in results:
            result.pop("_identifier_rank", None)

        return results[:limit]

    def _get_search_matrix_cache(
        self,
        backend_filter: Optional[str],
    ) -> _SearchMatrixCacheEntry:
        """Return a fresh-enough normalized matrix snapshot for search."""
        cache_key = backend_filter or None
        now = time.monotonic()

        with self._matrix_cache_lock:
            cached = self._matrix_cache.get(cache_key)
            if cached is not None:
                if now - cached.checked_at <= self._matrix_cache_ttl_seconds:
                    self._matrix_cache.move_to_end(cache_key)
                    return cached

                fingerprint = self._search_corpus_fingerprint(cache_key)
                if fingerprint == cached.fingerprint:
                    cached.checked_at = time.monotonic()
                    self._matrix_cache.move_to_end(cache_key)
                    return cached

                self._drop_matrix_cache_entry(cache_key)

            fresh = self._load_search_matrix_cache(cache_key)
            self._store_matrix_cache_entry(cache_key, fresh)
            return fresh

    def _load_search_matrix_cache(
        self,
        backend_filter: Optional[str],
    ) -> _SearchMatrixCacheEntry:
        rows = self._fetch_search_rows(backend_filter)
        fingerprint = self._fingerprint_from_rows(rows)
        now = time.monotonic()

        cached_rows: List[_CachedToolRow] = []
        purpose_vectors: List[np.ndarray] = []
        input_vectors: List[np.ndarray] = []
        output_vectors: List[np.ndarray] = []
        example_vectors: List[np.ndarray] = []

        for row in rows:
            (
                tool_id,
                name,
                backend,
                tool_data_json,
                purpose_bytes,
                input_bytes,
                output_bytes,
                example_bytes,
                capabilities_json,
                call_count,
                success_count,
                _total_latency,
                _updated_at,
            ) = row

            try:
                vectors = (
                    np.frombuffer(purpose_bytes, dtype=np.float64),
                    np.frombuffer(input_bytes, dtype=np.float64),
                    np.frombuffer(output_bytes, dtype=np.float64),
                    np.frombuffer(example_bytes, dtype=np.float64),
                )
                if (
                    not vectors[0].size
                    or vectors[1].shape != vectors[0].shape
                    or vectors[2].shape != vectors[0].shape
                    or vectors[3].shape != vectors[0].shape
                ):
                    logger.warning("Skipping tool with inconsistent embeddings: %s", tool_id)
                    continue

                tool_data = json.loads(tool_data_json)
                capabilities = tuple(json.loads(capabilities_json) if capabilities_json else [])
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping malformed indexed tool %s: %s", tool_id, exc)
                continue

            cached_rows.append(
                _CachedToolRow(
                    tool_id=tool_id,
                    name=name,
                    backend=backend,
                    tool=tool_data,
                    capabilities=capabilities,
                    call_count=int(call_count or 0),
                    success_count=int(success_count or 0),
                    lexical_terms=self._lexical_terms(tool_id, name, backend, tool_data),
                    normalized_name=_normalized_text(name),
                    identifier_forms=self._tool_identifier_forms(tool_id, name, backend, tool_data),
                    name_identifier_forms=frozenset(
                        _identifier_forms(name) | _identifier_forms(tool_data.get("name"))
                    ),
                )
            )
            purpose_vectors.append(vectors[0])
            input_vectors.append(vectors[1])
            output_vectors.append(vectors[2])
            example_vectors.append(vectors[3])

        purpose = self._normalize_matrix(purpose_vectors)
        inputs = self._normalize_matrix(input_vectors)
        outputs = self._normalize_matrix(output_vectors)
        examples = self._normalize_matrix(example_vectors)
        metadata_bytes = sum(
            len(str(row[0] or ""))
            + len(str(row[1] or ""))
            + len(str(row[2] or ""))
            + len(row[3] or "")
            + len(row[8] or "")
            for row in rows
        )
        byte_size = (
            purpose.nbytes
            + inputs.nbytes
            + outputs.nbytes
            + examples.nbytes
            + metadata_bytes
        )

        return _SearchMatrixCacheEntry(
            backend_filter=backend_filter,
            fingerprint=fingerprint,
            checked_at=now,
            built_at=now,
            rows=cached_rows,
            purpose=purpose,
            inputs=inputs,
            outputs=outputs,
            examples=examples,
            byte_size=byte_size,
        )

    def _fetch_search_rows(self, backend_filter: Optional[str]) -> List[Tuple[Any, ...]]:
        conn = sqlite3.connect(str(self.db_path))
        try:
            if backend_filter:
                cursor = conn.execute("""
                    SELECT tool_id, name, backend, tool_data,
                           purpose_embedding, input_embedding, output_embedding, example_embedding,
                           capabilities, call_count, success_count, total_latency_ms, updated_at
                    FROM tool_facets WHERE backend = ?
                """, (backend_filter,))
            else:
                cursor = conn.execute("""
                    SELECT tool_id, name, backend, tool_data,
                           purpose_embedding, input_embedding, output_embedding, example_embedding,
                           capabilities, call_count, success_count, total_latency_ms, updated_at
                    FROM tool_facets
                """)
            return cursor.fetchall()
        finally:
            conn.close()

    def _search_corpus_fingerprint(
        self,
        backend_filter: Optional[str],
    ) -> Tuple[Any, ...]:
        conn = sqlite3.connect(str(self.db_path))
        try:
            where = "WHERE backend = ?" if backend_filter else ""
            sql = f"""
                SELECT
                    COUNT(*),
                    COALESCE(MAX(updated_at), ''),
                    COALESCE(SUM(
                        COALESCE(LENGTH(purpose_embedding), 0) +
                        COALESCE(LENGTH(input_embedding), 0) +
                        COALESCE(LENGTH(output_embedding), 0) +
                        COALESCE(LENGTH(example_embedding), 0)
                    ), 0),
                    COALESCE(SUM(call_count), 0),
                    COALESCE(SUM(success_count), 0),
                    COALESCE(SUM(total_latency_ms), 0),
                    COALESCE(SUM(LENGTH(tool_data)), 0),
                    COALESCE(SUM(LENGTH(capabilities)), 0)
                FROM tool_facets
                {where}
            """
            params = (backend_filter,) if backend_filter else ()
            row = conn.execute(sql, params).fetchone()
            return self._fingerprint_tuple(row)
        finally:
            conn.close()

    def _fingerprint_from_rows(self, rows: List[Tuple[Any, ...]]) -> Tuple[Any, ...]:
        if not rows:
            return self._fingerprint_tuple((0, "", 0, 0, 0, 0.0, 0, 0))

        max_updated_at = max(str(row[12] or "") for row in rows)
        embedding_bytes = sum(
            sum(len(blob or b"") for blob in row[4:8])
            for row in rows
        )
        call_count_sum = sum(int(row[9] or 0) for row in rows)
        success_count_sum = sum(int(row[10] or 0) for row in rows)
        latency_sum = sum(float(row[11] or 0.0) for row in rows)
        tool_data_bytes = sum(len(row[3] or "") for row in rows)
        capabilities_bytes = sum(len(row[8] or "") for row in rows)
        return self._fingerprint_tuple((
            len(rows),
            max_updated_at,
            embedding_bytes,
            call_count_sum,
            success_count_sum,
            latency_sum,
            tool_data_bytes,
            capabilities_bytes,
        ))

    @staticmethod
    def _fingerprint_tuple(row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return (
            int(row[0] or 0),
            str(row[1] or ""),
            int(row[2] or 0),
            int(row[3] or 0),
            int(row[4] or 0),
            round(float(row[5] or 0.0), 6),
            int(row[6] or 0),
            int(row[7] or 0),
        )

    def _store_matrix_cache_entry(
        self,
        cache_key: Optional[str],
        entry: _SearchMatrixCacheEntry,
    ):
        if (
            self._matrix_cache_max_entries <= 0
            or self._matrix_cache_max_bytes <= 0
            or entry.byte_size > self._matrix_cache_max_bytes
        ):
            return

        self._drop_matrix_cache_entry(cache_key)
        self._matrix_cache[cache_key] = entry
        self._matrix_cache_total_bytes += entry.byte_size
        self._matrix_cache.move_to_end(cache_key)

        while (
            len(self._matrix_cache) > self._matrix_cache_max_entries
            or self._matrix_cache_total_bytes > self._matrix_cache_max_bytes
        ):
            _, evicted = self._matrix_cache.popitem(last=False)
            self._matrix_cache_total_bytes = max(
                0,
                self._matrix_cache_total_bytes - evicted.byte_size,
            )

    def _drop_matrix_cache_entry(self, cache_key: Optional[str]):
        evicted = self._matrix_cache.pop(cache_key, None)
        if evicted is not None:
            self._matrix_cache_total_bytes = max(
                0,
                self._matrix_cache_total_bytes - evicted.byte_size,
            )

    def _invalidate_matrix_cache(self):
        with self._matrix_cache_lock:
            self._matrix_cache.clear()
            self._matrix_cache_total_bytes = 0

    @staticmethod
    def _normalize_matrix(vectors: List[np.ndarray]) -> np.ndarray:
        if not vectors:
            return np.empty((0, 0), dtype=np.float64)
        matrix = np.vstack(vectors).astype(np.float64, copy=False)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        return np.divide(
            matrix,
            norms,
            out=np.zeros_like(matrix, dtype=np.float64),
            where=norms != 0,
        )

    @staticmethod
    def _normalize_vector(vector: np.ndarray) -> np.ndarray:
        vector = np.asarray(vector, dtype=np.float64).reshape(-1)
        norm = np.linalg.norm(vector)
        if norm == 0:
            return np.zeros_like(vector, dtype=np.float64)
        return vector / norm

    @staticmethod
    def _lexical_terms(
        tool_id: str,
        name: str,
        backend: str,
        tool_data: Dict[str, Any],
    ) -> frozenset[str]:
        annotations = tool_data.get("annotations") or {}
        tags = tool_data.get("tags") or []
        parts = [
            tool_id,
            name,
            backend,
            tool_data.get("description"),
            tool_data.get("category"),
            tool_data.get("source"),
            tool_data.get("backend"),
            tool_data.get("provider"),
            annotations.get("backend"),
            " ".join(str(tag) for tag in tags),
        ]
        return frozenset(term for part in parts for term in _tokens(part))

    @staticmethod
    def _tool_identifier_forms(
        tool_id: str,
        name: str,
        backend: str,
        tool_data: Dict[str, Any],
    ) -> frozenset[str]:
        forms: Set[str] = set()
        for value in (
            tool_id,
            f"{backend}:{name}",
            name,
            tool_data.get("id"),
            tool_data.get("name"),
        ):
            forms.update(_identifier_forms(value))
        return frozenset(forms)

    @staticmethod
    def _identifier_match_rank(
        query_forms: Set[str],
        cached: _CachedToolRow,
    ) -> int:
        if not query_forms:
            return 0
        if query_forms & cached.name_identifier_forms:
            return 2
        if query_forms & cached.identifier_forms:
            return 3
        return 0

    async def find_composable_tools(
        self,
        tool_id: str,
        direction: str = "forward",  # "forward" = tools that can follow, "backward" = tools that can precede
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find tools that compose well with the given tool."""
        conn = sqlite3.connect(str(self.db_path))

        if direction == "forward":
            cursor = conn.execute("""
                SELECT tc.target_tool, tc.weight, tc.success_rate, tc.occurrence_count,
                       tf.tool_data, tf.backend
                FROM tool_compositions tc
                JOIN tool_facets tf ON tc.target_tool = tf.tool_id
                WHERE tc.source_tool = ?
                ORDER BY tc.weight * tc.success_rate DESC
                LIMIT ?
            """, (tool_id, limit))
        else:
            cursor = conn.execute("""
                SELECT tc.source_tool, tc.weight, tc.success_rate, tc.occurrence_count,
                       tf.tool_data, tf.backend
                FROM tool_compositions tc
                JOIN tool_facets tf ON tc.source_tool = tf.tool_id
                WHERE tc.target_tool = ?
                ORDER BY tc.weight * tc.success_rate DESC
                LIMIT ?
            """, (tool_id, limit))

        results = []
        for row in cursor:
            related_tool_id, weight, success_rate, occurrences, tool_data_json, backend = row
            results.append({
                "tool_id": related_tool_id,
                "tool": json.loads(tool_data_json),
                "backend": backend,
                "composition_score": weight * success_rate,
                "occurrences": occurrences,
                "success_rate": success_rate
            })

        conn.close()
        return results

    async def record_execution(
        self,
        session_id: str,
        tool_sequence: List[str],
        success: bool,
        total_latency_ms: float
    ):
        """Record a tool execution for learning."""
        conn = sqlite3.connect(str(self.db_path))
        now = datetime.now(timezone.utc).isoformat()

        # Record execution history
        conn.execute("""
            INSERT INTO execution_history (session_id, tool_sequence, success, total_latency_ms, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (session_id, json.dumps(tool_sequence), 1 if success else 0, total_latency_ms, now))

        # Update tool statistics
        for tool_id in tool_sequence:
            conn.execute("""
                UPDATE tool_facets
                SET call_count = call_count + 1,
                    success_count = success_count + ?,
                    total_latency_ms = total_latency_ms + ?
                WHERE tool_id = ?
            """, (1 if success else 0, total_latency_ms / len(tool_sequence), tool_id))

        # Update composition graph edges
        for i in range(len(tool_sequence) - 1):
            source, target = tool_sequence[i], tool_sequence[i + 1]
            conn.execute("""
                INSERT INTO tool_compositions (source_tool, target_tool, weight, success_rate, occurrence_count)
                VALUES (?, ?, 1.0, ?, 1)
                ON CONFLICT(source_tool, target_tool) DO UPDATE SET
                    occurrence_count = occurrence_count + 1,
                    success_rate = (success_rate * occurrence_count + ?) / (occurrence_count + 1),
                    weight = 1.0 + LOG(occurrence_count + 1)
            """, (source, target, 1.0 if success else 0.0, 1.0 if success else 0.0))

        conn.commit()
        conn.close()
        self._invalidate_matrix_cache()

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive index statistics."""
        conn = sqlite3.connect(str(self.db_path))

        total_tools = conn.execute("SELECT COUNT(*) FROM tool_facets").fetchone()[0]
        total_compositions = conn.execute("SELECT COUNT(*) FROM tool_compositions").fetchone()[0]
        total_executions = conn.execute("SELECT COUNT(*) FROM execution_history").fetchone()[0]

        backends = conn.execute("""
            SELECT backend, COUNT(*) as count,
                   SUM(call_count) as calls,
                   AVG(CASE WHEN call_count > 0 THEN success_count * 1.0 / call_count ELSE 1 END) as avg_success
            FROM tool_facets GROUP BY backend
        """).fetchall()

        top_tools = conn.execute("""
            SELECT tool_id, call_count,
                   CASE WHEN call_count > 0 THEN success_count * 1.0 / call_count ELSE 1 END as success_rate
            FROM tool_facets ORDER BY call_count DESC LIMIT 10
        """).fetchall()

        top_compositions = conn.execute("""
            SELECT source_tool, target_tool, occurrence_count, success_rate
            FROM tool_compositions ORDER BY occurrence_count DESC LIMIT 10
        """).fetchall()

        conn.close()

        return {
            "total_tools": total_tools,
            "total_compositions": total_compositions,
            "total_executions": total_executions,
            "backends": {
                b[0]: {"count": b[1], "calls": b[2] or 0, "avg_success_rate": b[3] or 1.0}
                for b in backends
            },
            "top_tools": [
                {"tool_id": t[0], "calls": t[1], "success_rate": t[2]}
                for t in top_tools
            ],
            "top_compositions": [
                {"source": c[0], "target": c[1], "occurrences": c[2], "success_rate": c[3]}
                for c in top_compositions
            ]
        }

    # ==================== Private Methods ====================

    def _generate_purpose_text(self, tool: Dict[str, Any], backend: str = "") -> str:
        """Generate text describing what the tool does.

        The provider/backend label and human source must enter the embedded
        purpose text: semantic retrieval matches on this text, and a backend
        label kept only as a filter column makes provider-name queries
        (e.g. "cloudmail") match nothing.
        """
        provider = (
            backend
            or tool.get("backend")
            or (tool.get("annotations") or {}).get("backend")
            or ""
        )
        parts = [f"Tool: {tool.get('name', 'unknown')}"]
        if provider:
            parts.append(f"Provider: {provider}")
            parts.append(f"Provided by {provider}.")
        if source := tool.get("source"):
            parts.append(f"Source: {source}")
        if desc := tool.get("description"):
            parts.append(f"Purpose: {desc}")
        if tags := tool.get("tags"):
            parts.append(f"Tags: {', '.join(tags)}")
        if cat := tool.get("category"):
            parts.append(f"Category: {cat}")
        return "\n".join(parts)

    def _generate_input_text(self, tool: Dict[str, Any]) -> str:
        """Generate text describing what inputs the tool needs."""
        schema = tool.get("inputSchema", {})
        props = schema.get("properties", {})
        required = set(schema.get("required", []))

        if not props:
            return f"Tool {tool.get('name', '')} requires no inputs"

        parts = [f"Inputs for {tool.get('name', '')}:"]
        for name, prop in props.items():
            req = "(required)" if name in required else "(optional)"
            ptype = prop.get("type", "any")
            desc = prop.get("description", "")
            parts.append(f"- {name} {req}: {ptype} - {desc}")

        return "\n".join(parts)

    def _generate_output_text(self, tool: Dict[str, Any]) -> str:
        """Generate text describing what the tool produces."""
        schema = tool.get("outputSchema", {})
        props = schema.get("properties", {})

        if not props:
            # Infer from description
            desc = tool.get("description", "")
            return f"Output: {tool.get('name', '')} returns results. {desc}"

        parts = [f"Outputs from {tool.get('name', '')}:"]
        for name, prop in props.items():
            ptype = prop.get("type", "any")
            desc = prop.get("description", "")
            parts.append(f"- {name}: {ptype} - {desc}")

        return "\n".join(parts)

    def _generate_example_text(self, tool: Dict[str, Any]) -> str:
        """Generate synthetic example use cases."""
        name = tool.get("name", "")
        desc = tool.get("description", "")

        # Generate example queries that would trigger this tool
        examples = [
            f"Example: Use {name} when you need to {desc.lower()}" if desc else "",
            f"Example: Call {name} for {self._action_from_name(name)} operations",
        ]

        # Add examples from input schema
        schema = tool.get("inputSchema", {})
        props = schema.get("properties", {})
        if props:
            param_names = list(props.keys())[:3]
            examples.append(f"Example: {name} accepts {', '.join(param_names)}")

        return "\n".join([e for e in examples if e])

    def _action_from_name(self, name: str) -> str:
        """Infer action type from tool name."""
        name_lower = name.lower()
        for action, keywords in self.ACTION_KEYWORDS.items():
            if any(kw in name_lower for kw in keywords):
                return action
        return "general"

    def _extract_types(self, schema: Dict[str, Any]) -> List[str]:
        """Extract typed parameter signatures from schema."""
        props = schema.get("properties", {})
        types = []
        for name, prop in props.items():
            ptype = prop.get("type", "any")
            if ptype == "array" and "items" in prop:
                item_type = prop["items"].get("type", "any")
                ptype = f"array<{item_type}>"
            types.append(f"{ptype}:{name}")
        return types

    def _infer_capabilities(self, tool: Dict[str, Any]) -> List[str]:
        """Infer tool capabilities from schema and description."""
        caps = []
        name = tool.get("name", "").lower()
        desc = (tool.get("description", "") or "").lower()

        # Infer from name/description
        for action, keywords in self.ACTION_KEYWORDS.items():
            if any(kw in name or kw in desc for kw in keywords):
                caps.append(action)

        # Infer from schema
        schema = tool.get("inputSchema", {})
        props = schema.get("properties", {})

        if any("url" in p.lower() for p in props):
            caps.append("web")
        if any("query" in p.lower() for p in props):
            caps.append("query")
        if any("file" in p.lower() or "path" in p.lower() for p in props):
            caps.append("filesystem")
        if any("batch" in p.lower() or "items" in p.lower() for p in props):
            caps.append("batch")

        return list(set(caps))

    def _classify_intent(self, query: str) -> QueryIntent:
        """Classify the intent of a search query."""
        query_lower = query.lower()
        words = query_lower.split()

        # Detect action
        action = "general"
        action_confidence = 0.0
        for act, keywords in self.ACTION_KEYWORDS.items():
            matches = sum(1 for kw in keywords if kw in query_lower)
            if matches > action_confidence:
                action = act
                action_confidence = matches

        # Extract entities (nouns/objects in the query)
        entities = []
        entity_keywords = ["web", "file", "memory", "database", "api", "url", "data",
                         "text", "image", "document", "email", "notification"]
        for ent in entity_keywords:
            if ent in query_lower:
                entities.append(ent)

        # Detect constraints
        constraints = []
        if any(w in query_lower for w in ["fast", "quick", "rapid"]):
            constraints.append("fast")
        if any(w in query_lower for w in ["batch", "bulk", "multiple"]):
            constraints.append("batch")
        if any(w in query_lower for w in ["secure", "authenticated", "auth"]):
            constraints.append("authenticated")

        # Output expectation
        output = "single"
        if any(w in query_lower for w in ["list", "all", "multiple", "many"]):
            output = "list"
        if any(w in query_lower for w in ["stream", "real-time", "live"]):
            output = "stream"

        confidence = min(1.0, (action_confidence + len(entities) * 0.2 + len(constraints) * 0.1) / 3)

        return QueryIntent(
            action=action,
            entities=entities,
            constraints=constraints,
            output_expectation=output,
            confidence=confidence
        )

    def _compute_intent_match(self, intent: QueryIntent, capabilities: List[str], name: str) -> float:
        """Compute how well a tool matches the query intent."""
        score = 0.0

        # Action match
        if intent.action in capabilities:
            score += 0.5
        elif intent.action in name.lower():
            score += 0.3

        # Entity match
        for entity in intent.entities:
            if entity in capabilities or entity in name.lower():
                score += 0.2

        # Constraint match
        for constraint in intent.constraints:
            if constraint in capabilities:
                score += 0.1

        return min(1.0, score)

    async def _build_composition_graph(self, tool_info: List[Dict[str, Any]]):
        """Build composition graph based on type compatibility."""
        conn = sqlite3.connect(str(self.db_path))

        # Find tools with compatible input/output types
        for i, t1 in enumerate(tool_info):
            for t2 in tool_info[i+1:]:
                # Check if t1's outputs could be t2's inputs
                compatibility = self._type_compatibility(t1["output_types"], t2["input_types"])
                if compatibility > 0.5:
                    conn.execute("""
                        INSERT OR IGNORE INTO tool_compositions
                        (source_tool, target_tool, weight, success_rate)
                        VALUES (?, ?, ?, 1.0)
                    """, (t1["tool_id"], t2["tool_id"], compatibility))

                # Check reverse
                compatibility = self._type_compatibility(t2["output_types"], t1["input_types"])
                if compatibility > 0.5:
                    conn.execute("""
                        INSERT OR IGNORE INTO tool_compositions
                        (source_tool, target_tool, weight, success_rate)
                        VALUES (?, ?, ?, 1.0)
                    """, (t2["tool_id"], t1["tool_id"], compatibility))

        conn.commit()
        conn.close()

    def _type_compatibility(self, output_types: List[str], input_types: List[str]) -> float:
        """Compute type compatibility between outputs and inputs."""
        if not output_types or not input_types:
            return 0.3  # Unknown, but possible

        matches = 0
        for out_type in output_types:
            out_base = out_type.split(":")[0]  # e.g., "string" from "string:query"
            for in_type in input_types:
                in_base = in_type.split(":")[0]
                if out_base == in_base or out_base == "any" or in_base == "any":
                    matches += 1
                    break

        return matches / len(input_types) if input_types else 0.0


# Global instance
_advanced_index: Optional[AdvancedToolIndex] = None


def get_advanced_index() -> AdvancedToolIndex:
    """Get or create the global advanced tool index."""
    global _advanced_index
    if _advanced_index is None:
        _advanced_index = AdvancedToolIndex()
    return _advanced_index
