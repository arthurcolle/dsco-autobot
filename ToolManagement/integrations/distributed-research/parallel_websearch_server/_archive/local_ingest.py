"""Local-first hierarchical document ingest with local embeddings.

NO API calls for embeddings - uses sentence-transformers locally.
Only calls API for entity extraction after deduplication.
"""

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import httpx

logger = logging.getLogger(__name__)

# Lazy load sentence-transformers to avoid slow import at module level
_embedding_model = None
EMBEDDING_MODEL = 'jinaai/jina-embeddings-v4'
EMBEDDING_DIMENSIONS = 2048

# OpenRouter config for cheap extraction
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
EXTRACTION_MODEL = "openai/gpt-4.1-mini"  # Cheap and fast
EXTRACTION_MODEL_NANO = "openai/gpt-4.1-nano"  # Even cheaper for simple extractions

# GraphSub meta-type schema
ENTITY_TYPES = [
    "person", "organization", "technology", "concept", "location",
    "event", "document", "system", "protocol", "algorithm",
    "data_structure", "api", "framework", "language", "standard"
]

RELATIONSHIP_TYPES = [
    "uses", "implements", "extends", "depends_on", "created_by",
    "part_of", "related_to", "influences", "precedes", "follows",
    "authored", "maintains", "integrates_with", "competes_with",
    "enables", "requires", "defines", "references", "describes"
]

EXTRACTION_PROMPT = """Extract entities and relationships from this text. Be thorough - extract ALL meaningful entities and relationships.

ENTITY TYPES: {entity_types}
RELATIONSHIP TYPES: {relationship_types}

Return JSON with this exact structure:
{{
  "entities": [
    {{"name": "exact name", "type": "one of the types above", "description": "brief description"}}
  ],
  "relationships": [
    {{"source": "entity name", "target": "entity name", "type": "relationship type", "description": "context"}}
  ]
}}

TEXT:
{text}

JSON:"""


class OpenRouterExtractor:
    """Extract entities and relationships using cheap OpenRouter models."""

    def __init__(self, db_path: str = "knowledge_graph.db"):
        self.api_key = os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not set")
        self.db_path = db_path
        self._init_kg_tables()

    def _init_kg_tables(self):
        """Initialize knowledge graph tables - uses existing schema if present."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if entities table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='entities'")
        if not cursor.fetchone():
            # Use existing schema format from knowledge_graph.py
            cursor.execute("""
                CREATE TABLE entities (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    type TEXT NOT NULL,
                    aliases TEXT NOT NULL,
                    properties TEXT NOT NULL,
                    mentions INTEGER DEFAULT 1,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
            """)
            cursor.execute("CREATE INDEX idx_entities_type ON entities(type)")
            cursor.execute("CREATE INDEX idx_entities_text ON entities(text)")

        # Check if relationships table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='relationships'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE relationships (
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    properties TEXT NOT NULL,
                    weight INTEGER DEFAULT 1,
                    sources TEXT NOT NULL,
                    PRIMARY KEY (source_id, target_id, relation_type)
                )
            """)
            cursor.execute("CREATE INDEX idx_relationships_source ON relationships(source_id)")
            cursor.execute("CREATE INDEX idx_relationships_target ON relationships(target_id)")

        # Source chunks for entities (provenance) - new table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_sources (
                entity_id TEXT NOT NULL,
                chunk_id TEXT NOT NULL,
                PRIMARY KEY (entity_id, chunk_id)
            )
        """)

        conn.commit()
        conn.close()

    async def extract_from_text(self, text: str, model: str = None) -> Dict[str, Any]:
        """Extract entities and relationships from text using OpenRouter."""
        model = model or EXTRACTION_MODEL

        prompt = EXTRACTION_PROMPT.format(
            entity_types=", ".join(ENTITY_TYPES),
            relationship_types=", ".join(RELATIONSHIP_TYPES),
            text=text[:3000]  # Limit text size
        )

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "HTTP-Referer": "https://parallel-ai.local",
                    "X-Title": "Parallel AI KG Builder"
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 2000
                }
            )
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]

        # Parse JSON from response
        try:
            # Handle markdown code blocks
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            result = json.loads(content.strip())
            return {
                "entities": result.get("entities", []),
                "relationships": result.get("relationships", [])
            }
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse extraction response: {content[:200]}")
            return {"entities": [], "relationships": []}

    def _normalize_entity_id(self, name: str) -> str:
        """Generate consistent entity ID from name."""
        return hashlib.md5(name.lower().strip().encode()).hexdigest()[:12]

    def store_extraction(self, extraction: Dict[str, Any], chunk_id: str, content_hash: str) -> Dict[str, int]:
        """Store extracted entities and relationships in the knowledge graph."""
        from datetime import datetime
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()
        stats = {"new_entities": 0, "updated_entities": 0, "new_rels": 0, "updated_rels": 0}
        entity_id_map = {}  # name -> entity_id

        # Store entities (using existing schema: id, text, type, aliases, properties, mentions, first_seen, last_seen)
        for entity in extraction.get("entities", []):
            name = entity.get("name", "").strip()
            if not name or len(name) < 2:
                continue

            entity_id = self._normalize_entity_id(name)
            entity_id_map[name.lower()] = entity_id

            # Check if entity exists
            cursor.execute("SELECT id, mentions FROM entities WHERE id = ?", (entity_id,))
            existing = cursor.fetchone()

            if existing:
                # Update mention count and last_seen
                cursor.execute(
                    "UPDATE entities SET mentions = mentions + 1, last_seen = ? WHERE id = ?",
                    (now, entity_id)
                )
                stats["updated_entities"] += 1
            else:
                # Insert new entity
                properties = {
                    "description": entity.get("description", ""),
                    "source": "extraction"
                }
                cursor.execute("""
                    INSERT INTO entities (id, text, type, aliases, properties, mentions, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    entity_id,
                    name,
                    entity.get("type", "concept"),
                    json.dumps([]),  # aliases
                    json.dumps(properties),
                    1,
                    now,
                    now
                ))
                stats["new_entities"] += 1

            # Link entity to source chunk
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO entity_sources (entity_id, chunk_id) VALUES (?, ?)",
                    (entity_id, chunk_id)
                )
            except sqlite3.IntegrityError:
                pass

        # Store relationships (using existing schema: source_id, target_id, relation_type, properties, weight, sources)
        for rel in extraction.get("relationships", []):
            source_name = rel.get("source", "").strip().lower()
            target_name = rel.get("target", "").strip().lower()

            if source_name not in entity_id_map or target_name not in entity_id_map:
                continue

            source_id = entity_id_map[source_name]
            target_id = entity_id_map[target_name]
            rel_type = rel.get("type", "related_to")

            # Check if relationship exists (composite key)
            cursor.execute(
                "SELECT weight, sources FROM relationships WHERE source_id = ? AND target_id = ? AND relation_type = ?",
                (source_id, target_id, rel_type)
            )
            existing = cursor.fetchone()

            if existing:
                # Update weight and sources
                old_weight, old_sources = existing
                sources = json.loads(old_sources) if old_sources else []
                if chunk_id not in sources:
                    sources.append(chunk_id)
                cursor.execute(
                    "UPDATE relationships SET weight = ?, sources = ? WHERE source_id = ? AND target_id = ? AND relation_type = ?",
                    (old_weight + 1, json.dumps(sources), source_id, target_id, rel_type)
                )
                stats["updated_rels"] += 1
            else:
                properties = {"description": rel.get("description", "")}
                cursor.execute("""
                    INSERT INTO relationships (source_id, target_id, relation_type, properties, weight, sources)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    source_id,
                    target_id,
                    rel_type,
                    json.dumps(properties),
                    1,
                    json.dumps([chunk_id])
                ))
                stats["new_rels"] += 1

        # Cache the extraction
        cursor.execute("""
            INSERT OR REPLACE INTO extraction_cache (content_hash, entities, relationships)
            VALUES (?, ?, ?)
        """, (
            content_hash,
            json.dumps(extraction.get("entities", [])),
            json.dumps(extraction.get("relationships", []))
        ))

        conn.commit()
        conn.close()

        return stats

    def is_extracted(self, content_hash: str) -> bool:
        """Check if content has already been extracted."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM extraction_cache WHERE content_hash = ?", (content_hash,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    async def extract_from_chunks(self, levels: List[str] = None, batch_size: int = 10, max_concurrent: int = 5) -> Dict[str, Any]:
        """Extract entities from chunks at specified levels."""
        levels = levels or ["section", "paragraph"]

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get chunks that haven't been extracted yet
        placeholders = ",".join("?" * len(levels))
        cursor.execute(f"""
            SELECT c.chunk_id, c.content_hash, c.text, c.level
            FROM chunks c
            LEFT JOIN extraction_cache ec ON c.content_hash = ec.content_hash
            WHERE c.level IN ({placeholders}) AND ec.content_hash IS NULL
            ORDER BY c.level, c.position
        """, levels)

        chunks_to_process = cursor.fetchall()
        conn.close()

        total = len(chunks_to_process)
        logger.info(f"Found {total} chunks to extract from (levels: {levels})")

        if total == 0:
            return {"status": "complete", "message": "All chunks already extracted"}

        stats = {"processed": 0, "new_entities": 0, "updated_entities": 0, "new_rels": 0, "updated_rels": 0, "errors": 0}

        # Process in batches with concurrency limit
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_chunk(chunk_data):
            chunk_id, content_hash, text, level = chunk_data

            async with semaphore:
                try:
                    # Use nano model for shorter texts
                    model = EXTRACTION_MODEL_NANO if len(text) < 500 else EXTRACTION_MODEL
                    extraction = await self.extract_from_text(text, model)
                    chunk_stats = self.store_extraction(extraction, chunk_id, content_hash)

                    return {
                        "success": True,
                        **chunk_stats
                    }
                except Exception as e:
                    logger.error(f"Extraction failed for {chunk_id}: {e}")
                    return {"success": False, "error": str(e)}

        # Process all chunks
        for i in range(0, total, batch_size):
            batch = chunks_to_process[i:i+batch_size]
            results = await asyncio.gather(*[process_chunk(c) for c in batch])

            for r in results:
                if r.get("success"):
                    stats["processed"] += 1
                    stats["new_entities"] += r.get("new_entities", 0)
                    stats["updated_entities"] += r.get("updated_entities", 0)
                    stats["new_rels"] += r.get("new_rels", 0)
                    stats["updated_rels"] += r.get("updated_rels", 0)
                else:
                    stats["errors"] += 1

            logger.info(f"Progress: {min(i+batch_size, total)}/{total} - Entities: {stats['new_entities']}, Rels: {stats['new_rels']}")

        return stats

    async def validate_entities(self, sample_size: int = 100, max_concurrent: int = 5) -> Dict[str, Any]:
        """Validate extracted entities by checking against source text."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get entities with their source chunks that haven't been validated
        cursor.execute("""
            SELECT e.id, e.text, e.type, e.properties, es.chunk_id, c.text as chunk_text
            FROM entities e
            JOIN entity_sources es ON e.id = es.entity_id
            JOIN chunks c ON es.chunk_id = c.chunk_id
            WHERE e.properties NOT LIKE '%"validated"%'
            LIMIT ?
        """, (sample_size,))

        entities_to_validate = cursor.fetchall()
        conn.close()

        if not entities_to_validate:
            return {"status": "complete", "message": "No entities to validate"}

        logger.info(f"Validating {len(entities_to_validate)} entities...")

        validation_prompt = """Verify if this entity extraction is correct.

ENTITY: "{entity_name}" (type: {entity_type})
SOURCE TEXT: {source_text}

Is this entity actually mentioned or clearly implied in the source text?
Is the entity type correct?

Return JSON:
{{"valid": true/false, "confidence": 0.0-1.0, "reason": "brief explanation", "suggested_type": "if type is wrong"}}

JSON:"""

        stats = {"validated": 0, "invalid": 0, "errors": 0}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def validate_entity(entity_data):
            entity_id, name, etype, props, chunk_id, chunk_text = entity_data

            async with semaphore:
                try:
                    prompt = validation_prompt.format(
                        entity_name=name,
                        entity_type=etype,
                        source_text=chunk_text[:1500]
                    )

                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            f"{OPENROUTER_BASE_URL}/chat/completions",
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "HTTP-Referer": "https://parallel-ai.local",
                            },
                            json={
                                "model": EXTRACTION_MODEL_NANO,
                                "messages": [{"role": "user", "content": prompt}],
                                "temperature": 0.0,
                                "max_tokens": 200
                            }
                        )
                        response.raise_for_status()
                        data = response.json()

                    content = data["choices"][0]["message"]["content"]

                    # Parse validation result
                    if "```json" in content:
                        content = content.split("```json")[1].split("```")[0]
                    elif "```" in content:
                        content = content.split("```")[1].split("```")[0]

                    result = json.loads(content.strip())

                    # Update entity properties
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()

                    props_dict = json.loads(props) if props else {}
                    props_dict["validated"] = result.get("valid", False)
                    props_dict["validation_confidence"] = result.get("confidence", 0.0)
                    props_dict["validation_reason"] = result.get("reason", "")

                    if result.get("suggested_type") and result.get("suggested_type") != etype:
                        props_dict["suggested_type"] = result.get("suggested_type")

                    cursor.execute(
                        "UPDATE entities SET properties = ? WHERE id = ?",
                        (json.dumps(props_dict), entity_id)
                    )
                    conn.commit()
                    conn.close()

                    return {"valid": result.get("valid", False)}

                except Exception as e:
                    logger.error(f"Validation failed for {name}: {e}")
                    return {"error": str(e)}

        # Validate all entities
        results = await asyncio.gather(*[validate_entity(e) for e in entities_to_validate])

        for r in results:
            if "error" in r:
                stats["errors"] += 1
            elif r.get("valid"):
                stats["validated"] += 1
            else:
                stats["invalid"] += 1

        return stats

    async def validate_relationships(self, sample_size: int = 100, max_concurrent: int = 5) -> Dict[str, Any]:
        """Validate extracted relationships by checking against source text."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get relationships with source entities and chunks
        cursor.execute("""
            SELECT r.source_id, r.target_id, r.relation_type, r.properties, r.sources,
                   e1.text as source_name, e2.text as target_name
            FROM relationships r
            JOIN entities e1 ON r.source_id = e1.id
            JOIN entities e2 ON r.target_id = e2.id
            WHERE r.properties NOT LIKE '%"validated"%'
            LIMIT ?
        """, (sample_size,))

        rels_to_validate = cursor.fetchall()

        # Get chunk texts for each relationship
        rels_with_context = []
        for rel in rels_to_validate:
            source_id, target_id, rel_type, props, sources_json, source_name, target_name = rel
            sources = json.loads(sources_json) if sources_json else []

            if sources:
                cursor.execute("SELECT text FROM chunks WHERE chunk_id = ?", (sources[0],))
                chunk_row = cursor.fetchone()
                chunk_text = chunk_row[0] if chunk_row else ""
            else:
                chunk_text = ""

            rels_with_context.append((source_id, target_id, rel_type, props, source_name, target_name, chunk_text))

        conn.close()

        if not rels_with_context:
            return {"status": "complete", "message": "No relationships to validate"}

        logger.info(f"Validating {len(rels_with_context)} relationships...")

        validation_prompt = """Verify if this relationship extraction is correct.

RELATIONSHIP: "{source}" --[{rel_type}]--> "{target}"
SOURCE TEXT: {source_text}

Is this relationship actually stated or clearly implied in the source text?
Is the relationship type accurate?

Return JSON:
{{"valid": true/false, "confidence": 0.0-1.0, "reason": "brief explanation", "suggested_type": "if type is wrong"}}

JSON:"""

        stats = {"validated": 0, "invalid": 0, "errors": 0}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def validate_rel(rel_data):
            source_id, target_id, rel_type, props, source_name, target_name, chunk_text = rel_data

            async with semaphore:
                try:
                    prompt = validation_prompt.format(
                        source=source_name,
                        target=target_name,
                        rel_type=rel_type,
                        source_text=chunk_text[:1500]
                    )

                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            f"{OPENROUTER_BASE_URL}/chat/completions",
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "HTTP-Referer": "https://parallel-ai.local",
                            },
                            json={
                                "model": EXTRACTION_MODEL_NANO,
                                "messages": [{"role": "user", "content": prompt}],
                                "temperature": 0.0,
                                "max_tokens": 200
                            }
                        )
                        response.raise_for_status()
                        data = response.json()

                    content = data["choices"][0]["message"]["content"]

                    if "```json" in content:
                        content = content.split("```json")[1].split("```")[0]
                    elif "```" in content:
                        content = content.split("```")[1].split("```")[0]

                    result = json.loads(content.strip())

                    # Update relationship properties
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()

                    props_dict = json.loads(props) if props else {}
                    props_dict["validated"] = result.get("valid", False)
                    props_dict["validation_confidence"] = result.get("confidence", 0.0)
                    props_dict["validation_reason"] = result.get("reason", "")

                    cursor.execute(
                        "UPDATE relationships SET properties = ? WHERE source_id = ? AND target_id = ? AND relation_type = ?",
                        (json.dumps(props_dict), source_id, target_id, rel_type)
                    )
                    conn.commit()
                    conn.close()

                    return {"valid": result.get("valid", False)}

                except Exception as e:
                    logger.error(f"Validation failed for {source_name} -> {target_name}: {e}")
                    return {"error": str(e)}

        results = await asyncio.gather(*[validate_rel(r) for r in rels_with_context])

        for r in results:
            if "error" in r:
                stats["errors"] += 1
            elif r.get("valid"):
                stats["validated"] += 1
            else:
                stats["invalid"] += 1

        return stats

    async def run_validation_loop(self, batch_size: int = 50, max_batches: int = 100) -> Dict[str, Any]:
        """Run continuous validation of entities and relationships."""
        total_stats = {
            "entities_validated": 0,
            "entities_invalid": 0,
            "rels_validated": 0,
            "rels_invalid": 0,
            "batches": 0,
            "errors": 0
        }

        for batch in range(max_batches):
            logger.info(f"Validation batch {batch + 1}/{max_batches}")

            # Validate entities
            entity_stats = await self.validate_entities(sample_size=batch_size)
            if entity_stats.get("status") == "complete":
                logger.info("All entities validated")
            else:
                total_stats["entities_validated"] += entity_stats.get("validated", 0)
                total_stats["entities_invalid"] += entity_stats.get("invalid", 0)
                total_stats["errors"] += entity_stats.get("errors", 0)

            # Validate relationships
            rel_stats = await self.validate_relationships(sample_size=batch_size)
            if rel_stats.get("status") == "complete":
                logger.info("All relationships validated")
            else:
                total_stats["rels_validated"] += rel_stats.get("validated", 0)
                total_stats["rels_invalid"] += rel_stats.get("invalid", 0)
                total_stats["errors"] += rel_stats.get("errors", 0)

            total_stats["batches"] += 1

            # Check if done
            if entity_stats.get("status") == "complete" and rel_stats.get("status") == "complete":
                break

            logger.info(f"Batch {batch + 1} complete: entities={entity_stats}, rels={rel_stats}")

        return total_stats

    def get_kg_stats(self) -> Dict[str, Any]:
        """Get knowledge graph statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM entities")
        entity_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM relationships")
        rel_count = cursor.fetchone()[0]

        cursor.execute("SELECT type, COUNT(*) FROM entities GROUP BY type ORDER BY COUNT(*) DESC")
        entity_types = dict(cursor.fetchall())

        cursor.execute("SELECT relation_type, COUNT(*) FROM relationships GROUP BY relation_type ORDER BY COUNT(*) DESC")
        rel_types = dict(cursor.fetchall())

        cursor.execute("SELECT COUNT(*) FROM extraction_cache")
        cached_extractions = cursor.fetchone()[0]

        # Validation stats
        cursor.execute("SELECT COUNT(*) FROM entities WHERE properties LIKE '%\"validated\": true%'")
        entities_validated = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM entities WHERE properties LIKE '%\"validated\": false%'")
        entities_invalid = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM relationships WHERE properties LIKE '%\"validated\": true%'")
        rels_validated = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM relationships WHERE properties LIKE '%\"validated\": false%'")
        rels_invalid = cursor.fetchone()[0]

        # Top entities by mention count
        cursor.execute("SELECT text, type, mentions FROM entities ORDER BY mentions DESC LIMIT 20")
        top_entities = [{"name": r[0], "type": r[1], "mentions": r[2]} for r in cursor.fetchall()]

        conn.close()

        return {
            "total_entities": entity_count,
            "total_relationships": rel_count,
            "cached_extractions": cached_extractions,
            "entity_types": entity_types,
            "relationship_types": rel_types,
            "validation": {
                "entities_validated": entities_validated,
                "entities_invalid": entities_invalid,
                "entities_pending": entity_count - entities_validated - entities_invalid,
                "rels_validated": rels_validated,
                "rels_invalid": rels_invalid,
                "rels_pending": rel_count - rels_validated - rels_invalid,
            },
            "top_entities": top_entities
        }

def get_embedding_model():
    """Lazy load the embedding model."""
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading embedding model ({EMBEDDING_MODEL})...")
        _embedding_model = SentenceTransformer(
            EMBEDDING_MODEL,
            trust_remote_code=True,
            model_kwargs={'default_task': 'retrieval'}
        )
        logger.info(f"Embedding model loaded. Dimensions: {EMBEDDING_DIMENSIONS}")
    return _embedding_model


@dataclass
class HierarchicalChunk:
    """A chunk at any level of the hierarchy."""
    chunk_id: str
    content_hash: str
    text: str
    level: str  # 'document', 'section', 'paragraph', 'sentence'
    parent_id: Optional[str]
    doc_id: str
    position: int  # Position within parent
    char_start: int
    char_end: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[np.ndarray] = None


class LocalIngestPipeline:
    """Hierarchical chunking + local embeddings pipeline."""

    def __init__(self, db_path: str = "knowledge_graph.db"):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self):
        """Initialize SQLite schema for chunks and embeddings."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Chunks table with hierarchy
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                text TEXT NOT NULL,
                level TEXT NOT NULL,
                parent_id TEXT,
                doc_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                char_start INTEGER NOT NULL,
                char_end INTEGER NOT NULL,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(content_hash, level, doc_id)
            )
        """)

        # Embeddings table (separate for efficiency)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                chunk_id TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                model TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id)
            )
        """)

        # Entity extraction cache (to avoid re-extracting)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extraction_cache (
                content_hash TEXT PRIMARY KEY,
                entities TEXT NOT NULL,
                relationships TEXT NOT NULL,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(content_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_level ON chunks(level)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_parent ON chunks(parent_id)")

        conn.commit()
        conn.close()
        logger.info(f"Schema initialized at {self.db_path}")

    def _hash_content(self, text: str) -> str:
        """Generate content hash for deduplication."""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]

    def _generate_chunk_id(self, doc_id: str, level: str, position: int, parent_id: Optional[str] = None) -> str:
        """Generate unique chunk ID."""
        parts = [doc_id, level, str(position)]
        if parent_id:
            parts.append(parent_id[:8])
        return hashlib.md5('|'.join(parts).encode()).hexdigest()[:12]

    def chunk_document_hierarchically(self, text: str, doc_id: str, title: str = "") -> List[HierarchicalChunk]:
        """
        Chunk document into hierarchy:
        - Level 0: Full document
        - Level 1: Sections (split by ## headers or large gaps)
        - Level 2: Paragraphs (split by double newlines)
        - Level 3: Sentences (split by sentence boundaries)
        """
        chunks = []

        # Level 0: Document
        doc_hash = self._hash_content(text)
        doc_chunk_id = self._generate_chunk_id(doc_id, 'document', 0)
        doc_chunk = HierarchicalChunk(
            chunk_id=doc_chunk_id,
            content_hash=doc_hash,
            text=text[:10000],  # Store first 10k chars for document level
            level='document',
            parent_id=None,
            doc_id=doc_id,
            position=0,
            char_start=0,
            char_end=len(text),
            metadata={'title': title, 'total_chars': len(text)}
        )
        chunks.append(doc_chunk)

        # Level 1: Sections (split by markdown headers or large content blocks)
        sections = self._split_into_sections(text)
        for sec_idx, (sec_text, sec_start, sec_end) in enumerate(sections):
            if len(sec_text.strip()) < 50:
                continue

            sec_hash = self._hash_content(sec_text)
            sec_chunk_id = self._generate_chunk_id(doc_id, 'section', sec_idx, doc_chunk_id)
            sec_chunk = HierarchicalChunk(
                chunk_id=sec_chunk_id,
                content_hash=sec_hash,
                text=sec_text[:5000],
                level='section',
                parent_id=doc_chunk_id,
                doc_id=doc_id,
                position=sec_idx,
                char_start=sec_start,
                char_end=sec_end,
                metadata={'section_idx': sec_idx}
            )
            chunks.append(sec_chunk)

            # Level 2: Paragraphs
            paragraphs = self._split_into_paragraphs(sec_text)
            for para_idx, (para_text, para_start, para_end) in enumerate(paragraphs):
                if len(para_text.strip()) < 30:
                    continue

                para_hash = self._hash_content(para_text)
                para_chunk_id = self._generate_chunk_id(doc_id, 'paragraph', para_idx, sec_chunk_id)
                para_chunk = HierarchicalChunk(
                    chunk_id=para_chunk_id,
                    content_hash=para_hash,
                    text=para_text[:2000],
                    level='paragraph',
                    parent_id=sec_chunk_id,
                    doc_id=doc_id,
                    position=para_idx,
                    char_start=sec_start + para_start,
                    char_end=sec_start + para_end,
                    metadata={'para_idx': para_idx}
                )
                chunks.append(para_chunk)

                # Level 3: Sentences (only for paragraphs > 200 chars)
                if len(para_text) > 200:
                    sentences = self._split_into_sentences(para_text)
                    for sent_idx, (sent_text, sent_start, sent_end) in enumerate(sentences):
                        if len(sent_text.strip()) < 20:
                            continue

                        sent_hash = self._hash_content(sent_text)
                        sent_chunk_id = self._generate_chunk_id(doc_id, 'sentence', sent_idx, para_chunk_id)
                        sent_chunk = HierarchicalChunk(
                            chunk_id=sent_chunk_id,
                            content_hash=sent_hash,
                            text=sent_text[:500],
                            level='sentence',
                            parent_id=para_chunk_id,
                            doc_id=doc_id,
                            position=sent_idx,
                            char_start=sec_start + para_start + sent_start,
                            char_end=sec_start + para_start + sent_end,
                            metadata={'sent_idx': sent_idx}
                        )
                        chunks.append(sent_chunk)

        return chunks

    def _split_into_sections(self, text: str) -> List[Tuple[str, int, int]]:
        """Split text into sections by markdown headers or large gaps."""
        import re

        # Split by markdown headers (##, ###, etc.)
        pattern = r'(?=^#{1,4}\s+.+$)'
        parts = re.split(pattern, text, flags=re.MULTILINE)

        if len(parts) <= 1:
            # No headers, split by large gaps (3+ newlines)
            parts = re.split(r'\n{3,}', text)

        sections = []
        pos = 0
        for part in parts:
            if part.strip():
                start = text.find(part, pos)
                end = start + len(part)
                sections.append((part, start, end))
                pos = end

        return sections if sections else [(text, 0, len(text))]

    def _split_into_paragraphs(self, text: str) -> List[Tuple[str, int, int]]:
        """Split text into paragraphs by double newlines."""
        import re

        parts = re.split(r'\n\n+', text)
        paragraphs = []
        pos = 0

        for part in parts:
            if part.strip():
                start = text.find(part, pos)
                if start == -1:
                    start = pos
                end = start + len(part)
                paragraphs.append((part, start, end))
                pos = end

        return paragraphs if paragraphs else [(text, 0, len(text))]

    def _split_into_sentences(self, text: str) -> List[Tuple[str, int, int]]:
        """Split text into sentences."""
        import re

        # Simple sentence splitter
        pattern = r'(?<=[.!?])\s+(?=[A-Z])'
        parts = re.split(pattern, text)

        sentences = []
        pos = 0
        for part in parts:
            if part.strip():
                start = text.find(part, pos)
                if start == -1:
                    start = pos
                end = start + len(part)
                sentences.append((part.strip(), start, end))
                pos = end

        return sentences if sentences else [(text, 0, len(text))]

    def embed_chunks(self, chunks: List[HierarchicalChunk], batch_size: int = 4) -> List[HierarchicalChunk]:
        """Generate embeddings for chunks using local model."""
        import re
        model = get_embedding_model()

        # Get texts - truncate to 512 chars and strip URLs (jina v4 tries to load them as images)
        url_pattern = re.compile(r'https?://\S+')
        texts = [url_pattern.sub('[URL]', c.text[:512]) for c in chunks]

        logger.info(f"Generating embeddings for {len(texts)} chunks (batch_size={batch_size}, max_len=512)...")

        # Batch embed with MPS - small batches to avoid kernel size issues
        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            embeddings = model.encode(batch, show_progress_bar=False)
            all_embeddings.extend(embeddings)
            if (i // batch_size) % 50 == 0:
                logger.info(f"  Progress: {i}/{len(texts)} chunks embedded")

        # Attach embeddings to chunks
        for chunk, emb in zip(chunks, all_embeddings):
            chunk.embedding = emb

        logger.info(f"Generated {len(all_embeddings)} embeddings")
        return chunks

    def store_chunks(self, chunks: List[HierarchicalChunk]) -> Dict[str, int]:
        """Store chunks and embeddings in SQLite with deduplication."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {'new_chunks': 0, 'duplicates': 0, 'embeddings': 0}

        for chunk in chunks:
            # Check for duplicate by content hash
            cursor.execute(
                "SELECT chunk_id FROM chunks WHERE content_hash = ? AND level = ?",
                (chunk.content_hash, chunk.level)
            )
            existing = cursor.fetchone()

            if existing:
                stats['duplicates'] += 1
                continue

            # Insert chunk
            try:
                cursor.execute("""
                    INSERT INTO chunks (chunk_id, content_hash, text, level, parent_id,
                                       doc_id, position, char_start, char_end, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    chunk.chunk_id, chunk.content_hash, chunk.text, chunk.level,
                    chunk.parent_id, chunk.doc_id, chunk.position,
                    chunk.char_start, chunk.char_end, json.dumps(chunk.metadata)
                ))
                stats['new_chunks'] += 1

                # Insert embedding if available
                if chunk.embedding is not None:
                    cursor.execute("""
                        INSERT OR REPLACE INTO embeddings (chunk_id, embedding, model, dimensions)
                        VALUES (?, ?, ?, ?)
                    """, (
                        chunk.chunk_id,
                        chunk.embedding.tobytes(),
                        EMBEDDING_MODEL,
                        len(chunk.embedding)
                    ))
                    stats['embeddings'] += 1

            except sqlite3.IntegrityError:
                stats['duplicates'] += 1

        conn.commit()
        conn.close()

        logger.info(f"Stored: {stats['new_chunks']} new, {stats['duplicates']} duplicates, {stats['embeddings']} embeddings")
        return stats

    def ingest_document(self, text: str, doc_id: str, title: str = "") -> Dict[str, Any]:
        """Full ingest pipeline for a single document."""
        # 1. Hierarchical chunking
        chunks = self.chunk_document_hierarchically(text, doc_id, title)
        logger.info(f"Created {len(chunks)} hierarchical chunks for {doc_id}")

        # 2. Local embeddings
        chunks = self.embed_chunks(chunks)

        # 3. Store with deduplication
        stats = self.store_chunks(chunks)

        return {
            'doc_id': doc_id,
            'title': title,
            'total_chunks': len(chunks),
            'by_level': {
                'document': sum(1 for c in chunks if c.level == 'document'),
                'section': sum(1 for c in chunks if c.level == 'section'),
                'paragraph': sum(1 for c in chunks if c.level == 'paragraph'),
                'sentence': sum(1 for c in chunks if c.level == 'sentence'),
            },
            **stats
        }

    def ingest_corpus(self, corpus_path: str) -> Dict[str, Any]:
        """Ingest entire corpus from directory."""
        from parallel_websearch_server.batch_ingest import prepare_corpus

        corpus_path = Path(corpus_path)
        if not corpus_path.exists():
            raise ValueError(f"Corpus path not found: {corpus_path}")

        documents = prepare_corpus(corpus_path)
        logger.info(f"Found {len(documents)} documents in {corpus_path}")

        total_stats = {
            'documents_processed': 0,
            'total_chunks': 0,
            'new_chunks': 0,
            'duplicates': 0,
            'embeddings': 0,
        }

        for doc in documents:
            # Combine all chunks into full document text
            full_text = '\n\n'.join(chunk.text for chunk in doc.chunks)

            stats = self.ingest_document(full_text, doc.doc_id, doc.title)

            total_stats['documents_processed'] += 1
            total_stats['total_chunks'] += stats['total_chunks']
            total_stats['new_chunks'] += stats['new_chunks']
            total_stats['duplicates'] += stats['duplicates']
            total_stats['embeddings'] += stats['embeddings']

            logger.info(f"[{total_stats['documents_processed']}/{len(documents)}] {doc.filename}: {stats['total_chunks']} chunks")

        return total_stats

    def get_stats(self) -> Dict[str, Any]:
        """Get current database statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT level, COUNT(*) FROM chunks GROUP BY level")
        level_counts = dict(cursor.fetchall())

        cursor.execute("SELECT COUNT(*) FROM chunks")
        total_chunks = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM embeddings")
        total_embeddings = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT doc_id) FROM chunks")
        total_docs = cursor.fetchone()[0]

        conn.close()

        return {
            'total_chunks': total_chunks,
            'total_embeddings': total_embeddings,
            'total_documents': total_docs,
            'by_level': level_counts,
        }

    def search_similar(self, query: str, top_k: int = 10, level: str = 'paragraph') -> List[Dict[str, Any]]:
        """Search for similar chunks using embeddings."""
        model = get_embedding_model()
        query_embedding = model.encode([query])[0]

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get all embeddings at the specified level
        cursor.execute("""
            SELECT c.chunk_id, c.text, c.doc_id, c.metadata, e.embedding
            FROM chunks c
            JOIN embeddings e ON c.chunk_id = e.chunk_id
            WHERE c.level = ?
        """, (level,))

        results = []
        for row in cursor.fetchall():
            chunk_id, text, doc_id, metadata, emb_bytes = row
            embedding = np.frombuffer(emb_bytes, dtype=np.float32)

            # Cosine similarity
            similarity = np.dot(query_embedding, embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(embedding)
            )

            results.append({
                'chunk_id': chunk_id,
                'text': text,
                'doc_id': doc_id,
                'metadata': json.loads(metadata) if metadata else {},
                'similarity': float(similarity),
            })

        conn.close()

        # Sort by similarity and return top_k
        results.sort(key=lambda x: x['similarity'], reverse=True)
        return results[:top_k]


# CLI interface
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    pipeline = LocalIngestPipeline()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "ingest" and len(sys.argv) > 2:
            corpus_path = sys.argv[2]
            print(f"Ingesting corpus from {corpus_path}...")
            stats = pipeline.ingest_corpus(corpus_path)
            print(f"\nResults: {json.dumps(stats, indent=2)}")

        elif cmd == "extract":
            print("Starting entity extraction from chunks...")
            extractor = OpenRouterExtractor()
            stats = asyncio.run(extractor.extract_from_chunks(
                levels=["section", "paragraph"],
                batch_size=10,
                max_concurrent=5
            ))
            print(f"\nExtraction results: {json.dumps(stats, indent=2)}")
            print(f"\nKG Stats: {json.dumps(extractor.get_kg_stats(), indent=2)}")

        elif cmd == "kg-stats":
            extractor = OpenRouterExtractor()
            print(json.dumps(extractor.get_kg_stats(), indent=2))

        elif cmd == "validate":
            print("Starting validation of entities and relationships...")
            extractor = OpenRouterExtractor()
            stats = asyncio.run(extractor.run_validation_loop(
                batch_size=50,
                max_batches=200
            ))
            print(f"\nValidation results: {json.dumps(stats, indent=2)}")

        elif cmd == "stats":
            print(json.dumps(pipeline.get_stats(), indent=2))

        else:
            # Legacy: treat as corpus path
            print(f"Ingesting corpus from {cmd}...")
            stats = pipeline.ingest_corpus(cmd)
            print(f"\nResults: {json.dumps(stats, indent=2)}")

    else:
        print("Usage:")
        print("  python -m parallel_websearch_server.local_ingest ingest <corpus_path>")
        print("  python -m parallel_websearch_server.local_ingest extract")
        print("  python -m parallel_websearch_server.local_ingest stats")
        print("  python -m parallel_websearch_server.local_ingest kg-stats")
        print("\nCurrent stats:")
        print(json.dumps(pipeline.get_stats(), indent=2))
