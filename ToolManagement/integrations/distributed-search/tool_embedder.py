"""
Tool Schema Embedder
Uses Jina Embeddings v3 via MCP to embed all tool schemas
"""

import asyncio
import sqlite3
import json
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ToolEmbedder:
    """Embed tool schemas using Jina embeddings v3"""

    def __init__(self, db_path: str = "tool_index.db"):
        self.db_path = db_path
        self.conn = None
        self.jina_embed_func = None  # Will be set by caller
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency

        # Create schema
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tool_embeddings (
                tool_id TEXT PRIMARY KEY,
                server_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                embedding BLOB NOT NULL,
                schema_json TEXT NOT NULL,
                category TEXT NOT NULL,
                usage_count INTEGER DEFAULT 0,
                avg_latency_ms REAL DEFAULT 0.0,
                last_used TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_server
            ON tool_embeddings(server_id)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_category
            ON tool_embeddings(category)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_usage
            ON tool_embeddings(usage_count DESC)
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tool_usage_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                query TEXT NOT NULL,
                tool_sequence TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_session
            ON tool_usage_log(session_id)
        """)

        self.conn.commit()

    def set_embed_function(self, func):
        """Set the Jina embedding function (from MCP)"""
        self.jina_embed_func = func

    async def embed_all_tools(self, mcp_registry) -> int:
        """
        Embed all tools from all connected MCP servers.

        Args:
            mcp_registry: MCPRegistry instance with connected servers

        Returns:
            Number of tools embedded
        """
        logger.info("Starting tool embedding process...")

        # Get all tools from all connected servers
        all_tools = []
        for server_id, server_state in mcp_registry.connections.items():
            if not server_state.tools:
                continue

            for tool in server_state.tools:
                # Tools are already dictionaries
                tool_name = tool.get('name', tool.get('tool_name', 'unknown'))
                schema = tool.get('inputSchema', tool.get('input_schema', tool.get('schema', {})))
                description = tool.get('description', '')

                all_tools.append({
                    'server_id': server_id,
                    'tool_name': tool_name,
                    'schema': schema,
                    'description': description
                })

        if not all_tools:
            logger.warning("No tools found to embed")
            return 0

        logger.info(f"Found {len(all_tools)} tools across {len(mcp_registry.servers)} servers")

        # Create rich descriptions for embedding
        embedding_inputs = []
        tool_metadata = []

        for tool in all_tools:
            tool_id = f"{tool['server_id']}:{tool['tool_name']}"

            # Create embedding text
            embed_text = self._create_embedding_text(tool)
            embedding_inputs.append(embed_text)

            # Store metadata
            tool_metadata.append({
                'tool_id': tool_id,
                'server_id': tool['server_id'],
                'tool_name': tool['tool_name'],
                'schema': tool['schema'],
                'description': tool['description'],
                'category': self._categorize_tool(tool['description'])
            })

        # Batch embed using Jina (max 1000 at a time)
        logger.info(f"Embedding {len(embedding_inputs)} tools...")

        embeddings = []
        batch_size = 100  # Process in batches of 100

        for i in range(0, len(embedding_inputs), batch_size):
            batch = embedding_inputs[i:i + batch_size]

            try:
                result = await self.jina_embed_func({
                    "inputs": batch,
                    "model": "jina-embeddings-v3",
                    "task": "retrieval.passage",
                    "dimensions": 1024,
                    "late_chunking": False,
                    "normalize": True
                })

                # Extract embeddings
                batch_embeddings = [
                    np.array(item['embedding'], dtype=np.float32)
                    for item in result['data']
                ]
                embeddings.extend(batch_embeddings)

                logger.info(f"Embedded batch {i//batch_size + 1}/{(len(embedding_inputs)-1)//batch_size + 1}")

            except Exception as e:
                logger.error(f"Error embedding batch {i//batch_size + 1}: {e}")
                # Use zero vectors as fallback
                embeddings.extend([np.zeros(1024, dtype=np.float32)] * len(batch))

        # Store in database
        logger.info("Storing embeddings in database...")

        for metadata, embedding in zip(tool_metadata, embeddings):
            self._store_embedding(
                tool_id=metadata['tool_id'],
                server_id=metadata['server_id'],
                tool_name=metadata['tool_name'],
                embedding=embedding,
                schema=metadata['schema'],
                description=metadata['description'],
                category=metadata['category']
            )

        self.conn.commit()
        logger.info(f"✅ Successfully embedded {len(all_tools)} tools")

        return len(all_tools)

    def _create_embedding_text(self, tool: dict) -> str:
        """
        Create rich text representation for embedding.

        Format:
        Tool: {name}
        Category: {auto_category}
        Description: {description}
        Parameters: {param_names}
        """
        parts = [f"Tool: {tool['tool_name']}"]

        # Add description
        if desc := tool.get('description', ''):
            parts.append(f"Description: {desc}")

            # Extract use cases if present
            if 'use this when' in desc.lower():
                parts.append(desc)

        # Add parameters
        if schema := tool.get('schema', {}):
            if props := schema.get('properties', {}):
                param_names = ', '.join(props.keys())
                parts.append(f"Parameters: {param_names}")

        # Add category
        category = self._categorize_tool(tool.get('description', ''))
        parts.append(f"Category: {category}")

        return '\n'.join(parts)

    def _categorize_tool(self, description: str) -> str:
        """Auto-categorize tools based on description"""
        desc_lower = description.lower()

        if any(kw in desc_lower for kw in ['search', 'find', 'query', 'lookup', 'explore']):
            return 'search'
        elif any(kw in desc_lower for kw in ['read', 'extract', 'fetch', 'download', 'get', 'retrieve']):
            return 'extraction'
        elif any(kw in desc_lower for kw in ['analyze', 'summarize', 'generate', 'answer', 'reason']):
            return 'analysis'
        elif any(kw in desc_lower for kw in ['write', 'create', 'update', 'modify', 'save', 'store']):
            return 'mutation'
        elif any(kw in desc_lower for kw in ['compare', 'diff', 'similar', 'match']):
            return 'comparison'
        else:
            return 'utility'

    def _store_embedding(
        self,
        tool_id: str,
        server_id: str,
        tool_name: str,
        embedding: np.ndarray,
        schema: dict,
        description: str,
        category: str
    ):
        """Store embedding in database"""

        # Convert numpy array to bytes
        embedding_bytes = embedding.tobytes()
        schema_json = json.dumps(schema)

        # Upsert (insert or replace)
        self.conn.execute("""
            INSERT OR REPLACE INTO tool_embeddings
            (tool_id, server_id, tool_name, embedding, schema_json, category, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            tool_id,
            server_id,
            tool_name,
            embedding_bytes,
            schema_json,
            category,
            datetime.now()
        ))

    def get_all_embeddings(self) -> Dict[str, np.ndarray]:
        """Get all embeddings as dict {tool_id: embedding}"""

        cursor = self.conn.execute("""
            SELECT tool_id, embedding
            FROM tool_embeddings
        """)

        embeddings = {}
        for row in cursor:
            tool_id = row[0]
            embedding_bytes = row[1]
            embedding = np.frombuffer(embedding_bytes, dtype=np.float32)
            embeddings[tool_id] = embedding

        return embeddings

    def get_tool_schema(self, tool_id: str) -> Optional[dict]:
        """Get tool schema by ID"""

        cursor = self.conn.execute("""
            SELECT schema_json
            FROM tool_embeddings
            WHERE tool_id = ?
        """, (tool_id,))

        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return None

    def get_all_tool_ids(self) -> List[str]:
        """Get all tool IDs"""

        cursor = self.conn.execute("""
            SELECT tool_id
            FROM tool_embeddings
            ORDER BY usage_count DESC
        """)

        return [row[0] for row in cursor]

    def increment_usage(self, tool_id: str, latency_ms: float = 0.0):
        """Increment usage count for a tool"""

        # Update usage count and rolling average latency
        self.conn.execute("""
            UPDATE tool_embeddings
            SET usage_count = usage_count + 1,
                avg_latency_ms = (avg_latency_ms * usage_count + ?) / (usage_count + 1),
                last_used = ?
            WHERE tool_id = ?
        """, (latency_ms, datetime.now(), tool_id))

        self.conn.commit()

    def log_tool_sequence(self, session_id: str, query: str, tool_sequence: List[str]):
        """Log a tool usage sequence for graph learning"""

        self.conn.execute("""
            INSERT INTO tool_usage_log (session_id, query, tool_sequence)
            VALUES (?, ?, ?)
        """, (session_id, query, json.dumps(tool_sequence)))

        self.conn.commit()

    def get_hot_tier(self, top_n: int = 10) -> List[str]:
        """Get top N most-used tools"""

        cursor = self.conn.execute("""
            SELECT tool_id
            FROM tool_embeddings
            ORDER BY usage_count DESC
            LIMIT ?
        """, (top_n,))

        return [row[0] for row in cursor]

    def get_stats(self) -> dict:
        """Get embedding statistics"""

        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_tools,
                COUNT(DISTINCT server_id) as total_servers,
                SUM(usage_count) as total_usages,
                AVG(usage_count) as avg_usage_per_tool
            FROM tool_embeddings
        """)

        row = cursor.fetchone()

        return {
            'total_tools': row[0],
            'total_servers': row[1],
            'total_usages': row[2],
            'avg_usage_per_tool': row[3]
        }

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
