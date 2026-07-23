"""
Turso Database module for MCP Chat
Handles conversation and message persistence using libsql
"""

import os
import json
from typing import List, Optional, Dict, Any
from datetime import datetime

import libsql_client
from aiohttp import client_exceptions


def get_turso_url_candidates(url: str) -> List[str]:
    """
    Return all connection URL variants we should try for a Turso database.

    Newer libsql deployments often disable WebSockets, which means the
    canonical libsql:// URLs fail during the HTTP 1.1 upgrade. In that case
    retrying with HTTPS (Hrana over HTTP) usually works, so we include it as
    a fallback candidate.
    """
    urls = [url]
    if url.startswith("libsql://"):
        urls.append(f"https://{url[len('libsql://'):]}")
    return urls


class TursoDatabase:
    """Database handler for Turso/libSQL"""

    def __init__(self, url: str, auth_token: str):
        """
        Initialize Turso database connection

        Args:
            url: Turso database URL (e.g., libsql://your-db.turso.io)
            auth_token: Turso authentication token
        """
        self.url = url
        self.auth_token = auth_token
        self.client = None

    async def _close_client(self):
        """Close the current libsql client if it exists."""
        if self.client:
            await self.client.close()
            self.client = None

    async def connect(self):
        """Establish database connection"""
        last_error: Optional[Exception] = None
        candidates = get_turso_url_candidates(self.url)

        for idx, candidate in enumerate(candidates):
            try:
                self.client = libsql_client.create_client(
                    url=candidate,
                    auth_token=self.auth_token
                )
                await self.create_tables()
                if candidate != self.url:
                    print(
                        "INFO: Turso WebSocket failed with HTTP 505, "
                        f"using HTTPS fallback {candidate}"
                    )
                    self.url = candidate
                return
            except client_exceptions.WSServerHandshakeError as exc:
                await self._close_client()
                last_error = exc
                is_last_attempt = idx == len(candidates) - 1
                if exc.status == 505 and not is_last_attempt:
                    print("INFO: Turso WebSocket handshake returned HTTP 505. Retrying with HTTPS...")
                    continue
                raise
            except Exception as exc:
                await self._close_client()
                last_error = exc
                raise

        if last_error:
            raise last_error

    async def create_tables(self):
        """Create database tables if they don't exist"""
        # Conversations table
        await self.client.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                title TEXT,
                model TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata TEXT
            )
        """)

        # Messages table
        await self.client.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_calls TEXT,
                thinking TEXT,
                metadata TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
            )
        """)

        # Ensure older deployments have the metadata column
        await self._ensure_messages_metadata_column()

        # Create index for faster lookups
        await self.client.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_conversation
            ON messages(conversation_id)
        """)

        await self.client.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_updated
            ON conversations(updated_at DESC)
        """)

    async def create_conversation(
        self,
        conversation_id: str,
        title: Optional[str] = None,
        model: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new conversation"""
        now = datetime.utcnow().isoformat()

        await self.client.execute(
            """
            INSERT INTO conversations (id, title, model, created_at, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                conversation_id,
                title or "New conversation",
                model,
                now,
                now,
                json.dumps(metadata) if metadata else None
            ]
        )

        return {
            "id": conversation_id,
            "title": title or "New conversation",
            "model": model,
            "created_at": now,
            "updated_at": now,
            "metadata": metadata
        }

    async def _ensure_messages_metadata_column(self) -> None:
        """Ensure the messages table has a metadata column (adds it if missing)."""
        try:
            result = await self.client.execute("PRAGMA table_info(messages)")
            columns = {row[1] for row in result.rows}
            if "metadata" not in columns:
                await self.client.execute("ALTER TABLE messages ADD COLUMN metadata TEXT")
        except Exception:
            # Best-effort; missing metadata column is non-fatal
            pass

    async def add_message(
        self,
        conversation_id: str,
        role: str,
        content: str,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        thinking: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Add a message to a conversation"""
        now = datetime.utcnow().isoformat()

        result = await self.client.execute(
            """
            INSERT INTO messages (conversation_id, role, content, tool_calls, thinking, metadata, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                conversation_id,
                role,
                content,
                json.dumps(tool_calls) if tool_calls else None,
                thinking,
                json.dumps(metadata) if metadata else None,
                now
            ]
        )

        # Update conversation updated_at
        await self.client.execute(
            "UPDATE conversations SET updated_at = ? WHERE id = ?",
            [now, conversation_id]
        )

        return {
            "id": result.last_insert_rowid,
            "conversation_id": conversation_id,
            "role": role,
            "content": content,
            "tool_calls": tool_calls,
            "thinking": thinking,
            "metadata": metadata,
            "timestamp": now
        }

    async def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Get a conversation by ID"""
        result = await self.client.execute(
            "SELECT * FROM conversations WHERE id = ?",
            [conversation_id]
        )

        if not result.rows:
            return None

        row = result.rows[0]
        return {
            "id": row[0],
            "title": row[1],
            "model": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "metadata": json.loads(row[5]) if row[5] else None
        }

    async def get_conversation_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Get all messages for a conversation"""
        result = await self.client.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY timestamp ASC",
            [conversation_id]
        )

        messages = []
        for row in result.rows:
            messages.append({
                "id": row[0],
                "conversation_id": row[1],
                "role": row[2],
                "content": row[3],
                "tool_calls": json.loads(row[4]) if row[4] else None,
                "thinking": row[5],
                "metadata": json.loads(row[6]) if row[6] else None,
                "timestamp": row[7]
            })

        return messages

    async def list_conversations(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """List all conversations ordered by most recent"""
        result = await self.client.execute(
            "SELECT * FROM conversations ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            [limit, offset]
        )

        conversations = []
        for row in result.rows:
            conversations.append({
                "id": row[0],
                "title": row[1],
                "model": row[2],
                "created_at": row[3],
                "updated_at": row[4],
                "metadata": json.loads(row[5]) if row[5] else None
            })

        return conversations

    async def update_conversation_title(self, conversation_id: str, title: str):
        """Update conversation title"""
        now = datetime.utcnow().isoformat()
        await self.client.execute(
            "UPDATE conversations SET title = ?, updated_at = ? WHERE id = ?",
            [title, now, conversation_id]
        )

    async def delete_conversation(self, conversation_id: str):
        """Delete a conversation and all its messages"""
        await self.client.execute(
            "DELETE FROM conversations WHERE id = ?",
            [conversation_id]
        )

    async def close(self):
        """Close database connection"""
        await self._close_client()


# Global database instance
db: Optional[TursoDatabase] = None


async def init_database() -> Optional[TursoDatabase]:
    """Initialize database from environment variables"""
    global db

    turso_url = os.getenv("TURSO_DATABASE_URL")
    turso_token = os.getenv("TURSO_AUTH_TOKEN")

    if not turso_url or not turso_token:
        print("WARNING: Turso not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN")
        return None

    try:
        db = TursoDatabase(turso_url, turso_token)
        await db.connect()
        print(f"Connected to Turso database: {turso_url}")
        return db
    except Exception as e:
        print(f"WARNING: Failed to connect to Turso database: {e}")
        print("Application will continue without database persistence")
        return None


async def get_database() -> Optional[TursoDatabase]:
    """Get the database instance"""
    return db
