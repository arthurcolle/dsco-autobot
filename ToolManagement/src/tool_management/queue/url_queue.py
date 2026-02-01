"""
URL Queue for capturing and processing URLs from search results.

Captures URLs encountered during registry searches and associates them
with user accounts/sessions for future processing.
"""

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class URLStatus(str, Enum):
    """URL processing states."""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class URLSource(str, Enum):
    """Source of the URL."""
    SEARCH = "search"
    READ = "read"
    USER_INPUT = "user_input"
    CRAWL = "crawl"
    API = "api"


@dataclass
class QueuedURL:
    """Represents a URL in the processing queue."""
    id: str
    url: str
    source: URLSource
    source_query: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    status: URLStatus = URLStatus.PENDING
    title: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None  # Cached page content
    content_hash: Optional[str] = None
    processed_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    priority: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "url": self.url,
            "source": self.source.value,
            "source_query": self.source_query,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "status": self.status.value,
            "title": self.title,
            "description": self.description,
            "content": self.content,
            "content_hash": self.content_hash,
            "processed_data": self.processed_data,
            "error": self.error,
            "priority": self.priority,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueuedURL":
        return cls(
            id=data["id"],
            url=data["url"],
            source=URLSource(data.get("source", "search")),
            source_query=data.get("source_query"),
            user_id=data.get("user_id"),
            session_id=data.get("session_id"),
            status=URLStatus(data.get("status", "pending")),
            title=data.get("title"),
            description=data.get("description"),
            content=data.get("content"),
            content_hash=data.get("content_hash"),
            processed_data=data.get("processed_data"),
            error=data.get("error"),
            priority=data.get("priority", 0),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None,
            processed_at=datetime.fromisoformat(data["processed_at"]) if data.get("processed_at") else None,
            metadata=data.get("metadata", {}),
        )


class URLQueue:
    """
    Durable URL queue with SQLite backend.

    Captures URLs from search results and other sources,
    associates them with users/sessions, and manages processing state.
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = str(Path.home() / ".tool_management" / "url_queue.db")

        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """Get thread-local database connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        try:
            yield self._local.conn
        except Exception:
            self._local.conn.rollback()
            raise
        else:
            self._local.conn.commit()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queued_urls (
                    id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    url_hash TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'search',
                    source_query TEXT,
                    user_id TEXT,
                    session_id TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    title TEXT,
                    description TEXT,
                    content TEXT,
                    content_hash TEXT,
                    processed_data JSON,
                    error TEXT,
                    priority INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    processed_at TEXT,
                    metadata JSON
                )
            """)

            # Migration: add content column if it doesn't exist
            try:
                conn.execute("ALTER TABLE queued_urls ADD COLUMN content TEXT")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Indexes for efficient queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_status ON queued_urls(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_user ON queued_urls(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_session ON queued_urls(session_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_hash ON queued_urls(url_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_priority ON queued_urls(priority DESC, created_at ASC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_source ON queued_urls(source)")

    def _url_hash(self, url: str) -> str:
        """Generate hash for URL deduplication."""
        import hashlib
        # Normalize URL before hashing
        normalized = url.lower().rstrip("/")
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def enqueue(
        self,
        url: str,
        source: URLSource = URLSource.SEARCH,
        source_query: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        content: Optional[str] = None,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        dedupe: bool = True,
    ) -> Optional[QueuedURL]:
        """
        Add a URL to the queue.

        Args:
            url: The URL to queue
            source: Where the URL came from
            source_query: The search query that found this URL
            user_id: User account to associate with
            session_id: Session to associate with
            title: Page title if known
            description: Page description/snippet if known
            content: Fetched page content to cache
            priority: Processing priority (higher = sooner)
            metadata: Additional metadata
            dedupe: Skip if URL already exists for this user/session

        Returns:
            QueuedURL if added, None if skipped (duplicate)
        """
        url_hash = self._url_hash(url)

        with self._get_connection() as conn:
            # Check for duplicates if deduping
            if dedupe:
                cursor = conn.execute("""
                    SELECT id FROM queued_urls
                    WHERE url_hash = ? AND (user_id = ? OR (user_id IS NULL AND ? IS NULL))
                    AND (session_id = ? OR (session_id IS NULL AND ? IS NULL))
                """, (url_hash, user_id, user_id, session_id, session_id))

                if cursor.fetchone():
                    logger.debug(f"URL already queued: {url[:50]}...")
                    return None

            queued_url = QueuedURL(
                id=str(uuid.uuid4()),
                url=url,
                source=source,
                source_query=source_query,
                user_id=user_id,
                session_id=session_id,
                title=title,
                description=description,
                content=content,
                priority=priority,
                metadata=metadata or {},
            )

            conn.execute("""
                INSERT INTO queued_urls
                (id, url, url_hash, source, source_query, user_id, session_id,
                 status, title, description, content, priority, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                queued_url.id,
                queued_url.url,
                url_hash,
                queued_url.source.value,
                queued_url.source_query,
                queued_url.user_id,
                queued_url.session_id,
                queued_url.status.value,
                queued_url.title,
                queued_url.description,
                queued_url.content,
                queued_url.priority,
                queued_url.created_at.isoformat(),
                json.dumps(queued_url.metadata),
            ))

            logger.info(f"Queued URL: {url[:50]}... (user={user_id}, session={session_id})")
            return queued_url

    def enqueue_batch(
        self,
        urls: List[Dict[str, Any]],
        source: URLSource = URLSource.SEARCH,
        source_query: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        dedupe: bool = True,
    ) -> List[QueuedURL]:
        """
        Add multiple URLs to the queue efficiently.

        Args:
            urls: List of dicts with 'url', optionally 'title', 'description', 'content', 'metadata'
            source: Source for all URLs
            source_query: Query that found these URLs
            user_id: User to associate with
            session_id: Session to associate with
            dedupe: Skip duplicates

        Returns:
            List of successfully queued URLs
        """
        queued = []
        for url_data in urls:
            if isinstance(url_data, str):
                url_data = {"url": url_data}

            result = self.enqueue(
                url=url_data["url"],
                source=source,
                source_query=source_query,
                user_id=user_id,
                session_id=session_id,
                title=url_data.get("title"),
                description=url_data.get("description") or url_data.get("snippet"),
                content=url_data.get("content"),
                priority=int(url_data.get("priority", 0)),
                metadata=url_data.get("metadata"),
                dedupe=dedupe,
            )
            if result:
                queued.append(result)

        logger.info(f"Batch queued {len(queued)}/{len(urls)} URLs")
        return queued

    def get(self, url_id: str) -> Optional[QueuedURL]:
        """Get a queued URL by ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM queued_urls WHERE id = ?", (url_id,)
            )
            row = cursor.fetchone()
            if row:
                return self._row_to_url(row)
        return None

    def get_by_url(
        self,
        url: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Optional[QueuedURL]:
        """Get a queued URL by URL string."""
        url_hash = self._url_hash(url)
        with self._get_connection() as conn:
            query = "SELECT * FROM queued_urls WHERE url_hash = ?"
            params = [url_hash]

            if user_id is not None:
                query += " AND user_id = ?"
                params.append(user_id)
            if session_id is not None:
                query += " AND session_id = ?"
                params.append(session_id)

            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            if row:
                return self._row_to_url(row)
        return None

    def list_urls(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[URLStatus] = None,
        source: Optional[URLSource] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[QueuedURL]:
        """List queued URLs with filters."""
        with self._get_connection() as conn:
            query = "SELECT * FROM queued_urls WHERE 1=1"
            params = []

            if user_id is not None:
                query += " AND user_id = ?"
                params.append(user_id)
            if session_id is not None:
                query += " AND session_id = ?"
                params.append(session_id)
            if status is not None:
                query += " AND status = ?"
                params.append(status.value)
            if source is not None:
                query += " AND source = ?"
                params.append(source.value)

            query += " ORDER BY priority DESC, created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor = conn.execute(query, params)
            return [self._row_to_url(row) for row in cursor.fetchall()]

    def get_next_pending(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Optional[QueuedURL]:
        """Get the next pending URL for processing."""
        with self._get_connection() as conn:
            query = """
                SELECT * FROM queued_urls
                WHERE status = 'pending'
            """
            params = []

            if user_id is not None:
                query += " AND user_id = ?"
                params.append(user_id)
            if session_id is not None:
                query += " AND session_id = ?"
                params.append(session_id)

            query += " ORDER BY priority DESC, created_at ASC LIMIT 1"

            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            if row:
                return self._row_to_url(row)
        return None

    def update_status(
        self,
        url_id: str,
        status: URLStatus,
        error: Optional[str] = None,
        processed_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update the status of a queued URL."""
        with self._get_connection() as conn:
            now = datetime.utcnow().isoformat()

            updates = ["status = ?", "updated_at = ?"]
            params = [status.value, now]

            if error is not None:
                updates.append("error = ?")
                params.append(error)

            if processed_data is not None:
                updates.append("processed_data = ?")
                params.append(json.dumps(processed_data))

            if status == URLStatus.COMPLETED:
                updates.append("processed_at = ?")
                params.append(now)

            params.append(url_id)

            cursor = conn.execute(
                f"UPDATE queued_urls SET {', '.join(updates)} WHERE id = ?",
                params
            )
            return cursor.rowcount > 0

    def count(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[URLStatus] = None,
    ) -> int:
        """Count queued URLs."""
        with self._get_connection() as conn:
            query = "SELECT COUNT(*) FROM queued_urls WHERE 1=1"
            params = []

            if user_id is not None:
                query += " AND user_id = ?"
                params.append(user_id)
            if session_id is not None:
                query += " AND session_id = ?"
                params.append(session_id)
            if status is not None:
                query += " AND status = ?"
                params.append(status.value)

            cursor = conn.execute(query, params)
            return cursor.fetchone()[0]

    def stats(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get queue statistics."""
        with self._get_connection() as conn:
            base_query = "SELECT status, COUNT(*) as count FROM queued_urls WHERE 1=1"
            params = []

            if user_id is not None:
                base_query += " AND user_id = ?"
                params.append(user_id)
            if session_id is not None:
                base_query += " AND session_id = ?"
                params.append(session_id)

            base_query += " GROUP BY status"

            cursor = conn.execute(base_query, params)
            status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

            # Get source breakdown
            source_query = base_query.replace("status", "source")
            cursor = conn.execute(source_query, params)
            source_counts = {row["source"]: row["count"] for row in cursor.fetchall()}

            total = sum(status_counts.values())

            return {
                "total": total,
                "by_status": status_counts,
                "by_source": source_counts,
                "pending": status_counts.get("pending", 0),
                "completed": status_counts.get("completed", 0),
                "failed": status_counts.get("failed", 0),
            }

    def delete(self, url_id: str) -> bool:
        """Delete a queued URL."""
        with self._get_connection() as conn:
            cursor = conn.execute("DELETE FROM queued_urls WHERE id = ?", (url_id,))
            return cursor.rowcount > 0

    def cleanup_old(self, days: int = 30, status: Optional[URLStatus] = None) -> int:
        """Remove old entries from the queue."""
        with self._get_connection() as conn:
            query = "DELETE FROM queued_urls WHERE created_at < datetime('now', ?)"
            params = [f"-{days} days"]

            if status is not None:
                query += " AND status = ?"
                params.append(status.value)

            cursor = conn.execute(query, params)
            count = cursor.rowcount
            logger.info(f"Cleaned up {count} old URL entries")
            return count

    def _row_to_url(self, row: sqlite3.Row) -> QueuedURL:
        """Convert database row to QueuedURL object."""
        return QueuedURL(
            id=row["id"],
            url=row["url"],
            source=URLSource(row["source"]),
            source_query=row["source_query"],
            user_id=row["user_id"],
            session_id=row["session_id"],
            status=URLStatus(row["status"]),
            title=row["title"],
            description=row["description"],
            content=row["content"] if "content" in row.keys() else None,
            content_hash=row["content_hash"],
            processed_data=json.loads(row["processed_data"]) if row["processed_data"] else None,
            error=row["error"],
            priority=row["priority"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else datetime.utcnow(),
            updated_at=datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
            processed_at=datetime.fromisoformat(row["processed_at"]) if row["processed_at"] else None,
            metadata=json.loads(row["metadata"]) if row["metadata"] else {},
        )


# Global singleton
_url_queue: Optional[URLQueue] = None
_url_queue_lock = threading.Lock()


def get_url_queue(db_path: Optional[str] = None) -> URLQueue:
    """Get the global URL queue instance."""
    global _url_queue
    with _url_queue_lock:
        if _url_queue is None:
            _url_queue = URLQueue(db_path)
        return _url_queue
