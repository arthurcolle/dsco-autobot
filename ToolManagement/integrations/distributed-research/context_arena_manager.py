"""
Context Arena Manager
Manages isolated context arenas that keep tool outputs separate from main conversation
"""

import hashlib
import json
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict


class ArenaType(Enum):
    """Types of context arenas"""
    SEARCH = "search"
    CODE = "code"
    DOCUMENTATION = "docs"
    MEMORY = "memory"
    DATABASE = "database"
    LOGS = "logs"
    CUSTOM = "custom"


@dataclass
class Document:
    """Document stored in an arena"""
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    timestamp: datetime
    embedding: Optional[np.ndarray] = None
    token_count: int = 0
    access_count: int = 0
    last_accessed: Optional[datetime] = None

    def __post_init__(self):
        if not self.doc_id:
            self.doc_id = hashlib.md5(
                f"{self.content[:100]}{self.timestamp}".encode()
            ).hexdigest()[:12]
        if self.token_count == 0:
            # Rough estimate
            self.token_count = len(self.content) // 4


@dataclass
class Arena:
    """An isolated context arena"""
    name: str
    arena_type: ArenaType
    capacity: int = 1000  # Max documents
    retention: timedelta = timedelta(hours=1)
    documents: Dict[str, Document] = field(default_factory=dict)
    embeddings_index: Optional[Any] = None  # Would be FAISS or similar
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    total_tokens: int = 0

    def add_document(self, content: str, metadata: Optional[Dict] = None) -> str:
        """Add a document to the arena"""
        doc = Document(
            doc_id="",
            content=content,
            metadata=metadata or {},
            timestamp=datetime.now()
        )

        # Check capacity
        if len(self.documents) >= self.capacity:
            self._evict_oldest()

        self.documents[doc.doc_id] = doc
        self.total_tokens += doc.token_count

        # Generate embedding (placeholder - use real model in production)
        doc.embedding = self._generate_embedding(content)

        return doc.doc_id

    def _evict_oldest(self):
        """Evict oldest document when at capacity"""
        if not self.documents:
            return

        oldest = min(self.documents.values(), key=lambda d: d.timestamp)
        self.total_tokens -= oldest.token_count
        del self.documents[oldest.doc_id]

    def _generate_embedding(self, text: str) -> np.ndarray:
        """Generate embedding for text (placeholder)"""
        # In production, use actual embedding model
        # For now, create random embedding
        np.random.seed(hash(text) % 2**32)
        return np.random.randn(768)  # Standard embedding size

    def search(self, query: str, k: int = 5) -> List[Document]:
        """Search for relevant documents"""
        if not self.documents:
            return []

        # Generate query embedding
        query_embedding = self._generate_embedding(query)

        # Calculate similarities (placeholder - use vector DB in production)
        scores = []
        for doc_id, doc in self.documents.items():
            if doc.embedding is not None:
                # Cosine similarity
                similarity = np.dot(query_embedding, doc.embedding) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(doc.embedding)
                )
                scores.append((similarity, doc))

        # Sort by similarity and return top k
        scores.sort(key=lambda x: x[0], reverse=True)
        results = [doc for _, doc in scores[:k]]

        # Update access stats
        for doc in results:
            doc.access_count += 1
            doc.last_accessed = datetime.now()

        return results

    def clear(self):
        """Clear all documents from arena"""
        self.documents.clear()
        self.total_tokens = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get arena statistics"""
        return {
            'name': self.name,
            'type': self.arena_type.value,
            'document_count': len(self.documents),
            'total_tokens': self.total_tokens,
            'capacity_used': len(self.documents) / self.capacity,
            'created_at': self.created_at.isoformat(),
            'age_hours': (datetime.now() - self.created_at).total_seconds() / 3600
        }


class ContextArenaManager:
    """
    Manages multiple context arenas and provides RAG extraction
    """

    def __init__(self):
        self.arenas: Dict[str, Arena] = {}
        self.conversation_context: List[Dict[str, str]] = []
        self.max_conversation_tokens: int = 50000
        self.max_extraction_tokens: int = 2000

        # Statistics
        self.stats = {
            'total_documents': 0,
            'total_extractions': 0,
            'arena_creations': 0,
            'cross_arena_queries': 0
        }

        # Create default arenas
        self._create_default_arenas()

    def _create_default_arenas(self):
        """Create default arenas"""
        defaults = [
            ('search', ArenaType.SEARCH, 1000),
            ('code', ArenaType.CODE, 100),
            ('docs', ArenaType.DOCUMENTATION, 500),
            ('memory', ArenaType.MEMORY, 200),
        ]

        for name, arena_type, capacity in defaults:
            self.create_arena(name, arena_type, capacity)

    def create_arena(
        self,
        name: str,
        arena_type: ArenaType,
        capacity: int = 1000,
        retention: Optional[timedelta] = None
    ) -> Arena:
        """Create a new arena"""
        if name in self.arenas:
            raise ValueError(f"Arena '{name}' already exists")

        arena = Arena(
            name=name,
            arena_type=arena_type,
            capacity=capacity,
            retention=retention or timedelta(hours=1)
        )

        self.arenas[name] = arena
        self.stats['arena_creations'] += 1
        return arena

    def add_to_arena(
        self,
        arena_name: str,
        content: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """Add content to a specific arena"""
        if arena_name not in self.arenas:
            raise ValueError(f"Arena '{arena_name}' not found")

        doc_id = self.arenas[arena_name].add_document(content, metadata)
        self.stats['total_documents'] += 1
        return doc_id

    def route_tool_output(
        self,
        tool_name: str,
        output: str,
        metadata: Optional[Dict] = None
    ) -> Tuple[str, str]:
        """Route tool output to appropriate arena"""
        # Routing logic based on tool type
        arena_mapping = {
            'search': 'search',
            'web_search': 'search',
            'grep': 'code',
            'file_read': 'code',
            'git': 'code',
            'api_docs': 'docs',
            'database': 'database',
            'logs': 'logs'
        }

        # Determine arena
        arena_name = arena_mapping.get(tool_name, 'search')

        # Create arena if it doesn't exist
        if arena_name not in self.arenas:
            arena_type = ArenaType.CUSTOM
            for atype in ArenaType:
                if atype.value == arena_name:
                    arena_type = atype
                    break
            self.create_arena(arena_name, arena_type)

        # Add to arena
        doc_id = self.add_to_arena(arena_name, output, metadata)
        return arena_name, doc_id

    def extract_from_arena(
        self,
        query: str,
        arena_names: Optional[List[str]] = None,
        max_docs: int = 5
    ) -> str:
        """Extract relevant information from arena(s)"""
        if arena_names is None:
            arena_names = list(self.arenas.keys())

        all_results = []

        for arena_name in arena_names:
            if arena_name not in self.arenas:
                continue

            arena = self.arenas[arena_name]
            results = arena.search(query, k=max_docs)

            for doc in results:
                all_results.append({
                    'arena': arena_name,
                    'content': doc.content,
                    'relevance': doc.access_count,  # Simple relevance proxy
                    'metadata': doc.metadata
                })

        # Sort by relevance
        all_results.sort(key=lambda x: x['relevance'], reverse=True)

        # Extract and synthesize
        extraction = self._synthesize_extraction(query, all_results[:max_docs])
        self.stats['total_extractions'] += 1

        return extraction

    def _synthesize_extraction(
        self,
        query: str,
        results: List[Dict[str, Any]]
    ) -> str:
        """Synthesize extraction from search results"""
        if not results:
            return f"No relevant information found for query: {query}"

        # Simple synthesis (use LLM in production)
        synthesis = f"Found {len(results)} relevant results for: {query}\n\n"

        current_tokens = len(synthesis) // 4
        for result in results:
            preview = result['content'][:500]
            preview_tokens = len(preview) // 4

            if current_tokens + preview_tokens > self.max_extraction_tokens:
                break

            synthesis += f"From {result['arena']} arena:\n{preview}\n\n"
            current_tokens += preview_tokens

        return synthesis.strip()

    def multi_hop_extract(
        self,
        initial_query: str,
        max_hops: int = 3
    ) -> str:
        """Perform multi-hop extraction across arenas"""
        self.stats['cross_arena_queries'] += 1

        hop_results = []
        current_query = initial_query

        for hop in range(max_hops):
            # Determine relevant arenas for this hop
            relevant_arenas = self._identify_relevant_arenas(current_query)

            # Extract from identified arenas
            extraction = self.extract_from_arena(
                current_query,
                arena_names=relevant_arenas
            )

            hop_results.append({
                'hop': hop + 1,
                'query': current_query,
                'extraction': extraction
            })

            # Generate next query based on current results
            current_query = self._generate_followup_query(
                initial_query,
                extraction,
                hop
            )

            # Stop if we have sufficient information
            if self._is_sufficient(hop_results):
                break

        # Synthesize multi-hop results
        return self._synthesize_multi_hop(initial_query, hop_results)

    def _identify_relevant_arenas(self, query: str) -> List[str]:
        """Identify which arenas are relevant for a query"""
        # Simple keyword-based identification (use NLP in production)
        arena_keywords = {
            'search': ['search', 'web', 'internet', 'online'],
            'code': ['code', 'function', 'file', 'implementation', 'bug'],
            'docs': ['documentation', 'api', 'reference', 'guide'],
            'memory': ['previous', 'earlier', 'remember', 'history']
        }

        relevant = []
        query_lower = query.lower()

        for arena_name, keywords in arena_keywords.items():
            if arena_name in self.arenas:
                if any(kw in query_lower for kw in keywords):
                    relevant.append(arena_name)

        # Default to all if none identified
        return relevant if relevant else list(self.arenas.keys())

    def _generate_followup_query(
        self,
        initial_query: str,
        extraction: str,
        hop: int
    ) -> str:
        """Generate follow-up query for next hop"""
        # Simple follow-up generation (use LLM in production)
        if hop == 0:
            return f"More specific details about: {initial_query}"
        elif hop == 1:
            return f"Implementation or examples of: {initial_query}"
        else:
            return f"Related information to: {initial_query}"

    def _is_sufficient(self, hop_results: List[Dict]) -> bool:
        """Check if we have sufficient information"""
        # Simple check - stop after finding substantial results
        total_content = sum(
            len(r['extraction']) for r in hop_results
        )
        return total_content > 1000

    def _synthesize_multi_hop(
        self,
        initial_query: str,
        hop_results: List[Dict]
    ) -> str:
        """Synthesize results from multiple hops"""
        synthesis = f"Multi-hop extraction for: {initial_query}\n\n"

        for result in hop_results:
            synthesis += f"Hop {result['hop']}: {result['query']}\n"
            synthesis += f"{result['extraction'][:500]}...\n\n"

        return synthesis

    def add_to_conversation(self, message: Dict[str, str]):
        """Add message to main conversation (protected)"""
        # Validate message
        if 'role' not in message or 'content' not in message:
            raise ValueError("Message must have 'role' and 'content'")

        # Check token limit
        message_tokens = len(message['content']) // 4
        total_tokens = sum(len(m['content']) // 4 for m in self.conversation_context)

        if total_tokens + message_tokens > self.max_conversation_tokens:
            # Apply sliding window
            while total_tokens + message_tokens > self.max_conversation_tokens:
                if self.conversation_context:
                    removed = self.conversation_context.pop(0)
                    total_tokens -= len(removed['content']) // 4
                else:
                    break

        self.conversation_context.append(message)

    def get_conversation_context(self) -> List[Dict[str, str]]:
        """Get the clean conversation context"""
        return self.conversation_context.copy()

    def get_arena_stats(self) -> Dict[str, Any]:
        """Get statistics for all arenas"""
        arena_stats = {
            name: arena.get_stats()
            for name, arena in self.arenas.items()
        }

        total_docs = sum(
            len(arena.documents) for arena in self.arenas.values()
        )
        total_tokens = sum(
            arena.total_tokens for arena in self.arenas.values()
        )

        return {
            'arenas': arena_stats,
            'total_arenas': len(self.arenas),
            'total_documents': total_docs,
            'total_tokens_in_arenas': total_tokens,
            'conversation_tokens': sum(
                len(m['content']) // 4 for m in self.conversation_context
            ),
            'extraction_stats': self.stats
        }

    def clear_arena(self, arena_name: str):
        """Clear a specific arena"""
        if arena_name in self.arenas:
            self.arenas[arena_name].clear()

    def clear_old_documents(self):
        """Clear documents older than retention period"""
        now = datetime.now()
        for arena in self.arenas.values():
            to_remove = []
            for doc_id, doc in arena.documents.items():
                if now - doc.timestamp > arena.retention:
                    to_remove.append(doc_id)

            for doc_id in to_remove:
                doc = arena.documents[doc_id]
                arena.total_tokens -= doc.token_count
                del arena.documents[doc_id]


# Example usage
if __name__ == "__main__":
    # Create manager
    manager = ContextArenaManager()

    # Simulate tool outputs going to arenas
    manager.route_tool_output(
        'web_search',
        "Search results about Python async programming..." * 100,
        {'query': 'python async'}
    )

    manager.route_tool_output(
        'file_read',
        "def async_function():\n    await something()" * 50,
        {'file': 'main.py'}
    )

    # Agent extracts relevant information
    extraction = manager.extract_from_arena(
        "How to implement async functions in Python?",
        arena_names=['search', 'code']
    )

    print("Extraction (what goes to conversation):")
    print(extraction[:500])
    print("\n" + "="*50 + "\n")

    # Add clean extraction to conversation
    manager.add_to_conversation({
        'role': 'assistant',
        'content': extraction
    })

    # Multi-hop extraction
    multi_hop_result = manager.multi_hop_extract(
        "async programming patterns and best practices"
    )

    print("Multi-hop extraction:")
    print(multi_hop_result[:500])
    print("\n" + "="*50 + "\n")

    # Check stats
    stats = manager.get_arena_stats()
    print("Arena Statistics:")
    print(f"Total arenas: {stats['total_arenas']}")
    print(f"Total documents: {stats['total_documents']}")
    print(f"Tokens in arenas: {stats['total_tokens_in_arenas']:,}")
    print(f"Conversation tokens: {stats['conversation_tokens']:,}")
    print(f"Extractions performed: {stats['extraction_stats']['total_extractions']}")
    print(f"Cross-arena queries: {stats['extraction_stats']['cross_arena_queries']}")