"""
Context Orchestrator with Priority Eviction and Compression Pipeline
Complete system for managing LLM context to prevent token limit errors
"""

import json
import pickle
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import heapq
from datetime import datetime, timedelta
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor


class Priority(Enum):
    """Priority levels for context items"""
    CRITICAL = 10  # System prompts, current task
    HIGH = 8       # Recent user interactions
    MEDIUM = 6     # Recent assistant responses
    LOW = 4        # Search results, documentation
    MINIMAL = 2    # Old context, can be evicted


@dataclass
class ContextEntry:
    """Represents an entry in the context with full metadata"""
    content: str
    tokens: int
    priority: Priority
    timestamp: datetime
    entry_type: str
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.now)
    compressed: bool = False
    summary: Optional[str] = None
    embedding: Optional[List[float]] = None
    entry_id: str = field(default_factory=lambda: "")

    def __post_init__(self):
        if not self.entry_id:
            self.entry_id = hashlib.md5(
                f"{self.content[:100]}{self.timestamp}".encode()
            ).hexdigest()[:12]

    def __lt__(self, other):
        """For heap operations - lower priority values are evicted first"""
        return self.priority.value < other.priority.value


class CompressionStrategy(Enum):
    """Different compression strategies"""
    SUMMARIZATION = "summarization"
    TRUNCATION = "truncation"
    KEY_EXTRACTION = "key_extraction"
    SEMANTIC_COMPRESSION = "semantic_compression"


class ContextOrchestrator:
    """
    Main orchestrator for context management with multiple strategies
    """

    def __init__(
        self,
        max_tokens: int = 180000,
        warning_threshold: float = 0.8,
        critical_threshold: float = 0.95,
        enable_compression: bool = True,
        enable_caching: bool = True
    ):
        self.max_tokens = max_tokens
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.enable_compression = enable_compression
        self.enable_caching = enable_caching

        # Context storage
        self.context: List[ContextEntry] = []
        self.context_index: Dict[str, ContextEntry] = {}

        # Token budgets per category
        self.token_budgets = {
            'system': 5000,
            'conversation': 60000,
            'search': 70000,
            'code': 30000,
            'buffer': 15000
        }

        # Cache for compressed content
        self.compression_cache: Dict[str, str] = {}

        # Statistics
        self.stats = {
            'total_evictions': 0,
            'total_compressions': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'token_overflow_prevented': 0
        }

        # Priority queue for eviction
        self.eviction_queue = []

        # Thread pool for parallel operations
        self.executor = ThreadPoolExecutor(max_workers=4)

    def add_content(
        self,
        content: str,
        priority: Priority,
        entry_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Add content to context with automatic management
        """
        tokens = self._count_tokens(content)

        # Check if single item exceeds budget
        budget = self.token_budgets.get(entry_type, self.token_budgets['buffer'])
        if tokens > budget:
            # Try to compress before rejecting
            if self.enable_compression:
                content, tokens = self._emergency_compress(content, budget)
                if tokens > budget:
                    return False
            else:
                return False

        # Create entry
        entry = ContextEntry(
            content=content,
            tokens=tokens,
            priority=priority,
            timestamp=datetime.now(),
            entry_type=entry_type
        )

        # Check if we need to make space
        current_total = self.get_total_tokens()
        if current_total + tokens > self.max_tokens:
            self._make_space(tokens)

        # Add to context
        self.context.append(entry)
        self.context_index[entry.entry_id] = entry
        heapq.heappush(self.eviction_queue, entry)

        # Check thresholds
        self._check_thresholds()

        return True

    def _make_space(self, needed_tokens: int):
        """Make space for new content by evicting or compressing"""
        freed_tokens = 0

        # Phase 1: Evict low priority items
        freed_tokens += self._evict_low_priority(needed_tokens)

        if freed_tokens >= needed_tokens:
            return

        # Phase 2: Compress medium priority items
        if self.enable_compression:
            freed_tokens += self._compress_old_content(needed_tokens - freed_tokens)

        if freed_tokens >= needed_tokens:
            return

        # Phase 3: Emergency eviction
        freed_tokens += self._emergency_eviction(needed_tokens - freed_tokens)

    def _evict_low_priority(self, needed_tokens: int) -> int:
        """Evict low priority items"""
        freed_tokens = 0
        to_remove = []

        # Use heap to get lowest priority items
        temp_queue = []
        while self.eviction_queue and freed_tokens < needed_tokens:
            entry = heapq.heappop(self.eviction_queue)
            if entry.priority.value <= Priority.LOW.value:
                freed_tokens += entry.tokens
                to_remove.append(entry)
                self.stats['total_evictions'] += 1
            else:
                temp_queue.append(entry)

        # Restore non-evicted items to queue
        for entry in temp_queue:
            heapq.heappush(self.eviction_queue, entry)

        # Remove evicted items
        for entry in to_remove:
            self.context.remove(entry)
            del self.context_index[entry.entry_id]

        return freed_tokens

    def _compress_old_content(self, needed_tokens: int) -> int:
        """Compress older content to save space"""
        freed_tokens = 0
        now = datetime.now()

        for entry in self.context:
            if freed_tokens >= needed_tokens:
                break

            # Skip if already compressed or high priority
            if entry.compressed or entry.priority.value >= Priority.HIGH.value:
                continue

            # Compress if older than 10 minutes
            if now - entry.timestamp > timedelta(minutes=10):
                original_tokens = entry.tokens
                compressed_content = self._compress_content(entry)

                if compressed_content and len(compressed_content) < len(entry.content):
                    entry.summary = entry.content  # Keep original as summary
                    entry.content = compressed_content
                    entry.tokens = self._count_tokens(compressed_content)
                    entry.compressed = True

                    freed_tokens += original_tokens - entry.tokens
                    self.stats['total_compressions'] += 1

        return freed_tokens

    def _emergency_eviction(self, needed_tokens: int) -> int:
        """Emergency eviction of any content except critical"""
        freed_tokens = 0
        to_remove = []

        for entry in sorted(self.context, key=lambda x: x.priority.value):
            if freed_tokens >= needed_tokens:
                break

            if entry.priority != Priority.CRITICAL:
                freed_tokens += entry.tokens
                to_remove.append(entry)

        for entry in to_remove:
            self.context.remove(entry)
            if entry.entry_id in self.context_index:
                del self.context_index[entry.entry_id]
            self.stats['total_evictions'] += 1

        return freed_tokens

    def _compress_content(
        self,
        entry: ContextEntry,
        strategy: CompressionStrategy = CompressionStrategy.KEY_EXTRACTION
    ) -> Optional[str]:
        """Compress content using specified strategy"""
        # Check cache first
        cache_key = f"{entry.entry_id}_{strategy.value}"
        if self.enable_caching and cache_key in self.compression_cache:
            self.stats['cache_hits'] += 1
            return self.compression_cache[cache_key]

        self.stats['cache_misses'] += 1

        # Apply compression strategy
        if strategy == CompressionStrategy.TRUNCATION:
            compressed = self._truncate_content(entry.content)
        elif strategy == CompressionStrategy.KEY_EXTRACTION:
            compressed = self._extract_key_points(entry.content)
        elif strategy == CompressionStrategy.SUMMARIZATION:
            compressed = self._summarize_content(entry.content)
        else:
            compressed = self._semantic_compress(entry.content)

        # Cache result
        if self.enable_caching and compressed:
            self.compression_cache[cache_key] = compressed

        return compressed

    def _truncate_content(self, content: str, keep_ratio: float = 0.3) -> str:
        """Simple truncation strategy"""
        lines = content.split('\n')
        keep_lines = int(len(lines) * keep_ratio)

        if keep_lines < 3:
            return content[:int(len(content) * keep_ratio)] + "..."

        # Keep beginning and end
        result = lines[:keep_lines//2]
        result.append("... [truncated] ...")
        result.extend(lines[-(keep_lines//2):])

        return '\n'.join(result)

    def _extract_key_points(self, content: str) -> str:
        """Extract key points from content"""
        lines = content.split('\n')
        key_indicators = [
            'important', 'error', 'result', 'summary', 'conclusion',
            'def ', 'class ', 'function', 'return', 'TODO', 'FIXME'
        ]

        key_lines = []
        for line in lines:
            if any(indicator in line.lower() for indicator in key_indicators):
                key_lines.append(line)

        # If too few key lines, add some context
        if len(key_lines) < 5 and len(lines) > 10:
            key_lines.extend(lines[:2])  # Add beginning
            key_lines.extend(lines[-2:])  # Add end

        return '\n'.join(key_lines) if key_lines else self._truncate_content(content)

    def _summarize_content(self, content: str) -> str:
        """
        Summarize content (placeholder - use LLM in production)
        """
        # In production, use an LLM API for summarization
        # For now, use key extraction as fallback
        return self._extract_key_points(content)

    def _semantic_compress(self, content: str) -> str:
        """
        Semantic compression using embeddings (placeholder)
        """
        # In production, use embeddings to identify and keep
        # semantically important parts
        return self._extract_key_points(content)

    def _emergency_compress(self, content: str, target_tokens: int) -> Tuple[str, int]:
        """Emergency compression when content is too large"""
        # Try progressively more aggressive compression
        strategies = [
            CompressionStrategy.KEY_EXTRACTION,
            CompressionStrategy.TRUNCATION,
            CompressionStrategy.SUMMARIZATION
        ]

        for strategy in strategies:
            compressed = self._compress_content(
                ContextEntry(
                    content=content,
                    tokens=0,
                    priority=Priority.LOW,
                    timestamp=datetime.now(),
                    entry_type="emergency"
                ),
                strategy
            )

            if compressed:
                tokens = self._count_tokens(compressed)
                if tokens <= target_tokens:
                    return compressed, tokens

        # Last resort: hard truncate
        max_chars = target_tokens * 3  # Rough estimate
        return content[:max_chars] + "...", self._count_tokens(content[:max_chars])

    def _check_thresholds(self):
        """Check token usage against thresholds"""
        total_tokens = self.get_total_tokens()
        usage_ratio = total_tokens / self.max_tokens

        if usage_ratio >= self.critical_threshold:
            self._handle_critical_state()
        elif usage_ratio >= self.warning_threshold:
            self._handle_warning_state()

    def _handle_warning_state(self):
        """Handle warning threshold breach"""
        # Proactively compress old content
        if self.enable_compression:
            self._compress_old_content(self.max_tokens * 0.1)  # Free 10%

    def _handle_critical_state(self):
        """Handle critical threshold breach"""
        self.stats['token_overflow_prevented'] += 1

        # Aggressive eviction
        target_tokens = int(self.max_tokens * 0.7)  # Reduce to 70%
        current_tokens = self.get_total_tokens()
        self._make_space(current_tokens - target_tokens)

    def get_total_tokens(self) -> int:
        """Get total tokens in context"""
        return sum(entry.tokens for entry in self.context)

    def _count_tokens(self, text: str) -> int:
        """Count tokens (simplified - use actual tokenizer in production)"""
        # Rough estimate: 1 token per 4 characters
        return len(text) // 4

    def get_context_for_llm(
        self,
        max_tokens: Optional[int] = None,
        include_compressed: bool = False
    ) -> List[Dict[str, Any]]:
        """Get context formatted for LLM API"""
        max_tokens = max_tokens or self.max_tokens
        messages = []
        used_tokens = 0

        # Sort by priority and recency
        sorted_context = sorted(
            self.context,
            key=lambda x: (x.priority.value, x.timestamp),
            reverse=True
        )

        for entry in sorted_context:
            if used_tokens + entry.tokens > max_tokens:
                break

            content = entry.content
            if entry.compressed and entry.summary and include_compressed:
                content = f"[Compressed from: {entry.summary[:100]}...]\n{content}"

            message = {
                'role': self._get_role(entry.entry_type),
                'content': content,
                'metadata': {
                    'compressed': entry.compressed,
                    'priority': entry.priority.name,
                    'tokens': entry.tokens
                }
            }

            messages.append(message)
            used_tokens += entry.tokens

        return list(reversed(messages))  # Return in chronological order

    def _get_role(self, entry_type: str) -> str:
        """Map entry type to LLM role"""
        role_map = {
            'system': 'system',
            'user': 'user',
            'assistant': 'assistant',
            'search': 'assistant',
            'code': 'assistant'
        }
        return role_map.get(entry_type, 'assistant')

    def get_statistics(self) -> Dict[str, Any]:
        """Get detailed statistics"""
        return {
            **self.stats,
            'total_tokens': self.get_total_tokens(),
            'max_tokens': self.max_tokens,
            'usage_percentage': (self.get_total_tokens() / self.max_tokens) * 100,
            'entry_count': len(self.context),
            'compressed_count': sum(1 for e in self.context if e.compressed),
            'cache_size': len(self.compression_cache),
            'token_distribution': self._get_token_distribution()
        }

    def _get_token_distribution(self) -> Dict[str, int]:
        """Get token distribution by type"""
        distribution = defaultdict(int)
        for entry in self.context:
            distribution[entry.entry_type] += entry.tokens
        return dict(distribution)

    def save_state(self, filepath: str):
        """Save orchestrator state to file"""
        state = {
            'context': self.context,
            'stats': self.stats,
            'cache': self.compression_cache if self.enable_caching else {},
            'timestamp': datetime.now().isoformat()
        }

        with open(filepath, 'wb') as f:
            pickle.dump(state, f)

    def load_state(self, filepath: str):
        """Load orchestrator state from file"""
        with open(filepath, 'rb') as f:
            state = pickle.load(f)

        self.context = state['context']
        self.stats = state['stats']
        if self.enable_caching:
            self.compression_cache = state.get('cache', {})

        # Rebuild indices
        self.context_index = {e.entry_id: e for e in self.context}
        self.eviction_queue = list(self.context)
        heapq.heapify(self.eviction_queue)

    async def parallel_compress(
        self,
        entries: List[ContextEntry]
    ) -> List[Optional[str]]:
        """Compress multiple entries in parallel"""
        loop = asyncio.get_event_loop()
        tasks = []

        for entry in entries:
            task = loop.run_in_executor(
                self.executor,
                self._compress_content,
                entry
            )
            tasks.append(task)

        return await asyncio.gather(*tasks)


# Example usage
async def main():
    """Example usage of the Context Orchestrator"""
    orchestrator = ContextOrchestrator(
        max_tokens=50000,
        warning_threshold=0.8,
        critical_threshold=0.95
    )

    # Add various types of content
    orchestrator.add_content(
        "You are a helpful AI assistant.",
        Priority.CRITICAL,
        "system"
    )

    # Simulate conversation
    for i in range(50):
        # User message
        orchestrator.add_content(
            f"User question {i}: How do I implement feature {i}?",
            Priority.HIGH if i > 45 else Priority.MEDIUM,
            "user"
        )

        # Assistant response
        orchestrator.add_content(
            f"To implement feature {i}, you should follow these steps: " + "x" * 500,
            Priority.MEDIUM,
            "assistant"
        )

        # Occasional search results
        if i % 5 == 0:
            orchestrator.add_content(
                f"Search results for query {i}: " + "y" * 2000,
                Priority.LOW,
                "search"
            )

    # Get statistics
    stats = orchestrator.get_statistics()
    print("Context Orchestrator Statistics:")
    print(f"  Total tokens: {stats['total_tokens']:,}/{stats['max_tokens']:,}")
    print(f"  Usage: {stats['usage_percentage']:.1f}%")
    print(f"  Entries: {stats['entry_count']}")
    print(f"  Evictions: {stats['total_evictions']}")
    print(f"  Compressions: {stats['total_compressions']}")
    print(f"  Cache hits: {stats['cache_hits']}")
    print(f"  Overflows prevented: {stats['token_overflow_prevented']}")

    # Get context for LLM
    context = orchestrator.get_context_for_llm()
    print(f"\nPrepared context with {len(context)} messages")

    # Save state
    orchestrator.save_state("context_state.pkl")
    print("\nState saved to context_state.pkl")


if __name__ == "__main__":
    asyncio.run(main())