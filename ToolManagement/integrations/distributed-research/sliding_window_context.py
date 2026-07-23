"""
Sliding Window Context Manager
Prevents token limit errors by maintaining a fixed-size context window
"""

import tiktoken
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import deque
import json
from datetime import datetime


@dataclass
class ContextItem:
    """Represents a single item in the context"""
    content: str
    token_count: int
    timestamp: datetime
    priority: int  # 1-10, higher is more important
    item_type: str  # 'system', 'user', 'assistant', 'search', 'code'
    metadata: Dict[str, Any]


class SlidingWindowContextManager:
    """
    Manages conversation context with a sliding window to prevent token overflow.
    """

    def __init__(
        self,
        max_tokens: int = 180000,  # 90% of 200k limit
        model: str = "gpt-4",
        preserve_system: bool = True,
        compression_threshold: float = 0.8
    ):
        self.max_tokens = max_tokens
        self.model = model
        self.preserve_system = preserve_system
        self.compression_threshold = compression_threshold

        # Initialize tokenizer
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except:
            self.encoding = tiktoken.get_encoding("cl100k_base")

        # Context storage
        self.context_items: deque = deque()
        self.system_context: Optional[ContextItem] = None
        self.total_tokens: int = 0

        # Statistics
        self.eviction_count: int = 0
        self.compression_count: int = 0

    def count_tokens(self, text: str) -> int:
        """Count tokens in a text string"""
        return len(self.encoding.encode(text))

    def add_message(
        self,
        content: str,
        item_type: str = "user",
        priority: int = 5,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Add a message to the context.
        Returns True if successful, False if message is too large.
        """
        token_count = self.count_tokens(content)

        # Check if single message exceeds limit
        if token_count > self.max_tokens:
            return False

        # Create context item
        item = ContextItem(
            content=content,
            token_count=token_count,
            timestamp=datetime.now(),
            priority=priority,
            item_type=item_type,
            metadata=metadata or {}
        )

        # Handle system messages specially
        if item_type == "system" and self.preserve_system:
            if self.system_context:
                self.total_tokens -= self.system_context.token_count
            self.system_context = item
            self.total_tokens += token_count
        else:
            # Add to regular context
            self.context_items.append(item)
            self.total_tokens += token_count

        # Manage context size
        self._manage_context_size()

        return True

    def _manage_context_size(self):
        """Evict old messages or compress if approaching limit"""
        # Check if we need compression
        if self.total_tokens > self.max_tokens * self.compression_threshold:
            self._compress_old_messages()

        # Evict messages if still over limit
        while self.total_tokens > self.max_tokens and len(self.context_items) > 0:
            self._evict_oldest()

    def _evict_oldest(self):
        """Remove the oldest low-priority message"""
        if not self.context_items:
            return

        # Find lowest priority oldest item
        min_priority = min(item.priority for item in self.context_items)

        for i, item in enumerate(self.context_items):
            if item.priority == min_priority:
                removed_item = self.context_items.popleft()
                self.total_tokens -= removed_item.token_count
                self.eviction_count += 1
                break

    def _compress_old_messages(self):
        """Compress older messages to save space"""
        # Compress messages older than the last 10
        if len(self.context_items) <= 10:
            return

        compressed_items = []
        tokens_saved = 0

        for i, item in enumerate(list(self.context_items)[:-10]):
            if item.item_type in ['search', 'code'] and item.priority < 7:
                # Compress these items
                compressed = self._compress_content(item.content)
                new_tokens = self.count_tokens(compressed)
                tokens_saved += item.token_count - new_tokens

                item.content = compressed
                item.token_count = new_tokens
                item.metadata['compressed'] = True
                self.compression_count += 1

        self.total_tokens -= tokens_saved

    def _compress_content(self, content: str) -> str:
        """
        Compress content by extracting key points.
        In production, this could use an LLM for summarization.
        """
        lines = content.split('\n')

        # Simple compression: keep first/last parts and important lines
        if len(lines) > 20:
            important_lines = []
            important_lines.extend(lines[:5])  # Keep first 5 lines
            important_lines.append("... [content compressed] ...")

            # Keep lines with important keywords
            keywords = ['error', 'important', 'result', 'summary', 'conclusion']
            for line in lines[5:-5]:
                if any(kw in line.lower() for kw in keywords):
                    important_lines.append(line)

            important_lines.extend(lines[-5:])  # Keep last 5 lines
            return '\n'.join(important_lines)

        return content

    def get_context(self) -> List[Dict[str, Any]]:
        """Get the current context as a list of messages"""
        messages = []

        # Add system context if present
        if self.system_context:
            messages.append({
                'role': 'system',
                'content': self.system_context.content
            })

        # Add other messages
        for item in self.context_items:
            role = 'user' if item.item_type == 'user' else 'assistant'
            messages.append({
                'role': role,
                'content': item.content
            })

        return messages

    def get_context_with_budget(
        self,
        token_budget: int,
        prioritize_recent: bool = True
    ) -> List[Dict[str, Any]]:
        """Get context that fits within a specific token budget"""
        messages = []
        used_tokens = 0

        # Always include system message if present
        if self.system_context:
            messages.append({
                'role': 'system',
                'content': self.system_context.content
            })
            used_tokens += self.system_context.token_count

        # Select items based on strategy
        items = list(self.context_items)
        if prioritize_recent:
            items = reversed(items)  # Start from most recent

        selected_items = []
        for item in items:
            if used_tokens + item.token_count <= token_budget:
                selected_items.append(item)
                used_tokens += item.token_count

        # Reverse back if we prioritized recent
        if prioritize_recent:
            selected_items = reversed(selected_items)

        # Convert to messages
        for item in selected_items:
            role = 'user' if item.item_type == 'user' else 'assistant'
            messages.append({
                'role': role,
                'content': item.content
            })

        return messages

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the context manager"""
        return {
            'total_tokens': self.total_tokens,
            'max_tokens': self.max_tokens,
            'usage_percentage': (self.total_tokens / self.max_tokens) * 100,
            'message_count': len(self.context_items),
            'eviction_count': self.eviction_count,
            'compression_count': self.compression_count,
            'has_system_context': self.system_context is not None
        }

    def clear_context(self, preserve_system: bool = True):
        """Clear the context, optionally preserving system message"""
        self.context_items.clear()
        if not preserve_system:
            self.system_context = None
            self.total_tokens = 0
        else:
            self.total_tokens = (
                self.system_context.token_count if self.system_context else 0
            )

    def export_context(self, filepath: str):
        """Export context to a JSON file"""
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.get_statistics(),
            'context': self.get_context()
        }

        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

    def import_context(self, filepath: str):
        """Import context from a JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)

        self.clear_context(preserve_system=False)

        for message in data['context']:
            self.add_message(
                content=message['content'],
                item_type='system' if message['role'] == 'system' else message['role']
            )


# Example usage
if __name__ == "__main__":
    # Initialize the context manager
    cm = SlidingWindowContextManager(max_tokens=50000)

    # Add system message
    cm.add_message(
        "You are a helpful AI assistant.",
        item_type="system",
        priority=10
    )

    # Simulate conversation
    for i in range(100):
        # User message
        cm.add_message(
            f"User question {i}: How do I implement feature {i}?",
            item_type="user",
            priority=5
        )

        # Assistant response
        cm.add_message(
            f"Assistant response {i}: Here's how you implement feature {i}...",
            item_type="assistant",
            priority=5
        )

        # Occasionally add search results (lower priority, will be evicted first)
        if i % 10 == 0:
            cm.add_message(
                f"Search results for query {i}: [Large amount of search data...]",
                item_type="search",
                priority=3
            )

    # Check statistics
    stats = cm.get_statistics()
    print(f"Context Statistics:")
    print(f"  Total tokens: {stats['total_tokens']:,}/{stats['max_tokens']:,}")
    print(f"  Usage: {stats['usage_percentage']:.1f}%")
    print(f"  Messages: {stats['message_count']}")
    print(f"  Evictions: {stats['eviction_count']}")
    print(f"  Compressions: {stats['compression_count']}")

    # Get context for API call
    context = cm.get_context()
    print(f"\nContext ready with {len(context)} messages")