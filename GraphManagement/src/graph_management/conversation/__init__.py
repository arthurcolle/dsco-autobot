"""
Conversation graph service.

Provides graph-native conversation models with:
- Message persistence with embeddings
- Block decomposition
- ToolCall/ToolResult tracking
- Entity mentions
- Similarity auto-linking
"""

from .models import Conversation, Message, Block, ToolCall, ToolResult, Entity
from .schema import generate_schema_ddl

__all__ = [
    "Conversation",
    "Message",
    "Block",
    "ToolCall",
    "ToolResult",
    "Entity",
    "generate_schema_ddl",
]
