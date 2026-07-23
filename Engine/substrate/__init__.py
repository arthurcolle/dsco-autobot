"""
Coordination Substrate Layer

Provides integration with external systems for agent coordination:
- GraphSub: Stigmergic edges, agent state, capability tokens
- Redis: Mailbox (Streams), pub/sub, session state
- Temporal: Already integrated via parent Engine module

This layer abstracts the underlying infrastructure so agents interact
with a unified coordination interface.
"""

from .graphsub_client import GraphSubClient
from .redis_mailbox import RedisMailbox
from .stigmergy import StigmergicCoordinator
from .capability_tokens import CapabilityTokenManager

__all__ = [
    "GraphSubClient",
    "RedisMailbox",
    "StigmergicCoordinator",
    "CapabilityTokenManager",
]
