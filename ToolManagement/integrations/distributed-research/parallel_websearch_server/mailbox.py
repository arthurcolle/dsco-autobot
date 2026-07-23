"""Agent mailbox for asynchronous completion notifications."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field


class MailboxMessage(BaseModel):
    """Represents a single mailbox event."""

    message_id: str
    channel_id: str
    event_type: str
    job_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    delivered_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None


class MailboxDelivery(BaseModel):
    """Delivery payload returned to clients polling their mailbox."""

    channel_id: str
    messages: List[MailboxMessage] = Field(default_factory=list)
    pending_messages: int = 0


class MailboxManager:
    """In-memory mailbox store for agent notifications."""

    def __init__(self, max_messages_per_channel: int = 500) -> None:
        self._mailboxes: Dict[str, List[MailboxMessage]] = defaultdict(list)
        self._max_messages = max_messages_per_channel
        self._lock = asyncio.Lock()

    async def publish(
        self,
        channel_id: str,
        event_type: str,
        *,
        job_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> MailboxMessage:
        """Publish a new event into the specified channel."""
        message = MailboxMessage(
            message_id=f"msg_{uuid4().hex[:12]}",
            channel_id=channel_id,
            event_type=event_type,
            job_id=job_id,
            payload=payload or {},
            created_at=datetime.utcnow(),
        )

        async with self._lock:
            mailbox = self._mailboxes[channel_id]
            mailbox.append(message)

            # Trim oldest entries to honor per-channel cap
            if len(mailbox) > self._max_messages:
                overshoot = len(mailbox) - self._max_messages
                del mailbox[0:overshoot]

        return message

    async def pull(
        self,
        channel_id: str,
        *,
        max_messages: int = 20,
        auto_ack: bool = False,
    ) -> MailboxDelivery:
        """Retrieve pending messages for a channel."""
        if max_messages < 1:
            raise ValueError("max_messages must be >= 1")

        async with self._lock:
            mailbox = self._mailboxes.get(channel_id, [])
            pending = [msg for msg in mailbox if msg.acknowledged_at is None]
            selected: List[MailboxMessage] = []
            now = datetime.utcnow()

            for msg in pending[:max_messages]:
                if msg.delivered_at is None:
                    msg.delivered_at = now
                selected.append(msg.model_copy(deep=True))

                if auto_ack:
                    msg.acknowledged_at = now

            if auto_ack and selected:
                self._mailboxes[channel_id] = [
                    msg for msg in mailbox if msg.acknowledged_at is None
                ]
                pending_count = len(self._mailboxes[channel_id])
            else:
                pending_count = len([msg for msg in mailbox if msg.acknowledged_at is None])

        return MailboxDelivery(channel_id=channel_id, messages=selected, pending_messages=pending_count)

    async def ack(self, channel_id: str, message_ids: List[str]) -> Tuple[int, int]:
        """Acknowledge (delete) the provided message IDs."""
        if not message_ids:
            return 0, await self.pending_count(channel_id)

        id_set = set(message_ids)
        acknowledged = 0

        async with self._lock:
            mailbox = self._mailboxes.get(channel_id, [])
            now = datetime.utcnow()
            remaining: List[MailboxMessage] = []

            for msg in mailbox:
                if msg.message_id in id_set:
                    msg.acknowledged_at = now
                    acknowledged += 1
                else:
                    remaining.append(msg)

            self._mailboxes[channel_id] = remaining
            pending_count = len([msg for msg in remaining if msg.acknowledged_at is None])

        return acknowledged, pending_count

    async def pending_count(self, channel_id: str) -> int:
        """Return how many messages are still awaiting acknowledgement."""
        async with self._lock:
            mailbox = self._mailboxes.get(channel_id, [])
            return len([msg for msg in mailbox if msg.acknowledged_at is None])


_mailbox_manager: Optional[MailboxManager] = None


def get_mailbox_manager() -> MailboxManager:
    """Return the singleton mailbox manager instance."""
    global _mailbox_manager
    if _mailbox_manager is None:
        _mailbox_manager = MailboxManager()
    return _mailbox_manager


__all__ = [
    "MailboxDelivery",
    "MailboxManager",
    "MailboxMessage",
    "get_mailbox_manager",
]
