"""
Redis Mailbox System - Durable agent message passing

Uses Redis Streams for:
- Reliable task delivery (at-least-once semantics)
- Consumer groups for load balancing across agent pools
- Pub/Sub for high-priority signal broadcasts
- Message acknowledgment and tracking

Stream Structure:
- tasks:pending - Global task queue for any available agent
- tasks:{agent_id} - Direct messages to specific agent
- signals:broadcast - System-wide events (priority interrupts, etc.)
"""

import asyncio
import json
import logging
from typing import Optional, Dict, List, Any, Tuple, AsyncIterator
from dataclasses import dataclass, asdict
from datetime import datetime
import redis.asyncio as redis
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """Message in Redis stream."""
    id: str  # Redis message ID (timestamp-sequence)
    type: str  # "task", "signal", "control"
    payload: Dict[str, Any]
    source: Optional[str] = None
    priority: int = 5  # 1-10, higher = more urgent
    created_at: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        """Convert to Redis stream format (all string values)."""
        return {
            "type": self.type,
            "source": self.source or "system",
            "priority": str(self.priority),
            "payload": json.dumps(self.payload),
            "created_at": self.created_at or datetime.utcnow().isoformat()
        }

    @classmethod
    def from_redis(cls, msg_id: str, data: Dict[bytes, bytes]) -> "Message":
        """Parse message from Redis stream."""
        decoded = {
            k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v
            for k, v in data.items()
        }

        return cls(
            id=msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
            type=decoded.get("type", "unknown"),
            payload=json.loads(decoded.get("payload", "{}")),
            source=decoded.get("source"),
            priority=int(decoded.get("priority", 5)),
            created_at=decoded.get("created_at")
        )


class RedisMailbox:
    """
    Redis-based mailbox for agent communication.

    Features:
    - Durable delivery (messages persist until acknowledged)
    - Consumer groups (load balancing across agent pools)
    - Idempotent processing (message IDs prevent duplicates)
    - Pub/Sub broadcasts (for urgent signals)
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        agent_id: Optional[str] = None,
        pool_name: str = "default"
    ):
        """
        Initialize Redis mailbox.

        Args:
            redis_url: Redis connection URL
            agent_id: This agent's ID (for targeted messages)
            pool_name: Agent pool name (for consumer groups)
        """
        self.redis_url = redis_url
        self.agent_id = agent_id
        self.pool_name = pool_name
        self.redis_client: Optional[redis.Redis] = None
        self._pubsub_subscriptions: Dict[str, Any] = {}

    async def connect(self):
        """Establish Redis connection."""
        try:
            self.redis_client = await redis.from_url(
                self.redis_url,
                decode_responses=False
            )
            # Test connection
            await self.redis_client.ping()
            logger.info(f"Connected to Redis: {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Disconnected from Redis")

    async def create_consumer_group(
        self,
        stream_name: str,
        group_name: str,
        start_id: str = "$"  # Start from new messages
    ) -> bool:
        """
        Create a consumer group for load-balanced consumption.

        Args:
            stream_name: Name of Redis stream
            group_name: Name of consumer group
            start_id: Starting message ID ("$" for new messages, "0" for all)

        Returns:
            True if group created, False if already exists
        """
        try:
            await self.redis_client.xgroup_create(
                stream_name,
                group_name,
                id=start_id,
                mkstream=True
            )
            logger.info(f"Created consumer group {group_name} for {stream_name}")
            return True
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group {group_name} already exists")
                return False
            logger.error(f"Failed to create consumer group: {e}")
            raise

    async def publish_task(
        self,
        task_id: str,
        task_data: Dict[str, Any],
        priority: int = 5,
        target_agent_id: Optional[str] = None
    ) -> str:
        """
        Publish a task to the mailbox.

        Args:
            task_id: Unique task ID
            task_data: Task payload
            priority: Priority level (1-10)
            target_agent_id: If set, send to specific agent, else to pool

        Returns:
            Redis message ID
        """
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        message = Message(
            id="",  # Redis will assign
            type="task",
            payload={"task_id": task_id, **task_data},
            source="system",
            priority=priority
        )

        # Route to specific agent or general pool
        stream_name = (
            f"tasks:{target_agent_id}"
            if target_agent_id
            else "tasks:pending"
        )

        try:
            msg_id = await self.redis_client.xadd(
                stream_name,
                message.to_dict()
            )
            logger.info(f"Published task {task_id} to {stream_name}")
            return msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        except Exception as e:
            logger.error(f"Failed to publish task: {e}")
            raise

    async def consume_tasks(
        self,
        agent_id: str,
        pool_name: str = "default",
        block_ms: int = 1000,
        max_batch_size: int = 10
    ) -> AsyncIterator[Message]:
        """
        Consume tasks from agent-specific queue, then pool queue.

        Agents first check their direct message queue, then join the pool queue.

        Args:
            agent_id: This agent's ID
            pool_name: Agent pool name (for consumer group)
            block_ms: Milliseconds to block on empty stream
            max_batch_size: Maximum messages per batch

        Yields:
            Message objects
        """
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        # Create consumer groups if needed
        await self.create_consumer_group(f"tasks:{agent_id}", agent_id)
        await self.create_consumer_group("tasks:pending", pool_name)

        while True:
            messages = []

            # First, check agent-specific queue
            try:
                agent_stream_messages = await self.redis_client.xreadgroup(
                    {f"tasks:{agent_id}": ">"},
                    agent_id,
                    count=max_batch_size,
                    block=0  # Non-blocking first
                )

                if agent_stream_messages:
                    for msg_id, msg_data in agent_stream_messages[0][1]:
                        messages.append(Message.from_redis(msg_id, msg_data))
            except Exception as e:
                logger.debug(f"Error reading agent stream: {e}")

            # Then check pool queue
            if len(messages) < max_batch_size:
                try:
                    pool_messages = await self.redis_client.xreadgroup(
                        {"tasks:pending": ">"},
                        pool_name,
                        count=max_batch_size - len(messages),
                        block=block_ms
                    )

                    if pool_messages:
                        for msg_id, msg_data in pool_messages[0][1]:
                            messages.append(Message.from_redis(msg_id, msg_data))
                except Exception as e:
                    logger.debug(f"Error reading pool stream: {e}")

            # Yield messages
            for message in messages:
                yield message

            # If no messages and we're not blocking, sleep briefly
            if not messages:
                await asyncio.sleep(0.1)

    async def acknowledge_message(
        self,
        message_id: str,
        stream_name: str,
        group_name: str
    ) -> bool:
        """
        Acknowledge a message (mark as processed).

        Args:
            message_id: Redis message ID
            stream_name: Stream name
            group_name: Consumer group name

        Returns:
            True if acknowledged
        """
        try:
            await self.redis_client.xack(stream_name, group_name, message_id)
            logger.debug(f"Acknowledged message {message_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to acknowledge message: {e}")
            return False

    async def broadcast_signal(
        self,
        signal_type: str,
        signal_data: Dict[str, Any],
        priority: int = 10  # Signals have high priority
    ) -> str:
        """
        Broadcast a signal to all agents via Pub/Sub.

        Used for urgent events (shutdown, resource warning, etc).

        Args:
            signal_type: Type of signal
            signal_data: Signal payload
            priority: Priority level

        Returns:
            Pub/Sub message ID
        """
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        message = Message(
            id="",
            type="signal",
            payload=signal_data,
            source="system",
            priority=priority
        )

        channel = f"signals:{signal_type}"

        try:
            num_subscribers = await self.redis_client.publish(
                channel,
                json.dumps(message.to_dict())
            )
            logger.info(
                f"Broadcast signal {signal_type} to {num_subscribers} subscribers"
            )
            return channel
        except Exception as e:
            logger.error(f"Failed to broadcast signal: {e}")
            raise

    async def subscribe_to_signals(
        self,
        signal_patterns: List[str]
    ) -> AsyncIterator[Tuple[str, Message]]:
        """
        Subscribe to signals via Pub/Sub.

        Args:
            signal_patterns: List of signal type patterns (e.g., ["signals:*"])

        Yields:
            (channel, message) tuples
        """
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        pubsub = self.redis_client.pubsub()

        try:
            # Subscribe to patterns
            for pattern in signal_patterns:
                await pubsub.psubscribe(pattern)
                logger.info(f"Subscribed to signal pattern: {pattern}")

            # Yield messages
            async for raw_message in pubsub.listen():
                if raw_message["type"] == "pmessage":
                    channel = raw_message["channel"].decode()
                    data = json.loads(raw_message["data"])

                    message = Message(
                        id="",
                        type=data.get("type", "signal"),
                        payload=data.get("payload", {}),
                        source=data.get("source"),
                        priority=int(data.get("priority", 5)),
                        created_at=data.get("created_at")
                    )

                    yield (channel, message)
        finally:
            await pubsub.unsubscribe()
            await pubsub.close()

    async def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """
        Get information about a stream.

        Args:
            stream_name: Stream name

        Returns:
            Stream info dict
        """
        try:
            info = await self.redis_client.xinfo_stream(stream_name)
            return info
        except Exception as e:
            logger.error(f"Failed to get stream info: {e}")
            return {}

    async def trim_stream(
        self,
        stream_name: str,
        max_len: int = 10000
    ) -> int:
        """
        Trim stream to maximum length (garbage collection).

        Args:
            stream_name: Stream name
            max_len: Maximum stream length

        Returns:
            Number of messages removed
        """
        try:
            removed = await self.redis_client.xtrim(
                stream_name,
                maxlen=max_len,
                approximate=True
            )
            logger.info(f"Trimmed {stream_name}: removed {removed} messages")
            return removed
        except Exception as e:
            logger.error(f"Failed to trim stream: {e}")
            return 0

    async def close(self):
        """Close mailbox (disconnect Redis)."""
        await self.disconnect()

    @asynccontextmanager
    async def connection(self):
        """Context manager for mailbox connection."""
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
