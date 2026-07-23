"""Streaming utilities for SSE (Server-Sent Events) support."""

import asyncio
import json
import logging
from typing import AsyncGenerator, Callable, Any

from parallel_websearch_server.models import StreamEvent
from parallel_websearch_server.config import settings

logger = logging.getLogger(__name__)


class StreamManager:
    """Manager for handling streaming responses."""

    @staticmethod
    def format_sse_message(event_type: str, data: Any) -> str:
        """Format message as Server-Sent Event.

        Args:
            event_type: Type of event
            data: Event data

        Returns:
            Formatted SSE message
        """
        sse_data = {
            "type": event_type,
            "data": data,
        }
        return f"data: {json.dumps(sse_data)}\n\n"

    @staticmethod
    async def stream_with_heartbeat(
        generator: AsyncGenerator[StreamEvent, None],
        heartbeat_interval: int = settings.stream_heartbeat_interval,
    ) -> AsyncGenerator[str, None]:
        """Wrap a generator with heartbeat events.

        This ensures the connection stays alive if no data is being sent.

        Args:
            generator: Source event generator
            heartbeat_interval: Interval between heartbeat messages in seconds

        Yields:
            Formatted SSE messages
        """
        heartbeat_task = asyncio.create_task(_heartbeat_task(heartbeat_interval))
        pending = {heartbeat_task}

        try:
            async for event in generator:
                # Cancel pending heartbeat if we have data
                for task in pending:
                    if task == heartbeat_task:
                        task.cancel()

                yield StreamManager.format_sse_message(event.event_type, event.data)

                # Recreate heartbeat task
                heartbeat_task = asyncio.create_task(_heartbeat_task(heartbeat_interval))
                pending = {heartbeat_task}
        finally:
            # Clean up heartbeat task
            for task in pending:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

    @staticmethod
    async def stream_chunks(
        data: str,
        chunk_size: int = settings.stream_chunk_size,
    ) -> AsyncGenerator[str, None]:
        """Stream large text data in chunks.

        Args:
            data: Text data to stream
            chunk_size: Size of each chunk

        Yields:
            Formatted SSE messages with chunks
        """
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            yield StreamManager.format_sse_message(
                "chunk",
                {
                    "content": chunk,
                    "chunk_index": i // chunk_size,
                    "is_final": (i + chunk_size) >= len(data),
                },
            )
            # Brief delay to allow client processing
            await asyncio.sleep(0.01)

    @staticmethod
    async def stream_json_sequence(
        items: list,
    ) -> AsyncGenerator[str, None]:
        """Stream JSON items as a sequence.

        Args:
            items: List of items to stream

        Yields:
            Formatted SSE messages with items
        """
        for index, item in enumerate(items):
            yield StreamManager.format_sse_message(
                "item",
                {
                    "item": item,
                    "index": index,
                    "total": len(items),
                },
            )
            await asyncio.sleep(0.01)


async def _heartbeat_task(interval: int) -> None:
    """Internal task for generating heartbeat.

    Args:
        interval: Heartbeat interval in seconds
    """
    await asyncio.sleep(interval)
