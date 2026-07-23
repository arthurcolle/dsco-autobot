"""Shared SSE utilities with graceful fallbacks."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi.responses import StreamingResponse

try:
    # Preferred dependency that provides event retry/keep-alive helpers.
    from sse_starlette.sse import EventSourceResponse  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback path

    class EventSourceResponse(StreamingResponse):
        """Minimal SSE response that mimics sse-starlette."""

        media_type = "text/event-stream"

        def __init__(
            self,
            content: Any,
            status_code: int = 200,
            headers: Mapping[str, str] | None = None,
            media_type: str | None = None,
            background: Any | None = None,
            ping: float | None = None,  # noqa: ARG002  - for compatibility only
            retry: int | None = None,  # noqa: ARG002  - for compatibility only
        ) -> None:
            response_headers: dict[str, str] = {
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
            if headers:
                response_headers.update(dict(headers))
            super().__init__(
                content=content,
                status_code=status_code,
                headers=response_headers,
                media_type=media_type or self.media_type,
                background=background,
            )


__all__ = ["EventSourceResponse"]
