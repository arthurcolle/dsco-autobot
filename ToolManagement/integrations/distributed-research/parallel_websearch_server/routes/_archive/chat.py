"""Chat API routes (OpenAI-compatible)."""

import json
import logging
import time
from typing import AsyncGenerator

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from parallel_websearch_server.auth import get_bearer_auth_headers
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
)
from parallel_websearch_server.turso_store import get_turso_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["Chat"])


async def _stream_chat_completion(
    request: ChatCompletionRequest,
    client: httpx.AsyncClient,
) -> AsyncGenerator[str, None]:
    """Stream chat completion response."""
    try:
        async with client.stream(
            "POST",
            "/chat/completions",
            json=request.model_dump(exclude_none=True),
            headers=get_bearer_auth_headers(),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield line + "\n"
    except httpx.HTTPStatusError as e:
        error_data = {"error": f"Stream failed: {e.response.status_code}"}
        yield f"data: {json.dumps(error_data)}\n\n"


@router.post("/completions", operation_id="chat_completion")
async def chat_completion(
    request: ChatCompletionRequest,
    req: Request,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Create a chat completion.

    OpenAI-compatible endpoint for chat completions with web search capabilities.

    Args:
        request: Chat completion request with model and messages

    Returns:
        Chat completion response or streaming response
    """
    payload = request.model_dump(exclude_none=True)

    logger.info(
        "Dispatching /chat/completions request",
        extra={
            "model": request.model,
            "message_count": len(request.messages),
            "stream": request.stream,
        },
    )

    if request.stream:
        return StreamingResponse(
            _stream_chat_completion(request, client),
            media_type="text/event-stream",
        )

    start_time = time.time()
    try:
        response = await client.post(
            "/chat/completions",
            json=payload,
            headers=get_bearer_auth_headers(),
        )
        response.raise_for_status()
        body = response.json()
        duration_ms = int((time.time() - start_time) * 1000)

        logger.info(
            "Upstream /chat/completions completed",
            extra={
                "model": request.model,
                "choice_count": len(body.get("choices", [])),
                "upstream_request_id": response.headers.get("x-request-id"),
            },
        )

        # Persist to Turso
        try:
            turso = get_turso_store()
            usage = body.get("usage", {})
            response_content = None
            choices = body.get("choices", [])
            if choices:
                response_content = choices[0].get("message", {}).get("content")

            turso.save_chat_completion(
                model=request.model,
                messages=[m.model_dump() for m in request.messages],
                completion_id=body.get("id"),
                response=response_content,
                usage_prompt_tokens=usage.get("prompt_tokens"),
                usage_completion_tokens=usage.get("completion_tokens"),
                usage_total_tokens=usage.get("total_tokens"),
                duration_ms=duration_ms,
            )
        except Exception as e:
            logger.warning(f"Failed to persist chat completion to Turso: {e}")

        return ChatCompletionResponse(**body)
    except httpx.HTTPStatusError as e:
        logger.error(
            "Upstream /chat/completions error",
            extra={
                "model": request.model,
                "status_code": e.response.status_code,
                "upstream_request_id": e.response.headers.get("x-request-id"),
            },
        )
        error_payload = e.response.json() if e.response.text else {"error": "Chat completion failed"}
        raise HTTPException(
            status_code=e.response.status_code,
            detail=error_payload,
        )
    except httpx.RequestError as e:
        logger.exception(
            "HTTP client error while calling /chat/completions",
            extra={"model": request.model},
        )
        raise HTTPException(
            status_code=502,
            detail={"error": "Unable to reach upstream chat service"},
        ) from e
