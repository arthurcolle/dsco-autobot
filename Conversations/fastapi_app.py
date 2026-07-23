"""
Clean, production-ready FastAPI application.

- Health and readiness endpoints
- CORS support, configurable via env
- Simple example routes and models
- Structured logging
- Uvicorn entrypoint for local runs

Environment variables:
  - APP_NAME: Service title (default: Conversations API)
  - APP_VERSION: Version string (default: 0.1.0)
  - ENV: Environment name (default: development)
  - DEBUG: Enable debug logs when set to "1"
  - CORS_ORIGINS: Comma-separated list or "*" (default: *)
  - HOST: Bind host for uvicorn (default: 0.0.0.0)
  - PORT: Bind port for uvicorn (default: 8000)
  - RELOAD: Enable uvicorn reload when set to "1"
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

# Dataclasses for conversation management
from conversation_api_dataclasses import (
    Message as DCMessage,
    MessageCreate as DCMessageCreate,
    Attachment as DCAttachment,
    ImageAttachment as DCImageAttachment,
    FileAttachment as DCFileAttachment,
    LinkAttachment as DCLinkAttachment,
    AttachmentKind as DCAttachmentKind,
    MessagePreview as DCMessagePreview,
    MessageUI as DCMessageUI,
    ConversationUIConfig as DCConversationUIConfig,
    AttachmentIngestRequest as DCAttachmentIngestRequest,
    AttachmentIngestResponse as DCAttachmentIngestResponse,
    AttachmentUploadSpec as DCAttachmentUploadSpec,
    guess_attachment_kind,
    build_default_preview,
    build_ui_components_for_message,
    build_message_ui,
)


# ---------------------------
# Configuration and utilities
# ---------------------------


class Settings:
    """Lightweight settings loaded from environment.

    Avoids coupling to a specific Pydantic version.
    """

    def __init__(self) -> None:
        self.title: str = os.getenv("APP_NAME", "Conversations API")
        self.version: str = os.getenv("APP_VERSION", "0.1.0")
        self.env: str = os.getenv("ENV", "development")
        self.debug: bool = os.getenv("DEBUG", "0") == "1"
        raw_origins = os.getenv("CORS_ORIGINS", "*")
        self.cors_origins: List[str] = (
            [o.strip() for o in raw_origins.split(",") if o.strip()] if raw_origins != "*" else ["*"]
        )


def get_settings() -> Settings:
    return Settings()


START_TIME = time.time()


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Tidy overly chatty loggers in prod
    if not debug:
        for noisy in ("uvicorn.error", "uvicorn.access", "asyncio", "httpx"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------
# Lifespan
# ---------


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = logging.getLogger("lifespan")
    logger.info("Application starting up")
    try:
        yield
    finally:
        logger.info("Application shutting down")


# --------------
# Data models
# --------------


class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    service: str
    version: str
    environment: str


class EchoRequest(BaseModel):
    message: str


class EchoResponse(BaseModel):
    message: str
    length: int


# --------------
# Conversation-like minimal models (for compression APIs)
# --------------


class Message(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    # Either plain text or a list of content blocks (dicts with a 'type' field)
    content: Union[str, List[Dict[str, Any]]]


class CompressConfig(BaseModel):
    budget_tokens: int = 8192
    keep_last_n: int = 2
    preserve_tool_pairs: bool = True
    preserve_system: bool = True
    strategy: Literal["extractive", "abstractive", "hybrid"] = "extractive"
    query: Optional[str] = None
    # Weighting knobs for extractive ranking
    recency_weight: float = 0.6
    similarity_weight: float = 0.3
    role_weight: float = 0.1


class CompressRequest(BaseModel):
    messages: List[Message]
    config: CompressConfig


class CompressResponse(BaseModel):
    compressed_messages: List[Message]
    kept_indices: List[int]
    dropped_indices: List[int]
    summary: Optional[str] = None
    meta: Dict[str, Any] = {}


class ChunkToolResultRequest(BaseModel):
    content: Any
    max_tokens: int = 512
    max_chunks: int = 3


class ChunkToolResultResponse(BaseModel):
    chunks: List[str]
    meta: Dict[str, Any]


class ExpandKeepIndicesRequest(BaseModel):
    messages: List[Message]
    keep_indices: List[int]


class ExpandKeepIndicesResponse(BaseModel):
    expanded_indices: List[int]


class ValidatePairingRequest(BaseModel):
    messages: List[Message]


class ValidatePairingResponse(BaseModel):
    ok: bool
    problems: List[str] = []
    suggestions: List[str] = []


class EmbeddingsRequest(BaseModel):
    texts: List[str]


class EmbeddingsResponse(BaseModel):
    embeddings: List[List[float]] = []
    model: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    available: bool = False
    note: Optional[str] = None


class RerankRequest(BaseModel):
    query: str
    documents: List[Union[str, Dict[str, Any]]]
    top_n: Optional[int] = None


class RerankItem(BaseModel):
    index: int
    score: float
    text: str


class RerankResponse(BaseModel):
    results: List[RerankItem] = []
    available: bool = False
    note: Optional[str] = None


class ReadRequest(BaseModel):
    url: str
    format: Literal["markdown", "text"] = "markdown"
    include_images: bool = True
    include_links: bool = True


class ReadResponse(BaseModel):
    content: Optional[str] = None
    metadata: Dict[str, Any] = {}
    available: bool = False
    note: Optional[str] = None


class TokenBudgetRequest(BaseModel):
    messages: List[Message]
    model: Optional[str] = None


class TokenBudgetResponse(BaseModel):
    approx_tokens: int
    per_message: List[int]
    method: str = "heuristic_chars_per_token"


class PlanItem(BaseModel):
    index: int
    role: str
    tokens: int
    score: float
    kept: bool
    reasons: List[str] = []


class CompressPlanResponse(BaseModel):
    items: List[PlanItem]
    kept_indices: List[int]
    dropped_indices: List[int]
    approx_tokens_before: int
    approx_tokens_after: int


class CapabilitiesResponse(BaseModel):
    jina: bool
    embeddings: bool
    rerank: bool
    reader: bool


# --------------
# Utilities for simple compression + token estimates
# --------------


def _as_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    try:
        # Compact JSON for non-strings
        import json as _json

        return _json.dumps(content, separators=(",", ":"))
    except Exception:
        return str(content)


def _approx_tokens_for_text(text: str) -> int:
    # Simple heuristic: ~4 chars per token
    return max(1, (len(text) + 3) // 4)


def _approx_tokens_for_message(msg: Message) -> int:
    if isinstance(msg.content, str):
        return _approx_tokens_for_text(msg.content)
    total = 0
    for block in msg.content:
        if isinstance(block, dict):
            t = block.get("type")
            if t == "text":
                total += _approx_tokens_for_text(str(block.get("text", "")))
            elif t == "tool_use":
                total += _approx_tokens_for_text(_as_text(block.get("input")))
            elif t == "tool_result":
                total += _approx_tokens_for_text(_as_text(block.get("content")))
            else:
                total += _approx_tokens_for_text(_as_text(block))
        else:
            total += _approx_tokens_for_text(_as_text(block))
    # Small role overhead
    return total + 2


def _collect_tool_links(messages: List[Message]) -> Dict[str, Dict[str, List[int]]]:
    """Map tool_use id -> positions for uses/results.

    Returns dict: {tool_id: {"use": [i,...], "result": [i,...]}}
    """
    mapping: Dict[str, Dict[str, List[int]]] = {}
    for idx, m in enumerate(messages):
        blocks = m.content if isinstance(m.content, list) else []
        for b in blocks:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "tool_use" and b.get("id"):
                tid = str(b["id"]).strip()
                entry = mapping.setdefault(tid, {"use": [], "result": []})
                entry["use"].append(idx)
            elif b.get("type") == "tool_result" and b.get("tool_use_id"):
                tid = str(b["tool_use_id"]).strip()
                entry = mapping.setdefault(tid, {"use": [], "result": []})
                entry["result"].append(idx)
    return mapping


def _normalize_message(msg: Message) -> Message:
    # Ensure content is a list of blocks for non-string content
    if isinstance(msg.content, str):
        return msg
    blocks: List[Dict[str, Any]] = []
    for b in (msg.content or []):
        if isinstance(b, dict):
            t = b.get("type")
            if t in ("text", "tool_use", "tool_result", "image"):
                blocks.append(b)
            else:
                blocks.append({"type": "text", "text": _as_text(b)})
        else:
            blocks.append({"type": "text", "text": _as_text(b)})
    return Message(role=msg.role, content=blocks)


def _normalize_messages(messages: List[Message]) -> List[Message]:
    return [_normalize_message(m) for m in messages]


def _message_text(msg: Message) -> str:
    if isinstance(msg.content, str):
        return msg.content
    parts: List[str] = []
    for b in msg.content:
        if not isinstance(b, dict):
            parts.append(_as_text(b))
            continue
        t = b.get("type")
        if t == "text":
            parts.append(str(b.get("text", "")))
        elif t == "tool_use":
            # Include tool params succinctly
            parts.append(f"[TOOL_USE id={b.get('id','')} input={_as_text(b.get('input'))}]")
        elif t == "tool_result":
            parts.append(f"[TOOL_RESULT for={b.get('tool_use_id','')} content={_as_text(b.get('content'))}]")
        elif t == "image":
            parts.append(f"[IMAGE]")
        else:
            parts.append(_as_text(b))
    return "\n".join([p for p in parts if p])


def _tokenize(s: str) -> List[str]:
    import re as _re

    return [w for w in _re.findall(r"[A-Za-z0-9_]+", s.lower()) if w]


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    A = set(_tokenize(a))
    B = set(_tokenize(b))
    if not A or not B:
        return 0.0
    inter = len(A & B)
    union = len(A | B)
    if union == 0:
        return 0.0
    return inter / union


def _role_importance(role: str) -> float:
    # System > tool > user > assistant as a mild default
    if role == "system":
        return 1.0
    if role == "tool":
        return 0.8
    if role == "user":
        return 0.6
    if role == "assistant":
        return 0.5
    return 0.5


def _rank_messages(messages: List[Message], cfg: CompressConfig) -> List[float]:
    total = max(1, len(messages))
    query = cfg.query or ""
    scores: List[float] = []
    for i, m in enumerate(messages):
        recency = (i + 1) / total  # grows with index; later = higher
        sim = _similarity(query, _message_text(m)) if query else 0.0
        rolew = _role_importance(m.role)
        score = cfg.recency_weight * recency + cfg.similarity_weight * sim + cfg.role_weight * rolew
        scores.append(score)
    return scores


# --------------
# Routers
# --------------


api = APIRouter(prefix="/api", tags=["api"])


@api.post(
    "/echo",
    response_model=EchoResponse,
    summary="Echo a message",
    description="Returns the same message and its character length.",
)
async def echo(payload: EchoRequest) -> EchoResponse:
    msg = payload.message
    return EchoResponse(message=msg, length=len(msg))


@api.get(
    "/version",
    summary="Service version",
    description="Returns the service name and version.",
)
async def version(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    return {"service": settings.title, "version": settings.version}


# -----------------
# Context Compression + Tools + Capabilities
# -----------------


@api.post(
    "/context/chunk_tool_result",
    response_model=ChunkToolResultResponse,
    summary="Chunk large tool result",
    description="Break a large tool result content into token-sized chunks using a simple heuristic.",
)
async def chunk_tool_result(payload: ChunkToolResultRequest) -> ChunkToolResultResponse:
    text = _as_text(payload.content)
    # Heuristic: ~4 chars per token
    max_chars = max(1, payload.max_tokens) * 4
    chunks: List[str] = []
    i = 0
    while i < len(text) and len(chunks) < max(1, payload.max_chunks):
        chunks.append(text[i : i + max_chars])
        i += max_chars
    return ChunkToolResultResponse(chunks=chunks, meta={"max_tokens": payload.max_tokens, "max_chunks": payload.max_chunks})


@api.post(
    "/tools/expand_keep_indices",
    response_model=ExpandKeepIndicesResponse,
    summary="Expand keep indices for tool pairs",
    description="Ensure tool_use/tool_result pairs are both retained when compressing context.",
)
async def expand_keep_indices(payload: ExpandKeepIndicesRequest) -> ExpandKeepIndicesResponse:
    keep = set(int(i) for i in payload.keep_indices)
    links = _collect_tool_links(payload.messages)
    # If a kept index has a tool_use id, also keep any tool_result with that id (and vice versa)
    for tid, loc in links.items():
        use_idxs = set(loc.get("use") or [])
        res_idxs = set(loc.get("result") or [])
        if keep & use_idxs or keep & res_idxs:
            keep.update(use_idxs)
            keep.update(res_idxs)
    expanded = sorted(k for k in keep if 0 <= k < len(payload.messages))
    return ExpandKeepIndicesResponse(expanded_indices=expanded)


@api.post(
    "/tools/validate_pairing",
    response_model=ValidatePairingResponse,
    summary="Validate tool message pairing",
    description="Validate that tool_use ids have matching tool_result responses and are reasonably adjacent.",
)
async def validate_pairing(payload: ValidatePairingRequest) -> ValidatePairingResponse:
    links = _collect_tool_links(payload.messages)
    problems: List[str] = []
    suggestions: List[str] = []
    for tid, loc in links.items():
        uses = loc.get("use") or []
        results = loc.get("result") or []
        if not uses:
            problems.append(f"tool_result references unknown tool_use_id '{tid}'")
        if not results:
            problems.append(f"tool_use id '{tid}' has no matching tool_result")
        # Simple adjacency check: enforce that a result appears after a use
        if uses and results:
            if min(results) < min(uses):
                problems.append(f"tool_result for '{tid}' appears before tool_use")
    ok = not problems
    if not ok:
        suggestions.append("Ensure each tool_use id has a subsequent tool_result with matching tool_use_id.")
    return ValidatePairingResponse(ok=ok, problems=problems, suggestions=suggestions)


@api.post(
    "/context/compress",
    response_model=CompressResponse,
    summary="Compress a message list",
    description=(
        "Naive compression preserving the last N messages and optionally tool pairs, "
        "then trimming by ranked importance until within budget."
    ),
)
async def compress_context(payload: CompressRequest) -> CompressResponse:
    raw_messages = payload.messages
    cfg = payload.config
    messages = _normalize_messages(raw_messages)

    n = max(0, int(cfg.keep_last_n))
    budget = max(1, int(cfg.budget_tokens))

    # Precompute tokens and ranking
    per_msg = [_approx_tokens_for_message(m) for m in messages]
    scores = _rank_messages(messages, cfg)

    # Mandatory keeps: tail (last N) and optionally all system messages
    mandatory_tail = set(range(max(0, len(messages) - n), len(messages))) if n > 0 else set()
    mandatory_system = {i for i, m in enumerate(messages) if cfg.preserve_system and m.role == "system"}
    keep_set = set() | mandatory_tail | mandatory_system

    # Expand tool pairs for mandatory keeps
    if cfg.preserve_tool_pairs and keep_set:
        expanded = await expand_keep_indices(ExpandKeepIndicesRequest(messages=messages, keep_indices=sorted(keep_set)))
        keep_set = set(expanded.expanded_indices)

    # Greedy add by score until budget is met
    idxs = list(range(len(messages)))
    # Exclude already kept from candidates
    candidates = [i for i in idxs if i not in keep_set]
    candidates.sort(key=lambda i: scores[i], reverse=True)

    current_tokens = sum(per_msg[i] for i in keep_set)
    for i in candidates:
        need = per_msg[i]
        if current_tokens + need > budget:
            continue
        keep_set.add(i)
        current_tokens += need
        if cfg.preserve_tool_pairs:
            expanded = await expand_keep_indices(ExpandKeepIndicesRequest(messages=messages, keep_indices=sorted(keep_set)))
            newly_added = set(expanded.expanded_indices) - keep_set
            added_tokens = sum(per_msg[j] for j in newly_added)
            if current_tokens + added_tokens <= budget:
                keep_set |= set(expanded.expanded_indices)
                current_tokens += added_tokens

    kept_indices = sorted(keep_set)
    compressed_messages = [messages[i] for i in kept_indices]
    dropped_indices = [i for i in range(len(messages)) if i not in keep_set]

    # Simple abstractive summary of dropped content (if requested)
    summary = None
    if cfg.strategy in ("abstractive", "hybrid") and dropped_indices:
        # Create a naive multi-line summary from first sentences
        import re as _re

        lines: List[str] = []
        for i in dropped_indices[:10]:  # cap for brevity
            txt = _message_text(messages[i])
            sent = _re.split(r"(?<=[.!?])\s+", txt.strip())
            if sent and sent[0]:
                lines.append(f"- {sent[0][:200]}")
        summary = "\n".join(lines) if lines else None

    meta = {
        "approx_tokens_before": sum(_approx_tokens_for_message(m) for m in messages),
        "approx_tokens_after": sum(_approx_tokens_for_message(m) for m in compressed_messages),
        "strategy": cfg.strategy,
        "keep_last_n": cfg.keep_last_n,
        "preserve_tool_pairs": cfg.preserve_tool_pairs,
        "preserve_system": cfg.preserve_system,
    }

    return CompressResponse(
        compressed_messages=compressed_messages,
        kept_indices=kept_indices,
        dropped_indices=dropped_indices,
        summary=summary,
        meta=meta,
    )


@api.post(
    "/embeddings",
    response_model=EmbeddingsResponse,
    summary="Get embeddings (provider-gated)",
    description="Stub endpoint that reports availability based on JINA_API_KEY; integration pending.",
)
async def embeddings(payload: EmbeddingsRequest, request: Request) -> EmbeddingsResponse:
    jina_key = os.getenv("JINA_API_KEY")
    available = bool(jina_key)
    note = None if available else "Jina not configured (set JINA_API_KEY)."
    return EmbeddingsResponse(available=available, note=note)


@api.post(
    "/rerank",
    response_model=RerankResponse,
    summary="Rerank documents (provider-gated)",
    description="Stub endpoint that reports availability based on JINA_API_KEY; integration pending.",
)
async def rerank(payload: RerankRequest) -> RerankResponse:
    jina_key = os.getenv("JINA_API_KEY")
    available = bool(jina_key)
    note = None if available else "Jina not configured (set JINA_API_KEY)."
    return RerankResponse(available=available, note=note)


@api.post(
    "/read",
    response_model=ReadResponse,
    summary="Read webpage (provider-gated)",
    description="Stub endpoint that reports availability based on JINA_API_KEY; integration pending.",
)
async def read(payload: ReadRequest) -> ReadResponse:
    jina_key = os.getenv("JINA_API_KEY")
    available = bool(jina_key)
    note = None if available else "Jina not configured (set JINA_API_KEY)."
    return ReadResponse(content=None, metadata={}, available=available, note=note)


@api.post(
    "/token_budget/estimate",
    response_model=TokenBudgetResponse,
    summary="Estimate token usage",
    description="Heuristic token estimator across messages.",
)
async def token_budget_estimate(payload: TokenBudgetRequest) -> TokenBudgetResponse:
    per = [_approx_tokens_for_message(_normalize_message(m)) for m in payload.messages]
    return TokenBudgetResponse(approx_tokens=sum(per), per_message=per)


@api.post(
    "/context/plan",
    response_model=CompressPlanResponse,
    summary="Explain compression decisions",
    description="Return ranking, token counts, and keep/drop plan for a given budget.",
)
async def compression_plan(payload: CompressRequest) -> CompressPlanResponse:
    messages = _normalize_messages(payload.messages)
    cfg = payload.config
    per = [_approx_tokens_for_message(m) for m in messages]
    scores = _rank_messages(messages, cfg)

    # Reuse compress logic to get final keep/drop
    result = await compress_context(payload)
    kept = set(result.kept_indices)

    items: List[PlanItem] = []
    total = len(messages)
    for i, m in enumerate(messages):
        reasons: List[str] = []
        if cfg.preserve_system and m.role == "system":
            reasons.append("preserve_system")
        if i in range(max(0, total - cfg.keep_last_n), total):
            reasons.append("tail")
        # Tool pair reasons
        if cfg.preserve_tool_pairs:
            # Add coarse hints if message contains tool markers
            txt = _message_text(m)
            if "[TOOL_USE" in txt:
                reasons.append("tool_use")
            if "[TOOL_RESULT" in txt:
                reasons.append("tool_result")

        items.append(
            PlanItem(
                index=i,
                role=m.role,
                tokens=per[i],
                score=round(scores[i], 6),
                kept=(i in kept),
                reasons=reasons,
            )
        )

    return CompressPlanResponse(
        items=items,
        kept_indices=result.kept_indices,
        dropped_indices=result.dropped_indices,
        approx_tokens_before=sum(per),
        approx_tokens_after=sum(per[i] for i in result.kept_indices),
    )


@api.post(
    "/messages/normalize",
    response_model=Dict[str, Any],
    summary="Normalize messages",
    description="Coerce messages into a consistent block-based representation.",
)
async def normalize_messages_endpoint(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("messages") or []
    msgs = [Message(**m) if not isinstance(m, Message) else m for m in raw]
    normalized = _normalize_messages(msgs)
    return {"messages": [m.dict() for m in normalized]}


@api.get(
    "/capabilities",
    response_model=CapabilitiesResponse,
    summary="Service capabilities",
    description="Reports which optional provider-backed features are available.",
)
async def capabilities() -> CapabilitiesResponse:
    jina = bool(os.getenv("JINA_API_KEY"))
    return CapabilitiesResponse(jina=jina, embeddings=jina, rerank=jina, reader=jina)


# -----------------
# Message previews, UI, sending, status, attachments
# -----------------


class MessagePayload(BaseModel):
    role: str
    content: Any
    attachments: Optional[List[Dict[str, Any]]] = None
    metrics: Optional[Dict[str, Any]] = None


class PreviewRequest(BaseModel):
    message: MessagePayload


class PreviewResponse(BaseModel):
    preview: Dict[str, Any]


def _coerce_attachment(item: Dict[str, Any]) -> DCAttachment:
    kind_str = (item.get("kind") or "").lower().strip()
    kind = None
    if kind_str:
        try:
            kind = DCAttachmentKind(kind_str)
        except Exception:
            kind = None
    if not kind:
        kind = guess_attachment_kind(item.get("name"), item.get("content_type"))

    url = str(item.get("url") or item.get("src") or "")
    name = item.get("name")
    meta = item.get("meta") or {}

    if kind == DCAttachmentKind.image:
        return DCImageAttachment(url=url, name=name)
    if kind == DCAttachmentKind.link:
        return DCLinkAttachment(url=url, name=name)
    if kind == DCAttachmentKind.file:
        return DCFileAttachment(url=url, name=name)
    # Fall back to generic attachment
    return DCAttachment(kind=kind, url=url, name=name)


def _coerce_message(mp: MessagePayload) -> DCMessage:
    mc = DCMessageCreate(role=mp.role, content=mp.content)
    msg = DCMessage.from_create(mc)
    if mp.attachments:
        msg.attachments = [_coerce_attachment(a) for a in mp.attachments]
    return msg


@api.post(
    "/messages/preview",
    response_model=PreviewResponse,
    summary="Build a message preview",
    description="Generate a preview (title, summary, keywords, thumbnail, badges) for a message payload.",
)
async def message_preview(payload: PreviewRequest) -> PreviewResponse:
    msg = _coerce_message(payload.message)
    preview = build_default_preview(msg)
    return PreviewResponse(preview=jsonable_encoder(preview))


class UIRequest(BaseModel):
    message: MessagePayload
    ui_config: Optional[Dict[str, Any]] = None


class UIResponse(BaseModel):
    ui: Dict[str, Any]


@api.post(
    "/messages/ui",
    response_model=UIResponse,
    summary="Build UI components",
    description="Generate a declarative UI description for rendering a message.",
)
async def message_ui(payload: UIRequest) -> UIResponse:
    msg = _coerce_message(payload.message)
    ui_cfg = DCConversationUIConfig(**payload.ui_config) if payload.ui_config else None
    ui = build_message_ui(msg, ui_cfg)
    return UIResponse(ui=jsonable_encoder(ui))


class MessageSendRequest(BaseModel):
    conversation_id: Optional[str] = None
    message: MessagePayload
    metadata: Optional[Dict[str, Any]] = None


class MessageSendResponse(BaseModel):
    message: Dict[str, Any]
    conversation: Optional[Dict[str, Any]] = None


@api.post(
    "/messages/send",
    response_model=MessageSendResponse,
    summary="Send a message (stub)",
    description="Accept a message and return an enriched representation (no persistence in stub).",
)
async def message_send(payload: MessageSendRequest) -> MessageSendResponse:
    msg = _coerce_message(payload.message)
    # Compute preview + UI eagerly
    msg.preview = build_default_preview(msg)
    msg.ui = build_message_ui(msg)
    return MessageSendResponse(message=jsonable_encoder(msg), conversation=None)


class MessageStatusUpdate(BaseModel):
    message_id: str
    status: Literal["sending", "sent", "delivered", "read", "failed"]
    error: Optional[str] = None


@api.patch(
    "/messages/status",
    summary="Update message status (stub)",
    description="Echo endpoint to acknowledge a message status update.",
)
async def message_status(payload: MessageStatusUpdate) -> Dict[str, Any]:
    return {"ok": True, "message_id": payload.message_id, "status": payload.status, "error": payload.error}


class PresignRequest(BaseModel):
    name: str
    content_type: str
    size_bytes: Optional[int] = None
    kind: Optional[str] = None
    checksum: Optional[str] = None


class PresignResponse(BaseModel):
    upload: Dict[str, Any]
    attachment: Dict[str, Any]


@api.post(
    "/attachments/presign",
    response_model=PresignResponse,
    summary="Get an upload URL (stub)",
    description="Return a temporary upload specification and a placeholder attachment record.",
)
async def attachments_presign(payload: PresignRequest) -> PresignResponse:
    # Stub: generate a fake pre-signed URL and attachment object
    expires_at = int(time.time() + 5 * 60)
    upload = {
        "method": "PUT",
        "url": "https://example.com/upload/placeholder",
        "headers": {"content-type": payload.content_type},
        "expires_at": expires_at,
    }
    kind = payload.kind or guess_attachment_kind(payload.name, payload.content_type).value
    attachment = {
        "id": "att_" + str(int(time.time() * 1000)),
        "url": "https://cdn.example.com/placeholder/" + payload.name,
        "name": payload.name,
        "kind": kind,
    }
    return PresignResponse(upload=upload, attachment=attachment)


# -----------------
# App factory
# -----------------


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.debug)

    app = FastAPI(
        title=settings.title,
        version=settings.version,
        description=(
            "Conversations API — health, versioning, and basic utilities. "
            "Use the root (/) for a quick overview and /docs for interactive schema."
        ),
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    # CORS
    allow_origins = settings.cors_origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Built-in health + readiness
    @app.get(
        "/healthz",
        tags=["system"],
        response_model=HealthResponse,
        summary="Health check",
        description="Liveness probe with uptime and metadata.",
    )
    async def healthz(settings: Settings = Depends(get_settings)) -> HealthResponse:
        return HealthResponse(
            status="ok",
            uptime_seconds=max(0.0, time.time() - START_TIME),
            service=settings.title,
            version=settings.version,
            environment=settings.env,
        )

    @app.get(
        "/readyz",
        tags=["system"],
        summary="Readiness check",
        description="Lightweight readiness probe indicating service is ready to serve.",
    )  # lightweight readiness
    async def readyz() -> dict[str, str]:
        return {"status": "ready"}

    class EndpointDoc(BaseModel):
        method: str
        path: str
        summary: str
        description: str
        request_example: dict | None = None
        response_example: dict | None = None

    class APIEntryPoint(BaseModel):
        service: str
        version: str
        environment: str
        description: str
        links: dict[str, str]
        endpoints: list[EndpointDoc]
        usage: dict[str, str]

    def _endpoint_docs() -> list[EndpointDoc]:
        base: list[EndpointDoc] = [
            EndpointDoc(
                method="GET",
                path="/healthz",
                summary="Health check",
                description="Liveness probe with uptime and service metadata.",
                request_example=None,
                response_example={
                    "status": "ok",
                    "uptime_seconds": 12.34,
                    "service": "Conversations API",
                    "version": "0.1.0",
                    "environment": "development",
                },
            ),
            EndpointDoc(
                method="GET",
                path="/readyz",
                summary="Readiness check",
                description="Lightweight readiness probe indicating service is ready to accept traffic.",
                request_example=None,
                response_example={"status": "ready"},
            ),
            EndpointDoc(
                method="GET",
                path="/api/version",
                summary="Service version",
                description="Returns the service name and version string.",
                request_example=None,
                response_example={"service": "Conversations API", "version": "0.1.0"},
            ),
            EndpointDoc(
                method="POST",
                path="/api/echo",
                summary="Echo message",
                description="Echoes the provided message and includes its length.",
                request_example={"message": "hello"},
                response_example={"message": "hello", "length": 5},
            ),
        ]

        # Add API endpoints
        base.extend(
            [
                EndpointDoc(
                    method="POST",
                    path="/api/messages/preview",
                    summary="Message preview",
                    description="Generate title/summary/keywords/thumbnail/badges for a message.",
                    request_example={
                        "message": {
                            "role": "user",
                            "content": "Check this screenshot",
                            "attachments": [{"kind": "image", "url": "https://.../img.jpg"}],
                        }
                    },
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/messages/ui",
                    summary="Message UI",
                    description="Build a declarative UI component tree for a message.",
                    request_example={
                        "message": {"role": "assistant", "content": [{"type": "markdown", "markdown": "**Hi**"}]},
                        "ui_config": {"default_tool_result_collapsed": True},
                    },
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/messages/send",
                    summary="Send message (stub)",
                    description="Accept a message, compute preview + UI (no persistence).",
                    request_example={
                        "conversation_id": None,
                        "message": {"role": "user", "content": "hello"},
                    },
                ),
                EndpointDoc(
                    method="PATCH",
                    path="/api/messages/status",
                    summary="Update status (stub)",
                    description="Acknowledge message status update.",
                    request_example={"message_id": "m1", "status": "delivered"},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/attachments/presign",
                    summary="Pre-sign upload (stub)",
                    description="Return upload spec and a placeholder attachment record.",
                    request_example={"name": "file.pdf", "content_type": "application/pdf"},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/context/plan",
                    summary="Compression plan",
                    description="Explain ranked decisions, tokens, and which messages are kept/dropped.",
                    request_example={
                        "messages": [
                            {"role": "system", "content": "You are helpful."},
                            {"role": "user", "content": "hello"},
                        ],
                        "config": {"budget_tokens": 2000, "keep_last_n": 2},
                    },
                ),
                EndpointDoc(
                    method="GET",
                    path="/api/capabilities",
                    summary="Capabilities",
                    description="Reports provider-gated feature availability.",
                    response_example={"jina": False, "embeddings": False, "rerank": False, "reader": False},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/context/compress",
                    summary="Compress context",
                    description="Compress messages to fit a token budget, preserving last N and tool pairs.",
                    request_example={
                        "messages": [
                            {"role": "user", "content": "hello"},
                            {"role": "assistant", "content": [{"type": "text", "text": "hi"}]},
                        ],
                        "config": {"budget_tokens": 2000, "keep_last_n": 2},
                    },
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/context/chunk_tool_result",
                    summary="Chunk tool result",
                    description="Split large tool output into token-sized chunks (heuristic).",
                    request_example={"content": {"json": "big"}, "max_tokens": 512},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/tools/validate_pairing",
                    summary="Validate tool pairing",
                    description="Check tool_use/tool_result pairing and order.",
                    request_example={"messages": [{"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "input": {}}]}]},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/tools/expand_keep_indices",
                    summary="Expand keep indices",
                    description="Expand a set of indices to include tool pairs.",
                    request_example={"messages": [], "keep_indices": [0, 1]},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/embeddings",
                    summary="Embeddings (stub)",
                    description="Provider-gated embeddings endpoint; returns availability only in stub.",
                    request_example={"texts": ["hello", "world"]},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/rerank",
                    summary="Rerank (stub)",
                    description="Provider-gated rerank endpoint; returns availability only in stub.",
                    request_example={"query": "q", "documents": ["a", "b"]},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/read",
                    summary="Read (stub)",
                    description="Provider-gated reader endpoint; returns availability only in stub.",
                    request_example={"url": "https://example.com"},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/token_budget/estimate",
                    summary="Estimate token usage",
                    description="Heuristic estimator for token usage across messages.",
                    request_example={"messages": [{"role": "user", "content": "hello"}]},
                ),
                EndpointDoc(
                    method="POST",
                    path="/api/messages/normalize",
                    summary="Normalize messages",
                    description="Coerce messages to a consistent block format.",
                    request_example={"messages": [{"role": "user", "content": [{"foo": "bar"}]}]},
                ),
            ]
        )

        return base

    @app.get("/", tags=["system"], response_model=APIEntryPoint)
    async def root(settings: Settings = Depends(get_settings)) -> APIEntryPoint:
        return APIEntryPoint(
            service=settings.title,
            version=settings.version,
            environment=settings.env,
            description=(
                "HTTP interface for simple conversation utilities. "
                "See docs for schema and interactive testing."
            ),
            links={
                "docs": "/docs",
                "openapi": "/openapi.json",
                "health": "/healthz",
                "ready": "/readyz",
            },
            endpoints=_endpoint_docs(),
            usage={
                "health": "curl -s $BASE/healthz",
                "version": "curl -s $BASE/api/version",
                "echo": "curl -s -X POST $BASE/api/echo -H 'Content-Type: application/json' -d '{\"message\":\"hello\"}'",
            },
        )

    # Custom not found handler (optional polish)
    @app.exception_handler(404)
    async def not_found(_: Request, exc: Exception):  # type: ignore[override]
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": "Not Found"})

    # Register API routes
    app.include_router(api)

    return app


# Singleton app for ASGI servers
app = create_app()


if __name__ == "__main__":
    # Local development entrypoint: `python fastapi_app.py`
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "0") == "1"
    uvicorn.run("fastapi_app:app", host=host, port=port, reload=reload, log_level="debug")
