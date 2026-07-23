"""
Canonical dataclasses for the Conversation Management API.

These mirror the legacy Pydantic models in a lightweight, framework-agnostic form
and include small helpers to normalize content blocks and serialize to JSON‑safe dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Literal
from uuid import UUID, uuid4


# ---------------------------
# Utilities
# ---------------------------


def _now() -> datetime:
    return datetime.utcnow()


def _jsonify(obj: Any) -> Any:
    """Recursively convert dataclass instances, UUIDs, and datetimes to JSON‑safe types."""
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _jsonify(v) for k, v in asdict(obj).items()}
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    return obj


# ---------------------------
# Enums
# ---------------------------


class MessageRole(str, Enum):
    system = "system"
    user = "user"
    assistant = "assistant"
    tool = "tool"


class ContentBlockType(str, Enum):
    text = "text"
    markdown = "markdown"
    code = "code"
    image = "image"
    audio = "audio"
    video = "video"
    table = "table"
    tool_use = "tool_use"
    tool_result = "tool_result"


class AttachmentKind(str, Enum):
    image = "image"
    file = "file"
    audio = "audio"
    video = "video"
    link = "link"


# ---------------------------
# Content Blocks (input params)
# ---------------------------


@dataclass
class TextBlockParam:
    type: Literal["text"] = "text"
    text: str = ""


@dataclass
class ImageBlockParam:
    type: Literal["image"] = "image"
    image_url: str = ""


@dataclass
class ToolUseBlockParam:
    type: Literal["tool_use"] = "tool_use"
    id: str = ""
    input: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResultBlockParam:
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str = ""
    content: Any = None
    is_error: bool = False


@dataclass
class MarkdownBlockParam:
    type: Literal["markdown"] = "markdown"
    markdown: str = ""


@dataclass
class CodeBlockParam:
    type: Literal["code"] = "code"
    code: str = ""
    language: Optional[str] = None
    filename: Optional[str] = None
    line_numbers: bool = False


@dataclass
class AudioBlockParam:
    type: Literal["audio"] = "audio"
    audio_url: str = ""
    transcript: Optional[str] = None


@dataclass
class VideoBlockParam:
    type: Literal["video"] = "video"
    video_url: str = ""
    transcript: Optional[str] = None


@dataclass
class TableBlockParam:
    type: Literal["table"] = "table"
    headers: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)


ContentBlockParam = Union[
    TextBlockParam,
    MarkdownBlockParam,
    CodeBlockParam,
    ImageBlockParam,
    AudioBlockParam,
    VideoBlockParam,
    TableBlockParam,
    ToolUseBlockParam,
    ToolResultBlockParam,
]


# ---------------------------
# Content Blocks (canonical stored form)
# ---------------------------


@dataclass
class TextBlock:
    type: Literal["text"] = "text"
    text: str = ""


@dataclass
class ImageBlock:
    type: Literal["image"] = "image"
    image_url: str = ""


@dataclass
class MarkdownBlock:
    type: Literal["markdown"] = "markdown"
    markdown: str = ""


@dataclass
class CodeBlock:
    type: Literal["code"] = "code"
    code: str = ""
    language: Optional[str] = None
    filename: Optional[str] = None
    line_numbers: bool = False


@dataclass
class AudioBlock:
    type: Literal["audio"] = "audio"
    audio_url: str = ""
    transcript: Optional[str] = None


@dataclass
class VideoBlock:
    type: Literal["video"] = "video"
    video_url: str = ""
    transcript: Optional[str] = None


@dataclass
class TableBlock:
    type: Literal["table"] = "table"
    headers: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)


@dataclass
class ToolUseBlock:
    type: Literal["tool_use"] = "tool_use"
    id: str = ""
    input: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResultBlock:
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str = ""
    content: Any = None
    is_error: bool = False


ContentBlock = Union[
    TextBlock,
    MarkdownBlock,
    CodeBlock,
    ImageBlock,
    AudioBlock,
    VideoBlock,
    TableBlock,
    ToolUseBlock,
    ToolResultBlock,
]


def _coerce_param_to_block(p: Union[ContentBlockParam, Dict[str, Any], str, Any]) -> ContentBlock:
    if isinstance(p, str):
        return TextBlock(text=p)
    if isinstance(p, dict):
        t = p.get("type")
        if t == "text":
            return TextBlock(text=str(p.get("text", "")))
        if t == "markdown":
            return MarkdownBlock(markdown=str(p.get("markdown", p.get("text", ""))))
        if t == "code":
            return CodeBlock(
                code=str(p.get("code", "")),
                language=p.get("language"),
                filename=p.get("filename"),
                line_numbers=bool(p.get("line_numbers", False)),
            )
        if t == "image":
            return ImageBlock(image_url=str(p.get("image_url") or p.get("url") or ""))
        if t == "audio":
            return AudioBlock(audio_url=str(p.get("audio_url") or p.get("url") or ""), transcript=p.get("transcript"))
        if t == "video":
            return VideoBlock(video_url=str(p.get("video_url") or p.get("url") or ""), transcript=p.get("transcript"))
        if t == "table":
            return TableBlock(headers=list(p.get("headers") or []), rows=list(p.get("rows") or []))
        if t == "tool_use":
            return ToolUseBlock(id=str(p.get("id", "")), input=dict(p.get("input") or {}))
        if t == "tool_result":
            return ToolResultBlock(
                tool_use_id=str(p.get("tool_use_id", "")),
                content=p.get("content"),
                is_error=bool(p.get("is_error", False)),
            )
        # Fallback to text block with compact JSON
        try:
            import json as _json

            return TextBlock(text=_json.dumps(p, separators=(",", ":")))
        except Exception:
            return TextBlock(text=str(p))
    # If already a param dataclass instance
    if isinstance(p, TextBlockParam):
        return TextBlock(text=p.text)
    if isinstance(p, MarkdownBlockParam):
        return MarkdownBlock(markdown=p.markdown)
    if isinstance(p, CodeBlockParam):
        return CodeBlock(code=p.code, language=p.language, filename=p.filename, line_numbers=p.line_numbers)
    if isinstance(p, ImageBlockParam):
        return ImageBlock(image_url=p.image_url)
    if isinstance(p, AudioBlockParam):
        return AudioBlock(audio_url=p.audio_url, transcript=p.transcript)
    if isinstance(p, VideoBlockParam):
        return VideoBlock(video_url=p.video_url, transcript=p.transcript)
    if isinstance(p, TableBlockParam):
        return TableBlock(headers=p.headers, rows=p.rows)
    if isinstance(p, ToolUseBlockParam):
        return ToolUseBlock(id=p.id, input=p.input)
    if isinstance(p, ToolResultBlockParam):
        return ToolResultBlock(tool_use_id=p.tool_use_id, content=p.content, is_error=p.is_error)
    # Unknown types → stringify
    return TextBlock(text=str(p))


# ---------------------------
# Attachments, Previews, Reactions, Metrics
# ---------------------------


@dataclass
class PreviewAsset:
    url: str
    width: Optional[int] = None
    height: Optional[int] = None
    duration_ms: Optional[int] = None
    content_type: Optional[str] = None


@dataclass
class MediaMeta:
    width: Optional[int] = None
    height: Optional[int] = None
    duration_ms: Optional[int] = None
    size_bytes: Optional[int] = None
    content_type: Optional[str] = None
    aspect_ratio: Optional[float] = None
    dominant_color: Optional[str] = None
    page_count: Optional[int] = None
    codec: Optional[str] = None
    hash: Optional[str] = None


@dataclass
class Attachment:
    url: str
    kind: AttachmentKind
    id: str = field(default_factory=lambda: str(uuid4()))
    name: Optional[str] = None
    meta: MediaMeta = field(default_factory=MediaMeta)
    preview: Optional[PreviewAsset] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ImageAttachment(Attachment):
    kind: AttachmentKind = AttachmentKind.image
    alt_text: Optional[str] = None
    thumbnails: Dict[str, PreviewAsset] = field(default_factory=dict)


@dataclass
class FileAttachment(Attachment):
    kind: AttachmentKind = AttachmentKind.file
    extension: Optional[str] = None
    page_count: Optional[int] = None
    text_preview: Optional[str] = None


@dataclass
class LinkAttachment(Attachment):
    kind: AttachmentKind = AttachmentKind.link
    title: Optional[str] = None
    description: Optional[str] = None
    site_name: Optional[str] = None
    icon_url: Optional[str] = None


@dataclass
class Reaction:
    emoji: str
    count: int = 0
    user_ids: List[str] = field(default_factory=list)


class MessageStatus(str, Enum):
    sending = "sending"
    sent = "sent"
    delivered = "delivered"
    read = "read"
    failed = "failed"


@dataclass
class MessageMetrics:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: Optional[int] = None


@dataclass
class SourceRef:
    ref_id: str
    title: Optional[str] = None
    url: Optional[str] = None
    snippet: Optional[str] = None
    score: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CitationSpan:
    start: int
    end: int
    ref_id: str


@dataclass
class QuoteRef:
    message_id: str
    span_start: Optional[int] = None
    span_end: Optional[int] = None


@dataclass
class Badge:
    text: str
    color: Optional[str] = None
    kind: Optional[str] = None  # e.g., "info", "warning", "success"


@dataclass
class MessagePreview:
    title: Optional[str] = None
    summary: Optional[str] = None
    keywords: List[str] = field(default_factory=list)
    thumbnail_url: Optional[str] = None
    badges: List[Badge] = field(default_factory=list)
    color: Optional[str] = None


# ---------------------------
# Messages
# ---------------------------


@dataclass
class MessageCreate:
    role: MessageRole = MessageRole.user
    content: Union[str, List[ContentBlockParam]] = ""


@dataclass
class Message:
    role: MessageRole
    content: List[ContentBlock] = field(default_factory=list)
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    version: int = 1
    embedding: Optional[List[float]] = None
    is_stale: bool = False
    attachments: List[Attachment] = field(default_factory=list)
    reactions: List[Reaction] = field(default_factory=list)
    status: MessageStatus = MessageStatus.sent
    metrics: Optional[MessageMetrics] = None
    sources: List[SourceRef] = field(default_factory=list)
    citations: List[CitationSpan] = field(default_factory=list)
    quote: Optional[QuoteRef] = None
    preview: Optional[MessagePreview] = None
    ui: Optional["MessageUI"] = None
    custom: Dict[str, Any] = field(default_factory=dict)
    # Threading
    parent_id: Optional[str] = None
    root_id: Optional[str] = None
    thread_index: Optional[int] = None

    @staticmethod
    def from_create(m: Union[MessageCreate, Dict[str, Any]]) -> "Message":
        if isinstance(m, dict):
            role = MessageRole(m.get("role", "user"))
            content = m.get("content", "")
        else:
            role = m.role
            content = m.content
        if isinstance(content, str):
            blocks = [TextBlock(text=content)] if content else []
        else:
            blocks = [_coerce_param_to_block(b) for b in (content or [])]
        return Message(role=role, content=blocks)

    def to_dict(self) -> Dict[str, Any]:
        return _jsonify(self)


# ---------------------------
# Conversation
# ---------------------------


@dataclass
class Conversation:
    id: str = field(default_factory=lambda: str(uuid4()))
    title: Optional[str] = None
    system: Optional[str] = None
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    version: int = 1
    messages: List[Message] = field(default_factory=list)
    embedding: Optional[List[float]] = None
    ui_config: Optional["ConversationUIConfig"] = None
    labels: List[str] = field(default_factory=list)
    archived: bool = False
    pinned_message_ids: List[str] = field(default_factory=list)

    @staticmethod
    def new_with_messages(
        messages: List[Union[MessageCreate, Dict[str, Any]]],
        **kwargs: Any,
    ) -> "Conversation":
        return Conversation(messages=[Message.from_create(m) for m in messages], **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        return _jsonify(self)


# ---------------------------
# Requests / Responses
# ---------------------------


@dataclass
class CreateConversationRequest:
    messages: List[MessageCreate] = field(default_factory=list)
    title: Optional[str] = None
    system: Optional[str] = None
    agent_id: Optional[str] = None
    user_id: Optional[str] = None


@dataclass
class CreateConversationResponse:
    conversation: Conversation


@dataclass
class GetConversationResponse:
    conversation: Conversation


@dataclass
class AddMessageRequest:
    message: MessageCreate
    user_id: Optional[str] = None
    agent_id: Optional[str] = None


@dataclass
class AddMessageResponse:
    message: Message


@dataclass
class ListConversationsRequest:
    user_id: Optional[str] = None
    agent_id: Optional[str] = None
    page: int = 1
    page_size: int = 10


@dataclass
class ListConversationsResponse:
    conversations: List[Conversation] = field(default_factory=list)
    total_pages: int = 0
    total: int = 0


@dataclass
class DeleteLastMessageRequest:
    conversation_id: str


@dataclass
class DeleteLastMessageResponse:
    status: str
    removed_message_id: Optional[str] = None


@dataclass
class MessageSendRequest:
    conversation_id: Optional[str] = None
    message: MessageCreate = field(default_factory=MessageCreate)
    attachments: List[Attachment] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MessageSendResponse:
    message: Message
    conversation: Optional[Conversation] = None


@dataclass
class MessageStatusUpdate:
    message_id: str
    status: MessageStatus
    error: Optional[str] = None


@dataclass
class AttachmentUploadSpec:
    method: str  # e.g., PUT
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    fields: Dict[str, Any] = field(default_factory=dict)  # for form-post type
    expires_at: Optional[datetime] = None


@dataclass
class AttachmentIngestRequest:
    name: str
    content_type: str
    size_bytes: Optional[int] = None
    kind: AttachmentKind = AttachmentKind.file
    checksum: Optional[str] = None


@dataclass
class AttachmentIngestResponse:
    upload: AttachmentUploadSpec
    attachment: Attachment


# ---------------------------
# Composer / Draft state (UI support)
# ---------------------------


@dataclass
class DraftAttachment:
    name: str
    content_type: str
    size_bytes: Optional[int] = None
    local_path: Optional[str] = None
    kind: AttachmentKind = AttachmentKind.file


@dataclass
class MessageComposerState:
    text: str = ""
    attachments: List[DraftAttachment] = field(default_factory=list)
    selected_tools: List[str] = field(default_factory=list)
    mentions: List[str] = field(default_factory=list)
    max_tokens_hint: Optional[int] = None


# ---------------------------
# Conveniences
# ---------------------------


def conversation_from_request(req: CreateConversationRequest) -> Conversation:
    return Conversation.new_with_messages(
        messages=req.messages,
        title=req.title,
        system=req.system,
        agent_id=req.agent_id,
        user_id=req.user_id,
    )


# ---------------------------
# UI Component Schema (declarative render tree)
# ---------------------------


class UIComponentKind(str, Enum):
    text = "text"  # markdown/plain
    code = "code"
    image = "image"
    audio = "audio"
    video = "video"
    link_card = "link_card"
    tool_call = "tool_call"
    tool_result = "tool_result"
    attachment_gallery = "attachment_gallery"
    badge = "badge"
    reaction_bar = "reaction_bar"
    collapsible = "collapsible"
    chips = "chips"
    list = "list"
    grid = "grid"
    quote = "quote"
    markdown = "markdown"
    table = "table"


@dataclass
class UIComponent:
    kind: UIComponentKind
    props: Dict[str, Any] = field(default_factory=dict)
    children: List["UIComponent"] = field(default_factory=list)
    key: Optional[str] = None
    render_hint: Optional[str] = None  # e.g., inline|block|aside
    visible: bool = True


# Strongly-typed props (optional helpers)


@dataclass
class CodeProps:
    code: str
    language: Optional[str] = None
    filename: Optional[str] = None
    line_numbers: bool = False


@dataclass
class ImageProps:
    src: str
    alt: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    caption: Optional[str] = None


@dataclass
class LinkCardProps:
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    site_name: Optional[str] = None


@dataclass
class ToolCallProps:
    id: str
    name: Optional[str] = None
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResultProps:
    tool_use_id: str
    content: Any
    is_error: bool = False
    collapsed: bool = True


@dataclass
class AttachmentGalleryItem:
    kind: str
    url: str
    name: Optional[str] = None
    preview_url: Optional[str] = None
    content_type: Optional[str] = None


@dataclass
class AttachmentGalleryProps:
    items: List[AttachmentGalleryItem] = field(default_factory=list)
    max_rows: Optional[int] = None


@dataclass
class ReactionBarProps:
    reactions: List[Reaction] = field(default_factory=list)


@dataclass
class CollapsibleProps:
    title: Optional[str] = None
    collapsed: bool = True


@dataclass
class MarkdownProps:
    text: str


@dataclass
class TextProps:
    text: str


@dataclass
class MessageUI:
    components: List[UIComponent] = field(default_factory=list)
    preview: Optional[MessagePreview] = None


@dataclass
class ConversationUIConfig:
    theme: Dict[str, Any] = field(default_factory=dict)
    show_avatars: bool = True
    show_timestamps: bool = True
    group_window_seconds: int = 120
    bubble_variant: str = "rounded"  # rounded|square
    enable_reactions: bool = True
    default_tool_result_collapsed: bool = True
    attachments_gallery_max_rows: int = 2
    code_theme: str = "github-dark"
    density: str = "comfortable"  # compact|comfortable|spacious
    role_colors: Dict[str, str] = field(default_factory=lambda: {
        "system": "#8A8A8A",
        "user": "#3B82F6",
        "assistant": "#A855F7",
        "tool": "#10B981",
    })


# ---------------------------
# Helper: default preview builder (heuristic)
# ---------------------------


def build_default_preview(msg: Message) -> MessagePreview:
    # Prefer image attachment thumbnails
    thumb: Optional[str] = None
    for att in msg.attachments:
        if att.kind == AttachmentKind.image and att.preview and att.preview.url:
            thumb = att.preview.url
            break
    # Title: first non-empty line of text block
    title: Optional[str] = None
    summary: Optional[str] = None
    text_chunks: List[str] = []
    for b in msg.content:
        if isinstance(b, TextBlock) and b.text:
            text_chunks.append(b.text)
    merged = "\n".join(text_chunks).strip()
    if merged:
        lines = [ln for ln in merged.splitlines() if ln.strip()]
        if lines:
            title = lines[0][:140]
            rest = " ".join(lines[1:]).strip()
            if rest:
                summary = (rest[:280] + "…") if len(rest) > 280 else rest
    # Basic keyword extraction
    keywords: List[str] = extract_keywords(merged)
    # Basic badges
    badges: List[Badge] = build_badges(msg)
    color = None
    if any(b.kind == "warning" for b in badges):
        color = "yellow"
    elif any(b.kind == "error" for b in badges):
        color = "red"
    return MessagePreview(title=title, summary=summary, thumbnail_url=thumb, keywords=keywords, badges=badges, color=color)


def build_ui_components_for_message(
    msg: Message,
    ui_cfg: Optional[ConversationUIConfig] = None,
) -> List[UIComponent]:
    comps: List[UIComponent] = []
    # Map content blocks to components
    for b in msg.content:
        if isinstance(b, MarkdownBlock):
            comps.append(UIComponent(kind=UIComponentKind.markdown, props={"text": b.markdown}))
        elif isinstance(b, CodeBlock):
            props = CodeProps(code=b.code, language=b.language, filename=b.filename, line_numbers=b.line_numbers)
            comps.append(UIComponent(kind=UIComponentKind.code, props=asdict(props)))
        elif isinstance(b, TextBlock):
            comps.append(UIComponent(kind=UIComponentKind.markdown, props={"text": b.text}))
        elif isinstance(b, ImageBlock):
            imgp = ImageProps(src=b.image_url)
            comps.append(UIComponent(kind=UIComponentKind.image, props=asdict(imgp)))
        elif isinstance(b, AudioBlock):
            comps.append(UIComponent(kind=UIComponentKind.audio, props={"src": b.audio_url, "transcript": b.transcript}))
        elif isinstance(b, VideoBlock):
            comps.append(UIComponent(kind=UIComponentKind.video, props={"src": b.video_url, "transcript": b.transcript}))
        elif isinstance(b, TableBlock):
            comps.append(UIComponent(kind=UIComponentKind.table, props={"headers": b.headers, "rows": b.rows}))
        elif isinstance(b, ToolUseBlock):
            comps.append(
                UIComponent(
                    kind=UIComponentKind.tool_call,
                    props=asdict(ToolCallProps(id=b.id, args=b.input)),
                )
            )
        elif isinstance(b, ToolResultBlock):
            comps.append(
                UIComponent(
                    kind=UIComponentKind.tool_result,
                    props=asdict(ToolResultProps(
                        tool_use_id=b.tool_use_id,
                        content=b.content,
                        is_error=b.is_error,
                        collapsed=(ui_cfg.default_tool_result_collapsed if ui_cfg else True),
                    )),
                )
            )
        else:
            # Fallback as plain text
            comps.append(UIComponent(kind=UIComponentKind.text, props={"text": str(b)}))

    # Attachment gallery
    if msg.attachments:
        items = []
        for a in msg.attachments:
            items.append(AttachmentGalleryItem(
                kind=a.kind.value,
                url=a.url,
                name=a.name,
                preview_url=(a.preview.url if a.preview else None),
                content_type=(a.meta.content_type if a.meta else None),
            ))
        comps.append(UIComponent(kind=UIComponentKind.attachment_gallery, props=asdict(AttachmentGalleryProps(items=[asdict(i) for i in items], max_rows=(ui_cfg.attachments_gallery_max_rows if ui_cfg else None)))))

    # Reactions
    if msg.reactions and (ui_cfg.enable_reactions if ui_cfg else True):
        rb = ReactionBarProps(reactions=msg.reactions)
        comps.append(UIComponent(kind=UIComponentKind.reaction_bar, props=asdict(rb)))

    # Quote
    if msg.quote:
        comps.append(
            UIComponent(kind=UIComponentKind.quote, props={"message_id": msg.quote.message_id, "span": [msg.quote.span_start, msg.quote.span_end]})
        )

    return comps


def build_message_ui(msg: Message, ui_cfg: Optional[ConversationUIConfig] = None) -> "MessageUI":
    preview = msg.preview or build_default_preview(msg)
    components = build_ui_components_for_message(msg, ui_cfg)
    return MessageUI(components=components, preview=preview)


# ---------------------------
# Preview helpers: keywords and badges
# ---------------------------


_STOPWORDS: set = {
    "the","a","an","and","or","but","if","then","else","for","to","of","in","on","at","by","with","as","is","are","was","were","be","been","it","this","that","these","those","i","you","he","she","we","they","them","us","our","your","their"
}


def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
    import re as _re

    tokens = [t for t in _re.findall(r"[A-Za-z0-9_]{3,}", (text or "").lower()) if t not in _STOPWORDS]
    if not tokens:
        return []
    freq: Dict[str, int] = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1
    ranked = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    return [w for w,_ in ranked[:max_keywords]]


def build_badges(msg: Message) -> List[Badge]:
    badges: List[Badge] = []
    # Tool call indicator
    has_tool = any(isinstance(b, ToolUseBlock) for b in msg.content)
    if has_tool:
        badges.append(Badge(text="Tool", kind="info", color="blue"))
    # Error indicator
    has_error = any(isinstance(b, ToolResultBlock) and b.is_error for b in msg.content)
    if has_error:
        badges.append(Badge(text="Error", kind="error", color="red"))
    # Cost indicator
    if msg.metrics and msg.metrics.cost_usd and msg.metrics.cost_usd > 0:
        badges.append(Badge(text=f"${msg.metrics.cost_usd:.3f}", kind="neutral", color="cyan"))
    return badges


# ---------------------------
# Attachment helpers
# ---------------------------
# Attachment helpers
# ---------------------------


def guess_attachment_kind(name: Optional[str] = None, content_type: Optional[str] = None) -> AttachmentKind:
    if content_type:
        if content_type.startswith("image/"):
            return AttachmentKind.image
        if content_type.startswith("audio/"):
            return AttachmentKind.audio
        if content_type.startswith("video/"):
            return AttachmentKind.video
        if content_type in ("text/html", "application/xhtml+xml"):
            return AttachmentKind.link
    if name:
        lower = name.lower()
        if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg")):
            return AttachmentKind.image
        if lower.endswith((".mp3", ".wav", ".ogg", ".flac")):
            return AttachmentKind.audio
        if lower.endswith((".mp4", ".webm", ".mov", ".mkv")):
            return AttachmentKind.video
        if lower.endswith((".html", ".htm")):
            return AttachmentKind.link
    return AttachmentKind.file


def make_link_attachment(url: str, title: Optional[str] = None, description: Optional[str] = None, image_url: Optional[str] = None, site_name: Optional[str] = None) -> LinkAttachment:
    att = LinkAttachment(
        url=url,
        title=title,
        description=description,
        site_name=site_name,
        icon_url=None,
    )
    if image_url:
        att.preview = PreviewAsset(url=image_url)
    return att


# ---------------------------
# Foundational domain models: users, agents, tools, models, capabilities
# ---------------------------


@dataclass
class UserProfile:
    id: str
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    email: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolSchema:
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Optional[Dict[str, Any]] = None
    example_input: Optional[Dict[str, Any]] = None
    example_output: Optional[Dict[str, Any]] = None


@dataclass
class RegisteredTool:
    name: str
    schema: ToolSchema
    enabled: bool = True


@dataclass
class AgentProfile:
    id: str
    name: str
    system_prompt: Optional[str] = None
    tools: List[RegisteredTool] = field(default_factory=list)
    avatar_url: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelProfile:
    name: str
    family: Optional[str] = None
    max_input_tokens: int = 128000
    max_output_tokens: int = 4096
    tokenizer: Optional[str] = None
    supports_tools: bool = True
    supports_vision: bool = False


@dataclass
class CapabilityFlags:
    streaming: bool = True
    attachments: bool = True
    auth: bool = False
    embeddings: bool = False
    rerank: bool = False
    reader: bool = False
    jina: bool = False


@dataclass
class TokenUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


# ---------------------------
# Querying, pagination, and search
# ---------------------------


@dataclass
class CursorPageRequest:
    cursor: Optional[str] = None
    limit: int = 20


@dataclass
class PageInfo:
    cursor_next: Optional[str] = None
    has_more: bool = False
    total: Optional[int] = None


class FilterOp(str, Enum):
    eq = "eq"
    ne = "ne"
    lt = "lt"
    lte = "lte"
    gt = "gt"
    gte = "gte"
    contains = "contains"
    in_ = "in"


@dataclass
class SearchFilter:
    field: str
    op: FilterOp
    value: Any


@dataclass
class QuerySpec:
    q: Optional[str] = None
    filters: List[SearchFilter] = field(default_factory=list)
    sort_by: Optional[str] = None
    sort_desc: bool = False
    page: CursorPageRequest = field(default_factory=CursorPageRequest)


# ---------------------------
# Events (for SSE/WebSocket)
# ---------------------------


@dataclass
class EventEnvelope:
    type: str
    id: str = field(default_factory=lambda: str(uuid4()))
    time: datetime = field(default_factory=_now)
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


# Common event type strings (suggested):
#   message.created, message.updated, message.status, tool.delta, tool.result, typing.start, typing.stop


@dataclass
class APIError:
    code: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------
# Minimal UI view models (for lightweight UIs)
# ---------------------------


class Side(str, Enum):
    left = "left"
    right = "right"


@dataclass
class MinimalBadge:
    text: str
    color: Optional[str] = None


@dataclass
class MinimalAttachmentView:
    kind: str
    url: str
    label: Optional[str] = None


@dataclass
class MinimalMessageView:
    id: str
    role: str
    text: str
    time: datetime
    side: Side
    status: MessageStatus
    thumbnail_url: Optional[str] = None
    badges: List[MinimalBadge] = field(default_factory=list)
    attachments: List[MinimalAttachmentView] = field(default_factory=list)


@dataclass
class ChatView:
    messages: List[MinimalMessageView] = field(default_factory=list)
    show_avatars: bool = True
    typing: bool = False


def build_minimal_message_view(msg: Message) -> MinimalMessageView:
    # Prefer markdown/text content for preview text
    text = ""
    for b in msg.content:
        if isinstance(b, MarkdownBlock) and b.markdown:
            text = b.markdown
            break
        if isinstance(b, TextBlock) and b.text:
            text = b.text
            break
    # Side heuristic: user on right, others on left
    side = Side.right if msg.role == MessageRole.user else Side.left
    # Thumbnail
    thumb = None
    if msg.preview and msg.preview.thumbnail_url:
        thumb = msg.preview.thumbnail_url
    else:
        for a in msg.attachments:
            if a.kind == AttachmentKind.image and a.preview and a.preview.url:
                thumb = a.preview.url
                break
    # Badges (reuse richer preview badges but map to minimal)
    badges = []
    if msg.preview and msg.preview.badges:
        badges = [MinimalBadge(text=b.text, color=b.color) for b in msg.preview.badges]
    # Attachment chips
    attachments = [MinimalAttachmentView(kind=a.kind.value, url=a.url, label=(a.name or a.meta.content_type)) for a in msg.attachments]
    return MinimalMessageView(
        id=msg.id,
        role=msg.role.value,
        text=text,
        time=msg.created_at,
        side=side,
        status=msg.status,
        thumbnail_url=thumb,
        badges=badges,
        attachments=attachments,
    )


# ---------------------------
# Ownership, access control, retention
# ---------------------------


class AccessLevel(str, Enum):
    owner = "owner"
    editor = "editor"
    viewer = "viewer"


class Visibility(str, Enum):
    private = "private"
    shared = "shared"
    public = "public"


@dataclass
class AccessControlEntry:
    subject_type: str  # "user" | "agent" | "team"
    subject_id: str
    level: AccessLevel = AccessLevel.viewer


@dataclass
class RetentionPolicy:
    delete_after_days: Optional[int] = None
    archive_after_days: Optional[int] = None
    legal_hold: bool = False


# ---------------------------
# Conversation summaries and list items
# ---------------------------


@dataclass
class ConversationSummary:
    conversation_id: str
    title: Optional[str] = None
    last_message_preview: Optional[str] = None
    updated_at: datetime = field(default_factory=_now)
    participants: List[str] = field(default_factory=list)  # user/agent ids
    unread_count: int = 0
    labels: List[str] = field(default_factory=list)
    cost_usd: float = 0.0
    token_usage: TokenUsage = field(default_factory=TokenUsage)


@dataclass
class MinimalConversationItem:
    id: str
    title: str
    subtitle: Optional[str] = None
    updated_at: datetime = field(default_factory=_now)
    unread: bool = False
    badge: Optional[str] = None


def build_minimal_conversation_item(summary: ConversationSummary) -> MinimalConversationItem:
    title = summary.title or "Untitled"
    subtitle = summary.last_message_preview
    unread = summary.unread_count > 0
    badge = None
    if summary.labels:
        badge = summary.labels[0]
    return MinimalConversationItem(
        id=summary.conversation_id,
        title=title,
        subtitle=subtitle,
        updated_at=summary.updated_at,
        unread=unread,
        badge=badge,
    )


# ---------------------------
# Threading and revisions
# ---------------------------


@dataclass
class ThreadInfo:
    root_id: Optional[str] = None
    reply_count: int = 0
    last_reply_at: Optional[datetime] = None


@dataclass
class MessageRevision:
    id: str = field(default_factory=lambda: str(uuid4()))
    message_id: str = ""
    created_at: datetime = field(default_factory=_now)
    edited_by: Optional[str] = None  # user/agent id
    reason: Optional[str] = None
    content_snapshot: Dict[str, Any] = field(default_factory=dict)


# ---------------------------
# Notifications
# ---------------------------


@dataclass
class Notification:
    id: str = field(default_factory=lambda: str(uuid4()))
    type: str = "message"
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    created_at: datetime = field(default_factory=_now)
    read: bool = False
    data: Dict[str, Any] = field(default_factory=dict)


# ---------------------------
# Attachment pipeline states and link previews
# ---------------------------


class UploadStatus(str, Enum):
    requested = "requested"
    uploading = "uploading"
    processing = "processing"
    ready = "ready"
    failed = "failed"


@dataclass
class AttachmentState:
    attachment_id: str
    status: UploadStatus = UploadStatus.requested
    progress: Optional[float] = None  # 0..1
    error: Optional[str] = None


@dataclass
class LinkUnfurl:
    url: str
    canonical_url: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    icon_url: Optional[str] = None
    site_name: Optional[str] = None


# ---------------------------
# Tool invocation logging and traces
# ---------------------------


class InvocationStatus(str, Enum):
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


@dataclass
class ToolInvocation:
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    input: Dict[str, Any] = field(default_factory=dict)
    output: Optional[Any] = None
    status: InvocationStatus = InvocationStatus.running
    started_at: datetime = field(default_factory=_now)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    message_id: Optional[str] = None


@dataclass
class TraceSpan:
    trace_id: str
    span_id: str
    name: str
    start: datetime
    end: Optional[datetime] = None
    parent_span_id: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


# ---------------------------
# Moderation
# ---------------------------


@dataclass
class SafetyLabel:
    category: str
    score: float


@dataclass
class ModerationResult:
    flagged: bool = False
    labels: List[SafetyLabel] = field(default_factory=list)
    notes: Optional[str] = None


# ---------------------------
# Knowledge base primitives
# ---------------------------


@dataclass
class DocumentRef:
    id: str
    title: Optional[str] = None
    url: Optional[str] = None
    source: Optional[str] = None


@dataclass
class EmbeddingVector:
    vector: List[float]
    model: Optional[str] = None


@dataclass
class DocumentChunk:
    id: str
    document_id: str
    text: str
    embedding: Optional[EmbeddingVector] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RerankResultItem:
    index: int
    score: float
    text: str


# ---------------------------
# Context compression dataclasses (planning + results)
# ---------------------------


class CompressionStrategy(str, Enum):
    extractive = "extractive"
    abstractive = "abstractive"
    hybrid = "hybrid"


@dataclass
class CompressionConfig:
    budget_tokens: int = 8192
    keep_last_n: int = 2
    preserve_tool_pairs: bool = True
    preserve_system: bool = True
    strategy: CompressionStrategy = CompressionStrategy.extractive
    query: Optional[str] = None
    recency_weight: float = 0.6
    similarity_weight: float = 0.3
    role_weight: float = 0.1


@dataclass
class RankedItem:
    index: int
    tokens: int
    score: float
    kept: bool
    reasons: List[str] = field(default_factory=list)


@dataclass
class CompressionPlan:
    items: List[RankedItem] = field(default_factory=list)
    kept_indices: List[int] = field(default_factory=list)
    dropped_indices: List[int] = field(default_factory=list)
    approx_tokens_before: int = 0
    approx_tokens_after: int = 0


@dataclass
class CompressionResult:
    compressed_messages: List[Message] = field(default_factory=list)
    plan: CompressionPlan = field(default_factory=CompressionPlan)


__all__ = [
    # Enums
    "MessageRole",
    "ContentBlockType",
    "AttachmentKind",
    "FilterOp",
    "Side",
    # Param blocks
    "TextBlockParam",
    "MarkdownBlockParam",
    "CodeBlockParam",
    "ImageBlockParam",
    "AudioBlockParam",
    "VideoBlockParam",
    "TableBlockParam",
    "ToolUseBlockParam",
    "ToolResultBlockParam",
    "ContentBlockParam",
    # Canonical blocks
    "TextBlock",
    "MarkdownBlock",
    "CodeBlock",
    "ImageBlock",
    "AudioBlock",
    "VideoBlock",
    "TableBlock",
    "ToolUseBlock",
    "ToolResultBlock",
    "ContentBlock",
    # Attachments, previews, reactions, metrics
    "PreviewAsset",
    "MediaMeta",
    "Attachment",
    "ImageAttachment",
    "FileAttachment",
    "LinkAttachment",
    "Reaction",
    "MessageStatus",
    "MessageMetrics",
    "SourceRef",
    "CitationSpan",
    "QuoteRef",
    "Badge",
    "MessagePreview",
    # Messages
    "MessageCreate",
    "Message",
    # Conversation
    "Conversation",
    # Requests/Responses
    "CreateConversationRequest",
    "CreateConversationResponse",
    "GetConversationResponse",
    "AddMessageRequest",
    "AddMessageResponse",
    "ListConversationsRequest",
    "ListConversationsResponse",
    "DeleteLastMessageRequest",
    "DeleteLastMessageResponse",
    # Helpers
    "conversation_from_request",
    # UI schema
    "UIComponentKind",
    "UIComponent",
    # Typed UI props
    "CodeProps",
    "ImageProps",
    "LinkCardProps",
    "ToolCallProps",
    "ToolResultProps",
    "AttachmentGalleryItem",
    "AttachmentGalleryProps",
    "ReactionBarProps",
    "CollapsibleProps",
    "MarkdownProps",
    "TextProps",
    "MessageUI",
    "ConversationUIConfig",
    # Builders
    "build_default_preview",
    "build_ui_components_for_message",
    "build_message_ui",
    "build_minimal_message_view",
    # Compression
    "CompressionStrategy",
    "CompressionConfig",
    "RankedItem",
    "CompressionPlan",
    "CompressionResult",
    # Messaging and attachments flows
    "MessageSendRequest",
    "MessageSendResponse",
    "MessageStatusUpdate",
    "AttachmentUploadSpec",
    "AttachmentIngestRequest",
    "AttachmentIngestResponse",
    # Attachment helpers
    "guess_attachment_kind",
    "make_link_attachment",
    # Domain models
    "UserProfile",
    "AgentProfile",
    "ToolSchema",
    "RegisteredTool",
    "ModelProfile",
    "CapabilityFlags",
    "TokenUsage",
    # Query/Pagination
    "CursorPageRequest",
    "PageInfo",
    "SearchFilter",
    "QuerySpec",
    # Events and errors
    "EventEnvelope",
    "APIError",
    # Minimal UI views
    "MinimalBadge",
    "MinimalAttachmentView",
    "MinimalMessageView",
    "ChatView",
    # Ownership / ACL / retention
    "AccessLevel",
    "Visibility",
    "AccessControlEntry",
    "RetentionPolicy",
    # Conversation summaries
    "ConversationSummary",
    "MinimalConversationItem",
    "build_minimal_conversation_item",
    # Threads and revisions
    "ThreadInfo",
    "MessageRevision",
    # Notifications
    "Notification",
    # Attachment pipeline
    "UploadStatus",
    "AttachmentState",
    "LinkUnfurl",
    # Tools & traces
    "InvocationStatus",
    "ToolInvocation",
    "TraceSpan",
    # Moderation
    "SafetyLabel",
    "ModerationResult",
    # Knowledge base
    "DocumentRef",
    "EmbeddingVector",
    "DocumentChunk",
    "RerankResultItem",
]
