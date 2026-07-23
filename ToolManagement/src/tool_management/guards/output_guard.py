"""
Output Guard - Enforces size limits, link caps, and URL capture on MCP tool outputs.

Every tool output flows through this guard before being returned to the caller.
It enforces:
  1. Maximum output character length (default 32K chars, ~8K tokens)
  2. Maximum number of URLs/links in output (default 50)
  3. Extracts ALL URLs from output and queues them for background processing

Usage:
    from tool_management.guards import apply_output_guard

    # In handle_tools_call:
    raw_result = await tool.handler(arguments)
    return apply_output_guard(raw_result, tool_name=tool_name)
"""

from __future__ import annotations

import json
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Regex to extract URLs from text content
_URL_RE = re.compile(
    r'https?://[^\s"\'<>\]\)}{,]+',
    re.IGNORECASE,
)


@dataclass
class OutputGuardConfig:
    """Configuration for output enforcement."""
    max_output_chars: int = 32_000          # ~8K tokens
    max_links: int = 50                     # Max URLs kept in output
    max_content_field_chars: int = 16_000   # Max chars for any single 'content' field
    truncation_suffix: str = "\n\n[OUTPUT TRUNCATED - full result available via pagination or by requesting fewer results]"
    link_truncation_notice: str = "\n\n[LINKS TRUNCATED - {kept}/{total} URLs shown. All {total} URLs saved for background processing]"
    capture_urls: bool = True               # Whether to capture URLs for background processing
    enforce_limits: bool = True             # Master switch for enforcement


@dataclass
class GuardResult:
    """Result of applying the output guard."""
    output: Any                             # The (possibly modified) output
    original_chars: int = 0
    final_chars: int = 0
    truncated: bool = False
    urls_found: int = 0
    urls_captured: int = 0
    urls_in_output: int = 0
    links_truncated: bool = False


# Default config singleton
_default_config = OutputGuardConfig()
_config_lock = threading.Lock()


def set_default_config(config: OutputGuardConfig):
    """Set the default output guard configuration."""
    global _default_config
    with _config_lock:
        _default_config = config


def get_default_config() -> OutputGuardConfig:
    """Get the default output guard configuration."""
    return _default_config


def extract_urls(text: str) -> List[str]:
    """Extract all URLs from text, deduped, preserving order."""
    seen: Set[str] = set()
    urls: List[str] = []
    for match in _URL_RE.finditer(text):
        url = match.group(0).rstrip(".,;:!?)")
        normalized = url.lower().rstrip("/")
        if normalized not in seen:
            seen.add(normalized)
            urls.append(url)
    return urls


def _truncate_text(text: str, max_chars: int, suffix: str) -> str:
    """Truncate text to max_chars, ending cleanly at a word/line boundary."""
    if len(text) <= max_chars:
        return text

    cut_at = max_chars - len(suffix)
    if cut_at <= 0:
        return suffix

    # Try to cut at a newline or space boundary
    last_newline = text.rfind("\n", 0, cut_at)
    last_space = text.rfind(" ", 0, cut_at)
    break_at = max(last_newline, last_space, cut_at // 2)

    return text[:break_at] + suffix


def _limit_urls_in_text(text: str, max_links: int, notice_template: str) -> tuple[str, int, int, bool]:
    """
    Limit the number of URLs in text. Returns (new_text, total_urls, kept_urls, was_truncated).

    After max_links URLs, subsequent URLs are redacted from the output text.
    """
    urls = extract_urls(text)
    total = len(urls)

    if total <= max_links:
        return text, total, total, False

    # Redact URLs beyond the limit from the output text
    urls_to_redact = urls[max_links:]
    result = text
    for url in urls_to_redact:
        result = result.replace(url, "[URL saved for background processing]", 1)

    notice = notice_template.format(kept=max_links, total=total)
    result += notice

    return result, total, max_links, True


def _limit_urls_in_dict(data: Any, max_links: int, _counter: Optional[List[int]] = None) -> tuple[Any, int]:
    """
    Walk a dict/list structure and redact URL values beyond max_links.
    Returns (modified_data, total_urls_found).
    """
    if _counter is None:
        _counter = [0]  # [urls_found_so_far]

    if isinstance(data, str):
        urls = extract_urls(data)
        for url in urls:
            _counter[0] += 1
            if _counter[0] > max_links:
                data = data.replace(url, "[URL saved for background processing]", 1)
        return data, _counter[0]

    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            new_dict[k], _ = _limit_urls_in_dict(v, max_links, _counter)
        return new_dict, _counter[0]

    if isinstance(data, list):
        new_list = []
        for item in data:
            new_item, _ = _limit_urls_in_dict(item, max_links, _counter)
            new_list.append(new_item)
        return new_list, _counter[0]

    return data, _counter[0]


def _capture_urls_to_queue(
    urls: List[str],
    tool_name: str = "",
    source_context: Optional[Dict[str, Any]] = None,
):
    """Send extracted URLs to the URL queue for background processing."""
    try:
        from tool_management.queue.url_queue import get_url_queue, URLSource
    except ImportError:
        logger.debug("URL queue not available for capture")
        return 0

    try:
        queue = get_url_queue()
        queued_count = 0
        for url in urls:
            result = queue.enqueue(
                url=url,
                source=URLSource.API,
                source_query=f"tool_output:{tool_name}",
                metadata={
                    "captured_from": "output_guard",
                    "tool_name": tool_name,
                    **(source_context or {}),
                },
            )
            if result:
                queued_count += 1

        if queued_count > 0:
            logger.info(f"Output guard captured {queued_count} URLs from {tool_name}")
        return queued_count

    except Exception as e:
        logger.warning(f"Failed to capture URLs to queue: {e}")
        return 0


def _truncate_content_fields(data: Any, max_chars: int, suffix: str) -> Any:
    """Recursively truncate fields named 'content' that are oversized strings."""
    if isinstance(data, dict):
        result = {}
        for k, v in data.items():
            if k == "content" and isinstance(v, str) and len(v) > max_chars:
                result[k] = _truncate_text(v, max_chars, suffix)
            else:
                result[k] = _truncate_content_fields(v, max_chars, suffix)
        return result

    if isinstance(data, list):
        return [_truncate_content_fields(item, max_chars, suffix) for item in data]

    return data


def apply_output_guard(
    result: Any,
    tool_name: str = "",
    config: Optional[OutputGuardConfig] = None,
    source_context: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Apply the output guard to a tool result.

    This is the main entry point. Call it on the raw result before returning
    to the MCP caller.

    Args:
        result: Raw tool output (dict, string, or MCP content envelope)
        tool_name: Name of the tool that produced this output
        config: Optional override config (uses default if None)
        source_context: Optional metadata to attach to captured URLs

    Returns:
        The (possibly modified) result with size limits enforced and URLs captured.
    """
    if config is None:
        config = get_default_config()

    # MCP envelopes ({"content":[{"type":"text","text":"<json>"}]}) must be
    # guarded via the structured path: blind string truncation of the inner
    # serialized JSON produces unparseable output for large results.
    if (
        isinstance(result, dict)
        and isinstance(result.get("content"), list)
        and any(isinstance(b, dict) and b.get("type") == "text" for b in result["content"])
    ):
        return guard_mcp_content(result, tool_name, config, source_context)

    # Step 1: Serialize to measure size
    if isinstance(result, dict):
        text_repr = json.dumps(result)
    elif isinstance(result, str):
        text_repr = result
    else:
        text_repr = str(result)

    original_chars = len(text_repr)

    # Step 2: Extract all URLs for capture (always, regardless of enforce_limits)
    all_urls = extract_urls(text_repr)
    urls_captured = 0

    if config.capture_urls and all_urls:
        urls_captured = _capture_urls_to_queue(
            all_urls,
            tool_name=tool_name,
            source_context=source_context,
        )

    if not config.enforce_limits:
        return result

    # Step 3: Truncate oversized 'content' fields within the result
    if isinstance(result, dict):
        result = _truncate_content_fields(
            result,
            config.max_content_field_chars,
            config.truncation_suffix,
        )

    # Step 4: Limit URLs in the output
    links_truncated = False
    total_urls_in_output = len(all_urls)
    kept_urls = total_urls_in_output

    if total_urls_in_output > config.max_links:
        if isinstance(result, dict):
            result, total_urls_in_output = _limit_urls_in_dict(result, config.max_links)
            kept_urls = config.max_links
            links_truncated = True
        elif isinstance(result, str):
            result, total_urls_in_output, kept_urls, links_truncated = _limit_urls_in_text(
                result, config.max_links, config.link_truncation_notice,
            )

    # Step 5: Final size enforcement on the serialized output
    if isinstance(result, dict):
        final_text = json.dumps(result)
        if len(final_text) > config.max_output_chars:
            # Try to trim the biggest text fields
            result = _aggressive_trim(result, config.max_output_chars, config.truncation_suffix)
            final_text = json.dumps(result)

            # If still too big, shrink structurally (drop list items, trim
            # strings) so the payload REMAINS valid JSON - never chop the
            # serialized text, which yields unparseable output.
            if len(final_text) > config.max_output_chars:
                shrunk = _shrink_json_string(final_text, config.max_output_chars)
                if shrunk is not None:
                    try:
                        result = json.loads(shrunk)
                        if isinstance(result, dict):
                            result["_guard"] = {
                                "truncated": True,
                                "original_chars": original_chars,
                                "urls_found": len(all_urls),
                                "urls_captured": urls_captured,
                            }
                    except (json.JSONDecodeError, TypeError):
                        pass
    elif isinstance(result, str) and len(result) > config.max_output_chars:
        result = _truncate_text(result, config.max_output_chars, config.truncation_suffix)

    return result


def _aggressive_trim(data: Dict[str, Any], target_chars: int, suffix: str) -> Dict[str, Any]:
    """
    Aggressively trim string values in a dict to fit within target_chars.
    Targets the longest string fields first.
    """
    current_size = len(json.dumps(data))
    if current_size <= target_chars:
        return data

    # Collect all string field paths and their sizes
    string_fields: List[tuple[list, str, int]] = []
    _collect_strings(data, [], string_fields)

    # Sort by size descending - trim the biggest first
    string_fields.sort(key=lambda x: x[2], reverse=True)

    overshoot = current_size - target_chars
    for path, value, size in string_fields:
        if overshoot <= 0:
            break

        # Compute how much to trim this field
        new_size = max(200, size - overshoot - len(suffix))
        if new_size < size:
            trimmed = None
            if value.lstrip()[:1] in ("{", "["):
                # Serialized JSON payload: shrink structurally so it stays parseable
                trimmed = _shrink_json_string(value, new_size)
            if trimmed is None:
                trimmed = _truncate_text(value, new_size, suffix)
            _set_nested(data, path, trimmed)
            overshoot -= (size - len(trimmed))

    return data


def _shrink_json_string(text: str, max_chars: int) -> Optional[str]:
    """Shrink a serialized-JSON string to ~max_chars while keeping it valid JSON.

    Drops list items (biggest lists first), then truncates long inner strings.
    Returns None if the text isn't parseable JSON.
    """
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None

    def _all_lists(o: Any, acc: List[list]) -> List[list]:
        if isinstance(o, dict):
            for v in o.values():
                _all_lists(v, acc)
        elif isinstance(o, list):
            acc.append(o)
            for v in o:
                _all_lists(v, acc)
        return acc

    def _trunc_strings(o: Any, cap: int) -> Any:
        if isinstance(o, dict):
            return {k: _trunc_strings(v, cap) for k, v in o.items()}
        if isinstance(o, list):
            return [_trunc_strings(v, cap) for v in o]
        if isinstance(o, str) and len(o) > cap:
            return o[: cap - 1] + "…"
        return o

    trimmed = False
    # Pass 1: shorten long strings first (progressively) - keeps list items
    # (entities, results) intact at reduced verbosity.
    for cap in (2000, 800, 300):
        if len(json.dumps(obj, default=str)) <= max_chars:
            break
        obj = _trunc_strings(obj, cap)
        trimmed = True

    # Pass 2: only if still over budget, drop items from the biggest lists.
    for _ in range(10_000):
        if len(json.dumps(obj, default=str)) <= max_chars:
            break
        lists = [l for l in _all_lists(obj, []) if len(l) > 0]
        if not lists:
            break
        max(lists, key=len).pop()
        trimmed = True

    if trimmed and isinstance(obj, dict):
        obj["truncated"] = True
    return json.dumps(obj, default=str)


def _collect_strings(data: Any, path: list, result: list):
    """Collect all string fields with their paths and sizes."""
    if isinstance(data, str):
        result.append((list(path), data, len(data)))
    elif isinstance(data, dict):
        for k, v in data.items():
            _collect_strings(v, path + [k], result)
    elif isinstance(data, list):
        for i, v in enumerate(data):
            _collect_strings(v, path + [i], result)


def _set_nested(data: Any, path: list, value: Any):
    """Set a nested value by path."""
    for key in path[:-1]:
        data = data[key]
    data[path[-1]] = value


def guard_mcp_content(
    mcp_result: Dict[str, Any],
    tool_name: str = "",
    config: Optional[OutputGuardConfig] = None,
    source_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Apply the output guard specifically to an MCP content envelope.

    MCP results have the shape:
        {"content": [{"type": "text", "text": "..."}]}

    This function guards the inner text while preserving the envelope.
    """
    if config is None:
        config = get_default_config()

    if not isinstance(mcp_result, dict):
        return apply_output_guard(mcp_result, tool_name, config, source_context)

    content = mcp_result.get("content")
    if not isinstance(content, list):
        return apply_output_guard(mcp_result, tool_name, config, source_context)

    # Guard each content block
    new_content = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text", "")

            # Extract URLs from this text block
            all_urls = extract_urls(text)
            if config.capture_urls and all_urls:
                _capture_urls_to_queue(all_urls, tool_name=tool_name, source_context=source_context)

            if config.enforce_limits:
                # Try to parse as JSON for structured truncation
                try:
                    parsed = json.loads(text)
                    guarded = apply_output_guard(parsed, tool_name, config, source_context)
                    if isinstance(guarded, (dict, list)):
                        text = json.dumps(guarded, indent=2)
                        if len(text) > config.max_output_chars:
                            text = json.dumps(guarded)  # compact form to respect budget
                    else:
                        text = str(guarded)
                except (json.JSONDecodeError, TypeError):
                    # Plain text - just truncate
                    if len(text) > config.max_output_chars:
                        text = _truncate_text(text, config.max_output_chars, config.truncation_suffix)

                    # Limit URLs in plain text
                    urls_in_text = extract_urls(text)
                    if len(urls_in_text) > config.max_links:
                        text, _, _, _ = _limit_urls_in_text(
                            text, config.max_links, config.link_truncation_notice,
                        )

            new_content.append({"type": "text", "text": text})
        else:
            new_content.append(block)

    result = {**mcp_result, "content": new_content}
    return result
