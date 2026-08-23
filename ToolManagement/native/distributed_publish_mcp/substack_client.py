"""
Substack API Client - Full-Featured Python Implementation

A comprehensive Python client for the Substack API with:
- Proper ProseMirror document format support
- Image embedding
- Post creation, drafts, and publishing
- Notes (short-form content)
- Reader feed and subscriptions
- Bitcoin/Lightning integration hooks

Author: Built by exploring the Substack API
"""

import json
import os
import re
import threading
import time
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SubstackAPIError(RuntimeError):
    """A sanitized Substack API or response-contract failure."""


def _normalize_publication(publication: str) -> str:
    """Return a safe publication hostname without credentials, paths, or ports."""
    value = (publication or "").strip()
    parsed = urlparse(value if "://" in value else f"https://{value}")
    hostname = parsed.hostname or ""
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username
        or parsed.password
        or parsed.port
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("publication must be a hostname such as example.substack.com")
    if not re.fullmatch(r"(?=.{1,253}$)[A-Za-z0-9.-]+", hostname):
        raise ValueError("publication contains unsupported hostname characters")
    return hostname.lower().rstrip(".")


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SubstackProfile:
    """User profile"""
    id: int
    name: str
    handle: str
    photo_url: str = ""
    bio: str = ""

    @property
    def url(self) -> str:
        return f"https://substack.com/@{self.handle}"


@dataclass
class SubstackPost:
    """Published post"""
    id: int
    title: str
    slug: str
    subtitle: str = ""
    body_html: str = ""
    body_json: Dict = field(default_factory=dict)
    canonical_url: str = ""
    post_date: str = ""
    audience: str = "everyone"
    reactions: Dict = field(default_factory=dict)
    comment_count: int = 0
    wordcount: int = 0
    cover_image: str = ""
    type: str = "newsletter"


@dataclass
class SubstackDraft:
    """Draft post"""
    id: int
    title: str = ""
    subtitle: str = ""
    body_json: Dict = field(default_factory=dict)
    audience: str = "everyone"
    cover_image: str = ""


@dataclass
class SubstackNote:
    """Short-form note"""
    id: str
    body: str
    author_name: str = ""
    author_handle: str = ""
    published_at: str = ""
    reactions: int = 0


# =============================================================================
# DOCUMENT BUILDER - ProseMirror Format
# =============================================================================

"""
SUBSTACK PROSEMIRROR SCHEMA (empirically verified April 2026)

SAFE NODE TYPES (render in editor):
  paragraph          - attrs: {textAlign?}
  heading            - attrs: {level: 2|3|4}
  blockquote         - content: [paragraph]
  pullquote          - content: [paragraph]  (centered italic with rules)
  horizontalRule     - (no attrs)
  codeBlock          - attrs: {language?}
  bulletList         - content: [listItem]   (camelCase works, snake_case also works)
  orderedList        - attrs: {order: 1}, content: [listItem]
  listItem           - content: [paragraph]
  image2             - attrs: {src, alt, ...full attrs}  (USE THIS for images)
  youtube2           - attrs: {videoId, startTime?, endTime?}
  paywall            - (no attrs, inserts paid divider)
  button             - attrs: {url, text}
  embeddedPost       - attrs: {url}

CRASHED NODE TYPES (DO NOT USE):
  image              - plain image node crashes editor
  captionedImage     - crashes editor
  subscribeWidget    - crashes editor
  footnote           - crashes editor (inline)
  table/tableRow/etc - all crash editor

SAFE MARKS:
  strong             - bold
  em                 - italic
  code               - inline code
  link               - attrs: {href}

CRASHED MARKS (DO NOT USE):
  strike             - crashes editor
  underline          - crashes editor
  highlight          - crashes editor
"""


class SubstackDocument:
    """
    Build Substack-compatible documents using ProseMirror format.

    Usage:
        doc = SubstackDocument()
        doc.heading("My Title", level=2)
        doc.paragraph("Some text with ", doc.bold("bold"), " and ", doc.link("links", "https://..."))
        doc.image("https://...", caption="Photo caption")
        doc.bullet_list(["Item 1", "Item 2"])

        body_json = doc.build()
    """

    def __init__(self):
        self.content: List[Dict] = []

    # --- Text helpers ---

    @staticmethod
    def text(content: str, marks: List[Dict] = None) -> Dict:
        """Create a text node with optional marks"""
        node = {"type": "text", "text": content}
        if marks:
            node["marks"] = marks
        return node

    @staticmethod
    def bold(content: str) -> Dict:
        """Bold text"""
        return {"type": "text", "text": content, "marks": [{"type": "strong"}]}

    @staticmethod
    def italic(content: str) -> Dict:
        """Italic text"""
        return {"type": "text", "text": content, "marks": [{"type": "em"}]}

    @staticmethod
    def code(content: str) -> Dict:
        """Inline code"""
        return {"type": "text", "text": content, "marks": [{"type": "code"}]}

    @staticmethod
    def link(content: str, href: str) -> Dict:
        """Link text"""
        return {
            "type": "text",
            "text": content,
            "marks": [{"type": "link", "attrs": {"href": href, "title": None}}]
        }

    # --- Block elements ---

    def paragraph(self, *parts: Union[str, Dict]) -> 'SubstackDocument':
        """
        Add a paragraph with mixed content.

        Args:
            parts: Mix of strings and text nodes (from bold(), italic(), link(), etc.)
        """
        content = []
        for part in parts:
            if isinstance(part, str):
                content.append(self.text(part))
            elif isinstance(part, dict):
                content.append(part)

        if content:
            self.content.append({"type": "paragraph", "content": content})
        else:
            self.content.append({"type": "paragraph"})
        return self

    def heading(self, text: str, level: int = 2) -> 'SubstackDocument':
        """Add a heading (level 2, 3, or 4)"""
        level = max(2, min(4, level))  # Clamp to 2-4
        self.content.append({
            "type": "heading",
            "attrs": {"level": level},
            "content": [{"type": "text", "text": text}]
        })
        return self

    def image(self, src: str, alt: str = None, caption: str = None,
              width: int = None, height: int = None, bytes_size: int = None,
              content_type: str = None) -> 'SubstackDocument':
        """
        Add an image with optional caption.

        For best results, use upload_image() first to get proper dimensions.
        Note: internalRedirect will be set automatically when using create_draft().
        """
        image_node = {
            "type": "image2",
            "attrs": {
                "src": src,
                "srcNoWatermark": None,
                "fullscreen": None,
                "imageSize": None,
                "height": height,
                "width": width,
                "resizeWidth": None,
                "bytes": bytes_size,
                "alt": alt,
                "title": None,
                "type": content_type,
                "href": None,
                "belowTheFold": False,
                "topImage": False,
                "internalRedirect": None,  # Set by create_draft after we have draft_id
                "isProcessing": False,
                "align": None,
                "offset": False
            }
        }

        # Use image2 directly - captionedImage crashes the editor
        self.content.append(image_node)
        if caption:
            self.content.append({
                "type": "paragraph",
                "content": [{"type": "text", "text": caption, "marks": [{"type": "em"}]}]
            })
        return self

    def blockquote(self, text: str) -> 'SubstackDocument':
        """Add a blockquote"""
        self.content.append({
            "type": "blockquote",
            "content": [{
                "type": "paragraph",
                "content": [{"type": "text", "text": text}]
            }]
        })
        return self

    def bullet_list(self, items: List[str]) -> 'SubstackDocument':
        """Add a bullet list"""
        list_items = []
        for item in items:
            list_items.append({
                "type": "listItem",
                "content": [{
                    "type": "paragraph",
                    "content": [{"type": "text", "text": item}]
                }]
            })

        self.content.append({
            "type": "bulletList",
            "content": list_items
        })
        return self

    def numbered_list(self, items: List[str]) -> 'SubstackDocument':
        """Add a numbered/ordered list"""
        list_items = []
        for item in items:
            list_items.append({
                "type": "listItem",
                "content": [{
                    "type": "paragraph",
                    "content": [{"type": "text", "text": item}]
                }]
            })

        self.content.append({
            "type": "orderedList",
            "attrs": {"order": 1},
            "content": list_items
        })
        return self

    def pullquote(self, text: str) -> 'SubstackDocument':
        """Add a pull quote (centered italic text between horizontal rules)"""
        self.content.append({
            "type": "pullquote",
            "content": [{
                "type": "paragraph",
                "content": [{"type": "text", "text": text}]
            }]
        })
        return self

    def paywall(self) -> 'SubstackDocument':
        """Add a paywall divider (paid content below this line)"""
        self.content.append({"type": "paywall"})
        return self

    def button(self, text: str, url: str) -> 'SubstackDocument':
        """Add a CTA button"""
        self.content.append({
            "type": "button",
            "attrs": {"url": url, "text": text}
        })
        return self

    def embedded_post(self, url: str) -> 'SubstackDocument':
        """Embed another Substack post"""
        self.content.append({
            "type": "embeddedPost",
            "attrs": {"url": url}
        })
        return self

    def horizontal_rule(self) -> 'SubstackDocument':
        """Add a horizontal rule/divider"""
        self.content.append({"type": "horizontalRule"})
        return self

    def code_block(self, code: str, language: str = "") -> 'SubstackDocument':
        """Add a code block with optional language for syntax highlighting"""
        self.content.append({
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"type": "text", "text": code}]
        })
        return self

    def youtube(self, video_id: str) -> 'SubstackDocument':
        """Embed a YouTube video"""
        self.content.append({
            "type": "youtube2",
            "attrs": {"videoId": video_id, "startTime": None, "endTime": None}
        })
        return self

    def twitter(self, url: str, text: str = "", username: str = "") -> 'SubstackDocument':
        """Embed a tweet"""
        self.content.append({
            "type": "twitter2",
            "attrs": {
                "url": url,
                "full_text": text,
                "username": username,
                "name": "",
                "date": "",
                "photos": [],
                "quoted_tweet": {},
                "retweet_count": 0,
                "like_count": 0,
                "expanded_url": {},
                "video_url": None
            }
        })
        return self

    def build(self) -> Dict:
        """Build the final document"""
        return {
            "type": "doc",
            "content": self.content
        }

    def to_json(self) -> str:
        """Build and serialize to JSON string"""
        return json.dumps(self.build())


# =============================================================================
# MARKDOWN TO SUBSTACK CONVERTER
# =============================================================================

class MarkdownToSubstack:
    """Convert Markdown to Substack document format"""

    @staticmethod
    def convert(markdown: str) -> Dict:
        """
        Convert markdown text to Substack document format.

        Supports:
        - Headers (#, ##, ###, ####)
        - Code blocks (```language ... ```)
        - Bold (**text**)
        - Italic (*text* or _text_)
        - Links [text](url)
        - Images ![alt](url)
        - Blockquotes (> text)
        - Bullet lists (- item)
        - Numbered lists (1. item)
        - Horizontal rules (---)
        """
        doc = SubstackDocument()
        lines = markdown.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Skip empty lines
            if not stripped:
                i += 1
                continue

            # Horizontal rule
            if stripped in ['---', '***', '___']:
                doc.horizontal_rule()
                i += 1
                continue

            # Headers (h1-h4)
            header_match = re.match(r'^(#{1,4})\s+(.+)$', stripped)
            if header_match:
                level = len(header_match.group(1))
                text = header_match.group(2)
                doc.heading(text, level=level)
                i += 1
                continue

            # Fenced code blocks
            code_match = re.match(r'^```([^\s`]*)\s*$', stripped)
            if code_match:
                language = code_match.group(1) or ""
                code_lines = []
                i += 1
                while i < len(lines):
                    if lines[i].strip() == '```':
                        i += 1
                        break
                    code_lines.append(lines[i])
                    i += 1
                code_text = '\n'.join(code_lines)
                doc.code_block(code_text, language=language)
                continue

            # Images
            img_match = re.match(r'^!\[([^\]]*)\]\(([^)]+)\)$', stripped)
            if img_match:
                alt = img_match.group(1)
                src = img_match.group(2)
                doc.image(src, alt=alt)
                i += 1
                continue

            # Blockquotes
            if stripped.startswith('> '):
                quote_lines = []
                while i < len(lines) and lines[i].strip().startswith('> '):
                    quote_lines.append(lines[i].strip()[2:])
                    i += 1
                doc.blockquote(' '.join(quote_lines))
                continue

            # Bullet lists
            if stripped.startswith('- ') or stripped.startswith('* '):
                items = []
                while i < len(lines):
                    l = lines[i].strip()
                    if l.startswith('- ') or l.startswith('* '):
                        items.append(l[2:])
                        i += 1
                    elif l == '':
                        i += 1
                        break
                    else:
                        break
                if items:
                    doc.bullet_list(items)
                continue

            # Numbered lists
            num_match = re.match(r'^\d+\.\s+(.+)$', stripped)
            if num_match:
                items = []
                while i < len(lines):
                    l = lines[i].strip()
                    m = re.match(r'^\d+\.\s+(.+)$', l)
                    if m:
                        items.append(m.group(1))
                        i += 1
                    elif l == '':
                        i += 1
                        break
                    else:
                        break
                if items:
                    doc.numbered_list(items)
                continue

            # Markdown table (| col | col |) -> code block (Substack has no table nodes)
            if stripped.startswith('|') and stripped.endswith('|') and '|' in stripped[1:-1]:
                table_rows = []
                while i < len(lines):
                    l = lines[i].strip()
                    if l.startswith('|') and l.endswith('|'):
                        if not re.match(r'^\|[\s\-:|]+\|$', l):
                            cells = [c.strip() for c in l.split('|')[1:-1]]
                            table_rows.append(cells)
                        i += 1
                    else:
                        break
                if table_rows:
                    n_cols = max(len(r) for r in table_rows)
                    widths = [0] * n_cols
                    for r in table_rows:
                        for ci, c in enumerate(r):
                            if ci < n_cols:
                                widths[ci] = max(widths[ci], len(c.replace('**', '')))
                    code_lines = []
                    for ri, r in enumerate(table_rows):
                        parts = []
                        for ci in range(n_cols):
                            cell = r[ci].replace('**', '') if ci < len(r) else ''
                            parts.append(cell.ljust(widths[ci]))
                        code_lines.append('  '.join(parts).rstrip())
                        if ri == 0:
                            code_lines.append('  '.join('─' * w for w in widths))
                    doc.code_block('\n'.join(code_lines))
                continue

            # Regular paragraph - parse inline formatting
            paragraph_content = MarkdownToSubstack._parse_inline(stripped)
            if paragraph_content:
                doc.content.append({
                    "type": "paragraph",
                    "content": paragraph_content
                })
            i += 1

        return doc.build()

    @staticmethod
    def _parse_inline(text: str) -> List[Dict]:
        """Parse inline formatting (code, bold, italic, links)"""
        content = []

        # Pattern for links, bold, italic
        pattern = r'(`([^`]+)`|\*\*(.+?)\*\*|\*(.+?)\*|_(.+?)_|\[([^\]]+)\]\(([^)]+)\))'

        last_end = 0
        for match in re.finditer(pattern, text):
            # Add text before match
            if match.start() > last_end:
                content.append({"type": "text", "text": text[last_end:match.start()]})

            full_match = match.group(0)

            if full_match.startswith('`'):
                # Inline code
                content.append({
                    "type": "text",
                    "text": match.group(2),
                    "marks": [{"type": "code"}]
                })
            elif full_match.startswith('**'):
                # Bold
                content.append({
                    "type": "text",
                    "text": match.group(3),
                    "marks": [{"type": "strong"}]
                })
            elif full_match.startswith('['):
                # Link
                link_text = match.group(6)
                link_url = match.group(7)
                content.append({
                    "type": "text",
                    "text": link_text,
                    "marks": [{"type": "link", "attrs": {"href": link_url, "title": None}}]
                })
            elif full_match.startswith('*') or full_match.startswith('_'):
                # Italic
                italic_text = match.group(4) or match.group(5)
                content.append({
                    "type": "text",
                    "text": italic_text,
                    "marks": [{"type": "em"}]
                })

            last_end = match.end()

        # Add remaining text
        if last_end < len(text):
            content.append({"type": "text", "text": text[last_end:]})

        # If no formatting found, just return plain text
        if not content and text:
            content.append({"type": "text", "text": text})

        return content


# =============================================================================
# SUBSTACK API CLIENT
# =============================================================================

class SubstackClient:
    """
    Full-featured Substack API client.

    Usage:
        client = SubstackClient(
            token="your-substack.sid-cookie",
            publication="yourname.substack.com"
        )

        # Test connection
        profile = client.get_profile()
        print(f"Connected as {profile.name}")

        # Create a post
        doc = SubstackDocument()
        doc.heading("My Post Title", level=2)
        doc.image("https://example.com/image.jpg", caption="A cool image")
        doc.paragraph("This is my post content.")

        draft = client.create_draft(
            title="My Post Title",
            subtitle="A subtitle",
            body=doc
        )

        # Publish
        post = client.publish_draft(draft.id)
        print(f"Published: {post.canonical_url}")
    """

    def __init__(
        self,
        token: str,
        publication: str,
        rate_limit: float = 0.5,
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ):
        if not token or not token.strip():
            raise ValueError("token is required")
        if rate_limit < 0:
            raise ValueError("rate_limit must be non-negative")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        self.token = token.strip()
        self.publication = _normalize_publication(publication)
        self.rate_limit = float(rate_limit)
        self.timeout = float(timeout)
        self._last_request = 0.0
        self._rate_limit_lock = threading.Lock()

        # Base URLs
        self.pub_base = f"https://{self.publication}/api/v1"
        self.sub_base = "https://substack.com/api/v1"

        # Headers
        self.headers = {
            "Cookie": f"substack.sid={self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
        }

        # Retry only reads. Retrying draft creation, publication, or Notes can
        # duplicate irreversible actions when a response is lost in transit.
        self.session = session or requests.Session()
        retries = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # Cache
        self._user_id: Optional[int] = None

    def _rate_limit_wait(self):
        """Respect rate limits"""
        with self._rate_limit_lock:
            elapsed = time.monotonic() - self._last_request
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
            self._last_request = time.monotonic()

    def _request(self, method: str, base: str, path: str, data: Optional[Dict] = None) -> Any:
        self._rate_limit_wait()
        try:
            response = self.session.request(
                method,
                f"{base}{path}",
                headers=self.headers,
                json=data,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            suffix = f" (HTTP {status})" if status else ""
            raise SubstackAPIError(f"Substack request failed{suffix}") from exc

        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise SubstackAPIError("Substack returned a non-JSON response") from exc

    def _get(self, base: str, path: str) -> Dict:
        """GET request"""
        return self._request("GET", base, path)

    def _post(self, base: str, path: str, data: Dict) -> Dict:
        """POST request"""
        return self._request("POST", base, path, data)

    def _put(self, base: str, path: str, data: Dict) -> Dict:
        """PUT request"""
        return self._request("PUT", base, path, data)

    def _delete(self, base: str, path: str) -> bool:
        """DELETE request"""
        self._request("DELETE", base, path)
        return True

    def close(self) -> None:
        """Release pooled HTTP connections."""
        self.session.close()

    def __enter__(self) -> "SubstackClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    @staticmethod
    def _ensure_doc_structure(body_json: Dict) -> Dict:
        """Ensure a minimal ProseMirror doc shape"""
        if not isinstance(body_json, dict):
            return {"type": "doc", "content": []}
        if body_json.get("type") != "doc":
            body_json["type"] = "doc"
        content = body_json.get("content")
        if not isinstance(content, list):
            body_json["content"] = []
        return body_json

    @staticmethod
    def _parse_draft_body(raw_body: Any) -> Dict:
        """Parse draft_body into a document dict or raise on invalid JSON"""
        if raw_body is None or raw_body == "":
            return {"type": "doc", "content": []}
        if isinstance(raw_body, dict):
            return SubstackClient._ensure_doc_structure(raw_body)
        if isinstance(raw_body, str):
            try:
                parsed = json.loads(raw_body)
            except Exception as exc:
                raise ValueError(f"Could not parse draft_body JSON: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("draft_body JSON is not an object")
            return SubstackClient._ensure_doc_structure(parsed)
        raise ValueError(f"Unsupported draft_body type: {type(raw_body).__name__}")

    def _fix_internal_redirects(self, node: Any, draft_id: int) -> None:
        """Ensure image nodes include internalRedirect"""
        import urllib.parse

        if isinstance(node, list):
            for child in node:
                self._fix_internal_redirects(child, draft_id)
            return
        if not isinstance(node, dict):
            return

        if node.get("type") == "image2":
            attrs = node.setdefault("attrs", {})
            src = attrs.get("src", "")
            if src and not attrs.get("internalRedirect"):
                encoded_url = urllib.parse.quote(src, safe='')
                attrs["internalRedirect"] = f"https://{self.publication}/i/{draft_id}?img={encoded_url}"

        content = node.get("content")
        if isinstance(content, list):
            for child in content:
                self._fix_internal_redirects(child, draft_id)

    def upload_image(self, image_path: str) -> Dict:
        """
        Upload a local image to Substack's CDN.

        Args:
            image_path: Path to local image file

        Returns:
            Dict with keys: url, width, height, bytes, contentType
        """
        import base64
        import mimetypes
        from pathlib import Path

        path = Path(image_path).expanduser().resolve()
        if not path.is_file():
            raise ValueError("image_path must reference an existing regular file")
        max_bytes = int(os.getenv("SUBSTACK_MAX_IMAGE_BYTES", str(15 * 1024 * 1024)))
        size = path.stat().st_size
        if size <= 0:
            raise ValueError("image file is empty")
        if size > max_bytes:
            raise ValueError(f"image exceeds SUBSTACK_MAX_IMAGE_BYTES ({max_bytes})")

        # Read and encode the image
        with path.open('rb') as f:
            image_data = f.read()

        # Detect mime type
        mime_type, _ = mimetypes.guess_type(path.name)
        if not mime_type or not mime_type.startswith("image/"):
            raise ValueError("image_path must have a recognized image file type")

        # Create data URI
        b64_data = base64.b64encode(image_data).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{b64_data}"

        # Upload to Substack (must use JSON format)
        result = self._post(self.pub_base, "/image", {"image": data_uri})
        if not isinstance(result, dict) or not result.get("url"):
            raise SubstackAPIError("Substack image upload response did not include a URL")
        return {
            "url": result.get("url", ""),
            "width": result.get("imageWidth"),
            "height": result.get("imageHeight"),
            "bytes": result.get("bytes"),
            "contentType": result.get("contentType")
        }

    # --- Authentication & Profile ---

    def test_connection(self) -> bool:
        """Test if connected with valid credentials by fetching the archive"""
        try:
            r = self._get(self.pub_base, "/archive?limit=1")
            return isinstance(r, list)
        except (SubstackAPIError, ValueError, TypeError):
            return False

    def get_user_id(self) -> int:
        """Get authenticated user's ID from archive bylines or publication info"""
        if self._user_id:
            return self._user_id
        # Try archive bylines first
        try:
            posts = self._get(self.pub_base, "/archive?limit=1&sort=new")
            if posts and isinstance(posts, list) and len(posts) > 0:
                post = posts[0]
                # Check multiple possible byline keys
                for key in ("publishedBylines", "bylines", "authors"):
                    bylines = post.get(key, [])
                    if bylines and isinstance(bylines, list):
                        self._user_id = bylines[0].get("id")
                        if self._user_id:
                            return self._user_id
                # Fallback: author_id on the post itself
                aid = post.get("author_id")
                if aid:
                    self._user_id = aid
                    return self._user_id
        except:
            pass
        # Fallback: get handle then profile
        try:
            handle = self.get_handle()
            r = self._get(self.sub_base, f"/user/{handle}/public_profile")
            self._user_id = r.get("id")
            if self._user_id:
                return self._user_id
        except:
            pass
        raise ValueError("Could not determine user ID - check credentials")

    def get_handle(self) -> str:
        """Get authenticated user's handle"""
        r = self._get(self.sub_base, "/handle/options")
        for h in r.get("potentialHandles", []):
            if h.get("type") == "existing":
                return h.get("handle")
        raise ValueError("Could not find handle")

    def get_profile(self) -> SubstackProfile:
        """Get authenticated user's profile"""
        handle = self.get_handle()
        r = self._get(self.sub_base, f"/user/{handle}/public_profile")
        return SubstackProfile(
            id=r["id"],
            name=r["name"],
            handle=r["handle"],
            photo_url=r.get("photo_url", ""),
            bio=r.get("bio", "")
        )

    def get_user_profile(self, handle: str) -> Dict:
        """
        Get any user's public profile by their Substack handle.

        Returns rich data including:
        - name, bio, photo_url
        - rough_num_subscribers, followerCount
        - bestseller_tier, leaderboardRanking
        - publicationUsers (their publications)
        - userLinks (social links)
        - twitterAccount
        """
        return self._get(self.sub_base, f"/user/{handle}/public_profile")

    def search_linkedin(self, linkedin_handle: str) -> List[Dict]:
        """
        Search for Substack profiles by LinkedIn handle.

        Note: Only returns profiles that have:
        1. Linked their LinkedIn account to Substack
        2. Met certain platform authenticity thresholds

        Most profiles return empty results.
        """
        r = self._get(self.sub_base, f"/profile/search/linkedin/{linkedin_handle}")
        return r.get("results", [])

    # --- Publication Info ---

    def get_publication(self) -> Dict:
        """Get publication details"""
        return self._get(self.pub_base, "/publication")

    def get_settings(self) -> Dict:
        """Get publication settings"""
        return self._get(self.pub_base, "/settings")

    def get_categories(self) -> List[str]:
        """Get available categories"""
        r = self._get(self.pub_base, "/categories")
        return [c.get("name", c) if isinstance(c, dict) else c for c in r]

    # --- Posts ---

    def get_archive(self, limit: int = 50) -> List[SubstackPost]:
        """Get published posts"""
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 to 100")
        r = self._get(self.pub_base, f"/archive?sort=new&limit={limit}")
        if isinstance(r, dict):
            r = r.get("posts", [])
        if not isinstance(r, list):
            raise SubstackAPIError("Unexpected archive response shape")
        posts = []
        for p in r:
            posts.append(SubstackPost(
                id=p["id"],
                title=p.get("title", ""),
                slug=p.get("slug", ""),
                subtitle=p.get("subtitle", ""),
                body_html=p.get("body_html", ""),
                body_json=p.get("body_json", {}),
                canonical_url=p.get("canonical_url", ""),
                post_date=p.get("post_date", ""),
                audience=p.get("audience", "everyone"),
                reactions=p.get("reactions", {}),
                comment_count=p.get("comment_count", 0),
                wordcount=p.get("wordcount", 0),
                cover_image=p.get("cover_image", ""),
                type=p.get("type", "newsletter")
            ))
        return posts

    def get_post(self, post_id: int) -> SubstackPost:
        """Get a single post by ID"""
        r = self._get(self.sub_base, f"/posts/by-id/{post_id}")
        p = r.get("post", r)
        return SubstackPost(
            id=p["id"],
            title=p.get("title", ""),
            slug=p.get("slug", ""),
            subtitle=p.get("subtitle", ""),
            body_html=p.get("body_html", ""),
            body_json=p.get("body_json", {}),
            canonical_url=p.get("canonical_url", ""),
            post_date=p.get("post_date", ""),
            audience=p.get("audience", "everyone"),
            reactions=p.get("reactions", {}),
            comment_count=p.get("comment_count", 0),
            wordcount=p.get("wordcount", 0),
            cover_image=p.get("cover_image", ""),
            type=p.get("type", "newsletter")
        )

    # --- Drafts ---

    def get_drafts(self) -> List[SubstackDraft]:
        """Get all drafts"""
        r = self._get(self.pub_base, "/drafts")
        # Substack's current endpoint wraps the page in an object.  Retain
        # compatibility with the older bare-list response used by this client.
        if isinstance(r, dict):
            r = r.get("posts", [])
        if not isinstance(r, list):
            raise ValueError("Unexpected drafts response shape")
        drafts = []
        for d in r:
            drafts.append(SubstackDraft(
                id=d["id"],
                title=d.get("draft_title", ""),
                subtitle=d.get("draft_subtitle", ""),
                audience=d.get("audience", "everyone"),
                cover_image=d.get("cover_image", "")
            ))
        return drafts

    def get_draft(self, draft_id: int) -> Dict:
        """Get full draft details"""
        return self._get(self.pub_base, f"/drafts/{draft_id}")

    def create_draft(self, title: str, body: Union[SubstackDocument, Dict, str],
                    subtitle: str = "", audience: str = "everyone",
                    cover_image: str = None) -> SubstackDraft:
        """
        Create a new draft.

        Args:
            title: Post title
            body: SubstackDocument, dict (body_json), or markdown string
            subtitle: Post subtitle
            audience: "everyone", "only_paid", or "founding"
            cover_image: Cover image URL
        """
        title = (title or "").strip()
        if not title:
            raise ValueError("title is required")
        if audience not in {"everyone", "only_paid", "founding"}:
            raise ValueError("audience must be everyone, only_paid, or founding")

        # Convert body to JSON
        if isinstance(body, SubstackDocument):
            body_json = body.build()
        elif isinstance(body, str):
            body_json = MarkdownToSubstack.convert(body)
        else:
            body_json = body
        body_json = self._ensure_doc_structure(body_json)

        user_id = self.get_user_id()

        # Create draft with placeholder first to get ID
        placeholder = {"type": "doc", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "..."}]}]}
        data = {
            "type": "newsletter",
            "draft_title": title,
            "draft_subtitle": subtitle,
            "draft_body": json.dumps(placeholder),
            "draft_bylines": [{"id": user_id, "is_guest": False}],
            "audience": audience
        }

        if cover_image:
            data["cover_image"] = cover_image

        r = self._post(self.pub_base, "/drafts", data)
        draft_id = r["id"]

        # Fix up internalRedirect URLs for any images
        self._fix_internal_redirects(body_json.get("content", []), draft_id)

        # Update draft with corrected body
        self._put(self.pub_base, f"/drafts/{draft_id}", {"draft_body": json.dumps(body_json)})

        return SubstackDraft(
            id=draft_id,
            title=title,
            subtitle=subtitle,
            body_json=body_json,
            audience=audience,
            cover_image=cover_image or ""
        )

    def update_draft(self, draft_id: int, title: str = None,
                    body: Union[SubstackDocument, Dict, str] = None,
                    subtitle: str = None, cover_image: str = None) -> Dict:
        """Update an existing draft"""
        data = {}

        if title is not None:
            data["draft_title"] = title
        if subtitle is not None:
            data["draft_subtitle"] = subtitle
        if cover_image is not None:
            data["cover_image"] = cover_image

        if body is not None:
            if isinstance(body, SubstackDocument):
                body_json = body.build()
            elif isinstance(body, str):
                body_json = MarkdownToSubstack.convert(body)
            else:
                body_json = body
            body_json = self._ensure_doc_structure(body_json)
            self._fix_internal_redirects(body_json.get("content", []), draft_id)
            data["draft_body"] = json.dumps(body_json)

        return self._put(self.pub_base, f"/drafts/{draft_id}", data)

    def publish_draft(self, draft_id: int, send_email: bool = False) -> Dict:
        """Publish a draft"""
        if not isinstance(draft_id, int) or isinstance(draft_id, bool) or draft_id <= 0:
            raise ValueError("draft_id must be a positive integer")
        data = {
            "send": send_email,
            "share_automatically": False
        }
        return self._post(self.pub_base, f"/drafts/{draft_id}/publish", data)

    def delete_draft(self, draft_id: int) -> bool:
        """Delete a draft"""
        return self._delete(self.pub_base, f"/drafts/{draft_id}")

    # --- Notes (Short-form) ---

    def get_notes(self, limit: int = 20) -> List[Dict]:
        """Get notes from your feed"""
        r = self._get(self.pub_base, f"/notes?limit={limit}")
        return r.get("items", [])

    def post_note(self, text: str) -> Dict:
        """Post a simple text note"""
        text = (text or "").strip()
        if not text:
            raise ValueError("text is required")
        data = {
            "bodyJson": {
                "type": "doc",
                "attrs": {"schemaVersion": "v1"},
                "content": [{
                    "type": "paragraph",
                    "content": [{"type": "text", "text": text}]
                }]
            },
            "tabId": "for-you",
            "surface": "feed",
            "replyMinimumRole": "everyone"
        }
        return self._post(self.sub_base, "/comment/feed/", data)

    def post_note_with_link(self, text: str, link_url: str) -> Dict:
        """Post a note with a link attachment"""
        text = (text or "").strip()
        if not text:
            raise ValueError("text is required")
        parsed = urlparse(link_url or "")
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("link_url must be an absolute HTTP(S) URL")
        # Create attachment
        attach = self._post(self.sub_base, "/comment/attachment/", {
            "url": link_url,
            "type": "link"
        })

        # Post with attachment
        data = {
            "bodyJson": {
                "type": "doc",
                "attrs": {"schemaVersion": "v1"},
                "content": [{
                    "type": "paragraph",
                    "content": [{"type": "text", "text": text}]
                }]
            },
            "attachmentIds": [attach["id"]],
            "tabId": "for-you",
            "surface": "feed",
            "replyMinimumRole": "everyone"
        }
        return self._post(self.sub_base, "/comment/feed/", data)

    # --- Reader Feed ---

    def get_feed(self, limit: int = 20) -> List[Dict]:
        """Get your reader feed (posts from subscriptions)"""
        r = self._get(self.sub_base, f"/reader/feed?limit={limit}")
        return r.get("items", [])

    def get_subscriptions(self) -> Dict:
        """Get your subscriptions"""
        return self._get(self.sub_base, "/subscriptions")

    # --- High-level helpers ---

    def publish_markdown(self, markdown: str, title: str = None,
                        subtitle: str = None, send_email: bool = False) -> Dict:
        """
        Convert markdown to Substack format and publish.

        If title/subtitle not provided, extracts from markdown:
        - First # heading becomes title
        - First **bold** line after title becomes subtitle
        """
        lines = markdown.strip().split('\n')

        # Extract title if not provided
        if not title:
            for line in lines:
                if line.startswith('# '):
                    title = line[2:].strip()
                    break
            title = title or "Untitled"

        # Extract subtitle if not provided
        if not subtitle:
            for line in lines:
                if line.startswith('**') and line.endswith('**'):
                    subtitle = line[2:-2].strip()
                    break

        # Remove title/subtitle from body
        body_lines = []
        skip_next_hr = False
        for line in lines:
            if line.startswith('# ') and line[2:].strip() == title:
                continue
            if subtitle and line == f"**{subtitle}**":
                skip_next_hr = True
                continue
            if skip_next_hr and line.strip() == '---':
                skip_next_hr = False
                continue
            body_lines.append(line)

        body_md = '\n'.join(body_lines)

        # Create and publish
        draft = self.create_draft(title=title, subtitle=subtitle or "", body=body_md)
        return self.publish_draft(draft.id, send_email=send_email)


# =============================================================================
# BITCOIN / LIGHTNING INTEGRATION HOOKS
# =============================================================================

class BitcoinIntegration:
    """
    Bitcoin/Lightning integration utilities for Substack content.

    Features:
    - Generate Lightning invoices for tips
    - Add Bitcoin donation addresses to posts
    - Create paywalled content with BTC payments
    - Value4Value streaming sats
    """

    @staticmethod
    def lightning_tip_block(lnurl: str, message: str = "Support this content with Bitcoin") -> Dict:
        """
        Create a tip block with Lightning address.
        Returns a paragraph node with tip info.
        """
        return {
            "type": "paragraph",
            "content": [
                {"type": "text", "text": f"⚡ {message}: ", "marks": [{"type": "strong"}]},
                {"type": "text", "text": lnurl, "marks": [{"type": "code"}]}
            ]
        }

    @staticmethod
    def bitcoin_donation_block(address: str, message: str = "Bitcoin donations welcome") -> Dict:
        """Create a Bitcoin address donation block"""
        return {
            "type": "paragraph",
            "content": [
                {"type": "text", "text": f"₿ {message}: ", "marks": [{"type": "strong"}]},
                {"type": "text", "text": address, "marks": [{"type": "code"}]}
            ]
        }

    @staticmethod
    def paywall_notice(payment_url: str, price_sats: int) -> Dict:
        """Create a paywall notice block"""
        return {
            "type": "blockquote",
            "content": [{
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": f"🔒 This content requires {price_sats:,} sats. "},
                    {"type": "text", "text": "Pay with Lightning",
                     "marks": [{"type": "link", "attrs": {"href": payment_url, "title": None}}]},
                    {"type": "text", "text": " to unlock."}
                ]
            }]
        }

    @staticmethod
    def value4value_block(podcast_value_tag: str = None) -> Dict:
        """Create a Value4Value block for podcast/content monetization"""
        return {
            "type": "paragraph",
            "content": [
                {"type": "text", "text": "💸 Value4Value: ", "marks": [{"type": "strong"}]},
                {"type": "text", "text": "Stream sats while you listen/read. "},
                {"type": "text", "text": "Learn more",
                 "marks": [{"type": "link", "attrs": {"href": "https://value4value.info", "title": None}}]}
            ]
        }


# =============================================================================
# LIVE BLOGGING SUPPORT
# =============================================================================

class LiveBlogSession:
    """
    Helper class for managing live blogging sessions.

    Usage:
        client = SubstackClient(token, publication)
        session = LiveBlogSession(client)

        session.start("Building a Feature Live!")

        # As you code...
        session.add_update("Just implemented the login flow")
        session.add_code("def login(user, pw): ...", language="python")
        session.add_milestone("Authentication Complete")

        session.end(publish=True)
    """

    def __init__(self, client: SubstackClient):
        self.client = client
        self.draft_id: Optional[int] = None
        self.title: str = ""
        self.update_count: int = 0
        self.started_at: Optional[datetime] = None

    @property
    def active(self) -> bool:
        return self.draft_id is not None

    def start(self, title: str, subtitle: str = "") -> int:
        """Start a new live blogging session"""
        if self.active:
            raise RuntimeError("Session already active")

        doc = SubstackDocument()
        doc.paragraph(
            doc.bold("Live Blog Started"),
            f" at {datetime.now().strftime('%I:%M %p')}"
        )
        doc.horizontal_rule()

        draft = self.client.create_draft(title=title, subtitle=subtitle, body=doc)

        self.draft_id = draft.id
        self.title = title
        self.update_count = 0
        self.started_at = datetime.now()

        return draft.id

    def _append_content(self, content: List[Dict]):
        """Append content nodes to the draft"""
        if not self.active:
            raise RuntimeError("No active session")

        draft_data = self.client.get_draft(self.draft_id)
        current_body = draft_data.get("body_json") or draft_data.get("draft_body", "{}")

        try:
            body_json = SubstackClient._parse_draft_body(current_body)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        # Add timestamp
        body_json["content"].append({
            "type": "paragraph",
            "content": [{"type": "text", "text": f"[{datetime.now().strftime('%I:%M:%S %p')}]",
                        "marks": [{"type": "code"}]}]
        })

        # Add new content
        body_json["content"].extend(content)

        self.client.update_draft(draft_id=self.draft_id, body=body_json)
        self.update_count += 1

    def add_update(self, text: str):
        """Add a text update"""
        doc = SubstackDocument()
        doc.paragraph(text)
        self._append_content(doc.build()["content"])

    def add_code(self, code: str, language: str = "", filename: str = ""):
        """Add a code snippet"""
        content = []

        if filename:
            content.append({
                "type": "paragraph",
                "content": [{"type": "text", "text": f"File: {filename}", "marks": [{"type": "code"}]}]
            })

        content.append({
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"type": "text", "text": code}]
        })

        self._append_content(content)

    def add_milestone(self, title: str, description: str = ""):
        """Add a milestone marker"""
        doc = SubstackDocument()
        doc.horizontal_rule()
        doc.heading(f"Milestone: {title}", level=3)
        if description:
            doc.paragraph(description)
        self._append_content(doc.build()["content"])

    def add_image(self, url: str, caption: str = ""):
        """Add an image"""
        doc = SubstackDocument()
        doc.image(url, caption=caption)
        self._append_content(doc.build()["content"])

    def end(self, publish: bool = False, send_email: bool = False) -> Dict:
        """End the session"""
        if not self.active:
            raise RuntimeError("No active session")

        # Add closing
        duration = datetime.now() - self.started_at if self.started_at else None
        duration_str = str(duration).split('.')[0] if duration else "unknown"

        doc = SubstackDocument()
        doc.horizontal_rule()
        doc.paragraph(
            doc.bold("Live Blog Ended"),
            f" at {datetime.now().strftime('%I:%M %p')}"
        )
        doc.paragraph(f"Duration: {duration_str} | Updates: {self.update_count}")

        self._append_content(doc.build()["content"])

        result = {
            "draft_id": self.draft_id,
            "update_count": self.update_count,
            "duration": duration_str,
            "published": False
        }

        if publish:
            pub_result = self.client.publish_draft(self.draft_id, send_email=send_email)
            result["published"] = True
            result["url"] = pub_result.get("canonical_url", "")

        # Reset state
        self.draft_id = None
        self.title = ""
        self.update_count = 0
        self.started_at = None

        return result


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def quick_publish(token: str, publication: str, markdown_file: str,
                 send_email: bool = False) -> str:
    """
    Quick helper to publish a markdown file.

    Returns the published post URL.
    """
    with open(markdown_file, 'r') as f:
        content = f.read()

    client = SubstackClient(token, publication)
    result = client.publish_markdown(content, send_email=send_email)

    return result.get("canonical_url", "")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys
    import os

    token = os.getenv("SUBSTACK_SID", "")
    pub = os.getenv("SUBSTACK_PUBLICATION", "")

    if not token or not pub:
        print("Set SUBSTACK_SID and SUBSTACK_PUBLICATION environment variables")
        sys.exit(1)

    client = SubstackClient(token, pub)

    if client.test_connection():
        profile = client.get_profile()
        print(f"✅ Connected as {profile.name} (@{profile.handle})")

        # Show some stats
        posts = client.get_archive(limit=5)
        drafts = client.get_drafts()

        print(f"\n📊 Stats:")
        print(f"   Posts: {len(posts)}+ published")
        print(f"   Drafts: {len(drafts)} pending")
    else:
        print("❌ Connection failed")
