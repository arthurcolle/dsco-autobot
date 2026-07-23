"""
GPT-powered video analysis engine.

Provides: chapters, entities, summaries, temporal reasoning, OCR,
moment description, cross-video comparison, SRT/notes export.

All expensive results are cached to disk in the video's output directory.
"""

import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

import openai

logger = logging.getLogger(__name__)


class VideoAnalyzer:
    """Analysis engine for processed videos. Results are cached per-video."""

    def __init__(
        self,
        model: str = "gpt-4o",
        vision_model: str = "gpt-4o-mini",
    ):
        self.oai = openai.OpenAI()
        self.model = model
        self.vision_model = vision_model

    # ---- cache helpers -----------------------------------------------------

    def _cache_path(self, output_dir: str, key: str) -> Path:
        return Path(output_dir) / f"_cache_{key}.json"

    def _load_cache(self, output_dir: str, key: str) -> Optional[Any]:
        p = self._cache_path(output_dir, key)
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
        return None

    def _save_cache(self, output_dir: str, key: str, data: Any):
        p = self._cache_path(output_dir, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2, default=str))

    # ---- summaries ---------------------------------------------------------

    def generate_summary(
        self,
        transcript: str,
        title: str,
        output_dir: str,
        level: str = "medium",
    ) -> Dict:
        """Generate video summary at short/medium/detailed level."""
        cache_key = f"summary_{level}"
        cached = self._load_cache(output_dir, cache_key)
        if cached:
            return cached

        length_guide = {
            "short": "1-2 sentences, under 50 words",
            "medium": "A paragraph, 100-200 words covering main points",
            "detailed": (
                "A comprehensive summary, 300-500 words with all key topics, "
                "examples, and conclusions"
            ),
        }

        messages = [
            {
                "role": "system",
                "content": (
                    f"Summarize this video transcript. "
                    f"Length: {length_guide.get(level, length_guide['medium'])}. "
                    "Be precise and informative. Include specific details, "
                    "names, numbers, and technical terms mentioned."
                ),
            },
            {
                "role": "user",
                "content": f"Title: {title}\n\nTranscript:\n{transcript[:30000]}",
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model, messages=messages, max_completion_tokens=1000
        )
        result = {
            "level": level,
            "summary": resp.choices[0].message.content.strip(),
            "title": title,
        }
        self._save_cache(output_dir, cache_key, result)
        return result

    # ---- chapters ----------------------------------------------------------

    def detect_chapters(
        self,
        transcript: str,
        title: str,
        output_dir: str,
        chunks: Optional[List[Dict]] = None,
    ) -> List[Dict]:
        """Auto-detect topic chapters from transcript."""
        cached = self._load_cache(output_dir, "chapters")
        if cached:
            return cached

        # Build timed transcript for better chapter boundary detection
        timed_text = ""
        if chunks:
            for c in chunks:
                meta = c.get("metadata", c)
                start = meta.get("start_sec", 0)
                text = c.get("text", "")
                m, s = divmod(int(start), 60)
                timed_text += f"[{m}:{s:02d}] {text}\n"
        else:
            timed_text = transcript[:30000]

        messages = [
            {
                "role": "system",
                "content": (
                    "Analyze this video transcript and identify distinct topic "
                    "chapters/sections. For each chapter provide:\n"
                    '- "title": short descriptive title\n'
                    '- "start_sec": approximate start time in seconds\n'
                    '- "summary": 1-2 sentence summary\n\n'
                    "Return a JSON object with a single key \"chapters\" containing "
                    "an array. Identify 3-15 chapters depending on content density."
                ),
            },
            {
                "role": "user",
                "content": f"Title: {title}\n\nTranscript:\n{timed_text}",
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model,
            messages=messages,
            max_completion_tokens=2000,
            response_format={"type": "json_object"},
        )

        try:
            raw = json.loads(resp.choices[0].message.content)
            chapters = raw.get("chapters", raw) if isinstance(raw, dict) else raw
            if not isinstance(chapters, list):
                chapters = []
        except Exception:
            chapters = []

        self._save_cache(output_dir, "chapters", chapters)
        return chapters

    # ---- entities ----------------------------------------------------------

    def extract_entities(
        self, transcript: str, title: str, output_dir: str
    ) -> Dict:
        """Extract people, tools, concepts, locations, acronyms, values."""
        cached = self._load_cache(output_dir, "entities")
        if cached:
            return cached

        messages = [
            {
                "role": "system",
                "content": (
                    "Extract key entities from this video transcript. Return JSON:\n"
                    '- "people": [{name, context}]\n'
                    '- "tools": [{name, context}]\n'
                    '- "concepts": [{name, context}]\n'
                    '- "locations": [{name, context}]\n'
                    '- "acronyms": [{acronym, meaning}]\n'
                    '- "values": [{value, context}]\n\n'
                    "Include context for each. Return valid JSON object."
                ),
            },
            {
                "role": "user",
                "content": f"Title: {title}\n\nTranscript:\n{transcript[:30000]}",
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model,
            messages=messages,
            max_completion_tokens=2000,
            response_format={"type": "json_object"},
        )

        try:
            entities = json.loads(resp.choices[0].message.content)
        except Exception:
            entities = {}

        self._save_cache(output_dir, "entities", entities)
        return entities

    # ---- temporal reasoning ------------------------------------------------

    def describe_moment(
        self,
        timestamp_sec: float,
        transcript_context: str,
        frame_descriptions: List[str],
        title: str,
    ) -> str:
        """Describe what's happening at a specific moment."""
        frame_ctx = (
            "\n".join(frame_descriptions)
            if frame_descriptions
            else "No frame descriptions available."
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "Describe what is happening at this moment in the video "
                    "based on the transcript and visual frame descriptions. "
                    "Be specific and concise. Mention any on-screen elements."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Video: {title}\n"
                    f"Timestamp: {_fmt_time(timestamp_sec)}\n\n"
                    f"Transcript around this moment:\n{transcript_context}\n\n"
                    f"Visual context:\n{frame_ctx}"
                ),
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model, messages=messages, max_completion_tokens=500
        )
        return resp.choices[0].message.content.strip()

    def find_moment(
        self,
        query: str,
        chunks: List[Dict],
        frame_descs: List[Dict],
    ) -> List[Dict]:
        """Find when something specific happens."""
        context_parts = []
        for c in chunks:
            meta = c.get("metadata", c)
            text = c.get("text", "")
            start = meta.get("start_sec", 0)
            end = meta.get("end_sec", 0)
            context_parts.append(
                f"[{_fmt_time(start)}-{_fmt_time(end)}] {text}"
            )

        for f in frame_descs:
            meta = f.get("metadata", f)
            text = f.get("text", "")
            ts = meta.get("timestamp_sec", 0)
            context_parts.append(f"[{_fmt_time(ts)} VISUAL] {text}")

        context = "\n".join(context_parts)

        messages = [
            {
                "role": "system",
                "content": (
                    "Find when the described event/topic occurs in this video. "
                    "Return JSON with key \"moments\" containing an array of:\n"
                    '- "timestamp_sec": approximate seconds\n'
                    '- "timestamp_fmt": formatted time\n'
                    '- "description": what happens\n'
                    '- "confidence": "high"/"medium"/"low"\n\n'
                    "Return 1-5 most relevant moments."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Find when this happens: {query}\n\n"
                    f"Video content:\n{context[:25000]}"
                ),
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model,
            messages=messages,
            max_completion_tokens=1000,
            response_format={"type": "json_object"},
        )

        try:
            result = json.loads(resp.choices[0].message.content)
            return result.get("moments", [])
        except Exception:
            return []

    # ---- OCR ---------------------------------------------------------------

    def ocr_frame(self, frame_path: str) -> str:
        """Extract all visible text from a frame image."""
        import base64 as b64mod
        import io
        from PIL import Image

        with Image.open(frame_path) as img:
            w, h = img.size
            max_dim = 1536
            if w > max_dim or h > max_dim:
                ratio = min(max_dim / w, max_dim / h)
                img = img.resize(
                    (int(w * ratio), int(h * ratio)), Image.LANCZOS
                )
            if img.mode != "RGB":
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=90)
            b64 = b64mod.b64encode(buf.getvalue()).decode()

        messages = [
            {
                "role": "system",
                "content": (
                    "Extract ALL text visible in this image. Include UI labels, "
                    "numbers, axis labels, annotations, watermarks, titles — "
                    "everything readable. Preserve layout where possible. "
                    "If no text is visible, respond with exactly: NO_TEXT"
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{b64}"
                        },
                    },
                ],
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.vision_model, messages=messages, max_completion_tokens=1000
        )
        text = resp.choices[0].message.content.strip()
        return "" if text == "NO_TEXT" else text

    # ---- cross-video comparison -------------------------------------------

    def compare_videos(
        self, video1_info: Dict, video2_info: Dict
    ) -> Dict:
        """Compare content/topics across two videos."""
        messages = [
            {
                "role": "system",
                "content": (
                    "Compare these two videos. Return JSON with:\n"
                    '- "shared_topics": topics in both\n'
                    '- "unique_to_first": only in video 1\n'
                    '- "unique_to_second": only in video 2\n'
                    '- "contradictions": conflicting claims\n'
                    '- "complementary": how they complement each other\n'
                    '- "summary": brief comparison\n'
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Video 1: {video1_info.get('title', 'Unknown')}\n"
                    f"Transcript: {video1_info.get('transcript', '')[:15000]}\n\n"
                    f"Video 2: {video2_info.get('title', 'Unknown')}\n"
                    f"Transcript: {video2_info.get('transcript', '')[:15000]}"
                ),
            },
        ]

        resp = self.oai.chat.completions.create(
            model=self.model,
            messages=messages,
            max_completion_tokens=2000,
            response_format={"type": "json_object"},
        )

        try:
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return {"error": "Failed to parse comparison"}

    # ---- export ------------------------------------------------------------

    def generate_srt(self, chunks: List[Dict]) -> str:
        """Generate SRT subtitle content from transcript chunks."""
        lines = []
        for i, c in enumerate(chunks, 1):
            meta = c.get("metadata", c)
            start = meta.get("start_sec", 0)
            end = meta.get("end_sec", start + 5)
            text = c.get("text", "")

            lines.append(str(i))
            lines.append(f"{_srt_time(start)} --> {_srt_time(end)}")
            # Split long text into 2-line subtitles
            words = text.split()
            if len(words) > 12:
                mid = len(words) // 2
                lines.append(" ".join(words[:mid]))
                lines.append(" ".join(words[mid:]))
            else:
                lines.append(text)
            lines.append("")

        return "\n".join(lines)

    def generate_notes(
        self,
        title: str,
        transcript: str,
        chapters: List[Dict],
        entities: Dict,
        summary: str = "",
    ) -> str:
        """Generate structured markdown notes."""
        lines = [f"# {title}", ""]

        if summary:
            lines.extend(["## Summary", summary, ""])

        if chapters:
            lines.append("## Chapters")
            for ch in chapters:
                ts = ch.get("start_sec", 0)
                lines.append(
                    f"- **{_fmt_time(ts)}** — {ch.get('title', 'Untitled')}"
                )
                if ch.get("summary"):
                    lines.append(f"  {ch['summary']}")
            lines.append("")

        if entities:
            lines.append("## Key Entities")
            for category, items in entities.items():
                if not items:
                    continue
                lines.append(
                    f"### {category.replace('_', ' ').title()}"
                )
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            name = item.get("name", item.get("acronym", str(item)))
                            ctx = item.get("context", item.get("meaning", ""))
                            lines.append(f"- **{name}**: {ctx}")
                        else:
                            lines.append(f"- {item}")
                elif isinstance(items, dict):
                    for k, v in items.items():
                        lines.append(f"- **{k}**: {v}")
                lines.append("")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers (module-level)
# ---------------------------------------------------------------------------


def _fmt_time(sec: Any) -> str:
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return "??:??"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _srt_time(sec: Any) -> str:
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return "00:00:00,000"
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    ms = int((s % 1) * 1000)
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d},{ms:03d}"
