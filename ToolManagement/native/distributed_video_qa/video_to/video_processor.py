"""
Enhanced video processing pipeline.

download → frames (interval + scene detection) → audio → transcription →
describe frames → OCR → embed → manifest

Improvements over v1:
  - Scene detection via ffmpeg filter (not just dumb intervals)
  - Hybrid frame selection: scene changes + interval, deduplicated
  - OCR capability for text-heavy frames
  - Thumbnail generation for lightweight retrieval
  - Word-level timestamp support
  - Better error handling with fallbacks
  - Progress callbacks
"""

import os
import re
import json
import subprocess
import tempfile
import hashlib
import logging
import shutil
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import io

_whisper_lock = threading.Lock()

import numpy as np
from PIL import Image
import openai
import whisper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FrameData:
    index: int
    timestamp_sec: float
    image_path: str
    thumbnail_path: str = ""
    description: str = ""
    ocr_text: str = ""
    is_scene_change: bool = False
    embedding: Optional[List[float]] = None


@dataclass
class TranscriptChunk:
    index: int
    start_sec: float
    end_sec: float
    text: str
    embedding: Optional[List[float]] = None


@dataclass
class WordTimestamp:
    word: str
    start_sec: float
    end_sec: float


@dataclass
class VideoMetadata:
    video_id: str
    title: str
    duration_sec: float
    url: str
    channel: str = ""
    upload_date: str = ""
    description: str = ""
    thumbnail_url: str = ""


@dataclass
class ProcessedVideo:
    metadata: VideoMetadata
    output_dir: str
    frames: List[FrameData] = field(default_factory=list)
    transcript_chunks: List[TranscriptChunk] = field(default_factory=list)
    full_transcript: str = ""
    audio_path: str = ""
    word_timestamps: List[WordTimestamp] = field(default_factory=list)
    scene_changes: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": asdict(self.metadata),
            "output_dir": self.output_dir,
            "audio_path": self.audio_path,
            "full_transcript": self.full_transcript,
            "num_frames": len(self.frames),
            "num_chunks": len(self.transcript_chunks),
            "num_scene_changes": len(self.scene_changes),
            "scene_changes": self.scene_changes,
            "frames": [asdict(f) for f in self.frames],
            "transcript_chunks": [asdict(c) for c in self.transcript_chunks],
        }


# ---------------------------------------------------------------------------
# Core processor
# ---------------------------------------------------------------------------


class VideoProcessor:
    def __init__(
        self,
        output_root: str = "video_data",
        whisper_model: str = "base",
        frame_interval_sec: float = 10.0,
        max_frames: Optional[int] = None,
        vision_model: str = "gpt-4o-mini",
        embedding_model: str = "text-embedding-3-large",
        scene_threshold: float = 0.3,
        thumbnail_width: int = 320,
        openai_api_key: Optional[str] = None,
    ):
        self.output_root = Path(output_root)
        self.output_root.mkdir(parents=True, exist_ok=True)

        self.frame_interval_sec = frame_interval_sec
        self.max_frames = max_frames or 120
        self.vision_model = vision_model
        self.embedding_model = embedding_model
        self.scene_threshold = scene_threshold
        self.thumbnail_width = thumbnail_width

        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self.oai = openai.OpenAI(api_key=api_key)

        logger.info(f"Loading whisper model '{whisper_model}' …")
        self.whisper = whisper.load_model(whisper_model)
        logger.info("Whisper ready.")

    # ---- download ----------------------------------------------------------

    def download(self, url: str) -> tuple[str, VideoMetadata]:
        """Download video via yt-dlp, return (video_path, metadata)."""
        tmp = tempfile.mkdtemp(prefix="vid_")
        out_template = os.path.join(tmp, "%(id)s.%(ext)s")

        # Bypass datacenter-IP bot checks (Modal egress is a known-blocked range).
        # Alternate player clients + browser UA + retries dramatically improve
        # success from cloud hosts. Override via YTDLP_EXTRA_ARGS if needed.
        _ua = os.environ.get(
            "YTDLP_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        )
        # YouTube enforces PO tokens for web/mweb clients from datacenter IPs
        # (2025+), so a single client often returns an empty format set →
        # "Requested format is not available". Strategy: iterate a list of
        # player clients (cookie-authenticated tv/tv_embedded clients are the
        # most reliable from cloud) until one returns valid metadata+formats.
        # Override the order via YTDLP_PLAYER_CLIENTS (comma-separated groups
        # using '+' within a group, e.g. "tv,tv_embedded,web_safari+web").
        _clients_env = os.environ.get(
            "YTDLP_PLAYER_CLIENTS",
            "web,web_safari,tv,tv_embedded,mweb,web_embedded",
        )
        _client_groups = [c.strip().replace("+", ",") for c in _clients_env.split(",") if c.strip()]

        _extra = os.environ.get("YTDLP_EXTRA_ARGS", "").split()
        _cookies = os.environ.get("YTDLP_COOKIES", "")

        def _mk_bypass(player_client: str):
            b = [
                "--extractor-args", f"youtube:player_client={player_client}",
                "--extractor-args", "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416",
                "--user-agent", _ua,
                "--retries", "5",
                "--extractor-retries", "5",
                "--socket-timeout", "30",
                "--no-warnings",
            ]
            if _cookies:
                b += ["--cookies", _cookies]
            b += _extra
            return b

        # One-time verbose probe so we can confirm the bgutil POT plugin is
        # loaded and whether tokens are being fetched (logged once per process).
        if not getattr(VideoProcessor, "_pot_probed", False):
            try:
                # Diagnose EJS / JS runtime discovery
                try:
                    import shutil as _sh
                    logger.info("EJS diag: deno=%s node=%s PATH=%s",
                                _sh.which("deno"), _sh.which("node"),
                                os.environ.get("PATH", "")[:200])
                    _dv = subprocess.run(["yt-dlp", "-v", "--simulate",
                                          "--print", "%(id)s", url],
                                         capture_output=True, text=True, timeout=60)
                    for _l in (_dv.stderr or "").splitlines():
                        if any(k in _l for k in ["EJS","js runtime","JS runtime","Deno","deno","runtime","JSI","Bun","nsig","challenge"]):
                            logger.info("EJS diag: %s", _l[-200:])
                except Exception as _ee:
                    logger.info("EJS diag failed: %s", _ee)
                for _probe_pc in ["tv", "tv_embedded", "ios", "android", "mweb", "web_safari", "default"]:
                    _vp = subprocess.run(
                        ["yt-dlp", "--list-formats",
                         "--extractor-args", f"youtube:player_client={_probe_pc}",
                         "--extractor-args", "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416",
                         *( ["--cookies", _cookies] if _cookies else [] ),
                         url],
                        capture_output=True, text=True, timeout=60,
                    )
                    _out = (_vp.stdout or "")
                    # count non-storyboard rows (real audio/video)
                    _real = [l for l in _out.splitlines()
                             if l and l[0] not in "[-I" and "mhtml" not in l and "storyboard" not in l]
                    logger.info("POT probe client=%s rc=%s real_formats=%d sample=%s",
                                _probe_pc, _vp.returncode, len(_real),
                                (_real[0][:80] if _real else (_vp.stderr or "")[-120:]))
            except Exception as _pe:
                logger.info("POT probe failed: %s", _pe)
            VideoProcessor._pot_probed = True

        info = None
        _bypass = None
        _last_err = ""
        for _pc in _client_groups:
            _b = _mk_bypass(_pc)
            meta_cmd = ["yt-dlp", *_b, "--dump-json", "--no-download", url]
            logger.info(
                "yt-dlp meta try: player_client=%s cookies=%s",
                _pc, os.environ.get("YTDLP_COOKIES", "<none>"),
            )
            _mp = subprocess.run(meta_cmd, capture_output=True, text=True)
            if _mp.returncode == 0 and _mp.stdout.strip():
                try:
                    _info = json.loads(_mp.stdout.splitlines()[0])
                except Exception as _je:
                    _last_err = f"json parse ({_pc}): {_je}"
                    continue
                if _info.get("formats"):
                    info = _info
                    _bypass = _b
                    logger.info("yt-dlp meta OK via player_client=%s", _pc)
                    break
                _last_err = f"no formats from player_client={_pc}"
            else:
                _last_err = (_mp.stderr or "").strip()[-400:]
            logger.info("yt-dlp meta miss (%s): %s", _pc, _last_err[-160:])

        if info is None:
            raise RuntimeError(
                "yt-dlp metadata fetch failed for all player_clients %s. last: %s"
                % (_client_groups, _last_err)
            )

        video_id = info.get("id", hashlib.sha256(url.encode()).hexdigest()[:12])
        meta = VideoMetadata(
            video_id=video_id,
            title=info.get("title", ""),
            duration_sec=float(info.get("duration", 0)),
            url=url,
            channel=info.get("channel", info.get("uploader", "")),
            upload_date=info.get("upload_date", ""),
            description=info.get("description", "")[:2000],
            thumbnail_url=info.get("thumbnail", ""),
        )

        # Resilient format ladder: prefer <=1080p mp4+m4a, but degrade
        # gracefully to any available video+audio / progressive / best.
        # Different player clients expose different format sets, so a strict
        # selector can yield "Requested format is not available".
        _fmt = os.environ.get(
            "YTDLP_FORMAT",
            "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/"
            "bestvideo[height<=1080]+bestaudio/"
            "best[height<=1080]/"
            "bestvideo*+bestaudio/best",
        )
        dl_cmd = [
            "yt-dlp",
            *_bypass,
            "-f",
            _fmt,
            "--merge-output-format",
            "mp4",
            "-o",
            out_template,
            url,
        ]
        _dl = subprocess.run(dl_cmd, capture_output=True, text=True)
        if _dl.returncode != 0:
            raise RuntimeError(
                "yt-dlp download failed (rc=%d). stderr:\n%s"
                % (_dl.returncode, (_dl.stderr or "")[-2000:])
            )

        files = list(Path(tmp).glob("*.*"))
        if not files:
            raise RuntimeError(f"yt-dlp produced no output in {tmp}")
        video_path = str(files[0])
        logger.info(f"Downloaded → {video_path} ({meta.duration_sec:.0f}s)")
        return video_path, meta

    # ---- extract audio -----------------------------------------------------

    def extract_audio(self, video_path: str, out_dir: Path) -> str:
        audio_path = str(out_dir / "audio.wav")
        cmd = [
            "ffmpeg", "-y", "-i", video_path,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            audio_path,
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        logger.info(f"Audio extracted → {audio_path}")
        return audio_path

    # ---- scene detection ---------------------------------------------------

    def detect_scenes(self, video_path: str) -> List[float]:
        """Detect scene changes using ffmpeg's scene filter. Returns list of timestamps."""
        cmd = [
            "ffmpeg", "-i", video_path,
            "-vf", f"select='gt(scene,{self.scene_threshold})',showinfo",
            "-vsync", "vfr",
            "-f", "null", "-",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        # Parse timestamps from showinfo output
        timestamps = []
        for line in result.stderr.split("\n"):
            if "showinfo" in line and "pts_time:" in line:
                match = re.search(r"pts_time:\s*([\d.]+)", line)
                if match:
                    timestamps.append(float(match.group(1)))
        logger.info(f"Detected {len(timestamps)} scene changes")
        return timestamps

    # ---- extract frames ----------------------------------------------------

    def extract_frames(
        self, video_path: str, out_dir: Path, scene_timestamps: Optional[List[float]] = None
    ) -> List[FrameData]:
        """
        Hybrid frame extraction: scene changes + fixed intervals, deduplicated.
        Also generates thumbnails.
        """
        frames_dir = out_dir / "frames"
        thumbs_dir = out_dir / "thumbnails"
        frames_dir.mkdir(exist_ok=True)
        thumbs_dir.mkdir(exist_ok=True)

        # Get video duration
        probe_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", video_path,
        ]
        try:
            duration = float(subprocess.check_output(probe_cmd, text=True).strip())
        except (subprocess.CalledProcessError, ValueError):
            duration = 0

        # Build target timestamps: scene changes + interval sampling
        target_times: List[tuple[float, bool]] = []  # (timestamp, is_scene_change)

        # Add scene change timestamps
        if scene_timestamps:
            for ts in scene_timestamps:
                target_times.append((ts, True))

        # Add interval-based timestamps
        if duration > 0:
            t = 0.0
            while t < duration:
                target_times.append((t, False))
                t += self.frame_interval_sec

        # Deduplicate: merge timestamps within 2 seconds of each other
        target_times.sort(key=lambda x: x[0])
        merged: List[tuple[float, bool]] = []
        for ts, is_sc in target_times:
            if merged and abs(ts - merged[-1][0]) < 2.0:
                # Keep the one that's a scene change if either is
                if is_sc and not merged[-1][1]:
                    merged[-1] = (merged[-1][0], True)
                continue
            merged.append((ts, is_sc))

        # Cap at max_frames
        if len(merged) > self.max_frames:
            # Prioritize scene changes, then evenly sample the rest
            scenes = [m for m in merged if m[1]]
            intervals = [m for m in merged if not m[1]]
            if len(scenes) >= self.max_frames:
                indices = np.linspace(0, len(scenes) - 1, self.max_frames, dtype=int)
                merged = [scenes[i] for i in indices]
            else:
                remaining = self.max_frames - len(scenes)
                if intervals and remaining > 0:
                    indices = np.linspace(0, len(intervals) - 1, remaining, dtype=int)
                    sampled = [intervals[i] for i in indices]
                    merged = sorted(scenes + sampled, key=lambda x: x[0])
                else:
                    merged = scenes[: self.max_frames]

        # Extract each frame individually via ffmpeg
        frames: List[FrameData] = []
        for i, (ts, is_sc) in enumerate(merged):
            frame_path = str(frames_dir / f"frame_{i:05d}.jpg")
            thumb_path = str(thumbs_dir / f"thumb_{i:05d}.jpg")

            cmd = [
                "ffmpeg", "-y", "-ss", str(ts), "-i", video_path,
                "-frames:v", "1", "-q:v", "2", frame_path,
            ]
            try:
                subprocess.run(cmd, check=True, capture_output=True)
            except subprocess.CalledProcessError:
                continue

            if not os.path.exists(frame_path):
                continue

            # Generate thumbnail
            try:
                with Image.open(frame_path) as img:
                    ratio = self.thumbnail_width / img.width
                    new_h = int(img.height * ratio)
                    thumb = img.resize(
                        (self.thumbnail_width, new_h), Image.LANCZOS
                    )
                    thumb.save(thumb_path, "JPEG", quality=75)
            except Exception:
                thumb_path = frame_path

            frames.append(
                FrameData(
                    index=i,
                    timestamp_sec=ts,
                    image_path=frame_path,
                    thumbnail_path=thumb_path,
                    is_scene_change=is_sc,
                )
            )

        logger.info(
            f"Extracted {len(frames)} frames "
            f"({sum(1 for f in frames if f.is_scene_change)} scene changes, "
            f"{sum(1 for f in frames if not f.is_scene_change)} interval)"
        )
        return frames

    # ---- transcribe --------------------------------------------------------

    def transcribe(
        self, audio_path: str, chunk_duration_sec: float = 30.0
    ) -> tuple[str, List[TranscriptChunk], List[WordTimestamp]]:
        """Transcribe with whisper. Returns (full_text, chunks, word_timestamps)."""
        logger.info("Transcribing audio …")
        with _whisper_lock:
            result = self.whisper.transcribe(
                audio_path, verbose=False, word_timestamps=True
            )
        full_text = result["text"].strip()

        segments = result.get("segments", [])
        chunks: List[TranscriptChunk] = []
        words: List[WordTimestamp] = []

        buf_text = ""
        buf_start = 0.0
        buf_end = 0.0

        for seg in segments:
            if not buf_text:
                buf_start = seg["start"]
            buf_text += " " + seg["text"]
            buf_end = seg["end"]

            # Collect word-level timestamps
            for w in seg.get("words", []):
                words.append(
                    WordTimestamp(
                        word=w.get("word", ""),
                        start_sec=w.get("start", 0),
                        end_sec=w.get("end", 0),
                    )
                )

            if (buf_end - buf_start) >= chunk_duration_sec:
                chunks.append(
                    TranscriptChunk(
                        index=len(chunks),
                        start_sec=buf_start,
                        end_sec=buf_end,
                        text=buf_text.strip(),
                    )
                )
                buf_text = ""

        if buf_text.strip():
            chunks.append(
                TranscriptChunk(
                    index=len(chunks),
                    start_sec=buf_start,
                    end_sec=buf_end,
                    text=buf_text.strip(),
                )
            )

        logger.info(
            f"Transcribed {len(chunks)} chunks, {len(words)} words, "
            f"{len(full_text)} chars total"
        )
        return full_text, chunks, words

    # ---- vision description ------------------------------------------------

    def _encode_frame(self, path: str, max_dim: int = 1024) -> str:
        with Image.open(path) as img:
            w, h = img.size
            if w > max_dim or h > max_dim:
                ratio = min(max_dim / w, max_dim / h)
                img = img.resize(
                    (int(w * ratio), int(h * ratio)), Image.LANCZOS
                )
            if img.mode != "RGB":
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=85)
            return base64.b64encode(buf.getvalue()).decode()

    def _encode_frame_hires(self, path: str, max_dim: int = 2048) -> str:
        """High-res encode for screen content — preserves text readability."""
        with Image.open(path) as img:
            w, h = img.size
            if w > max_dim or h > max_dim:
                ratio = min(max_dim / w, max_dim / h)
                img = img.resize(
                    (int(w * ratio), int(h * ratio)), Image.LANCZOS
                )
            if img.mode != "RGB":
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="PNG", quality=95)
            return base64.b64encode(buf.getvalue()).decode()

    def _crop_quadrants(self, path: str, max_dim: int = 1536) -> List[str]:
        """
        Split a frame into 4 overlapping quadrants for detailed screen parsing.
        Returns list of base64 encoded crops: [top-left, top-right, bottom-left, bottom-right].
        """
        with Image.open(path) as img:
            w, h = img.size
            if img.mode != "RGB":
                img = img.convert("RGB")

            # 10% overlap so text on boundaries isn't cut
            overlap_x = int(w * 0.10)
            overlap_y = int(h * 0.10)
            mid_x = w // 2
            mid_y = h // 2

            regions = [
                (0, 0, mid_x + overlap_x, mid_y + overlap_y),           # top-left
                (mid_x - overlap_x, 0, w, mid_y + overlap_y),           # top-right
                (0, mid_y - overlap_y, mid_x + overlap_x, h),           # bottom-left
                (mid_x - overlap_x, mid_y - overlap_y, w, h),           # bottom-right
            ]

            crops_b64 = []
            for box in regions:
                crop = img.crop(box)
                cw, ch = crop.size
                if cw > max_dim or ch > max_dim:
                    ratio = min(max_dim / cw, max_dim / ch)
                    crop = crop.resize(
                        (int(cw * ratio), int(ch * ratio)), Image.LANCZOS
                    )
                buf = io.BytesIO()
                crop.save(buf, format="PNG", quality=95)
                crops_b64.append(base64.b64encode(buf.getvalue()).decode())
            return crops_b64

    def detect_screen_content(self, frame_path: str) -> bool:
        """
        Heuristic: detect if a frame is screen/UI content vs real-world footage.
        Screen content has high edge density, uniform backgrounds, text-like patterns.
        """
        try:
            with Image.open(frame_path) as img:
                img = img.convert("L").resize((320, 180))
                arr = np.array(img, dtype=np.float32)
                # Sobel-ish edge detection
                dx = np.abs(np.diff(arr, axis=1))
                dy = np.abs(np.diff(arr, axis=0))
                edge_density = (np.mean(dx) + np.mean(dy)) / 2
                # Screen content typically: high edge density (text, UI lines)
                # + low color variance in large regions
                std = np.std(arr)
                # Screens: edge_density > 8 and moderate std
                return edge_density > 8.0 and std < 80.0
        except Exception:
            return False

    def describe_frame(self, frame: FrameData, context: str = "", is_screencast: bool = False) -> str:
        """Use vision model to describe a single frame."""
        if is_screencast:
            return self._describe_screen_frame(frame, context)

        img_b64 = self._encode_frame(frame.image_path)
        ts_str = f"{frame.timestamp_sec:.1f}s"

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a precise video frame analyst. Describe what you see: "
                    "key visual elements, any text/UI visible, actions, scene composition. "
                    "Include any data, charts, graphs, or interface elements in detail. "
                    "2-4 sentences max."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"Frame at {ts_str}.{(' Context: ' + context) if context else ''}",
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{img_b64}"
                        },
                    },
                ],
            },
        ]
        resp = self.oai.chat.completions.create(
            model=self.vision_model, messages=messages, max_completion_tokens=300
        )
        return resp.choices[0].message.content.strip()

    def _describe_screen_frame(self, frame: FrameData, context: str = "") -> str:
        """
        Screen-share-optimized description: sends full-res image + zoomed quadrants
        to vision model for maximum text/UI extraction.
        """
        ts_str = f"{frame.timestamp_sec:.1f}s"

        # Full frame at high res
        full_b64 = self._encode_frame_hires(frame.image_path)

        # Zoomed quadrants for dense UI text
        try:
            quads = self._crop_quadrants(frame.image_path)
        except Exception:
            quads = []

        user_content: List[Dict[str, Any]] = [
            {
                "type": "text",
                "text": (
                    f"Frame at {ts_str}. This is a SCREEN RECORDING / SCREENCAST. "
                    f"{('Context: ' + context + '. ') if context else ''}"
                    "The full screenshot is shown first, followed by zoomed quadrants "
                    "(top-left, top-right, bottom-left, bottom-right) for detail."
                ),
            },
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{full_b64}", "detail": "high"},
            },
        ]

        for i, qb64 in enumerate(quads):
            labels = ["top-left", "top-right", "bottom-left", "bottom-right"]
            user_content.append({
                "type": "text",
                "text": f"Zoomed {labels[i]} quadrant:",
            })
            user_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{qb64}", "detail": "high"},
            })

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert screen content analyst. This is a SCREENCAST frame. "
                    "You are shown the full screenshot plus zoomed quadrants.\n\n"
                    "Extract and describe EVERYTHING visible:\n"
                    "1. APPLICATION: What software/website is shown? Window title, URL bar, tab names.\n"
                    "2. UI STATE: Menu items selected, panels open, toolbar buttons, checkboxes, dropdowns.\n"
                    "3. DATA/CHARTS: All axis labels, legend entries, data values, contour levels, "
                    "color scales. Read numbers precisely.\n"
                    "4. TEXT CONTENT: Every piece of readable text — labels, annotations, "
                    "status bars, tooltips, console output.\n"
                    "5. CURSOR/SELECTION: Where is the mouse? What's highlighted or selected?\n"
                    "6. LAYOUT: Describe spatial arrangement of panels/windows.\n\n"
                    "Be exhaustive. 5-10 sentences. Every readable detail matters."
                ),
            },
            {"role": "user", "content": user_content},
        ]
        resp = self.oai.chat.completions.create(
            model=self.vision_model, messages=messages, max_completion_tokens=800
        )
        return resp.choices[0].message.content.strip()

    def ocr_frame(self, frame_path: str, use_quadrants: bool = False) -> str:
        """Extract all visible text from a frame using vision model."""
        if use_quadrants:
            return self._ocr_frame_with_quadrants(frame_path)

        img_b64 = self._encode_frame_hires(frame_path)
        messages = [
            {
                "role": "system",
                "content": (
                    "Extract ALL text visible in this image. Include:\n"
                    "- Window titles, menu items, toolbar labels\n"
                    "- Data values, axis labels, legend text\n"
                    "- Status bar text, tooltips, annotations\n"
                    "- Any numbers, codes, filenames, URLs\n"
                    "Preserve spatial layout. Group by screen region. "
                    "If no text, respond with empty string."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{img_b64}", "detail": "high"},
                    },
                ],
            },
        ]
        resp = self.oai.chat.completions.create(
            model=self.vision_model, messages=messages, max_completion_tokens=1500
        )
        return resp.choices[0].message.content.strip()

    def _ocr_frame_with_quadrants(self, frame_path: str) -> str:
        """OCR with zoomed quadrants for maximum text extraction from dense screens."""
        full_b64 = self._encode_frame_hires(frame_path)
        quads = self._crop_quadrants(frame_path)

        user_content: List[Dict[str, Any]] = [
            {"type": "text", "text": "Full screenshot:"},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{full_b64}", "detail": "high"}},
        ]
        labels = ["top-left", "top-right", "bottom-left", "bottom-right"]
        for i, qb64 in enumerate(quads):
            user_content.append({"type": "text", "text": f"Zoomed {labels[i]}:"})
            user_content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{qb64}", "detail": "high"}})

        messages = [
            {
                "role": "system",
                "content": (
                    "Extract EVERY piece of text from this screen recording frame. "
                    "You see the full screenshot plus 4 zoomed quadrants for detail.\n\n"
                    "Read ALL: window titles, menus, buttons, labels, data values, "
                    "axis numbers, legend entries, status bars, URLs, filenames, "
                    "console output, annotations, tooltips.\n\n"
                    "Organize by screen region. Include exact numbers and values. "
                    "Miss nothing."
                ),
            },
            {"role": "user", "content": user_content},
        ]
        resp = self.oai.chat.completions.create(
            model=self.vision_model, messages=messages, max_completion_tokens=2000
        )
        return resp.choices[0].message.content.strip()

    def describe_frames_batch(
        self,
        frames: List[FrameData],
        context: str = "",
        max_workers: int = 4,
        do_ocr: bool = False,
        screencast_mode: Optional[str] = None,
    ) -> List[FrameData]:
        """
        Describe frames in parallel, optionally with OCR.

        screencast_mode:
          None / "auto" — auto-detect per frame
          "on"          — treat all frames as screen content (zoom+quadrants)
          "off"         — treat all as natural footage
        """
        # Auto-detect: sample a few frames to decide
        is_screencast_video = False
        if screencast_mode in (None, "auto"):
            sample_indices = [0, len(frames) // 2, -1] if len(frames) > 2 else range(len(frames))
            screen_votes = 0
            for idx in sample_indices:
                if self.detect_screen_content(frames[idx].image_path):
                    screen_votes += 1
            is_screencast_video = screen_votes > len(sample_indices) / 2
            if is_screencast_video:
                logger.info("AUTO-DETECTED: screencast content → using screen-aware extraction")
                do_ocr = True  # always OCR screencasts
        elif screencast_mode == "on":
            is_screencast_video = True
            do_ocr = True

        logger.info(
            f"Describing {len(frames)} frames with {self.vision_model} "
            f"(screencast={'YES' if is_screencast_video else 'no'}, OCR={'on' if do_ocr else 'off'}) …"
        )

        def _do(f: FrameData) -> FrameData:
            # Per-frame screen detection if auto mode
            frame_is_screen = is_screencast_video
            if screencast_mode in (None, "auto") and not is_screencast_video:
                frame_is_screen = self.detect_screen_content(f.image_path)

            try:
                f.description = self.describe_frame(f, context, is_screencast=frame_is_screen)
            except Exception as e:
                logger.warning(f"Frame {f.index} description failed: {e}")
                f.description = f"Frame at {f.timestamp_sec:.1f}s"

            if do_ocr:
                try:
                    f.ocr_text = self.ocr_frame(f.image_path, use_quadrants=frame_is_screen)
                except Exception as e:
                    logger.warning(f"Frame {f.index} OCR failed: {e}")
            return f

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_do, f): f for f in frames}
            done = []
            for fut in as_completed(futures):
                done.append(fut.result())
        done.sort(key=lambda f: f.index)
        logger.info("Frame descriptions complete.")
        return done

    # ---- embeddings --------------------------------------------------------

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        all_embs: List[List[float]] = []
        for i in range(0, len(texts), 2048):
            batch = texts[i : i + 2048]
            resp = self.oai.embeddings.create(
                model=self.embedding_model, input=batch
            )
            all_embs.extend([d.embedding for d in resp.data])
        return all_embs

    def embed_chunks(self, chunks: List[TranscriptChunk]) -> List[TranscriptChunk]:
        texts = [c.text for c in chunks]
        embeddings = self.embed_texts(texts)
        for c, emb in zip(chunks, embeddings):
            c.embedding = emb
        return chunks

    def embed_frames(self, frames: List[FrameData]) -> List[FrameData]:
        texts = []
        for f in frames:
            t = f"{f.timestamp_sec:.1f}s: {f.description}"
            if f.ocr_text:
                t += f" [OCR: {f.ocr_text}]"
            texts.append(t)
        embeddings = self.embed_texts(texts)
        for f, emb in zip(frames, embeddings):
            f.embedding = emb
        return frames

    # ---- full pipeline -----------------------------------------------------

    def process(
        self,
        url: str,
        describe_frames: bool = True,
        generate_embeddings: bool = True,
        chunk_duration_sec: float = 30.0,
        do_ocr: bool = False,
        detect_scenes: bool = True,
        screencast_mode: Optional[str] = None,
    ) -> ProcessedVideo:
        """
        Full pipeline:
        download → scene detect → frames → audio → transcribe → describe → OCR → embed

        screencast_mode: None/"auto" (detect), "on" (force screen-aware), "off" (force natural)
        """

        # 1. Download
        video_path, meta = self.download(url)
        out_dir = self.output_root / meta.video_id
        out_dir.mkdir(parents=True, exist_ok=True)

        # 2. Scene detection
        scene_timestamps: List[float] = []
        if detect_scenes:
            try:
                scene_timestamps = self.detect_scenes(video_path)
            except Exception as e:
                logger.warning(f"Scene detection failed: {e}")

        # 3. Extract audio + frames in parallel
        with ThreadPoolExecutor(max_workers=2) as pool:
            audio_fut = pool.submit(self.extract_audio, video_path, out_dir)
            frames_fut = pool.submit(
                self.extract_frames, video_path, out_dir, scene_timestamps
            )
            audio_path = audio_fut.result()
            frames = frames_fut.result()

        # 4. Transcribe
        full_transcript, chunks, word_timestamps = self.transcribe(
            audio_path, chunk_duration_sec
        )

        # 5. Describe frames (with optional OCR, screen-aware)
        if describe_frames:
            context = f"Video: {meta.title}"
            frames = self.describe_frames_batch(
                frames, context, do_ocr=do_ocr, screencast_mode=screencast_mode,
            )

        # 6. Embed
        if generate_embeddings:
            logger.info("Generating embeddings …")
            chunks = self.embed_chunks(chunks)
            frames = self.embed_frames(frames)

        # 7. Assemble result
        result = ProcessedVideo(
            metadata=meta,
            output_dir=str(out_dir),
            frames=frames,
            transcript_chunks=chunks,
            full_transcript=full_transcript,
            audio_path=audio_path,
            word_timestamps=word_timestamps,
            scene_changes=scene_timestamps,
        )

        # 8. Save manifest
        manifest_path = out_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        logger.info(f"Saved manifest → {manifest_path}")

        # Cleanup downloaded video (frames + audio are kept)
        try:
            os.unlink(video_path)
        except OSError:
            pass
        # Also clean up temp dir
        try:
            parent = Path(video_path).parent
            if parent.name.startswith("vid_"):
                shutil.rmtree(parent, ignore_errors=True)
        except Exception:
            pass

        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process a YouTube video into structured content"
    )
    parser.add_argument("url", help="YouTube URL")
    parser.add_argument(
        "--output", default="video_data", help="Output root directory"
    )
    parser.add_argument(
        "--whisper-model",
        default="base",
        choices=["tiny", "base", "small", "medium", "large"],
        help="Whisper model size",
    )
    parser.add_argument(
        "--frame-interval",
        type=float,
        default=10.0,
        help="Seconds between frame captures",
    )
    parser.add_argument(
        "--max-frames", type=int, default=120, help="Max frames to keep"
    )
    parser.add_argument(
        "--no-describe",
        action="store_true",
        help="Skip vision frame descriptions",
    )
    parser.add_argument(
        "--no-embed",
        action="store_true",
        help="Skip embedding generation",
    )
    parser.add_argument(
        "--ocr", action="store_true", help="Enable OCR on frames"
    )
    parser.add_argument(
        "--no-scene-detect",
        action="store_true",
        help="Disable scene detection",
    )
    parser.add_argument(
        "--chunk-duration",
        type=float,
        default=30.0,
        help="Transcript chunk duration (sec)",
    )
    args = parser.parse_args()

    proc = VideoProcessor(
        output_root=args.output,
        whisper_model=args.whisper_model,
        frame_interval_sec=args.frame_interval,
        max_frames=args.max_frames,
    )
    result = proc.process(
        args.url,
        describe_frames=not args.no_describe,
        generate_embeddings=not args.no_embed,
        chunk_duration_sec=args.chunk_duration,
        do_ocr=args.ocr,
        detect_scenes=not args.no_scene_detect,
    )
    print(f"\nDone! {result.metadata.title}")
    print(f"  Frames: {len(result.frames)}")
    print(f"  Scene changes: {len(result.scene_changes)}")
    print(f"  Chunks: {len(result.transcript_chunks)}")
    print(f"  Output: {result.output_dir}")
