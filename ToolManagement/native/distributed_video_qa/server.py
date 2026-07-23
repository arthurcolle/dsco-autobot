#!/usr/bin/env python3
"""
Distributed Video Q&A MCP

Provides semantic search, Q&A, transcription, and analysis over YouTube videos.
Wraps the video_to processing pipeline (VideoProcessor + VideoStore + VideoAnalyzer).

Run with:
    python -m native.distributed_video_qa.server --port 4005

Environment:
    VIDEO_QA_PATH   Path to the video_to source directory
                    Default: ~/Library/Mobile Documents/com~apple~CloudDocs/
                             Untitled Folder/NewMachine/what/video_to
    VIDEO_DATA_DIR  Where processed video data is stored (default: video_data)
    VIDEO_CHROMA_DIR ChromaDB persistence dir (default: video_chroma)
    WHISPER_MODEL   Whisper model size (default: base)
    OPENAI_API_KEY  Required for embeddings and vision
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from native.base import NativeMCPBase, CapabilityType

logger = logging.getLogger(__name__)

_VIDEO_QA_DEFAULT = str(
    Path.home()
    / "Library/Mobile Documents/com~apple~CloudDocs"
    / "Untitled Folder/NewMachine/what/video_to"
)
VIDEO_QA_PATH = os.environ.get("VIDEO_QA_PATH", _VIDEO_QA_DEFAULT)

if VIDEO_QA_PATH not in sys.path:
    sys.path.insert(0, VIDEO_QA_PATH)

DATA_DIR = os.environ.get("VIDEO_DATA_DIR", "video_data")
CHROMA_DIR = os.environ.get("VIDEO_CHROMA_DIR", "video_chroma")
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "base")
VISION_MODEL = os.environ.get("VISION_MODEL", "gpt-4o-mini")
QA_MODEL = os.environ.get("QA_MODEL", "gpt-4o")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-large")

_executor = ThreadPoolExecutor(max_workers=4)


def _fmt_time(sec) -> str:
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return "??:??"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


class DistributedVideoQAMCP(NativeMCPBase):
    SERVICE_ID = "distributed-video-qa"
    SERVICE_NAME = "Distributed Video Q&A MCP"
    VERSION = "1.0.0"

    CAPABILITIES = [
        CapabilityType.READ,
        CapabilityType.SEARCH,
        CapabilityType.EXECUTE,
    ]

    REQUIRES = []
    DEPENDS = []

    TOOLS = [
        {
            "name": "process_video",
            "description": "Download and process a YouTube video for Q&A. Extracts transcript, frames, and embeddings.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "YouTube URL"},
                    "frame_interval": {"type": "number", "default": 10.0},
                    "max_frames": {"type": "integer", "default": 120},
                    "enable_ocr": {"type": "boolean", "default": False},
                    "screencast_mode": {"type": "string", "default": "auto"},
                },
                "required": ["url"],
            },
        },
        {
            "name": "process_videos_batch",
            "description": "Process multiple YouTube videos in parallel.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of YouTube URLs",
                    },
                    "max_workers": {"type": "integer", "default": 4},
                    "enable_ocr": {"type": "boolean", "default": False},
                },
                "required": ["urls"],
            },
        },
        {
            "name": "list_videos",
            "description": "List all processed videos with metadata.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "ask_video",
            "description": "Ask a question about a processed video using RAG over transcript and frames.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "video_id": {"type": "string", "description": "Optional: filter to specific video"},
                    "n_context": {"type": "integer", "default": 8},
                },
                "required": ["question"],
            },
        },
        {
            "name": "search_video",
            "description": "Semantic search across video transcripts and frame descriptions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "video_id": {"type": "string"},
                    "n_results": {"type": "integer", "default": 10},
                    "search_type": {"type": "string", "enum": ["all", "transcript", "frames"], "default": "all"},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_transcript",
            "description": "Get the full transcript for a processed video.",
            "inputSchema": {
                "type": "object",
                "properties": {"video_id": {"type": "string"}},
                "required": ["video_id"],
            },
        },
        {
            "name": "summarize_video",
            "description": "Generate a summary of a video at short/medium/detailed level.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string"},
                    "level": {"type": "string", "enum": ["short", "medium", "detailed"], "default": "medium"},
                },
                "required": ["video_id"],
            },
        },
        {
            "name": "get_chapters",
            "description": "Auto-detect topic chapters from a video transcript.",
            "inputSchema": {
                "type": "object",
                "properties": {"video_id": {"type": "string"}},
                "required": ["video_id"],
            },
        },
        {
            "name": "extract_entities",
            "description": "Extract people, tools, concepts, and key terms from a video.",
            "inputSchema": {
                "type": "object",
                "properties": {"video_id": {"type": "string"}},
                "required": ["video_id"],
            },
        },
        {
            "name": "find_moment",
            "description": "Find when something specific happens in a video.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string"},
                    "query": {"type": "string", "description": "What to find, e.g. 'when does he show the architecture diagram'"},
                },
                "required": ["video_id", "query"],
            },
        },
        {
            "name": "ask_collection",
            "description": "Ask a question across ALL processed videos simultaneously.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "n_context": {"type": "integer", "default": 10},
                },
                "required": ["question"],
            },
        },
        {
            "name": "compare_videos",
            "description": "Compare content and approaches across two videos.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "video_id_1": {"type": "string"},
                    "video_id_2": {"type": "string"},
                },
                "required": ["video_id_1", "video_id_2"],
            },
        },
        {
            "name": "export_notes",
            "description": "Generate structured markdown notes for a video with summary, chapters, and entities.",
            "inputSchema": {
                "type": "object",
                "properties": {"video_id": {"type": "string"}},
                "required": ["video_id"],
            },
        },
        {
            "name": "delete_video",
            "description": "Remove a video from the store.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string"},
                    "delete_files": {"type": "boolean", "default": False},
                },
                "required": ["video_id"],
            },
        },
    ]

    def __init__(self, port: int, **kwargs):
        super().__init__(port=port, **kwargs)
        self._store = None
        self._processor = None
        self._analyzer = None
        self._oai = None

    def _get_store(self):
        if self._store is None:
            from video_store import VideoStore
            self._store = VideoStore(persist_dir=CHROMA_DIR, embedding_model=EMBEDDING_MODEL)
        return self._store

    def _get_processor(self):
        if self._processor is None:
            from video_processor import VideoProcessor
            self._processor = VideoProcessor(
                output_root=DATA_DIR,
                whisper_model=WHISPER_MODEL,
                vision_model=VISION_MODEL,
                embedding_model=EMBEDDING_MODEL,
            )
        return self._processor

    def _get_analyzer(self):
        if self._analyzer is None:
            from video_analysis import VideoAnalyzer
            self._analyzer = VideoAnalyzer(model=QA_MODEL, vision_model=VISION_MODEL)
        return self._analyzer

    def _get_oai(self):
        if self._oai is None:
            import openai
            self._oai = openai.OpenAI()
        return self._oai

    def _ok(self, data: Any) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps(data, indent=2, default=str)}]}

    def _err(self, msg: str) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps({"error": msg})}], "isError": True}

    async def _run_sync(self, fn, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))

    # -------------------------------------------------------------------------

    async def _tool_process_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        url = args.get("url")
        if not url:
            return self._err("url is required")

        def _run():
            proc = self._get_processor()
            proc.frame_interval_sec = args.get("frame_interval", 10.0)
            proc.max_frames = args.get("max_frames", 120)
            result = proc.process(
                url,
                describe_frames=True,
                generate_embeddings=True,
                chunk_duration_sec=30.0,
                do_ocr=args.get("enable_ocr", False),
                detect_scenes=True,
                screencast_mode=args.get("screencast_mode") if args.get("screencast_mode") != "auto" else None,
            )
            store = self._get_store()
            counts = store.ingest(result)
            scene_count = sum(1 for f in result.frames if f.is_scene_change)
            return {
                "status": "ok",
                "video_id": result.metadata.video_id,
                "title": result.metadata.title,
                "duration_fmt": _fmt_time(result.metadata.duration_sec),
                "channel": result.metadata.channel,
                "frames_extracted": len(result.frames),
                "scene_changes_detected": scene_count,
                "transcript_chunks": counts.get("transcript_chunks", 0),
                "transcript_length": len(result.full_transcript),
            }

        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_process_videos_batch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        urls = args.get("urls", [])
        if not urls:
            return self._err("urls list is required")

        max_workers = args.get("max_workers", 4)

        def _process_one(url: str) -> dict:
            try:
                proc = self._get_processor()
                result = proc.process(url, describe_frames=True, generate_embeddings=True,
                                      do_ocr=args.get("enable_ocr", False))
                store = self._get_store()
                counts = store.ingest(result)
                return {
                    "status": "ok", "url": url,
                    "video_id": result.metadata.video_id,
                    "title": result.metadata.title,
                    "duration_fmt": _fmt_time(result.metadata.duration_sec),
                    "transcript_chunks": counts.get("transcript_chunks", 0),
                }
            except Exception as e:
                return {"status": "error", "url": url, "error": str(e)}

        def _run():
            from concurrent.futures import ThreadPoolExecutor as TPE, as_completed
            results = []
            with TPE(max_workers=max_workers) as pool:
                futures = {pool.submit(_process_one, url): url for url in urls}
                for fut in as_completed(futures):
                    results.append(fut.result())
            results.sort(key=lambda r: urls.index(r["url"]))
            ok = [r for r in results if r["status"] == "ok"]
            errors = [r for r in results if r["status"] != "ok"]
            return {"total": len(urls), "succeeded": len(ok), "failed": len(errors), "results": results}

        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_list_videos(self, args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            store = self._get_store()
            videos = store.list_videos()
            for v in videos:
                if v.get("duration_sec"):
                    v["duration_fmt"] = _fmt_time(v["duration_sec"])
            return {"count": len(videos), "videos": videos}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_ask_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        question = args.get("question")
        if not question:
            return self._err("question is required")

        def _run():
            store = self._get_store()
            client = self._get_oai()
            video_id = args.get("video_id")
            n_context = args.get("n_context", 8)

            transcript_results = store.search_transcript(question, video_id=video_id, n_results=n_context)
            frame_results = store.search_frames(question, video_id=video_id, n_results=min(n_context, 5))

            context_parts = []
            if transcript_results:
                context_parts.append("=== TRANSCRIPT ===")
                for r in transcript_results:
                    meta = r.get("metadata", {})
                    t0, t1 = meta.get("start_sec", 0), meta.get("end_sec", 0)
                    context_parts.append(f"[{_fmt_time(t0)} → {_fmt_time(t1)}] {r['text']}")
            if frame_results:
                context_parts.append("\n=== FRAMES ===")
                for r in frame_results:
                    meta = r.get("metadata", {})
                    context_parts.append(f"[{_fmt_time(meta.get('timestamp_sec', 0))}] {r['text']}")

            messages = [
                {"role": "system", "content": "You are a video Q&A assistant. Answer based on the transcript and frame descriptions. Cite timestamps [MM:SS] when relevant."},
                {"role": "user", "content": f"Context:\n{chr(10).join(context_parts)}\n\nQuestion: {question}"},
            ]
            resp = client.chat.completions.create(model=QA_MODEL, messages=messages, max_completion_tokens=1500)
            return {"answer": resp.choices[0].message.content.strip(), "video_id": video_id}

        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_search_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args.get("query")
        if not query:
            return self._err("query is required")
        def _run():
            store = self._get_store()
            video_id = args.get("video_id")
            n = args.get("n_results", 10)
            t = args.get("search_type", "all")
            if t == "transcript":
                return {"transcript": store.search_transcript(query, video_id, n)}
            elif t == "frames":
                return {"frames": store.search_frames(query, video_id, n)}
            return store.search_all(query, video_id, n)
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_get_transcript(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            store = self._get_store()
            text = store.get_video_transcript(video_id)
            if not text:
                raise ValueError(f"No transcript found for {video_id}")
            return {"video_id": video_id, "transcript": text}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_summarize_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            info = store.get_video_info(video_id)
            if not info:
                raise ValueError(f"Video {video_id} not found")
            output_dir = os.path.join(DATA_DIR, video_id)
            result = analyzer.generate_summary(
                info.get("full_transcript", ""), info.get("title", video_id),
                output_dir, args.get("level", "medium")
            )
            result["title"] = info.get("title", video_id)
            return result
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_get_chapters(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            info = store.get_video_info(video_id)
            if not info:
                raise ValueError(f"Video {video_id} not found")
            chunks = store.get_all_chunks(video_id)
            chapters = analyzer.detect_chapters(
                info.get("full_transcript", ""), info.get("title", video_id),
                os.path.join(DATA_DIR, video_id), chunks
            )
            for ch in chapters:
                if "start_sec" in ch:
                    ch["start_fmt"] = _fmt_time(ch["start_sec"])
            return {"video_id": video_id, "title": info.get("title"), "chapters": chapters}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_extract_entities(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            info = store.get_video_info(video_id)
            if not info:
                raise ValueError(f"Video {video_id} not found")
            entities = analyzer.extract_entities(
                info.get("full_transcript", ""), info.get("title", video_id),
                os.path.join(DATA_DIR, video_id)
            )
            return {"video_id": video_id, **entities}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_find_moment(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        query = args.get("query")
        if not video_id or not query:
            return self._err("video_id and query are required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            chunks = store.get_all_chunks(video_id)
            frames = store.get_all_frames(video_id)
            moments = analyzer.find_moment(query, chunks, frames)
            return {"video_id": video_id, "query": query, "moments": moments}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_ask_collection(self, args: Dict[str, Any]) -> Dict[str, Any]:
        question = args.get("question")
        if not question:
            return self._err("question is required")
        def _run():
            store = self._get_store()
            client = self._get_oai()
            n = args.get("n_context", 10)
            transcript_results = store.search_transcript(question, n_results=n)
            frame_results = store.search_frames(question, n_results=min(n, 5))

            by_video: dict = {}
            for r in transcript_results:
                vid = r.get("metadata", {}).get("video_id", "unknown")
                by_video.setdefault(vid, []).append(r)

            context_parts = []
            for vid, results in by_video.items():
                info = store.get_video_info(vid)
                title = info.get("title", vid) if info else vid
                context_parts.append(f"\n=== {title} ({vid}) ===")
                for r in results:
                    meta = r.get("metadata", {})
                    context_parts.append(f"[{_fmt_time(meta.get('start_sec',0))} → {_fmt_time(meta.get('end_sec',0))}] {r['text']}")

            messages = [
                {"role": "system", "content": "You are a multi-video Q&A assistant. Cite which video and timestamps when relevant."},
                {"role": "user", "content": f"Context:\n{''.join(context_parts)}\n\nQuestion: {question}"},
            ]
            resp = client.chat.completions.create(model=QA_MODEL, messages=messages, max_completion_tokens=1500)
            return {"answer": resp.choices[0].message.content.strip(), "videos_searched": list(by_video.keys())}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_compare_videos(self, args: Dict[str, Any]) -> Dict[str, Any]:
        v1, v2 = args.get("video_id_1"), args.get("video_id_2")
        if not v1 or not v2:
            return self._err("video_id_1 and video_id_2 are required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            info1 = store.get_video_info(v1)
            info2 = store.get_video_info(v2)
            if not info1:
                raise ValueError(f"Video {v1} not found")
            if not info2:
                raise ValueError(f"Video {v2} not found")
            comparison = analyzer.compare_videos(
                {"title": info1.get("title", ""), "transcript": info1.get("full_transcript", "")},
                {"title": info2.get("title", ""), "transcript": info2.get("full_transcript", "")},
            )
            return {"video_1": {"id": v1, "title": info1.get("title")},
                    "video_2": {"id": v2, "title": info2.get("title")},
                    **comparison}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_export_notes(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            store = self._get_store()
            analyzer = self._get_analyzer()
            info = store.get_video_info(video_id)
            if not info:
                raise ValueError(f"Video {video_id} not found")
            title = info.get("title", video_id)
            transcript = info.get("full_transcript", "")
            out_dir = os.path.join(DATA_DIR, video_id)
            chunks = store.get_all_chunks(video_id)
            summary_data = analyzer.generate_summary(transcript, title, out_dir, "medium")
            chapters = analyzer.detect_chapters(transcript, title, out_dir, chunks)
            entities = analyzer.extract_entities(transcript, title, out_dir)
            notes = analyzer.generate_notes(title, transcript, chapters, entities,
                                             summary=summary_data.get("summary", ""))
            notes_path = os.path.join(out_dir, f"{video_id}_notes.md")
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            with open(notes_path, "w") as f:
                f.write(notes)
            return {"video_id": video_id, "saved_to": notes_path, "notes": notes}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))

    async def _tool_delete_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_id = args.get("video_id")
        if not video_id:
            return self._err("video_id is required")
        def _run():
            import shutil
            store = self._get_store()
            counts = store.delete_video(video_id)
            if args.get("delete_files"):
                out_dir = os.path.join(DATA_DIR, video_id)
                if os.path.isdir(out_dir):
                    shutil.rmtree(out_dir, ignore_errors=True)
                    counts["files_deleted"] = True
            return {"status": "deleted", "video_id": video_id, **counts}
        try:
            return self._ok(await self._run_sync(_run))
        except Exception as e:
            return self._err(str(e))


def create_server(port: Optional[int] = None) -> DistributedVideoQAMCP:
    port = port or int(os.environ.get("PORT", "4005"))
    return DistributedVideoQAMCP(port=port)


# module-level ASGI app for Modal @asgi_app import
app = create_server().app


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Video Q&A MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4005")))
    args = parser.parse_args()
    server = create_server(args.port)
    server.run()
