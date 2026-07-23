#!/usr/bin/env python3
"""Ingest every video in a YouTube playlist into the distributed-video-qa server.

Enumerates the playlist with yt-dlp, then calls process_video one at a time over
the server's JSON-RPC /mcp endpoint. Resilient to the native-MCP manager's
health-check auto-restart (which can drop a long in-flight request): downloaded
audio + frames are cached on disk, so a retry skips straight to embedding/store.
Already-stored videos are skipped.

Usage:
  python scripts/ingest_playlist.py "https://www.youtube.com/playlist?list=..."
  python scripts/ingest_playlist.py <url> --max-frames 8 --frame-interval 120
  VIDEO_QA_URL=http://localhost:4005 python scripts/ingest_playlist.py <url>
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

import httpx

VIDEO_QA = os.getenv("VIDEO_QA_URL", "http://localhost:4005").rstrip("/")


def enumerate_playlist(url: str) -> List[Dict[str, str]]:
    out = subprocess.run(
        ["yt-dlp", "--flat-playlist", "--print", "%(id)s\t%(title)s", url],
        capture_output=True, text=True, check=True,
    ).stdout
    videos = []
    for line in out.splitlines():
        if "\t" in line:
            vid, title = line.split("\t", 1)
            videos.append({"id": vid.strip(), "title": title.strip()})
    return videos


def _call(client: httpx.Client, tool: str, arguments: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    r = client.post(
        f"{VIDEO_QA}/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": tool, "arguments": arguments}},
        timeout=timeout,
    )
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise RuntimeError(body["error"].get("message", str(body["error"])))
    result = body.get("result", {})
    text = (result.get("content") or [{}])[0].get("text", "{}")
    try:
        payload = json.loads(text)
    except Exception:
        payload = {"raw": text}
    if result.get("isError"):
        raise RuntimeError(payload.get("error", text))
    return payload


def stored_ids(client: httpx.Client) -> set:
    try:
        data = _call(client, "list_videos", {}, timeout=30)
        return {v.get("video_id") for v in data.get("videos", [])}
    except Exception:
        return set()


def wait_healthy(client: httpx.Client, attempts: int = 30) -> bool:
    for _ in range(attempts):
        try:
            h = client.get(f"{VIDEO_QA}/health", timeout=8).json()
            if h.get("status") == "healthy":
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def process_one(client: httpx.Client, vid: str, title: str, frames: int,
                interval: float, ocr: bool, timeout: float, retries: int) -> Dict[str, Any]:
    url = f"https://www.youtube.com/watch?v={vid}"
    last_err: Optional[str] = None
    for _attempt in range(1, retries + 2):
        try:
            payload = _call(client, "process_video",
                            {"url": url, "max_frames": frames,
                             "frame_interval": interval, "enable_ocr": ocr},
                            timeout=timeout)
            return {"id": vid, "title": title, "ok": True,
                    "chunks": payload.get("transcript_chunks"),
                    "frames": payload.get("frames_extracted")}
        except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError,
                httpx.ReadTimeout) as e:
            # Server likely restarted mid-request; cached assets make retry cheap.
            last_err = f"{type(e).__name__}: {e}"
            wait_healthy(client)
            time.sleep(3)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(2)
    return {"id": vid, "title": title, "ok": False, "error": last_err}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("playlist")
    ap.add_argument("--max-frames", type=int, default=8)
    ap.add_argument("--frame-interval", type=float, default=120.0)
    ap.add_argument("--ocr", action="store_true")
    ap.add_argument("--timeout", type=float, default=1200.0)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--limit", type=int, default=0, help="process at most N (0 = all)")
    args = ap.parse_args()

    videos = enumerate_playlist(args.playlist)
    if args.limit:
        videos = videos[: args.limit]
    print(f"Video-QA: {VIDEO_QA}")
    print(f"Playlist: {len(videos)} videos\n", flush=True)

    with httpx.Client() as client:
        if not wait_healthy(client):
            print("Server not healthy; aborting.")
            sys.exit(1)
        done = stored_ids(client)
        results = []
        for i, v in enumerate(videos, 1):
            if v["id"] in done:
                print(f"[{i}/{len(videos)}] SKIP (already stored) {v['title']}", flush=True)
                results.append({"id": v["id"], "title": v["title"], "ok": True, "skipped": True})
                continue
            print(f"[{i}/{len(videos)}] processing {v['title']} ...", flush=True)
            t0 = time.time()
            res = process_one(client, v["id"], v["title"], args.max_frames,
                               args.frame_interval, args.ocr, args.timeout, args.retries)
            res["secs"] = round(time.time() - t0, 1)
            results.append(res)
            tag = "ok " if res["ok"] else "FAIL"
            extra = f"chunks={res.get('chunks')}" if res["ok"] else res.get("error", "")
            print(f"    [{tag}] {res['secs']}s {extra}", flush=True)

    ok = sum(1 for r in results if r["ok"])
    print(f"\n=== {ok}/{len(results)} ingested ===")
    for r in results:
        if not r["ok"]:
            print(f"  FAIL {r['title']}: {r.get('error')}")


if __name__ == "__main__":
    main()
