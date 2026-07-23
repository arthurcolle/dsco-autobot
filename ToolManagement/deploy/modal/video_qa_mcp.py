"""
Modal deployment for Distributed Video Q&A MCP.

Deploy with:
    modal deploy deploy/modal/video_qa_mcp.py
"""

import modal
from pathlib import Path

app = modal.App("distributed-video-qa")

SRC_DIR = Path(__file__).parent.parent.parent / "src"
NATIVE_DIR = Path(__file__).parent.parent.parent / "native"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "git", "nodejs", "npm", "curl", "ca-certificates")
    .pip_install(
        "bgutil-ytdlp-pot-provider==1.3.1",
        "fastapi[standard]==0.115.0",
        "uvicorn[standard]==0.32.0",
        "pydantic==2.9.2",
        "httpx==0.27.2",
        "numpy>=1.24.0",
        "openai>=1.0.0",
        "yt-dlp>=2025.1.1",
        "openai-whisper",
        "chromadb>=0.4.0",
        "Pillow>=10.0.0",
    )
    .run_commands(
        "npm install -g n && n 20 && /usr/local/bin/node -v && /usr/local/bin/npm -v",
        # Deno is the JS runtime yt-dlp's EJS uses to solve YouTube's nsig
        # challenge. Without it, format URLs are stripped (SABR-only) and
        # downloads fail even with valid PO tokens + cookies.
        "curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh -s -- -y || "
        "(curl -fsSL https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip -o /tmp/deno.zip && "
        "apt-get update && apt-get install -y unzip && unzip -o /tmp/deno.zip -d /usr/local/bin)",
        "/usr/local/bin/deno --version || deno --version",
        "git clone --single-branch --branch 1.3.1 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil-ytdlp-pot-provider",
        "cd /opt/bgutil-ytdlp-pot-provider/server && /usr/local/bin/npm ci && /usr/local/bin/npx tsc",
    )
    .add_local_dir(SRC_DIR, remote_path="/root/src")
    .add_local_dir(NATIVE_DIR, remote_path="/root/native")
)

video_volume = modal.Volume.from_name("video-qa-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/data": video_volume},
    timeout=600,
    min_containers=0,
    max_containers=5,
    buffer_containers=0,
    scaledown_window=60,
    secrets=[
        modal.Secret.from_name("openai-secret"),
        modal.Secret.from_name("youtube-cookies-firefox-v2"),
    ],
)
@modal.concurrent(max_inputs=10, target_inputs=2)
@modal.asgi_app()
def web():
    """Serve the Distributed Video Q&A MCP."""
    import sys
    import os

    sys.path.insert(0, "/root/src")
    sys.path.insert(0, "/root")

    os.environ["PORT"] = "8000"
    os.environ["REGISTRY_URL"] = "https://tools.distributed.systems"
    os.environ["SERVICE_URL"] = "https://arthurcolle--distributed-video-qa-web.modal.run"
    os.environ["VIDEO_QA_DATA_DIR"] = "/data/videos"
    os.environ["VIDEO_DATA_DIR"] = "/data/videos"
    os.environ["VIDEO_CHROMA_DIR"] = "/data/chroma"
    os.environ["VIDEO_QA_PATH"] = "/root/native/distributed_video_qa/video_to"
    # Make Deno (yt-dlp EJS nsig solver) discoverable.
    os.environ["PATH"] = "/usr/local/bin:" + os.environ.get("PATH", "")

    # Materialize YouTube cookies from the secret so yt-dlp can pass the
    # datacenter-IP bot check. Secret stays out of the image/git/logs.
    _b64 = os.environ.get("YT_COOKIES_B64", "")
    if _b64:
        import base64
        try:
            _data = base64.b64decode(_b64)
            if os.environ.get("YT_COOKIES_GZIP") == "1":
                import gzip
                _data = gzip.decompress(_data)
            _cookie_path = "/tmp/yt_cookies.txt"
            with open(_cookie_path, "wb") as _cf:
                _cf.write(_data)
            os.environ["YTDLP_COOKIES"] = _cookie_path
            print(f"[video-qa] YT cookies materialized: {len(_data)} bytes")
        except Exception as _e:  # pragma: no cover
            print(f"[video-qa] failed to materialize YT cookies: {_e}")

    # Start the bgutil POT (proof-of-origin token) HTTP server. YouTube
    # enforces PO tokens for datacenter IPs (Modal); without it, every
    # player_client returns an empty format set. yt-dlp auto-discovers the
    # provider on http://127.0.0.1:4416 via the installed plugin.
    try:
        import subprocess as _sp
        import time as _t
        _node = "/usr/local/bin/node" if os.path.exists("/usr/local/bin/node") else "node"
        _pot = _sp.Popen(
            [_node, "/opt/bgutil-ytdlp-pot-provider/server/build/main.js", "--port", "4416"],
            stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
        )
        os.environ["YTDLP_POT_BASE_URL"] = "http://127.0.0.1:4416"
        # give the server a moment to bind
        for _ in range(20):
            try:
                import urllib.request as _u
                _u.urlopen("http://127.0.0.1:4416/ping", timeout=1)
                print("[video-qa] bgutil POT server is up on :4416")
                break
            except Exception:
                _t.sleep(0.5)
        else:
            print("[video-qa] WARNING: bgutil POT server did not respond on :4416")
    except Exception as _e:
        print(f"[video-qa] failed to start bgutil POT server: {_e}")

    from native.distributed_video_qa.server import app
    return app
