from __future__ import annotations

"""
Modal deployment artifact that provisions a self-hosted Neo4j instance with a FastAPI probe
layer. The image installs Neo4j inside the container, keeps data in a Modal Volume, and
exposes health/probe/query endpoints for upstream orchestration services.
"""

import asyncio
import inspect
from datetime import date, datetime, time as time_, timedelta
from decimal import Decimal
import logging
from logging.handlers import RotatingFileHandler
import os
import shutil
import subprocess
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator
import sys

try:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    SRC_PATH = REPO_ROOT / "src"
except IndexError:
    # Inside Modal container: file is at /root/graph_management_image.py
    REPO_ROOT = Path("/root")
    SRC_PATH = REPO_ROOT
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.graph import Node as Neo4jNode, Relationship as Neo4jRelationship, Path as Neo4jPath
from neo4j.time import DateTime as Neo4jDateTime, Date as Neo4jDate, Time as Neo4jTime, Duration as Neo4jDuration
import modal
from modal.mount import Mount

_GRAPH_MANAGEMENT_DIR = SRC_PATH / "graph_management"


def _graph_management_mount() -> Mount:
    """Create a mount for the local graph_management package across Modal versions."""
    factory = getattr(Mount, "from_local_dir", None) or getattr(Mount, "_from_local_dir", None)
    if factory is None:
        msg = "Modal Mount helpers are unavailable; upgrade/downgrade the Modal client."
        raise AttributeError(msg)
    return factory(_GRAPH_MANAGEMENT_DIR, remote_path="/root/graph_management")


_FUNCTION_ACCEPTS_MOUNTS = "mounts" in inspect.signature(modal.App.function).parameters
if _FUNCTION_ACCEPTS_MOUNTS:
    graph_management_mount = _graph_management_mount()
else:
    graph_management_mount = None

# These imports will be available at module load time from the image
from graph_management.schemas import (
    QueryRequest,
    QueryResponse,
    SingleResultResponse,
    TransactionRequest,
    SchemaResponse as TypedSchemaResponse,
    IndexListResponse as TypedIndexListResponse,
    IndexInfo,
    ConstraintListResponse as TypedConstraintListResponse,
    ConstraintInfo,
)

APP_NAME = "graph-management-neo4j"
NEO4J_DATA_PATH = "/data/neo4j"
NEO4J_BOLT_PORT = 7687
NEO4J_HTTP_PORT = 7474
NEO4J_USER = "neo4j"
DEFAULT_PASSWORD = os.getenv("DEFAULT_NEO4J_PASSWORD_DEV", "Backstab2025!")
NEO4J_CONF_DIR = f"{NEO4J_DATA_PATH}/conf"
NEO4J_CONF_FILE = f"{NEO4J_CONF_DIR}/neo4j.conf"
SYSTEM_CONF_PATH = Path("/etc/neo4j/neo4j.conf")

CONFIG_OVERRIDES = {
    "server.default_listen_address": "0.0.0.0",
    "server.bolt.listen_address": f"0.0.0.0:{NEO4J_BOLT_PORT}",
    "server.http.listen_address": f"0.0.0.0:{NEO4J_HTTP_PORT}",
    "server.directories.data": f"{NEO4J_DATA_PATH}/data",
    "server.directories.logs": f"{NEO4J_DATA_PATH}/logs",
    "server.directories.import": f"{NEO4J_DATA_PATH}/import",
    "server.directories.plugins": f"{NEO4J_DATA_PATH}/plugins",
    "initial.dbms.default_database": "neo4j",
    "dbms.security.auth_enabled": "true",
}

# Persistent storage to keep graph data between container restarts.
volume = modal.Volume.from_name("graph-management-neo4j-data", create_if_missing=True)

# Modal image with Java runtime and Neo4j installed. FastAPI/uvicorn/neo4j driver included
# for the probe layer.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "wget",
        "gnupg",
        "curl",
        "ca-certificates",
    )
    .run_commands(
        "mkdir -p /etc/apt/keyrings",
        "echo 'deb http://deb.debian.org/debian bookworm-backports main' > /etc/apt/sources.list.d/backports.list",
        "wget -O /etc/apt/keyrings/adoptium.asc https://packages.adoptium.net/artifactory/api/gpg/key/public",
        "echo 'deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb bookworm main' > /etc/apt/sources.list.d/adoptium.list",
        "wget -O /etc/apt/keyrings/neo4j.asc https://debian.neo4j.com/neotechnology.gpg.key",
        "echo 'deb [signed-by=/etc/apt/keyrings/neo4j.asc] https://debian.neo4j.com stable latest' > /etc/apt/sources.list.d/neo4j.list",
        "apt-get update",
        "DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends temurin-21-jre neo4j cypher-shell",
        "rm -rf /var/lib/apt/lists/*",
    )
    .pip_install("fastapi[standard]", "uvicorn", "neo4j")
    .run_commands("mkdir -p /root/graph_management")
    .add_local_dir(_GRAPH_MANAGEMENT_DIR, remote_path="/root/graph_management")
    .add_local_file(
        str(_GRAPH_MANAGEMENT_DIR / "schemas.py"),
        "/root/graph_management/schemas.py"
    )
    .add_local_file(
        str(_GRAPH_MANAGEMENT_DIR / "__init__.py"),
        "/root/graph_management/__init__.py"
    )
)

app = modal.App(APP_NAME, image=image)

LOG_LEVEL = os.getenv("NEO4J_LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [neo4j-bootstrap] %(message)s",
)
logger = logging.getLogger("graph-management.neo4j")


def _setup_file_logging() -> None:
    """Attach a rotating file handler that persists bootstrap logs to the Modal volume.

    This writes to `/data/neo4j/logs/bootstrap.log` by default and rotates at ~5MB
    with up to 3 backups. The handler is added only once per process.
    """
    try:
        logs_dir = Path(f"{NEO4J_DATA_PATH}/logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / os.getenv("NEO4J_BOOTSTRAP_LOG", "bootstrap.log")

        # Avoid adding duplicate handlers across repeated calls.
        for h in logger.handlers:
            if isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == str(log_file):
                return

        file_handler = RotatingFileHandler(
            str(log_file), maxBytes=int(os.getenv("NEO4J_BOOTSTRAP_LOG_MAX_BYTES", "5242880")), backupCount=int(os.getenv("NEO4J_BOOTSTRAP_LOG_BACKUPS", "3"))
        )
        file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [neo4j-bootstrap] %(message)s"))
        logger.addHandler(file_handler)
        logger.info("File logging enabled at %s", log_file)
    except Exception:  # pragma: no cover — logging setup issues should not break startup
        # Fall back silently to stdout-only logging
        pass


def _log_tail_of_neo4j_logs(max_lines: int = 100) -> None:
    """Log the tail of Neo4j log files to aid debugging startup failures."""
    logs_path = Path(f"{NEO4J_DATA_PATH}/logs")
    if not logs_path.exists():
        logger.warning("Neo4j logs directory %s does not exist yet", logs_path)
        return
    log_files = sorted(logs_path.glob("*.log"))
    if not log_files:
        logger.warning("No Neo4j log files found in %s", logs_path)
        return
    for fp in log_files:
        try:
            contents = fp.read_text(errors="replace").splitlines()
            tail = contents[-max_lines:]
            logger.warning("--- Tail of %s (last %d lines) ---", fp, len(tail))
            for line in tail:
                logger.warning("%s", line)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed reading %s: %s", fp, exc)

JAVA_21_CANDIDATES = [
    Path("/usr/lib/jvm/temurin-21-jre-amd64"),
    Path("/usr/lib/jvm/temurin-21-jre"),
]


def _neo4j_password() -> str:
    pw = os.getenv("NEO4J_PASSWORD")
    if pw:
        return pw
    # Allow a clearly-marked dev fallback for local iteration only.
    if os.getenv("ALLOW_DEV_DEFAULT_PASSWORD") == "1":
        return DEFAULT_PASSWORD
    # If no secret provided and dev fallback not explicitly allowed, raise clearly.
    raise RuntimeError(
        "NEO4J_PASSWORD is not set. Provide via Modal Secret or set ALLOW_DEV_DEFAULT_PASSWORD=1 for local dev."
    )


def _resolve_java_home() -> str | None:
    env_java = os.getenv("JAVA_HOME")
    if env_java and Path(env_java).exists():
        return env_java

    for candidate in JAVA_21_CANDIDATES:
        if candidate.exists():
            return str(candidate)

    jvm_root = Path("/usr/lib/jvm")
    if jvm_root.exists():
        matches = sorted(jvm_root.glob("temurin-21-jre*"))
        if matches:
            return str(matches[0])
    return None


def _write_config(config_path: Path, config_overrides: dict[str, str]) -> None:
    if not config_path.exists():
        raise FileNotFoundError(f"Expected Neo4j config at {config_path}")

    existing_lines = config_path.read_text().splitlines()
    override_prefixes = tuple(f"{key}=" for key in config_overrides)

    sanitized_lines: list[str] = []
    for line in existing_lines:
        if line.startswith("# GraphManagement overrides"):
            continue
        if line.startswith(override_prefixes):
            continue
        sanitized_lines.append(line)

    if sanitized_lines and sanitized_lines[-1] != "":
        sanitized_lines.append("")
    sanitized_lines.append("# GraphManagement overrides (managed by GraphManagement)")
    for key, value in config_overrides.items():
        sanitized_lines.append(f"{key}={value}")

    config_path.write_text("\n".join(sanitized_lines) + "\n")


def _ensure_dirs() -> None:
    for subdir in ("data", "logs", "import", "plugins"):
        path = f"{NEO4J_DATA_PATH}/{subdir}"
        os.makedirs(path, exist_ok=True)
        logger.info("Ensured directory exists: %s", path)
    os.makedirs(NEO4J_CONF_DIR, exist_ok=True)
    logger.info("Ensured directory exists: %s", NEO4J_CONF_DIR)


def _prepare_config_env() -> dict[str, str]:
    config_path = Path(NEO4J_CONF_FILE)
    if not config_path.exists():
        if SYSTEM_CONF_PATH.exists():
            shutil.copy(SYSTEM_CONF_PATH, config_path)
            logger.info("Copied system neo4j.conf to %s", config_path)
        else:
            config_path.write_text("# GraphManagement base config\n")
            logger.info("Created new base neo4j.conf at %s", config_path)
    _write_config(config_path, CONFIG_OVERRIDES)
    logger.info("Applied config overrides to %s", config_path)
    env = os.environ.copy()
    env["NEO4J_CONF"] = NEO4J_CONF_DIR
    env["NEO4J_HOME"] = NEO4J_DATA_PATH
    logger.info("Prepared Neo4j env with NEO4J_CONF=%s", NEO4J_CONF_DIR)
    logger.info("Prepared Neo4j env with NEO4J_HOME=%s", NEO4J_DATA_PATH)

    java_home = _resolve_java_home()
    if java_home:
        env["JAVA_HOME"] = java_home
        env["PATH"] = f"{java_home}/bin:" + env["PATH"]
        logger.info("Using JAVA_HOME=%s", java_home)
    else:
        logger.warning("Temurin 21 JRE not found; falling back to system Java runtime")

    return env


def start_neo4j() -> None:
    """Install configuration overrides, ensure directories, and launch the Neo4j process."""
    _setup_file_logging()
    logger.info("Starting Neo4j bootstrap sequence")
    _ensure_dirs()

    neo4j_env = _prepare_config_env()

    password_cmd = ["neo4j-admin", "dbms", "set-initial-password", _neo4j_password()]
    logger.info("Setting initial Neo4j password via `%s`", " ".join(password_cmd[:-1] + ["****"]))
    pw_result = subprocess.run(password_cmd, check=False, env=neo4j_env, capture_output=True)
    if pw_result.returncode == 0:
        logger.info("Initial password set command completed successfully")
    else:
        stdout = pw_result.stdout.decode("utf-8", errors="replace") if pw_result.stdout else ""
        stderr = pw_result.stderr.decode("utf-8", errors="replace") if pw_result.stderr else ""
        logger.warning(
            "Initial password set command exited with %s\nstdout: %s\nstderr: %s",
            pw_result.returncode,
            stdout.strip(),
            stderr.strip(),
        )

    neo4j_log_path = Path(f"{NEO4J_DATA_PATH}/logs/neo4j-console.log")
    neo4j_log_file = open(neo4j_log_path, "w")
    subprocess.Popen(
        ["neo4j", "console"],
        stdout=neo4j_log_file,
        stderr=subprocess.STDOUT,
        env=neo4j_env,
    )
    logger.info("Launched `neo4j console` process (logging to %s)", neo4j_log_path)

    _wait_for_ready(neo4j_env)
    logger.info("Neo4j bootstrap sequence complete")


def _wait_for_ready(env: dict[str, str]) -> None:
    """Poll until cypher-shell can connect to Neo4j or the boot timeout expires."""
    timeout_seconds = int(os.getenv("NEO4J_BOOT_TIMEOUT_SECONDS", "120"))
    poll_interval = float(os.getenv("NEO4J_BOOT_POLL_INTERVAL_SECONDS", "2"))
    deadline = time.time() + timeout_seconds
    query = [
        "cypher-shell",
        "-a",
        f"neo4j://localhost:{NEO4J_BOLT_PORT}",
        "-u",
        NEO4J_USER,
        "-p",
        _neo4j_password(),
        "RETURN 1;",
    ]
    last_error: str | None = None

    logger.info(
        "Waiting for Neo4j readiness (timeout=%ss, poll_interval=%ss)", timeout_seconds, poll_interval
    )

    attempt = 0
    while time.time() < deadline:
        try:
            attempt += 1
            logger.info("Readiness check attempt %s: running cypher-shell", attempt)
            result = subprocess.run(query, capture_output=True, timeout=5, env=env)
        except subprocess.TimeoutExpired:
            last_error = "cypher-shell timed out after 5s"
            logger.warning("Readiness check attempt %s timed out after 5s", attempt)
        else:
            if result.returncode == 0:
                logger.info("Readiness check attempt %s succeeded", attempt)
                return
            stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
            stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
            combined = "\n".join(part for part in (stdout.strip(), stderr.strip()) if part)
            last_error = combined or f"cypher-shell exited with {result.returncode}"
            logger.warning(
                "Readiness check attempt %s failed (code=%s). Details:\n%s",
                attempt,
                result.returncode,
                last_error,
            )
        time.sleep(poll_interval)

    error_detail = f" Last attempt: {last_error}" if last_error else ""
    # Log neo4j console output for debugging
    console_log = Path(f"{NEO4J_DATA_PATH}/logs/neo4j-console.log")
    if console_log.exists():
        logger.error("Neo4j console output:\n%s", console_log.read_text(errors="replace")[-5000:])
    # Check if neo4j process is still alive
    import shutil as _shutil
    neo4j_bin = _shutil.which("neo4j")
    ps_result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
    java_procs = [l for l in ps_result.stdout.splitlines() if "java" in l.lower() or "neo4j" in l.lower()]
    logger.error("Java/Neo4j processes: %s", java_procs or "NONE FOUND")
    try:
        _log_tail_of_neo4j_logs()
    except Exception:
        pass
    logger.error(
        "Neo4j failed to start within %ss of waiting. Last-known issue: %s",
        timeout_seconds,
        last_error or "No additional detail",
    )
    raise RuntimeError(f"Neo4j failed to start within {timeout_seconds}s.{error_detail}")


def _driver_uri() -> str:
    return f"neo4j://localhost:{NEO4J_BOLT_PORT}"


def get_driver() -> AsyncDriver:
    return AsyncGraphDatabase.driver(_driver_uri(), auth=(NEO4J_USER, _neo4j_password()))


_driver: AsyncDriver | None = None


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    global _driver
    driver = get_driver()
    _driver = driver
    try:
        yield
    finally:
        await driver.close()
        _driver = None


fastapi_app = FastAPI(lifespan=lifespan)

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def driver_dep() -> AsyncDriver:
    if _driver is not None:
        return _driver
    return get_driver()


@fastapi_app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# -------------------------
# Serialization Utilities
# -------------------------

try:  # Spatial support is optional in some driver builds
    from neo4j.spatial import Point as Neo4jPoint
except Exception:  # pragma: no cover
    Neo4jPoint = None


def _serialize_value(val):
    if isinstance(val, (datetime, Neo4jDateTime)):
        return val.isoformat()
    if isinstance(val, (date, Neo4jDate)):
        return val.isoformat()
    if isinstance(val, (time_, Neo4jTime)):
        return val.isoformat()
    if isinstance(val, timedelta):
        return {
            "seconds": val.total_seconds(),
            "iso": f"PT{val.total_seconds()}S",
        }
    if isinstance(val, Neo4jDuration):
        return {
            "months": val.months,
            "days": val.days,
            "seconds": val.seconds,
            "nanoseconds": val.nanoseconds,
            "iso": val.iso_format(),
        }
    if Neo4jPoint is not None and isinstance(val, Neo4jPoint):
        return {
            "srid": getattr(val, "srid", None),
            "coordinates": [float(coord) for coord in val],
        }
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, Neo4jNode):
        # Prefer element_id if available for stability
        node = {
            "id": getattr(val, "id", None),
            "element_id": getattr(val, "element_id", None),
            "labels": list(getattr(val, "labels", []) or []),
            "properties": {k: _serialize_value(v) for k, v in dict(val).items()},
        }
        return node
    if isinstance(val, Neo4jRelationship):
        start_node = getattr(val, "start_node", None)
        end_node = getattr(val, "end_node", None)
        rel = {
            "id": getattr(val, "id", None),
            "element_id": getattr(val, "element_id", None),
            "type": getattr(val, "type", None),
            "start_node_id": getattr(start_node, "id", None) if start_node is not None else None,
            "start_element_id": getattr(start_node, "element_id", None) if start_node is not None else None,
            "end_node_id": getattr(end_node, "id", None) if end_node is not None else None,
            "end_element_id": getattr(end_node, "element_id", None) if end_node is not None else None,
            "properties": {k: _serialize_value(v) for k, v in dict(val).items()},
        }
        return rel
    if isinstance(val, Neo4jPath):
        return {
            "nodes": [_serialize_value(n) for n in getattr(val, "nodes", [])],
            "relationships": [_serialize_value(r) for r in getattr(val, "relationships", [])],
        }
    if isinstance(val, tuple):
        return [_serialize_value(v) for v in val]
    if isinstance(val, list):
        return [_serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: _serialize_value(v) for k, v in val.items()}
    return val


def _serialize_record(record) -> dict:
    try:
        data = record.data()  # neo4j.Record
    except AttributeError:
        # In tests, we often pass plain dicts
        data = dict(record)
    return {k: _serialize_value(v) for k, v in data.items()}


@fastapi_app.get("/probe")
async def probe(driver: AsyncDriver = Depends(driver_dep)) -> dict[str, int]:
    try:
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
            return {"ok": int(record["ok"])}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@fastapi_app.get("/logs/bootstrap")
async def bootstrap_logs(n: int = 200) -> dict[str, list[str]]:
    """Return the last N lines of the bootstrap log from the Modal volume."""
    try:
        log_file = Path(f"{NEO4J_DATA_PATH}/logs") / os.getenv("NEO4J_BOOTSTRAP_LOG", "bootstrap.log")
        if not log_file.exists():
            raise HTTPException(status_code=404, detail=f"Log file not found: {log_file}")
        lines = log_file.read_text(errors="replace").splitlines()
        tail = lines[-max(n, 0):]
        return {"lines": tail}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# -------------------------
# Streaming: SSE and WebSocket
# -------------------------

import json as _json
from starlette.responses import StreamingResponse


@fastapi_app.post("/v1/query/stream")
async def v1_query_stream(req: QueryRequest, driver: AsyncDriver = Depends(driver_dep)):
    async def event_stream():
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def heartbeat_task():
            try:
                while True:
                    await asyncio.sleep(10)
                    await queue.put(": ping\n\n")
            except asyncio.CancelledError:
                return

        async def producer():
            try:
                async with driver.session() as session:
                    res = await session.run(req.query, req.parameters)
                    first_sent = False
                    async for record in res:
                        payload = _json.dumps(_serialize_record(record))
                        await queue.put(f"event: record\n")
                        await queue.put(f"data: {payload}\n\n")
                        first_sent = True
                        if req.strategy == "one":
                            break
                    # end
                    await queue.put("event: end\n")
                    await queue.put("data: {}\n\n")
            except Exception as exc:
                err = _json.dumps({"error": str(exc)})
                await queue.put("event: error\n")
                await queue.put(f"data: {err}\n\n")
            finally:
                await queue.put(None)

        hb = asyncio.create_task(heartbeat_task())
        prod = asyncio.create_task(producer())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            for t in (hb, prod):
                t.cancel()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # for some proxies
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@fastapi_app.websocket("/v1/query/ws")
async def v1_query_ws(ws: WebSocket, driver: AsyncDriver = Depends(driver_dep)):
    await ws.accept()
    try:
        msg = await ws.receive_json()
        # accept both {query, parameters, strategy} and wrapped {type: run, ...}
        payload = msg.get("data") if isinstance(msg, dict) and "data" in msg else msg
        q = payload.get("query")
        params = payload.get("parameters") or {}
        strategy = payload.get("strategy", "all")
        if not q:
            await ws.send_json({"type": "error", "error": "query is required"})
            await ws.close(code=1003)
            return

        async def send_heartbeat():
            try:
                while True:
                    await asyncio.sleep(10)
                    await ws.send_json({"type": "heartbeat"})
            except asyncio.CancelledError:
                return

        hb_task = asyncio.create_task(send_heartbeat())
        try:
            async with driver.session() as session:
                res = await session.run(q, params)
                async for record in res:
                    await ws.send_json({"type": "record", "data": _serialize_record(record)})
                    if strategy == "one":
                        break
            await ws.send_json({"type": "end"})
        finally:
            hb_task.cancel()
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "error": str(exc)})
        finally:
            await ws.close(code=1011)


@fastapi_app.get("/logs/server")
async def server_logs(n: int = 200) -> dict[str, dict[str, list[str]]]:
    """Return the last N lines of each Neo4j server log file from the Modal volume."""
    try:
        logs_dir = Path(f"{NEO4J_DATA_PATH}/logs")
        if not logs_dir.exists():
            raise HTTPException(status_code=404, detail=f"Logs directory not found: {logs_dir}")
        output: dict[str, list[str]] = {}
        for fp in sorted(logs_dir.glob("*.log")):
            try:
                lines = fp.read_text(errors="replace").splitlines()
                output[fp.name] = lines[-max(n, 0):]
            except Exception as exc:
                output[fp.name] = [f"<error reading file: {exc}>"]
        return {"logs": output}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.post("/query")
async def query(
    payload: dict,
    driver: AsyncDriver = Depends(driver_dep),
) -> dict:
    query_str = payload.get("query")
    params = payload.get("parameters") or {}
    if not query_str:
        raise HTTPException(status_code=400, detail="query is required")
    try:
        async with driver.session() as session:
            result = await session.run(query_str, params)
            records = [_serialize_record(record) async for record in result]
            return {"results": records}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.post("/read")
async def read_query(
    payload: dict,
    driver: AsyncDriver = Depends(driver_dep),
) -> dict:
    query_str = payload.get("query")
    params = payload.get("parameters") or {}
    if not query_str:
        raise HTTPException(status_code=400, detail="query is required")
    try:
        async with driver.session() as session:
            result = await session.run(query_str, params)
            records = [_serialize_record(record) async for record in result]
            return {"results": records}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.post("/write")
async def write_query(
    payload: dict,
    driver: AsyncDriver = Depends(driver_dep),
) -> dict:
    query_str = payload.get("query")
    params = payload.get("parameters") or {}
    if not query_str:
        raise HTTPException(status_code=400, detail="query is required")
    try:
        async with driver.session() as session:
            result = await session.run(query_str, params)
            records = [_serialize_record(record) async for record in result]
            return {"results": records}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.post("/tx")
async def transaction(
    payload: dict,
    driver: AsyncDriver = Depends(driver_dep),
) -> dict:
    statements = payload.get("statements")
    if not isinstance(statements, list) or not all(
        isinstance(s, dict) and s.get("query") for s in statements
    ):
        raise HTTPException(status_code=400, detail="statements must be a list of {query, parameters}")

    try:
        async with driver.session() as session:
            tx = await session.begin_transaction()
            try:
                all_results: list[list[dict]] = []
                for s in statements:
                    params = s.get("parameters") or {}
                    res = await tx.run(s["query"], params)
                    recs = [_serialize_record(record) async for record in res]
                    all_results.append(recs)
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
            return {"results": all_results}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# -------------------------
# Typed /v1 endpoints
# -------------------------


@fastapi_app.post("/v1/query", response_model=SingleResultResponse | QueryResponse)
async def v1_query(req: QueryRequest, driver: AsyncDriver = Depends(driver_dep)):
    try:
        async with driver.session() as session:
            if req.strategy == "one":
                res = await session.run(req.query, req.parameters)
                record = await res.single()
                if record is None:
                    return SingleResultResponse(result=None)
                return SingleResultResponse(result=_serialize_record(record))
            elif req.strategy == "all":
                res = await session.run(req.query, req.parameters)
                records = [_serialize_record(r) async for r in res]
                return QueryResponse(results=records)
            else:
                await session.run(req.query, req.parameters)
                return QueryResponse(results=[])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.post("/v1/tx", response_model=QueryResponse)
async def v1_tx(req: TransactionRequest, driver: AsyncDriver = Depends(driver_dep)):
    if not req.statements:
        raise HTTPException(status_code=400, detail="statements must not be empty")
    try:
        async with driver.session() as session:
            tx = await session.begin_transaction()
            try:
                out: list = []
                for s in req.statements:
                    res = await tx.run(s.query, s.parameters)
                    out.append([_serialize_record(r) async for r in res])
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
            return QueryResponse(results=out)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/v1/schema", response_model=TypedSchemaResponse)
async def v1_schema(driver: AsyncDriver = Depends(driver_dep)):
    try:
        async with driver.session() as session:
            labels_res = await session.run("CALL db.labels()")
            rels_res = await session.run("CALL db.relationshipTypes()")
            props_res = await session.run("CALL db.propertyKeys()")

            # Normalize to plain lists
            labels = []
            async for rec in labels_res:
                data = rec.data()
                # Neo4j 5 returns {label: 'X'} or {value: 'X'} depending on proc aliasing
                labels.append(next(iter(data.values())))

            rels = []
            async for rec in rels_res:
                data = rec.data()
                rels.append(next(iter(data.values())))

            props = []
            async for rec in props_res:
                data = rec.data()
                props.append(next(iter(data.values())))

            return TypedSchemaResponse(labels=labels, relationshipTypes=rels, propertyKeys=props)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/v1/indexes", response_model=TypedIndexListResponse)
async def v1_indexes(driver: AsyncDriver = Depends(driver_dep)):
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, options RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            out: list[IndexInfo] = []
            async for rec in res:
                data = rec.data()
                out.append(IndexInfo(**data))
            return TypedIndexListResponse(indexes=out)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/v1/constraints", response_model=TypedConstraintListResponse)
async def v1_constraints(driver: AsyncDriver = Depends(driver_dep)):
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, options RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            out: list[ConstraintInfo] = []
            async for rec in res:
                data = rec.data()
                out.append(ConstraintInfo(**data))
            return TypedConstraintListResponse(constraints=out)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/schema")
async def schema(driver: AsyncDriver = Depends(driver_dep)) -> dict:
    try:
        async with driver.session() as session:
            labels_res = await session.run("CALL db.labels()")
            rels_res = await session.run("CALL db.relationshipTypes()")
            props_res = await session.run("CALL db.propertyKeys()")
            labels = [r for r in [record async for record in labels_res]]
            rels = [r for r in [record async for record in rels_res]]
            props = [r for r in [record async for record in props_res]]
            return {"labels": labels, "relationshipTypes": rels, "propertyKeys": props}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/indexes")
async def list_indexes(driver: AsyncDriver = Depends(driver_dep)) -> dict:
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, options RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            records = [record async for record in res]
            return {"indexes": records}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@fastapi_app.get("/constraints")
async def list_constraints(driver: AsyncDriver = Depends(driver_dep)) -> dict:
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, options RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            records = [record async for record in res]
            return {"constraints": records}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


_function_kwargs = dict(
    volumes={NEO4J_DATA_PATH: volume},
    timeout=86400,
    cpu=2.0,
    memory=4096,
    min_containers=1,
    # Uses the "distributed-systems" secret plus inline NEO4J_PASSWORD
    secrets=[
        modal.Secret.from_name("distributed-systems"),
        modal.Secret.from_name("neo4j-secrets"),
        modal.Secret.from_dict({"NEO4J_PASSWORD": "Backstab2025!"}),
    ],
)
if graph_management_mount is not None:
    _function_kwargs["mounts"] = [graph_management_mount]


@app.function(**_function_kwargs)
@modal.asgi_app()
def serve():
    """Launch Neo4j and expose the FastAPI layer when Modal spins up the container."""
    # Import here after the module is mounted
    from graph_management.local_env import load_local_env

    load_local_env()

    if not os.environ.get("NEO4J_BOOTSTRAPPED"):
        start_neo4j()
        os.environ["NEO4J_BOOTSTRAPPED"] = "1"
    return fastapi_app


async def _demo_ping() -> dict[str, int]:
    async with AsyncGraphDatabase.driver(
        _driver_uri(), auth=(NEO4J_USER, _neo4j_password())
    ) as driver:
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
    return {"ok": int(record["ok"])}


if __name__ == "__main__":
    print("Modal stub ready. Run `modal serve neo4j_modal_image.py` to start the service.")
    if os.getenv("RUN_LOCAL_PROBE") == "1":
        asyncio.run(_demo_ping())
