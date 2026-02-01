"""
Self-contained Neo4j deployment on Modal with persistent storage.

This module provides a complete Neo4j database running on Modal infrastructure
with persistent storage via Modal Volumes.
"""

from __future__ import annotations

import os
import subprocess
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException
from neo4j import AsyncDriver, AsyncGraphDatabase
import modal

APP_NAME = "neo4j-self-hosted"

# Modal Volume for persistent Neo4j data
volume = modal.Volume.from_name("neo4j-data", create_if_missing=True)

# Image with Neo4j and Python dependencies
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "wget",
        "gnupg",
        "curl",
        "openjdk-17-jre-headless",  # Neo4j requires Java
    )
    .run_commands(
        # Install Neo4j
        "wget -O - https://debian.neo4j.com/neotechnology.gpg.key | apt-key add -",
        "echo 'deb https://debian.neo4j.com stable latest' > /etc/apt/sources.list.d/neo4j.list",
        "apt-get update",
        "apt-get install -y neo4j",
    )
    .pip_install(
        "fastapi[standard]",
        "uvicorn",
        "neo4j",
    )
)

app = modal.App(APP_NAME, image=image)

# Configuration
NEO4J_DATA_PATH = "/data/neo4j"
NEO4J_BOLT_PORT = 7687
NEO4J_HTTP_PORT = 7474
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j_password")


def start_neo4j():
    """Start Neo4j server in the container."""
    print("Starting Neo4j server...")

    # Create data directories
    os.makedirs(f"{NEO4J_DATA_PATH}/data", exist_ok=True)
    os.makedirs(f"{NEO4J_DATA_PATH}/logs", exist_ok=True)
    os.makedirs(f"{NEO4J_DATA_PATH}/import", exist_ok=True)
    os.makedirs(f"{NEO4J_DATA_PATH}/plugins", exist_ok=True)

    # Configure Neo4j
    config_updates = [
        f"server.default_listen_address=0.0.0.0",
        f"server.bolt.listen_address=0.0.0.0:{NEO4J_BOLT_PORT}",
        f"server.http.listen_address=0.0.0.0:{NEO4J_HTTP_PORT}",
        f"server.directories.data={NEO4J_DATA_PATH}/data",
        f"server.directories.logs={NEO4J_DATA_PATH}/logs",
        f"server.directories.import={NEO4J_DATA_PATH}/import",
        f"server.directories.plugins={NEO4J_DATA_PATH}/plugins",
        "initial.dbms.default_database=neo4j",
        "dbms.security.auth_enabled=true",
    ]

    # Write configuration
    config_path = "/etc/neo4j/neo4j.conf"
    with open(config_path, "a") as f:
        f.write("\n# Modal deployment configuration\n")
        for config in config_updates:
            f.write(f"{config}\n")

    # Set initial password
    subprocess.run(
        ["neo4j-admin", "dbms", "set-initial-password", NEO4J_PASSWORD],
        check=False,  # May fail if already set
    )

    # Start Neo4j
    subprocess.Popen(
        ["neo4j", "console"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for Neo4j to be ready
    print("Waiting for Neo4j to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            result = subprocess.run(
                ["cypher-shell", "-u", "neo4j", "-p", NEO4J_PASSWORD, "RETURN 1;"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                print(f"Neo4j is ready after {i + 1} attempts!")
                return
        except subprocess.TimeoutExpired:
            pass
        time.sleep(2)

    raise RuntimeError("Neo4j failed to start within timeout")


def get_driver() -> AsyncDriver:
    """Create Neo4j driver for local connection."""
    uri = f"neo4j://localhost:{NEO4J_BOLT_PORT}"
    return AsyncGraphDatabase.driver(uri, auth=("neo4j", NEO4J_PASSWORD))


_driver: AsyncDriver | None = None


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan to manage Neo4j driver."""
    global _driver
    driver = get_driver()
    _driver = driver
    try:
        yield
    finally:
        _driver = None
        await driver.close()


fastapi_app = FastAPI(lifespan=lifespan)


async def driver_dep() -> AsyncDriver:
    """Dependency to get Neo4j driver."""
    if _driver is not None:
        return _driver
    return get_driver()


@fastapi_app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@fastapi_app.get("/probe")
async def probe(driver: AsyncDriver = Depends(driver_dep)) -> dict[str, int]:
    """Probe Neo4j connectivity."""
    try:
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
            return {"ok": record["ok"]}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@fastapi_app.post("/query")
async def query(
    query_request: dict,
    driver: AsyncDriver = Depends(driver_dep),
) -> dict:
    """Execute a Cypher query.

    Request body:
    {
        "query": "MATCH (n) RETURN n LIMIT 10",
        "parameters": {}  // optional
    }
    """
    try:
        query_str = query_request.get("query")
        parameters = query_request.get("parameters", {})

        if not query_str:
            raise HTTPException(status_code=400, detail="Query is required")

        async with driver.session() as session:
            result = await session.run(query_str, parameters)
            records = [record.data() async for record in result]
            return {"results": records}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.cls(
    volumes={NEO4J_DATA_PATH: volume},
    cpu=2.0,
    memory=4096,
    container_idle_timeout=600,
    allow_concurrent_inputs=100,
)
class Neo4jService:
    """Modal service that runs Neo4j with persistent storage."""

    def __enter__(self):
        """Initialize Neo4j when container starts."""
        print("Container starting, initializing Neo4j...")
        start_neo4j()
        print("Neo4j initialization complete")

    @modal.asgi_app()
    def serve(self):
        """Serve the FastAPI application."""
        return fastapi_app


if __name__ == "__main__":
    print("Run `modal deploy neo4j_self_hosted.py` to deploy Neo4j.")
    print("Or run `modal serve neo4j_self_hosted.py` for development.")
