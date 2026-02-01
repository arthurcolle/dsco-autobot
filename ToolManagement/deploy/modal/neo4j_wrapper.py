from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException
from neo4j import AsyncDriver, AsyncGraphDatabase
import modal

APP_NAME = "neo4j-bridge"

image = (
    modal.Image.debian_slim()
    .pip_install(
        "fastapi[standard]",
        "uvicorn",
        "neo4j",
    )
)

app = modal.App(APP_NAME, image=image)


def get_driver() -> AsyncDriver:
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not username or not password:
        missing = [
            name
            for name, value in (
                ("NEO4J_URI", uri),
                ("NEO4J_USER", username),
                ("NEO4J_PASSWORD", password),
            )
            if not value
        ]
        raise RuntimeError(f"Missing Neo4j connection settings: {', '.join(missing)}")
    return AsyncGraphDatabase.driver(uri, auth=(username, password))


_driver: AsyncDriver | None = None


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
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
    if _driver is not None:
        return _driver
    # Fallback in environments where lifespan is not triggered (e.g. tests)
    return get_driver()


@fastapi_app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@fastapi_app.get("/probe")
async def probe(driver: AsyncDriver = Depends(driver_dep)) -> dict[str, int]:
    try:
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
            return {"ok": record["ok"]}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.function(secrets=[modal.Secret.from_name("distributed-systems")])
@modal.asgi_app()
def serve():
    return fastapi_app


if __name__ == "__main__":
    print("Run `modal serve neo4j_modal_wrapper.py` to launch the probe service.")
