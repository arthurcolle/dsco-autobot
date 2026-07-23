"""Single source of truth for cross-service addressing.

``resolve(ServiceName.ENGINE)`` returns the base URL for a service, e.g.
``http://engine:8000`` in docker mode or ``http://localhost:8000`` locally.
Resolution order:

  1. Explicit env override — ``DSCO_ENGINE_URL``, ``DSCO_TOOL_GATEWAY_URL``, …
  2. Deploy mode (``DSCO_DEPLOY_MODE=local|docker|modal``):
       * ``docker``  -> compose container DNS name + canonical port
       * ``local``   -> localhost + canonical port
       * ``modal``   -> expects explicit ``DSCO_*_URL`` overrides per service
  3. Canonical port from :mod:`dsco_shared.config.ports`
"""
from __future__ import annotations

import os
from enum import Enum

from .ports import DOCKER_NAMES, SERVICE_PORTS

_LOCAL = "local"
_DOCKER = "docker"
# "modal" mode is also valid: it relies on explicit DSCO_*_URL overrides per
# service (handled in _env_override before the host logic runs).


class ServiceName(str, Enum):
    """Canonical application service identifiers."""

    ENGINE = "engine"
    COMPLETIONS = "completions"
    CONVERSATIONS = "conversations"
    TOOL_GATEWAY = "tool-gateway"
    TASK_SCHEDULER = "task-scheduler"
    GRAPH_MANAGEMENT = "graph-management"
    DYNAMIC_SCHEMA = "dynamic-schema"
    INGESTION = "ingestion"
    REALTIME = "realtime"


def _key(name: "ServiceName | str") -> str:
    return name.value if isinstance(name, ServiceName) else str(name)


def _env_override(name: str) -> str | None:
    key = f"DSCO_{name.upper().replace('-', '_')}_URL"
    val = os.getenv(key)
    return val.rstrip("/") if val else None


def deploy_mode() -> str:
    """Current deploy mode: ``local``, ``docker``, or ``modal``."""
    return os.getenv("DSCO_DEPLOY_MODE", _LOCAL).lower()


def canonical_port(name: "ServiceName | str") -> int:
    """Return the canonical HTTP port for a service."""
    n = _key(name)
    if n not in SERVICE_PORTS:
        raise KeyError(f"unknown service: {n!r}")
    return SERVICE_PORTS[n]


def _docker_host(name: str) -> str:
    """Container DNS name in docker mode (falls back to the canonical name)."""
    return DOCKER_NAMES.get(name, name)


def resolve(name: "ServiceName | str", scheme: str = "http") -> str:
    """Return the base URL for a service.

    >>> resolve(ServiceName.ENGINE)            # local:  http://localhost:8000
    >>> resolve("tool-gateway")                # docker: http://tool-management:8003
    >>> resolve(ServiceName.ENGINE, "ws")      #         ws://localhost:8000
    """
    n = _key(name)
    override = _env_override(n)
    if override:
        return override
    port = canonical_port(n)
    host = _docker_host(n) if deploy_mode() == _DOCKER else "localhost"
    return f"{scheme}://{host}:{port}"


# ── Infrastructure resolvers ──────────────────────────────────────────────
# These honor the conventional env vars (REDIS_URL, DATABASE_URL,
# TEMPORAL_ADDRESS) with registry-aware fallbacks.

def _infra_host(container: str) -> str:
    return container if deploy_mode() == _DOCKER else "localhost"


def redis_url() -> str:
    return os.getenv("REDIS_URL", f"redis://{_infra_host('redis')}:6379")


def postgres_url(db: str = "dsco") -> str:
    return os.getenv("DATABASE_URL", f"postgresql://{_infra_host('postgres')}:5432/{db}")


def temporal_address() -> str:
    return os.getenv("TEMPORAL_ADDRESS", f"{_infra_host('temporal')}:7233")
