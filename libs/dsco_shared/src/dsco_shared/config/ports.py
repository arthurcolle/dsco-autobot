"""Canonical service addressing table — the single source of truth.

Edited here (Python constants). ``scripts/render_topology.py`` renders these
into ``libs/service_registry.toml``, ``infra/docker/.env.example``, and
``infra/docker/prometheus.yml``, and *checks* the docker-compose port mappings
against this table so the app/compose drift that hid bugs before cannot recur.

``resolve()`` reads these constants directly — there is no runtime file
dependency, so the package works under both editable and wheel installs.
"""
from __future__ import annotations

# Application services (HTTP). Keys are canonical service names (kebab-case).
SERVICE_PORTS: dict[str, int] = {
    "engine": 8000,
    "completions": 8001,
    "conversations": 8002,
    "tool-gateway": 8003,
    "task-scheduler": 8004,
    "graph-management": 8005,
    "dynamic-schema": 8006,
    "ingestion": 8007,
    "realtime": 8008,
}

# Docker-compose service (container DNS) names where they differ from the
# canonical key above. In docker mode, services are addressed by these names.
DOCKER_NAMES: dict[str, str] = {
    "tool-gateway": "tool-management",
}

# Infrastructure services.
INFRA_PORTS: dict[str, int] = {
    "temporal": 7233,        # gRPC
    "temporal-ui": 8080,
    "redis": 6379,
    "postgres": 5432,
    "graphsub": 7879,
    "neo4j": 7687,           # bolt
    "neo4j-http": 7474,
    "prometheus": 9090,
    "grafana": 3000,
    "jaeger": 4317,          # OTLP gRPC
    "jaeger-ui": 16686,
    "otel-collector": 4317,
}
