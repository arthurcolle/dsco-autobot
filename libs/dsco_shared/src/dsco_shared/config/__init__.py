"""Addressing & configuration for dsco-autobot services."""

from .ports import DOCKER_NAMES, INFRA_PORTS, SERVICE_PORTS
from .service_registry import (
    ServiceName,
    canonical_port,
    deploy_mode,
    postgres_url,
    redis_url,
    resolve,
    temporal_address,
)

__all__ = [
    "ServiceName",
    "resolve",
    "canonical_port",
    "deploy_mode",
    "redis_url",
    "postgres_url",
    "temporal_address",
    "SERVICE_PORTS",
    "INFRA_PORTS",
    "DOCKER_NAMES",
]
