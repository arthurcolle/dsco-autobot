"""Compatibility shim that re-exports the Modal Neo4j image from infra.modal."""

from infra.modal.graph_management_image import app, serve, fastapi_app, driver_dep

__all__ = ["app", "serve", "fastapi_app", "driver_dep"]
