"""
Thin wrapper to serve the Graph Management Neo4j app via
`modal serve modal_wrapper.py`.

It re-exports the Modal `app` and the ASGI entrypoint `serve` from
`infra/modal/graph_management_image.py` (via `neo4j_modal_image.py`)
so you can use this shorter filename.
"""

from neo4j_modal_image import app, serve  # re-export for Modal CLI discovery

__all__ = ["app", "serve"]
