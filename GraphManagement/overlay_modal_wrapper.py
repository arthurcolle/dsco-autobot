"""
Thin wrapper to serve the Overlay Graph Management API via
`modal serve overlay_modal_wrapper.py`.

It re-exports the Modal `app` and the ASGI entrypoint from
`infra/modal/overlay_app.py` so the CLI can discover them without
renaming the main module-level objects.
"""

from infra.modal.overlay_app import app, serve  # re-export for Modal CLI discovery

__all__ = ["app", "serve"]
