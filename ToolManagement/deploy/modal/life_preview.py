"""Isolated Modal preview for the Distributed Life public experience.

Deploy with:
    modal deploy deploy/modal/life_preview.py

This app intentionally serves no production credentials, customer data, or tool
execution routes. It exists only to make the public experience reviewable.
"""

from pathlib import Path

import modal


app = modal.App("distributed-life-preview")

LANDING_FILE = (
    Path(__file__).parent.parent.parent
    / "src"
    / "tool_management"
    / "web"
    / "landing.py"
)
DEMO_FILE = (
    Path(__file__).parent.parent.parent
    / "src"
    / "tool_management"
    / "api"
    / "ultimate_demo_routes.py"
)

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("fastapi==0.115.0", "uvicorn==0.32.0", "httpx==0.27.2")
    .add_local_file(LANDING_FILE, remote_path="/root/landing.py")
    .add_local_file(DEMO_FILE, remote_path="/root/ultimate_demo_routes.py")
)


@app.function(
    image=image,
    timeout=120,
    min_containers=0,
    max_containers=1,
    scaledown_window=300,
)
@modal.asgi_app()
def web():
    """Serve the landing page without attaching any production capability."""
    import sys

    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

    sys.path.insert(0, "/root")
    from landing import render_landing
    from ultimate_demo_routes import router as ultimate_demo_router

    api = FastAPI(
        title="Distributed Life Preview",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    api.include_router(ultimate_demo_router)
    stats = {"tool_count": 2524, "backend_count": 10, "healthy_backends": 10}

    def landing_response(*, sign_in: bool = False) -> HTMLResponse:
        page = render_landing(**stats)
        if sign_in:
            page = page.replace(
                "</body>",
                "<script>window.addEventListener('DOMContentLoaded', function () { "
                "openAuth('login'); });</script></body>",
            )
        return HTMLResponse(
            page,
            headers={
                "Cache-Control": "no-store",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
            },
        )

    @api.get("/", response_class=HTMLResponse)
    async def root():
        return landing_response()

    @api.get("/health")
    async def health():
        return {"status": "ok", "preview": True, **stats}

    @api.get("/api/v1/status")
    async def status():
        return {"service": "distributed-life-preview", "preview": True, **stats}

    @api.api_route("/api/v1/users/{mode}", methods=["POST"])
    async def preview_auth(mode: str):
        return JSONResponse(
            status_code=503,
            content={
                "detail": (
                    "This is the isolated design preview. Private membership "
                    "enrollment remains disabled until the production secret rotation."
                )
            },
        )

    @api.get("/api/v1/me")
    @api.get("/api/v1/me/billing")
    @api.post("/api/v1/me/billing/checkout-session")
    @api.post("/api/v1/me/billing/portal-session")
    async def preview_private_account():
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Private account services are not attached to this design preview."
            },
        )

    @api.get("/account", response_class=HTMLResponse, include_in_schema=False)
    async def account():
        return landing_response(sign_in=True)

    @api.get("/docs", include_in_schema=False)
    async def docs():
        return RedirectResponse(url="/#infrastructure", status_code=302)

    @api.get("/mcp/health")
    async def mcp_health():
        return {"status": "preview", "message": "Production MCP is not attached."}

    return api
