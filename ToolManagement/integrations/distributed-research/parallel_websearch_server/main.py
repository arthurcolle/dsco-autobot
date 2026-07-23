"""Main entry point for the Parallel WebSearch Server."""

import sys
import uvicorn

from parallel_websearch_server.config import settings
from parallel_websearch_server.logging_config import configure_uvicorn_logging


def main():
    """Run the server."""
    # Configure logging to suppress noisy warnings
    configure_uvicorn_logging()

    uvicorn.run(
        "parallel_websearch_server.app:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        workers=settings.workers if not settings.reload else 1,
        log_level=settings.log_level,
        access_log=True,
    )


if __name__ == "__main__":
    main()
