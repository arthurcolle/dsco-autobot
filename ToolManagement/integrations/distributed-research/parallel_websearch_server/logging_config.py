"""Custom logging configuration to suppress noisy uvicorn warnings."""

import logging


class InvalidHTTPFilter(logging.Filter):
    """Filter out 'Invalid HTTP request received' warnings from uvicorn."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False to suppress the log record."""
        message = record.getMessage()
        # Suppress "Invalid HTTP request received" warnings
        if "Invalid HTTP request received" in message:
            return False
        return True


def configure_uvicorn_logging():
    """Configure uvicorn logging to suppress invalid HTTP warnings."""
    # Get the uvicorn.error logger (which emits the warnings)
    uvicorn_logger = logging.getLogger("uvicorn.error")

    # Add the filter to suppress invalid HTTP warnings
    uvicorn_logger.addFilter(InvalidHTTPFilter())

    # Also filter the main uvicorn logger
    main_uvicorn_logger = logging.getLogger("uvicorn")
    main_uvicorn_logger.addFilter(InvalidHTTPFilter())
