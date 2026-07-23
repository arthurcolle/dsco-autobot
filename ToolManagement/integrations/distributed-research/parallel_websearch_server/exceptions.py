"""Custom exceptions for the application."""

from typing import Optional, Dict, Any


class ParallelAPIException(Exception):
    """Base exception for Parallel API errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize exception.

        Args:
            message: Error message
            error_code: Error code for categorization
            details: Additional error details
        """
        self.message = message
        self.error_code = error_code or "INTERNAL_ERROR"
        self.details = details or {}
        super().__init__(self.message)


class InvalidQueryError(ParallelAPIException):
    """Exception for invalid query parameters."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception."""
        super().__init__(message, "INVALID_QUERY", details)


class APIConnectionError(ParallelAPIException):
    """Exception for API connection failures."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception."""
        super().__init__(message, "API_CONNECTION_ERROR", details)


class APITimeoutError(ParallelAPIException):
    """Exception for API timeout."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception."""
        super().__init__(message, "API_TIMEOUT", details)


class ExtractionError(ParallelAPIException):
    """Exception for content extraction failures."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception."""
        super().__init__(message, "EXTRACTION_ERROR", details)


class RateLimitError(ParallelAPIException):
    """Exception for rate limit exceeded."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception."""
        super().__init__(message, "RATE_LIMIT_EXCEEDED", details)
