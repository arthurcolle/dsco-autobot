"""Middleware for request/response processing with comprehensive audit logging."""

import json
import logging
import time
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from parallel_websearch_server.auth import cache_api_key_from_headers

logger = logging.getLogger(__name__)

# Maximum body size to capture (1MB)
MAX_BODY_CAPTURE_SIZE = 1024 * 1024

# Paths that should always have bodies captured (important API routes)
ALWAYS_CAPTURE_PATHS = {
    '/v1/tasks/runs',
    '/v1/tasks/runs/managed',
    '/v1beta/tasks/groups',
    '/v1/search',
    '/v1/extract',
    '/v1/findall',
}

# Paths to skip capture for (health checks, static, etc.)
SKIP_CAPTURE_PATHS = {
    '/health',
    '/healthz',
    '/ready',
    '/metrics',
    '/favicon.ico',
}


class ApiKeyCacheMiddleware:
    """Stash the last API key observed on inbound requests."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        header_map = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        cache_api_key_from_headers(header_map)
        await self.app(scope, receive, send)


class RequestIdMiddleware:
    """Streaming-safe request ID injector."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Decode headers once so downstream middleware can reuse the ID
        header_map = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        request_id = header_map.get("x-request-id", str(uuid.uuid4()))

        # Ensure request.state exists and stash the ID for FastAPI handlers
        state = scope.get("state")
        if state is None:
            # Initialize a plain dict for maximum compatibility; FastAPI may
            # populate this with a dict before creating Request.state objects.
            state = {}
            scope["state"] = state
        # Support both dict-style state and Starlette State objects.
        if isinstance(state, dict):
            state["request_id"] = request_id
        else:
            setattr(state, "request_id", request_id)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["x-request-id"] = request_id
                message["headers"] = headers.raw
            await send(message)

        await self.app(scope, receive, send_wrapper)


class LoggingMiddleware:
    """Streaming-safe request/response logger with optional body capture."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        state = scope.get("state")
        if isinstance(state, dict):
            request_id = state.get("request_id", "unknown")
        elif state is not None:
            request_id = getattr(state, "request_id", "unknown")
        else:
            request_id = "unknown"
        method = scope.get("method", "").upper()
        path = scope.get("path", "")
        client = scope.get("client")
        client_host = client[0] if isinstance(client, (list, tuple)) else None
        status_code_holder = {"status": None}

        logger.info(
            "Request started",
            extra={
                "request_id": request_id,
                "method": method,
                "path": path,
                "client": client_host,
            },
        )

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_code_holder["status"] = message.get("status")
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                process_time = time.time() - start_time
                headers["x-process-time"] = str(process_time)
                if request_id != "unknown":
                    if "x-request-id" not in headers:
                        headers["x-request-id"] = request_id
                message["headers"] = headers.raw
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            process_time = time.time() - start_time
            logger.error(
                "Request failed",
                extra={
                    "request_id": request_id,
                    "method": method,
                    "path": path,
                    "process_time": process_time,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

        process_time = time.time() - start_time
        logger.info(
            "Request completed",
            extra={
                "request_id": request_id,
                "method": method,
                "path": path,
                "status_code": status_code_holder["status"],
                "process_time": process_time,
            },
        )


class AuditMiddleware:
    """Comprehensive audit logging middleware with request/response body capture.

    This middleware captures complete request/response data and persists it
    to the audit log for debugging and analysis. Features:

    - Captures request headers, body, and query parameters
    - Captures response headers, body, and status code
    - Tracks timing information
    - Extracts correlation IDs (run_id, context_id, taskgroup_id)
    - Handles errors with full traceback capture
    - Respects body size limits to prevent memory issues
    - Skips health check endpoints for performance
    """

    def __init__(
        self,
        app: ASGIApp,
        capture_bodies: bool = True,
        max_body_size: int = MAX_BODY_CAPTURE_SIZE,
    ) -> None:
        self.app = app
        self.capture_bodies = capture_bodies
        self.max_body_size = max_body_size
        self._store = None

    def _get_store(self):
        """Lazy-load the local store to avoid circular imports."""
        if self._store is None:
            try:
                from parallel_websearch_server.local_store import get_local_store
                self._store = get_local_store()
            except Exception as e:
                logger.warning(f"Could not load local store for audit: {e}")
        return self._store

    def _should_capture(self, path: str) -> bool:
        """Determine if this path should have bodies captured."""
        # Skip health checks
        if path in SKIP_CAPTURE_PATHS:
            return False
        # Always capture important API routes
        for capture_path in ALWAYS_CAPTURE_PATHS:
            if path.startswith(capture_path):
                return True
        # Default: capture all API routes
        return path.startswith('/v1') or path.startswith('/mcp')

    def _extract_headers(self, scope: Scope) -> Dict[str, str]:
        """Extract headers from scope as a dict."""
        return {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }

    def _extract_query_params(self, scope: Scope) -> Optional[Dict[str, str]]:
        """Extract query parameters from scope."""
        query_string = scope.get("query_string", b"").decode("latin-1")
        if not query_string:
            return None
        params = {}
        for param in query_string.split("&"):
            if "=" in param:
                key, value = param.split("=", 1)
                params[key] = value
            else:
                params[param] = ""
        return params

    def _extract_correlation_ids(
        self,
        path: str,
        query_params: Optional[Dict],
        request_body: Optional[Any],
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract run_id, context_id, taskgroup_id from request data."""
        run_id = None
        context_id = None
        taskgroup_id = None

        # Extract from path
        if '/runs/' in path:
            parts = path.split('/runs/')
            if len(parts) > 1:
                run_id = parts[1].split('/')[0]
        if '/groups/' in path:
            parts = path.split('/groups/')
            if len(parts) > 1:
                taskgroup_id = parts[1].split('/')[0]

        # Extract from query params
        if query_params:
            context_id = query_params.get('context_id')

        # Extract from request body
        if isinstance(request_body, dict):
            if not run_id:
                run_id = request_body.get('run_id')
            if not taskgroup_id:
                taskgroup_id = request_body.get('taskgroup_id')

        return run_id, context_id, taskgroup_id

    def _determine_operation_type(self, method: str, path: str) -> str:
        """Classify the operation type based on method and path."""
        if '/tasks/runs/managed' in path:
            return 'task.run.managed'
        if '/tasks/runs' in path:
            if method == 'POST':
                return 'task.run.create'
            if '/result' in path:
                return 'task.run.result'
            return 'task.run.status'
        if '/tasks/groups' in path:
            return 'task.group'
        if '/search' in path:
            return 'search'
        if '/extract' in path:
            return 'extract'
        if '/findall' in path:
            return 'findall'
        if '/mcp' in path:
            return 'mcp'
        return 'unknown'

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        # Skip capture for excluded paths
        if path in SKIP_CAPTURE_PATHS:
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        started_at = datetime.utcnow().isoformat()

        # Extract request metadata
        state = scope.get("state")
        if isinstance(state, dict):
            request_id = state.get("request_id", str(uuid.uuid4()))
        elif state is not None:
            request_id = getattr(state, "request_id", str(uuid.uuid4()))
        else:
            request_id = str(uuid.uuid4())

        method = scope.get("method", "").upper()
        client = scope.get("client")
        client_host = client[0] if isinstance(client, (list, tuple)) else None
        headers = self._extract_headers(scope)
        query_params = self._extract_query_params(scope)
        user_agent = headers.get("user-agent")

        # Capture request body if enabled
        request_body = None
        request_body_chunks: List[bytes] = []
        total_request_size = 0

        if self.capture_bodies and self._should_capture(path):
            async def receive_wrapper() -> Message:
                nonlocal total_request_size
                message = await receive()
                if message["type"] == "http.request":
                    body = message.get("body", b"")
                    if total_request_size < self.max_body_size:
                        request_body_chunks.append(body)
                        total_request_size += len(body)
                return message
        else:
            receive_wrapper = receive

        # Capture response
        status_code = None
        response_headers: Dict[str, str] = {}
        response_body_chunks: List[bytes] = []
        total_response_size = 0
        error_info: Optional[Dict] = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_headers, total_response_size

            if message["type"] == "http.response.start":
                status_code = message.get("status")
                # Capture response headers
                raw_headers = message.get("headers", [])
                response_headers = {
                    h[0].decode("latin-1"): h[1].decode("latin-1")
                    for h in raw_headers
                }

            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                if self.capture_bodies and total_response_size < self.max_body_size:
                    response_body_chunks.append(body)
                    total_response_size += len(body)

            await send(message)

        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        except Exception as exc:
            error_info = {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            raise
        finally:
            # Calculate timing
            completed_at = datetime.utcnow().isoformat()
            duration_ms = int((time.time() - start_time) * 1000)

            # Parse request body
            if request_body_chunks:
                try:
                    raw_body = b"".join(request_body_chunks)
                    request_body = json.loads(raw_body.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    request_body = raw_body.decode("utf-8", errors="replace") if raw_body else None

            # Parse response body
            response_body = None
            if response_body_chunks:
                try:
                    raw_body = b"".join(response_body_chunks)
                    response_body = json.loads(raw_body.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    response_body = raw_body.decode("utf-8", errors="replace") if raw_body else None

            # Extract correlation IDs
            run_id, context_id, taskgroup_id = self._extract_correlation_ids(
                path, query_params, request_body
            )

            # Determine operation type
            operation_type = self._determine_operation_type(method, path)

            # Extract upstream request ID from response headers
            upstream_request_id = response_headers.get("x-parallel-request-id")

            # Determine success
            was_successful = error_info is None and (status_code is None or status_code < 400)

            # Count tokens for context visibility
            token_metrics = None
            try:
                from parallel_websearch_server.token_tracking import count_request_response_tokens, TokenModel
                token_metrics = count_request_response_tokens(
                    request_body=request_body,
                    response_body=response_body,
                    model=TokenModel.CLAUDE
                )
            except Exception as e:
                logger.warning(f"Token counting failed: {e}")

            # Save to audit log
            store = self._get_store()
            if store:
                try:
                    store.save_audit_log(
                        request_id=request_id,
                        method=method,
                        path=path,
                        started_at=started_at,
                        query_params=query_params,
                        request_headers=headers,
                        request_body=request_body,
                        client_ip=client_host,
                        user_agent=user_agent,
                        status_code=status_code,
                        response_headers=response_headers,
                        response_body=response_body,
                        completed_at=completed_at,
                        duration_ms=duration_ms,
                        upstream_request_id=upstream_request_id,
                        context_id=context_id,
                        run_id=run_id,
                        taskgroup_id=taskgroup_id,
                        error_type=error_info["type"] if error_info else None,
                        error_message=error_info["message"] if error_info else None,
                        error_traceback=error_info["traceback"] if error_info else None,
                        operation_type=operation_type,
                        was_successful=was_successful,
                        request_tokens=token_metrics.request_tokens if token_metrics else None,
                        response_tokens=token_metrics.response_tokens if token_metrics else None,
                        total_tokens=token_metrics.total_tokens if token_metrics else None,
                        request_bytes=token_metrics.request_bytes if token_metrics else None,
                        response_bytes=token_metrics.response_bytes if token_metrics else None,
                        total_bytes=token_metrics.total_bytes if token_metrics else None,
                        token_model=token_metrics.model.value if token_metrics else None,
                        size_bucket=token_metrics.size_bucket.value if token_metrics else None,
                    )
                except Exception as e:
                    logger.error(f"Failed to save audit log: {e}")


class CORSMiddleware:
    """CORS configuration helper."""

    @staticmethod
    def get_config():
        """Get CORS configuration."""
        return {
            "allow_origins": ["*"],
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        }
