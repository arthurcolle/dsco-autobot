"""Utilities for handling API key resolution and caching."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from threading import RLock
from typing import Dict, Mapping, Optional

from fastapi import HTTPException

from parallel_websearch_server.config import settings

logger = logging.getLogger(__name__)


class ApiKeyCache:
    """Thread-safe cache for the most recent Parallel API key."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._cache_path = self._resolve_cache_path()

        seeded = settings.api_key or None
        loaded_from_disk = False
        if not seeded:
            seeded = self._load_from_disk()
            loaded_from_disk = bool(seeded)

        self._key: Optional[str] = seeded

        # If the key came from env vars (or other config), persist it for future restarts.
        if seeded and self._cache_path and not loaded_from_disk:
            self._persist_to_disk(seeded, silent=True)

    def _resolve_cache_path(self) -> Optional[Path]:
        cache_file = settings.api_key_cache_file
        if not cache_file:
            return None
        return Path(cache_file).expanduser()

    def _load_from_disk(self) -> Optional[str]:
        """Read a cached API key from disk, if configured."""

        path = self._cache_path
        if not path:
            return None

        try:
            if path.exists():
                value = path.read_text(encoding="utf-8").strip()
                if value:
                    logger.info("Parallel API key loaded from cache file %s", path)
                    return value
        except Exception as exc:  # pragma: no cover - best effort logging
            logger.warning("Failed to load API key cache from %s: %s", path, exc)
        return None

    def _persist_to_disk(self, value: str, *, silent: bool = False) -> None:
        """Write the cached API key to disk for reuse across restarts."""

        path = self._cache_path
        if not path:
            return

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value, encoding="utf-8")
            try:
                os.chmod(path, 0o600)
            except OSError:
                # Best-effort permission tightening; ignore when unsupported (e.g., Windows).
                pass
            if not silent:
                logger.info("Parallel API key persisted to %s", path)
        except Exception as exc:  # pragma: no cover - best effort logging
            logger.warning("Failed to persist API key cache to %s: %s", path, exc)

    def store(self, key: Optional[str]) -> None:
        """Persist a non-empty API key for downstream requests."""

        sanitized = key.strip() if isinstance(key, str) else None
        if not sanitized:
            return

        with self._lock:
            previous = self._key
            self._key = sanitized

        if previous != sanitized:
            logger.info("Parallel API key cached from incoming request headers")
            self._persist_to_disk(sanitized)

    def get(self) -> Optional[str]:
        """Return the cached API key, if present."""

        with self._lock:
            return self._key


api_key_cache = ApiKeyCache()


def cache_api_key_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """Inspect HTTP headers and cache any provided Parallel API key."""

    # Normalize once for case-insensitive lookups.
    normalized = {key.lower(): value for key, value in headers.items()}

    key = normalized.get("x-api-key")
    if not key:
        authorization = normalized.get("authorization", "")
        if authorization.lower().startswith("bearer "):
            key = authorization.split(" ", 1)[1]

    if key:
        api_key_cache.store(key)

    return key


def _ensure_api_key(explicit_key: Optional[str] = None) -> str:
    """Resolve the API key or raise an HTTP 401 if none is available."""

    key = explicit_key or api_key_cache.get() or settings.api_key
    if not key:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Missing Parallel API key",
                "message": (
                    "Provide an x-api-key header once or set API_KEY/PARALLEL_API_KEY "
                    "in the environment before starting the server."
                ),
            },
        )
    return key


def get_auth_headers_api(
    api_key: Optional[str] = None,
    *,
    include_beta: bool = True,
    beta_type: str = "search_extract",
) -> Dict[str, str]:
    """Construct HTTP headers for upstream Parallel API calls.

    Args:
        api_key: Optional explicit API key
        include_beta: Whether to include beta header
        beta_type: Type of beta header - "search_extract" or "findall"

    Returns:
        Dictionary of HTTP headers
    """
    resolved = _ensure_api_key(api_key)
    headers = {"x-api-key": resolved}
    if include_beta:
        if beta_type == "findall":
            headers["parallel-beta"] = settings.parallel_beta_findall
        else:
            headers["parallel-beta"] = settings.parallel_beta_search_extract
    return headers


def get_bearer_auth_headers(api_key: Optional[str] = None) -> Dict[str, str]:
    """Construct HTTP headers using Bearer token format (for Chat API)."""
    resolved = _ensure_api_key(api_key)
    return {
        "Authorization": f"Bearer {resolved}",
        "Content-Type": "application/json",
    }


# Backwards-compatible alias for route modules.
get_auth_headers = get_auth_headers_api
