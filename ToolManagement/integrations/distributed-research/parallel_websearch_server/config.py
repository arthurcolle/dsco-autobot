"""Configuration module for Parallel Websearch Server."""

import logging
from typing import List, Optional

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API Configuration
    api_base_url: str = "https://api.parallel.ai"
    api_key: str = Field(
        default="",
        validation_alias=AliasChoices("api_key", "API_KEY", "parallel_api_key", "PARALLEL_API_KEY"),
    )
    api_version: str = "v1"
    # Beta headers for different API endpoints
    parallel_beta_search_extract: str = "search-extract-2025-10-10"
    parallel_beta_findall: str = "findall-2025-09-15"
    # Legacy alias for backwards compatibility
    parallel_beta_header: str = "search-extract-2025-10-10"
    api_key_cache_file: Optional[str] = Field(
        default=".parallel_api_key",
        validation_alias=AliasChoices("api_key_cache_file", "API_KEY_CACHE_FILE"),
    )
    cache_db_path: str = Field(
        default="parallel_context_cache.db",
        validation_alias=AliasChoices("cache_db_path", "CACHE_DB_PATH", "CACHE_SQLITE_PATH"),
        description="Path to the SQLite database used for caching context state",
    )
    default_parallel_processor: str = Field(
        default="lite",
        description="Default processor tier used for Parallel.ai task runs.",
        validation_alias=AliasChoices(
            "default_parallel_processor",
            "DEFAULT_PARALLEL_PROCESSOR",
            "default_processor",
            "DEFAULT_PROCESSOR",
        ),
    )
    allowed_parallel_processors: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional allowlist of Parallel.ai processors that callers may request. "
            "When unset, all processors defined by the Parallel API are permitted."
        ),
        validation_alias=AliasChoices(
            "allowed_parallel_processors",
            "ALLOWED_PARALLEL_PROCESSORS",
            "processor_allowlist",
            "PROCESSOR_ALLOWLIST",
        ),
    )

    # Server Configuration
    host: str = "127.0.0.1"
    port: int = 4002  # distributed-research port
    reload: bool = False
    workers: int = 4
    log_level: str = "info"
    environment: str = "development"

    # Tool Management Registry
    registry_url: str = Field(
        default="http://localhost:2016",
        validation_alias=AliasChoices("registry_url", "REGISTRY_URL", "TOOL_REGISTRY_URL"),
    )
    server_name: str = "distributed-research"
    server_version: str = "1.0.0"

    # Request Configuration
    request_timeout: int = 300  # 5 minutes
    max_concurrent_requests: int = 100

    # Streaming Configuration
    stream_chunk_size: int = 1024
    stream_timeout: int = 600  # 10 minutes
    stream_heartbeat_interval: int = 30
    mcp_http_path: str = "/mcp"
    mcp_sse_path: str = "/mcp/sse"

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"

    @field_validator("allowed_parallel_processors", mode="before")
    @classmethod
    def _normalize_allowed_processors(cls, value: Optional[object]) -> Optional[List[str]]:
        """Accept JSON arrays or comma-separated env strings for processor allowlists."""
        if value is None or value == "":
            return None

        items: List[str] = []
        if isinstance(value, str):
            items = [token.strip() for token in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(token).strip() for token in value if token is not None]
        else:  # pragma: no cover - defensive guardrail
            return None

        normalized = [token.lower() for token in items if token]
        if not normalized:
            return None
        # Preserve order but drop duplicates
        return list(dict.fromkeys(normalized))


def get_logger(name: str) -> logging.Logger:
    """Get configured logger instance."""
    return logging.getLogger(name)


# Global settings instance
settings = Settings()
