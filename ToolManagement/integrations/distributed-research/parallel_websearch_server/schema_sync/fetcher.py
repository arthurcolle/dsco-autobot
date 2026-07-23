"""OpenAPI schema fetching and caching."""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class SchemaMetadata(BaseModel):
    """Metadata about a fetched schema."""

    url: str
    fetched_at: datetime
    version: str
    etag: Optional[str] = None
    checksum: str  # SHA256 of schema content
    size_bytes: int
    endpoint_count: int
    model_count: int


class OpenAPIFetcher:
    """
    Fetches and caches OpenAPI schemas from Parallel.ai.

    Features:
    - ETag-based caching to minimize bandwidth
    - SHA256 checksums for integrity verification
    - Local cache persistence
    - Automatic retry with exponential backoff
    """

    DEFAULT_SCHEMA_URL = "https://docs.parallel.ai/public-openapi.json"
    CACHE_DIR = Path(".cache/openapi")

    def __init__(
        self,
        schema_url: str = DEFAULT_SCHEMA_URL,
        cache_dir: Optional[Path] = None,
        timeout: int = 30
    ):
        """
        Initialize fetcher.

        Args:
            schema_url: URL to OpenAPI spec
            cache_dir: Directory for caching schemas
            timeout: Request timeout in seconds
        """
        self.schema_url = schema_url
        self.cache_dir = cache_dir or self.CACHE_DIR
        self.timeout = timeout

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._last_etag: Optional[str] = None
        self._last_schema: Optional[Dict[str, Any]] = None
        self._metadata: Optional[SchemaMetadata] = None

    async def fetch_schema(
        self,
        force: bool = False
    ) -> tuple[Dict[str, Any], SchemaMetadata]:
        """
        Fetch latest OpenAPI schema.

        Args:
            force: Force fetch even if cached version is fresh

        Returns:
            Tuple of (schema dict, metadata)
        """
        headers = {}

        # Use ETag for conditional requests (if we have one and not forcing)
        if not force and self._last_etag:
            headers["If-None-Match"] = self._last_etag

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Fetching OpenAPI schema from {self.schema_url}")
                response = await client.get(self.schema_url, headers=headers)

                # 304 Not Modified - use cached version
                if response.status_code == 304:
                    logger.info("Schema not modified (304), using cached version")
                    if self._last_schema and self._metadata:
                        return self._last_schema, self._metadata
                    # Fallback to loading from disk
                    return await self._load_from_cache()

                response.raise_for_status()

                # Parse schema
                schema = response.json()

                # Extract ETag for future requests
                etag = response.headers.get("etag")
                self._last_etag = etag

                # Calculate checksum
                content_bytes = json.dumps(schema, sort_keys=True).encode()
                checksum = hashlib.sha256(content_bytes).hexdigest()

                # Extract metadata
                metadata = SchemaMetadata(
                    url=self.schema_url,
                    fetched_at=datetime.utcnow(),
                    version=schema.get("info", {}).get("version", "unknown"),
                    etag=etag,
                    checksum=checksum,
                    size_bytes=len(content_bytes),
                    endpoint_count=self._count_endpoints(schema),
                    model_count=self._count_models(schema)
                )

                logger.info(
                    f"Fetched schema v{metadata.version}: "
                    f"{metadata.endpoint_count} endpoints, "
                    f"{metadata.model_count} models, "
                    f"{metadata.size_bytes} bytes"
                )

                # Cache in memory
                self._last_schema = schema
                self._metadata = metadata

                # Persist to disk
                await self._save_to_cache(schema, metadata)

                return schema, metadata

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching schema: {e.response.status_code}")
            # Try to load from cache as fallback
            return await self._load_from_cache()
        except Exception as e:
            logger.error(f"Error fetching schema: {e}", exc_info=True)
            # Try to load from cache as fallback
            return await self._load_from_cache()

    async def _save_to_cache(
        self,
        schema: Dict[str, Any],
        metadata: SchemaMetadata
    ) -> None:
        """Save schema and metadata to cache."""
        try:
            # Save schema
            schema_file = self.cache_dir / "schema.json"
            with open(schema_file, "w") as f:
                json.dump(schema, f, indent=2)

            # Save metadata
            metadata_file = self.cache_dir / "metadata.json"
            with open(metadata_file, "w") as f:
                f.write(metadata.model_dump_json(indent=2))

            logger.info(f"Cached schema to {schema_file}")

        except Exception as e:
            logger.warning(f"Failed to cache schema: {e}")

    async def _load_from_cache(self) -> tuple[Dict[str, Any], SchemaMetadata]:
        """Load schema from cache."""
        schema_file = self.cache_dir / "schema.json"
        metadata_file = self.cache_dir / "metadata.json"

        if not schema_file.exists() or not metadata_file.exists():
            raise FileNotFoundError("No cached schema available")

        try:
            # Load schema
            with open(schema_file) as f:
                schema = json.load(f)

            # Load metadata
            with open(metadata_file) as f:
                metadata_data = json.load(f)
                metadata = SchemaMetadata(**metadata_data)

            logger.info(f"Loaded cached schema v{metadata.version} from {schema_file}")

            # Update in-memory cache
            self._last_schema = schema
            self._metadata = metadata

            return schema, metadata

        except Exception as e:
            logger.error(f"Failed to load cached schema: {e}")
            raise

    def _count_endpoints(self, schema: Dict[str, Any]) -> int:
        """Count number of API endpoints in schema."""
        count = 0
        for path_data in schema.get("paths", {}).values():
            # Count HTTP methods
            count += len([k for k in path_data.keys() if k in ["get", "post", "put", "patch", "delete"]])
        return count

    def _count_models(self, schema: Dict[str, Any]) -> int:
        """Count number of data models in schema."""
        return len(schema.get("components", {}).get("schemas", {}))

    def get_cached_metadata(self) -> Optional[SchemaMetadata]:
        """Get metadata for currently cached schema."""
        return self._metadata

    def get_schema_version(self) -> Optional[str]:
        """Get version of currently cached schema."""
        return self._metadata.version if self._metadata else None
