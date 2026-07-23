"""
OpenAPI schema synchronization system.

Automatically fetches, diffs, and synchronizes with Parallel.ai's public OpenAPI spec
to ensure our implementation stays current with their latest API changes.

Features:
- Real-time schema fetching from public-openapi.json
- Intelligent diffing to detect additions, changes, and deprecations
- Automatic model generation from schemas
- Webhook signature verification
- Change notifications and alerts
"""

from parallel_websearch_server.schema_sync.fetcher import OpenAPIFetcher
from parallel_websearch_server.schema_sync.differ import SchemaDiffer, SchemaChange
from parallel_websearch_server.schema_sync.sync_service import SchemaSyncService

__all__ = [
    "OpenAPIFetcher",
    "SchemaDiffer",
    "SchemaChange",
    "SchemaSyncService",
]
