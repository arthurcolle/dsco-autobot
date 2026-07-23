"""Automated schema synchronization service."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from pydantic import BaseModel

from parallel_websearch_server.schema_sync.differ import SchemaDiffer, SchemaDiffReport
from parallel_websearch_server.schema_sync.fetcher import OpenAPIFetcher, SchemaMetadata

logger = logging.getLogger(__name__)


class SyncConfig(BaseModel):
    """Configuration for schema synchronization."""

    check_interval_seconds: int = 3600  # Check every hour
    auto_sync: bool = True
    notify_on_changes: bool = True
    notify_on_breaking_changes_only: bool = False


class SyncEvent(BaseModel):
    """Event emitted when schema changes are detected."""

    timestamp: datetime
    old_version: str
    new_version: str
    diff_report: SchemaDiffReport
    auto_applied: bool = False


class SchemaSyncService:
    """
    Automated schema synchronization service.

    Features:
    - Periodic polling for schema updates
    - Automatic diff generation
    - Change notifications
    - Optional auto-sync
    """

    def __init__(
        self,
        config: Optional[SyncConfig] = None,
        on_change_callback: Optional[Callable[[SyncEvent], None]] = None
    ):
        """
        Initialize sync service.

        Args:
            config: Sync configuration
            on_change_callback: Callback invoked when changes detected
        """
        self.config = config or SyncConfig()
        self.on_change_callback = on_change_callback

        self.fetcher = OpenAPIFetcher()
        self.differ = SchemaDiffer()

        self._current_schema: Optional[dict] = None
        self._current_metadata: Optional[SchemaMetadata] = None
        self._last_check: Optional[datetime] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the synchronization service."""
        if self._running:
            logger.warning("Sync service already running")
            return

        self._running = True
        logger.info(
            f"Starting schema sync service "
            f"(check interval: {self.config.check_interval_seconds}s, "
            f"auto_sync: {self.config.auto_sync})"
        )

        # Initial fetch
        await self.check_for_updates()

        # Start background task
        if self.config.auto_sync:
            self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self) -> None:
        """Stop the synchronization service."""
        self._running = False

        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        logger.info("Schema sync service stopped")

    async def check_for_updates(self, force: bool = False) -> Optional[SyncEvent]:
        """
        Check for schema updates.

        Args:
            force: Force check even if interval hasn't elapsed

        Returns:
            SyncEvent if changes detected, None otherwise
        """
        # Check if we should skip (interval not elapsed)
        if not force and self._last_check:
            elapsed = (datetime.utcnow() - self._last_check).total_seconds()
            if elapsed < self.config.check_interval_seconds:
                logger.debug(f"Skipping check, {elapsed:.0f}s since last check")
                return None

        self._last_check = datetime.utcnow()

        try:
            # Fetch latest schema
            new_schema, new_metadata = await self.fetcher.fetch_schema(force=force)

            # If no previous schema, this is initial fetch
            if not self._current_schema:
                logger.info(f"Initial schema fetch: v{new_metadata.version}")
                self._current_schema = new_schema
                self._current_metadata = new_metadata
                return None

            # Check if schema changed
            if new_metadata.checksum == self._current_metadata.checksum:
                logger.debug("Schema unchanged")
                return None

            # Schema changed - generate diff
            logger.info(
                f"Schema change detected: "
                f"v{self._current_metadata.version} -> v{new_metadata.version}"
            )

            diff_report = self.differ.diff_schemas(
                self._current_schema,
                new_schema
            )

            # Create sync event
            event = SyncEvent(
                timestamp=datetime.utcnow(),
                old_version=self._current_metadata.version,
                new_version=new_metadata.version,
                diff_report=diff_report,
                auto_applied=self.config.auto_sync
            )

            # Log changes
            self._log_changes(diff_report)

            # Update current schema
            self._current_schema = new_schema
            self._current_metadata = new_metadata

            # Notify callback
            if self.on_change_callback:
                if self.config.notify_on_breaking_changes_only:
                    if diff_report.has_breaking_changes():
                        self.on_change_callback(event)
                elif self.config.notify_on_changes:
                    self.on_change_callback(event)

            return event

        except Exception as e:
            logger.error(f"Error checking for schema updates: {e}", exc_info=True)
            return None

    async def _sync_loop(self) -> None:
        """Background sync loop."""
        while self._running:
            try:
                await self.check_for_updates()
                await asyncio.sleep(self.config.check_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}", exc_info=True)
                # Back off on error
                await asyncio.sleep(60)

    def _log_changes(self, diff_report: SchemaDiffReport) -> None:
        """Log detected changes."""
        if diff_report.total_changes == 0:
            return

        logger.info(
            f"Schema changes detected: {diff_report.total_changes} total "
            f"({diff_report.breaking_changes} breaking, "
            f"{diff_report.major_changes} major, "
            f"{diff_report.minor_changes} minor, "
            f"{diff_report.patch_changes} patch)"
        )

        # Log breaking changes in detail
        for change in diff_report.get_changes_by_severity("breaking"):
            logger.warning(f"BREAKING: {change.description}")

        # Log major changes
        for change in diff_report.get_changes_by_severity("major"):
            logger.info(f"MAJOR: {change.description}")

    def get_current_version(self) -> Optional[str]:
        """Get currently cached schema version."""
        return self._current_metadata.version if self._current_metadata else None

    def get_last_check_time(self) -> Optional[datetime]:
        """Get timestamp of last check."""
        return self._last_check

    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running
