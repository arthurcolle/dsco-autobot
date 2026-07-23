"""Webhook event handlers."""

import logging
from typing import Callable, Dict, List, Optional

from parallel_websearch_server.webhooks.models import (
    WebhookEvent,
    WebhookEventType,
    FindAllCandidateEvent,
    FindAllRunEvent,
    TaskRunEvent,
)

logger = logging.getLogger(__name__)


# Type for event handler functions
EventHandler = Callable[[WebhookEvent], None]


class WebhookHandler:
    """
    Webhook event handler and router.

    Routes incoming webhook events to registered handlers based on event type.
    """

    def __init__(self):
        """Initialize handler."""
        self._handlers: Dict[WebhookEventType, List[EventHandler]] = {}
        self._global_handlers: List[EventHandler] = []

    def register(
        self,
        event_type: Optional[WebhookEventType] = None
    ) -> Callable:
        """
        Decorator to register an event handler.

        Usage:
            @handler.register(WebhookEventType.FINDALL_CANDIDATE_MATCHED)
            def on_candidate_matched(event: FindAllCandidateEvent):
                print(f"Candidate matched: {event.data.name}")

            @handler.register()  # Register for all events
            def on_any_event(event: WebhookEvent):
                print(f"Event received: {event.type}")

        Args:
            event_type: Event type to handle, or None for all events
        """
        def decorator(func: EventHandler) -> EventHandler:
            if event_type is None:
                # Global handler (called for all events)
                self._global_handlers.append(func)
                logger.info(f"Registered global handler: {func.__name__}")
            else:
                # Event-specific handler
                if event_type not in self._handlers:
                    self._handlers[event_type] = []
                self._handlers[event_type].append(func)
                logger.info(f"Registered handler for {event_type.value}: {func.__name__}")

            return func

        return decorator

    def handle_event(self, event: WebhookEvent) -> None:
        """
        Handle a webhook event.

        Dispatches to registered handlers based on event type.

        Args:
            event: Webhook event to handle
        """
        logger.info(f"Handling webhook event: {event.type}")

        # Call global handlers
        for handler in self._global_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(
                    f"Error in global handler {handler.__name__}: {e}",
                    exc_info=True
                )

        # Call event-specific handlers
        handlers = self._handlers.get(event.type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(
                    f"Error in handler {handler.__name__} for {event.type}: {e}",
                    exc_info=True
                )

    def unregister(
        self,
        func: EventHandler,
        event_type: Optional[WebhookEventType] = None
    ) -> bool:
        """
        Unregister a handler.

        Args:
            func: Handler function to unregister
            event_type: Event type, or None for global handlers

        Returns:
            True if handler was found and removed
        """
        if event_type is None:
            if func in self._global_handlers:
                self._global_handlers.remove(func)
                logger.info(f"Unregistered global handler: {func.__name__}")
                return True
        else:
            handlers = self._handlers.get(event_type, [])
            if func in handlers:
                handlers.remove(func)
                logger.info(f"Unregistered handler for {event_type.value}: {func.__name__}")
                return True

        return False

    def get_handler_count(self, event_type: Optional[WebhookEventType] = None) -> int:
        """
        Get number of registered handlers.

        Args:
            event_type: Event type to count, or None for all handlers

        Returns:
            Number of registered handlers
        """
        if event_type is None:
            # Count all handlers
            total = len(self._global_handlers)
            for handlers in self._handlers.values():
                total += len(handlers)
            return total
        else:
            return len(self._handlers.get(event_type, []))


# Example handlers

def log_all_events(event: WebhookEvent) -> None:
    """Example: Log all webhook events."""
    logger.info(f"Webhook event received: {event.type} at {event.timestamp}")


def handle_findall_match(event: FindAllCandidateEvent) -> None:
    """Example: Handle FindAll candidate match events."""
    if event.type == WebhookEventType.FINDALL_CANDIDATE_MATCHED:
        candidate = event.data
        logger.info(
            f"Candidate matched: {candidate.name} ({candidate.url}) "
            f"- Status: {candidate.match_status}"
        )

        # Example: Store to database, trigger notification, etc.
        # db.candidates.insert(candidate)
        # send_notification(f"New match found: {candidate.name}")


def handle_findall_completion(event: FindAllRunEvent) -> None:
    """Example: Handle FindAll run completion events."""
    if event.type == WebhookEventType.FINDALL_RUN_COMPLETED:
        run = event.data
        metrics = run.status.metrics

        logger.info(
            f"FindAll run {run.findall_id} completed: "
            f"{metrics.get('matched_candidates_count', 0)} matches found "
            f"from {metrics.get('generated_candidates_count', 0)} candidates"
        )

        # Example: Trigger downstream processing
        # process_matched_candidates(run.findall_id)


def handle_task_completion(event: TaskRunEvent) -> None:
    """Example: Handle task completion events."""
    if event.type == WebhookEventType.TASK_RUN_COMPLETED:
        task = event.data
        logger.info(f"Task {task.run_id} completed successfully")

        # Example: Extract results, update database, etc.
        # results = task.output
        # db.tasks.update(task.run_id, results=results)

    elif event.type == WebhookEventType.TASK_RUN_FAILED:
        task = event.data
        logger.error(f"Task {task.run_id} failed: {task.error}")

        # Example: Trigger retry, send alert, etc.
        # retry_task(task.run_id)
        # send_alert(f"Task failed: {task.error}")


# Pre-configured handler instance
default_handler = WebhookHandler()

# Register example handlers
default_handler.register()(log_all_events)
default_handler.register(WebhookEventType.FINDALL_CANDIDATE_MATCHED)(handle_findall_match)
default_handler.register(WebhookEventType.FINDALL_RUN_COMPLETED)(handle_findall_completion)
default_handler.register(WebhookEventType.TASK_RUN_COMPLETED)(handle_task_completion)
default_handler.register(WebhookEventType.TASK_RUN_FAILED)(handle_task_completion)
