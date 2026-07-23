"""
Webhook support for Parallel.ai events.

Handles:
- FindAll webhooks (candidate events, run completion)
- Task webhooks (completion, failure)
- HMAC signature verification
- Event routing and handlers
- Retry logic for failed deliveries
"""

from parallel_websearch_server.webhooks.models import (
    WebhookEvent,
    WebhookEventType,
    FindAllCandidateEvent,
    FindAllRunEvent,
    TaskRunEvent,
)
from parallel_websearch_server.webhooks.verification import WebhookVerifier
from parallel_websearch_server.webhooks.handlers import WebhookHandler

__all__ = [
    "WebhookEvent",
    "WebhookEventType",
    "FindAllCandidateEvent",
    "FindAllRunEvent",
    "TaskRunEvent",
    "WebhookVerifier",
    "WebhookHandler",
]
