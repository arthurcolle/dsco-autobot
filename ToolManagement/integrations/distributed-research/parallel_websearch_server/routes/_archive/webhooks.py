"""
FastAPI routes for webhook endpoints.

Receives webhooks from Parallel.ai for FindAll and Task events.
"""

import json
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import ValidationError

from parallel_websearch_server.config import settings
from parallel_websearch_server.webhooks.handlers import default_handler
from parallel_websearch_server.webhooks.models import (
    FindAllCandidateEvent,
    FindAllRunEvent,
    TaskRunEvent,
    WebhookEventType,
)
from parallel_websearch_server.webhooks.verification import WebhookVerifier, WebhookVerificationError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


@router.post(
    "/findall",
    status_code=status.HTTP_200_OK,
    operation_id="receive_findall_webhook",
    summary="Receive FindAll webhook events"
)
async def receive_findall_webhook(
    request: Request,
    x_parallel_signature: Optional[str] = Header(None, alias="X-Parallel-Signature"),
    x_parallel_timestamp: Optional[str] = Header(None, alias="X-Parallel-Timestamp"),
):
    """
    Receive webhook events from Parallel.ai FindAll API.

    Supports events:
    - findall.candidate.generated
    - findall.candidate.matched
    - findall.candidate.unmatched
    - findall.candidate.enriched
    - findall.run.completed
    - findall.run.cancelled
    - findall.run.failed

    Security:
    - Verifies HMAC-SHA256 signature if webhook secret is configured
    - Validates timestamp to prevent replay attacks
    """
    # Read raw body for signature verification
    body = await request.body()

    # Verify signature if secret is configured
    if hasattr(settings, 'webhook_secret') and settings.webhook_secret:
        if not x_parallel_signature:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-Parallel-Signature header"
            )

        try:
            verifier = WebhookVerifier(settings.webhook_secret)
            verifier.verify_signature(
                body,
                x_parallel_signature,
                x_parallel_timestamp
            )
        except WebhookVerificationError as e:
            logger.warning(f"Webhook verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Signature verification failed: {str(e)}"
            )

    # Parse event
    try:
        event_data = json.loads(body)
        event_type = event_data.get("type")

        # Validate event type
        try:
            event_type_enum = WebhookEventType(event_type)
        except ValueError:
            logger.warning(f"Unknown webhook event type: {event_type}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown event type: {event_type}"
            )

        # Parse into appropriate model
        if event_type.startswith("findall.candidate"):
            event = FindAllCandidateEvent(**event_data)
        elif event_type.startswith("findall.run"):
            event = FindAllRunEvent(**event_data)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unexpected event type for FindAll webhook: {event_type}"
            )

        # Handle event
        default_handler.handle_event(event)

        logger.info(f"Successfully processed FindAll webhook: {event_type}")

        return {
            "status": "success",
            "event_type": event_type,
            "timestamp": event.timestamp
        }

    except ValidationError as e:
        logger.error(f"Invalid webhook payload: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid webhook payload: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing webhook"
        )


@router.post(
    "/tasks",
    status_code=status.HTTP_200_OK,
    operation_id="receive_task_webhook",
    summary="Receive Task webhook events"
)
async def receive_task_webhook(
    request: Request,
    x_parallel_signature: Optional[str] = Header(None, alias="X-Parallel-Signature"),
    x_parallel_timestamp: Optional[str] = Header(None, alias="X-Parallel-Timestamp"),
):
    """
    Receive webhook events from Parallel.ai Task API.

    Supports events:
    - task.run.completed
    - task.run.failed
    - task.run.cancelled

    Security:
    - Verifies HMAC-SHA256 signature if webhook secret is configured
    - Validates timestamp to prevent replay attacks
    """
    # Read raw body for signature verification
    body = await request.body()

    # Verify signature if secret is configured
    if hasattr(settings, 'webhook_secret') and settings.webhook_secret:
        if not x_parallel_signature:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-Parallel-Signature header"
            )

        try:
            verifier = WebhookVerifier(settings.webhook_secret)
            verifier.verify_signature(
                body,
                x_parallel_signature,
                x_parallel_timestamp
            )
        except WebhookVerificationError as e:
            logger.warning(f"Webhook verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Signature verification failed: {str(e)}"
            )

    # Parse event
    try:
        event_data = json.loads(body)
        event_type = event_data.get("type")

        # Validate event type
        try:
            event_type_enum = WebhookEventType(event_type)
        except ValueError:
            logger.warning(f"Unknown webhook event type: {event_type}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown event type: {event_type}"
            )

        # Parse into task event model
        event = TaskRunEvent(**event_data)

        # Handle event
        default_handler.handle_event(event)

        logger.info(f"Successfully processed Task webhook: {event_type}")

        return {
            "status": "success",
            "event_type": event_type,
            "timestamp": event.timestamp
        }

    except ValidationError as e:
        logger.error(f"Invalid webhook payload: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid webhook payload: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing webhook"
        )


@router.get(
    "/status",
    operation_id="webhook_status",
    summary="Get webhook handler status"
)
async def get_webhook_status() -> Dict:
    """
    Get webhook handler status and statistics.

    Returns:
    - Number of registered handlers per event type
    - Configuration status
    """
    # Count handlers by type
    handler_counts = {}
    for event_type in WebhookEventType:
        count = default_handler.get_handler_count(event_type)
        if count > 0:
            handler_counts[event_type.value] = count

    global_count = len(default_handler._global_handlers)

    return {
        "status": "healthy",
        "webhook_secret_configured": hasattr(settings, 'webhook_secret') and bool(settings.webhook_secret),
        "signature_verification_enabled": hasattr(settings, 'webhook_secret') and bool(settings.webhook_secret),
        "handlers": {
            "global": global_count,
            "by_event_type": handler_counts,
            "total": default_handler.get_handler_count()
        }
    }
