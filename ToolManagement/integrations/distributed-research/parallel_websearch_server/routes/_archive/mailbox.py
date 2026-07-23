"""Routes for the agent mailbox notification system."""

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field, constr

from parallel_websearch_server.mailbox import MailboxDelivery, get_mailbox_manager

router = APIRouter(prefix="/v1/matching/mailbox", tags=["Matching Mailbox"])


class MailboxAckRequest(BaseModel):
    """Request payload for acknowledging mailbox messages."""

    message_ids: list[constr(strip_whitespace=True, min_length=1)] = Field(
        ..., description="List of message IDs to acknowledge", min_length=1, max_length=100
    )


class MailboxAckResponse(BaseModel):
    """Response after acknowledging mailbox messages."""

    acknowledged: int
    pending_messages: int


@router.get(
    "/{channel_id}",
    response_model=MailboxDelivery,
    summary="Fetch mailbox messages",
)
async def fetch_mailbox_messages(
    channel_id: str = Path(..., description="Mailbox channel identifier"),
    max_messages: int = Query(20, ge=1, le=100, description="Maximum number of messages to return"),
    auto_ack: bool = Query(
        False,
        description="If true, automatically acknowledges delivered messages",
    ),
):
    """
    Retrieve pending notifications for a mailbox channel.

    Agents can long-poll this endpoint (or call it periodically)
    to retrieve completion notices without keeping an SSE stream open.
    """
    manager = get_mailbox_manager()

    try:
        delivery = await manager.pull(channel_id, max_messages=max_messages, auto_ack=auto_ack)
    except ValueError as exc:  # max_messages validation
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return delivery


@router.post(
    "/{channel_id}/ack",
    response_model=MailboxAckResponse,
    summary="Acknowledge mailbox messages",
)
async def acknowledge_mailbox_messages(
    payload: MailboxAckRequest,
    channel_id: str = Path(..., description="Mailbox channel identifier"),
):
    """Delete/acknowledge messages from a mailbox channel."""
    manager = get_mailbox_manager()
    acknowledged, pending = await manager.ack(channel_id, payload.message_ids)

    return MailboxAckResponse(acknowledged=acknowledged, pending_messages=pending)
