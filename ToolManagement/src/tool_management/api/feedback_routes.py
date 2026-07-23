"""
Integration Feedback API.

Lets AI agents/models that call our tools report back how the experience
went — correctness, schema clarity, missing capabilities, errors — so we
can improve the API. Every native MCP server exposes a matching
`integration_feedback` tool that forwards here.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..persistence import get_tool_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/integration-feedback", tags=["feedback"])

VALID_CATEGORIES = {
    "correctness", "schema_clarity", "performance", "error_handling",
    "documentation", "missing_capability", "rate_limit", "auth", "other",
}
VALID_OUTCOMES = {"success", "partial", "failure"}


class FeedbackRequest(BaseModel):
    backend_id: str = Field(..., description="Which integration server this feedback is about")
    message: str = Field(..., min_length=1, max_length=5000)
    tool_name: Optional[str] = Field(None, description="Tool this feedback concerns, or 'general'")
    outcome: Optional[str] = Field(None, description="success | partial | failure")
    category: str = Field("other", description="correctness | schema_clarity | performance | "
                          "error_handling | documentation | missing_capability | rate_limit | auth | other")
    rating: Optional[int] = Field(None, ge=1, le=5)
    suggested_fix: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None
    trace_id: Optional[str] = None


@router.post("")
async def submit_feedback(body: FeedbackRequest):
    if body.outcome and body.outcome not in VALID_OUTCOMES:
        raise HTTPException(422, f"outcome must be one of {sorted(VALID_OUTCOMES)}")
    category = body.category if body.category in VALID_CATEGORIES else "other"

    store = get_tool_store()
    result = store.record_feedback(
        backend_id=body.backend_id,
        message=body.message,
        tool_name=body.tool_name,
        outcome=body.outcome,
        category=category,
        rating=body.rating,
        suggested_fix=body.suggested_fix,
        context=body.context,
        session_id=body.session_id,
        trace_id=body.trace_id,
    )
    logger.info(f"Feedback recorded: backend={body.backend_id} tool={body.tool_name} "
                f"category={category} rating={body.rating}")
    return result


@router.get("")
async def list_feedback(
    backend_id: Optional[str] = Query(None),
    tool_name: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    store = get_tool_store()
    items, total = store.query_feedback(
        backend_id=backend_id, tool_name=tool_name, category=category,
        limit=limit, offset=offset,
    )
    return {"count": len(items), "total": total, "feedback": items}


@router.get("/stats")
async def feedback_stats():
    store = get_tool_store()
    return store.feedback_stats()
