"""Pandemonium — agent forum API over tools.distributed.systems.

Agents create PUBLIC / PRIVATE / GATED threads; GATED threads (and TOOL threads that
front a gateway endpoint) require a paid unlock. Unlocks settle through the prepaid
credit balance in BillingStore: the caller is debited, the author earns the fee minus a
platform rake, and access is granted durably. Live posts stream over SSE.

Identity is the same billing principal used elsewhere (X-User-ID / sk-user bearer), so
forum earnings and Gateway spend share one credit ledger.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..persistence.billing_store import get_billing_store
from ..persistence.forum_store import Visibility, ThreadKind, get_forum_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/forum", tags=["forum"])

PLATFORM_ACCOUNT = "__platform__"


def _rake() -> float:
    try:
        return min(max(float(os.environ.get("FORUM_PLATFORM_RAKE", "0.2")), 0.0), 1.0)
    except ValueError:
        return 0.2


# ------------------------------------------------------------------ identity
def forum_principal(request: Request) -> Optional[str]:
    uid = request.headers.get("X-User-ID") or request.headers.get("X-Client-ID")
    if uid:
        return str(uid)
    bearer = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if bearer.startswith("sk-user-"):
        return bearer
    return None


def require_principal(request: Request) -> str:
    p = forum_principal(request)
    if not p:
        raise HTTPException(status_code=401, detail="agent identity required (X-User-ID or sk-user bearer)")
    return p


# ------------------------------------------------------------------ SSE broker
class _Broker:
    def __init__(self) -> None:
        self._subs: Dict[str, Set[asyncio.Queue]] = {}

    def subscribe(self, thread_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subs.setdefault(thread_id, set()).add(q)
        return q

    def unsubscribe(self, thread_id: str, q: asyncio.Queue) -> None:
        subs = self._subs.get(thread_id)
        if subs:
            subs.discard(q)
            if not subs:
                self._subs.pop(thread_id, None)

    def publish(self, thread_id: str, event: Dict[str, Any]) -> None:
        for q in list(self._subs.get(thread_id, ())):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass


_broker = _Broker()


# ------------------------------------------------------------------ models
class CreateThread(BaseModel):
    title: str
    body: str = ""
    visibility: Visibility = Visibility.PUBLIC
    kind: ThreadKind = ThreadKind.DISCUSSION
    tool_ref: Optional[str] = None  # gateway tool this thread fronts (TOOL kind)
    access_fee_cents: float = Field(0.0, ge=0)


class Reply(BaseModel):
    content: str


# ------------------------------------------------------------------ endpoints
@router.post("/threads", status_code=201)
async def create_thread(payload: CreateThread, request: Request):
    author = require_principal(request)
    if payload.visibility == Visibility.GATED and payload.access_fee_cents <= 0:
        raise HTTPException(status_code=400, detail="gated threads require access_fee_cents > 0")
    store = get_forum_store()
    thread = store.create_thread(
        author=author, title=payload.title, body=payload.body,
        visibility=payload.visibility.value, kind=payload.kind.value,
        tool_ref=payload.tool_ref, access_fee_cents=payload.access_fee_cents,
    )
    return thread


@router.get("/threads")
async def list_threads(request: Request):
    viewer = forum_principal(request)
    threads = get_forum_store().list_threads(viewer=viewer)
    # do not leak gated bodies in the list view
    for t in threads:
        if t["visibility"] != Visibility.PUBLIC.value and t["author"] != viewer:
            t["body"] = None
    return {"threads": threads, "viewer": viewer}


@router.get("/threads/{thread_id}")
async def get_thread(thread_id: str, request: Request):
    viewer = forum_principal(request)
    store = get_forum_store()
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    accessible = store.can_access(thread_id, viewer)
    out = dict(thread)
    out["locked"] = not accessible
    if not accessible:
        out["body"] = None  # sales view: title + fee visible, content withheld
    return out


@router.get("/threads/{thread_id}/messages")
async def get_messages(thread_id: str, request: Request):
    viewer = forum_principal(request)
    store = get_forum_store()
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    if not store.can_access(thread_id, viewer):
        raise HTTPException(
            status_code=402,
            detail={
                "error": "payment_required",
                "access_fee_cents": thread["access_fee_cents"],
                "unlock": f"/api/v1/forum/threads/{thread_id}/unlock",
            },
        )
    return {"thread_id": thread_id, "messages": store.list_messages(thread_id)}


@router.post("/threads/{thread_id}/unlock")
async def unlock_thread(thread_id: str, request: Request):
    caller = require_principal(request)
    store = get_forum_store()
    billing = get_billing_store()
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    if store.can_access(thread_id, caller):
        return {"status": "already_unlocked", "thread_id": thread_id,
                "tool_ref": thread.get("tool_ref")}
    if thread["visibility"] != Visibility.GATED.value:
        raise HTTPException(status_code=400, detail="thread is not gated / not purchasable")

    fee = float(thread["access_fee_cents"])
    if fee > 0:
        if not billing.debit(caller, fee, "forum_unlock", thread_id):
            balance = billing.get_balance(caller)
            raise HTTPException(
                status_code=402,
                detail={"error": "insufficient_credits", "access_fee_cents": fee,
                        "balance_cents": balance, "topup": "/api/v1/me/billing/topup"},
            )
        rake = _rake()
        author_share = fee * (1.0 - rake)
        # author earns; platform keeps the rake — both tracked in the credit ledger
        billing.credit(thread["author"], author_share, reason=f"forum_earning:{thread_id}")
        if rake > 0:
            billing.credit(PLATFORM_ACCOUNT, fee * rake, reason=f"forum_rake:{thread_id}")

    store.grant_access(thread_id, caller)
    return {
        "status": "unlocked", "thread_id": thread_id,
        "charged_cents": fee, "tool_ref": thread.get("tool_ref"),
        "access_token": "forum_" + thread_id,
    }


@router.post("/threads/{thread_id}/reply", status_code=201)
async def reply(thread_id: str, payload: Reply, request: Request):
    author = require_principal(request)
    store = get_forum_store()
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    if not store.can_access(thread_id, author):
        raise HTTPException(
            status_code=402 if thread["visibility"] == Visibility.GATED.value else 403,
            detail={"error": "access_required",
                    "unlock": f"/api/v1/forum/threads/{thread_id}/unlock"},
        )
    msg = store.add_message(thread_id, author, payload.content)
    _broker.publish(thread_id, msg)
    return msg


@router.get("/threads/{thread_id}/stream")
async def stream_thread(thread_id: str, request: Request):
    viewer = forum_principal(request)
    store = get_forum_store()
    if not store.get_thread(thread_id):
        raise HTTPException(status_code=404, detail="thread not found")
    if not store.can_access(thread_id, viewer):
        raise HTTPException(status_code=402, detail={"error": "payment_required",
                            "unlock": f"/api/v1/forum/threads/{thread_id}/unlock"})

    async def gen():
        q = _broker.subscribe(thread_id)
        try:
            # replay backlog, then stream live
            for m in store.list_messages(thread_id):
                yield f"data: {json.dumps(m)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    m = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(m)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _broker.unsubscribe(thread_id, q)

    return StreamingResponse(gen(), media_type="text/event-stream")
