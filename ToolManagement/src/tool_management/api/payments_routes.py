"""Unified agentic-payments API. Every rail tops up the same prepaid credit balance
(idempotent on the settlement's ext_ref), so agents can fund via card, stablecoin/x402,
Circle, Coinbase CDP, Lightning, or Cloudflare Pay and spend through the metered Gateway.

  GET  /api/v1/pay/providers            list enabled rails + capabilities
  POST /api/v1/pay/{provider}/topup     create a top-up intent (checkout_url / address / invoice / 402)
  POST /api/v1/pay/{provider}/settle    settle a proof/webhook -> credit balance (idempotent)
  GET  /api/v1/pay/x402/challenge       402 challenge for per-request USDC payment
  POST /api/v1/pay/ap2/authorize        verify an AP2 mandate (cross-rail authorization envelope)
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..payments import build_registry, verify_ap2_mandate
from ..persistence.billing_store import get_billing_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pay", tags=["payments"])


def _principal(request: Request) -> Optional[str]:
    uid = request.headers.get("X-User-ID") or request.headers.get("X-Client-ID")
    if uid:
        return str(uid)
    bearer = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return bearer if bearer.startswith("sk-user-") else None


def _require(request: Request) -> str:
    p = _principal(request)
    if not p:
        raise HTTPException(status_code=401, detail="agent identity required (X-User-ID or sk-user bearer)")
    return p


class TopupBody(BaseModel):
    amount_usd: float = Field(..., gt=0, le=100000)


@router.get("/providers")
async def list_providers():
    reg = build_registry()
    return {"providers": [
        {"name": p.name, "kind": p.kind, "capabilities": p.capabilities()} for p in reg.enabled()
    ]}


@router.post("/{provider}/topup")
async def create_topup(provider: str, body: TopupBody, request: Request):
    user = _require(request)
    p = build_registry().get(provider)
    if not p or not p.enabled():
        raise HTTPException(status_code=404, detail=f"payment provider not available: {provider}")
    intent = await p.create_topup(user, round(body.amount_usd * 100, 4), {})
    return intent.to_dict()


@router.post("/{provider}/settle")
async def settle(provider: str, request: Request):
    p = build_registry().get(provider)
    if not p or not p.enabled():
        raise HTTPException(status_code=404, detail=f"payment provider not available: {provider}")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    # allow the caller's identity to stand in for the paying user when the proof lacks one
    payload.setdefault("user_id", _principal(request))
    result = await p.settle(payload)
    if not result.ok:
        raise HTTPException(status_code=402, detail={"error": "settlement_failed", "reason": result.reason})
    if not result.user_id:
        raise HTTPException(status_code=400, detail="settlement did not resolve a paying user")
    balance = get_billing_store().credit(
        result.user_id, result.credited_cents, reason=result.reason or provider, ext_ref=result.ext_ref
    )
    return {"ok": True, "provider": provider, "credited_cents": result.credited_cents,
            "balance_cents": balance, "ext_ref": result.ext_ref}


@router.get("/x402/challenge")
async def x402_challenge(price_cents: float, resource: str = "gateway"):
    """HTTP 402 challenge for per-request USDC payment (agent signs, retries with proof)."""
    from ..payments.providers import X402Provider

    p = X402Provider()
    if not p.enabled():
        raise HTTPException(status_code=404, detail="x402 not available")
    return {"status": 402, **p.challenge(price_cents, resource)}


class AP2Body(BaseModel):
    mandate: dict


@router.post("/ap2/authorize")
async def ap2_authorize(body: AP2Body):
    """Verify a Google AP2 Intent/Cart Mandate (cross-rail authorization envelope)."""
    result = verify_ap2_mandate(body.mandate)
    if not result["ok"]:
        raise HTTPException(status_code=401, detail={"error": "mandate_rejected", "reason": result["reason"]})
    return result
