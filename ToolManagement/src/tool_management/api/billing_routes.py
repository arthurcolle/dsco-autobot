"""
Customer billing + resource provisioning (the DigitalOcean-style account flow):
sign up → put a card on file → provision resources → metered usage billed.

Stripe powers real cards when STRIPE_SECRET_KEY is set (setup-mode Checkout via
the REST API — no SDK). Without it, a clearly-labeled dev mode lets flows be
exercised end to end; setting the env var flips the same endpoints live.
"""

import logging
import os
import time
import uuid
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..persistence.billing_store import (
    RESOURCE_TYPES,
    PRICE_SHEET,
    get_billing_store,
)
from .user_routes import _require_user, public_base_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["billing"])

STRIPE_API = "https://api.stripe.com/v1"


def _stripe_key() -> Optional[str]:
    return os.environ.get("STRIPE_SECRET_KEY") or None


async def _stripe(method: str, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.request(
            method,
            f"{STRIPE_API}{path}",
            data=data,
            auth=(_stripe_key(), ""),
        )
    body = r.json()
    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Stripe error: {body.get('error', {}).get('message', r.status_code)}",
        )
    return body


# ==================== Billing ====================

@router.get("/me/billing")
async def billing_status(request: Request):
    user = _require_user(request)
    store = get_billing_store()
    customer = store.get_customer(user["user_id"])
    usage = store.usage_summary(user["user_id"])
    return {
        "user_id": user["user_id"],
        "billing_ready": bool(customer and customer["billing_ready"]),
        "card": (
            {"brand": customer["card_brand"], "last4": customer["card_last4"],
             "dev_mode": customer["dev_mode"]}
            if customer and customer["billing_ready"] else None
        ),
        "provider_configured": bool(_stripe_key()),
        "month_to_date": usage,
        "price_sheet": PRICE_SHEET,
    }


@router.post("/me/billing/add-card")
async def add_card(request: Request):
    """Start card capture. With Stripe configured: a setup-mode Checkout URL.
    Without: registers a dev-mode card so the account flow works end to end."""
    user = _require_user(request)
    store = get_billing_store()
    base = public_base_url()

    if not _stripe_key():
        store.upsert_customer(
            user["user_id"],
            provider="dev",
            card_brand="dev",
            card_last4="0000",
            billing_ready=True,
            dev_mode=True,
        )
        return {
            "billing_ready": True,
            "dev_mode": True,
            "note": "No STRIPE_SECRET_KEY configured — dev card recorded. "
                    "Set the env var in deploy/modal/modal_app.py to take real cards.",
        }

    customer = store.get_customer(user["user_id"])
    if not customer or not customer.get("provider_customer_id") or customer.get("dev_mode"):
        sc = await _stripe(
            "POST", "/customers",
            {"email": user["email"], "name": user.get("display_name") or "",
             "metadata[user_id]": user["user_id"]},
        )
        customer = store.upsert_customer(
            user["user_id"], provider="stripe",
            provider_customer_id=sc["id"], dev_mode=False,
        )

    session = await _stripe(
        "POST", "/checkout/sessions",
        {
            "mode": "setup",
            "customer": customer["provider_customer_id"],
            "payment_method_types[0]": "card",
            "success_url": f"{base}/api/v1/me/billing/confirm-card?session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base}/account",
        },
    )
    return {"checkout_url": session["url"], "session_id": session["id"]}


@router.get("/me/billing/confirm-card")
async def confirm_card(session_id: str = Query(...)):
    """Stripe success redirect — records the captured card and bounces to /account."""
    from fastapi.responses import RedirectResponse

    if not _stripe_key():
        raise HTTPException(status_code=409, detail="Stripe not configured")

    session = await _stripe("GET", f"/checkout/sessions/{session_id}")
    setup_intent_id = session.get("setup_intent")
    customer_id = session.get("customer")
    if not setup_intent_id or not customer_id:
        raise HTTPException(status_code=400, detail="Checkout session incomplete")

    si = await _stripe("GET", f"/setup_intents/{setup_intent_id}")
    pm_id = si.get("payment_method")
    card = {}
    if pm_id:
        pm = await _stripe("GET", f"/payment_methods/{pm_id}")
        card = pm.get("card") or {}
        await _stripe(
            "POST", f"/customers/{customer_id}",
            {"invoice_settings[default_payment_method]": pm_id},
        )

    store = get_billing_store()
    # Locate the user by stripe customer id
    from ..persistence.user_store import get_user_store

    target_user = None
    for u in get_user_store().list_users(limit=500):
        c = store.get_customer(u["user_id"])
        if c and c.get("provider_customer_id") == customer_id:
            target_user = u
            break
    if target_user:
        store.upsert_customer(
            target_user["user_id"],
            card_brand=card.get("brand"),
            card_last4=card.get("last4"),
            billing_ready=True,
            dev_mode=False,
        )
    return RedirectResponse(url=f"{public_base_url()}/account")


@router.get("/me/usage")
async def my_usage(request: Request, days: int = Query(30, ge=1, le=365)):
    user = _require_user(request)
    return get_billing_store().usage_summary(
        user["user_id"], since_ts=time.time() - days * 86400
    )


# ==================== Managed integration add-ons ====================

@router.get("/me/addons")
async def list_addons(request: Request):
    user = _require_user(request)
    store = get_billing_store()
    from ..integrations.provider_catalog import PROVIDERS

    addons = [
        r for r in store.list_resources(user["user_id"])
        if r["type"] == "integration-addon"
    ]
    for a in addons:
        meta = PROVIDERS.get(a["name"], {})
        a["display_name"] = meta.get("display_name", a["name"])
        a["logo_url"] = meta.get("logo_url")
        a["monthly_cents"] = meta.get("addon_monthly_cents")
    return {"addons": addons}


@router.post("/me/addons/{provider}", status_code=201)
async def enable_addon(request: Request, provider: str):
    """Enable a managed integration: no API key needed — served from pooled
    platform credentials, billed monthly at the provider's tier price."""
    user = _require_user(request)
    provider = provider.strip().lower()
    store = get_billing_store()
    from ..integrations.provider_catalog import PROVIDERS

    meta = PROVIDERS.get(provider)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider}")

    customer = store.get_customer(user["user_id"])
    if not (customer and customer["billing_ready"]):
        raise HTTPException(
            status_code=402,
            detail="Add a payment method first (Billing section on /account).",
        )
    if provider in store.active_addon_providers(user["user_id"]):
        raise HTTPException(status_code=409, detail=f"{provider} add-on already enabled")

    resource = store.create_resource(
        user["user_id"], "integration-addon", provider,
        {"display_name": meta["display_name"],
         "monthly_cents": meta["addon_monthly_cents"]},
    )
    # Monthly fee is idempotent per calendar month: re-enabling an add-on you
    # already paid for this month costs nothing.
    already_paid = store.addon_charged_this_month(user["user_id"], provider)
    if not already_paid:
        store.record_usage(
            user["user_id"], "integration_addon_monthly", ref=provider,
            unit_price_cents=meta["addon_monthly_cents"],
        )
    return {
        **resource,
        "monthly_cents": meta["addon_monthly_cents"],
        "charged_now": not already_paid,
        "note": (
            f"{meta['display_name']} re-enabled — already paid for this month, no new charge."
            if already_paid else
            f"{meta['display_name']} tools now run against managed credentials — "
            f"no API key needed."
        ),
    }


@router.delete("/me/addons/{provider}")
async def disable_addon(request: Request, provider: str):
    user = _require_user(request)
    provider = provider.strip().lower()
    store = get_billing_store()
    for r in store.list_resources(user["user_id"]):
        if r["type"] == "integration-addon" and r["name"] == provider:
            store.destroy_resource(user["user_id"], r["resource_id"])
            return {
                "disabled": True,
                "provider": provider,
                "note": "Already-paid time is not refunded — re-enabling before the "
                        "end of the month is free.",
            }
    raise HTTPException(status_code=404, detail=f"No active add-on for {provider}")


class BillingAdjustment(BaseModel):
    user_id: str
    amount_cents: float = Field(description="Positive = charge, negative = credit")
    note: str = Field(min_length=3, max_length=200)


@router.post("/admin/billing/adjust")
async def admin_billing_adjust(request: Request, body: BillingAdjustment):
    """Operator-only manual charge/credit (correction line on the customer's bill)."""
    auth_header = request.headers.get("authorization", "")
    token = auth_header[7:] if auth_header.lower().startswith("bearer ") else ""
    from ..oauth import get_token_identity

    identity = get_token_identity(token)
    is_operator = bool(identity) and not identity.get("user_id") and identity.get(
        "client_id"
    ) in ("operator", "password-client")
    if not is_operator:
        raise HTTPException(status_code=403, detail="Operator token required")

    get_billing_store().record_usage(
        body.user_id, "billing_adjustment", ref=body.note,
        unit_price_cents=body.amount_cents,
    )
    return {"adjusted": True, "user_id": body.user_id, "amount_cents": body.amount_cents}


# ==================== Resources ====================

class CreateResourceRequest(BaseModel):
    type: str = Field(description=f"One of: {sorted(RESOURCE_TYPES)}")
    name: str = Field(min_length=1, max_length=80)
    config: Dict[str, Any] = Field(default_factory=dict)


@router.get("/resources/types")
async def resource_types():
    return {"types": RESOURCE_TYPES}


@router.get("/me/resources")
async def list_resources(request: Request):
    user = _require_user(request)
    return {"resources": get_billing_store().list_resources(user["user_id"])}


@router.post("/me/resources", status_code=201)
async def create_resource(request: Request, body: CreateResourceRequest):
    user = _require_user(request)
    store = get_billing_store()

    if body.type not in RESOURCE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown resource type {body.type!r}. Available: {sorted(RESOURCE_TYPES)}",
        )

    # DigitalOcean model: card on file before provisioning.
    customer = store.get_customer(user["user_id"])
    if not (customer and customer["billing_ready"]):
        raise HTTPException(
            status_code=402,
            detail="Add a payment method first: POST /api/v1/me/billing/add-card "
                   "(or the Billing section on /account).",
        )

    config = dict(body.config)

    if body.type == "swarm":
        try:
            from ..swarm.persistence import get_swarm_db

            swarm_id = str(uuid.uuid4())
            get_swarm_db().create_swarm(
                swarm_id=swarm_id,
                name=body.name,
                host_id=user["user_id"],
                host_wallet=config.get("host_wallet", ""),
                host_fee_rate=float(config.get("host_fee_rate", 0.05)),
                min_stake=float(config.get("min_stake", 0)),
                config={"owner_user_id": user["user_id"], "version": "2.5.0"},
            )
            config["swarm_id"] = swarm_id
            config["api"] = f"/api/v1/swarm/{swarm_id}"
        except Exception as e:
            logger.error(f"Swarm provisioning failed: {e}")
            raise HTTPException(status_code=500, detail=f"Swarm provisioning failed: {e}")

    resource = store.create_resource(user["user_id"], body.type, body.name, config)
    store.record_usage(user["user_id"], f"{body.type.replace('-', '_')}_monthly",
                       ref=resource["resource_id"])
    return resource


@router.delete("/me/resources/{resource_id}")
async def destroy_resource(request: Request, resource_id: str):
    user = _require_user(request)
    store = get_billing_store()
    resource = store.get_resource(user["user_id"], resource_id)
    if not resource or resource["status"] == "destroyed":
        raise HTTPException(status_code=404, detail=f"Resource not found: {resource_id}")

    if resource["type"] == "swarm" and resource["config"].get("swarm_id"):
        try:
            from ..swarm.persistence import get_swarm_db

            db = get_swarm_db()
            if hasattr(db, "delete_swarm"):
                db.delete_swarm(resource["config"]["swarm_id"])
        except Exception as e:
            logger.warning(f"Swarm teardown failed (resource still marked destroyed): {e}")

    store.destroy_resource(user["user_id"], resource_id)
    return {"destroyed": True, "resource_id": resource_id}
