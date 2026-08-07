"""
Customer billing + resource provisioning (the DigitalOcean-style account flow):
sign up → put a card on file → provision resources → metered usage billed.

Stripe Checkout creates the paid subscription, Stripe's customer portal manages
it, and signature-verified webhooks are the authority for product entitlement.
The local synthetic billing path is disabled unless BILLING_DEV_MODE is explicit.
"""

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..persistence.billing_store import (
    RESOURCE_TYPES,
    PRICE_SHEET,
    USAGE_PRICE_SHEET,
    get_billing_store,
)
from .user_routes import _require_user, public_base_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["billing"])

STRIPE_API = "https://api.stripe.com/v1"
DEFAULT_STRIPE_API_VERSION = "2026-07-29.dahlia"
STRIPE_SIGNATURE_TOLERANCE_SECONDS = 300
MAX_STRIPE_WEBHOOK_BYTES = 1024 * 1024


def _stripe_key() -> Optional[str]:
    return os.environ.get("STRIPE_SECRET_KEY") or None


def _stripe_price_id() -> Optional[str]:
    return os.environ.get("STRIPE_PRICE_ID") or None


def _stripe_webhook_secret() -> Optional[str]:
    return os.environ.get("STRIPE_WEBHOOK_SECRET") or None


def _stripe_api_version() -> str:
    return os.environ.get("STRIPE_API_VERSION", DEFAULT_STRIPE_API_VERSION)


def _billing_dev_mode() -> bool:
    return os.environ.get("BILLING_DEV_MODE", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _stable_idempotency_key(kind: str, *parts: str) -> str:
    material = "\x1f".join(str(part) for part in parts)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"dsco-{kind}-{digest[:48]}"


def _request_attempt(request: Request, *, fallback: str) -> str:
    # Hashing prevents a caller from accidentally placing PII in Stripe's logs.
    return request.headers.get("idempotency-key") or fallback


async def _stripe(
    method: str,
    path: str,
    data: Optional[Dict[str, Any]] = None,
    *,
    idempotency_key: Optional[str] = None,
) -> Dict[str, Any]:
    key = _stripe_key()
    if not key:
        raise HTTPException(status_code=503, detail="Stripe billing is not configured")
    headers = {"Stripe-Version": _stripe_api_version()}
    if method.upper() == "POST":
        headers["Idempotency-Key"] = idempotency_key or _stable_idempotency_key(
            "request", path, str(uuid.uuid4())
        )
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.request(
            method,
            f"{STRIPE_API}{path}",
            data=data,
            auth=(key, ""),
            headers=headers,
        )
    try:
        body = r.json()
    except ValueError:
        body = {}
    if r.status_code >= 400:
        message = (
            body.get("error", {}).get("message", r.status_code)
            if isinstance(body, dict) else r.status_code
        )
        raise HTTPException(
            status_code=502,
            detail=f"Stripe error: {message}",
        )
    if not isinstance(body, dict):
        raise HTTPException(status_code=502, detail="Stripe returned an invalid response")
    return body


async def _get_or_create_stripe_customer(
    user: Dict[str, Any], store: Any
) -> Dict[str, Any]:
    customer = store.get_customer(user["user_id"])
    if customer and customer.get("provider_customer_id") and not customer.get("dev_mode"):
        return customer
    stripe_customer = await _stripe(
        "POST",
        "/customers",
        {
            "email": user["email"],
            "name": user.get("display_name") or "",
            "metadata[user_id]": user["user_id"],
        },
        idempotency_key=_stable_idempotency_key("customer", user["user_id"]),
    )
    return store.upsert_customer(
        user["user_id"],
        provider="stripe",
        provider_customer_id=stripe_customer["id"],
        billing_ready=False,
        dev_mode=False,
    )


def _ensure_dev_entitlement(user_id: str, store: Any) -> Dict[str, Any]:
    if not _billing_dev_mode():
        raise HTTPException(status_code=503, detail="Billing is not configured")
    customer = store.get_customer(user_id)
    # Never erase a durable Stripe customer mapping merely because a developer
    # temporarily enabled the local billing lane.
    if not customer or customer.get("provider") != "stripe":
        store.upsert_customer(
            user_id,
            provider="dev",
            provider_customer_id=None,
            card_brand="dev",
            card_last4="0000",
            billing_ready=True,
            dev_mode=True,
        )
    return store.upsert_subscription(
        user_id,
        provider="dev",
        provider_customer_id=None,
        provider_subscription_id="dev_" + hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:24],
        price_id="dev",
        status="active",
        access_until=None,
        cancel_at_period_end=False,
    )


def require_paid_entitlement(user_id: str, store: Optional[Any] = None) -> Dict[str, Any]:
    """Reusable paid-access gate for ToolManagement execution surfaces."""
    billing_store = store or get_billing_store()
    if not billing_store.has_paid_entitlement(user_id, allow_dev=_billing_dev_mode()):
        raise HTTPException(
            status_code=402,
            detail="An active paid subscription is required. Start Checkout from /account.",
        )
    return billing_store.get_subscription(user_id)


def _verify_stripe_signature(
    payload: bytes,
    signature_header: str,
    secret: str,
    *,
    now: Optional[float] = None,
    tolerance: int = STRIPE_SIGNATURE_TOLERANCE_SECONDS,
) -> None:
    timestamps = []
    signatures = []
    for component in signature_header.split(","):
        key, separator, value = component.strip().partition("=")
        if not separator:
            continue
        if key == "t":
            try:
                timestamps.append(int(value))
            except ValueError:
                continue
        elif key == "v1":
            signatures.append(value)
    if not timestamps or not signatures:
        raise ValueError("Malformed Stripe-Signature header")
    timestamp = timestamps[-1]
    clock = time.time() if now is None else now
    if abs(float(clock) - timestamp) > tolerance:
        raise ValueError("Stripe webhook timestamp is outside the allowed tolerance")
    expected = hmac.new(
        secret.encode("utf-8"),
        str(timestamp).encode("ascii") + b"." + payload,
        hashlib.sha256,
    ).hexdigest()
    if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
        raise ValueError("Invalid Stripe webhook signature")


def _validate_event_mode(event: Dict[str, Any]) -> None:
    key = _stripe_key() or ""
    expected_livemode: Optional[bool] = None
    if key.startswith(("sk_live_", "rk_live_")):
        expected_livemode = True
    elif key.startswith(("sk_test_", "rk_test_")):
        expected_livemode = False
    if expected_livemode is not None and bool(event.get("livemode")) != expected_livemode:
        raise ValueError("Stripe event mode does not match the configured API key")


# ==================== Billing ====================

@router.get("/me/billing")
async def billing_status(request: Request):
    user = _require_user(request)
    store = get_billing_store()
    customer = store.get_customer(user["user_id"])
    subscription = store.get_subscription(user["user_id"])
    entitled = store.has_paid_entitlement(
        user["user_id"], allow_dev=_billing_dev_mode()
    )
    usage = store.usage_summary(user["user_id"])
    return {
        "user_id": user["user_id"],
        # Preserve the legacy field name, but make it mean what callers need:
        # whether this account may consume paid ToolManagement resources.
        "billing_ready": entitled,
        "entitled": entitled,
        "payment_method_ready": bool(customer and customer["billing_ready"]),
        "provider_customer_id": customer.get("provider_customer_id") if customer else None,
        "card": (
            {"brand": customer["card_brand"], "last4": customer["card_last4"],
             "dev_mode": customer["dev_mode"]}
            if customer and customer["billing_ready"] else None
        ),
        "subscription": subscription,
        "provider_configured": bool(_stripe_key() and _stripe_price_id()),
        "webhook_configured": bool(_stripe_webhook_secret()),
        "dev_mode": _billing_dev_mode(),
        "month_to_date": usage,
        "price_sheet": PRICE_SHEET,
    }


@router.post("/me/billing/checkout-session")
async def create_checkout_session(request: Request):
    """Create a Stripe-hosted fixed-price subscription Checkout Session."""
    user = _require_user(request)
    store = get_billing_store()
    price_id = _stripe_price_id()

    if not (_stripe_key() and price_id):
        subscription = _ensure_dev_entitlement(user["user_id"], store)
        return {
            "billing_ready": True,
            "dev_mode": True,
            "subscription": subscription,
            "note": "BILLING_DEV_MODE granted a synthetic local subscription.",
        }

    existing = store.get_subscription(user["user_id"])
    if existing and existing.get("provider") == "dev":
        store.upsert_subscription(user["user_id"], status="canceled")
        existing = None
    if existing and existing.get("status") not in {"canceled", "incomplete_expired"}:
        raise HTTPException(
            status_code=409,
            detail="This account already has a subscription. Use the billing portal to manage it.",
        )

    base = public_base_url()
    if not _billing_dev_mode() and not base.startswith("https://"):
        raise HTTPException(status_code=503, detail="API_BASE_URL must use HTTPS for live billing")
    customer = await _get_or_create_stripe_customer(user, store)
    # One stable Checkout operation per account/price/day prevents transport
    # retries and repeated button clicks from creating parallel subscriptions.
    attempt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    session = await _stripe(
        "POST",
        "/checkout/sessions",
        {
            "mode": "subscription",
            "customer": customer["provider_customer_id"],
            "line_items[0][price]": price_id,
            "line_items[0][quantity]": "1",
            "client_reference_id": user["user_id"],
            "metadata[user_id]": user["user_id"],
            "metadata[price_id]": price_id,
            "subscription_data[metadata][user_id]": user["user_id"],
            "subscription_data[metadata][price_id]": price_id,
            "success_url": f"{base}/account?checkout=success&session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base}/account?checkout=cancelled",
        },
        idempotency_key=_stable_idempotency_key(
            "checkout", user["user_id"], price_id, attempt
        ),
    )
    return {
        "checkout_url": session["url"],
        "session_id": session["id"],
        "dev_mode": False,
    }


# ==================== Prepaid credits (usage-based Gateway / model access) =========

class TopupRequest(BaseModel):
    amount_usd: float = Field(..., gt=0, le=10000, description="Credits to purchase, in USD")


@router.get("/me/billing/credits")
async def get_credits(request: Request):
    """Current prepaid credit balance and the usage-based price sheet."""
    user = _require_user(request)
    store = get_billing_store()
    balance_cents = store.get_balance(user["user_id"])
    return {
        "user_id": user["user_id"],
        "balance_cents": balance_cents,
        "balance_usd": round(balance_cents / 100.0, 4),
        "usage_prices_cents": USAGE_PRICE_SHEET,
        "enforced": os.environ.get("USAGE_BILLING_ENFORCE", "").strip().lower()
        in {"1", "true", "yes", "on"},
    }


@router.post("/me/billing/topup")
async def topup_credits(payload: TopupRequest, request: Request):
    """Buy prepaid Gateway/model credits.

    Live: a Stripe one-time Checkout (mode=payment); the signature-verified webhook
    credits the balance idempotently. Dev (BILLING_DEV_MODE, no Stripe key): credits
    immediately so the flow is testable end-to-end without a card.
    """
    user = _require_user(request)
    store = get_billing_store()
    credit_cents = round(payload.amount_usd * 100)

    if not _stripe_key():
        if not _billing_dev_mode():
            raise HTTPException(status_code=503, detail="Stripe billing is not configured")
        new_balance = store.credit(user["user_id"], credit_cents, reason="dev_topup")
        return {"dev_mode": True, "credited_cents": credit_cents, "balance_cents": new_balance}

    base = public_base_url()
    if not _billing_dev_mode() and not base.startswith("https://"):
        raise HTTPException(status_code=503, detail="API_BASE_URL must use HTTPS for live billing")
    customer = await _get_or_create_stripe_customer(user, store)
    attempt = _request_attempt(request, fallback=str(uuid.uuid4()))
    session = await _stripe(
        "POST",
        "/checkout/sessions",
        {
            "mode": "payment",
            "customer": customer["provider_customer_id"],
            "line_items[0][price_data][currency]": "usd",
            "line_items[0][price_data][unit_amount]": str(credit_cents),
            "line_items[0][price_data][product_data][name]": "DSCO Gateway credits",
            "line_items[0][quantity]": "1",
            "client_reference_id": user["user_id"],
            "metadata[user_id]": user["user_id"],
            "metadata[kind]": "credit_topup",
            "metadata[credit_cents]": str(credit_cents),
            "payment_intent_data[metadata][user_id]": user["user_id"],
            "payment_intent_data[metadata][kind]": "credit_topup",
            "payment_intent_data[metadata][credit_cents]": str(credit_cents),
            "success_url": f"{base}/account?topup=success&session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base}/account?topup=cancelled",
        },
        idempotency_key=_stable_idempotency_key("topup", user["user_id"], str(credit_cents), attempt),
    )
    return {
        "checkout_url": session["url"],
        "session_id": session["id"],
        "credit_cents": credit_cents,
        "dev_mode": False,
    }


@router.post("/me/billing/portal-session")
async def create_portal_session(request: Request):
    """Create a fresh, short-lived Stripe customer-portal URL."""
    user = _require_user(request)
    store = get_billing_store()
    if not _stripe_key():
        raise HTTPException(status_code=503, detail="Stripe billing is not configured")
    customer = store.get_customer(user["user_id"])
    if not customer or not customer.get("provider_customer_id") or customer.get("dev_mode"):
        raise HTTPException(status_code=409, detail="No Stripe customer exists for this account")
    base = public_base_url()
    if not _billing_dev_mode() and not base.startswith("https://"):
        raise HTTPException(status_code=503, detail="API_BASE_URL must use HTTPS for live billing")
    attempt = _request_attempt(request, fallback=str(uuid.uuid4()))
    session = await _stripe(
        "POST",
        "/billing_portal/sessions",
        {
            "customer": customer["provider_customer_id"],
            "return_url": f"{base}/account",
        },
        idempotency_key=_stable_idempotency_key("portal", user["user_id"], attempt),
    )
    return {"portal_url": session["url"], "dev_mode": False}


@router.post("/billing/webhook")
async def stripe_webhook(request: Request):
    """Receive the Stripe events that authoritatively drive paid access."""
    secret = _stripe_webhook_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe webhook is not configured")
    payload = await request.body()
    if len(payload) > MAX_STRIPE_WEBHOOK_BYTES:
        raise HTTPException(status_code=413, detail="Stripe webhook payload is too large")
    try:
        _verify_stripe_signature(
            payload,
            request.headers.get("stripe-signature", ""),
            secret,
        )
        event = json.loads(payload)
        if not isinstance(event, dict):
            raise ValueError("Stripe event must be an object")
        _validate_event_mode(event)
        result = get_billing_store().apply_stripe_event(
            event,
            payload_hash=hashlib.sha256(payload).hexdigest(),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("Rejected Stripe webhook: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid Stripe webhook") from exc
    return {"received": True, **result}


@router.post("/me/billing/add-card")
async def add_card(request: Request):
    """Start card capture. With Stripe configured: a setup-mode Checkout URL.
    Without Stripe, this is available only under explicit BILLING_DEV_MODE."""
    user = _require_user(request)
    store = get_billing_store()
    base = public_base_url()

    if not _stripe_key():
        subscription = _ensure_dev_entitlement(user["user_id"], store)
        return {
            "billing_ready": True,
            "dev_mode": True,
            "subscription": subscription,
            "note": "BILLING_DEV_MODE granted a synthetic local subscription.",
        }

    if not _billing_dev_mode() and not base.startswith("https://"):
        raise HTTPException(status_code=503, detail="API_BASE_URL must use HTTPS for live billing")
    customer = await _get_or_create_stripe_customer(user, store)

    session = await _stripe(
        "POST", "/checkout/sessions",
        {
            "mode": "setup",
            "customer": customer["provider_customer_id"],
            "payment_method_types[0]": "card",
            "success_url": f"{base}/api/v1/me/billing/confirm-card?session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base}/account",
        },
        idempotency_key=_stable_idempotency_key(
            "setup",
            user["user_id"],
            _request_attempt(
                request,
                fallback=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            ),
        ),
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
            idempotency_key=_stable_idempotency_key(
                "default-payment-method", customer_id, str(pm_id)
            ),
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
    if not meta.get("live_capable") or meta.get("addon_monthly_cents") is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{meta['display_name']} is catalog-only today and cannot be sold as "
                "a managed integration add-on."
            ),
        )

    require_paid_entitlement(user["user_id"], store)
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

    require_paid_entitlement(user["user_id"], store)

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
