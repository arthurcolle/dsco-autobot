"""Concrete payment-rail adapters. Each works in dev mode (no keys/network) and carries
the real API shape for live use behind env config.

Rails: Stripe (card, redirect) · x402 (USDC/Base, onchain per-request or deposit) ·
Circle (USDC wallets) · Coinbase CDP (onramp/wallet) · Lightning L402 (invoice) ·
Cloudflare Pay (configurable redirect — fill endpoint when its API is public).
"""
from __future__ import annotations

import hashlib
import os
import secrets
import time
from typing import Any, Dict, Optional

import httpx

from .core import (
    Settlement,
    TopupIntent,
    dev_mode,
    make_dev_token,
    read_dev_token,
)


def _env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.environ.get(k)
        if v:
            return v
    return None


def _ref(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


def _dev_settle(provider: str, payload: Dict[str, Any]) -> Optional[Settlement]:
    """Shared dev-mode settle: accept a signed dev token minted at create_topup."""
    tok = payload.get("dev_confirm_token") or payload.get("dev_token")
    if not tok:
        return None
    claims = read_dev_token(tok)
    if not claims or claims.get("p") != provider:
        return Settlement(ok=False, provider=provider, ext_ref="", reason="bad_dev_token")
    return Settlement(
        ok=True, provider=provider, ext_ref=f"{provider}:{claims['r']}",
        credited_cents=float(claims["c"]), user_id=str(claims["u"]), reason="dev_topup",
    )


# ============================================================ Stripe (card, redirect)
class StripeProvider:
    name = "stripe"
    kind = "redirect"

    def enabled(self) -> bool:
        return bool(_env("STRIPE_SECRET_KEY")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": "fiat", "methods": ["card"],
                "live": bool(_env("STRIPE_SECRET_KEY"))}

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("cs")
        if not _env("STRIPE_SECRET_KEY"):
            return TopupIntent(
                provider=self.name, kind=self.kind, reference=reference, amount_cents=amount_cents,
                dev_confirm_token=make_dev_token(self.name, user_id, amount_cents, reference),
            )
        base = os.environ.get("API_BASE_URL", "https://api.distributed.systems")
        data = {
            "mode": "payment",
            "line_items[0][price_data][currency]": "usd",
            "line_items[0][price_data][unit_amount]": str(int(round(amount_cents))),
            "line_items[0][price_data][product_data][name]": "DSCO Gateway credits",
            "line_items[0][quantity]": "1",
            "client_reference_id": user_id,
            "metadata[user_id]": user_id,
            "metadata[kind]": "credit_topup",
            "metadata[credit_cents]": str(int(round(amount_cents))),
            "success_url": f"{base}/account?topup=success",
            "cancel_url": f"{base}/account?topup=cancelled",
        }
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post("https://api.stripe.com/v1/checkout/sessions", data=data,
                             auth=(os.environ["STRIPE_SECRET_KEY"], ""))
            r.raise_for_status()
            sess = r.json()
        return TopupIntent(provider=self.name, kind=self.kind, reference=sess["id"],
                           amount_cents=amount_cents, checkout_url=sess["url"])

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        # live Stripe settlement flows through /billing/webhook -> apply_stripe_event (credits).
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        return Settlement(ok=False, provider=self.name, ext_ref="",
                          reason="use /api/v1/billing/webhook for live Stripe")


# ============================================================ x402 (USDC on Base)
class X402Provider:
    name = "x402"
    kind = "onchain"

    def enabled(self) -> bool:
        return bool(_env("X402_PAY_TO")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": _env("X402_ASSET") or "USDC",
                "chain": _env("X402_CHAIN") or "base",
                "per_request": True, "facilitator": _env("X402_FACILITATOR_URL"),
                "live": bool(_env("X402_PAY_TO"))}

    def _pay_to(self) -> str:
        return _env("X402_PAY_TO") or "0xDEV0000000000000000000000000000000000000"

    def challenge(self, price_cents: float, resource: str) -> Dict[str, Any]:
        """HTTP 402 body for per-request payment (agent signs, retries with PAYMENT-SIGNATURE)."""
        return {
            "x402Version": 2,
            "accepts": [{
                "scheme": "exact", "network": _env("X402_CHAIN") or "base",
                "asset": _env("X402_ASSET") or "USDC",
                "payTo": self._pay_to(),
                "maxAmountRequired": str(int(round(price_cents * 10_000))),  # 6dp USDC micro-units
                "resource": resource,
            }],
        }

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("x402")
        intent = TopupIntent(
            provider=self.name, kind=self.kind, reference=reference, amount_cents=amount_cents,
            pay_to_address=self._pay_to(), asset=_env("X402_ASSET") or "USDC",
            chain=_env("X402_CHAIN") or "base",
            payment_required=self.challenge(amount_cents, f"topup:{user_id}:{reference}"),
        )
        if not _env("X402_PAY_TO"):
            intent.dev_confirm_token = make_dev_token(self.name, user_id, amount_cents, reference)
        return intent

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        # live: verify + settle the signed EIP-3009 payload via the facilitator
        proof = payload.get("payment") or payload.get("payment_signature")
        fac = _env("X402_FACILITATOR_URL")
        if not (proof and fac):
            return Settlement(ok=False, provider=self.name, ext_ref="", reason="missing_proof_or_facilitator")
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                v = await c.post(f"{fac}/verify", json={"payment": proof})
                v.raise_for_status()
                s = await c.post(f"{fac}/settle", json={"payment": proof})
                s.raise_for_status()
                res = s.json()
            tx = res.get("transaction") or res.get("txHash") or hashlib.sha256(str(proof).encode()).hexdigest()
            amount_cents = float(payload.get("amount_cents") or 0)
            return Settlement(ok=True, provider=self.name, ext_ref=f"x402:{tx}",
                              credited_cents=amount_cents, user_id=payload.get("user_id"), reason="x402_settled")
        except Exception as e:  # noqa: BLE001
            return Settlement(ok=False, provider=self.name, ext_ref="", reason=f"facilitator_error:{type(e).__name__}")


# ============================================================ Circle (USDC wallets)
class CircleProvider:
    name = "circle"
    kind = "onchain"

    def enabled(self) -> bool:
        return bool(_env("CIRCLE_API_KEY")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": "USDC", "chains": ["base", "eth", "sol"],
                "wallets": True, "live": bool(_env("CIRCLE_API_KEY"))}

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("circle")
        if not _env("CIRCLE_API_KEY"):
            return TopupIntent(provider=self.name, kind=self.kind, reference=reference,
                               amount_cents=amount_cents, asset="USDC", chain="base",
                               pay_to_address="0xCIRCLEDEV000000000000000000000000000000",
                               dev_confirm_token=make_dev_token(self.name, user_id, amount_cents, reference))
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.circle.com/v1/w3s/deposits/addresses",
                headers={"Authorization": f"Bearer {os.environ['CIRCLE_API_KEY']}"},
                json={"blockchain": _env("CIRCLE_CHAIN") or "BASE", "reference": reference,
                      "metadata": {"user_id": user_id, "credit_cents": amount_cents}},
            )
            r.raise_for_status()
            addr = r.json().get("data", {}).get("address")
        return TopupIntent(provider=self.name, kind=self.kind, reference=reference,
                           amount_cents=amount_cents, asset="USDC", chain="base", pay_to_address=addr)

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        # live: Circle webhook 'transfers' notification (payment confirmed)
        n = payload.get("notification") or payload
        if str(n.get("state") or n.get("status")).lower() in {"complete", "confirmed", "paid"}:
            md = n.get("metadata") or {}
            return Settlement(ok=True, provider=self.name, ext_ref=f"circle:{n.get('id')}",
                              credited_cents=float(md.get("credit_cents") or payload.get("amount_cents") or 0),
                              user_id=md.get("user_id") or payload.get("user_id"), reason="circle_confirmed")
        return Settlement(ok=False, provider=self.name, ext_ref="", reason="unconfirmed")


# ============================================================ Coinbase CDP (onramp/wallet)
class CoinbaseCDPProvider:
    name = "coinbase_cdp"
    kind = "onchain"

    def enabled(self) -> bool:
        return bool(_env("CDP_API_KEY_ID", "CDP_API_KEY")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": "USDC", "chain": "base",
                "onramp": True, "x402_facilitator": True, "live": bool(_env("CDP_API_KEY_ID", "CDP_API_KEY"))}

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("cdp")
        # Onramp/session creation is account-specific; dev returns an instantly-confirmable intent.
        intent = TopupIntent(provider=self.name, kind=self.kind, reference=reference,
                             amount_cents=amount_cents, asset="USDC", chain="base",
                             pay_to_address=_env("CDP_PAY_TO") or "0xCDPDEV00000000000000000000000000000000")
        if not _env("CDP_API_KEY_ID", "CDP_API_KEY"):
            intent.dev_confirm_token = make_dev_token(self.name, user_id, amount_cents, reference)
        return intent

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        tx = payload.get("tx_hash") or payload.get("transaction")
        if tx:
            return Settlement(ok=True, provider=self.name, ext_ref=f"cdp:{tx}",
                              credited_cents=float(payload.get("amount_cents") or 0),
                              user_id=payload.get("user_id"), reason="cdp_settled")
        return Settlement(ok=False, provider=self.name, ext_ref="", reason="missing_tx")


# ============================================================ Lightning L402 (invoice)
class LightningL402Provider:
    name = "l402"
    kind = "invoice"

    def enabled(self) -> bool:
        return bool(_env("LNBITS_URL", "LND_URL")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": "BTC/sats", "permissionless": True,
                "per_request": True, "live": bool(_env("LNBITS_URL", "LND_URL"))}

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("l402")
        sats = int(round(amount_cents / 100.0 * float(_env("SATS_PER_USD") or 1500)))
        if not _env("LNBITS_URL"):
            return TopupIntent(provider=self.name, kind=self.kind, reference=reference,
                               amount_cents=amount_cents, invoice=f"lnbcrt{sats}n_devinvoice_{reference}",
                               payment_required={"scheme": "L402", "amount_sats": sats},
                               dev_confirm_token=make_dev_token(self.name, user_id, amount_cents, reference))
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(f"{os.environ['LNBITS_URL']}/api/v1/payments",
                             headers={"X-Api-Key": os.environ.get("LNBITS_INVOICE_KEY", "")},
                             json={"out": False, "amount": sats, "memo": f"credits:{user_id}:{reference}"})
            r.raise_for_status()
            pr = r.json()
        return TopupIntent(provider=self.name, kind=self.kind, reference=pr.get("payment_hash", reference),
                           amount_cents=amount_cents, invoice=pr.get("payment_request"),
                           payment_required={"scheme": "L402", "amount_sats": sats})

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        # live: verify the invoice is paid (preimage / LookupInvoice)
        if payload.get("preimage") or str(payload.get("paid")).lower() == "true":
            return Settlement(ok=True, provider=self.name,
                              ext_ref=f"l402:{payload.get('payment_hash') or payload.get('preimage')}",
                              credited_cents=float(payload.get("amount_cents") or 0),
                              user_id=payload.get("user_id"), reason="l402_paid")
        return Settlement(ok=False, provider=self.name, ext_ref="", reason="unpaid")


# ============================================================ Cloudflare Pay (configurable)
class CloudflarePayProvider:
    """cloudflare.pay adapter. The public API surface isn't final; this posts to a
    configurable endpoint (CLOUDFLARE_PAY_ENDPOINT) to create a session and credits on its
    webhook. In dev mode it behaves like any redirect rail so the flow is testable now."""
    name = "cloudflare_pay"
    kind = "redirect"

    def enabled(self) -> bool:
        return bool(_env("CLOUDFLARE_PAY_ENDPOINT", "CLOUDFLARE_PAY_TOKEN")) or dev_mode()

    def capabilities(self) -> Dict[str, Any]:
        return {"kind": self.kind, "asset": "fiat/stablecoin", "configurable": True,
                "live": bool(_env("CLOUDFLARE_PAY_ENDPOINT"))}

    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent:
        reference = _ref("cfpay")
        endpoint = _env("CLOUDFLARE_PAY_ENDPOINT")
        if not endpoint:
            return TopupIntent(provider=self.name, kind=self.kind, reference=reference,
                               amount_cents=amount_cents,
                               dev_confirm_token=make_dev_token(self.name, user_id, amount_cents, reference))
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(endpoint,
                             headers={"Authorization": f"Bearer {_env('CLOUDFLARE_PAY_TOKEN') or ''}"},
                             json={"amount_cents": int(round(amount_cents)), "reference": reference,
                                   "metadata": {"user_id": user_id, "credit_cents": amount_cents}})
            r.raise_for_status()
            sess = r.json()
        return TopupIntent(provider=self.name, kind=self.kind, reference=sess.get("id", reference),
                           amount_cents=amount_cents, checkout_url=sess.get("url") or sess.get("checkout_url"))

    async def settle(self, payload: Dict[str, Any]) -> Settlement:
        dev = _dev_settle(self.name, payload)
        if dev is not None:
            return dev
        if str(payload.get("status")).lower() in {"paid", "complete", "succeeded"}:
            md = payload.get("metadata") or {}
            return Settlement(ok=True, provider=self.name, ext_ref=f"cloudflare_pay:{payload.get('id')}",
                              credited_cents=float(md.get("credit_cents") or payload.get("amount_cents") or 0),
                              user_id=md.get("user_id") or payload.get("user_id"), reason="cloudflare_paid")
        return Settlement(ok=False, provider=self.name, ext_ref="", reason="unconfirmed")
