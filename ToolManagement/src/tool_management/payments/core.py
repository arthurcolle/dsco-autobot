"""Payment-provider interface, registry, and AP2 mandate verification."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

_TRUTHY = {"1", "true", "yes", "on"}


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in _TRUTHY


def dev_mode() -> bool:
    return _truthy(os.environ.get("PAYMENTS_DEV_MODE", ""))


@dataclass
class TopupIntent:
    """What an agent needs to complete a top-up on a given rail."""
    provider: str
    kind: str                 # redirect | onchain | invoice
    reference: str            # provider-side id (session/invoice/nonce) — settlement key
    amount_cents: float
    checkout_url: Optional[str] = None       # redirect rails (Stripe, Cloudflare Pay)
    pay_to_address: Optional[str] = None     # onchain rails (x402, Circle, CDP)
    asset: Optional[str] = None              # e.g. "USDC"
    chain: Optional[str] = None              # e.g. "base"
    invoice: Optional[str] = None            # Lightning bolt11 (L402)
    payment_required: Optional[Dict[str, Any]] = None  # x402/L402 402 challenge headers
    dev_confirm_token: Optional[str] = None  # dev-mode: POST back to settle instantly
    expires_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Settlement:
    """Result of verifying a payment proof / webhook. Feeds BillingStore.credit()."""
    ok: bool
    provider: str
    ext_ref: str
    credited_cents: float = 0.0
    user_id: Optional[str] = None
    reason: str = ""


@runtime_checkable
class PaymentProvider(Protocol):
    name: str
    kind: str

    def enabled(self) -> bool: ...
    def capabilities(self) -> Dict[str, Any]: ...
    async def create_topup(self, user_id: str, amount_cents: float, meta: Dict[str, Any]) -> TopupIntent: ...
    async def settle(self, payload: Dict[str, Any]) -> Settlement: ...


# --------------------------------------------------------------------- dev helpers
def make_dev_token(provider: str, user_id: str, amount_cents: float, reference: str) -> str:
    """A signed dev-mode confirmation token so offline settle can't be forged in tests."""
    secret = os.environ.get("PAYMENTS_DEV_SECRET", "dev-secret").encode()
    body = json.dumps(
        {"p": provider, "u": user_id, "c": amount_cents, "r": reference}, sort_keys=True
    ).encode()
    sig = hmac.new(secret, body, hashlib.sha256).hexdigest()[:16]
    return base64.urlsafe_b64encode(body).decode() + "." + sig


def read_dev_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        body_b64, sig = token.split(".", 1)
        body = base64.urlsafe_b64decode(body_b64.encode())
        secret = os.environ.get("PAYMENTS_DEV_SECRET", "dev-secret").encode()
        expect = hmac.new(secret, body, hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(sig, expect):
            return None
        return json.loads(body)
    except Exception:
        return None


# --------------------------------------------------------------------- registry
class PaymentRegistry:
    def __init__(self) -> None:
        self._providers: Dict[str, PaymentProvider] = {}

    def register(self, provider: PaymentProvider) -> None:
        self._providers[provider.name] = provider

    def get(self, name: str) -> Optional[PaymentProvider]:
        return self._providers.get(name)

    def all(self) -> List[PaymentProvider]:
        return list(self._providers.values())

    def enabled(self) -> List[PaymentProvider]:
        return [p for p in self._providers.values() if p.enabled()]


_registry: Optional[PaymentRegistry] = None


def build_registry() -> PaymentRegistry:
    global _registry
    if _registry is not None:
        return _registry
    from .providers import (  # lazy import to avoid cycle
        CircleProvider,
        CloudflarePayProvider,
        CoinbaseCDPProvider,
        LightningL402Provider,
        StripeProvider,
        X402Provider,
    )

    reg = PaymentRegistry()
    for cls in (
        StripeProvider, X402Provider, CircleProvider, CoinbaseCDPProvider,
        LightningL402Provider, CloudflarePayProvider,
    ):
        try:
            reg.register(cls())
        except Exception:  # a misconfigured provider must not break the others
            pass
    _registry = reg
    return reg


# --------------------------------------------------------------------- AP2 mandates
def verify_ap2_mandate(mandate: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Verify a Google AP2 Intent/Cart Mandate (a signed Verifiable Credential) used as the
    cross-rail authorization envelope. Checks structure, expiry, and an HMAC/dev signature.

    Returns {ok, reason, user_id, max_amount_cents, scope}. Real deployments swap the HMAC
    check for the issuer's public-key (Ed25519/JWS) verification; the envelope shape is stable.
    """
    clock = time.time() if now is None else now
    try:
        vc = mandate.get("verifiableCredential") or mandate
        subject = (vc.get("credentialSubject") or {})
        proof = (vc.get("proof") or {})
        exp = float(vc.get("expirationDate") or subject.get("expires") or 0) or None
        if exp is not None and exp < clock:
            return {"ok": False, "reason": "mandate_expired"}
        if not subject.get("user_id"):
            return {"ok": False, "reason": "no_subject"}
        secret = os.environ.get("AP2_ISSUER_SECRET", "dev-secret").encode()
        signed = json.dumps(subject, sort_keys=True).encode()
        expect = hmac.new(secret, signed, hashlib.sha256).hexdigest()
        given = str(proof.get("jws") or proof.get("signature") or "")
        if not dev_mode() and not hmac.compare_digest(given, expect):
            return {"ok": False, "reason": "bad_signature"}
        return {
            "ok": True, "reason": "",
            "user_id": str(subject["user_id"]),
            "max_amount_cents": float(subject.get("max_amount_cents", 0) or 0),
            "scope": subject.get("scope") or subject.get("merchant"),
        }
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": f"malformed:{type(e).__name__}"}


def sign_ap2_subject(subject: Dict[str, Any]) -> str:
    """Test/helper: produce a valid dev signature over an AP2 credentialSubject."""
    secret = os.environ.get("AP2_ISSUER_SECRET", "dev-secret").encode()
    return hmac.new(secret, json.dumps(subject, sort_keys=True).encode(), hashlib.sha256).hexdigest()
