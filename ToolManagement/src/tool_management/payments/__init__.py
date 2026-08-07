"""Pluggable agentic-payment rails for the Gateway.

Every rail — card (Stripe), stablecoin/x402 (USDC on Base), Circle, Coinbase CDP,
Lightning/L402, Cloudflare Pay — is a PaymentProvider adapter that ultimately calls
BillingStore.credit(user_id, cents, ext_ref). Because credit() is idempotent on ext_ref,
webhook/proof replays can't double-credit. AP2 mandates are verified as an authorization
envelope over any rail.

Each provider works in dev mode (PAYMENTS_DEV_MODE=1) with no keys/network so the flow is
verifiable offline; live is a config switch (per-provider env vars).
"""
from .core import (
    PaymentProvider,
    PaymentRegistry,
    Settlement,
    TopupIntent,
    build_registry,
    dev_mode,
    verify_ap2_mandate,
)

__all__ = [
    "PaymentProvider",
    "PaymentRegistry",
    "Settlement",
    "TopupIntent",
    "build_registry",
    "dev_mode",
    "verify_ap2_mandate",
]
