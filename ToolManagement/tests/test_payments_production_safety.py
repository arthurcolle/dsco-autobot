"""Production payment adapters must reject unverified caller assertions."""
import pytest

from tool_management.payments.core import sign_ap2_subject, verify_ap2_mandate
from tool_management.payments.providers import (
    CircleProvider,
    CloudflarePayProvider,
    CoinbaseCDPProvider,
    LightningL402Provider,
    X402Provider,
)


EXPERIMENTAL = [
    (X402Provider, "X402_PAY_TO"),
    (CircleProvider, "CIRCLE_API_KEY"),
    (CoinbaseCDPProvider, "CDP_API_KEY_ID"),
    (LightningL402Provider, "LNBITS_URL"),
    (CloudflarePayProvider, "CLOUDFLARE_PAY_ENDPOINT"),
]


@pytest.mark.parametrize(("provider_cls", "credential"), EXPERIMENTAL)
def test_experimental_provider_does_not_enable_from_credential(monkeypatch, provider_cls, credential):
    monkeypatch.delenv("PAYMENTS_DEV_MODE", raising=False)
    monkeypatch.setenv(credential, "configured-but-not-sufficient")
    assert provider_cls().enabled() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_cls", [p[0] for p in EXPERIMENTAL])
async def test_forged_live_settlement_fails_closed(monkeypatch, provider_cls):
    monkeypatch.delenv("PAYMENTS_DEV_MODE", raising=False)
    result = await provider_cls().settle({
        "status": "paid",
        "paid": True,
        "tx_hash": "forged",
        "amount_cents": 1_000_000,
        "user_id": "attacker",
        "notification": {
            "id": "forged",
            "state": "complete",
            "metadata": {"user_id": "attacker", "credit_cents": 1_000_000},
        },
    })
    assert result.ok is False
    assert result.reason == "live_settlement_not_implemented"


def test_ap2_hmac_placeholder_is_rejected_in_production(monkeypatch):
    monkeypatch.delenv("PAYMENTS_DEV_MODE", raising=False)
    subject = {"user_id": "agent7", "max_amount_cents": 1000}
    mandate = {
        "verifiableCredential": {
            "credentialSubject": subject,
            "expirationDate": 9999999999,
            "proof": {"jws": sign_ap2_subject(subject)},
        }
    }
    result = verify_ap2_mandate(mandate)
    assert result == {"ok": False, "reason": "ap2_production_verifier_not_configured"}


def test_ap2_dev_envelope_still_requires_valid_signature(monkeypatch):
    monkeypatch.setenv("PAYMENTS_DEV_MODE", "1")
    mandate = {
        "verifiableCredential": {
            "credentialSubject": {"user_id": "agent7"},
            "expirationDate": 9999999999,
            "proof": {"jws": "forged"},
        }
    }
    assert verify_ap2_mandate(mandate) == {"ok": False, "reason": "bad_signature"}
