import base64
import hashlib
import json
import time

from coincurve import PrivateKey
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from tool_management.auth.wallet_owner import (
    build_swarm_owner_message,
    clear_swarm_owner_nonce_cache,
    require_swarm_owner_mutation,
    verify_electrum_signature,
)
from tool_management.api import swarm_routes


def _p2pkh_mainnet_from_private_key(private_key: PrivateKey) -> str:
    pubkey = private_key.public_key.format(compressed=True)
    payload = hashlib.new("ripemd160", hashlib.sha256(pubkey).digest()).digest()
    raw = b"\x00" + payload
    checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    data = raw + checksum
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    leading_zeros = len(data) - len(data.lstrip(b"\x00"))
    value = int.from_bytes(data, "big")
    chars = []
    while value > 0:
        value, rem = divmod(value, 58)
        chars.append(alphabet[rem])
    return ("1" * leading_zeros) + ("".join(reversed(chars)) if chars else "")


def _bitcoin_signed_message_hash(message: str) -> bytes:
    # Match wallet_owner._bitcoin_signed_message_hash without importing private symbols.
    magic = b"Bitcoin Signed Message:\n"
    msg_bytes = message.encode("utf-8")

    def _varint(value: int) -> bytes:
        if value < 253:
            return bytes([value])
        if value <= 0xFFFF:
            return b"\xfd" + value.to_bytes(2, "little")
        if value <= 0xFFFFFFFF:
            return b"\xfe" + value.to_bytes(4, "little")
        return b"\xff" + value.to_bytes(8, "little")

    payload = _varint(len(magic)) + magic + _varint(len(msg_bytes)) + msg_bytes
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()


def _electrum_signature_b64(private_key: PrivateKey, message: str) -> str:
    message_hash = _bitcoin_signed_message_hash(message)
    recoverable = private_key.sign_recoverable(message_hash, hasher=None)
    compact_sig = recoverable[:64]
    recovery_id = recoverable[64]
    # 31-34 => compressed P2PKH Bitcoin Signed Message signature headers.
    header = 31 + int(recovery_id)
    return base64.b64encode(bytes([header]) + compact_sig).decode("ascii")


def _owner_headers(
    *,
    private_key: PrivateKey,
    expected_address: str,
    method: str,
    path: str,
    body: bytes,
    owner_id: str,
    timestamp: int,
    nonce: str,
):
    message = build_swarm_owner_message(
        method=method,
        path=path,
        body_sha256=hashlib.sha256(body).hexdigest(),
        timestamp=timestamp,
        nonce=nonce,
        owner_id=owner_id,
    )
    signature_b64 = _electrum_signature_b64(private_key, message)
    return {
        "x-swarm-owner-address": expected_address,
        "x-swarm-owner-signature": signature_b64,
        "x-swarm-owner-timestamp": str(timestamp),
        "x-swarm-owner-nonce": nonce,
        "content-type": "application/json",
    }


def test_verify_electrum_signature_round_trip():
    private_key = PrivateKey()
    address = _p2pkh_mainnet_from_private_key(private_key)

    message = "DSCO test message"
    signature_b64 = _electrum_signature_b64(private_key, message)

    assert verify_electrum_signature(
        expected_address=address,
        message=message,
        signature_b64=signature_b64,
    )


def test_owner_mutation_requires_valid_signature(monkeypatch):
    clear_swarm_owner_nonce_cache()
    private_key = PrivateKey()
    address = _p2pkh_mainnet_from_private_key(private_key)
    owner_id = "Arthur Colle"

    monkeypatch.setenv("SWARM_OWNER_ENFORCE", "1")
    monkeypatch.setenv("SWARM_OWNER_WALLET_ADDRESS", address)
    monkeypatch.setenv("SWARM_OWNER_ID", owner_id)
    monkeypatch.setenv("SWARM_OWNER_CLOCK_SKEW_SECONDS", "300")

    app = FastAPI()

    @app.post("/swarm/mutate", dependencies=[Depends(require_swarm_owner_mutation)])
    async def mutate():
        return {"ok": True}

    @app.get("/swarm/read", dependencies=[Depends(require_swarm_owner_mutation)])
    async def read():
        return {"ok": True}

    client = TestClient(app)

    # Read operations do not require owner signature.
    read_response = client.get("/swarm/read")
    assert read_response.status_code == 200

    body = json.dumps({"value": 1}, separators=(",", ":")).encode("utf-8")
    timestamp = int(time.time())
    headers = _owner_headers(
        private_key=private_key,
        expected_address=address,
        method="POST",
        path="/swarm/mutate",
        body=body,
        owner_id=owner_id,
        timestamp=timestamp,
        nonce="nonce-0001",
    )

    ok_response = client.post("/swarm/mutate", content=body, headers=headers)
    assert ok_response.status_code == 200

    # Replay with same nonce must fail.
    replay_response = client.post("/swarm/mutate", content=body, headers=headers)
    assert replay_response.status_code == 409
    assert replay_response.json()["detail"]["code"] == "SWARM_OWNER_NONCE_REPLAY"

    # Missing signature headers must fail.
    missing_response = client.post("/swarm/mutate", content=body, headers={"content-type": "application/json"})
    assert missing_response.status_code == 401
    assert missing_response.json()["detail"]["code"] == "SWARM_OWNER_AUTH_REQUIRED"


def test_owner_mutation_allows_when_enforcement_disabled(monkeypatch):
    clear_swarm_owner_nonce_cache()
    monkeypatch.setenv("SWARM_OWNER_ENFORCE", "0")
    monkeypatch.delenv("SWARM_OWNER_WALLET_ADDRESS", raising=False)

    app = FastAPI()

    @app.post("/swarm/mutate", dependencies=[Depends(require_swarm_owner_mutation)])
    async def mutate():
        return {"ok": True}

    client = TestClient(app)
    response = client.post("/swarm/mutate", json={"value": 1})
    assert response.status_code == 200


def test_swarm_routes_mutation_is_owner_gated(monkeypatch):
    clear_swarm_owner_nonce_cache()
    private_key = PrivateKey()
    address = _p2pkh_mainnet_from_private_key(private_key)
    owner_id = "Arthur Colle"

    monkeypatch.setenv("SWARM_OWNER_ENFORCE", "1")
    monkeypatch.setenv("SWARM_OWNER_WALLET_ADDRESS", address)
    monkeypatch.setenv("SWARM_OWNER_ID", owner_id)

    app = FastAPI()
    app.include_router(swarm_routes.router)
    client = TestClient(app)

    payload = {
        "name": "owner swarm",
        "host_id": "arthur",
        "host_wallet": address,
        "host_fee_rate": 0.05,
        "min_stake": 10,
    }

    # No owner headers -> blocked before mutation.
    blocked = client.post("/swarm/create", json=payload)
    assert blocked.status_code == 401
    assert blocked.json()["detail"]["code"] == "SWARM_OWNER_AUTH_REQUIRED"
