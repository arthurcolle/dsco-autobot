"""
Swarm owner wallet authentication.

Enforces that mutation requests are signed by the configured owner wallet
using Electrum/Bitcoin Signed Message signatures.
"""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import time
from typing import Dict, Optional, Tuple

from coincurve import PublicKey
from fastapi import HTTPException, Request


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

_NONCE_CACHE: Dict[str, float] = {}
_NONCE_LOCK = threading.Lock()


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _hash160(data: bytes) -> bytes:
    ripemd = hashlib.new("ripemd160")
    ripemd.update(_sha256(data))
    return ripemd.digest()


def _encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint value must be non-negative")
    if value < 253:
        return bytes([value])
    if value <= 0xFFFF:
        return b"\xfd" + int(value).to_bytes(2, "little")
    if value <= 0xFFFFFFFF:
        return b"\xfe" + int(value).to_bytes(4, "little")
    return b"\xff" + int(value).to_bytes(8, "little")


def _base58_encode(data: bytes) -> str:
    if not data:
        return ""
    leading_zeros = 0
    for byte in data:
        if byte == 0:
            leading_zeros += 1
        else:
            break

    value = int.from_bytes(data, "big")
    chars = []
    while value > 0:
        value, rem = divmod(value, 58)
        chars.append(_B58_ALPHABET[rem])
    encoded = "".join(reversed(chars)) if chars else ""
    return ("1" * leading_zeros) + encoded


def _base58check(version: int, payload: bytes) -> str:
    raw = bytes([version]) + payload
    checksum = _sha256(_sha256(raw))[:4]
    return _base58_encode(raw + checksum)


def _bech32_polymod(values) -> int:
    generator = [0x3b6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for value in values:
        top = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ value
        for idx in range(5):
            if ((top >> idx) & 1) != 0:
                chk ^= generator[idx]
    return chk


def _bech32_hrp_expand(hrp: str):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def _bech32_create_checksum(hrp: str, data):
    values = _bech32_hrp_expand(hrp) + data
    polymod = _bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]


def _bech32_encode(hrp: str, data) -> str:
    charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    combined = data + _bech32_create_checksum(hrp, data)
    return hrp + "1" + "".join(charset[d] for d in combined)


def _convertbits(data: bytes, from_bits: int, to_bits: int, pad: bool = True):
    acc = 0
    bits = 0
    result = []
    max_v = (1 << to_bits) - 1
    max_acc = (1 << (from_bits + to_bits - 1)) - 1
    for value in data:
        if value < 0 or (value >> from_bits):
            return None
        acc = ((acc << from_bits) | value) & max_acc
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            result.append((acc >> bits) & max_v)
    if pad:
        if bits:
            result.append((acc << (to_bits - bits)) & max_v)
    elif bits >= from_bits or ((acc << (to_bits - bits)) & max_v):
        return None
    return result


def _bitcoin_signed_message_hash(message: str) -> bytes:
    magic = b"Bitcoin Signed Message:\n"
    msg_bytes = message.encode("utf-8")
    payload = _encode_varint(len(magic)) + magic + _encode_varint(len(msg_bytes)) + msg_bytes
    return _sha256(_sha256(payload))


def _network_for_address(address: str) -> str:
    value = address.strip().lower()
    if value.startswith("bc1"):
        return "mainnet"
    if value.startswith("tb1"):
        return "testnet"
    if value.startswith("bcrt1"):
        return "regtest"
    if value.startswith(("1", "3")):
        return "mainnet"
    if value.startswith(("m", "n", "2")):
        return "testnet"
    return "mainnet"


def _candidate_addresses(public_key: PublicKey, network: str) -> Tuple[str, str, str, str]:
    if network == "testnet":
        p2pkh_version = 0x6F
        p2sh_version = 0xC4
        hrp = "tb"
    elif network == "regtest":
        p2pkh_version = 0x6F
        p2sh_version = 0xC4
        hrp = "bcrt"
    else:
        p2pkh_version = 0x00
        p2sh_version = 0x05
        hrp = "bc"

    compressed = public_key.format(compressed=True)
    uncompressed = public_key.format(compressed=False)
    hash160_compressed = _hash160(compressed)
    hash160_uncompressed = _hash160(uncompressed)

    p2pkh_compressed = _base58check(p2pkh_version, hash160_compressed)
    p2pkh_uncompressed = _base58check(p2pkh_version, hash160_uncompressed)

    redeem_script = b"\x00\x14" + hash160_compressed
    p2sh_p2wpkh = _base58check(p2sh_version, _hash160(redeem_script))

    witness_data = [0] + (_convertbits(hash160_compressed, 8, 5, pad=True) or [])
    p2wpkh = _bech32_encode(hrp, witness_data)

    return p2pkh_compressed, p2pkh_uncompressed, p2sh_p2wpkh, p2wpkh


def _parse_recovery_header(header: int) -> Tuple[int, bool]:
    if header < 27 or header > 42:
        raise ValueError("Unsupported signature header")
    recovery_id = (header - 27) & 0x03
    compressed = header >= 31
    return recovery_id, compressed


def verify_electrum_signature(
    *,
    expected_address: str,
    message: str,
    signature_b64: str,
) -> bool:
    expected = str(expected_address or "").strip()
    signature_raw = str(signature_b64 or "").strip()
    if not expected or not signature_raw:
        return False

    try:
        signature = base64.b64decode(signature_raw, validate=True)
    except Exception:
        return False
    if len(signature) != 65:
        return False

    header = signature[0]
    compact_sig = signature[1:]
    try:
        recovery_id, _compressed = _parse_recovery_header(header)
        recoverable = compact_sig + bytes([recovery_id])
        msg_hash = _bitcoin_signed_message_hash(message)
        public_key = PublicKey.from_signature_and_message(recoverable, msg_hash, hasher=None)
    except Exception:
        return False

    network = _network_for_address(expected)
    candidates = _candidate_addresses(public_key, network)
    if expected.lower().startswith(("bc1", "tb1", "bcrt1")):
        candidate_set = {candidate.lower() for candidate in candidates}
        return expected.lower() in candidate_set
    return expected in candidates


def build_swarm_owner_message(
    *,
    method: str,
    path: str,
    body_sha256: str,
    timestamp: int,
    nonce: str,
    owner_id: str,
) -> str:
    # Stable canonical message that is signed by Electrum "sign message".
    return (
        "DSCO-SWARM-AUTH-V1\n"
        f"owner_id:{owner_id}\n"
        f"method:{method.upper()}\n"
        f"path:{path}\n"
        f"body_sha256:{body_sha256}\n"
        f"timestamp:{int(timestamp)}\n"
        f"nonce:{nonce}\n"
    )


def _nonce_ttl_seconds() -> int:
    return max(30, int(os.getenv("SWARM_OWNER_NONCE_TTL_SECONDS", "600")))


def _clock_skew_seconds() -> int:
    return max(5, int(os.getenv("SWARM_OWNER_CLOCK_SKEW_SECONDS", "300")))


def _prune_nonce_cache(now: float):
    ttl = _nonce_ttl_seconds()
    stale = [nonce for nonce, ts in _NONCE_CACHE.items() if (now - ts) > ttl]
    for nonce in stale:
        _NONCE_CACHE.pop(nonce, None)


def clear_swarm_owner_nonce_cache():
    with _NONCE_LOCK:
        _NONCE_CACHE.clear()


def _register_nonce(nonce: str, now: float) -> bool:
    with _NONCE_LOCK:
        _prune_nonce_cache(now)
        if nonce in _NONCE_CACHE:
            return False
        _NONCE_CACHE[nonce] = now
        max_entries = max(128, int(os.getenv("SWARM_OWNER_NONCE_MAX_ENTRIES", "10000")))
        if len(_NONCE_CACHE) > max_entries:
            for item_nonce, _ts in sorted(_NONCE_CACHE.items(), key=lambda item: item[1])[: len(_NONCE_CACHE) - max_entries]:
                _NONCE_CACHE.pop(item_nonce, None)
        return True


def is_swarm_owner_enforcement_enabled() -> bool:
    configured_address = str(os.getenv("SWARM_OWNER_WALLET_ADDRESS", "")).strip()
    if not configured_address:
        return False
    return str(os.getenv("SWARM_OWNER_ENFORCE", "1")).strip().lower() not in {"0", "false", "no", "off"}


async def require_swarm_owner_mutation(request: Request):
    # Only mutation endpoints influence swarm state.
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    if not is_swarm_owner_enforcement_enabled():
        return

    expected_address = str(os.getenv("SWARM_OWNER_WALLET_ADDRESS", "")).strip()
    expected_owner_id = str(os.getenv("SWARM_OWNER_ID", "Arthur Colle")).strip()
    expected_owner_id = expected_owner_id or "Arthur Colle"

    address_header = str(request.headers.get("x-swarm-owner-address", "")).strip()
    signature_header = str(request.headers.get("x-swarm-owner-signature", "")).strip()
    nonce_header = str(request.headers.get("x-swarm-owner-nonce", "")).strip()
    timestamp_header = str(request.headers.get("x-swarm-owner-timestamp", "")).strip()

    if not (address_header and signature_header and nonce_header and timestamp_header):
        raise HTTPException(
            status_code=401,
            detail={
                "code": "SWARM_OWNER_AUTH_REQUIRED",
                "message": (
                    "Swarm mutation requires owner wallet signature headers: "
                    "x-swarm-owner-address, x-swarm-owner-signature, "
                    "x-swarm-owner-timestamp, x-swarm-owner-nonce"
                ),
            },
        )

    if address_header != expected_address:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "SWARM_OWNER_ADDRESS_MISMATCH",
                "message": "Provided owner address does not match configured swarm owner address",
            },
        )

    try:
        timestamp = int(timestamp_header)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail={"code": "SWARM_OWNER_INVALID_TIMESTAMP", "message": "Invalid x-swarm-owner-timestamp"},
        )

    now = time.time()
    skew = _clock_skew_seconds()
    if abs(now - float(timestamp)) > skew:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "SWARM_OWNER_TIMESTAMP_SKEW",
                "message": f"Timestamp outside allowed skew ({skew}s)",
            },
        )

    if len(nonce_header) < 8:
        raise HTTPException(
            status_code=400,
            detail={"code": "SWARM_OWNER_INVALID_NONCE", "message": "Nonce too short"},
        )
    if not _register_nonce(nonce_header, now):
        raise HTTPException(
            status_code=409,
            detail={"code": "SWARM_OWNER_NONCE_REPLAY", "message": "Nonce has already been used"},
        )

    body = await request.body()
    body_sha256 = hashlib.sha256(body).hexdigest()
    message = build_swarm_owner_message(
        method=request.method,
        path=request.url.path,
        body_sha256=body_sha256,
        timestamp=timestamp,
        nonce=nonce_header,
        owner_id=expected_owner_id,
    )

    if not verify_electrum_signature(
        expected_address=expected_address,
        message=message,
        signature_b64=signature_header,
    ):
        # Ensure replay cache cannot be poisoned by invalid attempts with guessed nonce.
        with _NONCE_LOCK:
            _NONCE_CACHE.pop(nonce_header, None)
        raise HTTPException(
            status_code=401,
            detail={
                "code": "SWARM_OWNER_INVALID_SIGNATURE",
                "message": "Invalid Electrum signature for swarm owner mutation request",
            },
        )

    # Expose verified owner context to handlers.
    request.state.swarm_owner_verified = True
    request.state.swarm_owner_address = address_header
    request.state.swarm_owner_id = expected_owner_id
    request.state.swarm_owner_auth_id = secrets.token_hex(8)

