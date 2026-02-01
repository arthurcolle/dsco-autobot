"""
Authorization API Routes.

Endpoints for:
- Key management (generate, register)
- Warrant minting and delegation
- Warrant validation
- Receipt retrieval
- Authorization verification
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException, Depends, Header, Body
from pydantic import BaseModel, Field

from ..auth import (
    KeyPair,
    Warrant,
    Receipt,
    Constraint,
    ConstraintType,
    get_warrant_store,
    Subpath,
    Range,
    AllowedValues,
)
from ..auth.warrants import CRYPTO_ED25519_AVAILABLE, CRYPTO_AVAILABLE

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["authorization"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class KeyPairResponse(BaseModel):
    """Response with a generated key pair."""
    public_key: str = Field(..., description="Public key (hex)")
    private_key: str = Field(..., description="Private key (hex) - KEEP SECRET")


class RegisterKeyRequest(BaseModel):
    """Register a public key for an identity."""
    public_key: str = Field(..., description="Public key (hex)")
    identity_type: str = Field(..., description="Type: 'user' or 'agent'")
    identity_id: str = Field(..., description="User or agent identifier")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConstraintModel(BaseModel):
    """A constraint on a capability."""
    type: str = Field(..., description="Constraint type: subpath, range, enum, max_size")
    field: str = Field(..., description="Field to constrain")
    value: Any = Field(..., description="Constraint value")


class MintWarrantRequest(BaseModel):
    """Request to mint a new warrant."""
    issuer_private_key: str = Field(..., description="Issuer's private key (hex)")
    holder_public_key: str = Field(..., description="Holder's public key (hex)")
    capability: str = Field(..., description="Capability being authorized")
    constraints: List[ConstraintModel] = Field(default_factory=list)
    ttl_seconds: int = Field(default=3600, description="Time-to-live in seconds")
    can_delegate: bool = Field(default=False, description="Whether holder can delegate")
    issuer_id: Optional[str] = None
    holder_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DelegateWarrantRequest(BaseModel):
    """Request to delegate a warrant."""
    warrant_id: str = Field(..., description="ID of warrant to delegate")
    holder_private_key: str = Field(..., description="Current holder's private key (hex)")
    new_holder_public_key: str = Field(..., description="New holder's public key (hex)")
    narrower_constraints: List[ConstraintModel] = Field(default_factory=list)
    shorter_ttl_seconds: Optional[int] = None
    new_holder_id: Optional[str] = None


class ValidateWarrantRequest(BaseModel):
    """Request to validate a warrant for an action."""
    warrant_id: str = Field(..., description="Warrant ID")
    holder_private_key: str = Field(..., description="Holder's private key (hex) for proof")
    action: Dict[str, Any] = Field(..., description="Action to validate")


class WarrantResponse(BaseModel):
    """Warrant details."""
    id: str
    issuer_pubkey: str
    issuer_id: Optional[str]
    holder_pubkey: str
    holder_id: Optional[str]
    capability: str
    constraints: List[Dict[str, Any]]
    issued_at: str
    expires_at: Optional[str]
    can_delegate: bool
    delegation_depth: int
    parent_warrant_id: Optional[str]
    signature: str
    is_expired: bool


class ReceiptResponse(BaseModel):
    """Receipt details."""
    id: str
    warrant_id: str
    warrant_capability: str
    action_hash: str
    validated_at: str
    success: bool
    error: Optional[str]
    signature: str


class ValidationResponse(BaseModel):
    """Validation result."""
    valid: bool
    errors: List[str]
    receipt_id: Optional[str] = None


# =============================================================================
# KEY MANAGEMENT
# =============================================================================

# In-memory key registry (production: use secure storage)
_key_registry: Dict[str, Dict[str, Any]] = {}


@router.post("/keys/generate", response_model=KeyPairResponse)
async def generate_keypair():
    """
    Generate a new Ed25519 key pair.

    WARNING: The private key is returned ONCE. Store it securely.
    In production, use hardware security modules or passkeys.
    """
    keypair = KeyPair.generate()

    return KeyPairResponse(
        public_key=keypair.public_key.hex(),
        private_key=keypair.private_key.hex(),
    )


class VerifyDerivationRequest(BaseModel):
    private_key_hex: str


@router.post("/keys/verify-derivation")
async def verify_key_derivation(body: Dict[str, Any] = Body(...)):
    """
    Debug endpoint: verify that public key derivation works correctly.

    Takes a private key and returns both:
    - The public key derived from it
    - A test signature and verification result
    """
    private_key_hex = body.get("private_key_hex", "")
    try:
        # Derive public key from private key
        keypair = KeyPair.from_private_key(bytes.fromhex(private_key_hex))

        # Create a test message and sign it
        test_message = b"test_message_for_verification"
        signature = keypair.sign(test_message)

        # Verify the signature
        is_valid = KeyPair.verify(
            keypair.public_key,
            test_message,
            signature,
            keypair.algorithm,
        )

        return {
            "private_key_hex": private_key_hex,
            "derived_public_key_hex": keypair.public_key.hex(),
            "algorithm": keypair.algorithm.value,
            "test_signature_hex": signature.hex(),
            "signature_valid": is_valid,
            "crypto_ed25519_available": CRYPTO_ED25519_AVAILABLE,
            "crypto_available": CRYPTO_AVAILABLE,
        }
    except Exception as e:
        return {
            "error": str(e),
            "private_key_hex": private_key_hex,
        }


@router.post("/keys/register")
async def register_key(request: RegisterKeyRequest):
    """
    Register a public key for an identity.

    This creates a mapping from identity to public key for
    warrant issuance and validation.
    """
    _key_registry[request.public_key] = {
        "identity_type": request.identity_type,
        "identity_id": request.identity_id,
        "metadata": request.metadata,
    }

    return {
        "registered": True,
        "public_key": request.public_key,
        "identity_id": request.identity_id,
    }


@router.get("/keys/{public_key}")
async def get_key_info(public_key: str):
    """Get information about a registered key."""
    info = _key_registry.get(public_key)
    if not info:
        raise HTTPException(404, "Key not registered")

    return {
        "public_key": public_key,
        **info,
    }


# =============================================================================
# WARRANT MANAGEMENT
# =============================================================================

def _parse_constraints(constraints: List[ConstraintModel]) -> List[Constraint]:
    """Parse constraint models into Constraint objects."""
    result = []
    for c in constraints:
        ctype = ConstraintType(c.type)
        result.append(Constraint(type=ctype, field=c.field, value=c.value))
    return result


@router.post("/warrants/mint", response_model=WarrantResponse)
async def mint_warrant(request: MintWarrantRequest):
    """
    Mint a new warrant.

    The issuer signs the warrant, authorizing the holder to perform
    the specified capability within the given constraints and TTL.
    """
    try:
        # Reconstruct issuer key pair from private key (derives public key)
        issuer_key = KeyPair.from_private_key(
            bytes.fromhex(request.issuer_private_key)
        )

        holder_pubkey = bytes.fromhex(request.holder_public_key)
        constraints = _parse_constraints(request.constraints)

        warrant = Warrant.mint(
            issuer_key=issuer_key,
            holder_pubkey=holder_pubkey,
            capability=request.capability,
            constraints=constraints,
            ttl=timedelta(seconds=request.ttl_seconds),
            can_delegate=request.can_delegate,
            issuer_id=request.issuer_id,
            holder_id=request.holder_id,
            metadata=request.metadata,
        )

        # Store the warrant
        store = get_warrant_store()
        store.store_warrant(warrant)

        return WarrantResponse(
            id=warrant.id,
            issuer_pubkey=warrant.issuer_pubkey.hex(),
            issuer_id=warrant.issuer_id,
            holder_pubkey=warrant.holder_pubkey.hex(),
            holder_id=warrant.holder_id,
            capability=warrant.capability,
            constraints=[c.to_dict() for c in warrant.constraints],
            issued_at=warrant.issued_at.isoformat(),
            expires_at=warrant.expires_at.isoformat() if warrant.expires_at else None,
            can_delegate=warrant.can_delegate,
            delegation_depth=warrant.delegation_depth,
            parent_warrant_id=warrant.parent_warrant_id,
            signature=warrant.signature.hex(),
            is_expired=warrant.is_expired(),
        )

    except Exception as e:
        logger.error(f"Failed to mint warrant: {e}")
        raise HTTPException(500, f"Failed to mint warrant: {e}")


@router.post("/warrants/delegate", response_model=WarrantResponse)
async def delegate_warrant(request: DelegateWarrantRequest):
    """
    Delegate a warrant to a new holder.

    The current holder signs a new warrant with equal or narrower scope,
    transferring authority to the new holder.
    """
    store = get_warrant_store()
    warrant = store.get_warrant(request.warrant_id)

    if not warrant:
        raise HTTPException(404, "Warrant not found")

    if not warrant.can_delegate:
        raise HTTPException(400, "Warrant cannot be delegated")

    try:
        holder_key = KeyPair(
            private_key=bytes.fromhex(request.holder_private_key),
            public_key=warrant.holder_pubkey,  # Must match
        )

        new_holder_pubkey = bytes.fromhex(request.new_holder_public_key)
        narrower_constraints = _parse_constraints(request.narrower_constraints)

        shorter_ttl = None
        if request.shorter_ttl_seconds:
            shorter_ttl = timedelta(seconds=request.shorter_ttl_seconds)

        new_warrant = warrant.delegate(
            holder_key=holder_key,
            new_holder_pubkey=new_holder_pubkey,
            narrower_constraints=narrower_constraints if narrower_constraints else None,
            shorter_ttl=shorter_ttl,
            new_holder_id=request.new_holder_id,
        )

        store.store_warrant(new_warrant)

        return WarrantResponse(
            id=new_warrant.id,
            issuer_pubkey=new_warrant.issuer_pubkey.hex(),
            issuer_id=new_warrant.issuer_id,
            holder_pubkey=new_warrant.holder_pubkey.hex(),
            holder_id=new_warrant.holder_id,
            capability=new_warrant.capability,
            constraints=[c.to_dict() for c in new_warrant.constraints],
            issued_at=new_warrant.issued_at.isoformat(),
            expires_at=new_warrant.expires_at.isoformat() if new_warrant.expires_at else None,
            can_delegate=new_warrant.can_delegate,
            delegation_depth=new_warrant.delegation_depth,
            parent_warrant_id=new_warrant.parent_warrant_id,
            signature=new_warrant.signature.hex(),
            is_expired=new_warrant.is_expired(),
        )

    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"Failed to delegate warrant: {e}")
        raise HTTPException(500, f"Failed to delegate: {e}")


@router.get("/warrants/{warrant_id}", response_model=WarrantResponse)
async def get_warrant(warrant_id: str):
    """Get a warrant by ID."""
    store = get_warrant_store()
    warrant = store.get_warrant(warrant_id)

    if not warrant:
        raise HTTPException(404, "Warrant not found")

    return WarrantResponse(
        id=warrant.id,
        issuer_pubkey=warrant.issuer_pubkey.hex(),
        issuer_id=warrant.issuer_id,
        holder_pubkey=warrant.holder_pubkey.hex(),
        holder_id=warrant.holder_id,
        capability=warrant.capability,
        constraints=[c.to_dict() for c in warrant.constraints],
        issued_at=warrant.issued_at.isoformat(),
        expires_at=warrant.expires_at.isoformat() if warrant.expires_at else None,
        can_delegate=warrant.can_delegate,
        delegation_depth=warrant.delegation_depth,
        parent_warrant_id=warrant.parent_warrant_id,
        signature=warrant.signature.hex(),
        is_expired=warrant.is_expired(),
    )


@router.post("/warrants/validate", response_model=ValidationResponse)
async def validate_warrant(request: ValidateWarrantRequest):
    """
    Validate a warrant for an action.

    Checks:
    - Signature validity
    - Holder binding (key matches)
    - Expiry
    - Capability match
    - Constraint satisfaction

    Returns validation result and creates a receipt on success.
    """
    store = get_warrant_store()
    warrant = store.get_warrant(request.warrant_id)

    if not warrant:
        raise HTTPException(404, "Warrant not found")

    try:
        holder_key = KeyPair(
            private_key=bytes.fromhex(request.holder_private_key),
            public_key=warrant.holder_pubkey,
        )

        valid, errors = warrant.validate_for_action(holder_key, request.action)

        receipt_id = None
        if valid:
            # Create success receipt
            receipt = Receipt.create(
                warrant=warrant,
                action=request.action,
                validator_key=holder_key,  # Self-validation
                success=True,
            )
            store.store_receipt(receipt)
            receipt_id = receipt.id

        return ValidationResponse(
            valid=valid,
            errors=errors,
            receipt_id=receipt_id,
        )

    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(500, f"Validation error: {e}")


# =============================================================================
# RECEIPT MANAGEMENT
# =============================================================================

@router.get("/receipts/{receipt_id}", response_model=ReceiptResponse)
async def get_receipt(receipt_id: str):
    """Get a receipt by ID."""
    store = get_warrant_store()
    receipt = store.get_receipt(receipt_id)

    if not receipt:
        raise HTTPException(404, "Receipt not found")

    return ReceiptResponse(
        id=receipt.id,
        warrant_id=receipt.warrant_id,
        warrant_capability=receipt.warrant_capability,
        action_hash=receipt.action_hash,
        validated_at=receipt.validated_at.isoformat(),
        success=receipt.success,
        error=receipt.error,
        signature=receipt.signature.hex(),
    )


@router.get("/receipts/by-warrant/{warrant_id}")
async def get_receipts_for_warrant(warrant_id: str):
    """Get all receipts for a warrant."""
    store = get_warrant_store()
    receipts = store.get_receipts_for_warrant(warrant_id)

    return {
        "warrant_id": warrant_id,
        "receipts": [
            ReceiptResponse(
                id=r.id,
                warrant_id=r.warrant_id,
                warrant_capability=r.warrant_capability,
                action_hash=r.action_hash,
                validated_at=r.validated_at.isoformat(),
                success=r.success,
                error=r.error,
                signature=r.signature.hex(),
            )
            for r in receipts
        ],
    }


@router.post("/receipts/{receipt_id}/verify")
async def verify_receipt(receipt_id: str):
    """
    Verify a receipt's signature.

    This proves that the receipt was created by the validator
    and has not been tampered with.
    """
    store = get_warrant_store()
    receipt = store.get_receipt(receipt_id)

    if not receipt:
        raise HTTPException(404, "Receipt not found")

    is_valid = receipt.verify_signature()

    return {
        "receipt_id": receipt_id,
        "signature_valid": is_valid,
        "warrant_id": receipt.warrant_id,
        "action_hash": receipt.action_hash,
        "success": receipt.success,
    }


# =============================================================================
# CAPABILITY INSPECTION
# =============================================================================

@router.get("/capabilities")
async def list_capabilities():
    """
    List all capabilities and their typical constraints.

    Capabilities are defined by the tools in the registry.
    """
    # This would query the tool registry for available capabilities
    return {
        "capabilities": [
            {
                "name": "tool_execute",
                "description": "Execute a tool",
                "typical_constraints": [
                    {"type": "enum", "field": "tool_name", "description": "Allowed tool names"},
                    {"type": "range", "field": "max_tokens", "description": "Token limit range"},
                ],
            },
            {
                "name": "file_read",
                "description": "Read files",
                "typical_constraints": [
                    {"type": "subpath", "field": "path", "description": "Allowed path prefix"},
                    {"type": "max_size", "field": "size", "description": "Maximum file size"},
                ],
            },
            {
                "name": "file_write",
                "description": "Write files",
                "typical_constraints": [
                    {"type": "subpath", "field": "path", "description": "Allowed path prefix"},
                    {"type": "max_size", "field": "size", "description": "Maximum file size"},
                ],
            },
            {
                "name": "http_request",
                "description": "Make HTTP requests",
                "typical_constraints": [
                    {"type": "regex", "field": "url", "description": "Allowed URL pattern"},
                    {"type": "enum", "field": "method", "description": "Allowed HTTP methods"},
                ],
            },
            {
                "name": "transfer",
                "description": "Transfer funds/resources",
                "typical_constraints": [
                    {"type": "range", "field": "amount", "description": "Amount range"},
                    {"type": "enum", "field": "to", "description": "Allowed recipients"},
                ],
            },
        ],
    }
