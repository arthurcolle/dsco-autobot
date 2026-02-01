"""
Tenuo-style Warrant System for Tool Authorization.

Implements cryptographic, scoped, time-bound authorization for AI agents.
Each warrant is a signed authorization artifact that:
- Binds a human issuer to an agent holder
- Specifies capabilities and constraints
- Has a defined TTL (time-to-live)
- Can be verified independently of the agent runtime
- Produces receipts that prove authorization

Key Properties:
- Holder binding: Warrants are non-transferable - only the named holder can use them
- Attenuation: Delegated warrants can only narrow scope, never expand
- Non-repudiation: The issuer's signature is cryptographic proof of authorization
- Receipts: Every action produces a verifiable receipt, not just a log entry

Usage:
    # Human mints a warrant for an agent
    warrant = Warrant.mint(
        issuer_key=alice_key,
        holder_pubkey=agent_pubkey,
        capability="file_read",
        constraints={"path": Subpath("/data/reports")},
        ttl=timedelta(hours=1),
    )

    # Agent presents warrant to execute action
    receipt = resource.execute_with_warrant(warrant, action)

    # Verifier checks the authorization chain
    is_valid = receipt.verify(trusted_issuers)
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import base64

# =============================================================================
# CRYPTOGRAPHIC PRIMITIVES
# =============================================================================
#
# Current: Ed25519 (fast, 32-byte keys, ~128-bit security)
#
# Future-proof (2035+) options:
# 1. CRYSTALS-Dilithium (NIST PQC standard for signatures)
#    - Lattice-based, quantum-resistant
#    - ~2KB signatures, ~1.3KB public keys
#    - Available via: pip install pqcrypto or liboqs-python
#
# 2. SPHINCS+ (NIST PQC standard, hash-based)
#    - Conservative security assumptions
#    - Larger signatures (~8-50KB) but very confident security
#    - Stateless (unlike XMSS)
#
# 3. FALCON (NIST PQC standard)
#    - Compact signatures (~600 bytes)
#    - More complex implementation
#
# 4. Hybrid schemes (recommended for transition):
#    - Ed25519 + Dilithium (sign with both, verify both)
#    - Defense in depth during quantum transition
#
# Design principle: Algorithm agility - the Warrant structure stores
# an `algorithm` field so we can migrate without breaking existing warrants.

from enum import Enum

class SignatureAlgorithm(str, Enum):
    """Supported signature algorithms."""
    ED25519 = "ed25519"              # Current default
    DILITHIUM2 = "dilithium2"        # NIST PQC Level 2 (128-bit quantum)
    DILITHIUM3 = "dilithium3"        # NIST PQC Level 3 (192-bit quantum)
    DILITHIUM5 = "dilithium5"        # NIST PQC Level 5 (256-bit quantum)
    SPHINCS_SHA2_128F = "sphincs+"   # Hash-based, conservative
    HYBRID_ED25519_DILITHIUM = "hybrid"  # Ed25519 + Dilithium3


# Try to import cryptographic libraries
CRYPTO_ED25519_AVAILABLE = False
CRYPTO_PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey, Ed25519PublicKey
    )
    from cryptography.hazmat.primitives import serialization
    from cryptography.exceptions import InvalidSignature
    CRYPTO_ED25519_AVAILABLE = True
except ImportError:
    Ed25519PrivateKey = None
    Ed25519PublicKey = None
    serialization = None
    InvalidSignature = Exception

# Try to import post-quantum libraries (optional)
try:
    import oqs  # liboqs-python
    CRYPTO_PQC_AVAILABLE = True
except ImportError:
    oqs = None

# Backward compatibility
CRYPTO_AVAILABLE = CRYPTO_ED25519_AVAILABLE


# =============================================================================
# KEY MANAGEMENT
# =============================================================================

@dataclass
class KeyPair:
    """
    Crypto-agile key pair supporting multiple signature algorithms.

    Current: Ed25519 (fast, small keys)
    Future: CRYSTALS-Dilithium, SPHINCS+ (post-quantum)
    """
    private_key: bytes
    public_key: bytes
    algorithm: SignatureAlgorithm = SignatureAlgorithm.ED25519

    @classmethod
    def generate(cls, algorithm: SignatureAlgorithm = SignatureAlgorithm.ED25519) -> "KeyPair":
        """Generate a new key pair with the specified algorithm."""

        if algorithm == SignatureAlgorithm.ED25519:
            return cls._generate_ed25519()

        elif algorithm in (SignatureAlgorithm.DILITHIUM2, SignatureAlgorithm.DILITHIUM3, SignatureAlgorithm.DILITHIUM5):
            if CRYPTO_PQC_AVAILABLE:
                return cls._generate_dilithium(algorithm)
            else:
                raise ValueError(f"PQC not available. Install liboqs-python for {algorithm}")

        elif algorithm == SignatureAlgorithm.HYBRID_ED25519_DILITHIUM:
            # Hybrid: concatenate Ed25519 + Dilithium keys
            ed_pair = cls._generate_ed25519()
            if CRYPTO_PQC_AVAILABLE:
                dil_pair = cls._generate_dilithium(SignatureAlgorithm.DILITHIUM3)
                return cls(
                    private_key=ed_pair.private_key + dil_pair.private_key,
                    public_key=ed_pair.public_key + dil_pair.public_key,
                    algorithm=algorithm,
                )
            else:
                # Fallback to Ed25519 only
                return ed_pair

        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

    @classmethod
    def _generate_ed25519(cls) -> "KeyPair":
        """Generate Ed25519 key pair."""
        if not CRYPTO_ED25519_AVAILABLE:
            import os
            # Fallback: random bytes (NOT SECURE - demo only)
            priv = os.urandom(32)
            return cls(
                private_key=priv,
                public_key=hashlib.sha256(priv).digest(),
                algorithm=SignatureAlgorithm.ED25519,
            )

        private = Ed25519PrivateKey.generate()
        public = private.public_key()

        return cls(
            private_key=private.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption(),
            ),
            public_key=public.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            ),
            algorithm=SignatureAlgorithm.ED25519,
        )

    @classmethod
    def _generate_dilithium(cls, algorithm: SignatureAlgorithm) -> "KeyPair":
        """Generate CRYSTALS-Dilithium key pair (post-quantum)."""
        if not CRYPTO_PQC_AVAILABLE:
            raise ValueError("liboqs-python not installed")

        alg_name = {
            SignatureAlgorithm.DILITHIUM2: "Dilithium2",
            SignatureAlgorithm.DILITHIUM3: "Dilithium3",
            SignatureAlgorithm.DILITHIUM5: "Dilithium5",
        }[algorithm]

        signer = oqs.Signature(alg_name)
        public_key = signer.generate_keypair()
        private_key = signer.export_secret_key()

        return cls(
            private_key=private_key,
            public_key=public_key,
            algorithm=algorithm,
        )

    @classmethod
    def from_private_key(
        cls,
        private_key: bytes,
        algorithm: SignatureAlgorithm = SignatureAlgorithm.ED25519,
    ) -> "KeyPair":
        """Create KeyPair from private key, deriving public key."""

        if algorithm == SignatureAlgorithm.ED25519:
            if not CRYPTO_ED25519_AVAILABLE:
                return cls(
                    private_key=private_key,
                    public_key=hashlib.sha256(private_key).digest(),
                    algorithm=algorithm,
                )

            private = Ed25519PrivateKey.from_private_bytes(private_key)
            public = private.public_key()

            return cls(
                private_key=private_key,
                public_key=public.public_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PublicFormat.Raw,
                ),
                algorithm=algorithm,
            )

        elif algorithm in (SignatureAlgorithm.DILITHIUM2, SignatureAlgorithm.DILITHIUM3, SignatureAlgorithm.DILITHIUM5):
            if not CRYPTO_PQC_AVAILABLE:
                raise ValueError("liboqs-python not installed")
            # For Dilithium, we need both keys stored together
            # This is a simplified version - real impl would extract pubkey
            raise ValueError("Dilithium requires both keys - use generate()")

        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

    @property
    def public_key_hex(self) -> str:
        return self.public_key.hex()

    @property
    def public_key_b64(self) -> str:
        return base64.b64encode(self.public_key).decode()

    def sign(self, message: bytes) -> bytes:
        """Sign a message. Returns algorithm-specific signature."""

        if self.algorithm == SignatureAlgorithm.ED25519:
            if not CRYPTO_ED25519_AVAILABLE:
                return hashlib.sha256(self.private_key + message).digest()
            private = Ed25519PrivateKey.from_private_bytes(self.private_key)
            return private.sign(message)

        elif self.algorithm in (SignatureAlgorithm.DILITHIUM2, SignatureAlgorithm.DILITHIUM3, SignatureAlgorithm.DILITHIUM5):
            if not CRYPTO_PQC_AVAILABLE:
                raise ValueError("liboqs-python not installed")
            alg_name = self.algorithm.value.replace("dilithium", "Dilithium")
            signer = oqs.Signature(alg_name, self.private_key)
            return signer.sign(message)

        elif self.algorithm == SignatureAlgorithm.HYBRID_ED25519_DILITHIUM:
            # Sign with both algorithms, concatenate signatures
            ed_sig = self._sign_ed25519(message)
            if CRYPTO_PQC_AVAILABLE:
                dil_sig = self._sign_dilithium(message)
                # Format: [ed_sig_len:4][ed_sig][dil_sig]
                return len(ed_sig).to_bytes(4, 'big') + ed_sig + dil_sig
            return ed_sig

        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")

    def _sign_ed25519(self, message: bytes) -> bytes:
        """Sign with Ed25519 portion of key."""
        ed_private = self.private_key[:32]  # First 32 bytes for hybrid
        if not CRYPTO_ED25519_AVAILABLE:
            return hashlib.sha256(ed_private + message).digest()
        private = Ed25519PrivateKey.from_private_bytes(ed_private)
        return private.sign(message)

    def _sign_dilithium(self, message: bytes) -> bytes:
        """Sign with Dilithium portion of key."""
        dil_private = self.private_key[32:]  # Rest for Dilithium
        signer = oqs.Signature("Dilithium3", dil_private)
        return signer.sign(message)

    @staticmethod
    def verify(
        public_key: bytes,
        message: bytes,
        signature: bytes,
        algorithm: SignatureAlgorithm = SignatureAlgorithm.ED25519,
    ) -> bool:
        """Verify a signature."""

        if algorithm == SignatureAlgorithm.ED25519:
            if not CRYPTO_ED25519_AVAILABLE:
                expected = hashlib.sha256(public_key + message).digest()
                return signature == expected
            try:
                public = Ed25519PublicKey.from_public_bytes(public_key)
                public.verify(signature, message)
                return True
            except (InvalidSignature, Exception):
                return False

        elif algorithm in (SignatureAlgorithm.DILITHIUM2, SignatureAlgorithm.DILITHIUM3, SignatureAlgorithm.DILITHIUM5):
            if not CRYPTO_PQC_AVAILABLE:
                return False
            alg_name = algorithm.value.replace("dilithium", "Dilithium")
            verifier = oqs.Signature(alg_name)
            return verifier.verify(message, signature, public_key)

        elif algorithm == SignatureAlgorithm.HYBRID_ED25519_DILITHIUM:
            # Must verify BOTH signatures
            try:
                ed_sig_len = int.from_bytes(signature[:4], 'big')
                ed_sig = signature[4:4+ed_sig_len]
                dil_sig = signature[4+ed_sig_len:]

                # Ed25519 public key is first 32 bytes
                ed_pub = public_key[:32]
                ed_valid = KeyPair.verify(ed_pub, message, ed_sig, SignatureAlgorithm.ED25519)

                if CRYPTO_PQC_AVAILABLE and len(dil_sig) > 0:
                    dil_pub = public_key[32:]
                    dil_valid = KeyPair.verify(dil_pub, message, dil_sig, SignatureAlgorithm.DILITHIUM3)
                    return ed_valid and dil_valid

                return ed_valid
            except Exception:
                return False

        return False


# =============================================================================
# CAPABILITY CONSTRAINTS
# =============================================================================

class ConstraintType(str, Enum):
    """Types of capability constraints."""
    SUBPATH = "subpath"         # Path must be under prefix
    REGEX = "regex"             # Value must match regex
    RANGE = "range"             # Numeric value in range
    ENUM = "enum"               # Value must be in list
    MAX_SIZE = "max_size"       # Maximum size limit
    MAX_COUNT = "max_count"     # Maximum count
    TIME_WINDOW = "time_window" # Only valid during window
    RATE_LIMIT = "rate_limit"   # Rate limiting


@dataclass
class Constraint:
    """A single constraint on a capability."""
    type: ConstraintType
    field: str
    value: Any

    def check(self, actual: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if the actual value satisfies this constraint."""
        actual_value = actual.get(self.field)

        if self.type == ConstraintType.SUBPATH:
            if not isinstance(actual_value, str):
                return False, f"{self.field} must be a string path"
            if not actual_value.startswith(self.value):
                return False, f"{self.field} must be under {self.value}"
            return True, None

        elif self.type == ConstraintType.REGEX:
            import re
            if not isinstance(actual_value, str):
                return False, f"{self.field} must be a string"
            if not re.match(self.value, actual_value):
                return False, f"{self.field} does not match pattern"
            return True, None

        elif self.type == ConstraintType.RANGE:
            if not isinstance(actual_value, (int, float)):
                return False, f"{self.field} must be numeric"
            min_val, max_val = self.value
            if not (min_val <= actual_value <= max_val):
                return False, f"{self.field} must be in range [{min_val}, {max_val}]"
            return True, None

        elif self.type == ConstraintType.ENUM:
            if actual_value not in self.value:
                return False, f"{self.field} must be one of {self.value}"
            return True, None

        elif self.type == ConstraintType.MAX_SIZE:
            if isinstance(actual_value, (str, bytes)):
                if len(actual_value) > self.value:
                    return False, f"{self.field} exceeds max size {self.value}"
            return True, None

        elif self.type == ConstraintType.MAX_COUNT:
            if isinstance(actual_value, (list, tuple)):
                if len(actual_value) > self.value:
                    return False, f"{self.field} exceeds max count {self.value}"
            return True, None

        return True, None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,
            "field": self.field,
            "value": self.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Constraint":
        return cls(
            type=ConstraintType(data["type"]),
            field=data["field"],
            value=data["value"],
        )


# Convenience constraint builders
def Subpath(prefix: str, field: str = "path") -> Constraint:
    return Constraint(ConstraintType.SUBPATH, field, prefix)

def Range(min_val: float, max_val: float, field: str = "amount") -> Constraint:
    return Constraint(ConstraintType.RANGE, field, (min_val, max_val))

def AllowedValues(*values, field: str = "action") -> Constraint:
    return Constraint(ConstraintType.ENUM, field, list(values))

def MaxSize(size: int, field: str = "data") -> Constraint:
    return Constraint(ConstraintType.MAX_SIZE, field, size)


# =============================================================================
# WARRANT
# =============================================================================

@dataclass
class Warrant:
    """
    Cryptographic authorization warrant.

    A warrant is a signed statement that says:
    "Issuer X authorizes Holder Y to perform Capability Z
     under Constraints C until time T"

    Properties:
    - Immutable once signed
    - Holder-bound (only the named holder can use it)
    - Time-bound (expires at ttl)
    - Attenuable (can be delegated with narrower scope)
    """

    # Unique warrant ID
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Issuer (who authorized this)
    issuer_pubkey: bytes = field(default=b"")
    issuer_id: Optional[str] = None  # Human-readable issuer ID

    # Holder (who can use this)
    holder_pubkey: bytes = field(default=b"")
    holder_id: Optional[str] = None  # Agent/service ID

    # What is authorized
    capability: str = ""  # e.g., "file_read", "transfer", "search"
    constraints: List[Constraint] = field(default_factory=list)

    # When it's valid
    issued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None

    # Delegation chain
    parent_warrant_id: Optional[str] = None  # If this was delegated from another
    can_delegate: bool = False  # Whether holder can create sub-warrants
    delegation_depth: int = 0  # How many times delegated (0 = root)
    max_delegation_depth: int = 3  # Maximum allowed depth

    # Signature
    signature: bytes = field(default=b"")

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def mint(
        cls,
        issuer_key: KeyPair,
        holder_pubkey: bytes,
        capability: str,
        constraints: List[Constraint] = None,
        ttl: timedelta = None,
        can_delegate: bool = False,
        issuer_id: str = None,
        holder_id: str = None,
        metadata: Dict[str, Any] = None,
    ) -> "Warrant":
        """Mint a new warrant signed by the issuer."""
        now = datetime.now(timezone.utc)
        expires = now + (ttl or timedelta(hours=1))

        warrant = cls(
            issuer_pubkey=issuer_key.public_key,
            issuer_id=issuer_id,
            holder_pubkey=holder_pubkey,
            holder_id=holder_id,
            capability=capability,
            constraints=constraints or [],
            issued_at=now,
            expires_at=expires,
            can_delegate=can_delegate,
            delegation_depth=0,
            metadata=metadata or {},
        )

        # Sign the warrant
        warrant.signature = warrant._sign(issuer_key)

        return warrant

    def delegate(
        self,
        holder_key: KeyPair,
        new_holder_pubkey: bytes,
        narrower_constraints: List[Constraint] = None,
        shorter_ttl: timedelta = None,
        new_holder_id: str = None,
    ) -> "Warrant":
        """
        Delegate this warrant to a new holder.

        The new warrant can only have:
        - Same or narrower constraints
        - Same or shorter TTL
        - Same capability (cannot expand)
        """
        if not self.can_delegate:
            raise ValueError("This warrant cannot be delegated")

        if self.delegation_depth >= self.max_delegation_depth:
            raise ValueError(f"Maximum delegation depth ({self.max_delegation_depth}) reached")

        # Verify the delegating holder owns the holder key
        if holder_key.public_key != self.holder_pubkey:
            raise ValueError("Delegating key does not match warrant holder")

        # Calculate new expiry (can only be shorter)
        now = datetime.now(timezone.utc)
        if shorter_ttl:
            new_expires = now + shorter_ttl
            if new_expires > self.expires_at:
                new_expires = self.expires_at
        else:
            new_expires = self.expires_at

        # Combine constraints (new constraints are additive = more restrictive)
        combined_constraints = list(self.constraints)
        if narrower_constraints:
            combined_constraints.extend(narrower_constraints)

        new_warrant = Warrant(
            issuer_pubkey=holder_key.public_key,  # Delegator becomes issuer
            issuer_id=self.holder_id,
            holder_pubkey=new_holder_pubkey,
            holder_id=new_holder_id,
            capability=self.capability,
            constraints=combined_constraints,
            issued_at=now,
            expires_at=new_expires,
            parent_warrant_id=self.id,
            can_delegate=self.can_delegate,  # Preserve delegation ability
            delegation_depth=self.delegation_depth + 1,
            max_delegation_depth=self.max_delegation_depth,
            metadata={
                "parent_issuer": self.issuer_pubkey.hex(),
                "delegation_chain": self.metadata.get("delegation_chain", []) + [self.id],
            },
        )

        # Sign with the delegating holder's key
        new_warrant.signature = new_warrant._sign(holder_key)

        return new_warrant

    def _sign(self, key: KeyPair) -> bytes:
        """Sign the warrant."""
        message = self._signable_bytes()
        return key.sign(message)

    def _signable_bytes(self) -> bytes:
        """Get the bytes that should be signed."""
        data = {
            "id": self.id,
            "issuer": self.issuer_pubkey.hex(),
            "holder": self.holder_pubkey.hex(),
            "capability": self.capability,
            "constraints": [c.to_dict() for c in self.constraints],
            "issued_at": self.issued_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "parent": self.parent_warrant_id,
            "can_delegate": self.can_delegate,
            "depth": self.delegation_depth,
        }
        return json.dumps(data, sort_keys=True).encode()

    def verify_signature(self) -> bool:
        """Verify the warrant signature."""
        message = self._signable_bytes()
        return KeyPair.verify(self.issuer_pubkey, message, self.signature)

    def is_expired(self) -> bool:
        """Check if the warrant has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def check_constraints(self, action: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Check if an action satisfies all constraints."""
        errors = []
        for constraint in self.constraints:
            ok, error = constraint.check(action)
            if not ok:
                errors.append(error)
        return len(errors) == 0, errors

    def validate_for_action(
        self,
        holder_key: KeyPair,
        action: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        """
        Validate that this warrant authorizes an action.

        Returns (is_valid, list_of_errors).
        """
        errors = []

        # Check signature
        if not self.verify_signature():
            errors.append("Invalid signature")

        # Check holder binding
        if holder_key.public_key != self.holder_pubkey:
            errors.append("Holder key does not match warrant")

        # Check expiry
        if self.is_expired():
            errors.append(f"Warrant expired at {self.expires_at}")

        # Check capability
        if action.get("capability") != self.capability:
            errors.append(f"Action capability '{action.get('capability')}' != warrant capability '{self.capability}'")

        # Check constraints
        ok, constraint_errors = self.check_constraints(action)
        if not ok:
            errors.extend(constraint_errors)

        return len(errors) == 0, errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "issuer_pubkey": self.issuer_pubkey.hex(),
            "issuer_id": self.issuer_id,
            "holder_pubkey": self.holder_pubkey.hex(),
            "holder_id": self.holder_id,
            "capability": self.capability,
            "constraints": [c.to_dict() for c in self.constraints],
            "issued_at": self.issued_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "parent_warrant_id": self.parent_warrant_id,
            "can_delegate": self.can_delegate,
            "delegation_depth": self.delegation_depth,
            "max_delegation_depth": self.max_delegation_depth,
            "signature": self.signature.hex(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Warrant":
        return cls(
            id=data["id"],
            issuer_pubkey=bytes.fromhex(data["issuer_pubkey"]),
            issuer_id=data.get("issuer_id"),
            holder_pubkey=bytes.fromhex(data["holder_pubkey"]),
            holder_id=data.get("holder_id"),
            capability=data["capability"],
            constraints=[Constraint.from_dict(c) for c in data.get("constraints", [])],
            issued_at=datetime.fromisoformat(data["issued_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data.get("expires_at") else None,
            parent_warrant_id=data.get("parent_warrant_id"),
            can_delegate=data.get("can_delegate", False),
            delegation_depth=data.get("delegation_depth", 0),
            max_delegation_depth=data.get("max_delegation_depth", 3),
            signature=bytes.fromhex(data["signature"]),
            metadata=data.get("metadata", {}),
        )


# =============================================================================
# RECEIPT
# =============================================================================

@dataclass
class Receipt:
    """
    Cryptographic proof that an action was authorized.

    A receipt binds:
    - The warrant that authorized the action
    - The action that was performed
    - The time it was validated
    - The validator's signature

    This is the artifact you show later to prove authorization,
    not just a log entry.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # The warrant that authorized this
    warrant_id: str = ""
    warrant_capability: str = ""
    warrant_issuer: str = ""  # hex pubkey
    warrant_holder: str = ""  # hex pubkey

    # The action that was performed
    action: Dict[str, Any] = field(default_factory=dict)
    action_hash: str = ""  # SHA256 of canonical action JSON

    # When it was validated
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # The validator (resource server)
    validator_pubkey: bytes = field(default=b"")
    validator_id: Optional[str] = None

    # Outcome
    success: bool = True
    error: Optional[str] = None

    # Validator's signature over the receipt
    signature: bytes = field(default=b"")

    @classmethod
    def create(
        cls,
        warrant: Warrant,
        action: Dict[str, Any],
        validator_key: KeyPair,
        validator_id: str = None,
        success: bool = True,
        error: str = None,
    ) -> "Receipt":
        """Create a signed receipt for an action."""
        action_json = json.dumps(action, sort_keys=True)
        action_hash = hashlib.sha256(action_json.encode()).hexdigest()

        receipt = cls(
            warrant_id=warrant.id,
            warrant_capability=warrant.capability,
            warrant_issuer=warrant.issuer_pubkey.hex(),
            warrant_holder=warrant.holder_pubkey.hex(),
            action=action,
            action_hash=action_hash,
            validated_at=datetime.now(timezone.utc),
            validator_pubkey=validator_key.public_key,
            validator_id=validator_id,
            success=success,
            error=error,
        )

        # Sign the receipt
        receipt.signature = receipt._sign(validator_key)

        return receipt

    def _sign(self, key: KeyPair) -> bytes:
        message = self._signable_bytes()
        return key.sign(message)

    def _signable_bytes(self) -> bytes:
        data = {
            "id": self.id,
            "warrant_id": self.warrant_id,
            "warrant_capability": self.warrant_capability,
            "warrant_issuer": self.warrant_issuer,
            "warrant_holder": self.warrant_holder,
            "action_hash": self.action_hash,
            "validated_at": self.validated_at.isoformat(),
            "success": self.success,
            "error": self.error,
        }
        return json.dumps(data, sort_keys=True).encode()

    def verify_signature(self) -> bool:
        """Verify the receipt signature."""
        message = self._signable_bytes()
        return KeyPair.verify(self.validator_pubkey, message, self.signature)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "warrant_id": self.warrant_id,
            "warrant_capability": self.warrant_capability,
            "warrant_issuer": self.warrant_issuer,
            "warrant_holder": self.warrant_holder,
            "action": self.action,
            "action_hash": self.action_hash,
            "validated_at": self.validated_at.isoformat(),
            "validator_pubkey": self.validator_pubkey.hex(),
            "validator_id": self.validator_id,
            "success": self.success,
            "error": self.error,
            "signature": self.signature.hex(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Receipt":
        return cls(
            id=data["id"],
            warrant_id=data["warrant_id"],
            warrant_capability=data["warrant_capability"],
            warrant_issuer=data["warrant_issuer"],
            warrant_holder=data["warrant_holder"],
            action=data["action"],
            action_hash=data["action_hash"],
            validated_at=datetime.fromisoformat(data["validated_at"]),
            validator_pubkey=bytes.fromhex(data["validator_pubkey"]),
            validator_id=data.get("validator_id"),
            success=data["success"],
            error=data.get("error"),
            signature=bytes.fromhex(data["signature"]),
        )


# =============================================================================
# WARRANT STORE
# =============================================================================

class WarrantStore:
    """
    Storage for warrants and receipts.

    In production, this would be:
    - Append-only storage
    - Cryptographically signed timestamps
    - Replicated for durability
    """

    def __init__(self):
        self._warrants: Dict[str, Warrant] = {}
        self._receipts: Dict[str, Receipt] = {}
        self._by_holder: Dict[str, Set[str]] = {}  # holder_hex -> warrant_ids
        self._by_capability: Dict[str, Set[str]] = {}  # capability -> warrant_ids

    def store_warrant(self, warrant: Warrant):
        """Store a warrant."""
        self._warrants[warrant.id] = warrant

        holder_hex = warrant.holder_pubkey.hex()
        if holder_hex not in self._by_holder:
            self._by_holder[holder_hex] = set()
        self._by_holder[holder_hex].add(warrant.id)

        if warrant.capability not in self._by_capability:
            self._by_capability[warrant.capability] = set()
        self._by_capability[warrant.capability].add(warrant.id)

    def get_warrant(self, warrant_id: str) -> Optional[Warrant]:
        """Get a warrant by ID."""
        return self._warrants.get(warrant_id)

    def get_warrants_for_holder(self, holder_pubkey: bytes) -> List[Warrant]:
        """Get all warrants for a holder."""
        holder_hex = holder_pubkey.hex()
        warrant_ids = self._by_holder.get(holder_hex, set())
        return [self._warrants[wid] for wid in warrant_ids if wid in self._warrants]

    def get_active_warrant(
        self,
        holder_pubkey: bytes,
        capability: str,
    ) -> Optional[Warrant]:
        """Get an active (non-expired) warrant for a holder and capability."""
        for warrant in self.get_warrants_for_holder(holder_pubkey):
            if warrant.capability == capability and not warrant.is_expired():
                return warrant
        return None

    def store_receipt(self, receipt: Receipt):
        """Store a receipt."""
        self._receipts[receipt.id] = receipt

    def get_receipt(self, receipt_id: str) -> Optional[Receipt]:
        """Get a receipt by ID."""
        return self._receipts.get(receipt_id)

    def get_receipts_for_warrant(self, warrant_id: str) -> List[Receipt]:
        """Get all receipts for a warrant."""
        return [r for r in self._receipts.values() if r.warrant_id == warrant_id]


# =============================================================================
# AUTHORIZATION CONTEXT
# =============================================================================

@dataclass
class AuthorizationContext:
    """
    Context for authorized tool execution.

    Wraps a warrant and provides methods for:
    - Validating actions
    - Creating receipts
    - Delegating to sub-agents
    """

    warrant: Warrant
    holder_key: KeyPair
    validator_key: Optional[KeyPair] = None  # For creating receipts
    store: Optional[WarrantStore] = None

    def can_perform(self, action: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Check if this context authorizes an action."""
        return self.warrant.validate_for_action(self.holder_key, action)

    def execute_with_receipt(
        self,
        action: Dict[str, Any],
        executor: callable,
    ) -> Tuple[Any, Receipt]:
        """
        Execute an action and produce a receipt.

        Returns (result, receipt).
        """
        # Validate
        can, errors = self.can_perform(action)

        if not can:
            # Create failure receipt
            receipt = Receipt.create(
                warrant=self.warrant,
                action=action,
                validator_key=self.validator_key or self.holder_key,
                success=False,
                error="; ".join(errors),
            )
            if self.store:
                self.store.store_receipt(receipt)
            raise PermissionError(f"Warrant validation failed: {errors}")

        try:
            # Execute
            result = executor(action)

            # Create success receipt
            receipt = Receipt.create(
                warrant=self.warrant,
                action=action,
                validator_key=self.validator_key or self.holder_key,
                success=True,
            )

            if self.store:
                self.store.store_receipt(receipt)

            return result, receipt

        except Exception as e:
            # Create error receipt
            receipt = Receipt.create(
                warrant=self.warrant,
                action=action,
                validator_key=self.validator_key or self.holder_key,
                success=False,
                error=str(e),
            )
            if self.store:
                self.store.store_receipt(receipt)
            raise

    def delegate_to(
        self,
        sub_agent_pubkey: bytes,
        narrower_constraints: List[Constraint] = None,
        shorter_ttl: timedelta = None,
        sub_agent_id: str = None,
    ) -> "AuthorizationContext":
        """
        Delegate to a sub-agent with narrower scope.

        Returns a new AuthorizationContext for the sub-agent.
        """
        new_warrant = self.warrant.delegate(
            holder_key=self.holder_key,
            new_holder_pubkey=sub_agent_pubkey,
            narrower_constraints=narrower_constraints,
            shorter_ttl=shorter_ttl,
            new_holder_id=sub_agent_id,
        )

        if self.store:
            self.store.store_warrant(new_warrant)

        # Note: sub-agent needs their own KeyPair
        # This returns a partial context - sub-agent must provide their key
        return AuthorizationContext(
            warrant=new_warrant,
            holder_key=KeyPair(private_key=b"", public_key=sub_agent_pubkey),
            validator_key=self.validator_key,
            store=self.store,
        )


# =============================================================================
# GLOBAL STORE
# =============================================================================

_default_store = WarrantStore()


def get_warrant_store() -> WarrantStore:
    """Get the default warrant store."""
    return _default_store


def set_warrant_store(store: WarrantStore):
    """Set the default warrant store."""
    global _default_store
    _default_store = store
