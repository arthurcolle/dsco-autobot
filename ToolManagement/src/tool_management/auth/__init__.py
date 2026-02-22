"""
Authorization primitives for tool management.

Implements Tenuo-style cryptographic authorization:
- Warrants: Signed, scoped, time-bound authorization objects
- Receipts: Cryptographic proof of authorized actions
- Constraints: Path, range, enum, and other restrictions
- Delegation: Attenuable authority transfer to sub-agents
"""

from .warrants import (
    # Core types
    KeyPair,
    Warrant,
    Receipt,
    Constraint,
    ConstraintType,
    AuthorizationContext,
    WarrantStore,

    # Constraint builders
    Subpath,
    Range,
    AllowedValues,
    MaxSize,

    # Store management
    get_warrant_store,
    set_warrant_store,
)
from .wallet_owner import (
    build_swarm_owner_message,
    verify_electrum_signature,
    require_swarm_owner_mutation,
    clear_swarm_owner_nonce_cache,
    is_swarm_owner_enforcement_enabled,
)

__all__ = [
    "KeyPair",
    "Warrant",
    "Receipt",
    "Constraint",
    "ConstraintType",
    "AuthorizationContext",
    "WarrantStore",
    "Subpath",
    "Range",
    "AllowedValues",
    "MaxSize",
    "get_warrant_store",
    "set_warrant_store",
    "build_swarm_owner_message",
    "verify_electrum_signature",
    "require_swarm_owner_mutation",
    "clear_swarm_owner_nonce_cache",
    "is_swarm_owner_enforcement_enabled",
]
