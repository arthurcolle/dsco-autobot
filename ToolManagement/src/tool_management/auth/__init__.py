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
]
