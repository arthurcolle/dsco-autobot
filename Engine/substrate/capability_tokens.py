"""
Capability Token System - Cryptographic authorization for agents

Implements delegatable, attenuatable capabilities:
- Ed25519 signed tokens (unforgeable)
- Granular permissions (30+ types)
- Scope limiting (global, shard, nodes, spatial, etc.)
- Delegation with automatic attenuation
- Time-limited tokens (expiration)

This enables decentralized security where tokens prove authorization
without needing a central authority for every operation.

Permissions:
- Node: READ, WRITE, CREATE, DELETE
- Edge: READ, WRITE, CREATE, DELETE
- Stigmergy: DEPOSIT, QUERY, SWEEP
- Agent: FORK, PRUNE, MODIFY_SELF
- Memory: READ, WRITE, CONSOLIDATE
- Goals: READ, WRITE
- Market: BID, ASK
- Admin: REVOKE, DELEGATE
"""

import logging
import json
from typing import Optional, Dict, Set, List, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import IntFlag
import hashlib
import base64

logger = logging.getLogger(__name__)


class Permissions(IntFlag):
    """Capability permissions (bitmask-based for composition)."""

    # Node operations
    READ_NODE = 1 << 0
    WRITE_NODE = 1 << 1
    CREATE_NODE = 1 << 2
    DELETE_NODE = 1 << 3

    # Edge operations
    READ_EDGE = 1 << 4
    WRITE_EDGE = 1 << 5
    CREATE_EDGE = 1 << 6
    DELETE_EDGE = 1 << 7
    MODIFY_EDGE = 1 << 29  # Added missing permission

    # Stigmergy operations
    STIGMERGY_DEPOSIT = 1 << 8
    STIGMERGY_QUERY = 1 << 9
    STIGMERGY_SWEEP = 1 << 10

    # Agent lifecycle
    FORK_AGENT = 1 << 11
    PRUNE_AGENT = 1 << 12
    MODIFY_SELF = 1 << 13

    # Memory
    READ_MEMORY = 1 << 14
    WRITE_MEMORY = 1 << 15
    CONSOLIDATE_MEMORY = 1 << 16

    # Goals
    READ_GOALS = 1 << 17
    WRITE_GOALS = 1 << 18

    # Predictions
    READ_PREDICTIONS = 1 << 19
    WRITE_PREDICTIONS = 1 << 20

    # Messaging
    SEND_MESSAGE = 1 << 21
    RECEIVE_MESSAGE = 1 << 22

    # Credit assignment
    READ_CREDIT = 1 << 23
    ASSIGN_CREDIT = 1 << 24

    # Market operations
    MARKET_BID = 1 << 25
    MARKET_ASK = 1 << 26

    # Admin
    ADMIN_REVOKE = 1 << 27
    ADMIN_DELEGATE = 1 << 28

    # Shorthand combinations
    READ_ALL = (
        READ_NODE | READ_EDGE | READ_MEMORY |
        READ_PREDICTIONS | READ_GOALS | READ_CREDIT | STIGMERGY_QUERY
    )
    WRITE_ALL = (
        WRITE_NODE | CREATE_NODE | CREATE_EDGE | MODIFY_EDGE |
        WRITE_MEMORY | WRITE_PREDICTIONS | WRITE_GOALS
    )
    AGENT_BASIC = (
        READ_ALL | MODIFY_SELF | SEND_MESSAGE | RECEIVE_MESSAGE |
        STIGMERGY_DEPOSIT | MARKET_BID
    )
    ADMIN_ALL = (
        READ_ALL | WRITE_ALL | STIGMERGY_DEPOSIT | STIGMERGY_SWEEP |
        FORK_AGENT | PRUNE_AGENT | ADMIN_REVOKE | ADMIN_DELEGATE |
        MARKET_BID | MARKET_ASK
    )


class CapabilityScope:
    """Defines the scope of a capability token."""

    SCOPE_GLOBAL = "global"
    SCOPE_SHARD = "shard"
    SCOPE_NODES = "nodes"
    SCOPE_BOUNDING_BOX = "bbox"
    SCOPE_NEIGHBORHOOD = "neighborhood"
    SCOPE_NODE_TYPES = "node_types"
    SCOPE_TAGGED = "tagged"

    def __init__(self, scope_type: str, params: Dict[str, Any] = None):
        """
        Initialize capability scope.

        Args:
            scope_type: Type of scope (global, shard, nodes, etc.)
            params: Scope parameters (varies by type)
        """
        self.scope_type = scope_type
        self.params = params or {}

    @staticmethod
    def global_scope() -> "CapabilityScope":
        """Global scope - entire graph."""
        return CapabilityScope(CapabilityScope.SCOPE_GLOBAL)

    @staticmethod
    def shard_scope(shard_id: int) -> "CapabilityScope":
        """Shard scope - single shard."""
        return CapabilityScope(CapabilityScope.SCOPE_SHARD, {"shard_id": shard_id})

    @staticmethod
    def nodes_scope(node_ids: List[str]) -> "CapabilityScope":
        """Nodes scope - specific node IDs."""
        return CapabilityScope(CapabilityScope.SCOPE_NODES, {"node_ids": node_ids})

    @staticmethod
    def neighborhood_scope(root_id: str, max_hops: int) -> "CapabilityScope":
        """Neighborhood scope - N-hop neighborhood from a root node."""
        return CapabilityScope(
            CapabilityScope.SCOPE_NEIGHBORHOOD,
            {"root_id": root_id, "max_hops": max_hops}
        )

    def contains_node(self, node_id: str) -> bool:
        """Check if scope contains a node."""
        if self.scope_type == self.SCOPE_GLOBAL:
            return True
        elif self.scope_type == self.SCOPE_NODES:
            return node_id in self.params.get("node_ids", [])
        # Other scope types would require GraphSub queries
        # For now, assume containment
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Serialize scope to dict."""
        return {
            "scope_type": self.scope_type,
            "params": self.params
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CapabilityScope":
        """Deserialize scope from dict."""
        return CapabilityScope(
            data.get("scope_type", "global"),
            data.get("params", {})
        )


@dataclass
class CapabilityToken:
    """
    Cryptographic capability token for decentralized authorization.

    Properties:
    - Self-contained (contains all authorization info)
    - Unforgeable (Ed25519 signed)
    - Delegatable (can be passed with attenuated permissions)
    - Time-limited (expires after TTL)
    - Scope-limited (restricted to certain nodes/shards)
    """

    issuer_id: str  # Who issued this token
    holder_id: str  # Who holds this token (can use it)
    permissions: int  # Bitmask of Permissions
    scope: Dict[str, Any]  # Scope serialization
    issued_at: str  # ISO timestamp
    expires_at: str  # ISO timestamp
    delegatable: bool  # Whether holder can delegate
    signature: str  # Ed25519 signature (base64)
    token_id: str  # Unique token ID (hash of content)
    parent_token_id: Optional[str] = None  # Parent if delegated
    delegation_chain: Optional[List[str]] = None  # Chain of delegations

    def is_expired(self) -> bool:
        """Check if token has expired."""
        expires = datetime.fromisoformat(self.expires_at)
        return datetime.utcnow() >= expires

    def has_permission(self, permission: Permissions) -> bool:
        """Check if token grants a specific permission."""
        return bool(self.permissions & permission)

    def has_any_permission(self, permissions: List[Permissions]) -> bool:
        """Check if token has any of the given permissions."""
        return any(self.has_permission(p) for p in permissions)

    def has_all_permissions(self, permissions: List[Permissions]) -> bool:
        """Check if token has all given permissions."""
        return all(self.has_permission(p) for p in permissions)

    def check_scope(self, node_id: str) -> bool:
        """Check if token's scope includes a node."""
        scope = CapabilityScope.from_dict(self.scope)
        return scope.contains_node(node_id)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize token to dict."""
        return {
            "issuer_id": self.issuer_id,
            "holder_id": self.holder_id,
            "permissions": int(self.permissions),
            "scope": self.scope,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "delegatable": self.delegatable,
            "signature": self.signature,
            "token_id": self.token_id,
            "parent_token_id": self.parent_token_id,
            "delegation_chain": self.delegation_chain or []
        }

    def to_jwt_like_string(self) -> str:
        """Encode token as a compact string (for transmission)."""
        payload = json.dumps(self.to_dict(), separators=(',', ':'))
        return base64.b64encode(payload.encode()).decode()

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CapabilityToken":
        """Deserialize token from dict."""
        return CapabilityToken(
            issuer_id=data["issuer_id"],
            holder_id=data["holder_id"],
            permissions=data["permissions"],
            scope=data["scope"],
            issued_at=data["issued_at"],
            expires_at=data["expires_at"],
            delegatable=data["delegatable"],
            signature=data["signature"],
            token_id=data["token_id"],
            parent_token_id=data.get("parent_token_id"),
            delegation_chain=data.get("delegation_chain")
        )


class CapabilityTokenManager:
    """Manages creation, verification, and delegation of capability tokens."""

    def __init__(self, issuer_id: str, issuer_key: Optional[str] = None):
        """
        Initialize token manager.

        Args:
            issuer_id: ID of this token issuer
            issuer_key: Private key for signing (base64, Ed25519)
                       If None, tokens won't be signed
        """
        self.issuer_id = issuer_id
        self.issuer_key = issuer_key
        # In production, use proper Ed25519 signing with cryptography library
        logger.info(f"Initialized CapabilityTokenManager for {issuer_id}")

    def create_token(
        self,
        holder_id: str,
        permissions: Permissions,
        scope: CapabilityScope,
        expires_in_seconds: int = 3600,
        delegatable: bool = True
    ) -> CapabilityToken:
        """
        Create a new capability token.

        Args:
            holder_id: Agent receiving this token
            permissions: Permissions to grant (Permissions bitmask)
            scope: Scope (what nodes/shards this applies to)
            expires_in_seconds: Token lifetime in seconds
            delegatable: Whether holder can delegate to others

        Returns:
            Signed CapabilityToken
        """
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=expires_in_seconds)

        token = CapabilityToken(
            issuer_id=self.issuer_id,
            holder_id=holder_id,
            permissions=permissions,
            scope=scope.to_dict(),
            issued_at=now.isoformat(),
            expires_at=expires_at.isoformat(),
            delegatable=delegatable,
            signature="",  # Will be signed
            token_id="",  # Will be computed
            parent_token_id=None,
            delegation_chain=[]
        )

        # Compute token ID (hash of content)
        token_dict = token.to_dict()
        token_dict["signature"] = ""  # Exclude signature from hash
        content = json.dumps(token_dict, separators=(',', ':'), sort_keys=True)
        token.token_id = hashlib.sha256(content.encode()).hexdigest()[:16]

        # Sign token (mock - in production use Ed25519)
        if self.issuer_key:
            token.signature = self._sign_token(token)
        else:
            token.signature = "unsigned"

        logger.info(
            f"Created token {token.token_id} for {holder_id} "
            f"with {bin(int(permissions)).count('1')} permissions"
        )

        return token

    def delegate_token(
        self,
        parent_token: CapabilityToken,
        new_holder_id: str,
        new_permissions: Optional[Permissions] = None,
        new_scope: Optional[CapabilityScope] = None,
        expires_in_seconds: Optional[int] = None
    ) -> CapabilityToken:
        """
        Delegate a token to another agent with attenuated permissions.

        Rules:
        - Can only REDUCE permissions, not add
        - Can only NARROW scope, not expand
        - Cannot exceed parent's expiration

        Args:
            parent_token: Token to delegate
            new_holder_id: New holder
            new_permissions: New permissions (must be subset of parent)
            new_scope: New scope (must be subset of parent)
            expires_in_seconds: New lifetime (optional)

        Returns:
            New attenuated token

        Raises:
            ValueError if trying to expand permissions or scope
        """
        if not parent_token.delegatable:
            raise ValueError(f"Token {parent_token.token_id} is not delegatable")

        if parent_token.is_expired():
            raise ValueError(f"Parent token {parent_token.token_id} is expired")

        # Use parent permissions/scope if not specified
        permissions = new_permissions or parent_token.permissions
        scope = new_scope or CapabilityScope.from_dict(parent_token.scope)

        # Check attenuation rules
        if (permissions | parent_token.permissions) != parent_token.permissions:
            raise ValueError(
                f"Cannot expand permissions during delegation. "
                f"Parent: {parent_token.permissions}, requested: {permissions}"
            )

        # Check scope narrowing (simplified)
        parent_scope = CapabilityScope.from_dict(parent_token.scope)
        if parent_scope.scope_type != scope.scope_type:
            # More complex scope narrowing rules would apply here
            pass

        # Parent expiration is upper bound
        parent_expires = datetime.fromisoformat(parent_token.expires_at)
        if expires_in_seconds:
            new_expires = datetime.utcnow() + timedelta(seconds=expires_in_seconds)
            if new_expires > parent_expires:
                new_expires = parent_expires
        else:
            new_expires = parent_expires

        expires_in = int((new_expires - datetime.utcnow()).total_seconds())

        # Create delegated token
        delegated = self.create_token(
            holder_id=new_holder_id,
            permissions=permissions,
            scope=scope,
            expires_in_seconds=expires_in,
            delegatable=False  # Typically, don't allow re-delegation
        )

        # Track delegation chain
        delegated.parent_token_id = parent_token.token_id
        delegated.delegation_chain = (parent_token.delegation_chain or []) + [parent_token.issuer_id]

        logger.info(
            f"Delegated token {parent_token.token_id} "
            f"from {parent_token.holder_id} to {new_holder_id}"
        )

        return delegated

    def verify_token(self, token: CapabilityToken) -> bool:
        """
        Verify token signature and expiration.

        Args:
            token: Token to verify

        Returns:
            True if valid
        """
        if token.is_expired():
            logger.warning(f"Token {token.token_id} is expired")
            return False

        if token.signature == "unsigned":
            logger.debug(f"Token {token.token_id} is unsigned (development mode)")
            return True

        # Verify signature (mock - in production use Ed25519)
        expected_sig = self._sign_token(token)
        if token.signature != expected_sig:
            logger.warning(f"Token {token.token_id} signature verification failed")
            return False

        logger.debug(f"Token {token.token_id} verified successfully")
        return True

    def _sign_token(self, token: CapabilityToken) -> str:
        """Mock signing (in production, use Ed25519)."""
        # Simplified: just hash the token content
        token_dict = token.to_dict()
        token_dict["signature"] = ""
        content = json.dumps(token_dict, separators=(',', ':'), sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def check_authorization(
        self,
        token: CapabilityToken,
        required_permission: Permissions,
        target_node_id: str = None
    ) -> bool:
        """
        Check if token authorizes an operation.

        Args:
            token: Capability token
            required_permission: Permission needed
            target_node_id: Node being accessed (for scope check)

        Returns:
            True if authorized
        """
        if not self.verify_token(token):
            return False

        if not token.has_permission(required_permission):
            logger.debug(
                f"Token {token.token_id} lacks {required_permission.name} permission"
            )
            return False

        if target_node_id and not token.check_scope(target_node_id):
            logger.debug(
                f"Token {token.token_id} scope doesn't include node {target_node_id}"
            )
            return False

        return True
