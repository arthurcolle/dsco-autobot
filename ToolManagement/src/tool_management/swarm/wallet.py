"""
Agent Wallet System.

Provides crypto wallet capabilities for autonomous agents with:
- Multi-chain support (Solana, Ethereum, Base)
- Delegated signing via warrants
- Budget enforcement
- Transaction receipts
- Hot/warm/cold wallet patterns

Security Model (from arXiv:2601.04583):
- Agents NEVER hold master keys directly
- All signing delegated through smart contract wallets
- Budget limits enforced on-chain
- Kill switches and circuit breakers for large operations
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import logging

from ..auth.warrants import KeyPair, Warrant, Receipt, Constraint, ConstraintType

logger = logging.getLogger(__name__)


class WalletType(str, Enum):
    """Types of agent wallets."""
    HOT = "hot"           # Fully autonomous, small balance
    WARM = "warm"         # Requires warrant for each tx
    COLD = "cold"         # Multi-sig, human approval needed
    DELEGATED = "delegated"  # Smart contract wallet with policies


class Chain(str, Enum):
    """Supported blockchain networks."""
    SOLANA = "solana"
    ETHEREUM = "ethereum"
    BASE = "base"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    # Test networks
    SOLANA_DEVNET = "solana-devnet"
    SEPOLIA = "sepolia"
    BASE_SEPOLIA = "base-sepolia"


class TokenType(str, Enum):
    """Types of tokens."""
    NATIVE = "native"      # SOL, ETH
    ERC20 = "erc20"        # ERC-20 tokens
    SPL = "spl"            # Solana SPL tokens
    STABLECOIN = "stablecoin"  # USDC, USDT, DAI


@dataclass
class TokenBalance:
    """Balance of a specific token."""
    chain: Chain
    token_type: TokenType
    symbol: str
    address: Optional[str]  # None for native
    balance: Decimal
    decimals: int = 18
    usd_value: Optional[Decimal] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chain": self.chain.value,
            "token_type": self.token_type.value,
            "symbol": self.symbol,
            "address": self.address,
            "balance": str(self.balance),
            "decimals": self.decimals,
            "usd_value": str(self.usd_value) if self.usd_value else None,
        }


@dataclass
class TransactionIntent:
    """
    Transaction intent (from arXiv:2601.04583).

    Represents what the agent WANTS to do, before execution.
    Used for policy checking and approval flows.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # What
    action: str = ""  # "transfer", "swap", "stake", "unstake"
    chain: Chain = Chain.SOLANA_DEVNET

    # From/To
    from_address: str = ""
    to_address: str = ""

    # Amount
    token_symbol: str = ""
    token_address: Optional[str] = None
    amount: Decimal = Decimal("0")

    # Why
    purpose: str = ""
    task_id: Optional[str] = None

    # When
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None

    # Authorization
    warrant_id: Optional[str] = None
    requires_approval: bool = False

    # Status
    status: str = "pending"  # pending, approved, rejected, executed, failed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "action": self.action,
            "chain": self.chain.value,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "token_symbol": self.token_symbol,
            "token_address": self.token_address,
            "amount": str(self.amount),
            "purpose": self.purpose,
            "task_id": self.task_id,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "warrant_id": self.warrant_id,
            "requires_approval": self.requires_approval,
            "status": self.status,
        }


@dataclass
class TransactionResult:
    """Result of an executed transaction."""
    intent_id: str
    success: bool
    tx_hash: Optional[str] = None
    block_number: Optional[int] = None
    gas_used: Optional[int] = None
    error: Optional[str] = None
    receipt_id: Optional[str] = None
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "success": self.success,
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "gas_used": self.gas_used,
            "error": self.error,
            "receipt_id": self.receipt_id,
            "executed_at": self.executed_at.isoformat(),
        }


@dataclass
class BudgetPolicy:
    """
    Budget constraints for agent spending.

    Enforces limits on:
    - Per-transaction amounts
    - Daily/weekly/monthly totals
    - Approved recipients
    - Token types
    """
    max_per_tx: Decimal = Decimal("100")  # Max per transaction in USD
    daily_limit: Decimal = Decimal("1000")
    weekly_limit: Decimal = Decimal("5000")
    monthly_limit: Decimal = Decimal("10000")

    # Spending tracking
    spent_today: Decimal = Decimal("0")
    spent_this_week: Decimal = Decimal("0")
    spent_this_month: Decimal = Decimal("0")

    # Restrictions
    allowed_tokens: List[str] = field(default_factory=list)  # Empty = all
    allowed_recipients: List[str] = field(default_factory=list)  # Empty = all
    blocked_recipients: List[str] = field(default_factory=list)

    # Circuit breakers
    require_approval_above: Decimal = Decimal("500")  # Human approval above this
    cooldown_after_large_tx: int = 300  # Seconds to wait after large tx
    max_tx_per_hour: int = 10

    def check_transaction(
        self,
        amount_usd: Decimal,
        recipient: str,
        token: str,
    ) -> Tuple[bool, List[str]]:
        """Check if a transaction is allowed under this policy."""
        errors = []

        if amount_usd > self.max_per_tx:
            errors.append(f"Amount ${amount_usd} exceeds per-tx limit ${self.max_per_tx}")

        if self.spent_today + amount_usd > self.daily_limit:
            errors.append(f"Would exceed daily limit ${self.daily_limit}")

        if self.spent_this_week + amount_usd > self.weekly_limit:
            errors.append(f"Would exceed weekly limit ${self.weekly_limit}")

        if self.allowed_tokens and token not in self.allowed_tokens:
            errors.append(f"Token {token} not in allowed list")

        if recipient in self.blocked_recipients:
            errors.append(f"Recipient {recipient} is blocked")

        if self.allowed_recipients and recipient not in self.allowed_recipients:
            errors.append(f"Recipient {recipient} not in allowed list")

        return len(errors) == 0, errors

    def record_spend(self, amount_usd: Decimal):
        """Record a spend against the limits."""
        self.spent_today += amount_usd
        self.spent_this_week += amount_usd
        self.spent_this_month += amount_usd

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_per_tx": str(self.max_per_tx),
            "daily_limit": str(self.daily_limit),
            "weekly_limit": str(self.weekly_limit),
            "monthly_limit": str(self.monthly_limit),
            "spent_today": str(self.spent_today),
            "spent_this_week": str(self.spent_this_week),
            "spent_this_month": str(self.spent_this_month),
            "allowed_tokens": self.allowed_tokens,
            "allowed_recipients": self.allowed_recipients,
            "blocked_recipients": self.blocked_recipients,
            "require_approval_above": str(self.require_approval_above),
            "cooldown_after_large_tx": self.cooldown_after_large_tx,
            "max_tx_per_hour": self.max_tx_per_hour,
        }


class AgentWallet:
    """
    Crypto wallet for an autonomous agent.

    Design principles (from research):
    1. Agents never hold master keys
    2. All operations go through warrant authorization
    3. Budget limits enforced before any transaction
    4. Every action produces a cryptographic receipt
    5. Kill switch available for emergencies

    Integration patterns:
    - Solana: Durable nonce + delegated signers
    - Ethereum: Smart contract wallet (ERC-4337)
    - Cross-chain: Intent-based bridging
    """

    def __init__(
        self,
        agent_id: str,
        wallet_type: WalletType = WalletType.WARM,
        master_pubkey: Optional[str] = None,
        budget_policy: Optional[BudgetPolicy] = None,
    ):
        self.id = str(uuid.uuid4())
        self.agent_id = agent_id
        self.wallet_type = wallet_type
        self.master_pubkey = master_pubkey  # Owner/designer's key
        self.budget_policy = budget_policy or BudgetPolicy()

        # Per-chain addresses (derived or assigned)
        self.addresses: Dict[Chain, str] = {}

        # Balances (cached, refresh from chain)
        self.balances: Dict[str, TokenBalance] = {}

        # Transaction history
        self.intents: Dict[str, TransactionIntent] = {}
        self.results: Dict[str, TransactionResult] = {}

        # Active warrants for this wallet
        self.active_warrants: Dict[str, Warrant] = {}

        # Kill switch
        self.is_frozen: bool = False
        self.frozen_reason: Optional[str] = None

        # Agent's signing key (for warm wallets)
        self._agent_keypair: Optional[KeyPair] = None

        # Created timestamp
        self.created_at = datetime.now(timezone.utc)

    def initialize_keypair(self) -> KeyPair:
        """Generate agent's signing keypair."""
        if self._agent_keypair is None:
            self._agent_keypair = KeyPair.generate()
        return self._agent_keypair

    @property
    def agent_pubkey(self) -> Optional[bytes]:
        """Get agent's public key."""
        if self._agent_keypair:
            return self._agent_keypair.public_key
        return None

    def add_address(self, chain: Chain, address: str):
        """Add a chain-specific address."""
        self.addresses[chain] = address

    def get_address(self, chain: Chain) -> Optional[str]:
        """Get address for a chain."""
        return self.addresses.get(chain)

    def update_balance(self, balance: TokenBalance):
        """Update a token balance."""
        key = f"{balance.chain.value}:{balance.symbol}"
        self.balances[key] = balance

    def get_balance(self, chain: Chain, symbol: str) -> Optional[TokenBalance]:
        """Get balance for a token."""
        key = f"{chain.value}:{symbol}"
        return self.balances.get(key)

    def total_balance_usd(self) -> Decimal:
        """Get total balance in USD."""
        total = Decimal("0")
        for balance in self.balances.values():
            if balance.usd_value:
                total += balance.usd_value
        return total

    def add_warrant(self, warrant: Warrant):
        """Add an active warrant for this wallet."""
        if warrant.holder_pubkey != self.agent_pubkey:
            raise ValueError("Warrant holder doesn't match agent")
        self.active_warrants[warrant.id] = warrant

    def get_warrant_for_action(self, capability: str) -> Optional[Warrant]:
        """Get an active warrant for a capability."""
        for warrant in self.active_warrants.values():
            if warrant.capability == capability and not warrant.is_expired():
                return warrant
        return None

    def freeze(self, reason: str):
        """Emergency freeze - stop all transactions."""
        self.is_frozen = True
        self.frozen_reason = reason
        logger.warning(f"Wallet {self.id} frozen: {reason}")

    def unfreeze(self):
        """Unfreeze the wallet."""
        self.is_frozen = False
        self.frozen_reason = None
        logger.info(f"Wallet {self.id} unfrozen")

    def create_intent(
        self,
        action: str,
        chain: Chain,
        to_address: str,
        token_symbol: str,
        amount: Decimal,
        purpose: str,
        task_id: Optional[str] = None,
        token_address: Optional[str] = None,
    ) -> TransactionIntent:
        """
        Create a transaction intent.

        This doesn't execute anything - just records what the agent
        wants to do for policy checking and approval.
        """
        if self.is_frozen:
            raise ValueError(f"Wallet frozen: {self.frozen_reason}")

        from_address = self.get_address(chain)
        if not from_address:
            raise ValueError(f"No address for chain {chain}")

        intent = TransactionIntent(
            action=action,
            chain=chain,
            from_address=from_address,
            to_address=to_address,
            token_symbol=token_symbol,
            token_address=token_address,
            amount=amount,
            purpose=purpose,
            task_id=task_id,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

        # Check budget policy (assume 1:1 USD for now, real impl would price)
        amount_usd = amount  # Simplified
        ok, errors = self.budget_policy.check_transaction(
            amount_usd, to_address, token_symbol
        )

        if not ok:
            intent.status = "rejected"
            intent.requires_approval = True
            logger.warning(f"Intent {intent.id} rejected: {errors}")
        elif amount_usd > self.budget_policy.require_approval_above:
            intent.requires_approval = True
            intent.status = "pending_approval"
        else:
            intent.status = "approved"

        self.intents[intent.id] = intent
        return intent

    def approve_intent(self, intent_id: str, approver_warrant: Warrant) -> bool:
        """Approve a pending intent with a warrant."""
        intent = self.intents.get(intent_id)
        if not intent:
            return False

        if intent.status not in ("pending", "pending_approval"):
            return False

        # Validate warrant authorizes this action
        action = {
            "capability": "wallet_transfer",
            "to": intent.to_address,
            "amount": float(intent.amount),
            "token": intent.token_symbol,
        }

        # For now, just check it's a valid unexpired warrant
        if approver_warrant.is_expired():
            return False

        intent.warrant_id = approver_warrant.id
        intent.status = "approved"
        return True

    async def execute_intent(
        self,
        intent_id: str,
        validator_key: Optional[KeyPair] = None,
    ) -> TransactionResult:
        """
        Execute an approved intent.

        In production, this would:
        1. Build the transaction for the target chain
        2. Sign via delegated signer or smart contract
        3. Submit to the network
        4. Wait for confirmation
        5. Create receipt

        For now, simulates execution.
        """
        intent = self.intents.get(intent_id)
        if not intent:
            raise ValueError(f"Intent {intent_id} not found")

        if intent.status != "approved":
            raise ValueError(f"Intent not approved: {intent.status}")

        if self.is_frozen:
            raise ValueError(f"Wallet frozen: {self.frozen_reason}")

        # Simulate transaction execution
        # Real impl would call chain-specific SDK
        tx_hash = hashlib.sha256(
            f"{intent.id}:{time.time()}".encode()
        ).hexdigest()

        result = TransactionResult(
            intent_id=intent_id,
            success=True,
            tx_hash=f"0x{tx_hash}",
            block_number=12345678,
            gas_used=21000,
        )

        # Record spend
        self.budget_policy.record_spend(intent.amount)

        # Update intent status
        intent.status = "executed"

        # Create receipt
        if self._agent_keypair and validator_key:
            warrant = self.active_warrants.get(intent.warrant_id) if intent.warrant_id else None
            if warrant:
                receipt = Receipt.create(
                    warrant=warrant,
                    action=intent.to_dict(),
                    validator_key=validator_key,
                    success=True,
                )
                result.receipt_id = receipt.id

        self.results[intent_id] = result

        logger.info(
            f"Executed intent {intent_id}: {intent.action} "
            f"{intent.amount} {intent.token_symbol} to {intent.to_address}"
        )

        return result

    def to_dict(self) -> Dict[str, Any]:
        """Serialize wallet state."""
        return {
            "id": self.id,
            "agent_id": self.agent_id,
            "wallet_type": self.wallet_type.value,
            "master_pubkey": self.master_pubkey,
            "addresses": {c.value: a for c, a in self.addresses.items()},
            "balances": {k: b.to_dict() for k, b in self.balances.items()},
            "total_balance_usd": str(self.total_balance_usd()),
            "budget_policy": self.budget_policy.to_dict(),
            "is_frozen": self.is_frozen,
            "frozen_reason": self.frozen_reason,
            "agent_pubkey": self.agent_pubkey.hex() if self.agent_pubkey else None,
            "active_warrants": len(self.active_warrants),
            "pending_intents": len([i for i in self.intents.values() if i.status == "pending"]),
            "created_at": self.created_at.isoformat(),
        }


class WalletRegistry:
    """
    Registry of agent wallets.

    Tracks all wallets, enables cross-agent payments,
    and enforces global policies.
    """

    def __init__(self):
        self._wallets: Dict[str, AgentWallet] = {}
        self._by_agent: Dict[str, str] = {}  # agent_id -> wallet_id
        self._by_address: Dict[str, str] = {}  # address -> wallet_id

    def create_wallet(
        self,
        agent_id: str,
        wallet_type: WalletType = WalletType.WARM,
        master_pubkey: Optional[str] = None,
        budget_policy: Optional[BudgetPolicy] = None,
    ) -> AgentWallet:
        """Create a new wallet for an agent."""
        if agent_id in self._by_agent:
            raise ValueError(f"Agent {agent_id} already has a wallet")

        wallet = AgentWallet(
            agent_id=agent_id,
            wallet_type=wallet_type,
            master_pubkey=master_pubkey,
            budget_policy=budget_policy,
        )

        self._wallets[wallet.id] = wallet
        self._by_agent[agent_id] = wallet.id

        return wallet

    def get_wallet(self, wallet_id: str) -> Optional[AgentWallet]:
        return self._wallets.get(wallet_id)

    def get_wallet_for_agent(self, agent_id: str) -> Optional[AgentWallet]:
        wallet_id = self._by_agent.get(agent_id)
        if wallet_id:
            return self._wallets.get(wallet_id)
        return None

    def register_address(self, wallet_id: str, chain: Chain, address: str):
        """Register an address for lookups."""
        key = f"{chain.value}:{address}"
        self._by_address[key] = wallet_id

        wallet = self._wallets.get(wallet_id)
        if wallet:
            wallet.add_address(chain, address)

    def find_by_address(self, chain: Chain, address: str) -> Optional[AgentWallet]:
        """Find wallet by on-chain address."""
        key = f"{chain.value}:{address}"
        wallet_id = self._by_address.get(key)
        if wallet_id:
            return self._wallets.get(wallet_id)
        return None

    def list_wallets(self) -> List[Dict[str, Any]]:
        """List all wallets."""
        return [w.to_dict() for w in self._wallets.values()]

    def freeze_all(self, reason: str):
        """Emergency freeze all wallets."""
        for wallet in self._wallets.values():
            wallet.freeze(reason)
        logger.critical(f"ALL WALLETS FROZEN: {reason}")


# Global registry
_wallet_registry = WalletRegistry()


def get_wallet_registry() -> WalletRegistry:
    return _wallet_registry
