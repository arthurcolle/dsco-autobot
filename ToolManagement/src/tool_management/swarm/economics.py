"""
Economic Incentive System for Agent Swarms.

Implements:
- Budget constraints tied to warrants
- Payment channels for inter-agent payments
- Revenue sharing with host/designer
- Staking pools for swarm membership

Based on:
- Blockchain-enhanced incentive mechanisms (Nature Scientific Reports)
- Token-based swarm coordination (Fetch.ai, OLAS)
- Smart contract wallet patterns (ERC-4337)
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class RevenueModel(str, Enum):
    """How value flows to host/designer."""
    PERCENTAGE = "percentage"      # % of all transactions
    FLAT_FEE = "flat_fee"         # Fixed fee per transaction
    SUBSCRIPTION = "subscription"  # Time-based fee
    STAKING = "staking"           # Stake-based rewards
    HYBRID = "hybrid"             # Combination


@dataclass
class BudgetConstraint:
    """
    Budget constraint that can be attached to a warrant.

    Links cryptographic authorization to economic limits.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Budget limits
    total_budget: Decimal = Decimal("1000")
    spent: Decimal = Decimal("0")
    remaining: Decimal = field(init=False)

    # Per-operation limits
    max_per_operation: Decimal = Decimal("100")
    min_per_operation: Decimal = Decimal("0.01")

    # Time-based limits
    daily_limit: Optional[Decimal] = None
    hourly_limit: Optional[Decimal] = None
    spent_today: Decimal = Decimal("0")
    spent_this_hour: Decimal = Decimal("0")

    # Token restrictions
    allowed_tokens: List[str] = field(default_factory=lambda: ["USDC", "SOL", "ETH"])

    # Recipient restrictions
    allowed_recipients: List[str] = field(default_factory=list)

    # Linked warrant
    warrant_id: Optional[str] = None

    def __post_init__(self):
        self.remaining = self.total_budget - self.spent

    def can_spend(self, amount: Decimal, token: str = "USDC") -> Tuple[bool, str]:
        """Check if a spend is allowed."""
        if amount > self.remaining:
            return False, f"Insufficient budget: {amount} > {self.remaining}"

        if amount > self.max_per_operation:
            return False, f"Exceeds per-operation limit: {amount} > {self.max_per_operation}"

        if amount < self.min_per_operation:
            return False, f"Below minimum: {amount} < {self.min_per_operation}"

        if self.daily_limit and self.spent_today + amount > self.daily_limit:
            return False, f"Would exceed daily limit: {self.daily_limit}"

        if self.hourly_limit and self.spent_this_hour + amount > self.hourly_limit:
            return False, f"Would exceed hourly limit: {self.hourly_limit}"

        if self.allowed_tokens and token not in self.allowed_tokens:
            return False, f"Token {token} not allowed"

        return True, "OK"

    def record_spend(self, amount: Decimal):
        """Record a spend."""
        self.spent += amount
        self.remaining = self.total_budget - self.spent
        self.spent_today += amount
        self.spent_this_hour += amount

    def reset_daily(self):
        """Reset daily counter."""
        self.spent_today = Decimal("0")

    def reset_hourly(self):
        """Reset hourly counter."""
        self.spent_this_hour = Decimal("0")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "total_budget": str(self.total_budget),
            "spent": str(self.spent),
            "remaining": str(self.remaining),
            "max_per_operation": str(self.max_per_operation),
            "min_per_operation": str(self.min_per_operation),
            "daily_limit": str(self.daily_limit) if self.daily_limit else None,
            "hourly_limit": str(self.hourly_limit) if self.hourly_limit else None,
            "allowed_tokens": self.allowed_tokens,
            "warrant_id": self.warrant_id,
        }


@dataclass
class PaymentChannel:
    """
    Payment channel between two agents.

    Enables high-frequency, low-latency payments without
    on-chain transactions for every payment. Settles periodically.

    Based on state channel patterns (Lightning, Raiden).
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Participants
    agent_a: str = ""  # Agent ID
    agent_b: str = ""

    # Channel state
    balance_a: Decimal = Decimal("0")  # A's balance
    balance_b: Decimal = Decimal("0")  # B's balance
    total_deposited: Decimal = Decimal("0")

    # Settlement
    last_settled_at: Optional[datetime] = None
    pending_settlement: Decimal = Decimal("0")

    # Status
    is_open: bool = True
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Transaction log (off-chain)
    transactions: List[Dict[str, Any]] = field(default_factory=list)

    def deposit(self, agent_id: str, amount: Decimal):
        """Deposit into channel."""
        if agent_id == self.agent_a:
            self.balance_a += amount
        elif agent_id == self.agent_b:
            self.balance_b += amount
        else:
            raise ValueError(f"Agent {agent_id} not in channel")

        self.total_deposited += amount

    def transfer(
        self,
        from_agent: str,
        to_agent: str,
        amount: Decimal,
        purpose: str = "",
    ) -> bool:
        """Transfer within channel (off-chain)."""
        if from_agent == self.agent_a and to_agent == self.agent_b:
            if self.balance_a >= amount:
                self.balance_a -= amount
                self.balance_b += amount
            else:
                return False
        elif from_agent == self.agent_b and to_agent == self.agent_a:
            if self.balance_b >= amount:
                self.balance_b -= amount
                self.balance_a += amount
            else:
                return False
        else:
            return False

        self.transactions.append({
            "from": from_agent,
            "to": to_agent,
            "amount": str(amount),
            "purpose": purpose,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return True

    def settle(self) -> Dict[str, Decimal]:
        """
        Settle the channel on-chain.

        Returns the amounts to transfer on-chain.
        """
        self.last_settled_at = datetime.now(timezone.utc)

        # Calculate net transfers needed
        return {
            self.agent_a: self.balance_a,
            self.agent_b: self.balance_b,
        }

    def close(self) -> Dict[str, Decimal]:
        """Close the channel and return final balances."""
        self.is_open = False
        return self.settle()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "agent_a": self.agent_a,
            "agent_b": self.agent_b,
            "balance_a": str(self.balance_a),
            "balance_b": str(self.balance_b),
            "total_deposited": str(self.total_deposited),
            "is_open": self.is_open,
            "transaction_count": len(self.transactions),
            "last_settled_at": self.last_settled_at.isoformat() if self.last_settled_at else None,
            "opened_at": self.opened_at.isoformat(),
        }


@dataclass
class RevenueShare:
    """
    Revenue sharing configuration for host/designer.

    Defines how value flows back to the swarm creator.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Beneficiary
    host_id: str = ""
    host_wallet_address: str = ""

    # Revenue model
    model: RevenueModel = RevenueModel.PERCENTAGE

    # Rates
    percentage_rate: Decimal = Decimal("0.05")  # 5% of transactions
    flat_fee_per_tx: Decimal = Decimal("0.01")  # $0.01 per tx
    subscription_monthly: Decimal = Decimal("100")  # $100/month

    # Tracking
    total_earned: Decimal = Decimal("0")
    pending_payout: Decimal = Decimal("0")
    last_payout_at: Optional[datetime] = None

    # Payout settings
    min_payout: Decimal = Decimal("10")  # Min to trigger payout
    auto_payout: bool = True

    def calculate_fee(
        self,
        transaction_amount: Decimal,
        transaction_count: int = 1,
    ) -> Decimal:
        """Calculate fee for a transaction."""
        if self.model == RevenueModel.PERCENTAGE:
            return transaction_amount * self.percentage_rate

        elif self.model == RevenueModel.FLAT_FEE:
            return self.flat_fee_per_tx * transaction_count

        elif self.model == RevenueModel.HYBRID:
            pct_fee = transaction_amount * self.percentage_rate
            flat_fee = self.flat_fee_per_tx * transaction_count
            return pct_fee + flat_fee

        return Decimal("0")

    def record_revenue(self, amount: Decimal):
        """Record earned revenue."""
        self.total_earned += amount
        self.pending_payout += amount

    def process_payout(self) -> Optional[Decimal]:
        """Process payout if threshold met."""
        if self.pending_payout >= self.min_payout:
            payout = self.pending_payout
            self.pending_payout = Decimal("0")
            self.last_payout_at = datetime.now(timezone.utc)
            return payout
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "host_id": self.host_id,
            "host_wallet_address": self.host_wallet_address,
            "model": self.model.value,
            "percentage_rate": str(self.percentage_rate),
            "flat_fee_per_tx": str(self.flat_fee_per_tx),
            "total_earned": str(self.total_earned),
            "pending_payout": str(self.pending_payout),
            "last_payout_at": self.last_payout_at.isoformat() if self.last_payout_at else None,
        }


@dataclass
class StakingPool:
    """
    Staking pool for swarm membership and incentives.

    Agents stake tokens to:
    1. Join the swarm (membership requirement)
    2. Signal commitment/quality
    3. Earn rewards for good behavior
    4. Get slashed for bad behavior

    Based on proof-of-stake and DeFi staking patterns.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""

    # Pool configuration
    min_stake: Decimal = Decimal("100")  # Min to join
    max_stake: Decimal = Decimal("10000")  # Max per agent
    total_staked: Decimal = Decimal("0")

    # Rewards
    reward_rate_annual: Decimal = Decimal("0.10")  # 10% APY
    total_rewards_distributed: Decimal = Decimal("0")

    # Slashing
    slash_rate: Decimal = Decimal("0.05")  # 5% slash for violations
    total_slashed: Decimal = Decimal("0")

    # Participants
    stakes: Dict[str, Decimal] = field(default_factory=dict)  # agent_id -> amount
    rewards_earned: Dict[str, Decimal] = field(default_factory=dict)

    # Lock periods
    lock_duration: timedelta = field(default_factory=lambda: timedelta(days=7))
    unstake_requests: Dict[str, datetime] = field(default_factory=dict)

    def stake(self, agent_id: str, amount: Decimal) -> bool:
        """Stake tokens."""
        current = self.stakes.get(agent_id, Decimal("0"))

        if current + amount < self.min_stake and current == 0:
            return False  # Below minimum for new staker

        if current + amount > self.max_stake:
            return False  # Above maximum

        self.stakes[agent_id] = current + amount
        self.total_staked += amount

        if agent_id not in self.rewards_earned:
            self.rewards_earned[agent_id] = Decimal("0")

        return True

    def request_unstake(self, agent_id: str) -> Optional[datetime]:
        """Request to unstake (starts lock period)."""
        if agent_id not in self.stakes:
            return None

        unlock_at = datetime.now(timezone.utc) + self.lock_duration
        self.unstake_requests[agent_id] = unlock_at
        return unlock_at

    def complete_unstake(self, agent_id: str) -> Optional[Decimal]:
        """Complete unstake after lock period."""
        if agent_id not in self.unstake_requests:
            return None

        unlock_at = self.unstake_requests[agent_id]
        if datetime.now(timezone.utc) < unlock_at:
            return None  # Still locked

        amount = self.stakes.pop(agent_id, Decimal("0"))
        rewards = self.rewards_earned.pop(agent_id, Decimal("0"))
        del self.unstake_requests[agent_id]

        self.total_staked -= amount

        return amount + rewards

    def distribute_rewards(self):
        """Distribute rewards proportionally."""
        if self.total_staked == 0:
            return

        # Daily reward = annual rate / 365
        daily_rate = self.reward_rate_annual / 365

        for agent_id, stake in self.stakes.items():
            reward = stake * daily_rate
            self.rewards_earned[agent_id] = (
                self.rewards_earned.get(agent_id, Decimal("0")) + reward
            )
            self.total_rewards_distributed += reward

    def slash(self, agent_id: str, reason: str) -> Decimal:
        """Slash an agent's stake for violation."""
        stake = self.stakes.get(agent_id, Decimal("0"))
        if stake == 0:
            return Decimal("0")

        slash_amount = stake * self.slash_rate
        self.stakes[agent_id] = stake - slash_amount
        self.total_staked -= slash_amount
        self.total_slashed += slash_amount

        logger.warning(
            f"Slashed {agent_id} for {slash_amount}: {reason}"
        )

        return slash_amount

    def get_stake(self, agent_id: str) -> Decimal:
        """Get agent's current stake."""
        return self.stakes.get(agent_id, Decimal("0"))

    def is_member(self, agent_id: str) -> bool:
        """Check if agent is a staked member."""
        return self.stakes.get(agent_id, Decimal("0")) >= self.min_stake

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "min_stake": str(self.min_stake),
            "max_stake": str(self.max_stake),
            "total_staked": str(self.total_staked),
            "reward_rate_annual": str(self.reward_rate_annual),
            "total_rewards_distributed": str(self.total_rewards_distributed),
            "slash_rate": str(self.slash_rate),
            "total_slashed": str(self.total_slashed),
            "member_count": len([a for a, s in self.stakes.items() if s >= self.min_stake]),
            "lock_duration_days": self.lock_duration.days,
        }


class EconomicsEngine:
    """
    Central economics engine for the swarm.

    Coordinates:
    - Budget enforcement
    - Payment channels
    - Revenue sharing
    - Staking pools
    - Fee calculation
    """

    def __init__(self, host_id: str, host_wallet: str):
        self.host_id = host_id
        self.host_wallet = host_wallet

        # Components
        self.revenue_share = RevenueShare(
            host_id=host_id,
            host_wallet_address=host_wallet,
            model=RevenueModel.HYBRID,
        )
        self.staking_pool = StakingPool(name="swarm-membership")

        # Budget constraints by warrant
        self.budgets: Dict[str, BudgetConstraint] = {}

        # Payment channels
        self.channels: Dict[str, PaymentChannel] = {}

        # Transaction log
        self.transactions: List[Dict[str, Any]] = []

    def create_budget(
        self,
        warrant_id: str,
        total: Decimal,
        **kwargs,
    ) -> BudgetConstraint:
        """Create a budget constraint for a warrant."""
        budget = BudgetConstraint(
            warrant_id=warrant_id,
            total_budget=total,
            **kwargs,
        )
        self.budgets[warrant_id] = budget
        return budget

    def get_budget(self, warrant_id: str) -> Optional[BudgetConstraint]:
        """Get budget for a warrant."""
        return self.budgets.get(warrant_id)

    def open_channel(
        self,
        agent_a: str,
        agent_b: str,
        initial_deposit_a: Decimal = Decimal("0"),
        initial_deposit_b: Decimal = Decimal("0"),
    ) -> PaymentChannel:
        """Open a payment channel between agents."""
        channel = PaymentChannel(
            agent_a=agent_a,
            agent_b=agent_b,
        )

        if initial_deposit_a > 0:
            channel.deposit(agent_a, initial_deposit_a)
        if initial_deposit_b > 0:
            channel.deposit(agent_b, initial_deposit_b)

        self.channels[channel.id] = channel
        return channel

    def process_payment(
        self,
        from_agent: str,
        to_agent: str,
        amount: Decimal,
        purpose: str,
        warrant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process a payment between agents.

        1. Check budget constraint
        2. Calculate host fee
        3. Execute transfer (or use channel)
        4. Record transaction
        """
        # Check budget if warrant provided
        if warrant_id:
            budget = self.budgets.get(warrant_id)
            if budget:
                ok, msg = budget.can_spend(amount)
                if not ok:
                    return {"success": False, "error": msg}

        # Calculate and collect host fee
        fee = self.revenue_share.calculate_fee(amount)
        net_amount = amount - fee

        # Record fee
        self.revenue_share.record_revenue(fee)

        # Record budget spend
        if warrant_id and warrant_id in self.budgets:
            self.budgets[warrant_id].record_spend(amount)

        # Record transaction
        tx = {
            "id": str(uuid.uuid4()),
            "from": from_agent,
            "to": to_agent,
            "gross_amount": str(amount),
            "fee": str(fee),
            "net_amount": str(net_amount),
            "purpose": purpose,
            "warrant_id": warrant_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.transactions.append(tx)

        logger.info(
            f"Payment: {from_agent} -> {to_agent}: "
            f"${amount} (fee: ${fee}, net: ${net_amount})"
        )

        return {
            "success": True,
            "transaction": tx,
            "host_fee": str(fee),
        }

    def get_host_earnings(self) -> Dict[str, Any]:
        """Get host/designer earnings summary."""
        return {
            "total_earned": str(self.revenue_share.total_earned),
            "pending_payout": str(self.revenue_share.pending_payout),
            "transaction_count": len(self.transactions),
            "last_payout": self.revenue_share.last_payout_at.isoformat() if self.revenue_share.last_payout_at else None,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host_id": self.host_id,
            "revenue_share": self.revenue_share.to_dict(),
            "staking_pool": self.staking_pool.to_dict(),
            "budget_count": len(self.budgets),
            "channel_count": len(self.channels),
            "transaction_count": len(self.transactions),
        }
