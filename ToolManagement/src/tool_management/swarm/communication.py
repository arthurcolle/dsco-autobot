"""
Inter-Agent Communication System.

Enables agents to:
- Send messages to each other
- Negotiate task terms
- Form coalitions
- Share information
- Coordinate on complex tasks

Communication Patterns:
1. Direct messaging - Agent to agent
2. Broadcast - To all agents in swarm
3. Topic-based pub/sub - Subscribe to interests
4. Negotiation protocols - Auction, bargaining
5. Consensus - Voting, quorum

Based on:
- Contract Net Protocol (FIPA)
- Multi-agent negotiation theory
- Decentralized coordination mechanisms
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    """Types of inter-agent messages."""
    # Basic communication
    DIRECT = "direct"
    BROADCAST = "broadcast"
    TOPIC = "topic"

    # Task-related
    CALL_FOR_PROPOSAL = "cfp"          # Request bids for a task
    PROPOSE = "propose"                 # Submit a bid
    ACCEPT_PROPOSAL = "accept"          # Accept a bid
    REJECT_PROPOSAL = "reject"          # Reject a bid

    # Negotiation
    NEGOTIATE = "negotiate"             # Open negotiation
    COUNTER_OFFER = "counter_offer"     # Counter-proposal
    AGREE = "agree"                     # Agreement reached
    DISAGREE = "disagree"               # No agreement

    # Coordination
    INFORM = "inform"                   # Share information
    REQUEST = "request"                 # Request action
    CONFIRM = "confirm"                 # Confirm receipt/action
    CANCEL = "cancel"                   # Cancel request

    # Coalition
    INVITE = "invite"                   # Invite to coalition
    JOIN = "join"                       # Join coalition
    LEAVE = "leave"                     # Leave coalition

    # Consensus
    VOTE_REQUEST = "vote_request"
    VOTE = "vote"
    VOTE_RESULT = "vote_result"


class NegotiationStrategy(str, Enum):
    """Negotiation strategies."""
    FIXED = "fixed"                     # No negotiation, take it or leave it
    TIME_DECAY = "time_decay"           # Concede as deadline approaches
    TIT_FOR_TAT = "tit_for_tat"         # Mirror opponent's concessions
    BOULWARE = "boulware"               # Concede slowly at first, then fast
    CONCEDER = "conceder"               # Concede quickly at first


@dataclass
class Message:
    """A message between agents."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Routing
    sender_id: str = ""
    recipient_id: Optional[str] = None  # None for broadcast
    topic: Optional[str] = None

    # Content
    type: MessageType = MessageType.DIRECT
    content: Dict[str, Any] = field(default_factory=dict)

    # Metadata
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    reply_to: Optional[str] = None  # Message ID being replied to
    conversation_id: Optional[str] = None

    # Delivery tracking
    delivered: bool = False
    read: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "sender_id": self.sender_id,
            "recipient_id": self.recipient_id,
            "topic": self.topic,
            "type": self.type.value,
            "content": self.content,
            "timestamp": self.timestamp.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "reply_to": self.reply_to,
            "conversation_id": self.conversation_id,
            "delivered": self.delivered,
            "read": self.read,
        }


@dataclass
class Proposal:
    """A proposal/bid for a task."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Task reference
    task_id: str = ""
    cfp_id: str = ""  # Call for proposal ID

    # Bidder
    agent_id: str = ""
    agent_name: str = ""

    # Terms
    price: Decimal = Decimal("0")
    estimated_duration: int = 0  # seconds
    quality_commitment: float = 0.8  # 0-1 scale
    capabilities_offered: List[str] = field(default_factory=list)

    # Additional terms
    conditions: Dict[str, Any] = field(default_factory=dict)

    # Status
    status: str = "pending"  # pending, accepted, rejected, withdrawn
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "cfp_id": self.cfp_id,
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "price": str(self.price),
            "estimated_duration": self.estimated_duration,
            "quality_commitment": self.quality_commitment,
            "capabilities_offered": self.capabilities_offered,
            "conditions": self.conditions,
            "status": self.status,
            "submitted_at": self.submitted_at.isoformat(),
        }


@dataclass
class Negotiation:
    """A negotiation session between agents."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Parties
    initiator_id: str = ""
    responder_id: str = ""

    # Subject
    subject: str = ""  # What's being negotiated
    task_id: Optional[str] = None

    # Terms being negotiated
    initial_offer: Dict[str, Any] = field(default_factory=dict)
    current_offer: Dict[str, Any] = field(default_factory=dict)
    counter_offers: List[Dict[str, Any]] = field(default_factory=list)

    # Bounds
    initiator_min: Dict[str, Decimal] = field(default_factory=dict)
    initiator_max: Dict[str, Decimal] = field(default_factory=dict)
    responder_min: Dict[str, Decimal] = field(default_factory=dict)
    responder_max: Dict[str, Decimal] = field(default_factory=dict)

    # Status
    status: str = "active"  # active, agreed, failed, timeout
    rounds: int = 0
    max_rounds: int = 10

    # Timing
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    deadline: Optional[datetime] = None
    ended_at: Optional[datetime] = None

    # Agreement
    final_terms: Optional[Dict[str, Any]] = None

    def make_counter_offer(
        self,
        agent_id: str,
        offer: Dict[str, Any],
    ) -> bool:
        """Make a counter-offer."""
        if self.status != "active":
            return False

        if self.rounds >= self.max_rounds:
            self.status = "failed"
            self.ended_at = datetime.now(timezone.utc)
            return False

        self.counter_offers.append({
            "round": self.rounds + 1,
            "agent_id": agent_id,
            "offer": offer,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.current_offer = offer
        self.rounds += 1

        return True

    def accept(self, agent_id: str) -> bool:
        """Accept the current offer."""
        if self.status != "active":
            return False

        self.status = "agreed"
        self.final_terms = self.current_offer
        self.ended_at = datetime.now(timezone.utc)

        logger.info(f"Negotiation {self.id} agreed after {self.rounds} rounds")
        return True

    def reject(self, agent_id: str, reason: str = ""):
        """Reject and end negotiation."""
        self.status = "failed"
        self.ended_at = datetime.now(timezone.utc)
        logger.info(f"Negotiation {self.id} failed: {reason}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "initiator_id": self.initiator_id,
            "responder_id": self.responder_id,
            "subject": self.subject,
            "task_id": self.task_id,
            "current_offer": self.current_offer,
            "status": self.status,
            "rounds": self.rounds,
            "max_rounds": self.max_rounds,
            "started_at": self.started_at.isoformat(),
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "final_terms": self.final_terms,
        }


@dataclass
class Coalition:
    """
    A coalition of agents working together.

    Coalitions form to:
    - Tackle tasks requiring multiple capabilities
    - Share risk and rewards
    - Pool resources
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""

    # Leadership
    leader_id: str = ""

    # Members
    member_ids: Set[str] = field(default_factory=set)

    # Purpose
    task_id: Optional[str] = None
    goal: str = ""
    combined_capabilities: Set[str] = field(default_factory=set)

    # Economics
    reward_pool: Decimal = Decimal("0")
    reward_shares: Dict[str, Decimal] = field(default_factory=dict)  # agent_id -> share

    # Status
    status: str = "forming"  # forming, active, completed, dissolved
    formed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def add_member(self, agent_id: str, capabilities: List[str], share: Decimal):
        """Add a member to the coalition."""
        self.member_ids.add(agent_id)
        self.combined_capabilities.update(capabilities)
        self.reward_shares[agent_id] = share

    def remove_member(self, agent_id: str):
        """Remove a member from the coalition."""
        self.member_ids.discard(agent_id)
        self.reward_shares.pop(agent_id, None)

    def distribute_rewards(self) -> Dict[str, Decimal]:
        """Distribute reward pool according to shares."""
        if self.reward_pool == 0:
            return {}

        total_shares = sum(self.reward_shares.values())
        if total_shares == 0:
            return {}

        distributions = {}
        for agent_id, share in self.reward_shares.items():
            distributions[agent_id] = (share / total_shares) * self.reward_pool

        return distributions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "leader_id": self.leader_id,
            "member_ids": list(self.member_ids),
            "task_id": self.task_id,
            "goal": self.goal,
            "combined_capabilities": list(self.combined_capabilities),
            "reward_pool": str(self.reward_pool),
            "status": self.status,
            "member_count": len(self.member_ids),
        }


class MessageBus:
    """
    Message bus for inter-agent communication.

    Features:
    - Direct messaging
    - Pub/sub topics
    - Message persistence
    - Delivery guarantees
    """

    def __init__(self):
        # Message storage
        self.messages: Dict[str, Message] = {}
        self.inbox: Dict[str, List[str]] = defaultdict(list)  # agent_id -> message_ids

        # Topic subscriptions
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)  # topic -> agent_ids

        # Message handlers
        self.handlers: Dict[str, List[Callable]] = defaultdict(list)  # agent_id -> handlers

        # Conversations
        self.conversations: Dict[str, List[str]] = defaultdict(list)  # conv_id -> message_ids

    def subscribe(self, agent_id: str, topic: str):
        """Subscribe to a topic."""
        self.subscriptions[topic].add(agent_id)
        logger.debug(f"Agent {agent_id} subscribed to {topic}")

    def unsubscribe(self, agent_id: str, topic: str):
        """Unsubscribe from a topic."""
        self.subscriptions[topic].discard(agent_id)

    def register_handler(self, agent_id: str, handler: Callable):
        """Register a message handler for an agent."""
        self.handlers[agent_id].append(handler)

    async def send(self, message: Message) -> str:
        """Send a message."""
        self.messages[message.id] = message

        # Track conversation
        if message.conversation_id:
            self.conversations[message.conversation_id].append(message.id)

        # Route message
        if message.type == MessageType.BROADCAST:
            # Deliver to all agents (caller should filter)
            for agent_id in self.handlers.keys():
                if agent_id != message.sender_id:
                    self.inbox[agent_id].append(message.id)
                    await self._deliver(agent_id, message)

        elif message.type == MessageType.TOPIC and message.topic:
            # Deliver to topic subscribers
            for agent_id in self.subscriptions.get(message.topic, set()):
                if agent_id != message.sender_id:
                    self.inbox[agent_id].append(message.id)
                    await self._deliver(agent_id, message)

        elif message.recipient_id:
            # Direct message
            self.inbox[message.recipient_id].append(message.id)
            await self._deliver(message.recipient_id, message)

        message.delivered = True
        return message.id

    async def _deliver(self, agent_id: str, message: Message):
        """Deliver message to agent's handlers."""
        for handler in self.handlers.get(agent_id, []):
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)
            except Exception as e:
                logger.error(f"Handler error for {agent_id}: {e}")

    def get_inbox(self, agent_id: str, unread_only: bool = False) -> List[Message]:
        """Get messages for an agent."""
        message_ids = self.inbox.get(agent_id, [])
        messages = [self.messages[mid] for mid in message_ids if mid in self.messages]

        if unread_only:
            messages = [m for m in messages if not m.read]

        return messages

    def mark_read(self, message_id: str):
        """Mark a message as read."""
        if message_id in self.messages:
            self.messages[message_id].read = True

    def get_conversation(self, conversation_id: str) -> List[Message]:
        """Get all messages in a conversation."""
        message_ids = self.conversations.get(conversation_id, [])
        return [self.messages[mid] for mid in message_ids if mid in self.messages]


class NegotiationEngine:
    """
    Engine for multi-agent negotiation.

    Implements various negotiation protocols:
    - Contract Net Protocol
    - Auction-based allocation
    - Bilateral negotiation
    """

    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus

        # Active negotiations
        self.negotiations: Dict[str, Negotiation] = {}

        # Proposals
        self.proposals: Dict[str, List[Proposal]] = {}  # cfp_id -> proposals

        # Coalitions
        self.coalitions: Dict[str, Coalition] = {}

    async def call_for_proposals(
        self,
        sender_id: str,
        task_id: str,
        requirements: Dict[str, Any],
        deadline: datetime,
    ) -> str:
        """
        Broadcast a call for proposals (Contract Net Protocol).

        Returns CFP ID.
        """
        cfp_id = str(uuid.uuid4())

        message = Message(
            sender_id=sender_id,
            type=MessageType.CALL_FOR_PROPOSAL,
            content={
                "cfp_id": cfp_id,
                "task_id": task_id,
                "requirements": requirements,
                "deadline": deadline.isoformat(),
            },
            expires_at=deadline,
            conversation_id=cfp_id,
        )

        await self.message_bus.send(message)

        self.proposals[cfp_id] = []
        logger.info(f"CFP {cfp_id} broadcast for task {task_id}")

        return cfp_id

    async def submit_proposal(
        self,
        sender_id: str,
        cfp_id: str,
        proposal: Proposal,
    ) -> str:
        """Submit a proposal in response to CFP."""
        proposal.cfp_id = cfp_id
        proposal.agent_id = sender_id

        if cfp_id in self.proposals:
            self.proposals[cfp_id].append(proposal)

        # Send proposal message
        message = Message(
            sender_id=sender_id,
            type=MessageType.PROPOSE,
            content=proposal.to_dict(),
            conversation_id=cfp_id,
        )
        await self.message_bus.send(message)

        logger.info(f"Proposal {proposal.id} submitted for CFP {cfp_id}")
        return proposal.id

    async def evaluate_proposals(
        self,
        cfp_id: str,
        evaluator: Callable[[List[Proposal]], Optional[Proposal]] = None,
    ) -> Optional[Proposal]:
        """
        Evaluate proposals and select a winner.

        Default: select lowest price with quality >= 0.7
        """
        proposals = self.proposals.get(cfp_id, [])
        if not proposals:
            return None

        if evaluator:
            return evaluator(proposals)

        # Default evaluation: price-quality tradeoff
        valid = [p for p in proposals if p.quality_commitment >= 0.7]
        if not valid:
            valid = proposals

        # Sort by price
        valid.sort(key=lambda p: p.price)
        return valid[0] if valid else None

    async def accept_proposal(
        self,
        sender_id: str,
        proposal_id: str,
        cfp_id: str,
    ):
        """Accept a proposal."""
        proposals = self.proposals.get(cfp_id, [])
        for p in proposals:
            if p.id == proposal_id:
                p.status = "accepted"

                # Notify winner
                message = Message(
                    sender_id=sender_id,
                    recipient_id=p.agent_id,
                    type=MessageType.ACCEPT_PROPOSAL,
                    content={"proposal_id": proposal_id, "cfp_id": cfp_id},
                    conversation_id=cfp_id,
                )
                await self.message_bus.send(message)

            else:
                p.status = "rejected"

                # Notify losers
                message = Message(
                    sender_id=sender_id,
                    recipient_id=p.agent_id,
                    type=MessageType.REJECT_PROPOSAL,
                    content={"proposal_id": p.id, "cfp_id": cfp_id},
                    conversation_id=cfp_id,
                )
                await self.message_bus.send(message)

    async def start_negotiation(
        self,
        initiator_id: str,
        responder_id: str,
        subject: str,
        initial_offer: Dict[str, Any],
        bounds: Dict[str, Tuple[Decimal, Decimal]] = None,
    ) -> Negotiation:
        """Start a bilateral negotiation."""
        negotiation = Negotiation(
            initiator_id=initiator_id,
            responder_id=responder_id,
            subject=subject,
            initial_offer=initial_offer,
            current_offer=initial_offer,
            deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
        )

        if bounds:
            for key, (min_val, max_val) in bounds.items():
                negotiation.initiator_min[key] = min_val
                negotiation.initiator_max[key] = max_val

        self.negotiations[negotiation.id] = negotiation

        # Send negotiation request
        message = Message(
            sender_id=initiator_id,
            recipient_id=responder_id,
            type=MessageType.NEGOTIATE,
            content={
                "negotiation_id": negotiation.id,
                "subject": subject,
                "initial_offer": initial_offer,
            },
            conversation_id=negotiation.id,
        )
        await self.message_bus.send(message)

        return negotiation

    async def counter_offer(
        self,
        negotiation_id: str,
        agent_id: str,
        offer: Dict[str, Any],
    ) -> bool:
        """Make a counter-offer in a negotiation."""
        negotiation = self.negotiations.get(negotiation_id)
        if not negotiation:
            return False

        success = negotiation.make_counter_offer(agent_id, offer)
        if not success:
            return False

        # Determine recipient
        recipient_id = (
            negotiation.responder_id
            if agent_id == negotiation.initiator_id
            else negotiation.initiator_id
        )

        message = Message(
            sender_id=agent_id,
            recipient_id=recipient_id,
            type=MessageType.COUNTER_OFFER,
            content={
                "negotiation_id": negotiation_id,
                "offer": offer,
                "round": negotiation.rounds,
            },
            conversation_id=negotiation_id,
        )
        await self.message_bus.send(message)

        return True

    async def accept_negotiation(
        self,
        negotiation_id: str,
        agent_id: str,
    ) -> bool:
        """Accept the current offer in a negotiation."""
        negotiation = self.negotiations.get(negotiation_id)
        if not negotiation:
            return False

        success = negotiation.accept(agent_id)
        if not success:
            return False

        # Notify other party
        recipient_id = (
            negotiation.responder_id
            if agent_id == negotiation.initiator_id
            else negotiation.initiator_id
        )

        message = Message(
            sender_id=agent_id,
            recipient_id=recipient_id,
            type=MessageType.AGREE,
            content={
                "negotiation_id": negotiation_id,
                "final_terms": negotiation.final_terms,
            },
            conversation_id=negotiation_id,
        )
        await self.message_bus.send(message)

        return True

    async def form_coalition(
        self,
        leader_id: str,
        name: str,
        goal: str,
        invited_agents: List[str],
    ) -> Coalition:
        """Form a coalition and invite agents."""
        coalition = Coalition(
            name=name,
            leader_id=leader_id,
            goal=goal,
        )
        coalition.member_ids.add(leader_id)

        self.coalitions[coalition.id] = coalition

        # Send invitations
        for agent_id in invited_agents:
            message = Message(
                sender_id=leader_id,
                recipient_id=agent_id,
                type=MessageType.INVITE,
                content={
                    "coalition_id": coalition.id,
                    "name": name,
                    "goal": goal,
                    "leader_id": leader_id,
                },
                conversation_id=coalition.id,
            )
            await self.message_bus.send(message)

        return coalition

    async def join_coalition(
        self,
        coalition_id: str,
        agent_id: str,
        capabilities: List[str],
        requested_share: Decimal,
    ) -> bool:
        """Join a coalition."""
        coalition = self.coalitions.get(coalition_id)
        if not coalition or coalition.status != "forming":
            return False

        coalition.add_member(agent_id, capabilities, requested_share)

        # Notify leader
        message = Message(
            sender_id=agent_id,
            recipient_id=coalition.leader_id,
            type=MessageType.JOIN,
            content={
                "coalition_id": coalition_id,
                "capabilities": capabilities,
                "requested_share": str(requested_share),
            },
            conversation_id=coalition_id,
        )
        await self.message_bus.send(message)

        return True


# Global instances
_message_bus = MessageBus()
_negotiation_engine = NegotiationEngine(_message_bus)


def get_message_bus() -> MessageBus:
    return _message_bus


def get_negotiation_engine() -> NegotiationEngine:
    return _negotiation_engine
