"""
Billing Hooks - Integration points for billing and agent management systems.

Provides:
- Credit deduction hooks
- Organization-based billing
- Agent execution tracking
- Usage aggregation for invoicing

These hooks are designed to integrate with an external agent management system.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from .durable_store import ToolRequest, ToolResponse

logger = logging.getLogger(__name__)


# =============================================================================
# BILLING MODELS
# =============================================================================

class UsageRecord(BaseModel):
    """Single usage record for billing."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    request_id: str

    # Who
    client_id: str
    organization_id: Optional[str] = None
    agent_id: Optional[str] = None

    # What
    tool_id: str
    tool_name: str
    service_id: str

    # When
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Cost
    tokens_input: int = 0
    tokens_output: int = 0
    cost_cents: int = 0

    # Metadata
    success: bool = True
    execution_time_ms: float = 0


class CreditBalance(BaseModel):
    """Credit balance for an organization or client."""
    id: str
    balance_cents: int
    reserved_cents: int = 0  # For in-flight requests
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class BillingEvent(BaseModel):
    """Event emitted for billing system integration."""
    event_type: str  # "usage", "credit_deducted", "low_balance", etc.
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# BILLING HOOK INTERFACE
# =============================================================================

class BillingHook(ABC):
    """
    Abstract base for billing system integration.

    Implement this to connect tool usage to your billing system.
    """

    @abstractmethod
    async def check_balance(
        self,
        client_id: str,
        organization_id: Optional[str] = None,
        required_cents: int = 0,
    ) -> bool:
        """Check if client/org has sufficient balance."""
        pass

    @abstractmethod
    async def reserve_credits(
        self,
        client_id: str,
        request_id: str,
        amount_cents: int,
        organization_id: Optional[str] = None,
    ) -> bool:
        """Reserve credits before execution (for long-running tasks)."""
        pass

    @abstractmethod
    async def release_reservation(
        self,
        request_id: str,
        actual_cost_cents: int,
    ):
        """Release reservation and apply actual cost."""
        pass

    @abstractmethod
    async def record_usage(self, record: UsageRecord):
        """Record usage for billing."""
        pass

    @abstractmethod
    async def deduct_credits(
        self,
        client_id: str,
        amount_cents: int,
        request_id: str,
        organization_id: Optional[str] = None,
    ) -> bool:
        """Deduct credits from balance."""
        pass


# =============================================================================
# DEFAULT IMPLEMENTATIONS
# =============================================================================

class NoOpBillingHook(BillingHook):
    """No-op billing hook for testing or free tiers."""

    async def check_balance(self, *args, **kwargs) -> bool:
        return True

    async def reserve_credits(self, *args, **kwargs) -> bool:
        return True

    async def release_reservation(self, *args, **kwargs):
        pass

    async def record_usage(self, record: UsageRecord):
        logger.debug(f"Usage recorded: {record.tool_name} - {record.cost_cents} cents")

    async def deduct_credits(self, *args, **kwargs) -> bool:
        return True


class InMemoryBillingHook(BillingHook):
    """
    In-memory billing for development/testing.

    Not for production - use a database-backed implementation.
    """

    def __init__(self, default_balance_cents: int = 100000):
        self.balances: Dict[str, int] = {}  # client_id -> cents
        self.reservations: Dict[str, tuple] = {}  # request_id -> (client_id, cents)
        self.usage_records: List[UsageRecord] = []
        self.default_balance = default_balance_cents

    async def check_balance(
        self,
        client_id: str,
        organization_id: Optional[str] = None,
        required_cents: int = 0,
    ) -> bool:
        key = organization_id or client_id
        balance = self.balances.get(key, self.default_balance)
        return balance >= required_cents

    async def reserve_credits(
        self,
        client_id: str,
        request_id: str,
        amount_cents: int,
        organization_id: Optional[str] = None,
    ) -> bool:
        key = organization_id or client_id
        balance = self.balances.get(key, self.default_balance)

        if balance < amount_cents:
            return False

        self.balances[key] = balance - amount_cents
        self.reservations[request_id] = (key, amount_cents)
        return True

    async def release_reservation(
        self,
        request_id: str,
        actual_cost_cents: int,
    ):
        if request_id not in self.reservations:
            return

        key, reserved = self.reservations.pop(request_id)
        refund = reserved - actual_cost_cents

        if refund > 0:
            self.balances[key] = self.balances.get(key, 0) + refund

    async def record_usage(self, record: UsageRecord):
        self.usage_records.append(record)
        logger.debug(f"Usage: {record.tool_name} - {record.cost_cents}¢")

    async def deduct_credits(
        self,
        client_id: str,
        amount_cents: int,
        request_id: str,
        organization_id: Optional[str] = None,
    ) -> bool:
        key = organization_id or client_id
        balance = self.balances.get(key, self.default_balance)

        if balance < amount_cents:
            return False

        self.balances[key] = balance - amount_cents
        return True

    def get_balance(self, client_id: str) -> int:
        """Get current balance."""
        return self.balances.get(client_id, self.default_balance)

    def get_usage(
        self,
        client_id: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> List[UsageRecord]:
        """Get usage records."""
        records = self.usage_records

        if client_id:
            records = [r for r in records if r.client_id == client_id]
        if since:
            records = [r for r in records if r.timestamp >= since]

        return records


# =============================================================================
# AGENT INTEGRATION HOOKS
# =============================================================================

@dataclass
class AgentContext:
    """Context for agent-initiated tool calls."""
    agent_id: str
    agent_name: Optional[str] = None
    organization_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_task_id: Optional[str] = None


class AgentHook(ABC):
    """
    Hook for agent management system integration.

    Implement this to track tool usage by agents.
    """

    @abstractmethod
    async def on_tool_start(
        self,
        request: ToolRequest,
        context: AgentContext,
    ):
        """Called when an agent starts a tool execution."""
        pass

    @abstractmethod
    async def on_tool_complete(
        self,
        request: ToolRequest,
        response: ToolResponse,
        context: AgentContext,
    ):
        """Called when an agent completes a tool execution."""
        pass

    @abstractmethod
    async def get_allowed_tools(
        self,
        agent_id: str,
    ) -> Optional[List[str]]:
        """Get list of tool IDs the agent is allowed to use (None = all)."""
        pass


class NoOpAgentHook(AgentHook):
    """No-op agent hook for standalone use."""

    async def on_tool_start(self, *args, **kwargs):
        pass

    async def on_tool_complete(self, *args, **kwargs):
        pass

    async def get_allowed_tools(self, agent_id: str) -> Optional[List[str]]:
        return None  # All tools allowed


# =============================================================================
# HTTP WEBHOOK HOOK
# =============================================================================

class WebhookBillingHook(BillingHook):
    """
    Billing hook that calls external webhooks.

    Use this to integrate with your agent management API.
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        import httpx

        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method,
                f"{self.base_url}{path}",
                json=data,
                headers=headers,
            )
            response.raise_for_status()
            return response.json()

    async def check_balance(
        self,
        client_id: str,
        organization_id: Optional[str] = None,
        required_cents: int = 0,
    ) -> bool:
        try:
            result = await self._request("GET", f"/billing/balance", {
                "client_id": client_id,
                "organization_id": organization_id,
            })
            return result.get("balance_cents", 0) >= required_cents
        except Exception as e:
            logger.error(f"Balance check failed: {e}")
            return True  # Fail open (configure as needed)

    async def reserve_credits(
        self,
        client_id: str,
        request_id: str,
        amount_cents: int,
        organization_id: Optional[str] = None,
    ) -> bool:
        try:
            result = await self._request("POST", "/billing/reserve", {
                "client_id": client_id,
                "organization_id": organization_id,
                "request_id": request_id,
                "amount_cents": amount_cents,
            })
            return result.get("success", False)
        except Exception as e:
            logger.error(f"Credit reservation failed: {e}")
            return True

    async def release_reservation(
        self,
        request_id: str,
        actual_cost_cents: int,
    ):
        try:
            await self._request("POST", "/billing/release", {
                "request_id": request_id,
                "actual_cost_cents": actual_cost_cents,
            })
        except Exception as e:
            logger.error(f"Release reservation failed: {e}")

    async def record_usage(self, record: UsageRecord):
        try:
            await self._request("POST", "/billing/usage", record.model_dump())
        except Exception as e:
            logger.error(f"Usage recording failed: {e}")

    async def deduct_credits(
        self,
        client_id: str,
        amount_cents: int,
        request_id: str,
        organization_id: Optional[str] = None,
    ) -> bool:
        try:
            result = await self._request("POST", "/billing/deduct", {
                "client_id": client_id,
                "organization_id": organization_id,
                "request_id": request_id,
                "amount_cents": amount_cents,
            })
            return result.get("success", False)
        except Exception as e:
            logger.error(f"Credit deduction failed: {e}")
            return True


# =============================================================================
# BILLING MANAGER
# =============================================================================

class BillingManager:
    """
    Central billing manager for service wrappers.

    Coordinates billing hooks and provides a unified interface.
    """

    def __init__(
        self,
        billing_hook: Optional[BillingHook] = None,
        agent_hook: Optional[AgentHook] = None,
    ):
        self.billing_hook = billing_hook or NoOpBillingHook()
        self.agent_hook = agent_hook or NoOpAgentHook()
        self.event_handlers: List[Callable[[BillingEvent], None]] = []
        self._reserved_requests: Dict[str, int] = {}

    def on_event(self, handler: Callable[[BillingEvent], None]):
        """Register event handler."""
        self.event_handlers.append(handler)

    async def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit billing event."""
        event = BillingEvent(event_type=event_type, data=data)
        for handler in self.event_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler error: {e}")

    async def pre_execute(
        self,
        request: ToolRequest,
        estimated_cost_cents: int = 0,
        agent_context: Optional[AgentContext] = None,
    ) -> bool:
        """
        Pre-execution checks and reservations.

        Returns True if execution should proceed.
        """
        estimated_cost_cents = max(0, int(estimated_cost_cents))

        # Check balance
        has_balance = await self.billing_hook.check_balance(
            client_id=request.client_id or "unknown",
            organization_id=request.organization_id,
            required_cents=estimated_cost_cents,
        )

        if not has_balance:
            await self._emit_event("insufficient_balance", {
                "request_id": request.id,
                "client_id": request.client_id,
                "required_cents": estimated_cost_cents,
            })
            return False

        # Reserve credits for expensive operations
        if estimated_cost_cents > 100:  # Only reserve for >$1
            reserved = await self.billing_hook.reserve_credits(
                client_id=request.client_id or "unknown",
                request_id=request.id,
                amount_cents=estimated_cost_cents,
                organization_id=request.organization_id,
            )
            if not reserved:
                return False
            self._reserved_requests[request.id] = estimated_cost_cents

        # Notify agent system
        if agent_context:
            await self.agent_hook.on_tool_start(request, agent_context)

        return True

    async def post_execute(
        self,
        request: ToolRequest,
        response: ToolResponse,
        agent_context: Optional[AgentContext] = None,
    ):
        """Post-execution billing and tracking."""
        actual_cost_cents = max(0, int(request.cost_cents))
        reserved_amount = self._reserved_requests.pop(request.id, 0)

        # If credits were reserved up-front, settle against reservation.
        # If actual cost exceeded reservation, deduct the delta only.
        if reserved_amount > 0:
            await self.billing_hook.release_reservation(
                request_id=request.id,
                actual_cost_cents=actual_cost_cents,
            )
            if actual_cost_cents > reserved_amount:
                await self.billing_hook.deduct_credits(
                    client_id=request.client_id or "unknown",
                    amount_cents=actual_cost_cents - reserved_amount,
                    request_id=request.id,
                    organization_id=request.organization_id,
                )
        elif actual_cost_cents > 0:
            await self.billing_hook.deduct_credits(
                client_id=request.client_id or "unknown",
                amount_cents=actual_cost_cents,
                request_id=request.id,
                organization_id=request.organization_id,
            )

        # Record usage
        usage = UsageRecord(
            request_id=request.id,
            client_id=request.client_id or "unknown",
            organization_id=request.organization_id,
            agent_id=agent_context.agent_id if agent_context else None,
            tool_id=request.tool_id,
            tool_name=request.tool_name,
            service_id=request.service_id,
            tokens_input=response.tokens_input,
            tokens_output=response.tokens_output,
            cost_cents=request.cost_cents,
            success=response.status.value == "completed",
            execution_time_ms=response.execution_time_ms,
        )
        await self.billing_hook.record_usage(usage)

        # Emit event
        await self._emit_event("usage", usage.model_dump())

        # Notify agent system
        if agent_context:
            await self.agent_hook.on_tool_complete(request, response, agent_context)
