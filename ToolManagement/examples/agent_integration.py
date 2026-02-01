#!/usr/bin/env python3
"""
Example: Integrating Tool Services with Agent Management System.

This shows how to connect the white-label tool services with an
agent management platform (like distributed.systems).

The key integration points are:
1. BillingHook - Credits, usage tracking
2. AgentHook - Tool access control, execution tracking
3. RegistryClient - Tool discovery
4. Toolkit - Contextualized, renamed tools for agent personas
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tool_management.services import (
    BillingHook,
    BillingManager,
    AgentHook,
    AgentContext,
    UsageRecord,
    RegistryClient,
    ToolRequest,
    ToolResponse,
    # Toolkits
    Toolkit,
    ToolBinding,
    ToolkitManager,
    create_research_toolkit,
    create_data_toolkit,
    create_task_toolkit,
    create_full_toolkit,
)


# =============================================================================
# EXAMPLE: Custom billing hook for your platform
# =============================================================================

class PlatformBillingHook(BillingHook):
    """
    Example billing hook that integrates with your platform's credits system.

    Replace the placeholder calls with your actual database/API calls.
    """

    def __init__(self, db_session):
        """
        Args:
            db_session: Your database session (SQLAlchemy, etc.)
        """
        self.db = db_session

    async def check_balance(
        self,
        client_id: str,
        organization_id: str | None = None,
        required_cents: int = 0,
    ) -> bool:
        """Check if org/client has sufficient credits."""
        # Example: Query your credits table
        # org = await self.db.execute(
        #     select(Organization).where(Organization.id == organization_id)
        # )
        # return org.credits_cents >= required_cents

        # Placeholder
        print(f"Checking balance for {organization_id or client_id}: need {required_cents}¢")
        return True

    async def reserve_credits(
        self,
        client_id: str,
        request_id: str,
        amount_cents: int,
        organization_id: str | None = None,
    ) -> bool:
        """Reserve credits before expensive operations."""
        # Example: Create a credit reservation
        # await self.db.execute(
        #     insert(CreditReservation).values(
        #         request_id=request_id,
        #         organization_id=organization_id,
        #         amount_cents=amount_cents,
        #     )
        # )
        print(f"Reserving {amount_cents}¢ for request {request_id}")
        return True

    async def release_reservation(
        self,
        request_id: str,
        actual_cost_cents: int,
    ):
        """Release reservation and apply actual cost."""
        # Example: Delete reservation and deduct actual amount
        # reservation = await self.db.execute(
        #     select(CreditReservation).where(CreditReservation.request_id == request_id)
        # )
        # await self.db.execute(
        #     update(Organization)
        #     .where(Organization.id == reservation.organization_id)
        #     .values(credits_cents=Organization.credits_cents - actual_cost_cents)
        # )
        # await self.db.delete(reservation)
        print(f"Released reservation for {request_id}, actual cost: {actual_cost_cents}¢")

    async def record_usage(self, record: UsageRecord):
        """Record usage for billing/analytics."""
        # Example: Insert into usage_logs table
        # await self.db.execute(
        #     insert(UsageLog).values(
        #         request_id=record.request_id,
        #         organization_id=record.organization_id,
        #         agent_id=record.agent_id,
        #         tool_id=record.tool_id,
        #         cost_cents=record.cost_cents,
        #         tokens_input=record.tokens_input,
        #         tokens_output=record.tokens_output,
        #         timestamp=record.timestamp,
        #     )
        # )
        print(f"Usage recorded: {record.tool_name} - {record.cost_cents}¢ for agent {record.agent_id}")

    async def deduct_credits(
        self,
        client_id: str,
        amount_cents: int,
        request_id: str,
        organization_id: str | None = None,
    ) -> bool:
        """Deduct credits from organization."""
        # Example: Update organization credits
        # await self.db.execute(
        #     update(Organization)
        #     .where(Organization.id == organization_id)
        #     .values(credits_cents=Organization.credits_cents - amount_cents)
        # )
        print(f"Deducted {amount_cents}¢ from {organization_id or client_id}")
        return True


# =============================================================================
# EXAMPLE: Agent hook for tracking tool usage by agents
# =============================================================================

class PlatformAgentHook(AgentHook):
    """
    Example agent hook that integrates with your agent management system.

    This tracks which tools agents use and enforces access control.
    """

    def __init__(self, db_session):
        self.db = db_session

    async def on_tool_start(
        self,
        request: ToolRequest,
        context: AgentContext,
    ):
        """Called when agent starts using a tool."""
        # Example: Log to agent activity table
        # await self.db.execute(
        #     insert(AgentActivity).values(
        #         agent_id=context.agent_id,
        #         session_id=context.session_id,
        #         tool_id=request.tool_id,
        #         status="started",
        #         request_id=request.id,
        #     )
        # )
        print(f"Agent {context.agent_id} started using {request.tool_name}")

    async def on_tool_complete(
        self,
        request: ToolRequest,
        response: ToolResponse,
        context: AgentContext,
    ):
        """Called when agent finishes using a tool."""
        # Example: Update agent activity
        # await self.db.execute(
        #     update(AgentActivity)
        #     .where(AgentActivity.request_id == request.id)
        #     .values(
        #         status="completed" if response.status == "completed" else "failed",
        #         execution_time_ms=response.execution_time_ms,
        #     )
        # )
        print(
            f"Agent {context.agent_id} finished {request.tool_name} "
            f"({response.execution_time_ms:.0f}ms)"
        )

    async def get_allowed_tools(
        self,
        agent_id: str,
    ) -> list[str] | None:
        """Get list of tools the agent can use."""
        # Example: Query agent's allowed tools
        # agent = await self.db.execute(
        #     select(Agent).where(Agent.id == agent_id)
        # )
        # if agent.tool_access == "all":
        #     return None  # All tools allowed
        # return agent.allowed_tool_ids
        print(f"Checking tool access for agent {agent_id}")
        return None  # All tools allowed


# =============================================================================
# EXAMPLE: Full integration
# =============================================================================

async def demonstrate_integration():
    """Show how all the pieces fit together."""

    print("\n" + "=" * 60)
    print("Tool Service + Agent Management Integration Demo")
    print("=" * 60 + "\n")

    # 1. Set up billing manager with custom hooks
    billing_hook = PlatformBillingHook(db_session=None)  # Your DB session
    agent_hook = PlatformAgentHook(db_session=None)

    billing_manager = BillingManager(
        billing_hook=billing_hook,
        agent_hook=agent_hook,
    )

    # 2. Register event handlers for real-time updates
    def on_billing_event(event):
        print(f"Billing event: {event.event_type}")
        # Send to your event bus, websocket, etc.

    billing_manager.on_event(on_billing_event)

    # 3. Simulate an agent making a tool request
    print("\n--- Agent making tool request ---\n")

    request = ToolRequest(
        tool_name="search",
        tool_id="search-api:search",
        service_id="search-api",
        inputs={"query": "AI research papers 2024"},
        client_id="client-123",
        organization_id="org-456",
    )

    agent_context = AgentContext(
        agent_id="agent-789",
        agent_name="Research Assistant",
        organization_id="org-456",
        session_id="session-abc",
    )

    # Pre-execution checks
    print("\n1. Pre-execution checks:")
    can_proceed = await billing_manager.pre_execute(
        request=request,
        estimated_cost_cents=5,
        agent_context=agent_context,
    )

    if not can_proceed:
        print("Insufficient balance - request blocked")
        return

    # Simulate tool execution
    print("\n2. Executing tool...")

    response = ToolResponse(
        request_id=request.id,
        status="completed",
        result={"results": ["Paper 1", "Paper 2"]},
        execution_time_ms=1250.5,
        tokens_input=50,
        tokens_output=200,
    )
    request.cost_cents = 5

    # Post-execution billing
    print("\n3. Post-execution billing:")
    await billing_manager.post_execute(
        request=request,
        response=response,
        agent_context=agent_context,
    )

    print("\n" + "=" * 60)
    print("Integration complete!")
    print("=" * 60)


# =============================================================================
# EXAMPLE: Connecting to registry
# =============================================================================

async def demonstrate_registry_discovery():
    """Show how agents discover tools through the registry."""

    print("\n" + "=" * 60)
    print("Tool Discovery via Registry")
    print("=" * 60 + "\n")

    # Connect to registry
    registry = RegistryClient(
        registry_url="http://localhost:8000",
        api_key="your-api-key",
    )

    try:
        # Discover available tools
        print("Discovering tools...")
        tools = await registry.list_tools()

        print(f"\nFound {len(tools)} tools:")
        for tool in tools:
            print(f"  • {tool.name} ({tool.service_id})")
            print(f"    {tool.description[:60]}...")

        # Execute a tool through the registry
        print("\nExecuting tool through registry...")
        result = await registry.execute(
            tool_id="search-api:search",
            inputs={"query": "machine learning"},
        )

        print(f"Result: {result.status}")

    except Exception as e:
        print(f"Note: Registry not running ({e})")
        print("Start registry with: python deploy/registry_server.py")

    finally:
        await registry.close()


# =============================================================================
# EXAMPLE: Toolkits for agent personas
# =============================================================================

async def demonstrate_toolkits():
    """Show how toolkits provide contextualized tools for agent personas."""

    print("\n" + "=" * 60)
    print("Toolkits for Agent Personas")
    print("=" * 60 + "\n")

    # Create toolkit manager
    manager = ToolkitManager()

    # Register pre-built toolkits
    research_toolkit = create_research_toolkit()
    data_toolkit = create_data_toolkit()
    task_toolkit = create_task_toolkit()
    full_toolkit = create_full_toolkit()

    manager.register_toolkit(research_toolkit)
    manager.register_toolkit(data_toolkit)
    manager.register_toolkit(task_toolkit)
    manager.register_toolkit(full_toolkit)

    # Show what each agent persona sees
    print("Research Agent sees:")
    for name in research_toolkit.list_tools():
        binding = research_toolkit.get_binding(name)
        print(f"  • {name}: {binding.description_override or 'No description'}")

    print("\nData Agent sees:")
    for name in data_toolkit.list_tools():
        binding = data_toolkit.get_binding(name)
        print(f"  • {name}: {binding.description_override or 'No description'}")

    print("\nTask Agent sees:")
    for name in task_toolkit.list_tools():
        binding = task_toolkit.get_binding(name)
        print(f"  • {name}: {binding.description_override or 'No description'}")

    # Create a custom toolkit for a specific persona
    print("\n--- Creating Custom Toolkit ---\n")

    market_research_toolkit = Toolkit(
        name="Market Research Toolkit",
        description="Tools for competitive analysis and market research",
    )

    # Mix tools from both services with custom names
    market_research_toolkit.add_tool(
        agent_name="find_competitors",
        service_id="advanced-search-api",
        tool_name="deep_search",
        description_override="Find and analyze competitor companies",
    )
    market_research_toolkit.add_tool(
        agent_name="analyze_website",
        service_id="search-api",
        tool_name="read",
        description_override="Analyze a company's website content",
    )
    market_research_toolkit.add_tool(
        agent_name="extract_pricing",
        service_id="advanced-search-api",
        tool_name="extract_data",
        description_override="Extract pricing information from competitor pages",
    )
    market_research_toolkit.add_tool(
        agent_name="verify_claims",
        service_id="search-api",
        tool_name="verify",
        description_override="Verify competitor claims and statistics",
    )

    manager.register_toolkit(market_research_toolkit)

    print("Market Research Agent sees:")
    for name in market_research_toolkit.list_tools():
        binding = market_research_toolkit.get_binding(name)
        print(f"  • {name}: {binding.description_override}")

    # Show tool resolution
    print("\n--- Tool Resolution ---\n")
    print("When Market Research Agent calls 'find_competitors':")
    result = manager.resolve_tool(market_research_toolkit.id, "find_competitors")
    if result:
        service_id, tool_name = result
        print(f"  Routes to: {service_id} -> {tool_name}")
        print("  (Agent never sees 'advanced-search-api' or 'deep_search')")


# =============================================================================
# RUN DEMOS
# =============================================================================

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║     Agent Management + Tool Services Integration Demo        ║
║              Distributed Systems Platform                    ║
╚══════════════════════════════════════════════════════════════╝

This example shows how to integrate the tool services with
your agent management platform. The key integration points:

1. BillingHook - Connect to your credits/billing system
2. AgentHook - Track tool usage by agents
3. RegistryClient - Discover tools without seeing vendors
4. Toolkit - Contextualized, renamed tools for agent personas

""")

    asyncio.run(demonstrate_toolkits())
    asyncio.run(demonstrate_integration())
    asyncio.run(demonstrate_registry_discovery())
