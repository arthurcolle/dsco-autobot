"""
Agent Swarm Economy - Autonomous Agents with Crypto Capabilities.

A comprehensive system for running swarms of AI agents that can:
- Send and receive cryptocurrency
- Execute real-world projects autonomously
- Coordinate via economic incentives
- Generate value for the host/designer

Architecture:
                    ┌─────────────────┐
                    │  Agent Master   │
                    │ (Host/Designer) │
                    └────────┬────────┘
                             │ mints warrants
                             │ receives fees
                             ▼
              ┌──────────────────────────────┐
              │         Swarm Nexus          │
              │   (Coordination + Treasury)  │
              └──────────────┬───────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌────────────┐    ┌────────────┐    ┌────────────┐
    │  Agent A   │    │  Agent B   │    │  Agent C   │
    │  (Worker)  │◄──►│  (Worker)  │◄──►│  (Worker)  │
    │   Wallet   │    │   Wallet   │    │   Wallet   │
    └────────────┘    └────────────┘    └────────────┘

Components:

1. **Wallet System** (wallet.py, blockchain.py)
   - Multi-chain support (Solana, Ethereum, Base)
   - Budget policies with limits
   - Transaction intents and approvals
   - Real blockchain integration

2. **Economic Engine** (economics.py)
   - Budget constraints tied to warrants
   - Payment channels for inter-agent payments
   - Revenue sharing with host
   - Staking pools

3. **Swarm Coordination** (swarm.py)
   - Task queue with priority routing
   - Capability-based agent matching
   - Reputation system
   - Slashing for bad behavior

4. **Communication** (communication.py)
   - Direct messaging
   - Negotiation protocols (Contract Net)
   - Coalition formation
   - Consensus mechanisms

5. **Project Management** (projects.py)
   - Work Breakdown Structure
   - Task decomposition
   - Milestone tracking
   - Risk management

6. **Monitoring** (monitoring.py)
   - Metrics collection
   - Alert management
   - Audit logging
   - Health checking

7. **Orchestration** (orchestrator.py)
   - High-level project execution
   - Warrant minting for agents
   - Earnings collection

Based on Research:
- MIT Media Lab: Authenticated Delegation for AI Agents (arXiv:2501.09674)
- Autonomous Agents on Blockchains (arXiv:2601.04583)
- Blockchain-enhanced incentive mechanisms for MARL (Nature)
- Swarm Intelligence in Agentic AI (Industry Report)
"""

from .wallet import (
    AgentWallet,
    WalletType,
    Chain,
    BudgetPolicy,
    TransactionIntent,
    TransactionResult,
    get_wallet_registry,
)
from .swarm import (
    SwarmNexus,
    SwarmAgent,
    SwarmTask,
    AgentRole,
    AgentStatus,
    TaskStatus,
    TaskPriority,
    create_swarm,
    get_swarm,
    list_swarms,
)
from .economics import (
    BudgetConstraint,
    PaymentChannel,
    RevenueShare,
    RevenueModel,
    StakingPool,
    EconomicsEngine,
)
from .orchestrator import (
    SwarmOrchestrator,
    SwarmConfig,
    ProjectSpec,
    create_orchestrator,
    quick_swarm,
)
from .communication import (
    Message,
    MessageType,
    Proposal,
    Negotiation,
    Coalition,
    MessageBus,
    NegotiationEngine,
    get_message_bus,
    get_negotiation_engine,
)
from .projects import (
    Project,
    ProjectStatus,
    Milestone,
    Risk,
    WorkBreakdownItem,
    TaskDependency,
    TaskDecomposer,
    ProjectManager,
    get_project_manager,
)
from .monitoring import (
    MetricsCollector,
    AlertManager,
    AuditLogger,
    HealthChecker,
    SwarmMonitor,
    Alert,
    AlertSeverity,
    AuditEntry,
    AuditAction,
    get_monitor,
)
from .persistence import (
    SwarmDB,
    get_swarm_db,
)

__all__ = [
    # Wallet
    "AgentWallet",
    "WalletType",
    "Chain",
    "BudgetPolicy",
    "TransactionIntent",
    "TransactionResult",
    "get_wallet_registry",

    # Swarm
    "SwarmNexus",
    "SwarmAgent",
    "SwarmTask",
    "AgentRole",
    "AgentStatus",
    "TaskStatus",
    "TaskPriority",
    "create_swarm",
    "get_swarm",
    "list_swarms",

    # Economics
    "BudgetConstraint",
    "PaymentChannel",
    "RevenueShare",
    "RevenueModel",
    "StakingPool",
    "EconomicsEngine",

    # Orchestrator
    "SwarmOrchestrator",
    "SwarmConfig",
    "ProjectSpec",
    "create_orchestrator",
    "quick_swarm",

    # Communication
    "Message",
    "MessageType",
    "Proposal",
    "Negotiation",
    "Coalition",
    "MessageBus",
    "NegotiationEngine",
    "get_message_bus",
    "get_negotiation_engine",

    # Projects
    "Project",
    "ProjectStatus",
    "Milestone",
    "Risk",
    "WorkBreakdownItem",
    "TaskDependency",
    "TaskDecomposer",
    "ProjectManager",
    "get_project_manager",

    # Monitoring
    "MetricsCollector",
    "AlertManager",
    "AuditLogger",
    "HealthChecker",
    "SwarmMonitor",
    "Alert",
    "AlertSeverity",
    "AuditEntry",
    "AuditAction",
    "get_monitor",
    # Persistence
    "SwarmDB",
    "get_swarm_db",
]
