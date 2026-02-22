"""
DuckDB Persistence for Swarm Economy.

Stores all swarm data (swarms, agents, tasks, wallets, transactions)
in DuckDB with file-based persistence for Modal volumes.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# DuckDB import
try:
    import duckdb
except ImportError:
    duckdb = None
    logger.warning("DuckDB not installed, persistence disabled")


def get_db_path() -> str:
    """Get the database path from environment or default."""
    data_dir = os.getenv("DATA_DIR", "data")
    return str(Path(data_dir) / "swarm.duckdb")


class SwarmDB:
    """DuckDB-backed persistence for swarm data."""

    _instance: Optional["SwarmDB"] = None

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self._conn = None
        self._ensure_tables()

    @classmethod
    def get_instance(cls, db_path: Optional[str] = None) -> "SwarmDB":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls(db_path)
        return cls._instance

    @contextmanager
    def connection(self):
        """Get a database connection."""
        if duckdb is None:
            raise RuntimeError("DuckDB not installed")

        conn = duckdb.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        if duckdb is None:
            return

        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        with self.connection() as conn:
            # Swarms table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS swarms (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    host_id VARCHAR NOT NULL,
                    host_wallet VARCHAR NOT NULL,
                    host_fee_rate DOUBLE DEFAULT 0.05,
                    min_stake DOUBLE DEFAULT 10.0,
                    total_tasks_completed INTEGER DEFAULT 0,
                    total_payments_processed DOUBLE DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    config JSON
                )
            """)

            # Agents table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    name VARCHAR NOT NULL,
                    role VARCHAR DEFAULT 'worker',
                    capabilities JSON,
                    status VARCHAR DEFAULT 'idle',
                    current_task_id VARCHAR,
                    wallet_id VARCHAR,
                    reputation DOUBLE DEFAULT 0.5,
                    tasks_completed INTEGER DEFAULT 0,
                    tasks_failed INTEGER DEFAULT 0,
                    stake_amount DOUBLE DEFAULT 0.0,
                    quality_score DOUBLE DEFAULT 0.5,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata JSON,
                    mcp_profile JSON DEFAULT '{}',
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id)
                )
            """)

            # Tasks table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    description TEXT,
                    input_data JSON,
                    output_data JSON,
                    required_capabilities JSON,
                    bounty DOUBLE DEFAULT 10.0,
                    status VARCHAR DEFAULT 'pending',
                    priority VARCHAR DEFAULT 'normal',
                    assigned_to VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    deadline TIMESTAMP,
                    attempt_count INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 3,
                    error TEXT,
                    validated BOOLEAN DEFAULT FALSE,
                    validator_id VARCHAR,
                    validation_score DOUBLE,
                    parent_task_id VARCHAR,
                    mcp_backend VARCHAR,
                    mcp_tool VARCHAR,
                    workflow_id VARCHAR,
                    skill_pattern_id VARCHAR,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id),
                    FOREIGN KEY (assigned_to) REFERENCES agents(id)
                )
            """)

            # Wallets table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS wallets (
                    id VARCHAR PRIMARY KEY,
                    agent_id VARCHAR NOT NULL,
                    wallet_type VARCHAR DEFAULT 'warm',
                    master_pubkey VARCHAR,
                    addresses JSON,
                    balances JSON,
                    budget_policy JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Transactions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    from_agent VARCHAR,
                    to_agent VARCHAR,
                    amount DOUBLE NOT NULL,
                    fee DOUBLE DEFAULT 0.0,
                    purpose TEXT,
                    tx_hash VARCHAR,
                    chain VARCHAR,
                    status VARCHAR DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    metadata JSON,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id)
                )
            """)

            # Staking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stakes (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    agent_id VARCHAR NOT NULL,
                    amount DOUBLE NOT NULL,
                    staked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    unlocks_at TIMESTAMP,
                    status VARCHAR DEFAULT 'active',
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id),
                    FOREIGN KEY (agent_id) REFERENCES agents(id)
                )
            """)

            # Revenue/earnings table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS revenue (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    amount DOUBLE NOT NULL,
                    source VARCHAR,
                    transaction_id VARCHAR,
                    collected BOOLEAN DEFAULT FALSE,
                    collected_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id)
                )
            """)

            # Workflows table (MCP multi-step execution)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workflows (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    workflow_type VARCHAR DEFAULT 'sequential',
                    steps JSON,
                    results JSON,
                    status VARCHAR DEFAULT 'pending',
                    steps_completed INTEGER DEFAULT 0,
                    total_steps INTEGER DEFAULT 0,
                    agent_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error TEXT,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id),
                    FOREIGN KEY (agent_id) REFERENCES agents(id)
                )
            """)

            # Learned skills table (patterns from Memory MCP)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS skills (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    skill_type VARCHAR NOT NULL,
                    input_pattern JSON,
                    output_pattern JSON,
                    score DOUBLE DEFAULT 0.0,
                    created_by VARCHAR,
                    times_applied INTEGER DEFAULT 0,
                    success_rate DOUBLE DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id),
                    FOREIGN KEY (created_by) REFERENCES agents(id)
                )
            """)

            # Event subscriptions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS event_subscriptions (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    subscriber_id VARCHAR,
                    event_types JSON,
                    webhook_url VARCHAR,
                    filter_criteria JSON,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (swarm_id) REFERENCES swarms(id)
                )
            """)

            # =========================================================
            # MCP (Master Control Program) Tables
            # =========================================================

            # MCP Agent Profiles - Security clearances and permissions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_agent_profiles (
                    agent_id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    clearance INTEGER DEFAULT 0,
                    permissions JSON DEFAULT '[]',
                    restrictions JSON DEFAULT '[]',
                    supervisor_id VARCHAR,
                    trust_score DOUBLE DEFAULT 0.5,
                    violation_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # MCP Violations - Policy violation records
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_violations (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    agent_id VARCHAR NOT NULL,
                    action VARCHAR NOT NULL,
                    target VARCHAR,
                    reason TEXT NOT NULL,
                    severity INTEGER DEFAULT 1,
                    remediation TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # MCP Authorization Requests
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_auth_requests (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    agent_id VARCHAR NOT NULL,
                    action VARCHAR NOT NULL,
                    target VARCHAR,
                    justification TEXT,
                    context JSON,
                    status VARCHAR DEFAULT 'pending',
                    authorized BOOLEAN,
                    authorizer VARCHAR,
                    reason TEXT,
                    conditions JSON,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP
                )
            """)

            # MCP Audit Log - All MCP events
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_audit_log (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    event_type VARCHAR NOT NULL,
                    agent_id VARCHAR,
                    data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # =========================================================
            # Self-Improvement Tables
            # =========================================================

            # Code Changes - Proposed and implemented code modifications
            conn.execute("""
                CREATE TABLE IF NOT EXISTS code_changes (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    proposer_id VARCHAR NOT NULL,
                    file_path VARCHAR NOT NULL,
                    change_type VARCHAR NOT NULL,
                    description TEXT,
                    original_content TEXT,
                    proposed_content TEXT,
                    diff TEXT,
                    justification TEXT,
                    status VARCHAR DEFAULT 'proposed',
                    reviewers JSON DEFAULT '[]',
                    approvals JSON DEFAULT '[]',
                    rejections JSON DEFAULT '[]',
                    review_comments JSON DEFAULT '[]',
                    checksum_before VARCHAR,
                    checksum_after VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    implemented_at TIMESTAMP
                )
            """)

            # Improvement Proposals - High-level improvement plans
            conn.execute("""
                CREATE TABLE IF NOT EXISTS improvement_proposals (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR NOT NULL,
                    proposer_id VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description TEXT,
                    rationale TEXT,
                    change_ids JSON DEFAULT '[]',
                    status VARCHAR DEFAULT 'proposed',
                    priority INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # LLM API Usage Tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_usage (
                    id VARCHAR PRIMARY KEY,
                    swarm_id VARCHAR,
                    agent_id VARCHAR,
                    model VARCHAR NOT NULL,
                    prompt_tokens INTEGER DEFAULT 0,
                    completion_tokens INTEGER DEFAULT 0,
                    total_tokens INTEGER DEFAULT 0,
                    cost_usd DOUBLE DEFAULT 0.0,
                    task_type VARCHAR,
                    success BOOLEAN DEFAULT TRUE,
                    latency_ms INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_swarm ON agents(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_swarm ON tasks(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_swarm ON transactions(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workflows_swarm ON workflows(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_skills_swarm ON skills(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_swarm ON event_subscriptions(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_profiles_swarm ON mcp_agent_profiles(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_violations_swarm ON mcp_violations(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_violations_agent ON mcp_violations(agent_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_audit_swarm ON mcp_audit_log(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_code_changes_swarm ON code_changes(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_usage_swarm ON llm_usage(swarm_id)")

            logger.info(f"SwarmDB initialized at {self.db_path}")

    # =========================================================================
    # SWARM OPERATIONS
    # =========================================================================

    def create_swarm(
        self,
        swarm_id: str,
        name: str,
        host_id: str,
        host_wallet: str,
        host_fee_rate: float = 0.05,
        min_stake: float = 10.0,
        config: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Create a new swarm."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO swarms (id, name, host_id, host_wallet, host_fee_rate, min_stake, created_at, updated_at, config)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [swarm_id, name, host_id, host_wallet, host_fee_rate, min_stake, now, now, json.dumps(config or {})])

        return self.get_swarm(swarm_id)

    def get_swarm(self, swarm_id: str) -> Optional[Dict[str, Any]]:
        """Get a swarm by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM swarms WHERE id = ?", [swarm_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            swarm = dict(zip(columns, result))

            # Parse JSON fields
            if swarm.get("config"):
                swarm["config"] = json.loads(swarm["config"]) if isinstance(swarm["config"], str) else swarm["config"]

            return swarm

    def list_swarms(self) -> List[Dict[str, Any]]:
        """List all swarms."""
        with self.connection() as conn:
            results = conn.execute("SELECT * FROM swarms ORDER BY created_at DESC").fetchall()
            columns = [desc[0] for desc in conn.description]

            swarms = []
            for row in results:
                swarm = dict(zip(columns, row))
                if swarm.get("config"):
                    swarm["config"] = json.loads(swarm["config"]) if isinstance(swarm["config"], str) else swarm["config"]
                swarms.append(swarm)

            return swarms

    def update_swarm(self, swarm_id: str, **updates) -> Optional[Dict[str, Any]]:
        """Update a swarm."""
        if not updates:
            return self.get_swarm(swarm_id)

        with self.connection() as conn:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [datetime.now(timezone.utc), swarm_id]

            conn.execute(f"""
                UPDATE swarms SET {set_clause}, updated_at = ? WHERE id = ?
            """, values)

        return self.get_swarm(swarm_id)

    def delete_swarm(self, swarm_id: str) -> bool:
        """Delete a swarm and all related data."""
        with self.connection() as conn:
            # Delete in order due to foreign keys
            conn.execute("DELETE FROM revenue WHERE swarm_id = ?", [swarm_id])
            conn.execute("DELETE FROM stakes WHERE swarm_id = ?", [swarm_id])
            conn.execute("DELETE FROM transactions WHERE swarm_id = ?", [swarm_id])
            conn.execute("DELETE FROM tasks WHERE swarm_id = ?", [swarm_id])
            conn.execute("DELETE FROM agents WHERE swarm_id = ?", [swarm_id])
            conn.execute("DELETE FROM swarms WHERE id = ?", [swarm_id])
            return True

    # =========================================================================
    # AGENT OPERATIONS
    # =========================================================================

    def create_agent(
        self,
        agent_id: str,
        swarm_id: str,
        name: str,
        role: str = "worker",
        capabilities: Optional[List[str]] = None,
        wallet_id: Optional[str] = None,
        stake_amount: float = 0.0,
        metadata: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Create a new agent."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO agents (id, swarm_id, name, role, capabilities, wallet_id, stake_amount, joined_at, last_active, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                agent_id, swarm_id, name, role,
                json.dumps(capabilities or []),
                wallet_id, stake_amount, now, now,
                json.dumps(metadata or {})
            ])

        return self.get_agent(agent_id)

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get an agent by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM agents WHERE id = ?", [agent_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            agent = dict(zip(columns, result))

            # Parse JSON fields
            for field in ["capabilities", "metadata"]:
                if agent.get(field):
                    agent[field] = json.loads(agent[field]) if isinstance(agent[field], str) else agent[field]

            return agent

    def list_agents(self, swarm_id: str) -> List[Dict[str, Any]]:
        """List agents in a swarm."""
        with self.connection() as conn:
            results = conn.execute(
                "SELECT * FROM agents WHERE swarm_id = ? ORDER BY joined_at DESC",
                [swarm_id]
            ).fetchall()

            columns = [desc[0] for desc in conn.description]
            agents = []
            for row in results:
                agent = dict(zip(columns, row))
                for field in ["capabilities", "metadata"]:
                    if agent.get(field):
                        agent[field] = json.loads(agent[field]) if isinstance(agent[field], str) else agent[field]
                agents.append(agent)

            return agents

    def update_agent(self, agent_id: str, **updates) -> Optional[Dict[str, Any]]:
        """Update an agent."""
        if not updates:
            return self.get_agent(agent_id)

        # Serialize JSON fields
        for field in ["capabilities", "metadata"]:
            if field in updates and not isinstance(updates[field], str):
                updates[field] = json.dumps(updates[field])

        with self.connection() as conn:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [datetime.now(timezone.utc), agent_id]

            conn.execute(f"""
                UPDATE agents SET {set_clause}, last_active = ? WHERE id = ?
            """, values)

        return self.get_agent(agent_id)

    def get_idle_agents(self, swarm_id: str) -> List[Dict[str, Any]]:
        """Get idle agents in a swarm."""
        with self.connection() as conn:
            results = conn.execute(
                "SELECT * FROM agents WHERE swarm_id = ? AND status = 'idle'",
                [swarm_id]
            ).fetchall()

            columns = [desc[0] for desc in conn.description]
            agents = []
            for row in results:
                agent = dict(zip(columns, row))
                for field in ["capabilities", "metadata"]:
                    if agent.get(field):
                        agent[field] = json.loads(agent[field]) if isinstance(agent[field], str) else agent[field]
                agents.append(agent)

            return agents

    # =========================================================================
    # TASK OPERATIONS
    # =========================================================================

    def create_task(
        self,
        task_id: str,
        swarm_id: str,
        task_type: str,
        description: str,
        bounty: float = 10.0,
        priority: str = "normal",
        input_data: Optional[Dict] = None,
        required_capabilities: Optional[List[str]] = None,
        deadline: Optional[datetime] = None,
        parent_task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new task."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO tasks (id, swarm_id, type, description, bounty, priority, input_data, required_capabilities, deadline, parent_task_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                task_id, swarm_id, task_type, description, bounty, priority,
                json.dumps(input_data or {}),
                json.dumps(required_capabilities or []),
                deadline, parent_task_id, now
            ])

        return self.get_task(task_id)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get a task by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", [task_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            task = dict(zip(columns, result))

            # Parse JSON fields
            for field in ["input_data", "output_data", "required_capabilities"]:
                if task.get(field):
                    task[field] = json.loads(task[field]) if isinstance(task[field], str) else task[field]

            return task

    def list_tasks(self, swarm_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List tasks in a swarm."""
        with self.connection() as conn:
            if status:
                results = conn.execute(
                    "SELECT * FROM tasks WHERE swarm_id = ? AND status = ? ORDER BY created_at DESC",
                    [swarm_id, status]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM tasks WHERE swarm_id = ? ORDER BY created_at DESC",
                    [swarm_id]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            tasks = []
            for row in results:
                task = dict(zip(columns, row))
                for field in ["input_data", "output_data", "required_capabilities"]:
                    if task.get(field):
                        task[field] = json.loads(task[field]) if isinstance(task[field], str) else task[field]
                tasks.append(task)

            return tasks

    def get_pending_tasks(self, swarm_id: str) -> List[Dict[str, Any]]:
        """Get pending tasks ordered by priority."""
        priority_order = "CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 WHEN 'low' THEN 4 ELSE 5 END"

        with self.connection() as conn:
            results = conn.execute(f"""
                SELECT * FROM tasks
                WHERE swarm_id = ? AND status = 'pending'
                ORDER BY {priority_order}, created_at ASC
            """, [swarm_id]).fetchall()

            columns = [desc[0] for desc in conn.description]
            tasks = []
            for row in results:
                task = dict(zip(columns, row))
                for field in ["input_data", "output_data", "required_capabilities"]:
                    if task.get(field):
                        task[field] = json.loads(task[field]) if isinstance(task[field], str) else task[field]
                tasks.append(task)

            return tasks

    def update_task(self, task_id: str, **updates) -> Optional[Dict[str, Any]]:
        """Update a task."""
        if not updates:
            return self.get_task(task_id)

        # Serialize JSON fields
        for field in ["input_data", "output_data", "required_capabilities"]:
            if field in updates and not isinstance(updates[field], str):
                updates[field] = json.dumps(updates[field])

        with self.connection() as conn:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [task_id]

            conn.execute(f"""
                UPDATE tasks SET {set_clause} WHERE id = ?
            """, values)

        return self.get_task(task_id)

    def assign_task(self, task_id: str, agent_id: str) -> bool:
        """Assign a task to an agent."""
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            conn.execute("""
                UPDATE tasks SET assigned_to = ?, status = 'claimed', started_at = ?, attempt_count = attempt_count + 1
                WHERE id = ? AND status = 'pending'
            """, [agent_id, now, task_id])

            conn.execute("""
                UPDATE agents SET status = 'busy', current_task_id = ?, last_active = ?
                WHERE id = ?
            """, [task_id, now, agent_id])

            return True

    def complete_task(self, task_id: str, output_data: Dict, success: bool = True) -> bool:
        """Complete a task."""
        now = datetime.now(timezone.utc)
        task = self.get_task(task_id)
        if not task:
            return False

        with self.connection() as conn:
            if success:
                conn.execute("""
                    UPDATE tasks SET status = 'completed', output_data = ?, completed_at = ?
                    WHERE id = ?
                """, [json.dumps(output_data), now, task_id])
            else:
                error = output_data.get("error", "Unknown error")
                conn.execute("""
                    UPDATE tasks SET status = 'failed', error = ?, completed_at = ?
                    WHERE id = ?
                """, [error, now, task_id])

            # Update agent
            if task.get("assigned_to"):
                if success:
                    conn.execute("""
                        UPDATE agents SET
                            status = 'idle',
                            current_task_id = NULL,
                            tasks_completed = tasks_completed + 1,
                            reputation = LEAST(1.0, reputation + 0.01),
                            last_active = ?
                        WHERE id = ?
                    """, [now, task["assigned_to"]])
                else:
                    conn.execute("""
                        UPDATE agents SET
                            status = 'idle',
                            current_task_id = NULL,
                            tasks_failed = tasks_failed + 1,
                            reputation = GREATEST(0.0, reputation - 0.02),
                            last_active = ?
                        WHERE id = ?
                    """, [now, task["assigned_to"]])

            return True

    # =========================================================================
    # TRANSACTION OPERATIONS
    # =========================================================================

    def create_transaction(
        self,
        tx_id: str,
        swarm_id: str,
        from_agent: Optional[str],
        to_agent: Optional[str],
        amount: float,
        fee: float = 0.0,
        purpose: str = "",
        chain: str = "internal",
        metadata: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Record a transaction."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO transactions (id, swarm_id, from_agent, to_agent, amount, fee, purpose, chain, status, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)
            """, [tx_id, swarm_id, from_agent, to_agent, amount, fee, purpose, chain, now, json.dumps(metadata or {})])

            # Update swarm totals
            conn.execute("""
                UPDATE swarms SET total_payments_processed = total_payments_processed + ? WHERE id = ?
            """, [amount, swarm_id])

            # Record host revenue (fee)
            if fee > 0:
                import uuid
                conn.execute("""
                    INSERT INTO revenue (id, swarm_id, amount, source, transaction_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [str(uuid.uuid4()), swarm_id, fee, purpose, tx_id, now])

        return self.get_transaction(tx_id)

    def get_transaction(self, tx_id: str) -> Optional[Dict[str, Any]]:
        """Get a transaction by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM transactions WHERE id = ?", [tx_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            tx = dict(zip(columns, result))
            if tx.get("metadata"):
                tx["metadata"] = json.loads(tx["metadata"]) if isinstance(tx["metadata"], str) else tx["metadata"]
            return tx

    def list_transactions(self, swarm_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """List transactions for a swarm."""
        with self.connection() as conn:
            results = conn.execute(
                "SELECT * FROM transactions WHERE swarm_id = ? ORDER BY created_at DESC LIMIT ?",
                [swarm_id, limit]
            ).fetchall()

            columns = [desc[0] for desc in conn.description]
            txs = []
            for row in results:
                tx = dict(zip(columns, row))
                if tx.get("metadata"):
                    tx["metadata"] = json.loads(tx["metadata"]) if isinstance(tx["metadata"], str) else tx["metadata"]
                txs.append(tx)

            return txs

    # =========================================================================
    # STAKING OPERATIONS
    # =========================================================================

    def create_stake(
        self,
        stake_id: str,
        swarm_id: str,
        agent_id: str,
        amount: float,
        lock_days: int = 7,
    ) -> Dict[str, Any]:
        """Create a stake."""
        from datetime import timedelta
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            unlocks_at = now + timedelta(days=lock_days)

            conn.execute("""
                INSERT INTO stakes (id, swarm_id, agent_id, amount, staked_at, unlocks_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'active')
            """, [stake_id, swarm_id, agent_id, amount, now, unlocks_at])

            # Update agent stake
            conn.execute("""
                UPDATE agents SET stake_amount = stake_amount + ? WHERE id = ?
            """, [amount, agent_id])

        return self.get_stake(stake_id)

    def get_stake(self, stake_id: str) -> Optional[Dict[str, Any]]:
        """Get a stake by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM stakes WHERE id = ?", [stake_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))

    def get_total_staked(self, swarm_id: str) -> float:
        """Get total staked amount for a swarm."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM stakes WHERE swarm_id = ? AND status = 'active'",
                [swarm_id]
            ).fetchone()
            return result[0] if result else 0.0

    # =========================================================================
    # REVENUE OPERATIONS
    # =========================================================================

    def get_host_earnings(self, swarm_id: str) -> Dict[str, Any]:
        """Get host earnings summary."""
        with self.connection() as conn:
            # Total earned
            total = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE swarm_id = ?",
                [swarm_id]
            ).fetchone()[0]

            # Pending (not collected)
            pending = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE swarm_id = ? AND collected = FALSE",
                [swarm_id]
            ).fetchone()[0]

            # Transaction count
            count = conn.execute(
                "SELECT COUNT(*) FROM revenue WHERE swarm_id = ?",
                [swarm_id]
            ).fetchone()[0]

            # Last payout
            last = conn.execute(
                "SELECT MAX(collected_at) FROM revenue WHERE swarm_id = ? AND collected = TRUE",
                [swarm_id]
            ).fetchone()[0]

            return {
                "total_earned": total,
                "pending_payout": pending,
                "transaction_count": count,
                "last_payout": last.isoformat() if last else None,
            }

    def collect_earnings(self, swarm_id: str) -> float:
        """Collect pending earnings."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)

            # Get pending amount
            pending = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE swarm_id = ? AND collected = FALSE",
                [swarm_id]
            ).fetchone()[0]

            # Mark as collected
            conn.execute("""
                UPDATE revenue SET collected = TRUE, collected_at = ?
                WHERE swarm_id = ? AND collected = FALSE
            """, [now, swarm_id])

            return pending

    # =========================================================================
    # MCP PROFILE OPERATIONS
    # =========================================================================

    def update_agent_mcp_profile(self, agent_id: str, mcp_profile: Dict) -> Optional[Dict[str, Any]]:
        """Update an agent's MCP profile."""
        with self.connection() as conn:
            conn.execute("""
                UPDATE agents SET mcp_profile = ?, last_active = ?
                WHERE id = ?
            """, [json.dumps(mcp_profile), datetime.now(timezone.utc), agent_id])

        return self.get_agent(agent_id)

    def get_agent_mcp_profile(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get an agent's MCP profile."""
        agent = self.get_agent(agent_id)
        if not agent:
            return None

        profile = agent.get("mcp_profile", {})
        if isinstance(profile, str):
            profile = json.loads(profile)
        return profile

    # =========================================================================
    # WORKFLOW OPERATIONS
    # =========================================================================

    def create_workflow(
        self,
        workflow_id: str,
        swarm_id: str,
        steps: List[Dict],
        workflow_type: str = "sequential",
        agent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new workflow."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO workflows (id, swarm_id, workflow_type, steps, total_steps, agent_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [workflow_id, swarm_id, workflow_type, json.dumps(steps), len(steps), agent_id, now])

        return self.get_workflow(workflow_id)

    def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get a workflow by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM workflows WHERE id = ?", [workflow_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            workflow = dict(zip(columns, result))

            for field in ["steps", "results"]:
                if workflow.get(field):
                    workflow[field] = json.loads(workflow[field]) if isinstance(workflow[field], str) else workflow[field]

            return workflow

    def update_workflow(self, workflow_id: str, **updates) -> Optional[Dict[str, Any]]:
        """Update a workflow."""
        if not updates:
            return self.get_workflow(workflow_id)

        for field in ["steps", "results"]:
            if field in updates and not isinstance(updates[field], str):
                updates[field] = json.dumps(updates[field])

        with self.connection() as conn:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [workflow_id]

            conn.execute(f"""
                UPDATE workflows SET {set_clause} WHERE id = ?
            """, values)

        return self.get_workflow(workflow_id)

    def list_workflows(self, swarm_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List workflows in a swarm."""
        with self.connection() as conn:
            if status:
                results = conn.execute(
                    "SELECT * FROM workflows WHERE swarm_id = ? AND status = ? ORDER BY created_at DESC",
                    [swarm_id, status]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM workflows WHERE swarm_id = ? ORDER BY created_at DESC",
                    [swarm_id]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            workflows = []
            for row in results:
                workflow = dict(zip(columns, row))
                for field in ["steps", "results"]:
                    if workflow.get(field):
                        workflow[field] = json.loads(workflow[field]) if isinstance(workflow[field], str) else workflow[field]
                workflows.append(workflow)

            return workflows

    # =========================================================================
    # SKILL OPERATIONS
    # =========================================================================

    def create_skill(
        self,
        skill_id: str,
        swarm_id: str,
        skill_type: str,
        input_pattern: Dict,
        output_pattern: Dict,
        score: float = 0.0,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new learned skill."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO skills (id, swarm_id, skill_type, input_pattern, output_pattern, score, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [skill_id, swarm_id, skill_type, json.dumps(input_pattern), json.dumps(output_pattern), score, created_by, now, now])

        return self.get_skill(skill_id)

    def get_skill(self, skill_id: str) -> Optional[Dict[str, Any]]:
        """Get a skill by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM skills WHERE id = ?", [skill_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            skill = dict(zip(columns, result))

            for field in ["input_pattern", "output_pattern"]:
                if skill.get(field):
                    skill[field] = json.loads(skill[field]) if isinstance(skill[field], str) else skill[field]

            return skill

    def list_skills(self, swarm_id: str, skill_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """List skills in a swarm."""
        with self.connection() as conn:
            if skill_type:
                results = conn.execute(
                    "SELECT * FROM skills WHERE swarm_id = ? AND skill_type = ? ORDER BY score DESC",
                    [swarm_id, skill_type]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM skills WHERE swarm_id = ? ORDER BY score DESC",
                    [swarm_id]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            skills = []
            for row in results:
                skill = dict(zip(columns, row))
                for field in ["input_pattern", "output_pattern"]:
                    if skill.get(field):
                        skill[field] = json.loads(skill[field]) if isinstance(skill[field], str) else skill[field]
                skills.append(skill)

            return skills

    def apply_skill(self, skill_id: str, success: bool = True) -> bool:
        """Record skill application."""
        skill = self.get_skill(skill_id)
        if not skill:
            return False

        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            times_applied = skill.get("times_applied", 0) + 1
            # Update success rate with exponential moving average
            old_rate = skill.get("success_rate", 0.5)
            new_rate = old_rate * 0.9 + (1.0 if success else 0.0) * 0.1

            conn.execute("""
                UPDATE skills SET times_applied = ?, success_rate = ?, updated_at = ?
                WHERE id = ?
            """, [times_applied, new_rate, now, skill_id])

            return True

    # =========================================================================
    # EVENT SUBSCRIPTION OPERATIONS
    # =========================================================================

    def create_subscription(
        self,
        subscription_id: str,
        swarm_id: str,
        event_types: List[str],
        webhook_url: Optional[str] = None,
        subscriber_id: Optional[str] = None,
        filter_criteria: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Create an event subscription."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO event_subscriptions (id, swarm_id, subscriber_id, event_types, webhook_url, filter_criteria, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [subscription_id, swarm_id, subscriber_id, json.dumps(event_types), webhook_url, json.dumps(filter_criteria or {}), now])

        return self.get_subscription(subscription_id)

    def get_subscription(self, subscription_id: str) -> Optional[Dict[str, Any]]:
        """Get a subscription by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM event_subscriptions WHERE id = ?", [subscription_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            sub = dict(zip(columns, result))

            for field in ["event_types", "filter_criteria"]:
                if sub.get(field):
                    sub[field] = json.loads(sub[field]) if isinstance(sub[field], str) else sub[field]

            return sub

    def list_subscriptions(self, swarm_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """List subscriptions for a swarm."""
        with self.connection() as conn:
            if active_only:
                results = conn.execute(
                    "SELECT * FROM event_subscriptions WHERE swarm_id = ? AND active = TRUE",
                    [swarm_id]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM event_subscriptions WHERE swarm_id = ?",
                    [swarm_id]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            subs = []
            for row in results:
                sub = dict(zip(columns, row))
                for field in ["event_types", "filter_criteria"]:
                    if sub.get(field):
                        sub[field] = json.loads(sub[field]) if isinstance(sub[field], str) else sub[field]
                subs.append(sub)

            return subs

    def delete_subscription(self, subscription_id: str) -> bool:
        """Delete a subscription."""
        with self.connection() as conn:
            conn.execute("DELETE FROM event_subscriptions WHERE id = ?", [subscription_id])
            return True

    # =========================================================================
    # STATS
    # =========================================================================

    def get_swarm_stats(self, swarm_id: str) -> Dict[str, Any]:
        """Get comprehensive swarm statistics."""
        swarm = self.get_swarm(swarm_id)
        if not swarm:
            return {}

        with self.connection() as conn:
            # Agent counts
            agent_count = conn.execute(
                "SELECT COUNT(*) FROM agents WHERE swarm_id = ?", [swarm_id]
            ).fetchone()[0]

            idle_count = conn.execute(
                "SELECT COUNT(*) FROM agents WHERE swarm_id = ? AND status = 'idle'", [swarm_id]
            ).fetchone()[0]

            # Task counts
            pending_count = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE swarm_id = ? AND status = 'pending'", [swarm_id]
            ).fetchone()[0]

            completed_count = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE swarm_id = ? AND status = 'completed'", [swarm_id]
            ).fetchone()[0]

            # Totals
            total_staked = self.get_total_staked(swarm_id)
            earnings = self.get_host_earnings(swarm_id)

        return {
            "id": swarm_id,
            "name": swarm["name"],
            "host_id": swarm["host_id"],
            "agent_count": agent_count,
            "idle_agents": idle_count,
            "pending_tasks": pending_count,
            "total_tasks_completed": completed_count,
            "total_payments_processed": swarm["total_payments_processed"],
            "total_staked": total_staked,
            "host_earnings": earnings,
            "created_at": swarm["created_at"].isoformat() if swarm["created_at"] else None,
        }


    # =========================================================================
    # MCP AGENT PROFILE OPERATIONS
    # =========================================================================

    def upsert_mcp_profile(
        self,
        agent_id: str,
        swarm_id: str,
        clearance: int = 0,
        permissions: Optional[List[str]] = None,
        restrictions: Optional[List[str]] = None,
        supervisor_id: Optional[str] = None,
        trust_score: float = 0.5,
    ) -> Dict[str, Any]:
        """Create or update an MCP agent profile."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            # Try update first
            result = conn.execute(
                "SELECT agent_id FROM mcp_agent_profiles WHERE agent_id = ?", [agent_id]
            ).fetchone()

            if result:
                conn.execute("""
                    UPDATE mcp_agent_profiles SET
                        clearance = ?, permissions = ?, restrictions = ?,
                        supervisor_id = ?, trust_score = ?, updated_at = ?
                    WHERE agent_id = ?
                """, [clearance, json.dumps(permissions or []), json.dumps(restrictions or []),
                      supervisor_id, trust_score, now, agent_id])
            else:
                conn.execute("""
                    INSERT INTO mcp_agent_profiles
                    (agent_id, swarm_id, clearance, permissions, restrictions, supervisor_id, trust_score, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [agent_id, swarm_id, clearance, json.dumps(permissions or []),
                      json.dumps(restrictions or []), supervisor_id, trust_score, now, now])

        return self.get_mcp_profile(agent_id) or {}

    def get_mcp_profile(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get an MCP agent profile."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM mcp_agent_profiles WHERE agent_id = ?", [agent_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            profile = dict(zip(columns, result))

            for field in ["permissions", "restrictions"]:
                if profile.get(field):
                    profile[field] = json.loads(profile[field]) if isinstance(profile[field], str) else profile[field]

            return profile

    def list_mcp_profiles(self, swarm_id: str) -> List[Dict[str, Any]]:
        """List all MCP profiles for a swarm."""
        with self.connection() as conn:
            results = conn.execute(
                "SELECT * FROM mcp_agent_profiles WHERE swarm_id = ? ORDER BY clearance DESC",
                [swarm_id]
            ).fetchall()

            columns = [desc[0] for desc in conn.description]
            profiles = []
            for row in results:
                profile = dict(zip(columns, row))
                for field in ["permissions", "restrictions"]:
                    if profile.get(field):
                        profile[field] = json.loads(profile[field]) if isinstance(profile[field], str) else profile[field]
                profiles.append(profile)

            return profiles

    def update_mcp_trust_score(self, agent_id: str, delta: float) -> bool:
        """Update an agent's trust score."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                UPDATE mcp_agent_profiles SET
                    trust_score = GREATEST(0.0, LEAST(1.0, trust_score + ?)),
                    updated_at = ?
                WHERE agent_id = ?
            """, [delta, now, agent_id])
            return True

    # =========================================================================
    # MCP VIOLATION OPERATIONS
    # =========================================================================

    def create_violation(
        self,
        violation_id: str,
        swarm_id: str,
        agent_id: str,
        action: str,
        target: str,
        reason: str,
        severity: int = 1,
        remediation: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record a policy violation."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO mcp_violations (id, swarm_id, agent_id, action, target, reason, severity, remediation, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [violation_id, swarm_id, agent_id, action, target, reason, severity, remediation, now])

            # Update agent's violation count and trust score
            conn.execute("""
                UPDATE mcp_agent_profiles SET
                    violation_count = violation_count + 1,
                    trust_score = GREATEST(0.0, trust_score - ?),
                    updated_at = ?
                WHERE agent_id = ?
            """, [0.05 * severity, now, agent_id])

        return self.get_violation(violation_id) or {}

    def get_violation(self, violation_id: str) -> Optional[Dict[str, Any]]:
        """Get a violation by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM mcp_violations WHERE id = ?", [violation_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))

    def list_violations(self, swarm_id: str, agent_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """List violations for a swarm or agent."""
        with self.connection() as conn:
            if agent_id:
                results = conn.execute(
                    "SELECT * FROM mcp_violations WHERE swarm_id = ? AND agent_id = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, agent_id, limit]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM mcp_violations WHERE swarm_id = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, limit]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in results]

    # =========================================================================
    # MCP AUTHORIZATION REQUEST OPERATIONS
    # =========================================================================

    def create_auth_request(
        self,
        request_id: str,
        swarm_id: str,
        agent_id: str,
        action: str,
        target: str,
        justification: str,
        context: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Create an authorization request."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO mcp_auth_requests (id, swarm_id, agent_id, action, target, justification, context, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """, [request_id, swarm_id, agent_id, action, target, justification, json.dumps(context or {}), now])

        return self.get_auth_request(request_id) or {}

    def get_auth_request(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get an authorization request."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM mcp_auth_requests WHERE id = ?", [request_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            req = dict(zip(columns, result))

            for field in ["context", "conditions"]:
                if req.get(field):
                    req[field] = json.loads(req[field]) if isinstance(req[field], str) else req[field]

            return req

    def resolve_auth_request(
        self,
        request_id: str,
        authorized: bool,
        authorizer: str,
        reason: str,
        conditions: Optional[List[str]] = None,
        expires_at: Optional[datetime] = None,
    ) -> bool:
        """Resolve an authorization request."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                UPDATE mcp_auth_requests SET
                    status = ?, authorized = ?, authorizer = ?, reason = ?,
                    conditions = ?, expires_at = ?, resolved_at = ?
                WHERE id = ?
            """, ['approved' if authorized else 'denied', authorized, authorizer, reason,
                  json.dumps(conditions or []), expires_at, now, request_id])
            return True

    def list_pending_auth_requests(self, swarm_id: str) -> List[Dict[str, Any]]:
        """List pending authorization requests."""
        with self.connection() as conn:
            results = conn.execute(
                "SELECT * FROM mcp_auth_requests WHERE swarm_id = ? AND status = 'pending' ORDER BY created_at",
                [swarm_id]
            ).fetchall()

            columns = [desc[0] for desc in conn.description]
            reqs = []
            for row in results:
                req = dict(zip(columns, row))
                for field in ["context", "conditions"]:
                    if req.get(field):
                        req[field] = json.loads(req[field]) if isinstance(req[field], str) else req[field]
                reqs.append(req)

            return reqs

    # =========================================================================
    # MCP AUDIT LOG OPERATIONS
    # =========================================================================

    def log_mcp_event(
        self,
        swarm_id: str,
        event_type: str,
        agent_id: Optional[str] = None,
        data: Optional[Dict] = None,
    ) -> str:
        """Log an MCP audit event."""
        import uuid
        event_id = str(uuid.uuid4())

        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO mcp_audit_log (id, swarm_id, event_type, agent_id, data, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [event_id, swarm_id, event_type, agent_id, json.dumps(data or {}), now])

        return event_id

    def list_mcp_audit_log(self, swarm_id: str, event_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """List MCP audit log entries."""
        with self.connection() as conn:
            if event_type:
                results = conn.execute(
                    "SELECT * FROM mcp_audit_log WHERE swarm_id = ? AND event_type = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, event_type, limit]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM mcp_audit_log WHERE swarm_id = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, limit]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            logs = []
            for row in results:
                entry = dict(zip(columns, row))
                if entry.get("data"):
                    entry["data"] = json.loads(entry["data"]) if isinstance(entry["data"], str) else entry["data"]
                logs.append(entry)

            return logs

    # =========================================================================
    # CODE CHANGE OPERATIONS
    # =========================================================================

    def create_code_change(
        self,
        change_id: str,
        swarm_id: str,
        proposer_id: str,
        file_path: str,
        change_type: str,
        description: str,
        original_content: str,
        proposed_content: str,
        diff: str,
        justification: str = "",
        checksum_before: str = "",
    ) -> Dict[str, Any]:
        """Create a code change record."""
        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO code_changes
                (id, swarm_id, proposer_id, file_path, change_type, description,
                 original_content, proposed_content, diff, justification, checksum_before, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [change_id, swarm_id, proposer_id, file_path, change_type, description,
                  original_content, proposed_content, diff, justification, checksum_before, now])

        return self.get_code_change(change_id) or {}

    def get_code_change(self, change_id: str) -> Optional[Dict[str, Any]]:
        """Get a code change by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM code_changes WHERE id = ?", [change_id]
            ).fetchone()

            if not result:
                return None

            columns = [desc[0] for desc in conn.description]
            change = dict(zip(columns, result))

            for field in ["reviewers", "approvals", "rejections", "review_comments"]:
                if change.get(field):
                    change[field] = json.loads(change[field]) if isinstance(change[field], str) else change[field]

            return change

    def update_code_change(self, change_id: str, **updates) -> Optional[Dict[str, Any]]:
        """Update a code change."""
        if not updates:
            return self.get_code_change(change_id)

        # Serialize JSON fields
        for field in ["reviewers", "approvals", "rejections", "review_comments"]:
            if field in updates and not isinstance(updates[field], str):
                updates[field] = json.dumps(updates[field])

        with self.connection() as conn:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [change_id]
            conn.execute(f"UPDATE code_changes SET {set_clause} WHERE id = ?", values)

        return self.get_code_change(change_id)

    def list_code_changes(self, swarm_id: str, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """List code changes for a swarm."""
        with self.connection() as conn:
            if status:
                results = conn.execute(
                    "SELECT * FROM code_changes WHERE swarm_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, status, limit]
                ).fetchall()
            else:
                results = conn.execute(
                    "SELECT * FROM code_changes WHERE swarm_id = ? ORDER BY created_at DESC LIMIT ?",
                    [swarm_id, limit]
                ).fetchall()

            columns = [desc[0] for desc in conn.description]
            changes = []
            for row in results:
                change = dict(zip(columns, row))
                for field in ["reviewers", "approvals", "rejections", "review_comments"]:
                    if change.get(field):
                        change[field] = json.loads(change[field]) if isinstance(change[field], str) else change[field]
                changes.append(change)

            return changes

    # =========================================================================
    # LLM USAGE TRACKING
    # =========================================================================

    def track_llm_usage(
        self,
        swarm_id: Optional[str],
        agent_id: Optional[str],
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        cost_usd: float,
        task_type: Optional[str] = None,
        success: bool = True,
        latency_ms: Optional[int] = None,
    ) -> str:
        """Track LLM API usage."""
        import uuid
        usage_id = str(uuid.uuid4())

        with self.connection() as conn:
            now = datetime.now(timezone.utc)
            total_tokens = prompt_tokens + completion_tokens
            conn.execute("""
                INSERT INTO llm_usage
                (id, swarm_id, agent_id, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, task_type, success, latency_ms, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [usage_id, swarm_id, agent_id, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, task_type, success, latency_ms, now])

        return usage_id

    def get_llm_usage_stats(self, swarm_id: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
        """Get LLM usage statistics."""
        with self.connection() as conn:
            from datetime import timedelta
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            if swarm_id:
                result = conn.execute("""
                    SELECT
                        COUNT(*) as total_requests,
                        SUM(prompt_tokens) as total_prompt_tokens,
                        SUM(completion_tokens) as total_completion_tokens,
                        SUM(total_tokens) as total_tokens,
                        SUM(cost_usd) as total_cost,
                        AVG(latency_ms) as avg_latency,
                        SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                    FROM llm_usage
                    WHERE swarm_id = ? AND created_at >= ?
                """, [swarm_id, cutoff]).fetchone()
            else:
                result = conn.execute("""
                    SELECT
                        COUNT(*) as total_requests,
                        SUM(prompt_tokens) as total_prompt_tokens,
                        SUM(completion_tokens) as total_completion_tokens,
                        SUM(total_tokens) as total_tokens,
                        SUM(cost_usd) as total_cost,
                        AVG(latency_ms) as avg_latency,
                        SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                    FROM llm_usage
                    WHERE created_at >= ?
                """, [cutoff]).fetchone()

            if not result:
                return {"total_requests": 0, "total_cost": 0.0}

            return {
                "total_requests": result[0] or 0,
                "total_prompt_tokens": result[1] or 0,
                "total_completion_tokens": result[2] or 0,
                "total_tokens": result[3] or 0,
                "total_cost_usd": result[4] or 0.0,
                "avg_latency_ms": result[5] or 0,
                "successful_requests": result[6] or 0,
                "success_rate": (result[6] or 0) / max(1, result[0] or 1),
                "period_days": days,
            }

    def list_llm_usage_by_model(self, swarm_id: Optional[str] = None, days: int = 30) -> List[Dict[str, Any]]:
        """Get LLM usage breakdown by model."""
        with self.connection() as conn:
            from datetime import timedelta
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            if swarm_id:
                results = conn.execute("""
                    SELECT model, COUNT(*) as requests, SUM(total_tokens) as tokens, SUM(cost_usd) as cost
                    FROM llm_usage
                    WHERE swarm_id = ? AND created_at >= ?
                    GROUP BY model
                    ORDER BY cost DESC
                """, [swarm_id, cutoff]).fetchall()
            else:
                results = conn.execute("""
                    SELECT model, COUNT(*) as requests, SUM(total_tokens) as tokens, SUM(cost_usd) as cost
                    FROM llm_usage
                    WHERE created_at >= ?
                    GROUP BY model
                    ORDER BY cost DESC
                """, [cutoff]).fetchall()

            return [{"model": r[0], "requests": r[1], "tokens": r[2], "cost_usd": r[3]} for r in results]


# Singleton accessor
def get_swarm_db(db_path: Optional[str] = None) -> SwarmDB:
    """Get the SwarmDB singleton."""
    return SwarmDB.get_instance(db_path)
