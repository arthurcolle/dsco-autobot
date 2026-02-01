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

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_swarm ON agents(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_swarm ON tasks(swarm_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_swarm ON transactions(swarm_id)")

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


# Singleton accessor
def get_swarm_db(db_path: Optional[str] = None) -> SwarmDB:
    """Get the SwarmDB singleton."""
    return SwarmDB.get_instance(db_path)
