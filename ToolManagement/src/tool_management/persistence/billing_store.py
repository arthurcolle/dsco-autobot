"""
Customer billing + provisioned resources — the DigitalOcean-style account model.

customers:    card-on-file status per user (Stripe customer when configured)
usage_events: metered consumption (tool executions, resource-hours) priced per event
resources:    things a user has provisioned (swarms, projects) with lifecycle

All in DuckDB on DATA_DIR so it lives on the persistent volume.
"""

import json
import logging
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)

# Transparent v1 price sheet (cents). Tool executions are metered per call;
# resources bill flat monthly (0 during alpha).
PRICE_SHEET = {
    "tool_execution": 0.01,   # $0.0001/call → $0.10 per 1k calls
    "job_execution": 0.05,
    "swarm_monthly": 0,
    "api_project_monthly": 0,
}

RESOURCE_TYPES = {
    "swarm": {
        "description": "Agent swarm — spawn agents, submit tasks, execute LLM workflows",
        "monthly_cents": PRICE_SHEET["swarm_monthly"],
    },
    "api-project": {
        "description": "Named project for organizing usage and API traffic",
        "monthly_cents": PRICE_SHEET["api_project_monthly"],
    },
    "integration-addon": {
        "description": "Managed integration — no API key needed, served from pooled "
                       "platform credentials, priced per provider tier",
        "monthly_cents": None,  # per-provider, from the provider catalog
    },
}


class BillingStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "billing.duckdb")
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS customers (
                    user_id VARCHAR PRIMARY KEY,
                    provider VARCHAR,
                    provider_customer_id VARCHAR,
                    card_brand VARCHAR,
                    card_last4 VARCHAR,
                    billing_ready BOOLEAN DEFAULT FALSE,
                    dev_mode BOOLEAN DEFAULT FALSE,
                    created_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_events (
                    event_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    kind VARCHAR,
                    ref VARCHAR,
                    quantity DOUBLE DEFAULT 1,
                    unit_price_cents DOUBLE,
                    ts DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    resource_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    type VARCHAR,
                    name VARCHAR,
                    status VARCHAR,
                    config VARCHAR,
                    created_at DOUBLE,
                    destroyed_at DOUBLE
                )
                """
            )

    # ==================== Customers / cards ====================

    def get_customer(self, user_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT user_id, provider, provider_customer_id, card_brand, card_last4, "
                "billing_ready, dev_mode, created_at FROM customers WHERE user_id = ?",
                [user_id],
            ).fetchone()
        if not row:
            return None
        return {
            "user_id": row[0], "provider": row[1], "provider_customer_id": row[2],
            "card_brand": row[3], "card_last4": row[4],
            "billing_ready": bool(row[5]), "dev_mode": bool(row[6]), "created_at": row[7],
        }

    def upsert_customer(self, user_id: str, **fields: Any) -> Dict[str, Any]:
        existing = self.get_customer(user_id) or {
            "user_id": user_id, "provider": None, "provider_customer_id": None,
            "card_brand": None, "card_last4": None, "billing_ready": False,
            "dev_mode": False, "created_at": time.time(),
        }
        existing.update(fields)
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    existing["user_id"], existing["provider"],
                    existing["provider_customer_id"], existing["card_brand"],
                    existing["card_last4"], existing["billing_ready"],
                    existing["dev_mode"], existing["created_at"],
                ],
            )
        return existing

    # ==================== Usage ====================

    def record_usage(
        self,
        user_id: str,
        kind: str,
        ref: str = "",
        quantity: float = 1,
        unit_price_cents: Optional[float] = None,
    ) -> None:
        unit = unit_price_cents if unit_price_cents is not None else PRICE_SHEET.get(kind, 0)
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO usage_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["ue_" + secrets.token_hex(10), user_id, kind, ref, quantity, unit, time.time()],
            )

    def active_addon_providers(self, user_id: str) -> set:
        """Providers this user has enabled as managed add-ons."""
        return {
            r["name"]
            for r in self.list_resources(user_id)
            if r["type"] == "integration-addon" and r["status"] == "active"
        }

    @staticmethod
    def _month_start_ts() -> float:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        return datetime(now.year, now.month, 1, tzinfo=timezone.utc).timestamp()

    def addon_charged_this_month(self, user_id: str, provider: str) -> bool:
        """True if this add-on's monthly fee was already charged this calendar
        month — re-enabling within a paid month must not double-bill."""
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM usage_events WHERE user_id = ? AND ref = ? "
                "AND kind = 'integration_addon_monthly' AND unit_price_cents > 0 "
                "AND ts >= ? LIMIT 1",
                [user_id, provider, self._month_start_ts()],
            ).fetchone()
        return bool(row)

    def usage_summary(self, user_id: str, since_ts: Optional[float] = None) -> Dict[str, Any]:
        since = since_ts if since_ts is not None else time.time() - 30 * 86400
        with self._lock, self._connect() as con:
            rows = con.execute(
                """
                SELECT kind, sum(quantity), sum(quantity * unit_price_cents)
                FROM usage_events WHERE user_id = ? AND ts >= ?
                GROUP BY kind ORDER BY 3 DESC
                """,
                [user_id, since],
            ).fetchall()
            top_tools = con.execute(
                """
                SELECT ref, count(*) FROM usage_events
                WHERE user_id = ? AND ts >= ? AND kind = 'tool_execution'
                GROUP BY ref ORDER BY 2 DESC LIMIT 10
                """,
                [user_id, since],
            ).fetchall()
        lines = [
            {"kind": r[0], "quantity": r[1], "amount_cents": round(r[2], 4)}
            for r in rows
        ]
        return {
            "since": since,
            "lines": lines,
            "total_cents": round(sum(l["amount_cents"] for l in lines), 4),
            "top_tools": [{"tool": t[0], "calls": t[1]} for t in top_tools],
        }

    # ==================== Resources ====================

    def create_resource(
        self, user_id: str, rtype: str, name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        resource = {
            "resource_id": f"res_{secrets.token_hex(8)}",
            "user_id": user_id,
            "type": rtype,
            "name": name,
            "status": "active",
            "config": config,
            "created_at": time.time(),
            "destroyed_at": None,
        }
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO resources VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    resource["resource_id"], user_id, rtype, name, "active",
                    json.dumps(config), resource["created_at"], None,
                ],
            )
        return resource

    def list_resources(self, user_id: str, include_destroyed: bool = False) -> List[Dict[str, Any]]:
        q = "SELECT resource_id, type, name, status, config, created_at, destroyed_at " \
            "FROM resources WHERE user_id = ?"
        if not include_destroyed:
            q += " AND status != 'destroyed'"
        q += " ORDER BY created_at DESC"
        with self._lock, self._connect() as con:
            rows = con.execute(q, [user_id]).fetchall()
        return [
            {
                "resource_id": r[0], "type": r[1], "name": r[2], "status": r[3],
                "config": json.loads(r[4] or "{}"), "created_at": r[5],
                "destroyed_at": r[6],
            }
            for r in rows
        ]

    def get_resource(self, user_id: str, resource_id: str) -> Optional[Dict[str, Any]]:
        for r in self.list_resources(user_id, include_destroyed=True):
            if r["resource_id"] == resource_id:
                return r
        return None

    def destroy_resource(self, user_id: str, resource_id: str) -> bool:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM resources WHERE resource_id = ? AND user_id = ? AND status != 'destroyed'",
                [resource_id, user_id],
            ).fetchone()
            if not row:
                return False
            con.execute(
                "UPDATE resources SET status = 'destroyed', destroyed_at = ? WHERE resource_id = ?",
                [time.time(), resource_id],
            )
        return True


_billing_store: Optional[BillingStore] = None
_store_lock = threading.Lock()


def get_billing_store() -> BillingStore:
    global _billing_store
    with _store_lock:
        if _billing_store is None:
            _billing_store = BillingStore()
        return _billing_store
