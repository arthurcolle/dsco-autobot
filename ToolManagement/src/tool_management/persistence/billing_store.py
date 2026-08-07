"""
Customer billing + provisioned resources — the DigitalOcean-style account model.

customers:     card-on-file status per user (Stripe customer when configured)
subscriptions: durable paid-entitlement state, synchronized from Stripe webhooks
stripe_events: processed webhook IDs for idempotent event handling
usage_events:  metered consumption (tool executions, resource-hours) priced per event
resources:     things a user has provisioned (swarms, projects) with lifecycle

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

# Usage-based (pay-as-you-go) prices in cents per unit for the metered Gateway and
# checkpointed-model surfaces. Any key (a `kind`, a `ref`/model id, or "kind:ref")
# can be overridden at deploy time via the USAGE_PRICES env var (JSON {key: cents}).
USAGE_PRICE_SHEET = {
    "gateway_call": 0.05,      # per Gateway execute (routing + tool orchestration)
    "model_inference": 0.20,   # per checkpointed-model call (default; override per model id)
    "tool_execution": 0.01,
    "job_execution": 0.05,
}

# Prepaid credit balances are stored as integer micro-cents (µ¢) so sub-cent calls
# debit exactly, with no floating-point drift. 1 cent = 1000 µ¢.
UCENTS_PER_CENT = 1000


def _price_overrides() -> Dict[str, float]:
    raw = os.environ.get("USAGE_PRICES", "").strip()
    if not raw:
        return {}
    try:
        return {str(k): float(v) for k, v in json.loads(raw).items()}
    except Exception:
        logger.warning("USAGE_PRICES is not valid JSON; ignoring")
        return {}

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

ENTITLED_SUBSCRIPTION_STATUSES = frozenset({"active", "trialing"})
TERMINAL_SUBSCRIPTION_STATUSES = frozenset({"canceled", "incomplete_expired"})


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
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id VARCHAR PRIMARY KEY,
                    provider VARCHAR NOT NULL,
                    provider_customer_id VARCHAR,
                    provider_subscription_id VARCHAR,
                    price_id VARCHAR,
                    status VARCHAR NOT NULL,
                    access_until DOUBLE,
                    cancel_at_period_end BOOLEAN DEFAULT FALSE,
                    last_event_id VARCHAR,
                    last_event_created DOUBLE DEFAULT 0,
                    updated_at DOUBLE
                )
                """
            )
            # DuckDB 1.1 can't update a column referenced by a secondary index
            # inside an UPSERT. Ownership uniqueness is enforced under _lock in
            # _write_subscription instead.
            con.execute("DROP INDEX IF EXISTS subscriptions_provider_id_idx")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS stripe_events (
                    event_id VARCHAR PRIMARY KEY,
                    event_type VARCHAR NOT NULL,
                    object_id VARCHAR,
                    event_created DOUBLE,
                    processed_at DOUBLE,
                    payload_hash VARCHAR
                )
                """
            )
            # Prepaid credit balances (usage-based pay-as-you-go). Balance is
            # integer micro-cents; the ledger is append-only for audit and for
            # idempotent top-ups keyed by ext_ref (e.g. a Stripe event id).
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS credit_balances (
                    user_id VARCHAR PRIMARY KEY,
                    balance_ucents BIGINT DEFAULT 0,
                    updated_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS credit_ledger (
                    entry_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    delta_ucents BIGINT,
                    reason VARCHAR,
                    ext_ref VARCHAR,
                    ts DOUBLE
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

    # ==================== Subscriptions / entitlement ====================

    @staticmethod
    def _subscription_from_row(row: Any) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "user_id": row[0],
            "provider": row[1],
            "provider_customer_id": row[2],
            "provider_subscription_id": row[3],
            "price_id": row[4],
            "status": row[5],
            "access_until": row[6],
            "cancel_at_period_end": bool(row[7]),
            "last_event_id": row[8],
            "last_event_created": float(row[9] or 0),
            "updated_at": row[10],
        }

    @classmethod
    def _get_subscription_for_user(
        cls, con: duckdb.DuckDBPyConnection, user_id: str
    ) -> Optional[Dict[str, Any]]:
        row = con.execute(
            "SELECT user_id, provider, provider_customer_id, "
            "provider_subscription_id, price_id, status, access_until, "
            "cancel_at_period_end, last_event_id, last_event_created, updated_at "
            "FROM subscriptions WHERE user_id = ?",
            [user_id],
        ).fetchone()
        return cls._subscription_from_row(row)

    @classmethod
    def _get_subscription_for_provider_id(
        cls, con: duckdb.DuckDBPyConnection, provider_subscription_id: str
    ) -> Optional[Dict[str, Any]]:
        row = con.execute(
            "SELECT user_id, provider, provider_customer_id, "
            "provider_subscription_id, price_id, status, access_until, "
            "cancel_at_period_end, last_event_id, last_event_created, updated_at "
            "FROM subscriptions WHERE provider_subscription_id = ?",
            [provider_subscription_id],
        ).fetchone()
        return cls._subscription_from_row(row)

    @staticmethod
    def _write_subscription(
        con: duckdb.DuckDBPyConnection, subscription: Dict[str, Any]
    ) -> None:
        provider_subscription_id = subscription.get("provider_subscription_id")
        if provider_subscription_id:
            owner = con.execute(
                "SELECT user_id FROM subscriptions "
                "WHERE provider_subscription_id = ? AND user_id != ? LIMIT 1",
                [provider_subscription_id, subscription["user_id"]],
            ).fetchone()
            if owner:
                raise ValueError("Stripe subscription is already owned by another user")
        con.execute(
            """
            INSERT INTO subscriptions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                provider = excluded.provider,
                provider_customer_id = excluded.provider_customer_id,
                provider_subscription_id = excluded.provider_subscription_id,
                price_id = excluded.price_id,
                status = excluded.status,
                access_until = excluded.access_until,
                cancel_at_period_end = excluded.cancel_at_period_end,
                last_event_id = excluded.last_event_id,
                last_event_created = excluded.last_event_created,
                updated_at = excluded.updated_at
            """,
            [
                subscription["user_id"],
                subscription["provider"],
                subscription.get("provider_customer_id"),
                subscription.get("provider_subscription_id"),
                subscription.get("price_id"),
                subscription["status"],
                subscription.get("access_until"),
                bool(subscription.get("cancel_at_period_end", False)),
                subscription.get("last_event_id"),
                float(subscription.get("last_event_created") or 0),
                float(subscription.get("updated_at") or time.time()),
            ],
        )

    def get_subscription(self, user_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            return self._get_subscription_for_user(con, user_id)

    def get_subscription_by_provider_id(
        self, provider_subscription_id: str
    ) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            return self._get_subscription_for_provider_id(con, provider_subscription_id)

    def upsert_subscription(self, user_id: str, **fields: Any) -> Dict[str, Any]:
        """Write subscription state from a trusted internal source.

        Stripe webhook state should normally flow through ``apply_stripe_event``
        so the event receipt and entitlement transition are atomic. This method
        exists for the explicit local BILLING_DEV_MODE path and focused recovery.
        """
        with self._lock, self._connect() as con:
            existing = self._get_subscription_for_user(con, user_id) or {
                "user_id": user_id,
                "provider": "stripe",
                "provider_customer_id": None,
                "provider_subscription_id": None,
                "price_id": None,
                "status": "incomplete",
                "access_until": None,
                "cancel_at_period_end": False,
                "last_event_id": None,
                "last_event_created": 0,
                "updated_at": time.time(),
            }
            existing.update(fields)
            existing["updated_at"] = float(fields.get("updated_at") or time.time())
            self._write_subscription(con, existing)
            return existing

    def has_paid_entitlement(
        self,
        user_id: str,
        *,
        allow_dev: bool = False,
        now: Optional[float] = None,
    ) -> bool:
        """Return whether the durable subscription state grants product access.

        Stripe access is fail-closed: it requires an active/trialing state *and*
        a future access boundary. A synthetic dev subscription is honored only
        while the process is explicitly running with BILLING_DEV_MODE enabled.
        """
        subscription = self.get_subscription(user_id)
        if not subscription or subscription["status"] not in ENTITLED_SUBSCRIPTION_STATUSES:
            return False
        if subscription["provider"] == "dev":
            return allow_dev
        if subscription["provider"] != "stripe":
            return False
        access_until = subscription.get("access_until")
        clock = time.time() if now is None else now
        return access_until is not None and float(access_until) > float(clock)

    def has_active_entitlement(self, user_id: str) -> bool:
        """Compatibility name used by the public tool-execution gate."""
        allow_dev = os.environ.get("BILLING_DEV_MODE", "").strip().lower() in {
            "1", "true", "yes", "on",
        }
        return self.has_paid_entitlement(user_id, allow_dev=allow_dev)

    @staticmethod
    def _metadata_user_id(obj: Dict[str, Any]) -> Optional[str]:
        metadata = obj.get("metadata") or {}
        user_id = metadata.get("user_id") if isinstance(metadata, dict) else None
        if not user_id and obj.get("object") == "invoice":
            parent = obj.get("parent") or {}
            details = parent.get("subscription_details") or {}
            if not details:
                details = obj.get("subscription_details") or {}
            metadata = details.get("metadata") or {}
            user_id = metadata.get("user_id") if isinstance(metadata, dict) else None
        if not user_id and obj.get("object") == "checkout.session":
            user_id = obj.get("client_reference_id")
        return str(user_id) if user_id else None

    @staticmethod
    def _subscription_id_from_invoice(invoice: Dict[str, Any]) -> Optional[str]:
        subscription_id = invoice.get("subscription")
        if isinstance(subscription_id, dict):
            subscription_id = subscription_id.get("id")
        if subscription_id:
            return str(subscription_id)
        parent = invoice.get("parent") or {}
        details = parent.get("subscription_details") or {}
        subscription_id = details.get("subscription")
        if isinstance(subscription_id, dict):
            subscription_id = subscription_id.get("id")
        return str(subscription_id) if subscription_id else None

    @staticmethod
    def _period_end(obj: Dict[str, Any]) -> Optional[float]:
        candidates: List[float] = []
        for key in ("current_period_end", "trial_end"):
            value = obj.get(key)
            if isinstance(value, (int, float)):
                candidates.append(float(value))
        items = (obj.get("items") or {}).get("data") or []
        lines = (obj.get("lines") or {}).get("data") or []
        for item in [*items, *lines]:
            value = item.get("current_period_end") if isinstance(item, dict) else None
            if isinstance(value, (int, float)):
                candidates.append(float(value))
            period = item.get("period") if isinstance(item, dict) else None
            value = period.get("end") if isinstance(period, dict) else None
            if isinstance(value, (int, float)):
                candidates.append(float(value))
        return max(candidates) if candidates else None

    @staticmethod
    def _price_id(obj: Dict[str, Any]) -> Optional[str]:
        metadata = obj.get("metadata") or {}
        if isinstance(metadata, dict) and metadata.get("price_id"):
            return str(metadata["price_id"])
        items = (obj.get("items") or {}).get("data") or []
        lines = (obj.get("lines") or {}).get("data") or []
        for item in [*items, *lines]:
            if not isinstance(item, dict):
                continue
            price = item.get("price")
            if isinstance(price, dict) and price.get("id"):
                return str(price["id"])
            pricing = item.get("pricing") or {}
            details = pricing.get("price_details") or {}
            price = details.get("price")
            if isinstance(price, str):
                return price
            if isinstance(price, dict) and price.get("id"):
                return str(price["id"])
        return None

    @staticmethod
    def _customer_for_user(
        con: duckdb.DuckDBPyConnection, user_id: str
    ) -> Optional[Dict[str, Any]]:
        row = con.execute(
            "SELECT user_id, provider, provider_customer_id, card_brand, card_last4, "
            "billing_ready, dev_mode, created_at FROM customers WHERE user_id = ?",
            [user_id],
        ).fetchone()
        if not row:
            return None
        return {
            "user_id": row[0], "provider": row[1], "provider_customer_id": row[2],
            "card_brand": row[3], "card_last4": row[4], "billing_ready": bool(row[5]),
            "dev_mode": bool(row[6]), "created_at": row[7],
        }

    @staticmethod
    def _user_for_customer(
        con: duckdb.DuckDBPyConnection, provider_customer_id: Optional[str]
    ) -> Optional[str]:
        if not provider_customer_id:
            return None
        row = con.execute(
            "SELECT user_id FROM customers WHERE provider_customer_id = ? LIMIT 1",
            [provider_customer_id],
        ).fetchone()
        return str(row[0]) if row else None

    @staticmethod
    def _upsert_stripe_customer(
        con: duckdb.DuckDBPyConnection,
        user_id: str,
        provider_customer_id: Optional[str],
        *,
        billing_ready: Optional[bool] = None,
    ) -> None:
        row = BillingStore._customer_for_user(con, user_id) or {
            "user_id": user_id,
            "provider": "stripe",
            "provider_customer_id": None,
            "card_brand": None,
            "card_last4": None,
            "billing_ready": False,
            "dev_mode": False,
            "created_at": time.time(),
        }
        row.update({
            "provider": "stripe",
            "provider_customer_id": provider_customer_id or row.get("provider_customer_id"),
            "dev_mode": False,
        })
        if billing_ready is not None:
            row["billing_ready"] = bool(billing_ready)
        con.execute(
            """
            INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                provider = excluded.provider,
                provider_customer_id = excluded.provider_customer_id,
                card_brand = excluded.card_brand,
                card_last4 = excluded.card_last4,
                billing_ready = excluded.billing_ready,
                dev_mode = excluded.dev_mode,
                created_at = excluded.created_at
            """,
            [
                row["user_id"], row["provider"], row["provider_customer_id"],
                row["card_brand"], row["card_last4"], row["billing_ready"],
                row["dev_mode"], row["created_at"],
            ],
        )

    def apply_stripe_event(
        self, event: Dict[str, Any], *, payload_hash: str = ""
    ) -> Dict[str, Any]:
        """Atomically deduplicate and apply one verified Stripe event.

        This method intentionally accepts only an already signature-verified
        event. It stores no raw payload or payment details.
        """
        event_id = str(event.get("id") or "")
        event_type = str(event.get("type") or "")
        data_object = ((event.get("data") or {}).get("object") or {})
        if not event_id or not event_type or not isinstance(data_object, dict):
            raise ValueError("Malformed Stripe event")
        event_created = float(event.get("created") or 0)
        object_id = str(data_object.get("id") or "") or None

        with self._lock, self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                seen = con.execute(
                    "SELECT 1 FROM stripe_events WHERE event_id = ?", [event_id]
                ).fetchone()
                if seen:
                    con.execute("ROLLBACK")
                    return {"processed": False, "duplicate": True, "event_id": event_id}

                customer_id = data_object.get("customer")
                if isinstance(customer_id, dict):
                    customer_id = customer_id.get("id")
                customer_id = str(customer_id) if customer_id else None
                subscription_id: Optional[str] = None
                user_id = self._metadata_user_id(data_object)
                recognized = False
                fields: Dict[str, Any] = {}

                # One-time prepaid credit top-up (mode=payment) — credit the balance
                # and finish. Idempotent both via stripe_events (above) and credit_ledger.
                if event_type == "checkout.session.completed" and data_object.get("mode") == "payment":
                    md = data_object.get("metadata") or {}
                    uid = user_id or self._user_for_customer(con, customer_id)
                    amount_cents = 0.0
                    try:
                        amount_cents = float(md.get("credit_cents") or 0) or float(
                            data_object.get("amount_total") or 0
                        )
                    except (TypeError, ValueError):
                        amount_cents = 0.0
                    credited = False
                    if uid and amount_cents > 0:
                        credited = self._credit_locked(
                            con, uid, amount_cents, "stripe_topup", ext_ref=event_id
                        )
                    con.execute(
                        "INSERT INTO stripe_events VALUES (?, ?, ?, ?, ?, ?)",
                        [event_id, event_type, object_id, event_created, time.time(), payload_hash],
                    )
                    con.execute("COMMIT")
                    return {
                        "processed": True, "duplicate": False, "recognized": True,
                        "applied": credited, "credit": True,
                        "event_id": event_id, "user_id": uid,
                    }

                if event_type == "checkout.session.completed" and data_object.get("mode") == "subscription":
                    recognized = True
                    subscription_id = data_object.get("subscription")
                    if isinstance(subscription_id, dict):
                        subscription_id = subscription_id.get("id")
                    subscription_id = str(subscription_id) if subscription_id else None
                    payment_status = str(data_object.get("payment_status") or "")
                    fields = {
                        "price_id": self._price_id(data_object),
                        "status": (
                            "active" if payment_status == "paid" else
                            "trialing" if payment_status == "no_payment_required" else
                            "incomplete"
                        ),
                    }
                elif event_type.startswith("customer.subscription."):
                    recognized = True
                    subscription_id = object_id
                    fields = {
                        "price_id": self._price_id(data_object),
                        "status": (
                            "canceled" if event_type == "customer.subscription.deleted"
                            else str(data_object.get("status") or "incomplete")
                        ),
                        "access_until": self._period_end(data_object),
                        "cancel_at_period_end": bool(data_object.get("cancel_at_period_end", False)),
                    }
                elif event_type in {"invoice.paid", "invoice.payment_failed"}:
                    recognized = True
                    subscription_id = self._subscription_id_from_invoice(data_object)
                    fields = {
                        "price_id": self._price_id(data_object),
                        "status": "active" if event_type == "invoice.paid" else "past_due",
                    }
                    if event_type == "invoice.paid":
                        fields["access_until"] = self._period_end(data_object)

                mapped_subscription = (
                    self._get_subscription_for_provider_id(con, subscription_id)
                    if subscription_id else None
                )
                mapped_customer_user = self._user_for_customer(con, customer_id)
                for mapped_user in (
                    mapped_subscription.get("user_id") if mapped_subscription else None,
                    mapped_customer_user,
                ):
                    if user_id and mapped_user and user_id != mapped_user:
                        raise ValueError("Stripe event identity does not match stored billing owner")
                    user_id = user_id or mapped_user

                applied = False
                stale = False
                if recognized and subscription_id and not user_id:
                    # Do not acknowledge an out-of-order invoice before its
                    # customer/subscription owner is known. Rolling back leaves
                    # the event unclaimed so Stripe's retry can apply it after a
                    # mapping event arrives.
                    raise ValueError("Stripe billing owner is not known yet")
                if recognized and user_id and subscription_id:
                    existing = self._get_subscription_for_user(con, user_id)
                    if (
                        existing
                        and existing.get("provider_subscription_id")
                        and existing["provider_subscription_id"] != subscription_id
                        and existing.get("status") not in TERMINAL_SUBSCRIPTION_STATUSES
                    ):
                        raise ValueError("User already has a different non-terminal subscription")
                    if existing and existing.get("last_event_created", 0) > event_created:
                        stale = True
                        # Stripe doesn't guarantee event order. An invoice.paid
                        # event can be created before checkout.session.completed
                        # but arrive afterward. Merge its later access boundary
                        # without letting the older event roll back newer status.
                        paid_until = fields.get("access_until")
                        existing_until = existing.get("access_until")
                        if (
                            event_type == "invoice.paid"
                            and paid_until is not None
                            and (existing_until is None or float(paid_until) > float(existing_until))
                        ):
                            existing["access_until"] = paid_until
                            existing["updated_at"] = time.time()
                            self._write_subscription(con, existing)
                            applied = True
                        # A subscription-created event can likewise predate the
                        # Checkout completion event. It is safe to fill a missing
                        # period boundary while retaining the newer local status.
                        elif (
                            event_type.startswith("customer.subscription.")
                            and fields.get("access_until") is not None
                            and existing_until is None
                            and existing.get("status") in {"active", "trialing", "incomplete"}
                        ):
                            existing["access_until"] = fields["access_until"]
                            if existing.get("status") == "incomplete":
                                existing["status"] = fields.get("status", existing["status"])
                            existing["updated_at"] = time.time()
                            self._write_subscription(con, existing)
                            applied = True
                    else:
                        subscription = existing or {
                            "user_id": user_id,
                            "provider": "stripe",
                            "provider_customer_id": customer_id,
                            "provider_subscription_id": subscription_id,
                            "price_id": None,
                            "status": "incomplete",
                            "access_until": None,
                            "cancel_at_period_end": False,
                        }
                        if (
                            event_type in {"invoice.paid", "invoice.payment_failed"}
                            and subscription.get("status") in TERMINAL_SUBSCRIPTION_STATUSES
                        ):
                            fields.pop("status", None)
                        subscription.update({
                            "provider": "stripe",
                            "provider_customer_id": customer_id or subscription.get("provider_customer_id"),
                            "provider_subscription_id": subscription_id,
                            "last_event_id": event_id,
                            "last_event_created": event_created,
                            "updated_at": time.time(),
                        })
                        subscription.update({k: v for k, v in fields.items() if v is not None})
                        self._write_subscription(con, subscription)
                        self._upsert_stripe_customer(
                            con,
                            user_id,
                            customer_id,
                            billing_ready=(
                                data_object.get("payment_status") in {"paid", "no_payment_required"}
                                if event_type == "checkout.session.completed" else None
                            ),
                        )
                        applied = True

                con.execute(
                    "INSERT INTO stripe_events VALUES (?, ?, ?, ?, ?, ?)",
                    [event_id, event_type, object_id, event_created, time.time(), payload_hash],
                )
                con.execute("COMMIT")
                return {
                    "processed": True,
                    "duplicate": False,
                    "recognized": recognized,
                    "applied": applied,
                    "stale": stale,
                    "event_id": event_id,
                    "user_id": user_id,
                }
            except Exception:
                con.execute("ROLLBACK")
                raise

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

    # ==================== Prepaid credit balance (usage-based) ====================

    def price_cents(self, kind: str, ref: str = "") -> float:
        """Resolve the per-call price in cents. Precedence: USAGE_PRICES env
        override (by "kind:ref", then ref, then kind) → USAGE_PRICE_SHEET → PRICE_SHEET."""
        overrides = _price_overrides()
        for key in (f"{kind}:{ref}" if ref else None, ref or None, kind):
            if key and key in overrides:
                return overrides[key]
        if kind in USAGE_PRICE_SHEET:
            return USAGE_PRICE_SHEET[kind]
        return float(PRICE_SHEET.get(kind, 0.0))

    @staticmethod
    def _balance_ucents(con: duckdb.DuckDBPyConnection, user_id: str) -> int:
        row = con.execute(
            "SELECT balance_ucents FROM credit_balances WHERE user_id = ?", [user_id]
        ).fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _credit_locked(
        con: duckdb.DuckDBPyConnection,
        user_id: str,
        amount_cents: float,
        reason: str,
        ext_ref: Optional[str] = None,
    ) -> bool:
        """Credit within an existing connection. Idempotent on ext_ref (a repeated
        top-up event is a no-op). Returns True if a credit was applied."""
        if ext_ref:
            seen = con.execute(
                "SELECT 1 FROM credit_ledger WHERE ext_ref = ? LIMIT 1", [ext_ref]
            ).fetchone()
            if seen:
                return False
        ucents = int(round(float(amount_cents) * UCENTS_PER_CENT))
        if ucents <= 0:
            return False
        con.execute(
            "INSERT INTO credit_ledger VALUES (?, ?, ?, ?, ?, ?)",
            ["cl_" + secrets.token_hex(10), user_id, ucents, reason, ext_ref, time.time()],
        )
        con.execute(
            """
            INSERT INTO credit_balances VALUES (?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                balance_ucents = credit_balances.balance_ucents + excluded.balance_ucents,
                updated_at = excluded.updated_at
            """,
            [user_id, ucents, time.time()],
        )
        return True

    def _debit_locked(
        self,
        con: duckdb.DuckDBPyConnection,
        user_id: str,
        amount_cents: float,
        kind: str,
        ref: str,
    ) -> bool:
        """Debit within an existing connection. Never overdraws: returns False and
        leaves the balance unchanged when funds are insufficient."""
        ucents = int(round(float(amount_cents) * UCENTS_PER_CENT))
        if ucents <= 0:
            return True  # free call
        bal = self._balance_ucents(con, user_id)
        if bal < ucents:
            return False
        con.execute(
            "UPDATE credit_balances SET balance_ucents = balance_ucents - ?, updated_at = ? "
            "WHERE user_id = ?",
            [ucents, time.time(), user_id],
        )
        con.execute(
            "INSERT INTO credit_ledger VALUES (?, ?, ?, ?, ?, ?)",
            ["cl_" + secrets.token_hex(10), user_id, -ucents, "debit:" + kind, ref, time.time()],
        )
        # mirror into usage_events so invoices / usage_summary see the same spend
        con.execute(
            "INSERT INTO usage_events VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["ue_" + secrets.token_hex(10), user_id, kind, ref, 1, float(amount_cents), time.time()],
        )
        return True

    def get_balance(self, user_id: str) -> float:
        """Current prepaid balance in cents."""
        with self._lock, self._connect() as con:
            return self._balance_ucents(con, user_id) / UCENTS_PER_CENT

    def credit(
        self, user_id: str, amount_cents: float, reason: str = "topup",
        ext_ref: Optional[str] = None,
    ) -> float:
        """Add prepaid credits (from a Stripe top-up, wallet deposit, or grant).
        Idempotent on ext_ref. Returns the new balance in cents."""
        with self._lock, self._connect() as con:
            self._credit_locked(con, user_id, amount_cents, reason, ext_ref)
            return self._balance_ucents(con, user_id) / UCENTS_PER_CENT

    def debit(self, user_id: str, amount_cents: float, kind: str, ref: str = "") -> bool:
        with self._lock, self._connect() as con:
            return self._debit_locked(con, user_id, amount_cents, kind, ref)

    def try_charge(
        self, user_id: str, kind: str, ref: str = "", quantity: float = 1.0
    ) -> Dict[str, Any]:
        """Atomically price and debit one metered call. Returns allow/deny + price +
        resulting balance. This is the enforcement primitive for the Gateway."""
        price = self.price_cents(kind, ref) * quantity
        with self._lock, self._connect() as con:
            allowed = self._debit_locked(con, user_id, price, kind, ref)
            balance = self._balance_ucents(con, user_id) / UCENTS_PER_CENT
        return {
            "allowed": allowed,
            "price_cents": round(price, 6),
            "balance_cents": round(balance, 6),
        }

    def refund(self, user_id: str, amount_cents: float, ref: str = "") -> float:
        """Return credits for a charged-but-failed call. Returns new balance (cents)."""
        with self._lock, self._connect() as con:
            self._credit_locked(con, user_id, amount_cents, "refund", ext_ref=None)
            return self._balance_ucents(con, user_id) / UCENTS_PER_CENT

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
