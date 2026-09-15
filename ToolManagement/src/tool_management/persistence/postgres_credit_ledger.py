"""Postgres-backed prepaid-credit ledger for multi-container deployments.

The surrounding BillingStore still owns subscriptions, resource metadata, and
usage reporting. This component moves the money primitive itself to Postgres:
idempotent credits and conditional debits are serialized by the database, not
by a process-local lock.
"""
from __future__ import annotations

import secrets
import time
from typing import Optional


UCENTS_PER_CENT = 1000


class PostgresCreditLedger:
    def __init__(self, database_url: str):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment dependency guard
            raise RuntimeError("POSTGRES_URL requires psycopg") from exc
        self._psycopg = psycopg
        self.database_url = database_url
        self._init_db()

    def _connect(self):
        return self._psycopg.connect(self.database_url)

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS billing_credit_balances (
                    user_id TEXT PRIMARY KEY,
                    balance_ucents BIGINT NOT NULL DEFAULT 0,
                    updated_at DOUBLE PRECISION NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS billing_credit_ledger (
                    entry_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    delta_ucents BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    ext_ref TEXT,
                    ts DOUBLE PRECISION NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS billing_credit_idempotency (
                    ext_ref TEXT PRIMARY KEY,
                    entry_id TEXT NOT NULL UNIQUE,
                    created_at DOUBLE PRECISION NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS billing_credit_ledger_user_ts_idx
                ON billing_credit_ledger (user_id, ts DESC)
                """
            )

    @staticmethod
    def _ucents(amount_cents: float) -> int:
        return int(round(float(amount_cents) * UCENTS_PER_CENT))

    @staticmethod
    def _balance(con, user_id: str) -> int:
        row = con.execute(
            "SELECT balance_ucents FROM billing_credit_balances WHERE user_id = %s",
            (user_id,),
        ).fetchone()
        return int(row[0]) if row else 0

    def get_balance(self, user_id: str) -> float:
        with self._connect() as con:
            return self._balance(con, user_id) / UCENTS_PER_CENT

    def credit(
        self,
        user_id: str,
        amount_cents: float,
        reason: str = "topup",
        ext_ref: Optional[str] = None,
    ) -> float:
        balance, _ = self.credit_with_result(user_id, amount_cents, reason, ext_ref)
        return balance

    def credit_with_result(
        self,
        user_id: str,
        amount_cents: float,
        reason: str = "topup",
        ext_ref: Optional[str] = None,
    ) -> tuple[float, bool]:
        """Return (balance_cents, applied), atomically deduplicated on ext_ref."""
        ucents = self._ucents(amount_cents)
        if ucents <= 0:
            return self.get_balance(user_id), False
        entry_id = "cl_" + secrets.token_hex(10)
        now = time.time()
        with self._connect() as con:
            if ext_ref:
                claimed = con.execute(
                    """
                    INSERT INTO billing_credit_idempotency (ext_ref, entry_id, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ext_ref) DO NOTHING
                    RETURNING entry_id
                    """,
                    (ext_ref, entry_id, now),
                ).fetchone()
                if not claimed:
                    return self._balance(con, user_id) / UCENTS_PER_CENT, False
            con.execute(
                """
                INSERT INTO billing_credit_ledger
                    (entry_id, user_id, delta_ucents, reason, ext_ref, ts)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (entry_id, user_id, ucents, reason, ext_ref, now),
            )
            row = con.execute(
                """
                INSERT INTO billing_credit_balances (user_id, balance_ucents, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    balance_ucents = billing_credit_balances.balance_ucents
                        + EXCLUDED.balance_ucents,
                    updated_at = EXCLUDED.updated_at
                RETURNING balance_ucents
                """,
                (user_id, ucents, now),
            ).fetchone()
            return int(row[0]) / UCENTS_PER_CENT, True

    def debit(
        self,
        user_id: str,
        amount_cents: float,
        kind: str,
        ref: str = "",
    ) -> bool:
        ucents = self._ucents(amount_cents)
        if ucents <= 0:
            return True
        now = time.time()
        with self._connect() as con:
            row = con.execute(
                """
                UPDATE billing_credit_balances
                SET balance_ucents = balance_ucents - %s, updated_at = %s
                WHERE user_id = %s AND balance_ucents >= %s
                RETURNING balance_ucents
                """,
                (ucents, now, user_id, ucents),
            ).fetchone()
            if not row:
                return False
            con.execute(
                """
                INSERT INTO billing_credit_ledger
                    (entry_id, user_id, delta_ucents, reason, ext_ref, ts)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                ("cl_" + secrets.token_hex(10), user_id, -ucents, "debit:" + kind, ref, now),
            )
            return True
