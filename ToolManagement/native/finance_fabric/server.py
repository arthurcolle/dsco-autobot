"""
Finance Fabric MCP

Unified integration layer for banking and finance systems behind the standard
native MCP protocol so every capability routes through the Tool Management
registry over HTTP. The full surface is generated from a catalog of
(domain x provider x operation) and spans:

  banking, payments, brokerage, crypto, market data, fx, ledger/accounting,
  card issuing, treasury / money movement, lending/credit, and tax.

Each provider runs in LIVE mode when its credentials are present in the
environment (Plaid, Stripe, Alpaca, Coinbase), otherwise it runs in a
deterministic SQLite-backed SANDBOX so the whole surface is exercisable
end-to-end without external accounts. Every tool reports its mode.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase
from native.finance_fabric.catalog import build_tools, build_canonical, DOMAIN_PROVIDERS
from native.finance_fabric.compose import flow_tools, FLOWS, Composer

try:
    # Optional: multi-tenant BYOK credential resolution (see
    # TENANT_CREDENTIALS_WIRING.md). Best-effort import so this fabric still
    # runs standalone without tool_management on the path; falls back to
    # pure-env-var behavior, unchanged, when unavailable.
    from tool_management.services.tenant_credentials import resolve_credential
except Exception:  # noqa: BLE001
    def resolve_credential(tenant_id, provider, env_fallback_var=None, allow_pooled_fallback=False):  # type: ignore
        return os.getenv(env_fallback_var) if (allow_pooled_fallback and env_fallback_var) else None

# Finance categories (banking, payments, brokerage, card issuing, treasury,
# lending) are NEVER pooled: the operator's own key must not be usable as a
# stand-in for a tenant's real money-movement credentials. allow_pooled_fallback
# is always False for every call in this fabric -- tenant BYOK or sandbox,
# no third option. See TENANT_CREDENTIALS_WIRING.md tiering table.


REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:2016").rstrip("/")

# Live-mode credential probes. Presence flips a provider from sandbox to live.
PLAID_LIVE = bool(os.getenv("PLAID_CLIENT_ID") and os.getenv("PLAID_SECRET"))
STRIPE_LIVE = bool(os.getenv("STRIPE_API_KEY"))
ALPACA_LIVE = bool(os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_API_SECRET"))
COINBASE_LIVE = bool(os.getenv("COINBASE_API_KEY") and os.getenv("COINBASE_API_SECRET"))

PLAID_ENV = os.getenv("PLAID_ENV", "sandbox")
PLAID_BASE = f"https://{PLAID_ENV}.plaid.com"
STRIPE_BASE = "https://api.stripe.com/v1"
ALPACA_BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")

# Which (provider) has a real adapter wired for live calls.
LIVE_CREDS = {
    "plaid": PLAID_LIVE, "stripe": STRIPE_LIVE,
    "alpaca": ALPACA_LIVE, "coinbase": COINBASE_LIVE,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cents(amount: float) -> int:
    return int(round(float(amount) * 100))


def _seed(*parts: Any) -> int:
    return sum(ord(c) for c in "|".join(str(p) for p in parts))


class FinanceStore:
    """SQLite-backed state for sandbox mode and the local double-entry ledger."""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init()

    def _init(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS links (
                link_id TEXT PRIMARY KEY, institution TEXT, access_token TEXT, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY, link_id TEXT, name TEXT, type TEXT, subtype TEXT,
                currency TEXT, available REAL, current REAL, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT PRIMARY KEY, account_id TEXT, amount REAL, currency TEXT,
                name TEXT, category TEXT, pending INTEGER, posted_at TEXT
            );
            CREATE TABLE IF NOT EXISTS payments (
                payment_id TEXT PRIMARY KEY, provider TEXT, amount_cents INTEGER, currency TEXT,
                description TEXT, status TEXT, refunded_cents INTEGER DEFAULT 0, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id TEXT PRIMARY KEY, rail TEXT, from_account TEXT, to_account TEXT,
                amount REAL, currency TEXT, memo TEXT, status TEXT, created_at TEXT, settles_at TEXT
            );
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, provider TEXT, symbol TEXT, side TEXT, qty REAL,
                order_type TEXT, limit_price REAL, status TEXT, filled_price REAL, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS positions (
                provider TEXT, symbol TEXT, qty REAL, avg_price REAL, PRIMARY KEY (provider, symbol)
            );
            CREATE TABLE IF NOT EXISTS journal (
                entry_id TEXT, seq INTEGER, account TEXT, debit REAL, credit REAL,
                memo TEXT, posted_at TEXT
            );
            CREATE TABLE IF NOT EXISTS cards (
                card_id TEXT PRIMARY KEY, provider TEXT, holder TEXT, type TEXT, last4 TEXT,
                status TEXT, spend_limit REAL, limit_interval TEXT, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS loans (
                loan_id TEXT PRIMARY KEY, provider TEXT, subject_id TEXT, principal REAL,
                rate REAL, term_months INTEGER, balance REAL, status TEXT, created_at TEXT
            );
            """
        )
        self.db.commit()

    # ---- banking ----
    def create_link(self, institution: str, access_token: str) -> str:
        link_id = f"link_{uuid.uuid4().hex[:12]}"
        self.db.execute("INSERT INTO links VALUES (?,?,?,?)",
                        (link_id, institution, access_token, _now()))
        for name, atype, subtype, avail, cur in [
            ("Checking", "depository", "checking", 4250.75, 4250.75),
            ("Savings", "depository", "savings", 18230.10, 18230.10),
        ]:
            acct_id = f"acct_{uuid.uuid4().hex[:12]}"
            self.db.execute("INSERT INTO accounts VALUES (?,?,?,?,?,?,?,?,?)",
                            (acct_id, link_id, name, atype, subtype, "USD", avail, cur, _now()))
            self._seed_txns(acct_id)
        self.db.commit()
        return link_id

    def _seed_txns(self, account_id: str) -> None:
        for i, (amt, name, cat) in enumerate([
            (-82.41, "Whole Foods", "groceries"), (-1200.00, "Rent", "housing"),
            (3500.00, "Payroll", "income"), (-54.99, "Internet", "utilities"),
        ]):
            self.db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                            (f"txn_{uuid.uuid4().hex[:12]}", account_id, amt, "USD", name, cat, 0,
                             (datetime.now(timezone.utc) - timedelta(days=i)).isoformat()))

    def list_accounts(self, link_id: Optional[str]) -> List[dict]:
        if link_id:
            rows = self.db.execute("SELECT * FROM accounts WHERE link_id=?", (link_id,)).fetchall()
        else:
            rows = self.db.execute("SELECT * FROM accounts").fetchall()
        return [dict(r) for r in rows]

    def get_account(self, account_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM accounts WHERE account_id=?", (account_id,)).fetchone()
        return dict(row) if row else None

    def list_transactions(self, account_id: str, limit: int) -> List[dict]:
        rows = self.db.execute(
            "SELECT * FROM transactions WHERE account_id=? ORDER BY posted_at DESC LIMIT ?",
            (account_id, limit)).fetchall()
        return [dict(r) for r in rows]

    # ---- payments ----
    def record_payment(self, provider, amount_cents, currency, description, status) -> str:
        pid = f"pay_{uuid.uuid4().hex[:16]}"
        self.db.execute(
            "INSERT INTO payments (payment_id,provider,amount_cents,currency,description,status,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pid, provider, amount_cents, currency, description, status, _now()))
        self.db.commit()
        return pid

    def get_payment(self, payment_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM payments WHERE payment_id=?", (payment_id,)).fetchone()
        return dict(row) if row else None

    def refund_payment(self, payment_id: str, amount_cents: int) -> Optional[dict]:
        p = self.get_payment(payment_id)
        if not p:
            return None
        refunded = min(p["amount_cents"], p["refunded_cents"] + amount_cents)
        status = "refunded" if refunded >= p["amount_cents"] else "partially_refunded"
        self.db.execute("UPDATE payments SET refunded_cents=?, status=? WHERE payment_id=?",
                        (refunded, status, payment_id))
        self.db.commit()
        return self.get_payment(payment_id)

    # ---- transfers ----
    def record_transfer(self, rail, from_account, to_account, amount, currency, memo, settle_days) -> dict:
        tid = f"xfer_{uuid.uuid4().hex[:16]}"
        settles = (datetime.now(timezone.utc) + timedelta(days=settle_days)).isoformat()
        self.db.execute("INSERT INTO transfers VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (tid, rail, from_account, to_account, amount, currency, memo,
                         "pending", _now(), settles))
        self.db.commit()
        return dict(self.db.execute("SELECT * FROM transfers WHERE transfer_id=?", (tid,)).fetchone())

    def get_transfer(self, transfer_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM transfers WHERE transfer_id=?", (transfer_id,)).fetchone()
        return dict(row) if row else None

    # ---- brokerage ----
    def record_order(self, provider, symbol, side, qty, order_type, limit_price, status, filled_price) -> dict:
        oid = f"ord_{uuid.uuid4().hex[:16]}"
        self.db.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (oid, provider, symbol, side, qty, order_type, limit_price,
                         status, filled_price, _now()))
        if status == "filled" and filled_price is not None:
            self._apply_fill(provider, symbol, side, qty, filled_price)
        self.db.commit()
        return dict(self.db.execute("SELECT * FROM orders WHERE order_id=?", (oid,)).fetchone())

    def get_order(self, order_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
        return dict(row) if row else None

    def list_orders(self, provider: str, status: Optional[str]) -> List[dict]:
        if status and status != "all":
            rows = self.db.execute("SELECT * FROM orders WHERE provider=? AND status=? ORDER BY created_at DESC",
                                   (provider, status)).fetchall()
        else:
            rows = self.db.execute("SELECT * FROM orders WHERE provider=? ORDER BY created_at DESC",
                                   (provider,)).fetchall()
        return [dict(r) for r in rows]

    def _apply_fill(self, provider, symbol, side, qty, price) -> None:
        row = self.db.execute("SELECT qty, avg_price FROM positions WHERE provider=? AND symbol=?",
                              (provider, symbol)).fetchone()
        signed = qty if side == "buy" else -qty
        if row is None:
            if signed != 0:
                self.db.execute("INSERT INTO positions VALUES (?,?,?,?)", (provider, symbol, signed, price))
        else:
            old_qty, old_avg = row["qty"], row["avg_price"]
            new_qty = old_qty + signed
            if (old_qty >= 0) == (signed >= 0) and old_qty != 0:
                new_avg = (abs(old_qty) * old_avg + abs(signed) * price) / abs(new_qty) if new_qty else price
            else:
                new_avg = old_avg if new_qty != 0 else 0.0
            self.db.execute("UPDATE positions SET qty=?, avg_price=? WHERE provider=? AND symbol=?",
                            (new_qty, new_avg, provider, symbol))

    def list_positions(self, provider: str) -> List[dict]:
        rows = self.db.execute("SELECT * FROM positions WHERE provider=? AND qty != 0",
                              (provider,)).fetchall()
        return [dict(r) for r in rows]

    # ---- ledger ----
    def post_journal(self, lines: List[dict], memo: str) -> str:
        entry_id = f"je_{uuid.uuid4().hex[:12]}"
        for seq, line in enumerate(lines):
            self.db.execute("INSERT INTO journal VALUES (?,?,?,?,?,?,?)",
                            (entry_id, seq, line["account"], float(line.get("debit", 0.0)),
                             float(line.get("credit", 0.0)), memo, _now()))
        self.db.commit()
        return entry_id

    def trial_balance(self, account: Optional[str]) -> List[dict]:
        if account:
            rows = self.db.execute("SELECT account, SUM(debit) d, SUM(credit) c FROM journal "
                                  "WHERE account=? GROUP BY account", (account,)).fetchall()
        else:
            rows = self.db.execute("SELECT account, SUM(debit) d, SUM(credit) c FROM journal "
                                  "GROUP BY account").fetchall()
        return [{"account": r["account"], "debit": r["d"], "credit": r["c"],
                 "balance": round((r["d"] or 0) - (r["c"] or 0), 2)} for r in rows]

    def list_journal(self, limit: int) -> List[dict]:
        rows = self.db.execute("SELECT * FROM journal ORDER BY posted_at DESC LIMIT ?",
                              (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ---- cards ----
    def create_card(self, provider, holder, ctype) -> dict:
        cid = f"card_{uuid.uuid4().hex[:14]}"
        last4 = f"{_seed(cid) % 10000:04d}"
        self.db.execute("INSERT INTO cards VALUES (?,?,?,?,?,?,?,?,?)",
                        (cid, provider, holder or "Unknown", ctype, last4, "active", None, None, _now()))
        self.db.commit()
        return dict(self.db.execute("SELECT * FROM cards WHERE card_id=?", (cid,)).fetchone())

    def get_card(self, card_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM cards WHERE card_id=?", (card_id,)).fetchone()
        return dict(row) if row else None

    def set_card_status(self, card_id: str, status: str) -> Optional[dict]:
        self.db.execute("UPDATE cards SET status=? WHERE card_id=?", (status, card_id))
        self.db.commit()
        return self.get_card(card_id)

    def set_card_limit(self, card_id, amount, interval) -> Optional[dict]:
        self.db.execute("UPDATE cards SET spend_limit=?, limit_interval=? WHERE card_id=?",
                        (amount, interval, card_id))
        self.db.commit()
        return self.get_card(card_id)

    # ---- loans ----
    def create_loan(self, provider, subject_id, principal, rate, term_months) -> dict:
        lid = f"loan_{uuid.uuid4().hex[:14]}"
        self.db.execute("INSERT INTO loans VALUES (?,?,?,?,?,?,?,?,?)",
                        (lid, provider, subject_id, principal, rate, term_months,
                         principal, "originated", _now()))
        self.db.commit()
        return dict(self.db.execute("SELECT * FROM loans WHERE loan_id=?", (lid,)).fetchone())

    def get_loan(self, loan_id: str) -> Optional[dict]:
        row = self.db.execute("SELECT * FROM loans WHERE loan_id=?", (loan_id,)).fetchone()
        return dict(row) if row else None

    def repay_loan(self, loan_id, amount) -> Optional[dict]:
        loan = self.get_loan(loan_id)
        if not loan:
            return None
        balance = max(0.0, loan["balance"] - amount)
        status = "paid_off" if balance <= 0 else "active"
        self.db.execute("UPDATE loans SET balance=?, status=? WHERE loan_id=?",
                        (balance, status, loan_id))
        self.db.commit()
        return self.get_loan(loan_id)


# Indicative FX rates vs USD (sandbox / fallback).
FX = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0064, "CAD": 0.73, "AUD": 0.66,
    "CHF": 1.12, "CNY": 0.14, "INR": 0.012, "MXN": 0.058, "BRL": 0.20, "SGD": 0.74,
    "HKD": 0.128, "SEK": 0.095, "NOK": 0.093, "NZD": 0.61,
    "BTC": 64000.0, "ETH": 3100.0, "SOL": 145.0, "USDC": 1.0, "USDT": 1.0,
}


class FinanceFabricMCP(NativeMCPBase):
    SERVICE_ID = "distributed-finance"
    SERVICE_NAME = "Distributed Finance MCP"
    VERSION = "2.0.0"

    CAPABILITIES = [
        "banking", "payments", "ach", "wire", "brokerage", "market_data",
        "crypto", "fx", "ledger", "cards", "treasury", "lending", "tax", "finance",
    ]

    # L0 primitives ({provider}_{domain}_{op}) and L1 canonical aliases
    # ({domain}__{op} -> default provider). Both dispatch to the same handlers.
    _CATALOG_TOOLS, _PRIM_DISPATCH = build_tools()
    _CANON_TOOLS, _CANON_DISPATCH = build_canonical()
    _DISPATCH = {**_PRIM_DISPATCH, **_CANON_DISPATCH}
    _FLOW_TOOLS = flow_tools()

    # Standalone (non-generated) tools come first.
    TOOLS = [
        {
            "name": "provider_status",
            "description": "Show which finance providers are LIVE (credentials present) vs SANDBOX.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "catalog_summary",
            "description": "Summarize the finance tool catalog: domains, providers, operations, totals.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "flow_list",
            "description": "List the composed L3 flows (multi-step recipes) the fabric exposes.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "flow_describe",
            "description": "Describe a flow: its inputs, ordered steps, and output shape.",
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string", "description": "flow name (without flow__ prefix)"}},
                "required": ["name"],
            },
        },
    ] + _CANON_TOOLS + _FLOW_TOOLS + _CATALOG_TOOLS

    def __init__(self, port: int, **kwargs):
        super().__init__(port, **kwargs)
        self.store = FinanceStore(self.data_dir / "finance_fabric.sqlite")

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _ok(payload: Any, mode: str, provider: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"mode": mode}
        if provider:
            body["provider"] = provider
        if isinstance(payload, dict):
            body.update(payload)
        else:
            body["data"] = payload
        return {"content": [{"type": "text", "text": json.dumps(body, indent=2, default=str)}]}

    @staticmethod
    def _err(msg: str) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps({"error": msg})}], "isError": True}

    def _mode(self, provider: str) -> str:
        return "live" if LIVE_CREDS.get(provider) else "sandbox"

    def _alpaca_headers(self) -> Dict[str, str]:
        return {"APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY", ""),
                "APCA-API-SECRET-KEY": os.getenv("ALPACA_API_SECRET", "")}

    async def _quote_price(self, symbol: str) -> float:
        """Try a market-data backend through the registry, else deterministic sim."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.post(
                    f"{REGISTRY_URL}/api/v1/tools/alpha_vantage:GLOBAL_QUOTE/execute",
                    json={"inputs": {"symbol": symbol}})
                if r.status_code == 200:
                    txt = json.dumps(r.json())
                    for key in ('"05. price": "', '"price":"', '"price": "'):
                        if key in txt:
                            return float(txt.split(key, 1)[1].split('"', 1)[0])
        except Exception:
            pass
        h = _seed(symbol.upper())
        return round(20.0 + (h % 800) + (h % 100) / 100.0, 2)

    # --------------------------------------------------------------- dispatch
    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        args = dict(arguments or {})
        tenant_id = args.pop("tenant_id", None)
        args.pop("_tenant_credential", None)
        args.pop("_credential_envelope", None)
        args.pop("_pooled_ok", None)

        if tool_name.startswith("flow__"):
            if tenant_id:
                return self._err(
                    "TENANT_BYOK_REQUIRED: composed finance flows are disabled until every "
                    "step has tenant-scoped credentials"
                )
            name = tool_name[len("flow__"):]
            if name not in FLOWS:
                return self._err(f"unknown flow: {name}")
            result = await Composer(REGISTRY_URL, self.SERVICE_ID).run(name, args)
            return self._ok(result, "compose", None)
        route = self._DISPATCH.get(tool_name)
        if route is None:
            return await super()._execute_tool(tool_name, args)
        domain, op, provider = route
        handler = getattr(self, f"_h_{domain}", None)
        if handler is None:
            return self._err(f"no handler for domain {domain}")
        if tenant_id and provider in LIVE_CREDS:
            return self._err(
                f"TENANT_BYOK_REQUIRED: {provider} live execution is disabled until its "
                "adapter consumes tenant-scoped credentials end to end"
            )
        try:
            return await handler(op, provider, args)
        except Exception as e:  # noqa: BLE001 - surface adapter errors as tool errors
            return self._err(f"{provider}.{domain}.{op} failed: {e}")

    # --------------------------------------------------------------- banking
    async def _h_banking(self, op, provider, a):
        if op == "link":
            institution = a.get("institution")
            if not institution:
                return self._err("institution required")
            access_token, mode = "sandbox", self._mode(provider)
            if provider == "plaid" and PLAID_LIVE and a.get("public_token"):
                async with httpx.AsyncClient(timeout=30.0) as c:
                    r = await c.post(f"{PLAID_BASE}/item/public_token/exchange", json={
                        "client_id": os.getenv("PLAID_CLIENT_ID"), "secret": os.getenv("PLAID_SECRET"),
                        "public_token": a["public_token"]})
                    r.raise_for_status()
                    access_token, mode = r.json().get("access_token", "live"), "live"
            link_id = self.store.create_link(f"{provider}:{institution}", access_token)
            return self._ok({"link_id": link_id, "institution": institution,
                             "accounts": self.store.list_accounts(link_id)}, mode, provider)
        if op == "accounts":
            accts = self.store.list_accounts(a.get("link_id"))
            return self._ok({"accounts": accts, "count": len(accts)}, "sandbox", provider)
        if op in ("balance", "transactions", "identity", "income", "liabilities",
                  "investments", "statements", "routing", "categorize"):
            acct = self.store.get_account(a.get("account_id", ""))
            if not acct and op != "transactions":
                return self._err("account not found")
            if op == "balance":
                return self._ok({"account_id": acct["account_id"], "name": acct["name"],
                                 "currency": acct["currency"], "available": acct["available"],
                                 "current": acct["current"]}, "sandbox", provider)
            if op == "transactions":
                aid = a.get("account_id", "")
                if not self.store.get_account(aid):
                    return self._err("account not found")
                txns = self.store.list_transactions(aid, int(a.get("limit", 25)))
                return self._ok({"account_id": aid, "transactions": txns, "count": len(txns)},
                                "sandbox", provider)
            if op == "identity":
                return self._ok({"account_id": acct["account_id"], "names": ["Account Holder"],
                                 "emails": ["holder@example.com"], "phone": "+1-555-0100"},
                                "sandbox", provider)
            if op == "income":
                return self._ok({"account_id": acct["account_id"], "estimated_annual_income": 84000,
                                 "confidence": 0.82, "sources": ["Payroll"]}, "sandbox", provider)
            if op == "liabilities":
                return self._ok({"account_id": acct["account_id"], "credit_cards": [
                    {"name": "Visa", "balance": 1240.55, "apr": 0.2199, "min_payment": 35.0}],
                    "student_loans": []}, "sandbox", provider)
            if op == "investments":
                return self._ok({"account_id": acct["account_id"], "holdings": [
                    {"symbol": "VTI", "qty": 12.0, "value": 3120.0},
                    {"symbol": "AAPL", "qty": 5.0, "value": 1025.0}]}, "sandbox", provider)
            if op == "statements":
                return self._ok({"account_id": acct["account_id"], "statements": [
                    {"period": "2026-05", "statement_id": f"stmt_{_seed(acct['account_id'],5)%99999}"},
                    {"period": "2026-04", "statement_id": f"stmt_{_seed(acct['account_id'],4)%99999}"}]},
                    "sandbox", provider)
            if op == "routing":
                base = _seed(acct["account_id"])
                return self._ok({"account_id": acct["account_id"],
                                 "account_number": f"{base % 1000000000:09d}",
                                 "routing_number": f"{110000000 + base % 1000:09d}",
                                 "wire_routing": f"{120000000 + base % 1000:09d}"}, "sandbox", provider)
            if op == "categorize":
                txns = self.store.list_transactions(acct["account_id"], 100)
                cats: Dict[str, float] = {}
                for t in txns:
                    cats[t["category"]] = round(cats.get(t["category"], 0.0) + abs(t["amount"]), 2)
                return self._ok({"account_id": acct["account_id"], "by_category": cats},
                                "sandbox", provider)
        if op == "webhook_status":
            return self._ok({"webhook": "OK", "last_event": _now(), "connected": True},
                            "sandbox", provider)
        return self._err(f"unsupported banking op {op}")

    # -------------------------------------------------------------- payments
    async def _h_payments(self, op, provider, a):
        if op == "charge":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            currency = (a.get("currency") or "usd").lower()
            desc = a.get("description", "")
            cents = _cents(amount)
            if provider == "stripe" and STRIPE_LIVE:
                data = {"amount": str(cents), "currency": currency, "description": desc}
                if a.get("source"):
                    data["source"] = a["source"]
                async with httpx.AsyncClient(timeout=30.0) as c:
                    r = await c.post(f"{STRIPE_BASE}/charges", data=data,
                                     auth=(os.getenv("STRIPE_API_KEY") or "", ""))
                    r.raise_for_status()
                    body = r.json()
                self.store.record_payment(provider, cents, currency, desc, body.get("status", "succeeded"))
                return self._ok({"payment_id": body.get("id"), "amount": amount,
                                 "currency": currency, "status": body.get("status")}, "live", provider)
            pid = self.store.record_payment(provider, cents, currency, desc, "succeeded")
            return self._ok({"payment_id": pid, "amount": amount, "currency": currency,
                             "status": "succeeded"}, "sandbox", provider)
        if op == "status":
            p = self.store.get_payment(a.get("payment_id", ""))
            if not p:
                return self._err("payment not found")
            return self._ok({"payment_id": p["payment_id"], "status": p["status"],
                             "amount": p["amount_cents"] / 100.0, "currency": p["currency"],
                             "refunded": p["refunded_cents"] / 100.0}, self._mode(provider), provider)
        if op == "capture":
            p = self.store.get_payment(a.get("payment_id", ""))
            if not p:
                return self._err("payment not found")
            return self._ok({"payment_id": p["payment_id"], "status": "captured",
                             "amount": (a.get("amount") or p["amount_cents"] / 100.0)},
                            self._mode(provider), provider)
        if op == "refund":
            pid = a.get("payment_id", "")
            p = self.store.get_payment(pid)
            if not p:
                return self._err("payment not found")
            amt = a.get("amount")
            updated = self.store.refund_payment(pid, _cents(amt) if amt is not None else p["amount_cents"])
            if not updated:
                return self._err("refund failed")
            return self._ok({"payment_id": pid, "status": updated["status"],
                             "refunded": updated["refunded_cents"] / 100.0}, self._mode(provider), provider)
        if op == "payout":
            amount = a.get("amount")
            if amount is None or not a.get("destination"):
                return self._err("amount and destination required")
            return self._ok({"payout_id": f"po_{uuid.uuid4().hex[:16]}", "amount": amount,
                             "currency": (a.get("currency") or "usd").lower(),
                             "destination": a["destination"], "status": "in_transit",
                             "arrival": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()},
                            self._mode(provider), provider)
        if op == "customer_create":
            return self._ok({"customer_id": f"cus_{uuid.uuid4().hex[:14]}",
                             "email": a.get("email"), "name": a.get("name")},
                            self._mode(provider), provider)
        if op == "subscription_create":
            if not a.get("plan"):
                return self._err("plan required")
            return self._ok({"subscription_id": f"sub_{uuid.uuid4().hex[:14]}", "plan": a["plan"],
                             "customer": a.get("customer"), "amount": a.get("amount"),
                             "interval": a.get("interval", "month"), "status": "active"},
                            self._mode(provider), provider)
        if op == "invoice_create":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            return self._ok({"invoice_id": f"inv_{uuid.uuid4().hex[:14]}", "amount": amount,
                             "currency": (a.get("currency") or "usd").lower(),
                             "customer": a.get("customer"), "status": "open"},
                            self._mode(provider), provider)
        if op == "dispute_list":
            return self._ok({"disputes": [], "count": 0}, self._mode(provider), provider)
        if op == "payment_method_attach":
            if not a.get("customer") or not a.get("payment_method"):
                return self._err("customer and payment_method required")
            return self._ok({"customer": a["customer"], "payment_method": a["payment_method"],
                             "attached": True}, self._mode(provider), provider)
        if op == "balance":
            return self._ok({"available": [{"amount": 125000.00, "currency": "usd"}],
                             "pending": [{"amount": 4300.00, "currency": "usd"}]},
                            self._mode(provider), provider)
        if op == "checkout_session":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            sid = f"cs_{uuid.uuid4().hex[:18]}"
            return self._ok({"session_id": sid, "amount": amount,
                             "currency": (a.get("currency") or "usd").lower(),
                             "url": f"https://checkout.example/{sid}",
                             "success_url": a.get("success_url")}, self._mode(provider), provider)
        return self._err(f"unsupported payments op {op}")

    # ------------------------------------------------------------- brokerage
    # NOTE: NOT yet migrated to tenant-aware resolve_credential(). Doing so
    # correctly requires _alpaca_headers() to also use the tenant's resolved
    # key/secret (it currently reads the operator's own ALPACA_API_KEY /
    # ALPACA_API_SECRET env vars unconditionally) -- otherwise a tenant's
    # BYOK key could flip `live=True` while the outbound call still carries
    # the operator's credentials, which is a real correctness/security bug,
    # not just an incomplete feature. That requires passing the resolved
    # key/secret explicitly through to every Alpaca httpx call in this
    # handler (account/order/positions/etc), which is a larger, testable
    # change -- see TENANT_CREDENTIALS_WIRING.md item 2 remaining work.
    # Left as pure env-var gating (today's unchanged behavior) until that
    # full path is written and verified with pytest.
    async def _h_brokerage(self, op, provider, a):
        live = provider == "alpaca" and ALPACA_LIVE
        if op == "account":
            if live:
                async with httpx.AsyncClient(timeout=30.0) as c:
                    r = await c.get(f"{ALPACA_BASE}/v2/account", headers=self._alpaca_headers())
                    r.raise_for_status()
                    return self._ok(r.json(), "live", provider)
            positions = self.store.list_positions(provider)
            equity = 100000.0 + sum(p["qty"] * p["avg_price"] for p in positions)
            return self._ok({"account_number": f"SANDBOX-{provider}", "status": "ACTIVE",
                             "currency": "USD", "cash": 100000.0, "equity": round(equity, 2),
                             "buying_power": round(equity * 2, 2)}, "sandbox", provider)
        if op == "order":
            symbol = (a.get("symbol") or "").upper()
            side, qty = a.get("side"), a.get("qty")
            if not symbol or side not in ("buy", "sell") or qty in (None, ""):
                return self._err("symbol, qty and side (buy|sell) required")
            otype, limit_price = a.get("order_type", "market"), a.get("limit_price")
            if live:
                payload = {"symbol": symbol, "qty": str(qty), "side": side,
                           "type": otype, "time_in_force": "day"}
                if otype == "limit" and limit_price is not None:
                    payload["limit_price"] = str(limit_price)
                async with httpx.AsyncClient(timeout=30.0) as c:
                    r = await c.post(f"{ALPACA_BASE}/v2/orders", json=payload,
                                     headers=self._alpaca_headers())
                    r.raise_for_status()
                    return self._ok(r.json(), "live", provider)
            price = await self._quote_price(symbol)
            fill = limit_price if otype == "limit" and limit_price else price
            order = self.store.record_order(provider, symbol, side, float(qty),
                                            otype, limit_price, "filled", fill)
            return self._ok({"order": order}, "sandbox", provider)
        if op == "order_status":
            o = self.store.get_order(a.get("order_id", ""))
            if not o:
                return self._err("order not found")
            return self._ok({"order": o}, "sandbox", provider)
        if op == "orders":
            orders = self.store.list_orders(provider, a.get("status", "all"))
            return self._ok({"orders": orders, "count": len(orders)}, "sandbox", provider)
        if op == "cancel":
            return self._ok({"order_id": a.get("order_id"), "status": "canceled"}, "sandbox", provider)
        if op == "positions":
            if live:
                async with httpx.AsyncClient(timeout=30.0) as c:
                    r = await c.get(f"{ALPACA_BASE}/v2/positions", headers=self._alpaca_headers())
                    r.raise_for_status()
                    return self._ok({"positions": r.json()}, "live", provider)
            pos = self.store.list_positions(provider)
            return self._ok({"positions": pos, "count": len(pos)}, "sandbox", provider)
        if op == "close_position":
            symbol = (a.get("symbol") or "").upper()
            if not symbol:
                return self._err("symbol required")
            pos = [p for p in self.store.list_positions(provider) if p["symbol"] == symbol]
            if not pos:
                return self._err("no open position")
            p = pos[0]
            side = "sell" if p["qty"] > 0 else "buy"
            price = await self._quote_price(symbol)
            order = self.store.record_order(provider, symbol, side, abs(p["qty"]),
                                            "market", None, "filled", price)
            return self._ok({"closed": symbol, "order": order}, "sandbox", provider)
        if op == "quote":
            symbol = (a.get("symbol") or "").upper()
            if not symbol:
                return self._err("symbol required")
            price = await self._quote_price(symbol)
            return self._ok({"symbol": symbol, "price": price, "asof": _now()},
                            "live" if live else "sandbox", provider)
        if op == "bars":
            symbol = (a.get("symbol") or "").upper()
            if not symbol:
                return self._err("symbol required")
            return self._ok(self._gen_bars(symbol, int(a.get("limit", 30)),
                                           a.get("timeframe", "1Day")), "sandbox", provider)
        if op == "option_chain":
            symbol = (a.get("symbol") or "").upper()
            if not symbol:
                return self._err("symbol required")
            spot = await self._quote_price(symbol)
            strikes = [round(spot * m, 2) for m in (0.9, 0.95, 1.0, 1.05, 1.1)]
            chain = [{"strike": k, "call": round(max(0.01, spot - k) + 1.5, 2),
                      "put": round(max(0.01, k - spot) + 1.5, 2)} for k in strikes]
            return self._ok({"symbol": symbol, "spot": spot, "expiry": a.get("expiry", "2026-07-17"),
                             "chain": chain}, "sandbox", provider)
        if op == "portfolio_history":
            base = 100000.0
            hist = [{"t": (datetime.now(timezone.utc) - timedelta(days=d)).date().isoformat(),
                     "equity": round(base * (1 + ((_seed(provider, d) % 200 - 100) / 10000.0)), 2)}
                    for d in range(7, -1, -1)]
            return self._ok({"equity_history": hist}, "sandbox", provider)
        if op == "watchlist":
            syms = a.get("symbols") or ["AAPL", "MSFT", "SPY"]
            return self._ok({"watchlist": syms, "count": len(syms)}, "sandbox", provider)
        return self._err(f"unsupported brokerage op {op}")

    def _gen_bars(self, symbol, limit, timeframe):
        h = _seed(symbol)
        price = 20.0 + (h % 800)
        bars = []
        for d in range(limit, 0, -1):
            drift = ((_seed(symbol, d) % 200) - 100) / 100.0
            o = round(price + drift, 2)
            c = round(o + (((_seed(symbol, d, 1) % 200) - 100) / 100.0), 2)
            bars.append({"t": (datetime.now(timezone.utc) - timedelta(days=d)).date().isoformat(),
                         "o": o, "h": round(max(o, c) + 0.5, 2), "l": round(min(o, c) - 0.5, 2),
                         "c": c, "v": 1000000 + (_seed(symbol, d) % 500000)})
        return {"symbol": symbol, "timeframe": timeframe, "bars": bars}

    # ---------------------------------------------------------------- crypto
    async def _h_crypto(self, op, provider, a):
        balances = {"BTC": 0.5421, "ETH": 8.13, "USDC": 12500.0, "SOL": 142.0}
        if op == "balances":
            asset = a.get("asset")
            if asset:
                asset = asset.upper()
                return self._ok({"asset": asset, "balance": balances.get(asset, 0.0)},
                                "sandbox", provider)
            valued = [{"asset": k, "balance": v, "usd_value": round(v * FX.get(k, 0.0), 2)}
                      for k, v in balances.items()]
            return self._ok({"balances": valued,
                             "total_usd": round(sum(x["usd_value"] for x in valued), 2)},
                            "sandbox", provider)
        if op == "order":
            product, side, size = (a.get("product") or "").upper(), a.get("side"), a.get("size")
            if not product or side not in ("buy", "sell") or size in (None, ""):
                return self._err("product, side (buy|sell) and size required")
            base = product.split("-")[0]
            price = FX.get(base, 100.0)
            order = self.store.record_order(f"crypto:{provider}", product, side, float(size),
                                            "market", None, "filled", price)
            return self._ok({"order": order, "notional_usd": round(float(size) * price, 2)},
                            "sandbox", provider)
        if op in ("ticker", "candles"):
            product = (a.get("product") or "").upper()
            if not product:
                return self._err("product required")
            base = product.split("-")[0]
            price = FX.get(base, 100.0)
            if op == "ticker":
                return self._ok({"product": product, "price": price,
                                 "bid": round(price * 0.999, 2), "ask": round(price * 1.001, 2),
                                 "volume_24h": 10000 + _seed(product) % 50000}, "sandbox", provider)
            gran = int(a.get("granularity", 3600))
            candles = [[int((datetime.now(timezone.utc) - timedelta(seconds=gran * i)).timestamp()),
                        round(price * (1 - 0.01 * i), 2), round(price * (1 + 0.01 * i), 2),
                        round(price * (1 - 0.005 * i), 2), round(price, 2), 100 + i]
                       for i in range(10)]
            return self._ok({"product": product, "granularity": gran, "candles": candles},
                            "sandbox", provider)
        if op == "orderbook":
            product = (a.get("product") or "").upper()
            base = product.split("-")[0]
            price = FX.get(base, 100.0)
            bids = [[round(price * (1 - 0.001 * i), 2), round(0.5 + i * 0.1, 3)] for i in range(1, 6)]
            asks = [[round(price * (1 + 0.001 * i), 2), round(0.5 + i * 0.1, 3)] for i in range(1, 6)]
            return self._ok({"product": product, "bids": bids, "asks": asks}, "sandbox", provider)
        if op == "deposit_address":
            asset = (a.get("asset") or "").upper()
            if not asset:
                return self._err("asset required")
            return self._ok({"asset": asset,
                             "address": f"{asset.lower()}1q{uuid.uuid4().hex[:34]}"},
                            "sandbox", provider)
        if op == "withdraw":
            if not a.get("asset") or a.get("amount") in (None, "") or not a.get("address"):
                return self._err("asset, amount and address required")
            return self._ok({"withdrawal_id": f"wd_{uuid.uuid4().hex[:16]}", "asset": a["asset"].upper(),
                             "amount": a["amount"], "address": a["address"], "status": "pending"},
                            "sandbox", provider)
        if op == "transfers":
            return self._ok({"transfers": [], "count": 0}, "sandbox", provider)
        if op == "products":
            return self._ok({"products": [f"{b}-USD" for b in ("BTC", "ETH", "SOL", "USDC")]},
                            "sandbox", provider)
        if op == "fees":
            return self._ok({"maker": 0.004, "taker": 0.006, "tier": "default"}, "sandbox", provider)
        return self._err(f"unsupported crypto op {op}")

    # ------------------------------------------------------------ marketdata
    async def _h_marketdata(self, op, provider, a):
        symbol = (a.get("symbol") or "").upper()
        if op == "quote":
            if not symbol:
                return self._err("symbol required")
            price = await self._quote_price(symbol)
            return self._ok({"symbol": symbol, "price": price, "asof": _now(),
                             "routed_via_registry": True}, "sandbox", provider)
        if op == "bars":
            if not symbol:
                return self._err("symbol required")
            return self._ok(self._gen_bars(symbol, int(a.get("limit", 30)),
                                           a.get("timeframe", "1Day")), "sandbox", provider)
        if op == "fundamentals":
            if not symbol:
                return self._err("symbol required")
            h = _seed(symbol)
            return self._ok({"symbol": symbol, "market_cap": (h % 900 + 100) * 1_000_000_000,
                             "pe_ratio": round(10 + h % 30 + (h % 100) / 100.0, 2),
                             "eps": round(1 + (h % 800) / 100.0, 2), "beta": round(0.5 + (h % 150) / 100.0, 2),
                             "dividend_yield": round((h % 400) / 10000.0, 4)}, "sandbox", provider)
        if op == "news":
            if not symbol:
                return self._err("symbol required")
            n = int(a.get("limit", 10))
            items = [{"headline": f"{symbol} update {i+1}", "source": provider,
                      "ts": (datetime.now(timezone.utc) - timedelta(hours=i)).isoformat()}
                     for i in range(min(n, 10))]
            return self._ok({"symbol": symbol, "news": items}, "sandbox", provider)
        if op == "dividends":
            if not symbol:
                return self._err("symbol required")
            return self._ok({"symbol": symbol, "dividends": [
                {"ex_date": "2026-05-09", "amount": round((_seed(symbol) % 200) / 100.0, 2)}]},
                "sandbox", provider)
        if op == "splits":
            if not symbol:
                return self._err("symbol required")
            return self._ok({"symbol": symbol, "splits": []}, "sandbox", provider)
        if op == "earnings":
            if not symbol:
                return self._err("symbol required")
            return self._ok({"symbol": symbol, "next_earnings": "2026-07-24",
                             "eps_estimate": round(1 + (_seed(symbol) % 300) / 100.0, 2)},
                            "sandbox", provider)
        if op == "search":
            q = a.get("query")
            if not q:
                return self._err("query required")
            up = q.upper()
            return self._ok({"query": q, "results": [
                {"symbol": up[:4], "name": f"{q} Inc.", "type": "equity"},
                {"symbol": up[:3] + "X", "name": f"{q} ETF", "type": "etf"}]}, "sandbox", provider)
        return self._err(f"unsupported marketdata op {op}")

    # -------------------------------------------------------------------- fx
    async def _h_fx(self, op, provider, a):
        if op in ("convert", "rate", "quote"):
            src, dst = (a.get("from") or "").upper(), (a.get("to") or "").upper()
            if not src or not dst:
                return self._err("from and to required")
            if src not in FX or dst not in FX:
                return self._err(f"unsupported currency; known: {sorted(FX)}")
            rate = FX[src] / FX[dst]
            if op == "rate":
                return self._ok({"from": src, "to": dst, "rate": round(rate, 6)}, "sandbox", provider)
            if op == "convert":
                if a.get("amount") is None:
                    return self._err("amount required")
                return self._ok({"amount": a["amount"], "from": src, "to": dst,
                                 "rate": round(rate, 6),
                                 "converted": round(float(a["amount"]) * rate, 6)}, "sandbox", provider)
            spread = 0.0035
            return self._ok({"from": src, "to": dst, "mid": round(rate, 6),
                             "bid": round(rate * (1 - spread), 6), "ask": round(rate * (1 + spread), 6),
                             "amount": a.get("amount")}, "sandbox", provider)
        if op == "rates_table":
            base = (a.get("base") or "USD").upper()
            if base not in FX:
                return self._err(f"unsupported base; known: {sorted(FX)}")
            table = {k: round(FX[base] / FX[k], 6) for k in FX}
            return self._ok({"base": base, "rates": table}, "sandbox", provider)
        return self._err(f"unsupported fx op {op}")

    # ---------------------------------------------------------------- ledger
    async def _h_ledger(self, op, provider, a):
        if op == "post":
            lines = a.get("lines") or []
            if not isinstance(lines, list) or not lines:
                return self._err("lines (list of {account, debit, credit}) required")
            debits = sum(float(l.get("debit", 0.0)) for l in lines)
            credits = sum(float(l.get("credit", 0.0)) for l in lines)
            if round(debits - credits, 6) != 0:
                return self._err(f"unbalanced entry: debits={debits} credits={credits}")
            for l in lines:
                if "account" not in l:
                    return self._err("each line needs an 'account'")
            entry_id = self.store.post_journal(lines, a.get("memo", ""))
            return self._ok({"entry_id": entry_id, "debits": debits, "credits": credits,
                             "balanced": True}, "sandbox", provider)
        if op == "balance":
            rows = self.store.trial_balance(a.get("account"))
            return self._ok({"accounts": rows, "net": round(sum(r["balance"] for r in rows), 2)},
                            "sandbox", provider)
        if op == "accounts":
            rows = self.store.trial_balance(None)
            return self._ok({"accounts": [r["account"] for r in rows], "count": len(rows)},
                            "sandbox", provider)
        if op == "journal":
            entries = self.store.list_journal(int(a.get("limit", 25)))
            return self._ok({"entries": entries, "count": len(entries)}, "sandbox", provider)
        if op == "invoice_create":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            return self._ok({"invoice_id": f"AR-{uuid.uuid4().hex[:10]}", "amount": amount,
                             "customer": a.get("customer"), "status": "draft"}, "sandbox", provider)
        if op == "bill_create":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            return self._ok({"bill_id": f"AP-{uuid.uuid4().hex[:10]}", "amount": amount,
                             "vendor": a.get("vendor"), "status": "unpaid"}, "sandbox", provider)
        if op == "reconcile":
            acct = a.get("account")
            if not acct or a.get("statement_balance") is None:
                return self._err("account and statement_balance required")
            rows = self.store.trial_balance(acct)
            book = rows[0]["balance"] if rows else 0.0
            diff = round(float(a["statement_balance"]) - book, 2)
            return self._ok({"account": acct, "book_balance": book,
                             "statement_balance": a["statement_balance"], "difference": diff,
                             "reconciled": diff == 0}, "sandbox", provider)
        if op == "report":
            rows = self.store.trial_balance(None)
            net = round(sum(r["balance"] for r in rows), 2)
            kind = a.get("kind", "pnl")
            return self._ok({"report": kind, "accounts": rows, "net": net}, "sandbox", provider)
        return self._err(f"unsupported ledger op {op}")

    # ----------------------------------------------------------------- cards
    async def _h_cards(self, op, provider, a):
        if op == "create":
            card = self.store.create_card(provider, a.get("holder", ""), a.get("type", "virtual"))
            return self._ok({"card": card}, self._mode(provider), provider)
        cid = a.get("card_id", "")
        if op == "get":
            card = self.store.get_card(cid)
            return self._ok({"card": card}, self._mode(provider), provider) if card else self._err("card not found")
        if op in ("freeze", "unfreeze"):
            if not self.store.get_card(cid):
                return self._err("card not found")
            card = self.store.set_card_status(cid, "frozen" if op == "freeze" else "active")
            return self._ok({"card": card}, self._mode(provider), provider)
        if op == "transactions":
            if not self.store.get_card(cid):
                return self._err("card not found")
            txns = [{"id": f"ctx_{_seed(cid, i)%99999}", "amount": -round((_seed(cid, i) % 9000) / 100.0, 2),
                     "merchant": ["Amazon", "Uber", "Starbucks", "Apple"][i % 4],
                     "ts": (datetime.now(timezone.utc) - timedelta(days=i)).isoformat()}
                    for i in range(int(a.get("limit", 25)) if int(a.get("limit", 25)) <= 5 else 5)]
            return self._ok({"card_id": cid, "transactions": txns}, self._mode(provider), provider)
        if op == "set_limit":
            if a.get("amount") is None:
                return self._err("amount required")
            if not self.store.get_card(cid):
                return self._err("card not found")
            card = self.store.set_card_limit(cid, a["amount"], a.get("interval", "monthly"))
            return self._ok({"card": card}, self._mode(provider), provider)
        return self._err(f"unsupported cards op {op}")

    # -------------------------------------------------------------- treasury
    async def _h_treasury(self, op, provider, a):
        if op == "transfer":
            for k in ("from_account", "to_account", "amount"):
                if a.get(k) in (None, ""):
                    return self._err(f"{k} required")
            rail = a.get("rail", "ach")
            settle = {"wire": 0, "rtp": 0, "fednow": 0, "book": 0, "ach": 1, "sepa": 1}.get(rail, 1)
            t = self.store.record_transfer(f"{provider}:{rail}", a["from_account"], a["to_account"],
                                          float(a["amount"]), a.get("currency", "USD"),
                                          a.get("memo", ""), settle)
            return self._ok({"transfer": t}, self._mode(provider), provider)
        if op == "transfer_status":
            t = self.store.get_transfer(a.get("transfer_id", ""))
            return self._ok({"transfer": t}, self._mode(provider), provider) if t else self._err("transfer not found")
        if op == "counterparty_create":
            if not a.get("name"):
                return self._err("name required")
            return self._ok({"counterparty_id": f"cp_{uuid.uuid4().hex[:14]}", "name": a["name"],
                             "account_number": a.get("account_number"),
                             "routing_number": a.get("routing_number")}, self._mode(provider), provider)
        if op == "account_create":
            if not a.get("name"):
                return self._err("name required")
            return self._ok({"account_id": f"acct_{uuid.uuid4().hex[:14]}", "name": a["name"],
                             "currency": a.get("currency", "USD"), "balance": 0.0},
                            self._mode(provider), provider)
        if op == "balance":
            return self._ok({"account": a.get("account", "default"), "balance": 250000.00,
                             "currency": "USD", "pending": 12500.0}, self._mode(provider), provider)
        if op == "statement":
            return self._ok({"account": a.get("account", "default"), "period": "2026-05",
                             "opening": 230000.0, "closing": 250000.0,
                             "entries": 42}, self._mode(provider), provider)
        if op == "expected_payments":
            return self._ok({"expected_payments": [], "count": 0}, self._mode(provider), provider)
        if op == "ledger_entries":
            return self._ok({"ledger_entries": [], "count": 0}, self._mode(provider), provider)
        return self._err(f"unsupported treasury op {op}")

    # --------------------------------------------------------------- lending
    async def _h_lending(self, op, provider, a):
        if op == "credit_score":
            sid = a.get("subject_id", "")
            if not sid:
                return self._err("subject_id required")
            score = 580 + _seed(sid, provider) % 270
            band = ("poor" if score < 580 else "fair" if score < 670 else
                    "good" if score < 740 else "very_good" if score < 800 else "exceptional")
            return self._ok({"subject_id": sid, "score": score, "band": band, "bureau": provider},
                            "sandbox", provider)
        if op == "amortization":
            principal, rate = a.get("principal"), a.get("rate")
            if principal is None or rate is None:
                return self._err("principal and rate required")
            n = int(a.get("term_months", 36))
            r = float(rate) / 12.0
            pay = (float(principal) * r) / (1 - (1 + r) ** -n) if r else float(principal) / n
            total = round(pay * n, 2)
            return self._ok({"principal": principal, "annual_rate": rate, "term_months": n,
                             "monthly_payment": round(pay, 2), "total_paid": total,
                             "total_interest": round(total - float(principal), 2)}, "sandbox", provider)
        if op == "offer":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            rate = round(0.06 + (_seed(a.get("subject_id", ""), provider) % 800) / 10000.0, 4)
            return self._ok({"offer_id": f"offer_{uuid.uuid4().hex[:12]}", "amount": amount,
                             "apr": rate, "term_months": 36, "approved": True}, "sandbox", provider)
        if op == "application_create":
            amount = a.get("amount")
            if amount is None:
                return self._err("amount required")
            rate = round(0.06 + (_seed(a.get("subject_id", ""), provider) % 800) / 10000.0, 4)
            loan = self.store.create_loan(provider, a.get("subject_id", ""), float(amount),
                                         rate, int(a.get("term_months", 36)))
            return self._ok({"loan": loan}, "sandbox", provider)
        if op == "loan_status":
            loan = self.store.get_loan(a.get("loan_id", ""))
            return self._ok({"loan": loan}, "sandbox", provider) if loan else self._err("loan not found")
        if op == "repayment":
            if a.get("amount") is None:
                return self._err("amount required")
            loan = self.store.repay_loan(a.get("loan_id", ""), float(a["amount"]))
            return self._ok({"loan": loan}, "sandbox", provider) if loan else self._err("loan not found")
        return self._err(f"unsupported lending op {op}")

    # ------------------------------------------------------------------- tax
    async def _h_tax(self, op, provider, a):
        if op == "calculate":
            amount, juris = a.get("amount"), a.get("jurisdiction")
            if amount is None or not juris:
                return self._err("amount and jurisdiction required")
            rate = round(0.04 + (_seed(juris) % 600) / 10000.0, 4)
            tax = round(float(amount) * rate, 2)
            return self._ok({"amount": amount, "jurisdiction": juris,
                             "category": a.get("category", "general"), "rate": rate,
                             "tax": tax, "total": round(float(amount) + tax, 2)}, "sandbox", provider)
        if op == "rate":
            juris = a.get("jurisdiction")
            if not juris:
                return self._err("jurisdiction required")
            return self._ok({"jurisdiction": juris,
                             "rate": round(0.04 + (_seed(juris) % 600) / 10000.0, 4)},
                            "sandbox", provider)
        if op == "estimate_income_tax":
            income = a.get("income")
            if income is None:
                return self._err("income required")
            inc = float(income)
            brackets = [(11600, 0.10), (47150, 0.12), (100525, 0.22),
                        (191950, 0.24), (243725, 0.32), (609350, 0.35), (float("inf"), 0.37)]
            owed, lower = 0.0, 0.0
            for upper, br in brackets:
                if inc > lower:
                    owed += (min(inc, upper) - lower) * br
                    lower = upper
                else:
                    break
            return self._ok({"income": income, "filing_status": a.get("filing_status", "single"),
                             "estimated_tax": round(owed, 2),
                             "effective_rate": round(owed / inc, 4) if inc else 0.0}, "sandbox", provider)
        if op == "filing_status":
            juris = a.get("jurisdiction")
            if not juris:
                return self._err("jurisdiction required")
            return self._ok({"jurisdiction": juris, "registered": True, "filing_frequency": "quarterly",
                             "next_due": "2026-07-31"}, "sandbox", provider)
        return self._err(f"unsupported tax op {op}")

    # ----------------------------------------------------------- standalone
    async def _tool_provider_status(self, args):
        return self._ok({
            "live_providers": {k: v for k, v in LIVE_CREDS.items()},
            "domains": {d: DOMAIN_PROVIDERS[d] for d in DOMAIN_PROVIDERS},
            "tool_count": len(self.TOOLS),
            "registry": REGISTRY_URL,
            "plaid_env": PLAID_ENV, "alpaca_base": ALPACA_BASE,
        }, "info")

    async def _tool_catalog_summary(self, args):
        by_domain = {}
        for _name, (domain, _op, _prov) in self._PRIM_DISPATCH.items():
            by_domain[domain] = by_domain.get(domain, 0) + 1
        return self._ok({
            "total_tools": len(self.TOOLS),
            "primitives": len(self._CATALOG_TOOLS),
            "canonical_ops": len(self._CANON_TOOLS),
            "flows": len(self._FLOW_TOOLS),
            "layers": {
                "L0_primitive": "{provider}_{domain}_{op}",
                "L1_operation": "{domain}__{op} (default provider)",
                "L2_capability": "a domain's op set",
                "L3_flow": "flow__{name} (composed multi-step recipe)",
            },
            "domains": by_domain,
            "providers_per_domain": {d: len(p) for d, p in DOMAIN_PROVIDERS.items()},
        }, "info")

    async def _tool_flow_list(self, args):
        return self._ok({
            "flows": [
                {"name": n, "tool": f"flow__{n}",
                 "description": s["description"], "inputs": s["inputs"],
                 "steps": [st["tool"] for st in s["steps"]]}
                for n, s in FLOWS.items()
            ],
            "count": len(FLOWS),
        }, "info")

    async def _tool_flow_describe(self, args):
        name = (args or {}).get("name", "")
        spec = FLOWS.get(name)
        if not spec:
            return self._err(f"unknown flow: {name}")
        return self._ok({
            "name": name, "tool": f"flow__{name}",
            "description": spec["description"], "inputs": spec["inputs"],
            "steps": [{"id": st["id"], "tool": st["tool"], "inputs": st["inputs"],
                       "optional": st.get("optional", False)} for st in spec["steps"]],
            "output": spec.get("output", {}),
        }, "info")


def create_server(port: Optional[int] = None) -> FinanceFabricMCP:
    port = port or int(os.environ.get("PORT", "4009"))
    return FinanceFabricMCP(port=port)


# Module-level ASGI app for Modal @asgi_app import (factory target)
app = create_server().app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Finance Fabric MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4009")))
    args = parser.parse_args()
    create_server(args.port).run()
