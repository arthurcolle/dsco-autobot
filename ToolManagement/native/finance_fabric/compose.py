"""
Finance Fabric composition layer (L3 Flows).

A Flow is a named, provider-agnostic recipe that chains canonical operations
(L1, ``{domain}__{op}``) — or any registered fabric tool — into a higher-order
capability. Each step executes THROUGH the Tool Management registry over HTTP,
so a flow can compose anything in the fabric, not just finance tools.

Step inputs are templated. A value of the exact form ``{{path}}`` is resolved
to the referenced object (type-preserving); embedded ``{{path}}`` inside a
larger string is string-substituted. Paths address two roots:

  - ``input.<name>``                 a flow-level input
  - ``steps.<step_id>.<dotted.path>`` a prior step's parsed JSON output
                                      (numeric path segments index lists)

Flows are themselves exposed as tools (``flow__<name>``) and return a
``result + receipt`` envelope, mirroring the Unit Fabric ``execute`` contract so
flows nest inside agents / teams / orgs.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import httpx


_TEMPLATE_FULL = re.compile(r"^\{\{\s*([^}]+?)\s*\}\}$")
_TEMPLATE_EMB = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")


# A Flow step: id, the tool to call, and templated inputs. ``optional`` steps
# may fail without aborting the flow.
def _step(sid: str, tool: str, inputs: Dict[str, Any], optional: bool = False) -> Dict[str, Any]:
    return {"id": sid, "tool": tool, "inputs": inputs, "optional": optional}


# name -> {description, inputs:[...], steps:[...], output: template}
FLOWS: Dict[str, Dict[str, Any]] = {
    "kyc_onboard": {
        "description": "Link a bank, pull identity + verified income, and score the subject.",
        "inputs": ["institution", "subject_id"],
        "steps": [
            _step("link", "banking__link", {"institution": "{{input.institution}}"}),
            _step("identity", "banking__identity",
                  {"account_id": "{{steps.link.accounts.0.account_id}}"}),
            _step("income", "banking__income",
                  {"account_id": "{{steps.link.accounts.0.account_id}}"}),
            _step("score", "lending__credit_score", {"subject_id": "{{input.subject_id}}"}),
        ],
        "output": {
            "link_id": "{{steps.link.link_id}}",
            "identity": "{{steps.identity}}",
            "annual_income": "{{steps.income.estimated_annual_income}}",
            "credit_score": "{{steps.score.score}}",
            "band": "{{steps.score.band}}",
        },
    },
    "invoice_to_cash": {
        "description": "Raise an invoice, open a checkout session, and post the AR journal entry.",
        "inputs": ["amount", "customer"],
        "steps": [
            _step("invoice", "ledger__invoice_create",
                  {"amount": "{{input.amount}}", "customer": "{{input.customer}}"}),
            _step("checkout", "payments__checkout_session", {"amount": "{{input.amount}}"}),
            _step("post", "ledger__post", {
                "lines": [
                    {"account": "accounts_receivable", "debit": "{{input.amount}}"},
                    {"account": "revenue", "credit": "{{input.amount}}"},
                ],
                "memo": "invoice {{steps.invoice.invoice_id}}",
            }),
        ],
        "output": {
            "invoice_id": "{{steps.invoice.invoice_id}}",
            "checkout_url": "{{steps.checkout.url}}",
            "journal_entry": "{{steps.post.entry_id}}",
        },
    },
    "cash_concentration": {
        "description": "Link an institution and sweep the checking balance to a concentration account.",
        "inputs": ["institution", "concentration_account"],
        "steps": [
            _step("link", "banking__link", {"institution": "{{input.institution}}"}),
            _step("balance", "banking__balance",
                  {"account_id": "{{steps.link.accounts.0.account_id}}"}),
            _step("sweep", "treasury__transfer", {
                "rail": "rtp",
                "from_account": "{{steps.link.accounts.0.account_id}}",
                "to_account": "{{input.concentration_account}}",
                "amount": "{{steps.balance.available}}",
                "memo": "cash concentration sweep",
            }),
            _step("post", "ledger__post", {
                "lines": [
                    {"account": "concentration_cash", "debit": "{{steps.balance.available}}"},
                    {"account": "operating_cash", "credit": "{{steps.balance.available}}"},
                ],
                "memo": "sweep {{steps.sweep.transfer.transfer_id}}",
            }),
        ],
        "output": {
            "swept": "{{steps.balance.available}}",
            "transfer_id": "{{steps.sweep.transfer.transfer_id}}",
        },
    },
    "card_issue_and_fund": {
        "description": "Issue a card, set its spend limit, and fund it from an account.",
        "inputs": ["holder", "limit", "funding_account"],
        "steps": [
            _step("card", "cards__create", {"holder": "{{input.holder}}"}),
            _step("limit", "cards__set_limit",
                  {"card_id": "{{steps.card.card.card_id}}", "amount": "{{input.limit}}"}),
            _step("fund", "treasury__transfer", {
                "rail": "book",
                "from_account": "{{input.funding_account}}",
                "to_account": "{{steps.card.card.card_id}}",
                "amount": "{{input.limit}}",
                "memo": "card funding",
            }),
        ],
        "output": {
            "card_id": "{{steps.card.card.card_id}}",
            "last4": "{{steps.card.card.last4}}",
            "limit": "{{input.limit}}",
            "funding_transfer": "{{steps.fund.transfer.transfer_id}}",
        },
    },
    "loan_origination": {
        "description": "Score, pre-qualify, originate, disburse, and book a loan.",
        "inputs": ["subject_id", "amount", "funding_account"],
        "steps": [
            _step("score", "lending__credit_score", {"subject_id": "{{input.subject_id}}"}),
            _step("offer", "lending__offer",
                  {"subject_id": "{{input.subject_id}}", "amount": "{{input.amount}}"}),
            _step("application", "lending__application_create",
                  {"subject_id": "{{input.subject_id}}", "amount": "{{input.amount}}"}),
            _step("disburse", "treasury__transfer", {
                "rail": "ach",
                "from_account": "{{input.funding_account}}",
                "to_account": "{{input.subject_id}}",
                "amount": "{{input.amount}}",
                "memo": "loan disbursement",
            }),
            _step("post", "ledger__post", {
                "lines": [
                    {"account": "loans_receivable", "debit": "{{input.amount}}"},
                    {"account": "cash", "credit": "{{input.amount}}"},
                ],
                "memo": "loan {{steps.application.loan.loan_id}}",
            }),
        ],
        "output": {
            "credit_score": "{{steps.score.score}}",
            "apr": "{{steps.offer.apr}}",
            "loan_id": "{{steps.application.loan.loan_id}}",
            "disbursement": "{{steps.disburse.transfer.transfer_id}}",
        },
    },
    "fx_settlement": {
        "description": "Quote an FX pair, convert, and book the realized conversion.",
        "inputs": ["amount", "from", "to"],
        "steps": [
            _step("quote", "fx__quote",
                  {"from": "{{input.from}}", "to": "{{input.to}}", "amount": "{{input.amount}}"}),
            _step("convert", "fx__convert",
                  {"amount": "{{input.amount}}", "from": "{{input.from}}", "to": "{{input.to}}"}),
            _step("post", "ledger__post", {
                "lines": [
                    {"account": "fx_{{input.to}}", "debit": "{{steps.convert.converted}}"},
                    {"account": "fx_{{input.from}}", "credit": "{{steps.convert.converted}}"},
                ],
                "memo": "fx {{input.from}}->{{input.to}} @ {{steps.convert.rate}}",
            }),
        ],
        "output": {
            "rate": "{{steps.convert.rate}}",
            "converted": "{{steps.convert.converted}}",
            "journal_entry": "{{steps.post.entry_id}}",
        },
    },
    "portfolio_rebalance": {
        "description": "Quote a symbol and place a rebalancing order.",
        "inputs": ["symbol", "qty", "side"],
        "steps": [
            _step("quote", "brokerage__quote", {"symbol": "{{input.symbol}}"}),
            _step("order", "brokerage__order", {
                "symbol": "{{input.symbol}}", "qty": "{{input.qty}}", "side": "{{input.side}}",
            }),
        ],
        "output": {
            "price": "{{steps.quote.price}}",
            "order_id": "{{steps.order.order.order_id}}",
            "filled_price": "{{steps.order.order.filled_price}}",
        },
    },
    "month_end_close": {
        "description": "Produce a P&L report and a trial balance for the close.",
        "inputs": [],
        "steps": [
            _step("pnl", "ledger__report", {"kind": "pnl"}),
            _step("trial_balance", "ledger__balance", {}),
        ],
        "output": {
            "net": "{{steps.pnl.net}}",
            "accounts": "{{steps.trial_balance.accounts}}",
        },
    },
}


def flow_tools() -> List[Dict[str, Any]]:
    """Expose each Flow as an MCP tool ``flow__<name>``."""
    tools: List[Dict[str, Any]] = []
    for name, spec in FLOWS.items():
        props = {i: {"type": "string"} for i in spec["inputs"]}
        tools.append({
            "name": f"flow__{name}",
            "description": f"[flow] {spec['description']}",
            "inputSchema": {
                "type": "object",
                "properties": props,
                **({"required": list(spec["inputs"])} if spec["inputs"] else {}),
            },
        })
    return tools


def _lookup(path: str, ctx: Dict[str, Any]) -> Any:
    parts = path.split(".")
    cur: Any = ctx
    for p in parts:
        if cur is None:
            return None
        if isinstance(cur, list):
            if not p.isdigit() or int(p) >= len(cur):
                return None
            cur = cur[int(p)]
        elif isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


def _render(value: Any, ctx: Dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(v, ctx) for v in value]
    if isinstance(value, str):
        full = _TEMPLATE_FULL.match(value)
        if full:
            return _lookup(full.group(1), ctx)
        return _TEMPLATE_EMB.sub(lambda m: str(_lookup(m.group(1), ctx)), value)
    return value


class Composer:
    """Executes Flows by routing each step through the registry over HTTP."""

    def __init__(self, registry_url: str, backend_id: str = "finance-fabric"):
        self.registry_url = registry_url.rstrip("/")
        self.backend_id = backend_id

    def _qualify(self, tool: str) -> str:
        return tool if ":" in tool else f"{self.backend_id}:{tool}"

    async def _call(self, client: httpx.AsyncClient, tool: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        qualified = self._qualify(tool)
        r = await client.post(
            f"{self.registry_url}/api/v1/tools/{qualified}/execute",
            json={"inputs": inputs}, timeout=30.0,
        )
        r.raise_for_status()
        body = r.json()
        status = body.get("status")
        parsed: Any = None
        out = body.get("output")
        if isinstance(out, dict):
            content = out.get("content")
            if isinstance(content, list) and content:
                txt = content[0].get("text")
                try:
                    parsed = json.loads(txt)
                except Exception:
                    parsed = txt
            else:
                parsed = out
        ok = status == "success" and not (isinstance(parsed, dict) and "error" in parsed)
        return {"ok": ok, "status": status, "parsed": parsed, "error": body.get("error")}

    async def run(self, name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        spec = FLOWS.get(name)
        if not spec:
            return {"flow": name, "status": "failed", "error": f"unknown flow: {name}"}

        missing = [i for i in spec["inputs"] if i not in inputs]
        if missing:
            return {"flow": name, "status": "failed", "error": f"missing inputs: {missing}"}

        ctx: Dict[str, Any] = {"input": dict(inputs), "steps": {}}
        step_log: List[Dict[str, Any]] = []
        status = "success"

        async with httpx.AsyncClient(timeout=35.0) as client:
            for step in spec["steps"]:
                rendered = _render(step["inputs"], ctx)
                res = await self._call(client, step["tool"], rendered)
                ctx["steps"][step["id"]] = res["parsed"] if res["ok"] else {}
                step_log.append({
                    "id": step["id"], "tool": step["tool"],
                    "status": "ok" if res["ok"] else "error",
                    "output": res["parsed"],
                })
                if not res["ok"] and not step.get("optional"):
                    status = "failed"
                    break

        output = _render(spec.get("output", {}), ctx) if status == "success" else None
        ok_steps = sum(1 for s in step_log if s["status"] == "ok")
        return {
            "flow": name,
            "status": status,
            "result": output,
            "receipt": {
                "steps": len(spec["steps"]),
                "executed": len(step_log),
                "ok": ok_steps,
                "tools": [s["tool"] for s in step_log],
            },
            "trace": step_log,
        }
