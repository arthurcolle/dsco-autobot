"""
Finance Fabric catalog.

Declares the full surface of finance/banking operations as (domain, operation)
specs and the set of providers per domain. The cartesian product
provider x operation yields the hundreds of concrete MCP tools the fabric
exposes. Every concrete tool routes to the single canonical handler for its
(domain, operation); the provider only selects live-vs-sandbox behaviour.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


# Reusable property fragments.
def _p(**kw) -> Dict[str, Any]:
    return kw


# (operation, description, properties, required)
OpSpec = Tuple[str, str, Dict[str, Any], List[str]]


DOMAIN_OPS: Dict[str, List[OpSpec]] = {
    "banking": [
        ("link", "Link an institution and seed account access.",
         {"institution": _p(type="string"), "public_token": _p(type="string")}, ["institution"]),
        ("accounts", "List linked accounts.", {"link_id": _p(type="string")}, []),
        ("balance", "Get account balance (current + available).",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("transactions", "List recent transactions for an account.",
         {"account_id": _p(type="string"), "limit": _p(type="integer", default=25)}, ["account_id"]),
        ("identity", "Get account holder identity details.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("income", "Estimate verified income for an account.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("liabilities", "Get liabilities (loans, credit) for an account.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("investments", "Get investment holdings for an account.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("statements", "List available account statements.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("routing", "Get account/routing numbers for ACH setup.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("categorize", "Categorize transactions for an account.",
         {"account_id": _p(type="string")}, ["account_id"]),
        ("webhook_status", "Get aggregation webhook/connection status.", {}, []),
    ],
    "payments": [
        ("charge", "Create a payment/charge.",
         {"amount": _p(type="number"), "currency": _p(type="string", default="usd"),
          "description": _p(type="string"), "source": _p(type="string")}, ["amount"]),
        ("capture", "Capture a previously authorized payment.",
         {"payment_id": _p(type="string"), "amount": _p(type="number")}, ["payment_id"]),
        ("status", "Get payment status.", {"payment_id": _p(type="string")}, ["payment_id"]),
        ("refund", "Refund a payment fully or partially.",
         {"payment_id": _p(type="string"), "amount": _p(type="number")}, ["payment_id"]),
        ("payout", "Send a payout to a connected account/bank.",
         {"amount": _p(type="number"), "currency": _p(type="string", default="usd"),
          "destination": _p(type="string")}, ["amount", "destination"]),
        ("customer_create", "Create a customer record.",
         {"email": _p(type="string"), "name": _p(type="string")}, []),
        ("subscription_create", "Create a recurring subscription.",
         {"customer": _p(type="string"), "plan": _p(type="string"),
          "amount": _p(type="number"), "interval": _p(type="string", default="month")}, ["plan"]),
        ("invoice_create", "Create an invoice.",
         {"customer": _p(type="string"), "amount": _p(type="number"),
          "currency": _p(type="string", default="usd")}, ["amount"]),
        ("dispute_list", "List open disputes/chargebacks.", {}, []),
        ("payment_method_attach", "Attach a payment method to a customer.",
         {"customer": _p(type="string"), "payment_method": _p(type="string")},
         ["customer", "payment_method"]),
        ("balance", "Get the processor balance.", {}, []),
        ("checkout_session", "Create a hosted checkout session.",
         {"amount": _p(type="number"), "currency": _p(type="string", default="usd"),
          "success_url": _p(type="string")}, ["amount"]),
    ],
    "brokerage": [
        ("account", "Get brokerage account info.", {}, []),
        ("order", "Place an order.",
         {"symbol": _p(type="string"), "qty": _p(type="number"), "side": _p(type="string"),
          "order_type": _p(type="string", default="market"), "limit_price": _p(type="number")},
         ["symbol", "qty", "side"]),
        ("order_status", "Get the status of an order.",
         {"order_id": _p(type="string")}, ["order_id"]),
        ("orders", "List orders.", {"status": _p(type="string", default="all")}, []),
        ("cancel", "Cancel an open order.", {"order_id": _p(type="string")}, ["order_id"]),
        ("positions", "List open positions.", {}, []),
        ("quote", "Get a latest quote for a symbol.",
         {"symbol": _p(type="string")}, ["symbol"]),
        ("bars", "Get OHLCV bars for a symbol.",
         {"symbol": _p(type="string"), "timeframe": _p(type="string", default="1Day"),
          "limit": _p(type="integer", default=30)}, ["symbol"]),
        ("option_chain", "Get the option chain for an underlying.",
         {"symbol": _p(type="string"), "expiry": _p(type="string")}, ["symbol"]),
        ("portfolio_history", "Get portfolio equity history.", {}, []),
        ("watchlist", "Get/replace a watchlist.",
         {"symbols": _p(type="array", items=_p(type="string"))}, []),
        ("close_position", "Close a position by symbol.",
         {"symbol": _p(type="string")}, ["symbol"]),
    ],
    "crypto": [
        ("balances", "Get wallet balances.", {"asset": _p(type="string")}, []),
        ("order", "Place a market order.",
         {"product": _p(type="string"), "side": _p(type="string"), "size": _p(type="number")},
         ["product", "side", "size"]),
        ("ticker", "Get the ticker for a product.",
         {"product": _p(type="string")}, ["product"]),
        ("orderbook", "Get the order book for a product.",
         {"product": _p(type="string")}, ["product"]),
        ("deposit_address", "Get a deposit address for an asset.",
         {"asset": _p(type="string")}, ["asset"]),
        ("withdraw", "Withdraw an asset to an address.",
         {"asset": _p(type="string"), "amount": _p(type="number"), "address": _p(type="string")},
         ["asset", "amount", "address"]),
        ("transfers", "List recent deposits/withdrawals.", {}, []),
        ("products", "List tradable products.", {}, []),
        ("fees", "Get current fee tiers.", {}, []),
        ("candles", "Get OHLC candles for a product.",
         {"product": _p(type="string"), "granularity": _p(type="integer", default=3600)},
         ["product"]),
    ],
    "marketdata": [
        ("quote", "Get a latest quote.", {"symbol": _p(type="string")}, ["symbol"]),
        ("bars", "Get historical bars.",
         {"symbol": _p(type="string"), "timeframe": _p(type="string", default="1Day"),
          "limit": _p(type="integer", default=30)}, ["symbol"]),
        ("fundamentals", "Get company fundamentals.",
         {"symbol": _p(type="string")}, ["symbol"]),
        ("news", "Get recent news for a symbol.",
         {"symbol": _p(type="string"), "limit": _p(type="integer", default=10)}, ["symbol"]),
        ("dividends", "Get dividend history.", {"symbol": _p(type="string")}, ["symbol"]),
        ("splits", "Get stock split history.", {"symbol": _p(type="string")}, ["symbol"]),
        ("earnings", "Get earnings calendar/history.",
         {"symbol": _p(type="string")}, ["symbol"]),
        ("search", "Search instruments by keyword.",
         {"query": _p(type="string")}, ["query"]),
    ],
    "fx": [
        ("convert", "Convert an amount between currencies.",
         {"amount": _p(type="number"), "from": _p(type="string"), "to": _p(type="string")},
         ["amount", "from", "to"]),
        ("rate", "Get the indicative rate for a pair.",
         {"from": _p(type="string"), "to": _p(type="string")}, ["from", "to"]),
        ("quote", "Get a tradeable FX quote with spread.",
         {"from": _p(type="string"), "to": _p(type="string"), "amount": _p(type="number")},
         ["from", "to"]),
        ("rates_table", "Get a full rates table vs a base currency.",
         {"base": _p(type="string", default="USD")}, []),
    ],
    "ledger": [
        ("post", "Post a balanced double-entry journal entry.",
         {"lines": _p(type="array", items=_p(type="object")), "memo": _p(type="string")},
         ["lines"]),
        ("balance", "Get trial balance or a single account balance.",
         {"account": _p(type="string")}, []),
        ("accounts", "List chart-of-accounts entries seen in the ledger.", {}, []),
        ("invoice_create", "Create an accounting invoice.",
         {"customer": _p(type="string"), "amount": _p(type="number")}, ["amount"]),
        ("bill_create", "Create an accounts-payable bill.",
         {"vendor": _p(type="string"), "amount": _p(type="number")}, ["amount"]),
        ("journal", "List recent journal entries.",
         {"limit": _p(type="integer", default=25)}, []),
        ("reconcile", "Reconcile an account against a statement balance.",
         {"account": _p(type="string"), "statement_balance": _p(type="number")},
         ["account", "statement_balance"]),
        ("report", "Generate a P&L or balance-sheet summary.",
         {"kind": _p(type="string", default="pnl")}, []),
    ],
    "cards": [
        ("create", "Issue a new card.",
         {"holder": _p(type="string"), "type": _p(type="string", default="virtual")}, []),
        ("get", "Get a card by id.", {"card_id": _p(type="string")}, ["card_id"]),
        ("freeze", "Freeze a card.", {"card_id": _p(type="string")}, ["card_id"]),
        ("unfreeze", "Unfreeze a card.", {"card_id": _p(type="string")}, ["card_id"]),
        ("transactions", "List card transactions.",
         {"card_id": _p(type="string"), "limit": _p(type="integer", default=25)}, ["card_id"]),
        ("set_limit", "Set a spend limit on a card.",
         {"card_id": _p(type="string"), "amount": _p(type="number"),
          "interval": _p(type="string", default="monthly")}, ["card_id", "amount"]),
    ],
    "treasury": [
        ("transfer", "Initiate a money-movement transfer.",
         {"rail": _p(type="string", default="ach"), "from_account": _p(type="string"),
          "to_account": _p(type="string"), "amount": _p(type="number"),
          "currency": _p(type="string", default="USD"), "memo": _p(type="string")},
         ["from_account", "to_account", "amount"]),
        ("transfer_status", "Get the status of a transfer.",
         {"transfer_id": _p(type="string")}, ["transfer_id"]),
        ("counterparty_create", "Create a counterparty/payee.",
         {"name": _p(type="string"), "account_number": _p(type="string"),
          "routing_number": _p(type="string")}, ["name"]),
        ("account_create", "Create an internal ledger/virtual account.",
         {"name": _p(type="string"), "currency": _p(type="string", default="USD")}, ["name"]),
        ("balance", "Get a treasury account balance.",
         {"account": _p(type="string")}, []),
        ("statement", "Get a treasury account statement.",
         {"account": _p(type="string")}, []),
        ("expected_payments", "List expected/forecast payments.", {}, []),
        ("ledger_entries", "List treasury ledger entries.", {}, []),
    ],
    "lending": [
        ("credit_score", "Get a credit score for a subject.",
         {"subject_id": _p(type="string")}, ["subject_id"]),
        ("application_create", "Create a loan application.",
         {"subject_id": _p(type="string"), "amount": _p(type="number"),
          "term_months": _p(type="integer", default=36)}, ["amount"]),
        ("offer", "Get a pre-qualified loan offer.",
         {"subject_id": _p(type="string"), "amount": _p(type="number")}, ["amount"]),
        ("loan_status", "Get a loan's status.",
         {"loan_id": _p(type="string")}, ["loan_id"]),
        ("amortization", "Compute an amortization schedule.",
         {"principal": _p(type="number"), "rate": _p(type="number"),
          "term_months": _p(type="integer", default=36)}, ["principal", "rate"]),
        ("repayment", "Make a loan repayment.",
         {"loan_id": _p(type="string"), "amount": _p(type="number")}, ["loan_id", "amount"]),
    ],
    "tax": [
        ("calculate", "Calculate sales tax for a transaction.",
         {"amount": _p(type="number"), "jurisdiction": _p(type="string"),
          "category": _p(type="string", default="general")}, ["amount", "jurisdiction"]),
        ("rate", "Get the tax rate for a jurisdiction.",
         {"jurisdiction": _p(type="string")}, ["jurisdiction"]),
        ("estimate_income_tax", "Estimate income tax owed.",
         {"income": _p(type="number"), "filing_status": _p(type="string", default="single")},
         ["income"]),
        ("filing_status", "Get filing/registration status for a jurisdiction.",
         {"jurisdiction": _p(type="string")}, ["jurisdiction"]),
    ],
}


# Providers per domain. Concrete tools are {provider}_{domain}_{op} (with a few
# domain-specific naming tweaks below).
DOMAIN_PROVIDERS: Dict[str, List[str]] = {
    "banking": ["plaid", "teller", "mx", "yodlee", "finicity", "truelayer", "tink",
                "nordigen", "akoya", "salt_edge", "basiq", "plinqit"],
    "payments": ["stripe", "adyen", "square", "braintree", "paypal", "checkout", "worldpay",
                 "razorpay", "mollie", "dlocal", "klarna", "gocardless"],
    "brokerage": ["alpaca", "ibkr", "tradier", "robinhood", "schwab", "etrade", "fidelity",
                  "tastytrade", "drivewealth"],
    "crypto": ["coinbase", "kraken", "binance", "gemini", "bitstamp", "bitfinex", "okx",
               "bybit", "cryptocom", "kucoin"],
    "marketdata": ["polygon", "alphavantage", "finnhub", "iex", "twelvedata", "tiingo",
                   "marketstack", "eodhd", "quandl", "intrinio"],
    "fx": ["wise", "oanda", "currencylayer", "xe", "fixer", "openexchangerates",
           "frankfurter", "ecb", "currencycloud"],
    "ledger": ["quickbooks", "xero", "netsuite", "sage", "freshbooks", "zoho_books",
               "wave", "oracle_fusion", "sap_concur"],
    "cards": ["marqeta", "lithic", "stripe_issuing", "galileo", "highnote", "i2c",
              "treasury_prime", "adyen_issuing", "deserve"],
    "treasury": ["moderntreasury", "column", "increase", "unit", "dwolla", "sila",
                 "stripe_treasury", "currencycloud", "nium"],
    "lending": ["experian", "equifax", "transunion", "lexisnexis", "sentilink",
                "nova_credit", "plaid_cra", "finicity_verify", "sift"],
    "tax": ["avalara", "taxjar", "vertex", "stripe_tax", "sovos", "anrok",
            "quaderno", "sphere", "fonoa"],
}


def build_tools() -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str, str]]]:
    """Return (tool_definitions, dispatch_map) for the L0 provider primitives.

    dispatch_map: tool_name -> (domain, operation, provider).
    """
    tools: List[Dict[str, Any]] = []
    dispatch: Dict[str, Tuple[str, str, str]] = {}

    for domain, ops in DOMAIN_OPS.items():
        providers = DOMAIN_PROVIDERS.get(domain, [])
        for provider in providers:
            for op, desc, props, required in ops:
                name = f"{provider}_{domain}_{op}"
                tools.append({
                    "name": name,
                    "description": f"[{provider}] {desc}",
                    "inputSchema": {
                        "type": "object",
                        "properties": props,
                        **({"required": required} if required else {}),
                    },
                })
                dispatch[name] = (domain, op, provider)

    return tools, dispatch


def default_provider(domain: str) -> str:
    """The provider a canonical L1 operation resolves to."""
    return DOMAIN_PROVIDERS[domain][0]


def build_canonical() -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str, str]]]:
    """Return (tool_definitions, dispatch_map) for L1 canonical operations.

    Canonical tools are provider-agnostic and named ``{domain}__{op}``. They
    resolve to the domain's default provider and are the stable vocabulary that
    Flows (L3) and external composers should target.
    """
    tools: List[Dict[str, Any]] = []
    dispatch: Dict[str, Tuple[str, str, str]] = {}

    for domain, ops in DOMAIN_OPS.items():
        provider = default_provider(domain)
        for op, desc, props, required in ops:
            name = f"{domain}__{op}"
            tools.append({
                "name": name,
                "description": f"[canonical:{domain}] {desc} (default provider: {provider})",
                "inputSchema": {
                    "type": "object",
                    "properties": props,
                    **({"required": required} if required else {}),
                },
            })
            dispatch[name] = (domain, op, provider)

    return tools, dispatch
