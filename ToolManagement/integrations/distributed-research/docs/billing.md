## Billing Guardrails (Micro-Unit Credits)

To keep Parallel usage sustainable, we bill workloads in **micro-units** instead of raw dollars:

1. **Credits abstraction** – 1 credit = \$0.01 of upstream spend. Every processor tier publishes its credit cost per 1,000 runs (see `docs/choose_processor.md`). This lets us reason in whole numbers and cap exposure precisely.
2. **Ledger-first design** – When a task is requested we:
   - Estimate credits (`estimated_credits`) from processor tier and requested output fields.
   - Check the tenant/project balance + soft/hard limits.
   - Reserve credits so concurrent submissions cannot oversubscribe the budget.
3. **Post-run reconciliation** – Once the vendor result arrives we write the actual charge (`actual_credits`) and release/refund differences if the run failed or used a cheaper fallback tier.

### Data Model

| Table            | Purpose                                                    |
| ---------------- | ---------------------------------------------------------- |
| `credit_wallets` | One row per tenant/project with balance, limits, currency. |
| `credit_txns`    | Immutable ledger entries (reserve, capture, refund).       |
| `run_charges`    | Per-task metadata (processor, upstream run id, credits).   |

### API Hooks

- `POST /v1/tasks/runs`: accepts `billing_policy` (optional) with `max_processor`, `max_credits`, and `on_insufficient_funds` action.
- Scheduler enforces `settings.default_parallel_processor` and never escalates tiers unless the request explicitly opts in.
- Webhooks emit `task.run.charged` events so downstream finance systems can reconcile daily.

This approach ensures the default experience stays on inexpensive tiers (`lite`/`base`) while still giving advanced users a path to the premium processors without risking surprise costs.
