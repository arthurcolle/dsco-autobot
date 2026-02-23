# Real-World Use Cases: Full Spectrum Dominance

Date: February 23, 2026

This system now ships a durable use-case catalog at `data/use_case_catalog.json` and exposes first-class APIs for discovery, planning, and execution.

## Why this matters

- Agents get production-ready workflows instead of ad hoc tool calls.
- Economic controls are built in from the start (`max_total_cost_cents`, reintegration budgets, dedupe, follow-ups).
- Durable data loops are explicit in every scenario (`store_belief`, event publication, execution traces, follow-up actions).
- MCP clients can discover and execute high-value workflows immediately.

## New API surface

- `GET /api/v1/use-cases`
- `GET /api/v1/use-cases/{use_case_id}`
- `POST /api/v1/use-cases/{use_case_id}/plan`
- `POST /api/v1/use-cases/{use_case_id}/execute`

## Included use-case domains

- `market_intelligence`
- `operations`
- `knowledge_compounding`
- `compliance`
- `economics`
- `swarm_operations`
- `customer_intelligence`
- `knowledge_infrastructure`

## Dominance scenarios in catalog

- `market_intel_compound_briefing`
- `incident_response_live_ops`
- `memory_compounding_research_loop`
- `compliance_evidence_pack_generator`
- `economic_settlement_watchtower`
- `swarm_strategy_control_tower`
- `autonomous_customer_signal_radar`
- `knowledge_graph_bootstrap_pipeline`

## Example: discover by objective + economics

```bash
curl -s "http://localhost:8000/api/v1/use-cases?domain=market_intelligence&q=daily%20briefing&require_economic_hooks=true"
```

## Example: build a guarded plan without executing

```bash
curl -s -X POST "http://localhost:8000/api/v1/use-cases/market_intel_compound_briefing/plan" \
  -H "content-type: application/json" \
  -d '{
    "objective": "Generate a strategic brief on distributed autonomy",
    "overrides": {
      "query": "distributed autonomy startup funding",
      "num_results": 6,
      "deep_num_results": 14
    },
    "max_total_cost_cents": 220,
    "max_reintegration_cost": 9.5,
    "reintegration_overflow_policy": "truncate",
    "auto_tune_parallel": true
  }'
```

## Example: execute a full scenario

```bash
curl -s -X POST "http://localhost:8000/api/v1/use-cases/swarm_strategy_control_tower/execute" \
  -H "content-type: application/json" \
  -d '{
    "objective": "Route work to specialized agents with spend control",
    "overrides": {
      "mission": "evaluate data moat opportunities",
      "fanout_budget": 5
    },
    "max_total_cost_cents": 260,
    "max_reintegration_cost": 8.0,
    "reintegration_overflow_policy": "truncate",
    "trace_id": "tr_demo_swarm_001",
    "session_id": "sess_demo_swarm"
  }'
```

## Agent integration pattern

1. Discover catalog entries with `GET /api/v1/use-cases`.
2. Select by fit score, domain, and economic hooks.
3. Run `plan` first to check admission and budget risk.
4. Execute only admitted plans.
5. Persist and react to follow-up actions (`/api/v1/follow-ups`).
6. Close loop with analytics (`/api/v1/policy-decisions/analytics`, `/api/v1/execution-groups/analytics`).

## Durability posture

Each use-case is designed to compound data value over time:

- durable event records
- durable policy decisions
- durable follow-up action queue
- durable memory/belief updates
- durable execution-group convergence state

This supports a long-term data moat rather than one-off job execution.
