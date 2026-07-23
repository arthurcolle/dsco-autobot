---
name: dsco-cli architecture — genome, register file, wings/talons
description: Core architectural concepts of the dsco CLI — 4MB genome, 64-slot register file, wings+talons+immune system metaphor
type: reference
---

## Core Metaphor: Bird of Prey
- **Wings** = autonomy, emergence, coordination, exploration, soaring
- **Talons** = winning, gripping, capturing, precision execution, competitive edge (NOT guardrails)
- **Immune system** = guardrails, safety, kill switches, hardcoded rules — SEPARATE concept

Do NOT confuse Talons with safety. Talons is competitive execution.

## 4MB Dynamic Genome
The dsco binary has a special 4MB dynamic area — the "genome." It's the evolutionary unit, not a cache or config file.

**Payload structure:**
1. Identity & invariants — species ID, lineage, policy constants, trust roots
2. Memory schema — object kinds, slot definitions, journal structure
3. Reflexes — anomaly thresholds, response tables, safe-mode transitions
4. Cognition scaffolding — planning priors, action grammars, tool affordance graph
5. Defense signatures — probe families, protocol misuse patterns, extraction heuristics
6. Adaptation envelope — mutation ranges, rollback rules, fitness metrics

**Key law:** Germline vs somatic separation. Runtime (somatic) adaptation is free. Genome (germline) mutation only through signed pipeline.

## 64-Slot Register File Tool Management
- R0-R15 (CORE_ALWAYS): 13 hardwired, never evicted
- R16-R31 (CORE_WARM): 16 default-loaded, evictable
- R32-R55 (WORKING): 24 quorum-scored, turn-volatile
- R56-R63 (DISCOVERY): 8 progressive schema (name+desc only)

**Multi-signal quorum:** 4 signals vote — hot cache, co-occurrence, embedding similarity, hint-group. Need >= 2 to hold register.

**Budget-adaptive shrinking:** Full (64) → Mid (48) → Low (32) → Critical (16)

## Trading Infrastructure
`src/trading.c` (2420 lines): keccak256, RSA-PSS, secp256k1, EIP-712 signing, HMAC L2 auth. 10 Kalshi tools, 8 Polymarket tools, 5 cross-platform. dry_run=true by default.
