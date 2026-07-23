---
name: GSD (Get Shit Done) reference architecture
description: Meta-prompting and context engineering system — 16 agents, wave execution, XML plan format. Reference for dsco architecture decisions.
type: reference
---

**Repo:** https://github.com/gsd-build/get-shit-done (33.2K stars, MIT)

## Core Problem: Context Rot
Quality degrades as context fills. GSD solves via:
- Atomic tasks with fresh 200K context per subagent executor
- Orchestrator stays at ~10-15% context
- Plans ARE prompts (PLAN.md IS the execution prompt)
- Each task gets its own git commit

## Workflow
discuss → plan → execute → verify

## 16 Agents (key ones)
- gsd-executor: atomic plan execution, deviation auto-fix
- gsd-planner: decompose into 2-3 task plans
- gsd-plan-checker: 9-dimension pre-execution validation
- gsd-verifier: goal-backward verification
- gsd-debugger: hypothesis-testing, 7 investigation techniques

## Executor Rules
- Rule 1-3: Auto-fix bugs, missing auth/validation, blocking imports
- Rule 4: Architectural decisions → STOP, ask user
- Analysis paralysis guard: 5+ reads without edit → force action

## dsco Architecture Gaps vs GSD
- No wave dependency tracking
- Plans unstructured (GSD uses XML)
- Sub-agents get full conversation (should scope context)
- No pre-execution plan validation
- No persistent debug files
- Memory is flat KV (should be structured state)
- No deviation auto-fix protocol
- Compaction at 80% (GSD says ~50%)
