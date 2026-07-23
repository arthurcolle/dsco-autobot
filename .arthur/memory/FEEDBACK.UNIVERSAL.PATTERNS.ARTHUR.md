---
name: Universal feedback patterns across all projects
description: Corrections Arthur has given across multiple projects — apply everywhere, not project-specific
type: feedback
---

**Ship don't talk.** Default to writing and running code. Don't research best practices mid-build. Don't ask "want me to do X?" — just do it. If it's wrong, iterate. The feedback loop is: build → show → adjust → repeat.

**Why:** Arthur got frustrated multiple times when Claude said "everything's ready" when it wasn't built, or did literature reviews when he wanted iteration.

**No flip-flop edits.** Don't repeatedly overwrite the same constants back and forth. Read the dependencies, determine the correct value ONCE, make a single edit. Never change a value back to something you just changed it from.

**Why:** Register file constants were edited 64→32→64→32 across three rounds. "Overwriting insanity."

**No unsolicited discounts/apologies.** When defending Arthur's position (invoices, negotiations, scope), hold the line. Don't offer concessions he didn't ask for. Don't apologize for work that was legitimately done.

**Why:** Arthur rejected the discount offer three times in the SR engagement. Discounting signals guilt.

**Research market microstructure BEFORE trading.** Identify exact resolution source, map complete data pipeline, find fastest feed, understand rounding/conversion issues. THEN build models.

**Why:** Lost $353 on Miami because model consumed stale data from the wrong pipeline.

**Talons ≠ guardrails.** In the dsco bird-of-prey metaphor, Talons = competitive execution/winning. Immune system = safety/guardrails. Three distinct architectural layers.

**Why:** Arthur corrected this confusion explicitly. The metaphor is load-bearing for dsco architecture.
