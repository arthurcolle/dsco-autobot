---
name: Trajectory Management / SpringfieldFund platform
description: Job search SaaS with forum, AI pipeline, recruiter marketplace — FastAPI + HTMX + SQLite
type: project
---

**Stack:** FastAPI + Jinja2 templates, HTMX 1.9.12, vanilla JS with hash-based SPA tabs, SQLite (users.db + jobs.db), SSE via /api/stream/firehose, cookie-based session auth (bcrypt + itsdangerous)

**Forum (v2):** 13 tables, FTS5, @mentions, emoji reactions, polls, subscriptions, read tracking, reputation/badges, leaderboard, moderation, mark-as-solution, keyboard shortcuts, SSE live updates. 9 categories.

**Business model:**
- Free: job browsing (40+ sources), forum, resume preview. ~$0/user
- Pro ($29/mo): 15 AI pipeline runs, PDF export, company intel. ~92% margin
- Turbo ($79/mo): 60 runs, batch apply, autopilot, campaigns, API. ~89% margin
- Recruiter ($199/seat/mo): candidate search (43K+), job posting, promoted threads

**Next:** Stripe billing integration + tier-check middleware (~1 week)
