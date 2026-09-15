"""Catalog cleanup for the DuckDB `tools` table.

The catalog is built from two sources — the OpenAI codex_apps connectors
(materialized as canonical `{category}__{op}` + per-provider
`{provider}_{category}_{op}` tools on distributed-integration) and our own
integration servers. That generation leaves three fixable classes of cruft:

  1. junk rows   — empty/blank names, internal diagnostics (e.g. names starting
                   with '_', bare 'catalog_summary'/'_connectivity_test').
  2. blind rows  — empty descriptions, so retrieval has nothing to match on.
  3. reported-only — cross-backend duplicate names + provider-explosion near-dupes
                   (a product decision; we surface counts, never auto-delete).

diagnose() is read-only. cleanup(dry_run=True) reports what it WOULD do;
cleanup(dry_run=False) applies (1) delete-junk and (2) backfill-description, and
returns before/after counts. Removed tools fall out of the faceted+FTS index on
the next sync via the existing eviction path.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Internal/diagnostic names that are not client-facing tools.
_META_NAMES = {"_connectivity_test", "catalog_summary", "health", "ping", "_ping"}


def _humanize(name: str) -> str:
    """`aws_cloud_start_instance` -> 'aws cloud start instance' — a minimal
    description so a tool with an empty doc still has something to retrieve on."""
    return re.sub(r"[_]+", " ", re.sub(r"__+", " ", name or "")).strip()


def diagnose(store) -> Dict[str, Any]:
    with store._get_conn() as conn:
        def one(sql, params=None):
            try:
                r = conn.execute(sql, params or []).fetchone()
                return r[0] if r else 0
            except Exception as exc:  # noqa: BLE001
                logger.warning("catalog diag query failed (%s): %s", sql[:40], exc)
                return -1

        def rows(sql, params=None):
            try:
                return conn.execute(sql, params or []).fetchall()
            except Exception as exc:  # noqa: BLE001
                logger.warning("catalog diag rows failed (%s): %s", sql[:40], exc)
                return []

        total = one("SELECT count(*) FROM tools")
        empty_name = one("SELECT count(*) FROM tools WHERE name IS NULL OR trim(name)=''")
        empty_desc = one("SELECT count(*) FROM tools WHERE description IS NULL OR trim(description)=''")
        empty_schema = one(
            "SELECT count(*) FROM tools WHERE input_schema IS NULL "
            "OR CAST(input_schema AS VARCHAR) IN ('{}','null','')"
        )
        underscore = one("SELECT count(*) FROM tools WHERE starts_with(name, '_')")
        meta = one(
            f"SELECT count(*) FROM tools WHERE name IN ({','.join(['?'] * len(_META_NAMES))})",
            list(_META_NAMES),
        )
        dup_names = rows(
            "SELECT name, count(*) c FROM tools GROUP BY name HAVING count(*) > 1 "
            "ORDER BY c DESC LIMIT 15"
        )

    return {
        "total": total,
        "junk": {"empty_name": empty_name, "underscore_prefixed": underscore, "meta_named": meta},
        "blind": {"empty_description": empty_desc, "empty_input_schema": empty_schema},
        "reported_only": {
            "cross_backend_duplicate_names": [
                {"name": n, "occurrences": c} for (n, c) in dup_names
            ],
        },
    }


def cleanup(store, dry_run: bool = True) -> Dict[str, Any]:
    before = diagnose(store)
    would_delete = (
        before["junk"]["empty_name"]
        + before["junk"]["underscore_prefixed"]
        + before["junk"]["meta_named"]
    )
    would_backfill = before["blind"]["empty_description"]

    if dry_run:
        return {"dry_run": True, "would_delete": would_delete,
                "would_backfill_description": would_backfill, "diagnosis": before}

    deleted = backfilled = 0
    with store._get_conn() as conn:
        # (1) delete junk rows
        conn.execute(
            "DELETE FROM tools WHERE name IS NULL OR trim(name)='' "
            "OR starts_with(name, '_') "
            f"OR name IN ({','.join(['?'] * len(_META_NAMES))})",
            list(_META_NAMES),
        )
        # (2) backfill empty descriptions from the humanized name
        rows = conn.execute(
            "SELECT tool_id, name FROM tools WHERE description IS NULL OR trim(description)=''"
        ).fetchall()
        for tool_id, name in rows:
            conn.execute("UPDATE tools SET description=? WHERE tool_id=?", [_humanize(name), tool_id])
        backfilled = len(rows)

    after = diagnose(store)
    deleted = before["total"] - after["total"]
    logger.info("catalog_cleanup: deleted %d junk, backfilled %d descriptions", deleted, backfilled)
    return {"dry_run": False, "deleted": deleted, "backfilled_description": backfilled,
            "before_total": before["total"], "after_total": after["total"], "after": after}
