#!/usr/bin/env python3
"""
Index ALL tools into SemanticToolStore — both DuckDB registry + Alpha Vantage.

This fixes the core problem: only 64/266+ tools were indexed,
so financial queries had no relevant candidates.
"""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


async def main():
    from tool_management.embeddings.semantic_tool_store import SemanticToolStore

    db_path = os.path.join(DATA_DIR, "tool_embeddings.db")
    store = SemanticToolStore(db_path=db_path)

    stats_before = await store.get_stats()
    print(f"Before: {stats_before['total_tools']} tools indexed")

    # 1. Load existing DuckDB tools
    duckdb_tools = []
    try:
        import duckdb
        conn = duckdb.connect(os.path.join(DATA_DIR, "tools.duckdb"), read_only=True)
        rows = conn.execute(
            "SELECT name, description, backend_id, input_schema FROM tools"
        ).fetchall()
        for name, desc, backend, input_schema in rows:
            tool = {"name": name, "description": desc or ""}
            if input_schema:
                try:
                    tool["inputSchema"] = json.loads(input_schema) if isinstance(input_schema, str) else input_schema
                except (json.JSONDecodeError, TypeError):
                    pass
            duckdb_tools.append({"tool": tool, "backend": backend})
        conn.close()
        print(f"Loaded {len(duckdb_tools)} tools from DuckDB")
    except Exception as e:
        print(f"Could not load DuckDB tools: {e}")

    # 2. Load Alpha Vantage tools
    av_tools = []
    av_path = os.path.join(DATA_DIR, "alpha_vantage_tools.json")
    if os.path.exists(av_path):
        with open(av_path) as f:
            av_data = json.load(f)
        for t in av_data:
            av_tools.append({"tool": t, "backend": "alpha-vantage"})
        print(f"Loaded {len(av_tools)} Alpha Vantage tools")

    # 3. Combine and index
    all_tools = duckdb_tools + av_tools
    print(f"Total tools to index: {len(all_tools)}")

    # Index in batches (Jina embed has batch limits)
    batch_size = 32
    indexed = 0
    for i in range(0, len(all_tools), batch_size):
        batch = all_tools[i:i + batch_size]
        tool_dicts = [t["tool"] for t in batch]
        backend = batch[0]["backend"] if batch else None

        try:
            await store.index_tools_batch(tool_dicts, backend=backend)
            indexed += len(batch)
            print(f"  Indexed batch {i // batch_size + 1}: {len(batch)} tools ({indexed}/{len(all_tools)})")
        except Exception as e:
            print(f"  Batch {i // batch_size + 1} failed: {e}")
            # Try one-by-one
            for t in batch:
                try:
                    await store.index_tools_batch([t["tool"]], backend=t["backend"])
                    indexed += 1
                except Exception as e2:
                    print(f"    Failed to index {t['tool'].get('name')}: {e2}")

    stats_after = await store.get_stats()
    print(f"\nAfter: {stats_after['total_tools']} tools indexed (was {stats_before['total_tools']})")
    print(f"New tools added: {stats_after['total_tools'] - stats_before['total_tools']}")


if __name__ == "__main__":
    asyncio.run(main())
