"""Minimal async Turso/libSQL smoke test."""

import asyncio
import os

from libsql_client import Statement, create_client


async def main() -> None:
    db_url = os.environ["TURSO_DATABASE_URL"]          # https://...turso.io
    db_token = os.environ.get("TURSO_AUTH_TOKEN", "")  # token required unless db is public

    # libsql_client is async-only, so we keep the client inside an asyncio context.
    async with create_client(url=db_url, auth_token=db_token) as client:
        await client.execute(
            """
            CREATE TABLE IF NOT EXISTS notes (
                id         INTEGER PRIMARY KEY,
                title      TEXT    NOT NULL,
                body       TEXT,
                created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            """
        )

        await client.execute(
            "INSERT INTO notes (title, body) VALUES (?, ?)",
            ["hello turso", "first row via libsql-client"],
        )

        last_id_result = await client.execute("SELECT last_insert_rowid() AS id")
        last_id = last_id_result.rows[0]["id"]
        print("inserted id:", last_id)

        rows = (
            await client.execute(
                "SELECT id, title, body, created_at FROM notes ORDER BY id DESC LIMIT 5"
            )
        ).rows
        for row in rows:
            print(row["id"], row["title"], row["created_at"])

        await client.batch(
            [
                Statement("INSERT INTO notes (title, body) VALUES (?, ?)", ["batch 1", "ok"]),
                Statement("INSERT INTO notes (title, body) VALUES (?, ?)", ["batch 2", "ok"]),
            ]
        )

        rows = (await client.execute("SELECT id, title FROM notes ORDER BY id DESC")).rows
        print([{"id": r["id"], "title": r["title"]} for r in rows])


if __name__ == "__main__":
    asyncio.run(main())
