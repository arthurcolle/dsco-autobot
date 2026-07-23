"""Quick Turso-backed entry manager for one-off testing."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import click
import libsql_client
from aiohttp import client_exceptions

from turso_db import get_turso_url_candidates


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS entries (
    id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_entries_updated
ON entries (updated_at DESC)
"""


def parse_metadata(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """Decode a JSON metadata string if provided."""
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise click.BadParameter("metadata must be valid JSON") from exc


def format_metadata(raw: Optional[str]) -> str:
    """Pretty-print stored JSON metadata."""
    if not raw:
        return ""
    try:
        return json.dumps(json.loads(raw), indent=2)
    except json.JSONDecodeError:  # pragma: no cover
        return raw


async def ensure_tables(client: libsql_client.Client) -> None:
    """Create the entries table if it does not exist."""
    await client.execute(TABLE_SQL)
    await client.execute(INDEX_SQL)


async def run_with_client(
    url: str,
    token: str,
    handler: Callable[[libsql_client.Client], asyncio.Future],
) -> Any:
    """Create a libsql client, ensure tables, and run the handler."""
    candidates = get_turso_url_candidates(url)
    for idx, candidate in enumerate(candidates):
        client = libsql_client.create_client(url=candidate, auth_token=token)
        try:
            await ensure_tables(client)
            result = await handler(client)
            if idx > 0:
                click.echo(f"Using HTTPS fallback for Turso database: {candidate}", err=True)
            return result
        except client_exceptions.WSServerHandshakeError as exc:
            if exc.status == 505 and idx < len(candidates) - 1:
                click.echo(
                    "Turso WebSocket handshake failed with HTTP 505. Retrying with HTTPS...",
                    err=True,
                )
                continue
            raise
        finally:
            await client.close()
    raise RuntimeError("Unable to connect to Turso database with any URL candidate.")


def sync_run(url: str, token: str, handler: Callable[[libsql_client.Client], asyncio.Future]) -> Any:
    """Run an async handler inside asyncio.run."""
    return asyncio.run(run_with_client(url, token, handler))


@click.group()
@click.option("--url", envvar="TURSO_DATABASE_URL", help="libsql://-style Turso URL.")
@click.option("--token", envvar="TURSO_AUTH_TOKEN", help="Turso auth token or JWT.")
@click.pass_context
def cli(ctx: click.Context, url: Optional[str], token: Optional[str]) -> None:
    """Manage a single entries table in Turso for quick add/edit work."""
    if not url or not token:
        raise click.UsageError("Provide --url and --token or set TURSO_DATABASE_URL/TURSO_AUTH_TOKEN.")
    ctx.ensure_object(dict)
    ctx.obj["url"] = url
    ctx.obj["token"] = token


@cli.command()
@click.option("--id", "entry_id", help="Provide an ID if you want to reuse one.")
@click.option("--title", help="Optional title for this entry.")
@click.option("--content", prompt=True, help="Content to store for this entry.")
@click.option("--metadata", help="JSON metadata for this entry.")
@click.pass_context
def add(ctx: click.Context, entry_id: Optional[str], title: Optional[str], content: str, metadata: Optional[str]) -> None:
    """Store a new entry."""
    metadata_payload = parse_metadata(metadata)

    async def handler(client: libsql_client.Client) -> None:
        now = datetime.utcnow().isoformat()
        await client.execute(
            """
            INSERT INTO entries (id, title, content, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                entry_id or str(uuid.uuid4()),
                title,
                content,
                json.dumps(metadata_payload) if metadata_payload else None,
                now,
                now,
            ],
        )
        click.echo("Entry saved.")

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


@cli.command()
@click.option("--limit", default=20, help="How many entries to list.")
@click.pass_context
def list(ctx: click.Context, limit: int) -> None:
    """Show recent entries."""

    async def handler(client: libsql_client.Client) -> None:
        result = await client.execute(
            "SELECT id, title, created_at, updated_at FROM entries ORDER BY updated_at DESC LIMIT ?",
            [limit],
        )
        if not result.rows:
            click.echo("No entries found.")
            return

        for row in result.rows:
            click.echo(f"{row[0]}  ({row[1] or 'no title'}) updated {row[3]}")

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


@cli.command()
@click.argument("entry_id")
@click.pass_context
def view(ctx: click.Context, entry_id: str) -> None:
    """View an entry in detail."""

    async def handler(client: libsql_client.Client) -> None:
        result = await client.execute(
            "SELECT id, title, content, metadata, created_at, updated_at FROM entries WHERE id = ?",
            [entry_id],
        )
        if not result.rows:
            raise click.ClickException("Entry not found.")
        row = result.rows[0]
        click.echo(f"ID: {row[0]}")
        click.echo(f"Title: {row[1]}")
        click.echo(f"Created: {row[4]}")
        click.echo(f"Updated: {row[5]}")
        click.echo("\nContent:")
        click.echo(row[2])
        metadata_text = format_metadata(row[3])
        if metadata_text:
            click.echo("\nMetadata:")
            click.echo(metadata_text)

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


@cli.command()
@click.argument("entry_id")
@click.option("--title", help="Replace the title.")
@click.option("--content", help="Replace the content.")
@click.option("--metadata", help="Replace the metadata (JSON).")
@click.pass_context
def edit(ctx: click.Context, entry_id: str, title: Optional[str], content: Optional[str], metadata: Optional[str]) -> None:
    """Update an entry."""
    if not any([title, content, metadata]):
        raise click.UsageError("Use --title, --content, or --metadata to change something.")

    metadata_payload = parse_metadata(metadata) if metadata is not None else None

    async def handler(client: libsql_client.Client) -> None:
        fields = []
        params = []
        now = datetime.utcnow().isoformat()
        if title is not None:
            fields.append("title = ?")
            params.append(title)
        if content is not None:
            fields.append("content = ?")
            params.append(content)
        if metadata is not None:
            fields.append("metadata = ?")
            params.append(json.dumps(metadata_payload) if metadata_payload else None)
        fields.append("updated_at = ?")
        params.append(now)
        params.append(entry_id)
        await client.execute(
            f"UPDATE entries SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        click.echo("Entry updated.")

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


@cli.command()
@click.argument("entry_id")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def delete(ctx: click.Context, entry_id: str, confirm: bool) -> None:
    """Remove an entry."""
    if not confirm and not click.confirm(f"Delete entry {entry_id}?"):
        raise click.Abort()

    async def handler(client: libsql_client.Client) -> None:
        await client.execute("DELETE FROM entries WHERE id = ?", [entry_id])
        click.echo("Entry deleted.")

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Ensure the table is ready."""

    async def handler(client: libsql_client.Client) -> None:
        await ensure_tables(client)
        click.echo("Table is ready.")

    sync_run(ctx.obj["url"], ctx.obj["token"], handler)


if __name__ == "__main__":
    cli()
