#!/usr/bin/env python3
"""
Utility CLI for inspecting MCP chat conversations stored in Turso.

Examples:
  export TURSO_DATABASE_URL=libsql://foo.turso.io
  export TURSO_AUTH_TOKEN=...
  python turso_chat_cli.py conversations --limit 10
  python turso_chat_cli.py messages <conversation-id>
  python turso_chat_cli.py dump --pretty
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Awaitable, Dict, Optional

import click

from turso_db import TursoDatabase


async def _with_db(
    url: str,
    token: str,
    handler: Callable[[TursoDatabase], Awaitable[Any]],
) -> Any:
    """Connect to Turso, run handler, and ensure cleanup."""
    db = TursoDatabase(url, token)
    try:
        await db.connect()
        return await handler(db)
    finally:
        await db.close()


def _run(ctx: click.Context, handler: Callable[[TursoDatabase], Awaitable[Any]]) -> Any:
    """Run an async handler using the CLI context credentials."""

    async def runner() -> Any:
        return await _with_db(ctx.obj["url"], ctx.obj["token"], handler)

    return asyncio.run(runner())


def _print_table(rows: list[Dict[str, Any]]) -> None:
    """Basic left-aligned table printer."""
    if not rows:
        click.echo("No rows.")
        return
    headers = list(rows[0].keys())
    widths = {h: max(len(str(h)), *(len(str(row.get(h, ""))) for row in rows)) for h in headers}
    header_line = " | ".join(f"{h:<{widths[h]}}" for h in headers)
    sep = "-+-".join("-" * widths[h] for h in headers)
    click.echo(header_line)
    click.echo(sep)
    for row in rows:
        click.echo(" | ".join(f"{str(row.get(h, '')):<{widths[h]}}" for h in headers))


@click.group()
@click.option("--url", envvar="TURSO_DATABASE_URL", help="libsql:// URL for your Turso instance.")
@click.option("--token", envvar="TURSO_AUTH_TOKEN", help="Turso auth token / JWT.")
@click.pass_context
def cli(ctx: click.Context, url: Optional[str], token: Optional[str]) -> None:
    """Inspect stored MCP chat conversations and messages."""
    if ctx.resilient_parsing:
        ctx.obj = {}
        return
    if not url or not token:
        raise click.UsageError("Provide --url/--token or export TURSO_DATABASE_URL/TURSO_AUTH_TOKEN.")
    ctx.obj = {"url": url, "token": token}


@cli.command("conversations")
@click.option("--limit", default=20, show_default=True, help="How many rows to fetch.")
@click.option("--offset", default=0, show_default=True, help="Offset for pagination.")
@click.option("--json-output", is_flag=True, help="Emit raw JSON instead of a table.")
@click.pass_context
def list_conversations(ctx: click.Context, limit: int, offset: int, json_output: bool) -> None:
    """List stored conversations."""

    async def handler(db: TursoDatabase) -> list[Dict[str, Any]]:
        return await db.list_conversations(limit=limit, offset=offset)

    conversations = _run(ctx, handler)
    if json_output:
        click.echo(json.dumps(conversations, indent=2))
    else:
        rows = [
            {
                "id": conv["id"],
                "title": conv.get("title") or "New conversation",
                "model": conv.get("model") or "",
                "updated_at": conv.get("updated_at") or "",
            }
            for conv in conversations
        ]
        _print_table(rows)


@cli.command("messages")
@click.argument("conversation_id")
@click.option("--limit", default=0, show_default=True, help="Limit messages (0 = all).")
@click.option("--json-output", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def list_messages(ctx: click.Context, conversation_id: str, limit: int, json_output: bool) -> None:
    """Show messages for a conversation."""

    async def handler(db: TursoDatabase) -> Dict[str, Any]:
        conversation = await db.get_conversation(conversation_id)
        if not conversation:
            raise click.ClickException(f"Conversation {conversation_id} not found.")
        messages = await db.get_conversation_messages(conversation_id)
        if limit and limit > 0:
            messages = messages[-limit:]
        conversation["messages"] = messages
        return conversation

    convo = _run(ctx, handler)
    if json_output:
        click.echo(json.dumps(convo, indent=2))
        return

    click.echo(f"ID: {convo['id']}")
    click.echo(f"Title: {convo.get('title')}")
    click.echo(f"Model: {convo.get('model')}")
    click.echo(f"Updated: {convo.get('updated_at')}")
    if convo.get("metadata"):
        click.echo("Metadata:")
        click.echo(json.dumps(convo["metadata"], indent=2))
    click.echo("")
    for msg in convo.get("messages", []):
        click.echo(f"[{msg.get('timestamp')}] {msg.get('role')}")
        content = msg.get("content") or ""
        if len(content) > 500:
            content = content[:500] + "\n… [truncated]"
        click.echo(content)
        if msg.get("tool_calls"):
            click.echo("tool_calls: " + json.dumps(msg["tool_calls"], indent=2))
        if msg.get("metadata"):
            click.echo("metadata: " + json.dumps(msg["metadata"], indent=2))
        click.echo("-" * 60)


@cli.command("dump")
@click.option("--pretty", is_flag=True, help="Pretty-print the JSON output.")
@click.pass_context
def dump_database(ctx: click.Context, pretty: bool) -> None:
    """Export all conversations + messages as JSON."""

    async def handler(db: TursoDatabase) -> Dict[str, Any]:
        conversations = await db.list_conversations(limit=10_000, offset=0)
        full = []
        for conv in conversations:
            msgs = await db.get_conversation_messages(conv["id"])
            conv_copy = dict(conv)
            conv_copy["messages"] = msgs
            full.append(conv_copy)
        return {"conversations": full}

    data = _run(ctx, handler)
    click.echo(json.dumps(data, indent=2 if pretty else None))


if __name__ == "__main__":
    cli()
