"""
Secret store tools — agent-facing surface for credential management.

These tools let an agent (or operator) manage and *activate* secrets by name
without ever seeing the plaintext. Activation exports the resolved value into
the process environment, so downstream tools that read ``os.environ`` (the
common case) just work — no key needs to be pasted into chat or passed as a
visible argument.

Security stance (matches ``codex_apps_vault``): plaintext values are never
returned. ``secret_resolve`` reports existence + source + a masked preview only.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..registry import tool
from ..models import ToolVisibility
from ..core.secret_store import get_secret_store, redact, DEFAULT_SCOPE

_TAGS = ["secrets", "credentials", "security", "config"]


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.PUBLIC,
)
def secret_status() -> Dict[str, object]:
    """Report the secret store backend and stored scopes (no values)."""
    return get_secret_store().status()


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.PUBLIC,
)
def secret_list(scope: Optional[str] = None) -> Dict[str, List[str]]:
    """List stored secret scopes and their key names. Values are never returned.

    If ``scope`` is given, returns ``{scope: [keys]}`` for that scope only.
    """
    store = get_secret_store()
    if scope:
        return {scope: store.list_keys(scope)}
    return {s: store.list_keys(s) for s in store.list_scopes()}


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.PUBLIC,
)
def secret_has(name: str, scope: str = DEFAULT_SCOPE) -> Dict[str, object]:
    """Check whether a secret is resolvable (keychain, env, or .env)."""
    value, source = get_secret_store().resolve_with_source(name, scope=scope)
    return {"name": name, "present": value is not None, "source": source}


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.PUBLIC,
)
def secret_resolve(name: str, scope: str = DEFAULT_SCOPE) -> Dict[str, object]:
    """Resolve a secret by name and report its source + a masked preview.

    The plaintext is never returned. Use ``secret_activate`` to make a secret
    available to downstream tools for the current process.
    """
    value, source = get_secret_store().resolve_with_source(name, scope=scope)
    return {
        "name": name,
        "present": value is not None,
        "source": source,
        "preview": redact(value) if value is not None else "",
    }


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.PUBLIC,
)
def secret_activate(name: str, scope: str = DEFAULT_SCOPE) -> Dict[str, object]:
    """Resolve a secret and export it into the process environment by name.

    After activation, any tool that reads ``os.environ[name]`` (e.g. an
    OpenRouter client reading ``OPENROUTER_API_KEY``) will use the stored value.
    The plaintext is not surfaced. Returns existence, source, and a mask.
    """
    store = get_secret_store()
    value, source = store.resolve_with_source(name, scope=scope)
    if value is None:
        return {"name": name, "activated": False, "present": False, "source": None}
    store.activate(name, scope=scope)
    return {
        "name": name,
        "activated": True,
        "present": True,
        "source": source,
        "preview": redact(value),
    }


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.ORGANIZATION,
)
def secret_set(name: str, value: str, scope: str = DEFAULT_SCOPE) -> Dict[str, object]:
    """Store a secret in the secure backend (keychain on macOS, encrypted file otherwise).

    Prefer provisioning via the OS keychain or ``.env`` rather than passing the
    value through this call, which is visible in tool-call arguments.
    """
    ok = get_secret_store().set_secret(scope, name, value)
    return {"name": name, "scope": scope, "stored": ok, "preview": redact(value)}


@tool(
    tags=_TAGS,
    category="secrets",
    visibility=ToolVisibility.ORGANIZATION,
)
def secret_delete(name: str, scope: str = DEFAULT_SCOPE) -> Dict[str, object]:
    """Delete a stored secret from the secure backend (does not clear process env)."""
    deleted = get_secret_store().delete_secret(scope, name)
    return {"name": name, "scope": scope, "deleted": deleted}
