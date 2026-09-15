"""Offline regression for the login page's HTML/JavaScript boundary.

Execute the actual handler with a stub registry, without importing persistence
or booting the application (which can initialize external integrations).
"""
import ast
import asyncio
import html
import json
from pathlib import Path

import pytest
from fastapi import HTTPException, Query
from fastapi.responses import HTMLResponse


@pytest.mark.parametrize("mode,expected", [
    ("login", "login"),
    ("signup", "signup"),
    ('</script><script>/*regression-marker*/</script>', "login"),
    ('\";alert(1);//', "login"),
])
def test_authorize_mode_cannot_escape_inline_script(mode, expected):
    path = Path(__file__).parents[1] / "src/tool_management/oauth.py"
    node = next(n for n in ast.parse(path.read_text()).body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "authorize_form")
    node.decorator_list = []

    class Store:
        def get_client(self, _):
            return {"name": "test", "redirect_uris": ["http://localhost/callback"],
                    "token_endpoint_auth_method": "none"}

    namespace = {"Request": object, "Query": Query, "HTTPException": HTTPException,
                 "HTMLResponse": HTMLResponse, "html": html, "json": json,
                 "_base_url": lambda _: "https://example.test",
                 "_oauth_store": Store, "_SCOPE_DESCRIPTIONS": {}}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    response = asyncio.run(namespace["authorize_form"](
        None, client_id="test", redirect_uri="http://localhost/callback",
        response_type="code", state="state", code_challenge="challenge",
        code_challenge_method="S256", scope="mcp", mode=mode))
    body = response.body.decode()
    assert response.status_code == 200
    assert f'setMode({json.dumps(expected)});' in body
    assert '</script><script>/*regression-marker*/</script>' not in body
