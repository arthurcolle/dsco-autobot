"""Focused, network-free coverage for private GraphSub reconciliation."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tool_management.services.graph_reconciliation import (
    group_catalog_tools_by_backend,
    reconcile_persistent_catalog,
)


MODAL_APP = Path(__file__).resolve().parents[1] / "deploy" / "modal" / "modal_app.py"


def _modal_tree():
    return ast.parse(MODAL_APP.read_text(encoding="utf-8"))


def _function(tree, name):
    return next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )


def _decorator_names(node):
    names = set()
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name):
            names.add(f"{target.value.id}.{target.attr}")
    return names


def _called_names(node):
    return {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }


def test_modal_reconcile_is_private_and_shares_runtime_configuration():
    tree = _modal_tree()
    web = _function(tree, "web")
    reconcile = _function(tree, "reconcile_graphsub_projection")

    assert "app.function" in _decorator_names(reconcile)
    assert "modal.asgi_app" not in _decorator_names(reconcile)
    assert "modal.asgi_app" in _decorator_names(web)
    assert "_configure_production_environment" in _called_names(web)
    assert "_configure_production_environment" in _called_names(reconcile)
    assert "_project_announced_tools_to_graph" in _called_names(reconcile)
    assert "reconcile_persistent_catalog" in _called_names(reconcile)

    app_function = next(
        decorator
        for decorator in reconcile.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "function"
    )
    keyword_names = {
        keyword.arg: keyword.value.id
        for keyword in app_function.keywords
        if keyword.arg and isinstance(keyword.value, ast.Name)
    }
    assert keyword_names["image"] == "image"
    assert keyword_names["volumes"] == "registry_volumes"
    assert keyword_names["secrets"] == "registry_secrets"


def test_modal_production_environment_is_shared_and_contains_no_credentials():
    tree = _modal_tree()
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "PRODUCTION_ENV" for target in node.targets)
    )
    environment = ast.literal_eval(assignment.value)

    assert environment["DATA_DIR"] == "/data"
    assert environment["DSCO_DEPLOYMENT_MODE"] == "production"
    assert environment["GRAPH_SUBSTRATE"] == "graphsub"
    assert environment["SYNC_ON_STARTUP"] == "0"
    assert environment["ENABLE_ANNOUNCE_GRAPH_PROJECTION"] == "1"
    assert not {
        "BACKEND_BEARER_TOKEN",
        "GRAPHSUB_BEARER_TOKEN",
        "DEFAULT_BEARER_TOKEN",
        "INTERNAL_API_BEARER_TOKEN",
        "MCP_PASSWORD",
        "JINA_API_KEY",
        "PARALLEL_API_KEY",
        "TENANT_VAULT_MASTER_KEY",
        "CODEX_VAULT_MASTER_KEY",
    }.intersection(environment)


def test_catalog_tools_are_grouped_in_the_existing_announce_shape():
    grouped = group_catalog_tools_by_backend([
        SimpleNamespace(
            backend_id="alpha",
            backend_url="https://alpha.example",
            name="first",
            description="First tool",
        ),
        SimpleNamespace(
            backend_id="beta",
            backend_url="https://beta.example",
            name="second",
            description="Second tool",
        ),
        SimpleNamespace(
            backend_id="alpha",
            backend_url="https://alpha.example",
            name="third",
            description="Third tool",
        ),
    ])

    assert list(grouped) == ["alpha", "beta"]
    assert grouped["alpha"].backend_url == "https://alpha.example"
    assert grouped["alpha"].tools == [
        {"name": "first", "description": "First tool"},
        {"name": "third", "description": "Third tool"},
    ]


@pytest.mark.asyncio
async def test_reconcile_pages_groups_and_returns_only_sanitized_counts():
    catalog = [
        SimpleNamespace(
            backend_id="alpha",
            backend_url="https://alpha.example/private",
            name="secret-tool-one",
            description="payload-marker-one",
        ),
        SimpleNamespace(
            backend_id="beta",
            backend_url="https://beta.example/private",
            name="secret-tool-two",
            description="payload-marker-two",
        ),
        SimpleNamespace(
            backend_id="alpha",
            backend_url="https://alpha.example/private",
            name="secret-tool-three",
            description="payload-marker-three",
        ),
    ]

    class FakeStore:
        def __init__(self):
            self.calls = []

        def list_tools(self, *, limit, offset):
            self.calls.append((limit, offset))
            return catalog[offset:offset + limit]

    class ProjectionFailure(RuntimeError):
        pass

    projected = []

    async def project(backend_id, tools, backend_url):
        projected.append((backend_id, tools, backend_url))
        if backend_id == "beta":
            raise ProjectionFailure("payload-marker-two and an internal token")

    store = FakeStore()
    summary = await reconcile_persistent_catalog(store, project, page_size=2)

    assert store.calls == [(2, 0), (2, 2)]
    assert [call[0] for call in projected] == ["alpha", "beta"]
    assert [tool["name"] for tool in projected[0][1]] == [
        "secret-tool-one",
        "secret-tool-three",
    ]
    assert summary == {
        "catalog_tool_count": 3,
        "backend_count": 2,
        "projected_tool_count": 2,
        "failed_tool_count": 1,
        "error_count": 1,
        "errors": {"ProjectionFailure": 1},
    }

    serialized = json.dumps(summary)
    assert "payload-marker" not in serialized
    assert "secret-tool" not in serialized
    assert "alpha.example" not in serialized
    assert "internal token" not in serialized


@pytest.mark.asyncio
async def test_strict_projection_reuses_announce_transaction_logic_without_network(monkeypatch):
    from tool_management.api import tools_api
    from tool_management.services import graph_client

    class FakeGraph:
        def __init__(self):
            self.statements = None

        async def execute_transaction(self, statements):
            self.statements = statements

    graph = FakeGraph()
    monkeypatch.setattr(graph_client, "get_graph_substrate_client", lambda: graph)

    count = await tools_api._project_announced_tools_to_graph(
        "alpha",
        [
            {"name": "first", "description": "First tool"},
            {"name": "second", "description": "Second tool"},
        ],
        "https://alpha.example",
        raise_errors=True,
    )

    assert count == 2
    assert [statement["parameters"]["tid"] for statement in graph.statements] == [
        "alpha:first",
        "alpha:second",
    ]
