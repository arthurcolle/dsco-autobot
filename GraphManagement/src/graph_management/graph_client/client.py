from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple, Type, TypeVar, Union, get_args, get_origin

import httpx

from .models import GraphNodeModel, GraphRelationshipModel
from .registry import GraphRegistry
from ..schemas import (
    QueryRequest,
    UpsertNodeSchemaRequest,
    UpsertRelationshipSchemaRequest,
)

T = TypeVar("T")


class GraphManagementClient:
    """High-level client that offers decorators for Pydantic-based graph models."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        session: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._session = session or httpx.AsyncClient()
        self._owns_session = session is None
        self.registry = GraphRegistry()

    # ------------------------------------------------------------------
    # Decorators
    # ------------------------------------------------------------------

    def node(
        self,
        *,
        label: str | None = None,
        description: str | None = None,
        indexes: Sequence[Dict[str, Any]] | None = None,
        constraints: Sequence[Dict[str, Any]] | None = None,
        embedding: Sequence[float] | None = None,
    ) -> Callable[[Type[GraphNodeModel]], Type[GraphNodeModel]]:
        """Decorator used to register a Pydantic model as a graph node."""

        def decorator(cls: Type[GraphNodeModel]) -> Type[GraphNodeModel]:
            self.registry.register_node(
                cls,
                label=label,
                description=description,
                indexes=indexes,
                constraints=constraints,
                embedding=embedding,
            )
            setattr(cls, "__graph_client__", self)
            return cls

        return decorator

    def relationship(
        self,
        type_: str | None = None,
        *,
        start: Sequence[str] | Type[GraphNodeModel] | str,
        end: Sequence[str] | Type[GraphNodeModel] | str,
        direction: str = "outgoing",
        description: str | None = None,
        indexes: Sequence[Dict[str, Any]] | None = None,
        constraints: Sequence[Dict[str, Any]] | None = None,
    ) -> Callable[[Type[GraphRelationshipModel]], Type[GraphRelationshipModel]]:
        """Decorator used to register relationship models."""

        def decorator(cls: Type[GraphRelationshipModel]) -> Type[GraphRelationshipModel]:
            self.registry.register_relationship(
                cls,
                type_=type_,
                start=start,
                end=end,
                direction=direction,
                description=description,
                indexes=indexes,
                constraints=constraints,
            )
            setattr(cls, "__graph_client__", self)
            return cls

        return decorator

    def query(
        self,
        cypher: str,
        *,
        returns: Any | None = None,
        strategy: str = "all",
        record_key: str | None = None,
    ) -> Callable[[Callable[..., Awaitable[Dict[str, Any]] | Dict[str, Any]]], Callable[..., Awaitable[Any]]]:
        """Wrap a function that produces query parameters into a runnable async query."""

        def decorator(
            func: Callable[..., Awaitable[Dict[str, Any]] | Dict[str, Any]],
        ) -> Callable[..., Awaitable[Any]]:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                params = await self._maybe_await(func(*args, **kwargs))
                if params is None:
                    params = {}
                if not isinstance(params, dict):
                    raise TypeError("Query parameter function must return a dict")
                data = await self.run_query(cypher, params, strategy=strategy)
                return self._decode_query_response(data, returns, record_key, strategy)
            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def run_query(self, query: str, parameters: Dict[str, Any] | None = None, *, strategy: str = "default") -> Dict[str, Any]:
        payload = QueryRequest(query=query, parameters=parameters or {}, strategy=strategy).model_dump()
        return await self._post("/v1/query", json=payload)

    async def sync_schema(self) -> Dict[str, List[Dict[str, Any]]]:
        """Upsert all registered node and relationship schemas to the server."""
        results: Dict[str, List[Dict[str, Any]]] = {"nodes": [], "relationships": []}
        for spec in self.registry.node_specs():
            req = UpsertNodeSchemaRequest(spec=spec)
            resp = await self._post("/v1/registry/nodes", json=req.model_dump())
            results["nodes"].append(resp)
        for spec in self.registry.relationship_specs():
            req = UpsertRelationshipSchemaRequest(spec=spec)
            resp = await self._post("/v1/registry/relationships", json=req.model_dump())
            results["relationships"].append(resp)
        return results

    async def aclose(self) -> None:
        if self._owns_session:
            await self._session.aclose()

    async def __aenter__(self) -> GraphManagementClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _post(self, path: str, *, json: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("POST", path, json=json)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        url = self._absolute_url(path)
        headers = kwargs.pop("headers", {})
        if self.token:
            headers.setdefault("Authorization", f"Bearer {self.token}")
        response = await self._session.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        if "application/json" in response.headers.get("content-type", ""):
            return response.json()
        return {}

    def _absolute_url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    async def _maybe_await(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    def _decode_query_response(self, data: Dict[str, Any], returns: Any | None, record_key: str | None, strategy: str) -> Any:
        if returns is None:
            return data.get("results") or data.get("result")
        origin = get_origin(returns)
        if origin in (list, List):
            inner = get_args(returns)[0]
            rows = data.get("results", [])
            return [self._convert_record(row, inner, record_key) for row in rows]
        target_type, _ = self._strip_optional(returns)
        if strategy == "one":
            value = data.get("result")
            if value is None:
                return None
            if record_key and isinstance(value, dict):
                value = value.get(record_key)
            return self._convert_value(value, target_type)
        rows = data.get("results", [])
        if not rows:
            return None
        return self._convert_record(rows[0], target_type, record_key)

    def _convert_record(self, record: Dict[str, Any], target: Any, record_key: str | None) -> Any:
        if record_key:
            if record_key not in record:
                raise KeyError(f"Expected key '{record_key}' in query result")
            value = record[record_key]
        elif isinstance(record, dict) and len(record) == 1:
            value = next(iter(record.values()))
        else:
            value = record
        return self._convert_value(value, target)

    def _convert_value(self, value: Any, target: Any) -> Any:
        if value is None:
            return None
        target, _ = self._strip_optional(target)
        if inspect.isclass(target) and issubclass(target, GraphNodeModel):
            return target.from_serialized(value)
        if inspect.isclass(target) and issubclass(target, GraphRelationshipModel):
            return target.from_serialized(value)
        return value

    def _strip_optional(self, target: Any) -> Tuple[Any, bool]:
        origin = get_origin(target)
        if origin is Union:
            args = [arg for arg in get_args(target) if arg is not type(None)]  # noqa: E721
            if len(args) == 1:
                return args[0], True
        return target, False
