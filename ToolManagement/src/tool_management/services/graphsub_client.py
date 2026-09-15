"""Bounded GraphSub gateway adapter for registry metadata projection."""

import json
import re

import httpx


class GraphSubClient:
    """Use the authenticated /cypher gateway, never the legacy Neo4j API.

    Batches commit independently: a multi-batch call is not one atomic
    transaction. Callers may replay idempotent MERGE projections after failure.
    """

    MAX_TRANSACTION_BATCH_SIZE = 128
    MAX_QUERY_BYTES = 64 * 1024
    MAX_BATCH_QUERY_BYTES = 7 * 1024 * 1024
    # Consume literals/comments whole, so dollars within data are not bindings.
    _TOKENS = re.compile(
        r"'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\"|"
        r"`(?:``|[^`])*`|//[^\n]*|/\*[\s\S]*?\*/|\$([A-Za-z_][A-Za-z0-9_]*)"
    )

    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

    @classmethod
    def _cypher_literal(cls, value):
        if isinstance(value, dict):
            if any(not isinstance(k, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k) for k in value):
                raise ValueError("Unsafe GraphSub Cypher map keys")
            return "{" + ", ".join(k + ": " + cls._cypher_literal(v) for k, v in value.items()) + "}"
        if isinstance(value, list):
            return "[" + ", ".join(cls._cypher_literal(v) for v in value) + "]"
        if value is None or isinstance(value, (str, bool, int, float)):
            return json.dumps(value, ensure_ascii=True, allow_nan=False)
        raise ValueError("Unsupported GraphSub Cypher parameter type")

    @classmethod
    def _render_query(cls, query, parameters):
        if not isinstance(query, str) or not query.strip():
            raise ValueError("GraphSub query must be a non-empty string")
        if not isinstance(parameters, dict):
            raise ValueError("GraphSub parameters must be an object")

        def replace(match):
            name = match.group(1)
            if name is None:
                return match.group(0)
            if name not in parameters:
                raise ValueError("Missing GraphSub Cypher parameter: " + name)
            return cls._cypher_literal(parameters[name])

        rendered = cls._TOKENS.sub(replace, query)
        if len(rendered.encode('utf-8')) > cls.MAX_QUERY_BYTES:
            raise ValueError("GraphSub query exceeds gateway byte limit")
        return rendered

    async def execute_query(self, query, parameters=None, strategy="all"):
        rendered = self._render_query(query, parameters if parameters is not None else {})
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                self.base_url + '/cypher', headers=self.headers,
                json={"query": rendered},
            )
            response.raise_for_status()
            return response.json()

    async def execute_transaction(self, statements, strategy="all"):
        # Validate every statement before sending any write.
        rendered = [
            {"query": self._render_query(s["query"], s.get("parameters", {}))}
            for s in statements
        ]
        if not 1 <= self.MAX_TRANSACTION_BATCH_SIZE <= 128:
            raise ValueError("GraphSub batch size must be between 1 and 128")
        results = []
        batches = []
        batch, size, wire_size = [], 0, 32
        for statement in rendered:
            length = len(statement["query"].encode('utf-8'))
            wire_length = len(json.dumps(statement, ensure_ascii=True).encode('utf-8')) + 2
            if batch and (len(batch) >= self.MAX_TRANSACTION_BATCH_SIZE
                          or size + length > self.MAX_BATCH_QUERY_BYTES
                          or wire_size + wire_length > 8 * 1024 * 1024):
                batches.append(batch)
                batch, size, wire_size = [], 0, 32
            batch.append(statement)
            size += length
            wire_size += wire_length
        if batch:
            batches.append(batch)
        if not batches:
            return {"substrate": "graphsub", "results": []}
        async with httpx.AsyncClient(timeout=300.0) as client:
            for batch in batches:
                response = await client.post(
                    self.base_url + '/cypher', headers=self.headers,
                    json={"statements": batch},
                )
                response.raise_for_status()
                payload = response.json()
                results.extend(payload.get("results", [payload]))
        return {"substrate": "graphsub", "results": results}
