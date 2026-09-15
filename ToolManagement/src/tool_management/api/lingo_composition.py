"""Opt-in, bounded Lingo compositions over the existing ToolStore and run journal.

This is a synchronous operator-service profile, not a durable worker scheduler.
Reservation prevents re-dispatch of a retained key; it cannot make remote effects
exactly once or cancel work already accepted by another service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import time
from typing import Any, Literal
from uuid import uuid4

import httpx
from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

PROFILE = "lingo.v1"
MAX_JSON_BYTES = 131_072
MAX_REQUEST_BYTES = 65_536


def strict_json(text: str) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    def constant(_value):
        raise ValueError("nonfinite JSON number")

    value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    bounded_json(value)
    return value


def bounded_json(value: Any, limit: int = MAX_JSON_BYTES) -> str:
    def visit(item, depth=0):
        if depth > 32:
            raise ValueError("JSON depth exceeds 32")
        if item is None or type(item) in (str, bool):
            return
        if type(item) in (int, float):
            if not math.isfinite(item) or abs(item) > 9_007_199_254_740_991:
                raise ValueError("JSON number is outside the exact interoperability range")
            return
        if type(item) is list:
            for child in item:
                visit(child, depth + 1)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                visit(child, depth + 1)
            return
        raise ValueError("value is not JSON")

    visit(value)
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > limit:
        raise ValueError("JSON byte limit exceeded")
    return encoded


def _text(value: str, maximum: int = 128) -> str:
    if not value or len(value) > maximum or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("expected bounded nonempty text without control characters")
    return value


class LingoStep(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str
    name: str
    tool: str
    mode: Literal["passthrough", "map"] = "passthrough"
    input_from: str | None = None
    input_mapping: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=120, ge=0.01, le=120)

    @field_validator("id", "name")
    @classmethod
    def text_fields(cls, value):
        return _text(value)

    @field_validator("id")
    @classmethod
    def step_id(cls, value):
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", value):
            raise ValueError("step id must be an identifier")
        return value

    @field_validator("tool")
    @classmethod
    def tool_name(cls, value):
        _text(value, 200)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", value):
            raise ValueError("tool must be an identifier")
        return value

    @field_validator("input_mapping")
    @classmethod
    def mapping(cls, value):
        if len(value) > 32:
            raise ValueError("at most 32 mappings")
        for target, source in value.items():
            _text(target)
            _text(source)
        return value


class LingoWorkflow(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str
    name: str
    version: str
    steps: list[LingoStep] = Field(min_length=1, max_length=16)
    timeout_seconds: float = Field(default=120, ge=1, le=120)
    max_parallel_steps: Literal[1] = 1

    @field_validator("id", "name", "version")
    @classmethod
    def text_fields(cls, value):
        return _text(value)

    @field_validator("id", "version")
    @classmethod
    def identifiers(cls, value, info):
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", value) or len(value) > (64 if info.field_name == "version" else 128):
            raise ValueError("invalid workflow identifier")
        return value

    @field_validator("max_parallel_steps", mode="before")
    @classmethod
    def parallel_integer(cls, value):
        if type(value) is not int:
            raise ValueError("max_parallel_steps must be integer 1")
        return value

    @model_validator(mode="after")
    def ordered(self):
        seen = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError("duplicate step id")
            if step.input_from is not None and step.input_from not in seen:
                raise ValueError("input_from must reference an earlier step")
            seen.add(step.id)
        return self


class LingoRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    execution_profile: Literal["lingo.v1"]
    workflow: LingoWorkflow
    inputs: dict[str, Any]
    idempotency_key: str
    trace_id: str | None = None

    @field_validator("idempotency_key")
    @classmethod
    def key(cls, value):
        _text(value)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", value):
            raise ValueError("idempotency_key must be an identifier")
        return value

    @field_validator("trace_id")
    @classmethod
    def trace(cls, value):
        return None if value is None else _text(value)


def require_operator(request: Request):
    from .tools_api import resolve_execution_identity
    if os.getenv("REQUIRE_BEARER_AUTH", "1") != "1" and not request.headers.get("authorization"):
        return
    if resolve_execution_identity(request).get("kind") != "operator_service":
        raise HTTPException(403, detail={"error": "lingo_profile_requires_operator_service"})


def _reserve(store, request: LingoRequest, digest: str) -> tuple[str, bool]:
    """One atomic durable claim, retained for the lifetime of the execution row."""
    now = time.time()
    execution_id = str(uuid4())
    metadata = {"execution_profile": PROFILE, "request_hash": digest,
                "workflow_version": request.workflow.version,
                "definition": request.workflow.model_dump(mode="json"),
                "idempotency_retention": "execution_record_lifetime",
                "effect_semantics": "remote effects are not exactly once"}
    with store._lock:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT execution_id,request_hash FROM workflow_executions "
                "WHERE workflow_kind=? AND idempotency_key=? ORDER BY created_at LIMIT 1",
                (PROFILE, request.idempotency_key),
            ).fetchone()
            if row:
                if row["request_hash"] != digest:
                    raise HTTPException(409, detail={"error": "idempotency_conflict", "execution_id": row["execution_id"]})
                return row["execution_id"], True
            conn.execute(
                "INSERT INTO workflow_executions "
                "(execution_id,workflow_id,workflow_name,workflow_kind,workflow_type,trace_id,"
                "idempotency_key,request_hash,status,inputs_json,started_at,metadata_json,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (execution_id, request.workflow.id, request.workflow.name, PROFILE, PROFILE,
                 request.trace_id, request.idempotency_key, digest, "running",
                 bounded_json(request.inputs), now, bounded_json(metadata), now, now),
            )
    return execution_id, False


def _receipt(store, execution_id: str, replay: bool = False) -> dict:
    row = store.get_workflow_execution(execution_id, include_steps=True, include_events=False, include_artifacts=False)
    if not row:
        raise HTTPException(503, detail={"error": "execution_receipt_unavailable", "execution_id": execution_id})
    return {
        "execution_profile": PROFILE, "request_hash": row["request_hash"],
        "execution_id": execution_id, "workflow_id": row["workflow_id"],
        "workflow_name": row["workflow_name"], "status": row["status"],
        "output": row.get("output"),
        "step_results": {step["step_key"]: step["output"] for step in row.get("steps", []) if step["status"] == "completed"},
        "duration_ms": float(row.get("duration_ms") or 0), "error": row.get("error"),
        "trace_id": row.get("trace_id"), "idempotent_replay": replay,
        "idempotency_key": row.get("idempotency_key"),
    }


def reconcile_key(key: str, http_request: Request, store) -> dict:
    require_operator(http_request)
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key):
        raise HTTPException(422, detail={"error": "invalid_idempotency_key"})
    with store._conn() as conn:
        row = conn.execute(
            "SELECT execution_id FROM workflow_executions WHERE workflow_kind=? AND idempotency_key=? ORDER BY created_at LIMIT 1",
            (PROFILE, key),
        ).fetchone()
    if not row:
        raise HTTPException(404, detail={"error": "execution_not_found"})
    return _receipt(store, row["execution_id"], True)


def inspection_receipt(execution: dict) -> dict:
    """Drop redundant SQLite JSON text columns; never truncate evidence values."""
    result = {key: value for key, value in execution.items() if not key.endswith("_json")}
    for collection in ("steps", "events", "artifacts", "side_effects"):
        if collection in result:
            result[collection] = [{key: value for key, value in row.items() if not key.endswith("_json")}
                                  for row in result[collection]]
    try:
        bounded_json(result)
    except (ValueError, UnicodeError):
        raise HTTPException(413, detail={"error": "execution_inspection_too_large", "execution_id": execution["execution_id"],
                                          "reconcile_by_idempotency_key": True}) from None
    return result


class RemoteFailure(RuntimeError):
    def __init__(self, code: str, *, effect_unknown: bool = False):
        super().__init__(code)
        self.effect_unknown = effect_unknown


def unwrap_mcp(payload: Any, request_id: str) -> Any:
    if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0" or payload.get("id") != request_id:
        raise RemoteFailure("invalid_mcp_envelope", effect_unknown=True)
    if "error" in payload:
        raise RemoteFailure("mcp_protocol_error", effect_unknown=True)
    result = payload.get("result")
    if not isinstance(result, dict) or ("isError" in result and type(result["isError"]) is not bool):
        raise RemoteFailure("invalid_mcp_result", effect_unknown=True)
    if result.get("isError"):
        raise RemoteFailure("mcp_tool_error")
    if "structuredContent" in result:
        output = result["structuredContent"]
        if not isinstance(output, dict):
            raise RemoteFailure("invalid_mcp_structured_content", effect_unknown=True)
    else:
        content = result.get("content")
        if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict) or content[0].get("type") != "text" or not isinstance(content[0].get("text"), str):
            raise RemoteFailure("expected_one_json_text_result", effect_unknown=True)
        try:
            output = strict_json(content[0]["text"])
        except (ValueError, UnicodeError):
            raise RemoteFailure("invalid_tool_json", effect_unknown=True) from None
    if isinstance(output, dict) and (
        output.get("error") not in (None, False, "") or output.get("isError") is True
        or output.get("ok") is False or output.get("success") is False
        or output.get("status") in ("failed", "error", "cancelled", "timeout")
    ):
        raise RemoteFailure("tool_reported_failure")
    try:
        bounded_json(output)
    except (ValueError, UnicodeError):
        raise RemoteFailure("invalid_tool_json", effect_unknown=True) from None
    return output


async def _invoke(backend_id: str, url: str, name: str, inputs: dict, request_id: str, timeout: float) -> Any:
    from .tools_api import _backend_headers
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False) as client:
            async with client.stream("POST", url.rstrip("/") + "/mcp", headers=_backend_headers(backend_id),
                                     json={"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {"name": name, "arguments": inputs}}) as response:
                if response.status_code != 200:
                    raise RemoteFailure("backend_http_error", effect_unknown=True)
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > MAX_JSON_BYTES:
                        raise RemoteFailure("backend_response_too_large", effect_unknown=True)
                try:
                    payload = strict_json(chunks.decode("utf-8"))
                except (ValueError, UnicodeError):
                    raise RemoteFailure("invalid_backend_json", effect_unknown=True) from None
                return unwrap_mcp(payload, request_id)
    except httpx.HTTPError:
        raise RemoteFailure("backend_transport_error", effect_unknown=True) from None


def _mapped(value: Any, mapping: dict[str, str]) -> dict:
    if mapping:
        result = {}
        for target, source in mapping.items():
            if source == "$":
                result[target] = value
            elif isinstance(value, dict) and source in value:
                result[target] = value[source]
            else:
                raise ValueError("input_mapping source is absent")
    else:
        result = value
    if not isinstance(result, dict):
        raise ValueError("tool arguments must be an object; use '$' mapping to wrap a value")
    from .tools_api import assert_safe_tool_arguments
    assert_safe_tool_arguments(result)
    bounded_json(result)
    return result


async def execute_profile(raw: Any, http_request: Request, store) -> dict:
    require_operator(http_request)
    try:
        bounded_json(raw, MAX_REQUEST_BYTES)
        request = LingoRequest.model_validate(raw)
        from .tools_api import assert_safe_tool_arguments
        assert_safe_tool_arguments(request.inputs)
    except ValidationError:
        raise HTTPException(422, detail={"error": "invalid_lingo_composition", "reason": "request does not satisfy the bounded profile"}) from None
    except (ValueError, UnicodeError):
        raise HTTPException(422, detail={"error": "invalid_lingo_composition", "reason": "request exceeds JSON interoperability bounds"}) from None
    request.trace_id = request.trace_id or "tr_" + uuid4().hex
    definition = request.workflow.model_dump(mode="json")
    digest = hashlib.sha256(bounded_json({"execution_profile": PROFILE, "workflow": definition, "inputs": request.inputs}).encode()).hexdigest()
    execution_id, replay = _reserve(store, request, digest)
    if replay:
        return _receipt(store, execution_id, True)

    from ..persistence import get_tool_store
    started = time.monotonic()
    deadline = started + request.workflow.timeout_seconds
    results = {}
    current = request.inputs
    leaves = 0
    active_step = None
    step_started = None
    attempts = []
    metadata = {"execution_profile": PROFILE, "request_hash": digest,
                "workflow_version": request.workflow.version, "definition": definition,
                "idempotency_retention": "execution_record_lifetime",
                "effect_semantics": "remote effects are not exactly once"}

    def write_step(status, output=None, error=None):
        store.upsert_workflow_step(
            execution_id=execution_id, step_key=active_step.id, step_name=active_step.name,
            step_index=len(results), tool_name=active_step.tool, mode=active_step.mode,
            trace_id=request.trace_id, status=status, started_at=step_started,
            completed_at=time.time() if status != "running" else None,
            output_data=output, error=error, metadata={"attempts": attempts},
        )

    try:
        for step in request.workflow.steps:
            active_step, step_started, attempts = step, time.time(), []
            selected = results[step.input_from] if step.input_from is not None else current
            if step.mode == "map":
                if not isinstance(selected, list) or len(selected) > 32:
                    raise ValueError("map requires an array of at most 32 elements")
                items = selected
            else:
                items = [selected]
            if leaves + len(items) > 128:
                raise ValueError("workflow exceeds 128 leaf calls")
            # Resolve and validate every argument for this step before its first call.
            arguments = [_mapped(item, step.input_mapping) for item in items]
            backend_id, backend_url, name = get_tool_store().resolve_tool(step.tool)
            if not get_tool_store().get_tool_by_name(name, backend_id=backend_id):
                raise ValueError("tool is not registered on the selected backend")
            write_step("running")
            step_deadline = min(deadline, time.monotonic() + step.timeout_seconds)
            outputs = []
            for index, args in enumerate(arguments):
                remaining = step_deadline - time.monotonic()
                if remaining <= 0:
                    raise RemoteFailure("workflow_deadline_exceeded")
                attempt = {"index": index, "backend_id": backend_id, "tool": name,
                           "mcp_request_id": str(uuid4()), "status": "issued"}
                attempts.append(attempt)
                write_step("running")  # durable issued record precedes network dispatch
                leaves += 1
                try:
                    output = await asyncio.wait_for(_invoke(backend_id, backend_url, name, args, attempt["mcp_request_id"], remaining), timeout=remaining)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    attempt["status"] = "effect_unknown"
                    raise RemoteFailure("workflow_deadline_exceeded", effect_unknown=True) from None
                except RemoteFailure as exc:
                    attempt["status"] = "effect_unknown" if exc.effect_unknown else "failed"
                    raise
                attempt["status"] = "completed"
                outputs.append(output)
                write_step("running")
            current = outputs if step.mode == "map" else outputs[0]
            bounded_json({**results, step.id: current}, 60 * 1024)
            write_step("completed", output=current)
            results[step.id] = current
        status, error = "completed", None
    except Exception as exc:
        status = "failed"
        error = str(exc)[:256] if isinstance(exc, (ValueError, RemoteFailure)) else "workflow_execution_failed"
        if active_step:
            write_step("failed", error=error)
        metadata["effect_unknown"] = any(attempt["status"] in ("issued", "effect_unknown") for attempt in attempts)
    store.upsert_workflow_execution(
        execution_id=execution_id, workflow_id=request.workflow.id, workflow_name=request.workflow.name,
        workflow_kind=PROFILE, workflow_type=PROFILE, trace_id=request.trace_id,
        idempotency_key=request.idempotency_key, request_hash=digest, status=status,
        output=current if status == "completed" else None, error=error,
        completed_at=time.time(), duration_ms=(time.monotonic() - started) * 1000,
        metadata=metadata,
    )
    return _receipt(store, execution_id)
