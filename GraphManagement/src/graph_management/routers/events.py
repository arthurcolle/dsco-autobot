"""Events router for event streaming endpoints."""

from __future__ import annotations

import asyncio
import json as _json

from fastapi import APIRouter, Depends
from starlette.responses import StreamingResponse

from ..security import require_permissions, Principal
from .dependencies import (
    neo4j_query_all,
)

router = APIRouter(prefix="/v1/events", tags=["events"])


@router.get("/policy/stream")
async def policy_events_stream(_: Principal = Depends(require_permissions(["events:read"]))):
    """Stream policy decision events as Server-Sent Events."""
    async def event_stream():
        seen: set[str] = set()
        try:
            while True:
                rows = await neo4j_query_all(
                    "MATCH (e:Event {type: 'policy_decision'}) RETURN e ORDER BY e.ts DESC LIMIT 20",
                    {},
                )
                out = []
                for r in reversed(rows):
                    e = r["e"]["properties"]
                    eid = e.get("id")
                    if eid in seen:
                        continue
                    seen.add(eid)
                    out.append(e)
                for e in out:
                    yield f"event: policy_decision\n"
                    yield f"data: {_json.dumps(e)}\n\n"
                yield ": ping\n\n"
                await asyncio.sleep(3)
        except Exception as exc:
            err = {"error": str(exc)}
            yield "event: error\n"
            yield f"data: {_json.dumps(err)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)
