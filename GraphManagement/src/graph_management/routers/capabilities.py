"""Capabilities router for managing capability tokens."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..security import require_permissions, get_principal, Principal
from ..schemas import (
    CapabilityTokenCreateRequest,
    CapabilityTokenResponse,
)
from .dependencies import (
    neo4j_query_one,
    sanitize_label,
)

router = APIRouter(prefix="/v1/capabilities", tags=["capabilities"])


@router.post("", response_model=CapabilityTokenResponse)
async def create_capability(
    req: CapabilityTokenCreateRequest,
    _: Principal = Depends(require_permissions(["policy:write"])),
    principal: Principal = Depends(get_principal),
):
    """Create a new capability token for granular access control."""
    # Validate target fields based on kind
    if req.kind == "writer" and not req.target_label:
        raise HTTPException(status_code=400, detail="target_label is required for writer capability")
    if req.kind == "linker" and not req.relationship_type:
        raise HTTPException(status_code=400, detail="relationship_type is required for linker capability")

    label = sanitize_label(req.target_label) if req.target_label else None
    rtype = sanitize_label(req.relationship_type) if req.relationship_type else None
    subject = req.subject or principal.sub

    # Create token
    rec = await neo4j_query_one(
        """
        CREATE (c:CapabilityToken {
          id: toString(randomUUID()),
          sub: $sub,
          kind: $kind,
          target_label: $label,
          relationship_type: $rtype,
          version: $version,
          budget_per_day: $budget,
          used_today: 0,
          day_key: toString(date()),
          created_at: datetime(),
          note: $note
        })
        WITH c
        FOREACH (_ IN CASE WHEN $expires IS NULL THEN [] ELSE [1] END |
          SET c.expires_at = datetime($expires)
        )
        RETURN c
        """,
        {
            "sub": subject,
            "kind": req.kind,
            "label": label,
            "rtype": rtype,
            "version": req.version,
            "budget": int(req.budget_per_day or 0),
            "expires": req.expires_at,
            "note": req.note,
        },
    )
    c = rec.get("c", {}).get("properties", {}) if rec else {}
    return CapabilityTokenResponse(
        id=c.get("id"),
        kind=c.get("kind"),
        target_label=c.get("target_label"),
        relationship_type=c.get("relationship_type"),
        version=c.get("version"),
        subject=c.get("sub"),
        budget_per_day=int(c.get("budget_per_day", 0) or 0),
        used_today=int(c.get("used_today", 0) or 0),
        day_key=c.get("day_key"),
        expires_at=c.get("expires_at"),
        note=c.get("note"),
    )


@router.get("/{cid}", response_model=CapabilityTokenResponse)
async def get_capability(cid: str, _: Principal = Depends(require_permissions(["policy:read"]))):
    """Get a capability token by ID."""
    rec = await neo4j_query_one("MATCH (c:CapabilityToken {id: $id}) RETURN c", {"id": cid})
    if not rec:
        raise HTTPException(status_code=404, detail="Capability not found")
    c = rec["c"]["properties"]
    return CapabilityTokenResponse(
        id=c.get("id"),
        kind=c.get("kind"),
        target_label=c.get("target_label"),
        relationship_type=c.get("relationship_type"),
        version=c.get("version"),
        subject=c.get("sub"),
        budget_per_day=int(c.get("budget_per_day", 0) or 0),
        used_today=int(c.get("used_today", 0) or 0),
        day_key=c.get("day_key"),
        expires_at=c.get("expires_at"),
        note=c.get("note"),
    )
