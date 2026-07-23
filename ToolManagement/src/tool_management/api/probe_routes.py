"""
FastAPI routes for realtime MCP catalog probing and topology mapping.

Live connectivity testing, health scoring, and dynamic topology
for all 800+ MCP server integrations.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/integrations/probe", tags=["probe"])

# Singleton prober (lazy init)
_prober = None


def _get_prober():
    global _prober
    if _prober is None:
        from ..integrations.catalog_prober import CatalogProber
        _prober = CatalogProber()
    return _prober


# =============================================================================
# Request/Response models
# =============================================================================

class ProbeRequest(BaseModel):
    preset_ids: Optional[List[str]] = Field(None, description="Specific presets to probe (all if omitted)")
    categories: Optional[List[str]] = Field(None, description="Filter by categories")
    tiers: Optional[List[str]] = Field(None, description="Filter by tiers")
    concurrency: int = Field(40, ge=1, le=100)
    layers: Optional[List[str]] = Field(None, description="Probe layers: registry, repo, spawn, transport")


class StackProbeRequest(BaseModel):
    preset_ids: List[str]


# =============================================================================
# PROBE ROUTES
# =============================================================================


@router.post("/run")
async def run_probe(req: ProbeRequest):
    """
    Run a full probe across the catalog (or filtered subset).
    Tests package registry existence, GitHub repo liveness, and more.

    Returns comprehensive report with per-preset health scores.
    """
    from ..integrations.catalog_prober import ProbeLayer

    prober = _get_prober()
    layers = None
    if req.layers:
        layers = [ProbeLayer(l) for l in req.layers]

    report = await prober.probe_all(
        concurrency=req.concurrency,
        layers=layers,
        categories=req.categories,
        tiers=req.tiers,
        preset_ids=req.preset_ids,
    )
    return report


@router.post("/preset/{preset_id}")
async def probe_single(preset_id: str):
    """Probe a single preset and return detailed results."""
    prober = _get_prober()
    result = await prober.probe_preset(preset_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    return result


@router.post("/category/{category}")
async def probe_category(category: str):
    """Probe all presets in a category."""
    prober = _get_prober()
    return await prober.probe_category(category)


@router.post("/tier/{tier}")
async def probe_tier(tier: str):
    """Probe all presets in a tier (official, vendor, community)."""
    prober = _get_prober()
    return await prober.probe_tier(tier)


# =============================================================================
# TOPOLOGY ROUTES
# =============================================================================


@router.get("/topology")
async def get_topology():
    """
    Get the full live topology map: nodes, edges, and stats.

    Nodes are presets with health scores; edges are dependency relationships.
    Call /probe/run first to populate live data.
    """
    prober = _get_prober()
    return prober.get_topology()


@router.get("/topology/{preset_id}")
async def get_topology_node(preset_id: str):
    """Get a single topology node with full probe data."""
    prober = _get_prober()
    result = prober.get_node(preset_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    return result


@router.get("/topology/categories")
async def get_category_map():
    """Get topology grouped by category with health averages."""
    prober = _get_prober()
    return prober.get_category_map()


@router.get("/topology/export")
async def export_topology(format: str = Query("json", description="Export format: json or table")):
    """Export the full topology as JSON or compact table."""
    prober = _get_prober()
    if format == "table":
        return {"table": prober.export_status_table()}
    return {"topology": prober.get_topology()}


# =============================================================================
# STACK HEALTH
# =============================================================================


@router.post("/stack-health")
async def stack_health(req: StackProbeRequest):
    """
    Probe a specific stack of presets and return aggregate health.
    Includes dependency health propagation.
    """
    prober = _get_prober()
    report = await prober.probe_all(preset_ids=req.preset_ids)

    # Get node details
    nodes = []
    for pid in req.preset_ids:
        node = prober.get_node(pid)
        if node:
            nodes.append(node)

    return {
        "stack": req.preset_ids,
        "probe_report": report,
        "nodes": nodes,
    }


# =============================================================================
# BACKGROUND MONITORING
# =============================================================================


@router.post("/start-monitoring")
async def start_monitoring(
    background_tasks: BackgroundTasks,
    interval_seconds: int = Query(300, ge=60, le=3600),
    concurrency: int = Query(30, ge=1, le=100),
):
    """
    Start continuous background probing. Runs every `interval_seconds`.
    The topology is automatically updated after each cycle.
    """
    prober = _get_prober()

    async def _monitor():
        await prober.continuous_probe(
            interval_seconds=interval_seconds,
            concurrency=concurrency,
        )

    background_tasks.add_task(_monitor)
    return {
        "status": "monitoring_started",
        "interval_seconds": interval_seconds,
        "concurrency": concurrency,
    }
