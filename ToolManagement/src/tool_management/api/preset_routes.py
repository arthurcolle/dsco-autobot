"""
FastAPI routes for MCP Server Preset Catalog.

Advanced integration catalog with:
- Semantic search and intelligent recommendations
- Dependency graph resolution and stack planning
- Conflict detection between integrations
- Enterprise readiness scoring
- Composite workflow templates
- Install script generation
- Catalog health assessment
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..integrations.preset_catalog import (
    PRESET_CATALOG,
    CatalogEngine,
    COMPOSITE_WORKFLOWS,
    CATEGORY_TAXONOMY,
    get_preset_by_id,
    get_presets_by_category,
    get_presets_by_tag,
    get_presets_by_tier,
    get_all_categories,
    get_total_tool_count,
    get_catalog_summary,
)

router = APIRouter(prefix="/api/v1/integrations", tags=["integrations"])

# Lazy-init engine singleton
_engine: Optional[CatalogEngine] = None


def _get_engine() -> CatalogEngine:
    global _engine
    if _engine is None:
        _engine = CatalogEngine()
    return _engine


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class PresetSummary(BaseModel):
    id: str
    name: str
    description: str
    category: str
    tags: List[str]
    tool_count: int
    tier: str
    enterprise_priority: int
    transport: str
    install_type: str
    required_env: List[str]


class PresetDetail(BaseModel):
    id: str
    name: str
    description: str
    category: str
    tags: List[str]
    transport: str
    install_type: str
    install_cmd: str
    package: str
    repo: str
    tool_count: int
    required_env: List[str]
    optional_env: List[str]
    tier: str
    enterprise_priority: int


class CatalogSummaryResponse(BaseModel):
    total_integrations: int
    total_tools: int
    categories: Dict[str, int]
    tiers: Dict[str, int]
    enterprise_critical: int


class ActivateRequest(BaseModel):
    env_vars: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables / secrets required by this integration",
    )
    auto_index: bool = Field(
        default=True,
        description="Automatically index this integration's tools into SemanticToolStore",
    )


class ActivateResponse(BaseModel):
    preset_id: str
    status: str
    message: str
    install_cmd: str
    tools_indexed: int = 0
    missing_env: List[str] = []


class StackRequest(BaseModel):
    preset_ids: List[str] = Field(
        ..., description="List of desired preset IDs to resolve"
    )


class RecommendRequest(BaseModel):
    query: str = Field(..., description="Natural language description of what you need")
    max_results: int = Field(10, ge=1, le=50)
    tier_filter: Optional[str] = None
    category_filter: Optional[str] = None
    max_priority: int = Field(5, ge=1, le=5)


class InstallScriptRequest(BaseModel):
    preset_ids: List[str]
    shell: str = "bash"
    include_env_template: bool = True


# =============================================================================
# CORE ROUTES (backward-compatible)
# =============================================================================


@router.get("/presets", response_model=List[PresetSummary])
async def list_presets(
    category: Optional[str] = Query(None, description="Filter by category"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    tier: Optional[str] = Query(None, description="Filter by tier: official, vendor, community"),
    search: Optional[str] = Query(None, description="Search name/description"),
    min_priority: Optional[int] = Query(None, ge=1, le=5, description="Max enterprise priority (1=critical)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List available MCP server integrations with optional filtering."""
    results = list(PRESET_CATALOG)

    if category:
        results = [p for p in results if p["category"] == category]
    if tag:
        results = [p for p in results if tag in p.get("tags", [])]
    if tier:
        results = [p for p in results if p["tier"] == tier]
    if min_priority:
        results = [p for p in results if p.get("enterprise_priority", 5) <= min_priority]
    if search:
        q = search.lower()
        results = [
            p for p in results
            if q in p["name"].lower()
            or q in p["description"].lower()
            or any(q in t for t in p.get("tags", []))
        ]

    total = len(results)
    results = results[offset : offset + limit]

    return [
        PresetSummary(
            id=p["id"],
            name=p["name"],
            description=p["description"],
            category=p["category"],
            tags=p.get("tags", []),
            tool_count=p.get("tool_count", 0),
            tier=p["tier"],
            enterprise_priority=p.get("enterprise_priority", 5),
            transport=p.get("transport", "stdio"),
            install_type=p.get("install_type", "npm"),
            required_env=p.get("required_env", []),
        )
        for p in results
    ]


@router.get("/presets/summary", response_model=CatalogSummaryResponse)
async def catalog_summary():
    """Get a high-level summary of the integration catalog."""
    return CatalogSummaryResponse(**get_catalog_summary())


@router.get("/presets/categories")
async def list_categories():
    """List all integration categories with preset counts and taxonomy."""
    cats = get_all_categories()
    return {
        "categories": [
            {
                "name": name,
                "count": count,
                "taxonomy": CATEGORY_TAXONOMY.get(name, {}),
            }
            for name, count in cats.items()
        ],
        "total": len(cats),
    }


@router.get("/presets/{preset_id}", response_model=PresetDetail)
async def get_preset(preset_id: str):
    """Get full details for a specific integration preset."""
    preset = get_preset_by_id(preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    return PresetDetail(**preset)


@router.post("/presets/{preset_id}/activate", response_model=ActivateResponse)
async def activate_preset(preset_id: str, req: ActivateRequest):
    """
    Activate an integration preset.

    Validates required env vars are provided, returns install instructions,
    and optionally indexes the integration's tools into SemanticToolStore.
    """
    preset = get_preset_by_id(preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")

    missing = [
        var for var in preset.get("required_env", [])
        if var not in req.env_vars
    ]

    if missing:
        return ActivateResponse(
            preset_id=preset_id,
            status="missing_credentials",
            message=f"Missing required environment variables: {', '.join(missing)}",
            install_cmd=preset.get("install_cmd", ""),
            missing_env=missing,
        )

    tools_indexed = 0
    if req.auto_index:
        try:
            from ..embeddings.semantic_tool_store import SemanticToolStore
            import os

            data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
            db_path = os.path.join(data_dir, "tool_embeddings.db")
            store = SemanticToolStore(db_path=db_path)

            tool_dicts = _generate_tool_stubs(preset)
            if tool_dicts:
                await store.index_tools_batch(tool_dicts, backend=preset_id)
                tools_indexed = len(tool_dicts)
        except Exception:
            pass

    return ActivateResponse(
        preset_id=preset_id,
        status="activated",
        message=f"Integration '{preset['name']}' activated. Run: {preset.get('install_cmd', '')}",
        install_cmd=preset.get("install_cmd", ""),
        tools_indexed=tools_indexed,
    )


@router.get("/presets/{preset_id}/tools")
async def list_preset_tools(preset_id: str):
    """List known tools for a preset integration."""
    preset = get_preset_by_id(preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")

    tools = _generate_tool_stubs(preset)
    return {
        "preset_id": preset_id,
        "preset_name": preset["name"],
        "tool_count": preset.get("tool_count", 0),
        "known_tools": tools,
    }


# =============================================================================
# ADVANCED ROUTES — Semantic Search, Recommendations, Graph Resolution
# =============================================================================


@router.post("/presets/recommend")
async def recommend_presets(req: RecommendRequest):
    """
    Intelligent preset recommendation using TF-IDF semantic matching,
    tier boosting, and enterprise priority scoring.

    Returns ranked results with composite scores.
    """
    engine = _get_engine()
    results = engine.recommend(
        req.query,
        max_results=req.max_results,
        tier_filter=req.tier_filter,
        category_filter=req.category_filter,
        max_priority=req.max_priority,
    )
    return {
        "query": req.query,
        "results": results,
        "total": len(results),
    }


@router.get("/presets/search/semantic")
async def semantic_search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    min_score: float = Query(0.05, ge=0, le=1),
):
    """
    TF-IDF semantic search across the entire catalog.
    Returns presets ranked by cosine similarity.
    """
    engine = _get_engine()
    results = engine.search(q, limit=limit, min_score=min_score)
    return {
        "query": q,
        "results": [
            {**preset, "_similarity": round(score, 4)}
            for preset, score in results
        ],
        "total": len(results),
    }


@router.post("/presets/resolve-stack")
async def resolve_stack(req: StackRequest):
    """
    Resolve a full integration stack from desired presets.

    Computes transitive dependency closure, topological install order,
    conflict detection, and aggregate environment variable requirements.
    """
    engine = _get_engine()
    return engine.resolve_stack(req.preset_ids)


@router.post("/presets/detect-conflicts")
async def detect_conflicts(req: StackRequest):
    """
    Detect conflicts (redundancy, env collision, mutual exclusion)
    among a set of presets.
    """
    engine = _get_engine()
    conflicts = engine.detect_conflicts(req.preset_ids)
    return {
        "preset_ids": req.preset_ids,
        "conflicts": conflicts,
        "has_conflicts": len(conflicts) > 0,
    }


@router.get("/presets/{preset_id}/enterprise-score")
async def enterprise_score(preset_id: str):
    """
    Compute enterprise readiness score (0-100) for a preset.

    Factors: tier, priority, tool richness, repo presence,
    env simplicity, transport modernity.
    """
    engine = _get_engine()
    result = engine.enterprise_score(preset_id)
    if result["score"] == 0 and not engine.get_by_id(preset_id):
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    return result


@router.get("/presets/{preset_id}/similar")
async def find_similar(
    preset_id: str,
    limit: int = Query(5, ge=1, le=20),
):
    """Find presets most similar to the given one (for alternatives or dedup)."""
    engine = _get_engine()
    if not engine.get_by_id(preset_id):
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    results = engine.find_similar(preset_id, limit=limit)
    return {
        "preset_id": preset_id,
        "similar": [
            {"preset": preset, "similarity": round(score, 4)}
            for preset, score in results
        ],
    }


@router.get("/presets/{preset_id}/dependencies")
async def get_dependencies(preset_id: str):
    """Get dependency graph edges for a preset (what it needs, what needs it)."""
    engine = _get_engine()
    if not engine.get_by_id(preset_id):
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    deps = engine.get_dependencies(preset_id)
    dependents = engine.get_dependents(preset_id)
    return {
        "preset_id": preset_id,
        "dependencies": [
            {"target": d.target_id, "kind": d.kind, "reason": d.reason}
            for d in deps
        ],
        "depended_on_by": dependents,
    }


# =============================================================================
# COMPOSITE WORKFLOWS
# =============================================================================


@router.get("/workflows")
async def list_workflows(
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """List pre-built composite workflow templates."""
    engine = _get_engine()
    workflows = engine.get_workflows(category=category)
    return {
        "workflows": workflows,
        "total": len(workflows),
    }


@router.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: str):
    """Get a resolved workflow template with validated steps."""
    engine = _get_engine()
    resolved = engine.resolve_workflow(workflow_id)
    if not resolved:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found")
    return resolved


# =============================================================================
# CATALOG INTELLIGENCE
# =============================================================================


@router.get("/presets/categories/{category}/report")
async def category_report(category: str):
    """Deep report on a category: leaders, gaps, tier balance, tag distribution."""
    engine = _get_engine()
    report = engine.category_report(category)
    if "error" in report:
        raise HTTPException(status_code=404, detail=f"Category '{category}' not found")
    return report


@router.get("/catalog/health")
async def catalog_health():
    """
    Comprehensive catalog health assessment.

    Reports: health score, missing repos, duplicate detection,
    thin categories, tier balance.
    """
    engine = _get_engine()
    return engine.catalog_health()


@router.get("/catalog/duplicates")
async def find_duplicates(
    threshold: float = Query(0.75, ge=0.5, le=1.0, description="Similarity threshold"),
    limit: int = Query(20, ge=1, le=100),
):
    """Find likely duplicate entries in the catalog."""
    engine = _get_engine()
    dupes = engine.find_duplicates(threshold=threshold)
    return {
        "threshold": threshold,
        "duplicates": [
            {"a": a, "b": b, "similarity": score}
            for a, b, score in dupes[:limit]
        ],
        "total": len(dupes),
    }


@router.post("/catalog/install-script")
async def generate_install_script(req: InstallScriptRequest):
    """
    Generate a complete shell install script for a set of presets.

    Resolves dependencies, orders installation, includes env var template,
    and warns about conflicts.
    """
    engine = _get_engine()
    script = engine.generate_install_script(
        req.preset_ids,
        shell=req.shell,
        include_env_template=req.include_env_template,
    )
    return {
        "preset_ids": req.preset_ids,
        "shell": req.shell,
        "script": script,
    }


# =============================================================================
# CAPABILITY GRAPH & GAP ANALYSIS
# =============================================================================


@router.get("/capabilities")
async def list_capabilities():
    """List all abstract capabilities the catalog can provide."""
    engine = _get_engine()
    return {
        "capabilities": engine.list_capabilities(),
        "total": len(engine.list_capabilities()),
    }


@router.get("/capabilities/{capability}/providers")
async def capability_providers(capability: str):
    """Get all presets that provide a given abstract capability, ranked by quality."""
    engine = _get_engine()
    providers = engine.capability_providers(capability)
    if not providers:
        raise HTTPException(status_code=404, detail=f"Capability '{capability}' not found")
    return {
        "capability": capability,
        "providers": providers,
        "total": len(providers),
    }


@router.post("/capabilities/gap-analysis")
async def gap_analysis(req: StackRequest):
    """
    Given currently active presets, identify which abstract capabilities
    are covered and which have gaps with recommended providers.
    """
    engine = _get_engine()
    return engine.gap_analysis(req.preset_ids)


# =============================================================================
# COST OPTIMIZER
# =============================================================================


class CostOptimizeRequest(BaseModel):
    required_capabilities: List[str] = Field(
        ..., description="Capabilities you need (e.g., ['user-auth', 'relational-db'])"
    )
    monthly_budget: float = Field(1000.0, ge=0)
    prefer_free: bool = True


@router.post("/capabilities/optimize-cost")
async def optimize_cost(req: CostOptimizeRequest):
    """
    Given required capabilities and a budget, find the optimal
    set of presets using greedy set-cover with cost awareness.
    """
    engine = _get_engine()
    return engine.optimize_cost(
        req.required_capabilities,
        monthly_budget=req.monthly_budget,
        prefer_free=req.prefer_free,
    )


# =============================================================================
# MIGRATION PLANNER
# =============================================================================


@router.get("/migrations")
async def list_migrations(
    preset_id: Optional[str] = Query(None, description="Filter by preset"),
):
    """List available migration paths between presets."""
    engine = _get_engine()
    return {
        "migrations": engine.list_migrations(preset_id),
        "total": len(engine.list_migrations(preset_id)),
    }


@router.get("/migrations/{from_id}/{to_id}")
async def get_migration_plan(from_id: str, to_id: str):
    """
    Get a detailed migration plan between two presets including
    env mapping, breaking changes, difficulty, and step-by-step guide.
    """
    engine = _get_engine()
    plan = engine.migration_plan(from_id, to_id)
    if not plan:
        raise HTTPException(
            status_code=404,
            detail=f"No migration path found between '{from_id}' and '{to_id}'",
        )
    return plan


# =============================================================================
# SECURITY POSTURE
# =============================================================================


@router.post("/catalog/security-posture")
async def security_posture(req: StackRequest):
    """
    Compute aggregate security posture for a stack of presets.

    Evaluates transport security, tier trust, auth requirements,
    open-source auditability, and secret surface area.
    Returns grade (A-D) and risk factors.
    """
    engine = _get_engine()
    return engine.security_posture(req.preset_ids)


# =============================================================================
# COLLABORATIVE FILTERING
# =============================================================================


@router.post("/presets/coactivation-recommend")
async def coactivation_recommend(
    req: StackRequest,
    limit: int = Query(10, ge=1, le=50),
):
    """
    Collaborative filtering: given active presets, recommend others
    commonly co-activated. 'Users who activated X also activated Y.'
    """
    engine = _get_engine()
    results = engine.coactivation_recommend(req.preset_ids, limit=limit)
    return {
        "active_presets": req.preset_ids,
        "recommendations": results,
        "total": len(results),
    }


# =============================================================================
# STACK COMPARISON
# =============================================================================


class StackCompareRequest(BaseModel):
    stack_a: List[str] = Field(..., description="First stack of preset IDs")
    stack_b: List[str] = Field(..., description="Second stack of preset IDs")


@router.post("/catalog/compare-stacks")
async def compare_stacks(req: StackCompareRequest):
    """
    Compare two stacks across tools, cost, security, and capability coverage.
    Returns per-dimension winner and capability diff.
    """
    engine = _get_engine()
    return engine.compare_stacks(req.stack_a, req.stack_b)


# =============================================================================
# PRESET ALTERNATIVES & CLUSTERING
# =============================================================================


@router.get("/presets/{preset_id}/alternatives")
async def preset_alternatives(preset_id: str):
    """
    Comprehensive alternatives view: similar presets, migration paths,
    capability equivalents, and co-activation suggestions.
    """
    engine = _get_engine()
    result = engine.preset_alternatives(preset_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=f"Preset '{preset_id}' not found")
    return result


@router.get("/catalog/clusters")
async def semantic_clusters(
    k: int = Query(12, ge=2, le=50, description="Number of clusters"),
):
    """
    K-means clustering on TF-IDF vectors to discover natural preset
    groupings beyond manual categories.
    """
    engine = _get_engine()
    return engine.cluster(k=k)


# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def _generate_tool_stubs(preset: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Generate synthetic tool stubs from preset metadata.

    For presets where we know the specific tools, load from data files.
    For others, generate a descriptive stub for semantic search.
    """
    import os
    import json

    preset_id = preset["id"]

    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
    manifest_path = os.path.join(data_dir, f"{preset_id}_tools.json")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            return json.load(f)

    return [
        {
            "name": f"{preset_id}",
            "description": preset["description"],
        }
    ]
