from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .model import GROUND_ROBOT_SPECS, Matrice100Spec, PayloadSpec, ScenarioConfig, run_scenario, write_run
from .physics import validate_q9_contact
from .silicon import capability_probe


HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
RUN_DIR = HERE / "runs"
_run_lock = Lock()
_latest: dict[str, Any] | None = None


class RunRequest(BaseModel):
    fleet_size: int = Field(100, ge=40, le=100)
    duration_minutes: int = Field(720, ge=60, le=2880)
    seed: int = 903742
    wind_m_s: float = Field(3.0, ge=0, le=25)
    minimum_landing_reserve_pct: float = Field(25.0, ge=15, le=40)
    scenario: str = "full_factory_shift"


app = FastAPI(title="GEN-0 Factory Simulation", version="0.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["null", "http://127.0.0.1:8876", "http://localhost:8876"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.get("/api/health")
def health() -> dict[str, Any]:
    caps = capability_probe(PROJECT_ROOT)
    return {
        "status": "READY", "service": "GEN-0 multi-fidelity simulation rack", "version": app.version,
        "engines": {"factory": "SimPy", "robotics": "MuJoCo" if caps["python"]["mujoco"] else "UNAVAILABLE", "flight": "bounded M100 energy/kinematics model", "silicon": "capability-gated"},
        "latest_run": _latest["run_id"] if _latest else _latest_run_id(),
    }


@app.get("/api/specs")
def specs() -> dict[str, Any]:
    return {
        "matrice_100": Matrice100Spec().__dict__, "q9": PayloadSpec().__dict__,
        "ground_robot_design_targets": {kind: spec.__dict__ for kind, spec in GROUND_ROBOT_SPECS.items()},
    }


@app.get("/api/silicon/capabilities")
def silicon_capabilities() -> dict[str, Any]:
    return capability_probe(PROJECT_ROOT)


@app.post("/api/physics/q9")
def q9_physics() -> dict[str, Any]:
    return validate_q9_contact()


@app.post("/api/runs")
def create_run(request: RunRequest) -> dict[str, Any]:
    global _latest
    config = ScenarioConfig(**request.model_dump())
    with _run_lock:
        result = run_scenario(config)
        physics = validate_q9_contact()
        result["physics_validation"] = physics
        result["silicon"] = capability_probe(PROJECT_ROOT)
        artifact = write_run(result, RUN_DIR)
        result["artifact"] = str(artifact)
        _latest = result
        return result


def _latest_path() -> Path | None:
    candidates = sorted(RUN_DIR.glob("GEN0-*.json"), key=lambda path: path.stat().st_mtime, reverse=True) if RUN_DIR.exists() else []
    return candidates[0] if candidates else None


def _latest_run_id() -> str | None:
    path = _latest_path()
    return path.stem if path else None


@app.get("/api/runs/latest")
def latest_run() -> dict[str, Any]:
    if _latest is not None:
        return _latest
    path = _latest_path()
    if path is None:
        raise HTTPException(404, "No simulation run exists yet")
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    if not run_id.startswith("GEN0-") or "/" in run_id or ".." in run_id:
        raise HTTPException(400, "Invalid run id")
    path = RUN_DIR / f"{run_id}.json"
    if not path.exists():
        raise HTTPException(404, "Run not found")
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/")
def index() -> FileResponse:
    return FileResponse(PROJECT_ROOT / "index.html")


# Keep the API routes above this catch-all mount. Serving at the root preserves
# the original relative iframe URLs (`aircell.html` and `classic.html`).
app.mount("/", StaticFiles(directory=PROJECT_ROOT, html=True), name="universe")


def main() -> None:
    import uvicorn
    uvicorn.run("simulation.server:app", host="127.0.0.1", port=8876, reload=False)


if __name__ == "__main__":
    main()
