from __future__ import annotations

import argparse
import json
from pathlib import Path

from .model import ScenarioConfig, run_scenario, write_run
from .physics import validate_q9_contact
from .silicon import capability_probe


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the GEN-0 factory and swarm model")
    parser.add_argument("--fleet", type=int, default=100)
    parser.add_argument("--minutes", type=int, default=720)
    parser.add_argument("--seed", type=int, default=903742)
    parser.add_argument("--wind", type=float, default=3.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run_scenario(ScenarioConfig(fleet_size=args.fleet, duration_minutes=args.minutes, seed=args.seed, wind_m_s=args.wind))
    root = Path(__file__).resolve().parent.parent
    result["physics_validation"] = validate_q9_contact()
    result["silicon"] = capability_probe(root)
    target = args.output or write_run(result, Path(__file__).resolve().parent / "runs")
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "run_id": result["run_id"], "artifact": str(target), "invariants_passed": result["invariants_passed"],
        "sorties": result["fleet"]["sorties"], "tasks_completed": result["fleet"]["tasks_completed"],
        "ground_robots": result["robotics"]["count"], "ground_tasks_completed": result["robotics"]["tasks_completed"],
        "ground_units_commissioned": result["robotics"]["units_commissioned"], "ground_tasks_blocked": result["robotics"]["tasks_blocked"],
        "operations": f"{result['factory']['operations_completed']}/{result['factory']['operations_total']}",
        "quadruped_cycles": result["fleet"]["quadruped_recoveries"], "physics": result["physics_validation"]["status"],
    }, indent=2))


if __name__ == "__main__":
    main()
