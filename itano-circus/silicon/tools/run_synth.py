from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
EVIDENCE_PATH = ROOT / "evidence" / "C3-yosys.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    yosys = shutil.which("yosys")
    if not yosys:
        print(json.dumps({"status": "BLOCKED", "gate": "C3", "reason": "Yosys is not installed; synthesis pass was not claimed."}, indent=2))
        raise SystemExit(2)
    rtl = ROOT / "rtl" / "acx_dot8.sv"
    with tempfile.TemporaryDirectory(prefix="acx-dot8-yosys-") as temp:
        stats_path = Path(temp) / "stats.json"
        script = (
            f"read_verilog -sv {rtl}; "
            "hierarchy -check -top acx_dot8; proc; opt; check; "
            f"tee -o {stats_path} stat -json"
        )
        command = [yosys, "-q", "-p", script]
        run = subprocess.run(command, text=True, capture_output=True)
        if run.returncode:
            print(run.stdout)
            print(run.stderr, file=sys.stderr)
            raise SystemExit(run.returncode)
        stats = json.loads(stats_path.read_text(encoding="utf-8"))
    module = stats.get("modules", {}).get("\\acx_dot8")
    if not module or module.get("num_cells", 0) <= 0:
        print("Yosys produced no acx_dot8 cells", file=sys.stderr)
        raise SystemExit(1)
    cells = module.get("num_cells_by_type", {})
    if cells.get("$mul", 0) != 8 or cells.get("$add", 0) != 7:
        print(f"unexpected arithmetic structure: {cells}", file=sys.stderr)
        raise SystemExit(1)
    version = subprocess.run([yosys, "-V"], text=True, capture_output=True, check=True).stdout.strip()
    evidence = {
        "gate": "C3", "status": "PASS", "contract": "ACX-DOT8-R1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": {"name": "Yosys", "version": version, "path": yosys},
        "source_sha256": {str(rtl.relative_to(PROJECT_ROOT)): sha256(rtl)},
        "top_module": "acx_dot8", "problems": 0,
        "generic_rtlil": {
            "wires": module.get("num_wires"), "wire_bits": module.get("num_wire_bits"),
            "cells": module.get("num_cells"), "cell_types": cells,
        },
        "result": "PASS GENERIC SYNTHESIS",
        "scope": "Technology-independent RTLIL elaboration and structural checks only; no standard-cell mapping, PDK area, timing, power, DFT, place/route, or signoff claim.",
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "gate": "C3", "cells": module["num_cells"], "evidence": str(EVIDENCE_PATH)}, indent=2))


if __name__ == "__main__":
    main()
