from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
from typing import Any


TOOLS = {
    "verilator": "compiled SystemVerilog simulation and lint",
    "iverilog": "event-driven Verilog simulation",
    "yosys": "RTL synthesis",
    "sby": "formal-property orchestration",
    "openroad": "physical implementation",
    "docker": "pinned Linux simulation runners",
    "podman": "pinned Linux simulation runners",
}


def _valid_evidence(project_root: Path, filename: str, sources: tuple[str, ...]) -> tuple[bool, str]:
    path = project_root / "silicon" / "evidence" / filename
    if not path.exists():
        return False, f"{path} has not been generated"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return False, f"invalid evidence: {error}"
    if document.get("status") != "PASS":
        return False, f"evidence status is {document.get('status', 'MISSING')}"
    recorded = document.get("source_sha256", {})
    for relative in sources:
        source = project_root / relative
        if not source.exists():
            return False, f"source missing: {relative}"
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        if recorded.get(relative) != digest:
            return False, f"source changed since evidence: {relative}"
    return True, str(path)


def capability_probe(project_root: Path) -> dict[str, Any]:
    tools = {name: {"available": bool(shutil.which(name)), "path": shutil.which(name), "purpose": purpose} for name, purpose in TOOLS.items()}
    reference = project_root / "silicon" / "reference_model.py"
    rtl = project_root / "silicon" / "rtl" / "acx_dot8.sv"
    vectors = project_root / "silicon" / "vectors.json"
    rtl_pass, rtl_evidence = _valid_evidence(project_root, "C2-verilator.json", (
        "silicon/rtl/acx_dot8.sv", "silicon/tb/acx_dot8_tb.sv", "silicon/vectors.json",
    ))
    synth_pass, synth_evidence = _valid_evidence(project_root, "C3-yosys.json", ("silicon/rtl/acx_dot8.sv",))
    rtl_status = "PASS" if rtl_pass else "READY" if tools["verilator"]["available"] else "BLOCKED"
    synth_status = "PASS" if synth_pass else "READY" if tools["yosys"]["available"] else "BLOCKED"
    gates = [
        {"id":"A", "name":"workload + numerical contract", "status":"PASS" if vectors.exists() else "BLOCKED", "evidence":str(vectors) if vectors.exists() else "vectors.json missing"},
        {"id":"B", "name":"executable software model", "status":"PASS" if reference.exists() else "BLOCKED", "evidence":str(reference) if reference.exists() else "reference model missing"},
        {"id":"C1", "name":"synthesizable RTL present", "status":"PASS" if rtl.exists() else "BLOCKED", "evidence":str(rtl) if rtl.exists() else "RTL missing"},
        {"id":"C2", "name":"RTL simulation + lint", "status":rtl_status, "evidence":rtl_evidence if rtl_pass else f"{tools['verilator']['path'] or 'Verilator is not installed'}; {rtl_evidence}"},
        {"id":"C3", "name":"technology-independent synthesis + structural check", "status":synth_status, "evidence":synth_evidence if synth_pass else f"{tools['yosys']['path'] or 'Yosys is not installed'}; {synth_evidence}"},
        {"id":"C4", "name":"PDK mapping + area/timing/power signoff", "status":"BLOCKED", "evidence":"No target standard-cell library, extraction deck, timing corners, or signoff flow"},
        {"id":"D", "name":"licensed PDK/IP + foundry slot", "status":"BLOCKED", "evidence":"No contracted PDK, SRAM compiler, I/O/PLL/PHY IP, or foundry slot"},
        {"id":"E", "name":"package/test/board readiness", "status":"PARTIAL", "evidence":"G1 imported-FPGA carrier route exists; ASIC package/test contract does not"},
        {"id":"F", "name":"silicon yield/reliability/economics", "status":"BLOCKED", "evidence":"Requires fabricated and packaged first silicon"},
    ]
    return {
        "tools": tools,
        "python": {"mujoco": bool(importlib.util.find_spec("mujoco")), "simpy": bool(importlib.util.find_spec("simpy"))},
        "gates": gates,
        "current_claim": "ACX-DOT8-R1 verified seed block for a G1 architecture-learning vehicle; FPGA, DDR, VRM, and bare PCB remain imported",
        "manufacturable_gpu": False,
        "blocking_reason": "A verified dot-product block is not a GPU: the system still lacks a complete architecture and verification closure, contracted ASIC PDK/IP, physical signoff, wafer fabrication, and package/test path.",
    }
