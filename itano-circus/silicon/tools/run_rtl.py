from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
EVIDENCE_PATH = ROOT / "evidence" / "C2-verilator.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes(paths: list[Path]) -> dict[str, str]:
    return {str(path.relative_to(PROJECT_ROOT)): sha256(path) for path in paths}


def build_environment() -> tuple[dict[str, str], str | None]:
    """Prefer Homebrew GNU Make/GCC when Apple's license gate blocks make."""
    environment = os.environ.copy()
    gnu_make = Path("/opt/homebrew/opt/make/libexec/gnubin")
    if gnu_make.is_dir():
        environment["PATH"] = f"{gnu_make}:{environment.get('PATH', '')}"
    homebrew_clang = Path("/opt/homebrew/opt/llvm/bin/clang++")
    compilers = sorted(Path("/opt/homebrew/bin").glob("g++-*"), reverse=True)
    compiler = str(homebrew_clang) if homebrew_clang.is_file() else str(compilers[0]) if compilers else None
    return environment, compiler


def main() -> None:
    verilator = shutil.which("verilator")
    if not verilator:
        print(json.dumps({"status": "BLOCKED", "gate": "C2", "reason": "Verilator is not installed; RTL pass was not claimed."}, indent=2))
        raise SystemExit(2)
    rtl = ROOT / "rtl" / "acx_dot8.sv"
    testbench = ROOT / "tb" / "acx_dot8_tb.sv"
    vectors = ROOT / "vectors.json"
    sources = [rtl, testbench, vectors]
    environment, compiler = build_environment()
    lint_command = [
        verilator, "--lint-only", "--timing", "--assert", "-Wall",
        "--top-module", "acx_dot8_tb", str(rtl), str(testbench),
    ]
    lint = subprocess.run(lint_command, text=True, capture_output=True, env=environment)
    if lint.returncode:
        print(lint.stdout)
        print(lint.stderr, file=sys.stderr)
        raise SystemExit(lint.returncode)
    with tempfile.TemporaryDirectory(prefix="acx-dot8-") as temp:
        command = [
            verilator, "--binary", "--timing", "--assert", "-Wall",
            "--Mdir", str(Path(temp) / "obj"), "--top-module", "acx_dot8_tb",
            str(rtl), str(testbench),
        ]
        if compiler:
            command[1:1] = ["-MAKEFLAGS", f"CXX={compiler} LINK={compiler}"]
        build = subprocess.run(command, text=True, capture_output=True, env=environment)
        if build.returncode:
            print(build.stdout)
            print(build.stderr, file=sys.stderr)
            raise SystemExit(build.returncode)
        binary = Path(temp) / "obj" / "Vacx_dot8_tb"
        run = subprocess.run([str(binary)], text=True, capture_output=True)
        print(run.stdout)
        if run.returncode:
            print(run.stderr, file=sys.stderr)
            raise SystemExit(run.returncode)
        if "PASS ACX-DOT8-R1" not in run.stdout:
            print("self-checking testbench did not emit its pass sentinel", file=sys.stderr)
            raise SystemExit(1)

    version = subprocess.run([verilator, "--version"], text=True, capture_output=True, check=True).stdout.strip()
    vector_count = len(json.loads(vectors.read_text(encoding="utf-8"))["vectors"])
    evidence = {
        "gate": "C2", "status": "PASS", "contract": "ACX-DOT8-R1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": {"name": "Verilator", "version": version, "path": verilator},
        "compiler": compiler or "system default", "vectors_executed": vector_count,
        "source_sha256": source_hashes(sources),
        "checks": ["SystemVerilog lint", "compiled timing model", "assertions enabled", "self-checking six-vector simulation"],
        "result": "PASS ACX-DOT8-R1",
        "scope": "Block-level functional RTL evidence only; not GPU verification closure, CDC/RDC, formal coverage, FPGA correlation, or silicon signoff.",
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "gate": "C2", "vectors": vector_count, "evidence": str(EVIDENCE_PATH)}, indent=2))


if __name__ == "__main__":
    main()
