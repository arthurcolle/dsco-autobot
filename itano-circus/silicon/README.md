# ACX G1 silicon learning vehicle

This directory is deliberately smaller than a GPU. It is the first executable
slice of the G1 FPGA learning vehicle: a signed INT8 dot-product lane with an
exact numerical contract, golden vectors, synthesizable SystemVerilog, and a
tool probe that refuses to claim verification when the required tool is absent.

Current evidence:

- `vectors.json` freezes signed INT8 input and signed INT32 accumulation behavior.
- `reference_model.py` is the independent executable oracle.
- `rtl/acx_dot8.sv` is a one-stage synthesizable implementation candidate.
- `tests/test_reference.py` checks the oracle, range behavior, and every frozen vector.
- `evidence/C2-verilator.json` records a clean lint, compiled timing model, and
  self-checking execution of all six vectors.
- `evidence/C3-yosys.json` records technology-independent elaboration, structural
  checks, and the generic 8-multiply/7-add RTLIL result.

Evidence is source-hashed. The simulation service demotes a pass to `READY` as
soon as the RTL, testbench, or vectors change, until its tool is rerun.

This is not a GPU and not a tapeout candidate. Missing work includes an ISA,
register file, scheduler, memory system, DMA/IOMMU, cache/coherency policy,
compiler/runtime, verification closure, FPGA correlation, physical design,
licensed PDK/IP, DFT, package/test, and a foundry contract.

Run the available evidence:

```sh
simulation/.venv/bin/python -m unittest discover -s silicon/tests -v
simulation/.venv/bin/python silicon/reference_model.py silicon/vectors.json
simulation/.venv/bin/python silicon/tools/run_rtl.py
simulation/.venv/bin/python silicon/tools/run_synth.py
```

The current Mac has Verilator 5.050 and Yosys 0.67 installed. The RTL runner
prefers Homebrew GNU Make and LLVM when available, avoiding dependence on the
Apple developer-tool license gate. These passes are intentionally narrow:
technology-specific cell mapping, area, timing, power, DFT, physical design and
signoff remain blocked until a real PDK/library and contracted process exist.
