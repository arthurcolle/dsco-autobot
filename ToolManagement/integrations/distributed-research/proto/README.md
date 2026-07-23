Prototype: Sparse Strips CPU Rasterizer (Python)

Overview
- This is a minimal prototype of the sparse strips renderer described in docs/sparse_strips_plan.md.
- It rasterizes line-segment paths into an RGBA8 image using tiles and strips, with non-zero winding fill.

Setup
1) Create a venv (optional):
   python -m venv .venv && source .venv/bin/activate

2) Install deps (isolated from the root project):
   pip install numpy pillow

Run example
- python examples/render_example.py
- Produces examples/out.png

Notes
- This is a correctness-first prototype: single-sample coverage, single-threaded, no SIMD. The structure matches the design for later optimization.
- The prototype is intentionally isolated and does not modify root requirements.

