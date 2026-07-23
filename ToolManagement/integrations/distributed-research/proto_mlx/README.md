MLX-Accelerated Sparse Strips Rasterizer (macOS, Apple Silicon)

Overview
- This prototype uses Apple’s MLX array framework to evaluate winding and fills in parallel on Apple Silicon (GPU/CPU).
- It shares the sparse strips pipeline (tiles → strips → scanline events), but computes per-strip inside masks with MLX broadcasting and reductions.

Requirements
- macOS with Apple Silicon (M1/M2/M3).
- Python 3.9+.
- pip install mlx numpy pillow

Run Example
- python examples/render_mlx.py
- Produces examples/out_mlx.png

Notes
- Prototype uses single-sample coverage and solid colors.
- MLX computes winding without per-scanline sorting by summing sign(dy) for edges with x_intersect < x_center using vectorized comparisons.
- For large strips, the implementation processes columns in manageable chunks to bound memory.

