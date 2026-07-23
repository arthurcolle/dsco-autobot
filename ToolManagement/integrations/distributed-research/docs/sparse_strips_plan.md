Sparse Strips CPU Rasterizer — Design and Implementation Plan

Overview
- Goal: Implement a CPU-based 2D renderer that exploits sparsity via horizontal strips of tiles, enabling locality, incremental winding, and straightforward parallelization.
- Pipeline: Flatten → Bin to tiles → Merge tiles to strips → Strip-level winding → Pixel-level winding → Coverage → Paint → Blend.

Core Data Model
- Coordinate system: image space pixels; world coordinates are floats; sampling uses pixel centers at (x+0.5, y+0.5).
- Segments: line segments after flattening; curves split to monotone in y; horizontal segments ignored for winding.
- Tiles: uniform size (default 32×8 pixels). Bins store references to segment IDs overlapping the tile AABB.
- Strips: consecutive non-empty tiles in a tile-row (y-index) merged into a single work unit.

Edge Normalization
- Split curves into monotone-in-y segments before raster stage.
- Use half-open y-interval: include edge for scanline y if y0 ≤ y < y1 (with y measured at sample point y+0.5).
- Ignore edges with |dy| < eps_y for winding (nearly horizontal); still considered for anti-alias coverage when enabled.

Winding Computation
- Per scanline in a strip, build an event list of (x_intersect, delta) for all edges crossing that y.
- Intersection: x = x0 + (y - y0) * (dx/dy); delta = sign(dy).
- Sort events by x; sweep pixels left→right maintaining a running winding by consuming events when x_event < x_center - eps_x. Inside if winding != 0 (non-zero rule) or parity(winding) for even-odd.

Pseudocode (scanline events + incremental winding)
```
for y in strip.y_pixels:
  y_s = y + 0.5
  events = []
  for e in strip.edges:
    dy = e.y1 - e.y0
    if abs(dy) < eps_y: continue
    if (e.y0 <= y_s < e.y1) or (e.y1 <= y_s < e.y0):
      t = (y_s - e.y0) / dy
      x = e.x0 + t * (e.x1 - e.x0)
      delta = +1 if dy > 0 else -1
      events.push((x, delta))
  sort(events by x)
  winding = 0; ptr = 0
  for x in strip.x_pixels:
    xc = x + 0.5
    while ptr < len(events) and events[ptr].x < xc - eps_x:
      winding += events[ptr].delta; ptr += 1
    inside = (winding != 0)          # non-zero
          or ((winding & 1) != 0)    # even-odd
    if inside: blend(pixel(x,y))
```

Coverage
- Baseline: single-sample at pixel center for correctness and simplicity.
- Optional: 8× SSAA (2×4 or 4×2 grid) using incremental edge stepping across sub-rows; accumulate coverage as u8.

Painting and Blending
- Solid color: per-pixel RGBA over existing framebuffer using Porter-Duff over.
- Gradients/images: compute color per pixel; for prototype we keep solid color only.

SIMD
- Vector width: 4 or 8 pixels per iteration. In Python prototype, use NumPy vectorization where possible; production would target SSE2/AVX2.
- Alignment: align row strides to 32/64 bytes in native code. Prototype does not enforce alignment.

Multithreading
- Work units: strips. Use work-stealing deque per worker in native implementation. Prototype uses single-threaded execution; structure is amenable to parallelism.

Memory Layout
- Segments: SoA in native (x0[], y0[], dxdy[], y0[]), AoS tuples in prototype for simplicity.
- Tile bins: vector of lists of segment IDs per tile.
- Framebuffer: interleaved RGBA u8 with row-major order.

Numerical Precision
- Prototype: double precision for intersection, small epsilons: eps_y=1e-7, eps_x=1e-7.
- Production: store intersections as fixed-point (e.g., 24.8) to ensure determinism across platforms.

Clip Paths and Strokes
- Strokes expanded to fills in flattening step (joins/caps applied) → raster stage only sees fills.
- Clips incorporated by evaluating an additional winding stream per pixel and ANDing (for non-zero) or XOR (for even-odd). Prototype omits clips.

Edge Cases
- Degenerates: zero-length segments removed during preprocessing.
- Vertex ties: half-open y rule prevents double counting; for x ties, treat event exactly at pixel center as to the right (do not toggle winding) by using strict comparison x_event < x_center - eps_x.

Benchmarks and Validation
- Benchmarks: sparse glyph-like rectangles, triangles; measure MPix/s and wall-clock.
- Tests: rectangles and triangles with expected fills; parity vs non-zero; vertical edges and near-horizontal edges.

Deliverables in repo
- docs/sparse_strips_plan.md (this document)
- proto/sparse_strips (prototype renderer)
- examples/render_example.py (generate PNG)
- proto/README.md with setup instructions (isolated requirements)
Decisions That Resolve Open Questions
- Winding mechanics: Half-open y-range avoids vertex double-count; x tie-break via strict left-of-center comparison.
- Vertical edges: Fully supported through dx/dy; no special case needed beyond y half-open rule.
- Nearly horizontal edges: Ignored for winding (|dy| < eps_y); can still contribute to anti-alias coverage in advanced mode.
- Strip merging rationale: Reduces per-tile overhead, improves cache locality and work scheduling; 1-tile strips are kept for uniform handling and to skip empties.
- SIMD tail handling: In native, use masked stores (AVX2) or scalar cleanup. Prototype uses scalar loops with room for NumPy vectorization.
- Load balancing: Strips are work units; batch small strips to amortize scheduling; work-stealing deques minimize imbalance.
- Memory layout and prefetch: SoA for edges, interleaved RGBA buffer, per-strip contiguous event lists; prototype uses simple AoS for clarity.
- Precision choice: Prototype uses float64 for robustness; production should store intersections in fixed-point for determinism and speed.
- Clip paths: Treated as additional winding stream; combined logically during pixel test. Deferred in prototype.
- Coverage method: Start with single-sample; upgrade to 8× SSAA using incremental x(y) stepping per sub-row.

