import os
import sys
import time
import random

ROOT = os.path.dirname(__file__)
if os.path.dirname(ROOT) not in sys.path:
    sys.path.insert(0, os.path.dirname(ROOT))

from proto.sparse_strips import ImageSurface, Rasterizer, PathBuilder


def rect_segments(x, y, w, h):
    pb = PathBuilder()
    pb.move_to(x, y)
    pb.line_to(x + w, y)
    pb.line_to(x + w, y + h)
    pb.line_to(x, y + h)
    pb.close()
    return pb.build_segments()


def bench(num_rects=500, W=1024, H=768):
    rng = random.Random(42)
    segs = []
    for _ in range(num_rects):
        x = rng.uniform(0, W - 50)
        y = rng.uniform(0, H - 30)
        w = rng.uniform(5, 120)
        h = rng.uniform(5, 90)
        segs.extend(rect_segments(x, y, w, h))

    surf = ImageSurface.new_rgba8(W, H)
    rast = Rasterizer(tile_w=32, tile_h=8, fill_rule="nonzero")
    color = (50, 120, 200, 200)

    t0 = time.perf_counter()
    rast.draw_segments(surf, segs, color)
    t1 = time.perf_counter()
    dt = t1 - t0
    mpix = (W * H) / 1e6
    print(f"Rendered {num_rects} rects over {mpix:.2f} MPix in {dt*1000:.1f} ms => {mpix/dt:.2f} MPix/s")


if __name__ == "__main__":
    bench()

