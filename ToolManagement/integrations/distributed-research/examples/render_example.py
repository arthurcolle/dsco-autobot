import os
import sys

# Make project root importable when running directly
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from proto.sparse_strips import ImageSurface, Rasterizer, PathBuilder


def rect_path(x: float, y: float, w: float, h: float):
    pb = PathBuilder()
    pb.move_to(x, y)
    pb.line_to(x + w, y)
    pb.line_to(x + w, y + h)
    pb.line_to(x, y + h)
    pb.close()
    return pb.build_segments()


def tri_path(x0: float, y0: float, x1: float, y1: float, x2: float, y2: float):
    pb = PathBuilder()
    pb.move_to(x0, y0)
    pb.line_to(x1, y1)
    pb.line_to(x2, y2)
    pb.close()
    return pb.build_segments()


def main():
    W, H = 256, 256
    surf = ImageSurface.new_rgba8(W, H)
    rast = Rasterizer(tile_w=32, tile_h=8, fill_rule="nonzero")

    # Background rectangle (light gray)
    bg = rect_path(10, 10, 236, 236)
    rast.draw_segments(surf, bg, (230, 230, 235, 255))

    # Blue rectangle
    r1 = rect_path(30, 40, 140, 80)
    rast.draw_segments(surf, r1, (60, 120, 220, 220))

    # Red triangle overlapping
    t1 = tri_path(120, 80, 220, 180, 60, 200)
    rast.draw_segments(surf, t1, (220, 50, 50, 200))

    os.makedirs("examples", exist_ok=True)
    out_path = os.path.join("examples", "out.png")
    surf.save_png(out_path)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
