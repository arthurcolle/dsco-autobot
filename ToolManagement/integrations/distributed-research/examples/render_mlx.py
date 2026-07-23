import os
import sys

# Allow imports when run directly
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from proto_mlx.sparse_strips_mlx import ImageSurface, MlxRasterizer
from proto.sparse_strips.path import PathBuilder


def rect_path(x: float, y: float, w: float, h: float):
    pb = PathBuilder()
    pb.move_to(x, y)
    pb.line_to(x + w, y)
    pb.line_to(x + w, y + h)
    pb.line_to(x, y + h)
    pb.close()
    return pb.build_segments()


def main():
    try:
        import mlx.core as mx  # noqa: F401
    except Exception:
        print("MLX not installed. Try: pip install mlx (Apple Silicon only)")
        return

    W, H = 512, 384
    surf = ImageSurface.new_rgba8(W, H)
    rast = MlxRasterizer(tile_w=32, tile_h=8, fill_rule="nonzero")

    bg = rect_path(20, 20, W - 40, H - 40)
    rast.draw_segments(surf, bg, (245, 245, 248, 255))

    r1 = rect_path(40, 60, 220, 120)
    rast.draw_segments(surf, r1, (60, 120, 220, 220))

    r2 = rect_path(140, 110, 220, 120)
    rast.draw_segments(surf, r2, (220, 80, 80, 200))

    os.makedirs("examples", exist_ok=True)
    from proto.sparse_strips.rasterizer import ImageSurface as _Img
    # Reuse PNG helper from numpy buffer
    try:
        from PIL import Image
        Image.fromarray(surf.data, mode="RGBA").save("examples/out_mlx.png")
        print("Wrote examples/out_mlx.png")
    except Exception:
        print("Pillow not installed; cannot write PNG")


if __name__ == "__main__":
    main()

