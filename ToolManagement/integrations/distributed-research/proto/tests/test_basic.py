from proto.sparse_strips import ImageSurface, Rasterizer, PathBuilder


def build_rect(x, y, w, h):
    pb = PathBuilder()
    pb.move_to(x, y)
    pb.line_to(x + w, y)
    pb.line_to(x + w, y + h)
    pb.line_to(x, y + h)
    pb.close()
    return pb.build_segments()


def test_rectangle_fill():
    W, H = 32, 32
    surf = ImageSurface.new_rgba8(W, H)
    rast = Rasterizer(tile_w=16, tile_h=8, fill_rule="nonzero")
    segs = build_rect(8, 8, 16, 16)
    color = (0, 255, 0, 255)
    rast.draw_segments(surf, segs, color)

    # Inside center point should be filled
    cx, cy = 16, 16
    assert surf.data[cy, cx, 1] > 200  # green channel

    # Outside should remain empty
    assert surf.data[2, 2, 3] == 0

