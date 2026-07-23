from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple, Set

import numpy as np

try:
    import mlx.core as mx
except Exception as _e:  # pragma: no cover
    mx = None  # type: ignore


Point = Tuple[float, float]
Segment = Tuple[Point, Point]


@dataclass
class ImageSurface:
    width: int
    height: int
    data: np.ndarray

    @classmethod
    def new_rgba8(cls, width: int, height: int) -> "ImageSurface":
        arr = np.zeros((height, width, 4), dtype=np.uint8)
        return cls(width, height, arr)


class MlxRasterizer:
    def __init__(self, tile_w: int = 32, tile_h: int = 8, fill_rule: str = "nonzero") -> None:
        if mx is None:
            raise RuntimeError(
                "MLX not available. Install with `pip install mlx` on Apple Silicon."
            )
        assert fill_rule in ("nonzero", "evenodd")
        self.tile_w = tile_w
        self.tile_h = tile_h
        self.fill_rule = fill_rule
        self.eps_y = 1e-7
        self.eps_x = 1e-7
        # Pixel processing chunk to limit temporary tensor sizes
        self.pixel_chunk = 256

    # Public API
    def draw_segments(self, surface: ImageSurface, segments: List[Segment], color: Tuple[int, int, int, int]) -> None:
        if not segments:
            return
        bins, tiles_x, tiles_y = self._bin_segments(surface.width, surface.height, segments)
        strips = self._build_strips(bins, tiles_x, tiles_y)
        for (ty, x0, x1) in strips:
            self._rasterize_strip(surface, segments, bins, tiles_x, ty, x0, x1, color)

    # Internals
    def _bin_segments(self, width: int, height: int, segments: List[Segment]):
        tw, th = self.tile_w, self.tile_h
        tiles_x = (width + tw - 1) // tw
        tiles_y = (height + th - 1) // th
        bins: List[List[List[int]]] = [[[] for _ in range(tiles_x)] for _ in range(tiles_y)]
        for ei, ((x0, y0), (x1, y1)) in enumerate(segments):
            minx, maxx = (x0, x1) if x0 <= x1 else (x1, x0)
            miny, maxy = (y0, y1) if y0 <= y1 else (y1, y0)
            if maxx < 0 or maxy < 0 or minx >= width or miny >= height:
                continue
            tx0 = max(0, int(math.floor(minx / tw)))
            tx1 = min(tiles_x - 1, int(math.floor(maxx / tw)))
            ty0 = max(0, int(math.floor(miny / th)))
            ty1 = min(tiles_y - 1, int(math.floor(maxy / th)))
            for ty in range(ty0, ty1 + 1):
                for tx in range(tx0, tx1 + 1):
                    bins[ty][tx].append(ei)
        return bins, tiles_x, tiles_y

    def _build_strips(self, bins: List[List[List[int]]], tiles_x: int, tiles_y: int):
        strips: List[Tuple[int, int, int]] = []
        for ty in range(tiles_y):
            x = 0
            while x < tiles_x:
                while x < tiles_x and not bins[ty][x]:
                    x += 1
                if x >= tiles_x:
                    break
                start = x
                while x < tiles_x and bins[ty][x]:
                    x += 1
                end = x - 1
                strips.append((ty, start, end))
        return strips

    def _rasterize_strip(
        self,
        surface: ImageSurface,
        segments: List[Segment],
        bins: List[List[List[int]]],
        tiles_x: int,
        tile_row_y: int,
        tile_x0: int,
        tile_x1: int,
        color: Tuple[int, int, int, int],
    ) -> None:
        tw, th = self.tile_w, self.tile_h
        y0_pix = tile_row_y * th
        y1_pix = min(surface.height, (tile_row_y + 1) * th)
        x0_pix = tile_x0 * tw
        x1_pix = min(surface.width, (tile_x1 + 1) * tw)

        # Unique edge IDs used by this strip
        edge_ids: Set[int] = set()
        for tx in range(tile_x0, tile_x1 + 1):
            for ei in bins[tile_row_y][tx]:
                edge_ids.add(ei)
        if not edge_ids:
            return

        # Gather segment arrays (E,)
        segs = [segments[i] for i in edge_ids]
        x0 = mx.array([p0[0] for p0, _ in segs], dtype=mx.float32)
        y0 = mx.array([p0[1] for p0, _ in segs], dtype=mx.float32)
        x1 = mx.array([p1[0] for _, p1 in segs], dtype=mx.float32)
        y1 = mx.array([p1[1] for _, p1 in segs], dtype=mx.float32)

        dy = y1 - y0
        dx = x1 - x0
        # sign(dy): +1 for upward, -1 for downward
        delta = mx.where(dy > 0, mx.ones_like(dy), -mx.ones_like(dy))
        # Mask out nearly horizontal for winding
        mask_valid = mx.abs(dy) >= self.eps_y

        # Scanline sample positions (S,)
        y_samples_np = np.arange(y0_pix, y1_pix, dtype=np.float32) + 0.5
        y_samples = mx.array(y_samples_np)  # (S,)

        # Crossing mask per edge/scanline using half-open y interval
        # M = ((y0 <= y < y1) | (y1 <= y < y0)) & mask_valid
        y0E = y0[:, None]
        y1E = y1[:, None]
        yS = y_samples[None, :]
        incl_up = (y0E <= yS) & (yS < y1E)
        incl_dn = (y1E <= yS) & (yS < y0E)
        cross_mask = (incl_up | incl_dn) & (mask_valid[:, None])  # (E,S)

        # Intersections: x = x0 + (y - y0) * (dx / dy)
        # Safe division: where not crossing, value unused; still guard small dy
        inv_dy = mx.where(mx.abs(dy) > 0, 1.0 / dy, 0.0)
        x_intersections = x0[:, None] + (yS - y0E) * (dx[:, None] * inv_dy[:, None])  # (E,S)

        # Process pixel columns in chunks to bound memory
        sr, sg, sb, sa = color
        region = surface.data[y0_pix:y1_pix, x0_pix:x1_pix]  # (S, P, 4) as numpy
        S = region.shape[0]
        P = region.shape[1]
        base_x = x0_pix
        for off in range(0, P, self.pixel_chunk):
            count = min(self.pixel_chunk, P - off)
            x_centers_np = (np.arange(count, dtype=np.float32) + 0.5 + (base_x + off)).astype(np.float32)
            x_centers = mx.array(x_centers_np)  # (Pc,)

            # Compare intersections to pixel centers: (E,S,1) < (1,1,Pc) → (E,S,Pc)
            xES = x_intersections[:, :, None]
            xC = x_centers[None, None, :]
            le_mask = xES < (xC - self.eps_x)

            # Apply crossing mask and edge delta
            # cast bool -> int32
            contrib = le_mask & (cross_mask[:, :, None])
            contrib = contrib.astype(mx.int32) * delta[:, None, None].astype(mx.int32)
            # Sum over edges: (S,Pc)
            winding = mx.sum(contrib, axis=0)

            if self.fill_rule == "evenodd":
                inside = (winding & 1) != 0
            else:
                inside = winding != 0

            # Transfer mask to host and blend in numpy
            inside_np = inside.astype(mx.bool_).numpy()
            sub = region[:, off : off + count]  # (S,Pc,4)
            if sa == 255:
                sub[inside_np, 0] = sr
                sub[inside_np, 1] = sg
                sub[inside_np, 2] = sb
                sub[inside_np, 3] = 255
            else:
                sa_f = sa / 255.0
                inv = 1.0 - sa_f
                # Boolean indexing flattens first axis; apply channel-wise
                dr = sub[..., 0]
                dg = sub[..., 1]
                db = sub[..., 2]
                da = sub[..., 3]
                idx = inside_np
                dr[idx] = np.clip(sr * sa_f + dr[idx] * inv, 0, 255).astype(np.uint8)
                dg[idx] = np.clip(sg * sa_f + dg[idx] * inv, 0, 255).astype(np.uint8)
                db[idx] = np.clip(sb * sa_f + db[idx] * inv, 0, 255).astype(np.uint8)
                da[idx] = np.minimum(255, (sa + (da[idx].astype(np.float32) * inv)).astype(np.uint16)).astype(np.uint8)
