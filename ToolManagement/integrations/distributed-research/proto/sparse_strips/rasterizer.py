from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Dict, Set
import math

import numpy as np


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

    def save_png(self, path: str) -> None:
        try:
            from PIL import Image  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("Pillow not installed. pip install pillow") from e
        img = Image.fromarray(self.data, mode="RGBA")
        img.save(path)


class Rasterizer:
    def __init__(self, tile_w: int = 32, tile_h: int = 8, fill_rule: str = "nonzero") -> None:
        assert fill_rule in ("nonzero", "evenodd")
        self.tile_w = tile_w
        self.tile_h = tile_h
        self.fill_rule = fill_rule
        self.eps_y = 1e-7
        self.eps_x = 1e-7

    # --- Public API ---

    def draw_segments(self, surface: ImageSurface, segments: List[Segment], color: Tuple[int, int, int, int]) -> None:
        if not segments:
            return
        # Bin segments into tiles
        bins, tiles_x, tiles_y = self._bin_segments(surface.width, surface.height, segments)
        # Build strips (ranges of consecutive non-empty tiles per tile-row)
        strips = self._build_strips(bins, tiles_x, tiles_y)
        # Rasterize strips
        for (ty, x0, x1) in strips:
            self._rasterize_strip(surface, segments, bins, tiles_x, ty, x0, x1, color)

    # --- Internals ---

    def _bin_segments(self, width: int, height: int, segments: List[Segment]) -> Tuple[List[List[List[int]]], int, int]:
        tw, th = self.tile_w, self.tile_h
        tiles_x = (width + tw - 1) // tw
        tiles_y = (height + th - 1) // th
        bins: List[List[List[int]]] = [[[] for _ in range(tiles_x)] for _ in range(tiles_y)]

        for ei, ((x0, y0), (x1, y1)) in enumerate(segments):
            minx = min(x0, x1)
            maxx = max(x0, x1)
            miny = min(y0, y1)
            maxy = max(y0, y1)
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

    def _build_strips(self, bins: List[List[List[int]]], tiles_x: int, tiles_y: int) -> List[Tuple[int, int, int]]:
        # Returns list of (tile_row_y, tile_x_start_inclusive, tile_x_end_inclusive)
        strips: List[Tuple[int, int, int]] = []
        for ty in range(tiles_y):
            x = 0
            while x < tiles_x:
                # Skip empties
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

        # Union of segments used by this strip
        edge_ids: Set[int] = set()
        for tx in range(tile_x0, tile_x1 + 1):
            for ei in bins[tile_row_y][tx]:
                edge_ids.add(ei)

        if not edge_ids:
            return

        # Pre-fetch segment tuples for speed
        segs = [segments[i] for i in edge_ids]

        # Rasterize scanlines within this strip
        for y in range(y0_pix, y1_pix):
            y_sample = y + 0.5
            events: List[Tuple[float, int]] = []
            for ((x0, y0), (x1, y1)) in segs:
                dy = y1 - y0
                if abs(dy) < self.eps_y:
                    continue  # ignore nearly-horizontal for winding
                # Half-open y inclusion
                if (y0 <= y_sample < y1) or (y1 <= y_sample < y0):
                    t = (y_sample - y0) / dy
                    x = x0 + t * (x1 - x0)
                    delta = 1 if dy > 0 else -1
                    events.append((x, delta))
            if not events:
                continue
            events.sort(key=lambda p: p[0])

            ptr = 0
            winding = 0
            px = x0_pix
            while px < x1_pix:
                xc = px + 0.5
                # Consume events strictly to the left of pixel center
                while ptr < len(events) and events[ptr][0] < xc - self.eps_x:
                    winding += events[ptr][1]
                    ptr += 1

                inside = False
                if self.fill_rule == "nonzero":
                    inside = winding != 0
                else:  # evenodd
                    inside = (winding & 1) != 0

                if inside:
                    self._blend_pixel(surface.data, px, y, color)
                px += 1

    def _blend_pixel(self, buf: np.ndarray, x: int, y: int, src: Tuple[int, int, int, int]) -> None:
        # Straight alpha over
        sr, sg, sb, sa = src
        if sa == 255:
            buf[y, x, 0] = sr
            buf[y, x, 1] = sg
            buf[y, x, 2] = sb
            buf[y, x, 3] = 255
            return
        dr, dg, db, da = buf[y, x].tolist()
        sa_f = sa / 255.0
        inv = 1.0 - sa_f
        out_r = sr * sa_f + dr * inv
        out_g = sg * sa_f + dg * inv
        out_b = sb * sa_f + db * inv
        out_a = sa + int(da * inv + 0.5)
        buf[y, x, 0] = int(out_r + 0.5)
        buf[y, x, 1] = int(out_g + 0.5)
        buf[y, x, 2] = int(out_b + 0.5)
        buf[y, x, 3] = min(out_a, 255)

