from typing import List, Tuple, Optional


Point = Tuple[float, float]
Segment = Tuple[Point, Point]


class PathBuilder:
    """Builds basic paths as sequences of line segments.

    Prototype scope: move_to, line_to, close. Curves can be approximated
    by user-provided segments or extended later with flattening.
    """

    def __init__(self) -> None:
        self._subpaths: List[List[Point]] = []
        self._current: Optional[List[Point]] = None

    def move_to(self, x: float, y: float) -> None:
        self._flush_if_empty()
        self._current = [(x, y)]

    def line_to(self, x: float, y: float) -> None:
        if self._current is None:
            # Implicit move_to if not started
            self._current = [(x, y)]
        else:
            self._current.append((x, y))

    def close(self) -> None:
        if self._current and len(self._current) >= 2:
            self._subpaths.append(self._current)
            self._current = None

    def _flush_if_empty(self) -> None:
        if self._current is not None and len(self._current) >= 2:
            self._subpaths.append(self._current)
            self._current = None

    def build_segments(self) -> List[Segment]:
        # Include any in-progress subpath
        if self._current is not None and len(self._current) >= 2:
            self._subpaths.append(self._current)
            self._current = None

        segments: List[Segment] = []
        for pts in self._subpaths:
            if len(pts) < 2:
                continue
            # Close if start != end
            if pts[0] != pts[-1]:
                pts = pts + [pts[0]]
            for i in range(len(pts) - 1):
                p0, p1 = pts[i], pts[i + 1]
                if p0 == p1:
                    continue
                segments.append((p0, p1))
        return segments

