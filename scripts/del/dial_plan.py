"""The DIAL Terminal 3 departures-level floor plan (config/del/dial_t3_departures_plan.geojson) in local metres.

Used by build_terminal_layout.py to keep the L1 walkways, gates and checkpoints on open floor, and by
check_dial_plan.py to report anything that still lands in a wall, shop, room or void.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from airport_eye.spatial import LocalFrame, point_in_polygon  # noqa: E402

DEFAULT_PLAN = ROOT / "config/del/dial_t3_departures_plan.geojson"

# Spaces a passenger cannot stand in. Not listed (so open floor): corridor, exterior (the kerb strip), door,
# glass_wall_high (thin queue barriers), stairs_high (sub-square-metre markers) and the small unlabelled
# fixtures c2/c3/c4/c5 (counters, seats). "stairs" are the escalators and travelators.
# The processor and the two piers meet only across large "stairs" polygons (the ramps / escalators at each pier
# head), so walks that cannot go round may cross those; see route(allow=...).
CONNECTORS = frozenset({"stairs"})
BLOCKED = {"wall", "mini_wall", "glass", "glass_wall", "shop", "f&b", "wc", "private", "office", "stairs", "lift",
           "garden", "smoking", "lounge_area", "road"}
OUTSIDE = "outside plan"


class Plan:
    """The DIAL polygons in local metres, bucketed on a 10 m grid for quick point lookups."""

    CELL = 10.0

    def __init__(self, frame: LocalFrame, path: Path = DEFAULT_PLAN,
                 doors: list[tuple[float, float, float]] | tuple = ()):
        self.doors = [tuple(d) for d in doors]   # (x, y, radius): walls there are passable (doors the plan lacks)
        self.items: list[tuple[str, list[list[float]], tuple[float, float, float, float]]] = []
        self.grid: dict[tuple[int, int], list[int]] = {}
        for f in json.loads(Path(path).read_text(encoding="utf-8"))["features"]:
            ring = [list(frame.from_wgs84(lon, lat)) for lon, lat in f["geometry"]["coordinates"][0]]
            xs, ys = [p[0] for p in ring], [p[1] for p in ring]
            box = (min(xs), min(ys), max(xs), max(ys))
            self.items.append((f["properties"]["category"], ring, box))
            for i in range(int(box[0] // self.CELL), int(box[2] // self.CELL) + 1):
                for j in range(int(box[1] // self.CELL), int(box[3] // self.CELL) + 1):
                    self.grid.setdefault((i, j), []).append(len(self.items) - 1)

    def categories(self, x: float, y: float) -> list[str]:
        out = []
        for k in self.grid.get((int(x // self.CELL), int(y // self.CELL)), ()):
            cat, ring, (x0, y0, x1, y1) = self.items[k]
            if x0 <= x <= x1 and y0 <= y <= y1 and point_in_polygon(x, y, ring):
                out.append(cat)
        return out

    def blocked(self, x: float, y: float, allow: frozenset[str] = frozenset()) -> str | None:
        """Why a passenger cannot stand here (a category, or 'outside plan'), or None for open floor."""
        cats = self.categories(x, y)
        if not cats:
            return OUTSIDE
        hit = [c for c in cats if c in BLOCKED and c not in allow]
        if hit == ["wall"] and any(math.hypot(x - dx, y - dy) <= r for dx, dy, r in self.doors):
            return None
        return hit[0] if hit else None

    def clear(self, x: float, y: float, radius: float = 1.0, allow: frozenset[str] = frozenset()) -> bool:
        """Open floor here and on a ring ``radius`` metres around."""
        return self.blocked(x, y, allow) is None and all(
            self.blocked(x + radius * math.cos(a * math.pi / 4), y + radius * math.sin(a * math.pi / 4), allow) is None
            for a in range(8))

    def snap(self, x: float, y: float, max_m: float = 25.0, radius: float = 1.0) -> tuple[float, float]:
        """The nearest clear point (0.5 m search steps), or the point itself when it is clear or none is near."""
        if self.clear(x, y, radius):
            return x, y
        r = 0.5
        while r <= max_m:
            steps = max(8, int(2 * math.pi * r / 0.5))
            for k in range(steps):
                a = 2 * math.pi * k / steps
                px, py = x + r * math.cos(a), y + r * math.sin(a)
                if self.clear(px, py, radius):
                    return round(px, 1), round(py, 1)
            r += 0.5
        return x, y

    def route(self, x0: float, y0: float, x1: float, y1: float, margin_m: float = 80.0, cell: float = 1.0,
              allow: frozenset[str] = frozenset()) -> list[tuple[float, float]] | None:
        """Waypoints (ends excluded) of a walk from one point to the other on open floor, or None if there is none
        within ``margin_m`` of the straight line. A* on a ``cell``-metre grid, then cut to the fewest straight legs.
        ``allow``: blocked categories the walk may use (e.g. the escalator/ramp polygons that join the piers)."""
        import heapq

        def key(x: float, y: float) -> tuple[int, int]:
            return round(x / cell), round(y / cell)

        start, goal = key(x0, y0), key(x1, y1)
        lo = (min(start[0], goal[0]) - margin_m / cell, min(start[1], goal[1]) - margin_m / cell)
        hi = (max(start[0], goal[0]) + margin_m / cell, max(start[1], goal[1]) + margin_m / cell)
        free: dict[tuple[int, int], bool] = {start: True, goal: True}

        def is_free(c: tuple[int, int]) -> bool:
            if c not in free:
                free[c] = lo[0] <= c[0] <= hi[0] and lo[1] <= c[1] <= hi[1] and self.clear(c[0] * cell, c[1] * cell, 0.5, allow)
            return free[c]

        steps = [(dx, dy, math.hypot(dx, dy)) for dx in (-1, 0, 1) for dy in (-1, 0, 1) if dx or dy]
        came: dict[tuple[int, int], tuple[int, int] | None] = {start: None}
        cost = {start: 0.0}
        heap = [(0.0, start)]
        while heap:
            _, cur = heapq.heappop(heap)
            if cur == goal:
                break
            for dx, dy, d in steps:
                nxt = (cur[0] + dx, cur[1] + dy)
                if not is_free(nxt):
                    continue
                c = cost[cur] + d
                if c < cost.get(nxt, math.inf):
                    cost[nxt], came[nxt] = c, cur
                    heapq.heappush(heap, (c + math.hypot(goal[0] - nxt[0], goal[1] - nxt[1]), nxt))
        if goal not in came:
            return None
        path, c = [], goal
        while c is not None:
            path.append((c[0] * cell, c[1] * cell))
            c = came[c]
        path = [(x1, y1)] + path[1:-1] + [(x0, y0)]
        path.reverse()
        legs, i = [], 0   # keep only the corners: furthest point in a straight line each time
        while i < len(path) - 1:
            j = len(path) - 1
            while j > i + 1 and self.crossings(*path[i], *path[j], step=0.5, allow=allow):
                j -= 1
            legs.append(path[j])
            i = j
        return [(round(x, 1), round(y, 1)) for x, y in legs[:-1]]

    def crossings(self, x0: float, y0: float, x1: float, y1: float, step: float = 1.0,
                  allow: frozenset[str] = frozenset()) -> dict[str, int]:
        """Blocked categories along a straight walk, sampled every ``step`` metres (ends excluded)."""
        n = max(1, int(math.hypot(x1 - x0, y1 - y0) / step))
        hits: dict[str, int] = {}
        for k in range(1, n):
            why = self.blocked(x0 + (x1 - x0) * k / n, y0 + (y1 - y0) * k / n, allow)
            if why:
                hits[why] = hits.get(why, 0) + 1
        return hits
