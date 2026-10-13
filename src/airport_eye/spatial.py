"""Terminal-local frame, walkway graph and zone lookup.

``LocalFrame`` converts the terminal's local metric frame (x east, y north, metres)
to WGS84 and back. It is a local tangent-plane approximation around one origin
with an optional rotation: over a ~1 km terminal the error is a few centimetres,
far below the accuracy of the schematic layout itself. Swap in a surveyed
transform (control points from CAD/BIM) by changing the layout's ``frame``.

``WalkGraph`` is an undirected graph of walkway nodes; each node sits on one
floor and vertical links (escalators, lifts, ramps) are ordinary edges between
nodes on different floors.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Any

from .models import IndoorPosition, OutdoorPosition

EARTH_METRES_PER_DEG_LAT = 110_574.0
EARTH_METRES_PER_DEG_LON_EQUATOR = 111_320.0
WEB_MERCATOR_RADIUS_M = 6_378_137.0  # EPSG:3857 sphere


@dataclass(frozen=True)
class LocalFrame:
    frame_id: str
    origin_lat: float
    origin_lon: float
    rotation_deg: float = 0.0  # clockwise rotation of the local +y axis from true north
    # Metres above the vertical datum at the local origin / L0. Used as REFERENCE_Z so
    # global_elevation_m = reference_elevation_m + floor-relative elevation_m.
    # None until a surveyed (or agreed POC) value is set on the layout frame.
    reference_elevation_m: float | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocalFrame":
        ref = data.get("reference_elevation_m")
        return cls(data["frame_id"], data["origin_lat"], data["origin_lon"], data.get("rotation_deg", 0.0),
                   None if ref is None else float(ref))

    @property
    def _scale_x(self) -> float:
        return EARTH_METRES_PER_DEG_LON_EQUATOR * math.cos(math.radians(self.origin_lat))

    def to_wgs84(self, x: float, y: float) -> tuple[float, float]:
        """Local metres -> (longitude, latitude), rounded to 1e-7 degrees (~1 cm)."""
        angle = math.radians(self.rotation_deg)
        east = x * math.cos(angle) + y * math.sin(angle)
        north = -x * math.sin(angle) + y * math.cos(angle)
        return (round(self.origin_lon + east / self._scale_x, 7),
                round(self.origin_lat + north / EARTH_METRES_PER_DEG_LAT, 7))

    def from_wgs84(self, lon: float, lat: float) -> tuple[float, float]:
        east = (lon - self.origin_lon) * self._scale_x
        north = (lat - self.origin_lat) * EARTH_METRES_PER_DEG_LAT
        angle = math.radians(self.rotation_deg)
        return (east * math.cos(angle) - north * math.sin(angle),
                east * math.sin(angle) + north * math.cos(angle))


def to_web_mercator(longitude: float, latitude: float) -> tuple[float, float]:
    """WGS84 lon/lat -> EPSG:3857 metres (Web Mercator), matching the Esri hand-off snippet."""
    lon = math.radians(longitude)
    lat = math.radians(max(min(latitude, 85.05112878), -85.05112878))
    x = WEB_MERCATOR_RADIUS_M * lon
    y = WEB_MERCATOR_RADIUS_M * math.log(math.tan(math.pi / 4.0 + lat / 2.0))
    return round(x, 3), round(y, 3)


def point_in_polygon(x: float, y: float, polygon: list[list[float]]) -> bool:
    inside = False
    j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def polygon_centroid(polygon: list[list[float]]) -> tuple[float, float]:
    area = cx = cy = 0.0
    for (x0, y0), (x1, y1) in zip(polygon, polygon[1:] + polygon[:1]):
        cross = x0 * y1 - x1 * y0
        area += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    if abs(area) < 1e-9:
        return (sum(p[0] for p in polygon) / len(polygon), sum(p[1] for p in polygon) / len(polygon))
    return cx / (3 * area), cy / (3 * area)


@dataclass(frozen=True)
class Node:
    node_id: str
    x: float
    y: float
    floor: str


class WalkGraph:
    STRAIGHT_WALK_M = 25.0  # below this, walk straight instead of detouring via the graph

    def __init__(self, nodes: list[Node], edges: list[tuple[str, str, float]]):
        self.nodes = {n.node_id: n for n in nodes}
        self.adjacency: dict[str, list[tuple[str, float]]] = {n.node_id: [] for n in nodes}
        for a, b, cost in edges:
            if a not in self.nodes or b not in self.nodes:
                raise ValueError(f"edge {a}-{b} references an unknown node")
            self.adjacency[a].append((b, cost))
            self.adjacency[b].append((a, cost))
        self._cache: dict[tuple[str, str], list[str]] = {}
        # Connected groups. Landside, airside and so on are deliberately separate:
        # touchpoints (entry, security, emigration) are the only way across.
        self.component: dict[str, int] = {}
        for node_id in self.nodes:
            if node_id in self.component:
                continue
            group, stack = len(set(self.component.values())), [node_id]
            self.component[node_id] = group
            while stack:
                for neighbour, _ in self.adjacency[stack.pop()]:
                    if neighbour not in self.component:
                        self.component[neighbour] = group
                        stack.append(neighbour)

    @classmethod
    def from_layout(cls, layout: dict[str, Any]) -> "WalkGraph":
        nodes = [Node(n["id"], n["x"], n["y"], n["floor"]) for n in layout["walkways"]["nodes"]]
        by_id = {n.node_id: n for n in nodes}
        edges = []
        for edge in layout["walkways"]["edges"]:
            a, b = by_id[edge[0]], by_id[edge[1]]
            cost = edge[2] if len(edge) > 2 else math.hypot(a.x - b.x, a.y - b.y)
            edges.append((a.node_id, b.node_id, cost))
        return cls(nodes, edges)

    def nearest(self, x: float, y: float, floor: str, component: int | None = None) -> Node:
        candidates = [n for n in self.nodes.values()
                      if n.floor == floor and (component is None or self.component[n.node_id] == component)]
        if not candidates:
            raise ValueError(f"no walkway nodes on floor {floor}")
        return min(candidates, key=lambda n: (n.x - x) ** 2 + (n.y - y) ** 2)

    def route(self, start: str, end: str) -> list[Node]:
        """Shortest node path (Dijkstra), cached per pair."""
        key = (start, end)
        if key not in self._cache:
            best = {start: 0.0}
            previous: dict[str, str] = {}
            heap = [(0.0, start)]
            while heap:
                cost, node = heapq.heappop(heap)
                if node == end:
                    break
                if cost > best.get(node, math.inf):
                    continue
                for neighbour, step in self.adjacency[node]:
                    total = cost + step
                    if total < best.get(neighbour, math.inf):
                        best[neighbour] = total
                        previous[neighbour] = node
                        heapq.heappush(heap, (total, neighbour))
            if end != start and end not in previous:
                raise ValueError(f"no walkway route from {start} to {end}")
            path = [end]
            while path[-1] != start:
                path.append(previous[path[-1]])
            self._cache[key] = list(reversed(path))
        return [self.nodes[n] for n in self._cache[key]]

    def path_between(self, x0: float, y0: float, floor0: str,
                     x1: float, y1: float, floor1: str) -> list[tuple[float, float, str]]:
        """Waypoints from one point to another: walk to a nearby node, follow the graph, walk off.

        The passenger stays in the connected group of the node nearest to them (landside,
        airside and so on are separate: touchpoints are the crossings); start and end
        nodes are the best pair among the few nearest each point in that group. With no shared group, or for a short hop on one floor where the
        graph would be a detour, the passenger walks straight.
        """
        try:
            group = self.component[self.nearest(x0, y0, floor0).node_id]
        except ValueError:
            return [(x1, y1, floor1)]

        def closest(x: float, y: float, floor: str) -> list[Node]:
            return sorted((n for n in self.nodes.values() if n.floor == floor and self.component[n.node_id] == group),
                          key=lambda n: (n.x - x) ** 2 + (n.y - y) ** 2)[:4]

        best = None
        for start in closest(x0, y0, floor0):
            for end in closest(x1, y1, floor1):
                cost = (math.hypot(start.x - x0, start.y - y0) + math.hypot(start.x - end.x, start.y - end.y)
                        + math.hypot(end.x - x1, end.y - y1))
                if best is None or cost < best[0]:
                    best = (cost, start, end)
        if best is None:
            return [(x1, y1, floor1)]
        _, a, b = best
        direct = math.hypot(x1 - x0, y1 - y0)
        if floor0 == floor1 and direct < self.STRAIGHT_WALK_M \
                and math.hypot(a.x - x0, a.y - y0) + math.hypot(b.x - x1, b.y - y1) > direct:
            return [(x1, y1, floor1)]  # short hop: the graph would be a detour
        points = [(n.x, n.y, n.floor) for n in self.route(a.node_id, b.node_id)]
        return points + [(x1, y1, floor1)]


def indoor_to_outdoor(position: IndoorPosition, frame: LocalFrame, ground_msl: float) -> OutdoorPosition:
    """``z`` is height above the terminal's ground floor; ``ground_msl`` anchors it to MSL."""
    if position.coordinate_frame_id != frame.frame_id:
        raise ValueError(f"position is in {position.coordinate_frame_id}, frame is {frame.frame_id}")
    lon, lat = frame.to_wgs84(position.x, position.y)
    return OutdoorPosition(lat=lat, lon=lon, altitude=ground_msl + position.z, altitude_datum="MSL",
                           gis_location_id=position.zone_id)


def route_between(graph: WalkGraph, start_node: str, end_node: str) -> list[Node]:
    return graph.route(start_node, end_node)
