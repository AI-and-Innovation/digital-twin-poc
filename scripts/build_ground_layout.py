"""Build config/ground_layout.json: roads, kerbs, car parks and stands around RGIA Terminal 1.

REAL (OpenStreetMap, ODbL): the landside road network (approach and departure roads,
the elevated T1 departure drives, the Arrival Driveway, car park roads), car park
outlines, the RTC bus stop, the airside apron service roads and the aircraft stands
(aeroway=parking_position). SCHEMATIC (this file): which stretch of road is which kerb,
kerb rules (who may stop, maximum dwell), bay counts, car park capacities, GSE depots
and the terminal points airside vehicles serve. Same local frame as terminal_layout.json.

  python scripts/build_ground_layout.py                       # fetch from Overpass
  python scripts/build_ground_layout.py --osm-json config/osm_rgia_ground.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from airport_eye.spatial import LocalFrame  # noqa: E402

FRAME = LocalFrame("HYD-T1-LOCAL-METRES", origin_lat=17.2355, origin_lon=78.4295)
OVERPASS = "https://overpass-api.de/api/interpreter"
QUERY = ('[out:json][timeout:90];(way["highway"](17.228,78.415,17.247,78.440);'
         'node["aeroway"="parking_position"](17.228,78.415,17.247,78.440);'
         'way["amenity"="parking"](17.230,78.418,17.247,78.440);'
         'node["highway"="bus_stop"](17.230,78.418,17.247,78.440););out geom;')
DRIVABLE = {"primary", "primary_link", "secondary", "secondary_link", "tertiary", "tertiary_link",
            "unclassified", "residential", "service"}
SPEED_KMH = {"primary": 50, "primary_link": 40, "secondary": 45, "tertiary": 35, "tertiary_link": 30,
             "residential": 25, "unclassified": 30, "service": 20}
AIRSIDE_KMH = 25
CLIP = (-1000, 1450, -420, 1000)  # x0, x1, y0, y1 in local metres

# Kerbs: a stretch of an OSM way (by id), clipped to x range. Bays at 7 m per car, 15 m per bus.
KERBS = [
    {"kerb_id": "SYN-HYD-KERB-DEP-PRIVATE", "name": "Departures kerb, private cars", "way": 1284169810,
     "x": (-150, 140), "floor": "L1", "purpose": "drop_off", "vehicle_types": ["private_car"], "max_dwell_s": 180},
    {"kerb_id": "SYN-HYD-KERB-DEP-COMMERCIAL", "name": "Departures kerb, taxis and app cabs", "way": 251668991,
     "x": (-150, 140), "floor": "L1", "purpose": "drop_off", "vehicle_types": ["app_cab", "taxi"], "max_dwell_s": 120},
    {"kerb_id": "SYN-HYD-KERB-ARR-PRIVATE", "name": "Arrivals pick-up, private cars", "way": 251668997,
     "x": (-150, 140), "floor": "L0", "purpose": "pick_up", "vehicle_types": ["private_car"], "max_dwell_s": 300},
    {"kerb_id": "SYN-HYD-KERB-ARR-CAB", "name": "App cab and taxi pick-up zone", "way": 251669000,
     "x": (185, 300), "floor": "L0", "purpose": "pick_up", "vehicle_types": ["app_cab", "taxi"], "max_dwell_s": 300},
]
BUS_STOP = {"kerb_id": "SYN-HYD-KERB-BUS", "name": "AP/TS RTC bus stop", "floor": "L0", "purpose": "bus",
            "vehicle_types": ["bus"], "max_dwell_s": 600, "bays": 3}
CAR_PARKS = [  # OSM way id -> name, capacity (assumed)
    (251031350, "SYN-HYD-PARK-P1", "Car park P1 (west forecourt)", 1200),
    (251031352, "SYN-HYD-PARK-P2", "Car park P2 (east forecourt)", 1000),
]
DEPOTS = [  # airside GSE depots (schematic), near where the apron roads lead off
    {"depot_id": "SYN-HYD-GSE-DEPOT-EAST", "name": "East GSE depot", "near": (905, 5)},
    {"depot_id": "SYN-HYD-GSE-DEPOT-WEST", "name": "West GSE depot", "near": (-840, -120)},
]
SERVICE_POINTS = {  # terminal points airside vehicles drive to (schematic)
    "baggage_hall": (40, -95),
    "bus_gates_dom": (420, -95),
    "bus_gates_int": (-430, -105),
}
INT_STAND_MAX_X = -200  # stands west of this serve international flights (west wing)


def fetch(path: str | None) -> dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    request = urllib.request.Request(OVERPASS, data=urllib.parse.urlencode({"data": QUERY}).encode(),
                                     headers={"User-Agent": "airporteye-poc/0.1", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def local(p: dict) -> list[float]:
    x, y = FRAME.from_wgs84(p["lon"], p["lat"])
    return [round(x, 1), round(y, 1)]


def inside(p, clip=CLIP) -> bool:
    return clip[0] <= p[0] <= clip[1] and clip[2] <= p[1] <= clip[3]


def side_of(tags: dict, pts: list[list[float]]) -> str | None:
    ys = [p[1] for p in pts]
    if tags.get("highway") == "service" and min(ys) < -20 and max(ys) < 160:
        return "airside"
    if min(ys) > 40:
        return "landside"
    return None


def clip_polyline(pts, x0, x1):
    """The part of a polyline with x0 <= x <= x1, with points interpolated at the cut."""
    out = []
    for a, b in zip(pts, pts[1:]):
        for p, q in ((a, b),):
            lo, hi = sorted((p[0], q[0]))
            if hi < x0 or lo > x1 or hi == lo:
                if x0 <= p[0] <= x1 and (not out or out[-1] != p):
                    out.append(p)
                continue
            t0 = max(0.0, min(1.0, (x0 - p[0]) / (q[0] - p[0])))
            t1 = max(0.0, min(1.0, (x1 - p[0]) / (q[0] - p[0])))
            for t in sorted((t0, t1)):
                pt = [round(p[0] + (q[0] - p[0]) * t, 1), round(p[1] + (q[1] - p[1]) * t, 1)]
                if x0 - 0.01 <= pt[0] <= x1 + 0.01 and (not out or out[-1] != pt):
                    out.append(pt)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--osm-json")
    ap.add_argument("--out", default=str(ROOT / "config/ground_layout.json"))
    args = ap.parse_args()
    osm = fetch(args.osm_json)

    nodes: dict[str, dict] = {}
    edges: list[list] = []
    ways_by_id = {}
    for e in osm["elements"]:
        if e["type"] != "way":
            continue
        ways_by_id[e["id"]] = e
        t = e.get("tags", {})
        if t.get("highway") not in DRIVABLE or t.get("service") == "parking_aisle":
            continue
        pts = [local(p) for p in e["geometry"]]
        side = side_of(t, pts)
        # apron roads are tagged access=no/private: closed to the public, used by airside vehicles
        if side is None or (side == "landside" and t.get("access") in ("no", "private")):
            continue
        level = "L1" if t.get("bridge") in ("yes", "viaduct") else "L0"
        speed = AIRSIDE_KMH if side == "airside" else SPEED_KMH.get(t["highway"], 30)
        oneway = t.get("oneway") in ("yes", "true", "1") or t.get("junction") == "roundabout"
        reverse = t.get("oneway") == "-1"
        name = t.get("name") or ("Apron service road" if side == "airside" else "")
        for (na, pa), (nb, pb) in zip(zip(e["nodes"], pts), zip(e["nodes"][1:], pts[1:])):
            if not (inside(pa) or inside(pb)):
                continue
            for n, p in ((na, pa), (nb, pb)):
                nodes.setdefault(f"n{n}", {"id": f"n{n}", "x": p[0], "y": p[1], "side": side})
            length = round(math.hypot(pb[0] - pa[0], pb[1] - pa[1]), 1)
            a, b = (f"n{nb}", f"n{na}") if reverse else (f"n{na}", f"n{nb}")
            edges.append([a, b, length, speed, level, name, side, e["id"]])
            if not (oneway or reverse):
                edges.append([b, a, length, speed, level, name, side, e["id"]])

    kerbs = []
    for k in KERBS:
        way = ways_by_id[k["way"]]
        pts = clip_polyline([local(p) for p in way["geometry"]], *k["x"])
        length = sum(math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(pts, pts[1:]))
        kerbs.append({key: v for key, v in k.items() if key not in ("way", "x")} |
                     {"osm_way": k["way"], "polyline": pts, "length_m": round(length, 1), "bays": max(2, int(length // 7))})
    stop = next(e for e in osm["elements"] if e["type"] == "node" and e.get("tags", {}).get("highway") == "bus_stop")
    sx, sy = local(stop)
    kerbs.append(BUS_STOP | {"polyline": [[sx - 20, sy], [sx + 20, sy]], "length_m": 40.0, "point": [sx, sy]})

    car_parks = []
    for way_id, park_id, name, capacity in CAR_PARKS:
        poly = [local(p) for p in ways_by_id[way_id]["geometry"]][:-1]
        car_parks.append({"park_id": park_id, "name": name, "osm_way": way_id, "capacity": capacity, "polygon": poly})

    stands = []
    raw = sorted((local(e), e.get("tags", {}).get("ref")) for e in osm["elements"]
                 if e["type"] == "node" and e.get("tags", {}).get("aeroway") == "parking_position")
    unnamed = 0
    for (x, y), ref in raw:
        if not inside([x, y]):
            continue
        digits = "".join(ch for ch in (ref or "") if ch.isdigit())
        if digits:
            stand_id = f"SYN-HYD-STAND-{digits}"
        else:
            unnamed += 1
            stand_id = f"SYN-HYD-STAND-R{unnamed:02d}"
        stands.append({"stand_id": stand_id, "osm_ref": ref, "x": x, "y": y,
                       "flow": "int" if x < INT_STAND_MAX_X else "dom"})

    layout = {
        "_note": ("Roads, car park outlines, the bus stop and aircraft stands: OpenStreetMap (ODbL, (c) OpenStreetMap "
                  "contributors). Kerb assignments and rules, bay counts, car park capacities, GSE depots and terminal "
                  "service points are SCHEMATIC assumptions. Generated by scripts/build_ground_layout.py."),
        "osm_timestamp": osm.get("osm3s", {}).get("timestamp_osm_base"),
        "frame": {"frame_id": FRAME.frame_id, "origin_lat": FRAME.origin_lat, "origin_lon": FRAME.origin_lon,
                  "rotation_deg": 0.0, "reference_elevation_m": 617.0,
                  "reference_elevation_basis": "poc_approx_amsl_rgia_not_surveyed"},
        "floors": {"L1": {"z": 8.0}, "L0": {"z": 0.0}},
        "roads": {"nodes": list(nodes.values()), "edges": edges,
                  "edge_fields": ["from", "to", "length_m", "speed_kmh", "level", "name", "side", "osm_way"]},
        "kerbs": kerbs,
        "car_parks": car_parks,
        "stands": stands,
        "depots": [d | {"x": d["near"][0], "y": d["near"][1]} for d in DEPOTS],
        "service_points": {k: {"x": v[0], "y": v[1]} for k, v in SERVICE_POINTS.items()},
        "entries": {"east": [1000, 590], "west": [-900, 600]},
        "exits": {"east": [1000, 605], "west": [-900, 560]},
    }
    Path(args.out).write_text(json.dumps(layout, indent=1) + "\n", encoding="utf-8")
    print(f"Wrote {args.out}: {len(nodes)} road nodes, {len(edges)} edges, {len(kerbs)} kerbs, "
          f"{len(car_parks)} car parks, {len(stands)} stands")


if __name__ == "__main__":
    main()
