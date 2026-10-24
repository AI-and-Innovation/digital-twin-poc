"""Build config/terminal_layout.json for RGIA Terminal 1 (Hyderabad).

Two parts:

* REAL (OpenStreetMap, ODbL): the Terminal 1 outline (relation 13609059: west wing,
  central processor, east wing), the International Departures and General Aviation
  buildings, and the bus gates. Fetched from the Overpass API, or read from a saved
  response with --osm-json.
* SCHEMATIC (this file): floors, zones, touchpoint positions, queue directions,
  walkways and dwell areas, drawn in terminal-local metres inside the real
  outlines. These are informed guesses, NOT the terminal's floor plan. Replace
  them with GHIAL CAD/BIM drawings when available; nothing else has to change.

Usage:
  python scripts/build_terminal_layout.py                      # fetch from Overpass
  python scripts/build_terminal_layout.py --osm-json osm.json  # use a saved response
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

from airport_eye.spatial import LocalFrame, point_in_polygon  # noqa: E402

FRAME = LocalFrame("HYD-T1-LOCAL-METRES", origin_lat=17.2355, origin_lon=78.4295)
OVERPASS = "https://overpass-api.de/api/interpreter"
QUERY = ('[out:json][timeout:90];(rel(13609059);way(1016464110);way(1016464107);'
         'node["aeroway"="gate"](17.233,78.423,17.238,78.436););out geom;')
OSM_PARTS = {1016464108: "T1 west wing", 681497533: "T1 central processor", 1016464114: "T1 east wing",
             1016464110: "International Departures Terminal", 1016464107: "General Aviation Terminal"}

REFERENCE_ELEVATION_M = 617.0   # height of L0 / the local origin above MSL (POC placeholder, not surveyed)
FLOORS = {"L1": {"name": "Departures level", "z": 8.0}, "L0": {"name": "Arrivals / ground level", "z": 0.0}}


def box(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]


# Zones in lookup order: the first polygon on the floor that contains a point wins.
# "OSM:<name>" polygons are replaced with the real outline.
ZONES = [
    ("SYN-HYD-ZONE-SECURITY", "L1", "PRM security", [box(100, 15, 148, 50)]),
    ("SYN-HYD-ZONE-SECURITY-DOM", "L1", "Domestic security", [box(-40, 15, 100, 50)]),
    ("SYN-HYD-ZONE-SECURITY-INT", "L1", "International security", [box(-268, -6, -188, 60)]),
    ("SYN-HYD-ZONE-DOM-TRANSFER", "L1", "Domestic transfer", [box(160, -45, 220, -10)]),
    ("SYN-HYD-ZONE-TRANSFER", "L1", "Transfer desks", [box(-160, 15, -40, 50)]),
    ("SYN-HYD-ZONE-CHECKIN", "L1", "Check-in halls and link to the international block",
     [box(-160, 50, 148, 84), box(-268, 60, -188, 125), box(-190, 80, -160, 96)]),
    ("SYN-HYD-ZONE-DEPARTURES", "L1", "Departures entry and forecourt", [box(-160, 84, 148, 132)]),
    ("SYN-HYD-ZONE-KERB-DEP", "L1", "Departures kerb (drop-off drives)", [box(-160, 132, 150, 170)]),
    ("SYN-HYD-ZONE-DOM-DEPARTURES", "L1", "Domestic airside and gates",
     [box(-160, -58, 150, 15), "OSM:T1 east wing"]),
    ("SYN-HYD-ZONE-INT-DEPARTURES", "L1", "International airside and gates", ["OSM:T1 west wing"]),
    ("SYN-HYD-ZONE-ARRIVALS", "L0", "Arrivals hall and kerb", [box(-160, 60, 148, 132)]),
    ("SYN-HYD-ZONE-INT-ARRIVALS", "L0", "Immigration and international reclaim",
     [box(-200, -58, -20, 60), "OSM:T1 west wing", "OSM:International Departures Terminal"]),
    ("SYN-HYD-ZONE-DOM-ARRIVALS", "L0", "Domestic reclaim", [box(-20, -58, 150, 60), "OSM:T1 east wing"]),
    ("SYN-HYD-ZONE-CITYSIDE", "L0", "City-side check-in (J Pod) and ramp", [box(110, 132, 200, 215)]),
    ("SYN-HYD-ZONE-FORECOURT", "L0", "Arrivals kerb, car parks and bus stop", [box(-360, 132, 320, 300)]),
    ("SYN-HYD-ZONE-GA", "L0", "General aviation terminal", ["OSM:General Aviation Terminal", box(161, 12, 231, 30)]),
] + [(f"SYN-HYD-ZONE-VID-0{i}", "L1", f"Information point {i}", [box(x - 3, 85, x + 3, 91)])
     for i, x in enumerate([-140, -100, -60, -10, 50, 100, 135], start=1)] + [
    ("SYN-HYD-ZONE-TO-CONFIRM", "L1", "Location to confirm", [box(-10, -10, 10, 10)]),
]

# touchpoint_id: (x, y, floor, queue direction, service line width in metres)
TOUCHPOINTS = {
    "SYN-HYD-ENTRY": (20, 97, "L1", (0, 1), 24),
    "SYN-HYD-DIGIYATRA-ENTRY": (75, 97, "L1", (0, 1), 12),
    "SYN-HYD-PRM-ENTRY": (-35, 97, "L1", (0, 1), 4),
    "SYN-HYD-CHECKIN-DOM": (30, 60, "L1", (0, 1), 60),
    "SYN-HYD-KIOSK-DOM": (110, 72, "L1", (0, 1), 20),
    "SYN-HYD-BAGDROP-DOM": (-70, 60, "L1", (0, 1), 30),
    "SYN-HYD-CHECKIN-INT": (-228, 72, "L1", (0, 1), 42),
    "SYN-HYD-KIOSK-INT": (-255, 108, "L1", (0, 1), 12),
    "SYN-HYD-BAGDROP-INT": (-205, 102, "L1", (0, 1), 15),
    "SYN-HYD-SECURITY-DOM": (30, 22, "L1", (0, 1), 60),
    "SYN-HYD-SECURITY-PRM": (125, 22, "L1", (0, 1), 4),
    "SYN-HYD-SECURITY-INT": (-228, 18, "L1", (0, 1), 30),
    "SYN-HYD-EMIGRATION": (-235, -22, "L1", (0, 1), 36),
    "SYN-HYD-EMIGRATION-EGATE": (-283, -22, "L1", (0, 1), 9),
    "SYN-HYD-EMIGRATION-PRM": (-262, -22, "L1", (0, 1), 4),
    "SYN-HYD-BOARDING-DOM": (420, 5, "L1", (0, 1), 30),
    "SYN-HYD-BOARDING-INT": (-430, -10, "L1", (0, 1), 15),
    "SYN-HYD-TRANSFER-DOM": (190, -28, "L1", (0, 1), 12),
    "SYN-HYD-TRANSFER-DI": (-100, 32, "L1", (0, 1), 12),
    "SYN-HYD-INFO-G": (-120, 90, "L1", (0, 1), 4),
    "SYN-HYD-INFO-P": (0, -20, "L1", (0, 1), 4),
    "SYN-HYD-IMMIGRATION": (-90, 20, "L0", (0, -1), 48),
    "SYN-HYD-IMMIGRATION-EGATE": (-140, 20, "L0", (0, -1), 9),
    "SYN-HYD-RECLAIM-INT": (-90, 45, "L0", (0, -1), 60),
    "SYN-HYD-RECLAIM-DOM": (65, 30, "L0", (0, -1), 80),
    "SYN-HYD-INFO-ARR": (0, 80, "L0", (0, -1), 4),
    "SYN-HYD-CITY-CHECKIN": (140, 185, "L0", (0, 1), 12),
    "SYN-HYD-CITY-BAGDROP": (175, 185, "L0", (0, 1), 9),
    "SYN-HYD-GA-CHECK-IN": (196, 66, "L0", (0, 1), 3),
    "SYN-HYD-GA-SECURITY": (196, 48, "L0", (0, 1), 3),
    "SYN-HYD-GA-EMIGRATION": (214, 38, "L0", (0, 1), 3),
    "SYN-HYD-GA-IMMIGRATION": (178, 38, "L0", (0, -1), 3),
    "SYN-HYD-GA-CUSTOMS": (178, 58, "L0", (0, -1), 3),
} | {f"SYN-HYD-VID-0{i}": (x, 88, "L1", (0, 1), 3)
     for i, x in enumerate([-140, -100, -60, -10, 50, 100, 135], start=1)}

AREAS = {  # random points for dwelling, spawning and exits
    "kerb_departures": ("L1", [box(-130, 122, 130, 128)]),
    "city_parking": ("L0", [box(140, 198, 175, 212)]),
    "dom_dwell": ("L1", [box(-150, -50, 140, 8), box(240, -40, 460, 0)]),
    "int_dwell": ("L1", [box(-470, -40, -265, -8)]),
    "int_reclaim": ("L0", [box(-150, 30, -40, 55)]),
    "dom_reclaim": ("L0", [box(0, 10, 140, 55)]),
    "arrivals_hall": ("L0", [box(-150, 65, 140, 92)]),
    "arrivals_exit": ("L0", [box(-110, 116, 110, 122)]),
    "int_bus_arrival": ("L0", [box(-480, -30, -390, 15)]),
    "dom_bus_arrival": ("L0", [box(340, -35, 480, 5)]),
    "ga_landside": ("L0", [box(190, 86, 202, 92)]),
    "ga_airside": ("L0", [box(190, 16, 202, 24)]),
}

NODES = [
    # L1 departures: kerb, forecourt, landside hall, check-in, security, airside
    ("K_W", -130, 125, "L1"), ("K_M", 0, 125, "L1"), ("K_E", 130, 125, "L1"),
    ("E_PRM", -35, 105, "L1"), ("E_MAN", 20, 105, "L1"), ("E_DY", 75, 105, "L1"),
    ("LH_W", -150, 88, "L1"), ("LH_1", -70, 88, "L1"), ("LH_P", -35, 88, "L1"), ("LH_2", 20, 88, "L1"),
    ("LH_3", 75, 88, "L1"), ("LH_E", 140, 88, "L1"),
    ("CS_W", -150, 52, "L1"), ("CS_1", -70, 52, "L1"), ("CS_2", 30, 52, "L1"), ("CS_E", 140, 52, "L1"),
    ("SX_D", 30, 10, "L1"), ("SX_P", 125, 10, "L1"),
    ("AS_W", -150, 0, "L1"), ("AS_1", -60, 0, "L1"), ("AS_2", 30, 0, "L1"), ("AS_3", 100, 0, "L1"),
    ("AS_E", 150, -15, "L1"),
    ("EW_1", 220, -20, "L1"), ("EW_2", 330, -15, "L1"), ("EW_3", 420, -5, "L1"), ("EW_4", 490, 10, "L1"),
    ("EW_5", 515, 45, "L1"),
    # L1 international: link, IDT check-in and security, emigration, west wing
    ("LINK", -176, 88, "L1"), ("IDT_E", -195, 88, "L1"), ("IDT_C", -250, 100, "L1"), ("IDT_S", -228, 45, "L1"),
    ("IDT_X", -228, 8, "L1"), ("EM_N", -250, -8, "L1"), ("EM_S", -250, -35, "L1"),
    ("WW_1", -320, -25, "L1"), ("WW_2", -400, -15, "L1"), ("WW_3", -470, 0, "L1"), ("WW_4", -505, 30, "L1"),
    # L0 arrivals
    ("AK_W", -100, 118, "L0"), ("AK_M", 0, 118, "L0"), ("AK_E", 100, 118, "L0"),
    ("AH_W", -140, 80, "L0"), ("AH_M", 0, 80, "L0"), ("AH_E", 140, 80, "L0"),
    ("IM_S", -90, -15, "L0"), ("IM_N", -90, 32, "L0"), ("RI_N", -90, 58, "L0"),
    ("DR_S", 65, -10, "L0"), ("DR_N", 65, 58, "L0"),
    ("L0_W", -160, -40, "L0"), ("L0_1", -90, -40, "L0"), ("L0_2", 65, -40, "L0"), ("L0_E", 150, -30, "L0"),
    ("W0_1", -250, -30, "L0"), ("W0_2", -380, -15, "L0"), ("W0_3", -450, 0, "L0"),
    ("E0_1", 250, -20, "L0"), ("E0_2", 380, -10, "L0"), ("E0_3", 470, 5, "L0"),
    ("CITY", 150, 205, "L0"), ("CITY_X", 140, 170, "L0"),
    ("GA_L", 196, 90, "L0"), ("GA_I", 196, 57, "L0"), ("GA_A", 196, 22, "L0"),
]

EDGES = [
    ("K_W", "K_M"), ("K_M", "K_E"), ("K_W", "E_PRM"), ("K_M", "E_MAN"), ("K_E", "E_DY"),
    ("E_PRM", "E_MAN"), ("E_MAN", "E_DY"),
    ("LH_W", "LH_1"), ("LH_1", "LH_P"), ("LH_P", "LH_2"), ("LH_2", "LH_3"), ("LH_3", "LH_E"),
    ("LH_W", "CS_W"), ("LH_1", "CS_1"), ("LH_2", "CS_2"), ("LH_E", "CS_E"),
    ("CS_W", "CS_1"), ("CS_1", "CS_2"), ("CS_2", "CS_E"),
    ("SX_D", "AS_2"), ("SX_P", "AS_3"),
    ("AS_W", "AS_1"), ("AS_1", "AS_2"), ("AS_2", "AS_3"), ("AS_3", "AS_E"), ("AS_E", "EW_1"),
    ("EW_1", "EW_2"), ("EW_2", "EW_3"), ("EW_3", "EW_4"), ("EW_4", "EW_5"),
    ("LH_W", "LINK"), ("LINK", "IDT_E"), ("IDT_E", "IDT_C"), ("IDT_E", "IDT_S"), ("IDT_C", "IDT_S"),
    ("IDT_X", "EM_N"), ("EM_S", "WW_1"), ("WW_1", "WW_2"), ("WW_2", "WW_3"), ("WW_3", "WW_4"),
    ("AK_W", "AK_M"), ("AK_M", "AK_E"), ("AK_W", "AH_W"), ("AK_M", "AH_M"), ("AK_E", "AH_E"),
    ("AH_W", "AH_M"), ("AH_M", "AH_E"), ("AH_W", "RI_N"), ("RI_N", "IM_N"), ("AH_E", "DR_N"),
    ("DR_N", "DR_S"), ("DR_S", "L0_2"), ("IM_S", "L0_1"),
    ("L0_W", "L0_1"), ("L0_1", "L0_2"), ("L0_2", "L0_E"),
    ("L0_W", "W0_1"), ("W0_1", "W0_2"), ("W0_2", "W0_3"),
    ("L0_E", "E0_1"), ("E0_1", "E0_2"), ("E0_2", "E0_3"),
    ("CITY", "CITY_X"), ("CITY_X", "K_E", 60.0),  # lift / ramp up to the departures kerb
    ("GA_L", "GA_I"), ("GA_I", "GA_A"),
]

DOM_GATES = range(101, 125)
INT_GATES = range(131, 138)
ID_PREFIX = "SYN-HYD"
SPINES = {"dom": ("EW_", "AS_E"), "int": ("WW_",)}   # walkway node prefixes the gates hang off
DEFAULT_OUT = ROOT / "config/terminal_layout.json"
REFERENCE_ELEVATION_BASIS = "poc_approx_amsl_rgia_not_surveyed"
REFERENCE_ELEVATION_NOTE = ("POC placeholder (~617 m AMSL published for RGIA). Replace with GHIAL "
                            "surveyed height of the local origin / L0 above your vertical datum. "
                            "global_elevation_m = reference_elevation_m + elevation_m.")
NOTE = ("Building outlines and gates: OpenStreetMap (ODbL, (c) OpenStreetMap contributors), "
        "relation 13609059 'RGIA Terminal 1' and ways 1016464110, 1016464107. Floors, zones, "
        "touchpoint positions, walkways and areas are a SCHEMATIC drawn inside those outlines, "
        "not the terminal floor plan. Replace with GHIAL CAD/BIM when available. "
        "Generated by scripts/build_terminal_layout.py.")


def gate_flow(number: int) -> str | None:
    return "dom" if number in DOM_GATES else "int" if number in INT_GATES else None


def fetch(path: str | None, query: str = QUERY) -> dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    request = urllib.request.Request(OVERPASS, data=urllib.parse.urlencode({"data": query}).encode(),
                                     headers={"User-Agent": "airporteye-poc/0.1", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def local(point: dict, frame: LocalFrame = FRAME) -> list[float]:
    x, y = frame.from_wgs84(point["lon"], point["lat"])
    return [round(x, 1), round(y, 1)]


def ring(geometry: list[dict], frame: LocalFrame = FRAME) -> list[list[float]]:
    """Polygon ring without the repeated closing point. Relation members can be open
    ways that join their neighbours; those are closed with a straight segment."""
    points = [local(p, frame) for p in geometry]
    return points[:-1] if points[0] == points[-1] else points


def _edge_distance(p: list[float], polygon: list[list[float]]) -> float:
    """Distance from a point to a polygon's boundary (0 inside the polygon)."""
    if point_in_polygon(p[0], p[1], polygon):
        return 0.0
    best = math.inf
    for a, b in zip(polygon, polygon[1:] + polygon[:1]):
        dx, dy = b[0] - a[0], b[1] - a[1]
        t = max(0.0, min(1.0, ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / ((dx * dx + dy * dy) or 1.0)))
        best = min(best, math.hypot(p[0] - a[0] - t * dx, p[1] - a[1] - t * dy))
    return best


def main(spec=None) -> None:
    """Build the terminal layout. ``spec`` is a module with this module's settings (default: Hyderabad)."""
    spec = spec or sys.modules[__name__]
    frame = spec.FRAME
    parser = argparse.ArgumentParser(description=spec.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--osm-json", help="saved Overpass response (default: fetch)")
    parser.add_argument("--out", default=str(spec.DEFAULT_OUT))
    args = parser.parse_args()
    osm = fetch(args.osm_json, spec.QUERY)
    OSM_PARTS = spec.OSM_PARTS  # noqa: N806

    outlines: dict[str, list[list[float]]] = {}
    gates: list[dict] = []
    for element in osm["elements"]:
        if element["type"] == "relation":
            for member in element["members"]:
                if member["ref"] in OSM_PARTS:
                    outlines[OSM_PARTS[member["ref"]]] = ring(member["geometry"], frame)
        elif element["type"] == "way" and element["id"] in OSM_PARTS:
            outlines[OSM_PARTS[element["id"]]] = ring(element["geometry"], frame)
        elif element["type"] == "node" and element.get("tags", {}).get("aeroway") == "gate":
            gates.append({"ref": element["tags"].get("ref", ""), "osm_xy": local(element, frame)})
    missing = set(OSM_PARTS.values()) - set(outlines)
    if missing:
        raise SystemExit(f"OSM response is missing: {sorted(missing)}")

    nodes = [{"id": n, "x": x, "y": y, "floor": f} for n, x, y, f in spec.NODES]
    edges = [list(e) for e in spec.EDGES]
    spines = {flow: [n for n in nodes if n["id"].startswith(prefixes)] for flow, prefixes in spec.SPINES.items()}
    gate_list = []
    max_off = getattr(spec, "GATE_MAX_DISTANCE_M", None)   # drop gates mapped away from these buildings
    if max_off is not None:
        gates = [g for g in gates if min(_edge_distance(g["osm_xy"], o) for o in outlines.values()) <= max_off]
    for gate in sorted(gates, key=lambda g: g["ref"]):
        number = int("".join(ch for ch in gate["ref"] if ch.isdigit()) or 0)
        flow = spec.gate_flow(number)
        if flow is None:
            continue
        gx, gy = gate["osm_xy"]
        spine = min(spines[flow], key=lambda n: math.hypot(n["x"] - gx, n["y"] - gy))
        # pull the gate 10 m inside the building, towards the pier's walkway
        dist = math.hypot(spine["x"] - gx, spine["y"] - gy) or 1.0
        pull = min(getattr(spec, "GATE_PULL_IN_M", 10.0), dist - 2.0)
        x, y = round(gx + (spine["x"] - gx) * pull / dist, 1), round(gy + (spine["y"] - gy) * pull / dist, 1)
        gate_id = f"{spec.ID_PREFIX}-GATE-{number}"
        gate_list.append({"gate_id": gate_id, "osm_ref": gate["ref"], "flow": flow, "x": x, "y": y, "floor": "L1"})
        nodes.append({"id": gate_id, "x": x, "y": y, "floor": "L1"})
        edges.append([gate_id, spine["id"]])

    zones = []
    for zone_id, floor, name, polygons in spec.ZONES:
        shapes = [outlines[p[4:]] if isinstance(p, str) else p for p in polygons]
        zones.append({"zone_id": zone_id, "name": name, "floor": floor, "polygons": shapes})

    layout = {
        "_note": spec.NOTE,
        "osm_timestamp": osm.get("osm3s", {}).get("timestamp_osm_base"),
        "frame": {"frame_id": frame.frame_id, "origin_lat": frame.origin_lat, "origin_lon": frame.origin_lon,
                  "rotation_deg": frame.rotation_deg, "crs": "EPSG:4326",
                  "axes": "x east, y north, metres (local tangent plane)",
                  "reference_elevation_m": spec.REFERENCE_ELEVATION_M,
                  "reference_elevation_basis": spec.REFERENCE_ELEVATION_BASIS,
                  "_reference_elevation_note": spec.REFERENCE_ELEVATION_NOTE},
        "floors": spec.FLOORS,
        "buildings": [{"name": name, "polygon": outline} for name, outline in outlines.items()],
        "zones": zones,
        "touchpoints": {tp: {"x": x, "y": y, "floor": f, "queue_direction": list(d), "service_width_m": w}
                        for tp, (x, y, f, d, w) in spec.TOUCHPOINTS.items()},
        "gates": gate_list,
        "areas": {name: {"floor": floor, "polygons": polys} for name, (floor, polys) in spec.AREAS.items()},
        "walkways": {"nodes": nodes, "edges": edges},
    }
    if hasattr(spec, "postprocess"):   # e.g. Delhi: fit the walkways to the DIAL floor plan
        spec.postprocess(layout)
    # sanity: every touchpoint and node on a building floor should be inside a zone of its floor
    loose = [tp for tp, t in layout["touchpoints"].items()
             if not any(z["floor"] == t["floor"] and any(point_in_polygon(t["x"], t["y"], p) for p in z["polygons"])
                        for z in zones)]
    Path(args.out).write_text(json.dumps(layout, indent=1) + "\n", encoding="utf-8")
    print(f"Wrote {args.out}: {len(outlines)} outlines, {len(zones)} zones, {len(layout['touchpoints'])} touchpoints, "
          f"{len(gate_list)} gates, {len(nodes)} walkway nodes")
    if loose:
        print(f"Touchpoints outside every zone of their floor: {loose}")


if __name__ == "__main__":
    main()
