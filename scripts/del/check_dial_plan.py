"""Check the Delhi departures level (L1) against the DIAL floor plan.

Reports every L1 checkpoint (its service line and first queue rows), walkway node, walkway edge, gate and
dwell-area sample that lands in a space passengers cannot stand in on the DIAL plan (walls, shops, toilets,
private rooms, stairs, lifts, glass voids, gardens). Small unlabelled fixtures, queue barriers
and doors are ignored (see dial_plan.BLOCKED). Exit status 1 when anything is blocked, so it can run after a rebuild.

  python scripts/del/check_dial_plan.py
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from airport_eye.spatial import LocalFrame  # noqa: E402
from dial_plan import CONNECTORS, Plan  # noqa: E402


def queue_points(t: dict, rows: int = 6) -> list[tuple[float, float]]:
    """The service line ends and centre, and the first queue rows (same geometry as generators/passenger.py)."""
    dx, dy = t["queue_direction"]
    n = math.hypot(dx, dy) or 1.0
    dx, dy = dx / n, dy / n
    px, py = dy, -dx
    w = t["service_width_m"]
    per_row = max(2, int(min(w, 30) / 0.75))
    pts = [(t["x"] + px * s * w / 2, t["y"] + py * s * w / 2) for s in (-0.9, 0, 0.9)]
    for row in range(rows):
        for col in (0, per_row // 2, per_row - 1):
            lat = (col - (per_row - 1) / 2) * 0.75
            along = 2.5 + row * 0.9
            pts.append((t["x"] + dx * along + px * lat, t["y"] + dy * along + py * lat))
    return pts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layout", default=str(ROOT / "config/del/terminal_layout.json"))
    ap.add_argument("--plan", default=str(ROOT / "config/del/dial_t3_departures_plan.geojson"))
    ap.add_argument("--floor", default="L1")
    args = ap.parse_args()
    layout = json.loads(Path(args.layout).read_text(encoding="utf-8"))
    doors = layout.get("floor_plans", {}).get(args.floor, {}).get("assumed_doors", [])
    plan = Plan(LocalFrame.from_dict(layout["frame"]), Path(args.plan), doors=doors)
    problems: list[str] = []
    connectors = 0

    for tp, t in layout["touchpoints"].items():
        if t["floor"] != args.floor:
            continue
        bad = [(round(x, 1), round(y, 1), why) for x, y in queue_points(t) if (why := plan.blocked(x, y))]
        if bad:
            problems.append(f"checkpoint {tp}: {len(bad)} of {len(queue_points(t))} queue points blocked, e.g. {bad[:3]}")
    nodes = {n["id"]: n for n in layout["walkways"]["nodes"]}
    for n in nodes.values():
        allow = CONNECTORS if "~" in n["id"] else frozenset()   # a corner of a walk routed across a ramp
        if n["floor"] == args.floor and (why := plan.blocked(n["x"], n["y"], allow)):
            problems.append(f"node {n['id']} ({n['x']}, {n['y']}): {why}")
    for a, b in layout["walkways"]["edges"]:
        na, nb = nodes[a], nodes[b]
        if na["floor"] != args.floor or nb["floor"] != args.floor:
            continue
        length = math.hypot(nb["x"] - na["x"], nb["y"] - na["y"])
        hits = plan.crossings(na["x"], na["y"], nb["x"], nb["y"], allow=CONNECTORS)
        if hits:
            problems.append(f"edge {a} -> {b} ({length:.0f} m) crosses {hits}")
        elif plan.crossings(na["x"], na["y"], nb["x"], nb["y"]):
            connectors += 1
    rng = random.Random(1)
    for name, area in layout["areas"].items():
        if area["floor"] != args.floor:
            continue
        samples = []
        for poly in area["polygons"]:
            xs, ys = [p[0] for p in poly], [p[1] for p in poly]
            samples += [(rng.uniform(min(xs), max(xs)), rng.uniform(min(ys), max(ys))) for _ in range(200)]
        bad = [s for s in samples if plan.blocked(*s)]
        if len(bad) > 0.1 * len(samples):
            problems.append(f"area {name}: {len(bad) / len(samples):.0%} of sample points blocked")

    print("\n".join(problems) if problems else f"{args.floor}: nothing lands in a wall, shop, room or void on the DIAL plan")
    print(f"{connectors} walkway links cross a ramp / escalator polygon (the only way into the piers on the plan); "
          f"assumed doors (x, y, radius): {doors}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
