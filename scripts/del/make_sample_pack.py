"""Build the Delhi IGI T3 (DIAL) sample pack for Esri / GeoKno / DIAL: every feed as CSV and JSONL, the static map
layers, a one-instant snapshot to drop into a map, the DIAL plan fit drawing, a manifest and a README, then zip it.

  python scripts/del/make_sample_pack.py                       # 9 Oct 2026, 18:00-18:15 IST, positions every 30 s
  python scripts/del/make_sample_pack.py --minutes 5 --interval 5 --out share/delhi_sample_5s \\
      --zip share/AirportEye_Delhi_T3_sample_5s.zip

The out folder is rebuilt from scratch (only a folder this script made before, marked by .airporteye_pack, is
replaced). All records are synthetic; see the README it writes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from airport_eye.spatial import LocalFrame  # noqa: E402
from dial_plan import CONNECTORS, DEFAULT_PLAN, Plan  # noqa: E402

LAYOUT = ROOT / "config/del/terminal_layout.json"
TOPICS = json.loads((ROOT / "config/del/rest_proxy.json").read_text(encoding="utf-8"))["topics"]
MARKER = ".airporteye_pack"

# file stem, event type, rate, track id, what it is
FEEDS = [
    ("passenger_positions", "passenger.position", "every 5 s live", "passenger_id",
     "Every passenger: position, floor, zone, activity, checkpoint, flight, gate"),
    ("passenger_queues", "passenger.touchpoint", "every minute", "touchpoint_id",
     "Checkpoint queues: queue length, wait, lanes open, status against target"),
    ("zone_occupancy", "passenger.aggregate", "every minute", "zone_id", "People per zone and density"),
    ("journey_kpis", "passenger.journey_kpi", "every minute", "kpi_id",
     "Kerb-to-airside, airside-to-boarding and arrival-to-exit times (no location)"),
    ("vehicle_positions", "vehicle.position", "every 5 s live", "vehicle_id",
     "Private cars, app cabs, taxis, buses on the T3 roads and kerbs"),
    ("kerb_occupancy", "curb.occupancy", "every minute", "facility_id",
     "Departures and arrivals kerbs, bus station, T3 car parks"),
    ("road_traffic", "road.traffic", "every minute", "road_id", "IGI departure and arrival roads: vehicles, speed, congestion"),
    ("gse_positions", "gse.position", "every 5 s live", "vehicle_id",
     "Ground handling: baggage tractors, fuel, catering, apron buses, tugs, ground power"),
    ("stand_activity", "stand.activity", "every minute", "stand_id",
     "51 contact stands: flight, phase, vehicles present, late tasks, status"),
]
POSITION_FEEDS = {"passenger.position", "vehicle.position", "gse.position"}


def run(*cmd: str) -> None:
    print("$", " ".join(Path(c).name if c == sys.executable else c for c in cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def fresh_folder(out: Path) -> None:
    if out.exists():
        if not (out / MARKER).exists() and any(out.iterdir()):
            raise SystemExit(f"{out} exists and was not made by this script; pick another --out or empty it")
        shutil.rmtree(out)
    out.mkdir(parents=True)
    (out / MARKER).write_text("made by scripts/del/make_sample_pack.py\n", encoding="utf-8")


def jsonl(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def snapshot(out: Path, files: dict[str, Path]) -> tuple[str, int]:
    """Every passenger, vehicle and ground-handling vehicle at the last instant, as one GeoJSON point layer."""
    last = max(r["event_time_epoch_ms"] for r in jsonl(files["passenger.position"]))
    features = []
    for event_type in ("passenger.position", "vehicle.position", "gse.position"):
        for r in jsonl(files[event_type]):
            if r["event_time_epoch_ms"] == last:
                props = {k: v for k, v in r.items() if not isinstance(v, (dict, list))}
                features.append({"type": "Feature", "properties": props,
                                 "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]}})
    stamp = datetime.fromtimestamp(last / 1000, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = f"snapshot_positions_{stamp}.geojson"
    (out / name).write_text(json.dumps({"type": "FeatureCollection", "name": name[:-8], "features": features},
                                       separators=(",", ":")), encoding="utf-8")
    return name, len(features)


def open_floor_share(position_file: Path, layout: dict) -> float:
    frame = LocalFrame.from_dict(layout["frame"])
    plan = Plan(frame, doors=layout["floor_plans"]["L1"]["assumed_doors"])
    n = ok = 0
    for r in jsonl(position_file):
        if r.get("floor") == "L1":
            n += 1
            ok += plan.blocked(*frame.from_wgs84(r["longitude"], r["latitude"]), CONNECTORS) is None
    return ok / n if n else 1.0


PLAN_FILL = {"corridor": "#eef2f7", "corridor_high": "#dfe6f0", "exterior": "#f6f6f6", "road": "#b9b9b9",
             "shop": "#f6cf8c", "f&b": "#f2b48a", "wc": "#a9d9cd", "private": "#cfcfcf", "office": "#cbc2e3",
             "stairs": "#e9b3cf", "lift": "#b98db3", "wall": "#3d4450", "mini_wall": "#3d4450", "glass": "#a9d8f0",
             "glass_wall": "#6fb2dc", "glass_wall_high": "#6fb2dc", "garden": "#b9dc95"}


def fit_drawing(out: Path, layout: dict) -> str:
    """The DIAL departures plan with our L1 checkpoints and walkways on it (SVG, the terminal's local metres)."""
    frame = LocalFrame.from_dict(layout["frame"])
    plan = json.loads(DEFAULT_PLAN.read_text(encoding="utf-8"))["features"]
    x0, x1, y0, y1, k = -260.0, 260.0, -260.0, 230.0, 3.0
    width, height = (x1 - x0) * k, (y1 - y0) * k

    def p(x: float, y: float) -> str:
        return f"{(x - x0) * k:.1f},{(y1 - y) * k:.1f}"

    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0f} {height + 40:.0f}" '
           f'font-family="Helvetica, Arial, sans-serif"><rect width="100%" height="100%" fill="#fff"/>']
    big = {"exterior", "road", "corridor", "corridor_high", "garden"}
    for f in sorted(plan, key=lambda f: f["properties"]["category"] not in big):
        c = f["properties"]["category"]
        pts = " ".join(p(*frame.from_wgs84(lon, lat)) for lon, lat in f["geometry"]["coordinates"][0])
        svg.append(f'<polygon points="{pts}" fill="{PLAN_FILL.get(c, "#e3d8bd")}" stroke="#8a8f98" stroke-width="0.2"/>')
    nodes = {n["id"]: n for n in layout["walkways"]["nodes"]}
    for a, b in layout["walkways"]["edges"]:
        na, nb = nodes[a], nodes[b]
        if na["floor"] == nb["floor"] == "L1":
            svg.append(f'<polyline points="{p(na["x"], na["y"])} {p(nb["x"], nb["y"])}" stroke="#d6249f" '
                       f'stroke-width="1.2" fill="none"/>')
    for tp, t in layout["touchpoints"].items():
        if t["floor"] == "L1":
            cx, cy = p(t["x"], t["y"]).split(",")
            svg.append(f'<circle cx="{cx}" cy="{cy}" r="3.5" fill="#1d4ed8"/><text x="{float(cx) + 5}" y="{cy}" '
                       f'font-size="9" fill="#1d4ed8">{tp.removeprefix("SYN-DEL-")}</text>')
    svg.append(f'<text x="10" y="{height + 25:.0f}" font-size="14" fill="#222">DIAL T3 departures plan (level 2542) with '
               'AirportEye L1 checkpoints (blue) and walkways (pink). Local frame: +y landside, rotated 60 degrees.</text>')
    svg.append("</svg>")
    (out / "dial_plan_fit_L1.svg").write_text("\n".join(svg), encoding="utf-8")
    return "dial_plan_fit_L1.svg"


def readme(out: Path, start: datetime, minutes: int, interval: int, counts: dict, stems: dict, snap: tuple[str, int],
           open_share: float) -> None:
    ist, utc = start, start.astimezone(timezone.utc)
    zone = "IST" if start.utcoffset() and start.utcoffset().total_seconds() == 19800 else f"{start:%z}"
    rows = "\n".join(f"| `{stems[e]}.csv` / `.jsonl` | `{TOPICS[e]}` | {rate} | {counts[e]:,} | `{track}` | {what} |"
                     for _, e, rate, track, what in FEEDS)
    text = f"""# AirportEye POC: Delhi IGI (DIAL) Terminal 3 sample data

{minutes} simulated minutes at Terminal 3, starting {ist:%d %b %Y %H:%M} {zone} ({utc:%H:%M} UTC). The same feeds and the
same flat Esri JSON format as the Hyderabad (RGIA) POC. **All passengers, flights and vehicles are synthetic.**

Each `.csv` and `.jsonl` pair holds the same records. One JSONL line is one Kafka message. Positions are every {interval} s
in this sample (5 s live). Every record has `longitude`/`latitude` (WGS84, EPSG:4326), `floor`, `elevation_m` (height above
the ground floor), `global_elevation_m` and `event_time_epoch_ms` (UTC). IDs start `SYN-DEL-`.

| File | Kafka topic (Delhi) | Rate | Records here | Track / join ID | What it is |
|---|---|---|---|---|---|
{rows}
| `terminal_layout.geojson` | none | static | | `zone_id`, `touchpoint_id`, `gate_id` | Building outlines, zones, checkpoints, gates |
| `ground_layout.geojson` | none | static | | `facility_id`, `stand_id`, `road_id` | Kerbs, car parks, stands, roads, apron roads |
| `{snap[0]}` | none | one instant | {snap[1]:,} | `entity_id` | Every passenger, vehicle and ground vehicle at the last instant: drag into a map to check |
| `dial_plan_fit_L1.svg` | none | static | | | The DIAL departures plan with our checkpoints and walkways on it (open in a browser) |
| `one_record_per_feed.json` | none | none | | | One example record per feed |
| `manifest.json` | none | none | | | Every file with its record count and SHA-256 |

## Import into ArcGIS

- **ArcGIS Online:** Content > New item > upload the `.csv`. Location: Longitude = `longitude`, Latitude =
  `latitude`. For a GeoJSON file (the layouts or the snapshot), upload it as GeoJSON.
- **ArcGIS Pro:** use *XY Table To Point* on a `.csv` (X `longitude`, Y `latitude`, WGS 1984), or *JSON To Features* on a
  `.geojson`. For a time slider, enable time on `event_time` (UTC) or `event_time_epoch_ms`.
- **Tracks:** track ID as in the table, time `event_time_epoch_ms`. Symbolise passengers by `activity` and vehicles by
  `vehicle_type` / `state`. Filter `floor = 'L1'` (departures) or `'L0'` (arrivals).
- **Velocity / GeoEvent, same as live Kafka:** read the `.jsonl` (one flat JSON object per line, nothing to flatten).

## Check it

- `manifest.json` has the record count and SHA-256 of every file (`shasum -a 256 <file>` to compare).
- Positions stay on the right site: every record lies at T3 (latitude about 28.55, longitude about 77.08).
- Departures level `L1` follows the DIAL floor plan. {open_share:.1%} of the `L1` passenger positions here are on its open floor.
  Most of the rest are passengers getting out of cars on the kerb road, plus boarding queues at the gates.

## What is real and what is assumed

**Real:** the Terminal 3 outline (processor, international and domestic piers), the 51 contact gates, the landside
roads, kerb roads, car parks and the airport bus station (OpenStreetMap, ODbL). **The departures level `L1` follows the DIAL
floor plan** (`19267_f2_geojson.json`, level 2542, received 15 Oct 2026):
- checkpoints sit on the plan's counters and lanes: entry at the front-wall doors, check-in at the seven islands
  (international the west three, domestic the east three, bag drop the middle one), the emigration desk row with the
  international security lanes behind it, and the domestic security lanes;
- international departures clear **emigration first, then security**, as the plan lays them out;
- walkways, gate waiting areas and dwell areas are kept on open floor, off walls, shops, toilets, offices, escalators and voids.

**Assumed (to replace with DIAL data):**
- The plan has no labels, so which counters are which is our reading of the drawing.
- The piers join the processor across the ramp / escalator areas at each pier head. There is a door in the thin wall across
  the domestic pier head.
- The arrivals level `L0` (immigration, reclaim, arrivals hall) is schematic until the DIAL arrivals plan arrives.
- Floor heights: departures `L1` at 10 m and arrivals `L0` at 0 m, with ground at 237 m above sea level. These wait on the DIAL BIM levels.
- Zone boundaries, flight rates, passenger numbers, lanes open and vehicle fleets.
- Stands and apron roads: OpenStreetMap maps almost none at T3, so a stand is placed out from each gate.

**Scope:** the whole of Terminal 3 for now. Once GeoKno's selected POC area is confirmed, the data will be limited to it.

Made by `scripts/del/make_sample_pack.py` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.
"""
    (out / "README.md").write_text(text, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-09T18:00:00+05:30", help="start time with offset")
    ap.add_argument("--minutes", type=int, default=15)
    ap.add_argument("--interval", type=int, default=30, help="seconds between position records (5 = live rate)")
    ap.add_argument("--out", default=str(ROOT / "share/delhi_sample"))
    ap.add_argument("--zip", default=str(ROOT / "share/AirportEye_Delhi_T3_sample.zip"))
    args = ap.parse_args()
    start = datetime.fromisoformat(args.start)
    out, zip_path = Path(args.out).resolve(), Path(args.zip).resolve()

    run(sys.executable, "scripts/del/check_dial_plan.py")   # stop if the layout no longer fits the DIAL plan
    fresh_folder(out)
    run(sys.executable, "-m", "airport_eye", "pax-files", "--airport", "del", "--start", args.start,
        "--minutes", str(args.minutes), "--interval", str(args.interval), "--out", str(out))
    run(sys.executable, "scripts/export_map_layers.py", "--airport", "del", "--out", str(out))

    stamp = start.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    stems = {e: f"{stem}_{stamp}" for stem, e, *_ in FEEDS}
    files = {e: out / f"{s}.jsonl" for e, s in stems.items()}
    missing = [str(p) for p in files.values() if not p.exists()]
    if missing:
        raise SystemExit(f"pax-files did not write: {missing}")

    counts, examples = {}, {}
    for e, path in files.items():
        n = 0
        for r in jsonl(path):
            n += 1
            if e not in examples and (e != "passenger.position" or r.get("floor") == "L1"):
                examples[e] = r
        counts[e] = n
    (out / "one_record_per_feed.json").write_text(json.dumps(examples, indent=1) + "\n", encoding="utf-8")
    snap = snapshot(out, files)
    layout = json.loads(LAYOUT.read_text(encoding="utf-8"))
    fit_drawing(out, layout)
    share = open_floor_share(files["passenger.position"], layout)
    readme(out, start, args.minutes, args.interval, counts, stems, snap, share)

    manifest = {"made_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "airport": "DEL",
                "start": args.start, "minutes": args.minutes, "position_interval_seconds": args.interval,
                "kafka_topics": TOPICS, "files": {}}
    for f in sorted(out.iterdir()):
        if f.name in (MARKER, "manifest.json"):
            continue
        records = counts.get(next((e for e, s in stems.items() if f.stem == s), ""), None)
        manifest["files"][f.name] = {"bytes": f.stat().st_size, "sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
                                     **({"records": records} if records is not None else {})}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = zip_path.with_suffix(".zip.part")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for f in sorted(out.iterdir()):
            if f.name != MARKER:
                z.write(f, f"{out.name}/{f.name}")
    tmp.replace(zip_path)

    print(f"\n{out}: {sum(counts.values()):,} records in {len(counts)} feeds; "
          f"{share:.1%} of L1 passenger positions on open DIAL floor")
    print(f"{zip_path}: {zip_path.stat().st_size / 1e6:.1f} MB")
    print("Records per feed:", dict(Counter({e: counts[e] for e in counts}).most_common()))


if __name__ == "__main__":
    main()
