"""Export an airport's static map layers as plain GeoJSON (WGS84) for Esri / GeoKno.

  terminal_layout.geojson  building outlines and zones (polygons, join zone_id), checkpoints
                           (points, join touchpoint_id) and gates (points, join gate_id)
  ground_layout.geojson    kerbs (lines), car parks (polygons), stands (points), named roads
                           and apron roads (lines); join facility_id, stand_id or road_id

  python scripts/export_map_layers.py --airport del --out share/delhi_sample
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from airport_eye.airports import AIRPORTS  # noqa: E402
from airport_eye.spatial import LocalFrame  # noqa: E402
from map_viewer import ground_geojson, layout_geojson  # noqa: E402


def feature(geometry: dict, **props) -> dict:
    return {"type": "Feature", "geometry": geometry, "properties": props}


def terminal_layers(path: str) -> dict:
    layout = json.loads(Path(path).read_text(encoding="utf-8"))
    frame = LocalFrame.from_dict(layout["frame"])
    out = [f | {"properties": {"layer": "zone" if "zone_id" in f["properties"] else "building", **f["properties"]}}
           for f in layout_geojson(path)["features"]]
    for tp_id, t in layout["touchpoints"].items():
        out.append(feature({"type": "Point", "coordinates": list(frame.to_wgs84(t["x"], t["y"]))},
                           layer="checkpoint", touchpoint_id=tp_id, floor=t["floor"],
                           elevation_m=layout["floors"][t["floor"]]["z"]))
    for g in layout["gates"]:
        out.append(feature({"type": "Point", "coordinates": list(frame.to_wgs84(g["x"], g["y"]))},
                           layer="gate", gate_id=g["gate_id"], osm_ref=g["osm_ref"], flow=g["flow"], floor=g["floor"]))
    return {"type": "FeatureCollection", "name": "terminal_layout", "_note": layout.get("_note", ""), "features": out}


def ground_layers(path: str, road_ids: dict[str, str] | None = None) -> dict:
    g = ground_geojson(path)
    out = []
    for k in g["kerbs"]:
        line = k.get("line") or []
        geom = ({"type": "LineString", "coordinates": line} if len(line) > 1
                else {"type": "Point", "coordinates": k.get("point") or line[0]})
        out.append(feature(geom, layer="kerb", facility_id=k["id"], name=k["name"], floor=k.get("floor")))
    for c in g["car_parks"]:
        out.append(feature({"type": "Polygon", "coordinates": [c["polygon"] + [c["polygon"][0]]]},
                           layer="car_park", facility_id=c["id"], name=c["name"]))
    for s in g["stands"]:
        out.append(feature({"type": "Point", "coordinates": s["point"]}, layer="stand", stand_id=s["id"], flow=s["flow"]))
    for r in g["roads"]:
        out.append(feature({"type": "MultiLineString", "coordinates": r["segments"]}, layer="road",
                           road_id=(road_ids or {}).get(r["name"]), name=r["name"]))
    out.append(feature({"type": "MultiLineString", "coordinates": g["apron_roads"]}, layer="apron_road",
                       name="Apron service roads"))
    note = json.loads(Path(path).read_text(encoding="utf-8")).get("_note", "")
    return {"type": "FeatureCollection", "name": "ground_layout", "_note": note, "features": out}


def road_ids_for(path: str, prefix: str) -> dict[str, str]:
    """Same road_id the road.traffic feed uses (see GroundSimulation._road_index)."""
    names = {e[5] for e in json.loads(Path(path).read_text(encoding="utf-8"))["roads"]["edges"] if e[5] and e[6] == "landside"}
    ids = {}
    for name in names:
        slug = "".join(ch if ch.isalnum() else "-" for ch in name.upper()).strip("-")
        while "--" in slug:
            slug = slug.replace("--", "-")
        ids[name] = f"{prefix}-ROAD-{slug}"[:64]
    return ids


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--airport", choices=sorted(AIRPORTS), default="hyd")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    files = AIRPORTS[args.airport]
    prefix = json.loads((ROOT / files["tracking_config"]).read_text(encoding="utf-8")).get("id_prefix", "SYN-HYD")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, data in (("terminal_layout.geojson", terminal_layers(str(ROOT / files["zones"]))),
                       ("ground_layout.geojson", ground_layers(str(ROOT / files["ground"]),
                                                              road_ids_for(str(ROOT / files["ground"]), prefix)))):
        (out / name).write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
        print(f"Wrote {out / name}: {len(data['features'])} features")


if __name__ == "__main__":
    main()
