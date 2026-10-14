"""Local operations dashboard for the AirportEye feeds - no Esri software needed.

Reads the flat Esri-format records (the same ones ArcGIS Velocity / GeoEvent
Server would ingest) and shows what an operations team would look at: terminal
KPIs, checkpoint queues against their targets, zone occupancy, a passenger
heatmap and individual tracks, per floor, with a replay of the last 90 minutes.

  python scripts/map_viewer.py --bootstrap localhost:9092     # live from Kafka (all four topics)
  python scripts/map_viewer.py --simulate                     # run the simulation in-process, no Kafka
  python scripts/map_viewer.py --file generated_data/passenger_touchpoint.jsonl   # queue replay only

Then open http://127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import json
import math
import mimetypes
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
VIEWER = HERE / "viewer"
PREFIX = "airport-eye.poc.esri."
FEEDS = ("passenger.touchpoint", "passenger.position", "passenger.aggregate", "passenger.journey_kpi",
         "vehicle.position", "curb.occupancy", "road.traffic", "gse.position", "stand.activity")
VEHICLE_FIELDS = ("longitude", "latitude", "floor", "vehicle_type", "domain", "state", "speed_kmh", "heading_deg",
                  "facility_id", "stand_id", "flight_id", "task", "passengers_on_board", "dwell_seconds", "overstay",
                  "road_name")
POSITION_FIELDS = ("longitude", "latitude", "floor", "activity", "journey", "flight_id", "gate_id",
                   "indoor_zone_id", "touchpoint_id", "minutes_to_departure", "heading_deg")
TP_FIELDS = ("entity_id", "touchpoint_name", "touchpoint_type", "flow", "floor", "longitude", "latitude",
             "queue_length", "queue_limit", "passengers_in_service", "service_channels", "utilization_pct",
             "processed_passengers", "throughput_per_hour", "throughput_since_midnight", "estimated_wait_seconds",
             "avg_wait_time_seconds", "avg_processing_time_seconds", "p95_wait_seconds", "target_wait_seconds",
             "aera_existing_wait_seconds", "aera_proposed_wait_seconds", "status", "sla_breached", "zone_id",
             "event_time", "event_time_epoch_ms")
SNAPSHOT_MINUTES = 90
TRACK_POINTS = 360


class TwinState:
    """Latest view of all four feeds, short histories, per-minute snapshots and passenger tracks."""

    def __init__(self, stale_ms: int = 20_000) -> None:
        self.lock = threading.Lock()
        self.touchpoints: dict[str, dict] = {}
        self.tp_history: dict[str, deque] = {}
        self.zones: dict[str, dict] = {}
        self.kpi: dict | None = None
        self.kpi_history: deque = deque(maxlen=SNAPSHOT_MINUTES)
        self.positions: dict[str, tuple[int, list]] = {}
        self.vehicles: dict[str, tuple[int, list]] = {}
        self.curbs: dict[str, dict] = {}
        self.roads: dict[str, dict] = {}
        self.stands: dict[str, dict] = {}
        self.tracks: dict[str, deque] = {}
        self.timelines: dict[str, list] = {}
        self.ended: dict[str, int] = {}
        self.snapshots: deque = deque(maxlen=SNAPSHOT_MINUTES)
        self.newest_ms = 0
        self.received = {feed: 0 for feed in FEEDS}
        self.last_received_at: float | None = None
        self.source = ""
        self.error: str | None = None
        self.stale_ms = stale_ms

    # -- ingest ----------------------------------------------------------------
    def add(self, record: dict) -> None:
        kind = record.get("event_type")
        with self.lock:
            if kind in self.received:
                self.received[kind] += 1
            self.last_received_at = time.time()
            when = record.get("event_time_epoch_ms") or 0
            self.newest_ms = max(self.newest_ms, when)
            if kind == "passenger.position":
                self._add_position(record, when)
            elif kind == "passenger.touchpoint":
                tp = record.get("entity_id")
                self.touchpoints[tp] = {k: record.get(k) for k in TP_FIELDS}
                self.tp_history.setdefault(tp, deque(maxlen=60)).append(
                    [when, record.get("queue_length"), record.get("estimated_wait_seconds")])
            elif kind in ("vehicle.position", "gse.position"):
                vid = record.get("entity_id")
                if vid and record.get("longitude") is not None:
                    if record.get("state") in ("exited", "parked"):
                        self.vehicles.pop(vid, None)
                    else:
                        self.vehicles[vid] = (when, [record.get(f) for f in VEHICLE_FIELDS])
            elif kind == "curb.occupancy":
                self.curbs[record.get("entity_id")] = record
            elif kind == "road.traffic":
                self.roads[record.get("entity_id")] = record
            elif kind == "stand.activity":
                self.stands[record.get("entity_id")] = record
            elif kind == "passenger.aggregate":
                self.zones[record.get("zone_id")] = {k: v for k, v in record.items() if k != "cells"}
            elif kind == "passenger.journey_kpi":
                self.kpi = record
                self.kpi_history.append([when, record.get("people_in_terminal"), record.get("people_queuing"),
                                         record.get("kerb_to_airside_p50_minutes"),
                                         record.get("arrival_to_exit_p50_minutes")])
                self._snapshot(when)

    def _add_position(self, record: dict, when: int) -> None:
        pax = record.get("entity_id")
        if not pax or record.get("longitude") is None:
            return
        values = [record.get(f) for f in POSITION_FIELDS]
        track = self.tracks.setdefault(pax, deque(maxlen=TRACK_POINTS))
        track.append([when, values[0], values[1], values[2], values[3]])
        timeline = self.timelines.setdefault(pax, [])
        step = [when, record.get("activity"), record.get("touchpoint_id"), record.get("indoor_zone_id")]
        if not timeline or timeline[-1][1:3] != step[1:3]:
            timeline.append(step)
            del timeline[:-60]
        if record.get("activity") in ("boarded", "exited"):
            self.positions.pop(pax, None)
            self.ended[pax] = when
        else:
            self.positions[pax] = (when, values)

    def _prune(self) -> None:
        cutoff = self.newest_ms - self.stale_ms
        for pax in [p for p, (when, _) in self.positions.items() if when < cutoff]:
            del self.positions[pax]
            self.ended[pax] = self.newest_ms
        for vid in [v for v, (when, _) in self.vehicles.items() if when < cutoff]:
            del self.vehicles[vid]
        forget = self.newest_ms - 10 * 60_000  # keep ended tracks for 10 minutes
        for pax in [p for p, when in self.ended.items() if when < forget]:
            self.ended.pop(pax, None)
            self.tracks.pop(pax, None)
            self.timelines.pop(pax, None)

    def _positions_payload(self) -> dict:
        return {"fields": ["passenger_id", *POSITION_FIELDS],
                "rows": [[pax] + values for pax, (_, values) in self.positions.items()]}

    def _vehicles_payload(self) -> dict:
        return {"fields": ["vehicle_id", *VEHICLE_FIELDS],
                "rows": [[vid] + values for vid, (_, values) in self.vehicles.items()]}

    def _ground(self) -> dict:
        return {"curbs": list(self.curbs.values()), "roads": list(self.roads.values()),
                "stands": list(self.stands.values())}

    def _snapshot(self, when: int) -> None:
        self._prune()
        self.snapshots.append({"t": when, "kpi": self.kpi, "touchpoints": list(self.touchpoints.values()),
                               "zones": list(self.zones.values()), **self._ground(),
                               "positions": json.dumps(self._positions_payload(), separators=(",", ":")),
                               "vehicles": json.dumps(self._vehicles_payload(), separators=(",", ":"))})

    # -- views -----------------------------------------------------------------
    def state(self) -> dict:
        with self.lock:
            self._prune()
            age = None if self.last_received_at is None else round(time.time() - self.last_received_at, 1)
            touchpoints = [dict(tp, history=list(self.tp_history.get(tp["entity_id"], [])))
                           for tp in self.touchpoints.values()]
            return {"mode": "live", "t": self.newest_ms or None, "kpi": self.kpi,
                    "kpi_history": list(self.kpi_history), "touchpoints": touchpoints,
                    "zones": list(self.zones.values()), "positions": self._positions_payload(),
                    "vehicles": self._vehicles_payload(), **self._ground(),
                    "snapshots": [s["t"] for s in self.snapshots],
                    "stats": {"received": self.received, "source": self.source, "seconds_since_last": age,
                              "error": self.error}}

    def snapshot(self, t: int) -> dict | None:
        with self.lock:
            if not self.snapshots:
                return None
            snap = min(self.snapshots, key=lambda s: abs(s["t"] - t))
            kpi_history = [k for k in self.kpi_history if k[0] <= snap["t"]]
            touchpoints = [dict(tp, history=[h for h in self.tp_history.get(tp["entity_id"], []) if h[0] <= snap["t"]])
                           for tp in snap["touchpoints"]]
            return {"mode": "replay", "t": snap["t"], "kpi": snap["kpi"], "kpi_history": kpi_history,
                    "touchpoints": touchpoints, "zones": snap["zones"], "positions": json.loads(snap["positions"]),
                    "vehicles": json.loads(snap.get("vehicles") or '{"fields":[],"rows":[]}'),
                    "curbs": snap.get("curbs", []), "roads": snap.get("roads", []), "stands": snap.get("stands", []),
                    "snapshots": [s["t"] for s in self.snapshots],
                    "stats": {"received": self.received, "source": self.source, "seconds_since_last": None,
                              "error": self.error}}

    def track(self, pax: str) -> dict:
        with self.lock:
            latest = self.positions.get(pax)
            return {"passenger_id": pax, "fields": ["passenger_id", *POSITION_FIELDS],
                    "current": [pax] + latest[1] if latest else None, "ended": pax in self.ended,
                    "points": list(self.tracks.get(pax, [])), "timeline": list(self.timelines.get(pax, []))}


# -- sources -------------------------------------------------------------------
def consume_kafka(state: TwinState, bootstrap: str, topics: list[str], from_beginning: bool, group: str) -> None:
    from confluent_kafka import Consumer

    state.source = f"kafka {bootstrap}"
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "auto.offset.reset": "earliest" if from_beginning else "latest",
                         "enable.auto.commit": False, "allow.auto.create.topics": True,
                         # the publisher may create the topics after the viewer starts
                         "topic.metadata.refresh.interval.ms": 5000})
    consumer.subscribe(topics)
    while True:
        for msg in consumer.consume(num_messages=5000, timeout=1.0):
            if msg.error():
                state.error = str(msg.error())
                continue
            state.error = None
            try:
                state.add(json.loads(msg.value()))
            except ValueError:
                state.error = "skipped a message that is not JSON"


class _StatePublisher:
    """Lets the simulation publish straight into the dashboard (``--simulate``)."""

    def __init__(self, state: TwinState, locator) -> None:
        from airport_eye.esri import to_esri_record

        self.state, self.locator, self.to_record = state, locator, to_esri_record
        self.count = 0

    def publish(self, event) -> None:
        self.state.add(self.to_record(event, self.locator))
        self.count += 1

    def flush(self, timeout: float = 0.0) -> None:
        pass

    def stats(self) -> dict[str, int]:
        return {"queued": self.count, "delivered": self.count, "failed": 0}


def simulate(state: TwinState, tracking_config: str, zones: str, speed: float, scenario: str | None = None) -> None:
    from airport_eye.esri import ZoneLocator
    from airport_eye.tracking import run_tracking_stream

    state.source = f"in-process simulation (speed x{speed:g}{', ' + scenario if scenario else ''})"
    run_tracking_stream(tracking_config, _StatePublisher(state, ZoneLocator.from_file(zones)), speed=speed,
                        create_topics=False, scenario=scenario, log=lambda _m: None)


def replay_file(state: TwinState, path: str, zones: str, seconds_per_batch: float) -> None:
    """Feed the dashboard from a generated touchpoint JSONL file, one minute per batch, looping."""
    from airport_eye.esri import ZoneLocator, to_esri_record
    from airport_eye.models import EventEnvelope

    locator = ZoneLocator.from_file(zones)
    batches: dict[str, list[dict]] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record = to_esri_record(EventEnvelope.model_validate_json(line), locator)
                batches.setdefault(record["event_time"], []).append(record)
    state.source = f"file {Path(path).name} (queue replay, looping)"
    while batches:
        for key in sorted(batches):
            for record in batches[key]:
                state.add(record)
            time.sleep(seconds_per_batch)


def layout_geojson(path: str) -> dict:
    """Building outlines and zones from the terminal layout, in WGS84."""
    from airport_eye.spatial import LocalFrame

    layout = json.loads(Path(path).read_text(encoding="utf-8"))
    frame = LocalFrame.from_dict(layout["frame"])

    def ring(polygon):
        points = [list(frame.to_wgs84(x, y)) for x, y in polygon]
        return [points + points[:1]]

    features = [{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": ring(b["polygon"])},
                 "properties": {"kind": "building", "name": b["name"]}} for b in layout["buildings"]]
    for zone in layout["zones"]:
        for polygon in zone["polygons"]:
            features.append({"type": "Feature", "geometry": {"type": "Polygon", "coordinates": ring(polygon)},
                             "properties": {"kind": "zone", "zone_id": zone["zone_id"], "name": zone["name"],
                                            "floor": zone["floor"]}})
    # Checkpoint geometry for drawing counters and queue areas: the service-line centre,
    # the queue direction and the line width, plus degrees per metre to place points.
    lat0 = layout["frame"]["origin_lat"]
    per_m = {"lon": 1 / (111_320 * math.cos(math.radians(lat0))), "lat": 1 / 110_574}
    touchpoints = {}
    for tp_id, t in layout["touchpoints"].items():
        lon, lat = frame.to_wgs84(t["x"], t["y"])
        dx, dy = t["queue_direction"]
        norm = math.hypot(dx, dy) or 1.0
        touchpoints[tp_id] = {"lon": lon, "lat": lat, "floor": t["floor"], "dir": [dx / norm, dy / norm],
                              "width_m": t["service_width_m"], "per_row": max(2, int(min(t["service_width_m"], 30) / 0.75))}
    gates = []
    for g in layout.get("gates", []):
        lon, lat = frame.to_wgs84(g["x"], g["y"])
        gates.append({"gate_id": g["gate_id"], "ref": g.get("osm_ref"), "flow": g["flow"], "lon": lon, "lat": lat, "floor": g["floor"]})
    return {"type": "FeatureCollection", "features": features, "floors": layout["floors"],
            "touchpoints": touchpoints, "gates": gates, "deg_per_m": per_m, "note": layout.get("_note", "")}


def ground_geojson(path: str) -> dict | None:
    """Kerbs, car parks, stands and named roads from the ground layout, in WGS84."""
    from airport_eye.spatial import LocalFrame

    try:
        g = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    frame = LocalFrame.from_dict(g["frame"])
    ll = lambda pts: [list(frame.to_wgs84(x, y)) for x, y in pts]
    nodes = {n["id"]: (n["x"], n["y"]) for n in g["roads"]["nodes"]}
    roads: dict[str, list] = {}
    for a, b, _len, _kmh, level, name, side, _way in g["roads"]["edges"]:
        if name and side == "landside" and "Terminal 2" not in name and not name.startswith("T2 "):
            roads.setdefault(name, []).append([list(frame.to_wgs84(*nodes[a])), list(frame.to_wgs84(*nodes[b]))])
    apron = [[list(frame.to_wgs84(*nodes[a])), list(frame.to_wgs84(*nodes[b]))]
             for a, b, *_rest in g["roads"]["edges"] if _rest[4] == "airside"]
    return {"kerbs": [{"id": k["kerb_id"], "name": k["name"], "floor": k["floor"], "line": ll(k["polyline"])} for k in g["kerbs"]],
            "car_parks": [{"id": p["park_id"], "name": p["name"], "polygon": ll(p["polygon"])} for p in g["car_parks"]],
            "stands": [{"id": s["stand_id"], "ref": s.get("osm_ref"), "flow": s["flow"],
                        "point": list(frame.to_wgs84(s["x"], s["y"]))} for s in g["stands"]],
            "roads": [{"name": n, "segments": segs} for n, segs in roads.items()],
            "apron_roads": apron}


def make_handler(state: TwinState, layout: dict | None, ground: dict | None = None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):  # keep the console quiet
            pass

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, data, status: int = 200) -> None:
            self._send(status, json.dumps(data, separators=(",", ":")).encode(), "application/json")

        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            query = parse_qs(url.query)
            if url.path == "/api/state":
                return self._json(state.state())
            if url.path == "/api/snapshot":
                snap = state.snapshot(int(query.get("t", ["0"])[0]))
                return self._json(snap or {}, 200 if snap else 404)
            if url.path == "/api/track":
                return self._json(state.track(query.get("id", [""])[0]))
            if url.path == "/api/ground":
                return self._json(ground or {}, 200 if ground else 404)
            if url.path == "/api/layout":
                return self._json(layout or {}, 200 if layout else 404)
            rel = "index.html" if url.path in ("/", "") else url.path.lstrip("/")
            target = (VIEWER / rel).resolve()
            if VIEWER.resolve() not in target.parents or not target.is_file():
                return self._send(404, b"not found", "text/plain")
            kind = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
            self._send(200, target.read_bytes(), kind)

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic-prefix", default=PREFIX, help="topics are <prefix><event_type>")
    ap.add_argument("--from-beginning", action="store_true", help="read the topics from the start (default: newest only)")
    ap.add_argument("--simulate", action="store_true", help="run the terminal simulation in-process instead of reading Kafka")
    ap.add_argument("--speed", type=float, default=1.0, help="--simulate only: time compression")
    ap.add_argument("--tracking-config", default="config/passenger_tracking.json")
    ap.add_argument("--scenario", help="--simulate only: named scenario from the tracking config, e.g. 'peak'")
    ap.add_argument("--file", help="replay a generated touchpoint JSONL file instead of reading Kafka")
    ap.add_argument("--zones", default="config/terminal_layout.json")
    ap.add_argument("--ground", default="config/ground_layout.json", help="roads, kerbs and stands to draw")
    ap.add_argument("--replay-seconds", type=float, default=1.0, help="seconds per simulated minute in --file mode")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    state = TwinState()
    try:
        layout = layout_geojson(args.zones)
    except (OSError, KeyError, ValueError):
        layout = None  # older flat zone file: no outlines to draw
    if args.simulate:
        worker = threading.Thread(target=simulate, args=(state, args.tracking_config, args.zones, args.speed, args.scenario), daemon=True)
    elif args.file:
        worker = threading.Thread(target=replay_file, args=(state, args.file, args.zones, args.replay_seconds), daemon=True)
    else:
        topics = [args.topic_prefix + feed for feed in FEEDS]
        group = f"map-viewer-{int(time.time())}"
        worker = threading.Thread(target=consume_kafka, args=(state, args.bootstrap, topics, args.from_beginning, group),
                                  daemon=True)
    worker.start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(state, layout, ground_geojson(args.ground)))
    print(f"Dashboard: http://{args.host}:{args.port}   (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
