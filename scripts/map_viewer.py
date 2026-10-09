"""Local live map of the Esri feed, for checking the POC without any Esri software.

Reads the flat Esri-format records (the same ones ArcGIS Velocity / GeoEvent
Server would ingest) from Kafka and plots each touchpoint at its longitude and
latitude, coloured by queue length.

  python scripts/map_viewer.py --bootstrap localhost:9092          # live from Kafka
  python scripts/map_viewer.py --file generated_data/passenger_touchpoint.jsonl   # no broker needed

Then open http://127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
VIEWER = HERE / "viewer"
HISTORY = 60


class State:
    """Latest record per entity, a short queue history, and feed statistics."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest: dict[str, dict] = {}
        self.history: dict[str, deque] = {}
        self.received = 0
        self.last_received_at: float | None = None
        self.source = ""
        self.error: str | None = None

    def add(self, record: dict) -> None:
        entity = record.get("entity_id")
        if not entity:
            return
        with self.lock:
            self.latest[entity] = record  # last record received wins (one partition per key keeps order)
            hist = self.history.setdefault(entity, deque(maxlen=HISTORY))
            hist.append([record.get("event_time_epoch_ms"), record.get("queue_length")])
            self.received += 1
            self.last_received_at = time.time()

    def snapshot(self) -> dict:
        with self.lock:
            features = []
            for entity, rec in self.latest.items():
                if rec.get("longitude") is None or rec.get("latitude") is None:
                    continue
                features.append({"type": "Feature",
                                 "geometry": {"type": "Point", "coordinates": [rec["longitude"], rec["latitude"]]},
                                 "properties": {**rec, "history": list(self.history.get(entity, []))}})
            age = None if self.last_received_at is None else round(time.time() - self.last_received_at, 1)
            return {"type": "FeatureCollection", "features": features,
                    "stats": {"received": self.received, "entities": len(self.latest), "source": self.source,
                              "seconds_since_last": age, "error": self.error}}


def consume_kafka(state: State, bootstrap: str, topic: str, from_beginning: bool, group: str) -> None:
    from confluent_kafka import Consumer

    state.source = f"kafka {bootstrap} / {topic}"
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "auto.offset.reset": "earliest" if from_beginning else "latest",
                         "enable.auto.commit": False, "allow.auto.create.topics": True,
                         # the publisher may create the topic after the viewer starts
                         "topic.metadata.refresh.interval.ms": 5000})
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            state.error = str(msg.error())
            continue
        state.error = None
        try:
            state.add(json.loads(msg.value()))
        except ValueError:
            state.error = "skipped a message that is not JSON"


def replay_file(state: State, path: str, zones: str, config: str, seconds_per_batch: float) -> None:
    """Feed the viewer from a generated JSONL file, one simulated minute per batch, looping."""
    from airport_eye.esri import ZoneLocator, to_esri_record
    from airport_eye.models import EventEnvelope

    locator = ZoneLocator.from_file(zones)
    batches: dict[str, list[dict]] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record = to_esri_record(EventEnvelope.model_validate_json(line), locator)
                batches.setdefault(record["event_time"], []).append(record)
    state.source = f"file {Path(path).name} (replay, looping)"
    while batches:
        for key in sorted(batches):
            for record in batches[key]:
                state.add(record)
            time.sleep(seconds_per_batch)
        state.latest.clear()
        state.history.clear()


def make_handler(state: State):
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

        def do_GET(self):  # noqa: N802
            path = self.path.split("?")[0]
            if path == "/api/state":
                return self._send(200, json.dumps(state.snapshot()).encode(), "application/json")
            rel = "index.html" if path in ("/", "") else path.lstrip("/")
            target = (VIEWER / rel).resolve()
            if VIEWER.resolve() not in target.parents or not target.is_file():
                return self._send(404, b"not found", "text/plain")
            kind = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
            self._send(200, target.read_bytes(), kind)

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="airport-eye.poc.esri.passenger.touchpoint")
    ap.add_argument("--latest-only", action="store_true",
                    help="ignore records published before the viewer started (default: read the topic from the start, newest per touchpoint wins)")
    ap.add_argument("--file", help="replay a generated JSONL file instead of reading Kafka")
    ap.add_argument("--zones", default="config/zone_locations.json")
    ap.add_argument("--replay-seconds", type=float, default=1.0, help="seconds per simulated minute in --file mode")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    state = State()
    if args.file:
        worker = threading.Thread(target=replay_file, args=(state, args.file, args.zones, "", args.replay_seconds), daemon=True)
    else:
        group = f"map-viewer-{int(time.time())}"
        worker = threading.Thread(target=consume_kafka, args=(state, args.bootstrap, args.topic, not args.latest_only, group), daemon=True)
    worker.start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(state))
    print(f"Map viewer: http://{args.host}:{args.port}   (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
