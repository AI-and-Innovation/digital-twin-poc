import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator
from airport_eye.queue_files import LEADING_COLUMNS, QueueFileWriter, write_range
from airport_eye.streaming import run_stream

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "config/passenger_touchpoint.json"
LOCATOR = ZoneLocator.from_file(ROOT / "config/terminal_layout.json")


class QueueFileTests(unittest.TestCase):
    def test_range_writes_csv_geojson_and_json_with_the_same_records(self):
        with tempfile.TemporaryDirectory() as out:
            start = datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc)
            paths, count = write_range(RUN, LOCATOR, out, start=start, hours=0.5)
            self.assertEqual(count, 16 * 30)
            csv_path, geojson_path, json_path = paths
            with csv_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            features = json.loads(geojson_path.read_text())["features"]
            records = json.loads(json_path.read_text())
            self.assertEqual(len(rows), count)
            self.assertEqual(len(features), count)
            self.assertEqual(len(records), count)
            self.assertEqual(list(rows[0])[:len(LEADING_COLUMNS)], LEADING_COLUMNS)
            self.assertEqual(rows[0]["window_start"], "2026-10-09T02:30:00Z")
            self.assertIn(rows[0]["sla_breached"], ("", "true", "false"))
            point = features[0]
            self.assertEqual(point["geometry"]["coordinates"],
                             [point["properties"]["longitude"], point["properties"]["latitude"]])
            self.assertEqual(records[0]["queue_length"], int(rows[0]["queue_length"]))

    def test_live_rewrites_latest_and_appends_history(self):
        with tempfile.TemporaryDirectory() as out:
            writer = QueueFileWriter(out, LOCATOR)
            now = {"t": 0.0}

            def sleep(seconds):
                now["t"] += seconds

            run_stream(RUN, writer, minutes=3, start=datetime(2026, 10, 9, 3, 0, tzinfo=timezone.utc),
                       create_topics=False, clock=lambda: now["t"], sleep=sleep, log=lambda _m: None)
            with (Path(out) / "passenger_queue_latest.csv").open(encoding="utf-8") as handle:
                latest = list(csv.DictReader(handle))
            with writer.history.open(encoding="utf-8") as handle:
                history = list(csv.DictReader(handle))
            self.assertEqual(len(latest), 16)
            self.assertEqual({r["window_end"] for r in latest}, {"2026-10-09T03:02:00Z"})
            self.assertEqual(len(history), 48)
            self.assertEqual(writer.stats(), {"queued": 48, "delivered": 48, "failed": 0})


if __name__ == "__main__":
    unittest.main()
