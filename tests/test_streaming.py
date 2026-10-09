import json
import math
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.config import load_config
from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.models import EventEnvelope
from airport_eye.publishers.kafka import KafkaPublisher
from airport_eye.streaming import floor_to_interval, run_stream
from test_kafka_publisher import FakeProducer

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "config/passenger_touchpoint.json"


class StreamTests(unittest.TestCase):
    def make(self):
        kafka = load_config(ROOT / "config/poc.json").kafka
        holder = {}

        def factory(settings):
            holder["p"] = FakeProducer(settings)
            return holder["p"]

        pub = KafkaPublisher(kafka, fmt="esri", locator=ZoneLocator.from_file(ROOT / "config/terminal_layout.json"),
                             producer_factory=factory)
        pub.ensure_topics = lambda topics, timeout=20.0: topics  # no broker in unit tests
        return pub, holder

    def test_one_batch_per_minute_paced_in_real_time(self):
        pub, holder = self.make()
        now = {"t": 0.0}
        waits = []

        def sleep(seconds):
            waits.append(round(seconds, 6))
            now["t"] += seconds

        start = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)
        stats = run_stream(RUN, pub, minutes=3, speed=1.0, start=start, clock=lambda: now["t"],
                           sleep=sleep, log=lambda *_: None)
        self.assertEqual(stats["delivered"], 3 * 16)
        self.assertEqual(waits, [60.0, 60.0])  # first batch at once, then one per minute
        records = [json.loads(v) for _, _, v in holder["p"].sent]
        self.assertEqual({r["touchpoint_type"] for r in records},
                         {"entry", "check_in", "security", "boarding", "immigration"})
        self.assertEqual({t for t, _, _ in holder["p"].sent}, {"airport-eye.poc.esri.passenger.touchpoint"})
        times = sorted({r["event_time"] for r in records})
        self.assertEqual(times, ["2026-10-08T09:00:00Z", "2026-10-08T09:01:00Z", "2026-10-08T09:02:00Z"])

    def test_speed_compresses_time(self):
        pub, _ = self.make()
        now = {"t": 0.0}
        waits = []
        run_stream(RUN, pub, minutes=2, speed=60, start=datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc),
                   clock=lambda: now["t"], sleep=lambda s: (waits.append(s), now.update(t=now["t"] + s)),
                   log=lambda *_: None)
        self.assertEqual([round(w, 6) for w in waits], [1.0])

    def test_paced_publisher_catches_up_after_a_pause(self):
        from airport_eye.streaming import publish_paced

        class Pub:
            def __init__(self): self.sent = []
            def publish(self, e): self.sent.append(e)
            def flush(self): pass
            def stats(self): return {"delivered": len(self.sent), "failed": 0}

        origin = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
        batches = [(origin + timedelta(seconds=5 * i), [i]) for i in range(1, 7)]
        now = {"t": 0.0}
        waits = []

        def sleep(s):
            waits.append(round(s, 3))
            now["t"] += s
            if len(waits) == 2:
                now["t"] += 20  # the machine slept for 20 s during this wait

        pub = Pub()
        publish_paced(iter(batches), pub, origin=origin, clock=lambda: now["t"], sleep=sleep, log=lambda *_: None)
        self.assertEqual(pub.sent, [1, 2, 3, 4, 5, 6])
        self.assertEqual(waits, [5.0, 5.0])  # after the 20 s pause batches 3-6 were due: sent at once, no waiting

    def test_floor_to_interval(self):
        t = datetime(2026, 10, 8, 9, 7, 42, tzinfo=timezone.utc)
        self.assertEqual(floor_to_interval(t, 60), datetime(2026, 10, 8, 9, 7, tzinfo=timezone.utc))

    def test_touchpoints_sit_at_their_layout_positions_inside_the_terminal(self):
        import json
        from airport_eye.spatial import point_in_polygon
        layout = json.loads((ROOT / "config/terminal_layout.json").read_text())
        locator = ZoneLocator.from_layout(layout)
        a = locator.locate("SYN-HYD-ZONE-DEPARTURES", "SYN-HYD-ENTRY")
        b = locator.locate("SYN-HYD-ZONE-DEPARTURES", "SYN-HYD-DIGIYATRA-ENTRY")
        metres = math.hypot((a["longitude"] - b["longitude"]) * 106260, (a["latitude"] - b["latitude"]) * 110574)
        self.assertAlmostEqual(metres, 55, delta=1)  # 20 m and 75 m along the entry facade
        self.assertEqual(a["floor"], "L1")
        buildings = [bld["polygon"] for bld in layout["buildings"]]
        for tp in ("SYN-HYD-CHECKIN-DOM", "SYN-HYD-SECURITY-DOM", "SYN-HYD-IMMIGRATION", "SYN-HYD-SECURITY-INT"):
            x, y = locator.frame.from_wgs84(locator.touchpoints[tp]["longitude"], locator.touchpoints[tp]["latitude"])
            self.assertTrue(any(point_in_polygon(x, y, poly) for poly in buildings), tp)

    def test_every_active_touchpoint_has_a_location(self):
        locator = ZoneLocator.from_file(ROOT / "config/terminal_layout.json")
        with open(ROOT / "generated_data/passenger_touchpoint.jsonl", encoding="utf-8") as handle:
            for line in handle:
                rec = to_esri_record(EventEnvelope.model_validate_json(line), locator)
                self.assertIn("latitude", rec)
                self.assertIn("longitude", rec)


if __name__ == "__main__":
    unittest.main()
