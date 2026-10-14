import csv
import json
import math
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.generators.passenger import generate_ticks, load_tracking
from airport_eye.models import EventEnvelope
from airport_eye.spatial import LocalFrame, WalkGraph, point_in_polygon
from airport_eye.tracking import TwinFileWriter, write_tracking_range
from airport_eye.streaming import publish_paced

ROOT = Path(__file__).resolve().parents[1]
TRACKING = ROOT / "config/passenger_tracking.json"
START = datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc)


def short_run(minutes=6, warmup=90):
    config, layout, inventory, _ = load_tracking(TRACKING)
    config = config.model_copy(update={"warmup_minutes": warmup, "warmup_from_local_midnight": False})
    return config, layout, inventory, list(generate_ticks(config, layout, inventory, START, START + timedelta(minutes=minutes)))


def of_type(records, event_type="passenger.position"):
    return [r for r in records if r.event_type == event_type]


class FrameAndGraphTests(unittest.TestCase):
    def test_frame_round_trip_and_scale(self):
        frame = LocalFrame("F", 17.2355, 78.4295)
        self.assertEqual(frame.to_wgs84(0, 0), (78.4295, 17.2355))
        lon, lat = frame.to_wgs84(100, 0)
        self.assertAlmostEqual((lon - 78.4295) * 111320 * math.cos(math.radians(17.2355)), 100, delta=0.1)
        x, y = frame.from_wgs84(*frame.to_wgs84(-250.5, 80.25))
        self.assertAlmostEqual(x, -250.5, delta=0.02)
        self.assertAlmostEqual(y, 80.25, delta=0.02)
        rotated = LocalFrame("R", 17.2355, 78.4295, rotation_deg=90)
        x, y = rotated.from_wgs84(*rotated.to_wgs84(10, 0))
        self.assertAlmostEqual(x, 10, delta=0.02)

    def test_layout_places_touchpoints_and_gates_inside_the_terminal(self):
        layout = json.loads((ROOT / "config/terminal_layout.json").read_text())
        buildings = [b["polygon"] for b in layout["buildings"]]
        for gate in layout["gates"]:
            self.assertTrue(any(point_in_polygon(gate["x"], gate["y"], p) for p in buildings), gate["gate_id"])
        indoor = [tp for tp, t in layout["touchpoints"].items() if "CITY" not in tp and "ENTRY" not in tp]
        for tp in indoor:
            t = layout["touchpoints"][tp]
            self.assertTrue(any(point_in_polygon(t["x"], t["y"], p) for p in buildings), tp)

    def test_walkways_keep_landside_and_airside_apart(self):
        graph = WalkGraph.from_layout(json.loads((ROOT / "config/terminal_layout.json").read_text()))
        self.assertNotEqual(graph.component["K_M"], graph.component["AS_2"])  # kerb vs airside
        self.assertEqual(graph.component["AS_2"], graph.component["SYN-HYD-GATE-110"])
        path = graph.route("AS_2", "SYN-HYD-GATE-110")
        self.assertEqual((path[0].node_id, path[-1].node_id), ("AS_2", "SYN-HYD-GATE-110"))
        # from just after security to a far gate: stays airside, ends exactly at the target
        points = graph.path_between(30, 19, "L1", 480, 10, "L1")
        self.assertEqual(points[-1], (480, 10, "L1"))
        self.assertTrue(all(graph.component[graph.nearest(x, y, f).node_id] == graph.component["AS_2"]
                            for x, y, f in points[:-1]))


class ScenarioTests(unittest.TestCase):
    def test_peak_scenario_overrides_rates_profile_and_lanes(self):
        base, _, _, _ = load_tracking(TRACKING)
        peak, _, _, _ = load_tracking(TRACKING, "peak")
        sc = base.scenarios["peak"]
        self.assertEqual(peak.hourly_profile, sc.hourly_profile)
        for journey, stream in base.flights.items():
            self.assertAlmostEqual(peak.flights[journey].per_hour, stream.per_hour * sc.flight_multiplier)
        for tp, lanes in sc.open_channels.items():
            self.assertEqual(peak.open_channels[tp], lanes)
        self.assertEqual(peak.open_channels["SYN-HYD-ENTRY"], base.open_channels["SYN-HYD-ENTRY"])  # untouched lanes kept
        self.assertIsNotNone(peak._targets)
        with self.assertRaises(ValueError):
            load_tracking(TRACKING, "no-such-scenario")


class SimulationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config, cls.layout, cls.inventory, cls.ticks = short_run()

    def test_one_batch_per_interval_and_records_meet_the_contract(self):
        times = [when for when, _ in self.ticks]
        self.assertEqual(times[0], START + timedelta(seconds=5))
        self.assertTrue(all(b - a == timedelta(seconds=5) for a, b in zip(times, times[1:])))
        for _, records in self.ticks[:3] + [t for t in self.ticks if t[0].second == 0][:1]:
            self.assertGreater(len(of_type(records)), 200)
            for record in records:
                EventEnvelope.model_validate(record.model_dump())

    def test_positions_stay_inside_terminal_zones(self):
        zones = Counter(r.payload["indoor"]["zone_id"] for _, records in self.ticks for r in of_type(records))
        outside = zones.pop("SYN-HYD-ZONE-OUTSIDE", 0)
        self.assertLess(outside / (outside + sum(zones.values())), 0.001)

    def test_tracks_are_ordered_and_end_once(self):
        last_seq, ended = defaultdict(lambda: -1), set()
        for _, records in self.ticks:
            for r in of_type(records):
                self.assertNotIn(r.entity_id, ended)
                self.assertEqual(r.sequence_no, last_seq[r.entity_id] + 1)
                last_seq[r.entity_id] = r.sequence_no
                if r.payload["activity"] in ("boarded", "exited"):
                    ended.add(r.entity_id)
        self.assertTrue(ended)

    def test_queuing_passengers_stand_at_their_touchpoint(self):
        spots = {**self.layout["touchpoints"], **{g["gate_id"]: g for g in self.layout["gates"]}}
        boards = {"SYN-HYD-BOARDING-DOM", "SYN-HYD-BOARDING-INT"}
        checked = 0
        for _, records in self.ticks:
            for r in of_type(records):
                p = r.payload
                if p["activity"] not in ("queuing", "in_service") or p["touchpoint_id"] in boards:
                    continue
                spot = spots[p["touchpoint_id"]]
                distance = math.hypot(p["indoor"]["x"] - spot["x"], p["indoor"]["y"] - spot["y"])
                self.assertLess(distance, 60, p)
                self.assertEqual(p["indoor"]["floor"], spot["floor"])
                checked += 1
        self.assertGreater(checked, 0)

    def test_same_seed_and_start_give_the_same_tracks(self):
        _, _, _, again = short_run(minutes=1)
        key = lambda records: [(r.entity_id, r.payload["indoor"]["x"], r.payload["activity"]) for r in of_type(records)]
        self.assertEqual(key(self.ticks[0][1]), key(again[0][1]))

    def test_minute_feeds_agree_with_the_positions(self):
        minutes = [(when, records) for when, records in self.ticks if when.second == 0]
        self.assertGreaterEqual(len(minutes), 5)
        for when, records in minutes:
            positions = [r.payload for r in of_type(records)]
            present = [p for p in positions if p["activity"] not in ("boarded", "exited")]
            queuing = Counter(p["touchpoint_id"] for p in present if p["activity"] == "queuing")
            serving = Counter(p["touchpoint_id"] for p in present if p["activity"] == "in_service")
            for r in of_type(records, "passenger.touchpoint"):
                tp = r.payload
                self.assertEqual(tp["queue_length"], queuing.get(tp["touchpoint_id"], 0), tp["touchpoint_id"])
                self.assertEqual(tp["passengers_in_service"], serving.get(tp["touchpoint_id"], 0), tp["touchpoint_id"])
            in_zone = Counter(p["indoor"]["zone_id"] for p in present)
            for r in of_type(records, "passenger.aggregate"):
                self.assertEqual(r.payload["passenger_count"], in_zone.get(r.payload["zone_id"], 0), r.payload["zone_id"])
            kpi = of_type(records, "passenger.journey_kpi")[0].payload
            self.assertEqual(kpi["people_in_terminal"], len(present))
            self.assertEqual(kpi["people_queuing"], sum(queuing.values()))

    def test_queue_feed_covers_the_scope_and_counts_up(self):
        minutes = [records for when, records in self.ticks if when.second == 0]
        first = {r.entity_id: r.payload for r in of_type(minutes[0], "passenger.touchpoint")}
        last = {r.entity_id: r.payload for r in of_type(minutes[-1], "passenger.touchpoint")}
        self.assertEqual(len(first), 16)
        self.assertEqual({p["touchpoint_type"] for p in first.values()},
                         {"entry", "check_in", "security", "boarding", "immigration"})
        for tp_id, payload in last.items():
            self.assertGreaterEqual(payload["throughput_since_midnight"], first[tp_id]["throughput_since_midnight"])
        self.assertEqual(first["SYN-HYD-SECURITY-DOM"]["service_channels"], 12)  # open lanes, not installed

    def test_esri_record_has_wgs84_from_local_xy(self):
        locator = ZoneLocator.from_layout(self.layout)
        record = to_esri_record(of_type(self.ticks[0][1])[0], locator)
        x, y = locator.frame.from_wgs84(record["longitude"], record["latitude"])
        self.assertAlmostEqual(x, record["indoor_x"], delta=0.05)
        self.assertAlmostEqual(y, record["indoor_y"], delta=0.05)
        self.assertEqual(record["floor"], record["indoor_floor"])
        self.assertEqual(record["elevation_m"], {"L1": 8.0, "L0": 0.0}[record["floor"]])
        self.assertEqual(record["elevation_m"], record["indoor_z"])
        self.assertEqual(record["reference_elevation_m"], locator.reference_elevation_m)
        self.assertEqual(record["global_elevation_m"],
                         round(record["reference_elevation_m"] + record["elevation_m"], 3))
        self.assertEqual(record["spatial_reference_wkid"], 3857)
        self.assertIn("web_mercator_x", record)
        self.assertIn("web_mercator_y", record)
        self.assertTrue(17.22 < record["latitude"] < 17.25 and 78.41 < record["longitude"] < 78.44)


class OutputTests(unittest.TestCase):
    def test_live_writer_rewrites_latest_files(self):
        config, layout, inventory, ticks = short_run(minutes=1, warmup=30)
        with tempfile.TemporaryDirectory() as out:
            writer = TwinFileWriter(out, ZoneLocator.from_layout(layout), layout, history=True, position_history=True)
            now = {"t": 0.0}
            publish_paced(iter(ticks), writer, origin=START, clock=lambda: now["t"],
                          sleep=lambda s: now.update(t=now["t"] + s), log=lambda _m: None)
            with (Path(out) / "passenger_positions_latest.csv").open(encoding="utf-8") as handle:
                latest = list(csv.DictReader(handle))
            geojson = json.loads((Path(out) / "passenger_positions_latest.geojson").read_text())
            with (Path(out) / "passenger_positions_history.csv").open(encoding="utf-8") as handle:
                history = list(csv.DictReader(handle))
            zones = json.loads((Path(out) / "zone_occupancy_latest.geojson").read_text())
            queues_exist = (Path(out) / "passenger_queues_latest.csv").exists()
            kpi = json.loads((Path(out) / "journey_kpis_latest.json").read_text())
        self.assertEqual(len(latest), len(of_type(ticks[-1][1])))
        self.assertEqual(len(geojson["features"]), len(latest))
        self.assertEqual(len(history), sum(len(of_type(r)) for _, r in ticks))
        self.assertEqual(zones["features"][0]["geometry"]["type"], "MultiPolygon")
        self.assertTrue(queues_exist)
        self.assertEqual(kpi["event_type"], "passenger.journey_kpi")
        self.assertEqual({row["event_time"] for row in latest}, {"2026-10-09T02:31:00Z"})

    def test_range_writes_csv_and_jsonl(self):
        with tempfile.TemporaryDirectory() as out:
            config, layout, _, _ = load_tracking(TRACKING)
            paths, count = write_tracking_range(TRACKING, ZoneLocator.from_layout(layout), out, start=START, minutes=1.5)
            with paths[0].open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            with paths[1].open(encoding="utf-8") as handle:
                lines = handle.readlines()
        self.assertEqual(len(rows), len(lines))
        self.assertGreater(count, len(rows))  # positions plus the minute feeds
        self.assertEqual(list(rows[0])[:4], ["passenger_id", "journey", "activity", "touchpoint_id"])

    def test_range_writes_only_the_chosen_feeds(self):
        with tempfile.TemporaryDirectory() as out:
            config, layout, _, _ = load_tracking(TRACKING)
            paths, count = write_tracking_range(TRACKING, ZoneLocator.from_layout(layout), out, start=START,
                                                minutes=1.5, feeds=("curb.occupancy", "stand.activity"))
            self.assertEqual(sorted(p.name.split("_2026")[0] for p in Path(out).iterdir()),
                             ["kerb_occupancy", "kerb_occupancy", "stand_activity", "stand_activity"])
            lines = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in paths if p.suffix == ".jsonl")
        self.assertEqual(lines, count)


if __name__ == "__main__":
    unittest.main()
