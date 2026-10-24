import json
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.generators.passenger import generate_ticks, load_tracking
from airport_eye.models import EventEnvelope
from airport_eye.tracking import KPI_FEEDS, available_feeds

ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 10, 9, 11, 30, tzinfo=timezone.utc)   # 17:00 IST


def ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class KpiFeedTests(unittest.TestCase):
    """The AE-IT widget cards (kpi.* and flight.status), Hyderabad."""

    @classmethod
    def setUpClass(cls):
        config, layout, inventory, _ = load_tracking(ROOT / "config/passenger_tracking.json")
        quick = config.model_copy(update={"interval_seconds": 30})
        quick._targets, quick._ground_layout = config._targets, config._ground_layout
        cls.config, cls.layout = config, layout
        cls.ticks = list(generate_ticks(quick, layout, inventory, START, START + timedelta(minutes=20)))
        cls.records = [r for _, records in cls.ticks for r in records]
        cls.by_type = defaultdict(list)
        for r in cls.records:
            cls.by_type[r.event_type].append(r)

    def test_every_card_every_minute(self):
        minutes = sorted({r.event_time for r in self.by_type["kpi.overview"]})
        self.assertEqual(len(minutes), 20)
        desks = sum(len(c.desks) for c in self.config.kpi.checkpoints)
        for kind, per_minute in (("kpi.checkpoint", desks), ("kpi.curbside", 2), ("kpi.overview", 2)):
            self.assertEqual(len(self.by_type[kind]), per_minute * 20, kind)
        for r in self.records:
            if r.event_type in KPI_FEEDS:
                EventEnvelope.model_validate(r.model_dump())

    def test_desks_follow_the_simulated_queue(self):
        tp = {r.entity_id: r.payload for r in self.by_type["passenger.touchpoint"]
              if r.event_time == self.ticks[-1][0]}
        inventory = {t["touchpoint_id"]: t for t in json.loads(
            (ROOT / "config/hyderabad_touchpoints.json").read_text())["touchpoints"]}
        for r in self.by_type["kpi.checkpoint"]:
            p = r.payload
            sim = inventory[p["touchpoint_id"]]["simulation"]
            if p["avg_processing_time_seconds"] is not None:
                self.assertGreaterEqual(p["avg_processing_time_seconds"], sim["service_time_min_seconds"] - 0.1)
                self.assertLessEqual(p["avg_processing_time_seconds"], sim["service_time_max_seconds"] + 0.1)
            self.assertEqual(len(p["today_hourly_avg_wait_minutes"]), 24)
            if r.event_time == self.ticks[-1][0] and p["touchpoint_id"] in tp:   # emigration is not in that feed
                total, open_desks = tp[p["touchpoint_id"]]["queue_length"], self.config.open_channels[p["touchpoint_id"]]
                self.assertIn(p["queue_length"], (total // open_desks, total // open_desks + 1))
            wait = p["avg_wait_time_seconds"]
            if wait is not None:
                expected = "Happy" if wait < 300 else "Neutral" if wait <= 600 else "Unhappy"
                self.assertEqual(p["happiness_index"], expected)

    def test_flight_milestones_are_consistent(self):
        last = {}
        order = {"arrival": ["scheduled", "landed", "on_stand"], "departure": ["scheduled", "departed", "airborne"]}
        for r in self.by_type["flight.status"]:
            p = r.payload
            seq = order[p["direction"]]
            if p["flight_id"] in last:
                self.assertGreaterEqual(seq.index(p["status"]), seq.index(last[p["flight_id"]]), p)
            last[p["flight_id"]] = p["status"]
            if p["status"] == "on_stand":
                self.assertLess(ts(p["landing_time"]), ts(p["in_block_time"]))
                self.assertAlmostEqual((ts(p["in_block_time"]) - ts(p["scheduled_time"])) / 60, p["delay_minutes"], delta=0.1)
                self.assertEqual(p["on_time"], abs(p["delay_minutes"]) <= 3)
            if p["status"] == "airborne":
                self.assertLess(ts(p["off_block_time"]), ts(p["takeoff_time"]))
                self.assertAlmostEqual((ts(p["takeoff_time"]) - ts(p["off_block_time"])) / 60, p["taxi_minutes"], delta=0.1)
        self.assertTrue({"on_stand", "airborne"} <= set(last.values()))

    def test_overview_counts_match_the_flights(self):
        flights = {}
        for r in self.by_type["flight.status"]:
            flights[r.payload["flight_id"]] = r.payload
        r = [o for o in self.by_type["kpi.overview"] if o.payload["scope"] == "airport"][-1].payload
        lo, hi = ts(r["hour_start"]), ts(r["window_end"])
        arrived = [f for f in flights.values() if f["in_block_time"] and lo < ts(f["in_block_time"]) <= hi]
        departed = [f for f in flights.values() if f["off_block_time"] and lo < ts(f["off_block_time"]) <= hi]
        self.assertEqual(r["arrival_flights"], len(arrived))
        self.assertEqual(r["departure_flights"], len(departed))
        self.assertEqual(r["arrival_pax_actual"], sum(f["pax_actual"] for f in arrived))
        self.assertLessEqual(r["stands_occupied"], r["stands_total"])
        terminal = [o for o in self.by_type["kpi.overview"] if o.payload["scope"] == "terminal"][-1].payload
        self.assertEqual(terminal["departure_bags"], sum(f["bags"] for f in departed))
        self.assertIsNone(terminal["avg_taxi_in_minutes"])
        self.assertIsNone(r["arrival_bags"])

    def test_curbside_and_trolleys(self):
        for r in self.by_type["kpi.curbside"]:
            p = r.payload
            self.assertEqual(sum(p[f"this_hour_{t}"] for t in ("private_car", "app_cab", "taxi", "bus")),
                             p["vehicles_this_hour"])
            if p["side"] == "departures":
                self.assertTrue(0 <= p["trolleys_available"] <= p["trolleys_total"])
            else:
                self.assertIsNone(p["trolleys_available"])

    def test_flat_esri_records(self):
        locator = ZoneLocator.from_layout(self.layout)
        for kind in KPI_FEEDS:
            rec = to_esri_record(self.by_type[kind][-1], locator)
            self.assertIn("event_time_epoch_ms", rec)
            self.assertTrue(17.2 < rec["latitude"] < 17.26 and 78.4 < rec["longitude"] < 78.46, kind)
            for k, v in rec.items():
                self.assertNotIsInstance(v, (dict, list), f"{kind}.{k} must be flat")

    def test_no_kpi_section_no_kpi_feeds(self):
        no_kpi = self.config.model_copy(update={"kpi": None})
        self.assertFalse(set(available_feeds(no_kpi, KPI_FEEDS + ("passenger.position",))) & set(KPI_FEEDS))


if __name__ == "__main__":
    unittest.main()
