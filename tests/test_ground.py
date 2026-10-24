import json
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.generators.passenger import generate_ticks, load_tracking
from airport_eye.models import EventEnvelope
from airport_eye.spatial import point_in_polygon
from airport_eye.tracking import FEED_GROUPS, select_feeds

ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc)


def of_type(records, event_type):
    return [r for r in records if r.event_type == event_type]


class GroundTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config, layout, inventory, _ = load_tracking(ROOT / "config/passenger_tracking.json")
        quick = config.model_copy(update={"warmup_minutes": 120, "warmup_from_local_midnight": False})
        quick._targets, quick._ground_layout = config._targets, config._ground_layout
        cls.layout = layout
        cls.ground_layout = config._ground_layout
        cls.ticks = list(generate_ticks(quick, layout, inventory, START, START + timedelta(minutes=6)))

    def test_all_vehicle_feeds_are_produced_and_valid(self):
        types = Counter(r.event_type for _, records in self.ticks for r in records)
        for feed in FEED_GROUPS["landside"] + FEED_GROUPS["airside"]:
            self.assertGreater(types[feed], 0, feed)
        minute = [records for when, records in self.ticks if when.second == 0][0]
        for r in minute:
            if r.event_type in FEED_GROUPS["landside"] + FEED_GROUPS["airside"]:
                EventEnvelope.model_validate(r.model_dump())

    def test_kerbs_never_exceed_their_bays_and_match_the_vehicles(self):
        stopped = ("dropping_off", "at_kerb", "loading", "at_stop")
        for when, records in self.ticks:
            if when.second:
                continue
            vehicles = Counter(r.payload["facility_id"] for r in of_type(records, "vehicle.position")
                               if r.payload["state"] in stopped)
            for r in of_type(records, "curb.occupancy"):
                p = r.payload
                self.assertLessEqual(p["occupied"], p["capacity"], p["facility_id"])
                self.assertLessEqual(p["arrivals"], p["capacity"] + 60, p)   # one minute's worth, not the warm-up
                if p["facility_type"] != "car_park":
                    self.assertEqual(p["occupied"], vehicles.get(p["facility_id"], 0), p["facility_id"])

    def test_departing_passengers_first_appear_where_a_vehicle_let_them_out(self):
        seen = {r.entity_id for r in of_type(self.ticks[0][1], "passenger.position")}  # already inside at the start
        first = []
        for _, records in self.ticks[1:]:
            for r in of_type(records, "passenger.position"):
                p = r.payload
                if r.entity_id not in seen and p["journey"] in ("domestic_departure", "international_departure"):
                    first.append(p["indoor"]["zone_id"])
                seen.add(r.entity_id)
        self.assertTrue(first)
        drop_zones = {"SYN-HYD-ZONE-KERB-DEP", "SYN-HYD-ZONE-FORECOURT", "SYN-HYD-ZONE-DEPARTURES", "SYN-HYD-ZONE-CITYSIDE"}
        share = sum(z in drop_zones for z in first) / len(first)
        self.assertGreater(share, 0.95, Counter(first).most_common(5))

    def test_stands_match_the_ground_vehicles_serving_them(self):
        minute = [records for when, records in self.ticks if when.second == 0][-1]
        serving = Counter(r.payload["stand_id"] for r in of_type(minute, "gse.position") if r.payload["state"] == "servicing")
        stands = of_type(minute, "stand.activity")
        self.assertEqual(len(stands), len(self.ground_layout["stands"]))
        for r in stands:
            self.assertEqual(r.payload["vehicles_present"], serving.get(r.payload["stand_id"], 0), r.payload["stand_id"])
        statuses = Counter(r.payload["status"] for r in stands if r.payload["phase"] != "vacant")
        busy = sum(statuses.values())
        self.assertGreater(busy, 0)
        self.assertLess((statuses["delayed"]) / busy, 0.5, statuses)

    def test_vehicles_stay_on_the_map_near_the_airport(self):
        for _, records in self.ticks[-2:]:
            for r in of_type(records, "vehicle.position") + of_type(records, "gse.position"):
                p = r.payload
                self.assertTrue(-1100 < p["x"] < 1500 and -500 < p["y"] < 1000, p)
                self.assertTrue(17.22 < p["lat"] < 17.25 and 78.41 < p["lon"] < 78.45, p)

    def test_feed_groups(self):
        self.assertEqual(select_feeds("landside"), FEED_GROUPS["landside"])
        self.assertEqual(len(select_feeds("all")), 13)
        self.assertEqual(select_feeds("kpis"), ("kpi.checkpoint", "kpi.curbside", "kpi.overview", "flight.status"))
        self.assertEqual(select_feeds("passengers,airside"), FEED_GROUPS["passengers"] + FEED_GROUPS["airside"])
        with self.assertRaises(ValueError):
            select_feeds("trains")


if __name__ == "__main__":
    unittest.main()
