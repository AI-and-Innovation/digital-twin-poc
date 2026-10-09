import importlib.util
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.generators.passenger import generate_ticks, load_tracking

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("map_viewer", ROOT / "scripts/map_viewer.py")
map_viewer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(map_viewer)


class DashboardStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config, layout, inventory, _ = load_tracking(ROOT / "config/passenger_tracking.json")
        config = config.model_copy(update={"warmup_minutes": 60, "warmup_from_local_midnight": False})
        locator = ZoneLocator.from_layout(layout)
        start = datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc)
        cls.state = map_viewer.TwinState()
        cls.last_kpi = None
        for _, records in generate_ticks(config, layout, inventory, start, start + timedelta(minutes=3)):
            for event in records:
                record = to_esri_record(event, locator)
                cls.state.add(json.loads(json.dumps(record)))  # as it arrives from Kafka
                if event.event_type == "passenger.journey_kpi":
                    cls.last_kpi = record

    def test_state_has_all_feeds_and_agrees_with_the_kpi(self):
        state = self.state.state()
        self.assertEqual(len(state["touchpoints"]), 16)
        self.assertEqual(len(state["zones"]), len(json.loads((ROOT / "config/terminal_layout.json").read_text())["zones"]))
        self.assertEqual(state["kpi"]["people_in_terminal"], self.last_kpi["people_in_terminal"])
        self.assertGreater(len(state["positions"]["rows"]), 200)
        self.assertEqual(len(state["snapshots"]), 3)  # one per minute

    def test_snapshot_and_track(self):
        state = self.state.state()
        snap = self.state.snapshot(state["snapshots"][0])
        self.assertEqual(snap["mode"], "replay")
        self.assertEqual(snap["t"], state["snapshots"][0])
        pax = state["positions"]["rows"][0][0]
        track = self.state.track(pax)
        self.assertEqual(track["passenger_id"], pax)
        self.assertTrue(track["points"])
        self.assertTrue(track["timeline"])

    def test_layout_geojson_has_buildings_and_zones(self):
        layout = map_viewer.layout_geojson(str(ROOT / "config/terminal_layout.json"))
        kinds = {f["properties"]["kind"] for f in layout["features"]}
        self.assertEqual(kinds, {"building", "zone"})


if __name__ == "__main__":
    unittest.main()
