import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.generators.passenger import generate_ticks, load_tracking
from airport_eye.models import EventEnvelope
from airport_eye.spatial import LocalFrame
from airport_eye.tracking import FEEDS, KPI_FEEDS

ROOT = Path(__file__).resolve().parents[1]
DEL = ROOT / "config/del"
START = datetime(2026, 10, 9, 4, 30, tzinfo=timezone.utc)


class DelhiTests(unittest.TestCase):
    """Delhi (DIAL, IGI T3) runs on the same engine as Hyderabad, from config/del/ only."""

    @classmethod
    def setUpClass(cls):
        config, layout, inventory, _ = load_tracking(DEL / "passenger_tracking.json")
        quick = config.model_copy(update={"warmup_minutes": 120, "warmup_from_local_midnight": False})
        quick._targets, quick._ground_layout = config._targets, config._ground_layout
        cls.layout = layout
        cls.locator = ZoneLocator.from_layout(layout)
        cls.ticks = list(generate_ticks(quick, layout, inventory, START, START + timedelta(minutes=3)))
        cls.records = [r for _, records in cls.ticks for r in records]

    def test_all_feeds_with_delhi_ids_only(self):
        types = Counter(r.event_type for r in self.records)
        for feed in FEEDS:
            if feed in KPI_FEEDS:   # the AE-IT KPI cards are Hyderabad-only for now
                self.assertEqual(types[feed], 0, feed)
            else:
                self.assertGreater(types[feed], 0, feed)
        minute = [records for when, records in self.ticks if when.second == 0][-1]
        for r in minute:
            EventEnvelope.model_validate(r.model_dump())
        text = json.dumps([r.model_dump(mode="json") for r in minute[:2000]])
        self.assertNotIn("HYD", text)
        self.assertTrue(all(r.airport_id == "DEL" and r.terminal_id == "DEL-T3" for r in self.records))
        self.assertTrue(all(r.entity_id.startswith(("SYN-DEL-", "SYN-PAX-", "SYN-VEH-", "SYN-GSE-", "DEL-T3-"))
                            for r in self.records), Counter(r.entity_id[:8] for r in self.records).most_common(8))

    def test_positions_sit_at_terminal_3_inside_a_zone(self):
        last = [r for r in self.ticks[-1][1] if r.event_type == "passenger.position"]
        self.assertGreater(len(last), 500)
        outside = 0
        for r in last:
            rec = to_esri_record(r, self.locator)
            self.assertTrue(28.545 < rec["latitude"] < 28.565 and 77.072 < rec["longitude"] < 77.097, rec)
            outside += r.payload["indoor"]["zone_id"] == "SYN-DEL-ZONE-OUTSIDE"
        self.assertLess(outside / len(last), 0.02)

    def test_headings_are_compass_bearings_in_the_rotated_frame(self):
        import math
        first = {r.entity_id: r.payload for r in self.ticks[-2][1] if r.event_type == "vehicle.position"}
        errors = []
        for r in self.ticks[-1][1]:
            a, b = first.get(r.entity_id), r.payload
            if r.event_type != "vehicle.position" or a is None or b["speed_kmh"] < 10:
                continue
            de = (b["lon"] - a["lon"]) * math.cos(math.radians(28.55)) * 111320
            dn = (b["lat"] - a["lat"]) * 110574
            if math.hypot(de, dn) > 5:
                bearing = math.degrees(math.atan2(de, dn)) % 360
                errors.append(abs((bearing - b["heading_deg"] + 180) % 360 - 180))
        self.assertGreater(len(errors), 10)
        self.assertLess(sorted(errors)[len(errors) // 2], 5)

    def test_rotated_frame_round_trips(self):
        frame = LocalFrame.from_dict(self.layout["frame"])
        self.assertEqual(frame.rotation_deg, 60.0)
        lon, lat = frame.to_wgs84(123.4, -56.7)
        x, y = frame.from_wgs84(lon, lat)
        self.assertAlmostEqual(x, 123.4, delta=0.02)
        self.assertAlmostEqual(y, -56.7, delta=0.02)

    def test_delhi_topics_are_separate_from_hyderabad(self):
        hyd = json.loads((ROOT / "config/rest_proxy.json").read_text())["topics"]
        dl = json.loads((DEL / "rest_proxy.json").read_text())["topics"]
        self.assertEqual(set(dl), set(hyd) - set(KPI_FEEDS))
        self.assertTrue(all(t.startswith("aiop-del-topic-") for t in dl.values()))
        self.assertFalse(set(dl.values()) & set(hyd.values()))

    def test_departures_level_fits_the_dial_floor_plan(self):
        run = subprocess.run([sys.executable, str(ROOT / "scripts/del/check_dial_plan.py")],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stdout)

    def test_international_passengers_clear_emigration_before_security(self):
        sim_cfg, layout, inventory, _ = load_tracking(DEL / "passenger_tracking.json")
        self.assertTrue(sim_cfg.emigration_before_security)
        seen = {}
        for _, records in self.ticks:
            for r in records:
                tp = r.payload.get("touchpoint_id") if r.event_type == "passenger.position" else None
                if tp and ("EMIGRATION" in tp or tp == "SYN-DEL-SECURITY-INT"):
                    seen.setdefault(r.entity_id, []).append("E" if "EMIGRATION" in tp else "S")
        orders = {"".join(dict.fromkeys(v)) for v in seen.values()}
        self.assertIn("ES", orders)
        self.assertNotIn("SE", orders)

    def test_builders_reproduce_the_committed_files(self):
        with tempfile.TemporaryDirectory() as out:
            for script, osm, name in (("terminal", "osm_del_t3.json", "terminal_layout.json"),
                                      ("ground", "osm_del_ground.json", "ground_layout.json")):
                target = Path(out) / name
                subprocess.run([sys.executable, str(ROOT / f"scripts/del/build_{script}_layout.py"),
                                "--osm-json", str(DEL / osm), "--out", str(target)],
                               check=True, capture_output=True)
                if script == "terminal":   # the ground builder reads the committed terminal layout
                    self.assertEqual(target.read_text(), (DEL / name).read_text(), name)
                else:
                    self.assertEqual(json.loads(target.read_text()), json.loads((DEL / name).read_text()), name)


if __name__ == "__main__":
    unittest.main()
