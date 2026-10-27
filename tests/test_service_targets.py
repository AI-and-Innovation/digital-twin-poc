import json
import unittest
from datetime import timedelta
from pathlib import Path

from airport_eye.generators.passenger_touchpoint import generate
from airport_eye.models import EventEnvelope
from airport_eye.service_targets import ServiceTargets, nearest_rank, queue_attributes
from airport_eye.touchpoint_config import load_run, load_targets

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "config/passenger_touchpoint.json"


class TargetTests(unittest.TestCase):
    def setUp(self):
        self.config, self.inventory, _ = load_run(RUN)
        self.targets = load_targets(RUN, self.config)
        self.by_id = {tp.touchpoint_id: tp for tp in self.inventory.touchpoints}

    def test_every_in_scope_touchpoint_has_a_target_entry(self):
        in_scope = {tp.touchpoint_id for tp in self.inventory.touchpoints
                    if tp.enabled and tp.touchpoint_type in set(self.config.include_types)}
        self.assertEqual(in_scope, set(self.targets.touchpoints))

    def test_regulatory_and_operational_values_stay_separate(self):
        c = self.targets.classes
        self.assertEqual((c["security"].target_wait_minutes, c["security"].aera_existing_minutes,
                          c["security"].aera_proposed_minutes), (5, 5, 10))
        self.assertEqual((c["checkin_economy"].target_wait_minutes, c["checkin_economy"].aera_existing_minutes), (10, 20))
        self.assertEqual((c["entry_digiyatra"].target_wait_minutes, c["entry_digiyatra"].aera_proposed_minutes), (1, 5))
        self.assertIsNone(c["boarding"].target_wait_minutes)

    def test_queue_limit_follows_capacity_times_target(self):
        # Security domestic: 32 slots, mean service 37.5 s -> 51.2 people/min; 5 min target -> 256
        attrs = queue_attributes(self.targets, self.by_id["SYN-HYD-SECURITY-DOM"], queue_length=0, in_service=0,
                                 processed=0, interval_seconds=60, recent_waits=[])
        self.assertEqual(attrs["queue_limit"], 256)
        self.assertEqual(attrs["target_wait_seconds"], 300.0)
        self.assertEqual(attrs["aera_existing_wait_seconds"], 300.0)
        self.assertEqual(attrs["aera_proposed_wait_seconds"], 600.0)

    def test_status_and_breach(self):
        tp = self.by_id["SYN-HYD-DIGIYATRA-ENTRY"]  # target 60 s, 4 slots, 7 s mean -> 34.3/min
        kw = dict(in_service=2, processed=10, interval_seconds=60)
        calm = queue_attributes(self.targets, tp, queue_length=2, recent_waits=[3.0] * 10, **kw)
        self.assertEqual((calm["status"], calm["sla_breached"]), ("normal", False))
        busy = queue_attributes(self.targets, tp, queue_length=2, recent_waits=[45.0] * 10, **kw)
        self.assertEqual((busy["status"], busy["sla_breached"]), ("busy", False))
        bad = queue_attributes(self.targets, tp, queue_length=2, recent_waits=[90.0] * 10, **kw)
        self.assertEqual((bad["status"], bad["sla_breached"]), ("congested", True))
        deep = queue_attributes(self.targets, tp, queue_length=60, recent_waits=[], **kw)
        self.assertEqual(deep["status"], "congested")  # estimated wait alone can breach

    def test_boarding_has_no_target(self):
        attrs = queue_attributes(self.targets, self.by_id["SYN-HYD-BOARDING-DOM"], queue_length=5, in_service=1,
                                 processed=3, interval_seconds=60, recent_waits=[10.0] * 10)
        self.assertEqual(attrs["status"], "no_target")
        self.assertIsNone(attrs["queue_limit"])
        self.assertIsNone(attrs["sla_breached"])

    def test_p95_needs_enough_samples(self):
        tp = self.by_id["SYN-HYD-ENTRY"]
        few = queue_attributes(self.targets, tp, queue_length=0, in_service=0, processed=0, interval_seconds=60, recent_waits=[5.0] * 4)
        self.assertIsNone(few["p95_wait_seconds"])
        self.assertEqual(nearest_rank(list(map(float, range(1, 101))), 0.95), 95.0)

    def test_generated_records_carry_attributes_and_stay_valid(self):
        events = list(generate(self.config, self.inventory, self.targets))
        self.assertEqual(len(events), 960)
        for event in events:
            EventEnvelope.model_validate_json(event.model_dump_json())
            p = event.payload
            self.assertIsNotNone(p["touchpoint_name"])
            self.assertIn(p["status"], {"no_target", "normal", "busy", "congested"})
            if p["status"] != "no_target":
                self.assertIsInstance(p["sla_breached"], bool)
                self.assertGreaterEqual(p["queue_limit"], 0)
        # without targets the original columns are unchanged and the new ones empty
        plain = list(generate(self.config, self.inventory))
        self.assertIsNone(plain[0].payload["status"])
        self.assertEqual([e.payload["queue_length"] for e in plain], [e.payload["queue_length"] for e in events])


if __name__ == "__main__":
    unittest.main()


class SinceMidnightTests(unittest.TestCase):
    def setUp(self):
        self.config, self.inventory, _ = load_run(RUN)

    def test_total_throughput_counts_from_local_midnight(self):
        # Run A starts at 08:00 IST. Run B starts at midnight IST. They must agree afterwards.
        a = list(generate(self.config, self.inventory))
        b_cfg = self.config.model_copy(update={
            "start_time": self.config.start_time.replace(hour=0, minute=0),
            "end_time": self.config.end_time})
        b = {(e.entity_id, e.event_time): e.payload for e in generate(b_cfg, self.inventory)}
        for event in a:
            other = b[(event.entity_id, event.event_time)]
            self.assertEqual(event.payload["throughput_since_midnight"], other["throughput_since_midnight"])
            self.assertEqual(event.payload["queue_length"], other["queue_length"])
        first = {e.entity_id: e.payload for e in a if e.sequence_no == 0}
        self.assertGreater(first["SYN-HYD-ENTRY"]["throughput_since_midnight"], 1000)  # eight hours already in
        self.assertEqual(a[0].payload["service_day"], "2026-09-29")

    def test_total_is_cumulative_within_a_day(self):
        for entity in ("SYN-HYD-ENTRY", "SYN-HYD-SECURITY-DOM"):
            series = [e.payload for e in generate(self.config, self.inventory) if e.entity_id == entity]
            totals = [p["throughput_since_midnight"] for p in series]
            self.assertEqual(totals, sorted(totals))
            self.assertEqual(totals[-1] - totals[0], sum(p["processed_passengers"] for p in series[1:]))

    def test_total_restarts_at_local_midnight(self):
        late = self.config.start_time.replace(hour=23, minute=57)  # 23:57 IST
        cfg = self.config.model_copy(update={"start_time": late, "end_time": late + timedelta(minutes=6)})
        rows = [e.payload for e in generate(cfg, self.inventory) if e.entity_id == "SYN-HYD-ENTRY"]
        days = [r["service_day"] for r in rows]
        self.assertEqual(days, ["2026-09-29"] * 3 + ["2026-09-30"] * 3)
        self.assertGreater(rows[2]["throughput_since_midnight"], 1000)
        self.assertEqual(rows[3]["throughput_since_midnight"], rows[3]["processed_passengers"])

    def test_unknown_timezone_rejected(self):
        raw = self.config.model_dump()
        raw["timezone"] = "Mars/Olympus"
        with self.assertRaises(Exception):
            type(self.config).model_validate(raw)
