"""Contract smoke tests plus explicit placeholders for future simulation tests."""

import contextlib
import io
import json
import unittest
from pathlib import Path

from pydantic import ValidationError

from airport_eye.cli import main
from airport_eye.common import load_summary
from airport_eye.config import load_config
from airport_eye.models import EventEnvelope, PAYLOAD_MODELS
from airport_eye.publishers.kafka import KafkaPublisher

ROOT = Path(__file__).resolve().parents[1]


def sample(name):
    return json.loads((ROOT / "samples" / f"{name}.json").read_text(encoding="utf-8"))


class ContractTests(unittest.TestCase):
    def test_all_sample_types_validate(self):
        events = [EventEnvelope.model_validate_json(path.read_text(encoding="utf-8"))
                  for path in (ROOT / "samples").glob("*.json")]
        self.assertEqual(len(events), 17)
        self.assertEqual({event.event_type for event in events}, set(PAYLOAD_MODELS))

    def test_sample_relationships(self):
        payload = lambda name: sample(name)["payload"]
        asset, point, telemetry = map(payload, ["asset_registry", "point_registry", "facility_telemetry"])
        self.assertEqual(point["asset_id"], asset["asset_id"])
        self.assertEqual(telemetry["point_id"], point["point_id"])
        self.assertEqual(telemetry["asset_id"], asset["asset_id"])
        self.assertEqual(telemetry["unit"], point["unit"])
        flight = payload("aodb_flight")
        for name in ["passenger_position", "rms_assignment", "acdm_milestone", "aircraft_position"]:
            self.assertEqual(payload(name)["flight_id"], flight["flight_id"])
        self.assertEqual(payload("aircraft_position")["aircraft_id"], flight["aircraft_id"])
        self.assertEqual(payload("aircraft_position")["stand_id"], payload("rms_assignment")["resource_id"])

    def test_touchpoint_metrics_and_entity_key(self):
        event = sample("passenger_touchpoint")
        publisher = KafkaPublisher(load_config(ROOT / "config/poc.json").kafka)
        _, key, _ = publisher.prepare(EventEnvelope.model_validate(event))
        self.assertEqual(key, b"SYN-AIRPORT-001:SYN-ENTRY-001")
        event["payload"].update(processed_passengers=0, avg_wait_time_seconds=None,
                                avg_processing_time_seconds=None)
        EventEnvelope.model_validate(event)
        for changes in [
            {"queue_length": -1},
            {"passengers_in_service": -1},
            {"processed_passengers": -1},
            {"avg_wait_time_seconds": -1},
            {"avg_processing_time_seconds": -1},
            {"window_end": event["payload"]["window_start"]},
            {"processed_passengers": 1},
            {"avg_wait_time_seconds": 0},
            {"passenger_id": "SYN-PAX-000001"},
        ]:
            with self.subTest(changes=changes):
                invalid = {**event, "payload": {**event["payload"], **changes}}
                with self.assertRaises(ValidationError):
                    EventEnvelope.model_validate(invalid)

    def test_position_requires_synthetic_identification(self):
        event = sample("passenger_position")
        del event["payload"]["pnr_id"]
        with self.assertRaises(ValidationError):
            EventEnvelope.model_validate(event)
        event["payload"]["pnr_id"] = "REAL-123"
        with self.assertRaises(ValidationError):
            EventEnvelope.model_validate(event)

    def test_rejects_invalid_contracts(self):
        mutations = [
            ("facility_telemetry", lambda e: e.update(event_time="2026-01-01T08:00:00")),
            ("facility_telemetry", lambda e: e.update(sequence_no=-1)),
            ("facility_telemetry", lambda e: e.update(entity_id="wrong-key")),
            ("facility_telemetry", lambda e: e.update(event_type="unknown")),
            ("facility_telemetry", lambda e: e.update(schema_version="2.0.0")),
            ("passenger_position", lambda e: e["payload"].update(passenger_id="REAL-123")),
            ("passenger_touchpoint", lambda e: e["payload"].update(email="not-allowed")),
            ("aircraft_position", lambda e: e["payload"]["outdoor"].update(lat=91)),
            ("passenger_aggregate", lambda e: e["payload"].update(passenger_count=99)),
            ("rms_assignment", lambda e: e["payload"].update(end_time="2026-01-01T06:00:00Z")),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name, mutation=mutate):
                event = sample(name)
                mutate(event)
                with self.assertRaises(ValidationError):
                    EventEnvelope.model_validate(event)

    def test_scale_config_and_invalid_rates(self):
        config = load_config(ROOT / "config/poc.json")
        self.assertEqual(load_summary(config)["facility_events_per_second"], 20000)
        self.assertEqual(load_summary(config)["passenger_position_events_per_second"], 20000)
        raw = config.model_dump()
        raw["passengers"]["position_hz"] = 0
        with self.assertRaises(ValidationError):
            type(config).model_validate(raw)

    def test_kafka_record_preparation(self):
        config = load_config(ROOT / "config/poc.json")
        publisher = KafkaPublisher(config.kafka)
        event = EventEnvelope.model_validate(sample("passenger_position"))
        topic, key, value = publisher.prepare(event)
        self.assertEqual(topic, "airport-eye.poc.passenger.position")
        self.assertEqual(key, b"SYN-AIRPORT-001:SYN-PAX-000001")
        self.assertEqual(EventEnvelope.model_validate_json(value), event)
        # transport is covered in test_kafka_publisher.py

    def test_cli_validation_and_generate_stub(self):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["validate-samples", str(ROOT / "samples")]), 0)
            self.assertEqual(main(["validate-config", str(ROOT / "config/poc.json")]), 0)
            self.assertEqual(main(["generate", "passenger", "--config", str(ROOT / "config/poc.json")]), 2)
            self.assertEqual(main(["validate-samples", str(ROOT / "missing-samples")]), 2)

    @unittest.skip("TODO: implement generators, then verify transitions and sequence continuity")
    def test_simulation_temporal_consistency(self):
        pass

    @unittest.skip("TODO: implement publisher, then measure sustained rate, latency and backpressure")
    def test_kafka_load_and_delivery(self):
        pass


if __name__ == "__main__":
    unittest.main()
