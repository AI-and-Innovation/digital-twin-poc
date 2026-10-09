import json
import unittest
from pathlib import Path

from airport_eye.config import load_config
from airport_eye.esri import ZoneLocator, to_esri_record
from airport_eye.models import EventEnvelope
from airport_eye.publishers.kafka import KafkaPublishError, KafkaPublisher

ROOT = Path(__file__).resolve().parents[1]


def sample(name: str) -> EventEnvelope:
    return EventEnvelope.model_validate_json((ROOT / "samples" / name).read_text(encoding="utf-8"))


class FakeProducer:
    """Mimics the confluent-kafka Producer surface the publisher uses."""

    def __init__(self, settings, fail=False, buffer_errors=0):
        self.settings, self.fail, self.buffer_errors = settings, fail, buffer_errors
        self.sent, self.pending = [], []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))
        self.pending.append(on_delivery)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for cb in self.pending:
            cb("boom" if self.fail else None, None)
        self.pending = []
        return 0


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "config" / "poc.json").kafka

    def test_envelope_topic_and_key_unchanged(self):
        pub = KafkaPublisher(self.config)
        topic, key, value = pub.prepare(sample("facility_telemetry.json"))
        self.assertEqual(topic, "airport-eye.poc.facility.telemetry")
        self.assertEqual(key, b"SYN-AIRPORT-001:SYN-POINT-001")
        self.assertIn("payload", json.loads(value))

    def test_publish_and_flush_with_delivery_callbacks(self):
        producers = []
        pub = KafkaPublisher(self.config, producer_factory=lambda s: producers.append(FakeProducer(s)) or producers[-1])
        for _ in range(3):
            pub.publish(sample("facility_telemetry.json"))
        pub.flush()
        self.assertEqual(pub.stats(), {"queued": 3, "delivered": 3, "failed": 0})
        self.assertEqual(len(producers[0].sent), 3)
        self.assertEqual(producers[0].settings["acks"], "all")
        self.assertIs(producers[0].settings["enable.idempotence"], True)

    def test_backpressure_retries_on_full_queue(self):
        pub = KafkaPublisher(self.config, producer_factory=lambda s: FakeProducer(s, buffer_errors=2))
        pub.publish(sample("facility_telemetry.json"))
        pub.flush()
        self.assertEqual(pub.stats()["delivered"], 1)

    def test_failed_delivery_raises_on_flush(self):
        pub = KafkaPublisher(self.config, producer_factory=lambda s: FakeProducer(s, fail=True))
        pub.publish(sample("facility_telemetry.json"))
        with self.assertRaises(KafkaPublishError):
            pub.flush()

    def test_env_overrides_bootstrap(self):
        import os
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "broker.example:9093"
        try:
            self.assertEqual(KafkaPublisher(self.config).producer_settings()["bootstrap.servers"], "broker.example:9093")
        finally:
            del os.environ["KAFKA_BOOTSTRAP_SERVERS"]


class EsriProjectionTests(unittest.TestCase):
    def setUp(self):
        self.locator = ZoneLocator.from_file(ROOT / "config" / "terminal_layout.json")

    def test_record_is_flat_with_epoch_time(self):
        rec = to_esri_record(sample("facility_telemetry.json"), self.locator)
        self.assertNotIn("payload", rec)
        self.assertEqual(rec["point_id"], "SYN-POINT-001")
        self.assertEqual(rec["value"], 18.5)
        self.assertIsInstance(rec["event_time_epoch_ms"], int)
        self.assertEqual(rec["event_time"], "2026-01-01T08:00:00Z")
        self.assertNotIn("longitude", rec)  # telemetry carries no location

    def test_asset_registry_nested_location_is_flattened(self):
        rec = to_esri_record(sample("asset_registry.json"), self.locator)
        self.assertEqual(rec["indoor_x"], 25.0)
        self.assertEqual((rec["longitude"], rec["latitude"]), (0.001, 0.001))

    def test_zone_lookup_gives_geometry(self):
        zone = next(iter(self.locator.zones))
        rec = to_esri_record(sample("passenger_touchpoint.json"), ZoneLocator({"SYN-ZONE-ENTRY": self.locator.zones[zone]}))
        self.assertIn("longitude", rec)

    def test_esri_topic_is_separate(self):
        pub = KafkaPublisher(load_config(ROOT / "config" / "poc.json").kafka, fmt="esri", locator=self.locator)
        topic, _, value = pub.prepare(sample("facility_telemetry.json"))
        self.assertEqual(topic, "airport-eye.poc.esri.facility.telemetry")
        self.assertNotIn("payload", json.loads(value))


if __name__ == "__main__":
    unittest.main()
