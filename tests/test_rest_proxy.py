import base64
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from airport_eye.esri import ZoneLocator
from airport_eye.publishers.rest_proxy import RestProxyConfig, RestProxyError, RestProxyPublisher
from airport_eye.tracking import run_tracking_stream

ROOT = Path(__file__).resolve().parents[1]


class FakeProxy:
    """Records requests; answers like Confluent REST Proxy v2."""

    def __init__(self, topics, fail_first=0, reject=None):
        self.topics, self.requests, self.fail_first, self.reject = set(topics), [], fail_first, reject

    def __call__(self, method, url, body, headers, timeout):
        self.requests.append((method, url, json.loads(body) if body else None, headers))
        if method == "GET":
            return 200, json.dumps(sorted(self.topics)).encode()
        if self.fail_first:
            self.fail_first -= 1
            return 503, b"busy"
        topic = url.rsplit("/", 1)[1]
        if topic == self.reject:
            return 404, b'{"error_code":40401,"message":"Topic not found."}'
        n = len(json.loads(body)["records"])
        return 200, json.dumps({"offsets": [{"partition": 0, "offset": i} for i in range(n)]}).encode()


class RestProxyTests(unittest.TestCase):
    def setUp(self):
        self.config = RestProxyConfig.load(ROOT / "config/rest_proxy.json")
        self.locator = ZoneLocator.from_file(ROOT / "config/terminal_layout.json")

    def publisher(self, proxy):
        return RestProxyPublisher(self.config, self.locator, sender=proxy, user="u", password="p", sleep=lambda s: None)

    def test_one_simulated_minute_goes_to_the_four_topics_as_flat_esri_records(self):
        proxy = FakeProxy(self.config.topics.values())
        pub = self.publisher(proxy)
        now = {"t": 0.0}
        run_tracking_stream(ROOT / "config/passenger_tracking.json", pub, minutes=1,
                            start=datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc),
                            clock=lambda: now["t"], sleep=lambda s: now.update(t=now["t"] + s), log=lambda _m: None)
        posts = [r for r in proxy.requests if r[0] == "POST"]
        by_topic = {}
        for _, url, body, headers in posts:
            self.assertLessEqual(len(body["records"]), self.config.records_per_request)
            self.assertEqual(headers["content-type"], "application/vnd.kafka.json.v2+json")
            self.assertEqual(headers["authorization"], "Basic " + base64.b64encode(b"u:p").decode())
            by_topic.setdefault(url.rsplit("/", 1)[1], []).extend(body["records"])
        self.assertEqual(set(by_topic), set(self.config.topics.values()))
        queue = by_topic[self.config.topics["passenger.touchpoint"]]
        self.assertEqual(len(queue), 16)
        self.assertTrue(queue[0]["key"].startswith("HYD:SYN-HYD-"))
        pos = by_topic[self.config.topics["passenger.position"]][0]["value"]
        for field in ("entity_id", "event_time_epoch_ms", "longitude", "latitude", "floor", "elevation_m", "activity"):
            self.assertIn(field, pos)
        self.assertEqual(pub.stats()["failed"], 0)
        self.assertEqual(pub.stats()["delivered"], pub.stats()["queued"])

    def test_kpi_topics_not_created_yet_the_other_feeds_still_go(self):
        kpi = {t for k, t in self.config.topics.items() if k.startswith(("kpi.", "flight."))}
        proxy = FakeProxy(set(self.config.topics.values()) - kpi)
        pub = self.publisher(proxy)
        now, logs = {"t": 0.0}, []
        run_tracking_stream(ROOT / "config/passenger_tracking.json", pub, minutes=1,
                            start=datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc),
                            clock=lambda: now["t"], sleep=lambda s: now.update(t=now["t"] + s), log=logs.append)
        sent = {url.rsplit("/", 1)[1] for method, url, *_ in proxy.requests if method == "POST"}
        self.assertEqual(sent, set(self.config.topics.values()) - kpi)
        self.assertTrue(any("KPI topics not created yet" in m for m in logs))
        self.assertEqual(pub.stats()["failed"], 0)

    def test_missing_topics_are_reported_before_anything_is_sent(self):
        proxy = FakeProxy(["aiop-hyd-topic-kpi-event-v1"])
        pub = self.publisher(proxy)
        with self.assertRaises(RestProxyError) as ctx:
            pub.ensure_topics(list(self.config.topics.values()))
        self.assertIn("aiop-hyd-topic-pax-position-event-silver-v1", str(ctx.exception))
        self.assertFalse([r for r in proxy.requests if r[0] == "POST"])

    def test_retries_on_server_errors_and_fails_loudly_on_rejection(self):
        proxy = FakeProxy(self.config.topics.values(), fail_first=2)
        pub = self.publisher(proxy)
        pub._post("aiop-hyd-topic-pax-queue-event-gold-v1", [{"key": "k", "value": {"a": 1}}])
        self.assertEqual(pub.stats()["delivered"], 1)
        proxy = FakeProxy(self.config.topics.values(), reject="aiop-hyd-topic-pax-queue-event-gold-v1")
        pub = self.publisher(proxy)
        pub._buffer = {"aiop-hyd-topic-pax-queue-event-gold-v1": [{"key": "k", "value": {"a": 1}}]}
        with self.assertRaises(RestProxyError):
            pub.flush()

    def test_credentials_are_required(self):
        with self.assertRaises(RestProxyError):
            RestProxyPublisher(self.config, self.locator, sender=FakeProxy([]), user="", password="")


if __name__ == "__main__":
    unittest.main()
