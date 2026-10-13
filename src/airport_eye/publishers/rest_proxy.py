"""Publish the flat Esri records through a Confluent-style Kafka REST Proxy (v2 API).

For environments where only the REST proxy is reachable (no direct broker access):

    POST {url}/topics/{topic}
    content-type: application/vnd.kafka.json.v2+json
    {"records": [{"key": "HYD:SYN-PAX-000123", "value": {...flat Esri record...}}, ...]}

Topic names come from config/rest_proxy.json (one per feed). Credentials are never
stored in files: set KAFKA_REST_USER and KAFKA_REST_PASSWORD (and KAFKA_REST_URL to
override the URL). The publisher buffers records and sends them on ``flush`` in
chunks, so one simulation step is a handful of HTTP requests.
"""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import Field

from ..common import entity_key
from ..esri import ZoneLocator, to_esri_record
from ..models import EventEnvelope, Model, NonEmpty

CONTENT_TYPE = "application/vnd.kafka.json.v2+json"
ACCEPT = "application/vnd.kafka.v2+json"


class RestProxyConfig(Model):
    note: str = Field(default="", alias="_note")
    url: NonEmpty
    topics: dict[str, NonEmpty]  # event_type -> topic
    records_per_request: int = Field(default=500, gt=0)
    timeout_seconds: float = Field(default=30, gt=0)
    retries: int = Field(default=3, ge=0)

    model_config = Model.model_config | {"populate_by_name": True}

    @classmethod
    def load(cls, path: str | Path) -> "RestProxyConfig":
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))


class RestProxyError(RuntimeError):
    pass


Sender = Callable[[str, str, bytes | None, dict[str, str], float], tuple[int, bytes]]


def _urllib_send(method: str, url: str, body: bytes | None, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


class RestProxyPublisher:
    """Same calls as KafkaPublisher (publish / flush / stats / topic_for_type / ensure_topics)."""

    def __init__(self, config: RestProxyConfig, locator: ZoneLocator | None, *, sender: Sender | None = None,
                 user: str | None = None, password: str | None = None, url: str | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        self.config = config
        self.locator = locator
        self.url = (url or os.environ.get("KAFKA_REST_URL") or config.url).rstrip("/")
        user = user if user is not None else os.environ.get("KAFKA_REST_USER")
        password = password if password is not None else os.environ.get("KAFKA_REST_PASSWORD")
        if not user or not password:
            raise RestProxyError("Set KAFKA_REST_USER and KAFKA_REST_PASSWORD for the Kafka REST proxy")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"authorization": f"Basic {token}", "content-type": CONTENT_TYPE, "accept": ACCEPT}
        self.send = sender or _urllib_send
        self.sleep = sleep
        self._buffer: dict[str, list[dict[str, Any]]] = {}
        self.queued = self.delivered = self.failed = 0
        self.errors: list[str] = []

    # -- topics ----------------------------------------------------------------
    def topic_for_type(self, event_type: str) -> str:
        try:
            return self.config.topics[event_type]
        except KeyError as exc:
            raise RestProxyError(f"No REST proxy topic configured for {event_type}") from exc

    def missing_topics(self, topics: list[str]) -> list[str]:
        status, body = self.send("GET", f"{self.url}/topics", None, self.headers, self.config.timeout_seconds)
        if status != 200:
            raise RestProxyError(f"REST proxy {self.url} answered {status}: {body[:200]!r}")
        existing = set(json.loads(body))
        return [t for t in topics if t not in existing]

    def ensure_topics(self, topics: list[str], timeout: float = 20.0) -> list[str]:
        """The v2 proxy cannot create topics: fail early, naming the ones that do not exist yet."""
        missing = self.missing_topics(topics)
        if missing:
            raise RestProxyError("These topics do not exist on the cluster yet; ask the Kafka admin to create them "
                                 "(or rerun with --no-create-topics if the cluster auto-creates topics): "
                                 + ", ".join(missing))
        return []

    # -- publishing ------------------------------------------------------------
    def publish(self, event: EventEnvelope) -> None:
        record = {"key": entity_key(event).decode("utf-8"), "value": to_esri_record(event, self.locator)}
        self._buffer.setdefault(self.topic_for_type(event.event_type), []).append(record)
        self.queued += 1

    def _post(self, topic: str, records: list[dict[str, Any]]) -> None:
        body = json.dumps({"records": records}, separators=(",", ":"), default=str).encode("utf-8")
        url = f"{self.url}/topics/{topic}"
        for attempt in range(self.config.retries + 1):
            try:
                status, reply = self.send("POST", url, body, self.headers, self.config.timeout_seconds)
            except OSError as exc:  # network trouble: retry
                status, reply = 0, str(exc).encode()
            if status == 200:
                offsets = json.loads(reply).get("offsets", [])
                bad = [o for o in offsets if o.get("error_code") or o.get("error")]
                self.delivered += len(records) - len(bad)
                self.failed += len(bad)
                if bad and len(self.errors) < 20:
                    self.errors.append(f"{topic}: {bad[0].get('error')}")
                return
            if status in (408, 429) or status >= 500 or status == 0:
                self.sleep(min(2 ** attempt, 10))
                continue
            break  # 4xx: retrying will not help
        self.failed += len(records)
        if len(self.errors) < 20:
            self.errors.append(f"{topic}: HTTP {status} {reply[:300]!r}")

    def flush(self, timeout: float = 30.0) -> None:
        buffer, self._buffer = self._buffer, {}
        size = self.config.records_per_request
        for topic, records in buffer.items():
            for i in range(0, len(records), size):
                self._post(topic, records[i:i + size])
        if self.failed:
            raise RestProxyError(f"{self.failed} record(s) failed: {self.errors[:3]}")

    def stats(self) -> dict[str, int]:
        return {"queued": self.queued, "delivered": self.delivered, "failed": self.failed}
