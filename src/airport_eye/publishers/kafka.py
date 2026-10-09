"""Kafka publisher built on confluent-kafka (optional ``[kafka]`` extra).

``prepare()`` stays pure (no I/O). ``publish()`` lazily creates a producer,
applies backpressure when the local queue is full, and counts delivery results
through callbacks. ``flush()`` blocks until everything is acknowledged or the
timeout expires and raises if any message failed or is still undelivered.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from ..common import entity_key
from ..config import KafkaConfig
from ..esri import ZoneLocator, to_esri_record
from ..models import EventEnvelope

ESRI_TOPIC_INFIX = "esri"


class KafkaPublishError(RuntimeError):
    pass


class KafkaPublisher:
    def __init__(
        self,
        config: KafkaConfig,
        *,
        fmt: str = "envelope",
        locator: ZoneLocator | None = None,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
        overrides: dict[str, Any] | None = None,
    ):
        if fmt not in ("envelope", "esri"):
            raise ValueError("fmt must be 'envelope' or 'esri'")
        self.config = config
        self.fmt = fmt
        self.locator = locator
        self._factory = producer_factory
        self._overrides = overrides or {}
        self._producer: Any = None
        self.queued = 0
        self.delivered = 0
        self.failed = 0
        self.errors: list[str] = []

    # -- pure --------------------------------------------------------------
    def topic_for(self, event: EventEnvelope) -> str:
        return self.topic_for_type(event.event_type)

    def topic_for_type(self, event_type: str) -> str:
        infix = f"{ESRI_TOPIC_INFIX}." if self.fmt == "esri" else ""
        return f"{self.config.topic_prefix}.{infix}{event_type}"

    def prepare(self, event: EventEnvelope) -> tuple[str, bytes, bytes]:
        if self.fmt == "esri":
            import json

            body = json.dumps(to_esri_record(event, self.locator), separators=(",", ":")).encode("utf-8")
        else:
            body = event.model_dump_json().encode("utf-8")
        return self.topic_for(event), entity_key(event), body

    # -- transport ---------------------------------------------------------
    def producer_settings(self) -> dict[str, Any]:
        c = self.config
        settings: dict[str, Any] = {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or c.bootstrap_servers,
            "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL") or c.security_protocol,
            "acks": c.acks,
            "enable.idempotence": c.enable_idempotence,
            "compression.type": c.compression_type,
            "linger.ms": c.linger_ms,
            "queue.buffering.max.messages": c.queue_max_messages,
        }
        mechanism = os.environ.get("KAFKA_SASL_MECHANISM")
        if mechanism:
            settings["sasl.mechanism"] = mechanism
            settings["sasl.username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
            settings["sasl.password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
        settings.update(self._overrides)
        return settings

    def _ensure_producer(self) -> Any:
        if self._producer is None:
            settings = self.producer_settings()
            if self._factory is not None:
                self._producer = self._factory(settings)
            else:
                try:
                    from confluent_kafka import Producer
                except ImportError as exc:  # pragma: no cover
                    raise KafkaPublishError("Install the Kafka extra: pip install -e '.[kafka]'") from exc
                self._producer = Producer(settings)
        return self._producer

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            if len(self.errors) < 20:
                self.errors.append(str(err))
        else:
            self.delivered += 1

    def publish(self, event: EventEnvelope) -> None:
        topic, key, value = self.prepare(event)
        producer = self._ensure_producer()
        while True:
            try:
                producer.produce(topic, value=value, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: serve callbacks, then retry
        self.queued += 1
        producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        if self._producer is None:
            return
        remaining = self._producer.flush(timeout)
        if remaining:
            raise KafkaPublishError(f"{remaining} message(s) not delivered within {timeout}s")
        if self.failed:
            raise KafkaPublishError(f"{self.failed} message(s) failed: {self.errors[:3]}")

    def stats(self) -> dict[str, int]:
        return {"queued": self.queued, "delivered": self.delivered, "failed": self.failed}

    def ensure_topics(self, topics: list[str], timeout: float = 20.0) -> list[str]:
        """Create topics with the configured partitions/replication; existing ones are left alone.

        Returns the topics that were created. Brokers with auto-create enabled would
        make them with one partition, so we create them explicitly.
        """
        try:
            from confluent_kafka import KafkaException
            from confluent_kafka.admin import AdminClient, NewTopic
        except ImportError as exc:  # pragma: no cover
            raise KafkaPublishError("Install the Kafka extra: pip install -e '.[kafka]'") from exc
        settings = {k: v for k, v in self.producer_settings().items()
                    if k in ("bootstrap.servers", "security.protocol") or k.startswith("sasl.")
                    or k.startswith("ssl.") or k.startswith("test.mock")}
        admin = AdminClient(settings)
        unreachable = KafkaPublishError(
            f"Kafka broker at {settings['bootstrap.servers']} is not reachable. "
            "Is it running (docker compose --profile kafka up -d) and is the address right?")
        try:
            existing = set(admin.list_topics(timeout=min(timeout, 10.0)).topics)
        except KafkaException as exc:
            raise unreachable from exc
        topics = [t for t in topics if t not in existing]
        created: list[str] = []
        if not topics:
            return created
        futures = admin.create_topics([NewTopic(t, num_partitions=self.config.partitions_per_topic,
                                                replication_factor=self.config.replication_factor)
                                       for t in topics], operation_timeout=timeout)
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout)
                created.append(topic)
            except TimeoutError as exc:
                raise KafkaPublishError(
                    f"Kafka did not confirm creating topic {topic} within {timeout:.0f}s.") from exc
            except KafkaException as exc:
                if exc.args and getattr(exc.args[0], "name", lambda: "")() == "TOPIC_ALREADY_EXISTS":
                    continue
                raise KafkaPublishError(f"could not create topic {topic}: {exc}") from exc
        return created
