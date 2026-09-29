"""Kafka adapter scaffold. No network connections or publishing yet."""

from ..common import entity_key
from ..config import KafkaConfig
from ..models import EventEnvelope


class KafkaPublisher:
    def __init__(self, config: KafkaConfig):
        self.config = config

    def prepare(self, event: EventEnvelope) -> tuple[str, bytes, bytes]:
        return (
            f"{self.config.topic_prefix}.{event.event_type}",
            entity_key(event),
            event.model_dump_json().encode("utf-8"),
        )

    def publish(self, event: EventEnvelope) -> None:
        # TODO: lazy producer initialization, bounded queue/backpressure, delivery
        # callbacks, retries, polling, metrics, credentials and shutdown flush.
        raise NotImplementedError("Kafka transport is not implemented; use prepare() to inspect records")

    def flush(self) -> None:
        raise NotImplementedError("Kafka transport is not implemented")
