"""TODO: identified 1 Hz physical tracks, touchpoint queue/service metrics and heatmaps."""

from collections.abc import Iterator
from ..config import POCConfig
from ..models import EventEnvelope


def generate(config: POCConfig) -> Iterator[EventEnvelope]:
    raise NotImplementedError("Passenger simulation is not implemented")
