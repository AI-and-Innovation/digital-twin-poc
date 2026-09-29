"""TODO: synthetic PNRs, queues, entry/check-in/security state and 1 Hz tracks."""

from collections.abc import Iterator
from ..config import POCConfig
from ..models import EventEnvelope


def generate(config: POCConfig) -> Iterator[EventEnvelope]:
    raise NotImplementedError("Passenger simulation is not implemented")
