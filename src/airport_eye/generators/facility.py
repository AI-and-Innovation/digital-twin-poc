"""TODO: registry creation, sensor profiles, equipment states and failure scenarios."""

from collections.abc import Iterator
from ..config import POCConfig
from ..models import EventEnvelope


def generate(config: POCConfig) -> Iterator[EventEnvelope]:
    raise NotImplementedError("Facility simulation is not implemented")
