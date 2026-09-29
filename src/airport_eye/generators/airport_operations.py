"""TODO: one shared flight schedule feeding consistent AODB, RMS and A-CDM events."""

from collections.abc import Iterator
from ..config import POCConfig
from ..models import EventEnvelope


def generate(config: POCConfig) -> Iterator[EventEnvelope]:
    raise NotImplementedError("Airport operations simulation is not implemented")
