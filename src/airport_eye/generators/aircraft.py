"""TODO: approach through departure trajectories linked to flights and stand assignments."""

from collections.abc import Iterator
from ..config import POCConfig
from ..models import EventEnvelope


def generate(config: POCConfig) -> Iterator[EventEnvelope]:
    raise NotImplementedError("Aircraft movement simulation is not implemented")
