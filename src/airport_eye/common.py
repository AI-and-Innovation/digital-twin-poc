"""Shared event helpers; deterministic scheduling and seeded randomness come later."""

from .config import POCConfig
from .models import EventEnvelope


def entity_key(event: EventEnvelope) -> bytes:
    """Scope keys by airport to avoid collisions between airports."""
    return f"{event.airport_id}:{event.entity_id}".encode("utf-8")


def load_summary(config: POCConfig) -> dict[str, float]:
    return {
        "facility_events_per_second": config.telemetry.registered_points
        * config.telemetry.active_fraction * config.telemetry.update_hz,
        "passenger_position_events_per_second": config.passengers.active_tracks
        * config.passengers.position_hz,
        "flight_movements_per_hour": config.flights.movements_per_hour,
    }
