"""Validated configuration templates. Rates are workload targets, not benchmarks."""

import json
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field

from .models import Model, NonEmpty


class AirportConfig(Model):
    airport_id: NonEmpty
    terminal_ids: list[NonEmpty] = Field(min_length=1)
    annual_passengers: int = Field(gt=0)
    timezone: Literal["UTC"]
    indoor_coordinate_frame: NonEmpty
    outdoor_crs: Literal["EPSG:4326"]
    bim_lod_target: Literal[350]


class SimulationConfig(Model):
    seed: int
    start_time: AwareDatetime
    duration_seconds: int = Field(gt=0)
    time_scale: float = Field(gt=0)


class TelemetryConfig(Model):
    registered_points: int = Field(gt=0)
    update_hz: float = Field(gt=0)
    active_fraction: float = Field(gt=0, le=1)
    batch_size: int = Field(gt=0)


class PassengerConfig(Model):
    peak_arrivals_per_hour: int = Field(gt=0)
    active_tracks: int = Field(gt=0)
    position_hz: float = Field(gt=0)
    aggregate_hz: float = Field(gt=0)
    grid_cell_size_m: float = Field(gt=0)
    touchpoints: list[Literal["entry", "check_in", "security"]] = Field(min_length=1)


class FlightConfig(Model):
    movements_per_hour: int = Field(gt=0)
    scenario_movements_per_hour: list[Annotated[int, Field(gt=0)]] = Field(min_length=1)
    arrival_fraction: float = Field(ge=0, le=1)
    aircraft_position_hz: float = Field(gt=0)
    aodb_update_hz: float = Field(gt=0)
    rms_update_hz: float = Field(gt=0)
    acdm_event_driven: Literal[True]


class KafkaConfig(Model):
    bootstrap_servers: NonEmpty
    topic_prefix: NonEmpty
    partitions_per_topic: int = Field(gt=0)
    replication_factor: int = Field(gt=0)
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    acks: Literal["all"]
    enable_idempotence: Literal[True]
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"]
    linger_ms: int = Field(ge=0)
    queue_max_messages: int = Field(gt=0)
    entity_key_field: Literal["entity_id"]


class POCConfig(Model):
    airport: AirportConfig
    simulation: SimulationConfig
    telemetry: TelemetryConfig
    passengers: PassengerConfig
    flights: FlightConfig
    kafka: KafkaConfig


def load_config(path: str | Path) -> POCConfig:
    return POCConfig.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))
