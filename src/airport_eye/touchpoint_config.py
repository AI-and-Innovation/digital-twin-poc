"""Editable inventory and run configuration for passenger touchpoint generation."""

from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AwareDatetime, Field, model_validator

from .models import Model, NonEmpty


class QueueProfile(Model):
    arrival_rate_per_hour: float = Field(ge=0)
    service_channels: int = Field(ge=1)
    service_time_min_seconds: float = Field(gt=0)
    service_time_max_seconds: float = Field(gt=0)
    initial_queue_length: int = Field(ge=0)

    @model_validator(mode="after")
    def valid_service_range(self):
        if self.service_time_max_seconds < self.service_time_min_seconds:
            raise ValueError("Service time max must be >= min")
        return self


class Touchpoint(Model):
    touchpoint_id: NonEmpty
    name: NonEmpty
    touchpoint_type: NonEmpty
    terminal_id: NonEmpty
    zone_id: NonEmpty
    enabled: bool
    inventory_level: NonEmpty
    verification_status: NonEmpty
    source_refs: list[NonEmpty] = Field(min_length=1)
    notes: str
    simulation: QueueProfile


class Source(Model):
    source_id: NonEmpty
    url: NonEmpty
    title: NonEmpty
    accessed_on: NonEmpty


class TouchpointInventory(Model):
    airport_id: NonEmpty
    airport_name: NonEmpty
    scope: NonEmpty
    limitations: list[str]
    sources: list[Source]
    touchpoints: list[Touchpoint] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_ids_and_sources(self):
        ids = [tp.touchpoint_id for tp in self.touchpoints]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate touchpoint_id")
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("Duplicate source_id")
        for tp in self.touchpoints:
            if set(tp.source_refs) - set(source_ids):
                raise ValueError(f"Unknown source_refs for {tp.touchpoint_id}")
        if not any(tp.enabled for tp in self.touchpoints):
            raise ValueError("At least one touchpoint must be enabled")
        return self


class TouchpointRunConfig(Model):
    start_time: AwareDatetime
    end_time: AwareDatetime
    interval_seconds: int = Field(gt=0, strict=True)
    seed: int
    inventory_file: NonEmpty
    output_directory: NonEmpty
    # Optional scope filter. When set, only enabled touchpoints of these types are
    # generated; every other inventory entry is left in place but switched off for
    # this run (the JSON inventory cannot carry comments). Remove the key, or list
    # more types, to bring the others back.
    include_types: list[NonEmpty] | None = None
    # Airport-local time zone. "Total throughput since midnight" counts from this zone's
    # midnight, and resets when the local date changes.
    timezone: NonEmpty = "Asia/Kolkata"
    # Optional path (relative to this run config) to service targets; adds queue-health
    # attributes (limits, estimated/p95 wait, status) to every record.
    service_targets_file: NonEmpty | None = None

    @model_validator(mode="after")
    def valid_window(self):
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Unknown time zone: {self.timezone}") from exc
        duration = (self.end_time - self.start_time).total_seconds()
        if duration <= 0:
            raise ValueError("end_time must follow start_time")
        if duration % self.interval_seconds:
            raise ValueError("Time range must contain a whole number of intervals")
        return self


def load_run(path: str | Path) -> tuple[TouchpointRunConfig, TouchpointInventory, Path]:
    path = Path(path).resolve()
    config = TouchpointRunConfig.model_validate_json(path.read_text(encoding="utf-8"))
    inventory_path = (path.parent / config.inventory_file).resolve()
    inventory = TouchpointInventory.model_validate_json(inventory_path.read_text(encoding="utf-8"))
    output = (path.parent / config.output_directory).resolve()
    return config, inventory, output


def load_targets(path: str | Path, config: TouchpointRunConfig):
    """Service targets named by the run config, or None when it names none."""
    if config.service_targets_file is None:
        return None
    from .service_targets import ServiceTargets

    return ServiceTargets.load((Path(path).resolve().parent / config.service_targets_file).resolve())
