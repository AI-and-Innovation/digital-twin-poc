"""Version 1 event contract templates; all passenger identifiers are synthetic."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


SyntheticID = Annotated[str, Field(pattern=r"^SYN-[A-Za-z0-9-]+$")]
NonEmpty = Annotated[str, Field(min_length=1)]


class IndoorPosition(Model):
    coordinate_frame_id: NonEmpty
    x: float  # metres in the configured local frame
    y: float
    z: float
    floor: NonEmpty
    zone_id: NonEmpty
    grid_id: NonEmpty


class OutdoorPosition(Model):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    altitude: float  # metres; datum must be explicit
    altitude_datum: Literal["MSL", "WGS84_ELLIPSOID"]
    gis_location_id: NonEmpty


class AssetRegistry(Model):
    asset_id: NonEmpty
    asset_type: NonEmpty
    bim_element_id: NonEmpty
    zone_id: NonEmpty
    gis_location_id: NonEmpty
    indoor: IndoorPosition | None = None
    outdoor: OutdoorPosition | None = None

    @model_validator(mode="after")
    def consistent_zone(self):
        if self.indoor and self.indoor.zone_id != self.zone_id:
            raise ValueError("Asset and indoor position must share zone_id")
        return self


class PointRegistry(Model):
    point_id: NonEmpty
    asset_id: NonEmpty
    metric: NonEmpty
    value_type: Literal["number", "boolean", "string"]
    unit: NonEmpty
    nominal_update_hz: float = Field(gt=0)


class FacilityTelemetry(Model):
    point_id: NonEmpty
    asset_id: NonEmpty
    value: float | bool | str
    unit: NonEmpty
    quality: Literal["good", "uncertain", "bad"]
    equipment_status: Literal["on", "off", "fault", "unknown"]


class PassengerTouchpoint(Model):
    """Touchpoint queue snapshot and completed-service metrics for a time window."""

    touchpoint_id: NonEmpty
    touchpoint_type: NonEmpty  # Editable inventory taxonomy, including arrivals/transfers.
    zone_id: NonEmpty
    window_start: AwareDatetime
    window_end: AwareDatetime
    queue_length: int = Field(ge=0, description="Passengers waiting at window_end; excludes those in service")
    passengers_in_service: int = Field(ge=0)
    processed_passengers: int = Field(ge=0, description="Services completed in (window_start, window_end]")
    avg_wait_time_seconds: float | None = Field(ge=0, description="Mean queue-entry to service-start duration for completed services")
    avg_processing_time_seconds: float | None = Field(ge=0, description="Mean service-start to completion duration for completed services")
    # Optional queue-health attributes (see service_targets.py). All default to None so
    # earlier records and samples stay valid.
    throughput_since_midnight: int | None = Field(default=None, ge=0, description="Services completed from airport-local midnight to window_end")
    service_day: str | None = Field(default=None, description="Airport-local date the since-midnight total refers to")
    touchpoint_name: str | None = None
    flow: str | None = None
    service_channels: int | None = Field(default=None, ge=1)
    utilization_pct: float | None = Field(default=None, ge=0)
    throughput_per_hour: float | None = Field(default=None, ge=0)
    estimated_wait_seconds: float | None = Field(default=None, ge=0)
    p95_wait_seconds: float | None = Field(default=None, ge=0)
    target_wait_seconds: float | None = Field(default=None, gt=0, description="Operational target (not an SLA)")
    aera_existing_wait_seconds: float | None = Field(default=None, gt=0)
    aera_proposed_wait_seconds: float | None = Field(default=None, gt=0, description="August 2025 consultation proposal, not enacted")
    target_basis: Literal["recommended", "assumed_from_similar", "none"] | None = None
    queue_limit: int | None = Field(default=None, ge=0, description="Queue length at which the operational wait target would be reached")
    status: Literal["no_target", "normal", "busy", "congested"] | None = None
    sla_breached: bool | None = None

    @model_validator(mode="after")
    def valid_metrics(self):
        if self.window_end <= self.window_start:
            raise ValueError("window_end must follow window_start")
        durations = (self.avg_wait_time_seconds, self.avg_processing_time_seconds)
        if self.processed_passengers == 0 and any(value is not None for value in durations):
            raise ValueError("Duration averages must be null when no passengers completed service")
        if self.processed_passengers > 0 and any(value is None for value in durations):
            raise ValueError("Duration averages are required when passengers completed service")
        return self


class PassengerPosition(Model):
    """Identified synthetic passenger's physical location at envelope event_time."""

    passenger_id: SyntheticID
    pnr_id: SyntheticID
    flight_id: NonEmpty
    indoor: IndoorPosition
    speed_mps: float = Field(ge=0)
    heading_deg: float = Field(ge=0, lt=360)
    movement_state: Literal["walking", "stationary"]
    # Optional journey context (see generators/passenger.py). Defaults keep older records valid.
    journey: Literal["domestic_departure", "international_departure", "domestic_arrival",
                     "international_arrival", "ga_departure", "ga_arrival"] | None = None
    activity: Literal["walking", "queuing", "in_service", "dwelling", "at_gate", "reclaim",
                      "boarded", "exited"] | None = Field(
        default=None, description="'boarded' and 'exited' are the last record of a track")
    touchpoint_id: str | None = Field(default=None, description="Touchpoint being queued at or served by")
    gate_id: str | None = None


class HeatmapCell(Model):
    grid_id: NonEmpty
    passenger_count: int = Field(ge=0)
    area_m2: float = Field(gt=0)
    # Density is derived as passenger_count / area_m2.


class PassengerAggregate(Model):
    zone_id: NonEmpty
    coordinate_frame_id: NonEmpty
    floor: NonEmpty
    window_start: AwareDatetime
    window_end: AwareDatetime
    count_semantics: Literal["snapshot_at_window_end"]
    passenger_count: int = Field(ge=0)
    cells: list[HeatmapCell]
    # Optional occupancy context (see generators/passenger.py); defaults keep older records valid.
    zone_name: str | None = None
    area_m2: float | None = Field(default=None, gt=0)
    density_per_100m2: float | None = Field(default=None, ge=0)
    people_walking: int | None = Field(default=None, ge=0)
    people_queuing: int | None = Field(default=None, ge=0)
    people_in_service: int | None = Field(default=None, ge=0)
    people_dwelling: int | None = Field(default=None, ge=0)
    people_at_gate: int | None = Field(default=None, ge=0)
    people_reclaim: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def valid_window_and_counts(self):
        if self.window_end <= self.window_start:
            raise ValueError("window_end must follow window_start")
        if len({cell.grid_id for cell in self.cells}) != len(self.cells):
            raise ValueError("grid_id must be unique within an aggregate")
        if sum(cell.passenger_count for cell in self.cells) != self.passenger_count:
            raise ValueError("Cell counts must sum to the zone snapshot count")
        return self


class PassengerJourneyKpi(Model):
    """Terminal-wide passenger journey indicators for one window (rolling percentiles)."""

    kpi_id: NonEmpty
    window_start: AwareDatetime
    window_end: AwareDatetime
    rolling_window_minutes: float = Field(gt=0)
    people_in_terminal: int = Field(ge=0)
    people_departing: int = Field(ge=0)
    people_arriving: int = Field(ge=0)
    people_queuing: int = Field(ge=0)
    departures_cleared_security: int = Field(ge=0, description="Departing passengers who reached airside in the rolling window")
    kerb_to_airside_p50_minutes: float | None = Field(default=None, ge=0)
    kerb_to_airside_p90_minutes: float | None = Field(default=None, ge=0)
    passengers_boarded: int = Field(ge=0)
    airside_to_boarding_p50_minutes: float | None = Field(default=None, ge=0)
    arrivals_exited: int = Field(ge=0)
    arrival_to_exit_p50_minutes: float | None = Field(default=None, ge=0)
    arrival_to_exit_p90_minutes: float | None = Field(default=None, ge=0)
    longest_wait_touchpoint_id: str | None = None
    longest_estimated_wait_seconds: float | None = Field(default=None, ge=0)
    touchpoints_busy: int = Field(ge=0)
    touchpoints_congested: int = Field(ge=0)

    @model_validator(mode="after")
    def valid_window(self):
        if self.window_end <= self.window_start:
            raise ValueError("window_end must follow window_start")
        return self


class AODBFlight(Model):
    flight_id: NonEmpty
    flight_number: NonEmpty
    aircraft_id: NonEmpty
    direction: Literal["arrival", "departure"]
    origin_airport_id: NonEmpty
    destination_airport_id: NonEmpty
    scheduled_time: AwareDatetime
    estimated_time: AwareDatetime | None = None
    actual_time: AwareDatetime | None = None
    status: Literal["scheduled", "boarding", "airborne", "landed", "on_stand", "departed", "cancelled"]


class RMSAssignment(Model):
    assignment_id: NonEmpty
    flight_id: NonEmpty
    resource_id: NonEmpty
    resource_type: Literal["stand", "gate", "check_in_counter", "baggage_belt"]
    start_time: AwareDatetime
    end_time: AwareDatetime
    status: Literal["planned", "active", "released"]

    @model_validator(mode="after")
    def valid_interval(self):
        if self.end_time <= self.start_time:
            raise ValueError("Assignment end_time must follow start_time")
        return self


class ACDMMilestone(Model):
    flight_id: NonEmpty
    milestone_code: Literal["EOBT", "TOBT", "TSAT", "AOBT", "ATOT", "ALDT", "AIBT"]
    milestone_time: AwareDatetime
    time_kind: Literal["estimated", "target", "actual"]
    revision: int = Field(ge=0)


class AircraftPosition(Model):
    aircraft_id: NonEmpty
    flight_id: NonEmpty
    phase: Literal["approach", "landing", "taxi_in", "on_stand", "pushback", "taxi_out", "takeoff", "climb"]
    outdoor: OutdoorPosition
    ground_speed_mps: float = Field(ge=0)
    heading_deg: float = Field(ge=0, lt=360)
    vertical_speed_mps: float
    on_ground: bool
    runway_id: NonEmpty | None = None
    taxiway_id: NonEmpty | None = None
    stand_id: NonEmpty | None = None


PAYLOAD_MODELS = {
    "registry.asset": AssetRegistry,
    "registry.point": PointRegistry,
    "facility.telemetry": FacilityTelemetry,
    "passenger.touchpoint": PassengerTouchpoint,
    "passenger.position": PassengerPosition,
    "passenger.aggregate": PassengerAggregate,
    "passenger.journey_kpi": PassengerJourneyKpi,
    "aodb.flight": AODBFlight,
    "rms.assignment": RMSAssignment,
    "acdm.milestone": ACDMMilestone,
    "aircraft.position": AircraftPosition,
}

ENTITY_FIELDS = {
    "registry.asset": "asset_id", "registry.point": "point_id",
    "facility.telemetry": "point_id", "passenger.touchpoint": "touchpoint_id",
    "passenger.position": "passenger_id", "passenger.aggregate": "zone_id",
    "passenger.journey_kpi": "kpi_id",
    "aodb.flight": "flight_id", "rms.assignment": "assignment_id",
    "acdm.milestone": "flight_id", "aircraft.position": "aircraft_id",
}


class EventEnvelope(Model):
    event_id: UUID
    event_type: NonEmpty
    schema_version: Literal["1.0.0"]
    airport_id: NonEmpty
    terminal_id: NonEmpty | None
    source_system: NonEmpty
    entity_id: NonEmpty
    event_time: AwareDatetime
    ingestion_time: AwareDatetime
    sequence_no: int = Field(ge=0)
    payload: dict

    @model_validator(mode="after")
    def validate_payload(self):
        model = PAYLOAD_MODELS.get(self.event_type)
        if model is None:
            raise ValueError(f"Unsupported event_type: {self.event_type}")
        payload = model.model_validate(self.payload)
        if getattr(payload, ENTITY_FIELDS[self.event_type]) != self.entity_id:
            raise ValueError("entity_id must equal the payload entity key")
        return self
