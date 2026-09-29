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
    passenger_id: SyntheticID
    pnr_id: SyntheticID
    flight_id: NonEmpty
    touchpoint_id: NonEmpty
    touchpoint_type: Literal["entry", "check_in", "security"]
    processing_state: Literal["arrived", "queued", "started", "completed"]
    observed_at: AwareDatetime
    indoor: IndoorPosition


class PassengerPosition(Model):
    passenger_id: SyntheticID
    flight_id: NonEmpty
    indoor: IndoorPosition
    speed_mps: float = Field(ge=0)
    heading_deg: float = Field(ge=0, lt=360)
    movement_state: Literal["walking", "queued", "processing", "stationary"]


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

    @model_validator(mode="after")
    def valid_window_and_counts(self):
        if self.window_end <= self.window_start:
            raise ValueError("window_end must follow window_start")
        if len({cell.grid_id for cell in self.cells}) != len(self.cells):
            raise ValueError("grid_id must be unique within an aggregate")
        if sum(cell.passenger_count for cell in self.cells) != self.passenger_count:
            raise ValueError("Cell counts must sum to the zone snapshot count")
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
    "aodb.flight": AODBFlight,
    "rms.assignment": RMSAssignment,
    "acdm.milestone": ACDMMilestone,
    "aircraft.position": AircraftPosition,
}

ENTITY_FIELDS = {
    "registry.asset": "asset_id", "registry.point": "point_id",
    "facility.telemetry": "point_id", "passenger.touchpoint": "passenger_id",
    "passenger.position": "passenger_id", "passenger.aggregate": "zone_id",
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
