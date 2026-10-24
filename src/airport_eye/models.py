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
    minutes_to_departure: float | None = Field(default=None, description="Departing passengers: minutes until their flight's scheduled departure")


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


class VehiclePosition(Model):
    """A landside vehicle (car, cab, taxi, bus) or an airside ground handling vehicle at event_time."""

    vehicle_id: NonEmpty
    vehicle_type: Literal["private_car", "app_cab", "taxi", "bus", "baggage_tractor", "fuel_truck",
                          "catering_truck", "apron_bus", "pushback_tug", "gpu"]
    domain: Literal["landside", "airside"]
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    floor: NonEmpty                       # L1 = elevated departures drive, L0 = ground
    x: float                              # terminal-local metres
    y: float
    speed_kmh: float = Field(ge=0)
    heading_deg: float = Field(ge=0, lt=360)
    state: NonEmpty                       # driving, queuing, dropping_off, at_kerb, loading, leaving, parked, exited, idle, to_stand, servicing, ...
    road_name: str | None = None
    facility_id: str | None = Field(default=None, description="Kerb, bus stop or car park the vehicle is heading to or using")
    stand_id: str | None = None
    flight_id: str | None = None
    task: str | None = Field(default=None, description="Airside: the turnaround task being served")
    passengers_on_board: int = Field(ge=0)
    dwell_seconds: float | None = Field(default=None, ge=0)
    overstay: bool | None = None


class CurbOccupancy(Model):
    """Kerb, bus stop or car park occupancy for one window."""

    facility_id: NonEmpty
    name: NonEmpty
    facility_type: Literal["kerb", "bus_stop", "car_park"]
    purpose: NonEmpty
    floor: NonEmpty
    lat: float
    lon: float
    window_start: AwareDatetime
    window_end: AwareDatetime
    capacity: int = Field(ge=0)
    occupied: int = Field(ge=0)
    occupancy_pct: float = Field(ge=0)
    vehicles_queuing: int = Field(ge=0)
    arrivals: int = Field(ge=0)
    departures: int = Field(ge=0)
    avg_dwell_seconds: float | None = Field(default=None, ge=0)
    max_dwell_seconds: float | None = Field(default=None, gt=0)
    overstays: int = Field(ge=0)
    status: Literal["normal", "busy", "congested"]


class RoadTraffic(Model):
    """Vehicles and average speed on one named landside road for one window."""

    road_id: NonEmpty
    name: NonEmpty
    floor: NonEmpty
    lat: float
    lon: float
    window_start: AwareDatetime
    window_end: AwareDatetime
    vehicles: int = Field(ge=0)
    avg_speed_kmh: float | None = Field(default=None, ge=0)
    speed_limit_kmh: float = Field(gt=0)
    congestion: Literal["free", "slow", "congested"]
    length_m: float = Field(gt=0)


class StandActivity(Model):
    """An aircraft stand's turnaround status for one window."""

    stand_id: NonEmpty
    osm_ref: str | None = None
    flow: Literal["dom", "int"]
    lat: float
    lon: float
    floor: NonEmpty
    window_start: AwareDatetime
    window_end: AwareDatetime
    flight_id: str | None = None
    phase: Literal["vacant", "arrival_turnaround", "departure_preparation"]
    on_block: AwareDatetime | None = None
    off_block: AwareDatetime | None = None
    vehicles_present: int = Field(ge=0)
    baggage_tractor_present: int = Field(ge=0)
    fuel_truck_present: int = Field(ge=0)
    catering_truck_present: int = Field(ge=0)
    apron_bus_present: int = Field(ge=0)
    pushback_tug_present: int = Field(ge=0)
    gpu_present: int = Field(ge=0)
    tasks_total: int = Field(ge=0)
    tasks_done: int = Field(ge=0)
    tasks_late: int = Field(ge=0)
    status: Literal["vacant", "on_time", "at_risk", "delayed"]


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


# ---- AE-IT widget cards (finished KPI values), one record per card per minute ---------------------------------
class KpiCheckpoint(Model):
    """One desk / lane / counter card: queue, processing, wait, happiness, wait trend."""

    checkpoint_id: NonEmpty
    touchpoint_id: NonEmpty
    card: Literal["check_in", "security", "passport"]
    desk_number: int = Field(ge=1)
    desk_name: NonEmpty
    floor: NonEmpty
    lat: float
    lon: float
    window_start: AwareDatetime
    window_end: AwareDatetime
    desk_open: bool
    queue_length: int = Field(ge=0)
    avg_processing_time_seconds: float | None = Field(default=None, ge=0)
    avg_wait_time_seconds: float | None = Field(default=None, ge=0)
    happiness_index: Literal["Happy", "Neutral", "Unhappy"] | None = None
    passengers_served: int = Field(ge=0)
    rolling_window_minutes: float = Field(gt=0)
    wait_threshold_minutes: float = Field(gt=0)
    hour_avg_wait_minutes: float | None = Field(default=None, ge=0)
    previous_hour_avg_wait_minutes: float | None = Field(default=None, ge=0)
    today_hourly_avg_wait_minutes: list[float | None] = Field(min_length=24, max_length=24)


class KpiCurbside(Model):
    """Departures or arrivals curbside card."""

    curbside_id: NonEmpty
    name: NonEmpty
    side: Literal["departures", "arrivals"]
    floor: NonEmpty
    lat: float
    lon: float
    window_start: AwareDatetime
    window_end: AwareDatetime
    people_count: int = Field(ge=0)
    live_vehicle_count: int = Field(ge=0)
    vehicles_this_hour: int = Field(ge=0)
    vehicles_vs_previous_hour_pct: float | None = None
    avg_dwell_seconds: float | None = Field(default=None, ge=0)
    avg_dwell_vs_previous_hour_pct: float | None = None
    this_hour_private_car: int = Field(ge=0)
    this_hour_app_cab: int = Field(ge=0)
    this_hour_taxi: int = Field(ge=0)
    this_hour_bus: int = Field(ge=0)
    trolleys_available: int | None = Field(default=None, ge=0)
    trolleys_total: int | None = Field(default=None, ge=0)
    today_hourly_vehicles: list[int | None] = Field(min_length=24, max_length=24)


class KpiOverview(Model):
    """Airport or terminal overview card, for the clock hour so far."""

    overview_id: NonEmpty
    scope: Literal["airport", "terminal"]
    name: NonEmpty
    lat: float
    lon: float
    window_start: AwareDatetime
    window_end: AwareDatetime
    hour_start: AwareDatetime
    arrival_flights: int = Field(ge=0)
    arrival_flights_scheduled: int = Field(ge=0)
    arrival_vs_schedule_pct: float | None = None
    departure_flights: int = Field(ge=0)
    departure_flights_scheduled: int = Field(ge=0)
    departure_vs_schedule_pct: float | None = None
    stands_occupied: int = Field(ge=0)
    stands_total: int = Field(ge=0)
    ota_pct: float | None = Field(default=None, ge=0, le=100)
    otd_pct: float | None = Field(default=None, ge=0, le=100)
    avg_arrival_delay_minutes: float | None = None
    avg_departure_delay_minutes: float | None = None
    avg_taxi_in_minutes: float | None = Field(default=None, ge=0)
    taxi_in_target_minutes: float | None = Field(default=None, gt=0)
    taxi_in_vs_target_pct: float | None = None
    avg_taxi_out_minutes: float | None = Field(default=None, ge=0)
    taxi_out_target_minutes: float | None = Field(default=None, gt=0)
    taxi_out_vs_target_pct: float | None = None
    arrival_bags: int | None = Field(default=None, ge=0)
    arrival_bags_threshold: int | None = Field(default=None, ge=0)
    arrival_bags_vs_threshold_pct: float | None = None
    departure_bags: int | None = Field(default=None, ge=0)
    departure_bags_threshold: int | None = Field(default=None, ge=0)
    departure_bags_vs_threshold_pct: float | None = None
    arrival_pax_forecast: int = Field(ge=0)
    arrival_pax_actual: int = Field(ge=0)
    departure_pax_forecast: int = Field(ge=0)
    departure_pax_actual: int = Field(ge=0)
    today_hourly_arrival_pax_forecast: list[int] = Field(min_length=24, max_length=24)
    today_hourly_arrival_pax_actual: list[int] = Field(min_length=24, max_length=24)
    today_hourly_departure_pax_forecast: list[int] = Field(min_length=24, max_length=24)
    today_hourly_departure_pax_actual: list[int] = Field(min_length=24, max_length=24)


class FlightStatus(Model):
    """A flight's schedule and milestones, sent when it is first known and at each milestone."""

    flight_id: NonEmpty
    direction: Literal["arrival", "departure"]
    sector: Literal["domestic", "international"]
    status: Literal["scheduled", "landed", "on_stand", "departed", "airborne"]
    stand_id: NonEmpty | None = None
    gate_id: NonEmpty | None = None
    lat: float | None = None
    lon: float | None = None
    scheduled_time: AwareDatetime
    estimated_time: AwareDatetime
    landing_time: AwareDatetime | None = None       # ALDT
    in_block_time: AwareDatetime | None = None      # AIBT = ATA
    off_block_time: AwareDatetime | None = None     # AOBT = ATD
    takeoff_time: AwareDatetime | None = None       # ATOT
    delay_minutes: float | None = None              # ATA - STA / ATD - STD, once it happened
    on_time: bool | None = None
    taxi_minutes: float | None = Field(default=None, ge=0)
    pax_forecast: int = Field(ge=0)
    pax_actual: int = Field(ge=0)
    bags: int = Field(ge=0)


PAYLOAD_MODELS = {
    "registry.asset": AssetRegistry,
    "registry.point": PointRegistry,
    "facility.telemetry": FacilityTelemetry,
    "passenger.touchpoint": PassengerTouchpoint,
    "passenger.position": PassengerPosition,
    "passenger.aggregate": PassengerAggregate,
    "passenger.journey_kpi": PassengerJourneyKpi,
    "vehicle.position": VehiclePosition,
    "gse.position": VehiclePosition,
    "curb.occupancy": CurbOccupancy,
    "road.traffic": RoadTraffic,
    "stand.activity": StandActivity,
    "aodb.flight": AODBFlight,
    "rms.assignment": RMSAssignment,
    "acdm.milestone": ACDMMilestone,
    "aircraft.position": AircraftPosition,
    "kpi.checkpoint": KpiCheckpoint,
    "kpi.curbside": KpiCurbside,
    "kpi.overview": KpiOverview,
    "flight.status": FlightStatus,
}

ENTITY_FIELDS = {
    "registry.asset": "asset_id", "registry.point": "point_id",
    "facility.telemetry": "point_id", "passenger.touchpoint": "touchpoint_id",
    "passenger.position": "passenger_id", "passenger.aggregate": "zone_id",
    "passenger.journey_kpi": "kpi_id",
    "vehicle.position": "vehicle_id", "gse.position": "vehicle_id", "curb.occupancy": "facility_id",
    "road.traffic": "road_id", "stand.activity": "stand_id",
    "aodb.flight": "flight_id", "rms.assignment": "assignment_id",
    "acdm.milestone": "flight_id", "aircraft.position": "aircraft_id",
    "kpi.checkpoint": "checkpoint_id", "kpi.curbside": "curbside_id", "kpi.overview": "overview_id",
    "flight.status": "flight_id",
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
