"""Run the terminal simulation live (Kafka or files) or for a fixed time range (files).

One simulation, all as flat Esri records:

  passenger.position     every 5 s    one per passenger (longitude/latitude from local x/y)
  passenger.touchpoint   every minute queue length, waits, throughput, target status
  passenger.aggregate    every minute occupancy and density per zone
  passenger.journey_kpi  every minute terminal-wide journey times and counts
  vehicle / curb / road / gse / stand feeds from the ground simulation
  kpi.checkpoint, kpi.curbside, kpi.overview (every minute) and flight.status (on change): the AE-IT widget
  cards, when the config has a ``kpi`` section (generators/kpi.py)
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .esri import ZoneLocator, to_esri_record
from .generators.passenger import generate_ticks, load_tracking
from .models import EventEnvelope
from .queue_files import LEADING_COLUMNS as TOUCHPOINT_COLUMNS
from .queue_files import _replace, to_csv, to_geojson
from .spatial import LocalFrame
from .streaming import floor_to_interval, publish_paced

KPI_FEEDS = ("kpi.checkpoint", "kpi.curbside", "kpi.overview", "flight.status")
FEEDS = ("passenger.position", "passenger.touchpoint", "passenger.aggregate", "passenger.journey_kpi",
         "vehicle.position", "curb.occupancy", "road.traffic", "gse.position", "stand.activity") + KPI_FEEDS
POSITION_FEEDS = ("passenger.position", "vehicle.position", "gse.position")   # every interval
FEED_GROUPS = {"passengers": FEEDS[:4], "landside": ("vehicle.position", "curb.occupancy", "road.traffic"),
               "airside": ("gse.position", "stand.activity"), "kpis": KPI_FEEDS}


def available_feeds(config, feeds: tuple[str, ...]) -> tuple[str, ...]:
    """Drop the feeds this config cannot produce: vehicle feeds without a ground simulation, KPI cards
    without a ``kpi`` section."""
    if config.ground is None:
        feeds = tuple(f for f in feeds if f in FEED_GROUPS["passengers"] or f in KPI_FEEDS)
    if getattr(config, "kpi", None) is None:
        feeds = tuple(f for f in feeds if f not in KPI_FEEDS)
    return feeds


def select_feeds(groups: str | None) -> tuple[str, ...]:
    """'passengers,landside' -> the event types of those groups (None or 'all' = every feed)."""
    if not groups or groups == "all":
        return FEEDS
    chosen: list[str] = []
    for g in groups.split(","):
        g = g.strip()
        if g not in FEED_GROUPS:
            raise ValueError(f"Unknown feed group {g!r}; use {', '.join(FEED_GROUPS)} or all")
        chosen += [t for t in FEED_GROUPS[g] if t not in chosen]
    return tuple(chosen)
POSITION_TOPIC_TYPE = "passenger.position"
VEHICLE_LEADING = ["vehicle_id", "vehicle_type", "domain", "state", "event_time", "longitude", "latitude", "floor",
                   "elevation_m", "speed_kmh", "heading_deg", "road_name", "facility_id", "stand_id", "flight_id", "task",
                   "passengers_on_board", "dwell_seconds", "overstay"]
LEADING = {
    "vehicle.position": VEHICLE_LEADING, "gse.position": VEHICLE_LEADING,
    "curb.occupancy": ["facility_id", "name", "facility_type", "purpose", "floor", "window_end", "capacity", "occupied",
                       "occupancy_pct", "vehicles_queuing", "arrivals", "departures", "avg_dwell_seconds", "overstays",
                       "status", "longitude", "latitude"],
    "road.traffic": ["road_id", "name", "window_end", "vehicles", "avg_speed_kmh", "speed_limit_kmh", "congestion",
                     "longitude", "latitude"],
    "stand.activity": ["stand_id", "osm_ref", "flow", "window_end", "flight_id", "phase", "status", "on_block", "off_block",
                       "vehicles_present", "tasks_total", "tasks_done", "tasks_late", "longitude", "latitude"],
    "passenger.position": ["passenger_id", "journey", "activity", "touchpoint_id", "flight_id", "gate_id",
                           "minutes_to_departure",
                           "event_time", "longitude", "latitude", "floor", "elevation_m", "indoor_zone_id", "indoor_x", "indoor_y",
                           "speed_mps", "heading_deg", "movement_state"],
    "passenger.touchpoint": TOUCHPOINT_COLUMNS,
    "kpi.checkpoint": ["checkpoint_id", "desk_name", "card", "window_end", "desk_open", "queue_length",
                       "avg_processing_time_seconds", "avg_wait_time_seconds", "happiness_index", "passengers_served",
                       "hour_avg_wait_minutes", "previous_hour_avg_wait_minutes", "wait_threshold_minutes",
                       "longitude", "latitude", "floor"],
    "kpi.curbside": ["curbside_id", "side", "window_end", "people_count", "live_vehicle_count", "vehicles_this_hour",
                     "vehicles_vs_previous_hour_pct", "avg_dwell_seconds", "avg_dwell_vs_previous_hour_pct",
                     "this_hour_private_car", "this_hour_app_cab", "this_hour_taxi", "this_hour_bus",
                     "trolleys_available", "trolleys_total", "longitude", "latitude"],
    "kpi.overview": ["overview_id", "scope", "window_end", "arrival_flights", "arrival_vs_schedule_pct",
                     "departure_flights", "departure_vs_schedule_pct", "stands_occupied", "stands_total", "ota_pct",
                     "otd_pct", "avg_arrival_delay_minutes", "avg_departure_delay_minutes"],
    "flight.status": ["flight_id", "direction", "sector", "status", "event_time", "scheduled_time", "estimated_time",
                      "landing_time", "in_block_time", "off_block_time", "takeoff_time", "delay_minutes", "on_time",
                      "taxi_minutes", "stand_id", "gate_id", "pax_forecast", "pax_actual", "bags",
                      "longitude", "latitude"],
    "passenger.aggregate": ["zone_id", "zone_name", "floor", "window_end", "passenger_count", "density_per_100m2",
                            "area_m2", "people_walking", "people_queuing", "people_in_service", "people_dwelling",
                            "people_at_gate", "people_reclaim", "longitude", "latitude", "elevation_m"],
    "passenger.journey_kpi": ["window_end", "people_in_terminal", "people_departing", "people_arriving",
                              "people_queuing", "kerb_to_airside_p50_minutes", "kerb_to_airside_p90_minutes",
                              "airside_to_boarding_p50_minutes", "arrival_to_exit_p50_minutes",
                              "arrival_to_exit_p90_minutes", "longest_wait_touchpoint_id",
                              "longest_estimated_wait_seconds", "touchpoints_busy", "touchpoints_congested"],
}
STEMS = {"vehicle.position": "vehicle_positions", "gse.position": "gse_positions", "curb.occupancy": "kerb_occupancy",
         "road.traffic": "road_traffic", "stand.activity": "stand_activity",
         "passenger.position": "passenger_positions", "passenger.touchpoint": "passenger_queues",
         "passenger.aggregate": "zone_occupancy", "passenger.journey_kpi": "journey_kpis",
         "kpi.checkpoint": "kpi_checkpoints", "kpi.curbside": "kpi_curbside", "kpi.overview": "kpi_overview",
         "flight.status": "flight_status"}


def columns_for(event_type: str, record: dict[str, Any]) -> list[str]:
    lead = [c for c in LEADING.get(event_type, []) if c in record]
    return lead + [c for c in record if c not in lead]


def position_columns(record: dict[str, Any]) -> list[str]:
    return columns_for("passenger.position", record)


def zone_polygons(layout: dict[str, Any]) -> dict[str, list[list[list[float]]]]:
    """zone_id -> GeoJSON polygon rings in WGS84."""
    frame = LocalFrame.from_dict(layout["frame"])
    out: dict[str, list] = {}
    for zone in layout["zones"]:
        rings = []
        for polygon in zone["polygons"]:
            points = [list(frame.to_wgs84(x, y)) for x, y in polygon]
            rings.append([points + points[:1]])
        out[zone["zone_id"]] = rings
    return out


def zones_geojson(records: list[dict[str, Any]], polygons: dict[str, list]) -> dict[str, Any]:
    """Zone occupancy as polygons (one MultiPolygon per zone), ready for a choropleth."""
    features = []
    for record in records:
        rings = polygons.get(record["zone_id"])
        if not rings:
            continue
        props = {k: v for k, v in record.items() if k != "cells"}
        features.append({"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": rings},
                         "properties": props})
    return {"type": "FeatureCollection", "features": features}


def _window(config, start: datetime | None, minutes: float | None, live: bool) -> tuple[datetime, datetime]:
    begin = floor_to_interval(start or (datetime.now(timezone.utc) if live else config.start_time),
                              config.interval_seconds)
    if minutes is None:
        end = begin + (timedelta(days=3650) if live else config.end_time - config.start_time)
    else:
        end = begin + timedelta(minutes=minutes)
    return begin, end


def run_tracking_stream(config_path: str | Path, publisher: Any, *, minutes: float | None = None,
                        speed: float = 1.0, start: datetime | None = None, create_topics: bool = True,
                        scenario: str | None = None, interval: int | None = None, feeds: tuple[str, ...] = FEEDS,
                        clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
                        log: Callable[[str], None] = print) -> dict[str, int]:
    """Publish one batch per interval, in real time (``speed`` compresses time).

    Paced by the wall clock: if the machine sleeps, the feed catches up on wake
    (the missed batches go out at once) instead of running behind from then on.
    """
    if speed <= 0:
        raise ValueError("speed must be > 0")
    config, layout, inventory, _ = load_tracking(config_path, scenario)
    if interval:
        if interval <= 0 or 60 % interval:
            raise ValueError("interval must divide 60 (5, 10, 15, 20, 30 or 60 seconds)")
        targets = config._targets
        config = config.model_copy(update={"interval_seconds": interval})
        config._targets = targets
    if scenario:
        log(f"Scenario: {scenario} - {config.scenarios[scenario].description}")
    begin, end = _window(config, start, minutes, live=True)
    log(f"Terminal simulation: positions every {config.interval_seconds}s, queues/zones/journeys every minute, "
        f"speed x{speed:g}; warming up from local midnight first")
    feeds = available_feeds(config, feeds)
    log("Feeds: " + ", ".join(feeds))
    if create_topics and hasattr(publisher, "missing_topics"):
        # the KPI topics are new: until they exist, send the other feeds rather than stop everything
        kpi = {t: publisher.topic_for_type(t) for t in feeds if t in KPI_FEEDS}
        missing = set(publisher.missing_topics(list(kpi.values()))) if kpi else set()
        if missing:
            log("KPI topics not created yet, skipping them for now: " + ", ".join(sorted(missing)))
            feeds = tuple(f for f in feeds if kpi.get(f) not in missing)
    if create_topics:
        topics = [publisher.topic_for_type(t) for t in feeds]
        created = publisher.ensure_topics(topics)
        log("Topics ready: " + ", ".join(topics) + (f" (created {len(created)})" if created else ""))
    wanted = set(feeds)
    ticks = ((when, [e for e in records if e.event_type in wanted])
             for when, records in generate_ticks(config, layout, inventory, begin, end))
    publish_paced(ticks, publisher, origin=begin, speed=speed, clock=clock, sleep=sleep, log=log,
                  every=max(1, 60 // config.interval_seconds))
    return publisher.stats()


class TwinFileWriter:
    """Duck-types the publisher calls ``publish_paced`` makes.

    Each flush rewrites the "latest" files: passenger_positions_latest.csv/.geojson every
    interval, and passenger_queues_latest, zone_occupancy_latest (polygons) and
    journey_kpis_latest whenever a minute's records arrive. With ``history`` the
    per-minute feeds (and, if asked, positions) are also appended to *_history.csv.
    """

    def __init__(self, out_dir: str | Path, locator: ZoneLocator, layout: dict[str, Any] | None = None,
                 history: bool = False, position_history: bool = False):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.locator = locator
        self.polygons = zone_polygons(layout) if layout else {}
        self.history, self.position_history = history, position_history
        self._batch: dict[str, list[dict[str, Any]]] = {}
        self._names: dict[str, list[str]] = {}
        self.queued = self.delivered = 0

    def publish(self, event: EventEnvelope) -> None:
        self._batch.setdefault(event.event_type, []).append(to_esri_record(event, self.locator))
        self.queued += 1

    def _append_history(self, event_type: str, records: list[dict[str, Any]], names: list[str]) -> None:
        path = self.out_dir / f"{STEMS[event_type]}_history.csv"
        first = event_type not in self._names
        names = self._names.setdefault(event_type, names)
        with path.open("w" if first else "a", encoding="utf-8", newline="") as handle:
            handle.write(to_csv(records, names, header=first))

    def flush(self, timeout: float = 0.0) -> None:
        for event_type in POSITION_FEEDS:
            positions = self._batch.get(event_type, [])
            if not positions and event_type != "passenger.position":
                continue
            stem = STEMS[event_type]
            names = columns_for(event_type, positions[0]) if positions else LEADING[event_type]
            _replace(self.out_dir / f"{stem}_latest.csv", to_csv(positions, names))
            _replace(self.out_dir / f"{stem}_latest.geojson", json.dumps(to_geojson(positions, names)))
            if self.position_history and positions:
                self._append_history(event_type, positions, names)
        for event_type in FEEDS:
            if event_type in POSITION_FEEDS:
                continue
            records = self._batch.get(event_type)
            if not records:
                continue
            stem = STEMS[event_type]
            names = columns_for(event_type, records[0])
            if event_type == "passenger.aggregate":
                _replace(self.out_dir / f"{stem}_latest.geojson", json.dumps(zones_geojson(records, self.polygons)))
            elif event_type in ("passenger.touchpoint", "curb.occupancy", "road.traffic", "stand.activity",
                                "kpi.checkpoint", "kpi.curbside", "kpi.overview", "flight.status"):
                _replace(self.out_dir / f"{stem}_latest.geojson", json.dumps(to_geojson(records, names)))
            _replace(self.out_dir / f"{stem}_latest.csv", to_csv(records, names))
            if event_type == "passenger.journey_kpi":
                _replace(self.out_dir / f"{stem}_latest.json", json.dumps(records[-1], indent=1))
            if self.history:
                self._append_history(event_type, records, names)
        self.delivered += sum(len(v) for v in self._batch.values())
        self._batch = {}

    def stats(self) -> dict[str, int]:
        return {"queued": self.queued, "delivered": self.delivered, "failed": 0}


# The live position writer used to be positions-only; keep the old name working.
PositionFileWriter = TwinFileWriter


def write_tracking_range(config_path: str | Path, locator: ZoneLocator, out_dir: str | Path, *,
                         start: datetime | None = None, minutes: float | None = None,
                         scenario: str | None = None, feeds: tuple[str, ...] = FEEDS,
                         interval: int | None = None) -> tuple[list[Path], int]:
    """Simulate a time range and write the chosen feeds: a CSV and a JSONL (one Kafka message per line) each."""
    config, layout, inventory, _ = load_tracking(config_path, scenario)
    if interval:
        if interval <= 0 or 60 % interval:
            raise ValueError("interval must divide 60 (5, 10, 15, 20, 30 or 60 seconds)")
        private = (config._targets, config._ground_layout)
        config = config.model_copy(update={"interval_seconds": interval})
        config._targets, config._ground_layout = private
    begin, end = _window(config, start, minutes, live=False)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    tag = begin.strftime("%Y%m%dT%H%MZ")
    feeds = available_feeds(config, tuple(t for t in FEEDS if t in feeds))
    paths = {t: out / f"{STEMS[t]}_{tag}.csv" for t in feeds}
    jsonl_paths = {t: out / f"{STEMS[t]}_{tag}.jsonl" for t in feeds}
    handles = {t: p.open("w", encoding="utf-8", newline="") for t, p in paths.items()}
    jsonl = {t: p.open("w", encoding="utf-8", newline="\n") for t, p in jsonl_paths.items()}
    names: dict[str, list[str]] = {}
    count = 0
    try:
        for _, events in generate_ticks(config, layout, inventory, begin, end):
            grouped: dict[str, list[dict[str, Any]]] = {}
            for event in events:
                if event.event_type in handles:
                    grouped.setdefault(event.event_type, []).append(to_esri_record(event, locator))
            for event_type, records in grouped.items():
                if event_type not in names:
                    names[event_type] = columns_for(event_type, records[0])
                    handles[event_type].write(to_csv([], names[event_type]))
                handles[event_type].write(to_csv(records, names[event_type], header=False))
                for record in records:
                    jsonl[event_type].write(json.dumps(record, separators=(",", ":")) + "\n")
                count += len(records)
    finally:
        for handle in [*handles.values(), *jsonl.values()]:
            handle.close()
    return [p for t in feeds for p in (paths[t], jsonl_paths[t])], count
