"""Synthetic passenger tracking inside RGIA Terminal 1 - one simulation, four feeds.

Agent-based and deterministic for a given seed and start time:

* A flight schedule (with an hourly profile) creates passengers. Departures arrive
  at the kerb ahead of their flight; arrivals come off buses.
* Each passenger follows a journey plan: walk the terminal's walkway graph, queue
  at touchpoints (FIFO, open lanes and service times from config), dwell airside,
  wait at the gate and board, or reclaim bags and exit.

Everything published comes from those same passengers, so the feeds agree:

* ``passenger.position``     every interval (5 s): one record per passenger.
* ``passenger.touchpoint``   every minute: queue length, waits, throughput and target
                             status per touchpoint, measured from the passengers served.
* ``passenger.aggregate``    every minute: occupancy and density per zone, with 5 m cells.
* ``passenger.journey_kpi``  every minute: terminal-wide journey times and counts.

The simulation runs silently from local midnight (so "throughput since midnight"
is complete), with coarse steps until the last 30 minutes before the first record.
Positions are only as good as config/terminal_layout.json (real outlines from
OpenStreetMap, schematic interior). All identifiers are synthetic.
"""

from __future__ import annotations

import heapq
import json
import math
import random
from collections import Counter, deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, PrivateAttr

from ..models import EventEnvelope, Model, NonEmpty, PassengerAggregate, PassengerJourneyKpi, PassengerTouchpoint
from ..service_targets import ServiceTargets, nearest_rank, queue_attributes
from ..spatial import WalkGraph, point_in_polygon
from ..touchpoint_config import TouchpointInventory

DEPARTURES = ("domestic_departure", "international_departure", "ga_departure")
ARRIVALS = ("domestic_arrival", "international_arrival", "ga_arrival")
FLIGHT_PREFIX = {"domestic_departure": "DD", "international_departure": "ID", "ga_departure": "GD",
                 "domestic_arrival": "DA", "international_arrival": "IA", "ga_arrival": "GA"}
# Completing one of these puts a departing passenger airside.
AIRSIDE_TOUCHPOINTS = {"SYN-HYD-SECURITY-DOM", "SYN-HYD-SECURITY-PRM", "SYN-HYD-EMIGRATION",
                       "SYN-HYD-EMIGRATION-EGATE", "SYN-HYD-EMIGRATION-PRM", "SYN-HYD-GA-SECURITY"}
ACTIVITIES = ("walking", "queuing", "in_service", "dwelling", "at_gate", "reclaim")
KPI_ID = "HYD-T1-JOURNEYS"
SOURCE = "synthetic-passenger-tracker"


class FlightStream(Model):
    per_hour: float = Field(ge=0)
    passengers: tuple[int, int]
    arrive_before_departure_minutes: tuple[float, float] | None = None


class Boarding(Model):
    opens_before_departure_minutes: float = Field(gt=0)
    closes_before_departure_minutes: float = Field(ge=0)
    channels_per_gate: int = Field(ge=1)
    service_seconds: tuple[float, float]
    gate_turnaround_minutes: float = Field(ge=0)


class ArrivalBuses(Model):
    first_bus_after_arrival_minutes: float = Field(ge=0)
    minutes_between_buses: float = Field(gt=0)
    passengers_per_bus: int = Field(ge=1)


class Scenario(Model):
    """Named overrides on top of the base config (e.g. ``peak``: busy at any hour)."""

    description: str = ""
    flight_multiplier: float = Field(default=1.0, gt=0)
    hourly_profile: list[float] | None = Field(default=None, min_length=24, max_length=24)
    open_channels: dict[str, int] = Field(default_factory=dict)


class TrackingConfig(Model):
    note: str = Field(default="", alias="_note")
    start_time: AwareDatetime
    end_time: AwareDatetime
    interval_seconds: int = Field(gt=0)
    warmup_minutes: float = Field(ge=0)
    warmup_from_local_midnight: bool = True
    warmup_step_seconds: int = Field(default=30, gt=0)
    timezone: NonEmpty = "Asia/Kolkata"
    seed: int
    layout_file: NonEmpty
    inventory_file: NonEmpty
    service_targets_file: NonEmpty | None = None
    output_directory: NonEmpty
    hourly_profile: list[float] | None = Field(default=None, min_length=24, max_length=24)
    flights: dict[str, FlightStream]
    boarding: Boarding
    arrival_buses: ArrivalBuses
    open_channels: dict[str, int] = Field(default_factory=dict)
    queue_feed_types: list[str] | None = None
    kpi_rolling_minutes: float = Field(default=15, gt=0)
    shares: dict[str, float]
    walking_speed_mps: tuple[float, float]
    reclaim_wait_minutes: tuple[float, float]
    arrivals_hall_dwell_minutes: tuple[float, float]
    scenarios: dict[str, Scenario] = Field(default_factory=dict)
    _targets: ServiceTargets | None = PrivateAttr(default=None)

    def with_scenario(self, name: str | None) -> "TrackingConfig":
        """The config with a named scenario applied (None or "" = unchanged)."""
        if not name:
            return self
        if name not in self.scenarios:
            raise ValueError(f"Unknown scenario {name!r}; the config has: {', '.join(sorted(self.scenarios)) or 'none'}")
        sc = self.scenarios[name]
        flights = {k: v.model_copy(update={"per_hour": v.per_hour * sc.flight_multiplier}) for k, v in self.flights.items()}
        updated = self.model_copy(update={"flights": flights, "open_channels": {**self.open_channels, **sc.open_channels},
                                          "hourly_profile": sc.hourly_profile or self.hourly_profile})
        updated._targets = self._targets
        return updated

    model_config = Model.model_config | {"populate_by_name": True}


def load_tracking(path: str | Path, scenario: str | None = None,
                  ) -> tuple[TrackingConfig, dict[str, Any], TouchpointInventory, Path]:
    path = Path(path).resolve()
    config = TrackingConfig.model_validate_json(path.read_text(encoding="utf-8"))
    layout = json.loads((path.parent / config.layout_file).read_text(encoding="utf-8"))
    inventory = TouchpointInventory.model_validate_json((path.parent / config.inventory_file).read_text(encoding="utf-8"))
    if config.service_targets_file:
        config._targets = ServiceTargets.load(path.parent / config.service_targets_file)
    return config.with_scenario(scenario), layout, inventory, (path.parent / config.output_directory).resolve()


def _targets(config: TrackingConfig) -> ServiceTargets | None:
    return config._targets


@dataclass
class Flight:
    flight_id: str
    journey: str
    time: float  # departure (STD) or arrival (STA), epoch seconds
    gate_id: str | None
    passengers: int


@dataclass
class Passenger:
    pax_id: str
    pnr_id: str
    journey: str
    flight: Flight
    plan: list[tuple]
    speed: float
    x: float
    y: float
    floor: str
    spawned_at: float = 0.0
    airside_at: float | None = None
    joined: float = 0.0
    step: int = 0
    activity: str = "walking"
    touchpoint_id: str | None = None
    zone: str = ""
    path: list[tuple[float, float, str]] = field(default_factory=list)
    until: float = 0.0
    moved: float = 0.0
    heading: float = 0.0
    sequence: int = 0
    done: bool = False


@dataclass
class TouchpointStats:
    processed: int = 0
    wait_sum: float = 0.0
    service_sum: float = 0.0
    since_midnight: int = 0
    recent_starts: deque = field(default_factory=deque)  # (service start, wait) for the rolling p95


class Queue:
    """FIFO queue with parallel service channels, served event by event within each step."""

    def __init__(self, queue_id: str, x: float, y: float, floor: str, direction: tuple[float, float],
                 width: float, channels: int, service: tuple[float, float], touchpoint_id: str):
        self.queue_id, self.x, self.y, self.floor = queue_id, x, y, floor
        norm = math.hypot(*direction) or 1.0
        self.dx, self.dy = direction[0] / norm, direction[1] / norm
        self.px, self.py = self.dy, -self.dx  # perpendicular, along the service line
        self.width, self.channels, self.service = width, channels, service
        self.touchpoint_id = touchpoint_id
        self.waiting: deque[Passenger] = deque()
        self.serving: dict[int, tuple[float, float, float, Passenger]] = {}  # channel -> (end, start, wait, pax)
        self.free_at = [-math.inf] * channels
        self.per_row = max(2, int(min(width, 30) / 0.75))

    def slot(self, index: int) -> tuple[float, float]:
        row, col = divmod(index, self.per_row)
        if row % 2:
            col = self.per_row - 1 - col  # serpentine
        along = 2.5 + row * 0.9
        lateral = (col - (self.per_row - 1) / 2) * 0.75
        return self.x + self.dx * along + self.px * lateral, self.y + self.dy * along + self.py * lateral

    def channel_point(self, channel: int) -> tuple[float, float]:
        lateral = (channel - (self.channels - 1) / 2) * (self.width / self.channels)
        return self.x + self.px * lateral, self.y + self.py * lateral

    def exit_point(self) -> tuple[float, float]:
        return self.x - self.dx * 3, self.y - self.dy * 3

    def update(self, now: float, rng: random.Random, stats: TouchpointStats) -> list[tuple[Passenger, float]]:
        """Serve up to ``now``; returns (passenger, service end) for everyone who finished."""
        done = []
        progressed = True
        while progressed:
            progressed = False
            for channel in range(self.channels):
                current = self.serving.get(channel)
                if current is not None and current[0] <= now:
                    end, start, wait, pax = current
                    del self.serving[channel]
                    self.free_at[channel] = end
                    stats.processed += 1
                    stats.wait_sum += wait
                    stats.service_sum += end - start
                    stats.since_midnight += 1
                    done.append((pax, end))
                    progressed = True
                if channel not in self.serving and self.waiting:
                    pax = self.waiting[0]
                    start = max(self.free_at[channel], pax.joined)
                    if start <= now:
                        self.waiting.popleft()
                        wait = start - pax.joined
                        self.serving[channel] = (start + rng.uniform(*self.service), start, wait, pax)
                        stats.recent_starts.append((start, wait))
                        pax.activity = "in_service"
                        pax.x, pax.y = self.channel_point(channel)
                        pax.floor = self.floor
                        progressed = True
        for index, pax in enumerate(self.waiting):
            pax.x, pax.y = self.slot(index)
            pax.floor = self.floor
        return done


def _area(polygon: list[list[float]]) -> float:
    return abs(sum(x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in zip(polygon, polygon[1:] + polygon[:1]))) / 2


class TerminalSimulation:
    def __init__(self, config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
                 start: datetime, end: datetime):
        self.config, self.layout, self.inventory = config, layout, inventory
        # Two streams: the schedule (flights, passengers, plans) and motion (walk targets,
        # service times). Neither depends on the end time, so a longer run of the same
        # start replays the same tracks.
        self.plan_rng = random.Random(f"{config.seed}:{start.isoformat()}:plan")
        self.rng = random.Random(f"{config.seed}:{start.isoformat()}:motion")
        self.local = ZoneInfo(config.timezone)
        self.graph = WalkGraph.from_layout(layout)
        self.frame_id = layout["frame"]["frame_id"]
        self.floor_z = {name: f["z"] for name, f in layout["floors"].items()}
        self.zones_by_floor: dict[str, list[tuple[str, list]]] = {}
        self.zone_info: dict[str, dict[str, Any]] = {}
        for zone in layout["zones"]:
            self.zones_by_floor.setdefault(zone["floor"], []).append((zone["zone_id"], zone["polygons"]))
            self.zone_info[zone["zone_id"]] = {"name": zone["name"], "floor": zone["floor"],
                                               "area": round(sum(_area(p) for p in zone["polygons"]), 1)}
        self.areas = layout["areas"]
        self.gates = {g["gate_id"]: g for g in layout["gates"]}
        self.gate_pools = {flow: sorted(g["gate_id"] for g in layout["gates"] if g["flow"] == flow)
                           for flow in ("dom", "int")}
        self.touchpoints = {tp.touchpoint_id: tp for tp in inventory.touchpoints}
        self.queues: dict[str, Queue] = {}
        for tp_id, spot in layout["touchpoints"].items():
            tp = self.touchpoints.get(tp_id)
            if tp is None:
                continue
            sim = tp.simulation
            channels = config.open_channels.get(tp_id, sim.service_channels)
            self.queues[tp_id] = Queue(tp_id, spot["x"], spot["y"], spot["floor"], tuple(spot["queue_direction"]),
                                       spot["service_width_m"], channels,
                                       (sim.service_time_min_seconds, sim.service_time_max_seconds), tp_id)
        b = config.boarding
        board_tp = {"dom": "SYN-HYD-BOARDING-DOM", "int": "SYN-HYD-BOARDING-INT"}
        for gate in layout["gates"]:
            # the gate's walkway link points into the pier: queues and the holding area go that way
            node = self.graph.nodes[self.graph.adjacency[gate["gate_id"]][0][0]]
            inward = (node.x - gate["x"], node.y - gate["y"])
            self.queues[gate["gate_id"]] = Queue(gate["gate_id"], gate["x"], gate["y"], gate["floor"],
                                                 inward if any(inward) else (0, 1), 6, b.channels_per_gate,
                                                 b.service_seconds, board_tp[gate["flow"]])
        self.stats: dict[str, TouchpointStats] = {q.touchpoint_id: TouchpointStats() for q in self.queues.values()}
        types = set(config.queue_feed_types) if config.queue_feed_types else None
        self.feed_touchpoints = sorted(
            tp_id for tp_id, tp in self.touchpoints.items()
            if tp.enabled and (types is None or tp.touchpoint_type in types) and tp_id in self.stats)
        self.start = start.astimezone(timezone.utc)
        self.end = end.astimezone(timezone.utc)
        warm = self.start - timedelta(minutes=config.warmup_minutes)
        if config.warmup_from_local_midnight:
            midnight = self.start.astimezone(self.local).replace(hour=0, minute=0, second=0, microsecond=0)
            warm = min(warm, midnight.astimezone(timezone.utc))
        self.warm_start = warm
        self.day = self.warm_start.astimezone(self.local).date()
        self.active: list[Passenger] = []
        self.counter = 0
        self.sequences: Counter[str] = Counter()
        rolling = config.kpi_rolling_minutes * 60
        self.rolling = rolling
        self.ev_airside: deque[tuple[float, float]] = deque()   # (time, kerb->airside minutes)
        self.ev_boarded: deque[tuple[float, float]] = deque()   # (time, airside->boarding minutes)
        self.ev_exited: deque[tuple[float, float]] = deque()    # (time, arrival->exit minutes)
        self._init_schedule()

    # -- schedule ------------------------------------------------------------
    # Flights are generated lazily, a little ahead of simulated time, so a live run
    # with no end date costs the same as a short one. An hourly profile (local time)
    # shapes the rate by thinning.
    def _init_schedule(self) -> None:
        self.spawns: list[tuple[float, int, Passenger]] = []  # heap of (time, order, passenger)
        self._order = 0
        self._gate_free: dict[str, float] = {}
        self._streams: dict[str, dict[str, float]] = {}
        self._profile = self.config.hourly_profile or [1.0] * 24
        self._peak = max(self._profile) or 1.0
        for journey in DEPARTURES + ARRIVALS:
            stream = self.config.flights.get(journey)
            if stream is None or stream.per_hour <= 0:
                continue
            lead = (stream.arrive_before_departure_minutes or (0, 0))[1] * 60 if journey in DEPARTURES else 0
            self._streams[journey] = {"next": self.warm_start.timestamp() - 3600, "number": 0, "lead": lead}
            self._advance(journey)

    def _advance(self, journey: str) -> None:
        state, stream = self._streams[journey], self.config.flights[journey]
        rate = stream.per_hour * self._peak / 3600
        while True:
            state["next"] += self.plan_rng.expovariate(rate)
            hour = datetime.fromtimestamp(state["next"], self.local).hour
            if self.plan_rng.random() * self._peak <= self._profile[hour]:
                return

    def _extend_schedule(self, now: float) -> None:
        cfg, rng = self.config, self.plan_rng
        t0, t1 = self.warm_start.timestamp(), self.end.timestamp()
        for journey, state in self._streams.items():
            stream = cfg.flights[journey]
            # departures: generate far enough ahead that every passenger due by ``now`` exists
            while state["next"] <= now + state["lead"]:
                t = state["next"]
                self._advance(journey)
                state["number"] += 1
                gate = None
                if journey in ("domestic_departure", "international_departure"):
                    pool = self.gate_pools["dom" if journey == "domestic_departure" else "int"]
                    free = [g for g in pool if self._gate_free.get(g, -math.inf) <= t]
                    gate = rng.choice(free or pool)
                    self._gate_free[gate] = t + cfg.boarding.gate_turnaround_minutes * 60
                flight = Flight(f"SYN-{FLIGHT_PREFIX[journey]}{int(state['number']):04d}", journey, t, gate,
                                rng.randint(*stream.passengers))
                for k in range(flight.passengers):
                    if journey in DEPARTURES:
                        at = t - rng.uniform(*stream.arrive_before_departure_minutes) * 60
                    else:
                        buses = cfg.arrival_buses
                        at = t + (buses.first_bus_after_arrival_minutes
                                  + (k // buses.passengers_per_bus) * buses.minutes_between_buses) * 60 \
                            + rng.uniform(0, 20)
                    pax = self._passenger(flight, at)  # always drawn, so the stream never depends on t1
                    if t0 <= at < t1:
                        self._order += 1
                        heapq.heappush(self.spawns, (at, self._order, pax))

    def _point_in(self, area: str, rng: random.Random | None = None) -> tuple[float, float, str]:
        rng = rng or self.rng
        spec = self.areas[area]
        polygon = rng.choice(spec["polygons"])
        xs, ys = [p[0] for p in polygon], [p[1] for p in polygon]
        for _ in range(50):
            x, y = rng.uniform(min(xs), max(xs)), rng.uniform(min(ys), max(ys))
            if point_in_polygon(x, y, polygon):
                return x, y, spec["floor"]
        return sum(xs) / len(xs), sum(ys) / len(ys), spec["floor"]

    def _passenger(self, flight: Flight, at: float) -> Passenger:
        cfg, rng, s = self.config, self.plan_rng, self.config.shares
        journey = flight.journey
        prm = rng.random() < s["prm"]
        plan: list[tuple] = []
        start_area = {"domestic_departure": "kerb_departures", "international_departure": "kerb_departures",
                      "ga_departure": "ga_landside", "domestic_arrival": "dom_bus_arrival",
                      "international_arrival": "int_bus_arrival", "ga_arrival": "ga_airside"}[journey]
        if journey in ("domestic_departure", "international_departure"):
            domestic = journey == "domestic_departure"
            city = domestic and rng.random() < s["domestic_city_checkin"]
            if city:
                start_area = "city_parking"
                plan.append(("tp", "SYN-HYD-CITY-CHECKIN"))
            plan.append(("tp", "SYN-HYD-PRM-ENTRY" if prm else
                         "SYN-HYD-DIGIYATRA-ENTRY" if rng.random() < s["digiyatra_entry"] else "SYN-HYD-ENTRY"))
            if not city:
                r = rng.random()
                web = s["domestic_web_checkin_no_bags" if domestic else "international_web_checkin_no_bags"]
                kiosk = s["domestic_kiosk" if domestic else "international_kiosk"]
                suffix = "DOM" if domestic else "INT"
                if r < kiosk:
                    plan += [("tp", f"SYN-HYD-KIOSK-{suffix}"), ("tp", f"SYN-HYD-BAGDROP-{suffix}")]
                elif r >= kiosk + web:
                    plan.append(("tp", f"SYN-HYD-CHECKIN-{suffix}"))
            if domestic:
                plan.append(("tp", "SYN-HYD-SECURITY-PRM" if prm else "SYN-HYD-SECURITY-DOM"))
            else:
                plan.append(("tp", "SYN-HYD-SECURITY-INT"))
                plan.append(("tp", "SYN-HYD-EMIGRATION-PRM" if prm else
                             "SYN-HYD-EMIGRATION-EGATE" if rng.random() < s["emigration_egate"] else
                             "SYN-HYD-EMIGRATION"))
            b = cfg.boarding
            opens = flight.time - b.opens_before_departure_minutes * 60
            closes = flight.time - b.closes_before_departure_minutes * 60
            plan.append(("area", "dom_dwell" if domestic else "int_dwell"))
            plan.append(("dwell_until", opens - rng.uniform(0, 20) * 60, "dwelling", None))
            plan.append(("gate", flight.gate_id, rng.uniform(opens, closes)))
        elif journey == "ga_departure":
            plan += [("tp", "SYN-HYD-GA-CHECK-IN"), ("tp", "SYN-HYD-GA-SECURITY"), ("area", "ga_airside"),
                     ("end", "boarded")]
        elif journey == "ga_arrival":
            plan += [("tp", "SYN-HYD-GA-IMMIGRATION"), ("tp", "SYN-HYD-GA-CUSTOMS"), ("area", "ga_landside"),
                     ("end", "exited")]
        else:
            domestic = journey == "domestic_arrival"
            if not domestic:
                plan.append(("tp", "SYN-HYD-IMMIGRATION-EGATE" if rng.random() < s["immigration_egate"]
                             else "SYN-HYD-IMMIGRATION"))
            plan.append(("area", "dom_reclaim" if domestic else "int_reclaim"))
            if rng.random() < s["arrival_checked_bags"]:
                plan.append(("dwell_for", rng.uniform(*cfg.reclaim_wait_minutes) * 60, "reclaim",
                             "SYN-HYD-RECLAIM-DOM" if domestic else "SYN-HYD-RECLAIM-INT"))
            plan += [("area", "arrivals_hall"),
                     ("dwell_for", rng.uniform(*cfg.arrivals_hall_dwell_minutes) * 60, "dwelling", None),
                     ("area", "arrivals_exit"), ("end", "exited")]
        x, y, floor = self._point_in(start_area, rng)
        self.counter += 1
        return Passenger(f"SYN-PAX-{self.counter:06d}", f"SYN-PNR-{self.counter:06d}", journey, flight, plan,
                         rng.uniform(*cfg.walking_speed_mps), x, y, floor, spawned_at=at)

    # -- movement and state --------------------------------------------------
    def _walk_to(self, pax: Passenger, x: float, y: float, floor: str) -> None:
        pax.path = self.graph.path_between(pax.x, pax.y, pax.floor, x, y, floor)
        pax.activity = "walking"

    def _finish(self, pax: Passenger, activity: str, now: float) -> None:
        pax.done, pax.activity = True, activity
        if activity == "boarded" and pax.airside_at is not None:
            self.ev_boarded.append((now, (now - pax.airside_at) / 60))
        elif activity == "exited" and pax.journey in ARRIVALS:
            self.ev_exited.append((now, (now - pax.spawned_at) / 60))

    def _next_step(self, pax: Passenger, now: float) -> None:
        """Start the passenger's next plan step (called when the previous one is complete)."""
        pax.touchpoint_id = None
        if pax.step >= len(pax.plan):
            self._finish(pax, "exited", now)
            return
        step = pax.plan[pax.step]
        kind = step[0]
        if kind == "tp":
            queue = self.queues[step[1]]
            pax.touchpoint_id = step[1]
            self._walk_to(pax, *queue.slot(len(queue.waiting)), queue.floor)
        elif kind == "area":
            self._walk_to(pax, *self._point_in(step[1]))
        elif kind == "gate":
            gate = self.gates[step[1]]
            angle = self.rng.uniform(0, 2 * math.pi)
            queue = self.queues[step[1]]
            gx, gy = queue.x + queue.dx * 8, queue.y + queue.dy * 8  # holding area just inside the gate
            self._walk_to(pax, gx + 5 * math.cos(angle), gy + 5 * math.sin(angle), gate["floor"])
        elif kind in ("dwell_until", "dwell_for"):
            pax.until = step[1] if kind == "dwell_until" else now + step[1]
            pax.activity, pax.touchpoint_id = step[2], step[3]
            if pax.until <= now:
                pax.step += 1
                self._next_step(pax, now)
        elif kind == "end":
            self._finish(pax, step[1], now)

    def _join(self, pax: Passenger, queue: Queue, now: float) -> None:
        queue.waiting.append(pax)
        pax.joined = now
        pax.activity, pax.touchpoint_id = "queuing", queue.touchpoint_id

    def _arrived(self, pax: Passenger, now: float) -> None:
        step = pax.plan[pax.step]
        kind = step[0]
        if kind == "tp":
            self._join(pax, self.queues[step[1]], now)
        elif kind == "gate":
            pax.activity, pax.until = "at_gate", step[2]
        else:
            pax.step += 1
            self._next_step(pax, now)

    def _move(self, pax: Passenger, dt: float) -> None:
        budget = pax.speed * dt
        moved = 0.0
        while pax.path and budget > 0:
            tx, ty, tfloor = pax.path[0]
            dx, dy = tx - pax.x, ty - pax.y
            dist = math.hypot(dx, dy)
            if dist > 1e-9:
                pax.heading = (90 - math.degrees(math.atan2(dy, dx))) % 360
            if dist <= budget:
                pax.x, pax.y, pax.floor = tx, ty, tfloor
                budget -= dist
                moved += dist
                pax.path.pop(0)
            else:
                pax.x += dx / dist * budget
                pax.y += dy / dist * budget
                moved += budget
                budget = 0
        pax.moved = moved

    def tick(self, now: float, dt: float) -> list[Passenger]:
        """Advance to ``now``; returns everyone to report (including tracks that just ended)."""
        day = datetime.fromtimestamp(now, self.local).date()
        if day != self.day:  # local midnight: "since midnight" totals restart
            self.day = day
            for stats in self.stats.values():
                stats.since_midnight = 0
        self._extend_schedule(now)
        while self.spawns and self.spawns[0][0] <= now:
            _, _, pax = heapq.heappop(self.spawns)
            self.active.append(pax)
            self._next_step(pax, now)
        for pax in self.active:
            pax.moved = 0.0
            if pax.done:
                continue
            if pax.activity == "walking":
                self._move(pax, dt)
                if not pax.path:
                    self._arrived(pax, now)
            elif pax.activity in ("dwelling", "reclaim") and now >= pax.until:
                pax.step += 1
                self._next_step(pax, now)
            elif pax.activity == "at_gate" and now >= pax.until:
                self._join(pax, self.queues[pax.plan[pax.step][1]], now)
        for queue in self.queues.values():
            for pax, ended in queue.update(now, self.rng, self.stats[queue.touchpoint_id]):
                if queue.queue_id in self.gates:
                    self._finish(pax, "boarded", ended)
                    continue
                if queue.touchpoint_id in AIRSIDE_TOUCHPOINTS and pax.journey in DEPARTURES:
                    pax.airside_at = ended
                    self.ev_airside.append((ended, (ended - pax.spawned_at) / 60))
                pax.x, pax.y = queue.exit_point()
                pax.step += 1
                self._next_step(pax, now)
        report = self.active
        self.active = [p for p in self.active if not p.done]
        return report

    # -- records -------------------------------------------------------------
    def zone_of(self, x: float, y: float, floor: str) -> str:
        for zone_id, polygons in self.zones_by_floor.get(floor, []):
            if any(point_in_polygon(x, y, p) for p in polygons):
                return zone_id
        return "SYN-HYD-ZONE-OUTSIDE"

    def _envelope(self, event_type: str, entity: str, when: datetime, payload: dict[str, Any]) -> EventEnvelope:
        sequence = self.sequences[entity]
        self.sequences[entity] += 1
        return EventEnvelope.model_construct(
            event_id=uuid5(NAMESPACE_URL, f"airport-eye:{event_type}:{self.start.isoformat()}:{entity}:{sequence}"),
            event_type=event_type, schema_version="1.0.0", airport_id=self.inventory.airport_id,
            terminal_id="HYD-MAIN", source_system=SOURCE, entity_id=entity,
            event_time=when, ingestion_time=when, sequence_no=sequence, payload=payload)

    def record(self, pax: Passenger, when: datetime) -> EventEnvelope:
        speed = round(pax.moved / self.config.interval_seconds, 2)
        pax.zone = self.zone_of(pax.x, pax.y, pax.floor)
        payload = {
            "passenger_id": pax.pax_id, "pnr_id": pax.pnr_id, "flight_id": pax.flight.flight_id,
            "indoor": {"coordinate_frame_id": self.frame_id, "x": round(pax.x, 2), "y": round(pax.y, 2),
                       "z": self.floor_z.get(pax.floor, 0.0), "floor": pax.floor, "zone_id": pax.zone,
                       "grid_id": f"{pax.floor}-{math.floor(pax.x / 5)}-{math.floor(pax.y / 5)}"},
            "speed_mps": speed, "heading_deg": round(pax.heading, 1) % 360,
            "movement_state": "walking" if speed > 0.05 else "stationary",
            "journey": pax.journey, "activity": pax.activity, "touchpoint_id": pax.touchpoint_id,
            "gate_id": pax.flight.gate_id,
            "minutes_to_departure": round((pax.flight.time - when.timestamp()) / 60, 1)
            if pax.journey in DEPARTURES else None,
        }
        return self._envelope("passenger.position", pax.pax_id, when, payload)

    def minute_records(self, now: float, when: datetime, report: bool) -> list[EventEnvelope]:
        """Touchpoint, zone and journey records for the minute ending ``now``; resets the minute counters."""
        records: list[EventEnvelope] = []
        window_start = when - timedelta(seconds=60)
        targets = _targets(self.config)
        p95_window = (targets.p95_window_minutes if targets else 30) * 60
        for stats in self.stats.values():
            while stats.recent_starts and stats.recent_starts[0][0] < now - p95_window:
                stats.recent_starts.popleft()
        tp_payloads = []
        if report:
            by_tp: dict[str, list[Queue]] = {}
            for queue in self.queues.values():
                by_tp.setdefault(queue.touchpoint_id, []).append(queue)
            for tp_id in self.feed_touchpoints:
                tp, stats, queues = self.touchpoints[tp_id], self.stats[tp_id], by_tp[tp_id]
                channels = sum(q.channels for q in queues)
                queue_length = sum(len(q.waiting) for q in queues)
                in_service = sum(len(q.serving) for q in queues)
                extra = {}
                if targets is not None:
                    open_tp = tp.model_copy(update={"simulation": tp.simulation.model_copy(
                        update={"service_channels": channels})})
                    extra = queue_attributes(targets, open_tp, queue_length=queue_length, in_service=in_service,
                                             processed=stats.processed, interval_seconds=60,
                                             recent_waits=[w for _, w in stats.recent_starts])
                payload = PassengerTouchpoint(
                    touchpoint_id=tp_id, touchpoint_type=tp.touchpoint_type, zone_id=tp.zone_id,
                    window_start=window_start, window_end=when, queue_length=queue_length,
                    passengers_in_service=in_service, processed_passengers=stats.processed,
                    avg_wait_time_seconds=round(stats.wait_sum / stats.processed, 3) if stats.processed else None,
                    avg_processing_time_seconds=round(stats.service_sum / stats.processed, 3) if stats.processed else None,
                    throughput_since_midnight=stats.since_midnight, service_day=self.day.isoformat(), **extra,
                ).model_dump(mode="json")
                tp_payloads.append(payload)
                records.append(self._envelope("passenger.touchpoint", tp_id, when, payload))
        for stats in self.stats.values():
            stats.processed, stats.wait_sum, stats.service_sum = 0, 0.0, 0.0
        for events in (self.ev_airside, self.ev_boarded, self.ev_exited):
            while events and events[0][0] < now - self.rolling:
                events.popleft()
        if not report:
            return records

        people = [p for p in self.active if not p.done]
        cells: dict[str, Counter[str]] = {}
        activity: dict[str, Counter[str]] = {}
        for pax in people:
            zone = pax.zone or self.zone_of(pax.x, pax.y, pax.floor)
            cells.setdefault(zone, Counter())[f"{pax.floor}-{math.floor(pax.x / 5)}-{math.floor(pax.y / 5)}"] += 1
            activity.setdefault(zone, Counter())[pax.activity] += 1
        for zone_id, info in self.zone_info.items():
            zone_cells, acts = cells.get(zone_id, Counter()), activity.get(zone_id, Counter())
            count = sum(zone_cells.values())
            payload = PassengerAggregate(
                zone_id=zone_id, coordinate_frame_id=self.frame_id, floor=info["floor"],
                window_start=window_start, window_end=when, count_semantics="snapshot_at_window_end",
                passenger_count=count,
                cells=[{"grid_id": g, "passenger_count": n, "area_m2": 25.0} for g, n in sorted(zone_cells.items())],
                zone_name=info["name"], area_m2=info["area"],
                density_per_100m2=round(count / info["area"] * 100, 3) if info["area"] else None,
                **{f"people_{a}": acts.get(a, 0) for a in ACTIVITIES},
            ).model_dump(mode="json")
            records.append(self._envelope("passenger.aggregate", zone_id, when, payload))

        def pct(events: deque, q: float) -> float | None:
            values = [v for _, v in events]
            return round(nearest_rank(values, q), 1) if values else None

        waits = [(p.get("estimated_wait_seconds") or 0, p["touchpoint_id"]) for p in tp_payloads]
        longest = max(waits, default=(None, None))
        payload = PassengerJourneyKpi(
            kpi_id=KPI_ID, window_start=window_start, window_end=when,
            rolling_window_minutes=self.config.kpi_rolling_minutes,
            people_in_terminal=len(people),
            people_departing=sum(p.journey in DEPARTURES for p in people),
            people_arriving=sum(p.journey in ARRIVALS for p in people),
            people_queuing=sum(p.activity == "queuing" for p in people),
            departures_cleared_security=len(self.ev_airside),
            kerb_to_airside_p50_minutes=pct(self.ev_airside, 0.5),
            kerb_to_airside_p90_minutes=pct(self.ev_airside, 0.9),
            passengers_boarded=len(self.ev_boarded),
            airside_to_boarding_p50_minutes=pct(self.ev_boarded, 0.5),
            arrivals_exited=len(self.ev_exited),
            arrival_to_exit_p50_minutes=pct(self.ev_exited, 0.5),
            arrival_to_exit_p90_minutes=pct(self.ev_exited, 0.9),
            longest_wait_touchpoint_id=longest[1] if longest[0] else None,
            longest_estimated_wait_seconds=longest[0] if longest[0] else None,
            touchpoints_busy=sum(p.get("status") == "busy" for p in tp_payloads),
            touchpoints_congested=sum(p.get("status") == "congested" for p in tp_payloads),
        ).model_dump(mode="json")
        records.append(self._envelope("passenger.journey_kpi", KPI_ID, when, payload))
        return records


def generate_ticks(config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
                   start: datetime | None = None, end: datetime | None = None,
                   ) -> Iterator[tuple[datetime, list[EventEnvelope]]]:
    """Warm up silently, then yield (time, records) once per interval from ``start`` to ``end``.

    Every batch has the passenger positions; batches on a whole minute also carry the
    touchpoint, zone and journey records for the minute that just ended.
    """
    start = (start or config.start_time).astimezone(timezone.utc)
    end = (end or config.end_time).astimezone(timezone.utc)
    sim = TerminalSimulation(config, layout, inventory, start, end)
    dt = config.interval_seconds
    fine_from = start - timedelta(minutes=30)
    now = sim.warm_start
    while now < end:
        step = dt if now >= fine_from else min(config.warmup_step_seconds, (fine_from - now).total_seconds())
        # never step over a whole minute: the minute counters reset there
        to_minute = 60 - (now.timestamp() % 60)
        step = min(step, to_minute) if to_minute > 1e-6 else step
        now += timedelta(seconds=step)
        ts = now.timestamp()
        people = sim.tick(ts, step)
        reporting = now > start
        records = [sim.record(p, now) for p in people] if reporting else []
        if abs(ts - round(ts)) < 1e-3 and round(ts) % 60 == 0:
            records += sim.minute_records(ts, now, reporting)
        if reporting:
            yield now, records


def generate(config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
             start: datetime | None = None, end: datetime | None = None) -> Iterator[EventEnvelope]:
    for _, records in generate_ticks(config, layout, inventory, start, end):
        yield from records
