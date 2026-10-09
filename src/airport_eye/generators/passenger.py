"""Synthetic passenger tracking inside RGIA Terminal 1.

Agent-based and deterministic for a given seed and start time:

* A flight schedule (departures and arrivals, per journey type) creates passengers.
  Departures arrive at the kerb ahead of their flight; arrivals come off buses.
* Each passenger follows a journey plan: walk the terminal's walkway graph, queue
  at touchpoints (FIFO, service channels and times from the touchpoint inventory),
  dwell airside, wait at the gate and board, or reclaim bags and exit.
* Every ``interval_seconds`` each passenger in the terminal emits one
  ``passenger.position`` record with local x/y, floor, zone, speed, heading,
  activity and journey context. The last record of a track has activity
  ``boarded`` or ``exited``.

The queues here are connected: someone leaving check-in turns up at security.
Positions are only as good as config/terminal_layout.json (real outlines from
OpenStreetMap, schematic interior). All identifiers are synthetic.
"""

from __future__ import annotations

import heapq
import json
import math
import random
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pydantic import AwareDatetime, Field

from ..models import EventEnvelope, Model, NonEmpty
from ..spatial import WalkGraph, point_in_polygon
from ..touchpoint_config import TouchpointInventory

DEPARTURES = ("domestic_departure", "international_departure", "ga_departure")
ARRIVALS = ("domestic_arrival", "international_arrival", "ga_arrival")
FLIGHT_PREFIX = {"domestic_departure": "DD", "international_departure": "ID", "ga_departure": "GD",
                 "domestic_arrival": "DA", "international_arrival": "IA", "ga_arrival": "GA"}


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


class TrackingConfig(Model):
    note: str = Field(default="", alias="_note")
    start_time: AwareDatetime
    end_time: AwareDatetime
    interval_seconds: int = Field(gt=0)
    warmup_minutes: float = Field(ge=0)
    seed: int
    layout_file: NonEmpty
    inventory_file: NonEmpty
    output_directory: NonEmpty
    flights: dict[str, FlightStream]
    boarding: Boarding
    arrival_buses: ArrivalBuses
    shares: dict[str, float]
    walking_speed_mps: tuple[float, float]
    reclaim_wait_minutes: tuple[float, float]
    arrivals_hall_dwell_minutes: tuple[float, float]

    model_config = Model.model_config | {"populate_by_name": True}


def load_tracking(path: str | Path) -> tuple[TrackingConfig, dict[str, Any], TouchpointInventory, Path]:
    path = Path(path).resolve()
    config = TrackingConfig.model_validate_json(path.read_text(encoding="utf-8"))
    layout = json.loads((path.parent / config.layout_file).read_text(encoding="utf-8"))
    inventory = TouchpointInventory.model_validate_json((path.parent / config.inventory_file).read_text(encoding="utf-8"))
    return config, layout, inventory, (path.parent / config.output_directory).resolve()


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
    step: int = 0
    activity: str = "walking"
    touchpoint_id: str | None = None
    path: list[tuple[float, float, str]] = field(default_factory=list)
    until: float = 0.0
    moved: float = 0.0
    heading: float = 0.0
    sequence: int = 0
    done: bool = False


class Queue:
    """FIFO touchpoint (or gate) queue with parallel service channels and a physical layout."""

    def __init__(self, queue_id: str, x: float, y: float, floor: str, direction: tuple[float, float],
                 width: float, channels: int, service: tuple[float, float], touchpoint_id: str | None):
        self.queue_id, self.x, self.y, self.floor = queue_id, x, y, floor
        norm = math.hypot(*direction) or 1.0
        self.dx, self.dy = direction[0] / norm, direction[1] / norm
        self.px, self.py = self.dy, -self.dx  # perpendicular, along the service line
        self.width, self.channels, self.service = width, channels, service
        self.touchpoint_id = touchpoint_id
        self.waiting: deque[Passenger] = deque()
        self.serving: list[tuple[float, int, Passenger]] = []  # (ends, channel, pax)
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

    def update(self, now: float, rng: random.Random) -> list[Passenger]:
        finished = [pax for ends, _, pax in self.serving if ends <= now]
        self.serving = [s for s in self.serving if s[0] > now]
        busy = {channel for _, channel, _ in self.serving}
        for channel in range(self.channels):
            if not self.waiting:
                break
            if channel in busy:
                continue
            pax = self.waiting.popleft()
            self.serving.append((now + rng.uniform(*self.service), channel, pax))
            pax.activity = "in_service"
            pax.x, pax.y = self.channel_point(channel)
            pax.floor = self.floor
        for index, pax in enumerate(self.waiting):
            pax.x, pax.y = self.slot(index)
            pax.floor = self.floor
        return finished


class TerminalSimulation:
    def __init__(self, config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
                 start: datetime, end: datetime):
        self.config, self.layout = config, layout
        # Two streams: the schedule (flights, passengers, plans) and motion (walk targets,
        # service times). Neither depends on the end time, so a longer run of the same
        # start replays the same tracks.
        self.plan_rng = random.Random(f"{config.seed}:{start.isoformat()}:plan")
        self.rng = random.Random(f"{config.seed}:{start.isoformat()}:motion")
        self.graph = WalkGraph.from_layout(layout)
        self.frame_id = layout["frame"]["frame_id"]
        self.floor_z = {name: f["z"] for name, f in layout["floors"].items()}
        self.zones_by_floor: dict[str, list[tuple[str, list]]] = {}
        for zone in layout["zones"]:
            self.zones_by_floor.setdefault(zone["floor"], []).append((zone["zone_id"], zone["polygons"]))
        self.areas = layout["areas"]
        self.gates = {g["gate_id"]: g for g in layout["gates"]}
        self.gate_pools = {flow: sorted(g["gate_id"] for g in layout["gates"] if g["flow"] == flow)
                           for flow in ("dom", "int")}
        tp_specs = {tp.touchpoint_id: tp.simulation for tp in inventory.touchpoints}
        self.queues: dict[str, Queue] = {}
        for tp_id, spot in layout["touchpoints"].items():
            sim = tp_specs.get(tp_id)
            if sim is None:
                continue
            self.queues[tp_id] = Queue(tp_id, spot["x"], spot["y"], spot["floor"], tuple(spot["queue_direction"]),
                                       spot["service_width_m"], sim.service_channels,
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
        self.start = start.astimezone(timezone.utc)
        self.end = end.astimezone(timezone.utc)
        self.warm_start = self.start - timedelta(minutes=config.warmup_minutes)
        self.active: list[Passenger] = []
        self.counter = 0
        self._init_schedule()

    # -- schedule ------------------------------------------------------------
    # Flights are generated lazily, a little ahead of simulated time, so a live run
    # with no end date costs the same as a short one.
    def _init_schedule(self) -> None:
        self.spawns: list[tuple[float, int, Passenger]] = []  # heap of (time, order, passenger)
        self._order = 0
        self._gate_free: dict[str, float] = {}
        self._streams: dict[str, dict[str, float]] = {}
        for journey in DEPARTURES + ARRIVALS:
            stream = self.config.flights.get(journey)
            if stream is None or stream.per_hour <= 0:
                continue
            lead = (stream.arrive_before_departure_minutes or (0, 0))[1] * 60 if journey in DEPARTURES else 0
            first = self.warm_start.timestamp() - 3600 + self.plan_rng.expovariate(stream.per_hour / 3600)
            self._streams[journey] = {"next": first, "number": 0, "lead": lead}

    def _extend_schedule(self, now: float) -> None:
        cfg, rng = self.config, self.plan_rng
        t0, t1 = self.warm_start.timestamp(), self.end.timestamp()
        for journey, state in self._streams.items():
            stream = cfg.flights[journey]
            # departures: generate far enough ahead that every passenger due by ``now`` exists
            while state["next"] <= now + state["lead"]:
                t = state["next"]
                state["next"] = t + rng.expovariate(stream.per_hour / 3600)
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
                         rng.uniform(*cfg.walking_speed_mps), x, y, floor)

    # -- movement and state --------------------------------------------------
    def _walk_to(self, pax: Passenger, x: float, y: float, floor: str) -> None:
        pax.path = self.graph.path_between(pax.x, pax.y, pax.floor, x, y, floor)
        pax.activity = "walking"

    def _next_step(self, pax: Passenger, now: float) -> None:
        """Start the passenger's next plan step (called when the previous one is complete)."""
        pax.touchpoint_id = None
        if pax.step >= len(pax.plan):
            pax.done, pax.activity = True, "exited"
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
            pax.done, pax.activity = True, step[1]

    def _arrived(self, pax: Passenger, now: float) -> None:
        step = pax.plan[pax.step]
        kind = step[0]
        if kind == "tp":
            self.queues[step[1]].waiting.append(pax)
            pax.activity, pax.touchpoint_id = "queuing", step[1]
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
                queue = self.queues[pax.plan[pax.step][1]]
                queue.waiting.append(pax)
                pax.activity, pax.touchpoint_id = "queuing", queue.touchpoint_id
        for queue in self.queues.values():
            for pax in queue.update(now, self.rng):
                if queue.queue_id in self.gates:
                    pax.done, pax.activity = True, "boarded"
                else:
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

    def record(self, pax: Passenger, when: datetime, airport_id: str, terminal_id: str) -> EventEnvelope:
        speed = round(pax.moved / self.config.interval_seconds, 2)
        payload = {
            "passenger_id": pax.pax_id, "pnr_id": pax.pnr_id, "flight_id": pax.flight.flight_id,
            "indoor": {"coordinate_frame_id": self.frame_id, "x": round(pax.x, 2), "y": round(pax.y, 2),
                       "z": self.floor_z.get(pax.floor, 0.0), "floor": pax.floor,
                       "zone_id": self.zone_of(pax.x, pax.y, pax.floor),
                       "grid_id": f"{pax.floor}-{math.floor(pax.x / 5)}-{math.floor(pax.y / 5)}"},
            "speed_mps": speed, "heading_deg": round(pax.heading, 1) % 360,
            "movement_state": "walking" if speed > 0.05 else "stationary",
            "journey": pax.journey, "activity": pax.activity, "touchpoint_id": pax.touchpoint_id,
            "gate_id": pax.flight.gate_id,
        }
        sequence = pax.sequence
        pax.sequence += 1
        return EventEnvelope.model_construct(
            event_id=uuid5(NAMESPACE_URL, f"airport-eye:pax:{self.start.isoformat()}:{pax.pax_id}:{sequence}"),
            event_type="passenger.position", schema_version="1.0.0", airport_id=airport_id,
            terminal_id=terminal_id, source_system="synthetic-passenger-tracker", entity_id=pax.pax_id,
            event_time=when, ingestion_time=when, sequence_no=sequence, payload=payload)


def generate_ticks(config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
                   start: datetime | None = None, end: datetime | None = None,
                   ) -> Iterator[tuple[datetime, list[EventEnvelope]]]:
    """Warm up silently, then yield (time, records) once per interval from ``start`` to ``end``."""
    start = (start or config.start_time).astimezone(timezone.utc)
    end = (end or config.end_time).astimezone(timezone.utc)
    sim = TerminalSimulation(config, layout, inventory, start, end)
    dt = config.interval_seconds
    now = sim.warm_start
    terminal = "HYD-MAIN"
    while now < end:
        now += timedelta(seconds=dt)
        people = sim.tick(now.timestamp(), dt)
        if now <= start:  # warm-up: simulate, do not report
            continue
        yield now, [sim.record(p, now, inventory.airport_id, terminal) for p in people]


def generate(config: TrackingConfig, layout: dict[str, Any], inventory: TouchpointInventory,
             start: datetime | None = None, end: datetime | None = None) -> Iterator[EventEnvelope]:
    for _, records in generate_ticks(config, layout, inventory, start, end):
        yield from records
