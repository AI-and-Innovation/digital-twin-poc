"""Landside vehicles and kerbside, airside ground handling vehicles - driven by the passenger simulation.

Landside: departing passengers arrive by private car, app cab, taxi, RTC bus or via a car
park; arriving passengers leave the same ways. A vehicle drives the real road network
(OpenStreetMap) to its kerb, waits for a free bay (queuing on the drive when the kerb is
full), drops off or picks up, and leaves. Departing passengers enter the terminal only
when their vehicle has stopped, and pick-ups wait for their passengers to come out, so
a slow immigration queue lengthens kerb dwell.

Airside: each flight gets a stand; its turnaround needs ground power, baggage tractors,
catering, fuel, apron buses and a pushback tug at set times. Tasks are served by a fixed
fleet from two depots over the apron roads; when no vehicle is free the task starts late.

Feeds (every interval / every minute):
  vehicle.position  landside vehicles          curb.occupancy   kerbs, bus stop, car parks
  gse.position      airside vehicles           road.traffic     named roads: count, speed, congestion
                                               stand.activity   stands: flight, phase, vehicles, tasks
"""

from __future__ import annotations

import heapq
import math
import random
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pydantic import Field

from ..models import Model
from ..spatial import LocalFrame, point_in_polygon

KMH = 1 / 3.6
LANDSIDE_TYPES = ("private_car", "app_cab", "taxi", "bus")
GSE_TYPES = ("baggage_tractor", "fuel_truck", "catering_truck", "apron_bus", "pushback_tug", "gpu")
TYPE_CODE = {"private_car": "CAR", "app_cab": "CAB", "taxi": "TAXI", "bus": "BUS", "baggage_tractor": "BAG",
             "fuel_truck": "FUEL", "catering_truck": "CAT", "apron_bus": "ABUS", "pushback_tug": "TUG", "gpu": "GPU"}


class GroundConfig(Model):
    departure_modes: dict[str, float]           # private_car, app_cab, taxi, bus, car_park
    arrival_modes: dict[str, float]
    party_size: tuple[int, int] = (1, 4)
    dropoff_dwell_seconds: tuple[float, float] = (40, 150)
    pickup_load_seconds: tuple[float, float] = (30, 90)
    pickup_arrives_after_landing_minutes: tuple[float, float] = (15, 40)
    cab_response_minutes: tuple[float, float] = (3, 9)
    bus_every_minutes: float = Field(default=15, gt=0)
    bus_dwell_minutes: tuple[float, float] = (4, 8)
    car_park_initial_share: float = Field(default=0.55, ge=0, le=1)
    car_park_mean_stay_hours: float = Field(default=20, gt=0)   # long-stay cars of travellers from earlier trips leave too
    gse_fleet: dict[str, int] = Field(default_factory=dict)
    gse_late_after_seconds: float = 120


# ----------------------------------------------------------------------------- roads
class RoadGraph:
    """Directed road graph (one-way streets respected); routes by travel time."""

    def __init__(self, layout: dict[str, Any], side: str):
        self.nodes = {n["id"]: (n["x"], n["y"]) for n in layout["roads"]["nodes"] if n["side"] == side}
        self.out: dict[str, list[tuple[str, float, float, str, str]]] = {n: [] for n in self.nodes}
        for a, b, length, kmh, level, name, s, _way in layout["roads"]["edges"]:
            if s == side and a in self.nodes and b in self.nodes:
                self.out[a].append((b, length, kmh * KMH, level, name))
        self.ids = list(self.nodes)
        self._cache: dict[tuple[str, str], list[tuple] | None] = {}
        self._near: dict[tuple, list[str]] = {}

    def nearest(self, x: float, y: float, k: int = 1) -> list[str]:
        key = (round(x / 5), round(y / 5), k)   # 5 m buckets: same answer, far fewer sorts
        hit = self._near.get(key)
        if hit is None:
            hit = self._near[key] = sorted(self.ids, key=lambda n: (self.nodes[n][0] - x) ** 2 + (self.nodes[n][1] - y) ** 2)[:k]
        return hit

    def _route(self, a: str, b: str) -> list[tuple] | None:
        if (a, b) in self._cache:
            return self._cache[(a, b)]
        best, prev, heap = {a: 0.0}, {}, [(0.0, a)]
        while heap:
            cost, n = heapq.heappop(heap)
            if n == b:
                break
            if cost > best.get(n, math.inf):
                continue
            for m, length, speed, level, name in self.out[n]:
                c = cost + length / speed
                if c < best.get(m, math.inf):
                    best[m], prev[m] = c, (n, speed, level, name)
                    heapq.heappush(heap, (c, m))
        if b != a and b not in prev:
            self._cache[(a, b)] = None
            return None
        steps, n = [], b
        while n != a:
            p, speed, level, name = prev[n]
            steps.append((*self.nodes[n], level, speed, name))
            n = p
        path = [(*self.nodes[a], steps[-1][2] if steps else "L0", steps[-1][3] if steps else 5.0,
                 steps[-1][4] if steps else "")] + list(reversed(steps))
        self._cache[(a, b)] = path
        return path

    def route(self, x0, y0, x1, y1, level1="L0", last_speed=None) -> list[tuple]:
        """Waypoints (x, y, level, speed m/s, road name) from near (x0, y0) to (x1, y1)."""
        for a in self.nearest(x0, y0, 6):
            for b in self.nearest(x1, y1, 6):
                path = self._route(a, b)
                if path:
                    end_speed = last_speed or path[-1][3]
                    return list(path) + [(x1, y1, level1, end_speed, path[-1][4])]
        return [(x1, y1, level1, last_speed or 5.0, "")]


def path_length(path) -> float:
    return sum(math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(path, path[1:]))


def path_time(path) -> float:
    return sum(math.hypot(b[0] - a[0], b[1] - a[1]) / max(b[3], 0.5) for a, b in zip(path, path[1:]))


def along(polyline: list[list[float]], distance: float) -> tuple[float, float]:
    for a, b in zip(polyline, polyline[1:]):
        seg = math.hypot(b[0] - a[0], b[1] - a[1])
        if distance <= seg and seg > 0:
            t = distance / seg
            return a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t
        distance -= seg
    return tuple(polyline[-1])


# ----------------------------------------------------------------------------- entities
@dataclass
class Vehicle:
    vehicle_id: str
    vehicle_type: str
    domain: str                    # landside | airside
    x: float
    y: float
    level: str = "L0"
    state: str = "idle"
    path: list[tuple] = field(default_factory=list)
    plan: list[tuple] = field(default_factory=list)
    step: int = 0
    until: float = 0.0
    moved: float = 0.0
    heading: float = 0.0
    road: str = ""
    facility_id: str | None = None
    bay: int | None = None
    stopped_at: float | None = None
    passengers: list = field(default_factory=list)
    party: Any = None
    flight_id: str | None = None
    stand_id: str | None = None
    task: Any = None
    depot: str | None = None
    sequence: int = 0
    done: bool = False


@dataclass
class Facility:
    facility_id: str
    name: str
    facility_type: str            # kerb | bus_stop | car_park
    floor: str
    purpose: str
    capacity: int
    max_dwell_s: float
    polyline: list
    polygon: list | None = None
    occupants: dict = field(default_factory=dict)      # bay -> vehicle
    parked: int = 0
    queue: deque = field(default_factory=deque)
    arrivals: int = 0
    departures: int = 0
    dwell_sum: float = 0.0
    dwell_n: int = 0
    entry: tuple = (0.0, 0.0)

    def bay_point(self, bay: int) -> tuple[float, float]:
        length = sum(math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(self.polyline, self.polyline[1:]))
        return along(self.polyline, (bay + 0.5) * length / max(self.capacity, 1))

    def free_bay(self) -> int | None:
        for bay in range(self.capacity):
            if bay not in self.occupants:
                return bay
        return None

    @property
    def occupied(self) -> int:
        return self.parked if self.facility_type == "car_park" else len(self.occupants)


@dataclass
class Party:
    passengers: list
    mode: str
    flight: Any
    exited: int = 0
    vehicle: Vehicle | None = None
    ready_at: float | None = None


@dataclass
class Task:
    flight_id: str
    stand_id: str
    kind: str                     # gse type
    start: float                  # when the vehicle should be at the stand
    duration: float
    then: str = "depot"           # after the stand: depot | baggage_hall | bus_gates
    then_dwell: float = 0.0
    before: str | None = None     # before the stand: baggage_hall | bus_gates (loading for departures)
    before_dwell: float = 0.0
    state: str = "pending"        # pending | en_route | in_progress | done
    late: bool = False
    vehicle: Vehicle | None = None
    arrived: float | None = None


@dataclass
class StandUse:
    stand_id: str
    flight_id: str
    phase: str                    # arrival_turnaround | departure_preparation
    start: float
    end: float
    on_block: float | None = None
    off_block: float | None = None
    tasks: list = field(default_factory=list)


# ----------------------------------------------------------------------------- simulation
class GroundSimulation:
    def __init__(self, sim, layout: dict[str, Any], config: GroundConfig, seed: str):
        self.sim, self.layout, self.config = sim, layout, config
        self.P = sim.config.id_prefix
        self.rng = random.Random(f"{seed}:ground")
        self.frame = LocalFrame.from_dict(layout["frame"])
        self.land = RoadGraph(layout, "landside")
        self.air = RoadGraph(layout, "airside")
        self.facilities: dict[str, Facility] = {}
        for k in layout["kerbs"]:
            f = Facility(k["kerb_id"], k["name"], "bus_stop" if k["purpose"] == "bus" else "kerb", k["floor"],
                         k["purpose"], k["bays"], k["max_dwell_s"], k["polyline"])
            f.entry = tuple(k["polyline"][0])
            self.facilities[f.facility_id] = f
        for p in layout["car_parks"]:
            poly = p["polygon"]
            cx, cy = sum(q[0] for q in poly) / len(poly), sum(q[1] for q in poly) / len(poly)
            f = Facility(p["park_id"], p["name"], "car_park", "L0", "parking", p["capacity"], 0, [[cx, cy]], poly)
            f.parked = int(p["capacity"] * config.car_park_initial_share)
            f.entry = (cx, cy)
            self.facilities[f.facility_id] = f
        self.kerb_for = {("drop_off", "private_car"): f"{self.P}-KERB-DEP-PRIVATE",
                         ("drop_off", "app_cab"): f"{self.P}-KERB-DEP-COMMERCIAL",
                         ("drop_off", "taxi"): f"{self.P}-KERB-DEP-COMMERCIAL",
                         ("pick_up", "private_car"): f"{self.P}-KERB-ARR-PRIVATE",
                         ("pick_up", "app_cab"): f"{self.P}-KERB-ARR-CAB",
                         ("pick_up", "taxi"): f"{self.P}-KERB-ARR-CAB"}
        self.entries = layout["entries"]
        self.exits = layout["exits"]
        self.stands = {s["stand_id"]: s for s in layout["stands"]}
        self.stand_free: dict[str, float] = {s: -math.inf for s in self.stands}
        self.stand_uses: list[StandUse] = []
        self.points = {k: (v["x"], v["y"]) for k, v in layout["service_points"].items()}
        self.depots = {d["depot_id"]: (d["x"], d["y"]) for d in layout["depots"]}
        self.vehicles: list[Vehicle] = []          # landside, active
        self.fleet: list[Vehicle] = []             # airside, persistent
        self.spawns: list[tuple[float, int, str, Any]] = []   # heap of (time, order, kind, payload)
        self.order = 0
        self.counter = Counter()
        self.parties_by_pax: dict[str, Party] = {}
        self.tasks: list[Task] = []
        self.bus_pickups: deque = deque()          # (time, pax) arrival passengers heading for the RTC bus
        dom_depot, int_depot = f"{self.P}-GSE-DEPOT-EAST", f"{self.P}-GSE-DEPOT-WEST"
        for kind, count in config.gse_fleet.items():
            for i in range(count):
                depot = int_depot if i % 3 == 2 else dom_depot   # a third of each fleet based west
                x, y = self.depots[depot]
                x += self.rng.uniform(-25, 25)
                y += self.rng.uniform(-15, 15)
                v = Vehicle(f"SYN-GSE-{TYPE_CODE[kind]}-{i + 1:03d}", kind, "airside", x, y, depot=depot)
                self.fleet.append(v)
        self.next_bus = None
        self._bus_riders: dict[float, list] = {}

    # -- helpers ---------------------------------------------------------------
    def _push(self, at: float, kind: str, payload: Any) -> None:
        self.order += 1
        heapq.heappush(self.spawns, (at, self.order, kind, payload))

    def _new_id(self, kind: str) -> str:
        self.counter[kind] += 1
        return f"SYN-VEH-{TYPE_CODE[kind]}-{self.counter[kind]:05d}"

    def _pick(self, shares: dict[str, float]) -> str:
        r, acc = self.rng.random() * sum(shares.values()), 0.0
        for k, v in shares.items():
            acc += v
            if r <= acc:
                return k
        return next(iter(shares))

    def _side(self) -> str:
        return self.rng.choice(sorted(self.entries))   # entry/exit names from the layout ("east", "west" at HYD)

    # -- passengers in: departures ----------------------------------------------
    def add_departures(self, flight, arrivals: list[tuple[float, Any]]) -> list[tuple[float, Any]]:
        """Take the flight's departing passengers (kerb time, passenger); returns those NOT handled here."""
        cfg = self.config
        arrivals = sorted(arrivals, key=lambda a: a[0])
        i = 0
        while i < len(arrivals):
            mode = self._pick(cfg.departure_modes)
            if mode == "bus":
                at, pax = arrivals[i]
                stop_at = self._bus_slot(at)
                self._push(stop_at, "bus_dropoff", pax)
                i += 1
                continue
            size = self.rng.randint(*cfg.party_size)
            group = arrivals[i:i + size]
            i += size
            at = group[0][0]
            party = Party([p for _, p in group], mode, flight)
            if mode == "car_park":
                self._push(at - 60, "car_to_park", party)
            else:
                self._push(at, "dropoff", party)
        return []

    def _bus_slot(self, at: float) -> float:
        every = self.config.bus_every_minutes * 60
        return math.floor(at / every) * every

    # -- passengers in: arrivals ------------------------------------------------
    def add_arrival_passengers(self, flight, passengers: list) -> None:
        cfg = self.config
        i = 0
        while i < len(passengers):
            mode = self._pick(cfg.arrival_modes)
            size = 1 if mode == "bus" else self.rng.randint(*cfg.party_size)
            party = Party(passengers[i:i + size], mode, flight)
            for p in party.passengers:
                self.parties_by_pax[p.pax_id] = party
            i += size
            if mode == "private_car":   # the driver sets off to be there around when they think the flight is out
                eta = flight.time + self.rng.uniform(*cfg.pickup_arrives_after_landing_minutes) * 60
                self._push(eta, "pickup_car", party)

    def on_arrival_exit(self, pax, now: float) -> None:
        party = self.parties_by_pax.pop(pax.pax_id, None)
        if party is None:
            return
        party.exited += 1
        if party.exited < len(party.passengers):
            return
        party.ready_at = now
        if party.mode == "private_car":
            v = party.vehicle
            if v is not None and v.state == "at_kerb":
                v.until = now + self.rng.uniform(*self.config.pickup_load_seconds)
                v.state = "loading"
                v.passengers = list(party.passengers)
        elif party.mode in ("app_cab", "taxi"):
            self._push(now + self.rng.uniform(*self.config.cab_response_minutes) * 60, "pickup_cab", party)
        elif party.mode == "car_park":
            self._push(now + self.rng.uniform(3, 8) * 60, "car_from_park", party)
        else:
            self.bus_pickups.append((now, pax))

    # -- flights in: airside -----------------------------------------------------
    def _stand_for(self, flight, start: float, end: float) -> str:
        flow = "int" if flight.journey.startswith("international") else "dom"
        pool = [s for s, v in self.stands.items() if v["flow"] == flow] or list(self.stands)
        free = [s for s in pool if self.stand_free[s] <= start]
        stand = self.rng.choice(free or pool)
        self.stand_free[stand] = end
        return stand

    def add_flight(self, flight) -> None:
        if flight.journey.startswith("ga_"):
            return
        t = flight.time
        buses = math.ceil(flight.passengers / 60)
        if flight.journey.endswith("arrival"):
            stand = self._stand_for(flight, t - 300, t + 45 * 60)
            use = StandUse(stand, flight.flight_id, "arrival_turnaround", t - 300, t + 45 * 60, on_block=t)
            b = self.sim.config.arrival_buses
            tasks = [Task(flight.flight_id, stand, "gpu", t, 35 * 60)]
            tasks += [Task(flight.flight_id, stand, "baggage_tractor", t + 180 + 120 * k, 12 * 60, "baggage_hall", 300)
                      for k in range(2)]
            for k in range(buses):  # each bus reaches the terminal when its passengers appear there
                at_terminal = t + (b.first_bus_after_arrival_minutes + k * b.minutes_between_buses) * 60
                tasks.append(Task(flight.flight_id, stand, "apron_bus", at_terminal - 240, 90, "bus_gates", 60))
        else:
            stand = self._stand_for(flight, t - 75 * 60, t + 300)
            use = StandUse(stand, flight.flight_id, "departure_preparation", t - 75 * 60, t + 300, off_block=t)
            bo = self.sim.config.boarding
            opens = t - bo.opens_before_departure_minutes * 60
            tasks = [Task(flight.flight_id, stand, "gpu", t - 60 * 60, 55 * 60),
                     Task(flight.flight_id, stand, "catering_truck", t - 55 * 60, 18 * 60),
                     Task(flight.flight_id, stand, "fuel_truck", t - 45 * 60, 18 * 60),
                     Task(flight.flight_id, stand, "pushback_tug", t - 8 * 60, 9 * 60)]
            tasks += [Task(flight.flight_id, stand, "baggage_tractor", t - 40 * 60 + 150 * k, 12 * 60,
                           before="baggage_hall", before_dwell=240) for k in range(2)]
            tasks += [Task(flight.flight_id, stand, "apron_bus", opens + (5 + 8 * k) * 60, 120,
                           before="bus_gates", before_dwell=180) for k in range(buses)]
        warm = self.sim.warm_start.timestamp()
        tasks = [t for t in tasks if t.start >= warm]   # turnarounds that began before the simulation are not chased
        use.tasks = tasks
        self.stand_uses.append(use)
        self.tasks.extend(tasks)

    # -- landside vehicle plans ---------------------------------------------------
    def _spawn_landside(self, kind: str, now: float, plan: list[tuple], party=None, passengers=None) -> Vehicle:
        side = self._side()
        ex, ey = self.entries[side]
        v = Vehicle(self._new_id(kind), kind, "landside", ex, ey, state="driving", plan=plan, party=party,
                    passengers=list(passengers or []))
        if party is not None:
            v.flight_id = party.flight.flight_id
            party.vehicle = v
        self.vehicles.append(v)
        self._next(v, now)
        return v

    def _exit_plan(self) -> tuple:
        return ("exit", self._side())

    def _start_due(self, now: float) -> None:
        cfg = self.config
        while self.spawns and self.spawns[0][0] <= now:
            at, _, kind, payload = heapq.heappop(self.spawns)
            if kind == "dropoff":
                party = payload
                kerb = self.kerb_for[("drop_off", party.mode)]
                self._spawn_landside(party.mode, now, [("kerb", kerb, "drop_off"), self._exit_plan()], party,
                                     party.passengers)
            elif kind == "car_to_park":
                park = self.rng.choice([f for f in self.facilities.values() if f.facility_type == "car_park"])
                self._spawn_landside("private_car", now, [("park", park.facility_id)], payload, payload.passengers)
            elif kind == "car_from_park":
                park = self.rng.choice([f for f in self.facilities.values() if f.facility_type == "car_park"])
                x, y = self._point_in(park.polygon)
                v = Vehicle(self._new_id("private_car"), "private_car", "landside", x, y, state="leaving_car_park",
                            plan=[self._exit_plan()], passengers=list(payload.passengers),
                            flight_id=payload.flight.flight_id)
                park.parked = max(0, park.parked - 1)
                park.departures += 1
                self.vehicles.append(v)
                self._next(v, now)
            elif kind == "pickup_car":
                kerb = self.kerb_for[("pick_up", "private_car")]
                self._spawn_landside("private_car", now, [("kerb", kerb, "pick_up"), self._exit_plan()], payload)
            elif kind == "pickup_cab":
                kerb = self.kerb_for[("pick_up", payload.mode)]
                self._spawn_landside(payload.mode, now, [("kerb", kerb, "pick_up"), self._exit_plan()], payload)
                payload.vehicle.passengers = list(payload.passengers)
            elif kind == "bus_dropoff":
                self._bus_riders.setdefault(at, []).append(payload)
        # RTC buses run on a timetable; each brings the departing bus riders due at its stop time
        every = cfg.bus_every_minutes * 60
        if self.next_bus is None:
            self.next_bus = math.floor(now / every) * every + every
        while self.next_bus - 180 <= now:   # set off ~3 minutes before the stop time
            stop_time = self.next_bus
            riders = self._bus_riders.pop(stop_time, [])
            self._spawn_landside("bus", now, [("kerb", f"{self.P}-KERB-BUS", "bus"), self._exit_plan()],
                                 passengers=riders)
            self.next_bus += every

    def _parks(self) -> list[str]:
        return [f.facility_id for f in self.facilities.values() if f.facility_type == "car_park"]

    def _point_in(self, polygon) -> tuple[float, float]:
        xs, ys = [p[0] for p in polygon], [p[1] for p in polygon]
        for _ in range(30):
            x, y = self.rng.uniform(min(xs), max(xs)), self.rng.uniform(min(ys), max(ys))
            if point_in_polygon(x, y, polygon):
                return x, y
        return sum(xs) / len(xs), sum(ys) / len(ys)

    def _next(self, v: Vehicle, now: float) -> None:
        if v.step >= len(v.plan):
            v.done, v.state = True, "parked" if v.state == "parked" else "exited"
            return
        step = v.plan[v.step]
        graph = self.land if v.domain == "landside" else self.air
        if step[0] == "kerb":
            f = self.facilities[step[1]]
            v.facility_id = f.facility_id
            ex, ey = f.entry
            v.path = graph.route(v.x, v.y, ex, ey, f.floor, 15 * KMH)
            v.state = "driving"
        elif step[0] == "park":
            f = self.facilities[step[1]]
            v.facility_id = f.facility_id
            v.path = graph.route(v.x, v.y, *f.entry, "L0", 10 * KMH)
            v.state = "driving"
        elif step[0] == "exit":
            x, y = self.exits[step[1]]
            v.path = graph.route(v.x, v.y, x, y, "L0")
            v.state = "leaving"
            v.facility_id = None
        elif step[0] == "goto":         # airside: drive to a point
            v.path = graph.route(v.x, v.y, step[1], step[2], "L0", 10 * KMH)
            v.state = step[3]

    def _arrive_landside(self, v: Vehicle, now: float) -> None:
        step = v.plan[v.step]
        if step[0] == "kerb":
            f = self.facilities[step[1]]
            f.queue.append(v)
            v.state = "queuing"
        elif step[0] == "park":
            f = self.facilities[step[1]]
            f.parked = min(f.capacity, f.parked + 1)
            f.arrivals += 1
            v.x, v.y = self._point_in(f.polygon)
            for pax in v.passengers:   # they walk from the car park to the terminal
                self.sim.release(pax, now, v.x, v.y, "L0")
            v.passengers = []
            v.state = "parked"
            v.step += 1
            self._next(v, now)
        elif step[0] == "exit":
            v.done = True
            v.state = "exited"

    def _serve_kerbs(self, now: float) -> None:
        for f in self.facilities.values():
            if f.facility_type == "car_park":
                continue
            while f.queue:
                bay = f.free_bay()
                if bay is None:
                    break
                v = f.queue.popleft()
                f.occupants[bay] = v
                f.arrivals += 1
                v.bay, v.stopped_at = bay, now
                v.x, v.y = f.bay_point(bay)
                v.level = f.floor
                purpose = v.plan[v.step][2]
                if purpose == "drop_off" or (purpose == "bus" and v.passengers):
                    v.state = "dropping_off"
                    v.until = now + self.rng.uniform(*self.config.dropoff_dwell_seconds)
                    for pax in v.passengers:
                        self.sim.release(pax, now, v.x, v.y - 4, f.floor)
                    v.passengers = []
                elif purpose == "bus":
                    v.state = "at_stop"
                    v.until = now + self.rng.uniform(*self.config.bus_dwell_minutes) * 60
                elif v.party is not None and v.party.ready_at is not None:
                    v.state = "loading"
                    v.until = now + self.rng.uniform(*self.config.pickup_load_seconds)
                    v.passengers = list(v.party.passengers)
                else:
                    v.state = "at_kerb"      # waiting for its passengers to come out
                    v.until = math.inf
            # queued vehicles wait on the drive behind the kerb entry
            for i, v in enumerate(f.queue):
                v.moved = 0.0
                ex, ey = f.entry
                h = math.radians(v.heading)
                v.x, v.y = ex - 7 * (i + 1) * math.sin(h), ey - 7 * (i + 1) * math.cos(h)

    def _leave_kerb(self, v: Vehicle, now: float) -> None:
        f = self.facilities.get(v.facility_id or "")
        if f is not None and v.bay is not None:
            f.occupants.pop(v.bay, None)
            f.departures += 1
            f.dwell_sum += now - (v.stopped_at or now)
            f.dwell_n += 1
        if v.state == "at_stop" and v.vehicle_type == "bus":
            while self.bus_pickups and self.bus_pickups[0][0] <= now and len(v.passengers) < 60:
                v.passengers.append(self.bus_pickups.popleft()[1])
        v.bay = None
        v.step += 1
        self._next(v, now)

    # -- airside dispatch -----------------------------------------------------------
    def _dispatch(self, now: float) -> None:
        for task in self.tasks:
            if task.state != "pending":
                continue
            if now > task.start + 30 * 60:   # dispatch gives up on a task 30 minutes overdue
                task.state, task.late = "missed", True
                continue
            if task.start - 20 * 60 > now:   # only plan the next 20 minutes
                continue
            idle = [v for v in self.fleet if v.vehicle_type == task.kind and v.state == "idle"]
            if not idle:
                continue
            sx, sy = self._stand_spot(task)
            v = min(idle, key=lambda c: (c.x - sx) ** 2 + (c.y - sy) ** 2)
            plan = []
            if task.before:
                bx, by = self._terminal_point(task.before, task.stand_id)
                plan.append(("goto", bx, by, "to_terminal_first"))
                travel = path_time(self.air.route(v.x, v.y, bx, by)) + task.before_dwell \
                    + path_time(self.air.route(bx, by, sx, sy))
            else:
                travel = path_time(self.air.route(v.x, v.y, sx, sy))
            if now < task.start - travel - 60:
                continue     # too early to set off
            plan.append(("goto", sx, sy, "to_stand"))
            task.state, task.vehicle = "en_route", v
            v.task, v.flight_id, v.stand_id = task, task.flight_id, task.stand_id
            v.plan, v.step = plan, 0
            self._next(v, now)
        # drop finished tasks so the list stays short
        self.tasks = [t for t in self.tasks if t.state not in ("done", "missed") or now - t.start < 3600]

    KERB_WAIT_LIMIT_S = 600   # marshals move on a car waiting this long for its passengers
    OFFSETS = {"gpu": (6, 10), "fuel_truck": (14, 4), "catering_truck": (-14, 6), "baggage_tractor": (9, -9),
               "apron_bus": (-16, -12), "pushback_tug": (0, -22)}

    def _stand_spot(self, task: Task) -> tuple[float, float]:
        s = self.stands[task.stand_id]
        dx, dy = self.OFFSETS.get(task.kind, (0, 0))
        return s["x"] + dx, s["y"] + dy

    def _terminal_point(self, kind: str, stand_id: str) -> tuple[float, float]:
        if kind == "baggage_hall":
            return self.points["baggage_hall"]
        return self.points["bus_gates_int" if self.stands[stand_id]["flow"] == "int" else "bus_gates_dom"]

    def _arrive_airside(self, v: Vehicle, now: float) -> None:
        task = v.task
        if v.state == "to_terminal_first":
            v.state = "loading_at_terminal"
            v.until = now + (task.before_dwell if task else 120)
            return
        if v.state == "to_stand" and task is not None:
            task.state, task.arrived = "in_progress", now
            task.late = now > task.start + self.config.gse_late_after_seconds
            v.state = "servicing"
            v.until = max(now, task.start) + task.duration
        elif v.state == "to_terminal":
            v.state = "at_terminal"
            v.until = now + (task.then_dwell if task else 60)
        elif v.state == "to_depot":
            v.state, v.task, v.flight_id, v.stand_id = "idle", None, None, None
            v.path = []

    def _after_service(self, v: Vehicle, now: float) -> None:
        task = v.task
        if task is not None and task.state == "in_progress":
            task.state = "done"
        if v.state == "servicing" and task is not None and task.then in ("baggage_hall", "bus_gates"):
            key = task.then if task.then == "baggage_hall" else \
                ("bus_gates_int" if self.stands[task.stand_id]["flow"] == "int" else "bus_gates_dom")
            x, y = self.points[key]
            v.plan, v.step = [("goto", x, y, "to_terminal")], 0
            self._next(v, now)
            return
        x, y = self.depots[v.depot]
        v.plan, v.step = [("goto", x + self.rng.uniform(-25, 25), y + self.rng.uniform(-15, 15), "to_depot")], 0
        self._next(v, now)

    # -- movement --------------------------------------------------------------------
    def _move(self, v: Vehicle, dt: float) -> None:
        budget_t, moved = dt, 0.0
        while v.path and budget_t > 0:
            tx, ty, level, speed, name = v.path[0]
            dx, dy = tx - v.x, ty - v.y
            dist = math.hypot(dx, dy)
            v.road, v.level = name, level
            if dist > 1e-6:
                v.heading = (90 - math.degrees(math.atan2(dy, dx))) % 360
            reach = speed * budget_t
            if dist <= reach:
                v.x, v.y = tx, ty
                budget_t -= dist / max(speed, 0.1)
                moved += dist
                v.path.pop(0)
            else:
                v.x += dx / dist * reach
                v.y += dy / dist * reach
                moved += reach
                budget_t = 0
        v.moved = moved

    def _car_park_returns(self, now: float, dt: float) -> None:
        """Travellers back from earlier trips collect their cars: each parked car leaves at a rate of 1 / mean stay."""
        rate = dt / (self.config.car_park_mean_stay_hours * 3600)
        for park in self.facilities.values():
            if park.facility_type != "car_park" or park.parked <= 0:
                continue
            leaving = self._poisson(park.parked * rate)
            for _ in range(leaving):
                x, y = self._point_in(park.polygon)
                v = Vehicle(self._new_id("private_car"), "private_car", "landside", x, y, state="leaving_car_park",
                            plan=[self._exit_plan()])
                park.parked -= 1
                park.departures += 1
                self.vehicles.append(v)
                self._next(v, now)

    def _poisson(self, lam: float) -> int:
        """Poisson draw (Knuth); lam is small here, so this is a handful of random numbers."""
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1

    def tick(self, now: float, dt: float) -> None:
        self._start_due(now)
        self._car_park_returns(now, dt)
        for v in self.vehicles:
            v.moved = 0.0
            if v.done:
                continue
            if v.state in ("driving", "leaving", "leaving_car_park"):
                self._move(v, dt)
                if not v.path:
                    self._arrive_landside(v, now)
            elif v.state in ("dropping_off", "loading", "at_stop") and now >= v.until:
                self._leave_kerb(v, now)
            elif v.state == "at_kerb" and v.stopped_at is not None and now - v.stopped_at > self.KERB_WAIT_LIMIT_S:
                # kerb marshals move on cars waiting too long: it goes to a car park to wait
                party = v.party
                if party is not None:
                    party.mode, party.vehicle = "car_park", None
                v.party = None
                self._leave_kerb(v, now)
                v.plan[v.step:] = [("park", self.rng.choice(self._parks()))]
                self._next(v, now)
        self._serve_kerbs(now)
        self._dispatch(now)
        for v in self.fleet:
            v.moved = 0.0
            if v.state in ("to_stand", "to_terminal", "to_terminal_first", "to_depot"):
                self._move(v, dt)
                if not v.path:
                    self._arrive_airside(v, now)
            elif v.state == "servicing" and now >= v.until:
                self._after_service(v, now)
            elif v.state == "loading_at_terminal" and now >= v.until:
                v.step += 1
                self._next(v, now)
            elif v.state == "at_terminal" and now >= v.until:
                x, y = self.depots[v.depot]
                if v.task is not None:
                    v.task.state = "done"
                v.plan, v.step = [("goto", x + self.rng.uniform(-25, 25), y + self.rng.uniform(-15, 15), "to_depot")], 0
                self._next(v, now)
        self.stand_uses = [u for u in self.stand_uses if u.end > now - 600]

    def finished_landside(self) -> list[Vehicle]:
        done = [v for v in self.vehicles if v.done]
        self.vehicles = [v for v in self.vehicles if not v.done]
        return done

    # -- records -----------------------------------------------------------------------
    def position_payload(self, v: Vehicle, interval: float, now: float) -> dict[str, Any]:
        lon, lat = self.frame.to_wgs84(v.x, v.y)
        speed = round(v.moved / interval * 3.6, 1) if interval else 0.0
        dwell = overstay = None
        f = self.facilities.get(v.facility_id or "")
        if f is not None and v.bay is not None and v.stopped_at is not None:
            dwell = round(now - v.stopped_at, 1)
            overstay = bool(f.max_dwell_s and dwell > f.max_dwell_s)
        return {"vehicle_id": v.vehicle_id, "vehicle_type": v.vehicle_type, "domain": v.domain,
                "lat": lat, "lon": lon, "floor": v.level, "x": round(v.x, 1), "y": round(v.y, 1),
                "speed_kmh": speed, "heading_deg": round(v.heading + self.frame.rotation_deg, 1) % 360, "state": v.state,
                "road_name": v.road or None, "facility_id": v.facility_id, "stand_id": v.stand_id,
                "flight_id": v.flight_id, "task": v.task.kind if v.task is not None else None,
                "passengers_on_board": len(v.passengers),
                "dwell_seconds": dwell, "overstay": overstay}

    def facility_payloads(self, now: float, window_start: datetime, window_end: datetime) -> list[dict]:
        out = []
        for f in self.facilities.values():
            length = sum(math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(f.polyline, f.polyline[1:]))
            x, y = along(f.polyline, length / 2) if len(f.polyline) > 1 else tuple(f.polyline[0])
            lon, lat = self.frame.to_wgs84(x, y)
            dwells = [now - v.stopped_at for v in f.occupants.values() if v.stopped_at is not None]
            occupied = f.occupied
            share = occupied / f.capacity if f.capacity else 0.0
            overstays = sum(d > f.max_dwell_s for d in dwells) if f.facility_type != "car_park" else 0
            queuing = len(f.queue)
            status = ("congested" if share >= 0.9 or queuing >= 3 else "busy" if share >= 0.7 or queuing > 0
                      else "normal")
            out.append({"facility_id": f.facility_id, "name": f.name, "facility_type": f.facility_type,
                        "purpose": f.purpose, "floor": f.floor, "lat": lat, "lon": lon,
                        "window_start": window_start, "window_end": window_end,
                        "capacity": f.capacity, "occupied": occupied, "occupancy_pct": round(100 * share, 1),
                        "vehicles_queuing": queuing, "arrivals": f.arrivals, "departures": f.departures,
                        "avg_dwell_seconds": round(f.dwell_sum / f.dwell_n, 1) if f.dwell_n else None,
                        "max_dwell_seconds": f.max_dwell_s or None, "overstays": overstays, "status": status})
        self.reset_window()
        return out

    def reset_window(self) -> None:
        for f in self.facilities.values():
            f.arrivals = f.departures = f.dwell_n = 0
            f.dwell_sum = 0.0

    def road_payloads(self, window_start: datetime, window_end: datetime) -> list[dict]:
        groups: dict[str, list[Vehicle]] = {}
        stopped = ("dropping_off", "at_kerb", "loading", "at_stop", "parked")
        for v in self.vehicles:   # traffic = vehicles moving or held up on the road, not those parked at a kerb
            if v.road and not v.done and v.state not in stopped:
                groups.setdefault(v.road, []).append(v)
        out = []
        for name, info in self._road_index().items():
            vs = groups.get(name, [])
            speeds = [v.moved for v in vs]
            avg = round(sum(speeds) / len(speeds) / self.sim.config.interval_seconds * 3.6, 1) if speeds else None
            ratio = (avg / info["limit"]) if avg is not None else 1.0
            level = "free" if ratio >= 0.7 else "slow" if ratio >= 0.4 else "congested"
            lon, lat = self.frame.to_wgs84(*info["mid"])
            out.append({"road_id": info["road_id"], "name": name, "floor": info["level"], "lat": lat, "lon": lon,
                        "window_start": window_start, "window_end": window_end, "vehicles": len(vs),
                        "avg_speed_kmh": avg, "speed_limit_kmh": info["limit"], "congestion": level,
                        "length_m": round(info["length"], 1)})
        return out

    def _road_index(self) -> dict[str, dict]:
        if hasattr(self, "_roads"):
            return self._roads
        roads: dict[str, dict] = {}
        nodes = {n["id"]: (n["x"], n["y"]) for n in self.layout["roads"]["nodes"]}
        for a, b, length, kmh, level, name, side, _ in self.layout["roads"]["edges"]:
            if side != "landside" or not name:
                continue
            r = roads.setdefault(name, {"length": 0.0, "limit": kmh, "level": level, "pts": []})
            r["length"] += length
            r["pts"].append(nodes[a])
            if level == "L1":
                r["level"] = "L1"
        for name, r in roads.items():
            pts = r.pop("pts")
            r["mid"] = (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))
            slug = "".join(ch if ch.isalnum() else "-" for ch in name.upper()).strip("-")
            while "--" in slug:
                slug = slug.replace("--", "-")
            r["road_id"] = f"{self.P}-ROAD-{slug}"[:64]
        self._roads = {k: v for k, v in roads.items()
                       if v["length"] > 50 and "Terminal 2" not in k and not k.startswith("T2 ")}
        return self._roads

    def stand_payloads(self, now: float, window_start: datetime, window_end: datetime) -> list[dict]:
        active = {u.stand_id: u for u in self.stand_uses if u.start <= now <= u.end}
        present: dict[str, Counter] = {}
        for v in self.fleet:
            if v.stand_id and v.state == "servicing":
                present.setdefault(v.stand_id, Counter())[v.vehicle_type] += 1
        out = []
        for sid, s in self.stands.items():
            u = active.get(sid)
            lon, lat = self.frame.to_wgs84(s["x"], s["y"])
            counts = present.get(sid, Counter())
            tasks = u.tasks if u else []
            done = sum(t.state == "done" for t in tasks)
            late = sum(t.late or (t.state in ("pending", "en_route") and now > t.start + self.config.gse_late_after_seconds)
                       for t in tasks if t.start <= now)
            status = "vacant" if u is None else "delayed" if late >= 3 else "at_risk" if late >= 1 else "on_time"
            out.append({"stand_id": sid, "osm_ref": s.get("osm_ref"), "flow": s["flow"], "lat": lat, "lon": lon,
                        "floor": "L0", "window_start": window_start, "window_end": window_end,
                        "flight_id": u.flight_id if u else None, "phase": u.phase if u else "vacant",
                        "on_block": _iso(u.on_block) if u and u.on_block else None,
                        "off_block": _iso(u.off_block) if u and u.off_block else None,
                        "vehicles_present": sum(counts.values()),
                        **{f"{k}_present": counts.get(k, 0) for k in GSE_TYPES},
                        "tasks_total": len(tasks), "tasks_done": done, "tasks_late": late, "status": status})
        return out


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
