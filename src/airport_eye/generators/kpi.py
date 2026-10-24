"""Finished KPI cards for the AE-IT widgets, computed live inside the terminal simulation.

Four feeds, one minute apart (``flight.status`` when something changes):

  kpi.checkpoint  one record per chosen desk / lane / counter: queue, processing and wait time, happiness,
                  hourly wait trend (check-in, security, passport cards)
  kpi.curbside    departures and arrivals curbside: people, live vehicles, vehicles this hour and vs last hour,
                  dwell and vs last hour, vehicle type split, trolleys (departures)
  kpi.overview    airport and terminal overview for the clock hour so far: flights vs schedule, stand
                  occupancy, OTA / OTD, delays, taxi times (airport) or bags (terminal), PAX forecast vs actual
  flight.status   each flight's schedule and milestones (ALDT, AIBT, AOBT, ATOT) as they happen

What comes from the simulation: who is served at which desk and for how long, which vehicles stop at which
kerb, who stands at the kerb, every flight's in-block / off-block time, passengers and stand. What is drawn
here, per flight and repeatably: its schedule (actual minus a realistic delay), the estimate, taxi times,
forecast passengers and bags. Trolleys are an assumed model (the simulation has none).
"""

from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from ..models import FlightStatus, KpiCheckpoint, KpiCurbside, KpiOverview, Model, NonEmpty
from ..spatial import LocalFrame

if TYPE_CHECKING:   # pragma: no cover
    from .passenger import TerminalSimulation


class KpiDesks(Model):
    touchpoint_id: NonEmpty
    card: Literal["check_in", "security", "passport"]
    name: NonEmpty                       # e.g. "Domestic check-in desk" -> "Domestic check-in desk 05"
    desks: list[int] = Field(min_length=1)


class KpiTrolleys(Model):
    total: int = Field(default=420, ge=0)
    collection_every_minutes: float = Field(default=15, gt=0)
    back_after_minutes: float = Field(default=10, ge=0)      # a used trolley is collected once this old
    take_share: dict[str, float] = Field(default_factory=lambda: {"domestic": 0.25, "international": 0.6})


class KpiConfig(Model):
    """The ``kpi`` section of the tracking config; leave it out and no KPI feeds are produced."""

    airport_name: NonEmpty
    terminal_name: NonEmpty
    checkpoints: list[KpiDesks] = Field(default_factory=list)
    rolling_window_minutes: float = Field(default=15, gt=0)
    wait_threshold_minutes: float = Field(default=10, gt=0)
    kerb_people_radius_m: float = Field(default=12, gt=0)
    otp_window_minutes: float = Field(default=3, ge=0)
    taxi_in_minutes: tuple[float, float] = (4.0, 12.0)
    taxi_in_target_minutes: float = Field(default=8, gt=0)
    taxi_out_minutes: tuple[float, float] = (8.0, 20.0)
    taxi_out_target_minutes: float = Field(default=12, gt=0)
    bags_per_passenger: dict[str, float] = Field(default_factory=lambda: {"domestic": 1.1, "international": 1.6})
    trolleys: KpiTrolleys = Field(default_factory=KpiTrolleys)
    # Arrivals are generated this far ahead of their in-block time, so the schedule is known before a late
    # flight lands (a real AODB knows it days ahead). Longer than the longest delay drawn below.
    schedule_lead_minutes: float = Field(default=120, ge=80)


VEHICLE_TYPES = ("private_car", "app_cab", "taxi", "bus")


def delay_minutes(rng: random.Random) -> float:
    """Actual minus scheduled: most flights within 3 min, a tail of delays up to 75 min, a few early."""
    r = rng.random()
    if r < 0.62:
        return rng.uniform(-3, 3)
    if r < 0.72:
        return rng.uniform(-12, -3.1)
    if r < 0.93:
        return rng.uniform(3.1, 25)
    return rng.uniform(25, 75)


def happiness(wait_s: float | None, threshold_min: float) -> str | None:
    """Happy under half the threshold, Neutral up to it, Unhappy above it (until 'FRT' is defined)."""
    if wait_s is None:
        return None
    m = wait_s / 60
    return "Happy" if m < threshold_min / 2 else "Neutral" if m <= threshold_min else "Unhappy"


def pct_change(now: float | None, before: float | None) -> float | None:
    if now is None or not before:
        return None
    return round(100 * (now - before) / before, 1)


def _mean(xs: list[float], digits: int = 1) -> float | None:
    return round(sum(xs) / len(xs), digits) if xs else None


def _seg_distance(px: float, py: float, line: list) -> float:
    best = math.inf
    for (ax, ay), (bx, by) in zip(line, line[1:]):
        dx, dy = bx - ax, by - ay
        t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / ((dx * dx + dy * dy) or 1.0)))
        best = min(best, math.hypot(px - ax - t * dx, py - ay - t * dy))
    return best


@dataclass
class _Desk:
    spec: KpiDesks
    number: int
    entity: str
    lat: float
    lon: float
    floor: str
    services: deque = field(default_factory=deque)  # (start, end, wait), by start


@dataclass
class _Curbside:
    entity: str
    name: str
    side: str
    facilities: list
    lat: float
    lon: float
    floor: str
    visits: deque = field(default_factory=deque)    # [vehicle_type, stopped_at, left_at | None], by stop time
    open_visits: dict = field(default_factory=dict)  # vehicle id -> visit (its list in ``visits``)


class KpiTracker:
    """Watches one TerminalSimulation and produces the card records each minute."""

    def __init__(self, sim: "TerminalSimulation", cfg: KpiConfig):
        self.sim, self.cfg = sim, cfg
        self.P = sim.config.id_prefix
        self.tz = sim.local
        frame = LocalFrame.from_dict(sim.layout["frame"])
        self.desks: list[_Desk] = []
        for spec in cfg.checkpoints:
            queue = sim.queues.get(spec.touchpoint_id)
            if queue is None:
                raise ValueError(f"kpi checkpoint {spec.touchpoint_id} is not a simulated touchpoint")
            mine = {}
            for d in spec.desks:
                x, y = queue.channel_point(min(d, max(queue.channels, 1)) - 1)
                lon, lat = frame.to_wgs84(x, y)
                desk = _Desk(spec, d, f"{spec.touchpoint_id}-DESK-{d:02d}", lat, lon, queue.floor)
                self.desks.append(desk)
                mine[d - 1] = desk

            def served(channel: int, start: float, end: float, wait: float, mine=mine) -> None:
                if channel in mine:
                    mine[channel].services.append((start, end, wait))
            queue.on_service = served
        self.curbsides: list[_Curbside] = []
        g = sim.ground
        if g is not None:
            for side, entity, picks in (
                    ("departures", f"{self.P}-CURBSIDE-DEP",
                     [f for f in g.facilities.values() if f.facility_type == "kerb" and f.purpose == "drop_off"]),
                    ("arrivals", f"{self.P}-CURBSIDE-ARR",
                     [f for f in g.facilities.values()
                      if f.facility_type in ("kerb", "bus_stop") and f.purpose != "drop_off"])):
                if not picks:
                    continue
                line = picks[0].polyline
                lon, lat = g.frame.to_wgs84(*line[len(line) // 2])
                name = f"{side.capitalize()} curbside: " + ", ".join(f.name for f in picks)
                self.curbsides.append(_Curbside(entity, name, side, picks, lat, lon, picks[0].floor))
        self.dropoff_floors = {f.floor for c in self.curbsides if c.side == "departures" for f in c.facilities}
        self.curbside_of = {f.facility_id: c for c in self.curbsides for f in c.facilities}
        if g is not None:
            g.on_kerb = self._on_kerb
        self.stands_total = len(g.stands) if g is not None else 0
        self.stand_xy = {s: (v["x"], v["y"]) for s, v in g.stands.items()} if g is not None else {}
        self.ground_frame = g.frame if g is not None else frame
        origin = sim.layout["frame"]
        self.origin = (origin["origin_lat"], origin["origin_lon"])
        self.flights: dict[str, dict[str, Any]] = {}
        self.changed: set[str] = set()
        self.first_report = True
        t = cfg.trolleys
        self.trolleys_available, self.trolleys_out = t.total, deque()
        self.next_collection: float | None = None
        self.trolley_rng = random.Random(f"{sim.config.seed}:kpi:trolleys")

    # -- inputs from the simulation ---------------------------------------------------------------------------
    def add_flight(self, flight, use) -> None:
        if flight.journey.startswith("ga_"):
            return
        cfg = self.cfg
        rng = random.Random(f"kpi:{flight.flight_id}")
        arrival = flight.journey.endswith("arrival")
        sector = "international" if flight.journey.startswith("international") else "domestic"
        delay = delay_minutes(rng)
        taxi = rng.uniform(*(cfg.taxi_in_minutes if arrival else cfg.taxi_out_minutes))
        shares = self.sim.config.shares
        with_bags = shares.get("arrival_checked_bags", 0.6) if arrival else \
            1 - shares.get(f"{sector}_web_checkin_no_bags", 0.3)
        forecast = max(1, round(flight.passengers * rng.uniform(0.9, 1.12)))
        per = cfg.bags_per_passenger.get(sector, 1.2)
        self.flights[flight.flight_id] = {
            "flight_id": flight.flight_id, "arrival": arrival, "sector": sector, "actual": flight.time,
            "scheduled": flight.time - delay * 60, "estimated": flight.time + rng.gauss(0, 1.5) * 60,
            "delay": delay, "taxi": taxi,
            "runway": flight.time - taxi * 60 if arrival else flight.time + taxi * 60,   # ALDT / ATOT
            "pax": flight.passengers, "pax_forecast": forecast, "bags": round(flight.passengers * with_bags * per),
            "bags_forecast": round(forecast * with_bags * per), "gate_id": flight.gate_id,
            "stand_id": use.stand_id if use is not None else None,
            "stand_from": use.start if use is not None else None, "stand_to": use.end if use is not None else None,
            "status": None,
        }
        self.changed.add(flight.flight_id)

    def on_release(self, pax, now: float, floor: str) -> None:
        """A passenger gets out of a vehicle. At the departures kerb some take a trolley."""
        if not pax.journey.endswith("departure") or floor not in self.dropoff_floors:
            return
        t = self.cfg.trolleys
        self._collect(now)
        sector = "international" if pax.journey.startswith("international") else "domestic"
        if self.trolleys_available and self.trolley_rng.random() < t.take_share.get(sector, 0.3):
            self.trolleys_available -= 1
            self.trolleys_out.append(now)

    def _collect(self, now: float) -> None:
        t = self.cfg.trolleys
        every = t.collection_every_minutes * 60
        if self.next_collection is None:
            self.next_collection = math.floor(now / every) * every + every
        while self.next_collection <= now:
            cutoff = self.next_collection - t.back_after_minutes * 60
            while self.trolleys_out and self.trolleys_out[0] <= cutoff:
                self.trolleys_out.popleft()
                self.trolleys_available += 1
            self.next_collection += every

    def _on_kerb(self, event: str, facility, vehicle, now: float) -> None:
        c = self.curbside_of.get(facility.facility_id)
        if c is None:
            return
        if event == "stop":
            visit = [vehicle.vehicle_type, now, None]
            c.open_visits[vehicle.vehicle_id] = visit
            c.visits.append(visit)
        elif vehicle.vehicle_id in c.open_visits:
            c.open_visits.pop(vehicle.vehicle_id)[2] = now

    def observe(self, now: float) -> None:
        """Called every simulation step (desk services and kerb stops arrive through callbacks)."""
        self._collect(now)
        for f in self.flights.values():
            status = self._status(f, now)
            if status != f["status"]:
                f["status"] = status
                self.changed.add(f["flight_id"])

    @staticmethod
    def _status(f: dict, now: float) -> str:
        if f["arrival"]:
            return "on_stand" if now >= f["actual"] else "landed" if now >= f["runway"] else "scheduled"
        return "airborne" if now >= f["runway"] else "departed" if now >= f["actual"] else "scheduled"

    # -- output ----------------------------------------------------------------------------------------------
    def _hour_start(self, now: float) -> float:
        local = datetime.fromtimestamp(now, self.tz)
        start = local.replace(minute=0, second=0, microsecond=0).timestamp()
        return start - 3600 if local.minute == 0 and local.second == 0 else start   # 09:00 closes 08:00-09:00

    def _day_start(self, now: float) -> float:
        local = datetime.fromtimestamp(now - 1, self.tz)   # the 24:00 minute belongs to the day it closes
        return local.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    def minute_records(self, now: float, when: datetime) -> list[tuple[str, str, dict]]:
        """(event_type, entity_id, payload) for the minute ending ``now``."""
        cfg = self.cfg
        out: list[tuple[str, str, dict]] = []
        ws = when - timedelta(seconds=60)
        hour0, day0 = self._hour_start(now), self._day_start(now)
        hours = [day0 + h * 3600 for h in range(24)]
        lo = now - cfg.rolling_window_minutes * 60
        for d in self.desks:
            q = self.sim.queues[d.spec.touchpoint_id]
            n, waiting, k = q.channels, len(q.waiting), d.number - 1
            share = (waiting // n + (1 if k < waiting % n else 0)) if k < n else 0   # its share of the shared queue
            started = [s for s in d.services if lo < s[0] <= now]
            finished = [s for s in d.services if lo < s[1] <= now]
            wait = _mean([s[2] for s in started])
            hourly = []
            for h in hours:
                if h > now:
                    hourly.append(None)
                    continue
                ws_h = [s[2] for s in d.services if h < s[0] <= min(h + 3600, now)]
                hourly.append(round(sum(ws_h) / len(ws_h) / 60, 2) if ws_h else None)
            this_h = [s[2] for s in d.services if hour0 < s[0] <= now]
            prev_h = [s[2] for s in d.services if hour0 - 3600 < s[0] <= hour0]
            payload = KpiCheckpoint(
                checkpoint_id=d.entity, touchpoint_id=d.spec.touchpoint_id, card=d.spec.card, desk_number=d.number,
                desk_name=f"{d.spec.name} {d.number:02d}", floor=d.floor, lat=d.lat, lon=d.lon,
                window_start=ws, window_end=when, desk_open=k < n, queue_length=share,
                avg_processing_time_seconds=_mean([s[1] - s[0] for s in finished]),
                avg_wait_time_seconds=wait, happiness_index=happiness(wait, cfg.wait_threshold_minutes),
                passengers_served=len(finished), rolling_window_minutes=cfg.rolling_window_minutes,
                wait_threshold_minutes=cfg.wait_threshold_minutes,
                hour_avg_wait_minutes=round(sum(this_h) / len(this_h) / 60, 2) if this_h else None,
                previous_hour_avg_wait_minutes=round(sum(prev_h) / len(prev_h) / 60, 2) if prev_h else None,
                today_hourly_avg_wait_minutes=hourly).model_dump(mode="json")
            out.append(("kpi.checkpoint", d.entity, payload))

        active = [p for p in self.sim.active if not p.done]
        into = now - hour0
        for c in self.curbsides:
            def stops(a: float, b: float) -> list:
                return [v for v in c.visits if a < v[1] <= b]

            def dwell(a: float, b: float) -> float | None:
                return _mean([v[2] - v[1] for v in c.visits if v[2] is not None and a < v[2] <= b])
            this_hour = stops(hour0, now)
            d_now, d_before = dwell(hour0, now), dwell(hour0 - 3600, hour0)
            people = sum(1 for p in active if any(
                p.floor == f.floor and _seg_distance(p.x, p.y, f.polyline) <= cfg.kerb_people_radius_m
                for f in c.facilities))
            dep = c.side == "departures"
            payload = KpiCurbside(
                curbside_id=c.entity, name=c.name, side=c.side, floor=c.floor, lat=c.lat, lon=c.lon,
                window_start=ws, window_end=when, people_count=people,
                live_vehicle_count=sum(len(f.occupants) for f in c.facilities),
                vehicles_this_hour=len(this_hour),
                vehicles_vs_previous_hour_pct=pct_change(len(this_hour), len(stops(hour0 - 3600, hour0 - 3600 + into))),
                avg_dwell_seconds=d_now, avg_dwell_vs_previous_hour_pct=pct_change(d_now, d_before),
                **{f"this_hour_{t}": sum(1 for v in this_hour if v[0] == t) for t in VEHICLE_TYPES},
                trolleys_available=self.trolleys_available if dep else None,
                trolleys_total=cfg.trolleys.total if dep else None,
                today_hourly_vehicles=[len(stops(h, min(h + 3600, now))) if h <= now else None for h in hours],
            ).model_dump(mode="json")
            out.append(("kpi.curbside", c.entity, payload))

        known = list(self.flights.values())
        occupied = len({f["stand_id"] for f in known if f["stand_id"] and f["stand_from"] <= now < f["stand_to"]})
        lat, lon = self.origin
        for scope, entity, name in (("airport", f"{self.P}-AIRPORT-OVERVIEW", cfg.airport_name),
                                    ("terminal", f"{self.sim.config.terminal_id}-OVERVIEW", cfg.terminal_name)):
            out.append(("kpi.overview", entity, self._overview(scope, entity, name, known, now, hour0, hours,
                                                               occupied, lat, lon, ws, when)))

        report_all = self.first_report
        self.first_report = False
        for fid in sorted(self.flights if report_all else self.changed):
            f = self.flights[fid]
            if f["status"] is None:
                f["status"] = self._status(f, now)
            out.append(("flight.status", fid, self._flight_payload(f, now)))
        self.changed.clear()
        self._prune(day0)
        return out

    def _overview(self, scope, entity, name, known, now, hour0, hours, occupied, lat, lon, ws, when) -> dict:
        cfg = self.cfg
        arr = [f for f in known if f["arrival"]]
        dep = [f for f in known if not f["arrival"]]

        def window(fs, key, a, b):
            return [f for f in fs if a < f[key] <= b]
        a_act, d_act = window(arr, "actual", hour0, now), window(dep, "actual", hour0, now)
        a_sch, d_sch = window(arr, "scheduled", hour0, now), window(dep, "scheduled", hour0, now)

        def otp(fs):
            return round(100 * sum(abs(f["delay"]) <= cfg.otp_window_minutes for f in fs) / len(fs), 1) if fs else None
        extra: dict[str, Any] = {}
        if scope == "airport":
            ti, to = _mean([f["taxi"] for f in a_act]), _mean([f["taxi"] for f in d_act])
            extra = {"avg_taxi_in_minutes": ti, "taxi_in_target_minutes": cfg.taxi_in_target_minutes,
                     "taxi_in_vs_target_pct": pct_change(ti, cfg.taxi_in_target_minutes),
                     "avg_taxi_out_minutes": to, "taxi_out_target_minutes": cfg.taxi_out_target_minutes,
                     "taxi_out_vs_target_pct": pct_change(to, cfg.taxi_out_target_minutes)}
        else:
            ab, db = sum(f["bags"] for f in a_act), sum(f["bags"] for f in d_act)
            at, dt = sum(f["bags_forecast"] for f in a_sch), sum(f["bags_forecast"] for f in d_sch)
            extra = {"arrival_bags": ab, "arrival_bags_threshold": at, "arrival_bags_vs_threshold_pct": pct_change(ab, at),
                     "departure_bags": db, "departure_bags_threshold": dt,
                     "departure_bags_vs_threshold_pct": pct_change(db, dt)}

        def hourly(fs, key, value):
            return [sum(f[value] for f in fs if h < f[key] <= min(h + 3600, now)) if h <= now else 0 for h in hours]
        return KpiOverview(
            overview_id=entity, scope=scope, name=name, lat=lat, lon=lon, window_start=ws, window_end=when,
            hour_start=datetime.fromtimestamp(hour0, timezone.utc),
            arrival_flights=len(a_act), arrival_flights_scheduled=len(a_sch),
            arrival_vs_schedule_pct=pct_change(len(a_act), len(a_sch)),
            departure_flights=len(d_act), departure_flights_scheduled=len(d_sch),
            departure_vs_schedule_pct=pct_change(len(d_act), len(d_sch)),
            stands_occupied=occupied, stands_total=self.stands_total, ota_pct=otp(a_act), otd_pct=otp(d_act),
            avg_arrival_delay_minutes=_mean([f["delay"] for f in a_act]),
            avg_departure_delay_minutes=_mean([f["delay"] for f in d_act]),
            arrival_pax_forecast=sum(f["pax_forecast"] for f in a_sch), arrival_pax_actual=sum(f["pax"] for f in a_act),
            departure_pax_forecast=sum(f["pax_forecast"] for f in d_sch),
            departure_pax_actual=sum(f["pax"] for f in d_act),
            today_hourly_arrival_pax_forecast=hourly(arr, "scheduled", "pax_forecast"),
            today_hourly_arrival_pax_actual=hourly(arr, "actual", "pax"),
            today_hourly_departure_pax_forecast=hourly(dep, "scheduled", "pax_forecast"),
            today_hourly_departure_pax_actual=hourly(dep, "actual", "pax"),
            **extra).model_dump(mode="json")

    def _flight_payload(self, f: dict, now: float) -> dict:
        def at(t: float | None) -> datetime | None:
            return datetime.fromtimestamp(t, timezone.utc) if t is not None and t <= now else None
        happened = now >= f["actual"]
        lat = lon = None
        if f["stand_id"] in self.stand_xy:
            lon, lat = self.ground_frame.to_wgs84(*self.stand_xy[f["stand_id"]])
        a = f["arrival"]
        return FlightStatus(
            flight_id=f["flight_id"], direction="arrival" if a else "departure", sector=f["sector"],
            status=f["status"], stand_id=f["stand_id"], gate_id=f["gate_id"], lat=lat, lon=lon,
            scheduled_time=datetime.fromtimestamp(f["scheduled"], timezone.utc),
            estimated_time=datetime.fromtimestamp(f["estimated"], timezone.utc),
            landing_time=at(f["runway"]) if a else None, in_block_time=at(f["actual"]) if a else None,
            off_block_time=None if a else at(f["actual"]), takeoff_time=None if a else at(f["runway"]),
            delay_minutes=round(f["delay"], 1) if happened else None,
            on_time=(abs(f["delay"]) <= self.cfg.otp_window_minutes) if happened else None,
            taxi_minutes=round(f["taxi"], 1) if now >= max(f["actual"], f["runway"]) else None,
            pax_forecast=f["pax_forecast"], pax_actual=f["pax"] if happened else 0,
            bags=f["bags"] if happened else 0).model_dump(mode="json")

    def _prune(self, day0: float) -> None:
        keep = day0 - 2 * 3600   # today, plus the previous hours the "vs previous hour" figures look at
        for d in self.desks:
            while d.services and d.services[0][0] < keep:
                d.services.popleft()
        for c in self.curbsides:
            while c.visits and c.visits[0][1] < keep and c.visits[0][2] is not None:
                c.visits.popleft()
        for fid in [k for k, f in self.flights.items() if max(f["actual"], f["runway"]) < keep - 86400]:
            del self.flights[fid]
