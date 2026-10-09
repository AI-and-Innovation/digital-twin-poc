"""Run passenger tracking live (Kafka or files) or for a fixed time range (files).

Same record shape on every path: the flat Esri record of a ``passenger.position``
event, with ``longitude``/``latitude`` converted from the terminal's local x/y.
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
from .queue_files import _replace, to_csv, to_geojson
from .streaming import floor_to_interval, publish_paced

POSITION_TOPIC_TYPE = "passenger.position"
LEADING_COLUMNS = [
    "passenger_id", "journey", "activity", "touchpoint_id", "flight_id", "gate_id", "event_time",
    "longitude", "latitude", "floor", "indoor_zone_id", "indoor_x", "indoor_y", "speed_mps", "heading_deg",
    "movement_state",
]


def position_columns(record: dict[str, Any]) -> list[str]:
    return [c for c in LEADING_COLUMNS if c in record] + [c for c in record if c not in LEADING_COLUMNS]


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
                        clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                        log: Callable[[str], None] = print) -> dict[str, int]:
    """Publish one batch of positions per interval, in real time (``speed`` compresses time)."""
    if speed <= 0:
        raise ValueError("speed must be > 0")
    config, layout, inventory, _ = load_tracking(config_path)
    begin, end = _window(config, start, minutes, live=True)
    log(f"Passenger tracking: one batch every {config.interval_seconds}s, speed x{speed:g}; "
        f"warming up {config.warmup_minutes:g} simulated minutes first")
    if create_topics:
        topic = publisher.topic_for_type(POSITION_TOPIC_TYPE)
        created = publisher.ensure_topics([topic])
        log(f"Topic ready: {topic}" + (" (created)" if created else ""))
    ticks = generate_ticks(config, layout, inventory, begin, end)
    publish_paced(ticks, publisher, origin=begin, speed=speed, clock=clock, sleep=sleep, log=log,
                  every=max(1, 60 // config.interval_seconds))
    return publisher.stats()


class PositionFileWriter:
    """Duck-types the publisher calls ``publish_paced`` makes: each flush rewrites the latest files."""

    def __init__(self, out_dir: str | Path, locator: ZoneLocator, history: bool = False):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.locator = locator
        self.history = self.out_dir / "passenger_positions_history.csv" if history else None
        self._batch: list[dict[str, Any]] = []
        self._names: list[str] | None = None
        self.queued = self.delivered = 0

    def publish(self, event: EventEnvelope) -> None:
        self._batch.append(to_esri_record(event, self.locator))
        self.queued += 1

    def flush(self, timeout: float = 0.0) -> None:
        names = position_columns(self._batch[0]) if self._batch else (self._names or LEADING_COLUMNS)
        _replace(self.out_dir / "passenger_positions_latest.csv", to_csv(self._batch, names))
        _replace(self.out_dir / "passenger_positions_latest.geojson", json.dumps(to_geojson(self._batch, names)))
        if self.history is not None and self._batch:
            first = self._names is None
            self._names = self._names or names
            with self.history.open("w" if first else "a", encoding="utf-8", newline="") as handle:
                handle.write(to_csv(self._batch, self._names, header=first))
        self.delivered += len(self._batch)
        self._batch = []

    def stats(self) -> dict[str, int]:
        return {"queued": self.queued, "delivered": self.delivered, "failed": 0}


def write_tracking_range(config_path: str | Path, locator: ZoneLocator, out_dir: str | Path, *,
                         start: datetime | None = None, minutes: float | None = None) -> tuple[list[Path], int]:
    """Simulate a time range and write every position as CSV and JSONL (one record per line)."""
    config, layout, inventory, _ = load_tracking(config_path)
    begin, end = _window(config, start, minutes, live=False)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = "passenger_positions_" + begin.strftime("%Y%m%dT%H%MZ")
    csv_path, jsonl_path = out / f"{stem}.csv", out / f"{stem}.jsonl"
    count = 0
    names: list[str] | None = None
    with csv_path.open("w", encoding="utf-8", newline="") as csv_out, \
            jsonl_path.open("w", encoding="utf-8", newline="\n") as jsonl_out:
        for _, events in generate_ticks(config, layout, inventory, begin, end):
            records = [to_esri_record(event, locator) for event in events]
            if not records:
                continue
            if names is None:
                names = position_columns(records[0])
                csv_out.write(to_csv([], names))
            csv_out.write(to_csv(records, names, header=False))
            for record in records:
                jsonl_out.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += len(records)
    return [csv_path, jsonl_path], count
