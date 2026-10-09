"""Passenger queue feed as plain files (CSV, GeoJSON, JSON). No Kafka needed.

Two modes:

* ``write_range`` simulates a time range and writes it in one go.
* ``QueueFileWriter`` stands in for the Kafka publisher in ``run_stream``: each
  minute it rewrites ``passenger_queue_latest.*`` (atomically, so a reader never
  sees a half-written file) and appends the minute to
  ``passenger_queue_history.csv``.

Records are the same flat Esri records the Kafka topic carries; the queue fields
come first so the CSV reads well in Excel.
"""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .esri import ZoneLocator, to_esri_record
from .generators.passenger_touchpoint import generate
from .models import EventEnvelope
from .streaming import floor_to_interval
from .touchpoint_config import load_run, load_targets

LEADING_COLUMNS = [
    "touchpoint_id", "touchpoint_name", "touchpoint_type", "flow", "window_start", "window_end",
    "queue_length", "estimated_wait_seconds", "avg_wait_time_seconds", "p95_wait_seconds",
    "avg_processing_time_seconds", "processed_passengers", "throughput_since_midnight",
    "passengers_in_service", "service_channels", "utilization_pct", "throughput_per_hour",
    "target_wait_seconds", "queue_limit", "status", "sla_breached",
    "longitude", "latitude", "floor", "zone_id",
]


def columns(records: list[dict[str, Any]]) -> list[str]:
    seen = dict.fromkeys(LEADING_COLUMNS)
    for record in records:
        seen.update(dict.fromkeys(record))
    return [name for name in seen if any(name in r for r in records)]


def _cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def to_csv(records: list[dict[str, Any]], names: list[str], header: bool = True) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=names, lineterminator="\n")
    if header:
        writer.writeheader()
    for record in records:
        writer.writerow({name: _cell(record.get(name)) for name in names})
    return buffer.getvalue()


def to_geojson(records: list[dict[str, Any]], names: list[str]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature",
         "geometry": ({"type": "Point", "coordinates": [r["longitude"], r["latitude"]]}
                      if r.get("longitude") is not None and r.get("latitude") is not None else None),
         "properties": {name: r.get(name) for name in names}}
        for r in records]}


def _replace(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_files(records: list[dict[str, Any]], out_dir: Path, stem: str) -> list[Path]:
    """Write ``<stem>.csv``, ``<stem>.geojson`` and ``<stem>.json``; returns the paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    names = columns(records)
    ordered = [{name: r.get(name) for name in names} for r in records]
    paths = [out_dir / f"{stem}.csv", out_dir / f"{stem}.geojson", out_dir / f"{stem}.json"]
    _replace(paths[0], to_csv(records, names))
    _replace(paths[1], json.dumps(to_geojson(records, names), indent=1))
    _replace(paths[2], json.dumps(ordered, indent=1))
    return paths


def write_range(run_config_path: str | Path, locator: ZoneLocator, out_dir: str | Path, *,
                start: datetime | None = None, hours: float | None = None) -> tuple[list[Path], int]:
    """Simulate ``hours`` from ``start`` (defaults: the run config's own range) and write the files."""
    config, inventory, _ = load_run(run_config_path)
    targets = load_targets(run_config_path, config)
    begin = floor_to_interval(start, config.interval_seconds) if start else config.start_time
    if hours is None:
        length = config.end_time - config.start_time
    else:
        windows = round(hours * 3600 / config.interval_seconds)
        if windows < 1:
            raise ValueError("hours must cover at least one interval")
        length = timedelta(seconds=windows * config.interval_seconds)
    config = config.model_copy(update={"start_time": begin, "end_time": begin + length})
    records = [to_esri_record(event, locator) for event in generate(config, inventory, targets)]
    stem = "passenger_queue_" + begin.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    return write_files(records, Path(out_dir), stem), len(records)


class QueueFileWriter:
    """Duck-types the KafkaPublisher calls ``run_stream`` makes (create_topics=False)."""

    def __init__(self, out_dir: str | Path, locator: ZoneLocator):
        self.out_dir = Path(out_dir)
        self.locator = locator
        self.history = self.out_dir / "passenger_queue_history.csv"
        self._batch: list[dict[str, Any]] = []
        self._names: list[str] | None = None
        self.queued = 0
        self.delivered = 0

    def publish(self, event: EventEnvelope) -> None:
        self._batch.append(to_esri_record(event, self.locator))
        self.queued += 1

    def flush(self, timeout: float = 0.0) -> None:
        if not self._batch:
            return
        write_files(self._batch, self.out_dir, "passenger_queue_latest")
        if self._names is None:
            self._names = columns(self._batch)
            self.history.write_text(to_csv(self._batch, self._names), encoding="utf-8")
        else:
            with self.history.open("a", encoding="utf-8", newline="") as handle:
                handle.write(to_csv(self._batch, self._names, header=False))
        self.delivered += len(self._batch)
        self._batch = []

    def stats(self) -> dict[str, int]:
        return {"queued": self.queued, "delivered": self.delivered, "failed": 0}
