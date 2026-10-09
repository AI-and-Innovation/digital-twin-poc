"""Flat, Esri-friendly projection of EventEnvelope records.

ArcGIS Velocity and GeoEvent Server ingest one flat record per Kafka message
and build geometry from numeric x/y fields. The canonical envelope nests the
payload, so this module derives a flat record without changing the contract.

Conventions (confirm against the Esri team's actual feed definition):
  * payload fields are hoisted to the top level; nested objects are joined
    with "_" (same convention as Velocity's Flatten option);
  * ``entity_id`` is the track/ID field;
  * ``event_time_epoch_ms`` (UTC epoch milliseconds) is the time field, so no
    date-format string is needed; ISO-8601 strings are kept alongside;
  * ``longitude``/``latitude`` are WGS84 (EPSG:4326) and only present when a
    location can be resolved. Indoor x/y are NOT converted, because
    spatial.indoor_to_outdoor needs a calibrated transform that does not exist.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import EventEnvelope

ENVELOPE_FIELDS = (
    "event_id", "event_type", "schema_version", "airport_id", "terminal_id",
    "source_system", "entity_id", "sequence_no",
)


class ZoneLocator:
    """Maps zone_id -> WGS84 point using config/zone_locations.json."""

    # Several touchpoints share one zone. Without a spread they would all draw as a
    # single dot, so each gets a small, repeatable offset around the zone point
    # (about 6-14 m). An explicit entry under "touchpoints" in the file wins.
    SPREAD_DEG = (0.00006, 0.00013)

    def __init__(self, zones: dict[str, dict[str, Any]], touchpoints: dict[str, dict[str, Any]] | None = None):
        self.zones = zones
        self.touchpoints = touchpoints or {}

    @classmethod
    def from_file(cls, path: str | Path) -> "ZoneLocator":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(data["zones"], data.get("touchpoints"))

    def locate(self, zone_id: str | None, entity_id: str | None = None) -> dict[str, Any] | None:
        if entity_id and entity_id in self.touchpoints:
            return self.touchpoints[entity_id]
        zone = self.zones.get(zone_id) if zone_id else None
        if zone is None or not entity_id:
            return zone
        digest = hashlib.sha256(entity_id.encode("utf-8")).digest()
        angle = int.from_bytes(digest[:4], "big") / 2**32 * 2 * math.pi
        low, high = self.SPREAD_DEG
        radius = low + int.from_bytes(digest[4:8], "big") / 2**32 * (high - low)
        return {**zone,
                "longitude": round(zone["longitude"] + radius * math.cos(angle), 7),
                "latitude": round(zone["latitude"] + radius * math.sin(angle), 7)}


def _epoch_ms(value: datetime) -> int:
    return int(round(value.timestamp() * 1000))


def _flatten(prefix: str, value: Any, out: dict[str, Any]) -> None:
    if isinstance(value, dict):
        for key, inner in value.items():
            _flatten(f"{prefix}_{key}" if prefix else key, inner, out)
    elif isinstance(value, list):
        # Arrays stay as JSON strings, as Velocity does by default.
        out[prefix] = json.dumps(value, separators=(",", ":"))
    else:
        out[prefix] = value


def to_esri_record(event: EventEnvelope, locator: ZoneLocator | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {name: getattr(event, name) for name in ENVELOPE_FIELDS}
    record["event_id"] = str(event.event_id)
    record["event_time"] = event.event_time.isoformat().replace("+00:00", "Z")
    record["ingestion_time"] = event.ingestion_time.isoformat().replace("+00:00", "Z")
    record["event_time_epoch_ms"] = _epoch_ms(event.event_time)
    record["ingestion_time_epoch_ms"] = _epoch_ms(event.ingestion_time)

    payload = json.loads(json.dumps(event.payload, default=str))  # validated dict -> plain JSON types
    flat: dict[str, Any] = {}
    _flatten("", payload, flat)
    clash = set(flat) & set(record)
    if clash:
        raise ValueError(f"payload fields collide with envelope fields: {sorted(clash)}")
    record.update(flat)

    lon = lat = None
    floor = None
    if "outdoor_lat" in flat and "outdoor_lon" in flat:  # asset registry / aircraft
        lon, lat = flat["outdoor_lon"], flat["outdoor_lat"]
    elif "lat" in flat and "lon" in flat:
        lon, lat = flat["lon"], flat["lat"]
    elif locator is not None:
        zone = locator.locate(flat.get("zone_id") or flat.get("indoor_zone_id"),
                              flat.get("touchpoint_id"))
        if zone:
            lon, lat, floor = zone["longitude"], zone["latitude"], zone.get("floor")
    if lon is not None and lat is not None:
        record["longitude"] = lon
        record["latitude"] = lat
        if floor is not None and "floor" not in record:
            record["floor"] = floor
    return record
