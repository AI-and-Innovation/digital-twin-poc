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
    location can be resolved. Indoor x/y (passenger positions) are converted with
    the terminal layout's local frame (config/terminal_layout.json);
  * ``elevation_m`` is the floor's height above the terminal ground floor (local Z);
  * ``reference_elevation_m`` is the surveyed (or POC) height of L0 / the local
    origin above the vertical datum; ``global_elevation_m`` =
    ``reference_elevation_m`` + ``elevation_m`` (absolute Z for a global 3D scene);
  * ``web_mercator_x`` / ``web_mercator_y`` are the same point in EPSG:3857
    (``spatial_reference_wkid`` = 3857).
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import EventEnvelope
from .spatial import LocalFrame, polygon_centroid, to_web_mercator

ENVELOPE_FIELDS = (
    "event_id", "event_type", "schema_version", "airport_id", "terminal_id",
    "source_system", "entity_id", "sequence_no",
)


class ZoneLocator:
    """Maps touchpoints, zones and indoor x/y to WGS84.

    Reads either the terminal layout (config/terminal_layout.json: local-metre
    positions plus a frame) or the older flat zone file (zone_id -> lon/lat).
    """

    # Several touchpoints share one zone. Without a spread they would all draw as a
    # single dot, so each gets a small, repeatable offset around the zone point
    # (about 6-14 m). An explicit entry under "touchpoints" in the file wins.
    SPREAD_DEG = (0.00006, 0.00013)

    def __init__(self, zones: dict[str, dict[str, Any]], touchpoints: dict[str, dict[str, Any]] | None = None,
                 frame: LocalFrame | None = None, floor_heights: dict[str, float] | None = None,
                 reference_elevation_m: float | None = None):
        self.zones = zones
        self.touchpoints = touchpoints or {}
        self.frame = frame
        self.floor_heights = floor_heights or {}  # floor -> metres above the ground floor
        # Prefer an explicit value; otherwise take it from the layout frame.
        self.reference_elevation_m = (
            reference_elevation_m if reference_elevation_m is not None
            else (frame.reference_elevation_m if frame is not None else None))

    @classmethod
    def from_file(cls, path: str | Path) -> "ZoneLocator":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if "frame" in data:
            return cls.from_layout(data)
        return cls(data["zones"], data.get("touchpoints"),
                   reference_elevation_m=data.get("reference_elevation_m"))

    @classmethod
    def from_layout(cls, layout: dict[str, Any]) -> "ZoneLocator":
        frame = LocalFrame.from_dict(layout["frame"])

        def point(x: float, y: float, floor: str) -> dict[str, Any]:
            lon, lat = frame.to_wgs84(x, y)
            return {"longitude": lon, "latitude": lat, "floor": floor}

        zones = {}
        for zone in layout["zones"]:
            largest = max(zone["polygons"], key=lambda p: abs(sum(
                x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in zip(p, p[1:] + p[:1]))))
            zones[zone["zone_id"]] = point(*polygon_centroid(largest), zone["floor"])
        touchpoints = {tp: point(t["x"], t["y"], t["floor"]) for tp, t in layout["touchpoints"].items()}
        heights = {name: f["z"] for name, f in layout.get("floors", {}).items()}
        return cls(zones, touchpoints, frame, heights, frame.reference_elevation_m)

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
    elif locator is not None and locator.frame is not None and "indoor_x" in flat and "indoor_y" in flat \
            and flat.get("indoor_coordinate_frame_id") == locator.frame.frame_id:
        lon, lat = locator.frame.to_wgs84(flat["indoor_x"], flat["indoor_y"])
        floor = flat.get("indoor_floor")
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
        # Height above the terminal's ground floor (local Z).
        if locator is not None and record.get("floor") in locator.floor_heights:
            record["elevation_m"] = locator.floor_heights[record["floor"]]
        local_z = record.get("elevation_m")
        if local_z is None:
            local_z = flat.get("indoor_z")
        # Absolute Z for a global / Web Mercator scene: REFERENCE_Z + local floor height.
        if locator is not None and locator.reference_elevation_m is not None and local_z is not None:
            record["reference_elevation_m"] = locator.reference_elevation_m
            record["global_elevation_m"] = round(locator.reference_elevation_m + float(local_z), 3)
        mx, my = to_web_mercator(float(lon), float(lat))
        record["web_mercator_x"] = mx
        record["web_mercator_y"] = my
        record["spatial_reference_wkid"] = 3857
    return record
