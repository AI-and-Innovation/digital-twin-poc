"""Per-airport file sets. Hyderabad (RGIA T1) is the default; Delhi (DIAL, IGI T3) is kept
apart in config/del/, with its own output folder, local Kafka topic prefix and REST proxy topics.

``--airport del`` on pax-stream, pax-files and the map viewer swaps every path that was left
at its Hyderabad default for the Delhi one; paths given explicitly are kept.
"""

from __future__ import annotations

import argparse

AIRPORTS: dict[str, dict] = {
    "hyd": {"tracking_config": "config/passenger_tracking.json", "zones": "config/terminal_layout.json",
            "ground": "config/ground_layout.json", "rest_proxy": "config/rest_proxy.json", "out": "output",
            "kafka_prefix": "airport-eye.poc"},
    "del": {"tracking_config": "config/del/passenger_tracking.json", "zones": "config/del/terminal_layout.json",
            "ground": "config/del/ground_layout.json", "rest_proxy": "config/del/rest_proxy.json", "out": "output/del",
            "kafka_prefix": "airport-eye.poc.del"},
}


# What the dashboards show: names and the 3D camera (centre lon/lat, zoom, bearing of the map's top).
VIEWS: dict[str, dict] = {
    "hyd": {"airport": "RGIA Hyderabad", "terminal": "Terminal 1", "center": [78.4290, 17.2356], "zoom": 16.6,
            "bearing": -18, "central": [78.4288, 17.2357], "central_zoom": 18, "central_bearing": -25},
    "del": {"airport": "Delhi IGI", "terminal": "Terminal 3", "center": [77.0842, 28.5549], "zoom": 15.9,
            "bearing": 60, "central": [77.0842, 28.5549], "central_zoom": 17.6, "central_bearing": 55},
}


def add_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--airport", choices=sorted(AIRPORTS), default="hyd",
                        help="hyd = RGIA Terminal 1 (default); del = Delhi IGI Terminal 3 (config/del/)")


def apply(args: argparse.Namespace) -> argparse.Namespace:
    """Point every argument still at its Hyderabad default to the chosen airport's file."""
    chosen, default = AIRPORTS[args.airport], AIRPORTS["hyd"]
    for key in ("tracking_config", "zones", "ground", "rest_proxy", "out"):
        if getattr(args, key, None) == default[key]:
            setattr(args, key, chosen[key])
    return args
