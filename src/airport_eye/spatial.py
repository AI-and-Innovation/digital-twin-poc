"""Spatial contracts. Supply surveyed transforms, terminal graphs and airside routes later."""

from .models import IndoorPosition, OutdoorPosition


def indoor_to_outdoor(position: IndoorPosition) -> OutdoorPosition:
    raise NotImplementedError("Requires a calibrated local-frame to geodetic transform")


def route_between(start_node: str, end_node: str) -> list[IndoorPosition]:
    raise NotImplementedError("Requires a terminal walkable graph and floor transitions")
