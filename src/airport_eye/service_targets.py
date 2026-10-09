"""Service targets and the derived queue-health attributes for touchpoint records.

Three things are kept apart on purpose, as the guidance asks:
  * operational target  - what operations aims for (configurable, a starting point);
  * AERA existing benchmark - the regulator's current guideline;
  * AERA proposed benchmark - an August 2025 consultation proposal, NOT enacted.

Derived values (all computed per touchpoint per window):
  capacity/min      = service_channels * 60 / mean_service_seconds
  queue_limit       = capacity/min * target_wait_minutes        (people)
  estimated_wait    = queue_length / capacity/min * 60          (seconds)
  p95_wait          = 95th percentile (nearest rank) of waits of services that started
                      in the last p95_window_minutes
  ratio             = max(estimated_wait, p95_wait) / target_wait
  status            = no_target | normal (< busy_ratio) | busy (< 1) | congested (>= 1)
  sla_breached      = ratio >= 1  (against the OPERATIONAL target, not the AERA values)
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .models import Model, NonEmpty
from .touchpoint_config import Touchpoint

Status = Literal["no_target", "normal", "busy", "congested"]


class TargetClass(Model):
    label: NonEmpty
    target_wait_minutes: float | None = Field(gt=0)
    aera_existing_minutes: float | None = Field(gt=0)
    aera_proposed_minutes: float | None = Field(gt=0)
    processing_assumption: str


class TouchpointTarget(Model):
    class_: NonEmpty = Field(alias="class")
    flow: NonEmpty
    basis: Literal["recommended", "assumed_from_similar", "none"]


class ServiceTargets(Model):
    note: str = Field(alias="_note")
    p95_window_minutes: float = Field(gt=0)
    p95_min_samples: int = Field(ge=1)
    busy_ratio: float = Field(gt=0, lt=1)
    classes: dict[str, TargetClass]
    iata_reference_minutes: dict
    touchpoints: dict[str, TouchpointTarget]

    model_config = Model.model_config | {"populate_by_name": True}

    @model_validator(mode="after")
    def known_classes(self):
        for tp_id, entry in self.touchpoints.items():
            if entry.class_ not in self.classes:
                raise ValueError(f"{tp_id}: unknown class {entry.class_}")
        return self

    @classmethod
    def load(cls, path: str | Path) -> "ServiceTargets":
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))


def _seconds(minutes: float | None) -> float | None:
    return None if minutes is None else round(minutes * 60, 1)


def nearest_rank(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def queue_attributes(
    targets: ServiceTargets,
    tp: Touchpoint,
    *,
    queue_length: int,
    in_service: int,
    processed: int,
    interval_seconds: int,
    recent_waits: list[float],
) -> dict:
    """Extra record fields for one touchpoint window. Missing config entries give an empty dict."""
    entry = targets.touchpoints.get(tp.touchpoint_id)
    if entry is None:
        return {}
    cls = targets.classes[entry.class_]
    sim = tp.simulation
    mean_service = (sim.service_time_min_seconds + sim.service_time_max_seconds) / 2
    capacity_per_min = sim.service_channels * 60 / mean_service
    target_s = _seconds(cls.target_wait_minutes)
    estimated = round(queue_length / capacity_per_min * 60, 1)
    p95 = round(nearest_rank(recent_waits, 0.95), 1) if len(recent_waits) >= targets.p95_min_samples else None
    out = {
        "touchpoint_name": tp.name,
        "flow": entry.flow,
        "service_channels": sim.service_channels,
        "utilization_pct": round(100 * in_service / sim.service_channels, 1),
        "throughput_per_hour": round(processed * 3600 / interval_seconds, 1),
        "estimated_wait_seconds": estimated,
        "p95_wait_seconds": p95,
        "target_wait_seconds": target_s,
        "aera_existing_wait_seconds": _seconds(cls.aera_existing_minutes),
        "aera_proposed_wait_seconds": _seconds(cls.aera_proposed_minutes),
        "target_basis": entry.basis,
        "queue_limit": None,
        "status": "no_target",
        "sla_breached": None,
    }
    if target_s:
        out["queue_limit"] = int(capacity_per_min * target_s / 60)
        ratio = max(estimated, p95 or 0.0) / target_s
        out["status"] = "congested" if ratio >= 1 else "busy" if ratio >= targets.busy_ratio else "normal"
        out["sla_breached"] = ratio >= 1
    return out
