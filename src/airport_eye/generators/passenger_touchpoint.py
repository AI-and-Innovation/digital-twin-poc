"""Independent FIFO queues with Poisson arrivals and uniform service durations.

Produces measured completed-cohort wait/service averages, not independent random
KPI values. No real passenger identifiers or connected passenger journeys.
"""

import hashlib
import heapq
import json
import math
import random
from collections import deque
from collections.abc import Iterator
from datetime import timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import NAMESPACE_URL, uuid5

from ..models import EventEnvelope, PassengerTouchpoint
from ..touchpoint_config import Touchpoint, TouchpointInventory, TouchpointRunConfig, load_run


class QueueState:
    def __init__(self, touchpoint: Touchpoint, seed: int):
        self.profile = touchpoint.simulation
        # Separate stable random stream per ID; adding/reordering entries doesn't
        # change existing touchpoints' values.
        self.rng = random.Random(f"{seed}:{touchpoint.touchpoint_id}")
        self.waiting = deque([0.0] * self.profile.initial_queue_length)
        self.serving: list[tuple[float, float, float]] = []
        self.arrivals = 0
        self.completed = 0
        self.next_arrival = self._arrival_delay()
        self._start_services(0.0)

    def _arrival_delay(self) -> float:
        rate = self.profile.arrival_rate_per_hour / 3600
        return self.rng.expovariate(rate) if rate else math.inf

    def _start_services(self, now: float) -> None:
        while self.waiting and len(self.serving) < self.profile.service_channels:
            entered = self.waiting.popleft()
            service = self.rng.uniform(self.profile.service_time_min_seconds,
                                       self.profile.service_time_max_seconds)
            heapq.heappush(self.serving, (now + service, now - entered, service))

    def advance(self, cutoff: float) -> tuple[int, float | None, float | None]:
        count = 0
        wait_sum = service_sum = 0.0
        while True:
            departure = self.serving[0][0] if self.serving else math.inf
            if min(departure, self.next_arrival) > cutoff:
                break
            if departure <= self.next_arrival:
                now, wait, service = heapq.heappop(self.serving)
                count += 1
                self.completed += 1
                wait_sum += wait
                service_sum += service
                self._start_services(now)
            else:
                now = self.next_arrival
                self.waiting.append(now)
                self.arrivals += 1
                self.next_arrival = now + self._arrival_delay()
                self._start_services(now)
        return (count, round(wait_sum / count, 3) if count else None,
                round(service_sum / count, 3) if count else None)


def generate(config: TouchpointRunConfig, inventory: TouchpointInventory) -> Iterator[EventEnvelope]:
    start = config.start_time.astimezone(timezone.utc)
    end = config.end_time.astimezone(timezone.utc)
    touchpoints = sorted((tp for tp in inventory.touchpoints if tp.enabled), key=lambda tp: tp.touchpoint_id)
    states = {tp.touchpoint_id: QueueState(tp, config.seed) for tp in touchpoints}
    fingerprints = {
        tp.touchpoint_id: hashlib.sha256(json.dumps({
            "seed": config.seed, "start": start.isoformat(),
            "interval": config.interval_seconds, "airport": inventory.airport_id,
            "touchpoint": tp.model_dump(),
        }, sort_keys=True).encode()).hexdigest()
        for tp in touchpoints
    }
    windows = int((end - start).total_seconds() / config.interval_seconds)
    for sequence in range(windows):
        cutoff = (sequence + 1) * config.interval_seconds
        window_end = start + timedelta(seconds=cutoff)
        for tp in touchpoints:
            state = states[tp.touchpoint_id]
            count, wait, service = state.advance(cutoff)
            payload = PassengerTouchpoint(
                touchpoint_id=tp.touchpoint_id, touchpoint_type=tp.touchpoint_type,
                zone_id=tp.zone_id, window_start=window_end - timedelta(seconds=config.interval_seconds),
                window_end=window_end, queue_length=len(state.waiting),
                passengers_in_service=len(state.serving), processed_passengers=count,
                avg_wait_time_seconds=wait, avg_processing_time_seconds=service,
            )
            yield EventEnvelope(
                event_id=uuid5(NAMESPACE_URL, f"airport-eye:{fingerprints[tp.touchpoint_id]}:{sequence}"),
                event_type="passenger.touchpoint", schema_version="1.0.0",
                airport_id=inventory.airport_id, terminal_id=tp.terminal_id,
                source_system="synthetic-touchpoint-generator", entity_id=tp.touchpoint_id,
                event_time=window_end, ingestion_time=window_end, sequence_no=sequence,
                payload=payload.model_dump(mode="json"),
            )


def write_dataset(config_path: str | Path) -> tuple[Path, int]:
    config, inventory, output = load_run(config_path)
    output.mkdir(parents=True, exist_ok=True)
    destination = output / "passenger_touchpoint.jsonl"
    count = 0
    # Replace a prior run only after the new file is fully written successfully.
    temporary = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=output,
                                prefix=".passenger_touchpoint-", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            for event in generate(config, inventory):
                handle.write(event.model_dump_json() + "\n")
                count += 1
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination, count
