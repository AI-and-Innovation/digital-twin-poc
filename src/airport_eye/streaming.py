"""Live, paced publishing of the touchpoint queue simulation.

One batch per interval (default 60 s): every active touchpoint publishes one
record when its simulated window closes. The first batch is published right
away; event times are never ahead of the wall clock at speed 1. ``speed`` compresses time for testing
(speed=60 turns a simulated minute into one second). Use speed=1 for anything
that ingests the feed with real-time semantics, because event times are the
simulated window ends.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path
from typing import Any

from .generators.passenger_touchpoint import active_touchpoints, generate
from .publishers.kafka import KafkaPublisher
from .touchpoint_config import load_run, load_targets


def floor_to_interval(moment: datetime, seconds: int) -> datetime:
    epoch = int(moment.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=timezone.utc)


def run_stream(
    run_config_path: str | Path,
    publisher: KafkaPublisher,
    *,
    minutes: int | None = None,
    speed: float = 1.0,
    start: datetime | None = None,
    create_topics: bool = True,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> dict[str, int]:
    """Publish ``minutes`` batches (None = until interrupted). Returns publisher stats."""
    if speed <= 0:
        raise ValueError("speed must be > 0")
    config, inventory, _ = load_run(run_config_path)
    targets = load_targets(run_config_path, config)
    interval = config.interval_seconds
    # ``origin`` is the simulated "now". The first window closes at the origin, so the
    # first batch goes out immediately (its event time is never in the future) and the
    # next ones follow one interval apart.
    origin = floor_to_interval(start or datetime.now(timezone.utc), interval)
    first = origin - timedelta(seconds=interval)
    horizon = timedelta(days=3650) if minutes is None else timedelta(seconds=interval * minutes)
    live = config.model_copy(update={"start_time": first, "end_time": first + horizon})
    points = active_touchpoints(live, inventory)
    log(f"Streaming {len(points)} touchpoints "
        f"({', '.join(sorted({p.touchpoint_type for p in points}))}), one batch per {interval}s, speed x{speed:g}")
    if create_topics:
        created = publisher.ensure_topics([publisher.topic_for_type("passenger.touchpoint")])
        log(f"Topics ready: {publisher.topic_for_type('passenger.touchpoint')}" + (" (created)" if created else ""))
    batches = ((window_end, list(events)) for window_end, events in
               groupby(generate(live, inventory, targets), key=lambda e: e.event_time))
    publish_paced(batches, publisher, origin=origin, speed=speed, clock=clock, sleep=sleep, log=log)
    return publisher.stats()


def publish_paced(
    batches: Iterable[tuple[datetime, list]],
    publisher: Any,
    *,
    origin: datetime,
    speed: float = 1.0,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
    every: int = 1,
) -> None:
    """Publish each (event time, records) batch when its event time is due.

    ``origin`` is the event time that maps to "now"; at ``speed`` 1 a batch for
    origin + 60 s goes out 60 s after the first call. ``every`` thins the log.
    The default clock is the wall clock, so after the machine sleeps the overdue
    batches go out straight away and the feed is back on time.
    """
    began = clock()
    count = 0
    for when, events in batches:
        due = (when - origin).total_seconds() / speed
        wait = due - (clock() - began)
        if wait > 0:
            sleep(wait)
        for event in events:
            publisher.publish(event)
        publisher.flush()
        count += 1
        if count % every == 0 or count == 1:
            stats = publisher.stats()
            log(f"[{when.strftime('%H:%M:%SZ')}] batch {count}: {len(events)} records "
                f"(delivered {stats['delivered']}, failed {stats['failed']})")
