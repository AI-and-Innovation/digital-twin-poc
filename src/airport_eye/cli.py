"""Scaffold inspection and validation commands. Run from the repository root."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from pydantic import ValidationError

from . import airports
from .common import load_summary
from .config import load_config
from .models import EventEnvelope
from .generators.passenger_touchpoint import write_dataset
from .esri import ZoneLocator
from .publishers.kafka import KafkaPublishError, KafkaPublisher


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    config_parser = sub.add_parser("validate-config")
    config_parser.add_argument("path", nargs="?", default="config/poc.json")
    sample_parser = sub.add_parser("validate-samples")
    sample_parser.add_argument("path", nargs="?", default="samples")
    generate_parser = sub.add_parser("generate")
    generate_parser.add_argument("stream", choices=["passenger_touchpoint", "facility", "passenger", "airport_operations", "aircraft"])
    generate_parser.add_argument("--config", help="Run config; defaults depend on the selected stream")
    publish_parser = sub.add_parser("publish", help="Publish a generated JSONL dataset to Kafka")
    publish_parser.add_argument("file", help="JSONL file of EventEnvelope records")
    publish_parser.add_argument("--config", default="config/poc.json", help="Kafka settings are read from this config")
    publish_parser.add_argument("--format", choices=["envelope", "esri"], default="envelope")
    publish_parser.add_argument("--zones", default="config/terminal_layout.json", help="zone_id -> lon/lat file used by --format esri")
    publish_parser.add_argument("--bootstrap", help="Override bootstrap servers (or set KAFKA_BOOTSTRAP_SERVERS)")
    publish_parser.add_argument("--limit", type=int, help="Publish only the first N records")
    publish_parser.add_argument("--dry-run", action="store_true", help="Print topic/key/value for the first records; no network")
    export_parser = sub.add_parser("export", help="Write a generated JSONL dataset as flat Esri-format JSONL (one record per line) for sharing")
    export_parser.add_argument("file")
    export_parser.add_argument("--out", required=True)
    export_parser.add_argument("--zones", default="config/terminal_layout.json")
    stream_parser = sub.add_parser("stream", help="Run the touchpoint simulation live, one batch per interval, publishing to Kafka")
    stream_parser.add_argument("--run-config", default="config/passenger_touchpoint.json")
    stream_parser.add_argument("--config", default="config/poc.json", help="Kafka settings are read from this config")
    stream_parser.add_argument("--format", choices=["envelope", "esri"], default="esri")
    stream_parser.add_argument("--zones", default="config/terminal_layout.json")
    stream_parser.add_argument("--bootstrap", help="Override bootstrap servers (or set KAFKA_BOOTSTRAP_SERVERS)")
    stream_parser.add_argument("--minutes", type=int, help="Stop after this many batches (default: run until Ctrl-C)")
    stream_parser.add_argument("--speed", type=float, default=1.0, help="Time compression; 1 = real time, 60 = a minute per second")
    stream_parser.add_argument("--no-create-topics", action="store_true")
    files_parser = sub.add_parser("queue-files", help="Write the passenger queue data to CSV, GeoJSON and JSON files (no Kafka)")
    files_parser.add_argument("--run-config", default="config/passenger_touchpoint.json")
    files_parser.add_argument("--zones", default="config/terminal_layout.json")
    files_parser.add_argument("--out", default="output", help="Folder for the files (default: output)")
    files_parser.add_argument("--start", type=datetime.fromisoformat,
                              help="Start time with offset, e.g. 2026-10-09T08:00:00+05:30 (default: the run config's start)")
    files_parser.add_argument("--hours", type=float, help="Length to simulate (default: the run config's range)")
    files_parser.add_argument("--live", action="store_true",
                              help="Run in real time: rewrite passenger_queue_latest.* every minute and append to passenger_queue_history.csv")
    files_parser.add_argument("--minutes", type=int, help="--live only: stop after this many minutes (default: until Ctrl-C)")
    files_parser.add_argument("--speed", type=float, default=1.0, help="--live only: time compression; 60 = a minute per second")
    pax_stream = sub.add_parser("pax-stream", help="Live terminal simulation to Kafka: positions every 5 s; queues, zones and journeys every minute")
    pax_files = sub.add_parser("pax-files", help="Terminal simulation to files: a time range as CSV/JSONL, or --live latest files")
    for p in (pax_stream, pax_files):
        p.add_argument("--tracking-config", default="config/passenger_tracking.json")
        p.add_argument("--zones", default="config/terminal_layout.json")
        p.add_argument("--start", type=datetime.fromisoformat,
                       help="Start time with offset, e.g. 2026-10-09T08:00:00+05:30 (default: now when live)")
        p.add_argument("--minutes", type=float, help="How long to run (default: until Ctrl-C when live, else the config range)")
        p.add_argument("--speed", type=float, default=1.0, help="Live only: time compression; 12 = 5 s of simulation per wall second")
        p.add_argument("--scenario", help="Named scenario from the tracking config, e.g. 'peak' (busy at any hour)")
        p.add_argument("--feeds", default="all",
                       help="Which feed groups to send: passengers, landside, airside (comma-separated) or all")
        airports.add_argument(p)
    pax_stream.add_argument("--config", default="config/poc.json", help="Kafka settings are read from this config")
    pax_stream.add_argument("--bootstrap", help="Override bootstrap servers (or set KAFKA_BOOTSTRAP_SERVERS)")
    pax_stream.add_argument("--no-create-topics", action="store_true")
    pax_stream.add_argument("--rest-proxy", nargs="?", const="config/rest_proxy.json", metavar="CONFIG",
                            help="Publish through a Kafka REST proxy instead of a broker (topics + URL from CONFIG, "
                                 "default config/rest_proxy.json; credentials from KAFKA_REST_USER / KAFKA_REST_PASSWORD)")
    pax_stream.add_argument("--interval", type=int,
                            help="Seconds between position batches (must divide 60; default from the tracking config, 5). "
                                 "15 or 30 cuts the position volume on a shared cluster")
    pax_files.add_argument("--out", default="output", help="Folder for the files (default: output)")
    pax_files.add_argument("--interval", type=int,
                           help="Seconds between position records (must divide 60; default 5). 30 keeps files small")
    pax_files.add_argument("--live", action="store_true",
                           help="Run in real time and rewrite passenger_positions_latest.csv/.geojson every interval")
    pax_files.add_argument("--history", action="store_true",
                           help="--live only: also append the per-minute feeds (queues, zones, journeys) to *_history.csv")
    pax_files.add_argument("--position-history", action="store_true",
                           help="--live only: also append every position batch to passenger_positions_history.csv (large)")
    args = parser.parse_args(argv)
    if getattr(args, "airport", None):
        airports.apply(args)
    try:
        if args.command == "publish":
            return _publish(args)
        if args.command == "stream":
            return _stream(args)
        if args.command == "export":
            return _export(args)
        if args.command == "queue-files":
            return _queue_files(args)
        if args.command == "pax-stream":
            return _pax_stream(args)
        if args.command == "pax-files":
            return _pax_files(args)
        if args.command == "validate-config":
            config = load_config(args.path)
            print(json.dumps(load_summary(config), indent=2))
        elif args.command == "validate-samples":
            paths = sorted(Path(args.path).glob("*.json"))
            if not paths:
                raise ValueError(f"No JSON samples found in {args.path}")
            for path in paths:
                EventEnvelope.model_validate_json(path.read_text(encoding="utf-8"))
            print(f"Validated {len(paths)} sample events")
        elif args.stream == "passenger_touchpoint":
            path, count = write_dataset(args.config or "config/passenger_touchpoint.json")
            print(f"Generated {count} events in {path}")
        else:
            load_config(args.config or "config/poc.json")
            raise NotImplementedError(f"{args.stream} generator is a scaffold; simulation is not implemented")
    except (OSError, ValueError, ValidationError, NotImplementedError, KafkaPublishError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


def _publish(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    locator = ZoneLocator.from_file(args.zones) if args.format == "esri" else None
    overrides = {"bootstrap.servers": args.bootstrap} if args.bootstrap else None
    publisher = KafkaPublisher(config.kafka, fmt=args.format, locator=locator, overrides=overrides)
    sent = 0
    with open(args.file, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            if args.limit is not None and sent >= args.limit:
                break
            event = EventEnvelope.model_validate_json(line)
            if args.dry_run:
                topic, key, value = publisher.prepare(event)
                if sent < 3:
                    print(json.dumps({"topic": topic, "key": key.decode(), "value": json.loads(value)}, indent=2))
            else:
                publisher.publish(event)
            sent += 1
    if args.dry_run:
        print(f"Dry run: prepared {sent} record(s) as '{args.format}'")
        return 0
    publisher.flush()
    stats = publisher.stats()
    print(f"Published {stats['delivered']}/{stats['queued']} records to {publisher.producer_settings()['bootstrap.servers']} (failed: {stats['failed']})")
    return 0


def _stream(args: argparse.Namespace) -> int:
    from .streaming import run_stream

    config = load_config(args.config)
    locator = ZoneLocator.from_file(args.zones) if args.format == "esri" else None
    overrides = {"bootstrap.servers": args.bootstrap} if args.bootstrap else None
    publisher = KafkaPublisher(config.kafka, fmt=args.format, locator=locator, overrides=overrides)
    print(f"Kafka: {publisher.producer_settings()['bootstrap.servers']}  format: {args.format}")
    try:
        run_stream(args.run_config, publisher, minutes=args.minutes, speed=args.speed,
                   create_topics=not args.no_create_topics)
    except KeyboardInterrupt:
        publisher.flush()
        print("Stopped.", publisher.stats())
    return 0


def _export(args: argparse.Namespace) -> int:
    from .esri import to_esri_record

    locator = ZoneLocator.from_file(args.zones)
    count = 0
    with open(args.file, encoding="utf-8") as source, open(args.out, "w", encoding="utf-8", newline="\n") as target:
        for line in source:
            if line.strip():
                record = to_esri_record(EventEnvelope.model_validate_json(line), locator)
                target.write(json.dumps(record, separators=(",", ":")) + "\n")
                count += 1
    print(f"Wrote {count} Esri-format records to {args.out}")
    return 0


def _queue_files(args: argparse.Namespace) -> int:
    from .queue_files import QueueFileWriter, write_range

    locator = ZoneLocator.from_file(args.zones)
    if not args.live:
        if args.start is not None and args.start.tzinfo is None:
            raise ValueError("--start needs a UTC offset, e.g. 2026-10-09T08:00:00+05:30")
        paths, count = write_range(args.run_config, locator, args.out, start=args.start, hours=args.hours)
        print(f"Wrote {count} records:")
        for path in paths:
            print(f"  {path}")
        return 0
    from .streaming import run_stream

    writer = QueueFileWriter(args.out, locator)
    print(f"Writing {Path(args.out) / 'passenger_queue_latest.csv'} (.geojson, .json) every minute; "
          f"history in {writer.history}. Ctrl-C to stop.")
    try:
        run_stream(args.run_config, writer, minutes=args.minutes, speed=args.speed, start=args.start,
                   create_topics=False)
    except KeyboardInterrupt:
        writer.flush()
        print("Stopped.", writer.stats())
    return 0


def _check_start(start: datetime | None) -> None:
    if start is not None and start.tzinfo is None:
        raise ValueError("--start needs a UTC offset, e.g. 2026-10-09T08:00:00+05:30")


def _pax_stream(args: argparse.Namespace) -> int:
    from .tracking import run_tracking_stream, select_feeds

    _check_start(args.start)
    locator = ZoneLocator.from_file(args.zones)
    if args.rest_proxy:
        from .publishers.rest_proxy import RestProxyConfig, RestProxyPublisher

        publisher = RestProxyPublisher(RestProxyConfig.load(args.rest_proxy), locator)
        print(f"Kafka REST proxy: {publisher.url}")
        for event_type, topic in publisher.config.topics.items():
            print(f"  {event_type:24s} -> {topic}")
    else:
        config = load_config(args.config)
        overrides = {"bootstrap.servers": args.bootstrap} if args.bootstrap else None
        kafka = config.kafka
        if args.airport != "hyd":   # keep each airport's local topics apart
            kafka = kafka.model_copy(update={"topic_prefix": airports.AIRPORTS[args.airport]["kafka_prefix"]})
        publisher = KafkaPublisher(kafka, fmt="esri", locator=locator, overrides=overrides)
        print(f"Kafka: {publisher.producer_settings()['bootstrap.servers']}  topic: {publisher.topic_for_type('passenger.position')}")
    try:
        run_tracking_stream(args.tracking_config, publisher, minutes=args.minutes, speed=args.speed,
                            start=args.start, create_topics=not args.no_create_topics, scenario=args.scenario,
                            interval=args.interval, feeds=select_feeds(args.feeds))
    except KeyboardInterrupt:
        publisher.flush()
        print("Stopped.", publisher.stats())
    return 0


def _pax_files(args: argparse.Namespace) -> int:
    from .tracking import TwinFileWriter, run_tracking_stream, select_feeds, write_tracking_range

    _check_start(args.start)
    locator = ZoneLocator.from_file(args.zones)
    if not args.live:
        paths, count = write_tracking_range(args.tracking_config, locator, args.out, start=args.start,
                                            minutes=args.minutes, scenario=args.scenario,
                                            feeds=select_feeds(args.feeds), interval=args.interval)
        print(f"Wrote {count} records:")
        for path in paths:
            print(f"  {path}")
        return 0
    layout = json.loads(Path(args.zones).read_text(encoding="utf-8"))
    writer = TwinFileWriter(args.out, locator, layout, history=args.history, position_history=args.position_history)
    print(f"Writing {args.out}/passenger_positions_latest.* every interval and passenger_queues_latest.*, "
          f"zone_occupancy_latest.*, journey_kpis_latest.* every minute. Ctrl-C to stop.")
    try:
        run_tracking_stream(args.tracking_config, writer, minutes=args.minutes, speed=args.speed,
                            start=args.start, create_topics=False, scenario=args.scenario,
                            feeds=select_feeds(args.feeds))
    except KeyboardInterrupt:
        print("Stopped.", writer.stats())
    return 0
